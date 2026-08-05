"""Long-running Tendwire daemon lifecycle skeleton."""

from __future__ import annotations

import json
import signal
import threading
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from .config import Config
from .core.commands import CommandEnvelope
from .core.models import Snapshot, sanitize_public_mapping, utc_timestamp
from .daemon_api import (
    TendwireDaemonAPI,
    UnixSocketJSONServer,
    ensure_daemon_socket_not_active,
)
from .local_state import repair_config_state


def _valid_observation_timestamp(value: str | None) -> str | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    return value if parsed.tzinfo is not None and parsed.utcoffset() is not None else None


def _nonnegative_int(value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        return 0
    return max(0, value)


def _public_failure_type(value: Any) -> str | None:
    """Return a bounded exception type label, never arbitrary failure text."""
    if not isinstance(value, str) or not value or len(value) > 128:
        return None
    if any(not (character.isalnum() or character in "._") for character in value):
        return None
    return value


def _pending_ingestion_health(config: Config) -> dict[str, Any]:
    """Return the fixed durable pending aggregate without exposing row identity."""
    unavailable = {
        "status": "store_unavailable",
        "counts": {"fresh": 0, "stale": 0, "total": 0},
    }
    try:
        from .store.projection import backend_pending_health

        value = backend_pending_health(Path(config.db_path), config.host_id)
    except Exception:
        value = unavailable
    raw: Mapping[str, Any] = value if isinstance(value, Mapping) else unavailable
    raw_counts = raw.get("counts")
    status = raw.get("status")
    count_values = (
        tuple(raw_counts.get(key) for key in ("fresh", "stale", "total"))
        if isinstance(raw_counts, Mapping)
        else ()
    )
    valid_counts = len(count_values) == 3 and all(
        isinstance(value, int) and not isinstance(value, bool) and value >= 0
        for value in count_values
    )
    if (
        status not in {"healthy", "degraded", "store_unavailable"}
        or not valid_counts
        or count_values[2] != count_values[0] + count_values[1]
        or (status == "healthy" and count_values[1] != 0)
        or (status == "degraded" and count_values[1] == 0)
        or (status == "store_unavailable" and count_values != (0, 0, 0))
    ):
        status = "store_unavailable"
        counts = dict(unavailable["counts"])
    else:
        counts = dict(zip(("fresh", "stale", "total"), count_values, strict=True))
    return {
        "status": status,
        "counts": counts,
    }


def default_socket_path(config: Config) -> Path:
    """Return the daemon socket path for this config."""
    if config.socket_path is not None:
        return Path(config.socket_path)
    return Path(config.data_dir) / "tendwire.sock"


def _default_init_store(db_path: Path) -> None:
    from .store.schema import init_store

    init_store(db_path)


def _default_acp_supervisor_factory(config: Config, stop_event: threading.Event) -> Any:
    from .backends.acp_coordinator import production_acp_supervisor_factory

    return production_acp_supervisor_factory(config, stop_event)


@dataclass(frozen=True)
class DaemonHooks:
    """Dependency injection points for deterministic daemon tests."""

    init_store: Callable[[Path], None] = _default_init_store
    acp_supervisor_factory: Callable[[Config, threading.Event], Any | None] | None = (
        _default_acp_supervisor_factory
    )


class TendwireDaemon:
    """Owns store initialization, initial observation, API dispatch, and shutdown."""

    def __init__(
        self,
        config: Config,
        *,
        socket_path: str | Path | None = None,
        hooks: DaemonHooks | None = None,
        stop_event: threading.Event | None = None,
    ) -> None:
        self.config = config
        self.socket_path = Path(socket_path) if socket_path is not None else default_socket_path(config)
        self._prepare_socket_parent = socket_path is None and config.socket_path is None
        self.hooks = hooks or DaemonHooks()
        self.stop_event = stop_event or threading.Event()
        self.started_at = utc_timestamp()
        self._snapshot: Snapshot | None = None
        self._server: UnixSocketJSONServer | None = None
        self._acp_supervisor: Any | None = None
        self._acp_startup_failure_type: str | None = None
        self._stop_lock = threading.Lock()
        self._automatic_maintenance_status: dict[str, Any] | None = None
        self._last_retention_cycle_monotonic: float | None = None

    @property
    def snapshot(self) -> Snapshot | None:
        return self._snapshot

    @property
    def server(self) -> UnixSocketJSONServer | None:
        return self._server

    def start(self) -> None:
        if self._server is not None and self._server.listening:
            return
        if self.stop_event.is_set():
            raise RuntimeError("daemon cannot start after shutdown")
        server: UnixSocketJSONServer | None = None
        try:
            repair_config_state(
                self.config.data_dir,
                self.config.db_path,
                private_files=(
                    self.config.installation_key_path,
                    self.config.installation_key_marker_path,
                    self.config.installation_key_sentinel_path,
                ),
            )
            ensure_daemon_socket_not_active(
                self.socket_path,
                socket_group=self.config.socket_group,
            )
            if self.hooks.init_store is _default_init_store:
                _default_init_store(Path(self.config.db_path))
            else:
                self.hooks.init_store(Path(self.config.db_path))
            api = TendwireDaemonAPI(
                get_snapshot=self.get_snapshot,
                get_health=self.get_health,
                submit_command=self.submit_command,
                get_attention=self.get_attention,
                get_turns=self.get_turns,
                get_turn_delta=self.get_turn_delta,
                get_turn_content=self.get_turn_content,
                get_pending=self.get_pending,
                connector_call=self.connector_call,
            )
            server = UnixSocketJSONServer(
                self.socket_path,
                api.dispatch,
                stop_event=self.stop_event,
                socket_group=self.config.socket_group,
                prepare_parent=self._prepare_socket_parent,
                periodic_callback=self._connector_periodic_tick,
            )
            # Bind before ACP runtime/consumer threads can take store locks.
            # No connections are accepted until serve_forever(), after
            # this startup transaction has succeeded.
            server.start()
            self._connector_periodic_tick()
            self._start_acp_supervisor()
            from .store.projection import latest_snapshot

            self._snapshot = latest_snapshot(
                Path(self.config.db_path), self.config.host_id
            )
            if self._snapshot is None:
                raise RuntimeError("ACP supervisor did not publish a lifecycle snapshot")
            self._after_snapshot_saved()
            self._server = server

        except Exception:
            self.stop_event.set()
            supervisor = self._acp_supervisor
            self._acp_supervisor = None
            self._stop_acp_supervisor(supervisor)
            self._server = None
            if server is not None:
                try:
                    server.close()
                except Exception:
                    pass
            self._snapshot = None
            raise

    def serve_forever(self) -> None:
        if self._server is None:
            self.start()
        server = self._server
        if server is None:
            raise RuntimeError("daemon server did not start")
        server.serve_forever()

    def request_stop(self) -> None:
        """Request shutdown without performing teardown in signal context.

        Python signal handlers run on the main thread between bytecode
        instructions.  They must not enter the daemon's lifecycle locks or
        wait for worker threads: a second signal, or a signal delivered while
        cleanup already owns one of those locks, could otherwise deadlock the
        process.  The socket loop observes this event within its bounded
        accept timeout and performs normal teardown outside the handler.
        """
        self.stop_event.set()

    def stop(self) -> None:
        with self._stop_lock:
            self.stop_event.set()
            server = self._server
            supervisor = self._acp_supervisor
            self._server = None
            self._acp_supervisor = None

        if server is not None:
            try:
                server.close()
            except Exception:
                pass

        self._stop_acp_supervisor(supervisor)

    def _start_acp_supervisor(self) -> None:
        """Start the required ACP supervisor and fail the daemon closed."""
        self._acp_startup_failure_type = None

        factory = self.hooks.acp_supervisor_factory
        if factory is None:
            raise RuntimeError("ACP supervisor is required but unavailable")

        supervisor: Any | None = None
        try:
            supervisor = factory(self.config, self.stop_event)
            if supervisor is None:
                raise RuntimeError("ACP supervisor is required but unavailable")
            self._acp_supervisor = supervisor
            supervisor.start()
            health = self._acp_supervisor_health()
            if health["healthy"] is not True:
                failure_type = health.get("failure_type")
                self._acp_startup_failure_type = _public_failure_type(failure_type)
                raise RuntimeError("ACP supervisor did not become healthy")
        except Exception as exc:
            self._acp_startup_failure_type = (
                self._acp_startup_failure_type or type(exc).__name__
            )
            if supervisor is not None:
                self._stop_acp_supervisor(supervisor)
            self._acp_supervisor = None
            raise RuntimeError(
                "ACP supervisor is required but failed to start "
                f"({self._acp_startup_failure_type})"
            ) from None

    def _stop_acp_supervisor(self, supervisor: Any | None) -> None:
        """Best-effort bounded shutdown for the ACP supervisor."""
        if supervisor is None:
            return
        timeout = self.config.acp_shutdown_timeout_seconds
        stop = getattr(supervisor, "stop", None)
        if callable(stop):
            try:
                stop(timeout=timeout)
            except Exception:
                pass
        join = getattr(supervisor, "join", None)
        if callable(join):
            try:
                join(timeout=timeout)
            except Exception:
                pass

    def _acp_supervisor_health(self) -> dict[str, Any]:
        """Return a fixed, public-safe ACP lifecycle aggregate."""
        counters = {
            "updates_ingested": 0,
            "permissions_ingested": 0,
            "permissions_selected": 0,
            "permissions_cancelled": 0,
            "invalid_permission_selections": 0,
            "prompts_started": 0,
            "prompts_completed": 0,
            "prompts_failed": 0,
            "cancellation_requests": 0,
        }
        supervisor = self._acp_supervisor
        if supervisor is None:
            return {
                "required": True,
                "status": "unavailable",
                "healthy": False,
                "state": "unavailable",
                "failure_type": self._acp_startup_failure_type,
                "counters": counters,
            }

        status_method = getattr(supervisor, "status", None)
        try:
            raw = status_method() if callable(status_method) else None
        except Exception as exc:
            return {
                "required": True,
                "status": "degraded",
                "healthy": False,
                "state": "failed",
                "failure_type": type(exc).__name__,
                "counters": counters,
            }

        def field(name: str) -> Any:
            if isinstance(raw, Mapping):
                return raw.get(name)
            return getattr(raw, name, None)

        state_value = field("state")
        state = getattr(state_value, "value", state_value)
        if state not in {"new", "starting", "running", "stopping", "stopped", "failed"}:
            state = "unknown"
        healthy = field("healthy") is True and state == "running"
        for key in counters:
            counters[key] = _nonnegative_int(field(key))
        failure_type_value = field("failure_type")
        failure_type = _public_failure_type(failure_type_value)
        return {
            "required": True,
            "status": "healthy" if healthy else "degraded",
            "healthy": healthy,
            "state": state,
            "failure_type": failure_type,
            "last_reconcile_at": _valid_observation_timestamp(
                field("last_reconcile_at")
            ),
            "worker_count": _nonnegative_int(field("worker_count")),
            "counters": counters,
        }

    def _after_snapshot_saved(self) -> None:
        from .store.retention import RetentionPolicy, run_retention_cycle

        now = time.monotonic()
        previous = self._last_retention_cycle_monotonic
        if previous is not None and now - previous < self.config.store_maintenance_cadence_seconds:
            return
        self._last_retention_cycle_monotonic = now
        policy = RetentionPolicy(
            event_retention_days=self.config.event_retention_days,
            snapshot_retention_days=self.config.snapshot_retention_days,
            targetable_retention_days=max(
                30, self.config.acknowledged_final_retention_days
            ),
            route_content_retention_days=max(
                45, self.config.acknowledged_final_retention_days
            ),
            command_retention_days=(
                self.config.command_receipt_retention_seconds + 86_399
            ) // 86_400,
            batch_size=self.config.snapshot_maintenance_batch_size,
            turn_change_retention_days=self.config.turn_change_retention_days,
            turn_change_batch_size=self.config.turn_change_compaction_batch_size,
        )
        try:
            result = run_retention_cycle(Path(self.config.db_path), policy=policy)
            maintenance_status = {"ok": True, "status": "ok", "due": True, "result": result}
        except Exception:
            self._last_retention_cycle_monotonic = previous
            self._automatic_maintenance_status = {
                "ok": False,
                "status": "failed",
                "due": False,
                "examined": 0,
                "deleted": 0,
                "remaining_candidates": False,
                "agent_events_examined": 0,
                "agent_events_deleted": 0,
                "agent_events_remaining_candidates": False,
            }
        else:
            self._automatic_maintenance_status = maintenance_status

    def get_snapshot(self) -> Snapshot:
        from .store.projection import latest_snapshot

        snapshot = latest_snapshot(Path(self.config.db_path), self.config.host_id)
        if snapshot is not None:
            self._snapshot = snapshot
            return snapshot
        if self._snapshot is not None:
            return self._snapshot
        raise RuntimeError("daemon has no initial snapshot")

    def get_health(self) -> dict[str, Any]:
        snapshot = self.get_snapshot()
        from .store.db import store_status
        from .store.schema import STORE_SCHEMA_VERSION

        raw_store = store_status(Path(self.config.db_path), self.config.host_id)
        raw_counts = raw_store.get("counts")
        store_ok = (
            type(raw_store.get("schema_version")) is int
            and raw_store.get("schema_version") == 1
            and raw_store.get("ok") is True
            and raw_store.get("status") == "ok"
            and raw_store.get("host_id") == self.config.host_id
            and type(raw_store.get("store_schema_version")) is int
            and raw_store.get("store_schema_version") == STORE_SCHEMA_VERSION
            and isinstance(raw_counts, Mapping)
            and set(raw_counts) == {"turns", "agent_events", "connector_outbox"}
            and all(type(value) is int and value >= 0 for value in raw_counts.values())
        )
        counts = (
            {
                "turns": raw_counts["turns"],
                "agent_events": raw_counts["agent_events"],
                "outbox": raw_counts["connector_outbox"],
            }
            if store_ok
            else {"turns": 0, "agent_events": 0, "outbox": 0}
        )
        acp_health = self._acp_supervisor_health()
        maintenance = self._automatic_maintenance_status
        maintenance_ok = maintenance is None or maintenance.get("ok") is True
        pending = _pending_ingestion_health(self.config)
        healthy = (
            store_ok and maintenance_ok and acp_health["healthy"] is True
            and pending["status"] == "healthy"
        )
        return sanitize_public_mapping({
            "schema_version": 1,
            "status": "ok" if healthy else "degraded",
            "host_id": self.config.host_id,
            "daemon": {"status": "healthy", "started_at": self.started_at},
            "store": {
                "status": (
                    "unavailable" if not store_ok else "healthy" if maintenance_ok else "degraded"
                ),
                "schema_version": raw_store.get("store_schema_version") if store_ok else None,
                "counts": counts,
                "maintenance": None if maintenance is None else dict(maintenance),
            },
            "snapshot": {
                "updated_at": snapshot.updated_at,
                "content_fingerprint": snapshot.content_fingerprint,
            },
            "timestamps": {
                "last_snapshot_at": snapshot.updated_at,
                "last_reconcile_at": acp_health.get("last_reconcile_at"),
            },
            "backend": {
                "status": "healthy" if acp_health["healthy"] else "degraded",
                "ready": acp_health["healthy"],
                "running": acp_health.get("state") == "running",
            },
            "acp": acp_health,
            "pending_ingestion": pending,
            "limits": {
                "reconcile_interval_seconds": self.config.reconcile_interval_seconds,
                "event_retention_days": self.config.event_retention_days,
                "max_outbox_attempts": self.config.max_outbox_attempts,
            },
            "backend_health": [health.to_dict() for health in snapshot.backend_health],
        })

    def get_attention(self) -> Mapping[str, Any]:
        from .store.projection import attention_payload_from_store

        payload = attention_payload_from_store(
            Path(self.config.db_path),
            self.config.host_id,
        )
        return payload or {
            "schema_version": 1,
            "host_id": self.config.host_id,
            "ok": False,
            "status": "store_unavailable",
            "attention": [],
        }

    def get_pending(self) -> Mapping[str, Any]:
        """Return the durable pending projection exposed by the daemon."""
        from .store.pending import pending_payload_from_store

        return pending_payload_from_store(
            Path(self.config.db_path),
            self.config.host_id,
        )

    def get_turns(
        self,
        *,
        schema_version: int = 1,
        limit: int = 100,
        cursor: str | None = None,
        since: str | None = None,
    ) -> Mapping[str, Any]:
        from .store.turns import turns_payload_from_store

        return turns_payload_from_store(
            Path(self.config.db_path),
            self.config.host_id,
            snapshot=self.get_snapshot(),
            schema_version=schema_version,
            limit=limit,
            cursor=cursor,
            since=since,
        )

    def get_turn_content(self, params: Mapping[str, Any]) -> Mapping[str, Any]:
        """turn.content.get: return one immutable bounded canonical page."""
        from .store.turns import get_turn_content

        return get_turn_content(
            Path(self.config.db_path),
            self.config.host_id,
            turn_id=params.get("turn_id"),
            content_revision=params.get("content_revision"),
            field=params.get("field"),
            cursor=params.get("cursor"),
            schema_version=params.get("schema_version", 1),
        )

    def get_turn_delta(
        self,
        *,
        watermark: str | None = None,
        cursor: str | None = None,
        limit: int = 100,
    ) -> Mapping[str, Any]:
        """Read one cache-only delta page; this surface has no delivery authority."""
        from .store.turns import turn_delta_payload_from_store

        return turn_delta_payload_from_store(
            Path(self.config.db_path),
            self.config.host_id,
            watermark=watermark,
            cursor=cursor,
            limit=limit,
        )

    def connector_call(self, method: str, params: Mapping[str, Any]) -> Mapping[str, Any]:
        from .connectors import ConnectorOutboxAPI

        return ConnectorOutboxAPI(
            Path(self.config.db_path),
            self.config.host_id,
            default_lease_seconds=self.config.connector_claim_ttl_seconds,
            max_lease_seconds=self.config.connector_max_claim_ttl_seconds,
            ack_ttl_seconds=self.config.connector_ack_ttl_seconds,
            max_attempts=self.config.max_outbox_attempts,
        ).dispatch(method, params)

    def _connector_periodic_tick(self) -> None:
        """Eagerly reclaim expired connector work without waiting for a poll."""
        from .store.outbox import (
            connector_reclaim_due,
            reclaim_expired_connector_leases,
        )

        try:
            if not connector_reclaim_due(
                Path(self.config.db_path),
                self.config.host_id,
                None,
            ):
                return
            reclaim_expired_connector_leases(
                Path(self.config.db_path),
                self.config.host_id,
                None,
            )
        except Exception:
            # Startup and periodic maintenance remain best-effort; store health
            # is reported through the normal daemon health surface.
            return

    def submit_command(self, params: Mapping[str, Any]) -> CommandEnvelope | Mapping[str, Any]:
        # Preserve the submitted keys exactly so the existing command parser can
        # reject private/connector fields instead of receiving sanitized input.
        payload = json.dumps(
            dict(params),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        supervisor = self._acp_supervisor
        route = getattr(supervisor, "prompt_route", None)
        permission_router = (
            supervisor
            if callable(getattr(supervisor, "answer_permission_decision", None))
            else None
        )
        from .command_submission import submit_command

        return submit_command(
            self.config,
            payload,
            acp_prompt_router=route if callable(route) else None,
            acp_permission_router=permission_router,
        )


def run_daemon(
    config: Config,
    *,
    socket_path: str | Path | None = None,
    hooks: DaemonHooks | None = None,
    install_signal_handlers: bool = True,
) -> int:
    """Run the daemon until SIGINT, SIGTERM, or an injected stop event."""
    daemon = TendwireDaemon(config, socket_path=socket_path, hooks=hooks)
    previous_handlers: dict[int, Any] = {}

    def _handle_stop(_signum: int, _frame: Any) -> None:
        daemon.request_stop()

    if install_signal_handlers:
        for signum in (signal.SIGINT, signal.SIGTERM):
            previous_handlers[signum] = signal.getsignal(signum)
            signal.signal(signum, _handle_stop)

    try:
        daemon.start()
        daemon.serve_forever()
        return 0
    except KeyboardInterrupt:
        daemon.stop()
        return 0
    finally:
        daemon.stop()
        if install_signal_handlers:
            for signum, handler in previous_handlers.items():
                signal.signal(signum, handler)
