"""Long-running Tendwire daemon lifecycle skeleton."""

from __future__ import annotations

import json
import signal
import threading
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


def _nonnegative_float(value: Any) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    converted = float(value)
    if converted < 0 or converted != converted or converted in {float("inf"), float("-inf")}:
        return None
    return converted


_STORE_COUNT_FIELDS = (
    "snapshots",
    "events",
    "spaces",
    "workers",
    "turns",
    "pending_interactions",
    "attention_items",
    "commands",
    "command_receipts",
    "backend_health",
)
_OUTBOX_PUBLIC_STATUSES = frozenset(
    {
        "queued",
        "leased",
        "awaiting_ack",
        "delivered",
        "deferred",
        "retry",
        "dead_letter",
        "superseded",
        "unknown",
    }
)


def _validated_nonnegative_int(value: Any) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        return None
    return value


def _store_counts_health(value: Any) -> tuple[dict[str, int], bool]:
    fallback = {field: 0 for field in _STORE_COUNT_FIELDS}
    if not isinstance(value, Mapping) or set(value) != set(_STORE_COUNT_FIELDS):
        return fallback, False
    parsed = {
        field: _validated_nonnegative_int(value.get(field))
        for field in _STORE_COUNT_FIELDS
    }
    if any(count is None for count in parsed.values()):
        return fallback, False
    return {field: int(parsed[field]) for field in _STORE_COUNT_FIELDS}, True


def _outbox_health(value: Any) -> tuple[dict[str, Any], bool]:
    fallback = {
        "pending": 0,
        "leased": 0,
        "completed": 0,
        "by_status": {},
        "due": 0,
        "oldest_due_at": None,
        "overdue_awaiting_ack": 0,
        "drain_target_seconds": 30,
        "starved": False,
    }
    if not isinstance(value, Mapping) or set(value) != {
        "pending",
        "leased",
        "completed",
        "by_status",
        "due",
        "oldest_due_at",
        "overdue_awaiting_ack",
        "drain_target_seconds",
        "starved",
    }:
        return fallback, False
    by_status_value = value.get("by_status")
    if not isinstance(by_status_value, Mapping):
        return fallback, False
    by_status: dict[str, int] = {}
    for key, count_value in by_status_value.items():
        if not isinstance(key, str) or key not in _OUTBOX_PUBLIC_STATUSES:
            return fallback, False
        count = _validated_nonnegative_int(count_value)
        if count is None:
            return fallback, False
        by_status[key] = count
    pending = _validated_nonnegative_int(value.get("pending"))
    leased = _validated_nonnegative_int(value.get("leased"))
    completed = _validated_nonnegative_int(value.get("completed"))
    due = _validated_nonnegative_int(value.get("due"))
    overdue_awaiting_ack = _validated_nonnegative_int(
        value.get("overdue_awaiting_ack")
    )
    drain_target_seconds = _validated_nonnegative_int(
        value.get("drain_target_seconds")
    )
    oldest_due_at = value.get("oldest_due_at")
    starved = value.get("starved")
    if (
        pending is None
        or leased is None
        or completed is None
        or due is None
        or overdue_awaiting_ack is None
        or drain_target_seconds != 30
        or not isinstance(starved, bool)
        or (
            oldest_due_at is not None
            and _valid_observation_timestamp(oldest_due_at) is None
        )
        or due > pending
        or (due == 0 and oldest_due_at is not None)
        or (due > 0 and oldest_due_at is None)
        or pending
        != sum(by_status.get(status, 0) for status in ("queued", "deferred", "retry"))
        or leased != by_status.get("leased", 0)
        or completed
        != sum(by_status.get(status, 0) for status in ("delivered", "superseded"))
    ):
        return fallback, False
    return {
        "pending": pending,
        "leased": leased,
        "completed": completed,
        "by_status": by_status,
        "due": due,
        "oldest_due_at": oldest_due_at,
        "overdue_awaiting_ack": overdue_awaiting_ack,
        "drain_target_seconds": drain_target_seconds,
        "starved": starved,
    }, True


def _maintenance_health(
    config: Config,
    value: Any,
) -> tuple[dict[str, Any], bool]:
    fallback = {
        "last_completed_at": None,
        "status": "not_initialized",
        "snapshot_count": 0,
        "snapshot_retention_days": config.snapshot_retention_days,
        "snapshot_retention_count": config.snapshot_retention_count,
        "maintenance_batch_size": config.snapshot_maintenance_batch_size,
        "maintenance_cadence_seconds": config.store_maintenance_cadence_seconds,
        "backlog": False,
    }
    if not isinstance(value, Mapping) or set(value) != set(fallback):
        return fallback, False
    snapshot_count = _validated_nonnegative_int(value.get("snapshot_count"))
    last_completed_value = value.get("last_completed_at")
    last_completed_at = (
        None
        if last_completed_value is None
        else _valid_observation_timestamp(
            last_completed_value if isinstance(last_completed_value, str) else None
        )
    )
    status = value.get("status")
    policy_values = (
        ("snapshot_retention_days", config.snapshot_retention_days),
        ("snapshot_retention_count", config.snapshot_retention_count),
        ("maintenance_batch_size", config.snapshot_maintenance_batch_size),
        ("maintenance_cadence_seconds", config.store_maintenance_cadence_seconds),
    )
    valid = (
        isinstance(status, str)
        and status in {"not_initialized", "never", "ok", "failed"}
        and snapshot_count is not None
        and (
            last_completed_value is None
            or last_completed_at is not None
        )
        and all(
            type(value.get(field)) is int
            and value.get(field) == expected
            for field, expected in policy_values
        )
        and isinstance(value.get("backlog"), bool)
    )
    if not valid:
        return fallback, False
    return {
        **fallback,
        "last_completed_at": last_completed_at,
        "status": str(value["status"]),
        "snapshot_count": snapshot_count,
        "backlog": bool(value["backlog"]),
    }, True


_FINAL_RETENTION_COUNT_FIELDS = (
    "acknowledged",
    "unresolved",
    "queued",
    "leased",
    "deferred",
    "retry",
    "dead_letter",
    "awaiting_ack",
    "eligible",
)


def _final_retention_health(
    config: Config,
    value: Any,
) -> tuple[dict[str, Any], bool]:
    """Validate the fixed public retention aggregate without exposing row data."""
    fallback = {
        **{key: 0 for key in _FINAL_RETENTION_COUNT_FIELDS},
        "acknowledged_final_retention_days": (
            config.acknowledged_final_retention_days
        ),
        "acknowledged_final_retention_count": (
            config.acknowledged_final_retention_count
        ),
        "storage_pressure": False,
    }
    if not isinstance(value, Mapping):
        return fallback, False
    counts = tuple(value.get(key) for key in _FINAL_RETENTION_COUNT_FIELDS)
    valid_counts = all(
        isinstance(count, int) and not isinstance(count, bool) and count >= 0
        for count in counts
    )
    days_value = value.get("acknowledged_final_retention_days")
    count_value = value.get("acknowledged_final_retention_count")
    valid_policy = (
        type(days_value) is int
        and days_value == config.acknowledged_final_retention_days
        and type(count_value) is int
        and count_value == config.acknowledged_final_retention_count
    )
    storage_pressure = value.get("storage_pressure")
    component_counts = counts[2:-1]
    if (
        not valid_counts
        or not valid_policy
        or not isinstance(storage_pressure, bool)
        or counts[-1] > counts[0]
        or any(component > counts[1] for component in component_counts)
        or sum(component_counts) > counts[1]
        or (
            (counts[-1] > 0 or counts[1] > config.acknowledged_final_retention_count)
            and storage_pressure is not True
        )
    ):
        return fallback, False
    return {
        **dict(zip(_FINAL_RETENTION_COUNT_FIELDS, counts, strict=True)),
        "acknowledged_final_retention_days": (
            config.acknowledged_final_retention_days
        ),
        "acknowledged_final_retention_count": (
            config.acknowledged_final_retention_count
        ),
        "storage_pressure": storage_pressure,
    }, True


_COMMAND_REQUEST_STATES = (
    "reserved",
    "send_started",
    "accepted",
    "rejected",
    "uncertain",
)


def _command_requests_health(
    config: Config,
    value: Any,
) -> tuple[dict[str, Any], bool]:
    """Validate the fixed public command-request aggregate without row data."""
    fallback = {
        "total": 0,
        "states": {state: 0 for state in _COMMAND_REQUEST_STATES},
        "stale_active": 0,
        "eligible": 0,
        "retry_horizon_seconds": config.command_retry_horizon_seconds,
        "retention_seconds": config.command_receipt_retention_seconds,
        "retention_count": config.command_receipt_retention_count,
        "storage_pressure": False,
    }
    if not isinstance(value, Mapping) or set(value) != set(fallback):
        return fallback, False
    states_value = value.get("states")
    if (
        not isinstance(states_value, Mapping)
        or set(states_value) != set(_COMMAND_REQUEST_STATES)
    ):
        return fallback, False
    states = {
        state: _validated_nonnegative_int(states_value.get(state))
        for state in _COMMAND_REQUEST_STATES
    }
    total = _validated_nonnegative_int(value.get("total"))
    stale_active = _validated_nonnegative_int(value.get("stale_active"))
    eligible = _validated_nonnegative_int(value.get("eligible"))
    storage_pressure = value.get("storage_pressure")
    valid_policy = all(
        type(value.get(field)) is int and value.get(field) == expected
        for field, expected in (
            ("retry_horizon_seconds", config.command_retry_horizon_seconds),
            ("retention_seconds", config.command_receipt_retention_seconds),
            ("retention_count", config.command_receipt_retention_count),
        )
    )
    if (
        any(count is None for count in states.values())
        or total is None
        or stale_active is None
        or eligible is None
        or not valid_policy
        or not isinstance(storage_pressure, bool)
    ):
        return fallback, False
    state_counts = {state: int(states[state]) for state in _COMMAND_REQUEST_STATES}
    eligible_pool = (
        state_counts["reserved"]
        + state_counts["accepted"]
        + state_counts["rejected"]
        + state_counts["uncertain"]
    )
    if (
        total != sum(state_counts.values())
        or stale_active > state_counts["send_started"]
        or eligible
        > max(0, eligible_pool - config.command_receipt_retention_count)
        or storage_pressure is not bool(stale_active or eligible)
    ):
        return fallback, False
    return {
        **fallback,
        "total": total,
        "states": state_counts,
        "stale_active": stale_active,
        "eligible": eligible,
        "storage_pressure": storage_pressure,
    }, True


def _turn_ingestion_health(config: Config, scheduler: Any | None) -> dict[str, Any]:
    raw: Mapping[str, Any] = {}
    if scheduler is not None:
        try:
            status_value = scheduler.operational_status()
        except Exception:
            status_value = {}
        if isinstance(status_value, Mapping):
            raw = status_value
    status = raw.get("status")
    if status not in {"healthy", "stale", "degraded", "stopping"}:
        status = "stale" if scheduler is None else "degraded"
    return {
        "status": status,
        "queue": _nonnegative_int(raw.get("queue_depth")),
        "active": _nonnegative_int(raw.get("active")),
        "refreshed": _nonnegative_int(raw.get("refreshed")),
        "failed": _nonnegative_int(raw.get("failed")),
        "timed_out": _nonnegative_int(raw.get("timed_out")),
        "coalesced": _nonnegative_int(raw.get("coalesced")),
        "queue_full": _nonnegative_int(raw.get("queue_full")),
        "last_success": _valid_observation_timestamp(
            raw.get("last_success") if isinstance(raw.get("last_success"), str) else None
        ),
        "last_duration_ms": _nonnegative_float(raw.get("last_duration_ms")),
        "stale_age": _nonnegative_float(raw.get("stale_age_seconds")),
        "bounds": {
            "refresh_interval_seconds": config.turn_refresh_interval_seconds,
            "max_workers": config.turn_refresh_workers,
            "queue_capacity": _nonnegative_int(raw.get("queue_capacity")),
            "adapter_timeout_seconds": config.herdr_timeout_seconds,
        },
    }


def _pending_ingestion_health(config: Config) -> dict[str, Any]:
    """Return the fixed durable pending aggregate without exposing row identity."""
    unavailable = {
        "status": "store_unavailable",
        "counts": {"fresh": 0, "stale": 0, "total": 0},
    }
    raw: Mapping[str, Any] = unavailable
    if config.db_path is not None:
        try:
            from .store.sqlite import backend_pending_health

            value = backend_pending_health(Path(config.db_path), config.host_id)
        except Exception:
            value = unavailable
        if isinstance(value, Mapping):
            raw = value
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
        "bounds": {
            "stale_grace_seconds": config.pending_stale_grace_seconds,
        },
    }


def default_socket_path(config: Config) -> Path:
    """Return the daemon socket path for this config."""
    if config.socket_path is not None:
        return Path(config.socket_path)
    return Path(config.data_dir) / "tendwire.sock"


def _default_init_store(
    db_path: Path,
    *,
    connector_ack_ttl_seconds: int | None = None,
) -> None:
    from .store.sqlite import init_store

    kwargs = (
        {"connector_ack_ttl_seconds": connector_ack_ttl_seconds}
        if connector_ack_ttl_seconds is not None
        else {}
    )
    init_store(db_path, **kwargs)


def _default_observe_initial_snapshot(config: Config) -> Snapshot:
    from .cli import observe_public_snapshot

    return observe_public_snapshot(config, store_snapshot=True)


def _default_submit_command(config: Config, payload: str) -> CommandEnvelope:
    from .command_submission import submit_command

    return submit_command(config, payload)


def _default_turn_scheduler_factory(config: Config) -> Any:
    from .backends.herdr_turns import TurnIngestionScheduler

    return TurnIngestionScheduler(config)


@dataclass(frozen=True)
class DaemonHooks:
    """Dependency injection points for deterministic daemon tests."""

    init_store: Callable[[Path], None] = _default_init_store
    observe_initial_snapshot: Callable[[Config], Snapshot] = _default_observe_initial_snapshot
    submit_command: Callable[[Config, str], CommandEnvelope | Mapping[str, Any]] = _default_submit_command
    event_backend_factory: Callable[[Config, threading.Event], Any] | None = None
    turn_scheduler_factory: Callable[[Config], Any] = _default_turn_scheduler_factory


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
        self._event_backend: Any | None = None
        self._turn_scheduler: Any | None = None
        self._stop_lock = threading.Lock()
        self._automatic_maintenance_status: dict[str, Any] | None = None

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
        if self.config.db_path is None:
            raise RuntimeError("daemon requires a sqlite db path")

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
                _default_init_store(
                    Path(self.config.db_path),
                    connector_ack_ttl_seconds=(
                        self.config.connector_ack_ttl_seconds
                    ),
                )
            else:
                self.hooks.init_store(Path(self.config.db_path))
            self._connector_periodic_tick()
            if self.config.herdr_backend == "socket":
                self._snapshot = self._start_socket_event_backend()
            else:
                self._snapshot = self.hooks.observe_initial_snapshot(self.config)
                self._after_snapshot_saved()

            scheduler = self.hooks.turn_scheduler_factory(self.config)
            self._turn_scheduler = scheduler

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
            self._server = server
            # Bind before ingestion starts. Managed store connections and the
            # socket publisher lock the same parent directory, so allowing an
            # initial refresh first can make the daemon deadlock with itself.
            # Requests are not served until start() returns successfully.
            server.start()

            backend = self._event_backend
            callback_setter = (
                getattr(backend, "set_turn_refresh_callback", None)
                if backend is not None
                else None
            )
            if callable(callback_setter):
                callback_setter(scheduler.request_refresh)
            scheduler.start()
            scheduler.request_refresh()
        except Exception:
            self.stop_event.set()
            backend = self._event_backend
            callback_setter = (
                getattr(backend, "set_turn_refresh_callback", None)
                if backend is not None
                else None
            )
            if callable(callback_setter):
                try:
                    callback_setter(None)
                except Exception:
                    pass
            scheduler = self._turn_scheduler
            self._turn_scheduler = None
            if scheduler is not None:
                try:
                    scheduler.stop(
                        flush_timeout_seconds=self.config.herdr_timeout_seconds + 1.0
                    )
                except Exception:
                    pass
            self._event_backend = None
            if backend is not None:
                try:
                    backend.stop()
                except Exception:
                    pass
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
            backend = self._event_backend
            scheduler = self._turn_scheduler
            self._server = None
            self._event_backend = None
            self._turn_scheduler = None

        if server is not None:
            try:
                server.close()
            except Exception:
                pass

        if backend is not None:
            flush = getattr(backend, "flush", None)
            if callable(flush):
                try:
                    flush()
                except Exception:
                    pass
            callback_setter = getattr(backend, "set_turn_refresh_callback", None)
            if callable(callback_setter):
                try:
                    callback_setter(None)
                except Exception:
                    pass

        if scheduler is not None:
            try:
                scheduler.stop(
                    flush_timeout_seconds=self.config.herdr_timeout_seconds + 1.0
                )
            except Exception:
                pass

        if backend is not None:
            try:
                backend.stop()
            except Exception:
                pass

    def _after_snapshot_saved(self) -> None:
        if self.config.db_path is None:
            return
        from .store.sqlite import (
            SnapshotRetentionPolicy,
            compact_turn_change_journal,
            maybe_run_automatic_store_maintenance,
        )

        policy = SnapshotRetentionPolicy(
            retention_days=self.config.snapshot_retention_days,
            retention_count=self.config.snapshot_retention_count,
            batch_size=self.config.snapshot_maintenance_batch_size,
        )
        try:
            result = maybe_run_automatic_store_maintenance(
                Path(self.config.db_path),
                policy=policy,
                turn_model=self.config.turn_model,
                acknowledged_final_retention_days=(
                    self.config.acknowledged_final_retention_days
                ),
                acknowledged_final_retention_count=(
                    self.config.acknowledged_final_retention_count
                ),
                command_retry_horizon_seconds=(
                    self.config.command_retry_horizon_seconds
                ),
                command_receipt_retention_seconds=(
                    self.config.command_receipt_retention_seconds
                ),
                command_receipt_retention_count=(
                    self.config.command_receipt_retention_count
                ),
                cadence_seconds=self.config.store_maintenance_cadence_seconds,
            )
            turn_change_result: Mapping[str, Any] = {"ok": True}
            if bool(result.get("due")):
                turn_change_result = compact_turn_change_journal(
                    Path(self.config.db_path),
                    self.config.host_id,
                    retention_days=self.config.turn_change_retention_days,
                    retention_count=self.config.turn_change_retention_count,
                    batch_size=self.config.turn_change_compaction_batch_size,
                )
            snapshot_result = result.get("snapshot")
            snapshot_counts = snapshot_result if isinstance(snapshot_result, Mapping) else {}
            maintenance_status = {
                "ok": bool(result.get("ok")) and bool(turn_change_result.get("ok")),
                "status": (
                    str(result.get("status") or "unknown")
                    if bool(turn_change_result.get("ok"))
                    else "failed"
                ),
                "due": bool(result.get("due")),
                "examined": int(snapshot_counts.get("examined") or 0),
                "deleted": int(snapshot_counts.get("deleted") or 0),
                "remaining_candidates": bool(snapshot_counts.get("remaining_candidates")),
            }
        except Exception:
            self._automatic_maintenance_status = {
                "ok": False,
                "status": "failed",
                "due": False,
                "examined": 0,
                "deleted": 0,
                "remaining_candidates": False,
            }
        else:
            self._automatic_maintenance_status = maintenance_status

    def _start_socket_event_backend(self) -> Snapshot:
        if self.hooks.event_backend_factory is None:
            from .backends.herdr_events import HerdrEventBackend

            backend = HerdrEventBackend(self.config, stop_event=self.stop_event)
        else:
            backend = self.hooks.event_backend_factory(self.config, self.stop_event)
        self._event_backend = backend
        backend.start(wait_for_reconcile=True)
        from .store.sqlite import SnapshotObservationContext, latest_snapshot, save_snapshot

        snapshot = latest_snapshot(Path(self.config.db_path), self.config.host_id)
        if snapshot is not None:
            return snapshot
        from .backends.herdr_cli import herdr_backend_health
        from .core.projector import project_from_observations

        backend_health = (
            backend.health.to_backend_health()
            if hasattr(backend, "health")
            else herdr_backend_health("unknown")
        )
        snapshot = project_from_observations(
            self.config,
            backend_health=[backend_health],
        )
        save_snapshot(
            Path(self.config.db_path),
            snapshot,
            turn_model=self.config.turn_model,
            observation=SnapshotObservationContext(
                authority="none",
                observed_at=_valid_observation_timestamp(backend_health.observed_at),
            ),
        )
        self._after_snapshot_saved()
        return snapshot

    def get_snapshot(self) -> Snapshot:
        if self.config.db_path is not None:
            from .store.sqlite import latest_snapshot

            snapshot = latest_snapshot(Path(self.config.db_path), self.config.host_id)
            if snapshot is not None:
                self._snapshot = snapshot
                return snapshot
        if self._snapshot is not None:
            return self._snapshot
        raise RuntimeError("daemon has no initial snapshot")

    def get_health(self) -> dict[str, Any]:
        snapshot = self.get_snapshot()
        command_requests_default, _ = _command_requests_health(self.config, None)
        store_payload: dict[str, Any] = {
            "schema_version": 1,
            "ok": False,
            "status": "store_unavailable",
            "host_id": self.config.host_id,
            "counts": {},
            "outbox": {
                "pending": 0,
                "leased": 0,
                "completed": 0,
                "by_status": {},
                "due": 0,
                "oldest_due_at": None,
                "overdue_awaiting_ack": 0,
                "drain_target_seconds": 30,
                "starved": False,
            },
            "final_retention": {
                **{key: 0 for key in _FINAL_RETENTION_COUNT_FIELDS},
                "acknowledged_final_retention_days": (
                    self.config.acknowledged_final_retention_days
                ),
                "acknowledged_final_retention_count": (
                    self.config.acknowledged_final_retention_count
                ),
                "storage_pressure": False,
            },
            "command_requests": command_requests_default,
            "maintenance": {
                "last_completed_at": None,
                "status": "not_initialized",
                "snapshot_count": 0,
                "snapshot_retention_days": self.config.snapshot_retention_days,
                "snapshot_retention_count": self.config.snapshot_retention_count,
                "maintenance_batch_size": self.config.snapshot_maintenance_batch_size,
                "maintenance_cadence_seconds": self.config.store_maintenance_cadence_seconds,
                "backlog": False,
            },
        }
        if self.config.db_path is not None:
            from .store.sqlite import store_status

            store_payload = store_status(
                Path(self.config.db_path),
                self.config.host_id,
                acknowledged_final_retention_days=(
                    self.config.acknowledged_final_retention_days
                ),
                acknowledged_final_retention_count=(
                    self.config.acknowledged_final_retention_count
                ),
                snapshot_retention_days=self.config.snapshot_retention_days,
                snapshot_retention_count=self.config.snapshot_retention_count,
                maintenance_batch_size=self.config.snapshot_maintenance_batch_size,
                maintenance_cadence_seconds=self.config.store_maintenance_cadence_seconds,
                command_retry_horizon_seconds=(
                    self.config.command_retry_horizon_seconds
                ),
                command_receipt_retention_seconds=(
                    self.config.command_receipt_retention_seconds
                ),
                command_receipt_retention_count=(
                    self.config.command_receipt_retention_count
                ),
            )
        store_origin_valid = (
            type(store_payload.get("schema_version")) is int
            and store_payload.get("schema_version") == 1
            and
            store_payload.get("ok") is True
            and store_payload.get("status") == "ok"
            and store_payload.get("host_id") == self.config.host_id
        )
        counts, counts_valid = _store_counts_health(
            store_payload.get("counts") if store_origin_valid else None
        )
        outbox, outbox_valid = _outbox_health(
            store_payload.get("outbox") if store_origin_valid else None
        )
        final_retention, final_retention_valid = _final_retention_health(
            self.config,
            store_payload.get("final_retention") if store_origin_valid else None,
        )
        command_requests, command_requests_valid = _command_requests_health(
            self.config,
            store_payload.get("command_requests") if store_origin_valid else None,
        )
        maintenance, maintenance_valid = _maintenance_health(
            self.config,
            store_payload.get("maintenance") if store_origin_valid else None,
        )
        if maintenance["snapshot_count"] != counts["snapshots"]:
            maintenance, maintenance_valid = _maintenance_health(self.config, None)
        store_ok = bool(
            store_origin_valid
            and counts_valid
            and outbox_valid
            and final_retention_valid
            and command_requests_valid
            and maintenance_valid
        )
        backend_runtime: dict[str, Any] = {}
        if self._event_backend is not None and hasattr(self._event_backend, "operational_status"):
            status_value = getattr(self._event_backend, "operational_status")
            if isinstance(status_value, Mapping):
                backend_runtime = dict(status_value)
        backend_maintenance = backend_runtime.get("automatic_maintenance")
        runtime_maintenance = (
            backend_maintenance
            if isinstance(backend_maintenance, Mapping)
            else self._automatic_maintenance_status
        )
        if runtime_maintenance is not None:
            maintenance["last_check"] = dict(runtime_maintenance)
        maintenance_degraded = (
            not maintenance_valid
            or maintenance.get("status") == "failed"
            or bool(maintenance.get("backlog"))
            or (
                runtime_maintenance is not None
                and not bool(runtime_maintenance.get("ok"))
            )
            or not final_retention_valid
            or bool(final_retention["storage_pressure"])
            or not command_requests_valid
            or bool(command_requests["storage_pressure"])
            or bool(outbox["starved"])
        )
        pending_ingestion = _pending_ingestion_health(self.config)
        stored_last_event_at = _valid_observation_timestamp(
            store_payload.get("last_event_at")
            if store_ok and isinstance(store_payload.get("last_event_at"), str)
            else None
        )
        stored_last_snapshot_at = _valid_observation_timestamp(
            store_payload.get("last_snapshot_at")
            if store_ok and isinstance(store_payload.get("last_snapshot_at"), str)
            else None
        )
        last_event_at = backend_runtime.get("last_event_at") or stored_last_event_at
        last_snapshot_at = (
            backend_runtime.get("last_snapshot_at")
            or stored_last_snapshot_at
            or snapshot.updated_at
        )
        payload = {
            "schema_version": 1,
            "status": (
                "ok"
                if store_ok
                and not maintenance_degraded
                and pending_ingestion["status"] == "healthy"
                else "degraded"
            ),
            "host_id": self.config.host_id,
            "turn_model": self.config.turn_model,
            "daemon": {
                "status": "healthy",
                "started_at": self.started_at,
            },
            "store": {
                "status": (
                    "unavailable"
                    if not store_ok
                    else "degraded"
                    if maintenance_degraded
                    else "healthy"
                ),
                "counts": counts,
                "outbox": outbox,
                "final_retention": final_retention,
                "command_requests": command_requests,
                "last_event_at": stored_last_event_at,
                "last_snapshot_at": stored_last_snapshot_at,
                "maintenance": maintenance,
            },
            "snapshot": {
                "updated_at": snapshot.updated_at,
                "content_fingerprint": snapshot.content_fingerprint,
            },
            "timestamps": {
                "last_snapshot_at": last_snapshot_at,
                "last_event_at": last_event_at,
                "last_reconcile_at": backend_runtime.get("last_reconcile_at"),
            },
            "backend": {
                "status": backend_runtime.get("status"),
                "outcome": backend_runtime.get("outcome"),
                "ready": backend_runtime.get("ready"),
                "running": backend_runtime.get("running"),
                "reconcile_enabled": backend_runtime.get(
                    "reconcile_enabled",
                    self.config.reconcile_interval_seconds > 0,
                ),
            },
            "turn_ingestion": _turn_ingestion_health(
                self.config,
                self._turn_scheduler,
            ),
            "pending_ingestion": pending_ingestion,
            "limits": {
                "event_debounce_seconds": self.config.event_debounce_seconds,
                "reconcile_interval_seconds": self.config.reconcile_interval_seconds,
                "event_retention_days": self.config.event_retention_days,
                "output_excerpt_chars": self.config.output_excerpt_chars,
                "max_workers": self.config.max_workers,
                "max_outbox_attempts": self.config.max_outbox_attempts,
                "outbox_claim_ttl_seconds": self.config.connector_claim_ttl_seconds,
                "outbox_max_claim_ttl_seconds": (
                    self.config.connector_max_claim_ttl_seconds
                ),
                "outbox_ack_ttl_seconds": self.config.connector_ack_ttl_seconds,
                "acknowledged_final_retention_days": (
                    self.config.acknowledged_final_retention_days
                ),
                "acknowledged_final_retention_count": (
                    self.config.acknowledged_final_retention_count
                ),
                "command_retry_horizon_seconds": (
                    self.config.command_retry_horizon_seconds
                ),
                "command_receipt_retention_seconds": (
                    self.config.command_receipt_retention_seconds
                ),
                "command_receipt_retention_count": (
                    self.config.command_receipt_retention_count
                ),
                "snapshot_retention_days": self.config.snapshot_retention_days,
                "snapshot_retention_count": self.config.snapshot_retention_count,
                "snapshot_maintenance_batch_size": self.config.snapshot_maintenance_batch_size,
                "store_maintenance_cadence_seconds": self.config.store_maintenance_cadence_seconds,
            },
            "backend_health": [health.to_dict() for health in snapshot.backend_health],
        }
        return sanitize_public_mapping(payload)

    def get_attention(self) -> Mapping[str, Any]:
        if self.config.db_path is not None:
            from .store.sqlite import attention_payload_from_store

            payload = attention_payload_from_store(
                Path(self.config.db_path),
                self.config.host_id,
            )
            if payload is not None:
                return payload
        from .core.attention import attention_payload_from_snapshot

        return attention_payload_from_snapshot(self.get_snapshot())

    def get_pending(self) -> Mapping[str, Any]:
        """Return the durable pending projection shared with the CLI fallback."""
        from .store.sqlite import pending_payload_from_store

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
        if self.config.db_path is None:
            return {
                "schema_version": schema_version,
                "host_id": self.config.host_id,
                "ok": False,
                "status": "store_unavailable",
            }
        from .store.sqlite import turns_payload_from_store

        return turns_payload_from_store(
            Path(self.config.db_path),
            self.config.host_id,
            snapshot=self.get_snapshot(),
            schema_version=schema_version,
            limit=limit,
            cursor=cursor,
            since=since,
            turn_refresh_interval_seconds=self.config.turn_refresh_interval_seconds,
            turn_model=self.config.turn_model,
        )

    def get_turn_content(self, params: Mapping[str, Any]) -> Mapping[str, Any]:
        """turn.content.get: return one immutable bounded canonical page."""
        if self.config.db_path is None:
            return {
                "schema_version": 1,
                "ok": False,
                "status": "store_unavailable",
                "error": {
                    "code": "store_unavailable",
                    "message": "daemon requires a sqlite db path for this method",
                },
            }
        from .store.sqlite import get_turn_content

        return get_turn_content(
            Path(self.config.db_path),
            self.config.host_id,
            turn_id=params.get("turn_id"),
            content_revision=params.get("content_revision"),
            field=params.get("field"),
            cursor=params.get("cursor"),
            schema_version=params.get("schema_version", 1),
            turn_model=self.config.turn_model,
        )

    def get_turn_delta(
        self,
        *,
        watermark: str | None = None,
        cursor: str | None = None,
        limit: int = 100,
    ) -> Mapping[str, Any]:
        """Read one cache-only delta page; this surface has no delivery authority."""
        if self.config.db_path is None:
            return {
                "schema_version": 1,
                "projection_schema_version": 2,
                "host_id": self.config.host_id,
                "ok": False,
                "status": "store_unavailable",
            }
        from .store.sqlite import turn_delta_payload_from_store

        return turn_delta_payload_from_store(
            Path(self.config.db_path),
            self.config.host_id,
            watermark=watermark,
            cursor=cursor,
            limit=limit,
            turn_model=self.config.turn_model,
        )

    def connector_call(self, method: str, params: Mapping[str, Any]) -> Mapping[str, Any]:
        if self.config.db_path is None:
            return {
                "schema_version": 1,
                "ok": False,
                "status": "store_unavailable",
                "host_id": self.config.host_id,
                "name": str(params.get("name") or ""),
                "error": {
                    "code": "store_unavailable",
                    "message": "daemon requires a sqlite db path for this method",
                },
            }
        from .connectors import ConnectorOutboxAPI

        return ConnectorOutboxAPI(
            Path(self.config.db_path),
            self.config.host_id,
            default_lease_seconds=self.config.connector_claim_ttl_seconds,
            max_lease_seconds=self.config.connector_max_claim_ttl_seconds,
            ack_ttl_seconds=self.config.connector_ack_ttl_seconds,
            max_attempts=self.config.max_outbox_attempts,
            turn_model=self.config.turn_model,
        ).dispatch(method, params)

    def _connector_periodic_tick(self) -> None:
        """Eagerly reclaim expired connector work without waiting for a poll."""
        if self.config.db_path is None:
            return
        from .store.sqlite import (
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
        return self.hooks.submit_command(self.config, payload)


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
