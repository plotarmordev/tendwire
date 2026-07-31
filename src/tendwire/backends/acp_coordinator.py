"""Production Herdr-to-ACP worker discovery and runtime coordination.

Herdr remains the authority for live worker identity and mints one-shot attach
endpoints.  This module validates that private contract, supervises one ACP
runtime per worker generation, and exposes opaque prompt routes to the daemon.
No endpoint ticket, process argv, cwd, adapter identity, or ACP session ID is
part of the public health surface.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

from ..config import Config
from ..core.models import Worker, WorkerBinding
from ..store.sqlite import list_worker_bindings, upsert_worker_bindings
from .acp_client import AcpClient
from .acp_runtime import AcpRuntime, RuntimeState, SessionOpenMode
from .herdr_socket import HerdrSocketClient


class AcpCoordinatorError(RuntimeError):
    """The private Herdr ACP endpoint contract or supervisor failed."""


@dataclass(frozen=True, slots=True)
class HerdrAcpEndpoint:
    command: tuple[str, ...]
    cwd: Path
    generation: str
    session_mode: SessionOpenMode
    session_id: str | None


@dataclass(frozen=True, slots=True)
class HerdrAcpStatus:
    generation: str
    lifecycle: str


@dataclass(slots=True)
class _RuntimeSlot:
    continuity: WorkerBinding
    generation: str
    runtime: AcpRuntime


class _PromptRoute:
    def __init__(self, owner: "AcpRuntimeCoordinator", worker: Worker) -> None:
        self._owner = owner
        self._worker = worker

    @property
    def binding_fingerprint(self) -> str:
        slot = self._owner._current_slot(self._worker)
        binding = getattr(slot.runtime, "_binding", None)
        return (
            str(binding.private_fingerprint)
            if isinstance(binding, WorkerBinding)
            else ""
        )

    def prompt(
        self,
        text: str,
        *,
        producer_turn_id: str,
        timeout: float,
    ) -> object:
        slot = self._owner._current_slot(self._worker)
        self._owner._require_attached_generation(slot)
        return slot.runtime.submit_prompt(
            text,
            producer_turn_id=producer_turn_id,
            acknowledgement_timeout=timeout,
        )


EndpointClientFactory = Callable[[Config], Any]
RuntimeFactory = Callable[..., AcpRuntime]
ClientFactory = Callable[..., AcpClient]


class AcpRuntimeCoordinator:
    """Reconcile Herdr worker authority into per-generation ACP runtimes."""

    def __init__(
        self,
        config: Config,
        stop_event: threading.Event,
        *,
        endpoint_client_factory: EndpointClientFactory | None = None,
        runtime_factory: RuntimeFactory = AcpRuntime,
        client_factory: ClientFactory = AcpClient,
        reconcile_interval: float | None = None,
    ) -> None:
        if config.db_path is None:
            raise ValueError("ACP coordinator requires a sqlite db path")
        self.config = config
        self._daemon_stop = stop_event
        self._endpoint_client_factory = (
            endpoint_client_factory or _default_endpoint_client_factory
        )
        self._runtime_factory = runtime_factory
        self._client_factory = client_factory
        self._reconcile_interval = max(
            1.0,
            float(
                config.turn_refresh_interval_seconds
                if reconcile_interval is None
                else reconcile_interval
            ),
        )
        self._lock = threading.RLock()
        self._stop = threading.Event()
        self._slots: dict[str, _RuntimeSlot] = {}
        self._thread: threading.Thread | None = None
        self._state = RuntimeState.NEW
        self._failure_type: str | None = None
        self._required_degraded = False

    def start(self) -> "AcpRuntimeCoordinator":
        with self._lock:
            if self._state is RuntimeState.RUNNING:
                return self
            if self._state is not RuntimeState.NEW:
                raise AcpCoordinatorError("ACP coordinator cannot be restarted")
            self._state = RuntimeState.STARTING
        try:
            self._reconcile(strict=self.config.agent_event_source == "acp_required")
        except Exception as exc:
            with self._lock:
                self._state = RuntimeState.FAILED
                self._failure_type = type(exc).__name__
            self._stop_all()
            raise
        with self._lock:
            self._state = RuntimeState.RUNNING
            thread = threading.Thread(
                target=self._run,
                name="tendwire-acp-coordinator",
                daemon=True,
            )
            self._thread = thread
            thread.start()
        return self

    def stop(self, *, timeout: float | None = None) -> None:
        limit = (
            self.config.acp_shutdown_timeout_seconds
            if timeout is None
            else float(timeout)
        )
        if limit <= 0:
            raise ValueError("stop timeout must be positive")
        with self._lock:
            if self._state is RuntimeState.STOPPED:
                return
            self._state = RuntimeState.STOPPING
        self._stop.set()
        self._stop_all(timeout=limit)
        thread = self._thread
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=limit)
        with self._lock:
            if self._state is not RuntimeState.FAILED:
                self._state = RuntimeState.STOPPED

    def join(self, timeout: float | None = None) -> bool:
        thread = self._thread
        if thread is None or thread is threading.current_thread():
            return True
        thread.join(timeout=timeout)
        return not thread.is_alive()

    def status(self) -> dict[str, Any]:
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
        with self._lock:
            state = self._state
            slots = tuple(self._slots.values())
            failure_type = self._failure_type
            required_degraded = self._required_degraded
        healthy = state is RuntimeState.RUNNING and not required_degraded
        for slot in slots:
            try:
                status = slot.runtime.status()
            except Exception as exc:  # noqa: BLE001
                healthy = False
                failure_type = failure_type or type(exc).__name__
                continue
            healthy = healthy and status.healthy
            for field in counters:
                counters[field] += max(0, int(getattr(status, field, 0)))
            failure_type = failure_type or status.failure_type
        return {
            "state": state.value,
            "healthy": healthy,
            "failure_type": failure_type,
            **counters,
        }

    def prompt_route(self, worker: Worker) -> _PromptRoute | None:
        try:
            self._current_slot(worker)
        except AcpCoordinatorError:
            # A just-observed worker may not have reached the periodic pass.
            try:
                self._reconcile_worker(worker.id, strict=False)
                self._current_slot(worker)
            except Exception:  # noqa: BLE001
                return None
        return _PromptRoute(self, worker)

    def owns_worker(self, worker_id: str, worker_fingerprint: str) -> bool:
        """Return whether a healthy ACP slot currently owns this exact worker."""
        with self._lock:
            slot = self._slots.get(worker_id)
        return bool(
            slot is not None
            and slot.continuity.worker_fingerprint == worker_fingerprint
            and slot.runtime.status().healthy
        )

    def _current_slot(self, worker: Worker) -> _RuntimeSlot:
        with self._lock:
            if self._state is not RuntimeState.RUNNING:
                raise AcpCoordinatorError("ACP coordinator is not running")
            slot = self._slots.get(worker.id)
        if slot is None:
            raise AcpCoordinatorError("ACP worker route is unavailable")
        if slot.continuity.worker_fingerprint != worker.fingerprint:
            raise AcpCoordinatorError("ACP worker authority is stale")
        if not slot.runtime.status().healthy:
            raise AcpCoordinatorError("ACP worker runtime is unhealthy")
        return slot

    def _run(self) -> None:
        while not self._stop.wait(self._reconcile_interval):
            if self._daemon_stop.is_set():
                return
            try:
                self._reconcile(strict=False)
            except Exception as exc:  # noqa: BLE001
                with self._lock:
                    self._failure_type = type(exc).__name__

    def _continuity_bindings(self) -> tuple[dict[str, WorkerBinding], int]:
        bindings = list_worker_bindings(
            Path(self.config.db_path),
            self.config.host_id,
            backend="herdr",
        )
        grouped: dict[str, list[WorkerBinding]] = {}
        for binding in bindings:
            if binding.sendable and binding.target_kind in {
                "agent_id",
                "agent",
                "name",
                "label",
                "terminal_id",
                "pane_id",
            }:
                grouped.setdefault(binding.worker_id, []).append(binding)
        current = {
            worker_id: rows[0]
            for worker_id, rows in grouped.items()
            if len(rows) == 1
        }
        ambiguities = sum(1 for rows in grouped.values() if len(rows) != 1)
        return current, ambiguities

    def _reconcile(self, *, strict: bool) -> None:
        current, ambiguities = self._continuity_bindings()
        with self._lock:
            stale = [worker_id for worker_id in self._slots if worker_id not in current]
        for worker_id in stale:
            self._retire_worker(worker_id)
        failures: list[BaseException] = [
            AcpCoordinatorError("worker has ambiguous Herdr authority")
            for _ in range(ambiguities)
        ]
        for worker_id, continuity in current.items():
            try:
                self._reconcile_binding(continuity)
            except Exception as exc:  # noqa: BLE001
                failures.append(exc)
                self._retire_worker(worker_id)
        with self._lock:
            self._required_degraded = bool(failures) and (
                self.config.agent_event_source == "acp_required"
            )
            if failures:
                self._failure_type = type(failures[0]).__name__
            elif not self._required_degraded:
                self._failure_type = None
        if strict and failures:
            raise AcpCoordinatorError("one or more ACP workers failed to attach")

    def _reconcile_worker(self, worker_id: str, *, strict: bool) -> None:
        current, _ambiguities = self._continuity_bindings()
        continuity = current.get(worker_id)
        if continuity is None:
            if strict:
                raise AcpCoordinatorError("worker has no unique Herdr authority")
            return
        self._reconcile_binding(continuity)

    def _reconcile_binding(self, continuity: WorkerBinding) -> None:
        with self._lock:
            existing = self._slots.get(continuity.worker_id)
        if (
            existing is not None
            and _same_continuity(existing.continuity, continuity)
            and existing.runtime.status().healthy
        ):
            try:
                self._require_attached_generation(existing)
                return
            except Exception:
                self._retire_worker(continuity.worker_id, expected=existing)
                existing = None
        if existing is not None:
            self._retire_worker(continuity.worker_id, expected=existing)
        endpoint = self._resolve_endpoint(continuity)
        runtime = self._build_runtime(continuity, endpoint)
        try:
            runtime.start()
        except Exception:
            try:
                runtime.stop(timeout=self.config.acp_shutdown_timeout_seconds)
            except Exception:
                pass
            raise
        slot = _RuntimeSlot(continuity, endpoint.generation, runtime)
        with self._lock:
            displaced = self._slots.get(continuity.worker_id)
            self._slots[continuity.worker_id] = slot
        if displaced is not None:
            self._stop_runtime(displaced.runtime)

    def _resolve_endpoint(self, continuity: WorkerBinding) -> HerdrAcpEndpoint:
        client = self._endpoint_client_factory(self.config)
        try:
            connect = getattr(client, "connect", None)
            if callable(connect):
                connect()
            result = client.agent_acp_endpoint(
                continuity.target_value,
                timeout=self.config.herdr_timeout_seconds,
            )
        finally:
            close = getattr(client, "close", None)
            if callable(close):
                close()
        return _parse_endpoint(self.config, continuity, result)

    def _resolve_status(self, continuity: WorkerBinding) -> HerdrAcpStatus:
        client = self._endpoint_client_factory(self.config)
        try:
            connect = getattr(client, "connect", None)
            if callable(connect):
                connect()
            result = client.agent_acp_status(
                continuity.target_value,
                timeout=self.config.herdr_timeout_seconds,
            )
        finally:
            close = getattr(client, "close", None)
            if callable(close):
                close()
        return _parse_status(continuity, result)

    def _require_attached_generation(self, slot: _RuntimeSlot) -> None:
        try:
            status = self._resolve_status(slot.continuity)
        except Exception:
            self._retire_worker(slot.continuity.worker_id, expected=slot)
            raise
        if (
            status.lifecycle != "acp_owned_attached"
            or status.generation != slot.generation
        ):
            self._retire_worker(slot.continuity.worker_id, expected=slot)
            raise AcpCoordinatorError("ACP worker generation lease is not current")

    def _build_runtime(
        self,
        continuity: WorkerBinding,
        endpoint: HerdrAcpEndpoint,
    ) -> AcpRuntime:
        client = self._client_factory(
            endpoint.command,
            cwd=endpoint.cwd,
            request_timeout=self.config.acp_request_timeout_seconds,
            prompt_timeout=float(self.config.submission_hard_ttl_seconds),
            close_timeout=self.config.acp_shutdown_timeout_seconds,
            max_frame_bytes=self.config.acp_max_frame_bytes,
        )
        if endpoint.session_mode is SessionOpenMode.NEW:
            binding = continuity
            callback = self._bind_new_session
        else:
            assert endpoint.session_id is not None
            binding = _derived_binding(continuity, endpoint.session_id)
            upsert_worker_bindings(Path(self.config.db_path), [binding])
            callback = None
        return self._runtime_factory(
            client,
            config=self.config,
            binding=binding,
            cwd=endpoint.cwd,
            session_mode=endpoint.session_mode,
            session_id=endpoint.session_id,
            stream_generation=endpoint.generation,
            session_binding_callback=callback,
            poll_timeout=min(0.25, self.config.acp_request_timeout_seconds),
            stop_timeout=self.config.acp_shutdown_timeout_seconds,
        )

    def _bind_new_session(
        self,
        session_id: str,
        continuity: WorkerBinding,
    ) -> WorkerBinding:
        bound = _derived_binding(continuity, session_id)
        upsert_worker_bindings(Path(self.config.db_path), [bound])
        return bound

    def _retire_worker(
        self,
        worker_id: str,
        *,
        expected: _RuntimeSlot | None = None,
    ) -> None:
        with self._lock:
            slot = self._slots.get(worker_id)
            if slot is None or (expected is not None and slot is not expected):
                return
            self._slots.pop(worker_id, None)
        self._stop_runtime(slot.runtime)

    def _stop_runtime(self, runtime: AcpRuntime, *, timeout: float | None = None) -> None:
        try:
            runtime.stop(
                timeout=(
                    self.config.acp_shutdown_timeout_seconds
                    if timeout is None
                    else timeout
                )
            )
        except Exception:
            pass

    def _stop_all(self, *, timeout: float | None = None) -> None:
        with self._lock:
            slots = tuple(self._slots.values())
            self._slots.clear()
        if not slots:
            return
        total = (
            self.config.acp_shutdown_timeout_seconds
            if timeout is None
            else timeout
        )
        deadline = time.monotonic() + total
        for slot in slots:
            self._stop_runtime(
                slot.runtime,
                timeout=max(0.001, deadline - time.monotonic()),
            )


def _default_endpoint_client_factory(config: Config) -> HerdrSocketClient:
    return HerdrSocketClient(timeout=config.herdr_timeout_seconds)


def _derived_binding(
    continuity: WorkerBinding,
    session_id: str,
) -> WorkerBinding:
    return replace(
        continuity,
        backend="acp",
        turn_target_kind="acp_session_id",
        turn_target_value=session_id,
        private_fingerprint="",
    )


def _same_continuity(left: WorkerBinding, right: WorkerBinding) -> bool:
    """Compare authority identity while ignoring observation lease refreshes."""
    return (
        left.host_id,
        left.worker_id,
        left.worker_fingerprint,
        left.backend,
        left.target_kind,
        left.target_value,
        left.private_fingerprint,
        left.sendable,
    ) == (
        right.host_id,
        right.worker_id,
        right.worker_fingerprint,
        right.backend,
        right.target_kind,
        right.target_value,
        right.private_fingerprint,
        right.sendable,
    )


def _nonempty_text(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value or value.strip() != value:
        raise AcpCoordinatorError(f"Herdr ACP endpoint {field} is invalid")
    if len(value) > 4096 or "\x00" in value:
        raise AcpCoordinatorError(f"Herdr ACP endpoint {field} is invalid")
    return value


def _parse_endpoint(
    config: Config,
    continuity: WorkerBinding,
    value: Any,
) -> HerdrAcpEndpoint:
    """Strictly validate the private, one-shot Herdr launch contract."""
    if not isinstance(value, Mapping) or set(value) != {
        "type",
        "endpoint",
        "worker",
        "adapter",
        "session",
        "cwd",
        "lifecycle",
    }:
        raise AcpCoordinatorError("Herdr ACP endpoint response shape is invalid")
    if (
        value.get("type") != "agent_acp_endpoint"
        or value.get("lifecycle") != "acp_owned_ready"
    ):
        raise AcpCoordinatorError("Herdr ACP endpoint is not ready")
    endpoint = value.get("endpoint")
    worker = value.get("worker")
    adapter = value.get("adapter")
    session = value.get("session")
    if not all(isinstance(item, Mapping) for item in (endpoint, worker, adapter, session)):
        raise AcpCoordinatorError("Herdr ACP endpoint nested shape is invalid")
    assert isinstance(endpoint, Mapping)
    assert isinstance(worker, Mapping)
    assert isinstance(adapter, Mapping)
    assert isinstance(session, Mapping)
    if set(endpoint) != {"transport", "command", "args", "protocol_version"}:
        raise AcpCoordinatorError("Herdr ACP launch specification is invalid")
    if (
        endpoint.get("transport") != "stdio"
        or type(endpoint.get("protocol_version")) is not int
        or endpoint.get("protocol_version") != 1
    ):
        raise AcpCoordinatorError("Herdr ACP launch protocol is unsupported")
    command = _nonempty_text(endpoint.get("command"), "command")
    if command != "herdr" or Path(config.herdr_bin).name != "herdr":
        raise AcpCoordinatorError("Herdr ACP endpoint executable is not configured Herdr")
    raw_generation = worker.get("generation")
    if (
        type(raw_generation) is not int
        or raw_generation < 0
        or raw_generation > (1 << 64) - 1
    ):
        raise AcpCoordinatorError("Herdr ACP endpoint generation is invalid")
    generation = str(raw_generation)
    args = endpoint.get("args")
    if (
        not isinstance(args, list)
        or len(args) != 7
        or args[0:2] != ["agent", "acp-attach"]
        or args[3] != "--generation"
        or args[5] != "--ticket"
    ):
        raise AcpCoordinatorError("Herdr ACP attach arguments are invalid")
    target = _nonempty_text(args[2], "target")
    if target != continuity.target_value:
        raise AcpCoordinatorError("Herdr ACP endpoint target changed")
    if _nonempty_text(args[4], "argument generation") != generation:
        raise AcpCoordinatorError("Herdr ACP endpoint generation is inconsistent")
    _nonempty_text(args[6], "ticket")
    if set(worker) != {
        "terminal_id",
        "workspace_id",
        "tab_id",
        "pane_id",
        "name",
        "agent",
        "generation",
    }:
        raise AcpCoordinatorError("Herdr ACP worker identity shape is invalid")
    pane_id = _nonempty_text(worker.get("pane_id"), "pane_id")
    if continuity.target_kind == "pane_id" and pane_id != continuity.target_value:
        raise AcpCoordinatorError("Herdr ACP pane authority changed")
    for field in ("terminal_id", "workspace_id", "tab_id", "name", "agent"):
        _nonempty_text(worker.get(field), field)
    if set(adapter) != {"name", "version"}:
        raise AcpCoordinatorError("Herdr ACP adapter identity shape is invalid")
    _nonempty_text(adapter.get("name"), "adapter name")
    _nonempty_text(adapter.get("version"), "adapter version")
    if set(session) not in ({"mode"}, {"id", "mode"}):
        raise AcpCoordinatorError("Herdr ACP session shape is invalid")
    try:
        mode = SessionOpenMode(session.get("mode"))
    except (TypeError, ValueError) as exc:
        raise AcpCoordinatorError("Herdr ACP session mode is invalid") from exc
    session_id_value = session.get("id")
    if mode is SessionOpenMode.NEW:
        if "id" in session and session_id_value is not None:
            raise AcpCoordinatorError("new Herdr ACP endpoint already has a session")
        session_id = None
    else:
        session_id = _nonempty_text(session_id_value, "session id")
    cwd = Path(_nonempty_text(value.get("cwd"), "cwd"))
    if not cwd.is_absolute():
        raise AcpCoordinatorError("Herdr ACP cwd must be absolute")
    return HerdrAcpEndpoint(
        command=(
            config.herdr_bin,
            *(_nonempty_text(item, "argument") for item in args),
        ),
        cwd=cwd,
        generation=generation,
        session_mode=mode,
        session_id=session_id,
    )


def _parse_status(
    continuity: WorkerBinding,
    value: Any,
) -> HerdrAcpStatus:
    """Validate Herdr's non-ticketing ACP generation/lease status."""
    if not isinstance(value, Mapping) or set(value) != {
        "type",
        "worker",
        "adapter",
        "session",
        "cwd",
        "lifecycle",
    }:
        raise AcpCoordinatorError("Herdr ACP status response shape is invalid")
    lifecycle = value.get("lifecycle")
    if value.get("type") != "agent_acp_status" or lifecycle not in {
        "acp_owned_ready",
        "acp_owned_attached",
    }:
        raise AcpCoordinatorError("Herdr ACP status lifecycle is invalid")
    worker = value.get("worker")
    adapter = value.get("adapter")
    session = value.get("session")
    if not all(isinstance(item, Mapping) for item in (worker, adapter, session)):
        raise AcpCoordinatorError("Herdr ACP status nested shape is invalid")
    assert isinstance(worker, Mapping)
    assert isinstance(adapter, Mapping)
    assert isinstance(session, Mapping)
    if set(worker) != {
        "terminal_id",
        "workspace_id",
        "tab_id",
        "pane_id",
        "name",
        "agent",
        "generation",
    }:
        raise AcpCoordinatorError("Herdr ACP status worker shape is invalid")
    raw_generation = worker.get("generation")
    if (
        type(raw_generation) is not int
        or raw_generation < 0
        or raw_generation > (1 << 64) - 1
    ):
        raise AcpCoordinatorError("Herdr ACP status generation is invalid")
    for field in (
        "terminal_id",
        "workspace_id",
        "tab_id",
        "pane_id",
        "name",
        "agent",
    ):
        _nonempty_text(worker.get(field), field)
    if (
        continuity.target_kind == "pane_id"
        and worker.get("pane_id") != continuity.target_value
    ):
        raise AcpCoordinatorError("Herdr ACP status pane authority changed")
    if set(adapter) != {"name", "version"}:
        raise AcpCoordinatorError("Herdr ACP status adapter shape is invalid")
    _nonempty_text(adapter.get("name"), "adapter name")
    _nonempty_text(adapter.get("version"), "adapter version")
    if set(session) not in ({"mode"}, {"id", "mode"}):
        raise AcpCoordinatorError("Herdr ACP status session shape is invalid")
    try:
        mode = SessionOpenMode(session.get("mode"))
    except (TypeError, ValueError) as exc:
        raise AcpCoordinatorError("Herdr ACP status session mode is invalid") from exc
    if mode is SessionOpenMode.NEW:
        if "id" in session and session.get("id") is not None:
            raise AcpCoordinatorError("new Herdr ACP status already has a session")
    else:
        _nonempty_text(session.get("id"), "session id")
    cwd = Path(_nonempty_text(value.get("cwd"), "cwd"))
    if not cwd.is_absolute():
        raise AcpCoordinatorError("Herdr ACP status cwd must be absolute")
    return HerdrAcpStatus(str(raw_generation), str(lifecycle))


def production_acp_runtime_factory(
    config: Config,
    stop_event: threading.Event,
) -> AcpRuntimeCoordinator:
    """Build the stock daemon's Herdr-backed multi-worker ACP coordinator."""
    return AcpRuntimeCoordinator(config, stop_event)
