"""Supervised ACP session runtime bound to one Tendwire worker.

The lower-level :mod:`acp_client` owns subprocess framing and JSON-RPC request
correlation.  This module connects that transport to durable ACP ingestion,
keeps both inbound event queues drained, and exposes a deliberately redacted
health surface suitable for operator APIs.
"""

from __future__ import annotations

import threading
import time
import uuid
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any

from ..config import Config
from ..core.models import WorkerBinding
from .acp_client import AcpClient
from .acp_ingestion import AcpSessionIngestor
from .acp_protocol import PermissionRequest, PromptResult, SessionResult


class AcpRuntimeError(RuntimeError):
    """Base error raised by the supervised ACP runtime."""


class AcpRuntimeStateError(AcpRuntimeError):
    """An operation is invalid for the runtime's current lifecycle state."""


class AcpRuntimeProtocolError(AcpRuntimeError):
    """The ACP client returned data that cannot be safely bound or finalized."""


class AcpRuntimeStopTimeout(AcpRuntimeError, TimeoutError):
    """The runtime could not stop all supervised work within its deadline."""


class SessionOpenMode(str, Enum):
    NEW = "new"
    LOAD = "load"
    RESUME = "resume"


class RuntimeState(str, Enum):
    NEW = "new"
    STARTING = "starting"
    RUNNING = "running"
    STOPPING = "stopping"
    STOPPED = "stopped"
    FAILED = "failed"


@dataclass(frozen=True, slots=True)
class AcpRuntimeStatus:
    """Public-safe runtime health and counters.

    This type intentionally has no command, process, session, worker, target,
    or exception-message fields.  Those values are private routing material.
    """

    state: RuntimeState
    healthy: bool
    updates_ingested: int
    permissions_ingested: int
    permissions_selected: int
    permissions_cancelled: int
    invalid_permission_selections: int
    prompts_started: int
    prompts_completed: int
    prompts_failed: int
    cancellation_requests: int
    failure_type: str | None


PermissionCallback = Callable[[PermissionRequest], str | None]
IngestorFactory = Callable[..., AcpSessionIngestor]


class AcpRuntime:
    """Run and durably ingest exactly one ACP session for one worker binding.

    Permission requests fail closed: the default response is ``cancelled``.
    A callback can authorize an action only by returning the ID of an option
    present in that exact request.
    """

    def __init__(
        self,
        client: AcpClient,
        *,
        config: Config,
        binding: WorkerBinding,
        cwd: str | Path,
        session_mode: SessionOpenMode | str = SessionOpenMode.NEW,
        session_id: str | None = None,
        stream_generation: str | None = None,
        client_capabilities: Mapping[str, Any] | None = None,
        mcp_servers: Sequence[Mapping[str, Any]] = (),
        additional_directories: Sequence[str | Path] = (),
        permission_callback: PermissionCallback | None = None,
        ingestor: AcpSessionIngestor | None = None,
        ingestor_factory: IngestorFactory = AcpSessionIngestor,
        poll_timeout: float = 0.05,
        stop_timeout: float = 3.0,
    ) -> None:
        try:
            mode = SessionOpenMode(session_mode)
        except ValueError as exc:
            raise ValueError(f"unsupported ACP session mode {session_mode!r}") from exc
        if mode is not SessionOpenMode.NEW and not session_id:
            raise ValueError(f"session_id is required for ACP {mode.value}")
        if mode is SessionOpenMode.NEW and session_id is not None:
            raise ValueError("session_id must be omitted when creating an ACP session")
        if binding.host_id != config.host_id:
            raise ValueError("ACP runtime binding host does not match configuration")
        if not binding.private_fingerprint:
            raise ValueError("ACP runtime requires an authenticated private binding")
        resolved_cwd = Path(cwd)
        if not resolved_cwd.is_absolute():
            raise ValueError("ACP runtime cwd must be absolute")
        if poll_timeout <= 0 or stop_timeout <= 0:
            raise ValueError("ACP runtime timeouts must be positive")

        self._client = client
        self._config = config
        self._binding = binding
        self._cwd = resolved_cwd
        self._session_mode = mode
        self._requested_session_id = session_id
        self._stream_generation = stream_generation or uuid.uuid4().hex
        self._client_capabilities = dict(client_capabilities or {})
        self._mcp_servers = tuple(dict(server) for server in mcp_servers)
        self._additional_directories = tuple(Path(path) for path in additional_directories)
        self._permission_callback = permission_callback
        self._provided_ingestor = ingestor
        self._ingestor_factory = ingestor_factory
        self._poll_timeout = float(poll_timeout)
        self._stop_timeout = float(stop_timeout)

        self._state = RuntimeState.NEW
        self._session_id: str | None = None
        self._ingestor: AcpSessionIngestor | None = None
        self._failure: BaseException | None = None
        self._state_lock = threading.RLock()
        self._ingest_lock = threading.Lock()
        self._prompt_lock = threading.Lock()
        self._idle_condition = threading.Condition(self._state_lock)
        self._stop_event = threading.Event()
        self._threads: tuple[threading.Thread, ...] = ()
        self._update_idle_epoch = 0

        self._updates_ingested = 0
        self._permissions_ingested = 0
        self._permissions_selected = 0
        self._permissions_cancelled = 0
        self._invalid_permission_selections = 0
        self._prompts_started = 0
        self._prompts_completed = 0
        self._prompts_failed = 0
        self._cancellation_requests = 0

    def __enter__(self) -> "AcpRuntime":
        return self.start()

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        try:
            self.stop()
        except BaseException:
            if exc is None:
                raise

    def start(self) -> "AcpRuntime":
        """Initialize capabilities, open one session, and start consumers."""

        with self._state_lock:
            if self._state is RuntimeState.RUNNING:
                return self
            if self._state is not RuntimeState.NEW:
                raise AcpRuntimeStateError(
                    f"cannot start ACP runtime in state {self._state.value}"
                )
            self._state = RuntimeState.STARTING
        try:
            self._client.initialize(client_capabilities=self._client_capabilities)
            session = self._open_session()
            if not isinstance(session, SessionResult) or not session.session_id:
                raise AcpRuntimeProtocolError(
                    "ACP session setup returned an invalid response"
                )
            self._session_id = session.session_id
            self._ingestor = self._make_ingestor(session.session_id)
            threads = (
                threading.Thread(
                    target=self._consume_updates,
                    name="tendwire-acp-updates",
                    daemon=True,
                ),
                threading.Thread(
                    target=self._consume_permissions,
                    name="tendwire-acp-permissions",
                    daemon=True,
                ),
            )
            self._threads = threads
            with self._state_lock:
                self._state = RuntimeState.RUNNING
            for thread in threads:
                thread.start()
        except BaseException as exc:
            self._record_failure(exc)
            raise
        return self

    def prompt(
        self,
        prompt: str | Sequence[Mapping[str, Any]],
        *,
        producer_turn_id: str | None = None,
        timeout: float | None = None,
        drain_timeout: float | None = None,
    ) -> PromptResult:
        """Submit one prompt and finalize only after its prior updates drain."""

        wait_limit = self._stop_timeout if drain_timeout is None else float(drain_timeout)
        if wait_limit <= 0:
            raise ValueError("drain_timeout must be positive")
        with self._prompt_lock:
            self.raise_if_failed()
            session_id, ingestor = self._running_components()
            with self._state_lock:
                self._prompts_started += 1
            try:
                ingestor.start_turn(producer_turn_id=producer_turn_id)
            except BaseException as exc:
                with self._state_lock:
                    self._prompts_failed += 1
                self._record_failure(exc)
                raise
            try:
                result = self._client.prompt(session_id, prompt, timeout=timeout)
            except BaseException:
                with self._state_lock:
                    self._prompts_failed += 1
                raise
            if not isinstance(result, PromptResult):
                error = AcpRuntimeProtocolError(
                    "ACP prompt returned an invalid response"
                )
                with self._state_lock:
                    self._prompts_failed += 1
                self._record_failure(error)
                raise error

            # The transport dispatches updates before the prompt response, but
            # the consumer runs on another thread.  Requiring a queue timeout
            # after the response is a barrier: every earlier queued update has
            # been durably ingested before the turn is marked complete.
            try:
                self._wait_for_post_response_idle(wait_limit)
                with self._ingest_lock:
                    ingestor.mark_prompt_complete()
            except BaseException as exc:
                with self._state_lock:
                    self._prompts_failed += 1
                self._record_failure(exc)
                raise
            with self._state_lock:
                self._prompts_completed += 1
            return result

    def cancel(self) -> None:
        """Cancel the active session and any permission requests pending in it."""

        self.raise_if_failed()
        session_id, _ = self._running_components()
        self._client.cancel(session_id)
        with self._state_lock:
            self._cancellation_requests += 1

    def status(self) -> AcpRuntimeStatus:
        """Return redacted health and counters safe for a public status API."""

        with self._state_lock:
            return AcpRuntimeStatus(
                state=self._state,
                healthy=self._state is RuntimeState.RUNNING and self._failure is None,
                updates_ingested=self._updates_ingested,
                permissions_ingested=self._permissions_ingested,
                permissions_selected=self._permissions_selected,
                permissions_cancelled=self._permissions_cancelled,
                invalid_permission_selections=self._invalid_permission_selections,
                prompts_started=self._prompts_started,
                prompts_completed=self._prompts_completed,
                prompts_failed=self._prompts_failed,
                cancellation_requests=self._cancellation_requests,
                failure_type=(
                    type(self._failure).__name__ if self._failure is not None else None
                ),
            )

    def raise_if_failed(self) -> None:
        """Raise the original background/runtime failure without redaction."""

        with self._state_lock:
            failure = self._failure
        if failure is not None:
            raise failure

    def join(self, timeout: float | None = None) -> bool:
        """Wait a bounded interval for consumer threads; return whether all exited."""

        wait_limit = self._stop_timeout if timeout is None else float(timeout)
        if wait_limit <= 0:
            raise ValueError("join timeout must be positive")
        deadline = time.monotonic() + wait_limit
        for thread in self._threads:
            if thread is threading.current_thread():
                continue
            thread.join(timeout=max(0.0, deadline - time.monotonic()))
        return all(
            thread is threading.current_thread() or not thread.is_alive()
            for thread in self._threads
        )

    def stop(self, *, timeout: float | None = None) -> None:
        """Close transport and consumers without waiting beyond one deadline."""

        wait_limit = self._stop_timeout if timeout is None else float(timeout)
        if wait_limit <= 0:
            raise ValueError("stop timeout must be positive")
        with self._state_lock:
            if self._state is RuntimeState.STOPPED:
                return
            if self._state is RuntimeState.NEW:
                self._state = RuntimeState.STOPPED
                return
            if self._state is not RuntimeState.FAILED:
                self._state = RuntimeState.STOPPING
        self._stop_event.set()
        close_failures: list[BaseException] = []

        def close_client() -> None:
            try:
                self._client.close()
            except BaseException as exc:
                close_failures.append(exc)

        closer = threading.Thread(
            target=close_client,
            name="tendwire-acp-close",
            daemon=True,
        )
        deadline = time.monotonic() + wait_limit
        closer.start()
        closer.join(timeout=max(0.0, deadline - time.monotonic()))
        remaining = max(0.0, deadline - time.monotonic())
        joined = (
            self.join(timeout=remaining)
            if remaining > 0
            else all(not thread.is_alive() for thread in self._threads)
        )
        if closer.is_alive() or not joined:
            error = AcpRuntimeStopTimeout(
                "ACP runtime did not stop within the configured deadline"
            )
            self._record_failure(error)
            raise error
        if close_failures:
            self._record_failure(close_failures[0])
            raise close_failures[0]
        with self._state_lock:
            if self._failure is None:
                self._state = RuntimeState.STOPPED
            failure = self._failure
        if failure is not None:
            raise failure

    def _open_session(self) -> SessionResult:
        options = {
            "mcp_servers": self._mcp_servers,
            "additional_directories": self._additional_directories,
        }
        if self._session_mode is SessionOpenMode.NEW:
            return self._client.new_session(self._cwd, **options)
        assert self._requested_session_id is not None
        if self._session_mode is SessionOpenMode.LOAD:
            return self._client.load_session(
                self._requested_session_id, self._cwd, **options
            )
        return self._client.resume_session(
            self._requested_session_id, self._cwd, **options
        )

    def _make_ingestor(self, session_id: str) -> AcpSessionIngestor:
        if self._provided_ingestor is not None:
            existing_session = getattr(self._provided_ingestor, "session_id", session_id)
            if existing_session != session_id:
                raise ValueError("provided ACP ingestor is bound to another session")
            return self._provided_ingestor
        return self._ingestor_factory(
            self._config,
            session_id=session_id,
            stream_generation=self._stream_generation,
            binding=self._binding,
        )

    def _consume_updates(self) -> None:
        try:
            while True:
                try:
                    update = self._client.next_update(timeout=self._poll_timeout)
                except TimeoutError:
                    with self._idle_condition:
                        self._update_idle_epoch += 1
                        self._idle_condition.notify_all()
                    if self._stop_event.is_set():
                        return
                    continue
                if update.session_id != self._session_id:
                    raise AcpRuntimeProtocolError(
                        "ACP update belongs to a different session"
                    )
                ingestor = self._require_ingestor()
                with self._ingest_lock:
                    ingestor.ingest_update(update.raw)
                with self._state_lock:
                    self._updates_ingested += 1
        except BaseException as exc:
            if not self._stop_event.is_set():
                self._record_failure(exc)

    def _consume_permissions(self) -> None:
        try:
            while True:
                try:
                    request = self._client.next_permission_request(
                        timeout=self._poll_timeout
                    )
                except TimeoutError:
                    if self._stop_event.is_set():
                        return
                    continue
                if request.session_id != self._session_id:
                    raise AcpRuntimeProtocolError(
                        "ACP permission belongs to a different session"
                    )
                ingestor = self._require_ingestor()
                with self._ingest_lock:
                    ingestor.ingest_permission_request(
                        request.raw,
                        source_event_id=f"permission:{request.request_id}",
                    )
                with self._state_lock:
                    self._permissions_ingested += 1

                selected: str | None = None
                callback_failure: BaseException | None = None
                if self._permission_callback is not None:
                    try:
                        candidate = self._permission_callback(request)
                        if candidate is not None and candidate in {
                            option.option_id for option in request.options
                        }:
                            selected = candidate
                        elif candidate is not None:
                            with self._state_lock:
                                self._invalid_permission_selections += 1
                    except BaseException as exc:
                        callback_failure = exc
                if selected is None:
                    self._client.respond_permission(
                        request.request_id,
                        cancelled=True,
                    )
                    with self._state_lock:
                        self._permissions_cancelled += 1
                else:
                    self._client.respond_permission(
                        request.request_id,
                        option_id=selected,
                    )
                    with self._state_lock:
                        self._permissions_selected += 1
                if callback_failure is not None:
                    raise callback_failure
        except BaseException as exc:
            if not self._stop_event.is_set():
                self._record_failure(exc)

    def _wait_for_post_response_idle(self, timeout: float) -> None:
        deadline = time.monotonic() + timeout
        with self._idle_condition:
            epoch = self._update_idle_epoch
            while self._update_idle_epoch <= epoch:
                if self._failure is not None:
                    raise self._failure
                if self._state is not RuntimeState.RUNNING:
                    raise AcpRuntimeStateError(
                        "ACP runtime stopped before prompt updates drained"
                    )
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise AcpRuntimeStopTimeout(
                        "ACP prompt updates did not drain before the deadline"
                    )
                self._idle_condition.wait(timeout=remaining)

    def _running_components(self) -> tuple[str, AcpSessionIngestor]:
        with self._state_lock:
            if self._state is not RuntimeState.RUNNING:
                raise AcpRuntimeStateError(
                    f"ACP runtime is not running ({self._state.value})"
                )
            session_id = self._session_id
            ingestor = self._ingestor
        if session_id is None or ingestor is None:  # pragma: no cover - invariant guard
            raise AcpRuntimeStateError("ACP runtime has no bound session")
        return session_id, ingestor

    def _require_ingestor(self) -> AcpSessionIngestor:
        ingestor = self._ingestor
        if ingestor is None:  # pragma: no cover - consumers start after binding
            raise AcpRuntimeStateError("ACP runtime has no ingestor")
        return ingestor

    def _record_failure(self, failure: BaseException) -> None:
        with self._idle_condition:
            if self._failure is None:
                self._failure = failure
            self._state = RuntimeState.FAILED
            self._stop_event.set()
            self._idle_condition.notify_all()


__all__ = [
    "AcpRuntime",
    "AcpRuntimeError",
    "AcpRuntimeProtocolError",
    "AcpRuntimeStateError",
    "AcpRuntimeStatus",
    "AcpRuntimeStopTimeout",
    "PermissionCallback",
    "RuntimeState",
    "SessionOpenMode",
]
