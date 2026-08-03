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
from typing import Any, Protocol

from ..config import Config
from ..core.models import WorkerBinding, stable_fingerprint
from ..store.sqlite import expire_worker_bindings, list_worker_bindings
from .acp_ingestion import AcpSessionIngestor
from .acp_protocol import (
    PermissionRequest,
    PromptResult,
    RequestId,
    SessionResult,
    SessionUpdate,
    StopReason,
    SteeringResult,
)


class AcpRuntimeError(RuntimeError):
    """Base error raised by the supervised ACP runtime."""


class AcpRuntimeStateError(AcpRuntimeError):
    """An operation is invalid for the runtime's current lifecycle state."""


class AcpRuntimeProtocolError(AcpRuntimeError):
    """The ACP client returned data that cannot be safely bound or finalized."""


class AcpRuntimeBindingError(AcpRuntimeProtocolError):
    """The runtime's authenticated worker binding is no longer current."""


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


@dataclass(frozen=True, slots=True)
class PermissionSelection:
    """One selected option plus transport acknowledgement hooks.

    The callback that selected an option must not report its durable command as
    accepted until the JSON-RPC response frame has actually been written.
    """

    option_id: str
    response_written: Callable[[], None]
    response_failed: Callable[[BaseException], None]


PermissionCallback = Callable[[PermissionRequest], str | PermissionSelection | None]
IngestorFactory = Callable[..., AcpSessionIngestor]


class SessionBindingCallback(Protocol):
    """Atomically persist an ACP binding derived from one continuity anchor."""

    def __call__(
        self,
        session_id: str,
        continuity_binding: WorkerBinding,
    ) -> WorkerBinding: ...


class AcpRuntimeClient(Protocol):
    """Adapter-neutral client surface required by :class:`AcpRuntime`."""

    def initialize(
        self,
        *,
        client_capabilities: Mapping[str, Any] | None = None,
    ) -> object: ...

    def new_session(
        self,
        cwd: Path,
        *,
        mcp_servers: Sequence[Mapping[str, Any]] = (),
        additional_directories: Sequence[Path] = (),
    ) -> SessionResult: ...

    def load_session(
        self,
        session_id: str,
        cwd: Path,
        *,
        mcp_servers: Sequence[Mapping[str, Any]] = (),
        additional_directories: Sequence[Path] = (),
    ) -> SessionResult: ...

    def resume_session(
        self,
        session_id: str,
        cwd: Path,
        *,
        mcp_servers: Sequence[Mapping[str, Any]] = (),
        additional_directories: Sequence[Path] = (),
    ) -> SessionResult: ...

    def prompt(
        self,
        session_id: str,
        prompt: str | Sequence[Mapping[str, Any]],
        *,
        timeout: float | None = None,
        on_send_start: Callable[[], None] | None = None,
        on_submitted: Callable[[], None] | None = None,
    ) -> PromptResult: ...

    def prepare_prompt(
        self,
        prompt: str | Sequence[Mapping[str, Any]],
    ) -> tuple[Mapping[str, Any], ...]: ...

    @property
    def steering_supported(self) -> bool: ...

    def steer_session(
        self,
        session_id: str,
        prompt: str | Sequence[Mapping[str, Any]],
        *,
        timeout: float | None = None,
        on_send_start: Callable[[], None] | None = None,
        on_submitted: Callable[[], None] | None = None,
    ) -> SteeringResult: ...

    def cancel(self, session_id: str) -> None: ...

    def next_session_event(
        self,
        *,
        timeout: float,
    ) -> SessionUpdate | PermissionRequest: ...

    def respond_permission(
        self,
        request_id: RequestId,
        *,
        option_id: str | None = None,
        cancelled: bool = False,
    ) -> None: ...

    def close(self) -> None: ...


class AcpRuntime:
    """Run and durably ingest exactly one ACP session for one worker binding.

    Permission requests fail closed: the default response is ``cancelled``.
    A callback can authorize an action only by returning the ID of an option
    present in that exact request.
    """

    def __init__(
        self,
        client: AcpRuntimeClient,
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
        session_binding_callback: SessionBindingCallback | None = None,
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
        if mode is SessionOpenMode.NEW and session_binding_callback is None:
            raise ValueError("session_binding_callback is required for ACP new")
        if mode is not SessionOpenMode.NEW and session_binding_callback is not None:
            raise ValueError(
                "session_binding_callback is only valid when creating an ACP session"
            )
        if binding.host_id != config.host_id:
            raise ValueError("ACP runtime binding host does not match configuration")
        if not binding.private_fingerprint:
            raise ValueError("ACP runtime requires an authenticated private binding")
        if (
            mode is SessionOpenMode.NEW
            and binding.turn_target_kind == "acp_session_id"
        ):
            raise ValueError(
                "ACP new requires a non-ACP worker continuity binding"
            )
        if mode is not SessionOpenMode.NEW:
            if binding.backend != "acp":
                raise ValueError("ACP load/resume requires an ACP backend binding")
            if binding.turn_target_kind != "acp_session_id":
                raise ValueError("ACP runtime requires an ACP session worker binding")
            if binding.turn_target_value != session_id:
                raise ValueError("ACP runtime session does not match the worker binding")
        if config.db_path is None:
            raise ValueError("ACP runtime requires a sqlite db path")
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
        self._client_capabilities = _runtime_client_capabilities(
            client_capabilities
        )
        self._mcp_servers = tuple(dict(server) for server in mcp_servers)
        self._additional_directories = tuple(Path(path) for path in additional_directories)
        self._permission_callback = permission_callback
        self._session_binding_callback = session_binding_callback
        self._provided_ingestor = ingestor
        self._ingestor_factory = ingestor_factory
        self._poll_timeout = float(poll_timeout)
        self._stop_timeout = float(stop_timeout)

        self._state = RuntimeState.NEW
        self._session_id: str | None = None
        self._ingestor: AcpSessionIngestor | None = None
        self._failure: BaseException | None = None
        self._state_lock = threading.RLock()
        # Session binders are embedding callbacks and may synchronously call
        # stop().  Reentrancy must terminate startup, not deadlock on our own
        # lifecycle lock.
        self._lifecycle_lock = threading.RLock()
        self._ingest_lock = threading.Lock()
        self._prompt_lock = threading.Lock()
        self._steering_lock = threading.Lock()
        self._idle_condition = threading.Condition(self._state_lock)
        self._stop_event = threading.Event()
        self._threads: tuple[threading.Thread, ...] = ()
        self._event_idle_epoch = 0
        self._setup_replay = False
        # NEW acquires authority through its binder during start. LOAD/RESUME
        # are handed an existing live ACP lease and own releasing it once the
        # runtime stops or fails.
        self._provisional_binding: WorkerBinding | None = (
            binding if mode is not SessionOpenMode.NEW else None
        )
        self._close_thread: threading.Thread | None = None
        self._close_failures: list[BaseException] = []
        self._prompt_threads: set[threading.Thread] = set()

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

        with self._lifecycle_lock:
            with self._state_lock:
                if self._state is RuntimeState.RUNNING:
                    return self
                if self._state is not RuntimeState.NEW:
                    raise AcpRuntimeStateError(
                        f"cannot start ACP runtime in state {self._state.value}"
                    )
                self._state = RuntimeState.STARTING
            try:
                self._require_current_binding(self._binding)
                self._client.initialize(client_capabilities=self._client_capabilities)
                if self._session_mode is SessionOpenMode.LOAD:
                    assert self._requested_session_id is not None
                    # ACP load may synchronously replay more updates than the
                    # bounded client queue can hold before returning.  Bind and
                    # drain the ordered stream before issuing the request.
                    self._session_id = self._requested_session_id
                    self._ingestor = self._make_ingestor(self._requested_session_id)
                    self._setup_replay = True
                    self._start_consumer()
                session = self._open_session()
                if not isinstance(session, SessionResult) or not session.session_id:
                    raise AcpRuntimeProtocolError(
                        "ACP session setup returned an invalid response"
                    )
                if self._session_mode is SessionOpenMode.NEW:
                    self._binding = self._bind_new_session(session.session_id)
                else:
                    if session.session_id != self._binding.turn_target_value:
                        raise AcpRuntimeProtocolError(
                            "ACP session setup did not return the bound session"
                        )
                    if session.session_id != self._requested_session_id:
                        raise AcpRuntimeProtocolError(
                            "ACP session setup returned an unexpected session"
                        )
                    self._require_current_binding(self._binding)
                if self._session_mode is SessionOpenMode.LOAD:
                    self._wait_for_event_idle(
                        self._stop_timeout,
                        allowed_state=RuntimeState.STARTING,
                    )
                    with self._ingest_lock:
                        self._require_ingestor().reset_after_load()
                    self._setup_replay = False
                else:
                    self._session_id = session.session_id
                    self._ingestor = self._make_ingestor(session.session_id)
                    self._start_consumer()
                with self._state_lock:
                    if self._failure is not None:
                        raise self._failure
                    self._state = RuntimeState.RUNNING
            except BaseException as exc:
                self._release_derived_binding(reason="acp_startup_rollback")
                self._record_failure(exc)
                # ``__enter__`` is never completed when start fails, so no
                # caller cleanup can be assumed. Bound shutdown prevents an
                # initialized adapter or a partially started consumer leaking.
                self._shutdown_transport(self._stop_timeout)
                raise
        return self

    def prompt(
        self,
        prompt: str | Sequence[Mapping[str, Any]],
        *,
        producer_turn_id: str | None = None,
        timeout: float | None = None,
        drain_timeout: float | None = None,
        on_send_start: Callable[[], None] | None = None,
        on_submitted: Callable[[], None] | None = None,
    ) -> PromptResult:
        """Submit one prompt and finalize only after its prior updates drain."""

        wait_limit = self._stop_timeout if drain_timeout is None else float(drain_timeout)
        if wait_limit <= 0:
            raise ValueError("drain_timeout must be positive")
        with self._prompt_lock:
            self.raise_if_failed()
            session_id, ingestor = self._running_components()
            if not isinstance(producer_turn_id, str) or not producer_turn_id.strip():
                raise ValueError("producer_turn_id must be non-empty text")
            stable_producer_turn_id = producer_turn_id.strip()
            with self._state_lock:
                self._prompts_started += 1
            try:
                self._require_current_binding(self._binding)
                prepared_prompt = _prepare_prompt_content(self._client, prompt)
                prompt_event = ingestor.begin_prompt(
                    prepared_prompt,
                    producer_turn_id=stable_producer_turn_id,
                )
                _raise_for_binding_rejection(prompt_event)
            except BaseException as exc:
                with self._state_lock:
                    self._prompts_failed += 1
                self._record_failure(exc)
                raise
            try:
                prompt_kwargs: dict[str, Any] = {"timeout": timeout}
                if on_send_start is not None:
                    prompt_kwargs["on_send_start"] = on_send_start
                if on_submitted is not None:
                    prompt_kwargs["on_submitted"] = on_submitted
                result = self._client.prompt(
                    session_id,
                    prepared_prompt,
                    **prompt_kwargs,
                )
            except BaseException as exc:
                with self._state_lock:
                    self._prompts_failed += 1
                # Once a turn has been opened locally, a timed-out or failed
                # prompt cannot be retried safely: late updates would otherwise
                # be attributed to the next turn. Best-effort cancellation
                # contains the remote work and the runtime becomes terminal.
                self._close_failed_prompt(session_id, ingestor)
                self._record_failure(exc)
                raise
            if not isinstance(result, PromptResult):
                error = AcpRuntimeProtocolError(
                    "ACP prompt returned an invalid response"
                )
                with self._state_lock:
                    self._prompts_failed += 1
                self._close_failed_prompt(session_id, ingestor)
                self._record_failure(error)
                raise error

            # The transport dispatches updates before the prompt response, but
            # the consumer runs on another thread.  Requiring a queue timeout
            # after the response is a barrier: every earlier queued update has
            # been durably ingested before the turn is marked complete.
            try:
                self._wait_for_event_idle(wait_limit)
                with self._ingest_lock:
                    completion = ingestor.mark_prompt_complete(result.stop_reason)
                    _raise_for_binding_rejection(completion)
            except BaseException as exc:
                with self._state_lock:
                    self._prompts_failed += 1
                self._record_failure(exc)
                raise
            with self._state_lock:
                self._prompts_completed += 1
            return result

    def submit_prompt(
        self,
        prompt: str | Sequence[Mapping[str, Any]],
        *,
        producer_turn_id: str,
        acknowledgement_timeout: float,
        completion_timeout: float | None = None,
        on_send_start: Callable[[], None] | None = None,
    ) -> None:
        """Start a prompt and return after its complete frame is written.

        End-of-turn completion continues under runtime supervision. A caller
        can therefore durably acknowledge delivery without blocking for the
        agent's entire turn. If acknowledgement is not observed, delivery is
        uncertain and the command layer must never retry it automatically.
        """

        if acknowledgement_timeout <= 0:
            raise ValueError("acknowledgement_timeout must be positive")
        acknowledged = threading.Event()
        finished = threading.Event()
        failures: list[BaseException] = []

        def run_prompt() -> None:
            try:
                self.prompt(
                    prompt,
                    producer_turn_id=producer_turn_id,
                    timeout=completion_timeout,
                    on_send_start=on_send_start,
                    on_submitted=acknowledged.set,
                )
            except BaseException as exc:
                failures.append(exc)
            finally:
                finished.set()
                with self._state_lock:
                    self._prompt_threads.discard(threading.current_thread())

        thread = threading.Thread(
            target=run_prompt,
            name="tendwire-acp-prompt",
            daemon=True,
        )
        with self._state_lock:
            self._prompt_threads.add(thread)
        thread.start()
        deadline = time.monotonic() + acknowledgement_timeout
        while True:
            if acknowledged.is_set():
                return
            if finished.is_set():
                if failures:
                    raise failures[0]
                raise AcpRuntimeProtocolError(
                    "ACP prompt completed without a submission acknowledgement"
                )
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise AcpRuntimeStopTimeout(
                    "ACP prompt submission acknowledgement timed out"
                )
            acknowledged.wait(min(remaining, 0.01))

    def can_steer(self) -> bool:
        """Return whether this runtime can append to a live ACP turn."""

        try:
            supported = getattr(self._client, "steering_supported", False) is True
        except Exception:
            supported = False
        if not supported:
            return False
        with self._state_lock:
            if self._state is not RuntimeState.RUNNING or self._failure is not None:
                return False
        with self._ingest_lock:
            ingestor = self._ingestor
            can_append = getattr(ingestor, "can_append_prompt", None)
            return bool(callable(can_append) and can_append())

    def submit_steering(
        self,
        prompt: str | Sequence[Mapping[str, Any]],
        *,
        producer_turn_id: str,
        acknowledgement_timeout: float,
        on_send_start: Callable[[], None] | None = None,
    ) -> SteeringResult:
        """Inject one input into the current turn through ACP steering."""

        if acknowledgement_timeout <= 0:
            raise ValueError("acknowledgement_timeout must be positive")
        if not isinstance(producer_turn_id, str) or not producer_turn_id.strip():
            raise ValueError("producer_turn_id must be non-empty text")
        with self._steering_lock:
            self.raise_if_failed()
            session_id, ingestor = self._running_components()
            prepared = _prepare_prompt_content(self._client, prompt)
            if not self.can_steer():
                raise AcpRuntimeStateError("ACP steering is unavailable")

            def start_and_record() -> None:
                if on_send_start is not None:
                    on_send_start()
                with self._ingest_lock:
                    result = ingestor.append_prompt(
                        prepared,
                        producer_turn_id=producer_turn_id.strip(),
                    )
                    _raise_for_binding_rejection(result)

            return self._client.steer_session(
                session_id,
                prepared,
                timeout=acknowledgement_timeout,
                on_send_start=start_and_record,
            )

    def cancel(self) -> None:
        """Cancel the active session and any permission requests pending in it."""

        self.raise_if_failed()
        session_id, _ = self._running_components()
        try:
            self._cancel_session(session_id)
        except BaseException as exc:
            self._record_failure(exc)
            raise

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
        with self._state_lock:
            threads = (*self._threads, *self._prompt_threads)
        for thread in threads:
            if thread is threading.current_thread() or thread.ident is None:
                continue
            thread.join(timeout=max(0.0, deadline - time.monotonic()))
        return all(
            thread is threading.current_thread()
            or thread.ident is None
            or not thread.is_alive()
            for thread in threads
        )

    def stop(self, *, timeout: float | None = None) -> None:
        """Close transport and consumers without waiting beyond one deadline."""

        wait_limit = self._stop_timeout if timeout is None else float(timeout)
        if wait_limit <= 0:
            raise ValueError("stop timeout must be positive")
        deadline = time.monotonic() + wait_limit
        if not self._lifecycle_lock.acquire(timeout=wait_limit):
            error = AcpRuntimeStopTimeout(
                "ACP runtime lifecycle did not stop within the configured deadline"
            )
            self._record_failure(error)
            raise error
        try:
            with self._idle_condition:
                if self._state is RuntimeState.STOPPED:
                    return
                if self._state is RuntimeState.NEW:
                    self._state = RuntimeState.STOPPED
                    return
                if self._state is not RuntimeState.FAILED:
                    self._state = RuntimeState.STOPPING
                self._stop_event.set()
                self._idle_condition.notify_all()

            remaining = max(0.0, deadline - time.monotonic())
            if remaining <= 0 or not self._shutdown_transport(remaining):
                error = AcpRuntimeStopTimeout(
                    "ACP runtime did not stop within the configured deadline"
                )
                self._record_failure(error)
                raise error

            self._release_derived_binding(reason="acp_runtime_stopped")

            with self._state_lock:
                failure = self._failure
            if self._close_failures and failure is None:
                failure = self._close_failures[0]
                self._record_failure(failure)
            with self._state_lock:
                if failure is None:
                    self._state = RuntimeState.STOPPED
            if failure is not None:
                raise failure
        finally:
            self._lifecycle_lock.release()

    def _shutdown_transport(self, timeout: float) -> bool:
        """Start adapter close at most once and join all supervised work."""

        if self._close_thread is None:

            def close_client() -> None:
                try:
                    self._client.close()
                except BaseException as exc:
                    self._close_failures.append(exc)

            self._close_thread = threading.Thread(
                target=close_client,
                name="tendwire-acp-close",
                daemon=True,
            )
            self._close_thread.start()

        deadline = time.monotonic() + timeout
        self._close_thread.join(timeout=max(0.0, deadline - time.monotonic()))
        remaining = max(0.0, deadline - time.monotonic())
        consumers_joined = (
            self.join(timeout=remaining)
            if remaining > 0
            else all(
                thread.ident is None or not thread.is_alive()
                for thread in self._threads
            )
        )
        return not self._close_thread.is_alive() and consumers_joined

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

    def _bind_new_session(self, session_id: str) -> WorkerBinding:
        callback = self._session_binding_callback
        if callback is None:  # pragma: no cover - constructor invariant
            raise AcpRuntimeBindingError("ACP session binding is unavailable")
        continuity = self._binding
        self._require_current_binding(continuity)
        existing_acp = self._binding_fingerprints(continuity.host_id, backend="acp")
        try:
            bound = callback(session_id, continuity)
            with self._state_lock:
                if self._state is not RuntimeState.STARTING:
                    raise AcpRuntimeStateError(
                        "ACP runtime stopped during session binding"
                    )
            if not isinstance(bound, WorkerBinding):
                raise AcpRuntimeBindingError(
                    "ACP session binder returned an invalid binding"
                )
            if (
                bound.host_id != continuity.host_id
                or bound.worker_id != continuity.worker_id
                or bound.worker_fingerprint != continuity.worker_fingerprint
                or bound.backend != "acp"
                or bound.target_kind != continuity.target_kind
                or bound.target_value != continuity.target_value
            ):
                raise AcpRuntimeBindingError(
                    "ACP session binder changed worker continuity"
                )
            if (
                bound.turn_target_kind != "acp_session_id"
                or bound.turn_target_value != session_id
            ):
                raise AcpRuntimeBindingError(
                    "ACP session binder returned the wrong session"
                )
            if (
                not bound.private_fingerprint
                or bound.private_fingerprint == continuity.private_fingerprint
            ):
                raise AcpRuntimeBindingError(
                    "ACP session binder did not establish a distinct private binding"
                )
            # The callback must add a distinct ACP binding. It must not
            # repurpose or overwrite the Herdr continuity row it was given.
            self._require_current_binding(continuity)
            self._require_current_binding(bound)
        except BaseException:
            self._expire_new_acp_bindings(continuity.host_id, existing_acp)
            raise
        if bound.private_fingerprint not in existing_acp:
            self._provisional_binding = bound
        return bound

    def _binding_fingerprints(self, host_id: str, *, backend: str) -> set[str]:
        db_path = self._config.db_path
        if db_path is None:  # pragma: no cover - constructor invariant
            return set()
        return {
            item.private_fingerprint
            for item in list_worker_bindings(Path(db_path), host_id, backend=backend)
        }

    def _expire_new_acp_bindings(
        self,
        host_id: str,
        existing_fingerprints: set[str],
    ) -> None:
        db_path = self._config.db_path
        if db_path is None:  # pragma: no cover - constructor invariant
            return
        current = list_worker_bindings(Path(db_path), host_id, backend="acp")
        created = [
            item
            for item in current
            if item.private_fingerprint not in existing_fingerprints
        ]
        for item in created:
            expire_worker_bindings(
                Path(db_path),
                host_id,
                backend="acp",
                private_fingerprints=[item.private_fingerprint],
                # Callback clocks are untrusted.  Using the row's own
                # observation instant guarantees a future-dated provisional
                # lease is still revocable.
                now=item.observed_at,
                reason="acp_startup_rollback",
            )

    def _release_derived_binding(self, *, reason: str) -> None:
        bound = self._provisional_binding
        self._provisional_binding = None
        if bound is None or self._config.db_path is None:
            return
        expire_worker_bindings(
            Path(self._config.db_path),
            bound.host_id,
            backend="acp",
            private_fingerprints=[bound.private_fingerprint],
            now=bound.observed_at,
            reason=reason,
        )

    def _require_current_binding(self, expected: WorkerBinding) -> None:
        db_path = self._config.db_path
        if db_path is None:  # pragma: no cover - constructor invariant
            raise AcpRuntimeBindingError("ACP binding store is unavailable")
        current = list_worker_bindings(
            Path(db_path),
            expected.host_id,
            backend=expected.backend,
        )
        # Herdr may refresh only the observation lease while an ACP endpoint
        # is being initialized.  That does not change routing authority and
        # must not invalidate the in-flight generation transaction.  Every
        # identity and routing field remains exact; process-owned ACP rows are
        # still checked byte-for-byte so their revocation cannot be masked.
        if expected.backend == "herdr":
            present = any(
                _same_binding_authority(item, expected)
                for item in current
            )
        else:
            present = expected in current
        if not present:
            raise AcpRuntimeBindingError("ACP worker binding is not current")

    def _start_consumer(self) -> None:
        if self._threads:
            return
        thread = threading.Thread(
            target=self._consume_session_events,
            name="tendwire-acp-session-events",
            daemon=True,
        )
        self._threads = (thread,)
        thread.start()

    def _consume_session_events(self) -> None:
        try:
            while True:
                try:
                    event = self._client.next_session_event(timeout=self._poll_timeout)
                except TimeoutError:
                    with self._idle_condition:
                        self._event_idle_epoch += 1
                        self._idle_condition.notify_all()
                    if self._stop_event.is_set():
                        return
                    continue
                setup_replay = self._setup_replay
                if isinstance(event, SessionUpdate):
                    if event.session_id != self._session_id:
                        raise AcpRuntimeProtocolError(
                            "ACP update belongs to a different session"
                        )
                    ingestor = self._require_ingestor()
                    with self._ingest_lock:
                        outcome = ingestor.ingest_update(
                            event.raw,
                            replay=setup_replay,
                            setup_replay=setup_replay,
                        )
                        _raise_for_binding_rejection(outcome)
                    if _outcome_has_persisted_event(outcome):
                        with self._state_lock:
                            self._updates_ingested += 1
                elif isinstance(event, PermissionRequest):
                    self._handle_permission(
                        event,
                        setup_replay=setup_replay,
                    )
                else:  # pragma: no cover - typed protocol invariant
                    raise AcpRuntimeProtocolError("ACP session event type is invalid")
        except BaseException as exc:
            if not self._stop_event.is_set():
                self._record_failure(exc)

    def _handle_permission(
        self,
        request: PermissionRequest,
        *,
        setup_replay: bool = False,
    ) -> None:
        """Journal then resolve one permission, failing closed before response."""

        response_attempted = False
        try:
            if request.session_id != self._session_id:
                raise AcpRuntimeProtocolError(
                    "ACP permission belongs to a different session"
                )
            ingestor = self._require_ingestor()
            with self._ingest_lock:
                outcome = ingestor.ingest_permission_request(
                    request.raw,
                    source_event_id=_permission_source_event_id(request.request_id),
                    replay=setup_replay,
                    setup_replay=setup_replay,
                )
                _raise_for_binding_rejection(outcome)
            if _outcome_has_persisted_event(outcome):
                with self._state_lock:
                    self._permissions_ingested += 1

            selected: str | None = None
            selection: PermissionSelection | None = None
            callback_failure: BaseException | None = None
            if self._permission_callback is not None:
                try:
                    candidate = self._permission_callback(request)
                    candidate_id = (
                        candidate.option_id
                        if isinstance(candidate, PermissionSelection)
                        else candidate
                    )
                    if candidate_id is not None and candidate_id in {
                        option.option_id for option in request.options
                    }:
                        selected = candidate_id
                        selection = (
                            candidate
                            if isinstance(candidate, PermissionSelection)
                            else None
                        )
                    elif candidate_id is not None:
                        with self._state_lock:
                            self._invalid_permission_selections += 1
                except BaseException as exc:
                    callback_failure = exc
            response_attempted = True
            if selected is None:
                self._client.respond_permission(
                    request.request_id,
                    cancelled=True,
                )
                with self._state_lock:
                    self._permissions_cancelled += 1
            else:
                try:
                    self._client.respond_permission(
                        request.request_id,
                        option_id=selected,
                    )
                except BaseException as exc:
                    if selection is not None:
                        selection.response_failed(exc)
                    raise
                if selection is not None:
                    selection.response_written()
                with self._state_lock:
                    self._permissions_selected += 1
            if callback_failure is not None:
                raise callback_failure
        except BaseException:
            if not response_attempted:
                # No response bytes have been attempted yet, so cancellation
                # is safe. Never retry after respond_permission itself fails:
                # a partial JSON-RPC frame may already have reached the agent.
                try:
                    self._client.respond_permission(
                        request.request_id,
                        cancelled=True,
                    )
                except BaseException:
                    pass
                else:
                    with self._state_lock:
                        self._permissions_cancelled += 1
            raise

    def _wait_for_event_idle(
        self,
        timeout: float,
        *,
        allowed_state: RuntimeState = RuntimeState.RUNNING,
    ) -> None:
        deadline = time.monotonic() + timeout
        with self._idle_condition:
            event_epoch = self._event_idle_epoch
            while self._event_idle_epoch <= event_epoch:
                if self._failure is not None:
                    raise self._failure
                if self._state is not allowed_state:
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

    def _cancel_session(self, session_id: str) -> None:
        self._client.cancel(session_id)
        with self._state_lock:
            self._cancellation_requests += 1

    def _close_failed_prompt(
        self,
        session_id: str,
        ingestor: AcpSessionIngestor,
    ) -> None:
        """Best-effort cancel and durable closure after ``begin_prompt``.

        The original prompt exception remains authoritative even if either
        cleanup operation fails.  ``mark_prompt_complete`` is idempotent, so
        this method cannot create a second completion for an already closed
        turn.
        """

        try:
            self._cancel_session(session_id)
        except BaseException:
            pass
        try:
            # The prompt response (including an invalid one) can overtake the
            # consumer thread after earlier session/update frames were queued.
            # Preserve those updates in the failed turn before writing its
            # terminal marker. A late post-cancel update remains harmless:
            # the ingestor rejects turn-scoped updates after completion.
            self._wait_for_event_idle(self._stop_timeout)
        except BaseException:
            pass
        try:
            with self._ingest_lock:
                completion = ingestor.mark_prompt_complete(
                    StopReason.CANCELLED
                )
                _raise_for_binding_rejection(completion)
        except BaseException:
            pass

    def _record_failure(self, failure: BaseException) -> None:
        self._release_derived_binding(reason="acp_runtime_failed")
        with self._idle_condition:
            if self._failure is None:
                self._failure = failure
            self._state = RuntimeState.FAILED
            self._stop_event.set()
            self._idle_condition.notify_all()


def _permission_source_event_id(request_id: RequestId) -> str:
    """Return a bounded opaque ID while preserving JSON-RPC ID types."""

    return f"permission:{stable_fingerprint({'request_id': request_id})}"


def _prepare_prompt_content(
    client: AcpRuntimeClient,
    prompt: str | Sequence[Mapping[str, Any]],
) -> tuple[Mapping[str, Any], ...]:
    """Validate before persistence when the transport exposes its validator."""

    prepare = getattr(client, "prepare_prompt", None)
    if callable(prepare):
        prepared = tuple(prepare(prompt))
    elif isinstance(prompt, str):
        prepared = ({"type": "text", "text": prompt},)
    else:
        prepared = tuple(dict(block) for block in prompt)
    if not prepared or any(not isinstance(block, Mapping) for block in prepared):
        raise ValueError("prompt must contain at least one content block")
    return tuple(dict(block) for block in prepared)


def _runtime_client_capabilities(
    capabilities: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """Return only client capabilities this runtime can actually service.

    The runtime handles ACP session updates and permission requests, neither of
    which is advertised through ``clientCapabilities``. It has no handlers for
    filesystem, terminal, elicitation, or session-config requests. Known keys
    are therefore stripped even when supplied by an embedding caller. Unknown
    top-level keys could advertise extension methods and are rejected.
    """
    if capabilities is None:
        return {}
    if not isinstance(capabilities, Mapping):
        raise ValueError("client_capabilities must be a mapping or None")
    known = {"fs", "terminal", "session", "elicitation", "_meta"}
    if any(not isinstance(key, str) or key not in known for key in capabilities):
        raise ValueError(
            "ACP runtime cannot advertise unsupported client capabilities"
        )
    return {}


def _same_binding_authority(left: WorkerBinding, right: WorkerBinding) -> bool:
    """Compare durable route authority while ignoring observer lease refreshes."""

    return (
        left.host_id,
        left.worker_id,
        left.worker_fingerprint,
        left.backend,
        left.target_kind,
        left.target_value,
        left.turn_target_kind,
        left.turn_target_value,
        left.sendable,
        left.reason,
        left.private_fingerprint,
    ) == (
        right.host_id,
        right.worker_id,
        right.worker_fingerprint,
        right.backend,
        right.target_kind,
        right.target_value,
        right.turn_target_kind,
        right.turn_target_value,
        right.sendable,
        right.reason,
        right.private_fingerprint,
    )


def _raise_for_binding_rejection(outcome: object) -> None:
    """Make every stale durable-binding outcome terminal and public-safe."""
    if outcome is None:
        return
    reason = getattr(outcome, "ignored_reason", None)
    event = getattr(outcome, "event", None)
    turn = getattr(outcome, "turn", None)
    event_status = getattr(event, "status", None)
    turn_stale = getattr(turn, "stale_binding", False)
    if (
        reason in {"stale_binding", "binding_changed"}
        or event_status == "binding_changed"
        or turn_stale is True
    ):
        raise AcpRuntimeBindingError("ACP worker binding is no longer current")


def _outcome_has_persisted_event(outcome: object) -> bool:
    """Count only updates that reached the durable event boundary."""

    return getattr(outcome, "event", None) is not None


__all__ = [
    "AcpRuntime",
    "AcpRuntimeBindingError",
    "AcpRuntimeClient",
    "AcpRuntimeError",
    "AcpRuntimeProtocolError",
    "AcpRuntimeStateError",
    "AcpRuntimeStatus",
    "AcpRuntimeStopTimeout",
    "PermissionCallback",
    "RuntimeState",
    "SessionBindingCallback",
    "SessionOpenMode",
]
