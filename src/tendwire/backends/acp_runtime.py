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
from contextlib import suppress
from dataclasses import dataclass, replace
from enum import Enum
from pathlib import Path
from typing import Any

from ..config import Config
from ..core.models import WorkerBinding, stable_fingerprint
from ..store.projection import list_worker_bindings
from .acp_ingestion import AcpSessionIngestor
from .acp_protocol import (
    PermissionRequest,
    SessionUpdate,
    StopReason,
    SteeringOutcome,
)


class AcpRuntimeStateError(RuntimeError):
    """An operation is invalid for the runtime's current lifecycle state."""


class AcpRuntimeProtocolError(RuntimeError):
    """The ACP client returned data that cannot be safely bound or finalized."""


class AcpRuntimeBindingError(AcpRuntimeProtocolError):
    """The runtime's authenticated worker binding is no longer current."""


class AcpRuntimeStopTimeout(RuntimeError, TimeoutError):
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
class AcpWorkerSessionStatus:
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

    @classmethod
    def counter_names(cls) -> tuple[str, ...]:
        return tuple(
            name
            for name in cls.__dataclass_fields__
            if name not in {"state", "healthy", "failure_type"}
        )


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
SessionBindingCallback = Callable[[str, WorkerBinding], WorkerBinding]
# BoundedAcpConnection is the one concrete transport contract. Repeating its
# complete surface here created a second client layer that could drift.
AcpSessionConnection = Any


class AcpWorkerSession:
    """Run and durably ingest exactly one ACP session for one worker binding.

    Permission requests fail closed: the default response is ``cancelled``.
    A callback can authorize an action only by returning the ID of an option
    present in that exact request.
    """

    def __init__(
        self,
        client: AcpSessionConnection,
        *,
        config: Config,
        binding: WorkerBinding,
        cwd: str | Path,
        session_mode: SessionOpenMode | str = SessionOpenMode.NEW,
        session_id: str | None = None,
        stream_generation: str | None = None,
        permission_callback: PermissionCallback | None = None,
        session_binding_callback: SessionBindingCallback | None = None,
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
        if mode is SessionOpenMode.NEW and binding.turn_target_kind == "acp_session_id":
            raise ValueError("ACP new requires a non-ACP worker continuity binding")
        if mode is not SessionOpenMode.NEW:
            if binding.backend != "acp":
                raise ValueError("ACP load/resume requires an ACP backend binding")
            if binding.turn_target_kind != "acp_session_id":
                raise ValueError("ACP runtime requires an ACP session worker binding")
            if binding.turn_target_value != session_id:
                raise ValueError("ACP runtime session does not match the worker binding")
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
        self._permission_callback = permission_callback
        self._session_binding_callback = session_binding_callback
        self._ingestor_factory = ingestor_factory
        self._poll_timeout = float(poll_timeout)
        self._stop_timeout = float(stop_timeout)

        self._state = RuntimeState.NEW
        self._session_id: str | None = None
        self._ingestor: AcpSessionIngestor | None = None
        self._failure: BaseException | None = None
        # Session binders are embedding callbacks and may synchronously call
        # stop().  Reentrancy must terminate startup, not deadlock on our own
        # lifecycle lock.
        self._lifecycle_lock = threading.RLock()
        self._ingest_lock = threading.Lock()
        self._prompt_lock = threading.Lock()
        self._steering_lock = threading.Lock()
        self._idle_condition = threading.Condition()
        self._stop_event = threading.Event()
        self._threads: tuple[threading.Thread, ...] = ()
        self._event_idle_epoch = 0
        self._close_thread: threading.Thread | None = None
        self._prompt_threads: set[threading.Thread] = set()

        self._counters = dict.fromkeys(AcpWorkerSessionStatus.counter_names(), 0)

    def start(self) -> "AcpWorkerSession":
        """Initialize capabilities, open one session, and start consumers."""

        with self._lifecycle_lock:
            with self._idle_condition:
                if self._state is RuntimeState.RUNNING:
                    return self
                if self._state is not RuntimeState.NEW:
                    raise AcpRuntimeStateError(
                        f"cannot start ACP runtime in state {self._state.value}"
                    )
                self._state = RuntimeState.STARTING
            try:
                self._require_current_binding(self._binding)
                self._client.initialize()
                if self._session_mode is SessionOpenMode.LOAD:
                    assert self._requested_session_id is not None
                    # ACP load may synchronously replay more updates than the
                    # bounded client queue can hold before returning.  Bind and
                    # drain it before issuing the request. Historical replay is
                    # intentionally discarded; Tendwire only projects live work.
                    self._session_id = self._requested_session_id
                    self._start_consumer()
                session = self._open_session()
                if not isinstance(session, str) or not session:
                    raise AcpRuntimeProtocolError(
                        "ACP session setup returned an invalid response"
                    )
                if self._session_mode is SessionOpenMode.NEW:
                    self._binding = self._bind_new_session(session)
                else:
                    if session != self._binding.turn_target_value:
                        raise AcpRuntimeProtocolError(
                            "ACP session setup did not return the bound session"
                        )
                    if session != self._requested_session_id:
                        raise AcpRuntimeProtocolError(
                            "ACP session setup returned an unexpected session"
                        )
                    self._require_current_binding(self._binding)
                if self._session_mode is SessionOpenMode.LOAD:
                    self._wait_for_event_idle(
                        self._stop_timeout,
                        allowed_state=RuntimeState.STARTING,
                    )
                self._session_id = session
                self._ingestor = self._ingestor_factory(
                    self._config,
                    session_id=session,
                    stream_generation=self._stream_generation,
                    binding=self._binding,
                )
                self._start_consumer()
                with self._idle_condition:
                    if self._failure is not None:
                        raise self._failure
                    self._state = RuntimeState.RUNNING
            except BaseException as exc:
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
    ) -> StopReason:
        """Submit one prompt and finalize only after its prior updates drain."""

        wait_limit = self._stop_timeout if drain_timeout is None else float(drain_timeout)
        if wait_limit <= 0:
            raise ValueError("drain_timeout must be positive")
        with self._prompt_lock:
            self.raise_if_failed()
            session_id, ingestor = self._running_components()
            if not isinstance(producer_turn_id, str) or not producer_turn_id.strip():
                raise ValueError("producer_turn_id must be non-empty text")
            self._increment("prompts_started")
            close_on_failure = False
            try:
                self._require_current_binding(self._binding)
                prepared_prompt = _prepare_prompt_content(self._client, prompt)
                prompt_event = ingestor.begin_prompt(
                    prepared_prompt,
                    producer_turn_id=producer_turn_id.strip(),
                )
                _raise_for_binding_rejection(prompt_event)
                close_on_failure = True
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
                if not isinstance(result, StopReason):
                    raise AcpRuntimeProtocolError(
                        "ACP prompt returned an invalid response"
                    )
                close_on_failure = False
                # A queue timeout after the response is a barrier: every
                # earlier update is durable before the turn is completed.
                self._wait_for_event_idle(wait_limit)
                with self._ingest_lock:
                    completion = ingestor.mark_prompt_complete(result)
                    _raise_for_binding_rejection(completion)
            except BaseException as exc:
                self._increment("prompts_failed")
                # After begin_prompt, transport/response failure is terminal
                # and must close the local turn before another can start.
                if close_on_failure:
                    self._close_failed_prompt(session_id, ingestor)
                self._record_failure(exc)
                raise
            self._increment("prompts_completed")
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
                with self._idle_condition:
                    self._prompt_threads.discard(threading.current_thread())

        thread = threading.Thread(
            target=run_prompt,
            name="tendwire-acp-prompt",
            daemon=True,
        )
        with self._idle_condition:
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

        if self._client.steering_supported is not True:
            return False
        with self._idle_condition:
            if self._state is not RuntimeState.RUNNING or self._failure is not None:
                return False
        with self._ingest_lock:
            ingestor = self._ingestor
            return bool(ingestor is not None and ingestor.can_append_prompt())

    def submit_steering(
        self,
        prompt: str | Sequence[Mapping[str, Any]],
        *,
        producer_turn_id: str,
        acknowledgement_timeout: float,
        on_send_start: Callable[[], None] | None = None,
    ) -> SteeringOutcome:
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

            deadline = time.monotonic() + acknowledgement_timeout
            result = self._client.steer_session(
                session_id,
                prepared,
                timeout=acknowledgement_timeout,
                on_send_start=start_and_record,
            )
            if result is not SteeringOutcome.FAILED:
                return result

            # The steering extension defines ``failed`` as a definite
            # non-application outcome.  Retrying once is therefore safe and
            # prevents a transient adapter/app-server race from turning a
            # live Telegram message into a terminal drop.  The durable input
            # and transport-boundary callback were already recorded by the
            # first attempt, so the retry deliberately omits the callback and
            # stays inside the caller's original acknowledgement budget.
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return result
            return self._client.steer_session(
                session_id,
                prepared,
                timeout=remaining,
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

    def status(self) -> AcpWorkerSessionStatus:
        """Return redacted health and counters safe for a public status API."""

        with self._idle_condition:
            return AcpWorkerSessionStatus(
                state=self._state,
                healthy=self._state is RuntimeState.RUNNING and self._failure is None,
                **self._counters,
                failure_type=(
                    type(self._failure).__name__ if self._failure is not None else None
                ),
            )

    def _increment(self, name: str) -> None:
        with self._idle_condition:
            self._counters[name] += 1

    def raise_if_failed(self) -> None:
        """Raise the original background/runtime failure without redaction."""

        with self._idle_condition:
            failure = self._failure
        if failure is not None:
            raise failure

    def join(self, timeout: float | None = None) -> bool:
        """Wait a bounded interval for all supervised work to exit."""

        wait_limit = self._stop_timeout if timeout is None else float(timeout)
        if wait_limit <= 0:
            raise ValueError("join timeout must be positive")
        return self._join_threads(time.monotonic() + wait_limit)

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

            with self._idle_condition:
                failure = self._failure
            if failure is not None:
                self._record_failure(failure)
                raise failure
            with self._idle_condition:
                self._state = RuntimeState.STOPPED
        finally:
            self._lifecycle_lock.release()

    def _shutdown_transport(self, timeout: float) -> bool:
        """Start adapter close at most once and join all supervised work."""

        if self._close_thread is None:

            def close_client() -> None:
                try:
                    self._client.close()
                except BaseException as exc:
                    self._record_failure(exc)

            self._close_thread = threading.Thread(
                target=close_client,
                name="tendwire-acp-close",
                daemon=True,
            )
            self._close_thread.start()

        deadline = time.monotonic() + timeout
        self._close_thread.join(timeout=max(0.0, deadline - time.monotonic()))
        consumers_joined = self._join_threads(deadline)
        return not self._close_thread.is_alive() and consumers_joined

    def _join_threads(self, deadline: float) -> bool:
        with self._idle_condition:
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

    def _open_session(self) -> str:
        if self._session_mode is SessionOpenMode.NEW:
            return self._client.new_session(self._cwd)
        assert self._requested_session_id is not None
        method = "load_session" if self._session_mode is SessionOpenMode.LOAD else "resume_session"
        return getattr(self._client, method)(self._requested_session_id, self._cwd)

    def _bind_new_session(self, session_id: str) -> WorkerBinding:
        callback = self._session_binding_callback
        if callback is None:  # pragma: no cover - constructor invariant
            raise AcpRuntimeBindingError("ACP session binding is unavailable")
        continuity = self._binding
        self._require_current_binding(continuity)
        bound = callback(session_id, continuity)
        with self._idle_condition:
            if self._state is not RuntimeState.STARTING:
                raise AcpRuntimeStateError(
                    "ACP runtime stopped during session binding"
                )
        if not isinstance(bound, WorkerBinding):
            raise AcpRuntimeBindingError("ACP session binder returned an invalid binding")
        normalized = replace(
            bound,
            backend=continuity.backend,
            turn_target_kind=continuity.turn_target_kind,
            turn_target_value=continuity.turn_target_value,
            observed_at=continuity.observed_at,
            expires_at=continuity.expires_at,
            private_fingerprint=continuity.private_fingerprint,
        )
        if normalized != continuity:
            raise AcpRuntimeBindingError("ACP session binder changed worker continuity")
        if (
            bound.backend != "acp"
            or bound.turn_target_kind != "acp_session_id"
            or bound.turn_target_value != session_id
        ):
            raise AcpRuntimeBindingError("ACP session binder returned the wrong session")
        if (
            not bound.private_fingerprint
            or bound.private_fingerprint == continuity.private_fingerprint
        ):
            raise AcpRuntimeBindingError(
                "ACP session binder did not establish a distinct private binding"
            )
        # The coordinator owns creation and retirement; the runtime verifies
        # that both continuity and the returned session lease are current.
        self._require_current_binding(continuity)
        self._require_current_binding(bound)
        return bound

    def _require_current_binding(self, expected: WorkerBinding) -> None:
        db_path = self._config.db_path
        current = list_worker_bindings(
            Path(db_path),
            expected.host_id,
            backend=expected.backend,
        )
        # Observation timestamps are lease metadata, not route authority.
        # The store canonicalizes them and Herdr may refresh them while an
        # endpoint starts. Revocation is still fail-closed because the query
        # excludes expired rows and every identity/routing field remains exact.
        present = any(_same_binding_authority(item, expected) for item in current)
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
                if self._ingestor is None:
                    if isinstance(event, PermissionRequest):
                        self._client.respond_permission(event.request_id, cancelled=True)
                    continue
                if isinstance(event, SessionUpdate):
                    if event.session_id != self._session_id:
                        raise AcpRuntimeProtocolError(
                            "ACP update belongs to a different session"
                        )
                    ingestor = self._require_ingestor()
                    with self._ingest_lock:
                        outcome = ingestor.ingest_update(
                            event.raw,
                        )
                        _raise_for_binding_rejection(outcome)
                    if getattr(outcome, "event", None) is not None:
                        self._increment("updates_ingested")
                elif isinstance(event, PermissionRequest):
                    self._handle_permission(event)
                else:  # pragma: no cover - typed protocol invariant
                    raise AcpRuntimeProtocolError("ACP session event type is invalid")
        except BaseException as exc:
            if not self._stop_event.is_set():
                self._record_failure(exc)

    def _handle_permission(
        self,
        request: PermissionRequest,
    ) -> None:
        """Journal then resolve one permission, failing closed before response."""

        try:
            if request.session_id != self._session_id:
                raise AcpRuntimeProtocolError(
                    "ACP permission belongs to a different session"
                )
            ingestor = self._require_ingestor()
            with self._ingest_lock:
                outcome = ingestor.ingest_permission_request(
                    request.raw,
                    source_event_id="permission:" + stable_fingerprint(
                        {"request_id": request.request_id}
                    ),
                )
                _raise_for_binding_rejection(outcome)
            if getattr(outcome, "event", None) is not None:
                self._increment("permissions_ingested")

            callback_failure: BaseException | None = None
            try:
                candidate = (
                    self._permission_callback(request)
                    if self._permission_callback is not None
                    else None
                )
            except BaseException as exc:
                candidate = None
                callback_failure = exc
            selection = (
                candidate if isinstance(candidate, PermissionSelection) else None
            )
            selected = selection.option_id if selection is not None else candidate
            offered = {option.option_id for option in request.options}
            if selected is not None and selected not in offered:
                self._increment("invalid_permission_selections")
                selected = None
                selection = None
        except BaseException:
            self._cancel_permission(request.request_id)
            raise
        try:
            self._client.respond_permission(
                request.request_id,
                option_id=selected,
                cancelled=selected is None,
            )
        except BaseException as exc:
            if selection is not None:
                selection.response_failed(exc)
            raise
        if selection is not None:
            selection.response_written()
        counter = "permissions_selected" if selected is not None else "permissions_cancelled"
        self._increment(counter)
        if callback_failure is not None:
            raise callback_failure

    def _cancel_permission(self, request_id: object) -> None:
        """Fail closed only before any response bytes have been attempted."""

        try:
            self._client.respond_permission(request_id, cancelled=True)
        except BaseException:
            return
        self._increment("permissions_cancelled")

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
        with self._idle_condition:
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
        self._increment("cancellation_requests")

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

        with suppress(BaseException):
            self._cancel_session(session_id)
        with suppress(BaseException):
            # The prompt response (including an invalid one) can overtake the
            # consumer thread after earlier session/update frames were queued.
            # Preserve those updates in the failed turn before writing its
            # terminal marker. A late post-cancel update remains harmless:
            # the ingestor rejects turn-scoped updates after completion.
            self._wait_for_event_idle(self._stop_timeout)
        with suppress(BaseException):
            with self._ingest_lock:
                completion = ingestor.mark_prompt_complete(StopReason.CANCELLED)
                _raise_for_binding_rejection(completion)

    def _record_failure(self, failure: BaseException) -> None:
        with self._idle_condition:
            if self._failure is None:
                self._failure = failure
            self._state = RuntimeState.FAILED
            self._stop_event.set()
            self._idle_condition.notify_all()


def _prepare_prompt_content(
    client: AcpSessionConnection,
    prompt: str | Sequence[Mapping[str, Any]],
) -> tuple[Mapping[str, Any], ...]:
    """Validate before persistence when the transport exposes its validator."""

    prepared = tuple(client.prepare_prompt(prompt))
    if not prepared or any(not isinstance(block, Mapping) for block in prepared):
        raise ValueError("prompt must contain at least one content block")
    return tuple(dict(block) for block in prepared)


def _same_binding_authority(left: WorkerBinding, right: WorkerBinding) -> bool:
    """Compare durable route authority while ignoring observer lease refreshes."""
    return replace(
        left,
        observed_at=right.observed_at,
        expires_at=right.expires_at,
    ) == right


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
