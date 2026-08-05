"""Synchronous ACP v1 client over a supervised subprocess's stdio.

The public client is intentionally independent from Tendwire's daemon and
persistence layers.  A single reader thread owns stdout so concurrent requests,
streamed ``session/update`` notifications, and agent permission requests cannot
steal each other's frames.
"""

from __future__ import annotations

import math
import os
import queue
import select
import signal
import subprocess
import threading
import time
from collections import deque
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any, Callable

from acp.schema import (
    HttpMcpServer,
    InitializeResponse as UpstreamInitializeResponse,
    LoadSessionResponse as UpstreamLoadSessionResponse,
    McpServerStdio,
    NewSessionResponse as UpstreamNewSessionResponse,
    PromptRequest as UpstreamPromptRequest,
    PromptResponse as UpstreamPromptResponse,
    ResumeSessionResponse as UpstreamResumeSessionResponse,
    SseMcpServer,
)
from pydantic import ValidationError

from tendwire import __version__

from .acp_protocol import (
    ACP_PROTOCOL_VERSION,
    DEFAULT_MAX_FRAME_BYTES,
    AcpEnvelopeError,
    AcpFramingError,
    AcpProtocolError,
    JsonRpcNotification,
    JsonRpcRequest,
    JsonRpcResponse,
    PermissionRequest,
    RequestId,
    SessionUpdate,
    StopReason,
    SteeringOutcome,
    decode_json_line,
    encode_message,
    parse_permission_request,
    parse_session_update,
)

_DEFAULT_REQUEST_TIMEOUT = 30.0
_DEFAULT_PROMPT_TIMEOUT = 60.0 * 60.0
_DEFAULT_CLOSE_TIMEOUT = 3.0
_DEFAULT_QUEUE_SIZE = 4096
_METHOD_NOT_FOUND = -32601


class AcpClientError(AcpProtocolError):
    """Base error for ACP client lifecycle and transport failures."""


class AcpClientStateError(AcpClientError, RuntimeError):
    """An operation is invalid in the client's current state."""


class AcpTransportError(AcpClientError, ConnectionError):
    """The ACP subprocess or stdio stream failed."""


class AcpRequestTimeoutError(AcpClientError, TimeoutError):
    """A request did not receive a response before its deadline."""


class AcpCapabilityError(AcpClientError, RuntimeError):
    """The requested optional method was not advertised by the agent."""


class AcpProtocolVersionError(AcpClientError):
    """The agent selected an ACP version this client does not support."""


class AcpEventQueueFullError(AcpTransportError):
    """The consumer fell behind the bounded lossless event queues."""


class ClientState(str, Enum):
    NEW = "new"
    RUNNING = "running"
    INITIALIZED = "initialized"
    CLOSING = "closing"
    CLOSED = "closed"
    FAILED = "failed"


@dataclass(slots=True)
class _PendingRequest:
    waiter: queue.Queue[JsonRpcResponse | BaseException]


SessionEvent = SessionUpdate | PermissionRequest


class BoundedAcpConnection:
    """Thread-safe ACP connection with bounded subprocess stdio framing."""

    def __init__(
        self,
        argv: Sequence[str | os.PathLike[str]],
        *,
        cwd: str | os.PathLike[str] | None = None,
        env: Mapping[str, str] | None = None,
        request_timeout: float = _DEFAULT_REQUEST_TIMEOUT,
        prompt_timeout: float = _DEFAULT_PROMPT_TIMEOUT,
        close_timeout: float = _DEFAULT_CLOSE_TIMEOUT,
        max_frame_bytes: int = DEFAULT_MAX_FRAME_BYTES,
        max_pending_events: int = _DEFAULT_QUEUE_SIZE,
    ) -> None:
        command = tuple(os.fspath(item) for item in argv)
        if not command or any(
            not isinstance(item, str) or not item or "\x00" in item for item in command
        ):
            raise ValueError("argv must contain at least one non-empty argument")
        self.argv = command
        self.cwd = _absolute_path(cwd, "process cwd") if cwd is not None else None
        self.env = _validated_env(env)
        self.request_timeout = _positive_timeout(request_timeout, "request_timeout")
        self.prompt_timeout = _positive_timeout(prompt_timeout, "prompt_timeout")
        self.close_timeout = _positive_timeout(close_timeout, "close_timeout")
        if max_frame_bytes <= 0:
            raise ValueError("max_frame_bytes must be positive")
        if max_pending_events <= 0:
            raise ValueError("max_pending_events must be positive")
        self.max_frame_bytes = max_frame_bytes
        self.max_pending_events = max_pending_events
        self._state = ClientState.NEW
        self._process: subprocess.Popen[bytes] | None = None
        self._state_lock = threading.RLock()
        self._write_lock = threading.Lock()
        self._initialize_lock = threading.Lock()
        self._request_id_lock = threading.Lock()
        self._next_id = 1
        self._pending: dict[RequestId, _PendingRequest] = {}
        self._pending_lock = threading.Lock()
        self._pending_permissions: dict[RequestId, PermissionRequest] = {}
        self._cancelled_sessions: set[str] = set()
        self._active_prompts: dict[str, int] = {}
        self._permission_lock = threading.Lock()
        # Updates and permission requests share one reader-ordered queue.  A
        # pair of duplicate queues can both reorder cross-kind events and fail
        # the transport when an embedding consumes only one of them.
        self._session_events: deque[SessionEvent] = deque()
        self._session_event_condition = threading.Condition()
        self._reader_thread: threading.Thread | None = None
        self._stderr_thread: threading.Thread | None = None
        self._stop = threading.Event()
        self._closed = threading.Event()
        self._failure: BaseException | None = None
        self._capabilities: Mapping[str, Any] | None = None
        self._steering_supported = False

    @property
    def state(self) -> ClientState:
        with self._state_lock:
            return self._state

    @property
    def capabilities(self) -> Mapping[str, Any] | None:
        return self._capabilities

    @property
    def steering_supported(self) -> bool:
        """Whether the agent explicitly advertised the steering extension."""

        return self._steering_supported

    def start(self) -> "BoundedAcpConnection":
        with self._state_lock:
            if self._state in {ClientState.RUNNING, ClientState.INITIALIZED}:
                return self
            if self._state is not ClientState.NEW:
                raise AcpClientStateError(f"cannot start ACP client in state {self._state.value}")
            try:
                process = subprocess.Popen(
                    self.argv,
                    stdin=subprocess.PIPE,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    cwd=self.cwd,
                    env=self.env,
                    bufsize=0,
                    close_fds=True,
                    start_new_session=os.name == "posix",
                )
            except OSError as exc:
                self._state = ClientState.FAILED
                self._failure = AcpTransportError(
                    f"could not start ACP agent {self.argv[0]!r}"
                )
                raise self._failure from exc
            self._process = process
            assert process.stdin is not None
            try:
                os.set_blocking(process.stdin.fileno(), False)
            except OSError as exc:
                process.kill()
                process.wait()
                self._state = ClientState.FAILED
                self._failure = AcpTransportError(
                    "could not configure ACP agent stdin"
                )
                raise self._failure from exc
            self._state = ClientState.RUNNING
            self._reader_thread = threading.Thread(
                target=self._reader_main,
                name=f"acp-reader-{process.pid}",
                daemon=True,
            )
            self._stderr_thread = threading.Thread(
                target=self._stderr_main,
                name=f"acp-stderr-{process.pid}",
                daemon=True,
            )
            self._reader_thread.start()
            self._stderr_thread.start()
        return self

    def initialize(
        self,
        *,
        client_capabilities: Mapping[str, Any] | None = None,
        client_name: str = "tendwire",
        client_version: str = __version__,
        client_title: str = "Tendwire",
        timeout: float | None = None,
    ) -> Mapping[str, Any]:
        """Perform ACP v1 capability negotiation.

        ACP v1 does not define a post-response ``initialized`` notification.
        """
        self.start()
        if not client_name or not client_version:
            raise ValueError("client_name and client_version must be non-empty")
        with self._initialize_lock:
            with self._state_lock:
                if self._state is ClientState.INITIALIZED:
                    assert self._capabilities is not None
                    return self._capabilities
                if self._state is not ClientState.RUNNING:
                    self._raise_unusable()
            client_info: dict[str, Any] = {
                "name": client_name,
                "version": client_version,
            }
            if client_title:
                client_info["title"] = client_title
            result = self.request(
                "initialize",
                {
                    "protocolVersion": ACP_PROTOCOL_VERSION,
                    "clientCapabilities": dict(client_capabilities or {}),
                    "clientInfo": client_info,
                },
                timeout=timeout,
                require_initialized=False,
            )
            raw = _require_mapping(result, "initialize result")
            _validate_upstream(
                UpstreamInitializeResponse,
                raw,
                "initialize result",
            )
            version = raw.get("protocolVersion")
            if (
                not isinstance(version, int)
                or isinstance(version, bool)
                or version != ACP_PROTOCOL_VERSION
            ):
                failure = AcpProtocolVersionError(
                    f"agent selected unsupported ACP protocol version {version!r}"
                )
                self._set_failed(failure)
                # ACP v1 says clients should close a connection after an
                # unsupported selection.  Do the bounded cleanup here so direct
                # callers cannot accidentally leave the adapter process alive.
                try:
                    self.close()
                except BaseException:
                    pass
                raise failure
            capabilities_value = raw.get("agentCapabilities", {})
            if not isinstance(capabilities_value, Mapping):
                capabilities_value = {}
            meta = raw.get("_meta")
            steering = meta.get("steering") if isinstance(meta, Mapping) else None
            self._steering_supported = (
                isinstance(steering, Mapping) and steering.get("supported") is True
            )
            parsed = dict(capabilities_value)
            with self._state_lock:
                if self._state is not ClientState.RUNNING:
                    self._raise_unusable()
                self._capabilities = parsed
                self._state = ClientState.INITIALIZED
            return parsed

    def request(
        self,
        method: str,
        params: Mapping[str, Any] | None = None,
        *,
        timeout: float | None = None,
        require_initialized: bool = True,
        on_writing: Callable[[], None] | None = None,
        on_written: Callable[[], None] | None = None,
    ) -> Any:
        if require_initialized:
            self._require_initialized()
        else:
            self._require_running()
        wait_timeout = self.request_timeout if timeout is None else _positive_timeout(
            timeout, "timeout"
        )
        deadline = time.monotonic() + wait_timeout
        request_id = self._new_request_id()
        waiter: queue.Queue[JsonRpcResponse | BaseException] = queue.Queue(maxsize=1)
        pending = _PendingRequest(waiter)
        with self._pending_lock:
            self._pending[request_id] = pending
        try:
            self._write(
                {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "method": method,
                    "params": dict(params or {}),
                },
                deadline=deadline,
                on_writing=on_writing,
            )
            if on_written is not None:
                on_written()
        except BaseException:
            with self._pending_lock:
                if self._pending.get(request_id) is pending:
                    self._pending.pop(request_id, None)
            raise
        try:
            response = waiter.get(timeout=max(0.0, deadline - time.monotonic()))
        except queue.Empty as exc:
            with self._pending_lock:
                if self._pending.get(request_id) is pending:
                    self._pending.pop(request_id, None)
                try:
                    response = waiter.get_nowait()
                except queue.Empty:
                    response = None
            if response is not None:
                if isinstance(response, BaseException):
                    raise response
                return response.result_or_raise()
            raise AcpRequestTimeoutError(
                f"ACP request {method!r} timed out after {wait_timeout:g}s"
            ) from exc
        with self._pending_lock:
            if self._pending.get(request_id) is pending:
                self._pending.pop(request_id, None)
        if isinstance(response, BaseException):
            raise response
        return response.result_or_raise()

    def new_session(
        self,
        cwd: str | os.PathLike[str],
        *,
        mcp_servers: Sequence[Mapping[str, Any]] = (),
        additional_directories: Sequence[str | os.PathLike[str]] = (),
        timeout: float | None = None,
    ) -> str:
        params = self._session_setup_params(
            cwd,
            mcp_servers=mcp_servers,
            additional_directories=additional_directories,
        )
        result = self.request("session/new", params, timeout=timeout)
        raw = _require_mapping(result, "session/new result")
        _validate_upstream(UpstreamNewSessionResponse, raw, "session/new result")
        return _parse_session_result(raw, require_session_id=True)

    def load_session(
        self,
        session_id: str,
        cwd: str | os.PathLike[str],
        *,
        mcp_servers: Sequence[Mapping[str, Any]] = (),
        additional_directories: Sequence[str | os.PathLike[str]] = (),
        timeout: float | None = None,
    ) -> str:
        return self._open_existing_session(
            "load", session_id, cwd, mcp_servers, additional_directories, timeout
        )

    def resume_session(
        self,
        session_id: str,
        cwd: str | os.PathLike[str],
        *,
        mcp_servers: Sequence[Mapping[str, Any]] = (),
        additional_directories: Sequence[str | os.PathLike[str]] = (),
        timeout: float | None = None,
    ) -> str:
        return self._open_existing_session(
            "resume", session_id, cwd, mcp_servers, additional_directories, timeout
        )

    def _open_existing_session(
        self,
        operation: str,
        session_id: str,
        cwd: str | os.PathLike[str],
        mcp_servers: Sequence[Mapping[str, Any]],
        additional_directories: Sequence[str | os.PathLike[str]],
        timeout: float | None,
    ) -> str:
        self._require_capability(
            "loadSession" if operation == "load" else "sessionResume"
        )
        params = self._session_setup_params(
            cwd,
            mcp_servers=mcp_servers,
            additional_directories=additional_directories,
        )
        params["sessionId"] = _nonempty(session_id, "session_id")
        method = f"session/{operation}"
        result = self.request(method, params, timeout=timeout)
        if operation == "load" and result is None:
            return session_id
        raw = _require_mapping(result, f"{method} result")
        model = (
            UpstreamLoadSessionResponse
            if operation == "load"
            else UpstreamResumeSessionResponse
        )
        _validate_upstream(model, raw, f"{method} result")
        _parse_session_result(raw, require_session_id=False)
        return session_id

    def prompt(
        self,
        session_id: str,
        prompt: str | Sequence[Mapping[str, Any]],
        *,
        timeout: float | None = None,
        on_send_start: Callable[[], None] | None = None,
        on_submitted: Callable[[], None] | None = None,
    ) -> StopReason:
        content = list(self.prepare_prompt(prompt))
        session_id = _nonempty(session_id, "session_id")
        with self._permission_lock:
            if self._active_prompts.get(session_id, 0) == 0:
                # A new turn supersedes an unconfirmed cancellation from a
                # prior timed-out turn.
                self._cancelled_sessions.discard(session_id)
            self._active_prompts[session_id] = self._active_prompts.get(session_id, 0) + 1
        response_received = False
        try:
            _validate_upstream(
                UpstreamPromptRequest,
                {"sessionId": session_id, "prompt": content},
                "session/prompt params",
            )
            result = self.request(
                "session/prompt",
                {"sessionId": session_id, "prompt": content},
                timeout=self.prompt_timeout if timeout is None else timeout,
                on_writing=on_send_start,
                on_written=on_submitted,
            )
            response_received = True
        finally:
            with self._permission_lock:
                active = self._active_prompts.get(session_id, 0) - 1
                if active > 0:
                    self._active_prompts[session_id] = active
                else:
                    self._active_prompts.pop(session_id, None)
                    if response_received:
                        self._cancelled_sessions.discard(session_id)
        raw = _require_mapping(result, "session/prompt result")
        _validate_upstream(UpstreamPromptResponse, raw, "session/prompt result")
        stop_reason = raw.get("stopReason")
        try:
            parsed_reason = StopReason(stop_reason)
        except (ValueError, TypeError) as exc:
            raise AcpEnvelopeError("session/prompt returned an invalid stopReason") from exc
        return parsed_reason

    def steer_session(
        self,
        session_id: str,
        prompt: str | Sequence[Mapping[str, Any]],
        *,
        timeout: float | None = None,
        on_send_start: Callable[[], None] | None = None,
        on_submitted: Callable[[], None] | None = None,
    ) -> SteeringOutcome:
        """Inject input into an active turn through an advertised extension.

        ``codex-acp`` serializes these requests per session and either injects
        into the live turn or starts a new turn after the prior one drains.
        The method is never used unless the initialize response opted in.
        """

        if not self.steering_supported:
            raise AcpCapabilityError("agent did not advertise steering capability")
        content = list(self.prepare_prompt(prompt))
        result = self.request(
            "_session/steering",
            {
                "sessionId": _nonempty(session_id, "session_id"),
                "prompt": content,
            },
            timeout=timeout,
            on_writing=on_send_start,
            on_written=on_submitted,
        )
        raw = _require_mapping(result, "_session/steering result")
        try:
            outcome = SteeringOutcome(raw.get("outcome"))
        except (TypeError, ValueError) as exc:
            raise AcpEnvelopeError(
                "_session/steering returned an invalid outcome"
            ) from exc
        return outcome

    def prepare_prompt(
        self,
        prompt: str | Sequence[Mapping[str, Any]],
    ) -> tuple[Mapping[str, Any], ...]:
        """Validate prompt content without sending it to the agent."""

        if isinstance(prompt, str):
            content: list[Mapping[str, Any]] = [{"type": "text", "text": prompt}]
        else:
            content = list(prompt)
        if not content:
            raise ValueError("prompt must contain at least one content block")
        self._require_initialized()
        assert self.capabilities is not None
        return tuple(
            _validated_prompt_content_block(block, self.capabilities)
            for block in content
        )

    def cancel(self, session_id: str) -> None:
        """Cancel a turn and cancel all outstanding permissions for the session."""
        session_id = _nonempty(session_id, "session_id")
        with self._permission_lock:
            self._cancelled_sessions.add(session_id)
        try:
            self._require_running()
            self._write({"jsonrpc": "2.0", "method": "session/cancel", "params": {"sessionId": session_id}})
        except BaseException:
            with self._permission_lock:
                self._cancelled_sessions.discard(session_id)
            raise
        while True:
            with self._permission_lock:
                pending_ids = [
                    request_id
                    for request_id, request in self._pending_permissions.items()
                    if request.session_id == session_id
                ]
            if not pending_ids:
                break
            for request_id in pending_ids:
                try:
                    self.respond_permission(request_id, cancelled=True)
                except AcpClientStateError:
                    # A consumer may have resolved it concurrently.
                    continue
        with self._permission_lock:
            if self._active_prompts.get(session_id, 0) == 0:
                self._cancelled_sessions.discard(session_id)

    def respond_permission(
        self,
        request_id: RequestId,
        *,
        option_id: str | None = None,
        cancelled: bool = False,
    ) -> None:
        if cancelled == (option_id is not None):
            raise ValueError("select exactly one of option_id or cancelled=True")
        with self._permission_lock:
            request = self._pending_permissions.get(request_id)
            if request is None:
                raise AcpClientStateError("permission request is not pending")
            if option_id is not None and option_id not in {
                option.option_id for option in request.options
            }:
                raise ValueError("option_id was not offered by this permission request")
            del self._pending_permissions[request_id]
        if cancelled:
            outcome = {"outcome": "cancelled"}
        else:
            outcome = {"outcome": "selected", "optionId": option_id}
        try:
            self._write(
                {"jsonrpc": "2.0", "id": request_id, "result": {"outcome": outcome}}
            )
        except BaseException:
            # A partial frame may have reached the peer.  Retrying the same
            # JSON-RPC response is unsafe; transport failure is terminal.
            raise

    def next_session_event(self, *, timeout: float | None = None) -> SessionEvent:
        """Return the next update or permission request in exact reader order."""
        deadline = None
        if timeout is not None:
            deadline = time.monotonic() + _positive_timeout(timeout, "timeout")
        with self._session_event_condition:
            while True:
                while self._session_events:
                    candidate = self._session_events.popleft()
                    if isinstance(candidate, PermissionRequest):
                        with self._permission_lock:
                            if self._pending_permissions.get(candidate.request_id) is not candidate:
                                continue
                    self._session_event_condition.notify_all()
                    return candidate
                if self.state in {
                    ClientState.FAILED,
                    ClientState.CLOSING,
                    ClientState.CLOSED,
                }:
                    self._raise_unusable()
                remaining = None if deadline is None else deadline - time.monotonic()
                if remaining is not None and remaining <= 0:
                    raise AcpRequestTimeoutError("timed out waiting for ACP session event")
                self._session_event_condition.wait(timeout=remaining)

    def close(self) -> None:
        wait_for_other_close = False
        closed_without_process = False
        with self._state_lock:
            if self._state in {ClientState.CLOSED, ClientState.NEW}:
                self._state = ClientState.CLOSED
                self._closed.set()
                closed_without_process = True
            elif self._state is ClientState.CLOSING:
                wait_for_other_close = True
            else:
                was_failed = self._state is ClientState.FAILED
                self._state = ClientState.CLOSING
        if closed_without_process:
            self._signal_queues()
            return
        if wait_for_other_close:
            self._closed.wait(timeout=self.close_timeout * 3)
            return
        process = self._process
        try:
            if process is not None:
                acquired_write = self._write_lock.acquire(timeout=self.close_timeout)
                if not acquired_write:
                    # A blocked frame cannot be completed safely during close.
                    # Terminating our private process group wakes the writer.
                    self._signal_process(process, signal.SIGTERM)
                    acquired_write = self._write_lock.acquire(
                        timeout=self.close_timeout
                    )
                if not acquired_write:
                    self._signal_process(process, signal.SIGKILL)
                    acquired_write = self._write_lock.acquire(
                        timeout=self.close_timeout
                    )
                if not acquired_write:
                    raise AcpTransportError("timed out closing ACP agent stdin")
                try:
                    if process.stdin is not None:
                        try:
                            process.stdin.close()
                        except OSError:
                            pass
                finally:
                    self._write_lock.release()
                try:
                    process.wait(timeout=self.close_timeout)
                except subprocess.TimeoutExpired:
                    self._signal_process(process, signal.SIGTERM)
                    try:
                        process.wait(timeout=self.close_timeout)
                    except subprocess.TimeoutExpired:
                        self._signal_process(process, signal.SIGKILL)
                        process.wait(timeout=self.close_timeout)
                # The adapter can exit while leaving a spawned agent process
                # holding its inherited stdio descriptors.  The private process
                # group makes those descendants safe to terminate as one unit.
                self._signal_process(process, signal.SIGTERM)
            self._stop.set()
            for thread in (self._reader_thread, self._stderr_thread):
                if thread is not None and thread is not threading.current_thread():
                    thread.join(timeout=self.close_timeout)
            if process is not None and any(
                thread is not None and thread.is_alive()
                for thread in (self._reader_thread, self._stderr_thread)
            ):
                self._signal_process(process, signal.SIGKILL)
                for thread in (self._reader_thread, self._stderr_thread):
                    if thread is not None and thread is not threading.current_thread():
                        thread.join(timeout=self.close_timeout)
            self._fail_pending(AcpTransportError("ACP client closed"))
            with self._permission_lock:
                self._pending_permissions.clear()
                self._cancelled_sessions.clear()
                self._active_prompts.clear()
            self._signal_queues()
            with self._state_lock:
                self._state = ClientState.FAILED if was_failed else ClientState.CLOSED
        except BaseException as exc:
            with self._state_lock:
                if self._failure is None:
                    self._failure = AcpTransportError(
                        f"failed to stop ACP agent: {type(exc).__name__}"
                    )
                self._state = ClientState.FAILED
            self._fail_pending(self._failure)
            self._signal_queues()
            raise
        finally:
            self._stop.set()
            self._closed.set()

    def _session_setup_params(
        self,
        cwd: str | os.PathLike[str],
        *,
        mcp_servers: Sequence[Mapping[str, Any]],
        additional_directories: Sequence[str | os.PathLike[str]],
    ) -> dict[str, Any]:
        self._require_initialized()
        params: dict[str, Any] = {
            "cwd": _absolute_path(cwd, "cwd"),
            "mcpServers": [self._validated_mcp_server(server) for server in mcp_servers],
        }
        directories = [
            _absolute_path(directory, "additional directory")
            for directory in additional_directories
        ]
        if directories:
            self._require_capability("additionalDirectories")
            params["additionalDirectories"] = directories
        return params

    def _validated_mcp_server(self, server: Mapping[str, Any]) -> dict[str, Any]:
        assert self.capabilities is not None
        return _validated_mcp_server(server, self.capabilities)

    def _require_capability(self, name: str) -> None:
        self._require_initialized()
        assert self.capabilities is not None
        supported = {
            "loadSession": _capability(self.capabilities, "loadSession"),
            "sessionResume": _capability(
                self.capabilities, "sessionCapabilities", "resume", object_value=True
            ),
            "additionalDirectories": _capability(
                self.capabilities,
                "sessionCapabilities",
                "additionalDirectories",
                object_value=True,
            ),
        }.get(name, False)
        if not supported:
            raise AcpCapabilityError(f"agent did not advertise {name} capability")

    def _new_request_id(self) -> int:
        with self._request_id_lock:
            request_id = self._next_id
            if request_id > 2**63 - 1:
                raise AcpClientStateError("ACP request ID space exhausted")
            self._next_id += 1
        return request_id

    def _write(
        self,
        envelope: Mapping[str, Any],
        *,
        deadline: float | None = None,
        on_writing: Callable[[], None] | None = None,
    ) -> None:
        payload = encode_message(envelope, max_frame_bytes=self.max_frame_bytes)
        if deadline is None:
            deadline = time.monotonic() + self.request_timeout
        remaining_timeout = max(0.0, deadline - time.monotonic())
        if not self._write_lock.acquire(timeout=remaining_timeout):
            raise AcpRequestTimeoutError("timed out waiting to write ACP frame")
        try:
            self._require_running()
            process = self._process
            if process is None or process.stdin is None:
                raise AcpTransportError("ACP agent stdin is unavailable")
            try:
                fd = process.stdin.fileno()
                remaining = memoryview(payload)
                bytes_written = 0
                write_started = False
                while remaining:
                    wait = deadline - time.monotonic()
                    if wait <= 0:
                        raise AcpRequestTimeoutError(
                            "timed out writing ACP frame to agent"
                        )
                    if not self._wait_writable(fd, wait):
                        raise AcpRequestTimeoutError(
                            "timed out writing ACP frame to agent"
                        )
                    # Cross the durable send boundary only after the write
                    # lock is held and the transport reports writable. A
                    # timeout waiting for either condition has written zero
                    # bytes and remains safely retryable. The callback still
                    # runs before the first write, so a process crash cannot
                    # leave an externally visible frame without a receipt.
                    if not write_started:
                        if on_writing is not None:
                            on_writing()
                        write_started = True
                    try:
                        written = self._write_chunk(fd, remaining)
                    except BlockingIOError:
                        continue
                    if written <= 0:
                        raise BrokenPipeError("zero-byte write to ACP agent stdin")
                    bytes_written += written
                    remaining = remaining[written:]
            except AcpRequestTimeoutError as exc:
                if bytes_written:
                    failure = AcpTransportError(
                        "ACP frame write timed out after a partial write"
                    )
                    self._set_failed(failure)
                    raise failure from exc
                raise
            except (BrokenPipeError, OSError) as exc:
                failure = AcpTransportError("ACP agent stdin disconnected")
                self._set_failed(failure)
                raise failure from exc
        finally:
            self._write_lock.release()

    @staticmethod
    def _wait_writable(fd: int, timeout: float) -> bool:
        _, writable, _ = select.select([], [fd], [], timeout)
        return bool(writable)

    @staticmethod
    def _write_chunk(fd: int, data: memoryview) -> int:
        return os.write(fd, data)

    @staticmethod
    def _signal_process(process: subprocess.Popen[bytes], signum: int) -> None:
        try:
            if os.name == "posix":
                os.killpg(process.pid, signum)
            elif signum == signal.SIGTERM:
                process.terminate()
            else:
                process.kill()
        except ProcessLookupError:
            pass

    def _reader_main(self) -> None:
        process = self._process
        assert process is not None and process.stdout is not None
        buffer = bytearray()
        try:
            while not self._stop.is_set():
                newline = buffer.find(b"\n")
                if newline < 0:
                    room = self.max_frame_bytes + 1 - len(buffer)
                    chunk = os.read(process.stdout.fileno(), min(64 * 1024, room))
                    if not chunk:
                        if self._stop.is_set() or self.state is ClientState.CLOSING:
                            return
                        if buffer:
                            raise AcpFramingError("ACP stdout ended during a JSON frame")
                        returncode = process.poll()
                        suffix = f" (exit {returncode})" if returncode is not None else ""
                        raise AcpTransportError(f"ACP agent stdout disconnected{suffix}")
                    buffer.extend(chunk)
                    if len(buffer) > self.max_frame_bytes and b"\n" not in buffer:
                        raise AcpFramingError(
                            "ACP stdout frame exceeds configured size limit"
                        )
                    continue
                line = bytes(buffer[: newline + 1])
                del buffer[: newline + 1]
                if len(line) > self.max_frame_bytes:
                    raise AcpFramingError("ACP stdout frame exceeds configured size limit")
                message = decode_json_line(
                    line,
                    max_frame_bytes=self.max_frame_bytes,
                )
                self._dispatch(message)
        except BaseException as exc:
            if not self._stop.is_set():
                self._set_failed(exc)

    def _stderr_main(self) -> None:
        process = self._process
        assert process is not None and process.stderr is not None
        try:
            while True:
                if not os.read(process.stderr.fileno(), 4096):
                    return
        except OSError:
            return

    def _dispatch(
        self, message: JsonRpcRequest | JsonRpcNotification | JsonRpcResponse
    ) -> None:
        if isinstance(message, JsonRpcResponse):
            if message.request_id is None:
                return
            with self._pending_lock:
                pending = self._pending.get(message.request_id)
                if pending is not None:
                    try:
                        pending.waiter.put_nowait(message)
                    except queue.Full as exc:
                        raise AcpEnvelopeError(
                            "duplicate ACP response for one request ID"
                        ) from exc
            return
        if isinstance(message, JsonRpcNotification):
            if message.method == "session/update":
                self._put_session_event(parse_session_update(message.params))
            return

        if message.method == "session/request_permission":
            try:
                parsed = parse_permission_request(message)
            except AcpProtocolError as exc:
                self._write(
                    {
                        "jsonrpc": "2.0",
                        "id": message.request_id,
                        "error": {
                            "code": -32602,
                            "message": "Invalid permission request",
                            "data": str(exc),
                        },
                    }
                )
                return
            with self._permission_lock:
                if message.request_id in self._pending_permissions:
                    raise AcpEnvelopeError("duplicate pending permission request id")
                cancelled = parsed.session_id in self._cancelled_sessions
                if not cancelled:
                    self._pending_permissions[message.request_id] = parsed
            if cancelled:
                self._write(
                    {
                        "jsonrpc": "2.0",
                        "id": message.request_id,
                        "result": {"outcome": {"outcome": "cancelled"}},
                    }
                )
            else:
                self._put_session_event(parsed)
        else:
            self._write(
                {
                    "jsonrpc": "2.0",
                    "id": message.request_id,
                    "error": {"code": _METHOD_NOT_FOUND, "message": "Method not found"},
                }
            )

    def _put_session_event(self, value: SessionEvent) -> None:
        deadline = time.monotonic() + min(self.request_timeout, 0.5)
        with self._session_event_condition:
            while len(self._session_events) >= self.max_pending_events:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise AcpEventQueueFullError(
                        "ACP event queue is full; refusing to drop protocol data"
                    )
                self._session_event_condition.wait(timeout=remaining)
            self._session_events.append(value)
            self._session_event_condition.notify_all()

    def _set_failed(self, failure: BaseException) -> None:
        if not isinstance(failure, AcpClientError | AcpProtocolError):
            failure = AcpTransportError(str(failure))
        with self._state_lock:
            if self._state in {ClientState.CLOSING, ClientState.CLOSED}:
                return
            self._failure = failure
            self._state = ClientState.FAILED
        self._stop.set()
        self._fail_pending(failure)
        self._signal_queues()

    def _fail_pending(self, failure: BaseException) -> None:
        with self._pending_lock:
            pending_requests = tuple(self._pending.values())
            self._pending.clear()
        for pending in pending_requests:
            try:
                pending.waiter.put_nowait(failure)
            except queue.Full:
                pass

    def _signal_queues(self) -> None:
        with self._session_event_condition:
            self._session_event_condition.notify_all()

    def _require_running(self) -> None:
        with self._state_lock:
            if self._state not in {ClientState.RUNNING, ClientState.INITIALIZED}:
                self._raise_unusable()

    def _require_initialized(self) -> None:
        with self._state_lock:
            if self._state is not ClientState.INITIALIZED:
                if self._state in {ClientState.FAILED, ClientState.CLOSED, ClientState.CLOSING}:
                    self._raise_unusable()
                raise AcpClientStateError("ACP client has not been initialized")

    def _raise_unusable(self) -> None:
        if self._failure is not None:
            raise AcpTransportError(f"ACP client failed: {self._failure}") from self._failure
        raise AcpClientStateError(f"ACP client is {self._state.value}")


def _positive_timeout(value: float | int, name: str) -> float:
    result = float(value)
    if not math.isfinite(result) or result <= 0:
        raise ValueError(f"{name} must be finite and positive")
    return result


def _nonempty(value: str, name: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{name} must be a non-empty string")
    return value


def _absolute_path(value: str | os.PathLike[str], name: str) -> str:
    result = os.fspath(value)
    if not isinstance(result, str) or not result or "\x00" in result:
        raise ValueError(f"{name} must be a non-empty text path without NUL bytes")
    if not Path(result).is_absolute():
        raise ValueError(f"{name} must be an absolute path")
    return result


def _capability(
    capabilities: Mapping[str, Any], *path: str, object_value: bool = False
) -> bool:
    value: Any = capabilities
    for key in path:
        value = value.get(key) if isinstance(value, Mapping) else None
    return isinstance(value, Mapping) if object_value else value is True


def _validated_prompt_content_block(
    value: Mapping[str, Any],
    capabilities: Mapping[str, Any],
) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError("each prompt content block must be an object")
    kind = value.get("type")
    if kind not in {"text", "image", "audio", "resource_link", "resource"}:
        raise ValueError("prompt content block type is not valid ACP v1")
    capability = {
        "image": (_capability(capabilities, "promptCapabilities", "image"), "image"),
        "audio": (_capability(capabilities, "promptCapabilities", "audio"), "audio"),
        "resource": (
            _capability(capabilities, "promptCapabilities", "embeddedContext"),
            "embeddedContext",
        ),
    }.get(kind)
    if capability is not None and not capability[0]:
        raise AcpCapabilityError(
            f"agent did not advertise prompt {capability[1]} capability"
        )
    try:
        prompt = UpstreamPromptRequest.model_validate(
            {"sessionId": "validation", "prompt": [dict(value)]}
        ).model_dump(by_alias=True, exclude_none=True)["prompt"]
    except ValidationError as exc:
        raise ValueError(f"{kind} content does not match the upstream ACP schema") from exc
    return prompt[0]


def _validated_mcp_server(
    value: Mapping[str, Any],
    capabilities: Mapping[str, Any],
) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError("each MCP server must be an object")
    transport = value.get("type")
    models = {None: McpServerStdio, "http": HttpMcpServer, "sse": SseMcpServer}
    model = models.get(transport)
    if model is None:
        raise ValueError("MCP server type is not valid stable ACP v1")
    capability = {
        "http": _capability(capabilities, "mcpCapabilities", "http"),
        "sse": _capability(capabilities, "mcpCapabilities", "sse"),
    }.get(transport, True)
    if not capability:
        raise AcpCapabilityError(
            f"agent did not advertise MCP {str(transport).upper()} capability"
        )
    try:
        result = model.model_validate(dict(value)).model_dump(
            by_alias=True, exclude_none=True
        )
    except ValidationError as exc:
        raise ValueError("MCP server does not match the upstream ACP schema") from exc
    if transport is None:
        result["command"] = _absolute_path(result["command"], "MCP server.command")
    return result


def _validated_env(env: Mapping[str, str] | None) -> dict[str, str] | None:
    if env is None:
        return None
    result: dict[str, str] = {}
    for key, value in env.items():
        if (
            not isinstance(key, str)
            or not key
            or "=" in key
            or "\x00" in key
            or not isinstance(value, str)
            or "\x00" in value
        ):
            raise ValueError("env must contain valid string names and values")
        result[key] = value
    return result


def _require_mapping(value: Any, name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise AcpEnvelopeError(f"{name} must be an object")
    return value


def _validate_upstream(model: Any, value: Mapping[str, Any], name: str) -> None:
    """Validate one stable ACP payload with the official generated schema."""

    try:
        model.model_validate(dict(value))
    except ValidationError as exc:
        raise AcpEnvelopeError(
            f"{name} does not match the upstream ACP schema"
        ) from exc


def _parse_session_result(
    raw: Mapping[str, Any], *, require_session_id: bool
) -> str:
    session_id = raw.get("sessionId", "")
    if require_session_id and (not isinstance(session_id, str) or not session_id):
        raise AcpEnvelopeError("session/new result.sessionId must be a non-empty string")
    return session_id if isinstance(session_id, str) else ""
