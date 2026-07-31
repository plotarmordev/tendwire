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
import subprocess
import threading
from collections import deque
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from types import MappingProxyType
from typing import Any, TypeVar

from tendwire import __version__

from .acp_protocol import (
    ACP_PROTOCOL_VERSION,
    DEFAULT_MAX_FRAME_BYTES,
    AgentCapabilities,
    AcpEnvelopeError,
    AcpFramingError,
    AcpProtocolError,
    InitializeResult,
    JsonRpcNotification,
    JsonRpcRequest,
    JsonRpcResponse,
    PermissionRequest,
    PromptResult,
    RequestId,
    SessionInfo,
    SessionPage,
    SessionResult,
    SessionUpdate,
    StopReason,
    decode_json_line,
    encode_message,
    error_envelope,
    notification_envelope,
    parse_permission_request,
    parse_session_update,
    request_envelope,
    result_envelope,
)

_DEFAULT_REQUEST_TIMEOUT = 30.0
_DEFAULT_PROMPT_TIMEOUT = 60.0 * 60.0
_DEFAULT_CLOSE_TIMEOUT = 3.0
_DEFAULT_QUEUE_SIZE = 4096
_DEFAULT_STDERR_LIMIT = 64 * 1024
_METHOD_NOT_FOUND = -32601
_INTERNAL_ERROR = -32603


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


@dataclass(frozen=True, slots=True)
class RawNotification:
    method: str
    params: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class InboundRequest:
    request_id: RequestId
    method: str
    params: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class ProcessExit:
    returncode: int
    stderr_tail: str


_T = TypeVar("_T")
_END = object()


class AcpClient:
    """Thread-safe, blocking ACP v1 client for one agent subprocess."""

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
        stderr_limit_bytes: int = _DEFAULT_STDERR_LIMIT,
    ) -> None:
        command = tuple(os.fspath(item) for item in argv)
        if not command or any(not item for item in command):
            raise ValueError("argv must contain at least one non-empty argument")
        self.argv = command
        self.cwd = os.fspath(cwd) if cwd is not None else None
        self.env = dict(env) if env is not None else None
        self.request_timeout = _positive_timeout(request_timeout, "request_timeout")
        self.prompt_timeout = _positive_timeout(prompt_timeout, "prompt_timeout")
        self.close_timeout = _positive_timeout(close_timeout, "close_timeout")
        if max_frame_bytes <= 0:
            raise ValueError("max_frame_bytes must be positive")
        if max_pending_events <= 0:
            raise ValueError("max_pending_events must be positive")
        if stderr_limit_bytes <= 0:
            raise ValueError("stderr_limit_bytes must be positive")
        self.max_frame_bytes = max_frame_bytes
        self.max_pending_events = max_pending_events
        self.stderr_limit_bytes = stderr_limit_bytes

        self._state = ClientState.NEW
        self._process: subprocess.Popen[bytes] | None = None
        self._state_lock = threading.RLock()
        self._write_lock = threading.Lock()
        self._request_id_lock = threading.Lock()
        self._next_id = 1
        self._pending: dict[RequestId, queue.Queue[JsonRpcResponse | BaseException]] = {}
        self._pending_lock = threading.Lock()
        self._pending_permissions: dict[RequestId, PermissionRequest] = {}
        self._permission_lock = threading.Lock()
        self._updates: queue.Queue[SessionUpdate | object] = queue.Queue(max_pending_events)
        self._permissions: queue.Queue[PermissionRequest | object] = queue.Queue(
            max_pending_events
        )
        self._notifications: queue.Queue[RawNotification | object] = queue.Queue(
            max_pending_events
        )
        self._inbound_requests: queue.Queue[InboundRequest | object] = queue.Queue(
            max_pending_events
        )
        self._reader_thread: threading.Thread | None = None
        self._stderr_thread: threading.Thread | None = None
        self._stop = threading.Event()
        self._stderr_chunks: deque[bytes] = deque()
        self._stderr_size = 0
        self._stderr_lock = threading.Lock()
        self._failure: BaseException | None = None
        self._exit: ProcessExit | None = None
        self._initialize_result: InitializeResult | None = None

    def __enter__(self) -> "AcpClient":
        self.start()
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        self.close()

    @property
    def state(self) -> ClientState:
        with self._state_lock:
            return self._state

    @property
    def process(self) -> subprocess.Popen[bytes] | None:
        return self._process

    @property
    def capabilities(self) -> AgentCapabilities | None:
        result = self._initialize_result
        return result.capabilities if result is not None else None

    @property
    def initialize_result(self) -> InitializeResult | None:
        return self._initialize_result

    @property
    def failure(self) -> BaseException | None:
        return self._failure

    @property
    def exit(self) -> ProcessExit | None:
        process = self._process
        if process is not None:
            returncode = process.poll()
            if returncode is not None:
                self._exit = ProcessExit(returncode, self.stderr_tail())
        return self._exit

    def stderr_tail(self) -> str:
        with self._stderr_lock:
            data = b"".join(self._stderr_chunks)
        return data.decode("utf-8", errors="replace")

    def start(self) -> "AcpClient":
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
                )
            except OSError as exc:
                self._state = ClientState.FAILED
                self._failure = AcpTransportError(
                    f"could not start ACP agent {self.argv[0]!r}"
                )
                raise self._failure from exc
            self._process = process
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
    ) -> InitializeResult:
        """Perform ACP v1 capability negotiation.

        ACP v1 does not define a post-response ``initialized`` notification;
        :meth:`initialized` is available only for adapters that explicitly
        require that compatibility extension.
        """
        self.start()
        if not client_name or not client_version:
            raise ValueError("client_name and client_version must be non-empty")
        with self._state_lock:
            if self._state is ClientState.INITIALIZED:
                assert self._initialize_result is not None
                return self._initialize_result
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
        version = raw.get("protocolVersion")
        if version != ACP_PROTOCOL_VERSION:
            raise AcpProtocolVersionError(
                f"agent selected unsupported ACP protocol version {version!r}"
            )
        capabilities_value = raw.get("agentCapabilities", {})
        if not isinstance(capabilities_value, Mapping):
            capabilities_value = {}
        agent_info_value = raw.get("agentInfo")
        agent_info = (
            MappingProxyType(dict(agent_info_value))
            if isinstance(agent_info_value, Mapping)
            else None
        )
        auth_methods_value = raw.get("authMethods", [])
        auth_methods = tuple(
            MappingProxyType(dict(item))
            for item in auth_methods_value
            if isinstance(item, Mapping)
        ) if isinstance(auth_methods_value, list) else ()
        parsed = InitializeResult(
            protocol_version=version,
            capabilities=AgentCapabilities.from_mapping(capabilities_value),
            agent_info=agent_info,
            auth_methods=auth_methods,
            raw=MappingProxyType(dict(raw)),
        )
        with self._state_lock:
            if self._state is not ClientState.RUNNING:
                self._raise_unusable()
            self._initialize_result = parsed
            self._state = ClientState.INITIALIZED
        return parsed

    def initialized(self) -> None:
        """Send the non-standard ``initialized`` compatibility notification."""
        self._require_initialized()
        self.notify("initialized", {})

    def request(
        self,
        method: str,
        params: Mapping[str, Any] | None = None,
        *,
        timeout: float | None = None,
        require_initialized: bool = True,
    ) -> Any:
        if require_initialized:
            self._require_initialized()
        else:
            self._require_running()
        wait_timeout = self.request_timeout if timeout is None else _positive_timeout(
            timeout, "timeout"
        )
        request_id = self._new_request_id()
        waiter: queue.Queue[JsonRpcResponse | BaseException] = queue.Queue(maxsize=1)
        with self._pending_lock:
            self._pending[request_id] = waiter
        try:
            self._write(request_envelope(request_id, method, params))
        except BaseException:
            with self._pending_lock:
                self._pending.pop(request_id, None)
            raise
        try:
            response = waiter.get(timeout=wait_timeout)
        except queue.Empty as exc:
            with self._pending_lock:
                self._pending.pop(request_id, None)
            raise AcpRequestTimeoutError(
                f"ACP request {method!r} timed out after {wait_timeout:g}s"
            ) from exc
        if isinstance(response, BaseException):
            raise response
        return response.result_or_raise()

    def notify(self, method: str, params: Mapping[str, Any] | None = None) -> None:
        self._require_running()
        self._write(notification_envelope(method, params))

    def new_session(
        self,
        cwd: str | os.PathLike[str],
        *,
        mcp_servers: Sequence[Mapping[str, Any]] = (),
        additional_directories: Sequence[str | os.PathLike[str]] = (),
        timeout: float | None = None,
    ) -> SessionResult:
        params = self._session_setup_params(
            cwd,
            mcp_servers=mcp_servers,
            additional_directories=additional_directories,
        )
        result = self.request("session/new", params, timeout=timeout)
        raw = _require_mapping(result, "session/new result")
        return _parse_session_result(raw, require_session_id=True)

    def load_session(
        self,
        session_id: str,
        cwd: str | os.PathLike[str],
        *,
        mcp_servers: Sequence[Mapping[str, Any]] = (),
        additional_directories: Sequence[str | os.PathLike[str]] = (),
        timeout: float | None = None,
    ) -> SessionResult:
        self._require_capability("loadSession")
        params = self._session_setup_params(
            cwd,
            mcp_servers=mcp_servers,
            additional_directories=additional_directories,
        )
        params["sessionId"] = _nonempty(session_id, "session_id")
        result = self.request("session/load", params, timeout=timeout)
        raw = _require_mapping(result, "session/load result")
        parsed = _parse_session_result(raw, require_session_id=False)
        return SessionResult(session_id, parsed.modes, parsed.config_options, parsed.raw)

    def resume_session(
        self,
        session_id: str,
        cwd: str | os.PathLike[str],
        *,
        mcp_servers: Sequence[Mapping[str, Any]] = (),
        additional_directories: Sequence[str | os.PathLike[str]] = (),
        timeout: float | None = None,
    ) -> SessionResult:
        self._require_capability("sessionResume")
        params = self._session_setup_params(
            cwd,
            mcp_servers=mcp_servers,
            additional_directories=additional_directories,
        )
        params["sessionId"] = _nonempty(session_id, "session_id")
        result = self.request("session/resume", params, timeout=timeout)
        raw = _require_mapping(result, "session/resume result")
        parsed = _parse_session_result(raw, require_session_id=False)
        return SessionResult(session_id, parsed.modes, parsed.config_options, parsed.raw)

    def list_sessions(
        self,
        *,
        cwd: str | os.PathLike[str] | None = None,
        cursor: str | None = None,
        timeout: float | None = None,
    ) -> SessionPage:
        self._require_capability("sessionList")
        params: dict[str, Any] = {}
        if cwd is not None:
            params["cwd"] = _absolute_path(cwd, "cwd")
        if cursor is not None:
            params["cursor"] = _nonempty(cursor, "cursor")
        result = self.request("session/list", params, timeout=timeout)
        raw = _require_mapping(result, "session/list result")
        raw_sessions = raw.get("sessions")
        if not isinstance(raw_sessions, list):
            raise AcpEnvelopeError("session/list result.sessions must be an array")
        sessions = tuple(_parse_session_info(item) for item in raw_sessions)
        next_cursor = raw.get("nextCursor")
        if next_cursor is not None and not isinstance(next_cursor, str):
            next_cursor = None
        return SessionPage(sessions, next_cursor, MappingProxyType(dict(raw)))

    def prompt(
        self,
        session_id: str,
        prompt: str | Sequence[Mapping[str, Any]],
        *,
        timeout: float | None = None,
    ) -> PromptResult:
        if isinstance(prompt, str):
            content: list[Mapping[str, Any]] = [{"type": "text", "text": prompt}]
        else:
            content = list(prompt)
        if not content:
            raise ValueError("prompt must contain at least one content block")
        for block in content:
            if not isinstance(block, Mapping) or not isinstance(block.get("type"), str):
                raise ValueError("each prompt content block must have a string type")
        result = self.request(
            "session/prompt",
            {"sessionId": _nonempty(session_id, "session_id"), "prompt": content},
            timeout=self.prompt_timeout if timeout is None else timeout,
        )
        raw = _require_mapping(result, "session/prompt result")
        stop_reason = raw.get("stopReason")
        try:
            parsed_reason = StopReason(stop_reason)
        except (ValueError, TypeError) as exc:
            raise AcpEnvelopeError("session/prompt returned an invalid stopReason") from exc
        return PromptResult(parsed_reason, MappingProxyType(dict(raw)))

    def cancel(self, session_id: str) -> None:
        """Cancel a turn and cancel all outstanding permissions for the session."""
        session_id = _nonempty(session_id, "session_id")
        self.notify("session/cancel", {"sessionId": session_id})
        with self._permission_lock:
            pending_ids = [
                request_id
                for request_id, request in self._pending_permissions.items()
                if request.session_id == session_id
            ]
        for request_id in pending_ids:
            self.respond_permission(request_id, cancelled=True)

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
            self._write(result_envelope(request_id, {"outcome": outcome}))
        except BaseException:
            # Preserve retryability when nothing was written successfully.
            with self._permission_lock:
                self._pending_permissions[request_id] = request
            raise

    def next_update(self, *, timeout: float | None = None) -> SessionUpdate:
        return self._queue_get(self._updates, timeout, "session update")

    def next_permission_request(
        self, *, timeout: float | None = None
    ) -> PermissionRequest:
        return self._queue_get(self._permissions, timeout, "permission request")

    def next_notification(self, *, timeout: float | None = None) -> RawNotification:
        return self._queue_get(self._notifications, timeout, "notification")

    def next_inbound_request(self, *, timeout: float | None = None) -> InboundRequest:
        return self._queue_get(self._inbound_requests, timeout, "inbound request")

    def reject_inbound_request(
        self,
        request_id: RequestId,
        *,
        code: int = _METHOD_NOT_FOUND,
        message: str = "Method not supported by Tendwire ACP client",
    ) -> None:
        self._write(error_envelope(request_id, code, message))

    def close(self) -> None:
        with self._state_lock:
            if self._state in {ClientState.CLOSED, ClientState.NEW}:
                self._state = ClientState.CLOSED
                return
            if self._state is ClientState.CLOSING:
                return
            was_failed = self._state is ClientState.FAILED
            self._state = ClientState.CLOSING
        self._stop.set()
        process = self._process
        if process is not None:
            with self._write_lock:
                if process.stdin is not None:
                    try:
                        process.stdin.close()
                    except OSError:
                        pass
            try:
                process.wait(timeout=self.close_timeout)
            except subprocess.TimeoutExpired:
                process.terminate()
                try:
                    process.wait(timeout=self.close_timeout)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=self.close_timeout)
            self._exit = ProcessExit(process.returncode, self.stderr_tail())
        for thread in (self._reader_thread, self._stderr_thread):
            if thread is not None and thread is not threading.current_thread():
                thread.join(timeout=self.close_timeout)
        self._fail_pending(AcpTransportError("ACP client closed"))
        self._signal_queues()
        with self._state_lock:
            self._state = ClientState.FAILED if was_failed else ClientState.CLOSED

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
            "mcpServers": [dict(server) for server in mcp_servers],
        }
        directories = [
            _absolute_path(directory, "additional directory")
            for directory in additional_directories
        ]
        if directories:
            self._require_capability("additionalDirectories")
            params["additionalDirectories"] = directories
        return params

    def _require_capability(self, name: str) -> None:
        self._require_initialized()
        assert self.capabilities is not None
        supported = {
            "loadSession": self.capabilities.load_session,
            "sessionList": self.capabilities.session_list,
            "sessionResume": self.capabilities.session_resume,
            "additionalDirectories": self.capabilities.additional_directories,
        }.get(name, False)
        if not supported:
            raise AcpCapabilityError(f"agent did not advertise {name} capability")

    def _new_request_id(self) -> int:
        with self._request_id_lock:
            request_id = self._next_id
            self._next_id += 1
        return request_id

    def _write(self, envelope: Mapping[str, Any]) -> None:
        payload = encode_message(envelope, max_frame_bytes=self.max_frame_bytes)
        with self._write_lock:
            self._require_running()
            process = self._process
            if process is None or process.stdin is None:
                raise AcpTransportError("ACP agent stdin is unavailable")
            try:
                remaining = memoryview(payload)
                while remaining:
                    written = os.write(process.stdin.fileno(), remaining)
                    if written <= 0:
                        raise BrokenPipeError("zero-byte write to ACP agent stdin")
                    remaining = remaining[written:]
            except (BrokenPipeError, OSError) as exc:
                failure = AcpTransportError("ACP agent stdin disconnected")
                self._set_failed(failure)
                raise failure from exc

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
                        if self._stop.is_set():
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
            while not self._stop.is_set():
                chunk = os.read(process.stderr.fileno(), 4096)
                if not chunk:
                    return
                with self._stderr_lock:
                    self._stderr_chunks.append(chunk)
                    self._stderr_size += len(chunk)
                    while self._stderr_size > self.stderr_limit_bytes:
                        excess = self._stderr_size - self.stderr_limit_bytes
                        first = self._stderr_chunks[0]
                        if len(first) <= excess:
                            self._stderr_size -= len(self._stderr_chunks.popleft())
                        else:
                            self._stderr_chunks[0] = first[excess:]
                            self._stderr_size -= excess
        except OSError:
            return

    def _dispatch(
        self, message: JsonRpcRequest | JsonRpcNotification | JsonRpcResponse
    ) -> None:
        if isinstance(message, JsonRpcResponse):
            if message.request_id is None:
                raise AcpEnvelopeError("uncorrelated ACP response with null id")
            with self._pending_lock:
                waiter = self._pending.pop(message.request_id, None)
            if waiter is None:
                # A late response after timeout cannot be safely correlated to a
                # live operation.  Keep the transport usable and surface it as a
                # raw diagnostic notification.
                self._put_lossless(
                    self._notifications,
                    RawNotification(
                        "$/orphan_response",
                        MappingProxyType({"id": message.request_id}),
                    ),
                )
                return
            waiter.put_nowait(message)
            return
        if isinstance(message, JsonRpcNotification):
            if message.method == "session/update":
                self._put_lossless(self._updates, parse_session_update(message.params))
            else:
                self._put_lossless(
                    self._notifications,
                    RawNotification(message.method, message.params),
                )
            return

        if message.method == "session/request_permission":
            try:
                parsed = parse_permission_request(message)
            except AcpProtocolError as exc:
                self._write(
                    error_envelope(
                        message.request_id,
                        -32602,
                        "Invalid permission request",
                        data=str(exc),
                    )
                )
                return
            with self._permission_lock:
                if message.request_id in self._pending_permissions:
                    raise AcpEnvelopeError("duplicate pending permission request id")
                self._pending_permissions[message.request_id] = parsed
            self._put_lossless(self._permissions, parsed)
        else:
            self._put_lossless(
                self._inbound_requests,
                InboundRequest(message.request_id, message.method, message.params),
            )

    def _put_lossless(self, target: queue.Queue[Any], value: Any) -> None:
        try:
            target.put_nowait(value)
        except queue.Full as exc:
            raise AcpEventQueueFullError(
                "ACP event queue is full; refusing to drop protocol data"
            ) from exc

    def _queue_get(
        self,
        source: queue.Queue[_T | object],
        timeout: float | None,
        description: str,
    ) -> _T:
        if timeout is not None:
            timeout = _positive_timeout(timeout, "timeout")
        try:
            item = source.get(timeout=timeout)
        except queue.Empty as exc:
            raise AcpRequestTimeoutError(f"timed out waiting for ACP {description}") from exc
        if item is _END:
            self._raise_unusable()
            raise AcpTransportError("ACP event stream ended")
        return item  # type: ignore[return-value]

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
            waiters = tuple(self._pending.values())
            self._pending.clear()
        for waiter in waiters:
            try:
                waiter.put_nowait(failure)
            except queue.Full:
                pass

    def _signal_queues(self) -> None:
        for target in (
            self._updates,
            self._permissions,
            self._notifications,
            self._inbound_requests,
        ):
            try:
                target.put_nowait(_END)
            except queue.Full:
                # A full queue already has readable data; the recorded client
                # state reports terminal failure once consumers drain it.
                pass

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
    if not result or not Path(result).is_absolute():
        raise ValueError(f"{name} must be an absolute path")
    return result


def _require_mapping(value: Any, name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise AcpEnvelopeError(f"{name} must be an object")
    return value


def _parse_session_result(
    raw: Mapping[str, Any], *, require_session_id: bool
) -> SessionResult:
    session_id = raw.get("sessionId", "")
    if require_session_id and (not isinstance(session_id, str) or not session_id):
        raise AcpEnvelopeError("session/new result.sessionId must be a non-empty string")
    modes_value = raw.get("modes")
    modes = (
        MappingProxyType(dict(modes_value)) if isinstance(modes_value, Mapping) else None
    )
    config_value = raw.get("configOptions", [])
    config_options = tuple(
        MappingProxyType(dict(item))
        for item in config_value
        if isinstance(item, Mapping)
    ) if isinstance(config_value, list) else ()
    return SessionResult(
        session_id=session_id if isinstance(session_id, str) else "",
        modes=modes,
        config_options=config_options,
        raw=MappingProxyType(dict(raw)),
    )


def _parse_session_info(value: Any) -> SessionInfo:
    raw = _require_mapping(value, "session info")
    session_id = raw.get("sessionId")
    cwd = raw.get("cwd")
    if not isinstance(session_id, str) or not session_id:
        raise AcpEnvelopeError("session info.sessionId must be a non-empty string")
    if not isinstance(cwd, str) or not Path(cwd).is_absolute():
        raise AcpEnvelopeError("session info.cwd must be an absolute path")
    directories_value = raw.get("additionalDirectories", [])
    directories = tuple(
        item
        for item in directories_value
        if isinstance(item, str) and Path(item).is_absolute()
    ) if isinstance(directories_value, list) else ()
    title = raw.get("title")
    updated_at = raw.get("updatedAt")
    return SessionInfo(
        session_id=session_id,
        cwd=cwd,
        additional_directories=directories,
        title=title if isinstance(title, str) else None,
        updated_at=updated_at if isinstance(updated_at, str) else None,
        raw=MappingProxyType(dict(raw)),
    )
