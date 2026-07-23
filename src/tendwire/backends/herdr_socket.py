"""Inactive stdlib Herdr Unix socket client.

This module is additive and is not imported by Tendwire's production
observation or CLI paths. It exposes a low-level JSON-line client plus thin
wrappers for the PR8-allowed Herdr methods only.
"""

from __future__ import annotations

import socket
import time
from collections import deque
from collections.abc import Iterable, Iterator, Mapping
from typing import Any

from .herdr_protocol import (
    HerdrEnvelopeError,
    HerdrErrorResponse,
    HerdrProtocolError,
    HerdrRequestIdMismatchError,
    HERDR_EVENTS_SUBSCRIBE_METHOD,
    build_events_subscribe_params,
    build_request,
    ensure_response_id,
    error_payload,
    frame_request,
    is_error_response,
    is_event,
    is_result_response,
    parse_json_line,
    resolve_socket_path,
    result_payload,
    validate_server_envelope,
)

_DEFAULT_TIMEOUT_SECONDS = 5.0
_RECV_SIZE = 4096
_MAX_PENDING_EVENTS = 1024


class HerdrSocketError(HerdrProtocolError):
    """Base error for Herdr socket transport failures."""


class HerdrSocketTimeoutError(HerdrSocketError, TimeoutError):
    """Raised when a request or event read exceeds its timeout."""


class HerdrSocketDisconnectedError(HerdrSocketError, ConnectionError):
    """Raised when the socket disconnects before a complete expected response."""


class HerdrSocketConnectionError(HerdrSocketError, ConnectionError):
    """Raised when the Unix socket cannot be opened."""


class HerdrEventStream(Iterator[dict[str, Any]]):
    """Iterator over events correlated to a subscription request id."""

    def __init__(
        self,
        client: "HerdrSocketClient",
        subscription_id: str,
        ack: Any,
        *,
        timeout: float | None = None,
    ) -> None:
        self.client = client
        self.subscription_id = subscription_id
        self.ack = ack
        self.timeout = timeout
        self._closed = False

    def __iter__(self) -> "HerdrEventStream":
        return self

    def __next__(self) -> dict[str, Any]:
        if self._closed:
            raise StopIteration
        try:
            return self.client.read_event(self.subscription_id, timeout=self.timeout)
        except HerdrSocketDisconnectedError:
            self._closed = True
            raise StopIteration from None

    def close(self) -> None:
        self._closed = True


class HerdrSocketClient:
    """Synchronous Herdr JSON-line client over a Unix domain socket."""

    def __init__(
        self,
        socket_path: str | None = None,
        *,
        timeout: float = _DEFAULT_TIMEOUT_SECONDS,
    ) -> None:
        self.socket_path = resolve_socket_path(socket_path)
        self.timeout = self._validate_timeout(timeout)
        self._socket: socket.socket | None = None
        self._buffer = bytearray()
        self._pending_events: deque[dict[str, Any]] = deque()

    def __enter__(self) -> "HerdrSocketClient":
        self.connect()
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        self.close()

    @staticmethod
    def _validate_timeout(timeout: float | int | None) -> float:
        if timeout is None:
            return _DEFAULT_TIMEOUT_SECONDS
        value = float(timeout)
        if value <= 0:
            raise ValueError("timeout must be positive")
        return value

    def connect(self) -> "HerdrSocketClient":
        if self._socket is not None:
            return self
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.settimeout(self.timeout)
        try:
            sock.connect(self.socket_path)
        except OSError as exc:
            sock.close()
            raise HerdrSocketConnectionError(
                f"could not connect to Herdr socket {self.socket_path!r}"
            ) from exc
        self._socket = sock
        return self

    def close(self) -> None:
        sock = self._socket
        self._socket = None
        self._buffer.clear()
        self._pending_events.clear()
        if sock is None:
            return
        try:
            sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        sock.close()

    def request(
        self,
        method: str,
        params: Mapping[str, Any] | None = None,
        *,
        timeout: float | None = None,
    ) -> Any:
        """Send one request and return its raw result payload."""
        request_id, deadline = self._send_request(method, params, timeout=timeout)
        response = self._read_response(
            request_id,
            deadline=deadline,
            allow_uncorrelated_error=False,
        )
        if is_error_response(response):
            raise HerdrErrorResponse(error_payload(response), request_id)
        return result_payload(response)

    def subscribe(
        self,
        method: str,
        params: Mapping[str, Any] | None = None,
        *,
        timeout: float | None = None,
        event_timeout: float | None = None,
    ) -> HerdrEventStream:
        """Send a subscription request and return an iterator over its events."""
        request_id, deadline = self._send_request(method, params, timeout=timeout)
        response = self._read_response(
            request_id,
            deadline=deadline,
            allow_uncorrelated_error=True,
        )
        if is_error_response(response):
            raise HerdrErrorResponse(error_payload(response), request_id)
        return HerdrEventStream(
            self,
            request_id,
            result_payload(response),
            timeout=self.timeout if event_timeout is None else event_timeout,
        )

    def events_subscribe(
        self,
        event_names: Iterable[str] | str | None = None,
        *,
        timeout: float | None = None,
        event_timeout: float | None = None,
    ) -> HerdrEventStream:
        """Subscribe to the official Herdr event stream."""
        return self.subscribe(
            HERDR_EVENTS_SUBSCRIBE_METHOD,
            build_events_subscribe_params(event_names),
            timeout=timeout,
            event_timeout=event_timeout,
        )

    def read_event(self, subscription_id: str, *, timeout: float | None = None) -> dict[str, Any]:
        envelope = (
            self._pending_events.popleft()
            if self._pending_events
            else self._read_server_envelope(deadline=self._deadline(timeout))
        )
        if not is_event(envelope):
            raise HerdrEnvelopeError("expected Herdr event envelope")
        if envelope.get("id") is not None:
            ensure_response_id(envelope, subscription_id)
        return envelope

    def workspace_list(
        self,
        params: Mapping[str, Any] | None = None,
        *,
        timeout: float | None = None,
    ) -> Any:
        return self.request("workspace.list", params, timeout=timeout)

    def tab_list(
        self,
        params: Mapping[str, Any] | None = None,
        *,
        timeout: float | None = None,
    ) -> Any:
        return self.request("tab.list", params, timeout=timeout)

    def pane_list(
        self,
        params: Mapping[str, Any] | None = None,
        *,
        timeout: float | None = None,
    ) -> Any:
        return self.request("pane.list", params, timeout=timeout)

    def agent_list(
        self,
        params: Mapping[str, Any] | None = None,
        *,
        timeout: float | None = None,
    ) -> Any:
        return self.request("agent.list", params, timeout=timeout)

    def pane_get(
        self,
        params: Mapping[str, Any] | None = None,
        *,
        timeout: float | None = None,
    ) -> Any:
        return self.request("pane.get", params, timeout=timeout)

    def agent_get(
        self,
        params: Mapping[str, Any] | None = None,
        *,
        timeout: float | None = None,
    ) -> Any:
        return self.request("agent.get", params, timeout=timeout)

    def pane_read(
        self,
        params: Mapping[str, Any] | None = None,
        *,
        timeout: float | None = None,
    ) -> Any:
        return self.request("pane.read", params, timeout=timeout)

    def agent_send(
        self,
        params: Mapping[str, Any] | None = None,
        *,
        timeout: float | None = None,
    ) -> Any:
        return self.request("agent.send", params, timeout=timeout)

    def _send_request(
        self,
        method: str,
        params: Mapping[str, Any] | None,
        *,
        timeout: float | None,
    ) -> tuple[str, float]:
        self.connect()
        request = build_request(method, params)
        request_id = str(request["id"])
        deadline = self._deadline(timeout)
        self._write(frame_request(request), deadline=deadline)
        return request_id, deadline

    def _deadline(self, timeout: float | None) -> float:
        return time.monotonic() + self._validate_timeout(self.timeout if timeout is None else timeout)

    def _remaining(self, deadline: float) -> float:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise HerdrSocketTimeoutError("Herdr socket request timed out")
        return remaining

    def _active_socket(self) -> socket.socket:
        if self._socket is None:
            self.connect()
        if self._socket is None:
            raise HerdrSocketDisconnectedError("Herdr socket is not connected")
        return self._socket

    def _write(self, payload: bytes, *, deadline: float) -> None:
        sock = self._active_socket()
        try:
            sock.settimeout(self._remaining(deadline))
            sock.sendall(payload)
        except (BrokenPipeError, ConnectionResetError) as exc:
            self.close()
            try:
                self.connect()
                sock = self._active_socket()
                sock.settimeout(self._remaining(deadline))
                sock.sendall(payload)
            except socket.timeout as retry_exc:
                raise HerdrSocketTimeoutError("Herdr socket write timed out") from retry_exc
            except OSError as retry_exc:
                self.close()
                raise HerdrSocketDisconnectedError("Herdr socket disconnected during write") from retry_exc
        except socket.timeout as exc:
            raise HerdrSocketTimeoutError("Herdr socket write timed out") from exc
        except OSError as exc:
            self.close()
            raise HerdrSocketDisconnectedError("Herdr socket disconnected during write") from exc

    def _read_response(
        self,
        request_id: str,
        *,
        deadline: float,
        allow_uncorrelated_error: bool,
    ) -> dict[str, Any]:
        while True:
            envelope = self._read_server_envelope(
                deadline=deadline,
                allow_uncorrelated_error=allow_uncorrelated_error,
            )
            if is_event(envelope):
                if len(self._pending_events) >= _MAX_PENDING_EVENTS:
                    raise HerdrEnvelopeError(
                        "too many Herdr events arrived before the response"
                    )
                self._pending_events.append(envelope)
                continue
            if (
                allow_uncorrelated_error
                and is_error_response(envelope)
                and envelope.get("id") == ""
            ):
                # Herdr 0.7.5 does not correlate request-schema errors.  They
                # still belong to the only in-flight synchronous request and
                # must reach the caller as a server error, not tear down the
                # event loop as a malformed envelope.
                payload = envelope.get("error")
                if not isinstance(payload, Mapping):
                    raise HerdrEnvelopeError("Herdr error payload must be an object")
                raise HerdrErrorResponse(
                    dict(payload),
                    request_id,
                    uncorrelated=True,
                )
            ensure_response_id(envelope, request_id)
            if not (is_result_response(envelope) or is_error_response(envelope)):
                raise HerdrEnvelopeError("expected Herdr response envelope")
            return envelope

    def _read_server_envelope(
        self,
        *,
        deadline: float,
        allow_uncorrelated_error: bool = False,
    ) -> dict[str, Any]:
        line = self._read_line(deadline=deadline)
        envelope = parse_json_line(line)
        return validate_server_envelope(
            envelope,
            allow_uncorrelated_error=allow_uncorrelated_error,
        )

    def _read_line(self, *, deadline: float) -> bytes:
        while True:
            newline_index = self._buffer.find(b"\n")
            if newline_index >= 0:
                line = bytes(self._buffer[: newline_index + 1])
                del self._buffer[: newline_index + 1]
                return line

            sock = self._active_socket()
            try:
                sock.settimeout(self._remaining(deadline))
                chunk = sock.recv(_RECV_SIZE)
            except socket.timeout as exc:
                raise HerdrSocketTimeoutError("Herdr socket read timed out") from exc
            except OSError as exc:
                self.close()
                raise HerdrSocketDisconnectedError("Herdr socket disconnected during read") from exc

            if not chunk:
                self.close()
                raise HerdrSocketDisconnectedError(
                    "Herdr socket disconnected before a complete line was received"
                )
            self._buffer.extend(chunk)
