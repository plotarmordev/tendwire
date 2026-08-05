"""Synchronous client for Herdr lifecycle discovery and ACP ownership."""

from __future__ import annotations

import socket
import time
from collections.abc import Iterable, Mapping
from typing import Any

from .herdr_protocol import (
    HerdrErrorResponse,
    HerdrFrameTooLargeError,
    HerdrProtocolError,
    build_request,
    ensure_response_id,
    error_payload,
    frame_request,
    is_error_response,
    parse_json_line,
    resolve_socket_path,
    result_payload,
    validate_server_envelope,
)

_DEFAULT_TIMEOUT_SECONDS = 5.0
_RECV_SIZE = 4096
_MAX_FRAME_BYTES = 8 * 1024 * 1024


class HerdrSocketError(HerdrProtocolError):
    """Base error for Herdr socket transport failures."""


class HerdrSocketTimeoutError(HerdrSocketError, TimeoutError):
    """Raised when a request or event read exceeds its timeout."""


class HerdrSocketDisconnectedError(HerdrSocketError, ConnectionError):
    """Raised when the socket disconnects before a complete expected response."""


class HerdrSocketConnectionError(HerdrSocketError, ConnectionError):
    """Raised when the Unix socket cannot be opened."""


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
        """Send one strictly correlated request and return its raw result payload."""
        try:
            request_id, deadline = self._send_request(method, params, timeout=timeout)
            response = self._read_response(request_id, deadline=deadline)
            if is_error_response(response):
                raise HerdrErrorResponse(error_payload(response), request_id)
            return result_payload(response)
        finally:
            # Herdr accepts exactly one request on each Unix connection.
            self.close()

    def workspace_list(
        self,
        params: Mapping[str, Any] | None = None,
        *,
        timeout: float | None = None,
    ) -> Any:
        return self.request("workspace.list", params, timeout=timeout)

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

    def agent_acp_endpoint(
        self,
        target: str,
        *,
        timeout: float | None = None,
    ) -> Any:
        """Mint a one-shot, private ACP attach endpoint for one live agent."""
        return self.request(
            "agent.acp_endpoint",
            {"target": target},
            timeout=timeout,
        )

    def agent_acp_status(
        self,
        target: str,
        *,
        timeout: float | None = None,
    ) -> Any:
        """Read ACP ownership/generation without minting an attach ticket."""
        return self.request(
            "agent.acp_status",
            {"target": target},
            timeout=timeout,
        )

    def agent_acp_console_exchange(
        self,
        target: str,
        *,
        generation: int,
        lease: str,
        after_input_sequence: int = 0,
        output: Iterable[Mapping[str, Any]] = (),
        timeout: float | None = None,
    ) -> Any:
        """Exchange pane input and idempotent ACP output as coordinator."""
        return self.request(
            "agent.acp_console_exchange",
            {
                "target": target,
                "generation": generation,
                "lease": lease,
                "role": "coordinator",
                "output": [dict(item) for item in output],
                "after_input_sequence": after_input_sequence,
                "after_output_sequence": 0,
            },
            timeout=timeout,
        )

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
        frame = frame_request(request)
        if len(frame) > _MAX_FRAME_BYTES:
            raise HerdrFrameTooLargeError("Herdr request frame is too large")
        self._write(frame, deadline=deadline)
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
    ) -> dict[str, Any]:
        envelope = self._read_server_envelope(deadline=deadline)
        ensure_response_id(envelope, request_id)
        return envelope

    def _read_server_envelope(self, *, deadline: float) -> dict[str, Any]:
        envelope = parse_json_line(self._read_line(deadline=deadline))
        return validate_server_envelope(envelope)

    def _read_line(self, *, deadline: float) -> bytes:
        while True:
            newline_index = self._buffer.find(b"\n")
            if newline_index >= 0:
                if newline_index + 1 > _MAX_FRAME_BYTES:
                    self.close()
                    raise HerdrFrameTooLargeError("Herdr response frame is too large")
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
            if len(self._buffer) > _MAX_FRAME_BYTES:
                self.close()
                raise HerdrFrameTooLargeError("Herdr response frame is too large")
