"""Pure helpers for the Herdr socket JSON-line protocol.

The helpers in this module provide a small, stdlib-only foundation for
Tendwire's opt-in socket backend and for direct protocol tests.
"""

from __future__ import annotations

import json
import os
import uuid
from collections.abc import Mapping
from pathlib import Path
from typing import Any

_SOCKET_ENV_ORDER = (
    "TENDWIRE_HERDR_SOCKET",
    "HERDR_SOCKET_PATH",
)
_SESSION_ENV_ORDER = (
    "TENDWIRE_HERDR_SESSION",
    "HERDR_SESSION",
)

class HerdrProtocolError(Exception):
    """Base error for Herdr socket protocol failures."""


class HerdrSocketPathError(HerdrProtocolError, ValueError):
    """Raised when a socket path or session name is invalid."""


class HerdrMalformedLineError(HerdrProtocolError, ValueError):
    """Raised when a JSON-line frame is not valid UTF-8 JSON."""


class HerdrEnvelopeError(HerdrProtocolError, ValueError):
    """Raised when a decoded JSON object is not a valid protocol envelope."""


class HerdrFrameTooLargeError(HerdrProtocolError, ValueError):
    """Raised when one JSON-line frame exceeds the fixed transport bound."""


class HerdrRequestIdMismatchError(HerdrEnvelopeError):
    """Raised when a server envelope is not correlated to the expected id."""


class HerdrErrorResponse(HerdrProtocolError):
    """Raised for a valid protocol error response."""

    def __init__(
        self,
        error: Any,
        request_id: str,
    ) -> None:
        self.error = error
        self.request_id = request_id
        message = "Herdr returned an error response"
        if isinstance(error, Mapping):
            raw_message = error.get("message")
            if isinstance(raw_message, str) and raw_message:
                message = raw_message
        super().__init__(message)


def _home_path(home: str | os.PathLike[str] | None = None) -> Path:
    if home is None:
        return Path.home()
    return Path(home)


def _validate_socket_path(path: str, *, home: str | os.PathLike[str] | None = None) -> str:
    if not isinstance(path, str):
        raise HerdrSocketPathError("Herdr socket path must be a string")
    if not path.strip():
        raise HerdrSocketPathError("Herdr socket path must not be empty")
    stripped = path.strip()
    if stripped == "~":
        resolved = _home_path(home)
    elif stripped.startswith("~/"):
        resolved = _home_path(home) / stripped[2:]
    else:
        resolved = Path(os.path.expanduser(stripped))
    if not resolved.is_absolute():
        raise HerdrSocketPathError("Herdr socket path must be absolute")
    return str(resolved)


def _session_socket_path(session: str, *, home: str | os.PathLike[str] | None = None) -> str:
    if not isinstance(session, str) or not session.strip():
        raise HerdrSocketPathError("Herdr session name must not be empty")
    name = session.strip()
    if name in {".", ".."} or "/" in name or "\\" in name:
        raise HerdrSocketPathError("Herdr session name must be a single path segment")
    return str(_home_path(home) / ".config" / "herdr" / "sessions" / name / "herdr.sock")


def resolve_socket_path(
    socket_path: str | os.PathLike[str] | None = None,
    *,
    env: Mapping[str, str] | None = None,
    home: str | os.PathLike[str] | None = None,
) -> str:
    """Resolve a Herdr Unix socket path using the frozen PR8 order."""
    if socket_path is not None:
        return _validate_socket_path(os.fspath(socket_path), home=home)

    environ = os.environ if env is None else env
    for name in _SOCKET_ENV_ORDER:
        value = environ.get(name, "")
        if value and value.strip():
            return _validate_socket_path(value, home=home)

    for name in _SESSION_ENV_ORDER:
        value = environ.get(name, "")
        if value and value.strip():
            return _session_socket_path(value, home=home)

    return str(_home_path(home) / ".config" / "herdr" / "herdr.sock")


def new_request_id() -> str:
    """Return a unique string request id."""
    return f"req-{uuid.uuid4().hex}"


def build_request(
    method: str,
    params: Mapping[str, Any] | None = None,
    *,
    request_id: str | None = None,
) -> dict[str, Any]:
    """Build a Herdr JSON-line request envelope."""
    if not isinstance(method, str) or not method:
        raise HerdrEnvelopeError("request method must be a non-empty string")
    if request_id is None:
        request_id = new_request_id()
    if not isinstance(request_id, str) or not request_id:
        raise HerdrEnvelopeError("request id must be a non-empty string")
    if params is None:
        params = {}
    if not isinstance(params, Mapping):
        raise HerdrEnvelopeError("request params must be an object")
    return {"id": request_id, "method": method, "params": dict(params)}


def frame_request(request: Mapping[str, Any]) -> bytes:
    """Encode one request object as UTF-8 JSON Lines."""
    try:
        payload = json.dumps(request, ensure_ascii=False, separators=(",", ":"))
    except (TypeError, ValueError) as exc:
        raise HerdrEnvelopeError("request is not JSON serializable") from exc
    return payload.encode("utf-8") + b"\n"


def parse_json_line(line: bytes | str) -> dict[str, Any]:
    """Decode one UTF-8 JSON object line."""
    if isinstance(line, bytes):
        try:
            text = line.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise HerdrMalformedLineError("Herdr line is not valid UTF-8") from exc
    elif isinstance(line, str):
        text = line
    else:
        raise HerdrMalformedLineError("Herdr line must be bytes or text")

    text = text.removesuffix("\n").removesuffix("\r")
    if not text:
        raise HerdrMalformedLineError("Herdr line is empty")

    try:
        value = json.loads(text)
    except json.JSONDecodeError as exc:
        raise HerdrMalformedLineError("Herdr line is not valid JSON") from exc
    if not isinstance(value, dict):
        raise HerdrEnvelopeError("Herdr envelope must be a JSON object")
    return value


def _validated_id(envelope: Mapping[str, Any]) -> str:
    request_id = envelope.get("id")
    if not isinstance(request_id, str) or not request_id:
        raise HerdrEnvelopeError("Herdr envelope id must be a non-empty string")
    return request_id


def is_result_response(envelope: Mapping[str, Any]) -> bool:
    return "result" in envelope and "error" not in envelope and "event" not in envelope


def is_error_response(envelope: Mapping[str, Any]) -> bool:
    return "error" in envelope and "result" not in envelope and "event" not in envelope


def is_response(envelope: Mapping[str, Any]) -> bool:
    return is_result_response(envelope) or is_error_response(envelope)


def validate_response(envelope: Mapping[str, Any]) -> dict[str, Any]:
    """Validate a response envelope while tolerating unknown fields."""
    if not is_response(envelope):
        raise HerdrEnvelopeError("Herdr response must contain exactly one of result or error")
    _validated_id(envelope)
    return dict(envelope)


def validate_server_envelope(envelope: Mapping[str, Any]) -> dict[str, Any]:
    """Validate a decoded server response envelope."""
    if is_response(envelope):
        return validate_response(envelope)
    _validated_id(envelope)
    raise HerdrEnvelopeError("Herdr envelope is not a response")


def ensure_response_id(envelope: Mapping[str, Any], expected_id: str) -> None:
    """Ensure an envelope is correlated with the expected request id."""
    actual_id = _validated_id(envelope)
    if actual_id != expected_id:
        raise HerdrRequestIdMismatchError(
            f"Herdr response id mismatch: expected {expected_id!r}, got {actual_id!r}"
        )


def result_payload(response: Mapping[str, Any]) -> Any:
    response = validate_response(response)
    if not is_result_response(response):
        raise HerdrEnvelopeError("Herdr response does not contain a result")
    return response.get("result")


def error_payload(response: Mapping[str, Any]) -> Any:
    response = validate_response(response)
    if not is_error_response(response):
        raise HerdrEnvelopeError("Herdr response does not contain an error")
    return response.get("error")
