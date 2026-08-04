"""Pure helpers for the Herdr socket JSON-line protocol.

The helpers in this module provide a small, stdlib-only foundation for
Tendwire's opt-in socket backend and for direct protocol tests.
"""

from __future__ import annotations

import json
import os
import uuid
from collections.abc import Iterable, Mapping
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

HERDR_EVENTS_SUBSCRIBE_METHOD = "events.subscribe"
HERDR_OFFICIAL_EVENT_NAMES = (
    "workspace.created",
    "workspace.updated",
    "workspace.renamed",
    "workspace.closed",
    "workspace.focused",
    "pane.created",
    "pane.closed",
    "pane.updated",
    "pane.focused",
    "pane.moved",
    "pane.exited",
    "pane.agent_detected",
    "pane.output_matched",
    "pane.agent_status_changed",
    "worktree.created",
    "worktree.opened",
    "worktree.removed",
)
HERDR_OFFICIAL_EVENT_NAME_SET = frozenset(HERDR_OFFICIAL_EVENT_NAMES)


class HerdrProtocolError(Exception):
    """Base error for Herdr socket protocol failures."""


class HerdrSocketPathError(HerdrProtocolError, ValueError):
    """Raised when a socket path or session name is invalid."""


class HerdrMalformedLineError(HerdrProtocolError, ValueError):
    """Raised when a JSON-line frame is not valid UTF-8 JSON."""


class HerdrEnvelopeError(HerdrProtocolError, ValueError):
    """Raised when a decoded JSON object is not a valid protocol envelope."""


class HerdrRequestIdMismatchError(HerdrEnvelopeError):
    """Raised when a server envelope is not correlated to the expected id."""


class HerdrErrorResponse(HerdrProtocolError):
    """Raised for a valid protocol error response."""

    def __init__(
        self,
        error: Any,
        request_id: str,
        *,
        uncorrelated: bool = False,
    ) -> None:
        self.error = error
        self.request_id = request_id
        self.uncorrelated = uncorrelated
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


def _expand_home(path: str, *, home: str | os.PathLike[str] | None = None) -> Path:
    if path == "~":
        return _home_path(home)
    if path.startswith("~/"):
        return _home_path(home) / path[2:]
    return Path(os.path.expanduser(path))


def _validate_socket_path(path: str, *, home: str | os.PathLike[str] | None = None) -> str:
    if not isinstance(path, str):
        raise HerdrSocketPathError("Herdr socket path must be a string")
    if not path.strip():
        raise HerdrSocketPathError("Herdr socket path must not be empty")
    resolved = _expand_home(path.strip(), home=home)
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


def _validate_event_subscription_name(name: Any) -> str:
    if not isinstance(name, str):
        raise HerdrEnvelopeError("Herdr event subscription names must be strings")
    if not name:
        raise HerdrEnvelopeError("Herdr event subscription names must not be empty")
    if name.strip() != name or name not in HERDR_OFFICIAL_EVENT_NAME_SET:
        raise HerdrEnvelopeError(f"unsupported Herdr event subscription {name!r}")
    return name


def build_events_subscribe_params(event_names: Iterable[str] | str | None = None) -> dict[str, Any]:
    """Return official events.subscribe params for validated Herdr event names."""
    if event_names is None:
        names = HERDR_OFFICIAL_EVENT_NAMES
    elif isinstance(event_names, str):
        names = (event_names,)
    else:
        try:
            names = tuple(event_names)
        except TypeError as exc:
            raise HerdrEnvelopeError("Herdr event subscriptions must be iterable") from exc
    return {"subscriptions": [{"type": _validate_event_subscription_name(name)} for name in names]}


def build_events_subscribe_request(
    event_names: Iterable[str] | str | None = None,
    *,
    request_id: str | None = None,
) -> dict[str, Any]:
    """Build an official Herdr event subscription request envelope."""
    return build_request(
        HERDR_EVENTS_SUBSCRIBE_METHOD,
        build_events_subscribe_params(event_names),
        request_id=request_id,
    )


def frame_request(request: Mapping[str, Any]) -> bytes:
    """Encode one request object as UTF-8 JSON Lines."""
    try:
        payload = json.dumps(request, ensure_ascii=False, separators=(",", ":"))
    except (TypeError, ValueError) as exc:
        raise HerdrEnvelopeError("request is not JSON serializable") from exc
    return payload.encode("utf-8") + b"\n"


def build_request_line(
    method: str,
    params: Mapping[str, Any] | None = None,
    *,
    request_id: str | None = None,
) -> tuple[str, bytes]:
    """Build and frame a request, returning its id and newline-terminated bytes."""
    request = build_request(method, params, request_id=request_id)
    return str(request["id"]), frame_request(request)


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

    if text.endswith("\n"):
        text = text[:-1]
    if text.endswith("\r"):
        text = text[:-1]
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


def is_event(envelope: Mapping[str, Any]) -> bool:
    return "event" in envelope and "result" not in envelope and "error" not in envelope


def validate_response(
    envelope: Mapping[str, Any],
    *,
    allow_uncorrelated_error: bool = False,
) -> dict[str, Any]:
    """Validate a response envelope while tolerating unknown fields."""
    if not is_response(envelope):
        raise HerdrEnvelopeError("Herdr response must contain exactly one of result or error")
    # Herdr 0.7.5 emits ``{"id":"", "error":...}`` when subscription
    # parameters fail schema validation. Only the subscription negotiation
    # path may opt into that compatibility exception; ordinary requests remain
    # strictly correlated.
    error = envelope.get("error")
    uncorrelated_subscription_error = (
        allow_uncorrelated_error
        and is_error_response(envelope)
        and envelope.get("id") == ""
        and isinstance(error, Mapping)
        and error.get("code") == "invalid_request"
        and isinstance(error.get("message"), str)
        and error["message"].startswith("invalid request:")
    )
    if not uncorrelated_subscription_error:
        _validated_id(envelope)
    return dict(envelope)


def validate_event(envelope: Mapping[str, Any]) -> dict[str, Any]:
    """Validate an event envelope while preserving its raw data.

    The confirmed Herdr ``EventEnvelope`` consists of ``event`` and ``data``.
    A generic ``id`` is tolerated as subscription correlation only; it is not
    authoritative producer event identity. Unknown top-level fields remain
    available for forward-compatible consumers.
    """
    request_id = envelope.get("id")
    if request_id is not None and (not isinstance(request_id, str) or not request_id):
        raise HerdrEnvelopeError("Herdr event id must be a non-empty string when present")
    event_name = envelope.get("event")
    if not isinstance(event_name, str) or not event_name:
        raise HerdrEnvelopeError("Herdr event name must be a non-empty string")
    if not is_event(envelope):
        raise HerdrEnvelopeError("Herdr event must not contain result or error fields")
    return dict(envelope)


def validate_server_envelope(
    envelope: Mapping[str, Any],
    *,
    allow_uncorrelated_error: bool = False,
) -> dict[str, Any]:
    """Validate a decoded server response or event envelope."""
    if is_response(envelope):
        return validate_response(
            envelope,
            allow_uncorrelated_error=allow_uncorrelated_error,
        )
    if is_event(envelope):
        return validate_event(envelope)
    _validated_id(envelope)
    raise HerdrEnvelopeError("Herdr envelope is neither a response nor an event")


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
