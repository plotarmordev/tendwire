"""Strict, stdlib-only primitives for the ACP v1 stdio protocol.

ACP uses JSON-RPC 2.0 messages delimited by newlines.  This module deliberately
does not know about processes or threads; :mod:`acp_client` owns that transport
lifecycle and uses the validated envelopes defined here.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from enum import Enum
from types import MappingProxyType
from typing import Any, TypeAlias

from acp import schema as acp_schema
from pydantic import ValidationError

JSONRPC_VERSION = "2.0"
ACP_PROTOCOL_VERSION = 1
DEFAULT_MAX_FRAME_BYTES = 8 * 1024 * 1024

RequestId: TypeAlias = str | int | None

_MIN_REQUEST_NUMBER = -(2**63)
_MAX_REQUEST_NUMBER = 2**63 - 1
_SESSION_UPDATE_KINDS = frozenset({
    "user_message_chunk", "agent_message_chunk", "agent_thought_chunk",
    "tool_call", "tool_call_update", "plan", "available_commands_update",
    "current_mode_update", "config_option_update", "session_info_update",
    "usage_update",
})


class AcpProtocolError(Exception):
    """Base class for ACP framing and envelope errors."""


class AcpFramingError(AcpProtocolError, ValueError):
    """A stdio frame is incomplete, oversized, or not valid JSON."""


class AcpEnvelopeError(AcpProtocolError, ValueError):
    """A decoded value is not a valid JSON-RPC 2.0 envelope."""


class AcpRemoteError(AcpProtocolError):
    """A correlated JSON-RPC error returned by the ACP agent."""

    def __init__(
        self,
        code: int,
        message: str,
        *,
        request_id: RequestId | None,
        data: Any = None,
    ) -> None:
        self.code = code
        self.message = message
        self.request_id = request_id
        self.data = data
        super().__init__(f"ACP error {code}: {message}")


class StopReason(str, Enum):
    END_TURN = "end_turn"
    MAX_TOKENS = "max_tokens"
    MAX_TURN_REQUESTS = "max_turn_requests"
    REFUSAL = "refusal"
    CANCELLED = "cancelled"


class SteeringOutcome(str, Enum):
    """Outcome returned by the capability-gated Codex ACP steering extension."""

    INJECTED = "injected"
    STARTED_NEW_TURN = "startedNewTurn"
    NOT_ACTIVE = "notActive"
    FAILED = "failed"


@dataclass(frozen=True, slots=True)
class SteeringResult:
    """One correlated response from the inject-only steering extension."""

    outcome: SteeringOutcome
    correlation_id: str | None = None


@dataclass(frozen=True, slots=True)
class JsonRpcRequest:
    request_id: RequestId
    method: str
    params: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class JsonRpcNotification:
    method: str
    params: Mapping[str, Any]

@dataclass(frozen=True, slots=True)
class JsonRpcResponse:
    request_id: RequestId | None
    result: Any = None
    error: Mapping[str, Any] | None = None

    def result_or_raise(self) -> Any:
        if self.error is None:
            return self.result
        raise AcpRemoteError(
            self.error["code"],
            self.error["message"],
            request_id=self.request_id,
            data=self.error.get("data"),
        )


JsonRpcMessage: TypeAlias = JsonRpcRequest | JsonRpcNotification | JsonRpcResponse


@dataclass(frozen=True, slots=True)
class SessionUpdate:
    session_id: str
    raw: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class PermissionOption:
    option_id: str
    name: str
    kind: str


@dataclass(frozen=True, slots=True)
class PermissionRequest:
    request_id: RequestId
    session_id: str
    tool_call: Mapping[str, Any]
    options: tuple[PermissionOption, ...]
    meta: Mapping[str, Any] | None
    raw: Mapping[str, Any]


def _freeze_mapping(value: Mapping[str, Any]) -> Mapping[str, Any]:
    # A shallow immutable copy is intentional: arbitrary extension payloads are
    # retained verbatim and callers should treat all raw values as read-only.
    return MappingProxyType(dict(value))


def _valid_request_id(value: Any) -> bool:
    # ACP v1 inherits JSON-RPC's String, integral Number, or Null request IDs.
    # The official schema represents Number as a signed 64-bit integer.
    return value is None or isinstance(value, str) or (
        type(value) is int and _MIN_REQUEST_NUMBER <= value <= _MAX_REQUEST_NUMBER
    )


def _reject_constant(value: str) -> None:
    raise ValueError(f"non-finite JSON number {value}")


def _object_without_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON object key {key!r}")
        result[key] = value
    return result


def decode_json_line(
    line: bytes | bytearray | memoryview | str,
    *,
    max_frame_bytes: int = DEFAULT_MAX_FRAME_BYTES,
    require_newline: bool = True,
) -> JsonRpcMessage:
    """Decode and validate exactly one newline-delimited JSON-RPC message."""
    if max_frame_bytes <= 0:
        raise ValueError("max_frame_bytes must be positive")
    try:
        raw = line.encode("utf-8") if isinstance(line, str) else bytes(line)
    except UnicodeEncodeError as exc:
        raise AcpFramingError("ACP frame is not valid UTF-8") from exc
    if not raw:
        raise AcpFramingError("empty ACP frame")
    if len(raw) > max_frame_bytes:
        raise AcpFramingError("ACP frame exceeds configured size limit")
    if require_newline and not raw.endswith(b"\n"):
        raise AcpFramingError("ACP frame is not newline terminated")
    payload = raw.removesuffix(b"\n").removesuffix(b"\r")
    if b"\n" in payload or b"\r" in payload:
        raise AcpFramingError("ACP frame contains an embedded line break")
    if not payload:
        raise AcpFramingError("empty ACP JSON payload")
    try:
        value = json.loads(
            payload.decode("utf-8", errors="strict"),
            parse_constant=_reject_constant,
            object_pairs_hook=_object_without_duplicates,
        )
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        raise AcpFramingError("ACP frame is not strict UTF-8 JSON") from exc
    return validate_envelope(value)


def validate_envelope(value: Any) -> JsonRpcMessage:
    """Validate a decoded JSON-RPC 2.0 object and return a typed envelope."""
    if not isinstance(value, Mapping):
        raise AcpEnvelopeError("ACP JSON-RPC envelope must be an object")
    if value.get("jsonrpc") != JSONRPC_VERSION:
        raise AcpEnvelopeError("ACP envelope must declare jsonrpc '2.0'")

    has_method, has_id = "method" in value, "id" in value
    has_result, has_error = "result" in value, "error" in value

    if has_method:
        if has_result or has_error:
            raise AcpEnvelopeError("JSON-RPC call cannot contain result or error")
        method = value["method"]
        if not isinstance(method, str) or not method:
            raise AcpEnvelopeError("JSON-RPC method must be a non-empty string")
        params = value.get("params", {})
        if not isinstance(params, Mapping):
            raise AcpEnvelopeError("ACP method params must be an object")
        if not has_id:
            return JsonRpcNotification(method, _freeze_mapping(params))
        request_id = value["id"]
        if not _valid_request_id(request_id):
            raise AcpEnvelopeError(
                "JSON-RPC request id must be a string, signed 64-bit integer, or null"
            )
        return JsonRpcRequest(request_id, method, _freeze_mapping(params))

    if not has_id:
        raise AcpEnvelopeError("JSON-RPC response must contain an id")
    request_id = value["id"]
    if request_id is not None and not _valid_request_id(request_id):
        raise AcpEnvelopeError("JSON-RPC response id is invalid")
    if has_result == has_error:
        raise AcpEnvelopeError("JSON-RPC response must contain exactly one of result or error")
    if has_error:
        error = value["error"]
        if not isinstance(error, Mapping):
            raise AcpEnvelopeError("JSON-RPC error must be an object")
        code = error.get("code")
        message = error.get("message")
        if not isinstance(code, int) or isinstance(code, bool):
            raise AcpEnvelopeError("JSON-RPC error code must be an integer")
        if not isinstance(message, str):
            raise AcpEnvelopeError("JSON-RPC error message must be a string")
        return JsonRpcResponse(request_id, error=_freeze_mapping(error))
    return JsonRpcResponse(request_id, result=value["result"])


def encode_message(
    value: Mapping[str, Any],
    *,
    max_frame_bytes: int = DEFAULT_MAX_FRAME_BYTES,
) -> bytes:
    """Validate and encode a JSON-RPC object as one bounded UTF-8 line."""
    validate_envelope(value)
    try:
        payload = json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
        ).encode("utf-8") + b"\n"
    except (TypeError, ValueError, UnicodeEncodeError) as exc:
        raise AcpEnvelopeError("ACP envelope is not strict JSON serializable") from exc
    if len(payload) > max_frame_bytes:
        raise AcpFramingError("ACP frame exceeds configured size limit")
    return payload


def parse_session_update(params: Mapping[str, Any]) -> SessionUpdate:
    session_id = _required_string(params, "sessionId")
    update = params.get("update")
    if not isinstance(update, Mapping):
        raise AcpEnvelopeError("session/update params.update must be an object")
    kind_value = _required_string(update, "sessionUpdate")
    if kind_value in _SESSION_UPDATE_KINDS:
        try:
            acp_schema.SessionNotification.model_validate(dict(params))
        except ValidationError as exc:
            raise AcpEnvelopeError(
                "session/update params do not match the upstream ACP schema"
            ) from exc
    return SessionUpdate(session_id, _freeze_mapping(params))


def parse_permission_request(request: JsonRpcRequest) -> PermissionRequest:
    if request.method != "session/request_permission":
        raise AcpEnvelopeError("request is not session/request_permission")
    params = request.params
    try:
        validated = acp_schema.RequestPermissionRequest.model_validate(
            dict(params)
        )
    except ValidationError as exc:
        fields = {error.get("loc", ())[-1] for error in exc.errors() if error.get("loc")}
        if "kind" in fields:
            raise AcpEnvelopeError("permission option kind is not valid ACP v1") from exc
        if fields & {"tool_call_id", "toolCallId"}:
            raise AcpEnvelopeError("permission request is missing toolCallId") from exc
        raise AcpEnvelopeError(
            "permission request does not match the upstream ACP schema"
        ) from exc
    raw = validated.model_dump(by_alias=True, exclude_none=True)
    options = raw["options"]
    option_ids = [option["optionId"] for option in options]
    if len(option_ids) != len(set(option_ids)):
        raise AcpEnvelopeError("permission option IDs must be unique")
    return PermissionRequest(
        request.request_id,
        raw["sessionId"],
        _freeze_mapping(raw["toolCall"]),
        tuple(
            PermissionOption(option["optionId"], option["name"], option["kind"])
            for option in options
        ),
        _freeze_mapping(raw["_meta"]) if "_meta" in raw else None,
        _freeze_mapping(params),
    )


def _required_string(value: Mapping[str, Any], key: str) -> str:
    result = value.get(key)
    if not isinstance(result, str) or not result:
        raise AcpEnvelopeError(f"{key} must be a non-empty string")
    return result
