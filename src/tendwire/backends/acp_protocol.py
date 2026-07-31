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

JSONRPC_VERSION = "2.0"
ACP_PROTOCOL_VERSION = 1
DEFAULT_MAX_FRAME_BYTES = 8 * 1024 * 1024

RequestId: TypeAlias = str | int | None

_MIN_REQUEST_NUMBER = -(2**63)
_MAX_REQUEST_NUMBER = 2**63 - 1


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


class MessageKind(str, Enum):
    REQUEST = "request"
    RESPONSE = "response"
    NOTIFICATION = "notification"


class SessionUpdateKind(str, Enum):
    USER_MESSAGE_CHUNK = "user_message_chunk"
    AGENT_MESSAGE_CHUNK = "agent_message_chunk"
    AGENT_THOUGHT_CHUNK = "agent_thought_chunk"
    TOOL_CALL = "tool_call"
    TOOL_CALL_UPDATE = "tool_call_update"
    PLAN = "plan"
    AVAILABLE_COMMANDS_UPDATE = "available_commands_update"
    CURRENT_MODE_UPDATE = "current_mode_update"
    CONFIG_OPTION_UPDATE = "config_option_update"
    SESSION_INFO_UPDATE = "session_info_update"
    USAGE_UPDATE = "usage_update"


class StopReason(str, Enum):
    END_TURN = "end_turn"
    MAX_TOKENS = "max_tokens"
    MAX_TURN_REQUESTS = "max_turn_requests"
    REFUSAL = "refusal"
    CANCELLED = "cancelled"


class PermissionOptionKind(str, Enum):
    ALLOW_ONCE = "allow_once"
    ALLOW_ALWAYS = "allow_always"
    REJECT_ONCE = "reject_once"
    REJECT_ALWAYS = "reject_always"


@dataclass(frozen=True, slots=True)
class JsonRpcRequest:
    request_id: RequestId
    method: str
    params: Mapping[str, Any]

    @property
    def kind(self) -> MessageKind:
        return MessageKind.REQUEST


@dataclass(frozen=True, slots=True)
class JsonRpcNotification:
    method: str
    params: Mapping[str, Any]

    @property
    def kind(self) -> MessageKind:
        return MessageKind.NOTIFICATION


@dataclass(frozen=True, slots=True)
class JsonRpcResponse:
    request_id: RequestId | None
    result: Any = None
    error: Mapping[str, Any] | None = None

    @property
    def kind(self) -> MessageKind:
        return MessageKind.RESPONSE

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
class AgentCapabilities:
    """Captured ACP capabilities with convenient stable-v1 feature checks."""

    raw: Mapping[str, Any]

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any] | None) -> "AgentCapabilities":
        return cls(_freeze_mapping(value or {}))

    @property
    def load_session(self) -> bool:
        return self.raw.get("loadSession") is True

    @property
    def session_list(self) -> bool:
        return _is_capability_object(self._session_capabilities().get("list"))

    @property
    def session_resume(self) -> bool:
        return _is_capability_object(self._session_capabilities().get("resume"))

    @property
    def session_close(self) -> bool:
        return _is_capability_object(self._session_capabilities().get("close"))

    @property
    def session_delete(self) -> bool:
        return _is_capability_object(self._session_capabilities().get("delete"))

    @property
    def additional_directories(self) -> bool:
        return _is_capability_object(
            self._session_capabilities().get("additionalDirectories")
        )

    def _session_capabilities(self) -> Mapping[str, Any]:
        value = self.raw.get("sessionCapabilities")
        return value if isinstance(value, Mapping) else {}


@dataclass(frozen=True, slots=True)
class InitializeResult:
    protocol_version: int
    capabilities: AgentCapabilities
    agent_info: Mapping[str, Any] | None
    auth_methods: tuple[Mapping[str, Any], ...]
    raw: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class SessionResult:
    session_id: str
    modes: Mapping[str, Any] | None
    config_options: tuple[Mapping[str, Any], ...]
    raw: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class SessionInfo:
    session_id: str
    cwd: str
    additional_directories: tuple[str, ...]
    title: str | None
    updated_at: str | None
    raw: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class SessionPage:
    sessions: tuple[SessionInfo, ...]
    next_cursor: str | None
    raw: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class PromptResult:
    stop_reason: StopReason
    raw: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class SessionUpdate:
    session_id: str
    update_kind: SessionUpdateKind | str
    update: Mapping[str, Any]
    meta: Mapping[str, Any] | None
    raw: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class PermissionOption:
    option_id: str
    name: str
    kind: PermissionOptionKind | str
    raw: Mapping[str, Any]


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


def _is_capability_object(value: Any) -> bool:
    # ACP advertises optional capabilities with an object (often simply {}).
    return isinstance(value, Mapping)


def _valid_request_id(value: Any) -> bool:
    # ACP v1 inherits JSON-RPC's String, integral Number, or Null request IDs.
    # The official schema represents Number as a signed 64-bit integer.
    return (
        value is None
        or isinstance(value, str)
        or (
            isinstance(value, int)
            and not isinstance(value, bool)
            and _MIN_REQUEST_NUMBER <= value <= _MAX_REQUEST_NUMBER
        )
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
    if isinstance(line, str):
        try:
            raw = line.encode("utf-8")
        except UnicodeEncodeError as exc:
            raise AcpFramingError("ACP frame is not valid UTF-8") from exc
    else:
        raw = bytes(line)
    if not raw:
        raise AcpFramingError("empty ACP frame")
    if len(raw) > max_frame_bytes:
        raise AcpFramingError("ACP frame exceeds configured size limit")
    if require_newline and not raw.endswith(b"\n"):
        raise AcpFramingError("ACP frame is not newline terminated")
    payload = raw[:-1] if raw.endswith(b"\n") else raw
    if payload.endswith(b"\r"):
        payload = payload[:-1]
    if b"\n" in payload or b"\r" in payload:
        raise AcpFramingError("ACP frame contains an embedded line break")
    if not payload:
        raise AcpFramingError("empty ACP JSON payload")
    try:
        text = payload.decode("utf-8", errors="strict")
        value = json.loads(
            text,
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

    has_method = "method" in value
    has_id = "id" in value
    has_result = "result" in value
    has_error = "error" in value

    if has_method:
        if has_result or has_error:
            raise AcpEnvelopeError("JSON-RPC call cannot contain result or error")
        method = value["method"]
        if not isinstance(method, str) or not method:
            raise AcpEnvelopeError("JSON-RPC method must be a non-empty string")
        params = value.get("params", {})
        if not isinstance(params, Mapping):
            raise AcpEnvelopeError("ACP method params must be an object")
        frozen_params = _freeze_mapping(params)
        if not has_id:
            return JsonRpcNotification(method=method, params=frozen_params)
        request_id = value["id"]
        if not _valid_request_id(request_id):
            raise AcpEnvelopeError(
                "JSON-RPC request id must be a string, signed 64-bit integer, or null"
            )
        return JsonRpcRequest(
            request_id=request_id,
            method=method,
            params=frozen_params,
        )

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
        return JsonRpcResponse(
            request_id=request_id,
            error=_freeze_mapping(error),
        )
    return JsonRpcResponse(request_id=request_id, result=value["result"])


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


def request_envelope(
    request_id: RequestId,
    method: str,
    params: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    value = {
        "jsonrpc": JSONRPC_VERSION,
        "id": request_id,
        "method": method,
        "params": dict(params or {}),
    }
    validate_envelope(value)
    return value


def notification_envelope(
    method: str,
    params: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    value = {
        "jsonrpc": JSONRPC_VERSION,
        "method": method,
        "params": dict(params or {}),
    }
    validate_envelope(value)
    return value


def result_envelope(request_id: RequestId, result: Any) -> dict[str, Any]:
    value = {"jsonrpc": JSONRPC_VERSION, "id": request_id, "result": result}
    validate_envelope(value)
    return value


def error_envelope(
    request_id: RequestId | None,
    code: int,
    message: str,
    *,
    data: Any = None,
) -> dict[str, Any]:
    error: dict[str, Any] = {"code": code, "message": message}
    if data is not None:
        error["data"] = data
    value = {"jsonrpc": JSONRPC_VERSION, "id": request_id, "error": error}
    validate_envelope(value)
    return value


def parse_session_update(params: Mapping[str, Any]) -> SessionUpdate:
    session_id = _required_string(params, "sessionId")
    update = params.get("update")
    if not isinstance(update, Mapping):
        raise AcpEnvelopeError("session/update params.update must be an object")
    kind_value = _required_string(update, "sessionUpdate")
    try:
        kind: SessionUpdateKind | str = SessionUpdateKind(kind_value)
    except ValueError:
        # ACP extensions and future stable revisions remain observable.
        kind = kind_value
    meta = params.get("_meta")
    if meta is not None and not isinstance(meta, Mapping):
        meta = None
    return SessionUpdate(
        session_id=session_id,
        update_kind=kind,
        update=_freeze_mapping(update),
        meta=_freeze_mapping(meta) if isinstance(meta, Mapping) else None,
        raw=_freeze_mapping(params),
    )


def parse_permission_request(request: JsonRpcRequest) -> PermissionRequest:
    if request.method != "session/request_permission":
        raise AcpEnvelopeError("request is not session/request_permission")
    params = request.params
    session_id = _required_string(params, "sessionId")
    tool_call = params.get("toolCall")
    if not isinstance(tool_call, Mapping):
        raise AcpEnvelopeError("permission request toolCall must be an object")
    raw_options = params.get("options")
    if not isinstance(raw_options, list) or not raw_options:
        raise AcpEnvelopeError("permission request options must be a non-empty array")
    options: list[PermissionOption] = []
    seen_option_ids: set[str] = set()
    for raw in raw_options:
        if not isinstance(raw, Mapping):
            raise AcpEnvelopeError("permission option must be an object")
        option_id = _required_string(raw, "optionId")
        if option_id in seen_option_ids:
            raise AcpEnvelopeError("permission option IDs must be unique")
        seen_option_ids.add(option_id)
        name = _required_string(raw, "name")
        kind_value = _required_string(raw, "kind")
        try:
            kind: PermissionOptionKind | str = PermissionOptionKind(kind_value)
        except ValueError:
            kind = kind_value
        options.append(
            PermissionOption(
                option_id=option_id,
                name=name,
                kind=kind,
                raw=_freeze_mapping(raw),
            )
        )
    meta = params.get("_meta")
    return PermissionRequest(
        request_id=request.request_id,
        session_id=session_id,
        tool_call=_freeze_mapping(tool_call),
        options=tuple(options),
        meta=_freeze_mapping(meta) if isinstance(meta, Mapping) else None,
        raw=_freeze_mapping(params),
    )


def _required_string(value: Mapping[str, Any], key: str) -> str:
    result = value.get(key)
    if not isinstance(result, str) or not result:
        raise AcpEnvelopeError(f"{key} must be a non-empty string")
    return result
