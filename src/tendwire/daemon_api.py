"""Local stdlib JSON request/response API for the Tendwire daemon."""

from __future__ import annotations

import errno
import json
import math
import os
import re
import socket
import struct
import threading
import time
from collections.abc import Callable, Iterator, Mapping
from concurrent.futures import Future, wait
from queue import Empty, Full, Queue
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from ._version import __version__
from .core.commands import (
    CommandEnvelope,
    error_value,
    parse_command_request,
    validate_public_command_envelope,
)
from .core.models import (
    Snapshot,
    public_json_dumps,
    sanitize_public_mapping,
)
from .core.turns import (
    TURN_DELTA_DEFAULT_LIMIT,
    TURN_DELTA_MAX_LIMIT,
    TURN_LIST_DEFAULT_LIMIT,
    TURN_LIST_MAX_LIMIT,
)
from .connectors.protocol import (
    valid_canonical_utc as _valid_utc,
    valid_delivery_key,
    valid_generic_payload,
    valid_turn_final_delivery,
)
from .local_state import (
    EntryIdentity,
    LocalStateError,
    LocalStateErrorCode,
    enforce_bound_socket_permissions_at,
    open_resolved_parent,
    owned_socket_identity_at,
    pin_owned_socket_at,
    pin_socket_for_client_at,
    prepare_resolved_private_parent,
    proc_fd_path,
    resolve_socket_group,
    socket_bind_umask,
    unlink_verified_socket_at,
    validate_private_socket_parent_at,
    validate_socket_group_parent_at,
)


API_SCHEMA_VERSION = 1
MAX_REQUEST_BYTES = 1024 * 1024
MAX_RESPONSE_BYTES = 1024 * 1024
MAX_PUBLIC_REQUEST_ID_CHARS = 128
_SOCKET_STARTUP_LOCK_TIMEOUT_SECONDS = 1.0
_SOCKET_STARTUP_LOCK_RETRY_SECONDS = 0.01
_SERVER_LISTEN_BACKLOG = 32
_CAMEL_CASE_BOUNDARY_RE = re.compile(r"(?<=[a-z0-9])(?=[A-Z])")
_REVISION_RE = re.compile(r"twrev1\.[A-Za-z0-9_-]{43}")
_FINAL_RE = re.compile(r"twfinal1\.[A-Za-z0-9_-]{43}")
_PLAN_RE = re.compile(r"twplan1\.[A-Za-z0-9_-]{1,256}")
_REF_RE = re.compile(r"twref1\.[A-Za-z0-9_-]{43}")
_CONNECTOR_NAME_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,63}")
_RESTORE_REVISION_RE = re.compile(r"twrev1\.[A-Za-z0-9_-]+")
_CONNECTOR_NAME_FORBIDDEN = (
    "backend_target", "pane_id", "session_id", "terminal_id", "chat_id", "topic_id",
    "message_id", "bot_token", "shell", "argv", "environment", "stdout", "stderr",
)
_SEAL_TOKEN = object()


class _SealedResponse(dict[str, Any]):
    """Completed API response with an immutable serialization snapshot."""

    __slots__ = ("_encoded",)

    def __init__(self, value: Mapping[str, Any], token: object) -> None:
        if token is not _SEAL_TOKEN:
            raise TypeError("daemon response seal is private")
        super().__init__(value)
        object.__setattr__(self, "_encoded", json.dumps(
            value, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8"))

    @property
    def encoded(self) -> bytes:
        return self._encoded

    def __setattr__(self, _name: str, _value: Any) -> None:
        raise TypeError("daemon response seal is immutable")


_CONNECTOR_PRIVATE_KEYS = frozenset(
    {
        "backend_target", "binding_private_fingerprint", "private_fingerprint",
        "private_binding", "pane_id", "session_id", "terminal_id", "chat_id",
        "topic_id", "message_id", "bot_token", "credential", "credentials",
        "secret", "secrets", "password", "api_key", "socket_path", "argv",
        "environment", "stdin", "stdout", "stderr",
    }
)
_CONNECTOR_COMMON = frozenset({"schema_version", "ok", "status", "host_id", "name"})
_CONNECTOR_ERROR_STATUSES = frozenset(
    {
        "invalid_params",
        "invalid_payload",
        "invalid_ref",
        "stale_ref",
        "store_unavailable",
        "unknown_method",
        "revision_not_found",
        "stale_revision",
        "content_unavailable",
        "plan_not_found",
        "plan_conflict",
        "part_conflict",
        "plan_incomplete",
        "plan_not_failed",
        "not_recoverable",
        "request_conflict",
        "ack_deadline_expired",
        "not_retryable",
    }
)
_CONNECTOR_RESULT_SPECS = {
    method: (frozenset(statuses.split()), tuple(frozenset(fields.split()) for fields in shapes))
    for method, (statuses, shapes) in {
        "connector.poll": ("ok", ("items",)),
        "connector.reclaim": ("ok", ("reclaimed",)),
        "connector.renew": ("renewed", ("ref key attempt leased_until",)),
        "connector.ack": ("acknowledged", ("ref key attempt",)),
        "connector.release": ("released superseded", ("ref key attempt",)),
        "connector.fail": ("retry_scheduled attempts_exhausted superseded", ("ref key attempt", "ref key attempt available_at")),
        "connector.defer": ("deferred superseded", ("ref key attempt", "ref key attempt available_at")),
        "connector.inspect": ("ok", ("total items",)),
        "connector.retry": ("requeued", ("key retry_generation prior_attempt_count", "key retry_generation prior_attempt_count warning")),
        "connector.prepare": ("ok recovered", (
            "plan_token state generation part_count accepted_ordinals",
            "plan_token state generation part_count ordinal accepted_ordinals",
            "plan_token state generation part_count job_count accepted_ordinals",
            "failed_plan_token plan_token generation content_revision state acknowledged_prefix_count executable_job_count retained_failed_job_count prior_attempt_count idempotent_replay",
        )),
    }.items()
}


def _exact_json_value(
    value: Any, *, depth: int = 0, budget: list[int] | None = None,
) -> Any:
    """Copy interoperable JSON without redacting protocol-defined fields."""
    budget = [0] if budget is None else budget
    budget[0] += 1
    if depth > 12 or budget[0] > 4096:
        raise ValueError("connector response is too deeply nested")
    if isinstance(value, str):
        try:
            value.encode("utf-8")
        except UnicodeError:
            raise ValueError("connector response contains invalid text") from None
        return value
    if value is None or isinstance(value, (bool, int)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("connector response contains a non-finite number")
        return value
    if isinstance(value, Mapping):
        result: dict[str, Any] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise ValueError("connector response keys must be strings")
            try:
                key.encode("utf-8")
            except UnicodeError:
                raise ValueError("connector response contains an invalid key") from None
            if key.lower() in _CONNECTOR_PRIVATE_KEYS:
                raise ValueError("connector response contains a private key")
            result[key] = _exact_json_value(item, depth=depth + 1, budget=budget)
        return result
    if isinstance(value, (list, tuple)):
        return [_exact_json_value(item, depth=depth + 1, budget=budget) for item in value]
    raise ValueError("connector response contains a non-JSON value")


def _closed_mapping(value: Any, fields: set[str]) -> Mapping[str, Any] | None:
    return value if isinstance(value, Mapping) and set(value) == fields else None


def _matches(pattern: re.Pattern[str], value: Any) -> bool:
    return isinstance(value, str) and pattern.fullmatch(value) is not None


def _optional_match(pattern: re.Pattern[str], value: Any) -> bool:
    return value is None or _matches(pattern, value)


def _exact_int(value: Any, minimum: int = 0) -> bool:
    return type(value) is int and value >= minimum


def _public_exact_identifier(field: str, value: Any) -> bool:
    if not isinstance(value, str) or not value or len(value) > 512:
        return False
    return sanitize_public_mapping({field: value}).get(field) == value


def _valid_connector_key(name: str, value: Any) -> bool:
    if not isinstance(value, str) or not value:
        return False
    return bool(
        valid_delivery_key(value)
        if name == "turn-final"
        else _public_exact_identifier("key", value)
    )


def _request_connector_name(value: Any) -> str:
    if not isinstance(value, str) or not _matches(_CONNECTOR_NAME_RE, value):
        return ""
    lowered, compact = value.lower(), re.sub(r"[^a-z0-9]", "", value.lower())
    return value if _public_exact_identifier("name", value) and not any(
        token in lowered or token.replace("_", "") in compact
        for token in _CONNECTOR_NAME_FORBIDDEN
    ) else ""


def _valid_ref_fields(value: Mapping[str, Any], *, dates_required: bool = False) -> bool:
    dates = (value.get("leased_until"), value.get("available_at"))
    return bool(
        _matches(_REF_RE, value.get("ref"))
        and _exact_int(value.get("attempt"), 1)
        and all(
            _valid_utc(item) if dates_required else item is None or _valid_utc(item)
            for item in dates
        )
    )


_INSPECT_FIELDS = frozenset(
    {
        "kind",
        "key",
        "final_identity",
        "failed_plan_token",
        "decision_ref",
        "target_key",
        "reason",
        "attempt_count",
        "prior_attempt_count",
        "created_at",
        "terminal_at",
        "retryable",
        "recoverable",
    }
)
_INSPECT_KINDS = frozenset("working final_ready final_part decision retire".split())
_INSPECT_REASONS = frozenset(
    {
        "temporary",
        "rate_limited",
        "provider_rejected",
        "provider_uncertain",
        "invalid_payload",
        "content_unavailable",
        "route_unavailable",
        "provider_binding_unknown",
        "lease_expired",
        "ack_deadline_expired",
        "superseded",
        "attempts_exhausted",
        "operator_recovery",
    }
)


def _valid_inspect_item(value: Any) -> bool:
    item = _closed_mapping(value, set(_INSPECT_FIELDS))
    return bool(
        item
        and item.get("kind") in _INSPECT_KINDS
        and _valid_connector_key("turn-final", item.get("key"))
        and (item.get("decision_ref") is None or _public_exact_identifier("decision_ref", item["decision_ref"]))
        and _optional_match(_FINAL_RE, item.get("final_identity"))
        and _optional_match(_PLAN_RE, item.get("failed_plan_token"))
        and (item.get("target_key") is None or _valid_connector_key("turn-final", item["target_key"]))
        and _exact_int(item.get("attempt_count"))
        and _exact_int(item.get("prior_attempt_count"))
        and item.get("reason") in _INSPECT_REASONS
        and _valid_utc(item.get("created_at"))
        and _valid_utc(item.get("terminal_at"))
        and type(item.get("retryable")) is bool
        and type(item.get("recoverable")) is bool
    )


def _valid_nonpoll_connector_result(method: str, value: Mapping[str, Any]) -> bool:
    name = str(value.get("name"))
    if method == "connector.reclaim":
        return type(value.get("reclaimed")) is int and value["reclaimed"] >= 0
    if method in {
        "connector.renew", "connector.ack", "connector.release",
        "connector.fail", "connector.defer",
    }:
        available = "available_at" in value
        needs_available = (
            (method == "connector.fail" and value.get("status") == "retry_scheduled")
            or (method == "connector.defer" and value.get("status") == "deferred")
        )
        return bool(
            _valid_ref_fields(value)
            and _valid_connector_key(name, value.get("key"))
            and (method not in {"connector.fail", "connector.defer"} or available == needs_available)
        )
    if method == "connector.retry":
        return bool(
            name == "turn-final"
            and _valid_connector_key(name, value.get("key"))
            and _exact_int(value.get("retry_generation"), 1)
            and _exact_int(value.get("prior_attempt_count"))
            and value.get("warning") in {None, "provider_acceptance_may_have_occurred"}
        )
    if method == "connector.prepare":
        recovering = "failed_plan_token" in value
        accepted = value.get("accepted_ordinals", [])
        part_count = value.get("part_count")
        counts = (
            "acknowledged_prefix_count", "executable_job_count",
            "retained_failed_job_count", "prior_attempt_count",
        )
        return bool(
            name == "turn-final"
            and _matches(_PLAN_RE, value.get("plan_token"))
            and (not recovering or _matches(_PLAN_RE, value.get("failed_plan_token")))
            and value.get("status") == ("recovered" if recovering else "ok")
            and _exact_int(value.get("generation"), 1)
            and (part_count is None or _exact_int(part_count, 1) and part_count <= 10_000)
            and ("ordinal" not in value or _exact_int(value["ordinal"]) and value["ordinal"] < part_count)
            and ("job_count" not in value or _exact_int(value["job_count"], 1))
            and all(field not in value or _exact_int(value[field]) for field in counts)
            and ("content_revision" not in value or _matches(_REVISION_RE, value["content_revision"]))
            and isinstance(accepted, list)
            and all(_exact_int(item) and part_count is not None and item < part_count for item in accepted)
            and accepted == sorted(set(accepted))
            and value.get("state") in "preparing active waiting_predecessor completed failed superseded".split()
            and ("idempotent_replay" not in value or type(value["idempotent_replay"]) is bool)
        )
    if method == "connector.inspect":
        items = value.get("items")
        return bool(
            name == "turn-final"
            and _exact_int(value.get("total"))
            and isinstance(items, list)
            and all(_valid_inspect_item(item) for item in items)
            and value["total"] >= len(items)
        )
    return False


def _validated_connector_result(
    method: str, result: Mapping[str, Any], *, expected_name: str,
) -> dict[str, Any]:
    copied = _exact_json_value(result)
    if not isinstance(copied, dict):
        raise ValueError("connector result must be an object")
    if (
        not _CONNECTOR_COMMON <= copied.keys()
        or copied.get("schema_version") != 1
        or type(copied.get("schema_version")) is not int
        or type(copied.get("ok")) is not bool
    ):
        raise ValueError("connector result has an invalid envelope")
    result_name = copied.get("name")
    if not _public_exact_identifier("host_id", copied.get("host_id")):
        raise ValueError("connector result identity is invalid")
    if copied["ok"] is False:
        if (
            result_name not in {"", expected_name}
            or (bool(result_name) and not _matches(_CONNECTOR_NAME_RE, result_name))
            or copied.keys() != _CONNECTOR_COMMON | {"message"}
            or copied.get("message") != copied.get("status")
            or copied.get("status") not in _CONNECTOR_ERROR_STATUSES
        ):
            raise ValueError("connector error has an invalid envelope")
        return copied
    if not expected_name or result_name != expected_name:
        raise ValueError("connector result identity is invalid")
    statuses, field_sets = _CONNECTOR_RESULT_SPECS[method]
    if not any(copied.keys() == _CONNECTOR_COMMON | fields for fields in field_sets):
        raise ValueError("connector result has unexpected fields")
    if copied.get("status") not in statuses:
        raise ValueError("connector result status is invalid")
    if method == "connector.poll":
        items = copied.get("items")
        item_fields = {"key", "ref", "attempt", "leased_until", "available_at", "payload"}
        if not isinstance(items, list):
            raise ValueError("connector poll items are invalid")
        for item in items:
            if not isinstance(item, Mapping) or set(item) not in {frozenset(item_fields), frozenset(item_fields | {"created_at"})}:
                raise ValueError("connector poll item has unexpected fields")
            if (
                not _valid_connector_key(copied["name"], item.get("key"))
                or not _valid_ref_fields(item, dates_required=True)
                or (
                    item.get("created_at") is not None
                    and not _valid_utc(item.get("created_at"))
                )
            ):
                raise ValueError("connector poll item values are invalid")
            payload = item["payload"]
            valid_payload = (
                valid_turn_final_delivery(payload, item["key"], copied["host_id"])
                if copied["name"] == "turn-final" else valid_generic_payload(payload)
            )
            if not valid_payload:
                raise ValueError("connector poll payload is invalid")
    elif not _valid_nonpoll_connector_result(method, copied):
        raise ValueError("connector result values are invalid")
    return copied


_REQUEST_ID_FORBIDDEN_SEGMENTS = frozenset(
    {
        "telegram",
        "chat",
        "chats",
        "topic",
        "topics",
        "message",
        "messages",
        "thread",
        "threads",
        "token",
        "tokens",
        "auth",
        "authorization",
        "bearer",
        "cookie",
        "cookies",
        "credential",
        "credentials",
        "delivery",
        "deliveries",
        "route",
        "routes",
        "connector",
        "connectors",
        "herdres",
        "backend",
        "target",
        "targets",
        "terminal",
        "terminals",
        "pane",
        "panes",
        "tab",
        "tabs",
        "window",
        "windows",
        "tty",
        "pty",
        "pid",
        "pids",
        "process",
        "processes",
        "tmux",
        "screen",
        "agent_session",
        "session",
        "sessions",
        "private",
        "argv",
        "args",
        "env",
        "raw",
        "payload",
        "payloads",
        "control",
        "controls",
        "escape",
        "escapes",
        "stdin",
        "stderr",
        "stdout",
        "shell",
        "secret",
        "secrets",
        "password",
        "passwords",
        "api_key",
        "api_keys",
        "apikey",
    }
)
_REQUEST_ID_FORBIDDEN_COMPACT = frozenset(
    segment.replace("_", "") for segment in _REQUEST_ID_FORBIDDEN_SEGMENTS
)

_METHOD_PARAMS = {
    method: frozenset(fields.split())
    for method, fields in {
        "ping": "", "health.get": "", "snapshot.get": "", "attention.list": "",
        "pending.list": "", "turn.list": "schema_version limit cursor since",
        "turn.delta": "watermark cursor limit",
        "turn.content.get": "schema_version turn_id content_revision field cursor",
    }.items()
}
_METHOD_PARAMS.update({"command.submit": None, **dict.fromkeys(_CONNECTOR_RESULT_SPECS)})
REQUIRED_METHODS = frozenset(_METHOD_PARAMS)
_SIMPLE_METHODS = {
    "health.get": ("_get_health", True),
    "snapshot.get": ("_get_snapshot", False),
    "attention.list": ("_get_attention", False),
    "pending.list": ("_get_pending", False),
}
_PAGE_METHODS = {
    "turn.list": (
        "_get_turns", ("cursor", "since"), TURN_LIST_DEFAULT_LIMIT,
        TURN_LIST_MAX_LIMIT,
    ),
    "turn.delta": (
        "_get_turn_delta", ("watermark", "cursor"), TURN_DELTA_DEFAULT_LIMIT,
        TURN_DELTA_MAX_LIMIT,
    ),
}


class DaemonAPIError(Exception):
    """Base class for daemon transport and protocol failures."""


class DaemonUnavailable(DaemonAPIError):
    """Raised when the local daemon socket cannot be reached safely."""

    def __init__(
        self,
        message: str = "daemon socket is unavailable",
        *,
        code: LocalStateErrorCode | None = None,
        timed_out: bool = False,
        request_started: bool = False,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.timed_out = timed_out
        self.request_started = request_started


class DaemonProtocolError(DaemonAPIError):
    """Raised when a daemon response cannot be parsed or trusted."""

    def __init__(
        self,
        message: str,
        *,
        request_started: bool = False,
    ) -> None:
        super().__init__(message)
        self.request_started = request_started

def _request_id_has_forbidden_segment_sequence(normalized: str) -> bool:
    wrapped = f"_{normalized}_"
    return any(f"_{forbidden}_" in wrapped for forbidden in _REQUEST_ID_FORBIDDEN_SEGMENTS)


def _public_request_id(value: Any) -> str | int | float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if not isinstance(value, str):
        return None
    if not value or len(value) > MAX_PUBLIC_REQUEST_ID_CHARS:
        return None
    if not all(char.isascii() and (char.isalnum() or char in "._:-") for char in value):
        return None
    separated = _CAMEL_CASE_BOUNDARY_RE.sub("_", value)
    normalized = re.sub(r"[^a-z0-9]+", "_", separated.lower()).strip("_")
    compact = normalized.replace("_", "")
    if normalized in _REQUEST_ID_FORBIDDEN_SEGMENTS or compact in _REQUEST_ID_FORBIDDEN_COMPACT:
        return None
    if _request_id_has_forbidden_segment_sequence(normalized):
        return None
    if any(
        compact.endswith(forbidden)
        for forbidden in _REQUEST_ID_FORBIDDEN_COMPACT
        if len(forbidden) >= 5
    ):
        return None
    return value


def success_response(result: Mapping[str, Any] | None = None, *, request_id: Any = None) -> dict[str, Any]:
    response: dict[str, Any] = {
        "schema_version": API_SCHEMA_VERSION,
        "ok": True,
        "status": "ok",
        "result": sanitize_public_mapping(dict(result or {})),
        "error": None,
    }
    public_id = _public_request_id(request_id)
    if public_id is not None:
        response["id"] = public_id
    return sanitize_public_mapping(response)


def _connector_success_response(
    method: str,
    result: Mapping[str, Any],
    *,
    expected_name: str,
    request_id: Any = None,
) -> dict[str, Any]:
    response = dict(
        schema_version=API_SCHEMA_VERSION,
        ok=True,
        status="ok",
        result=_validated_connector_result(method, result, expected_name=expected_name),
        error=None,
    )
    public_id = _public_request_id(request_id)
    if public_id is not None:
        response["id"] = public_id
    return response


def _daemon_identity_response(
    result: Mapping[str, Any],
    *,
    request_id: Any = None,
) -> dict[str, Any]:
    """Add the local daemon identity after generic public-field sanitation."""
    response = success_response(result, request_id=request_id)
    public_result = response.get("result")
    if isinstance(public_result, dict):
        public_result["version"] = __version__
        public_result["pid"] = os.getpid()
    return response


def error_response(
    code: str,
    message: str,
    *,
    details: Mapping[str, Any] | None = None,
    request_id: Any = None,
) -> dict[str, Any]:
    response: dict[str, Any] = {
        "schema_version": API_SCHEMA_VERSION,
        "ok": False,
        "status": "error",
        "result": None,
        "error": error_value(code, message, details=dict(details or {})),
    }
    public_id = _public_request_id(request_id)
    if public_id is not None:
        response["id"] = public_id
    return sanitize_public_mapping(response)


def _snapshot_dict(snapshot: Snapshot) -> dict[str, Any]:
    return sanitize_public_mapping(snapshot.to_dict())


def _command_result(value: Any, request_params: Mapping[str, Any]) -> dict[str, Any]:
    if isinstance(value, CommandEnvelope):
        data: Any = value.to_dict()
    elif hasattr(value, "to_dict"):
        data = value.to_dict()
    else:
        data = value
    if not isinstance(data, dict):
        raise ValueError("command.submit returned a non-object result")
    request, parse_error = parse_command_request(
        json.dumps(dict(request_params), ensure_ascii=False, separators=(",", ":"))
    )
    if parse_error is not None:
        request = None
    return validate_public_command_envelope(
        CommandEnvelope.from_dict(data), request
    ).to_dict()


def _command_success_response(
    value: Any,
    *,
    request_params: Mapping[str, Any],
    request_id: Any = None,
) -> dict[str, Any]:
    command_result = _command_result(value, request_params)
    response = success_response({}, request_id=request_id)
    response["result"] = command_result
    return response


def _unknown_params_error(
    params: Mapping[str, Any], allowed: set[str], method: str, request_id: Any,
) -> dict[str, Any] | None:
    unknown = [key for key in params if str(key) not in allowed]
    return error_response(
        "invalid_params", f"{method} contains unknown parameters",
        details={"field_count": len(unknown)}, request_id=request_id,
    ) if unknown else None


def _page_params(
    params: Mapping[str, Any], token_names: tuple[str, str], default: int,
    maximum: int, request_id: Any,
) -> tuple[tuple[Any, Any, Any] | None, dict[str, Any] | None]:
    limit = params.get("limit", default)
    if type(limit) is not int or not 1 <= limit <= maximum:
        return None, error_response(
            "invalid_params", "limit must be an integer in the supported range",
            details={"field": "limit", "minimum": 1, "maximum": maximum},
            request_id=request_id,
        )
    tokens = tuple(params.get(name) for name in token_names)
    for name, token in zip(token_names, tokens, strict=True):
        if token is not None and (not isinstance(token, str) or not token):
            return None, error_response(
                "invalid_params", f"{name} must be a non-empty string or null",
                details={"field": name}, request_id=request_id,
            )
    if all(token is not None for token in tokens):
        return None, error_response(
            "invalid_params", f"{token_names[0]} and {token_names[1]} cannot be combined",
            details={"fields": list(token_names)}, request_id=request_id,
        )
    return (limit, *tokens), None


class TendwireDaemonAPI:
    """Dispatch stable local daemon methods through injected public helpers."""

    def __init__(
        self,
        *,
        get_snapshot: Callable[[], Snapshot],
        get_health: Callable[[], Mapping[str, Any]],
        submit_command: Callable[[Mapping[str, Any]], Mapping[str, Any] | CommandEnvelope],
        get_attention: Callable[[], Mapping[str, Any]],
        get_turns: Callable[..., Mapping[str, Any]],
        get_turn_delta: Callable[..., Mapping[str, Any]],
        get_turn_content: Callable[[Mapping[str, Any]], Mapping[str, Any]],
        get_pending: Callable[[], Mapping[str, Any]],
        connector_call: Callable[[str, Mapping[str, Any]], Mapping[str, Any]],
    ) -> None:
        callbacks = {
            "get_snapshot": get_snapshot,
            "get_health": get_health,
            "submit_command": submit_command,
            "get_attention": get_attention,
            "get_turns": get_turns,
            "get_turn_delta": get_turn_delta,
            "get_turn_content": get_turn_content,
            "get_pending": get_pending,
            "connector_call": connector_call,
        }
        invalid = sorted(name for name, callback in callbacks.items() if not callable(callback))
        if invalid:
            raise TypeError(f"daemon callbacks must be callable: {', '.join(invalid)}")
        for name, callback in callbacks.items():
            setattr(self, f"_{name}", callback)

    def dispatch(self, request: Any) -> dict[str, Any]:
        try:
            return _SealedResponse(self._dispatch(request), _SEAL_TOKEN)
        except (TypeError, ValueError, UnicodeError, RecursionError) as exc:
            return _SealedResponse(error_response(
                "internal_error", "daemon method failed",
                details={"type": type(exc).__name__},
                request_id=request.get("id") if isinstance(request, Mapping) else None,
            ), _SEAL_TOKEN)

    def _dispatch(self, request: Any) -> dict[str, Any]:
        if not isinstance(request, Mapping):
            return error_response("invalid_request", "request must be a JSON object")

        request_id = request.get("id")
        unknown = sorted(str(key) for key in request if str(key) not in {"id", "method", "params"})
        if unknown:
            return error_response(
                "invalid_request",
                "request contains unknown top-level fields",
                details={"field_count": len(unknown)},
                request_id=request_id,
            )

        method = request.get("method")
        if not isinstance(method, str) or not method:
            return error_response(
                "invalid_request",
                "method is required",
                details={"field": "method"},
                request_id=request_id,
            )
        allowed_params = _METHOD_PARAMS.get(method)
        if method not in _METHOD_PARAMS:
            return error_response(
                "unknown_method",
                "unknown method",
                details={"allowed": sorted(REQUIRED_METHODS)},
                request_id=request_id,
            )

        params = request.get("params", {})
        if params is None:
            params = {}
        if not isinstance(params, Mapping):
            return error_response(
                "invalid_params",
                "params must be a JSON object",
                details={"field": "params"},
                request_id=request_id,
            )
        if allowed_params is not None:
            invalid = _unknown_params_error(
                params, set(allowed_params), method, request_id
            )
            if invalid:
                return invalid

        try:
            if method == "ping":
                return _daemon_identity_response(
                    {"pong": True, "methods": sorted(REQUIRED_METHODS)},
                    request_id=request_id,
                )
            simple = _SIMPLE_METHODS.get(method)
            if simple is not None:
                callback_name, include_identity = simple
                result = getattr(self, callback_name)()
                if method == "snapshot.get":
                    result = _snapshot_dict(result)
                response = _daemon_identity_response if include_identity else success_response
                return response(result, request_id=request_id)
            page_spec = _PAGE_METHODS.get(method)
            if page_spec is not None:
                schema_version = params.get("schema_version", 1)
                if method == "turn.list" and (
                    schema_version not in {1, 2} or isinstance(schema_version, bool)
                ):
                    return error_response(
                        "unsupported_schema",
                        "unsupported turn list schema version",
                        details={"supported_turn_schema_versions": [1, 2]},
                        request_id=request_id,
                    )
                callback_name, tokens, default, maximum = page_spec
                page, invalid = _page_params(params, tokens, default, maximum, request_id)
                if invalid:
                    return invalid
                assert page is not None
                result = dict(getattr(self, callback_name)(
                    **{
                        "limit": page[0],
                        **dict(zip(tokens, page[1:], strict=True)),
                        **({"schema_version": schema_version} if method == "turn.list" else {}),
                    }
                ))
                response = success_response(result, request_id=request_id)
                (_restore_turn_list_text if method == "turn.list" else _restore_turn_delta_text)(
                    response, result
                )
                return response
            if method == "turn.content.get":
                content_schema = params.get("schema_version", 1)
                if content_schema != 1 or isinstance(content_schema, bool):
                    return error_response(
                        "unsupported_schema",
                        "unsupported turn content schema version",
                        details={"supported_content_schema_versions": [1]},
                        request_id=request_id,
                    )
                for field_name in ("turn_id", "content_revision"):
                    value = params.get(field_name)
                    if not isinstance(value, str) or not value:
                        return error_response(
                            "invalid_params", f"{field_name} is required",
                            details={"field": field_name}, request_id=request_id,
                        )
                if params.get("field") not in {"user_text", "assistant_final_text"}:
                    return error_response(
                        "invalid_params",
                        "field is invalid",
                        details={"field": "field"},
                        request_id=request_id,
                    )
                cursor = params.get("cursor")
                if cursor is not None and (not isinstance(cursor, str) or not cursor):
                    return error_response(
                        "invalid_params",
                        "cursor must be a non-empty string or null",
                        details={"field": "cursor"},
                        request_id=request_id,
                    )
                result = dict(self._get_turn_content(dict(params)))
                response = success_response(result, request_id=request_id)
                _restore_content_page_text(response, result)
                return response
            if method == "command.submit":
                return _command_success_response(
                    self._submit_command(dict(params)),
                    request_params=params,
                    request_id=request_id,
                )
            if method.startswith("connector."):
                connector_result = dict(self._connector_call(method, dict(params)))
                return _connector_success_response(
                    method,
                    connector_result,
                    expected_name=_request_connector_name(params.get("name")),
                    request_id=request_id,
                )
        except Exception as exc:  # noqa: BLE001
            return error_response(
                "internal_error",
                "daemon method failed",
                details={"type": type(exc).__name__},
                request_id=request_id,
            )

        raise AssertionError("required daemon method is not dispatched")


def _restore_turn_text(
    target: dict[str, Any], original: Mapping[str, Any], schema_version: int,
) -> None:
    descriptors = (original.get("content") or {}).get("fields", {})
    for field in ("user_text", "assistant_final_text"):
        text = original.get(field)
        descriptor = descriptors.get(field) if isinstance(descriptors, Mapping) else None
        trusted_inline = schema_version == 1 or (
            isinstance(descriptor, Mapping)
            and descriptor.get("availability") == "complete"
            and descriptor.get("inline") is True
        )
        if trusted_inline and isinstance(text, str):
            target[field] = text
    for key in ("user_preview", "assistant_final_preview"):
        if isinstance(original.get(key), str):
            target[key] = original[key]


def _restore_turn_list_text(
    response: dict[str, Any],
    original_result: Mapping[str, Any],
) -> None:
    """Restore trusted canonical inline fields/previews after generic sanitation."""
    if original_result.get("schema_version") not in {1, 2}:
        return
    original_turns = original_result.get("turns")
    result = response.get("result")
    if not isinstance(original_turns, list) or not isinstance(result, dict):
        return
    sanitized_turns = result.get("turns")
    if not isinstance(sanitized_turns, list):
        return
    by_id = {
        item.get("id"): item
        for item in sanitized_turns
        if isinstance(item, dict) and isinstance(item.get("id"), str)
    }
    for original in original_turns:
        if not isinstance(original, Mapping):
            continue
        target = by_id.get(original.get("id"))
        if not isinstance(target, dict):
            continue
        _restore_turn_text(target, original, original_result["schema_version"])


def _restore_content_page_text(
    response: dict[str, Any],
    original_result: Mapping[str, Any],
) -> None:
    """Restore an already-canonical page after generic public string bounding."""
    text = original_result.get("text")
    if not isinstance(text, str):
        return
    if original_result.get("schema_version") != 1:
        return
    if original_result.get("field") not in {"user_text", "assistant_final_text"}:
        return
    if original_result.get("availability") != "complete":
        return
    result = response.get("result")
    if isinstance(result, dict):
        result["text"] = text
        content_revision = original_result.get("content_revision")
        if _matches(_RESTORE_REVISION_RE, content_revision):
            result["content_revision"] = content_revision


def _restore_turn_delta_text(
    response: dict[str, Any],
    original_result: Mapping[str, Any],
) -> None:
    """Restore trusted list-v2 inline fields/previews inside delta upserts."""
    result = response.get("result")
    original_changes = original_result.get("changes")
    if not isinstance(result, dict) or not isinstance(original_changes, list):
        return
    target_changes = result.get("changes")
    if not isinstance(target_changes, list):
        return
    for original, target in zip(original_changes, target_changes, strict=False):
        if not isinstance(original, Mapping) or not isinstance(target, dict):
            continue
        original_turn = original.get("turn")
        target_turn = target.get("turn")
        if not isinstance(original_turn, Mapping) or not isinstance(target_turn, dict):
            continue
        _restore_turn_text(target_turn, original_turn, 2)


def _serialized_response(response: Mapping[str, Any]) -> bytes:
    if isinstance(response, _SealedResponse):
        return response.encoded
    value = sanitize_public_mapping(response)
    original = response.get("result")
    public = value.get("result")
    if (
        isinstance(original, Mapping)
        and isinstance(public, dict)
        and original.get("version") == __version__
        and original.get("pid") == os.getpid()
    ):
        public.update(version=__version__, pid=os.getpid())
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def _ensure_unix_socket_supported() -> None:
    if not hasattr(socket, "AF_UNIX"):
        raise DaemonUnavailable("Unix domain sockets are not supported on this platform")


def _read_json_frame(
    conn: socket.socket,
    *,
    max_bytes: int = MAX_REQUEST_BYTES,
    deadline: float | None = None,
) -> bytes:
    chunks: list[bytes] = []
    total = 0
    while True:
        if deadline is not None:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise socket.timeout("JSON frame deadline exceeded")
            conn.settimeout(remaining)
        chunk = conn.recv(4096)
        if not chunk:
            break
        if b"\n" in chunk:
            head, _tail = chunk.split(b"\n", 1)
            chunks.append(head)
            total += len(head)
            if total > max_bytes:
                raise DaemonProtocolError("request exceeds maximum frame size")
            break
        chunks.append(chunk)
        total += len(chunk)
        if total > max_bytes:
            raise DaemonProtocolError("request exceeds maximum frame size")
    return b"".join(chunks)


def _local_state_unavailable(exc: LocalStateError) -> DaemonUnavailable:
    return DaemonUnavailable(
        "daemon socket local state is invalid",
        code=exc.code,
    )


def _close_fd(fd: int | None) -> None:
    if fd is not None:
        try:
            os.close(fd)
        except OSError:
            pass


@contextmanager
def _socket_startup_lock(parent_fd: int) -> Iterator[None]:
    """Serialize stale cleanup and socket publication within one parent."""

    try:
        import fcntl
    except ImportError:
        raise DaemonUnavailable(
            "secure daemon socket startup is unsupported",
            code=LocalStateErrorCode.UNSUPPORTED_PLATFORM,
        ) from None
    deadline = time.monotonic() + _SOCKET_STARTUP_LOCK_TIMEOUT_SECONDS
    while True:
        try:
            fcntl.flock(parent_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            break
        except OSError as exc:
            if exc.errno == errno.EINTR:
                continue
            if exc.errno not in {errno.EACCES, errno.EAGAIN}:
                raise DaemonUnavailable(
                    "daemon socket startup lock failed",
                    code=LocalStateErrorCode.OPERATION_FAILED,
                ) from None
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise DaemonUnavailable(
                    "daemon socket startup lock timed out",
                    code=LocalStateErrorCode.OPERATION_FAILED,
                ) from None
            time.sleep(min(_SOCKET_STARTUP_LOCK_RETRY_SECONDS, remaining))
    try:
        yield
    finally:
        while True:
            try:
                fcntl.flock(parent_fd, fcntl.LOCK_UN)
                break
            except OSError as exc:
                if exc.errno != errno.EINTR:
                    break


def _cleanup_stale_socket(parent_fd: int, leaf: str, address: str) -> None:
    pinned = pin_owned_socket_at(parent_fd, leaf)
    if pinned is None:
        return
    pin_fd, identity = pinned
    probe = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        probe.settimeout(0.1)
        probe.connect(address)
    except OSError as exc:
        if exc.errno != errno.ECONNREFUSED:
            raise DaemonUnavailable("daemon socket state is ambiguous") from None
        unlink_verified_socket_at(parent_fd, leaf, identity)
    else:
        raise DaemonUnavailable("daemon socket is already active")
    finally:
        probe.close()
        _close_fd(pin_fd)


def ensure_daemon_socket_not_active(
    socket_path: str | os.PathLike[str],
    *,
    socket_group: str | None = None,
) -> None:
    """Fail before startup work when a live process already owns the socket.

    This probe is read-only. Missing endpoints and owned socket files whose
    listener has gone away are left for the server's locked stale-socket
    cleanup. A connected peer is active even when it is older, non-Tendwire,
    slow, or returns an unrecognized protocol response.
    """
    client = DaemonAPIClient(
        socket_path,
        socket_group=socket_group,
        timeout_seconds=0.2,
    )
    try:
        response = client.request("ping")
    except DaemonUnavailable as exc:
        if exc.code is LocalStateErrorCode.MISSING_ENTRY:
            return
        if exc.code is None and not exc.request_started and not exc.timed_out:
            # Connection refused: the securely pinned socket entry is stale.
            return
        if exc.code is LocalStateErrorCode.PEER_VALIDATION_FAILED:
            raise DaemonUnavailable(
                _active_daemon_conflict_message(),
                code=exc.code,
            ) from None
        if exc.request_started or exc.timed_out:
            raise DaemonUnavailable(_active_daemon_conflict_message()) from None
        raise
    except DaemonProtocolError as exc:
        if exc.request_started:
            raise DaemonUnavailable(_active_daemon_conflict_message()) from None
        raise
    raise DaemonUnavailable(_active_daemon_conflict_message(response))


def _active_daemon_conflict_message(
    response: Mapping[str, Any] | None = None,
) -> str:
    result = response.get("result") if isinstance(response, Mapping) else None
    holder_version = result.get("version") if isinstance(result, Mapping) else None
    holder_pid = result.get("pid") if isinstance(result, Mapping) else None
    holder = (
        f"holder is tendwire {holder_version}"
        if isinstance(holder_version, str) and holder_version
        else "holder version is unknown"
    )
    if isinstance(holder_pid, int) and not isinstance(holder_pid, bool) and holder_pid > 0:
        holder += f" (PID {holder_pid})"
    return (
        f"daemon socket is already active: {holder}; "
        f"refusing to start tendwire {__version__}"
    )


class _DaemonRequestExecutor:
    """Fixed daemon-worker executor that cannot block interpreter shutdown."""

    def __init__(
        self,
        *,
        max_workers: int,
        queue_capacity: int,
        thread_name_prefix: str,
    ) -> None:
        self._queue: Queue[
            tuple[Future[Any], Callable[..., Any], tuple[Any, ...]] | None
        ] = Queue(maxsize=queue_capacity)
        self._lock = threading.Lock()
        self._shutdown = False
        threads = [
            threading.Thread(
                target=self._worker,
                name=f"{thread_name_prefix}_{index}",
                daemon=True,
            )
            for index in range(max_workers)
        ]
        started_threads: list[threading.Thread] = []
        try:
            for thread in threads:
                thread.start()
                started_threads.append(thread)
        except BaseException:  # noqa: BLE001
            self._shutdown = True
            for _thread in started_threads:
                self._queue.put_nowait(None)
            raise
        self._threads = tuple(started_threads)

    def submit(
        self,
        function: Callable[..., Any],
        *args: Any,
    ) -> Future[Any]:
        future: Future[Any] = Future()
        with self._lock:
            if self._shutdown:
                raise RuntimeError("request executor has shut down")
            self._queue.put_nowait((future, function, args))
        return future

    def shutdown(self, *, cancel_futures: bool) -> None:
        with self._lock:
            if self._shutdown:
                return
            self._shutdown = True
        if cancel_futures:
            while True:
                try:
                    item = self._queue.get_nowait()
                except Empty:
                    break
                try:
                    if item is not None:
                        future, _function, _args = item
                        future.cancel()
                finally:
                    self._queue.task_done()
        for _thread in self._threads:
            self._queue.put_nowait(None)

    def _worker(self) -> None:
        while True:
            item = self._queue.get()
            try:
                if item is None:
                    return
                future, function, args = item
                if not future.set_running_or_notify_cancel():
                    continue
                try:
                    result = function(*args)
                except BaseException as exc:  # noqa: BLE001
                    future.set_exception(exc)
                else:
                    future.set_result(result)
            finally:
                self._queue.task_done()


class UnixSocketJSONServer:
    """Bounded concurrent Unix-socket JSON request server."""

    def __init__(
        self,
        socket_path: str | os.PathLike[str],
        dispatcher: Callable[[Any], Mapping[str, Any]],
        *,
        stop_event: threading.Event | None = None,
        accept_timeout_seconds: float = 0.2,
        client_timeout_seconds: float = 1.0,
        max_request_bytes: int = MAX_REQUEST_BYTES,
        max_response_bytes: int = MAX_RESPONSE_BYTES,
        socket_group: str | None = None,
        prepare_parent: bool = False,
        request_workers: int = 8,
        max_in_flight_requests: int = 32,
        shutdown_grace_seconds: float = 6.0,
        periodic_callback: Callable[[], Any] | None = None,
        periodic_interval_seconds: float = 1.0,
    ) -> None:
        if (
            not isinstance(request_workers, int)
            or isinstance(request_workers, bool)
            or request_workers < 1
        ):
            raise ValueError("request_workers must be a positive integer")
        if (
            not isinstance(max_in_flight_requests, int)
            or isinstance(max_in_flight_requests, bool)
            or max_in_flight_requests < request_workers
        ):
            raise ValueError(
                "max_in_flight_requests must be an integer at least request_workers"
            )
        if (
            isinstance(shutdown_grace_seconds, bool)
            or not isinstance(shutdown_grace_seconds, (int, float))
            or not math.isfinite(float(shutdown_grace_seconds))
            or shutdown_grace_seconds < 0
        ):
            raise ValueError("shutdown_grace_seconds must be finite and non-negative")
        self.socket_path = Path(socket_path)
        self.dispatcher = dispatcher
        self.stop_event = stop_event or threading.Event()
        self.accept_timeout_seconds = accept_timeout_seconds
        self.client_timeout_seconds = client_timeout_seconds
        self.max_request_bytes = max_request_bytes
        self.max_response_bytes = max_response_bytes
        self.socket_group = socket_group
        self.prepare_parent = prepare_parent
        self.request_workers = request_workers
        self.max_in_flight_requests = max_in_flight_requests
        self.shutdown_grace_seconds = float(shutdown_grace_seconds)
        self.periodic_callback = periodic_callback
        self.periodic_interval_seconds = max(
            self.accept_timeout_seconds,
            float(periodic_interval_seconds),
        )
        self._next_periodic_at = time.monotonic() + self.periodic_interval_seconds
        self._listener: socket.socket | None = None
        self._identity: EntryIdentity | None = None
        self._pin_fd: int | None = None
        self._parent_fd: int | None = None
        self._leaf: str | None = None
        self._lifecycle_lock = threading.Lock()
        self._tracking_lock = threading.RLock()
        self._admission = threading.BoundedSemaphore(max_in_flight_requests)
        self._executor: _DaemonRequestExecutor | None = None
        self._periodic_executor: _DaemonRequestExecutor | None = None
        self._periodic_future: Future[Any] | None = None
        self._closed = False
        self._accepting = False
        self._futures: dict[Future[None], socket.socket] = {}

    @property
    def listening(self) -> bool:
        return self._listener is not None

    def start(self) -> None:
        if self._listener is not None:
            return
        if (
            self._identity is not None
            or self._pin_fd is not None
            or self._parent_fd is not None
            or self._leaf is not None
        ):
            raise DaemonUnavailable(
                "daemon socket cleanup is pending",
                code=LocalStateErrorCode.OPERATION_FAILED,
            )
        if self.stop_event.is_set() or self._closed:
            raise DaemonUnavailable("daemon socket server has already stopped")
        _ensure_unix_socket_supported()

        parent_fd: int | None = None
        listener: socket.socket | None = None
        try:
            resolved_group = resolve_socket_group(self.socket_group)
            if self.prepare_parent:
                if resolved_group is not None:
                    raise DaemonUnavailable(
                        "group sharing requires an explicit protected socket parent",
                        code=LocalStateErrorCode.INSECURE_SOCKET_PARENT,
                    )
                parent_fd, leaf, _result = prepare_resolved_private_parent(
                    self.socket_path
                )
            elif resolved_group is not None:
                parent_fd, leaf = open_resolved_parent(self.socket_path)
                validate_socket_group_parent_at(
                    parent_fd,
                    self.socket_group,
                )
            else:
                try:
                    parent_fd, leaf = open_resolved_parent(self.socket_path)
                except LocalStateError as exc:
                    if exc.code is not LocalStateErrorCode.MISSING_ENTRY:
                        raise
                    parent_fd, leaf = open_resolved_parent(
                        self.socket_path,
                        create_missing=True,
                    )
                validate_private_socket_parent_at(parent_fd)

            address = proc_fd_path(parent_fd, leaf)
            listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            identity: EntryIdentity | None = None
            pin_fd: int | None = None
            with _socket_startup_lock(parent_fd):
                try:
                    _cleanup_stale_socket(parent_fd, leaf, address)
                    with socket_bind_umask(self.socket_group):
                        listener.bind(address)
                    identity = owned_socket_identity_at(parent_fd, leaf)
                    if identity is None:
                        raise DaemonUnavailable("daemon socket could not be started")
                    pinned = pin_owned_socket_at(parent_fd, leaf)
                    if pinned is None:
                        raise DaemonUnavailable("daemon socket could not be started")
                    pin_fd, pinned_identity = pinned
                    if pinned_identity != identity:
                        raise DaemonUnavailable(
                            "daemon socket changed during startup",
                            code=LocalStateErrorCode.ENTRY_CHANGED,
                        )
                    enforce_bound_socket_permissions_at(
                        parent_fd,
                        leaf,
                        socket_group=self.socket_group,
                        expected=identity,
                    )
                    listener.listen(_SERVER_LISTEN_BACKLOG)
                    listener.settimeout(self.accept_timeout_seconds)
                    executor = _DaemonRequestExecutor(
                        max_workers=self.request_workers,
                        queue_capacity=self.max_in_flight_requests,
                        thread_name_prefix="tendwire-daemon-api",
                    )
                    periodic_executor = (
                        _DaemonRequestExecutor(
                            max_workers=1,
                            queue_capacity=1,
                            thread_name_prefix="tendwire-daemon-periodic",
                        )
                        if self.periodic_callback is not None
                        else None
                    )
                except Exception:
                    if "periodic_executor" in locals() and periodic_executor is not None:
                        periodic_executor.shutdown(cancel_futures=True)
                    if "executor" in locals():
                        executor.shutdown(cancel_futures=True)
                    self._rollback_bound_socket(
                        listener,
                        parent_fd,
                        leaf,
                        identity,
                        pin_fd,
                    )
                    raise
            self._listener = listener
            self._identity = identity
            self._pin_fd = pin_fd
            self._parent_fd = parent_fd
            self._leaf = leaf
            self._executor = executor
            self._periodic_executor = periodic_executor
            with self._tracking_lock:
                self._accepting = True
            listener = None
            parent_fd = None
        except LocalStateError as exc:
            raise _local_state_unavailable(exc) from None
        except OSError:
            raise DaemonUnavailable("daemon socket could not be started") from None
        finally:
            if listener is not None:
                listener.close()
            if parent_fd is not None and self._parent_fd != parent_fd:
                _close_fd(parent_fd)

    def _rollback_bound_socket(
        self,
        listener: socket.socket,
        parent_fd: int,
        leaf: str,
        identity: EntryIdentity | None,
        pin_fd: int | None,
    ) -> None:
        listener.close()
        if identity is None:
            _close_fd(pin_fd)
            return
        try:
            unlink_verified_socket_at(parent_fd, leaf, identity)
        except LocalStateError as exc:
            if exc.code not in {
                LocalStateErrorCode.MISSING_ENTRY,
                LocalStateErrorCode.WRONG_TYPE,
                LocalStateErrorCode.WRONG_OWNER,
                LocalStateErrorCode.ENTRY_CHANGED,
            }:
                self._identity = identity
                self._pin_fd = pin_fd
                self._parent_fd = parent_fd
                self._leaf = leaf
                return
        _close_fd(pin_fd)

    def serve_forever(self) -> None:
        self.start()
        try:
            while not self.stop_event.is_set():
                self._run_periodic_callback_if_due()
                listener = self._listener
                if listener is None:
                    break
                try:
                    conn, _addr = listener.accept()
                except TimeoutError:
                    continue
                except socket.timeout:
                    continue
                except OSError:
                    if self.stop_event.is_set():
                        break
                    raise DaemonUnavailable("daemon socket request loop failed") from None
                self._submit_connection(conn)
        finally:
            self.close()

    def _run_periodic_callback_if_due(self) -> None:
        callback = self.periodic_callback
        future = self._periodic_future
        if future is not None:
            if not future.done():
                return
            self._periodic_future = None
        if callback is None or time.monotonic() < self._next_periodic_at:
            return
        self._next_periodic_at = time.monotonic() + self.periodic_interval_seconds
        try:
            executor = self._periodic_executor
            if executor is not None:
                self._periodic_future = executor.submit(callback)
        except (RuntimeError, Full):
            # Periodic maintenance is best-effort and must not stop the API loop.
            return

    def _submit_connection(self, conn: socket.socket) -> None:
        if not self._admission.acquire(blocking=False):
            self._reject_connection(conn, "server_busy")
            return
        with self._tracking_lock:
            executor = self._executor
            if not self._accepting or executor is None:
                self._admission.release()
                self._reject_connection(conn, "daemon_stopping")
                return
            try:
                future = executor.submit(self._handle_connection, conn)
            except RuntimeError:
                self._admission.release()
                self._reject_connection(conn, "daemon_stopping")
                return
            self._futures[future] = conn
            future.add_done_callback(self._request_finished)

    def _request_finished(self, future: Future[None]) -> None:
        with self._tracking_lock:
            conn = self._futures.pop(future, None)
        if conn is not None:
            if future.cancelled() and self.stop_event.is_set():
                self._reject_connection(conn, "daemon_stopping")
            else:
                try:
                    conn.close()
                except OSError:
                    pass
            self._admission.release()

    def _reject_connection(self, conn: socket.socket, code: str) -> None:
        try:
            if code == "server_busy":
                response = error_response(
                    "server_busy",
                    "daemon request capacity is full",
                    details={"retryable": True},
                )
            else:
                response = error_response(
                    "daemon_stopping",
                    "daemon is stopping",
                    details={"retryable": True},
                )
            encoded = _serialized_response(response)
            conn.settimeout(0.01)
            conn.sendall(encoded + b"\n")
            try:
                conn.shutdown(socket.SHUT_WR)
            except OSError:
                pass
            for _chunk_index in range(4):
                try:
                    chunk = conn.recv(
                        4096,
                        getattr(socket, "MSG_DONTWAIT", 0),
                    )
                except (BlockingIOError, InterruptedError):
                    break
                except OSError:
                    break
                if not chunk or b"\n" in chunk:
                    break
        except (OSError, TimeoutError):
            pass
        finally:
            try:
                conn.close()
            except OSError:
                pass

    def _handle_connection(self, conn: socket.socket) -> None:
        with conn:
            read_deadline = time.monotonic() + self.client_timeout_seconds
            try:
                raw = _read_json_frame(
                    conn,
                    max_bytes=self.max_request_bytes,
                    deadline=read_deadline,
                )
                if not raw:
                    response = error_response("invalid_request", "empty request")
                else:
                    request = json.loads(raw.decode("utf-8"))
                    response = self.dispatcher(request)
            except json.JSONDecodeError:
                response = error_response(
                    "invalid_request",
                    "invalid request JSON",
                    details={"field": "request"},
                )
            except Exception as exc:  # noqa: BLE001
                response = error_response(
                    "internal_error",
                    "daemon request failed",
                    details={"type": type(exc).__name__},
                )
            if self.stop_event.is_set():
                return
            try:
                try:
                    encoded = _serialized_response(response)
                except (TypeError, ValueError, UnicodeError, RecursionError) as exc:
                    encoded = _serialized_response(error_response(
                        "internal_error", "daemon response serialization failed",
                        details={"type": type(exc).__name__},
                    ))
                if len(encoded) > self.max_response_bytes:
                    encoded = _serialized_response(
                        error_response(
                            "response_too_large",
                            "daemon response exceeds maximum frame size",
                            details={"max_response_bytes": self.max_response_bytes},
                        )
                    )
                if self.stop_event.is_set():
                    return
                conn.settimeout(self.client_timeout_seconds)
                conn.sendall(encoded + b"\n")
            except (OSError, TimeoutError):
                return

    def close(self) -> None:
        with self._lifecycle_lock:
            self._close_locked()

    def _close_locked(self) -> None:
        self.stop_event.set()
        with self._tracking_lock:
            self._accepting = False
            self._closed = True
            executor = self._executor
            self._executor = None
            periodic_executor = self._periodic_executor
            periodic_future = self._periodic_future
            self._periodic_executor = None
            self._periodic_future = None
            futures = tuple(self._futures) if executor is not None else ()
        listener = self._listener
        self._listener = None
        if listener is not None:
            listener.close()

        if executor is not None:
            for future in futures:
                future.cancel()
            with self._tracking_lock:
                running = tuple(self._futures)
            if running and self.shutdown_grace_seconds > 0:
                wait(running, timeout=self.shutdown_grace_seconds)
            with self._tracking_lock:
                connections = tuple(self._futures.values())
            for conn in connections:
                try:
                    conn.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                try:
                    conn.close()
                except OSError:
                    pass
            executor.shutdown(cancel_futures=True)
        if periodic_future is not None:
            periodic_future.cancel()
        if periodic_executor is not None:
            periodic_executor.shutdown(cancel_futures=True)

        identity = self._identity
        pin_fd = self._pin_fd
        parent_fd = self._parent_fd
        leaf = self._leaf
        if identity is not None:
            if parent_fd is None or leaf is None:
                raise DaemonUnavailable(
                    "daemon socket cleanup failed",
                    code=LocalStateErrorCode.OPERATION_FAILED,
                )
            try:
                unlink_verified_socket_at(parent_fd, leaf, identity)
            except LocalStateError as exc:
                if exc.code not in {
                    LocalStateErrorCode.MISSING_ENTRY,
                    LocalStateErrorCode.WRONG_TYPE,
                    LocalStateErrorCode.WRONG_OWNER,
                    LocalStateErrorCode.ENTRY_CHANGED,
                }:
                    raise DaemonUnavailable(
                        "daemon socket cleanup failed",
                        code=exc.code,
                    ) from None
        self._identity = None
        self._pin_fd = None
        self._parent_fd = None
        self._leaf = None
        _close_fd(pin_fd)
        _close_fd(parent_fd)


@contextmanager
def _validated_client_socket(
    path: Path,
    socket_group: str | None,
) -> Iterator[tuple[int, str, EntryIdentity, int, str]]:
    parent_fd: int | None = None
    pin_fd: int | None = None
    try:
        try:
            parent_fd, leaf = open_resolved_parent(path, path_only=True)
            pin_fd, identity, expected_peer_uid = pin_socket_for_client_at(
                parent_fd, leaf, socket_group
            )
            address = proc_fd_path(parent_fd, leaf)
        except LocalStateError as exc:
            raise _local_state_unavailable(exc) from None
        yield parent_fd, leaf, identity, expected_peer_uid, address
    finally:
        _close_fd(pin_fd)
        _close_fd(parent_fd)


def _recheck_connected_socket(
    parent_fd: int,
    leaf: str,
    expected: EntryIdentity,
    socket_group: str | None,
) -> None:
    recheck_fd: int | None = None
    try:
        try:
            recheck_fd, current, _owner_uid = pin_socket_for_client_at(
                parent_fd, leaf, socket_group
            )
        except LocalStateError as exc:
            raise _local_state_unavailable(exc) from None
        if current != expected:
            raise DaemonUnavailable(
                "daemon socket local state is invalid",
                code=LocalStateErrorCode.ENTRY_CHANGED,
            )
    finally:
        _close_fd(recheck_fd)


def _validate_connected_peer(conn: socket.socket, expected_uid: int) -> None:
    if not hasattr(socket, "SO_PEERCRED"):
        raise DaemonUnavailable(
            "daemon peer validation is unsupported",
            code=LocalStateErrorCode.UNSUPPORTED_PLATFORM,
        )
    credentials = struct.Struct("3i")
    try:
        raw = conn.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, credentials.size)
        _pid, peer_uid, _gid = credentials.unpack(raw)
    except (OSError, struct.error):
        raise DaemonUnavailable(
            "daemon peer validation failed",
            code=LocalStateErrorCode.PEER_VALIDATION_FAILED,
        ) from None
    if peer_uid != expected_uid:
        raise DaemonUnavailable(
            "daemon peer ownership is invalid",
            code=LocalStateErrorCode.WRONG_OWNER,
        )


class DaemonAPIClient:
    """Blocking one-request client for the local daemon JSON socket."""

    def __init__(
        self,
        socket_path: str | os.PathLike[str],
        *,
        timeout_seconds: float = 1.0,
        max_response_bytes: int = MAX_RESPONSE_BYTES,
        socket_group: str | None = None,
    ) -> None:
        self.socket_path = Path(socket_path)
        self.timeout_seconds = timeout_seconds
        self.max_response_bytes = max_response_bytes
        self.socket_group = socket_group

    def request(self, method: str, params: Mapping[str, Any] | None = None) -> dict[str, Any]:
        _ensure_unix_socket_supported()
        deadline = time.monotonic() + self.timeout_seconds
        payload = {"method": method, "params": dict(params or {})}
        raw_payload = json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8") + b"\n"
        with _validated_client_socket(
            self.socket_path,
            self.socket_group,
        ) as (parent_fd, leaf, identity, expected_peer_uid, address):
            conn = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            try:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise socket.timeout("daemon request deadline exceeded")
                conn.settimeout(remaining)
                conn.connect(address)
                _recheck_connected_socket(
                    parent_fd,
                    leaf,
                    identity,
                    self.socket_group,
                )
                _validate_connected_peer(conn, expected_peer_uid)
            except (TimeoutError, socket.timeout):
                conn.close()
                raise DaemonUnavailable(
                    "daemon socket request timed out",
                    timed_out=True,
                    request_started=False,
                ) from None
            except DaemonUnavailable:
                conn.close()
                raise
            except OSError:
                conn.close()
                raise DaemonUnavailable(
                    "daemon socket is unavailable",
                    request_started=False,
                ) from None
            request_started = False
            try:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise socket.timeout("daemon request deadline exceeded")
                conn.settimeout(remaining)
                request_started = True
                conn.sendall(raw_payload)
                raw_response = _read_json_frame(
                    conn,
                    max_bytes=self.max_response_bytes,
                    deadline=deadline,
                )
            except (TimeoutError, socket.timeout):
                raise DaemonUnavailable(
                    "daemon socket request timed out",
                    timed_out=True,
                    request_started=request_started,
                ) from None
            except DaemonProtocolError as exc:
                raise DaemonProtocolError(
                    str(exc),
                    request_started=request_started,
                ) from None
            except OSError:
                raise DaemonProtocolError(
                    "daemon request outcome is uncertain",
                    request_started=request_started,
                ) from None
            finally:
                conn.close()

        if not raw_response:
            raise DaemonProtocolError(
                "empty daemon response",
                request_started=True,
            )
        try:
            response = json.loads(raw_response.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            raise DaemonProtocolError(
                "invalid daemon response JSON",
                request_started=True,
            ) from None
        if not isinstance(response, dict):
            raise DaemonProtocolError(
                "daemon response must be a JSON object",
                request_started=True,
            )
        return response
