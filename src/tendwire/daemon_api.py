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
from .core.attention import attention_payload_from_snapshot
from .core.commands import CommandEnvelope, error_value
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
    pending_payload_from_snapshot,
    turns_payload_from_snapshot,
)
from .local_state import (
    EntryIdentity,
    LocalStateError,
    LocalStateErrorCode,
    PermissionState,
    enforce_bound_socket_permissions_at,
    inspect_owned_socket_at,
    open_resolved_parent,
    owned_socket_identity_at,
    pin_group_socket_for_client_at,
    pin_owned_socket_at,
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

REQUIRED_METHODS = frozenset(
    {
        "ping",
        "health.get",
        "snapshot.get",
        "attention.list",
        "turn.list",
        "turn.delta",
        "turn.content.get",
        "pending.list",
        "command.submit",
        "connector.prepare",
        "connector.poll",
        "connector.ack",
        "connector.fail",
        "connector.defer",
        "connector.renew",
        "connector.release",
        "connector.reclaim",
        "connector.retry",
        "connector.inspect",
    }
)


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
    parts = tuple(part for part in normalized.split("_") if part)
    for forbidden in _REQUEST_ID_FORBIDDEN_SEGMENTS:
        forbidden_parts = tuple(part for part in forbidden.split("_") if part)
        if not forbidden_parts:
            continue
        part_count = len(forbidden_parts)
        if any(
            parts[index : index + part_count] == forbidden_parts
            for index in range(len(parts) - part_count + 1)
        ):
            return True
    return False


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


def _command_result(value: Any) -> dict[str, Any]:
    if isinstance(value, CommandEnvelope):
        data: Any = value.to_dict()
    elif hasattr(value, "to_dict"):
        data = value.to_dict()
    else:
        data = value
    if not isinstance(data, dict):
        raise ValueError("command.submit returned a non-object result")
    return CommandEnvelope.from_dict(data).to_dict()


def _command_success_response(
    value: Any,
    *,
    request_id: Any = None,
) -> dict[str, Any]:
    command_result = _command_result(value)
    response = success_response({}, request_id=request_id)
    response["result"] = command_result
    return response


class TendwireDaemonAPI:
    """Dispatch stable local daemon methods through injected public helpers."""

    def __init__(
        self,
        *,
        get_snapshot: Callable[[], Snapshot],
        get_health: Callable[[], Mapping[str, Any]],
        submit_command: Callable[[Mapping[str, Any]], Mapping[str, Any] | CommandEnvelope],
        get_attention: Callable[[], Mapping[str, Any]] | None = None,
        get_turns: Callable[..., Mapping[str, Any]] | None = None,
        get_turn_delta: Callable[..., Mapping[str, Any]] | None = None,
        get_turn_content: Callable[[Mapping[str, Any]], Mapping[str, Any]] | None = None,
        get_pending: Callable[[], Mapping[str, Any]] | None = None,
        connector_call: Callable[[str, Mapping[str, Any]], Mapping[str, Any]] | None = None,
    ) -> None:
        self._get_snapshot = get_snapshot
        self._get_health = get_health
        self._submit_command = submit_command
        self._get_attention = get_attention
        self._get_turns = get_turns
        self._get_turn_delta = get_turn_delta
        self._get_turn_content = get_turn_content
        self._get_pending = get_pending
        self._connector_call = connector_call

    def dispatch(self, request: Any) -> dict[str, Any]:
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
        if method not in REQUIRED_METHODS:
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

        try:
            if method == "ping":
                return _daemon_identity_response(
                    {"pong": True, "methods": sorted(REQUIRED_METHODS)},
                    request_id=request_id,
                )
            if method == "health.get":
                return _daemon_identity_response(
                    self._get_health(),
                    request_id=request_id,
                )
            if method == "snapshot.get":
                return success_response(_snapshot_dict(self._get_snapshot()), request_id=request_id)
            if method == "attention.list":
                if self._get_attention is not None:
                    return success_response(self._get_attention(), request_id=request_id)
                return success_response(attention_payload_from_snapshot(self._get_snapshot()), request_id=request_id)
            if method == "turn.list":
                allowed_turn_params = {
                    "schema_version",
                    "limit",
                    "cursor",
                    "since",
                }
                unknown_params = sorted(
                    str(key) for key in params if str(key) not in allowed_turn_params
                )
                if unknown_params:
                    return error_response(
                        "invalid_params",
                        "turn.list contains unknown parameters",
                        details={"field_count": len(unknown_params)},
                        request_id=request_id,
                    )
                schema_version = params.get("schema_version", 1)
                if schema_version not in {1, 2} or isinstance(schema_version, bool):
                    return error_response(
                        "unsupported_schema",
                        "unsupported turn list schema version",
                        details={"supported_turn_schema_versions": [1, 2]},
                        request_id=request_id,
                    )
                limit = params.get("limit", TURN_LIST_DEFAULT_LIMIT)
                if (
                    not isinstance(limit, int)
                    or isinstance(limit, bool)
                    or not 1 <= limit <= TURN_LIST_MAX_LIMIT
                ):
                    return error_response(
                        "invalid_params",
                        "limit must be an integer in the supported range",
                        details={
                            "field": "limit",
                            "minimum": 1,
                            "maximum": TURN_LIST_MAX_LIMIT,
                        },
                        request_id=request_id,
                    )
                cursor = params.get("cursor")
                since = params.get("since")
                for token_name, token in (("cursor", cursor), ("since", since)):
                    if token is not None and (
                        not isinstance(token, str) or not token
                    ):
                        return error_response(
                            "invalid_params",
                            f"{token_name} must be a non-empty string or null",
                            details={"field": token_name},
                            request_id=request_id,
                        )
                if cursor is not None and since is not None:
                    return error_response(
                        "invalid_params",
                        "cursor and since cannot be combined",
                        details={"fields": ["cursor", "since"]},
                        request_id=request_id,
                    )
                if self._get_turns is not None:
                    turn_result = dict(
                        self._get_turns(
                            schema_version=schema_version,
                            limit=limit,
                            cursor=cursor,
                            since=since,
                        )
                    )
                else:
                    snapshot = self._get_snapshot()
                    if schema_version == 1:
                        try:
                            turn_result = turns_payload_from_snapshot(snapshot)
                        except ValueError as exc:
                            if str(exc) != "upgrade_required":
                                raise
                            turn_result = {
                                "schema_version": 1,
                                "ok": False,
                                "status": "upgrade_required",
                                "required_turn_schema_version": 2,
                            }
                    else:
                        turn_result = turns_payload_from_snapshot(snapshot, schema_version=2)
                response = success_response(turn_result, request_id=request_id)
                _restore_turn_list_text(response, turn_result)
                return response
            if method == "turn.delta":
                allowed_delta_params = {"watermark", "cursor", "limit"}
                unknown_delta = sorted(
                    str(key) for key in params if str(key) not in allowed_delta_params
                )
                if unknown_delta:
                    return error_response(
                        "invalid_params",
                        "turn.delta contains unknown parameters",
                        details={"field_count": len(unknown_delta)},
                        request_id=request_id,
                    )
                delta_limit = params.get("limit", TURN_DELTA_DEFAULT_LIMIT)
                if (
                    not isinstance(delta_limit, int)
                    or isinstance(delta_limit, bool)
                    or not 1 <= delta_limit <= TURN_DELTA_MAX_LIMIT
                ):
                    return error_response(
                        "invalid_params",
                        "limit must be an integer in the supported range",
                        details={
                            "field": "limit", "minimum": 1,
                            "maximum": TURN_DELTA_MAX_LIMIT,
                        },
                        request_id=request_id,
                    )
                delta_watermark = params.get("watermark")
                delta_cursor = params.get("cursor")
                for token_name, token in (
                    ("watermark", delta_watermark), ("cursor", delta_cursor)
                ):
                    if token is not None and (not isinstance(token, str) or not token):
                        return error_response(
                            "invalid_params",
                            f"{token_name} must be a non-empty string or null",
                            details={"field": token_name},
                            request_id=request_id,
                        )
                if delta_watermark is not None and delta_cursor is not None:
                    return error_response(
                        "invalid_params",
                        "watermark and cursor cannot be combined",
                        details={"fields": ["watermark", "cursor"]},
                        request_id=request_id,
                    )
                if self._get_turn_delta is None:
                    delta_result = {
                        "schema_version": 1,
                        "projection_schema_version": 2,
                        "ok": False,
                        "status": "store_unavailable",
                    }
                else:
                    delta_result = dict(self._get_turn_delta(
                        watermark=delta_watermark,
                        cursor=delta_cursor,
                        limit=delta_limit,
                    ))
                response = success_response(delta_result, request_id=request_id)
                _restore_turn_delta_text(response, delta_result)
                return response
            if method == "turn.content.get":
                allowed_content_params = {
                    "schema_version",
                    "turn_id",
                    "content_revision",
                    "field",
                    "cursor",
                }
                unknown_content_params = sorted(
                    str(key) for key in params if str(key) not in allowed_content_params
                )
                if unknown_content_params:
                    return error_response(
                        "invalid_params",
                        "turn.content.get contains unknown parameters",
                        details={"field_count": len(unknown_content_params)},
                        request_id=request_id,
                    )
                content_schema = params.get("schema_version", 1)
                if content_schema != 1 or isinstance(content_schema, bool):
                    return error_response(
                        "unsupported_schema",
                        "unsupported turn content schema version",
                        details={"supported_content_schema_versions": [1]},
                        request_id=request_id,
                    )
                if not isinstance(params.get("turn_id"), str) or not params.get("turn_id"):
                    return error_response(
                        "invalid_params",
                        "turn_id is required",
                        details={"field": "turn_id"},
                        request_id=request_id,
                    )
                if not isinstance(params.get("content_revision"), str) or not params.get(
                    "content_revision"
                ):
                    return error_response(
                        "invalid_params",
                        "content_revision is required",
                        details={"field": "content_revision"},
                        request_id=request_id,
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
                if self._get_turn_content is None:
                    return success_response(
                        {
                            "schema_version": 1,
                            "ok": False,
                            "status": "store_unavailable",
                            "error": {
                                "code": "store_unavailable",
                                "message": "content store is unavailable",
                            },
                        },
                        request_id=request_id,
                    )
                result = dict(self._get_turn_content(dict(params)))
                response = success_response(result, request_id=request_id)
                _restore_content_page_text(response, result)
                return response
            if method == "pending.list":
                if self._get_pending is not None:
                    return success_response(self._get_pending(), request_id=request_id)
                return success_response(pending_payload_from_snapshot(self._get_snapshot()), request_id=request_id)
            if method == "command.submit":
                return _command_success_response(
                    self._submit_command(dict(params)),
                    request_id=request_id,
                )
            if method.startswith("connector."):
                if self._connector_call is None:
                    return success_response(
                        {
                            "schema_version": 1,
                            "ok": False,
                            "status": "store_unavailable",
                            "error": {
                                "code": "store_unavailable",
                                "message": "store is unavailable",
                            },
                        },
                        request_id=request_id,
                    )
                connector_result = dict(self._connector_call(method, dict(params)))
                response = success_response(connector_result, request_id=request_id)
                if method.startswith("connector."):
                    _restore_plan_token(response, connector_result)
                return response
        except Exception as exc:  # noqa: BLE001
            return error_response(
                "internal_error",
                "daemon method failed",
                details={"type": type(exc).__name__},
                request_id=request_id,
            )

        return error_response(
            "unknown_method",
            "unknown method",
            request_id=request_id,
        )


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
        descriptors = (original.get("content") or {}).get("fields", {})
        for field in ("user_text", "assistant_final_text"):
            text = original.get(field)
            descriptor = descriptors.get(field) if isinstance(descriptors, Mapping) else None
            trusted_inline = original_result.get("schema_version") == 1 or (
                isinstance(descriptor, Mapping)
                and descriptor.get("availability") == "complete"
                and descriptor.get("inline") is True
            )
            if trusted_inline and isinstance(text, str):
                target[field] = text
        for preview_key in ("user_preview", "assistant_final_preview"):
            preview = original.get(preview_key)
            if isinstance(preview, str):
                target[preview_key] = preview


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
        if (
            isinstance(content_revision, str)
            and re.fullmatch(r"twrev1\.[A-Za-z0-9_-]+", content_revision)
            is not None
        ):
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
        wrapper = {"result": {"turns": [target_turn]}}
        _restore_turn_list_text(
            wrapper,
            {"schema_version": 2, "turns": [original_turn]},
        )


def _restore_plan_token(
    response: dict[str, Any],
    original_result: Mapping[str, Any],
) -> None:
    result = response.get("result")
    if not isinstance(result, dict):
        return

    def restore(target: dict[str, Any], original: Mapping[str, Any]) -> None:
        for key in (
            "plan_token",
            "failed_plan_token",
            "recovered_plan_token",
            "replaces_plan_token",
            "recovers_plan_token",
        ):
            token = original.get(key)
            if (
                isinstance(token, str)
                and re.fullmatch(r"twplan1\.[A-Za-z0-9_-]+", token) is not None
            ):
                target[key] = token
        final_identity = original.get("final_identity")
        if (
            isinstance(final_identity, str)
            and re.fullmatch(r"twfinal1\.[A-Za-z0-9_-]+", final_identity)
            is not None
        ):
            target["final_identity"] = final_identity
        content_revision = original.get("content_revision")
        if (
            isinstance(content_revision, str)
            and re.fullmatch(r"twrev1\.[A-Za-z0-9_-]+", content_revision)
            is not None
        ):
            target["content_revision"] = content_revision
        delivery_key = original.get("key")
        if (
            isinstance(delivery_key, str)
            and re.fullmatch(
                r"turn-final:revision:twfinal1\.[A-Za-z0-9_-]+",
                delivery_key,
            )
            is not None
        ):
            target["key"] = delivery_key
        for nested_key in ("turn", "final", "payload", "content"):
            nested_original = original.get(nested_key)
            nested_target = target.get(nested_key)
            if isinstance(nested_original, Mapping) and isinstance(nested_target, dict):
                restore(nested_target, nested_original)
        original_items = original.get("items")
        target_items = target.get("items")
        if isinstance(original_items, list) and isinstance(target_items, list):
            for target_item, original_item in zip(
                target_items,
                original_items,
                strict=False,
            ):
                if isinstance(target_item, dict) and isinstance(original_item, Mapping):
                    restore(target_item, original_item)

    restore(result, original_result)


def _serialized_response(response: Mapping[str, Any]) -> bytes:
    """Serialize one public response while preserving canonical content pages."""
    sanitized = sanitize_public_mapping(response)
    original_result = response.get("result")
    if isinstance(original_result, Mapping):
        # PID is normally excluded from public projections. The local daemon
        # identity is the one narrow exception: it lets startup diagnostics
        # identify the exact socket holder without exposing backend process
        # details. Require the current version/PID pair before restoring it.
        if (
            original_result.get("version") == __version__
            and original_result.get("pid") == os.getpid()
            and isinstance(sanitized.get("result"), dict)
        ):
            sanitized["result"]["version"] = __version__
            sanitized["result"]["pid"] = os.getpid()
        _restore_turn_list_text(sanitized, original_result)
        _restore_turn_delta_text(sanitized, original_result)
        _restore_content_page_text(sanitized, original_result)
        _restore_plan_token(sanitized, original_result)
        try:
            command_result = _command_result(dict(original_result))
        except (TypeError, ValueError):
            pass
        else:
            sanitized["result"] = command_result
    return json.dumps(
        sanitized,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
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
        try:
            os.close(pin_fd)
        except OSError:
            pass


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
        self._connections: set[socket.socket] = set()
        self._futures: set[Future[None]] = set()
        self._future_connections: dict[Future[None], socket.socket] = {}

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
                try:
                    os.close(parent_fd)
                except OSError:
                    pass

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
            if pin_fd is not None:
                try:
                    os.close(pin_fd)
                except OSError:
                    pass
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
        if pin_fd is not None:
            try:
                os.close(pin_fd)
            except OSError:
                pass

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
            self._connections.add(conn)
            try:
                future = executor.submit(self._handle_connection, conn)
            except RuntimeError:
                self._connections.discard(conn)
                self._admission.release()
                self._reject_connection(conn, "daemon_stopping")
                return
            self._futures.add(future)
            self._future_connections[future] = conn
            future.add_done_callback(self._request_finished)

    def _request_finished(self, future: Future[None]) -> None:
        with self._tracking_lock:
            conn = self._future_connections.pop(future, None)
            self._futures.discard(future)
            if conn is not None:
                self._connections.discard(conn)
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
                    response = dict(self.dispatcher(request))
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
                encoded = _serialized_response(response)
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
                connections = tuple(self._connections)
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
        if pin_fd is not None:
            try:
                os.close(pin_fd)
            except OSError:
                pass
        if parent_fd is not None:
            try:
                os.close(parent_fd)
            except OSError:
                pass


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
            if socket_group is not None:
                pin_fd, identity, expected_peer_uid = (
                    pin_group_socket_for_client_at(
                        parent_fd,
                        leaf,
                        socket_group,
                    )
                )
            else:
                pinned = pin_owned_socket_at(parent_fd, leaf)
                if pinned is None:
                    raise DaemonUnavailable(
                        "daemon socket local state is invalid",
                        code=LocalStateErrorCode.MISSING_ENTRY,
                    )
                pin_fd, identity = pinned
                inspected = inspect_owned_socket_at(parent_fd, leaf)
                current_identity = owned_socket_identity_at(parent_fd, leaf)
                if (
                    current_identity != identity
                    or inspected.state is PermissionState.ABSENT
                    or inspected.mode != 0o600
                ):
                    raise DaemonUnavailable(
                        "daemon socket local state is invalid",
                        code=(
                            LocalStateErrorCode.ENTRY_CHANGED
                            if current_identity != identity
                            else LocalStateErrorCode.INSECURE_MODE
                        ),
                    )
                expected_peer_uid = os.geteuid()
            address = proc_fd_path(parent_fd, leaf)
        except LocalStateError as exc:
            raise _local_state_unavailable(exc) from None
        yield parent_fd, leaf, identity, expected_peer_uid, address
    finally:
        if pin_fd is not None:
            try:
                os.close(pin_fd)
            except OSError:
                pass
        if parent_fd is not None:
            try:
                os.close(parent_fd)
            except OSError:
                pass


def _recheck_connected_socket(
    parent_fd: int,
    leaf: str,
    expected: EntryIdentity,
    socket_group: str | None,
) -> None:
    recheck_fd: int | None = None
    try:
        try:
            if socket_group is None:
                current = owned_socket_identity_at(parent_fd, leaf)
            else:
                recheck_fd, current, _owner_uid = pin_group_socket_for_client_at(
                    parent_fd,
                    leaf,
                    socket_group,
                )
        except LocalStateError as exc:
            raise _local_state_unavailable(exc) from None
        if current != expected:
            raise DaemonUnavailable(
                "daemon socket local state is invalid",
                code=LocalStateErrorCode.ENTRY_CHANGED,
            )
    finally:
        if recheck_fd is not None:
            try:
                os.close(recheck_fd)
            except OSError:
                pass


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
