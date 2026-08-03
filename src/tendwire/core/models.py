"""Neutral data models for Tendwire snapshots.

These models are intentionally device-neutral. They contain no Telegram,
Herdres delivery, chat/topic/message ID, or connector-specific routing state.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import threading
import unicodedata
from collections import OrderedDict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


SCHEMA_VERSION = 2
FINGERPRINT_HEX_CHARS = 24

CANONICAL_STATUSES = frozenset(
    {"unknown", "active", "idle", "waiting", "blocked", "warning", "done", "failed", "closed"}
)

_STATUS_ALIASES = {
    "": "unknown",
    "ok": "active",
    "okay": "active",
    "ready": "active",
    "running": "active",
    "run": "active",
    "online": "active",
    "connected": "active",
    "healthy": "active",
    "success": "done",
    "open": "active",
    "working": "active",
    "busy": "active",
    "processing": "active",
    "in-progress": "active",
    "in_progress": "active",
    "thinking": "active",
    "executing": "active",
    "responding": "waiting",
    "awaiting-input": "waiting",
    "awaiting_input": "waiting",
    "needs-input": "waiting",
    "needs_input": "waiting",
    "paused": "idle",
    "pause": "idle",
    "sleeping": "idle",
    "wait": "waiting",
    "pending": "waiting",
    "queued": "waiting",
    "queue": "waiting",
    "blocked": "blocked",
    "block": "blocked",
    "stalled": "blocked",
    "stuck": "blocked",
    "warn": "warning",
    "warning": "warning",
    "degraded": "warning",
    "error": "failed",
    "errors": "failed",
    "fail": "failed",
    "failure": "failed",
    "crashed": "failed",
    "crash": "failed",
    "panic": "failed",
    "closed": "closed",
    "complete": "done",
    "completed": "done",
    "done": "done",
    "stopped": "closed",
    "exited": "closed",
    "terminated": "closed",
}

_SEVERITY_ALIASES = {
    "": "info",
    "warn": "warning",
    "warning": "warning",
    "critical": "critical",
    "error": "critical",
    "failed": "critical",
    "failure": "critical",
    "info": "info",
    "notice": "info",
    "debug": "info",
}

FORBIDDEN_FIELD_NAMES = frozenset(
    {
        "telegram",
        "telegram_chat_id",
        "telegram_chat_ids",
        "telegram_id",
        "telegram_ids",
        "telegram_message_id",
        "telegram_message_ids",
        "telegram_thread_id",
        "telegram_thread_ids",
        "telegram_topic_id",
        "telegram_topic_ids",
        "chat_id",
        "chat_ids",
        "topic_id",
        "topic_ids",
        "message_id",
        "message_ids",
        "thread_id",
        "thread_ids",
        "token",
        "tokens",
        "bot_token",
        "bot_tokens",
        "auth",
        "auth_token",
        "auth_tokens",
        "authorization",
        "authorization_header",
        "authorization_headers",
        "bearer_token",
        "bearer_tokens",
        "cookie",
        "cookies",
        "credential",
        "credentials",
        "delivery",
        "delivery_id",
        "delivery_ids",
        "deliveries",
        "route",
        "route_id",
        "route_ids",
        "routes",
        "connector",
        "connector_id",
        "connector_ids",
        "connectors",
        "herdres_delivery",
        "backend_target",
        "backend_target_id",
        "backend_target_ids",
        "backend_targets",
        "terminal",
        "terminal_id",
        "terminal_ids",
        "terminals",
        "pane_id",
        "pane_ids",
        "tab_id",
        "tab_ids",
        "window_id",
        "window_ids",
        "tty",
        "pty",
        "pid",
        "pids",
        "process_id",
        "process_ids",
        "process",
        "tmux",
        "tmux_session",
        "tmux_session_id",
        "tmux_session_ids",
        "tmux_sessions",
        "tmux_window",
        "tmux_window_id",
        "tmux_window_ids",
        "tmux_windows",
        "tmux_pane",
        "tmux_pane_id",
        "tmux_pane_ids",
        "tmux_panes",
        "screen",
        "screen_session",
        "screen_session_id",
        "screen_session_ids",
        "screen_sessions",
        "screen_window",
        "screen_window_id",
        "screen_window_ids",
        "screen_windows",
        "agent_session",
        "agent_session_id",
        "agent_session_ids",
        "agent_sessions",
        "session",
        "session_id",
        "session_ids",
        "sessions",
        "herdr_state",
        "herdres_state",
        "target_kind",
        "target_value",
        "turn_target_kind",
        "turn_target_value",
        "private",
        "private_binding",
        "private_bindings",
        "private_fingerprint",
        "private_fingerprints",
        "argv",
        "args",
        "command",
        "command_arg",
        "command_args",
        "command_argv",
        "command_argvs",
        "command_line",
        "command_lines",
        "command_payload",
        "command_payloads",
        "command_text",
        "command_texts",
        "env",
        "environment",
        "raw_arg",
        "raw_args",
        "raw_argv",
        "raw_argvs",
        "raw_command",
        "raw_command_line",
        "raw_command_lines",
        "raw_payload",
        "raw_payloads",
        "raw_control",
        "raw_controls",
        "shell_command",
        "shell_commands",
        "terminal_control",
        "terminal_controls",
        "control_sequence",
        "control_sequences",
        "escape_sequence",
        "escape_sequences",
        "ansi_escape",
        "stdin",
        "stderr",
        "stdout",
        "shell",
        "secret",
        "secrets",
        "password",
        "passwords",
        "api_keys",
        "api_key",
        "tool_id",
        "tool_ids",
        "tool_use_id",
        "tool_use_ids",
        "tool_call_id",
        "tool_call_ids",
        "decision_id",
        "decision_ids",
        "pending_decision_id",
        "pending_decision_ids",
        "cwd",
        "workdir",
        "working_dir",
        "working_directory",
        "project_root",
        "repository_root",
        "repo_root",
        "path",
        "paths",
        "file_path",
        "file_paths",
        "filepath",
        "filepaths",
        "socket_path",
        "socket_paths",
        "url",
        "urls",
        "endpoint",
        "endpoints",
        "network_endpoint",
        "network_endpoints",
        "ip",
        "ip_address",
        "port",
        "headers",
        "output",
    }
)
_FORBIDDEN_FIELD_COMPACT = frozenset(name.replace("_", "") for name in FORBIDDEN_FIELD_NAMES)
_FORBIDDEN_BACKEND_NAME_TEXT = frozenset(
    {
        "private",
        "raw",
        "secret",
        "token",
    }
)
_PUBLIC_TEXT_ALLOWED_FIELD_WORDS = frozenset(
    {
        "connector",
        "connectors",
        "delivery",
        "deliveries",
        "herdres",
        "herdr",
        "outbox",
        "telegram",
        "terminal",
        "terminals",
    }
)
_PUBLIC_TEXT_ALLOWED_FIELD_WORDS_COMPACT = frozenset(
    word.replace("_", "") for word in _PUBLIC_TEXT_ALLOWED_FIELD_WORDS
)
_TEXT_FORBIDDEN_FIELD_NAMES = frozenset(
    name for name in FORBIDDEN_FIELD_NAMES if name not in _PUBLIC_TEXT_ALLOWED_FIELD_WORDS
)
_CAMEL_CASE_BOUNDARY_RE = re.compile(r"(?<=[a-z0-9])(?=[A-Z])")
_PUBLIC_PHRASE_MAX_COMPACT_CHARS = max(
    len(value.replace("_", ""))
    for value in (
        FORBIDDEN_FIELD_NAMES
        | _PUBLIC_TEXT_ALLOWED_FIELD_WORDS
        | _PUBLIC_TEXT_ALLOWED_FIELD_WORDS_COMPACT
    )
)
_BACKEND_MESSAGE_LABEL_RE = re.compile(
    r"[A-Za-z][A-Za-z0-9_-]*\s+[A-Za-z][A-Za-z0-9_-]*|[A-Za-z][A-Za-z0-9_.-]*"
)
_RAW_COMMAND_HEAD_RE = re.compile(
    r"^(?:sudo\s+)?(?:env\s+)?"
    r"(?:bash|sh|zsh|fish|cmd|powershell|pwsh|python\d*|node|npm|npx|git|gh|docker|"
    r"kubectl|make|pytest|herdr|tendwire|tmux|screen|curl|wget|ssh|scp|rsync|rm|cat|sed)(?:\s|$)",
    re.IGNORECASE,
)
_RAW_COMMAND_OPTION_RE = re.compile(r"\s--?[A-Za-z0-9][A-Za-z0-9_-]*")
_SHELL_META_RE = re.compile(r"[;&|`$<>]")
_PUBLIC_DROP = object()
_PUBLIC_ZERO_WIDTH_RE = re.compile("[\u200b-\u200f\u202a-\u202e\u2060-\u2064\ufeff]")
_PUBLIC_COMPOUND_PRIVATE_RE = re.compile(
    r"(?i)\b(?:pane[_ -]?id|terminal[_ -]?id|session[_ -]?id|"
    r"tool[_ -]?(?:use|call)[_ -]?id|pending[_ -]?decision[_ -]?id|"
    r"backend[_ -]?target|private[_ -]?fingerprint|chat[_ -]?id|topic[_ -]?id|"
    r"message[_ -]?id|thread[_ -]?id|socket[_ -]?path|working[_ -]?directory)"
    r"\s*(?:[:=]|\s)\s*(?:\"[^\"\n]*\"|'[^'\n]*'|[^\s,;]+)"
)
_PUBLIC_LABELLED_PRIVATE_RE = re.compile(
    r"(?i)\b(?:cwd|workdir|argv|environment|env|stdin|stdout|stderr|command|token|"
    r"secret|password|api[_ -]?key|authorization|credential)\s*[:=]\s*"
    r"(?:\"[^\"\n]*\"|'[^'\n]*'|[^\s,;]+)"
)
_PUBLIC_CREDENTIAL_URL_RE = re.compile(
    r"(?i)\b[a-z][a-z0-9+.-]*://[^\s/:@]+:[^\s/@]+@[^\s<>()]+"
)
_PUBLIC_SOCKET_URI_RE = re.compile(r"(?i)\b(?:unix|socket|file)://[^\s<>()]+")
_PUBLIC_PATH_RE = re.compile(
    r"(?<![\w/:])/(?:[\w.@+-]+)(?:/[\w.@+-]+)+"
    r"|(?<![\w/])/(?:etc|root|home|var|opt|srv|usr|tmp|run|proc|sys|mnt|media|boot|dev|private)\b"
    r"|(?<!\w)~/(?:[\w.@+-]+/)*[\w.@+-]+"
    r"|\b(?:home|Users|root)/(?:[\w.@+-]+/)+[\w.@+-]+"
    r"|\b[A-Za-z]:\\[^\s\"']+"
    r"|\\\\[^\s\"']+"
)
_PUBLIC_PRIVATE_ENDPOINT_RE = re.compile(
    r"\b(?:10(?:\.\d{1,3}){3}|127(?:\.\d{1,3}){3}|169\.254(?:\.\d{1,3}){2}|"
    r"192\.168(?:\.\d{1,3}){2}|172\.(?:1[6-9]|2\d|3[01])(?:\.\d{1,3}){2})"
    r"(?::\d{1,5})?\b"
    r"|(?i:\b(?:fc|fd)[0-9a-f:]*:[0-9a-f:]+\b|\bfe80:[0-9a-f:]+\b|(?<!:)::1\b)"
)
_PUBLIC_PROVIDER_CREDENTIAL_RE = re.compile(
    r"\bsk-[A-Za-z0-9_-]{6,}\b"
    r"|\bgh[oprsu]_[A-Za-z0-9]{6,}\b"
    r"|\bxox[baprs]-[A-Za-z0-9-]{6,}\b"
    r"|\bAKIA[0-9A-Z]{12,}\b"
    r"|\bAIza[0-9A-Za-z_-]{10,}\b"
    r"|\bglpat-[A-Za-z0-9_-]{8,}\b"
    r"|\bnpm_[A-Za-z0-9]{20,}\b"
    r"|\bpypi-[A-Za-z0-9_-]{20,}\b"
    r"|\b\d{6,}:[A-Za-z0-9_-]{20,}\b"
)
_PUBLIC_JWT_RE = re.compile(
    r"\beyJ[A-Za-z0-9_-]{4,}\.[A-Za-z0-9_-]{4,}\.[A-Za-z0-9_-]{4,}\b"
)
_PUBLIC_BEARER_RE = re.compile(r"(?i)\b(?:Bearer|Basic)\s+[A-Za-z0-9._~+/=-]{8,}")
_PUBLIC_ENV_ASSIGNMENT_RE = re.compile(
    r"(?<![A-Za-z0-9_])[A-Z][A-Z0-9_]{2,}=(?:\"[^\"\n]*\"|'[^'\n]*'|[^\s;&|]+)"
)
_PUBLIC_TELEGRAM_CHAT_ID_RE = re.compile(r"-100\d{10,}")

_PUBLIC_PRIVATE_IDENTIFIER_RE = re.compile(
    r"\b(?:toolu|tool_use|call|session|sess)_[A-Za-z0-9_-]{6,}\b"
    r"|\bw(?=[0-9a-z]*\d)[0-9a-z]+:[a-z][0-9a-z]*\b"
    r"|\bterm[_-][0-9a-z_-]{4,}\b"
    rf"|(?<!\d){_PUBLIC_TELEGRAM_CHAT_ID_RE.pattern}\b",
    re.IGNORECASE,
)
_PUBLIC_EMAIL_RE = re.compile(r"\b[\w.+-]+@[\w-]+\.[\w][\w.-]*\b")
_PUBLIC_SENSITIVE_TEXT_RES = (
    _PUBLIC_COMPOUND_PRIVATE_RE,
    _PUBLIC_LABELLED_PRIVATE_RE,
    _PUBLIC_CREDENTIAL_URL_RE,
    _PUBLIC_SOCKET_URI_RE,
    _PUBLIC_PATH_RE,
    _PUBLIC_PRIVATE_ENDPOINT_RE,
    _PUBLIC_PROVIDER_CREDENTIAL_RE,
    _PUBLIC_JWT_RE,
    _PUBLIC_BEARER_RE,
    _PUBLIC_ENV_ASSIGNMENT_RE,
    _PUBLIC_PRIVATE_IDENTIFIER_RE,
    _PUBLIC_EMAIL_RE,
)
_PUBLIC_SENSITIVE_TEXT_RE = re.compile(
    "|".join(
        (
            f"(?i:{pattern.pattern.removeprefix('(?i)')})"
            if pattern.flags & re.IGNORECASE
            else f"(?:{pattern.pattern})"
        )
        for pattern in _PUBLIC_SENSITIVE_TEXT_RES
    )
)
_PUBLIC_SENSITIVE_CROSSING_RE = re.compile(
    r"(?i)(?:"
    r"\b(?:pane[_ -]?id|terminal[_ -]?id|session[_ -]?id|"
    r"tool[_ -]?(?:use|call)[_ -]?id|pending[_ -]?decision[_ -]?id|"
    r"backend[_ -]?target|private[_ -]?fingerprint|chat[_ -]?id|topic[_ -]?id|"
    r"message[_ -]?id|thread[_ -]?id|socket[_ -]?path|working[_ -]?directory)"
    r"\s*(?:[:=]|\s)\s*(?:\"[^\"\n]*|'[^'\n]*'|[^\s,;]*)"
    r"|\b(?:cwd|workdir|argv|environment|env|stdin|stdout|stderr|command|token|"
    r"secret|password|api[_ -]?key|authorization|credential)\s*[:=]\s*"
    r"(?:\"[^\"\n]*|'[^'\n]*'|[^\s,;]*)"
    r"|\b[a-z][a-z0-9+.-]*://[^\s/:@]+:[^\s<>()]*"
    r"|\b(?:sk-|gh[oprsu]_|xox[baprs]-|AKIA|AIza|glpat-|npm_|pypi-)"
    r"[A-Za-z0-9_-]*"
    r"|(?<!\d)\d{6,}:[A-Za-z0-9_-]*"
    r"|\beyJ[A-Za-z0-9_.-]*"
    r"|\b(?:Bearer|Basic)\s+[A-Za-z0-9._~+/=-]*"
    r"|(?<![A-Za-z0-9_])[A-Z][A-Z0-9_]{2,}="
    r"(?:\"[^\"\n]*|'[^'\n]*'|[^\s;&|]*)"
    r")\Z"
)
_PUBLIC_ALLOWED_MAPPING_KEYS = frozenset(
    {
        "action_id",
        "active_tab_id",
        "attention_id",
        "choice_id",
        "content_fingerprint",
        "delivery_state",
        "fingerprint",
        "host_id",
        "id",
        "max_outbox_attempts",
        "origin_command_id",
        "outbox_ack_ttl_seconds",
        "outbox_claim_ttl_seconds",
        "outbox_max_claim_ttl_seconds",
        "output_excerpt_chars",
        "raw_status",
        "request_id",
        "row_id",
        "segment_id",
        "source_turn_id",
        "space_id",
        "submission_verdict",
        "submission_id",
        "transport_state",
        "turn_id",
        "worker_fingerprint",
        "worker_id",
    }
)
_PUBLIC_STRUCTURAL_MAPPING_KEY_SUFFIXES = (
    "_id",
    "_ids",
    "_fingerprint",
    "_fingerprints",
)
_PUBLIC_VALUE_TEXT_MAX_CHARS = 12000
_PUBLIC_SUBMISSION_VERDICTS = frozenset(
    {
        "submitted",
        "written_to_pty",
        "agent_not_ready",
        "agent_target_ambiguous",
        "agent_prompt_not_received",
        "agent_prompt_unsubmitted",
        "agent_input_pending",
        "agent_prompt_stalled",
        "steering_failed",
        "unknown",
    }
)
_PUBLIC_SANITIZE_CACHE_DEFAULT_SIZE = 2048
_PUBLIC_SANITIZER_CONFIG_VERSION = 1
_PUBLIC_FREE_TEXT_KEYS = frozenset(
    {
        "assistant_final_text",
        "assistant_stream_text",
        "description",
        "detail",
        "fields",
        "label",
        "message",
        "name",
        "prompt",
        "question",
        "raw_status",
        "reason",
        "status_line",
        "request_id",
        "summary",
        "title",
        "user_text",
    }
)
_PUBLIC_OPAQUE_ID_RE = re.compile(
    r"^(?:attn|choice|pending|space|turn|turnsrc|worker)-[0-9a-f]{24}$"
)

WORKER_BINDING_ACTIVE_EXPIRES_AT = "9999-12-31T23:59:59+00:00"


def _is_forbidden_field_name(key: Any) -> bool:
    normalized = str(key).lower().replace("-", "_").replace(".", "_")
    compact = normalized.replace("_", "")
    return normalized in FORBIDDEN_FIELD_NAMES or compact in _FORBIDDEN_FIELD_COMPACT


def _is_forbidden_backend_message_label(value: str) -> bool:
    separated = _CAMEL_CASE_BOUNDARY_RE.sub("_", value)
    normalized = "_".join(part for part in re.split(r"[\s_.-]+", separated.lower()) if part)
    compact = normalized.replace("_", "")
    return normalized in FORBIDDEN_FIELD_NAMES or compact in _FORBIDDEN_FIELD_COMPACT


def _is_forbidden_public_text_phrase(value: str) -> bool:
    # This helper sits in a token-window scan and sees arbitrary model output.
    # Normalize in one bounded pass so a hash/base64-dense token cannot cause
    # repeated full-size regex substitutions and temporary strings.
    normalized_chars: list[str] = []
    compact_chars: list[str] = []
    previous = ""
    separated = True
    for char in value:
        camel_boundary = previous in "abcdefghijklmnopqrstuvwxyz0123456789" and char in (
            "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        )
        is_separator = camel_boundary or char in "_.-" or char.isspace()
        if is_separator:
            if normalized_chars and not separated:
                normalized_chars.append("_")
            separated = True
            if camel_boundary:
                lowered = char.lower()
                compact_chars.append(lowered)
                normalized_chars.append(lowered)
                separated = False
            previous = char
            continue
        lowered = char.lower()
        compact_chars.append(lowered)
        if len(compact_chars) > _PUBLIC_PHRASE_MAX_COMPACT_CHARS:
            return False
        normalized_chars.append(lowered)
        separated = False
        previous = char
    if normalized_chars and normalized_chars[-1] == "_":
        normalized_chars.pop()
    normalized = "".join(normalized_chars)
    compact = "".join(compact_chars)
    if normalized in _PUBLIC_TEXT_ALLOWED_FIELD_WORDS or compact in _PUBLIC_TEXT_ALLOWED_FIELD_WORDS_COMPACT:
        return False
    return normalized in FORBIDDEN_FIELD_NAMES or compact in _FORBIDDEN_FIELD_COMPACT


def _looks_like_raw_command(value: str) -> bool:
    text = value.strip()
    if not text:
        return False
    if any(ord(char) < 32 or 0x80 <= ord(char) <= 0x9F for char in text):
        return True
    if _SHELL_META_RE.search(text):
        return True
    if _RAW_COMMAND_HEAD_RE.search(text):
        return True
    if _RAW_COMMAND_OPTION_RE.search(text):
        return True
    first = text.split(maxsplit=1)[0]
    return ("/" in first or first.endswith((".bat", ".cmd", ".exe", ".py", ".sh"))) and " " in text


def _public_tendwire_action_value(value: Any) -> str | None:
    clean = sanitize_forbidden_fields(value)
    if not isinstance(clean, str):
        return None
    text = clean.strip()
    if not text or _looks_like_raw_command(text) or _contains_forbidden_public_text(text):
        return None
    return text


_SNAPSHOT_CONTENT_IGNORED_KEYS = frozenset({"updated_at", "observed_at", "content_fingerprint"})

BACKEND_HEALTH_STATUSES = frozenset({"healthy", "degraded", "unavailable", "unknown"})
BACKEND_HEALTH_OUTCOMES = frozenset(
    {
        "healthy_non_empty",
        "empty_healthy",
        "missing_binary",
        "launch_error",
        "timeout",
        "deadline_exhausted",
        "nonzero",
        "malformed_json",
        "protocol_error",
        "socket_disconnected",
        "worker_cap_exceeded",
        "continuity_unavailable",
        "unknown",
    }
)
BACKEND_HEALTH_COUNT_KEYS = frozenset({"spaces", "workers"})


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_timestamp(dt: datetime | None = None) -> str:
    """Return an ISO-8601 UTC timestamp string."""
    if dt is None:
        dt = _utc_now()
    return dt.astimezone(timezone.utc).isoformat()


def stable_json_dumps(value: Any, *, indent: int | None = None) -> str:
    """Serialize JSON deterministically for hashing and snapshot output."""
    return json.dumps(
        sanitize_forbidden_fields(value),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        indent=indent,
    )


def stable_sha256(value: Any) -> str:
    """Return the SHA-256 hex digest of Tendwire's stable JSON encoding."""
    return hashlib.sha256(stable_json_dumps(value).encode("utf-8")).hexdigest()


def private_stable_sha256(value: Any) -> str:
    """Return a deterministic digest for private, non-public identity material."""
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def worker_binding_private_fingerprint(
    *,
    host_id: str,
    backend: str,
    identity_material: Any,
    length: int = FINGERPRINT_HEX_CHARS,
) -> str:
    """Return a host/backend-scoped private binding identity fingerprint.

    Unlike public snapshot fingerprints, this deliberately does not sanitize
    backend identity material before hashing. Callers must not serialize the
    returned value into public snapshot or command payloads.
    """
    return private_stable_sha256(
        {
            "host_id": str(host_id),
            "backend": str(backend),
            "identity": identity_material,
        }
    )[:length]


def stable_fingerprint(value: Any, *, length: int = FINGERPRINT_HEX_CHARS) -> str:
    """Return a fixed-width stable fingerprint for Tendwire content."""
    return stable_sha256(value)[:length]


def normalize_status(status: Any) -> str:
    """Map arbitrary adapter status values into Tendwire's canonical set."""
    raw = "" if status is None else str(status).strip().lower().replace("_", "-")
    if raw in CANONICAL_STATUSES:
        return raw
    return _STATUS_ALIASES.get(raw, "unknown")


def normalize_severity(severity: Any) -> str:
    """Normalize historical attention levels into a compact severity string."""
    raw = "" if severity is None else str(severity).strip().lower().replace("_", "-")
    return _SEVERITY_ALIASES.get(raw, raw or "info")


def sanitize_forbidden_fields(value: Any) -> Any:
    """Return a JSON-safe value with connector/routing field names removed."""
    if isinstance(value, datetime):
        return utc_timestamp(value)
    if isinstance(value, Mapping):
        sanitized: dict[str, Any] = {}
        for key, item in value.items():
            if _is_forbidden_field_name(key):
                continue
            key_text = str(key)
            sanitized[key_text] = sanitize_forbidden_fields(item)
        return sanitized
    if isinstance(value, tuple | list):
        return [sanitize_forbidden_fields(item) for item in value]
    if isinstance(value, set | frozenset):
        items = [sanitize_forbidden_fields(item) for item in value]
        return sorted(items, key=stable_json_dumps)
    if value is None or isinstance(value, str | int | float | bool):
        return value
    return str(value)


def _public_text_tokens(value: str) -> list[str]:
    separated = _CAMEL_CASE_BOUNDARY_RE.sub(" ", value)
    return [
        part.lower()
        for part in re.split(r"[^A-Za-z0-9]+", separated)
        if part
    ]


def _contains_forbidden_public_text(value: str) -> bool:
    tokens = _public_text_tokens(value)
    if not tokens:
        return False
    if set(tokens) & (_FORBIDDEN_BACKEND_NAME_TEXT - {"raw"}):
        return True
    for index in range(len(tokens)):
        for size in range(1, min(4, len(tokens) - index) + 1):
            phrase = "_".join(tokens[index : index + size])
            if phrase == "command":
                continue
            if _is_forbidden_public_text_phrase(phrase):
                return True
    return bool(set(tokens) & (_TEXT_FORBIDDEN_FIELD_NAMES - {"command"}))


def _is_public_structural_mapping_key(value: str) -> bool:
    return value in {"id", "ids", "fingerprint", "fingerprints"} or value.endswith(
        _PUBLIC_STRUCTURAL_MAPPING_KEY_SUFFIXES
    )


def _is_forbidden_public_mapping_key(value: str) -> bool:
    key_text = str(value)
    tokens = _public_text_tokens(value)
    if not tokens:
        return False
    normalized = "_".join(tokens)
    if key_text == normalized and normalized in _PUBLIC_ALLOWED_MAPPING_KEYS:
        return False
    if _is_public_structural_mapping_key(normalized):
        return True
    if _contains_forbidden_public_text(value):
        return True
    sensitive_key_tokens = _FORBIDDEN_BACKEND_NAME_TEXT | {
        "connector",
        "delivery",
        "herdres",
        "outbox",
        "private",
        "raw",
        "route",
        "telegram",
        "provider",
        "transport",
    }
    return bool(set(tokens) & sensitive_key_tokens)


def _public_safe_text(value: Any, *, default: str = "") -> str:
    raw = _string_value(value).strip()
    if not raw or _contains_forbidden_public_text(raw):
        return default
    text = sanitize_public_text(raw)
    if not text or _contains_forbidden_public_text(text):
        return default
    return text


def _public_safe_identity(value: Any, *, prefix: str, default: str = "unknown") -> str:
    raw = _string_value(value, default).strip() or default
    text = sanitize_public_text(raw)
    if text == raw and not _contains_forbidden_public_text(text):
        return " ".join(text.split())
    return f"{prefix}-{stable_fingerprint({'type': prefix, 'raw_id': raw})}"


def _public_safe_fingerprint(value: Any) -> str:
    raw = _string_value(value).strip()
    text = sanitize_public_text(raw)
    if not text or text != raw or _contains_forbidden_public_text(text):
        return ""
    return " ".join(text.split())


def _optional_public_safe_identity(value: Any, *, prefix: str) -> str | None:
    if value is None:
        return None
    return _public_safe_identity(value, prefix=prefix)


def _optional_public_safe_text(value: Any) -> str | None:
    if value is None:
        return None
    text = _public_safe_text(value)
    return text or None


def _contains_connector_private_text(value: str) -> bool:
    return bool(
        set(_public_text_tokens(value))
        & {"backend", "connector", "delivery", "herdr", "herdres", "provider", "telegram", "transport"}
    )


def _redact_and_truncate_public_text(text: str, max_chars: int | None) -> str:
    """Redact a bounded prefix plus any sensitive value crossing its boundary."""
    if max_chars is None:
        return _PUBLIC_SENSITIVE_TEXT_RE.sub("[redacted]", text)

    limit = max(0, max_chars)
    if not text or limit == 0:
        return ""
    if len(text) <= limit:
        return _PUBLIC_SENSITIVE_TEXT_RE.sub("[redacted]", text)

    marker = "\n[truncated]"
    if limit <= len(marker):
        return marker[:limit]
    prefix_limit = limit - len(marker)
    prefix = text[:prefix_limit]
    crossing = _PUBLIC_SENSITIVE_CROSSING_RE.search(prefix)
    if crossing is None:
        redacted = _PUBLIC_SENSITIVE_TEXT_RE.sub("[redacted]", prefix)
    else:
        safe_head = _PUBLIC_SENSITIVE_TEXT_RE.sub(
            "[redacted]",
            prefix[: crossing.start()],
        )
        redacted = safe_head + "[redacted]"
    return redacted[:prefix_limit].rstrip() + marker


def _public_sanitize_cache_size() -> int:
    raw = os.environ.get(
        "TENDWIRE_SANITIZE_CACHE_SIZE",
        str(_PUBLIC_SANITIZE_CACHE_DEFAULT_SIZE),
    )
    try:
        return max(0, min(65536, int(raw)))
    except ValueError:
        return _PUBLIC_SANITIZE_CACHE_DEFAULT_SIZE


_PUBLIC_SANITIZE_CACHE_SIZE = _public_sanitize_cache_size()
_PUBLIC_SANITIZE_CACHE: OrderedDict[tuple[Any, ...], str] = OrderedDict()
_PUBLIC_SANITIZE_CACHE_LOCK = threading.RLock()


def _public_sanitize_cache_key(
    value: str,
    *,
    max_chars: int | None,
    collapse_whitespace: bool,
    strip_outer: bool,
) -> tuple[Any, ...]:
    digest = hashlib.blake2b(
        value.encode("utf-8", errors="surrogatepass"),
        digest_size=16,
    ).digest()
    return (
        _PUBLIC_SANITIZER_CONFIG_VERSION,
        max_chars,
        collapse_whitespace,
        strip_outer,
        len(value),
        digest,
    )


def _clear_public_sanitize_cache() -> None:
    """Clear the process-local text sanitizer cache (primarily for tests)."""
    with _PUBLIC_SANITIZE_CACHE_LOCK:
        _PUBLIC_SANITIZE_CACHE.clear()


def sanitize_public_text(
    value: Any,
    *,
    max_chars: int | None = None,
    collapse_whitespace: bool = False,
    strip_outer: bool = True,
) -> str:
    """Redact recognizable private values while preserving ordinary public prose.

    This deliberately does not claim to detect arbitrary shapeless secrets copied
    into free-form model text. Known private source shapes, labelled private data,
    provider credentials, endpoints, and generated metadata are blocked here;
    tool adapters must still construct progress from allowlisted fields.

    Redaction semantically precedes truncation, including when an arbitrarily long
    credential crosses the visible boundary. ``strip_outer=False`` is reserved for
    already-typed canonical text whose remaining code points must be lossless.
    """
    if not isinstance(value, str):
        return ""
    cache_key: tuple[Any, ...] | None = None
    if _PUBLIC_SANITIZE_CACHE_SIZE:
        cache_key = _public_sanitize_cache_key(
            value,
            max_chars=max_chars,
            collapse_whitespace=collapse_whitespace,
            strip_outer=strip_outer,
        )
        with _PUBLIC_SANITIZE_CACHE_LOCK:
            cached = _PUBLIC_SANITIZE_CACHE.get(cache_key)
            if cached is not None:
                _PUBLIC_SANITIZE_CACHE.move_to_end(cache_key)
                return cached
    text = unicodedata.normalize("NFKC", value).replace("\x00", "")
    text = _PUBLIC_ZERO_WIDTH_RE.sub("", text)
    text = _redact_and_truncate_public_text(text, max_chars)
    if collapse_whitespace:
        text = " ".join(text.split())
    elif strip_outer:
        text = text.strip()
    if cache_key is not None:
        with _PUBLIC_SANITIZE_CACHE_LOCK:
            _PUBLIC_SANITIZE_CACHE[cache_key] = text
            _PUBLIC_SANITIZE_CACHE.move_to_end(cache_key)
            while len(_PUBLIC_SANITIZE_CACHE) > _PUBLIC_SANITIZE_CACHE_SIZE:
                _PUBLIC_SANITIZE_CACHE.popitem(last=False)
    return text


def sanitize_canonical_turn_text(value: object) -> str | None:
    """Return lossless public-safe canonical turn text.

    Canonical turn content uses the same NFKC, forbidden-code-point removal,
    and recognizable-private-value redaction as other public text, but it is
    never truncated and its remaining leading/trailing whitespace is retained.
    Non-string values are absent rather than being stringified.
    """
    if not isinstance(value, str):
        return None
    return sanitize_public_text(value, max_chars=None, strip_outer=False)


def sanitize_public_value(
    value: Any,
    *,
    backend_neutral: bool = False,
    _field: str = "",
    _nested: bool = False,
) -> Any:
    """Return one recursively sanitized JSON-safe public value.

    Mapping keys are untrusted text and are retained only when their original
    spelling is already public-safe. Recognizable numeric Telegram chat IDs are
    private. Ordinary numeric topic/message IDs are ambiguous and require key
    provenance at the adapter boundary.
    """
    normalized_field = str(_field).strip().lower().replace("-", "_")
    if normalized_field == "composer_state":
        return _PUBLIC_DROP if _nested else None
    if normalized_field == "submission_verdict":
        if not isinstance(value, str):
            return _PUBLIC_DROP if _nested else None
        verdict = sanitize_public_text(
            value,
            max_chars=_PUBLIC_VALUE_TEXT_MAX_CHARS,
        )
        if verdict not in _PUBLIC_SUBMISSION_VERDICTS:
            return _PUBLIC_DROP if _nested else None
        return verdict
    if isinstance(value, datetime):
        return utc_timestamp(value)
    if isinstance(value, Mapping):
        result: dict[str, Any] = {}
        for key, item in value.items():
            key_text = str(key)
            if sanitize_public_text(
                key_text,
                max_chars=_PUBLIC_VALUE_TEXT_MAX_CHARS,
            ) != key_text:
                continue
            structured_outbox = key_text == "outbox" and isinstance(item, Mapping)
            if not structured_outbox and (
                _is_forbidden_field_name(key_text)
                or _is_forbidden_public_mapping_key(key_text)
            ):
                continue
            sanitized = sanitize_public_value(
                item,
                backend_neutral=backend_neutral,
                _field=key_text,
                _nested=True,
            )
            if sanitized is not _PUBLIC_DROP:
                result[key_text] = sanitized
        return result
    if isinstance(value, tuple | list):
        result_list: list[Any] = []
        for item in value:
            sanitized = sanitize_public_value(
                item,
                backend_neutral=backend_neutral,
                _field=_field,
                _nested=True,
            )
            if sanitized is not _PUBLIC_DROP:
                result_list.append(sanitized)
        return result_list
    if isinstance(value, set | frozenset):
        result_set = [
            sanitize_public_value(
                item,
                backend_neutral=backend_neutral,
                _field=_field,
                _nested=True,
            )
            for item in value
        ]
        return sorted(
            (item for item in result_set if item is not _PUBLIC_DROP),
            key=stable_json_dumps,
        )
    if isinstance(value, str):
        text = sanitize_public_text(value, max_chars=_PUBLIC_VALUE_TEXT_MAX_CHARS)
        field_text = str(_field)
        normalized_field = field_text.strip().lower().replace("-", "_")
        if backend_neutral and (
            "[redacted]" in text
            or _contains_connector_private_text(value)
            or _contains_connector_private_text(text)
        ):
            return _PUBLIC_DROP if _nested else None
        if (
            field_text in _PUBLIC_ALLOWED_MAPPING_KEYS
            and _is_public_structural_mapping_key(field_text)
        ):
            if "[redacted]" in text or _looks_like_raw_command(text):
                return _PUBLIC_DROP if _nested else None
            return text
        if normalized_field in _PUBLIC_FREE_TEXT_KEYS:
            return text
        if "[redacted]" in text:
            return _PUBLIC_DROP if _nested else None
        if _PUBLIC_OPAQUE_ID_RE.fullmatch(text):
            return text
        if _contains_forbidden_public_text(text) or _looks_like_raw_command(text):
            return _PUBLIC_DROP if _nested else None
        return text
    if (
        isinstance(value, int)
        and not isinstance(value, bool)
        and _PUBLIC_TELEGRAM_CHAT_ID_RE.fullmatch(str(value))
    ):
        return _PUBLIC_DROP if _nested else None
    if value is None or isinstance(value, int | float | bool):
        return value
    return _PUBLIC_DROP if _nested else None


def sanitize_public_mapping(value: Any, *, backend_neutral: bool = False) -> dict[str, Any]:
    clean = sanitize_public_value(
        value if isinstance(value, Mapping) else {},
        backend_neutral=backend_neutral,
    )
    return clean if isinstance(clean, dict) else {}

def public_json_dumps(value: Any, *, indent: int | None = None) -> str:
    """Serialize a final public value after recursive value sanitization."""
    return json.dumps(
        sanitize_public_value(value),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        indent=indent,
    )


def _string_value(value: Any, default: str = "") -> str:
    if value is None:
        return default
    text = str(value)
    return text if text else default


def _optional_string(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)


def _optional_timestamp(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return utc_timestamp(value)
    return str(value)


def _public_safe_backend_name(value: Any) -> str:
    text = _string_value(value, "unknown").strip().lower()
    clean = "".join(char for char in text if char.isalnum() or char in {"_", "-"})
    if clean == "herdr" or clean.startswith(("herdr_", "herdr-")):
        return clean[:40]
    compact = clean.replace("_", "").replace("-", "")
    if _is_forbidden_public_text_phrase(clean) or any(
        marker in clean or marker.replace("_", "") in compact
        for marker in _FORBIDDEN_BACKEND_NAME_TEXT
    ):
        return "unknown"
    return clean[:40] or "unknown"


def _public_safe_backend_message(value: Any) -> str:
    text = _string_value(value)
    if not text:
        return ""
    collapsed = " ".join(text.split())
    for match in _BACKEND_MESSAGE_LABEL_RE.finditer(collapsed):
        if _is_forbidden_backend_message_label(match.group(0)):
            return "Backend health details redacted"
    token_text = "".join(
        char.lower() if char.isalnum() or char == "_" else " "
        for char in collapsed
    )
    tokens = set(token_text.split())
    sensitive_markers = {
        "argv",
        "env",
        "environment",
        "password",
        "secret",
        "secrets",
        "stderr",
        "stdout",
        "token",
    }
    if tokens & sensitive_markers:
        return "Backend health details redacted"
    if tokens & (_TEXT_FORBIDDEN_FIELD_NAMES - {"command"}):
        return "Backend health details redacted"
    if len(collapsed) > 160:
        return collapsed[:157].rstrip() + "..."
    return collapsed


def _backend_health_status(value: Any) -> str:
    status = _string_value(value, "unknown").strip().lower().replace("-", "_")
    return status if status in BACKEND_HEALTH_STATUSES else "unknown"


def _backend_health_outcome(value: Any) -> str:
    outcome = _string_value(value, "unknown").strip().lower().replace("-", "_")
    return outcome if outcome in BACKEND_HEALTH_OUTCOMES else "unknown"


def _backend_health_counts(value: Any) -> dict[str, int]:
    if not isinstance(value, Mapping):
        return {}
    counts: dict[str, int] = {}
    for key, item in value.items():
        key_text = str(key).lower().strip().replace("-", "_")
        if key_text not in BACKEND_HEALTH_COUNT_KEYS:
            continue
        if isinstance(item, bool):
            continue
        if isinstance(item, int):
            count = item
        elif isinstance(item, str) and item.isdigit():
            count = int(item)
        else:
            continue
        if count < 0:
            continue
        counts[key_text] = count
    return counts


def _is_turn_api_volatile_key(key: object) -> bool:
    normalized = str(key).strip().lower().replace("-", "_").replace(".", "_")
    return (
        normalized
        in (
            "turn",
            "turn_epoch",
            "last_completed_turn",
            "outcome",
            "state_change_seq",
        )
        or normalized.startswith("turn_")
    )


def _strip_turn_api_volatile(value: Any) -> Any:
    """Drop Herdr turn-API counters from identity-bearing metadata.

    These values advance as panes complete turns or change state. Hashing them
    into worker or space fingerprints re-keys identity per observation, so the
    exclusion belongs at the shared model choke point.
    """
    if isinstance(value, Mapping):
        return {
            key: _strip_turn_api_volatile(item)
            for key, item in value.items()
            if not _is_turn_api_volatile_key(key)
        }
    if isinstance(value, list):
        return [_strip_turn_api_volatile(item) for item in value]
    return value


def _status_and_meta(status: Any, meta: Any) -> tuple[str, dict[str, Any]]:
    raw_status = _string_value(status, "unknown").strip()
    normalized = normalize_status(raw_status)
    clean_meta = _strip_turn_api_volatile(sanitize_public_mapping(meta))
    if raw_status and raw_status.lower().replace("_", "-") != normalized:
        public_raw_status = _public_safe_text(raw_status)
        if public_raw_status:
            clean_meta["raw_status"] = public_raw_status
    return normalized, clean_meta


def _merge_meta(data: Mapping[str, Any], known_keys: set[str]) -> dict[str, Any]:
    explicit_meta = data.get("meta", {})
    merged: dict[str, Any] = {
        str(key): value for key, value in data.items() if str(key) not in known_keys
    }
    if isinstance(explicit_meta, Mapping):
        merged.update(explicit_meta)
    return sanitize_public_mapping(merged)


def _strip_snapshot_content_volatile(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {
            str(key): _strip_snapshot_content_volatile(item)
            for key, item in value.items()
            if str(key).lower() not in _SNAPSHOT_CONTENT_IGNORED_KEYS
        }
    if isinstance(value, list | tuple):
        return [_strip_snapshot_content_volatile(item) for item in value]
    return value


def attention_identity_payload(
    *,
    host_id: str,
    source: str,
    kind: str,
    severity: str,
    reason: str,
    status: str,
) -> dict[str, str]:
    """Return the stable identity payload for an attention condition."""
    return {
        "host_id": str(host_id),
        "source": str(source),
        "kind": str(kind),
        "severity": normalize_severity(severity),
        "reason": str(reason),
        "status": normalize_status(status),
    }


def attention_fingerprint(
    *,
    host_id: str,
    source: str,
    kind: str,
    severity: str,
    reason: str,
    status: str,
) -> str:
    """Return the deterministic fingerprint for an attention condition."""
    return stable_fingerprint(
        attention_identity_payload(
            host_id=host_id,
            source=source,
            kind=kind,
            severity=severity,
            reason=reason,
            status=status,
        )
    )


def attention_id(
    *,
    host_id: str,
    source: str,
    kind: str,
    severity: str,
    reason: str,
    status: str,
) -> str:
    """Return the deterministic public ID for an attention condition."""
    return f"attn-{attention_fingerprint(host_id=host_id, source=source, kind=kind, severity=severity, reason=reason, status=status)}"


@dataclass(frozen=True, init=False)
class SuggestedAction:
    """A neutral action suggestion with no connector delivery state."""

    action_id: str = ""
    label: str = ""
    tendwire_action: str = ""
    params: dict[str, Any] = field(default_factory=dict)

    def __init__(
        self,
        action_id: str = "",
        label: str = "",
        tendwire_action: str = "",
        params: Mapping[str, Any] | None = None,
        *,
        command: str | None = None,
    ) -> None:
        label = _public_safe_text(label, default="Action")
        tendwire_action = _string_value(tendwire_action)
        command_alias = _optional_string(command)
        public_tendwire_action = _public_tendwire_action_value(tendwire_action)
        explicit_tendwire_action = public_tendwire_action is not None
        params = sanitize_public_mapping(params)
        action_id = _public_safe_text(action_id) or stable_fingerprint(
            {"label": label, "tendwire_action": public_tendwire_action or "", "params": params}
        )
        object.__setattr__(self, "action_id", action_id)
        object.__setattr__(self, "label", label)
        object.__setattr__(self, "tendwire_action", tendwire_action)
        object.__setattr__(self, "params", params)
        object.__setattr__(self, "_command", command_alias)
        object.__setattr__(self, "_explicit_tendwire_action", explicit_tendwire_action)

    @property
    def command(self) -> str:
        """Backward-compatible in-process alias; not serialized."""
        return getattr(self, "_command", None) or self.tendwire_action

    @property
    def has_public_tendwire_action(self) -> bool:
        """Whether tendwire_action came from the explicit public field."""
        return bool(getattr(self, "_explicit_tendwire_action", False))

    def to_dict(self) -> dict[str, Any]:
        payload = {
            "action_id": self.action_id,
            "label": self.label,
            "params": sanitize_public_mapping(self.params),
        }
        public_tendwire_action = _public_tendwire_action_value(self.tendwire_action)
        if self.has_public_tendwire_action and public_tendwire_action is not None:
            payload["tendwire_action"] = public_tendwire_action
        return sanitize_public_mapping(payload)

    @classmethod
    def from_dict(cls, data: "SuggestedAction | Mapping[str, Any]") -> "SuggestedAction":
        if isinstance(data, SuggestedAction):
            return data
        command = data.get("command") if isinstance(data, Mapping) else None
        clean = sanitize_public_mapping(data if isinstance(data, Mapping) else {})
        return cls(
            action_id=_string_value(clean.get("action_id")),
            label=_string_value(clean.get("label")),
            tendwire_action=_string_value(clean.get("tendwire_action")),
            params=clean.get("params", {}),
            command=_optional_string(command),
        )


@dataclass(frozen=True)
class Space:
    """A neutral space observation (e.g. a Herdr space / project context)."""

    id: str
    name: str
    status: str = "unknown"
    meta: dict[str, Any] = field(default_factory=dict)
    updated_at: str | None = None
    status_line: str | None = None
    fingerprint: str = ""

    def __post_init__(self) -> None:
        space_id = _public_safe_identity(self.id, prefix="space")
        name = _public_safe_text(self.name, default=space_id)
        status, meta = _status_and_meta(self.status, self.meta)
        updated_at = _optional_timestamp(self.updated_at)
        status_line = _optional_public_safe_text(self.status_line)
        fingerprint = _public_safe_fingerprint(self.fingerprint) or stable_fingerprint(
            {
                "type": "space",
                "id": space_id,
                "name": name,
                "status": status,
                "status_line": status_line,
                "meta": meta,
            }
        )

        object.__setattr__(self, "id", space_id)
        object.__setattr__(self, "name", name)
        object.__setattr__(self, "status", status)
        object.__setattr__(self, "meta", meta)
        object.__setattr__(self, "updated_at", updated_at)
        object.__setattr__(self, "status_line", status_line)
        object.__setattr__(self, "fingerprint", fingerprint)

    def to_dict(self) -> dict[str, Any]:
        return sanitize_public_mapping({
            "id": self.id,
            "name": self.name,
            "status": self.status,
            "updated_at": self.updated_at,
            "status_line": self.status_line,
            "fingerprint": self.fingerprint,
            "meta": sanitize_public_mapping(self.meta),
        })

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "Space":
        clean = sanitize_forbidden_fields(data)
        known = {"id", "name", "status", "meta", "updated_at", "status_line", "summary", "fingerprint"}
        space_id = _string_value(clean.get("id", clean.get("name", "unknown")), "unknown")
        return cls(
            id=space_id,
            name=_string_value(clean.get("name", space_id), space_id),
            status=clean.get("status", "unknown"),
            meta=_merge_meta(clean, known),
            updated_at=clean.get("updated_at"),
            status_line=clean.get("status_line", clean.get("summary")),
            fingerprint=_string_value(clean.get("fingerprint")),
        )


@dataclass(frozen=True)
class Worker:
    """A neutral worker observation (e.g. a running terminal agent)."""

    id: str
    name: str
    status: str = "unknown"
    space_id: str | None = None
    meta: dict[str, Any] = field(default_factory=dict)
    last_seen_at: str | None = None
    summary: str | None = None
    fingerprint: str = ""
    backend_target: dict[str, Any] | None = field(default=None, compare=False, repr=False)

    def __post_init__(self) -> None:
        worker_id = _public_safe_identity(self.id, prefix="worker")
        name = _public_safe_text(self.name, default=worker_id)
        status, meta = _status_and_meta(self.status, self.meta)
        space_id = _optional_public_safe_identity(self.space_id, prefix="space")
        last_seen_at = _optional_timestamp(self.last_seen_at)
        summary = _optional_public_safe_text(self.summary)
        fingerprint = _public_safe_fingerprint(self.fingerprint) or stable_fingerprint(
            {
                "type": "worker",
                "id": worker_id,
                "name": name,
                "status": status,
                "space_id": space_id,
                "summary": summary,
                "meta": meta,
            }
        )

        object.__setattr__(self, "id", worker_id)
        object.__setattr__(self, "name", name)
        object.__setattr__(self, "status", status)
        object.__setattr__(self, "space_id", space_id)
        object.__setattr__(self, "meta", meta)
        object.__setattr__(self, "last_seen_at", last_seen_at)
        object.__setattr__(self, "summary", summary)
        object.__setattr__(self, "fingerprint", fingerprint)
        backend_target = None
        if isinstance(self.backend_target, Mapping):
            backend_target = {str(key): value for key, value in self.backend_target.items()}
        object.__setattr__(self, "backend_target", backend_target)

    def to_dict(self) -> dict[str, Any]:
        return sanitize_public_mapping({
            "id": self.id,
            "name": self.name,
            "status": self.status,
            "space_id": self.space_id,
            "last_seen_at": self.last_seen_at,
            "summary": self.summary,
            "fingerprint": self.fingerprint,
            "meta": sanitize_public_mapping(self.meta),
        })

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "Worker":
        clean = sanitize_forbidden_fields(data)
        known = {
            "id",
            "name",
            "status",
            "space_id",
            "space",
            "meta",
            "last_seen_at",
            "updated_at",
            "summary",
            "status_line",
            "fingerprint",
        }
        worker_id = _string_value(clean.get("id", clean.get("name", "unknown")), "unknown")
        return cls(
            id=worker_id,
            name=_string_value(clean.get("name", worker_id), worker_id),
            status=clean.get("status", "unknown"),
            space_id=clean.get("space_id", clean.get("space")),
            meta=_merge_meta(clean, known),
            last_seen_at=clean.get("last_seen_at", clean.get("updated_at")),
            summary=clean.get("summary", clean.get("status_line")),
            fingerprint=_string_value(clean.get("fingerprint")),
        )


@dataclass(frozen=True, init=False)
class AttentionSignal:
    """A pure, neutral attention signal produced from snapshot state."""

    id: str
    kind: str
    severity: str
    status: str
    reason: str
    source: str
    updated_at: str | None
    suggested_actions: list[SuggestedAction]
    fingerprint: str
    meta: dict[str, Any]

    def __init__(
        self,
        id: str | None = None,
        level: str | None = None,
        reason: str = "",
        source: str = "",
        *,
        kind: str = "general",
        severity: str | None = None,
        status: str = "unknown",
        updated_at: Any = None,
        suggested_actions: Iterable[SuggestedAction | Mapping[str, Any]] | SuggestedAction | Mapping[str, Any] | None = None,
        fingerprint: str | None = None,
        meta: Mapping[str, Any] | None = None,
        host_id: str | None = None,
    ) -> None:
        resolved_kind = _public_safe_text(kind, default="general")
        resolved_severity = normalize_severity(severity if severity is not None else level)
        resolved_status, clean_meta = _status_and_meta(status, meta or {})
        resolved_reason = _public_safe_text(reason)
        resolved_source = _public_safe_text(source, default="unknown")
        resolved_updated_at = _optional_timestamp(updated_at)
        actions = self._coerce_actions(suggested_actions)
        resolved_fingerprint = _public_safe_fingerprint(fingerprint) or attention_fingerprint(
            host_id=_string_value(host_id),
            source=resolved_source,
            kind=resolved_kind,
            severity=resolved_severity,
            reason=resolved_reason,
            status=resolved_status,
        )
        resolved_id = _public_safe_text(id) or f"attn-{resolved_fingerprint}"

        object.__setattr__(self, "id", resolved_id)
        object.__setattr__(self, "kind", resolved_kind)
        object.__setattr__(self, "severity", resolved_severity)
        object.__setattr__(self, "status", resolved_status)
        object.__setattr__(self, "reason", resolved_reason)
        object.__setattr__(self, "source", resolved_source)
        object.__setattr__(self, "updated_at", resolved_updated_at)
        object.__setattr__(self, "suggested_actions", actions)
        object.__setattr__(self, "fingerprint", resolved_fingerprint)
        object.__setattr__(self, "meta", clean_meta)

    @staticmethod
    def _coerce_actions(
        suggested_actions: Iterable[SuggestedAction | Mapping[str, Any]] | SuggestedAction | Mapping[str, Any] | None,
    ) -> list[SuggestedAction]:
        if suggested_actions is None:
            return []
        if isinstance(suggested_actions, SuggestedAction) or isinstance(suggested_actions, Mapping):
            return [SuggestedAction.from_dict(suggested_actions)]
        return [SuggestedAction.from_dict(action) for action in suggested_actions]

    @property
    def level(self) -> str:
        """Backward-compatible alias for severity."""
        return self.severity

    def to_dict(self) -> dict[str, Any]:
        return sanitize_public_mapping({
            "id": self.id,
            "kind": self.kind,
            "severity": self.severity,
            "status": self.status,
            "reason": self.reason,
            "source": self.source,
            "updated_at": self.updated_at,
            "suggested_actions": [action.to_dict() for action in self.suggested_actions],
            "fingerprint": self.fingerprint,
            "meta": sanitize_public_mapping(self.meta),
        })

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "AttentionSignal":
        clean = sanitize_forbidden_fields(data)
        known = {
            "id",
            "kind",
            "severity",
            "level",
            "status",
            "reason",
            "source",
            "updated_at",
            "suggested_actions",
            "fingerprint",
            "meta",
            "host_id",
        }
        return cls(
            id=_string_value(clean.get("id")) or None,
            level=clean.get("level"),
            kind=_string_value(clean.get("kind", "general"), "general"),
            severity=clean.get("severity"),
            status=_string_value(clean.get("status", "unknown"), "unknown"),
            reason=_string_value(clean.get("reason")),
            source=_string_value(clean.get("source")),
            updated_at=clean.get("updated_at"),
            suggested_actions=clean.get("suggested_actions", []),
            fingerprint=_string_value(clean.get("fingerprint")) or None,
            meta=_merge_meta(clean, known),
            host_id=_string_value(clean.get("host_id")),
        )


@dataclass(frozen=True)
class BackendHealth:
    """Public-safe backend observation health for a snapshot or command path."""

    name: str
    status: str
    outcome: str
    observed_at: str | None = None
    message: str = ""
    counts: dict[str, int] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "name", _public_safe_backend_name(self.name))
        object.__setattr__(self, "status", _backend_health_status(self.status))
        object.__setattr__(self, "outcome", _backend_health_outcome(self.outcome))
        object.__setattr__(self, "observed_at", _optional_timestamp(self.observed_at))
        object.__setattr__(self, "message", _public_safe_backend_message(self.message))
        object.__setattr__(self, "counts", _backend_health_counts(self.counts))

    @property
    def healthy(self) -> bool:
        return self.status == "healthy"

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "name": self.name,
            "status": self.status,
            "outcome": self.outcome,
            "observed_at": self.observed_at,
            "message": self.message,
        }
        if self.counts:
            payload["counts"] = dict(self.counts)
        return sanitize_public_mapping(payload)

    @classmethod
    def from_dict(cls, data: "BackendHealth | Mapping[str, Any]") -> "BackendHealth":
        if isinstance(data, BackendHealth):
            return data
        clean = sanitize_forbidden_fields(data if isinstance(data, Mapping) else {})
        return cls(
            name=clean.get("name", "unknown"),
            status=clean.get("status", "unknown"),
            outcome=clean.get("outcome", "unknown"),
            observed_at=clean.get("observed_at"),
            message=clean.get("message", ""),
            counts=clean.get("counts", {}),
        )


@dataclass(frozen=True)
class Snapshot:
    """Device-neutral top-level snapshot shape."""

    host_id: str
    updated_at: str
    spaces: list[Space] = field(default_factory=list)
    workers: list[Worker] = field(default_factory=list)
    attention: list[AttentionSignal] = field(default_factory=list)
    backend_health: list[BackendHealth] = field(default_factory=list)
    schema_version: int = SCHEMA_VERSION
    content_fingerprint: str = ""

    def __post_init__(self) -> None:
        host_id = _string_value(self.host_id, "unknown")
        updated_at = _string_value(_optional_timestamp(self.updated_at), utc_timestamp())
        spaces = sorted(
            (space if isinstance(space, Space) else Space.from_dict(space) for space in self.spaces),
            key=lambda space: (space.id, space.fingerprint),
        )
        workers = sorted(
            (worker if isinstance(worker, Worker) else Worker.from_dict(worker) for worker in self.workers),
            key=lambda worker: (worker.id, worker.fingerprint),
        )
        attention = sorted(
            (
                signal if isinstance(signal, AttentionSignal) else AttentionSignal.from_dict(signal)
                for signal in self.attention
            ),
            key=lambda signal: (signal.id, signal.fingerprint),
        )
        backend_health = sorted(
            (
                health if isinstance(health, BackendHealth) else BackendHealth.from_dict(health)
                for health in self.backend_health
            ),
            key=lambda health: (health.name, health.status, health.outcome),
        )

        object.__setattr__(self, "host_id", host_id)
        object.__setattr__(self, "updated_at", updated_at)
        object.__setattr__(self, "spaces", list(spaces))
        object.__setattr__(self, "workers", list(workers))
        object.__setattr__(self, "attention", list(attention))
        object.__setattr__(self, "backend_health", list(backend_health))
        object.__setattr__(self, "schema_version", SCHEMA_VERSION)
        object.__setattr__(self, "content_fingerprint", self.compute_content_fingerprint())

    def _content_dict(self) -> dict[str, Any]:
        return _strip_snapshot_content_volatile(
            {
                "schema_version": self.schema_version,
                "host_id": self.host_id,
                "spaces": [space.to_dict() for space in self.spaces],
                "workers": [worker.to_dict() for worker in self.workers],
                "attention": [signal.to_dict() for signal in self.attention],
                "backend_health": [health.to_dict() for health in self.backend_health],
            }
        )

    def compute_content_fingerprint(self) -> str:
        """Return the deterministic fingerprint excluding volatile timestamps."""
        return stable_fingerprint(self._content_dict())

    def to_dict(self) -> dict[str, Any]:
        return sanitize_public_mapping({
            "schema_version": self.schema_version,
            "host_id": self.host_id,
            "updated_at": self.updated_at,
            "spaces": [space.to_dict() for space in self.spaces],
            "workers": [worker.to_dict() for worker in self.workers],
            "attention": [signal.to_dict() for signal in self.attention],
            "backend_health": [health.to_dict() for health in self.backend_health],
            "content_fingerprint": self.content_fingerprint,
        })

    def to_json(self, indent: int | None = None) -> str:
        return public_json_dumps(self.to_dict(), indent=indent)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "Snapshot":
        clean = sanitize_forbidden_fields(data)
        return cls(
            host_id=_string_value(clean.get("host_id", "unknown"), "unknown"),
            updated_at=_string_value(clean.get("updated_at"), utc_timestamp()),
            spaces=[Space.from_dict(space) for space in clean.get("spaces", [])],
            workers=[Worker.from_dict(worker) for worker in clean.get("workers", [])],
            attention=[AttentionSignal.from_dict(signal) for signal in clean.get("attention", [])],
            backend_health=[BackendHealth.from_dict(health) for health in clean.get("backend_health", [])],
            schema_version=SCHEMA_VERSION,
            content_fingerprint=_string_value(clean.get("content_fingerprint")),
        )

    @classmethod
    def from_json(cls, payload: str) -> "Snapshot":
        return cls.from_dict(json.loads(payload))


@dataclass(frozen=True)
class WorkerBinding:
    """Private local binding between a public worker and backend send target."""

    host_id: str
    worker_id: str
    worker_fingerprint: str
    backend: str
    target_kind: str
    target_value: str
    turn_target_kind: str | None = None
    turn_target_value: str | None = None
    sendable: bool = False
    reason: str | None = None
    observed_at: str | None = None
    expires_at: str | None = None
    private_fingerprint: str = ""

    def __post_init__(self) -> None:
        host_id = _string_value(self.host_id, "unknown")
        worker_id = _string_value(self.worker_id, "unknown")
        worker_fingerprint = _string_value(self.worker_fingerprint)
        backend = _string_value(self.backend, "unknown")
        target_kind = _string_value(self.target_kind)
        target_value = _string_value(self.target_value)
        turn_target_kind = _optional_string(self.turn_target_kind)
        turn_target_value = _optional_string(self.turn_target_value)
        observed_at = _string_value(_optional_timestamp(self.observed_at), utc_timestamp())
        expires_at = _string_value(
            _optional_timestamp(self.expires_at),
            WORKER_BINDING_ACTIVE_EXPIRES_AT,
        )
        reason = _optional_string(self.reason)
        private_fingerprint = _string_value(self.private_fingerprint) or worker_binding_private_fingerprint(
            host_id=host_id,
            backend=backend,
            identity_material={
                "target_kind": target_kind,
                "target_value": target_value,
                "turn_target_kind": turn_target_kind,
                "turn_target_value": turn_target_value,
            },
        )

        object.__setattr__(self, "host_id", host_id)
        object.__setattr__(self, "worker_id", worker_id)
        object.__setattr__(self, "worker_fingerprint", worker_fingerprint)
        object.__setattr__(self, "backend", backend)
        object.__setattr__(self, "target_kind", target_kind)
        object.__setattr__(self, "target_value", target_value)
        object.__setattr__(self, "turn_target_kind", turn_target_kind)
        object.__setattr__(self, "turn_target_value", turn_target_value)
        object.__setattr__(self, "sendable", bool(self.sendable))
        object.__setattr__(self, "reason", reason)
        object.__setattr__(self, "observed_at", observed_at)
        object.__setattr__(self, "expires_at", expires_at)
        object.__setattr__(self, "private_fingerprint", private_fingerprint)

    def backend_target(self) -> dict[str, Any]:
        """Return the private in-memory backend target shape for command routing."""
        return {
            "kind": self.target_kind,
            "value": self.target_value,
            "sendable": self.sendable,
            "reason": self.reason,
        }


def _worker_binding_duplicate_group_key(binding: WorkerBinding) -> tuple[str, str, str]:
    return (binding.host_id, binding.backend, binding.private_fingerprint)


def _worker_binding_private_row_key(
    binding: WorkerBinding,
) -> tuple[str, str, str, str, str | None, str | None]:
    return (
        binding.worker_id,
        binding.worker_fingerprint,
        binding.target_kind,
        binding.target_value,
        binding.turn_target_kind,
        binding.turn_target_value,
    )


def _duplicate_worker_binding_private_fingerprint(binding: WorkerBinding) -> str:
    return worker_binding_private_fingerprint(
        host_id=binding.host_id,
        backend=binding.backend,
        identity_material={
            "duplicate_backend_target": True,
            "original_private_fingerprint": binding.private_fingerprint,
            "worker_id": binding.worker_id,
            "worker_fingerprint": binding.worker_fingerprint,
            "target_kind": binding.target_kind,
            "target_value": binding.target_value,
            "turn_target_kind": binding.turn_target_kind,
            "turn_target_value": binding.turn_target_value,
            "host_id": binding.host_id,
            "backend": binding.backend,
        },
    )


def _duplicate_separated_worker_binding(binding: WorkerBinding) -> WorkerBinding:
    private_fingerprint = _duplicate_worker_binding_private_fingerprint(binding)
    if (
        binding.private_fingerprint == private_fingerprint
        and binding.sendable is False
        and binding.reason == "duplicate_backend_target"
    ):
        return binding
    return WorkerBinding(
        host_id=binding.host_id,
        worker_id=binding.worker_id,
        worker_fingerprint=binding.worker_fingerprint,
        backend=binding.backend,
        target_kind=binding.target_kind,
        target_value=binding.target_value,
        turn_target_kind=binding.turn_target_kind,
        turn_target_value=binding.turn_target_value,
        sendable=False,
        reason="duplicate_backend_target",
        observed_at=binding.observed_at,
        expires_at=binding.expires_at,
        private_fingerprint=private_fingerprint,
    )


def separate_duplicate_worker_bindings(bindings: Iterable[WorkerBinding]) -> list[WorkerBinding]:
    """Split colliding private binding identities into unsendable private rows."""
    binding_list = list(bindings)
    groups: dict[tuple[str, str, str], list[WorkerBinding]] = {}
    for binding in binding_list:
        groups.setdefault(_worker_binding_duplicate_group_key(binding), []).append(binding)

    duplicate_keys = {
        key
        for key, group in groups.items()
        if len({_worker_binding_private_row_key(binding) for binding in group}) > 1
    }
    if not duplicate_keys:
        return binding_list

    return [
        _duplicate_separated_worker_binding(binding)
        if _worker_binding_duplicate_group_key(binding) in duplicate_keys
        else binding
        for binding in binding_list
    ]
