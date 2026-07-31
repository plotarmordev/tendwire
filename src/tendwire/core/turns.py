"""Public turn and pending-interaction contracts for Tendwire.

This module is pure stdlib plus sibling core models. It owns public, neutral
turn/pending JSON shapes and conservative projections from public snapshots.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import re
import time
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any, Literal

from .models import (
    AttentionSignal,
    BackendHealth,
    FORBIDDEN_FIELD_NAMES,
    Snapshot,
    Worker,
    _FORBIDDEN_BACKEND_NAME_TEXT,
    _is_forbidden_public_mapping_key,
    _is_forbidden_public_text_phrase,
    _TEXT_FORBIDDEN_FIELD_NAMES,
    normalize_status,
    sanitize_canonical_turn_text,
    public_json_dumps,
    sanitize_public_mapping,
    sanitize_public_text,
    sanitize_public_value,
    sanitize_forbidden_fields,
    stable_fingerprint,
    stable_json_dumps,
    utc_timestamp,
    _optional_string,
    _optional_timestamp,
    _string_value,
)


TURN_SCHEMA_VERSION = 1
TURN_LIST_SCHEMA_VERSION = 2
TURN_CONTENT_SCHEMA_VERSION = 1
TURN_TEXT_MAX_CHARS = 12000
TURN_STREAM_TEXT_MAX_CHARS = 4000
TURN_CONTENT_PREVIEW_MAX_CHARS = 1000
TURN_CONTENT_PAGE_MAX_UTF8_BYTES = 48 * 1024
TURN_LIST_DEFAULT_LIMIT = 100
TURN_LIST_MAX_LIMIT = 250
TURN_LIST_CURSOR_TTL_SECONDS = 900
TURN_DELTA_SCHEMA_VERSION = 1
TURN_DELTA_PROJECTION_SCHEMA_VERSION = TURN_LIST_SCHEMA_VERSION
TURN_DELTA_DEFAULT_LIMIT = 100
TURN_DELTA_MAX_LIMIT = 500
TURN_DELTA_CURSOR_TTL_SECONDS = 300
TURN_DELTA_MAX_BATCH_SEQUENCES = 10_000
TURN_DELTA_BOOTSTRAP_MAX_ROWS = 100_000
TURN_DELTA_BOOTSTRAP_MAX_PAGES = 2_000

TURN_CONTENT_FIELDS = ("user_text", "assistant_final_text")
TURN_CONTENT_AVAILABILITIES = frozenset({"absent", "complete", "known_incomplete"})

TURN_KINDS = frozenset({"task", "message", "review", "unknown"})
PENDING_KINDS = frozenset(
    {
        "approval",
        "question",
        "choice",
        "review",
        "confirm_destructive_action",
        "unknown",
    }
)
PENDING_STATUSES = frozenset({"open", "answered", "cancelled", "expired", "unknown"})

_TURN_KIND_ALIASES = {
    "": "unknown",
    "task": "task",
    "work": "task",
    "job": "task",
    "message": "message",
    "note": "message",
    "review": "review",
    "inspect": "review",
}
_PENDING_KIND_ALIASES = {
    "": "unknown",
    "approve": "approval",
    "approval": "approval",
    "requires_approval": "approval",
    "requires-approval": "approval",
    "question": "question",
    "ask": "question",
    "input": "question",
    "choice": "choice",
    "select": "choice",
    "review": "review",
    "manual_review": "review",
    "manual-review": "review",
    "confirm": "confirm_destructive_action",
    "confirmation": "confirm_destructive_action",
    "destructive": "confirm_destructive_action",
    "confirm_destructive": "confirm_destructive_action",
    "confirm-destructive": "confirm_destructive_action",
    "confirm_destructive_action": "confirm_destructive_action",
    "confirm-destructive-action": "confirm_destructive_action",
}
_PENDING_STATUS_ALIASES = {
    "": "unknown",
    "open": "open",
    "pending": "open",
    "waiting": "open",
    "active": "open",
    "new": "open",
    "answered": "answered",
    "done": "answered",
    "complete": "answered",
    "completed": "answered",
    "resolved": "answered",
    "accepted": "answered",
    "cancelled": "cancelled",
    "canceled": "cancelled",
    "closed": "cancelled",
    "rejected": "cancelled",
    "expired": "expired",
    "timed_out": "expired",
    "timed-out": "expired",
    "timeout": "expired",
}
_VOLATILE_KEYS = frozenset(
    {
        "updated_at",
        "observed_at",
        "created_at",
        "started_at",
        "completed_at",
        "expires_at",
        "last_seen_at",
        "timestamp",
        "fingerprint",
        "content_fingerprint",
    }
)
_HUMAN_META_KEYS = frozenset(
    {
        "needs_human",
        "human_input_required",
        "requires_human",
        "approval_required",
        "requires_approval",
        "needs_input",
        "awaiting_input",
    }
)
_APPROVAL_RE = re.compile(r"\bapproval|approve|approved\b", re.IGNORECASE)
_DESTRUCTIVE_RE = re.compile(r"\bdelete|destroy|destructive|irreversible|remove|wipe\b", re.IGNORECASE)
_QUESTION_RE = re.compile(r"\?|question|\bask(?:ing)?\b|\binput\b", re.IGNORECASE)
_REVIEW_RE = re.compile(r"\breview|inspect|manual\b", re.IGNORECASE)
_RAW_COMMAND_HEAD_RE = re.compile(
    r"^(?:sudo\s+)?(?:env\s+)?"
    r"(?:bash|sh|zsh|fish|cmd|powershell|pwsh|python\d*|node|npm|npx|git|gh|docker|"
    r"kubectl|make|pytest|herdr|tendwire|tmux|screen)(?:\s|$)",
    re.IGNORECASE,
)
_RAW_COMMAND_OPTION_RE = re.compile(r"\s--?[A-Za-z0-9][A-Za-z0-9_-]*")
_SHELL_META_RE = re.compile(r"[;&|`$<>]")
_CAMEL_CASE_BOUNDARY_RE = re.compile(r"(?<=[a-z0-9])(?=[A-Z])")
_AUTOMATION_JOB_TEMPLATE_RE = re.compile(
    r"^[A-Za-z][A-Za-z0-9_. -]{1,80}\s+job\s*\n\s*\nTemplate:\s*\S+",
    re.IGNORECASE,
)
_AUTOMATION_VALIDATOR_RE = re.compile(
    r"^Your previous response did not contain a valid [A-Za-z0-9_.-]+ JSON object\.\s*"
    r"\nValidation errors(?:\s*\([^)]*\))?:",
    re.IGNORECASE,
)
_AUTOMATION_RESULT_KEY_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_.-]{0,80}_result$")
_AUTOMATION_RESULT_FIELDS = frozenset(
    {
        "changes_made",
        "decision",
        "delegations",
        "findings",
        "needs",
        "status",
        "summary",
        "tests_run",
    }
)


def _normalize_turn_kind(kind: Any) -> str:
    raw = _string_value(kind, "unknown").strip().lower().replace(" ", "_")
    normalized = raw.replace("-", "_")
    return _TURN_KIND_ALIASES.get(normalized, _TURN_KIND_ALIASES.get(raw, "unknown"))


def _normalize_pending_kind(kind: Any) -> str:
    raw = _string_value(kind, "unknown").strip().lower().replace(" ", "_")
    normalized = raw.replace("-", "_")
    return _PENDING_KIND_ALIASES.get(normalized, _PENDING_KIND_ALIASES.get(raw, "unknown"))


def _normalize_pending_status(status: Any) -> str:
    raw = _string_value(status, "unknown").strip().lower().replace(" ", "_")
    normalized = raw.replace("_", "-")
    return _PENDING_STATUS_ALIASES.get(raw, _PENDING_STATUS_ALIASES.get(normalized, "unknown"))


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, (int, float)):
        return value != 0
    text = str(value).strip().lower()
    return text not in {"", "0", "false", "no", "off", "none", "null"}


def _clean_meta(value: Any) -> dict[str, Any]:
    return sanitize_public_mapping(value if isinstance(value, Mapping) else {})


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


def _public_text(value: Any, *, default: str = "") -> str:
    text = sanitize_public_text(_string_value(value).strip())
    if not text or _contains_forbidden_public_text(text) or _looks_like_raw_command(text):
        return default
    return " ".join(text.split())


def _optional_public_text(value: Any) -> str | None:
    if value is None:
        return None
    text = _public_text(value)
    return text or None


def _optional_public_fingerprint(value: Any) -> str | None:
    if value is None:
        return None
    raw = _string_value(value).strip()
    if not raw or _contains_forbidden_public_text(raw):
        return None
    text = sanitize_public_text(raw)
    if text != raw or _contains_forbidden_public_text(text):
        return None
    return text


def _public_turn_text(value: Any) -> str | None:
    return sanitize_canonical_turn_text(value)


def _public_stream_text(value: Any) -> str | None:
    text = sanitize_public_text(value, max_chars=None)
    if not text:
        return None
    return text[-TURN_STREAM_TEXT_MAX_CHARS:]




def redact_private_prompt_text(value: Any, *, max_chars: int = TURN_TEXT_MAX_CHARS) -> str:
    """Best-effort sanitizer for user-facing pending prompt and choice text.

    Arbitrary shapeless secrets in free-form prose cannot be detected reliably;
    known private source shapes and provider credentials are still redacted.
    Non-string backend values are rejected instead of stringified.
    """
    return sanitize_public_text(
        value,
        max_chars=max_chars,
        collapse_whitespace=True,
    )

def _base64url(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")


def _domain_digest(domain: str, value: Mapping[str, Any]) -> str:
    encoded = stable_json_dumps({"domain": domain, **value}).encode("utf-8")
    return _base64url(hashlib.sha256(encoded).digest())


def _validate_content_field(field: str) -> str:
    if field not in TURN_CONTENT_FIELDS:
        raise ValueError("invalid_content_field")
    return field


def _validate_content_state(state: str, text: str | None) -> str:
    if not isinstance(state, str):
        raise ValueError("invalid_content_availability")
    if text is not None and not isinstance(text, str):
        raise ValueError("invalid_content_text")
    if state not in TURN_CONTENT_AVAILABILITIES:
        raise ValueError("invalid_content_availability")
    if state == "absent" and text is not None:
        raise ValueError("absent_content_has_text")
    if state != "absent" and text is None:
        raise ValueError("available_content_has_no_text")
    return state


def _inferred_content_state(text: str | None, state: str | None) -> str:
    return _validate_content_state(state or ("absent" if text is None else "complete"), text)


def content_revision(
    turn_id: str,
    user_text: str | None,
    final_text: str | None,
    user_state: str,
    final_state: str,
) -> str:
    """Return the stable opaque identity of one immutable canonical revision."""
    clean_turn_id = str(turn_id)
    if not clean_turn_id:
        raise ValueError("invalid_turn_id")
    _validate_content_state(user_state, user_text)
    _validate_content_state(final_state, final_text)
    digest = _domain_digest(
        "tendwire.turn-content-revision.v1",
        {
            "turn_id": clean_turn_id,
            "user_text": user_text,
            "assistant_final_text": final_text,
            "user_state": user_state,
            "final_state": final_state,
        },
    )
    return f"twrev1.{digest}"


def turn_final_delivery_identity(
    host_id: str,
    turn_id: str,
    content_revision: str,
) -> str:
    """Return the stable neutral identity of one final revision delivery."""
    clean_host_id = str(host_id)
    clean_turn_id = str(turn_id)
    clean_revision = str(content_revision)
    if not clean_host_id:
        raise ValueError("invalid_host_id")
    if not clean_turn_id:
        raise ValueError("invalid_turn_id")
    if not clean_revision:
        raise ValueError("invalid_content_revision")
    digest = _domain_digest(
        "tendwire.turn-final-delivery.v1",
        {
            "host_id": clean_host_id,
            "turn_id": clean_turn_id,
            "content_revision": clean_revision,
        },
    )
    return f"twfinal1.{digest}"


def content_segment_id(revision: str, field: str, index: int) -> str:
    """Return a stable opaque identity for a transport segment."""
    _validate_content_field(field)
    if not isinstance(index, int) or isinstance(index, bool) or index < 0:
        raise ValueError("invalid_segment_index")
    digest = _domain_digest(
        "tendwire.turn-content-segment.v1",
        {"content_revision": str(revision), "field": field, "index": index},
    )
    return f"twseg1.{digest}"


@dataclass(frozen=True)
class ContentCursorPosition:
    """Integrity-bound continuation coordinates decoded from an opaque cursor."""

    index: int
    segment_id: str
    start_char: int
    start_byte: int


def content_cursor(
    revision: str,
    field: str,
    index: int,
    *,
    start_char: int | None = None,
    start_byte: int | None = None,
) -> str:
    """Encode one deterministic cursor bound to its segment and exact start."""
    _validate_content_field(field)
    if start_char is None or start_byte is None:
        if index != 0 or start_char is not None or start_byte is not None:
            raise ValueError("invalid_cursor")
        start_char = 0
        start_byte = 0
    coordinates = (index, start_char, start_byte)
    if any(
        not isinstance(value, int) or isinstance(value, bool) or value < 0
        for value in coordinates
    ):
        raise ValueError("invalid_cursor")
    segment_id = content_segment_id(revision, field, index)
    material = {
        "content_revision": str(revision),
        "field": field,
        "segment_id": segment_id,
        "index": index,
        "start_char": start_char,
        "start_byte": start_byte,
    }
    integrity = _domain_digest(
        "tendwire.turn-content-cursor-integrity.v2",
        material,
    )
    body = stable_json_dumps(
        {
            "b": start_byte,
            "c": start_char,
            "h": integrity,
            "i": index,
            "s": segment_id,
            "v": 2,
        }
    ).encode("utf-8")
    return f"twcur1.{_base64url(body)}"


def decode_content_cursor(
    cursor: str,
    *,
    revision: str,
    field: str,
    count: int,
) -> ContentCursorPosition:
    """Validate a cursor and return its revision-bound continuation coordinates."""
    _validate_content_field(field)
    if not isinstance(count, int) or isinstance(count, bool) or count < 1:
        raise ValueError("invalid_cursor")
    if not isinstance(cursor, str) or not cursor.startswith("twcur1."):
        raise ValueError("invalid_cursor")
    encoded = cursor.removeprefix("twcur1.")
    if (
        not encoded
        or len(encoded) > 1024
        or any(
            char
            not in "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
            for char in encoded
        )
    ):
        raise ValueError("invalid_cursor")
    try:
        padding = "=" * (-len(encoded) % 4)
        raw = base64.b64decode(encoded + padding, altchars=b"-_", validate=True)
        if _base64url(raw) != encoded:
            raise ValueError("noncanonical cursor encoding")
        body = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, ValueError, json.JSONDecodeError):
        raise ValueError("invalid_cursor") from None
    if (
        not isinstance(body, dict)
        or set(body) != {"b", "c", "h", "i", "s", "v"}
        or body.get("v") != 2
    ):
        raise ValueError("invalid_cursor")
    index = body.get("i")
    start_char = body.get("c")
    start_byte = body.get("b")
    segment_id = body.get("s")
    integrity = body.get("h")
    if (
        any(
            not isinstance(value, int) or isinstance(value, bool) or value < 0
            for value in (index, start_char, start_byte)
        )
        or not 0 <= index < count
        or not isinstance(segment_id, str)
        or segment_id != content_segment_id(revision, field, index)
        or not isinstance(integrity, str)
    ):
        raise ValueError("invalid_cursor")
    expected = _domain_digest(
        "tendwire.turn-content-cursor-integrity.v2",
        {
            "content_revision": str(revision),
            "field": field,
            "segment_id": segment_id,
            "index": index,
            "start_char": start_char,
            "start_byte": start_byte,
        },
    )
    if not hmac.compare_digest(integrity, expected):
        raise ValueError("invalid_cursor")
    return ContentCursorPosition(index, segment_id, start_char, start_byte)


@dataclass(frozen=True)
class TurnListCursorPosition:
    """Validated continuation coordinates for one stable turn-list traversal."""

    schema_version: int
    limit: int
    since_sequence: int
    watermark: int
    floor_sequence: int
    traversal_generation: int
    worker_id: str
    list_sequence: int
    turn_id: str
    store_epoch: str
    expires_at: int


@dataclass(frozen=True)
class TurnSincePosition:
    """Validated durable insertion watermark for a turn-list poll."""

    schema_version: int
    watermark: int
    store_epoch: str


@dataclass(frozen=True)
class TurnDeltaWatermarkPosition:
    """Validated durable mutation checkpoint for one host projection."""

    schema_version: int
    projection_schema_version: int
    sequence: int
    store_epoch: str


@dataclass(frozen=True)
class TurnDeltaCursorPosition:
    """Validated continuation coordinates for one frozen delta batch."""

    schema_version: int
    projection_schema_version: int
    mode: Literal["bootstrap", "changes"]
    limit: int
    accepted_sequence: int
    batch_high: int
    insertion_high: int
    page_number: int
    position_sequence: int
    position_worker_id: str
    position_turn_id: str
    store_epoch: str
    expires_at: int


def _decode_turn_list_token(token: str, prefix: str, status: str) -> dict[str, Any]:
    if not isinstance(token, str) or not token.startswith(prefix):
        raise ValueError(status)
    encoded = token.removeprefix(prefix)
    if (
        not encoded
        or len(encoded) > 2048
        or any(
            character
            not in "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
            for character in encoded
        )
    ):
        raise ValueError(status)
    try:
        padding = "=" * (-len(encoded) % 4)
        raw = base64.b64decode(encoded + padding, altchars=b"-_", validate=True)
        if _base64url(raw) != encoded:
            raise ValueError("noncanonical token encoding")
        body = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, ValueError, json.JSONDecodeError):
        raise ValueError(status) from None
    if not isinstance(body, dict):
        raise ValueError(status)
    return body


def _turn_list_nonnegative_integer(value: Any, status: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise ValueError(status)
    return value


def turn_list_cursor(
    host_id: str,
    *,
    schema_version: int,
    limit: int,
    since_sequence: int,
    watermark: int,
    floor_sequence: int,
    traversal_generation: int,
    worker_id: str,
    list_sequence: int,
    turn_id: str,
    store_epoch: str,
    expires_at: int,
) -> str:
    """Encode an opaque cursor bound to the complete original list request."""
    status = "invalid_cursor"
    host = str(host_id)
    worker = str(worker_id)
    turn = str(turn_id)
    epoch = str(store_epoch)
    if (
        not host
        or not worker
        or not turn
        or not epoch
        or max(map(len, (host, worker, turn, epoch))) > 512
    ):
        raise ValueError(status)
    schema = _turn_list_nonnegative_integer(schema_version, status)
    page_limit = _turn_list_nonnegative_integer(limit, status)
    since_value = _turn_list_nonnegative_integer(since_sequence, status)
    high = _turn_list_nonnegative_integer(watermark, status)
    floor = _turn_list_nonnegative_integer(floor_sequence, status)
    generation = _turn_list_nonnegative_integer(traversal_generation, status)
    sequence = _turn_list_nonnegative_integer(list_sequence, status)
    expiry = _turn_list_nonnegative_integer(expires_at, status)
    if (
        schema not in {1, TURN_LIST_SCHEMA_VERSION}
        or not 1 <= page_limit <= TURN_LIST_MAX_LIMIT
        or since_value > high
        or sequence <= since_value
        or sequence > high
        or (high and not 1 <= floor <= high)
        or generation < 1
        or not high
        or not expiry
    ):
        raise ValueError(status)
    material = {
        "expires_at": expiry,
        "floor_sequence": floor,
        "traversal_generation": generation,
        "host_id": host,
        "limit": page_limit,
        "list_sequence": sequence,
        "schema_version": schema,
        "since_sequence": since_value,
        "store_epoch": epoch,
        "turn_id": turn,
        "watermark": high,
        "worker_id": worker,
    }
    body = stable_json_dumps(
        {
            "e": expiry,
            "f": floor,
            "g": generation,
            "h": _domain_digest(
                "tendwire.turn-list-cursor-integrity.v1",
                material,
            ),
            "l": page_limit,
            "p": [worker, sequence, turn],
            "q": epoch,
            "s": since_value,
            "v": 1,
            "w": high,
            "x": schema,
            "z": host,
        }
    ).encode("utf-8")
    return f"twlist1.{_base64url(body)}"


def decode_turn_list_cursor(
    cursor: str,
    *,
    host_id: str,
    schema_version: int,
    limit: int,
    now: float | int | None = None,
) -> TurnListCursorPosition:
    """Strictly validate a list cursor and its request binding."""
    status = "invalid_cursor"
    body = _decode_turn_list_token(cursor, "twlist1.", status)
    if set(body) != {"e", "f", "g", "h", "l", "p", "q", "s", "v", "w", "x", "z"}:
        raise ValueError(status)
    if body.get("v") != 1:
        raise ValueError(status)
    position = body.get("p")
    if (
        not isinstance(position, list)
        or len(position) != 3
        or not isinstance(position[0], str)
        or not position[0]
        or not isinstance(position[2], str)
        or not position[2]
        or not isinstance(body.get("h"), str)
        or not isinstance(body.get("q"), str)
        or not body.get("q")
        or not isinstance(body.get("z"), str)
        or not body.get("z")
    ):
        raise ValueError(status)
    try:
        expiry = _turn_list_nonnegative_integer(body.get("e"), status)
        floor = _turn_list_nonnegative_integer(body.get("f"), status)
        generation = _turn_list_nonnegative_integer(body.get("g"), status)
        page_limit = _turn_list_nonnegative_integer(body.get("l"), status)
        sequence = _turn_list_nonnegative_integer(position[1], status)
        since_value = _turn_list_nonnegative_integer(body.get("s"), status)
        high = _turn_list_nonnegative_integer(body.get("w"), status)
        schema = _turn_list_nonnegative_integer(body.get("x"), status)
    except ValueError:
        raise ValueError(status) from None
    host = str(body["z"])
    worker = str(position[0])
    turn = str(position[2])
    epoch = str(body["q"])
    if (
        host != str(host_id)
        or schema != schema_version
        or page_limit != limit
        or schema not in {1, TURN_LIST_SCHEMA_VERSION}
        or not 1 <= page_limit <= TURN_LIST_MAX_LIMIT
        or since_value > high
        or not high
        or sequence <= since_value
        or sequence > high
        or not 1 <= floor <= high
        or generation < 1
        or max(map(len, (host, worker, turn, epoch))) > 512
    ):
        raise ValueError(status)
    expected = _domain_digest(
        "tendwire.turn-list-cursor-integrity.v1",
        {
            "expires_at": expiry,
            "floor_sequence": floor,
            "traversal_generation": generation,
            "host_id": host,
            "limit": page_limit,
            "list_sequence": sequence,
            "schema_version": schema,
            "since_sequence": since_value,
            "store_epoch": epoch,
            "turn_id": turn,
            "watermark": high,
            "worker_id": worker,
        },
    )
    if not hmac.compare_digest(str(body["h"]), expected):
        raise ValueError(status)
    current = time.time() if now is None else float(now)
    if not current < expiry:
        raise ValueError("cursor_expired")
    return TurnListCursorPosition(
        schema,
        page_limit,
        since_value,
        high,
        floor,
        generation,
        worker,
        sequence,
        turn,
        epoch,
        expiry,
    )


def turn_since_token(
    host_id: str,
    *,
    schema_version: int,
    watermark: int,
    store_epoch: str,
) -> str:
    """Encode a durable opaque watermark for later insertion discovery."""
    status = "invalid_cursor"
    host = str(host_id)
    epoch = str(store_epoch)
    schema = _turn_list_nonnegative_integer(schema_version, status)
    high = _turn_list_nonnegative_integer(watermark, status)
    if (
        not host
        or not epoch
        or max(len(host), len(epoch)) > 512
        or schema not in {1, TURN_LIST_SCHEMA_VERSION}
    ):
        raise ValueError(status)
    material = {
        "host_id": host,
        "schema_version": schema,
        "store_epoch": epoch,
        "watermark": high,
    }
    body = stable_json_dumps(
        {
            "h": _domain_digest(
                "tendwire.turn-list-since-integrity.v1",
                material,
            ),
            "q": epoch,
            "v": 1,
            "w": high,
            "x": schema,
            "z": host,
        }
    ).encode("utf-8")
    return f"twsince1.{_base64url(body)}"


def decode_turn_since_token(
    token: str,
    *,
    host_id: str,
    schema_version: int,
) -> TurnSincePosition:
    """Strictly validate a durable list watermark token."""
    status = "invalid_cursor"
    body = _decode_turn_list_token(token, "twsince1.", status)
    if set(body) != {"h", "q", "v", "w", "x", "z"} or body.get("v") != 1:
        raise ValueError(status)
    if (
        not isinstance(body.get("h"), str)
        or not isinstance(body.get("q"), str)
        or not body.get("q")
        or not isinstance(body.get("z"), str)
        or not body.get("z")
    ):
        raise ValueError(status)
    high = _turn_list_nonnegative_integer(body.get("w"), status)
    schema = _turn_list_nonnegative_integer(body.get("x"), status)
    host = str(body["z"])
    epoch = str(body["q"])
    if (
        host != str(host_id)
        or schema != schema_version
        or schema not in {1, TURN_LIST_SCHEMA_VERSION}
        or max(len(host), len(epoch)) > 512
    ):
        raise ValueError(status)
    expected = _domain_digest(
        "tendwire.turn-list-since-integrity.v1",
        {
            "host_id": host,
            "schema_version": schema,
            "store_epoch": epoch,
            "watermark": high,
        },
    )
    if not hmac.compare_digest(str(body["h"]), expected):
        raise ValueError(status)
    return TurnSincePosition(schema, high, epoch)


def _turn_delta_host_digest(host_id: str) -> str:
    host = str(host_id)
    if not host or len(host) > 512:
        raise ValueError("invalid_watermark")
    return _domain_digest("tendwire.turn-delta-host.v1", {"host_id": host})


def turn_delta_watermark(
    host_id: str,
    *,
    sequence: int,
    store_epoch: str,
    schema_version: int = TURN_DELTA_SCHEMA_VERSION,
    projection_schema_version: int = TURN_DELTA_PROJECTION_SCHEMA_VERSION,
) -> str:
    """Encode a host/schema/store-bound durable mutation checkpoint."""
    status = "invalid_watermark"
    schema = _turn_list_nonnegative_integer(schema_version, status)
    projection = _turn_list_nonnegative_integer(projection_schema_version, status)
    accepted = _turn_list_nonnegative_integer(sequence, status)
    epoch = str(store_epoch)
    if (
        schema != TURN_DELTA_SCHEMA_VERSION
        or projection != TURN_DELTA_PROJECTION_SCHEMA_VERSION
        or not epoch
        or len(epoch) > 512
    ):
        raise ValueError(status)
    host_digest = _turn_delta_host_digest(host_id)
    material = {
        "host_digest": host_digest,
        "projection_schema_version": projection,
        "schema_version": schema,
        "sequence": accepted,
        "store_epoch": epoch,
        "token_version": 1,
    }
    body = stable_json_dumps(
        {
            "h": _domain_digest("tendwire.turn-delta-watermark-integrity.v1", material),
            "p": projection,
            "q": epoch,
            "s": accepted,
            "v": 1,
            "x": schema,
            "z": host_digest,
        }
    ).encode("utf-8")
    return f"twdelta1.{_base64url(body)}"


def decode_turn_delta_watermark(
    token: str,
    *,
    host_id: str,
    schema_version: int = TURN_DELTA_SCHEMA_VERSION,
    projection_schema_version: int = TURN_DELTA_PROJECTION_SCHEMA_VERSION,
) -> TurnDeltaWatermarkPosition:
    """Validate a watermark with distinct host and schema mismatch outcomes."""
    status = "invalid_watermark"
    body = _decode_turn_list_token(token, "twdelta1.", status)
    if set(body) != {"h", "p", "q", "s", "v", "x", "z"} or body.get("v") != 1:
        raise ValueError(status)
    if any(not isinstance(body.get(key), str) or not body.get(key) for key in ("h", "q", "z")):
        raise ValueError(status)
    try:
        schema = _turn_list_nonnegative_integer(body.get("x"), status)
        projection = _turn_list_nonnegative_integer(body.get("p"), status)
        accepted = _turn_list_nonnegative_integer(body.get("s"), status)
    except ValueError:
        raise ValueError(status) from None
    epoch = str(body["q"])
    host_digest = str(body["z"])
    if len(epoch) > 512 or len(host_digest) > 512:
        raise ValueError(status)
    expected_integrity = _domain_digest(
        "tendwire.turn-delta-watermark-integrity.v1",
        {
            "host_digest": host_digest,
            "projection_schema_version": projection,
            "schema_version": schema,
            "sequence": accepted,
            "store_epoch": epoch,
            "token_version": 1,
        },
    )
    if not hmac.compare_digest(str(body["h"]), expected_integrity):
        raise ValueError(status)
    if not hmac.compare_digest(host_digest, _turn_delta_host_digest(host_id)):
        raise ValueError("cross_host_watermark")
    if schema != schema_version or projection != projection_schema_version:
        raise ValueError("incompatible_schema")
    return TurnDeltaWatermarkPosition(schema, projection, accepted, epoch)


def turn_delta_cursor(
    host_id: str,
    *,
    mode: Literal["bootstrap", "changes"],
    limit: int,
    accepted_sequence: int,
    batch_high: int,
    insertion_high: int,
    page_number: int,
    position_sequence: int,
    position_worker_id: str,
    position_turn_id: str,
    store_epoch: str,
    expires_at: int,
    schema_version: int = TURN_DELTA_SCHEMA_VERSION,
    projection_schema_version: int = TURN_DELTA_PROJECTION_SCHEMA_VERSION,
) -> str:
    """Encode a short-lived cursor bound to one frozen bootstrap/change batch."""
    status = "invalid_cursor"
    if mode not in {"bootstrap", "changes"}:
        raise ValueError(status)
    values = {
        "schema": schema_version,
        "projection": projection_schema_version,
        "limit": limit,
        "accepted": accepted_sequence,
        "batch_high": batch_high,
        "insertion_high": insertion_high,
        "page_number": page_number,
        "position": position_sequence,
        "expiry": expires_at,
    }
    try:
        parsed = {key: _turn_list_nonnegative_integer(value, status) for key, value in values.items()}
    except ValueError:
        raise ValueError(status) from None
    worker = str(position_worker_id)
    turn = str(position_turn_id)
    epoch = str(store_epoch)
    if (
        parsed["schema"] != TURN_DELTA_SCHEMA_VERSION
        or parsed["projection"] != TURN_DELTA_PROJECTION_SCHEMA_VERSION
        or not 1 <= parsed["limit"] <= TURN_DELTA_MAX_LIMIT
        or parsed["accepted"] > parsed["batch_high"]
        or parsed["page_number"] < 1
        or not epoch
        or not turn
        or mode == "bootstrap" and not worker
        or mode == "changes" and parsed["position"] <= parsed["accepted"]
        or max(map(len, (worker, turn, epoch))) > 512
    ):
        raise ValueError(status)
    host_digest = _turn_delta_host_digest(host_id)
    material = {
        "accepted_sequence": parsed["accepted"],
        "batch_high": parsed["batch_high"],
        "expires_at": parsed["expiry"],
        "host_digest": host_digest,
        "insertion_high": parsed["insertion_high"],
        "limit": parsed["limit"],
        "mode": mode,
        "position_sequence": parsed["position"],
        "page_number": parsed["page_number"],
        "position_turn_id": turn,
        "position_worker_id": worker,
        "projection_schema_version": parsed["projection"],
        "schema_version": parsed["schema"],
        "store_epoch": epoch,
        "token_version": 1,
    }
    body = stable_json_dumps(
        {
            "a": parsed["accepted"],
            "b": parsed["batch_high"],
            "e": parsed["expiry"],
            "h": _domain_digest("tendwire.turn-delta-cursor-integrity.v1", material),
            "i": parsed["insertion_high"],
            "l": parsed["limit"],
            "m": mode,
            "n": parsed["page_number"],
            "p": [worker, parsed["position"], turn],
            "q": epoch,
            "r": parsed["projection"],
            "v": 1,
            "x": parsed["schema"],
            "z": host_digest,
        }
    ).encode("utf-8")
    return f"twdeltac1.{_base64url(body)}"


def decode_turn_delta_cursor(
    cursor: str,
    *,
    host_id: str,
    limit: int,
    now: float | int | None = None,
    schema_version: int = TURN_DELTA_SCHEMA_VERSION,
    projection_schema_version: int = TURN_DELTA_PROJECTION_SCHEMA_VERSION,
) -> TurnDeltaCursorPosition:
    """Strictly validate a delta cursor and the complete page request binding."""
    status = "invalid_cursor"
    body = _decode_turn_list_token(cursor, "twdeltac1.", status)
    if set(body) != {"a", "b", "e", "h", "i", "l", "m", "n", "p", "q", "r", "v", "x", "z"} or body.get("v") != 1:
        raise ValueError(status)
    position = body.get("p")
    if (
        not isinstance(position, list)
        or len(position) != 3
        or not isinstance(position[0], str)
        or not isinstance(position[2], str)
        or not position[2]
        or body.get("m") not in {"bootstrap", "changes"}
        or any(not isinstance(body.get(key), str) or not body.get(key) for key in ("h", "q", "z"))
    ):
        raise ValueError(status)
    try:
        accepted = _turn_list_nonnegative_integer(body.get("a"), status)
        batch_high = _turn_list_nonnegative_integer(body.get("b"), status)
        expiry = _turn_list_nonnegative_integer(body.get("e"), status)
        insertion_high = _turn_list_nonnegative_integer(body.get("i"), status)
        page_limit = _turn_list_nonnegative_integer(body.get("l"), status)
        page_number = _turn_list_nonnegative_integer(body.get("n"), status)
        position_sequence = _turn_list_nonnegative_integer(position[1], status)
        projection = _turn_list_nonnegative_integer(body.get("r"), status)
        schema = _turn_list_nonnegative_integer(body.get("x"), status)
    except ValueError:
        raise ValueError(status) from None
    mode = str(body["m"])
    worker = str(position[0])
    turn = str(position[2])
    epoch = str(body["q"])
    host_digest = str(body["z"])
    material = {
        "accepted_sequence": accepted,
        "batch_high": batch_high,
        "expires_at": expiry,
        "host_digest": host_digest,
        "insertion_high": insertion_high,
        "limit": page_limit,
        "mode": mode,
        "page_number": page_number,
        "position_sequence": position_sequence,
        "position_turn_id": turn,
        "position_worker_id": worker,
        "projection_schema_version": projection,
        "schema_version": schema,
        "store_epoch": epoch,
        "token_version": 1,
    }
    if not hmac.compare_digest(
        str(body["h"]),
        _domain_digest("tendwire.turn-delta-cursor-integrity.v1", material),
    ):
        raise ValueError(status)
    if (
        not hmac.compare_digest(host_digest, _turn_delta_host_digest(host_id))
        or schema != schema_version
        or projection != projection_schema_version
        or page_limit != limit
        or not 1 <= page_limit <= TURN_DELTA_MAX_LIMIT
        or accepted > batch_high
        or page_number < 1
        or not epoch
        or mode == "bootstrap" and not worker
        or mode == "changes" and position_sequence <= accepted
        or max(map(len, (worker, turn, epoch, host_digest))) > 512
    ):
        raise ValueError(status)
    current = time.time() if now is None else float(now)
    if not current < expiry:
        raise ValueError("expired_cursor")
    return TurnDeltaCursorPosition(
        schema,
        projection,
        mode,
        page_limit,
        accepted,
        batch_high,
        insertion_high,
        page_number,
        position_sequence,
        worker,
        turn,
        epoch,
        expiry,
    )


def _utf8_code_point_width(character: str) -> int:
    value = ord(character)
    if 0xD800 <= value <= 0xDFFF:
        raise ValueError("canonical text contains an invalid Unicode surrogate")
    if value <= 0x7F:
        return 1
    if value <= 0x7FF:
        return 2
    if value <= 0xFFFF:
        return 3
    return 4


@dataclass(frozen=True)
class ContentSegment:
    """One exact half-open canonical code-point transport page."""

    index: int
    start_char: int
    end_char: int
    start_byte: int
    end_byte: int
    text: str
    char_length: int
    byte_length: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "index": self.index,
            "start_char": self.start_char,
            "end_char": self.end_char,
            "start_byte": self.start_byte,
            "end_byte": self.end_byte,
            "text": self.text,
            "char_length": self.char_length,
            "byte_length": self.byte_length,
        }


def segment_canonical_text(
    text: str,
    *,
    max_utf8_bytes: int = TURN_CONTENT_PAGE_MAX_UTF8_BYTES,
) -> tuple[ContentSegment, ...]:
    """Split canonical text into exact UTF-8-bounded code-point pages."""
    if not isinstance(text, str):
        raise TypeError("canonical text must be a string")
    if (
        not isinstance(max_utf8_bytes, int)
        or isinstance(max_utf8_bytes, bool)
        or max_utf8_bytes < 1
    ):
        raise ValueError("max_utf8_bytes must be positive")
    if not text:
        return ()

    segments: list[ContentSegment] = []
    start = 0
    start_byte = 0
    byte_length = 0
    for offset, character in enumerate(text):
        width = _utf8_code_point_width(character)
        if width > max_utf8_bytes:
            raise ValueError("max_utf8_bytes cannot hold one code point")
        if byte_length and byte_length + width > max_utf8_bytes:
            page = text[start:offset]
            segments.append(
                ContentSegment(
                    index=len(segments),
                    start_char=start,
                    end_char=offset,
                    start_byte=start_byte,
                    end_byte=start_byte + byte_length,
                    text=page,
                    char_length=offset - start,
                    byte_length=byte_length,
                )
            )
            start = offset
            start_byte += byte_length
            byte_length = 0
        byte_length += width
    page = text[start:]
    segments.append(
        ContentSegment(
            index=len(segments),
            start_char=start,
            end_char=len(text),
            start_byte=start_byte,
            end_byte=start_byte + byte_length,
            text=page,
            char_length=len(text) - start,
            byte_length=byte_length,
        )
    )
    return tuple(segments)


@dataclass(frozen=True)
class ContentFieldDescriptor:
    """Bounded public metadata for one canonical content field."""

    availability: str
    inline: bool
    char_length: int
    byte_length: int
    page_count: int
    first_cursor: str | None

    def __post_init__(self) -> None:
        if self.availability not in TURN_CONTENT_AVAILABILITIES:
            raise ValueError("invalid_content_availability")
        if any(
            not isinstance(value, int) or isinstance(value, bool) or value < 0
            for value in (self.char_length, self.byte_length, self.page_count)
        ):
            raise ValueError("invalid_content_length")
        if not isinstance(self.inline, bool):
            raise ValueError("invalid_inline_state")
        if self.first_cursor is not None and not isinstance(self.first_cursor, str):
            raise ValueError("invalid_first_cursor")
        if self.availability != "complete" and (
            self.inline or self.page_count or self.first_cursor is not None
        ):
            raise ValueError("incomplete_content_cannot_be_inline_or_pageable")
        if self.availability == "absent" and (self.char_length or self.byte_length):
            raise ValueError("absent_content_has_length")
        if self.inline and self.first_cursor is not None:
            raise ValueError("inline_content_has_cursor")
        if (
            self.availability == "complete"
            and not self.inline
            and self.char_length
            and (not self.page_count or self.first_cursor is None)
        ):
            raise ValueError("paged_content_requires_cursor")

    def to_dict(self) -> dict[str, Any]:
        return {
            "availability": self.availability,
            "inline": self.inline,
            "char_length": self.char_length,
            "byte_length": self.byte_length,
            "page_count": self.page_count,
            "first_cursor": self.first_cursor,
        }


@dataclass(frozen=True)
class TurnContentDescriptor:
    """Immutable v1 description of a canonical turn content revision."""

    schema_version: int
    content_revision: str
    known_incomplete: bool
    fields: Mapping[str, ContentFieldDescriptor]

    def __post_init__(self) -> None:
        if self.schema_version != TURN_CONTENT_SCHEMA_VERSION:
            raise ValueError("invalid_content_schema_version")
        if set(self.fields) != set(TURN_CONTENT_FIELDS):
            raise ValueError("invalid_content_fields")
        if not isinstance(self.known_incomplete, bool):
            raise ValueError("invalid_known_incomplete")
        if any(
            not isinstance(descriptor, ContentFieldDescriptor)
            for descriptor in self.fields.values()
        ):
            raise ValueError("invalid_content_field_descriptor")
        expected_incomplete = any(
            descriptor.availability == "known_incomplete"
            for descriptor in self.fields.values()
        )
        if self.known_incomplete != expected_incomplete:
            raise ValueError("inconsistent_known_incomplete")
        immutable = MappingProxyType(dict(self.fields))
        object.__setattr__(self, "fields", immutable)

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "content_revision": self.content_revision,
            "known_incomplete": self.known_incomplete,
            "fields": {
                field: self.fields[field].to_dict()
                for field in TURN_CONTENT_FIELDS
            },
        }


def _content_field_descriptor(
    revision: str,
    field: str,
    text: str | None,
    state: str,
    *,
    inline_max_chars: int,
) -> ContentFieldDescriptor:
    if text is None:
        return ContentFieldDescriptor(state, False, 0, 0, 0, None)
    char_length = len(text)
    if state == "complete":
        segments = segment_canonical_text(text)
        page_count = len(segments)
        byte_length = sum(segment.byte_length for segment in segments)
    else:
        page_count = 0
        byte_length = len(text.encode("utf-8"))
    inline = state == "complete" and char_length <= inline_max_chars
    first_cursor = (
        content_cursor(revision, field, 0, start_char=0, start_byte=0)
        if state == "complete" and not inline and page_count
        else None
    )
    return ContentFieldDescriptor(
        availability=state,
        inline=inline,
        char_length=char_length,
        byte_length=byte_length,
        page_count=page_count,
        first_cursor=first_cursor,
    )


def build_turn_content_descriptor(
    turn_id: str,
    user_text: str | None,
    final_text: str | None,
    *,
    user_state: str | None = None,
    final_state: str | None = None,
    inline_max_chars: int = TURN_TEXT_MAX_CHARS,
) -> TurnContentDescriptor:
    """Describe one canonical revision without copying its content."""
    if (
        not isinstance(inline_max_chars, int)
        or isinstance(inline_max_chars, bool)
        or inline_max_chars < 0
    ):
        raise ValueError("inline_max_chars must be nonnegative")
    resolved_user_state = _inferred_content_state(user_text, user_state)
    resolved_final_state = _inferred_content_state(final_text, final_state)
    revision = content_revision(
        turn_id,
        user_text,
        final_text,
        resolved_user_state,
        resolved_final_state,
    )
    fields = {
        "user_text": _content_field_descriptor(
            revision,
            "user_text",
            user_text,
            resolved_user_state,
            inline_max_chars=inline_max_chars,
        ),
        "assistant_final_text": _content_field_descriptor(
            revision,
            "assistant_final_text",
            final_text,
            resolved_final_state,
            inline_max_chars=inline_max_chars,
        ),
    }
    return TurnContentDescriptor(
        schema_version=TURN_CONTENT_SCHEMA_VERSION,
        content_revision=revision,
        known_incomplete=(
            resolved_user_state == "known_incomplete"
            or resolved_final_state == "known_incomplete"
        ),
        fields=fields,
    )


def project_turn_content(
    turn_id: str,
    user_text: str | None,
    final_text: str | None,
    *,
    user_state: str | None = None,
    final_state: str | None = None,
    inline_max_chars: int = TURN_TEXT_MAX_CHARS,
    preview_max_chars: int = TURN_CONTENT_PREVIEW_MAX_CHARS,
) -> dict[str, Any]:
    """Return the bounded v2 inline/preview projection for canonical content."""
    if (
        not isinstance(preview_max_chars, int)
        or isinstance(preview_max_chars, bool)
        or preview_max_chars < 0
    ):
        raise ValueError("preview_max_chars must be nonnegative")
    descriptor = build_turn_content_descriptor(
        turn_id,
        user_text,
        final_text,
        user_state=user_state,
        final_state=final_state,
        inline_max_chars=inline_max_chars,
    )
    projected: dict[str, Any] = {"content": descriptor.to_dict()}
    values = {
        "user_text": (user_text, "user_preview"),
        "assistant_final_text": (final_text, "assistant_final_preview"),
    }
    for field, (text, preview_key) in values.items():
        field_descriptor = descriptor.fields[field]
        if field_descriptor.inline:
            projected[field] = text
        elif text is not None:
            projected[preview_key] = text[:preview_max_chars]
    return projected

def project_persisted_turn_content(
    revision: str,
    *,
    user_state: str,
    user_char_length: int,
    user_byte_length: int,
    user_page_count: int,
    user_inline: str | None,
    user_preview: str | None,
    final_state: str,
    final_char_length: int,
    final_byte_length: int,
    final_page_count: int,
    final_inline: str | None,
    final_preview: str | None,
    inline_max_chars: int = TURN_TEXT_MAX_CHARS,
    preview_max_chars: int = TURN_CONTENT_PREVIEW_MAX_CHARS,
) -> dict[str, Any]:
    """Project persisted descriptors and bounded SQL text without canonical scans."""
    if (
        not isinstance(revision, str)
        or not revision
        or not isinstance(inline_max_chars, int)
        or isinstance(inline_max_chars, bool)
        or inline_max_chars < 0
        or not isinstance(preview_max_chars, int)
        or isinstance(preview_max_chars, bool)
        or preview_max_chars < 0
    ):
        raise ValueError("invalid_persisted_content_descriptor")
    inputs = {
        "user_text": (
            user_state,
            user_char_length,
            user_byte_length,
            user_page_count,
            user_inline,
            user_preview,
            "user_preview",
        ),
        "assistant_final_text": (
            final_state,
            final_char_length,
            final_byte_length,
            final_page_count,
            final_inline,
            final_preview,
            "assistant_final_preview",
        ),
    }
    fields: dict[str, ContentFieldDescriptor] = {}
    projected: dict[str, Any] = {}
    for field, (
        state,
        char_length,
        byte_length,
        page_count,
        inline_text,
        preview_text,
        preview_key,
    ) in inputs.items():
        if state not in TURN_CONTENT_AVAILABILITIES or any(
            not isinstance(value, int) or isinstance(value, bool) or value < 0
            for value in (char_length, byte_length, page_count)
        ):
            raise ValueError("invalid_persisted_content_descriptor")
        inline = state == "complete" and char_length <= inline_max_chars and char_length > 0
        if state == "absent":
            if any((char_length, byte_length, page_count)) or any(
                value is not None for value in (inline_text, preview_text)
            ):
                raise ValueError("invalid_persisted_content_descriptor")
        elif state == "known_incomplete":
            if page_count or inline_text is not None:
                raise ValueError("invalid_persisted_content_descriptor")
        elif char_length:
            if page_count < 1 or (inline and not isinstance(inline_text, str)):
                raise ValueError("invalid_persisted_content_descriptor")
        elif any((byte_length, page_count)) or inline_text not in (None, ""):
            raise ValueError("invalid_persisted_content_descriptor")
        if isinstance(inline_text, str):
            if not inline or len(inline_text) != char_length:
                raise ValueError("invalid_persisted_content_descriptor")
            projected[field] = inline_text
        elif state != "absent" and isinstance(preview_text, str):
            if len(preview_text) > preview_max_chars:
                raise ValueError("invalid_persisted_content_descriptor")
            projected[preview_key] = preview_text
        fields[field] = ContentFieldDescriptor(
            availability=state,
            inline=inline,
            char_length=char_length,
            byte_length=byte_length,
            page_count=page_count if state == "complete" else 0,
            first_cursor=(
                content_cursor(revision, field, 0, start_char=0, start_byte=0)
                if state == "complete" and not inline and page_count
                else None
            ),
        )
    descriptor = TurnContentDescriptor(
        schema_version=TURN_CONTENT_SCHEMA_VERSION,
        content_revision=revision,
        known_incomplete=any(
            descriptor.availability == "known_incomplete"
            for descriptor in fields.values()
        ),
        fields=fields,
    )
    return {"content": descriptor.to_dict(), **projected}


def build_turn_content_page(
    turn_id: str,
    revision: str,
    field: str,
    text: str,
    *,
    cursor: str | None = None,
    max_utf8_bytes: int = TURN_CONTENT_PAGE_MAX_UTF8_BYTES,
) -> dict[str, Any]:
    """Build one trusted lossless content-page payload from canonical text."""
    _validate_content_field(field)
    segments = segment_canonical_text(text, max_utf8_bytes=max_utf8_bytes)
    count = len(segments)
    if not count:
        raise ValueError("content_has_no_segments")
    position = (
        ContentCursorPosition(
            index=0,
            segment_id=content_segment_id(revision, field, 0),
            start_char=0,
            start_byte=0,
        )
        if cursor is None
        else decode_content_cursor(
            cursor,
            revision=revision,
            field=field,
            count=count,
        )
    )
    segment = segments[position.index]
    if (
        position.segment_id != content_segment_id(revision, field, segment.index)
        or position.start_char != segment.start_char
        or position.start_byte != segment.start_byte
    ):
        raise ValueError("invalid_cursor")
    return {
        "schema_version": TURN_CONTENT_SCHEMA_VERSION,
        "turn_id": str(turn_id),
        "content_revision": str(revision),
        "field": field,
        "availability": "complete",
        "segment_id": position.segment_id,
        "index": position.index,
        "count": count,
        "text": segment.text,
        "segment_char_length": segment.char_length,
        "segment_byte_length": segment.byte_length,
        "total_char_length": len(text),
        "total_byte_length": segments[-1].end_byte,
        "next_cursor": (
            content_cursor(
                revision,
                field,
                position.index + 1,
                start_char=segment.end_char,
                start_byte=segment.end_byte,
            )
            if position.index + 1 < count
            else None
        ),
    }


def _healthy_empty_pending_health() -> dict[str, Any]:
    """Return the fixed health projection for snapshot-only pending wrappers."""
    return {
        "status": "healthy",
        "counts": {"fresh": 0, "stale": 0, "total": 0},
    }


def _unavailable_pending_health() -> dict[str, Any]:
    """Return the fixed fail-closed health projection when durability is unknown."""
    return {
        "status": "store_unavailable",
        "counts": {"fresh": 0, "stale": 0, "total": 0},
    }


def recompute_pending_content_fingerprint(payload: Mapping[str, Any]) -> str:
    """Recompute a pending payload's content_fingerprint after its pending_interactions list
    is rewritten (e.g. the daemon backend overlay), matching pending_payload_from_snapshot."""
    return _content_fingerprint(
        {
            "schema_version": payload.get("schema_version", TURN_SCHEMA_VERSION),
            "host_id": payload.get("host_id"),
            "pending_interactions": payload.get("pending_interactions", []),
            "backend_health": payload.get("backend_health", []),
            "pending_health": payload.get(
                "pending_health",
                _unavailable_pending_health(),
            ),
        }
    )


def _automation_text(value: Any) -> str:
    if not isinstance(value, str):
        return ""
    return value.lstrip().replace("\r\n", "\n")


def _looks_like_automation_user_text(value: Any) -> bool:
    text = _automation_text(value)
    return bool(text and (_AUTOMATION_JOB_TEMPLATE_RE.match(text) or _AUTOMATION_VALIDATOR_RE.match(text)))


def _strip_json_fence(value: str) -> str:
    text = value.strip()
    if not text.startswith("```"):
        return text
    lines = text.splitlines()
    if len(lines) < 3 or lines[-1].strip() != "```":
        return text
    opener = lines[0].strip().lower()
    if opener not in {"```", "```json"}:
        return text
    return "\n".join(lines[1:-1]).strip()


def _looks_like_automation_result_text(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    text = _strip_json_fence(value)
    if not text.startswith("{"):
        return False
    try:
        payload = json.loads(text)
    except (TypeError, json.JSONDecodeError):
        return False
    if not isinstance(payload, Mapping) or len(payload) != 1:
        return False
    key, result = next(iter(payload.items()))
    if not isinstance(key, str) or not _AUTOMATION_RESULT_KEY_RE.match(key):
        return False
    if not isinstance(result, Mapping):
        return False
    return bool(set(result) & _AUTOMATION_RESULT_FIELDS)


def is_internal_automation_turn_payload(payload: Mapping[str, Any]) -> bool:
    """Return true for machine protocol turns that should not be public chat."""
    user_text = payload.get("user_text")
    if _looks_like_automation_user_text(user_text):
        return True
    if isinstance(user_text, str) and user_text.strip():
        return False
    return any(
        _looks_like_automation_result_text(payload.get(key))
        for key in ("assistant_final_text", "assistant_stream_text")
    )


def _public_identity(value: Any, *, prefix: str, default: str = "unknown") -> str:
    raw = _string_value(value, default).strip() or default
    text = sanitize_public_text(raw)
    if text == raw and not _contains_forbidden_public_text(text):
        return " ".join(text.split())
    return f"{prefix}-{stable_fingerprint({'type': prefix, 'raw_id': raw})}"


def _optional_public_identity(value: Any, *, prefix: str) -> str | None:
    if value is None:
        return None
    return _public_identity(value, prefix=prefix)


def _clean_public_value(value: Any) -> Any:
    return sanitize_public_value(value)


def _normalized_key(value: Any) -> str:
    return str(value).strip().lower().replace("-", "_")


def _compact_key(value: Any) -> str:
    return _normalized_key(value).replace("_", "")


def _strip_volatile(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {
            str(key): _strip_volatile(item)
            for key, item in value.items()
            if str(key).lower() not in _VOLATILE_KEYS
        }
    if isinstance(value, list | tuple):
        return [_strip_volatile(item) for item in value]
    return value


def _stable_id(prefix: str, value: Any) -> str:
    return f"{prefix}-{stable_fingerprint(_strip_volatile(sanitize_public_value(value)))}"


def _content_fingerprint(value: Any) -> str:
    return stable_fingerprint(_strip_volatile(sanitize_public_value(value)))


def _opaque_public_id(prefix: str, raw_value: Any, public_material: Any) -> str:
    raw = _string_value(raw_value).strip()
    if re.fullmatch(rf"{re.escape(prefix)}-[0-9a-f]{{24}}", raw):
        return raw
    return _stable_id(prefix, {"seed": raw, "public": public_material})


def turn_source_id_candidates(
    raw_value: Any,
    *,
    meta: Mapping[str, Any],
    source: Any,
    kind: Any,
) -> tuple[str, ...]:
    """Return canonical and compatibility source tokens in match order."""

    raw_source_turn_id = _optional_public_text(raw_value)
    if not raw_source_turn_id:
        return ()

    normalized_source = _public_text(source, default="snapshot")
    normalized_kind = _normalize_turn_kind(kind)
    legacy_token = _opaque_public_id(
        "turnsrc",
        raw_source_turn_id,
        {"source": normalized_source, "kind": normalized_kind},
    )
    owner_identity = _turn_owner_identity(meta)
    if owner_identity is None:
        return (legacy_token,)

    owner_token = _opaque_public_id(
        "turnsrc",
        raw_source_turn_id,
        {
            "identity_domain": "stable-owner-source-v1",
            "stable_key": owner_identity[0],
            "stable_key_version": owner_identity[1],
            "kind": normalized_kind,
        },
    )
    if owner_token == legacy_token:
        return (owner_token,)
    return owner_token, legacy_token


def _meta_value(meta: Mapping[str, Any], normalized_key: str) -> Any | None:
    normalized_target = _normalized_key(normalized_key)
    compact_target = normalized_target.replace("_", "")
    for key, value in meta.items():
        if _normalized_key(key) == normalized_target or _compact_key(key) == compact_target:
            return value
    return None


def _optional_public_description(value: Any) -> str | None:
    clean = sanitize_public_value(value)
    if clean in (None, {}, [], ""):
        return None
    if isinstance(clean, Mapping) or isinstance(clean, list):
        return public_json_dumps(clean)
    return _optional_string(clean)


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


def _public_choice_value(value: Any) -> Any | None:
    clean = sanitize_public_value(value)
    if clean in (None, {}, [], ""):
        return None
    if isinstance(clean, str):
        return None if _looks_like_raw_command(clean) else clean
    return clean


def _public_suggested_action_value(action: Any) -> Any | None:
    if not getattr(action, "has_public_tendwire_action", False):
        return None
    return _public_choice_value(getattr(action, "tendwire_action", ""))


def _is_pending_routing_meta_key(key: Any) -> bool:
    return _compact_key(key) in {"workerid", "spaceid"}


PendingObservationKind = Literal[
    "open_prompt",
    "read_succeeded_no_prompt",
    "read_succeeded_invalid_prompt",
    "read_succeeded_unsupported_decision",
    "read_failed",
    "worker_authoritatively_absent",
]


@dataclass(frozen=True)
class PendingObservedChoice:
    """One safe public choice plus its private 1-based picker route."""

    choice_id: str
    label: str
    picker_ordinal: int

    def __post_init__(self) -> None:
        if (
            not isinstance(self.choice_id, str)
            or re.fullmatch(r"choice-[0-9a-f]{24}", self.choice_id) is None
        ):
            raise ValueError("invalid pending observation choice id")
        if not isinstance(self.label, str) or not self.label.strip():
            raise ValueError("invalid pending observation choice label")
        if (
            not isinstance(self.picker_ordinal, int)
            or isinstance(self.picker_ordinal, bool)
            or self.picker_ordinal < 1
        ):
            raise ValueError("invalid pending observation picker ordinal")


@dataclass(frozen=True)
class PendingObservation:
    """One explicit source-read outcome with no public serializer."""

    kind: PendingObservationKind
    question: str | None = None
    pending_kind: str | None = None
    choices: tuple[PendingObservedChoice, ...] = ()
    revision_digest: str | None = None
    decision_kind: Literal["single", "multi", "plan"] | None = None
    decision_options: tuple[str, ...] = ()
    decision_multi_select: bool = False
    decision_question_count: int = 0

    def __post_init__(self) -> None:
        if self.kind not in {
            "open_prompt",
            "read_succeeded_no_prompt",
            "read_succeeded_invalid_prompt",
            "read_succeeded_unsupported_decision",
            "read_failed",
            "worker_authoritatively_absent",
        }:
            raise ValueError("invalid pending observation kind")
        if self.kind == "open_prompt":
            if not isinstance(self.question, str) or not self.question.strip():
                raise ValueError("open pending observation requires a question")
            if (
                not isinstance(self.revision_digest, str)
                or not self.revision_digest
            ):
                raise ValueError("open pending observation requires a revision digest")
            if not isinstance(self.choices, tuple) or not all(
                isinstance(choice, PendingObservedChoice) for choice in self.choices
            ):
                raise ValueError("invalid pending observation choices")
            choice_ids = [choice.choice_id for choice in self.choices]
            ordinals = [choice.picker_ordinal for choice in self.choices]
            if len(choice_ids) != len(set(choice_ids)) or len(ordinals) != len(
                set(ordinals)
            ):
                raise ValueError("pending observation choices must be unique")
            if self.pending_kind is not None and (
                not isinstance(self.pending_kind, str)
                or not self.pending_kind.strip()
            ):
                raise ValueError("invalid pending observation prompt kind")
            if self.decision_kind is None:
                if (
                    self.decision_options
                    or self.decision_multi_select
                    or self.decision_question_count != 0
                ):
                    raise ValueError("non-decision prompt cannot carry decision data")
            else:
                if self.decision_kind not in {"single", "multi", "plan"}:
                    raise ValueError("invalid pending observation decision kind")
                if (
                    not isinstance(self.decision_options, tuple)
                    or not self.decision_options
                    or not all(
                        isinstance(label, str) and label.strip()
                        for label in self.decision_options
                    )
                ):
                    raise ValueError("decision prompt requires options")
                if self.decision_multi_select is not (
                    self.decision_kind == "multi"
                ):
                    raise ValueError("decision multi-select flag does not match kind")
                if (
                    not isinstance(self.decision_question_count, int)
                    or isinstance(self.decision_question_count, bool)
                    or self.decision_question_count < 1
                ):
                    raise ValueError("invalid decision question count")
        elif (
            self.question is not None
            or self.pending_kind is not None
            or self.choices
            or self.revision_digest is not None
            or self.decision_kind is not None
            or self.decision_options
            or self.decision_multi_select
            or self.decision_question_count != 0
        ):
            raise ValueError("non-open pending observation cannot carry prompt data")


@dataclass(frozen=True)
class InteractionChoice:
    """A finite public-safe choice for a pending interaction."""

    choice_id: str = ""
    label: str = ""
    value: Any | None = None
    description: str | None = None
    params: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        label = _public_text(self.label, default="Action")
        description = _optional_public_description(self.description)
        params = _clean_meta(self.params)
        value = _public_choice_value(self.value)
        choice_material = {"label": label}
        raw_choice_id = _string_value(self.choice_id).strip()
        choice_id = (
            _opaque_public_id("choice", raw_choice_id, choice_material)
            if raw_choice_id
            else _stable_id("choice", choice_material)
        )

        object.__setattr__(self, "choice_id", choice_id)
        object.__setattr__(self, "label", label)
        object.__setattr__(self, "value", value)
        object.__setattr__(self, "description", description)
        object.__setattr__(self, "params", params)

    def to_dict(self) -> dict[str, Any]:
        return {
            "choice_id": self.choice_id,
            "label": self.label,
        }

    @classmethod
    def from_dict(cls, data: "InteractionChoice | Mapping[str, Any]") -> "InteractionChoice":
        if isinstance(data, InteractionChoice):
            return data
        clean = sanitize_forbidden_fields(data if isinstance(data, Mapping) else {})
        return cls(
            choice_id=_string_value(clean.get("choice_id")),
            label=_string_value(clean.get("label")),
            value=clean.get("value"),
            description=clean.get("description"),
            params=clean.get("params", {}),
        )


def _turn_owner_identity(meta: Mapping[str, Any]) -> tuple[str, int] | None:
    stable_key = meta.get("stable_key")
    stable_key_version = meta.get("stable_key_version")
    if (
        isinstance(stable_key, str)
        and stable_key.startswith("wsk1_")
        and len(stable_key) == 69
        and all(char in "0123456789abcdef" for char in stable_key[5:])
        and type(stable_key_version) is int
        and stable_key_version == 1
    ):
        return stable_key, 1
    return None


@dataclass(frozen=True)
class Turn:
    """A public, neutral representation of a worker turn."""

    host_id: str
    worker_id: str
    status: str = "unknown"
    kind: str = "unknown"
    source: str = "snapshot"
    worker_fingerprint: str | None = None
    space_id: str | None = None
    title: str | None = None
    summary: str | None = None
    user_text: str | None = None
    assistant_final_text: str | None = None
    assistant_stream_text: str | None = None
    model: str | None = None
    complete: bool | None = None
    has_open_turn: bool | None = None
    started_at: str | None = None
    updated_at: str | None = None
    completed_at: str | None = None
    origin_command_id: str | None = None
    source_turn_id: str | None = None
    superseded_by_turn_id: str | None = None
    superseded_at: str | None = None
    meta: dict[str, Any] = field(default_factory=dict)
    id: str = ""
    fingerprint: str = ""
    schema_version: int = TURN_SCHEMA_VERSION

    def __post_init__(self) -> None:
        host_id = _string_value(self.host_id, "unknown")
        worker_id = _public_identity(self.worker_id, prefix="worker")
        status = normalize_status(self.status)
        kind = _normalize_turn_kind(self.kind)
        source = _public_text(self.source, default="snapshot")
        worker_fingerprint = _optional_public_fingerprint(self.worker_fingerprint)
        space_id = _optional_public_identity(self.space_id, prefix="space")
        title = _optional_public_text(self.title)
        summary = _optional_public_text(self.summary)
        user_text = _public_turn_text(self.user_text)
        assistant_final_text = _public_turn_text(self.assistant_final_text)
        assistant_stream_text = _public_stream_text(self.assistant_stream_text)
        model = _optional_public_text(self.model)
        started_at = _optional_timestamp(self.started_at)
        updated_at = _optional_timestamp(self.updated_at)
        completed_at = _optional_timestamp(self.completed_at)
        origin_command_id = _optional_public_text(self.origin_command_id)
        raw_source_turn_id = _optional_public_text(self.source_turn_id)
        superseded_by_turn_id = _optional_public_text(
            self.superseded_by_turn_id
        )
        superseded_at = _optional_timestamp(self.superseded_at)
        meta = _clean_meta(self.meta)
        source_id_candidates = turn_source_id_candidates(
            raw_source_turn_id,
            meta=meta,
            source=source,
            kind=kind,
        )
        source_turn_id = source_id_candidates[0] if source_id_candidates else None
        owner_identity = _turn_owner_identity(meta)
        content_identity_payload = {
            "schema_version": TURN_SCHEMA_VERSION,
            "host_id": host_id,
            "worker_id": worker_id,
            "space_id": space_id,
            "kind": kind,
            "source": source,
            "origin_command_id": origin_command_id,
        }
        if owner_identity is not None:
            content_identity_payload["stable_key"] = owner_identity[0]
            content_identity_payload["stable_key_version"] = owner_identity[1]
        else:
            content_identity_payload["stable_key_status"] = "unavailable"
        if source_turn_id:
            content_identity_payload["source_turn_id"] = source_turn_id

        if owner_identity is not None:
            identity_payload = {
                "identity_domain": "stable-owner-turn-v1",
                "schema_version": TURN_SCHEMA_VERSION,
                "host_id": host_id,
                "stable_key": owner_identity[0],
                "stable_key_version": owner_identity[1],
                "kind": kind,
            }
            if source_turn_id:
                identity_payload["source_turn_id"] = source_turn_id
            elif origin_command_id:
                identity_payload["origin_command_id"] = origin_command_id
            else:
                identity_payload["owner_placeholder"] = True
        else:
            identity_payload = content_identity_payload

        content_payload = {
            **content_identity_payload,
            "worker_fingerprint": worker_fingerprint,
            "status": status,
            "title": title,
            "summary": summary,
            "user_text": user_text,
            "assistant_final_text": assistant_final_text,
            "assistant_stream_text": assistant_stream_text,
            "model": model,
            "complete": self.complete if isinstance(self.complete, bool) else None,
            "has_open_turn": self.has_open_turn if isinstance(self.has_open_turn, bool) else None,
            "meta": meta,
        }
        turn_id = _stable_id("turn", identity_payload)
        fingerprint = stable_fingerprint(_strip_volatile(content_payload))

        object.__setattr__(self, "schema_version", TURN_SCHEMA_VERSION)
        object.__setattr__(self, "id", turn_id)
        object.__setattr__(self, "host_id", host_id)
        object.__setattr__(self, "worker_id", worker_id)
        object.__setattr__(self, "worker_fingerprint", worker_fingerprint)
        object.__setattr__(self, "space_id", space_id)
        object.__setattr__(self, "status", status)
        object.__setattr__(self, "kind", kind)
        object.__setattr__(self, "title", title)
        object.__setattr__(self, "summary", summary)
        object.__setattr__(self, "user_text", user_text)
        object.__setattr__(self, "assistant_final_text", assistant_final_text)
        object.__setattr__(self, "assistant_stream_text", assistant_stream_text)
        object.__setattr__(self, "model", model)
        object.__setattr__(self, "complete", self.complete if isinstance(self.complete, bool) else None)
        object.__setattr__(self, "has_open_turn", self.has_open_turn if isinstance(self.has_open_turn, bool) else None)
        object.__setattr__(self, "started_at", started_at)
        object.__setattr__(self, "updated_at", updated_at)
        object.__setattr__(self, "completed_at", completed_at)
        object.__setattr__(self, "source", source)
        object.__setattr__(self, "origin_command_id", origin_command_id)
        object.__setattr__(self, "source_turn_id", source_turn_id)
        object.__setattr__(
            self,
            "superseded_by_turn_id",
            superseded_by_turn_id,
        )
        object.__setattr__(self, "superseded_at", superseded_at)
        object.__setattr__(self, "fingerprint", fingerprint)
        object.__setattr__(self, "meta", meta)

    def to_dict(self) -> dict[str, Any]:
        # Canonical prompt/final values were sanitized once in __post_init__.
        # Re-routing this trusted shape through the generic mapping sanitizer
        # would silently impose its unrelated 12,000-character value bound.
        return {
            "schema_version": self.schema_version,
            "id": self.id,
            "host_id": self.host_id,
            "worker_id": self.worker_id,
            "worker_fingerprint": self.worker_fingerprint,
            "space_id": self.space_id,
            "status": self.status,
            "kind": self.kind,
            "title": self.title,
            "summary": self.summary,
            "user_text": self.user_text,
            "assistant_final_text": self.assistant_final_text,
            "assistant_stream_text": self.assistant_stream_text,
            "model": self.model,
            "complete": self.complete,
            "has_open_turn": self.has_open_turn,
            "started_at": self.started_at,
            "updated_at": self.updated_at,
            "completed_at": self.completed_at,
            "source": self.source,
            "origin_command_id": self.origin_command_id,
            "source_turn_id": self.source_turn_id,
            "superseded_by_turn_id": self.superseded_by_turn_id,
            "superseded_at": self.superseded_at,
            "fingerprint": self.fingerprint,
            "meta": _clean_meta(self.meta),
        }

    def to_json(self, indent: int | None = None) -> str:
        return stable_json_dumps(self.to_dict(), indent=indent)

    @classmethod
    def from_dict(cls, data: "Turn | Mapping[str, Any]") -> "Turn":
        if isinstance(data, Turn):
            return data
        clean = sanitize_forbidden_fields(data if isinstance(data, Mapping) else {})
        return cls(
            id=_string_value(clean.get("id")),
            host_id=_string_value(clean.get("host_id", "unknown"), "unknown"),
            worker_id=_string_value(clean.get("worker_id", "unknown"), "unknown"),
            worker_fingerprint=clean.get("worker_fingerprint"),
            space_id=clean.get("space_id"),
            status=clean.get("status", "unknown"),
            kind=clean.get("kind", "unknown"),
            title=clean.get("title"),
            summary=clean.get("summary"),
            user_text=clean.get("user_text"),
            assistant_final_text=clean.get("assistant_final_text"),
            assistant_stream_text=clean.get("assistant_stream_text"),
            model=clean.get("model"),
            complete=clean.get("complete") if isinstance(clean.get("complete"), bool) else None,
            has_open_turn=clean.get("has_open_turn") if isinstance(clean.get("has_open_turn"), bool) else None,
            started_at=clean.get("started_at"),
            updated_at=clean.get("updated_at"),
            completed_at=clean.get("completed_at"),
            source=clean.get("source", "snapshot"),
            origin_command_id=clean.get("origin_command_id"),
            source_turn_id=clean.get("source_turn_id"),
            superseded_by_turn_id=clean.get("superseded_by_turn_id"),
            superseded_at=clean.get("superseded_at"),
            fingerprint=_string_value(clean.get("fingerprint")),
            meta=clean.get("meta", {}),
        )

    @classmethod
    def from_json(cls, payload: str) -> "Turn":
        return cls.from_dict(json.loads(payload))


@dataclass(frozen=True)
class PendingInteraction:
    """A public, neutral human interaction request."""

    host_id: str
    worker_id: str
    question: str
    kind: str = "unknown"
    choices: list[InteractionChoice] = field(default_factory=list)
    status: str = "open"
    worker_fingerprint: str | None = None
    space_id: str | None = None
    created_at: str | None = None
    updated_at: str | None = None
    expires_at: str | None = None
    meta: dict[str, Any] = field(default_factory=dict)
    id: str = ""
    fingerprint: str = ""
    schema_version: int = TURN_SCHEMA_VERSION

    def __post_init__(self) -> None:
        host_id = _string_value(self.host_id, "unknown")
        worker_id = _public_identity(self.worker_id, prefix="worker")
        worker_fingerprint = _optional_public_fingerprint(self.worker_fingerprint)
        space_id = _optional_public_identity(self.space_id, prefix="space")
        kind = _normalize_pending_kind(self.kind)
        question = _public_text(self.question, default="Action requires attention")
        choices = [
            choice if isinstance(choice, InteractionChoice) else InteractionChoice.from_dict(choice)
            for choice in self.choices
        ]
        status = _normalize_pending_status(self.status)
        created_at = _optional_timestamp(self.created_at)
        updated_at = _optional_timestamp(self.updated_at)
        expires_at = _optional_timestamp(self.expires_at)
        meta = _clean_meta(self.meta)
        identity_payload = {
            "schema_version": TURN_SCHEMA_VERSION,
            "host_id": host_id,
            "worker_id": worker_id,
            "space_id": space_id,
            "kind": kind,
            "question": question,
            "choice_ids": [choice.choice_id for choice in choices],
            "source": _meta_value(meta, "source") or _meta_value(meta, "attention_id"),
        }
        content_payload = {
            **identity_payload,
            "worker_fingerprint": worker_fingerprint,
            "choices": [choice.to_dict() for choice in choices],
            "status": status,
            "meta": meta,
        }
        raw_interaction_id = _string_value(self.id).strip()
        interaction_id = (
            raw_interaction_id
            if re.fullmatch(r"pending-[0-9a-f]{24}", raw_interaction_id)
            else _stable_id("pending", identity_payload)
        )
        fingerprint = _content_fingerprint(content_payload)

        object.__setattr__(self, "schema_version", TURN_SCHEMA_VERSION)
        object.__setattr__(self, "id", interaction_id)
        object.__setattr__(self, "host_id", host_id)
        object.__setattr__(self, "worker_id", worker_id)
        object.__setattr__(self, "worker_fingerprint", worker_fingerprint)
        object.__setattr__(self, "space_id", space_id)
        object.__setattr__(self, "kind", kind)
        object.__setattr__(self, "question", question)
        object.__setattr__(self, "choices", list(choices))
        object.__setattr__(self, "status", status)
        object.__setattr__(self, "created_at", created_at)
        object.__setattr__(self, "updated_at", updated_at)
        object.__setattr__(self, "expires_at", expires_at)
        object.__setattr__(self, "fingerprint", fingerprint)
        object.__setattr__(self, "meta", meta)

    def to_dict(self) -> dict[str, Any]:
        return sanitize_public_mapping({
            "schema_version": self.schema_version,
            "id": self.id,
            "host_id": self.host_id,
            "worker_id": self.worker_id,
            "worker_fingerprint": self.worker_fingerprint,
            "space_id": self.space_id,
            "kind": self.kind,
            "question": self.question,
            "choices": [choice.to_dict() for choice in self.choices],
            "status": self.status,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "expires_at": self.expires_at,
            "fingerprint": self.fingerprint,
            "meta": _clean_meta(self.meta),
        })

    def to_json(self, indent: int | None = None) -> str:
        return public_json_dumps(self.to_dict(), indent=indent)

    @classmethod
    def from_dict(cls, data: "PendingInteraction | Mapping[str, Any]") -> "PendingInteraction":
        if isinstance(data, PendingInteraction):
            return data
        clean = sanitize_forbidden_fields(data if isinstance(data, Mapping) else {})
        return cls(
            id=_string_value(clean.get("id")),
            host_id=_string_value(clean.get("host_id", "unknown"), "unknown"),
            worker_id=_string_value(clean.get("worker_id", "unknown"), "unknown"),
            worker_fingerprint=clean.get("worker_fingerprint"),
            space_id=clean.get("space_id"),
            kind=clean.get("kind", "unknown"),
            question=_string_value(clean.get("question")),
            choices=[InteractionChoice.from_dict(choice) for choice in clean.get("choices", [])],
            status=clean.get("status", "open"),
            created_at=clean.get("created_at"),
            updated_at=clean.get("updated_at"),
            expires_at=clean.get("expires_at"),
            fingerprint=_string_value(clean.get("fingerprint")),
            meta=clean.get("meta", {}),
        )

    @classmethod
    def from_json(cls, payload: str) -> "PendingInteraction":
        return cls.from_dict(json.loads(payload))


def _worker_origin_command_id(worker: Worker) -> str | None:
    value = _meta_value(worker.meta, "origin_command_id")
    return _optional_string(value)


def turns_from_snapshot(snapshot: Snapshot) -> list[Turn]:
    """Derive deterministic public turns from public snapshot workers."""
    turns: list[Turn] = []
    for worker in snapshot.workers:
        worker_meta = _clean_meta(worker.meta)
        turns.append(
            Turn(
                host_id=snapshot.host_id,
                worker_id=worker.id,
                worker_fingerprint=worker.fingerprint,
                space_id=worker.space_id,
                status=worker.status,
                kind="task",
                title=worker.name,
                summary=worker.summary,
                updated_at=worker.last_seen_at,
                source=f"worker:{worker.id}",
                origin_command_id=_worker_origin_command_id(worker),
                meta=worker_meta,
            )
        )
    return sorted(turns, key=lambda turn: (turn.id, turn.fingerprint))


def _signal_worker_id(signal: AttentionSignal) -> str | None:
    worker_id = _optional_string(_meta_value(signal.meta, "worker_id"))
    if worker_id:
        return worker_id
    source = signal.source
    if source.startswith("worker:"):
        return source.split(":", 1)[1] or None
    return None


def _signal_is_human_actionable(signal: AttentionSignal) -> bool:
    if signal.suggested_actions:
        return True
    for key, value in signal.meta.items():
        if str(key).strip().lower().replace("-", "_") in _HUMAN_META_KEYS and _truthy(value):
            return True
    reason = signal.reason.strip()
    return bool(reason and (_APPROVAL_RE.search(reason) or _QUESTION_RE.search(reason) or _REVIEW_RE.search(reason)))


def _kind_from_signal(signal: AttentionSignal) -> str:
    text_parts = [signal.kind, signal.reason]
    for action in signal.suggested_actions:
        public_action_value = _public_suggested_action_value(action)
        text_parts.append(action.label)
        if isinstance(public_action_value, str):
            text_parts.append(public_action_value)
    text = " ".join(part for part in text_parts if part)
    explicit_kind = _normalize_pending_kind(_meta_value(signal.meta, "interaction_kind"))
    if explicit_kind != "unknown":
        return explicit_kind
    if _DESTRUCTIVE_RE.search(text):
        return "confirm_destructive_action"
    if _APPROVAL_RE.search(text):
        return "approval"
    if signal.suggested_actions:
        return "choice"
    if _REVIEW_RE.search(text):
        return "review"
    if _QUESTION_RE.search(text):
        return "question"
    return "review"


def _choices_from_signal(signal: AttentionSignal) -> list[InteractionChoice]:
    choices: list[InteractionChoice] = []
    for action in signal.suggested_actions:
        public_value = _public_suggested_action_value(action)
        label = action.label or (public_value if isinstance(public_value, str) else "") or "Action"
        choice_id = action.action_id if public_value is not None else ""
        choices.append(
            InteractionChoice(
                choice_id=choice_id,
                label=label,
                value=public_value,
                params=action.params,
            )
        )
    return sorted(choices, key=lambda choice: (choice.choice_id, choice.label))


def _pending_status_from_signal(signal: AttentionSignal) -> str:
    explicit = _meta_value(signal.meta, "pending_status")
    if explicit is not None:
        return _normalize_pending_status(explicit)
    normalized = normalize_status(signal.status)
    if normalized in {"done", "closed"}:
        return "answered"
    if normalized == "failed":
        return "open"
    return "open"


def _pending_public_meta_from_signal(signal: AttentionSignal) -> dict[str, Any]:
    meta = _clean_meta(
        {
            "attention_id": signal.id,
            "attention_kind": signal.kind,
            "attention_severity": signal.severity,
            "attention_status": signal.status,
            "source": signal.source,
        }
    )
    for key, value in _clean_meta(signal.meta).items():
        if _is_pending_routing_meta_key(key):
            continue
        meta[str(key)] = value
    return _clean_meta(meta)


def pending_from_snapshot(snapshot: Snapshot) -> list[PendingInteraction]:
    """Derive deterministic public pending interactions from attention signals."""
    workers = {worker.id: worker for worker in snapshot.workers}
    interactions: list[PendingInteraction] = []
    for signal in snapshot.attention:
        if not _signal_is_human_actionable(signal):
            continue
        worker_id = _signal_worker_id(signal)
        if not worker_id:
            continue
        worker = workers.get(worker_id)
        space_id = _optional_string(_meta_value(signal.meta, "space_id"))
        if space_id is None and worker is not None:
            space_id = worker.space_id
        meta = _pending_public_meta_from_signal(signal)
        interactions.append(
            PendingInteraction(
                host_id=snapshot.host_id,
                worker_id=worker_id,
                worker_fingerprint=worker.fingerprint if worker is not None else None,
                space_id=space_id,
                kind=_kind_from_signal(signal),
                question=signal.reason,
                choices=_choices_from_signal(signal),
                status=_pending_status_from_signal(signal),
                created_at=signal.updated_at,
                updated_at=signal.updated_at,
                meta=meta,
            )
        )
    return sorted(interactions, key=lambda item: (item.id, item.fingerprint))


def _backend_health_payload(backend_health: Iterable[BackendHealth]) -> list[dict[str, Any]]:
    return [BackendHealth.from_dict(health).to_dict() for health in backend_health]


def turns_payload_from_snapshot(
    snapshot: Snapshot,
    *,
    schema_version: int = TURN_SCHEMA_VERSION,
) -> dict[str, Any]:
    """Return a negotiated bounded public turn-list projection.

    Legacy v1 remains available only when every canonical field is safely
    inline. Callers requesting v2 receive explicit content metadata/previews.
    """
    if schema_version not in {TURN_SCHEMA_VERSION, TURN_LIST_SCHEMA_VERSION}:
        raise ValueError("unsupported_turn_schema_version")
    turns: list[dict[str, Any]] = []
    for turn in turns_from_snapshot(snapshot):
        item = turn.to_dict()
        if schema_version == TURN_SCHEMA_VERSION:
            if any(
                isinstance(item.get(field), str)
                and len(item[field]) > TURN_TEXT_MAX_CHARS
                for field in TURN_CONTENT_FIELDS
            ):
                raise ValueError("upgrade_required")
        else:
            user_text = item.pop("user_text")
            final_text = item.pop("assistant_final_text")
            item.update(project_turn_content(turn.id, user_text, final_text))
        turns.append(item)
    backend_health = _backend_health_payload(snapshot.backend_health)
    payload = {
        "schema_version": schema_version,
        "host_id": snapshot.host_id,
        "updated_at": snapshot.updated_at,
        "turns": turns,
        "backend_health": backend_health,
    }
    payload["content_fingerprint"] = stable_fingerprint(
        _strip_volatile(
            {
                "schema_version": payload["schema_version"],
                "host_id": payload["host_id"],
                "turns": turns,
                "backend_health": backend_health,
            }
        )
    )
    return payload


def pending_payload_from_snapshot(snapshot: Snapshot) -> dict[str, Any]:
    """Return the public JSON wrapper for projected pending interactions."""
    pending = [interaction.to_dict() for interaction in pending_from_snapshot(snapshot)]
    backend_health = _backend_health_payload(snapshot.backend_health)
    pending_health = _healthy_empty_pending_health()
    payload = {
        "schema_version": TURN_SCHEMA_VERSION,
        "host_id": snapshot.host_id,
        "updated_at": snapshot.updated_at,
        "pending_interactions": pending,
        "backend_health": backend_health,
        "pending_health": pending_health,
    }
    payload["content_fingerprint"] = _content_fingerprint(
        {
            "schema_version": payload["schema_version"],
            "host_id": payload["host_id"],
            "pending_interactions": pending,
            "backend_health": backend_health,
            "pending_health": pending_health,
        }
    )
    return sanitize_public_mapping(payload)


def payload_to_json(payload: Mapping[str, Any], *, indent: int | None = None) -> str:
    """Serialize a turn/pending wrapper using Tendwire stable JSON."""
    return public_json_dumps(payload, indent=indent)
