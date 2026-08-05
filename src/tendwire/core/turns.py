"""Durable turn cursors, content paging, and pending-interaction contracts."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import re
import time
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, Literal

from .models import (
    _contains_forbidden_public_text,
    _looks_like_raw_command,
    _optional_public_safe_identity,
    _public_safe_fingerprint,
    _public_safe_identity,
    sanitize_public_mapping,
    sanitize_public_text,
    sanitize_public_value,
    sanitize_forbidden_fields,
    stable_fingerprint,
    stable_json_dumps,
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

PENDING_KINDS = frozenset("approval question choice review confirm_destructive_action unknown".split())
PENDING_STATUSES = frozenset({"open", "answered", "cancelled", "expired", "unknown"})

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
def _normalize_pending_kind(kind: Any) -> str:
    raw = _string_value(kind, "unknown").strip().lower()
    return raw if raw in PENDING_KINDS else "unknown"


def _normalize_pending_status(status: Any) -> str:
    raw = _string_value(status, "unknown").strip().lower()
    return raw if raw in PENDING_STATUSES else "unknown"


def _clean_meta(value: Any) -> dict[str, Any]:
    return sanitize_public_mapping(value if isinstance(value, Mapping) else {})


def _public_text(value: Any, *, default: str = "") -> str:
    text = sanitize_public_text(_string_value(value).strip())
    if not text or _contains_forbidden_public_text(text) or _looks_like_raw_command(text):
        return default
    return " ".join(text.split())


def _optional_public_fingerprint(value: Any) -> str | None:
    return _public_safe_fingerprint(value) or None


def _base64url(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")


def _encode_token(prefix: str, body: Mapping[str, Any]) -> str:
    return prefix + _base64url(stable_json_dumps(body).encode("utf-8"))


def _decode_token(
    token: str, prefix: str, status: str, *, max_encoded: int = 2048,
) -> dict[str, Any]:
    if not isinstance(token, str) or not token.startswith(prefix):
        raise ValueError(status)
    encoded = token.removeprefix(prefix)
    if not encoded or len(encoded) > max_encoded or not re.fullmatch(r"[\w-]+", encoded, re.ASCII):
        raise ValueError(status)
    try:
        raw = base64.b64decode(encoded + "=" * (-len(encoded) % 4), altchars=b"-_", validate=True)
        if _base64url(raw) != encoded:
            raise ValueError("noncanonical token encoding")
        body = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, ValueError, json.JSONDecodeError):
        raise ValueError(status) from None
    if not isinstance(body, dict):
        raise ValueError(status)
    return body


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
    clean_turn_id = str(turn_id)
    if not clean_turn_id:
        raise ValueError("invalid_turn_id")
    _validate_content_state(user_state, user_text)
    _validate_content_state(final_state, final_text)
    digest = _domain_digest("tendwire.turn-content-revision.v1", {
        "turn_id": clean_turn_id, "user_text": user_text,
        "assistant_final_text": final_text, "user_state": user_state,
        "final_state": final_state,
    })
    return f"twrev1.{digest}"


def turn_final_delivery_identity(
    host_id: str,
    turn_id: str,
    content_revision: str,
) -> str:
    clean_host_id, clean_turn_id, clean_revision = map(str, (host_id, turn_id, content_revision))
    if not clean_host_id:
        raise ValueError("invalid_host_id")
    if not clean_turn_id:
        raise ValueError("invalid_turn_id")
    if not clean_revision:
        raise ValueError("invalid_content_revision")
    digest = _domain_digest("tendwire.turn-final-delivery.v1", {
        "host_id": clean_host_id, "turn_id": clean_turn_id,
        "content_revision": clean_revision,
    })
    return f"twfinal1.{digest}"


def content_segment_id(revision: str, field: str, index: int) -> str:
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
    _validate_content_field(field)
    if start_char is None or start_byte is None:
        if index != 0 or start_char is not None or start_byte is not None:
            raise ValueError("invalid_cursor")
        start_char = 0
        start_byte = 0
    coordinates = (index, start_char, start_byte)
    if any(not isinstance(value, int) or isinstance(value, bool) or value < 0 for value in coordinates):
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
    integrity = _domain_digest("tendwire.turn-content-cursor-integrity.v2", material)
    return _encode_token(
        "twcur1.", {
            "b": start_byte, "c": start_char, "h": integrity,
            "i": index, "s": segment_id, "v": 2,
        },
    )


def decode_content_cursor(
    cursor: str,
    *,
    revision: str,
    field: str,
    count: int,
) -> ContentCursorPosition:
    _validate_content_field(field)
    if not isinstance(count, int) or isinstance(count, bool) or count < 1:
        raise ValueError("invalid_cursor")
    body = _decode_token(cursor, "twcur1.", "invalid_cursor", max_encoded=1024)
    if (
        not isinstance(body, dict)
        or set(body) != {"b", "c", "h", "i", "s", "v"}
        or body.get("v") != 2
    ):
        raise ValueError("invalid_cursor")
    index, start_char, start_byte, segment_id, integrity = (
        body.get(key) for key in ("i", "c", "b", "s", "h")
    )
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
    schema_version: int
    watermark: int
    store_epoch: str


@dataclass(frozen=True)
class TurnDeltaWatermarkPosition:
    schema_version: int
    projection_schema_version: int
    sequence: int
    store_epoch: str


@dataclass(frozen=True)
class TurnDeltaCursorPosition:
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


def _turn_list_nonnegative_integer(value: Any, status: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise ValueError(status)
    return value


def _list_cursor_material(
    host: str, schema: int, page_limit: int, since: int, high: int, floor: int,
    generation: int, worker: str, sequence: int, turn: str, epoch: str, expiry: int,
) -> dict[str, Any]:
    return {
        "expires_at": expiry, "floor_sequence": floor,
        "traversal_generation": generation, "host_id": host, "limit": page_limit,
        "list_sequence": sequence, "schema_version": schema,
        "since_sequence": since, "store_epoch": epoch, "turn_id": turn,
        "watermark": high, "worker_id": worker,
    }


def _delta_cursor_material(
    host_digest: str, mode: str, schema: int, projection: int, limit: int,
    accepted: int, batch_high: int, insertion_high: int, page_number: int,
    position: int, worker: str, turn: str, epoch: str, expiry: int,
) -> dict[str, Any]:
    return {
        "accepted_sequence": accepted, "batch_high": batch_high,
        "expires_at": expiry, "host_digest": host_digest,
        "insertion_high": insertion_high, "limit": limit, "mode": mode,
        "page_number": page_number, "position_sequence": position,
        "position_turn_id": turn, "position_worker_id": worker,
        "projection_schema_version": projection, "schema_version": schema,
        "store_epoch": epoch, "token_version": 1,
    }


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
    status = "invalid_cursor"
    host, worker, turn, epoch = map(str, (host_id, worker_id, turn_id, store_epoch))
    if not all((host, worker, turn, epoch)) or max(map(len, (host, worker, turn, epoch))) > 512:
        raise ValueError(status)
    schema, page_limit, since_value, high, floor, generation, sequence, expiry = (
        _turn_list_nonnegative_integer(value, status) for value in (
            schema_version, limit, since_sequence, watermark, floor_sequence,
            traversal_generation, list_sequence, expires_at,
        )
    )
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
    material = _list_cursor_material(host, schema, page_limit, since_value, high, floor,
                                     generation, worker, sequence, turn, epoch, expiry)
    return _encode_token(
        "twlist1.", {
            "e": expiry,
            "f": floor,
            "g": generation,
            "h": _domain_digest("tendwire.turn-list-cursor-integrity.v1", material),
            "l": page_limit,
            "p": [worker, sequence, turn],
            "q": epoch,
            "s": since_value,
            "v": 1,
            "w": high,
            "x": schema,
            "z": host,
        },
    )


def decode_turn_list_cursor(
    cursor: str,
    *,
    host_id: str,
    schema_version: int,
    limit: int,
    now: float | int | None = None,
) -> TurnListCursorPosition:
    status = "invalid_cursor"
    body = _decode_token(cursor, "twlist1.", status)
    if set(body) != set("efghlpqsvwxz") or body.get("v") != 1:
        raise ValueError(status)
    position = body.get("p")
    if (
        not isinstance(position, list) or len(position) != 3
        or not all(isinstance(value, str) and value for value in (position[0], position[2], body.get("h"), body.get("q"), body.get("z")))
    ):
        raise ValueError(status)
    expiry, floor, generation, page_limit, sequence, since_value, high, schema = (
        _turn_list_nonnegative_integer(value, status) for value in (
            body.get("e"), body.get("f"), body.get("g"), body.get("l"),
            position[1], body.get("s"), body.get("w"), body.get("x"),
        )
    )
    host, worker, turn, epoch = map(str, (body["z"], position[0], position[2], body["q"]))
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
    material = _list_cursor_material(host, schema, page_limit, since_value, high, floor,
                                     generation, worker, sequence, turn, epoch, expiry)
    expected = _domain_digest("tendwire.turn-list-cursor-integrity.v1", material)
    if not hmac.compare_digest(str(body["h"]), expected):
        raise ValueError(status)
    current = time.time() if now is None else float(now)
    if not current < expiry:
        raise ValueError("cursor_expired")
    return TurnListCursorPosition(schema, page_limit, since_value, high, floor, generation,
                                  worker, sequence, turn, epoch, expiry)


def turn_since_token(
    host_id: str,
    *,
    schema_version: int,
    watermark: int,
    store_epoch: str,
) -> str:
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
    material = {"host_id": host, "schema_version": schema, "store_epoch": epoch, "watermark": high}
    return _encode_token(
        "twsince1.", {
            "h": _domain_digest("tendwire.turn-list-since-integrity.v1", material),
            "q": epoch,
            "v": 1,
            "w": high,
            "x": schema,
            "z": host,
        },
    )


def decode_turn_since_token(
    token: str,
    *,
    host_id: str,
    schema_version: int,
) -> TurnSincePosition:
    status = "invalid_cursor"
    body = _decode_token(token, "twsince1.", status)
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
    material = {"host_id": host, "schema_version": schema, "store_epoch": epoch, "watermark": high}
    expected = _domain_digest("tendwire.turn-list-since-integrity.v1", material)
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
        "host_digest": host_digest, "projection_schema_version": projection,
        "schema_version": schema, "sequence": accepted, "store_epoch": epoch,
        "token_version": 1,
    }
    return _encode_token(
        "twdelta1.", {
            "h": _domain_digest("tendwire.turn-delta-watermark-integrity.v1", material),
            "p": projection,
            "q": epoch,
            "s": accepted,
            "v": 1,
            "x": schema,
            "z": host_digest,
        },
    )


def decode_turn_delta_watermark(
    token: str,
    *,
    host_id: str,
    schema_version: int = TURN_DELTA_SCHEMA_VERSION,
    projection_schema_version: int = TURN_DELTA_PROJECTION_SCHEMA_VERSION,
) -> TurnDeltaWatermarkPosition:
    status = "invalid_watermark"
    body = _decode_token(token, "twdelta1.", status)
    if set(body) != {"h", "p", "q", "s", "v", "x", "z"} or body.get("v") != 1:
        raise ValueError(status)
    if any(not isinstance(body.get(key), str) or not body.get(key) for key in ("h", "q", "z")):
        raise ValueError(status)
    schema, projection, accepted = (
        _turn_list_nonnegative_integer(body.get(key), status) for key in "xps"
    )
    epoch = str(body["q"])
    host_digest = str(body["z"])
    if len(epoch) > 512 or len(host_digest) > 512:
        raise ValueError(status)
    material = {
        "host_digest": host_digest, "projection_schema_version": projection,
        "schema_version": schema, "sequence": accepted, "store_epoch": epoch,
        "token_version": 1,
    }
    expected_integrity = _domain_digest("tendwire.turn-delta-watermark-integrity.v1", material)
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
    parsed = {key: _turn_list_nonnegative_integer(value, status) for key, value in values.items()}
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
    material = _delta_cursor_material(
        host_digest, mode, parsed["schema"], parsed["projection"], parsed["limit"], parsed["accepted"],
        parsed["batch_high"], parsed["insertion_high"], parsed["page_number"], parsed["position"],
        worker, turn, epoch, parsed["expiry"],
    )
    return _encode_token(
        "twdeltac1.", {
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
        },
    )


def decode_turn_delta_cursor(
    cursor: str,
    *,
    host_id: str,
    limit: int,
    now: float | int | None = None,
    schema_version: int = TURN_DELTA_SCHEMA_VERSION,
    projection_schema_version: int = TURN_DELTA_PROJECTION_SCHEMA_VERSION,
) -> TurnDeltaCursorPosition:
    status = "invalid_cursor"
    body = _decode_token(cursor, "twdeltac1.", status)
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
    accepted, batch_high, expiry, insertion_high, page_limit, page_number, projection, schema = (
        _turn_list_nonnegative_integer(body.get(key), status) for key in "abeilnrx"
    )
    position_sequence = _turn_list_nonnegative_integer(position[1], status)
    mode, worker, turn, epoch, host_digest = map(
        str, (body["m"], position[0], position[2], body["q"], body["z"])
    )
    material = _delta_cursor_material(host_digest, mode, schema, projection, page_limit, accepted,
                                      batch_high, insertion_high, page_number, position_sequence,
                                      worker, turn, epoch, expiry)
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
        schema, projection, mode, page_limit, accepted, batch_high, insertion_high,
        page_number, position_sequence, worker, turn, epoch, expiry,
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
    index: int
    start_char: int
    end_char: int
    start_byte: int
    end_byte: int
    text: str
    char_length: int
    byte_length: int


def _content_segment(
    index: int, text: str, start: int, end: int, start_byte: int, byte_length: int,
) -> ContentSegment:
    return ContentSegment(index, start, end, start_byte, start_byte + byte_length,
                          text[start:end], end - start, byte_length)

def segment_canonical_text(
    text: str,
    *,
    max_utf8_bytes: int = TURN_CONTENT_PAGE_MAX_UTF8_BYTES,
) -> tuple[ContentSegment, ...]:
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
            segments.append(_content_segment(len(segments), text, start, offset,
                                             start_byte, byte_length))
            start = offset
            start_byte += byte_length
            byte_length = 0
        byte_length += width
    segments.append(_content_segment(len(segments), text, start, len(text),
                                     start_byte, byte_length))
    return tuple(segments)


def _content_field_descriptor(
    revision: str,
    field: str,
    text: str | None,
    state: str,
    *,
    inline_max_chars: int,
) -> dict[str, Any]:
    char_length = len(text) if text is not None else 0
    complete = state == "complete"
    page_count = len(segment_canonical_text(text)) if complete and text else 0
    inline = complete and char_length <= inline_max_chars
    return {
        "availability": state, "inline": inline, "char_length": char_length,
        "byte_length": len(text.encode("utf-8")) if text is not None else 0,
        "page_count": page_count,
        "first_cursor": content_cursor(revision, field, 0, start_char=0, start_byte=0)
        if complete and not inline and page_count else None,
    }


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
    if not isinstance(inline_max_chars, int) or isinstance(inline_max_chars, bool) or inline_max_chars < 0:
        raise ValueError("inline_max_chars must be nonnegative")
    if not isinstance(preview_max_chars, int) or isinstance(preview_max_chars, bool) or preview_max_chars < 0:
        raise ValueError("preview_max_chars must be nonnegative")
    states = (_inferred_content_state(user_text, user_state),
              _inferred_content_state(final_text, final_state))
    revision = content_revision(turn_id, user_text, final_text, *states)
    values = {
        "user_text": (user_text, states[0], "user_preview"),
        "assistant_final_text": (final_text, states[1], "assistant_final_preview"),
    }
    fields = {
        name: _content_field_descriptor(revision, name, text, state,
                                        inline_max_chars=inline_max_chars)
        for name, (text, state, _preview) in values.items()
    }
    projected: dict[str, Any] = {"content": {
        "schema_version": TURN_CONTENT_SCHEMA_VERSION, "content_revision": revision,
        "known_incomplete": "known_incomplete" in states, "fields": fields,
    }}
    for field, (text, _state, preview_key) in values.items():
        if fields[field]["inline"]:
            projected[field] = text
        elif text is not None:
            projected[preview_key] = text[:preview_max_chars]
    return projected

def build_turn_content_page(
    turn_id: str,
    revision: str,
    field: str,
    text: str,
    *,
    cursor: str | None = None,
    max_utf8_bytes: int = TURN_CONTENT_PAGE_MAX_UTF8_BYTES,
) -> dict[str, Any]:
    _validate_content_field(field)
    segments = segment_canonical_text(text, max_utf8_bytes=max_utf8_bytes)
    count = len(segments)
    if not count:
        raise ValueError("content_has_no_segments")
    position = (ContentCursorPosition(0, content_segment_id(revision, field, 0), 0, 0)
                if cursor is None else decode_content_cursor(
                    cursor, revision=revision, field=field, count=count))
    segment = segments[position.index]
    if (
        position.segment_id != content_segment_id(revision, field, segment.index)
        or position.start_char != segment.start_char
        or position.start_byte != segment.start_byte
    ):
        raise ValueError("invalid_cursor")
    return {
        "schema_version": TURN_CONTENT_SCHEMA_VERSION, "turn_id": str(turn_id),
        "content_revision": str(revision), "field": field, "availability": "complete",
        "segment_id": position.segment_id, "index": position.index, "count": count,
        "text": segment.text, "segment_char_length": segment.char_length,
        "segment_byte_length": segment.byte_length, "total_char_length": len(text),
        "total_byte_length": segments[-1].end_byte,
        "next_cursor": (
            content_cursor(revision, field, position.index + 1,
                           start_char=segment.end_char, start_byte=segment.end_byte)
            if position.index + 1 < count else None
        ),
    }


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


def _meta_value(meta: Mapping[str, Any], normalized_key: str) -> Any | None:
    normalized_target = _normalized_key(normalized_key)
    compact_target = normalized_target.replace("_", "")
    for key, value in meta.items():
        if _normalized_key(key) == normalized_target or _compact_key(key) == compact_target:
            return value
    return None


PENDING_OBSERVATION_KINDS = frozenset(
    "open_prompt read_succeeded_no_prompt read_succeeded_invalid_prompt "
    "read_succeeded_unsupported_decision read_failed worker_authoritatively_absent".split()
)


@dataclass(frozen=True)
class PendingObservedChoice:
    choice_id: str
    label: str
    picker_ordinal: int

    def __post_init__(self) -> None:
        if not isinstance(self.choice_id, str) or re.fullmatch(r"choice-[0-9a-f]{24}", self.choice_id) is None:
            raise ValueError("invalid pending observation choice id")
        if not isinstance(self.label, str) or not self.label.strip():
            raise ValueError("invalid pending observation choice label")
        if not isinstance(self.picker_ordinal, int) or isinstance(self.picker_ordinal, bool) or self.picker_ordinal < 1:
            raise ValueError("invalid pending observation picker ordinal")


@dataclass(frozen=True)
class PendingObservation:
    kind: str
    question: str | None = None
    pending_kind: str | None = None
    choices: tuple[PendingObservedChoice, ...] = ()
    revision_digest: str | None = None
    decision_kind: Literal["single", "multi", "plan"] | None = None
    decision_options: tuple[str, ...] = ()
    decision_multi_select: bool = False
    decision_question_count: int = 0

    def __post_init__(self) -> None:
        if self.kind not in PENDING_OBSERVATION_KINDS:
            raise ValueError("invalid pending observation kind")
        payload = (self.question, self.pending_kind, self.choices, self.revision_digest,
                   self.decision_kind, self.decision_options, self.decision_multi_select,
                   self.decision_question_count)
        if self.kind != "open_prompt":
            if payload != (None, None, (), None, None, (), False, 0):
                raise ValueError("non-open pending observation cannot carry prompt data")
            return
        if not isinstance(self.question, str) or not self.question.strip():
            raise ValueError("open pending observation requires a question")
        if not isinstance(self.revision_digest, str) or not self.revision_digest:
            raise ValueError("open pending observation requires a revision digest")
        if not isinstance(self.choices, tuple) or not all(isinstance(c, PendingObservedChoice) for c in self.choices):
            raise ValueError("invalid pending observation choices")
        choice_ids = [choice.choice_id for choice in self.choices]
        ordinals = [choice.picker_ordinal for choice in self.choices]
        if len(choice_ids) != len(set(choice_ids)) or len(ordinals) != len(set(ordinals)):
            raise ValueError("pending observation choices must be unique")
        if self.pending_kind is not None and (not isinstance(self.pending_kind, str) or not self.pending_kind.strip()):
            raise ValueError("invalid pending observation prompt kind")
        if self.decision_kind is None:
            if self.decision_options or self.decision_multi_select or self.decision_question_count:
                raise ValueError("non-decision prompt cannot carry decision data")
            return
        if self.decision_kind not in {"single", "multi", "plan"}:
            raise ValueError("invalid pending observation decision kind")
        if not isinstance(self.decision_options, tuple) or not self.decision_options or not all(
            isinstance(label, str) and label.strip() for label in self.decision_options
        ):
            raise ValueError("decision prompt requires options")
        if self.decision_multi_select is not (self.decision_kind == "multi"):
            raise ValueError("decision multi-select flag does not match kind")
        if not isinstance(self.decision_question_count, int) or isinstance(self.decision_question_count, bool) or self.decision_question_count < 1:
            raise ValueError("invalid decision question count")


@dataclass(frozen=True)
class InteractionChoice:
    choice_id: str = ""
    label: str = ""

    def __post_init__(self) -> None:
        label = _public_text(self.label, default="Action")
        raw_choice_id = _string_value(self.choice_id).strip()
        material = {"label": label}
        choice_id = (_opaque_public_id("choice", raw_choice_id, material) if raw_choice_id
                     else _stable_id("choice", material))
        object.__setattr__(self, "choice_id", choice_id)
        object.__setattr__(self, "label", label)

    def to_dict(self) -> dict[str, Any]:
        return {"choice_id": self.choice_id, "label": self.label}

    @classmethod
    def from_dict(cls, data: "InteractionChoice | Mapping[str, Any]") -> "InteractionChoice":
        if isinstance(data, InteractionChoice):
            return data
        clean = sanitize_forbidden_fields(data if isinstance(data, Mapping) else {})
        return cls(choice_id=_string_value(clean.get("choice_id")), label=_string_value(clean.get("label")))


@dataclass(frozen=True)
class PendingInteraction:
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
        worker_id = _public_safe_identity(self.worker_id, prefix="worker")
        kind = _normalize_pending_kind(self.kind)
        question = _public_text(self.question, default="Action requires attention")
        choices = [choice if isinstance(choice, InteractionChoice) else InteractionChoice.from_dict(choice)
                   for choice in self.choices]
        status = _normalize_pending_status(self.status)
        worker_fingerprint = _optional_public_fingerprint(self.worker_fingerprint)
        space_id = _optional_public_safe_identity(self.space_id, prefix="space")
        created_at, updated_at, expires_at = map(
            _optional_timestamp, (self.created_at, self.updated_at, self.expires_at)
        )
        meta = _clean_meta(self.meta)
        identity_payload = {
            "schema_version": TURN_SCHEMA_VERSION, "host_id": host_id,
            "worker_id": worker_id, "space_id": space_id, "kind": kind, "question": question,
            "choice_ids": [choice.choice_id for choice in choices],
            "source": _meta_value(meta, "source") or _meta_value(meta, "attention_id"),
        }
        content_payload = {
            **identity_payload, "worker_fingerprint": worker_fingerprint,
            "choices": [choice.to_dict() for choice in choices],
            "status": status, "meta": meta,
        }
        raw_interaction_id = _string_value(self.id).strip()
        interaction_id = (raw_interaction_id if re.fullmatch(r"pending-[0-9a-f]{24}", raw_interaction_id)
                          else _stable_id("pending", identity_payload))
        normalized = {
            "schema_version": TURN_SCHEMA_VERSION, "id": interaction_id, "host_id": host_id,
            "worker_id": worker_id, "worker_fingerprint": worker_fingerprint, "space_id": space_id,
            "kind": kind, "question": question, "choices": list(choices), "status": status,
            "created_at": created_at, "updated_at": updated_at, "expires_at": expires_at,
            "fingerprint": _content_fingerprint(content_payload), "meta": meta,
        }
        for name, value in normalized.items():
            object.__setattr__(self, name, value)

    def to_dict(self) -> dict[str, Any]:
        return sanitize_public_mapping({
            "schema_version": self.schema_version, "id": self.id, "host_id": self.host_id,
            "worker_id": self.worker_id, "worker_fingerprint": self.worker_fingerprint,
            "space_id": self.space_id, "kind": self.kind, "question": self.question,
            "choices": [choice.to_dict() for choice in self.choices],
            "status": self.status, "created_at": self.created_at, "updated_at": self.updated_at,
            "expires_at": self.expires_at, "fingerprint": self.fingerprint, "meta": _clean_meta(self.meta),
        })
