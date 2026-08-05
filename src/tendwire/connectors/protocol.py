"""Pure validation for exact public connector payloads and delivery keys."""

from __future__ import annotations

import base64
import hashlib
import json
import re
from collections.abc import Mapping
from datetime import datetime
from typing import Any

from ..core.models import sanitize_public_mapping
from ..core.turns import TURN_CONTENT_PAGE_MAX_UTF8_BYTES, decode_content_cursor

_UTC = re.compile(r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{6}Z")
_REVISION = re.compile(r"twrev1\.[A-Za-z0-9_-]{43}")
_FINAL = re.compile(r"twfinal1\.[A-Za-z0-9_-]{43}")
_PLAN = re.compile(r"twplan1\.[A-Za-z0-9_-]{1,256}")
_PRESENTATION = re.compile(r"[A-Za-z0-9._-]{1,128}")
_STABLE_KEY = re.compile(r"wsk1_[0-9a-f]{64}")
_ROUTE = re.compile(r"twroute1\.[A-Za-z0-9_-]{43}")
_PARTITION = re.compile(r"twpart1_[0-9a-f]{64}")
_DELIVERY_KEY = re.compile(
    r"turn-final:(?:working:twwork1\.[A-Za-z0-9_-]{43}"
    r"|revision:twfinal1\.[A-Za-z0-9_-]{43}"
    r"|decision:twdecision1\.[A-Za-z0-9_-]{43}"
    r"|retire:twretire1\.[A-Za-z0-9_-]{43}"
    r"|twplan1\.[A-Za-z0-9_-]{1,256}:[0-9]{6})"
)
_REPLACEABLE_KEY = re.compile(
    r"turn-final:(?:working:twwork1\.[A-Za-z0-9_-]{43}"
    r"|revision:twfinal1\.[A-Za-z0-9_-]{43}"
    r"|twplan1\.[A-Za-z0-9_-]{1,256}:[0-9]{6})"
)


def valid_canonical_utc(value: Any) -> bool:
    if not isinstance(value, str) or _UTC.fullmatch(value) is None:
        return False
    try:
        parsed = datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%fZ")
    except ValueError:
        return False
    return parsed.strftime("%Y-%m-%dT%H:%M:%S.%fZ") == value


def valid_delivery_key(value: Any) -> bool:
    return isinstance(value, str) and _DELIVERY_KEY.fullmatch(value) is not None


def valid_generic_payload(value: Any) -> bool:
    try:
        json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")
        return isinstance(value, Mapping) and sanitize_public_mapping(value) == value
    except (KeyError, TypeError, ValueError, UnicodeError, OverflowError, RecursionError):
        return False


def _closed(value: Any, fields: set[str]) -> Mapping[str, Any] | None:
    return value if isinstance(value, Mapping) and set(value) == fields else None


def _matches(pattern: re.Pattern[str], value: Any) -> bool:
    return isinstance(value, str) and pattern.fullmatch(value) is not None


def _integer(value: Any, minimum: int = 0) -> bool:
    return type(value) is int and value >= minimum


def _public_identifier(field: str, value: Any) -> bool:
    return bool(
        isinstance(value, str) and 0 < len(value) <= 512
        and sanitize_public_mapping({field: value}).get(field) == value
    )


def _replacement_key(value: Any) -> bool:
    return value is None or bool(isinstance(value, str) and _REPLACEABLE_KEY.fullmatch(value))


def _digest(value: Any) -> str:
    encoded = json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False,
    ).encode("utf-8")
    return base64.urlsafe_b64encode(hashlib.sha256(encoded).digest()).rstrip(b"=").decode()


def _final_turn(payload: Mapping[str, Any], extras: set[str] | None = None) -> Mapping[str, Any] | None:
    turn = _closed(payload.get("turn"), {"turn_id", "final_identity", "content_revision"} | (extras or set()))
    return turn if (
        turn is not None and _public_identifier("turn_id", turn.get("turn_id"))
        and _matches(_FINAL, turn.get("final_identity"))
        and _matches(_REVISION, turn.get("content_revision"))
    ) else None


def _content_descriptor(value: Any) -> bool:
    content = _closed(value, {"schema_version", "content_revision", "known_incomplete", "fields"})
    if content is None or type(content.get("schema_version")) is not int or content["schema_version"] != 1:
        return False
    revision = content.get("content_revision")
    fields = _closed(content.get("fields"), {"user_text", "assistant_final_text"})
    if not _matches(_REVISION, revision) or content.get("known_incomplete") is not False or fields is None:
        return False
    descriptor_fields = {"availability", "inline", "char_length", "byte_length", "page_count", "first_cursor"}
    for field, raw in fields.items():
        item = _closed(raw, descriptor_fields)
        if item is None or item.get("availability") not in {"absent", "complete"}:
            return False
        lengths = tuple(item.get(name) for name in ("char_length", "byte_length", "page_count"))
        if not all(_integer(number) for number in lengths):
            return False
        chars, bytes_, pages = lengths
        inline, cursor = item.get("inline"), item.get("first_cursor")
        if item["availability"] == "absent":
            if inline is not None or lengths != (0, 0, 0) or cursor is not None:
                return False
            continue
        if inline is not None:
            if not isinstance(inline, str) or (chars, bytes_) != (len(inline), len(inline.encode())):
                return False
            if bytes_ > TURN_CONTENT_PAGE_MAX_UTF8_BYTES or pages or cursor is not None:
                return False
            continue
        if not chars or not chars <= bytes_ <= chars * 4 or bytes_ <= TURN_CONTENT_PAGE_MAX_UTF8_BYTES:
            return False
        minimum = (bytes_ + TURN_CONTENT_PAGE_MAX_UTF8_BYTES - 1) // TURN_CONTENT_PAGE_MAX_UTF8_BYTES
        maximum = (bytes_ + TURN_CONTENT_PAGE_MAX_UTF8_BYTES - 4) // (TURN_CONTENT_PAGE_MAX_UTF8_BYTES - 3)
        if not minimum <= pages <= maximum or not isinstance(cursor, str):
            return False
        try:
            position = decode_content_cursor(cursor, revision=str(revision), field=str(field), count=pages)
        except (TypeError, ValueError):
            return False
        if (position.index, position.start_char, position.start_byte) != (0, 0, 0):
            return False
    return True


def valid_part_spans(value: Any) -> bool:
    if not isinstance(value, list) or not 1 <= len(value) <= 64:
        return False
    order, previous = {"user_text": 0, "assistant_final_text": 1}, None
    for raw in value:
        item = _closed(raw, {"field", "start_char", "end_char"})
        if item is None or item.get("field") not in order:
            return False
        start, end = item.get("start_char"), item.get("end_char")
        if not _integer(start) or not _integer(end) or start >= end:
            return False
        coordinate = (order[str(item["field"])], start)
        if previous is not None and coordinate < previous:
            return False
        previous = (coordinate[0], end)
    return True


def _body(payload: Mapping[str, Any], kind: str) -> bool:
    if kind == "working":
        turn = _closed(payload.get("turn"), {"turn_id", "content_revision", "replaces_key", "text"})
        text = _closed(turn.get("text") if turn else None, {"assistant_stream_text", "char_length", "byte_length"})
        value = text.get("assistant_stream_text") if text else None
        return bool(
            turn and text and _public_identifier("turn_id", turn.get("turn_id"))
            and _matches(_REVISION, turn.get("content_revision")) and _replacement_key(turn.get("replaces_key"))
            and isinstance(value, str) and _integer(text.get("char_length")) and _integer(text.get("byte_length"))
            and text["char_length"] == len(value)
            and text.get("byte_length") == len(value.encode())
        )
    if kind == "final_ready":
        turn = _final_turn(payload, {"replaces_key", "content"})
        return bool(
            turn and _replacement_key(turn.get("replaces_key")) and _content_descriptor(turn.get("content"))
            and turn["content"]["content_revision"] == turn["content_revision"]
        )
    if kind == "final_part":
        turn = _final_turn(payload)
        plan = _closed(payload.get("plan"), {"plan_token", "generation", "presentation_version", "ordinal", "part_count", "spans"})
        lineage = _closed(payload.get("lineage"), {"recovered_from_plan_token", "predecessor_key", "replaces_key"})
        return bool(
            turn and plan and lineage and _matches(_PLAN, plan.get("plan_token"))
            and _integer(plan.get("generation"), 1) and _matches(_PRESENTATION, plan.get("presentation_version"))
            and _integer(plan.get("ordinal")) and _integer(plan.get("part_count"), 1)
            and plan["part_count"] <= 10_000 and plan["ordinal"] < plan["part_count"]
            and _replacement_key(lineage.get("predecessor_key")) and _replacement_key(lineage.get("replaces_key"))
            and (lineage.get("recovered_from_plan_token") is None or _matches(_PLAN, lineage["recovered_from_plan_token"]))
            and valid_part_spans(plan.get("spans"))
        )
    if kind == "retire":
        turn = _final_turn(payload)
        retire = _closed(payload.get("retire"), {"target_key", "target_kind", "target_ordinal", "predecessor_key", "plan_token", "generation", "reason"})
        return bool(
            turn and retire and valid_delivery_key(retire.get("target_key"))
            and valid_delivery_key(retire.get("predecessor_key"))
            and retire.get("target_kind") in {"working", "final_part", "decision"}
            and retire.get("reason") in {"working_replaced", "final_replaced", "excess_part", "decision_resolved"}
            and (retire.get("plan_token") is None or _matches(_PLAN, retire["plan_token"]))
            and (retire.get("generation") is None or _integer(retire["generation"], 1))
            and (retire.get("target_ordinal") is None or _integer(retire["target_ordinal"]))
        )
    if kind == "decision":
        decision = _closed(payload.get("decision"), {"decision_ref", "revision_digest", "mode", "title", "body", "choices"})
        choices = decision.get("choices") if decision else None
        return bool(
            decision and _public_identifier("decision_ref", decision.get("decision_ref"))
            and _public_identifier("revision_digest", decision.get("revision_digest"))
            and decision.get("mode") in {"single", "multi", "plan"}
            and isinstance(decision.get("title"), str) and len(decision["title"].encode()) <= 4096
            and isinstance(decision.get("body"), str) and len(decision["body"].encode()) <= 16_384
            and isinstance(choices, list) and 1 <= len(choices) <= 64
            and all(
                (item := _closed(choice, {"ordinal", "option_ref", "label"})) is not None
                and _integer(item.get("ordinal")) and item.get("option_ref") == str(item["ordinal"] + 1)
                and isinstance(item.get("label"), str) and bool(item["label"])
                and len(item["label"].encode()) <= 256 for choice in choices
            ) and [choice["ordinal"] for choice in choices] == list(range(len(choices)))
        )
    return False


def _retire_key(payload: Mapping[str, Any], key: str) -> bool:
    retire = payload["retire"]
    target_kind, target = retire["target_kind"], retire["target_key"]
    patterns = {
        "working": r"turn-final:working:twwork1\.[A-Za-z0-9_-]{43}",
        "final_part": r"turn-final:twplan1\.[A-Za-z0-9_-]{1,256}:([0-9]{6})",
        "decision": r"turn-final:decision:twdecision1\.[A-Za-z0-9_-]{43}",
    }
    target_match = re.fullmatch(patterns[target_kind], target)
    if target_match is None:
        return False
    if target_kind == "decision":
        return bool(
            target == retire["predecessor_key"] and retire["plan_token"] is None
            and retire["generation"] is None and retire["target_ordinal"] is None
            and retire["reason"] == "decision_resolved"
            and re.fullmatch(r"turn-final:retire:twretire1\.[A-Za-z0-9_-]{43}", key)
        )
    token = retire["plan_token"]
    if not _matches(_PLAN, token) or not _integer(retire["generation"], 1):
        return False
    if target_kind == "working":
        if retire["target_ordinal"] is not None or retire["reason"] != "working_replaced":
            return False
    elif (
        not _integer(retire["target_ordinal"])
        or int(target_match.group(1)) != retire["target_ordinal"] + 1
        or retire["reason"] not in {"final_replaced", "excess_part"}
    ):
        return False
    predecessor = re.fullmatch(rf"turn-final:{re.escape(str(token))}:([0-9]{{6}})", retire["predecessor_key"])
    return bool(
        predecessor
        and key == f"turn-final:{token}:{int(predecessor.group(1)) + 1:06d}"
    )


def _valid_turn_final_delivery(payload: Any, key: Any, host_id: Any) -> bool:
    if not isinstance(payload, Mapping) or not valid_delivery_key(key) or not _public_identifier("host_id", host_id):
        return False
    kind = payload.get("kind")
    fields = {
        "working": {"schema_version", "kind", "created_at", "worker", "route", "turn"},
        "final_ready": {"schema_version", "kind", "created_at", "worker", "route", "turn"},
        "final_part": {"schema_version", "kind", "created_at", "worker", "route", "turn", "plan", "lineage"},
        "retire": {"schema_version", "kind", "created_at", "worker", "route", "turn", "retire"},
        "decision": {"schema_version", "kind", "created_at", "worker", "route", "decision"},
    }
    versions = {"working": 1, "final_ready": 3, "final_part": 2, "retire": 1, "decision": 1}
    if kind not in fields or set(payload) != fields[kind] or type(payload.get("schema_version")) is not int or payload["schema_version"] != versions[kind]:
        return False
    worker = _closed(payload.get("worker"), {"worker_id", "stable_key", "stable_key_version", "route_generation"})
    route = _closed(payload.get("route"), {"partition_key", "partition_sequence"})
    if not (
        valid_canonical_utc(payload.get("created_at")) and worker and route
        and _public_identifier("worker_id", worker.get("worker_id"))
        and _matches(_STABLE_KEY, worker.get("stable_key")) and type(worker.get("stable_key_version")) is int
        and worker["stable_key_version"] == 1 and _matches(_ROUTE, worker.get("route_generation"))
        and _matches(_PARTITION, route.get("partition_key")) and _integer(route.get("partition_sequence"), 1)
        and _body(payload, str(kind))
    ):
        return False
    if kind == "working":
        turn = payload["turn"]
        expected = "turn-final:working:twwork1." + _digest([
            "tendwire.working.v1", [host_id, turn["turn_id"], turn["content_revision"], worker["route_generation"]],
        ])
    elif kind == "final_ready":
        expected = f"turn-final:revision:{payload['turn']['final_identity']}"
    elif kind == "final_part":
        plan = payload["plan"]
        expected = f"turn-final:{plan['plan_token']}:{plan['ordinal'] + 1:06d}"
    elif kind == "decision":
        decision = payload["decision"]
        decision_ref = "pending-" + hashlib.sha256(json.dumps(
            [host_id, worker["worker_id"], decision["revision_digest"]],
            ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False,
        ).encode()).hexdigest()[:24]
        if decision["decision_ref"] != decision_ref:
            return False
        expected = "turn-final:decision:twdecision1." + _digest({
            "domain": "tendwire.decision.v1", "host_id": host_id, "decision_ref": decision_ref,
            "revision_digest": decision["revision_digest"], "route_generation": worker["route_generation"],
        })
    else:
        return _retire_key(payload, str(key))
    return key == expected


def valid_turn_final_delivery(payload: Any, key: Any, host_id: Any) -> bool:
    """Validate without ever propagating failures from hostile JSON values."""
    try:
        json.dumps(
            {"host_id": host_id, "key": key, "payload": payload}, ensure_ascii=False,
            sort_keys=True, separators=(",", ":"), allow_nan=False,
        ).encode("utf-8")
        return _valid_turn_final_delivery(payload, key, host_id)
    except (KeyError, TypeError, ValueError, UnicodeError, OverflowError, RecursionError):
        return False
