"""Pure validation for exact public connector payloads and delivery keys."""

from __future__ import annotations

import base64
import hashlib
import json
import math
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
_REF = re.compile(r"twref1\.[A-Za-z0-9_-]{43}")
_CONNECTOR_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,63}")
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
_CONNECTOR_NAME_FORBIDDEN = (
    "backend_target", "pane_id", "session_id", "terminal_id", "chat_id",
    "topic_id", "message_id", "bot_token", "shell", "argv", "environment",
    "stdout", "stderr",
)
CONNECTOR_PRIVATE_KEYS = frozenset(_CONNECTOR_NAME_FORBIDDEN) | {
    "binding_private_fingerprint", "private_fingerprint", "private_binding",
    "credential", "credentials", "secret", "secrets", "password", "api_key",
    "socket_path", "stdin",
}
_CONNECTOR_COMMON = frozenset({"schema_version", "ok", "status", "host_id", "name"})
_CONNECTOR_ERROR_STATUSES = frozenset(
    {
        "invalid_params", "invalid_payload", "invalid_ref", "stale_ref",
        "store_unavailable", "unknown_method", "revision_not_found",
        "stale_revision", "content_unavailable", "plan_not_found",
        "plan_conflict", "part_conflict", "plan_incomplete", "plan_not_failed",
        "not_recoverable", "request_conflict", "ack_deadline_expired",
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
CONNECTOR_METHODS = frozenset(_CONNECTOR_RESULT_SPECS)


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


def _copy_connector_json(
    value: Any,
    *,
    depth: int = 0,
    budget: list[int] | None = None,
) -> Any:
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
            if key.lower() in CONNECTOR_PRIVATE_KEYS:
                raise ValueError("connector response contains a private key")
            result[key] = _copy_connector_json(
                item,
                depth=depth + 1,
                budget=budget,
            )
        return result
    if isinstance(value, (list, tuple)):
        return [
            _copy_connector_json(item, depth=depth + 1, budget=budget)
            for item in value
        ]
    raise ValueError("connector response contains a non-JSON value")


def _public_identifier(field: str, value: Any) -> bool:
    return bool(
        isinstance(value, str) and 0 < len(value) <= 512
        and sanitize_public_mapping({field: value}).get(field) == value
    )


def connector_text_is_public(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    lowered = value.lower()
    compact = re.sub(r"[^a-z0-9]", "", lowered)
    return not any(
        token in lowered or token.replace("_", "") in compact
        for token in _CONNECTOR_NAME_FORBIDDEN
    )


def connector_name(value: Any) -> str:
    if (
        not isinstance(value, str)
        or _CONNECTOR_NAME.fullmatch(value) is None
        or not connector_text_is_public(value)
    ):
        return ""
    return value if _public_identifier("name", value) else ""


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


def _valid_connector_key(name: str, value: Any) -> bool:
    if not isinstance(value, str) or not value:
        return False
    if name == "turn-final":
        return valid_delivery_key(value)
    return _public_identifier("key", value)


def _valid_ref_fields(value: Mapping[str, Any], *, dates_required: bool = False) -> bool:
    dates = (value.get("leased_until"), value.get("available_at"))
    return bool(
        _matches(_REF, value.get("ref"))
        and _integer(value.get("attempt"), 1)
        and all(
            valid_canonical_utc(item)
            if dates_required
            else item is None or valid_canonical_utc(item)
            for item in dates
        )
    )


_INSPECT_FIELDS = frozenset(
    {
        "kind", "key", "final_identity", "failed_plan_token", "decision_ref",
        "target_key", "reason", "attempt_count", "prior_attempt_count",
        "created_at", "terminal_at", "retryable", "recoverable",
    }
)
_INSPECT_KINDS = frozenset("working final_ready final_part decision retire".split())
_INSPECT_REASONS = frozenset(
    {
        "temporary", "rate_limited", "provider_rejected", "provider_uncertain",
        "invalid_payload", "content_unavailable", "route_unavailable",
        "provider_binding_unknown", "lease_expired", "ack_deadline_expired",
        "superseded", "attempts_exhausted", "operator_recovery",
    }
)


def _valid_inspect_item(value: Any) -> bool:
    item = _closed(value, set(_INSPECT_FIELDS))
    return bool(
        item
        and item.get("kind") in _INSPECT_KINDS
        and _valid_connector_key("turn-final", item.get("key"))
        and (
            item.get("decision_ref") is None
            or _public_identifier("decision_ref", item["decision_ref"])
        )
        and (item.get("final_identity") is None or _matches(_FINAL, item["final_identity"]))
        and (
            item.get("failed_plan_token") is None
            or _matches(_PLAN, item["failed_plan_token"])
        )
        and (
            item.get("target_key") is None
            or _valid_connector_key("turn-final", item["target_key"])
        )
        and _integer(item.get("attempt_count"))
        and _integer(item.get("prior_attempt_count"))
        and item.get("reason") in _INSPECT_REASONS
        and valid_canonical_utc(item.get("created_at"))
        and valid_canonical_utc(item.get("terminal_at"))
        and type(item.get("retryable")) is bool
        and type(item.get("recoverable")) is bool
    )


def _valid_nonpoll_connector_result(method: str, value: Mapping[str, Any]) -> bool:
    name = str(value.get("name"))
    if method == "connector.reclaim":
        return _integer(value.get("reclaimed"))
    if method in {
        "connector.renew",
        "connector.ack",
        "connector.release",
        "connector.fail",
        "connector.defer",
    }:
        available = "available_at" in value
        needs_available = (
            method == "connector.fail" and value.get("status") == "retry_scheduled"
        ) or (method == "connector.defer" and value.get("status") == "deferred")
        return bool(
            _valid_ref_fields(value)
            and _valid_connector_key(name, value.get("key"))
            and (
                method not in {"connector.fail", "connector.defer"}
                or available == needs_available
            )
        )
    if method == "connector.retry":
        return bool(
            name == "turn-final"
            and _valid_connector_key(name, value.get("key"))
            and _integer(value.get("retry_generation"), 1)
            and _integer(value.get("prior_attempt_count"))
            and value.get("warning")
            in {None, "provider_acceptance_may_have_occurred"}
        )
    if method == "connector.prepare":
        recovering = "failed_plan_token" in value
        accepted = value.get("accepted_ordinals", [])
        part_count = value.get("part_count")
        counts = (
            "acknowledged_prefix_count",
            "executable_job_count",
            "retained_failed_job_count",
            "prior_attempt_count",
        )
        return bool(
            name == "turn-final"
            and _matches(_PLAN, value.get("plan_token"))
            and (
                not recovering
                or _matches(_PLAN, value.get("failed_plan_token"))
            )
            and value.get("status") == ("recovered" if recovering else "ok")
            and _integer(value.get("generation"), 1)
            and (
                part_count is None
                or _integer(part_count, 1) and part_count <= 10_000
            )
            and (
                "ordinal" not in value
                or _integer(value["ordinal"]) and value["ordinal"] < part_count
            )
            and ("job_count" not in value or _integer(value["job_count"], 1))
            and all(field not in value or _integer(value[field]) for field in counts)
            and (
                "content_revision" not in value
                or _matches(_REVISION, value["content_revision"])
            )
            and isinstance(accepted, list)
            and all(
                _integer(item) and part_count is not None and item < part_count
                for item in accepted
            )
            and accepted == sorted(set(accepted))
            and value.get("state")
            in {
                "preparing", "active", "waiting_predecessor", "completed",
                "failed", "superseded",
            }
            and (
                "idempotent_replay" not in value
                or type(value["idempotent_replay"]) is bool
            )
        )
    if method == "connector.inspect":
        items = value.get("items")
        return bool(
            name == "turn-final"
            and _integer(value.get("total"))
            and isinstance(items, list)
            and all(_valid_inspect_item(item) for item in items)
            and value["total"] >= len(items)
        )
    return False


def validated_connector_result(
    method: str,
    result: Mapping[str, Any],
    *,
    expected_name: str,
) -> dict[str, Any]:
    copied = _copy_connector_json(result)
    if method not in CONNECTOR_METHODS:
        raise ValueError("connector method is invalid")
    if not isinstance(copied, dict):
        raise ValueError("connector result must be an object")
    if (
        not _CONNECTOR_COMMON <= copied.keys()
        or type(copied.get("schema_version")) is not int
        or copied.get("schema_version") != 1
        or type(copied.get("ok")) is not bool
    ):
        raise ValueError("connector result has an invalid envelope")
    result_name = copied.get("name")
    if not _public_identifier("host_id", copied.get("host_id")):
        raise ValueError("connector result identity is invalid")
    if copied["ok"] is False:
        if (
            result_name not in {"", expected_name}
            or (bool(result_name) and connector_name(result_name) != result_name)
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
            fields = frozenset(item_fields)
            dated_fields = frozenset(item_fields | {"created_at"})
            if not isinstance(item, Mapping) or set(item) not in {fields, dated_fields}:
                raise ValueError("connector poll item has unexpected fields")
            if (
                not _valid_connector_key(copied["name"], item.get("key"))
                or not _valid_ref_fields(item, dates_required=True)
                or (
                    item.get("created_at") is not None
                    and not valid_canonical_utc(item.get("created_at"))
                )
            ):
                raise ValueError("connector poll item values are invalid")
            payload = item["payload"]
            valid_payload = (
                valid_turn_final_delivery(payload, item["key"], copied["host_id"])
                if copied["name"] == "turn-final"
                else valid_generic_payload(payload)
            )
            if not valid_payload:
                raise ValueError("connector poll payload is invalid")
    elif not _valid_nonpoll_connector_result(method, copied):
        raise ValueError("connector result values are invalid")
    return copied
