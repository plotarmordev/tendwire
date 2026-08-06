"""Neutral connector outbox API above the SQLite store.

This module is intentionally Tendwire-only. It exposes opaque refs and sanitized
payloads without importing core runtime connectors or backend-specific concepts.
"""

from __future__ import annotations

from collections.abc import Mapping
import math
import re
from pathlib import Path
from typing import Any

from ..config import DEFAULT_CONNECTOR_ACK_TTL_SECONDS
from ..store.outbox import (
    ack_connector_delivery,
    defer_connector_delivery,
    fail_connector_delivery,
    poll_connector_outbox,
    inspect_connector_outbox,
    prepare_connector_plan_begin,
    prepare_connector_plan_commit,
    prepare_connector_plan_recover,
    prepare_connector_plan_part,
    reclaim_expired_connector_leases,
    release_connector_delivery,
    renew_connector_delivery,
    retry_connector_dead_letter,
)


def _text(value: Any, default: str = "") -> str:
    if value is None:
        return default
    result = str(value).strip()
    return result if result else default


def _strict_int(value: Any, *, minimum: int, maximum: int) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int):
        return None
    return value if minimum <= value <= maximum else None


def _exact_fields(
    data: Mapping[str, Any],
    required: set[str],
    optional: set[str] | frozenset[str] = frozenset(),
) -> bool:
    return set(data) >= required and set(data) <= required | set(optional)


_CONNECTOR_REF_PREFIX = "twref1."
_CONNECTOR_REF_CHARS = frozenset("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_")
_CONNECTOR_NAME_CHARS = frozenset("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-")
_PLAN_TOKEN_PREFIX = "twplan1."
_REVISION_PREFIX = "twrev1."
_TURN_FINAL_NAME = "turn-final"
_FINAL_ID_PREFIX = "twfinal1."
_FINAL_KEY_PREFIX = "turn-final:revision:twfinal1."
_PREPARE_MAX_PARTS = 10_000
_PREPARE_MAX_SPANS = 64
_PREPARE_FIELDS = frozenset({"user_text", "assistant_final_text"})
_PREPARE_VERSION_CHARS = _CONNECTOR_NAME_CHARS
_FORBIDDEN_PUBLIC_TEXT = (
    "backend_target",
    "pane_id",
    "session_id",
    "terminal_id",
    "chat_id",
    "topic_id",
    "message_id",
    "bot_token",
    "shell",
    "argv",
    "environment",
    "stdout",
    "stderr",
)
_FORBIDDEN_PROTOCOL_KEYS = frozenset(
    {
        "backend_target",
        "binding_private_fingerprint",
        "private_fingerprint",
        "private_binding",
        "pane_id",
        "session_id",
        "terminal_id",
        "chat_id",
        "topic_id",
        "message_id",
        "bot_token",
        "credential",
        "credentials",
        "secret",
        "secrets",
        "password",
        "api_key",
        "socket_path",
        "argv",
        "environment",
        "stdin",
        "stdout",
        "stderr",
    }
)
_DECLARED_EXACT_TEXT_FIELDS = frozenset(
    {"assistant_stream_text", "inline", "title", "body", "label"}
)
_PRIVATE_VALUE_RE = re.compile(
    r"(?i)(?:\b(?:pane|session|terminal|chat|topic|message)[_-]?id\s*[:=]|"
    r"\b(?:token|secret|password|credential|api[_-]?key)\s*[:=]|"
    r"(?:^|\s)/(?:home|root|etc|var|tmp|run|proc|sys|usr)/|"
    r"\b(?:sk-|gh[oprsu]_|xox[baprs]-|AKIA|AIza|glpat-|npm_|pypi-)[A-Za-z0-9_-]+)"
)
def _contains_forbidden_public_text(value: str) -> bool:
    lowered = value.lower()
    compact = "".join(char for char in lowered if char.isalnum())
    return any(token in lowered or token.replace("_", "") in compact for token in _FORBIDDEN_PUBLIC_TEXT)


def _opaque_token(value: Any, prefix: str) -> str:
    token = _text(value)
    if not token.startswith(prefix):
        return ""
    body = token[len(prefix) :]
    if not body or any(char not in _CONNECTOR_REF_CHARS for char in body):
        return ""
    return token


def _plan_token(value: Any) -> str:
    return _opaque_token(value, _PLAN_TOKEN_PREFIX)


def _protocol_copy(
    value: Any,
    *,
    depth: int = 0,
    budget: list[int] | None = None,
    field: str = "",
) -> Any:
    if budget is None:
        budget = [0]
    budget[0] += 1
    if depth > 12 or budget[0] > 4096:
        raise ValueError("protocol object is too deep")
    if isinstance(value, Mapping):
        result: dict[str, Any] = {}
        for raw_key, child in value.items():
            if not isinstance(raw_key, str) or raw_key.lower() in _FORBIDDEN_PROTOCOL_KEYS:
                raise ValueError("protocol object contains a private key")
            result[raw_key] = _protocol_copy(
                child, depth=depth + 1, budget=budget, field=raw_key
            )
        return result
    if isinstance(value, list):
        return [
            _protocol_copy(child, depth=depth + 1, budget=budget, field=field)
            for child in value
        ]
    if type(value) is float and not math.isfinite(value):
        raise ValueError("protocol object contains a non-finite number")
    if value is None or type(value) in {bool, int, float}:
        return value
    if isinstance(value, str):
        if field not in _DECLARED_EXACT_TEXT_FIELDS and _PRIVATE_VALUE_RE.search(value):
            raise ValueError("protocol object contains a private value")
        return value
    raise ValueError("protocol object contains an unsupported value")


def _clean_mapping(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise ValueError("protocol object must be a mapping")
    return _protocol_copy(value)


def _bounded_int_or_default(
    value: Any,
    default: int,
    *,
    minimum: int,
    maximum: int,
) -> int | None:
    if value is None:
        return default
    return _strict_int(value, minimum=minimum, maximum=maximum)


def _error(status: str, *, host_id: str, name: str = "") -> dict[str, Any]:
    return {
        "schema_version": 1,
        "ok": False,
        "status": status,
        "host_id": host_id,
        "name": name,
        "message": status,
    }


def _ref(value: Any) -> str:
    ref = _text(value)
    if not ref.startswith(_CONNECTOR_REF_PREFIX):
        return ""
    token = ref[len(_CONNECTOR_REF_PREFIX) :]
    if not token or any(char not in _CONNECTOR_REF_CHARS for char in token):
        return ""
    return ref


def _name(value: Any) -> str:
    name = _text(value)
    if not name or len(name) > 64:
        return ""
    if any(char not in _CONNECTOR_NAME_CHARS for char in name):
        return ""
    if _contains_forbidden_public_text(name):
        return ""
    return name

def _request_id(value: Any) -> str:
    request_id = _text(value)
    if not request_id or len(request_id) > 128:
        return ""
    if any(char not in _CONNECTOR_NAME_CHARS for char in request_id):
        return ""
    if _contains_forbidden_public_text(request_id):
        return ""
    return request_id


class ConnectorOutboxAPI:
    """Public-neutral facade for connector delivery and lease operations."""

    def __init__(
        self,
        db_path: str | Path | None,
        host_id: str,
        *,
        default_lease_seconds: int = 60,
        max_lease_seconds: int = 300,
        ack_ttl_seconds: int = DEFAULT_CONNECTOR_ACK_TTL_SECONDS,
        max_attempts: int = 10,
    ) -> None:
        self.db_path = Path(db_path) if db_path is not None else None
        self.host_id = str(host_id)
        values = {
            "default_lease_seconds": (default_lease_seconds, 1, 86_400),
            "max_lease_seconds": (max_lease_seconds, 1, 86_400),
            "ack_ttl_seconds": (ack_ttl_seconds, 1, 86_400),
            "max_attempts": (max_attempts, 1, 1_000_000),
        }
        parsed: dict[str, int] = {}
        for field, (value, minimum, maximum) in values.items():
            valid = _strict_int(value, minimum=minimum, maximum=maximum)
            if valid is None:
                raise ValueError(f"{field} is invalid")
            parsed[field] = valid
        if parsed["default_lease_seconds"] > parsed["max_lease_seconds"]:
            raise ValueError("default lease exceeds maximum")
        self.default_lease_seconds = parsed["default_lease_seconds"]
        self.max_lease_seconds = parsed["max_lease_seconds"]
        self.ack_ttl_seconds = parsed["ack_ttl_seconds"]
        self.max_attempts = parsed["max_attempts"]

    def _require_store(self, name: str = "") -> dict[str, Any] | None:
        if self.db_path is None:
            return _error("store_unavailable", host_id=self.host_id, name=name)
        return None

    def prepare(self, params: Mapping[str, Any] | None = None) -> dict[str, Any]:
        data = dict(params or {})
        if data.get("schema_version") != 1 or isinstance(
            data.get("schema_version"), bool
        ):
            return _error("invalid_params", host_id=self.host_id)
        action = data.get("action")
        name = _name(data.get("name"))
        if name != _TURN_FINAL_NAME or action not in {"begin", "part", "commit", "recover"}:
            return _error("invalid_params", host_id=self.host_id)
        unavailable = self._require_store(name)
        if unavailable is not None:
            return unavailable
        assert self.db_path is not None

        if action == "begin":
            required = {
                "schema_version",
                "action",
                "name",
                "turn_id",
                "content_revision",
                "presentation_version",
                "part_count",
                "source_ref",
            }
            if set(data) != required:
                return _error("invalid_params", host_id=self.host_id, name=name)
            turn_id = _text(data.get("turn_id"))
            revision = _opaque_token(data.get("content_revision"), _REVISION_PREFIX)
            version = _text(data.get("presentation_version"))
            part_count = data.get("part_count")
            source_ref = (
                _ref(data.get("source_ref"))
            )
            valid_turn_id = turn_id.startswith("turn-") or re.fullmatch(
                r"acpt_[0-9a-f]{24}", turn_id
            ) is not None
            if (
                not valid_turn_id
                or len(turn_id) > 128
                or any(char not in _CONNECTOR_NAME_CHARS for char in turn_id)
                or not revision
                or not version
                or len(version) > 128
                or any(char not in _PREPARE_VERSION_CHARS for char in version)
                or _contains_forbidden_public_text(version)
                or isinstance(part_count, bool)
                or not isinstance(part_count, int)
                or part_count < 1
                or part_count > _PREPARE_MAX_PARTS
            ):
                return _error("invalid_params", host_id=self.host_id, name=name)
            if not source_ref:
                return _error("invalid_ref", host_id=self.host_id, name=name)
            return prepare_connector_plan_begin(
                self.db_path,
                self.host_id,
                name=name,
                turn_id=turn_id,
                content_revision=revision,
                presentation_version=version,
                part_count=part_count,
                source_ref=source_ref,
            )

        if action == "recover":
            if set(data) != {
                "schema_version",
                "action",
                "name",
                "failed_plan_token",
                "request_id",
            }:
                return _error("invalid_params", host_id=self.host_id, name=name)
            failed_plan_token = _plan_token(data.get("failed_plan_token"))
            request_id = _request_id(data.get("request_id"))
            if not failed_plan_token or not request_id:
                return _error("invalid_params", host_id=self.host_id, name=name)
            return prepare_connector_plan_recover(
                self.db_path,
                self.host_id,
                name=name,
                failed_plan_token=failed_plan_token,
                request_id=request_id,
                ack_ttl_seconds=self.ack_ttl_seconds,
            )

        token = _plan_token(data.get("plan_token"))
        if not token:
            return _error("invalid_params", host_id=self.host_id, name=name)
        if action == "commit":
            required = {
                "schema_version",
                "action",
                "name",
                "plan_token",
                "source_ref",
            }
            if set(data) != required:
                return _error("invalid_params", host_id=self.host_id, name=name)
            source_ref = _ref(data.get("source_ref"))
            if not source_ref:
                return _error("invalid_ref", host_id=self.host_id, name=name)
            return prepare_connector_plan_commit(
                self.db_path,
                self.host_id,
                name=name,
                plan_token=token,
                source_ref=source_ref,
                ack_ttl_seconds=self.ack_ttl_seconds,
            )

        if set(data) != {
            "schema_version",
            "action",
            "name",
            "plan_token",
            "ordinal",
            "spans",
        }:
            return _error("invalid_params", host_id=self.host_id, name=name)
        ordinal = data.get("ordinal")
        raw_spans = data.get("spans")
        if (
            isinstance(ordinal, bool)
            or not isinstance(ordinal, int)
            or ordinal < 0
            or not isinstance(raw_spans, list)
            or not raw_spans
            or len(raw_spans) > _PREPARE_MAX_SPANS
        ):
            return _error("invalid_params", host_id=self.host_id, name=name)
        spans: list[dict[str, Any]] = []
        for raw_span in raw_spans:
            if not isinstance(raw_span, Mapping) or set(raw_span) != {
                "field",
                "start_char",
                "end_char",
            }:
                return _error("invalid_params", host_id=self.host_id, name=name)
            field = raw_span.get("field")
            start = raw_span.get("start_char")
            end = raw_span.get("end_char")
            if (
                field not in _PREPARE_FIELDS
                or isinstance(start, bool)
                or isinstance(end, bool)
                or not isinstance(start, int)
                or not isinstance(end, int)
                or start < 0
                or end <= start
            ):
                return _error("invalid_params", host_id=self.host_id, name=name)
            spans.append(
                {
                    "field": str(field),
                    "start_char": start,
                    "end_char": end,
                }
            )
        return prepare_connector_plan_part(
            self.db_path,
            self.host_id,
            name=name,
            plan_token=token,
            ordinal=ordinal,
            spans=spans,
        )


    def poll(self, params: Mapping[str, Any] | None = None) -> dict[str, Any]:
        data = dict(params or {})
        if not _exact_fields(data, {"name"}, {"limit", "lease_seconds"}):
            return _error("invalid_params", host_id=self.host_id)
        name = _name(data.get("name"))
        if not name:
            return _error("invalid_params", host_id=self.host_id)
        limit = _bounded_int_or_default(
            data.get("limit"),
            1,
            minimum=1,
            maximum=100,
        )
        lease_seconds = _bounded_int_or_default(
            data.get("lease_seconds"),
            self.default_lease_seconds,
            minimum=1,
            maximum=self.max_lease_seconds if name == _TURN_FINAL_NAME else 86_400,
        )
        if limit is None or lease_seconds is None:
            return _error("invalid_params", host_id=self.host_id, name=name)
        unavailable = self._require_store(name)
        if unavailable is not None:
            return unavailable
        assert self.db_path is not None
        store_result = poll_connector_outbox(
            self.db_path,
            self.host_id,
            name,
            limit=limit,
            lease_seconds=lease_seconds,
            max_attempts=self.max_attempts,
        )
        items: list[dict[str, Any]] = []
        for item in store_result.get("items", []):
            if not isinstance(item, Mapping):
                continue
            ref = _ref(item.get("ref"))
            if not ref:
                continue
            try:
                clean_payload = _clean_mapping(item.get("payload"))
            except ValueError:
                return _error("invalid_payload", host_id=self.host_id, name=name)
            public_item = {
                "key": str(item.get("key") or ""),
                "ref": ref,
                "attempt": int(item.get("attempt") or 0),
                "leased_until": str(item.get("leased_until") or ""),
                "available_at": str(item.get("available_at") or ""),
                "payload": clean_payload,
            }
            if name == _TURN_FINAL_NAME:
                public_item["created_at"] = str(item.get("created_at") or "")
            items.append(public_item)
        return {
            "schema_version": 1,
            "ok": bool(store_result.get("ok", False)),
            "status": str(store_result.get("status") or "ok"),
            "host_id": self.host_id,
            "name": name,
            "items": items,
        }

    def reclaim(self, params: Mapping[str, Any] | None = None) -> dict[str, Any]:
        data = dict(params or {})
        if set(data) != {"name"}:
            return _error("invalid_params", host_id=self.host_id)
        name = _name(data.get("name"))
        if not name:
            return _error("invalid_params", host_id=self.host_id)
        unavailable = self._require_store(name)
        if unavailable is not None:
            return unavailable
        assert self.db_path is not None
        return reclaim_expired_connector_leases(self.db_path, self.host_id, name)

    def _live_request(
        self,
        params: Mapping[str, Any] | None,
        required: set[str],
        optional: set[str] | frozenset[str] = frozenset(),
    ) -> tuple[dict[str, Any], str, str] | dict[str, Any]:
        data = dict(params or {})
        name = _name(data.get("name"))
        ref = _ref(data.get("ref")) or None
        if not _exact_fields(data, {"name", "ref"} | required, optional):
            return _error("invalid_params", host_id=self.host_id, name=name)
        if not name:
            return _error("invalid_params", host_id=self.host_id)
        if ref is None:
            return _error("invalid_ref", host_id=self.host_id, name=name)
        unavailable = self._require_store(name)
        return unavailable or (data, name, ref)

    def ack(self, params: Mapping[str, Any] | None = None) -> dict[str, Any]:
        request = self._live_request(params, set(), {"response"})
        if isinstance(request, dict):
            return request
        data, name, live_ref = request
        assert self.db_path is not None
        try:
            response = _clean_mapping(data.get("response")) if "response" in data else None
        except ValueError:
            return _error("invalid_params", host_id=self.host_id, name=name)
        return ack_connector_delivery(
            self.db_path,
            host_id=self.host_id,
            name=name,
            ref=live_ref,
            response=response,
        )

    def fail(self, params: Mapping[str, Any] | None = None) -> dict[str, Any]:
        return self._schedule("fail", params)

    def defer(self, params: Mapping[str, Any] | None = None) -> dict[str, Any]:
        return self._schedule("defer", params)

    def renew(self, params: Mapping[str, Any] | None = None) -> dict[str, Any]:
        request = self._live_request(params, {"lease_seconds"})
        if isinstance(request, dict):
            return request
        data, name, live_ref = request
        assert self.db_path is not None
        lease_seconds = _bounded_int_or_default(
            data.get("lease_seconds"),
            self.default_lease_seconds,
            minimum=1,
            maximum=(
                self.max_lease_seconds
                if name == _TURN_FINAL_NAME
                else 86400
            ),
        )
        if lease_seconds is None:
            return _error("invalid_params", host_id=self.host_id, name=name)
        return renew_connector_delivery(
            self.db_path,
            host_id=self.host_id,
            name=name,
            ref=live_ref,
            lease_seconds=lease_seconds,
        )

    def release(self, params: Mapping[str, Any] | None = None) -> dict[str, Any]:
        request = self._live_request(params, set())
        if isinstance(request, dict):
            return request
        _data, name, live_ref = request
        assert self.db_path is not None
        return release_connector_delivery(
            self.db_path,
            host_id=self.host_id,
            name=name,
            ref=live_ref,
        )

    def _schedule(self, action: str, params: Mapping[str, Any] | None) -> dict[str, Any]:
        request = self._live_request(
            params,
            {"reason"},
            {"delay_seconds", "available_at", "response"},
        )
        if isinstance(request, dict):
            return request
        data, name, live_ref = request
        assert self.db_path is not None
        delay_seconds = None
        if "delay_seconds" in data:
            delay_seconds = _strict_int(
                data.get("delay_seconds"),
                minimum=0,
                maximum=31_536_000,
            )
            if delay_seconds is None:
                return _error("invalid_params", host_id=self.host_id, name=name)
        available_at = data.get("available_at")
        if available_at is not None and not isinstance(available_at, str):
            return _error("invalid_params", host_id=self.host_id, name=name)
        try:
            response = _clean_mapping(data.get("response")) if "response" in data else None
        except ValueError:
            return _error("invalid_params", host_id=self.host_id, name=name)
        kwargs = {
            "host_id": self.host_id,
            "name": name,
            "ref": live_ref,
            "reason": _text(data.get("reason")),
            "response": response,
            "available_at": available_at,
            "delay_seconds": delay_seconds,
        }
        if action == "fail":
            return fail_connector_delivery(self.db_path, max_attempts=self.max_attempts, **kwargs)
        return defer_connector_delivery(self.db_path, **kwargs)

    def inspect(self, params: Mapping[str, Any] | None = None) -> dict[str, Any]:
        data = dict(params or {})
        if set(data) != {
            "schema_version",
            "name",
            "status",
            "limit",
        }:
            return _error("invalid_params", host_id=self.host_id)
        name = _name(data.get("name"))
        status = _text(data.get("status"))
        limit = data.get("limit")
        if (
            data.get("schema_version") != 1
            or isinstance(data.get("schema_version"), bool)
            or name != _TURN_FINAL_NAME
            or status != "dead_letter"
            or isinstance(limit, bool)
            or not isinstance(limit, int)
            or limit < 1
            or limit > 100
        ):
            return _error("invalid_params", host_id=self.host_id, name=name)
        unavailable = self._require_store(name)
        if unavailable is not None:
            return unavailable
        assert self.db_path is not None
        return inspect_connector_outbox(
            self.db_path,
            self.host_id,
            name=name,
            status=status,
            limit=limit,
        )

    def retry(self, params: Mapping[str, Any] | None = None) -> dict[str, Any]:
        data = dict(params or {})
        if set(data) not in (
            {"schema_version", "name", "key"},
            {"schema_version", "name", "final_identity"},
        ):
            return _error("invalid_params", host_id=self.host_id)
        name = _name(data.get("name"))
        key = _text(data.get("key"))
        key_valid = bool(
            key.startswith(_FINAL_KEY_PREFIX)
            and _opaque_token(key[len("turn-final:revision:"):], _FINAL_ID_PREFIX)
        ) or bool(
            re.fullmatch(r"turn-final:decision:twdecision1\.[A-Za-z0-9_-]{43}", key)
        ) or bool(
            re.fullmatch(r"turn-final:retire:twretire1\.[A-Za-z0-9_-]{43}", key)
        )
        final_identity = _opaque_token(
            data.get("final_identity"),
            _FINAL_ID_PREFIX,
        )
        if (
            data.get("schema_version") != 1
            or isinstance(data.get("schema_version"), bool)
            or name != _TURN_FINAL_NAME
            or (
                "key" in data
                and not key_valid
            )
            or ("final_identity" in data and not final_identity)
        ):
            return _error("invalid_params", host_id=self.host_id, name=name)
        unavailable = self._require_store(name)
        if unavailable is not None:
            return unavailable
        assert self.db_path is not None
        return retry_connector_dead_letter(
            self.db_path,
            self.host_id,
            key=key or None,
            final_identity=final_identity or None,
        )


    def dispatch(self, method: str, params: Mapping[str, Any] | None = None) -> dict[str, Any]:
        handlers = {
            "connector.prepare": self.prepare,
            "connector.poll": self.poll,
            "connector.ack": self.ack,
            "connector.fail": self.fail,
            "connector.defer": self.defer,
            "connector.renew": self.renew,
            "connector.release": self.release,
            "connector.reclaim": self.reclaim,
            "connector.inspect": self.inspect,
            "connector.retry": self.retry,
        }
        handler = handlers.get(method) if isinstance(method, str) else None
        return handler(params) if handler else _error("unknown_method", host_id=self.host_id)
