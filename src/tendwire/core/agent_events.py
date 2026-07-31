"""Canonical structured events emitted by agent-protocol backends.

The event contract deliberately keeps source identifiers and the unsanitized
payload on the private side of Tendwire's trust boundary.  A separate public
projection is produced at construction time so connector code never has to
guess which source fields are safe to expose.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Literal

from .models import sanitize_public_mapping, utc_timestamp

AgentEventKind = Literal[
    "user_message",
    "agent_message",
    "thought",
    "tool_call",
    "tool_call_update",
    "plan",
    "usage",
    "session_info",
    "extension",
]
AgentEventVisibility = Literal["private", "public"]

AGENT_EVENT_KINDS = frozenset(
    {
        "user_message",
        "agent_message",
        "thought",
        "tool_call",
        "tool_call_update",
        "plan",
        "usage",
        "session_info",
        "extension",
    }
)
AGENT_EVENT_VISIBILITIES = frozenset({"private", "public"})
AGENT_EVENT_MAX_PAYLOAD_BYTES = 64 * 1024
AGENT_EVENT_MAX_PUBLIC_PAYLOAD_BYTES = 64 * 1024
AGENT_EVENT_MAX_TEXT_CHARS = 32 * 1024
AGENT_EVENT_MAX_COLLECTION_ITEMS = 256
AGENT_EVENT_MAX_TOTAL_ITEMS = 4096
AGENT_EVENT_MAX_DEPTH = 12
AGENT_EVENT_MAX_IDENTIFIER_CHARS = 2048
AGENT_EVENT_QUERY_DEFAULT_LIMIT = 100
AGENT_EVENT_QUERY_MAX_LIMIT = 1000
AGENT_EVENT_SCHEMA_VERSION = 1


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def _fingerprint(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def normalize_agent_event_identifier(
    value: Any,
    field: str,
    *,
    required: bool = False,
) -> str | None:
    """Validate an opaque identifier without changing its source identity.

    Protocol identifiers are byte-significant.  Compatibility normalization or
    trimming would make distinct source IDs collide (for example ``"1"`` and
    ``"①"`` under NFKC), defeating the journal's replay-conflict checks.
    """
    if value is None:
        if required:
            raise ValueError(f"{field} must not be empty")
        return None
    if not isinstance(value, str):
        raise ValueError(f"{field} must be text or None")
    if not value.strip():
        raise ValueError(f"{field} must not be empty")
    if "\x00" in value:
        raise ValueError(f"{field} must not contain NUL")
    if len(value) > AGENT_EVENT_MAX_IDENTIFIER_CHARS:
        raise ValueError(f"{field} is too long")
    try:
        value.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise ValueError(f"{field} must contain valid Unicode") from exc
    return value


def _timestamp(value: str | datetime | None) -> str:
    if value is None:
        return utc_timestamp()
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        raw = value.strip()
        if not raw or len(raw) > 64:
            raise ValueError("observed_at must be an aware ISO-8601 timestamp")
        if raw.endswith(("Z", "z")):
            raw = raw[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(raw)
        except ValueError as exc:
            raise ValueError(
                "observed_at must be an aware ISO-8601 timestamp"
            ) from exc
    else:
        raise ValueError("observed_at must be an aware ISO-8601 timestamp")
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("observed_at must be an aware ISO-8601 timestamp")
    return parsed.astimezone(timezone.utc).isoformat()


def _valid_unicode(value: str) -> str:
    try:
        value.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise ValueError(
            "agent event payload contains invalid Unicode"
        ) from exc
    return value


def _normalize_payload_value(
    value: Any,
    *,
    depth: int = 0,
    item_budget: list[int] | None = None,
) -> Any:
    if item_budget is None:
        item_budget = [AGENT_EVENT_MAX_TOTAL_ITEMS]
    if depth > AGENT_EVENT_MAX_DEPTH:
        raise ValueError("agent event payload is nested too deeply")
    if value is None or isinstance(value, bool | int):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("agent event payload contains a non-finite number")
        return value
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("agent event payload contains a naive datetime")
        return utc_timestamp(value)
    if isinstance(value, str):
        if len(value) > AGENT_EVENT_MAX_TEXT_CHARS:
            raise ValueError("agent event payload text is too long")
        return _valid_unicode(value)
    if isinstance(value, Mapping):
        if len(value) > AGENT_EVENT_MAX_COLLECTION_ITEMS:
            raise ValueError("agent event payload mapping has too many entries")
        item_budget[0] -= len(value)
        if item_budget[0] < 0:
            raise ValueError("agent event payload has too many total items")
        result: dict[str, Any] = {}
        for raw_key, item in value.items():
            if not isinstance(raw_key, str):
                raise ValueError("agent event payload keys must be text")
            key = _valid_unicode(raw_key)
            if not key or len(key) > 256:
                raise ValueError("agent event payload contains an invalid key")
            result[key] = _normalize_payload_value(
                item,
                depth=depth + 1,
                item_budget=item_budget,
            )
        return result
    if isinstance(value, tuple | list):
        if len(value) > AGENT_EVENT_MAX_COLLECTION_ITEMS:
            raise ValueError("agent event payload sequence has too many entries")
        item_budget[0] -= len(value)
        if item_budget[0] < 0:
            raise ValueError("agent event payload has too many total items")
        return [
            _normalize_payload_value(
                item,
                depth=depth + 1,
                item_budget=item_budget,
            )
            for item in value
        ]
    raise ValueError("agent event payload must contain only JSON-safe values")


def normalize_agent_event_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Return a bounded, deterministic, JSON-safe private payload."""
    if not isinstance(payload, Mapping):
        raise ValueError("agent event payload must be a mapping")
    normalized = _normalize_payload_value(payload)
    if not isinstance(normalized, dict):  # Defensive; mappings normalize to dicts.
        raise ValueError("agent event payload must be a mapping")
    payload_size = len(_canonical_json(normalized).encode("utf-8"))
    if payload_size > AGENT_EVENT_MAX_PAYLOAD_BYTES:
        raise ValueError("agent event payload is too large")
    return normalized


def public_agent_event_payload(
    payload: Mapping[str, Any],
    *,
    visibility: AgentEventVisibility,
) -> dict[str, Any]:
    """Build the bounded public projection for an event payload."""
    if visibility == "private":
        return {}
    public = sanitize_public_mapping(payload, backend_neutral=True)
    if (
        len(_canonical_json(public).encode("utf-8"))
        > AGENT_EVENT_MAX_PUBLIC_PAYLOAD_BYTES
    ):
        raise ValueError("public agent event payload is too large")
    return public


@dataclass(frozen=True)
class AgentEvent:
    """One immutable structured source event before durable sequencing."""

    event_id: str
    kind: AgentEventKind
    source: str
    worker_id: str
    visibility: AgentEventVisibility
    observed_at: str
    payload: dict[str, Any]
    public_payload: dict[str, Any]
    payload_fingerprint: str
    source_session_id: str | None = None
    source_turn_id: str | None = None
    source_item_id: str | None = None
    source_message_id: str | None = None
    source_event_id: str | None = None
    source_sequence: int | None = None

    def public_dict(self, *, sequence: int | None = None) -> dict[str, Any]:
        """Return a connector-safe view with all source identifiers omitted."""
        result: dict[str, Any] = {
            "schema_version": AGENT_EVENT_SCHEMA_VERSION,
            "event_id": self.event_id,
            "kind": self.kind,
            "worker_id": self.worker_id,
            "visibility": self.visibility,
            "observed_at": self.observed_at,
            "payload": deepcopy(self.public_payload),
        }
        if sequence is not None:
            result["sequence"] = int(sequence)
        return result


def agent_event(
    *,
    kind: AgentEventKind | str,
    source: str,
    worker_id: str,
    payload: Mapping[str, Any],
    source_session_id: str | None = None,
    source_turn_id: str | None = None,
    source_item_id: str | None = None,
    source_message_id: str | None = None,
    source_event_id: str | None = None,
    source_sequence: int | None = None,
    visibility: AgentEventVisibility | str = "private",
    observed_at: str | datetime | None = None,
) -> AgentEvent:
    """Validate and construct an event with deterministic retry identity.

    Sources must provide either their event identifier or a session-local
    monotonically assigned sequence.  The source timestamp is intentionally
    excluded from identity so replaying the same notification deduplicates.
    """
    normalized_kind = str(kind).strip().lower()
    if normalized_kind not in AGENT_EVENT_KINDS:
        raise ValueError("unsupported agent event kind")
    normalized_visibility = str(visibility).strip().lower()
    if normalized_visibility not in AGENT_EVENT_VISIBILITIES:
        raise ValueError("visibility must be private or public")
    if normalized_kind == "thought" and normalized_visibility != "private":
        raise ValueError("thought events must remain private")
    if normalized_kind == "extension" and normalized_visibility != "private":
        raise ValueError("extension events must remain private")
    normalized_source = normalize_agent_event_identifier(
        source, "source", required=True
    )
    normalized_worker = normalize_agent_event_identifier(
        worker_id, "worker_id", required=True
    )
    session_id = normalize_agent_event_identifier(
        source_session_id, "source_session_id"
    )
    turn_id = normalize_agent_event_identifier(source_turn_id, "source_turn_id")
    item_id = normalize_agent_event_identifier(source_item_id, "source_item_id")
    message_id = normalize_agent_event_identifier(
        source_message_id, "source_message_id"
    )
    event_id = normalize_agent_event_identifier(source_event_id, "source_event_id")
    if source_sequence is not None and (
        isinstance(source_sequence, bool)
        or not isinstance(source_sequence, int)
        or source_sequence < 0
        or source_sequence > (1 << 63) - 1
    ):
        raise ValueError("source_sequence must be a nonnegative SQLite integer")
    if event_id is None and source_sequence is None:
        raise ValueError("source_event_id or source_sequence is required")
    if event_id is None and session_id is None:
        raise ValueError("source_session_id is required with source_sequence")
    normalized_payload = normalize_agent_event_payload(payload)
    public_payload = public_agent_event_payload(
        normalized_payload,
        visibility=normalized_visibility,  # type: ignore[arg-type]
    )
    identity: dict[str, Any] = {
        "schema_version": AGENT_EVENT_SCHEMA_VERSION,
        "source": normalized_source,
        "session_id": session_id,
    }
    if event_id is not None:
        identity.update({"identity": "event_id", "value": event_id})
    else:
        identity.update({"identity": "sequence", "value": source_sequence})
    return AgentEvent(
        event_id=_fingerprint(identity),
        kind=normalized_kind,  # type: ignore[arg-type]
        source=normalized_source or "",
        worker_id=normalized_worker or "",
        visibility=normalized_visibility,  # type: ignore[arg-type]
        observed_at=_timestamp(observed_at),
        payload=normalized_payload,
        public_payload=public_payload,
        payload_fingerprint=_fingerprint(normalized_payload),
        source_session_id=session_id,
        source_turn_id=turn_id,
        source_item_id=item_id,
        source_message_id=message_id,
        source_event_id=event_id,
        source_sequence=source_sequence,
    )


@dataclass(frozen=True)
class StoredAgentEvent:
    """One durably sequenced structured agent event."""

    sequence: int
    host_id: str
    event: AgentEvent

    def public_dict(self) -> dict[str, Any]:
        return self.event.public_dict(sequence=self.sequence)


@dataclass(frozen=True)
class AppendAgentEventResult:
    sequence: int
    event_id: str
    inserted: bool


@dataclass(frozen=True)
class AppendBoundAgentEventResult:
    """Atomic binding check and journal append outcome."""

    status: Literal["inserted", "replayed", "binding_changed"]
    event_id: str
    sequence: int | None = None

    @property
    def inserted(self) -> bool:
        return self.status == "inserted"


class AgentEventIdentityConflict(RuntimeError):
    """The same deterministic source identity was reused for other content."""
