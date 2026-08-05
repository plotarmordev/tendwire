"""Authoritative append-only ACP event journal."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Literal

from ..core.agent_events import (
    AGENT_EVENT_QUERY_DEFAULT_LIMIT,
    AGENT_EVENT_QUERY_MAX_LIMIT,
    AgentEvent,
    AgentEventIdentityConflict,
    AppendAgentEventResult,
    StoredAgentEvent,
    agent_event,
    normalize_agent_event_identifier,
)
from .db import read_transaction, write_transaction


def _stored(row: Any) -> StoredAgentEvent:
    event = AgentEvent(
        event_id=str(row["event_id"]),
        kind=str(row["kind"]),
        source=str(row["backend"]),
        worker_id=str(row["worker_id"]),
        visibility=str(row["visibility"]),
        observed_at=str(row["observed_at"]),
        payload=json.loads(row["payload_json"]),
        public_payload=json.loads(row["public_payload_json"]),
        payload_fingerprint=str(row["payload_fingerprint"]),
        source_session_id=row["session_id"],
        source_turn_id=row["source_turn_id"],
        source_item_id=row["source_item_id"],
        source_message_id=row["source_message_id"],
        source_event_id=row["source_event_id"],
        source_sequence=row["source_sequence"],
    )
    return StoredAgentEvent(sequence=int(row["id"]), host_id=str(row["host_id"]), event=event)


def _same(stored: StoredAgentEvent, incoming: AgentEvent) -> bool:
    """Return whether two rows describe the same producer event.

    ``observed_at`` is assigned by the receiving Tendwire process when the ACP
    producer does not provide its own timestamp.  A reconstructed connection
    therefore observes an otherwise identical source event at a later wall
    clock time.  The first persisted value remains the authoritative ordering
    time, but it is not part of producer identity and must not turn a replay
    into an identity conflict.
    """

    event = stored.event
    return (
        event.kind == incoming.kind
        and event.source == incoming.source
        and event.worker_id == incoming.worker_id
        and event.visibility == incoming.visibility
        and event.payload_fingerprint == incoming.payload_fingerprint
        and event.source_session_id == incoming.source_session_id
        and event.source_turn_id == incoming.source_turn_id
        and event.source_item_id == incoming.source_item_id
        and event.source_message_id == incoming.source_message_id
        and event.source_event_id == incoming.source_event_id
        and event.source_sequence == incoming.source_sequence
    )


def _append(conn: Any, host_id: str, event: AgentEvent) -> AppendAgentEventResult:
    values = (
        host_id, event.source, event.worker_id, event.source_session_id,
        event.event_id, event.kind, event.visibility, event.source_turn_id,
        event.source_item_id, event.source_message_id, event.source_event_id,
        event.source_sequence, event.observed_at, event.payload_fingerprint,
        json.dumps(event.payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False),
        json.dumps(event.public_payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False),
    )
    cursor = conn.execute(
        """INSERT INTO agent_events(
        host_id,backend,worker_id,session_id,event_id,kind,visibility,source_turn_id,
        source_item_id,source_message_id,source_event_id,source_sequence,observed_at,
        payload_fingerprint,payload_json,public_payload_json)
        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(host_id,event_id) DO NOTHING""",
        values,
    )
    row = conn.execute("SELECT * FROM agent_events WHERE host_id=? AND event_id=?", (host_id, event.event_id)).fetchone()
    if row is None:
        raise RuntimeError("agent_event_append_failed")
    stored = _stored(row)
    if not _same(stored, event):
        raise AgentEventIdentityConflict(event.event_id)
    return AppendAgentEventResult(stored.sequence, event.event_id, cursor.rowcount == 1)


def record_agent_event(db_path: Path | str, host_id: str, **event_fields: Any) -> AppendAgentEventResult:
    normalized = normalize_agent_event_identifier(host_id, "host_id", required=True) or ""
    event = agent_event(**event_fields)
    with write_transaction(db_path) as conn:
        return _append(conn, normalized, event)


def list_agent_events(
    db_path: Path | str,
    host_id: str,
    *,
    worker_id: str | None = None,
    source: str | None = None,
    session_id: str | None = None,
    turn_id: str | None = None,
    visibility: Literal["private", "public"] | None = None,
    after_sequence: int = 0,
    limit: int = AGENT_EVENT_QUERY_DEFAULT_LIMIT,
) -> tuple[StoredAgentEvent, ...]:
    if isinstance(after_sequence, bool) or not isinstance(after_sequence, int) or after_sequence < 0:
        raise ValueError("after_sequence must be a nonnegative integer")
    if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= AGENT_EVENT_QUERY_MAX_LIMIT:
        raise ValueError(f"limit must be between 1 and {AGENT_EVENT_QUERY_MAX_LIMIT}")
    if visibility not in {None, "private", "public"}:
        raise ValueError("visibility must be private, public, or None")
    clauses = ["host_id=?", "id>?"]
    values: list[Any] = [normalize_agent_event_identifier(host_id, "host_id", required=True), after_sequence]
    for column, value, field in (
        ("worker_id", worker_id, "worker_id"), ("backend", source, "source"),
        ("session_id", session_id, "source_session_id"), ("source_turn_id", turn_id, "source_turn_id"),
        ("visibility", visibility, "visibility"),
    ):
        if value is not None:
            clauses.append(f"{column}=?")
            values.append(value if field == "visibility" else normalize_agent_event_identifier(value, field))
    values.append(limit)
    with read_transaction(db_path) as conn:
        rows = conn.execute(f"SELECT * FROM agent_events WHERE {' AND '.join(clauses)} ORDER BY id LIMIT ?", values).fetchall()
    return tuple(_stored(row) for row in rows)


__all__ = ("list_agent_events", "record_agent_event")
