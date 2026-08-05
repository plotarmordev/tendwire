"""Authoritative agent-event journal behavior."""

from __future__ import annotations

from pathlib import Path

import pytest

from tendwire.core.agent_events import (
    AgentEvent,
    AgentEventIdentityConflict,
    AppendAgentEventResult,
    agent_event,
)
from tendwire.store.db import write_transaction
from tendwire.store.events import _append as append_agent_event
from tendwire.store.events import list_agent_events
from tendwire.store.retention import RetentionPolicy, run_retention_cycle
from tendwire.store.schema import init_store


def _event(
    *,
    event_id: str,
    text: str,
    observed_at: str,
    visibility: str = "private",
) -> AgentEvent:
    return agent_event(
        kind="agent_message",
        source="acp",
        worker_id="worker-a",
        payload={"text": text},
        source_session_id="session-a",
        source_turn_id="turn-a",
        source_event_id=event_id,
        visibility=visibility,
        observed_at=observed_at,
    )


def _append(
    db_path: Path,
    host_id: str,
    event: AgentEvent,
) -> AppendAgentEventResult:
    with write_transaction(db_path) as conn:
        return append_agent_event(conn, host_id, event)


def test_identical_authoritative_event_replays_without_second_row(tmp_path: Path) -> None:
    db_path = tmp_path / "events.db"
    init_store(db_path)
    event = _event(
        event_id="event-a",
        text="hello",
        observed_at="2026-08-01T00:00:00Z",
    )
    first = _append(db_path, "host-a", event)
    replay = _append(db_path, "host-a", event)
    assert first.inserted is True
    assert replay.inserted is False
    assert replay.sequence == first.sequence
    assert len(list_agent_events(db_path, "host-a")) == 1


def test_same_identity_with_changed_payload_conflicts(tmp_path: Path) -> None:
    db_path = tmp_path / "events.db"
    init_store(db_path)
    _append(
        db_path,
        "host-a",
        _event(
            event_id="event-a",
            text="first",
            observed_at="2026-08-01T00:00:00Z",
        ),
    )
    with pytest.raises(AgentEventIdentityConflict):
        _append(
            db_path,
            "host-a",
            _event(
                event_id="event-a",
                text="changed",
                observed_at="2026-08-01T00:00:00Z",
            ),
        )


def test_queries_are_host_scoped_filterable_and_paged(tmp_path: Path) -> None:
    db_path = tmp_path / "events.db"
    init_store(db_path)
    for index in range(4):
        _append(
            db_path,
            "host-a",
            _event(
                event_id=f"event-{index}",
                text=str(index),
                observed_at=f"2026-08-01T00:00:0{index}Z",
                visibility="public" if index % 2 else "private",
            ),
        )
    _append(
        db_path,
        "host-b",
        _event(event_id="other", text="other", observed_at="2026-08-01T00:00:05Z"),
    )
    first = list_agent_events(db_path, "host-a", limit=2)
    second = list_agent_events(db_path, "host-a", after_sequence=first[-1].sequence, limit=2)
    assert [item.event.payload["text"] for item in first + second] == ["0", "1", "2", "3"]
    assert [item.event.payload["text"] for item in list_agent_events(db_path, "host-a", visibility="public")] == ["1", "3"]
    assert [item.event.payload["text"] for item in list_agent_events(db_path, "host-b")] == ["other"]


@pytest.mark.parametrize("kwargs", [{"limit": 0}, {"after_sequence": -1}, {"visibility": "secret"}])
def test_query_bounds_reject_invalid_values(tmp_path: Path, kwargs: dict[str, object]) -> None:
    db_path = tmp_path / "events.db"
    init_store(db_path)
    with pytest.raises(ValueError):
        list_agent_events(db_path, "host-a", **kwargs)


def test_retention_deletes_only_a_bounded_old_batch(tmp_path: Path) -> None:
    db_path = tmp_path / "events.db"
    init_store(db_path)
    for index in range(5):
        _append(
            db_path,
            "host-a",
            _event(
                event_id=f"old-{index}",
                text=f"old-{index}",
                observed_at="2026-06-01T00:00:00Z",
            ),
        )
    _append(
        db_path,
        "host-a",
        _event(event_id="recent", text="recent", observed_at="2026-08-04T00:00:00Z"),
    )
    result = run_retention_cycle(
        db_path,
        policy=RetentionPolicy(event_retention_days=7, batch_size=3),
        now="2026-08-05T00:00:00Z",
    )
    assert result["agent_events"] == 3
    assert [item.event.payload["text"] for item in list_agent_events(db_path, "host-a")] == ["old-3", "old-4", "recent"]
