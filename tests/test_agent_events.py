from __future__ import annotations

import sqlite3
from dataclasses import replace
from pathlib import Path

import pytest

from tendwire.core.agent_events import (
    AGENT_EVENT_KINDS,
    AGENT_EVENT_MAX_PAYLOAD_BYTES,
    AgentEventIdentityConflict,
    agent_event,
)
from tendwire.store import sqlite as store_sqlite


def _message_event(
    *,
    sequence: int,
    text: str = "hello",
    visibility: str = "public",
):
    return agent_event(
        kind="agent_message",
        source="acp",
        worker_id="worker-1",
        source_session_id="private-session-1",
        source_turn_id="private-turn-1",
        source_item_id="private-item-1",
        source_message_id="private-message-1",
        source_sequence=sequence,
        visibility=visibility,
        payload={"text": text},
        observed_at="2026-07-31T00:00:00+00:00",
    )


def test_agent_event_contract_covers_acp_primary_kinds() -> None:
    assert AGENT_EVENT_KINDS == {
        "user_message",
        "agent_message",
        "thought",
        "tool_call",
        "tool_call_update",
        "plan",
        "usage",
        "session_info",
    }


def test_append_is_ordered_and_replay_is_idempotent(tmp_path: Path) -> None:
    db_path = tmp_path / "store.db"
    first = _message_event(sequence=10)
    second = _message_event(sequence=11, text="world")

    inserted = store_sqlite.append_agent_event(db_path, "host-1", first)
    replayed = store_sqlite.append_agent_event(db_path, "host-1", first)
    later = store_sqlite.append_agent_event(db_path, "host-1", second)

    assert inserted.inserted is True
    assert replayed.inserted is False
    assert replayed.sequence == inserted.sequence
    assert later.sequence > inserted.sequence
    events = store_sqlite.list_agent_events(db_path, "host-1")
    assert [stored.event.payload["text"] for stored in events] == [
        "hello",
        "world",
    ]


def test_deterministic_identity_rejects_changed_replay(tmp_path: Path) -> None:
    db_path = tmp_path / "store.db"
    original = _message_event(sequence=4, text="original")
    corrupt_replay = _message_event(sequence=4, text="changed")
    assert original.event_id == corrupt_replay.event_id
    store_sqlite.append_agent_event(db_path, "host-1", original)

    with pytest.raises(AgentEventIdentityConflict):
        store_sqlite.append_agent_event(db_path, "host-1", corrupt_replay)

    stored = store_sqlite.list_agent_events(db_path, "host-1")
    assert len(stored) == 1
    assert stored[0].event.payload == {"text": "original"}


def test_private_ids_and_payload_never_enter_public_projection(tmp_path: Path) -> None:
    db_path = tmp_path / "store.db"
    event = agent_event(
        kind="agent_message",
        source="acp",
        worker_id="worker-1",
        source_session_id="session-secret",
        source_item_id="item-secret",
        source_message_id="message-secret",
        source_event_id="event-secret",
        visibility="public",
        payload={
            "text": "safe status",
            "session_id": "payload-session-secret",
            "cwd": "/home/smith/private-repository",
        },
    )
    store_sqlite.append_agent_event(db_path, "host-1", event)

    private = store_sqlite.list_agent_events(
        db_path,
        "host-1",
        session_id="session-secret",
    )
    assert private[0].event.source_item_id == "item-secret"
    assert private[0].event.payload["cwd"] == "/home/smith/private-repository"
    public = store_sqlite.list_public_agent_events(db_path, "host-1")
    assert public[0]["payload"] == {"text": "safe status"}
    encoded = repr(public[0])
    assert "session-secret" not in encoded
    assert "item-secret" not in encoded
    assert "message-secret" not in encoded
    assert "event-secret" not in encoded
    assert "/home/smith" not in encoded


def test_thought_events_are_private_and_not_publicly_listed(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="thought events must remain private"):
        agent_event(
            kind="thought",
            source="acp",
            worker_id="worker-1",
            source_session_id="session-1",
            source_sequence=1,
            visibility="public",
            payload={"text": "reasoning summary"},
        )

    db_path = tmp_path / "store.db"
    thought = agent_event(
        kind="thought",
        source="acp",
        worker_id="worker-1",
        source_session_id="session-1",
        source_sequence=1,
        payload={"text": "reasoning summary"},
    )
    store_sqlite.append_agent_event(db_path, "host-1", thought)
    assert store_sqlite.list_public_agent_events(db_path, "host-1") == ()
    assert store_sqlite.list_agent_events(db_path, "host-1")[0].event.payload == {
        "text": "reasoning summary"
    }


def test_queries_filter_worker_session_turn_and_cursor(tmp_path: Path) -> None:
    db_path = tmp_path / "store.db"
    first = _message_event(sequence=1)
    second = agent_event(
        kind="plan",
        source="acp",
        worker_id="worker-2",
        source_session_id="session-2",
        source_turn_id="turn-2",
        source_sequence=2,
        payload={"entries": [{"content": "test", "status": "pending"}]},
    )
    first_result = store_sqlite.append_agent_event(db_path, "host-1", first)
    store_sqlite.append_agent_event(db_path, "host-1", second)

    assert len(
        store_sqlite.list_agent_events(db_path, "host-1", worker_id="worker-1")
    ) == 1
    assert len(
        store_sqlite.list_agent_events(
            db_path, "host-1", session_id="session-2", turn_id="turn-2"
        )
    ) == 1
    after = store_sqlite.list_agent_events(
        db_path, "host-1", after_sequence=first_result.sequence
    )
    assert [stored.event.kind for stored in after] == ["plan"]


def test_payload_is_bounded_and_json_safe() -> None:
    with pytest.raises(ValueError, match="payload text is too long"):
        _message_event(sequence=1, text="x" * (AGENT_EVENT_MAX_PAYLOAD_BYTES + 1))
    with pytest.raises(ValueError, match="JSON-safe"):
        agent_event(
            kind="usage",
            source="acp",
            worker_id="worker-1",
            source_event_id="event-1",
            payload={"opaque": object()},
        )


def test_store_rejects_noncanonical_public_projection(tmp_path: Path) -> None:
    event = _message_event(sequence=1)
    tampered = replace(event, public_payload={"session_id": "private-session"})
    with pytest.raises(ValueError, match="canonical agent event contract"):
        store_sqlite.append_agent_event(tmp_path / "store.db", "host-1", tampered)


def test_v21_migration_is_idempotent_and_preserves_existing_store(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "store.db"
    store_sqlite.init_store(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute("DROP TABLE agent_events")
        conn.execute("PRAGMA user_version = 21")

    store_sqlite.init_store(db_path)
    store_sqlite.init_store(db_path)
    with sqlite3.connect(db_path) as conn:
        assert conn.execute("PRAGMA user_version").fetchone() == (22,)
        columns = {
            str(row[1]) for row in conn.execute("PRAGMA table_info(agent_events)")
        }
    assert {"sequence", "event_id", "private_payload_json"} <= columns
