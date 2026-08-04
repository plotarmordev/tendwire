from __future__ import annotations

import sqlite3
import threading
import time
import tracemalloc
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

import pytest

from tendwire.core.agent_events import (
    AGENT_EVENT_KINDS,
    AGENT_EVENT_MAX_PAYLOAD_BYTES,
    AGENT_EVENT_MAX_TOTAL_ITEMS,
    AgentEventIdentityConflict,
    agent_event,
)
from tendwire.core.models import WorkerBinding
from tendwire.store import sqlite as store_sqlite

from .store_helpers import record_test_agent_event, read_public_test_agent_events


# Exact production table shapes from schema v22 and v23.  These fixtures do
# not use the current target DDL, because doing so masks cross-version rebuild
# failures when historical public tool/plan rows are populated.
_HISTORICAL_V22_AGENT_EVENTS_DDL = """
CREATE TABLE agent_events (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    host_id TEXT NOT NULL,
    event_id TEXT NOT NULL,
    kind TEXT NOT NULL CHECK (
        kind IN (
            'user_message', 'agent_message', 'thought', 'tool_call',
            'tool_call_update', 'plan', 'usage', 'session_info'
        )
    ),
    source TEXT NOT NULL,
    worker_id TEXT NOT NULL,
    visibility TEXT NOT NULL CHECK (visibility IN ('private', 'public')),
    source_session_id TEXT,
    source_turn_id TEXT,
    source_item_id TEXT,
    source_message_id TEXT,
    source_event_id TEXT,
    source_sequence INTEGER CHECK (source_sequence >= 0),
    observed_at TEXT NOT NULL,
    payload_fingerprint TEXT NOT NULL,
    private_payload_json TEXT NOT NULL,
    public_payload_json TEXT NOT NULL,
    UNIQUE (host_id, event_id),
    CHECK (source_event_id IS NOT NULL OR source_sequence IS NOT NULL),
    CHECK (kind != 'thought' OR visibility = 'private')
)
"""

_HISTORICAL_V23_AGENT_EVENTS_DDL = """
CREATE TABLE agent_events (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    host_id TEXT NOT NULL,
    event_id TEXT NOT NULL,
    kind TEXT NOT NULL CHECK (
        kind IN (
            'user_message', 'agent_message', 'thought', 'tool_call',
            'tool_call_update', 'plan', 'usage', 'session_info', 'extension'
        )
    ),
    source TEXT NOT NULL,
    worker_id TEXT NOT NULL,
    visibility TEXT NOT NULL CHECK (visibility IN ('private', 'public')),
    source_session_id TEXT,
    source_turn_id TEXT,
    source_item_id TEXT,
    source_message_id TEXT,
    source_event_id TEXT,
    source_sequence INTEGER CHECK (source_sequence >= 0),
    observed_at TEXT NOT NULL,
    payload_fingerprint TEXT NOT NULL,
    private_payload_json TEXT NOT NULL,
    public_payload_json TEXT NOT NULL,
    UNIQUE (host_id, event_id),
    CHECK (length(host_id) BETWEEN 1 AND 2048),
    CHECK (instr(host_id, char(0)) = 0),
    CHECK (length(event_id) = 64),
    CHECK (length(source) BETWEEN 1 AND 2048),
    CHECK (instr(source, char(0)) = 0),
    CHECK (length(worker_id) BETWEEN 1 AND 2048),
    CHECK (instr(worker_id, char(0)) = 0),
    CHECK (
        source_session_id IS NULL OR (
            length(source_session_id) BETWEEN 1 AND 2048
            AND instr(source_session_id, char(0)) = 0
        )
    ),
    CHECK (
        source_turn_id IS NULL OR (
            length(source_turn_id) BETWEEN 1 AND 2048
            AND instr(source_turn_id, char(0)) = 0
        )
    ),
    CHECK (
        source_item_id IS NULL OR (
            length(source_item_id) BETWEEN 1 AND 2048
            AND instr(source_item_id, char(0)) = 0
        )
    ),
    CHECK (
        source_message_id IS NULL OR (
            length(source_message_id) BETWEEN 1 AND 2048
            AND instr(source_message_id, char(0)) = 0
        )
    ),
    CHECK (
        source_event_id IS NULL OR (
            length(source_event_id) BETWEEN 1 AND 2048
            AND instr(source_event_id, char(0)) = 0
        )
    ),
    CHECK (length(observed_at) BETWEEN 20 AND 40),
    CHECK (length(payload_fingerprint) = 64),
    CHECK (
        CASE WHEN json_valid(private_payload_json)
        THEN json_type(private_payload_json) = 'object' ELSE 0 END
    ),
    CHECK (length(CAST(private_payload_json AS BLOB)) <= 65536),
    CHECK (
        CASE WHEN json_valid(public_payload_json)
        THEN json_type(public_payload_json) = 'object' ELSE 0 END
    ),
    CHECK (length(CAST(public_payload_json AS BLOB)) <= 65536),
    CHECK (visibility != 'private' OR public_payload_json = '{}'),
    CHECK (source_event_id IS NOT NULL OR source_sequence IS NOT NULL),
    CHECK (
        source_event_id IS NOT NULL
        OR (source_session_id IS NOT NULL AND source_sequence IS NOT NULL)
    ),
    CHECK (kind NOT IN ('thought', 'extension') OR visibility = 'private')
)
"""


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
        "extension",
    }


def test_append_is_ordered_and_replay_is_idempotent(tmp_path: Path) -> None:
    db_path = tmp_path / "store.db"
    first = _message_event(sequence=10)
    second = _message_event(sequence=11, text="world")

    inserted = record_test_agent_event(db_path, "host-1", first)
    replayed = record_test_agent_event(db_path, "host-1", first)
    later = record_test_agent_event(db_path, "host-1", second)

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
    record_test_agent_event(db_path, "host-1", original)

    with pytest.raises(AgentEventIdentityConflict):
        record_test_agent_event(db_path, "host-1", corrupt_replay)

    stored = store_sqlite.list_agent_events(db_path, "host-1")
    assert len(stored) == 1
    assert stored[0].event.payload == {"text": "original"}


def test_source_identity_reuse_cannot_evade_conflict_by_changing_kind_or_sequence(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "store.db"
    original = _message_event(sequence=4)
    changed_kind = agent_event(
        kind="plan",
        source="acp",
        worker_id="worker-1",
        source_session_id="private-session-1",
        source_event_id="stable-source-id",
        source_sequence=5,
        payload={"entries": []},
    )
    original_with_id = agent_event(
        kind="agent_message",
        source="acp",
        worker_id="worker-1",
        source_session_id="private-session-1",
        source_event_id="stable-source-id",
        source_sequence=4,
        payload={"text": "hello"},
    )
    assert original_with_id.event_id == changed_kind.event_id
    record_test_agent_event(db_path, "host-1", original_with_id)
    with pytest.raises(AgentEventIdentityConflict):
        record_test_agent_event(db_path, "host-1", changed_kind)

    changed_sequence_kind = agent_event(
        kind="plan",
        source="acp",
        worker_id="worker-1",
        source_session_id="private-session-1",
        source_sequence=original.source_sequence,
        payload={"entries": []},
    )
    assert original.event_id == changed_sequence_kind.event_id
    record_test_agent_event(db_path, "host-2", original)
    with pytest.raises(AgentEventIdentityConflict):
        record_test_agent_event(db_path, "host-2", changed_sequence_kind)


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
    record_test_agent_event(db_path, "host-1", event)

    private = store_sqlite.list_agent_events(
        db_path,
        "host-1",
        session_id="session-secret",
    )
    assert private[0].event.source_item_id == "item-secret"
    assert private[0].event.payload["cwd"] == "/home/smith/private-repository"
    public = read_public_test_agent_events(db_path, "host-1")
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
    record_test_agent_event(db_path, "host-1", thought)
    assert read_public_test_agent_events(db_path, "host-1") == ()
    assert store_sqlite.list_agent_events(db_path, "host-1")[0].event.payload == {
        "text": "reasoning summary"
    }


@pytest.mark.parametrize("kind", ["tool_call", "tool_call_update", "plan"])
def test_tool_and_plan_events_are_private_only(kind: str, tmp_path: Path) -> None:
    with pytest.raises(ValueError, match=rf"{kind} events must remain private"):
        agent_event(
            kind=kind,
            source="acp",
            worker_id="worker-1",
            source_session_id="session-1",
            source_sequence=1,
            visibility="public",
            payload={"content": [{"type": "text", "text": "private tool data"}]},
        )

    event = agent_event(
        kind=kind,
        source="acp",
        worker_id="worker-1",
        source_session_id="session-1",
        source_sequence=1,
        payload={"content": [{"type": "text", "text": "private tool data"}]},
    )
    db_path = tmp_path / f"{kind}.db"
    record_test_agent_event(db_path, "host-1", event)
    assert read_public_test_agent_events(db_path, "host-1") == ()
    assert "private tool data" in repr(
        store_sqlite.list_agent_events(db_path, "host-1")
    )
    with sqlite3.connect(db_path) as conn, pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            "UPDATE agent_events SET visibility = 'public' WHERE event_id = ?",
            (event.event_id,),
        )


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
    first_result = record_test_agent_event(db_path, "host-1", first)
    record_test_agent_event(db_path, "host-1", second)

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
    for value in (float("nan"), float("inf"), float("-inf")):
        with pytest.raises(ValueError, match="non-finite"):
            agent_event(
                kind="usage",
                source="acp",
                worker_id="worker-1",
                source_event_id="event-1",
                payload={"tokens": value},
            )
    with pytest.raises(ValueError, match="too many total items"):
        agent_event(
            kind="usage",
            source="acp",
            worker_id="worker-1",
            source_event_id="event-1",
            payload={
                str(index): [0] * 256
                for index in range((AGENT_EVENT_MAX_TOTAL_ITEMS // 256) + 1)
            },
        )


def test_payload_and_opaque_ids_preserve_valid_unicode_exactly() -> None:
    circled = agent_event(
        kind="agent_message",
        source="acp",
        worker_id="worker-1",
        source_event_id="①",
        visibility="public",
        payload={"text": "① and fullwidth ｅ and NUL \x00 remain private-exact"},
    )
    ascii_event = agent_event(
        kind="agent_message",
        source="acp",
        worker_id="worker-1",
        source_event_id="1",
        payload={"text": "1"},
    )
    assert circled.event_id != ascii_event.event_id
    assert circled.source_event_id == "①"
    assert circled.payload["text"] == "① and fullwidth ｅ and NUL \x00 remain private-exact"
    assert "\x00" not in str(circled.public_payload.get("text", ""))

    with pytest.raises(ValueError, match="valid Unicode"):
        _message_event(sequence=9, text="\ud800")
    with pytest.raises(ValueError, match="must not contain NUL"):
        agent_event(
            kind="usage",
            source="acp",
            worker_id="worker-1",
            source_event_id="bad\x00id",
            payload={},
        )


def test_journal_payload_is_adapter_neutral_and_preserves_namespaced_extensions(
    tmp_path: Path,
) -> None:
    payload = {
        "text": "portable message",
        "org.example.agent/experimental-v2": {
            "futureField": [1, "two", {"enabled": True}],
            "_meta": {"opaque": "retained privately"},
        },
    }
    event = agent_event(
        kind="extension",
        source="org.example.agent/v2",
        worker_id="worker-1",
        source_event_id="extension-event",
        payload=payload,
    )
    record_test_agent_event(tmp_path / "store.db", "host-1", event)
    stored = store_sqlite.list_agent_events(tmp_path / "store.db", "host-1")
    assert stored[0].event.source == "org.example.agent/v2"
    assert stored[0].event.payload == payload
    with pytest.raises(ValueError, match="extension events must remain private"):
        agent_event(
            kind="extension",
            source="org.example.agent/v2",
            worker_id="worker-1",
            source_event_id="unsafe-public-extension",
            visibility="public",
            payload=payload,
        )


def test_observed_at_is_strict_aware_and_canonical_utc() -> None:
    event = agent_event(
        kind="usage",
        source="acp",
        worker_id="worker-1",
        source_event_id="event-1",
        payload={"at": datetime(2026, 7, 31, tzinfo=timezone.utc)},
        observed_at="2026-08-01T08:00:00+08:00",
    )
    assert event.observed_at == "2026-08-01T00:00:00+00:00"
    assert event.payload["at"] == "2026-07-31T00:00:00+00:00"
    for invalid in ("not-a-time", "/home/private", "2026-07-31T00:00:00"):
        with pytest.raises(ValueError, match="aware ISO-8601"):
            agent_event(
                kind="usage",
                source="acp",
                worker_id="worker-1",
                source_event_id="event-1",
                payload={},
                observed_at=invalid,
            )
    with pytest.raises(ValueError, match="naive datetime"):
        agent_event(
            kind="usage",
            source="acp",
            worker_id="worker-1",
            source_event_id="event-1",
            payload={"at": datetime(2026, 7, 31)},
        )


def test_store_fails_closed_when_public_projection_is_corrupted(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "store.db"
    record_test_agent_event(db_path, "host-1", _message_event(sequence=1))
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "UPDATE agent_events SET public_payload_json = ?",
            ('{"cwd":"/home/private","text":"safe"}',),
        )
    with pytest.raises(store_sqlite.StoreSchemaError, match="invalid_agent_event_row"):
        read_public_test_agent_events(db_path, "host-1")


def test_host_scoping_and_concurrent_replay_are_isolated(tmp_path: Path) -> None:
    db_path = tmp_path / "store.db"
    event = _message_event(sequence=1)
    store_sqlite.init_store(db_path)

    with ThreadPoolExecutor(max_workers=8) as executor:
        results = list(
            executor.map(
                lambda _: record_test_agent_event(db_path, "host-1", event),
                range(24),
            )
        )
    assert sum(result.inserted for result in results) == 1
    assert len({result.sequence for result in results}) == 1

    other = record_test_agent_event(db_path, "host-2", event)
    assert other.inserted is True
    assert len(store_sqlite.list_agent_events(db_path, "host-1")) == 1
    assert len(store_sqlite.list_agent_events(db_path, "host-2")) == 1
    assert store_sqlite.list_agent_events(db_path, "host-3") == ()
    with pytest.raises(ValueError, match="host_id must not be empty"):
        store_sqlite.list_agent_events(db_path, "   ")




def test_database_constraints_and_indexes_cover_public_and_source_identity(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "store.db"
    record_test_agent_event(db_path, "host-1", _message_event(sequence=1))
    with sqlite3.connect(db_path) as conn:
        indexes = {
            str(row[1]) for row in conn.execute("PRAGMA index_list(agent_events)")
        }
        assert {
            "idx_agent_events_host_visibility_sequence",
            "idx_agent_events_source_event_identity",
            "idx_agent_events_source_sequence_identity",
        } <= indexes
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "UPDATE agent_events SET kind = 'thought' WHERE host_id = 'host-1'"
            )
        conn.rollback()
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "UPDATE agent_events SET public_payload_json = '[]' "
                "WHERE host_id = 'host-1'"
            )


def test_retention_removes_private_payload_without_replay_state(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "store.db"
    old = replace(
        _message_event(sequence=1, text="private historical payload", visibility="private"),
        observed_at="2026-06-01T00:00:00+00:00",
    )
    recent = replace(
        _message_event(sequence=2, text="recent", visibility="private"),
        observed_at="2026-07-30T00:00:00+00:00",
    )
    inserted = record_test_agent_event(db_path, "host-1", old)
    record_test_agent_event(db_path, "host-1", recent)

    result = store_sqlite.cleanup_agent_event_retention(
        db_path,
        "host-1",
        retention_days=7,
        now="2026-07-31T00:00:00+00:00",
    )

    assert result["deleted"] == 1
    assert "tombstoned" not in result
    assert [item.event.payload["text"] for item in store_sqlite.list_agent_events(db_path, "host-1")] == ["recent"]
    with sqlite3.connect(db_path) as conn:
        encoded = "\n".join(conn.iterdump())
    assert "agent_event_tombstones" not in encoded
    assert "private historical payload" not in encoded

    replay = record_test_agent_event(db_path, "host-1", old)
    assert replay.inserted is True
    assert replay.sequence > inserted.sequence
    with pytest.raises(AgentEventIdentityConflict):
        record_test_agent_event(
            db_path,
            "host-1",
            _message_event(sequence=1, text="changed", visibility="private"),
        )


def test_retention_streams_metadata_for_exact_16_mib_private_row(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "large-retention.db"
    text_limit = 4 * 1024 * 1024
    empty_overhead = len(
        store_sqlite._canonical_json(
            {"part_a": "", "part_b": "", "part_c": "", "part_d": ""}
        ).encode("utf-8")
    )
    payload = {
        "part_a": "a" * text_limit,
        "part_b": "b" * text_limit,
        "part_c": "c" * text_limit,
        "part_d": "d" * (text_limit - empty_overhead),
    }
    assert len(store_sqlite._canonical_json(payload).encode("utf-8")) == (
        AGENT_EVENT_MAX_PAYLOAD_BYTES
    )
    event = agent_event(
        kind="agent_message",
        source="acp",
        worker_id="worker-1",
        source_session_id="private-session-1",
        source_sequence=91,
        visibility="private",
        payload=payload,
        observed_at="2026-06-01T00:00:00+00:00",
    )
    record_test_agent_event(db_path, "host-1", event)
    assert "private_payload_json" not in (
        store_sqlite._AGENT_EVENT_RETENTION_SELECT.lower()
    )

    def forbidden_full_row(_row: object) -> object:
        raise AssertionError("retention must not materialize a private event row")

    monkeypatch.setattr(store_sqlite, "_agent_event_from_row", forbidden_full_row)
    tracemalloc.start()
    result = store_sqlite.cleanup_agent_event_retention(
        db_path,
        "host-1",
        retention_days=7,
        now="2026-07-31T00:00:00+00:00",
    )
    _current_bytes, peak_bytes = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    assert result["deleted"] == 1
    assert peak_bytes < 8 * 1024 * 1024
    with sqlite3.connect(db_path) as conn:
        assert conn.execute(
            "SELECT COUNT(*) FROM agent_events WHERE host_id = ?",
            ("host-1",),
        ).fetchone() == (0,)


def test_automatic_maintenance_retires_agent_events_only_when_due(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "automatic-agent-retention.db"
    old = replace(
        _message_event(sequence=1, text="old", visibility="private"),
        observed_at="2026-01-01T00:00:00+00:00",
    )
    recent = replace(
        _message_event(sequence=2, text="recent", visibility="private"),
        observed_at="2026-01-31T23:00:00+00:00",
    )
    record_test_agent_event(db_path, "host-1", old)
    record_test_agent_event(db_path, "host-1", recent)
    policy = store_sqlite.SnapshotRetentionPolicy(
        retention_days=30,
        retention_count=100,
        batch_size=10,
    )

    first = store_sqlite.maybe_run_automatic_store_maintenance(
        db_path,
        policy=policy,
        agent_event_host_id="host-1",
        agent_event_retention_days=7,
        cadence_seconds=3600,
        now="2026-02-01T00:00:00+00:00",
    )
    late_old = replace(
        _message_event(sequence=3, text="late old", visibility="private"),
        observed_at="2026-01-02T00:00:00+00:00",
    )
    record_test_agent_event(db_path, "host-1", late_old)
    not_due = store_sqlite.maybe_run_automatic_store_maintenance(
        db_path,
        policy=policy,
        agent_event_host_id="host-1",
        agent_event_retention_days=7,
        cadence_seconds=3600,
        now="2026-02-01T00:10:00+00:00",
    )
    second = store_sqlite.maybe_run_automatic_store_maintenance(
        db_path,
        policy=policy,
        agent_event_host_id="host-1",
        agent_event_retention_days=7,
        cadence_seconds=3600,
        now="2026-02-01T01:00:00+00:00",
    )

    assert first["agent_events"]["deleted"] == 1
    assert not_due["status"] == "not_due"
    assert not_due["agent_events"]["deleted"] == 0
    assert second["agent_events"]["deleted"] == 1
    assert [
        item.event.payload["text"]
        for item in store_sqlite.list_agent_events(db_path, "host-1")
    ] == ["recent"]
    replay = record_test_agent_event(db_path, "host-1", old)
    assert replay.inserted is True
    with pytest.raises(AgentEventIdentityConflict):
        record_test_agent_event(
            db_path,
            "host-1",
            _message_event(sequence=1, text="changed", visibility="private"),
        )


def test_journal_accepts_acp_sized_private_text(tmp_path: Path) -> None:
    event = _message_event(sequence=1, text="x" * (64 * 1024), visibility="private")
    result = record_test_agent_event(tmp_path / "store.db", "host-1", event)
    assert result.inserted is True
