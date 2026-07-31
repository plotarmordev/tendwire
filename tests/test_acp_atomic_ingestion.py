"""Atomicity and recovery tests for ACP journal-to-turn ingestion."""

from __future__ import annotations

import sqlite3
from dataclasses import replace
from pathlib import Path

import pytest

from tendwire.backends.acp_ingestion import AcpSessionIngestor
from tendwire.config import Config
from tendwire.core.agent_events import AgentEvent, agent_event
from tendwire.core.models import WorkerBinding
from tendwire.core.projector import project_from_raw
from tendwire.store import sqlite as store_sqlite
from tendwire.store.sqlite import (
    append_agent_event_and_apply_turn_for_binding,
    cleanup_agent_event_retention,
    init_store,
    list_agent_events,
    save_snapshot,
    turns_payload_from_store,
    upsert_worker_bindings,
)


def _store(
    tmp_path: Path,
    *,
    source: str = "acp_preferred",
    stable_owner: bool = False,
) -> tuple[Config, WorkerBinding]:
    db_path = tmp_path / "events.db"
    config = Config(host_id="host-a", db_path=db_path, agent_event_source=source)
    snapshot = project_from_raw(
        config,
        workers=[
            {
                "id": "worker-a",
                "name": "Worker A",
                "meta": (
                    {
                        "stable_key": "wsk1_" + ("a" * 64),
                        "stable_key_version": 1,
                    }
                    if stable_owner
                    else {}
                ),
            }
        ],
    )
    init_store(db_path)
    save_snapshot(db_path, snapshot)
    worker = snapshot.workers[0]
    binding = WorkerBinding(
        host_id=config.host_id,
        worker_id=worker.id,
        worker_fingerprint=worker.fingerprint,
        backend="herdr",
        target_kind="pane_id",
        target_value="pane-a",
        turn_target_kind="acp_session_id",
        turn_target_value="session-a",
        sendable=True,
        private_fingerprint="private-a",
    )
    upsert_worker_bindings(db_path, [binding])
    return config, binding


def _event(
    binding: WorkerBinding,
    *,
    observed_at: str = "2026-07-31T00:00:00+00:00",
    source_turn_id: str = "turn-a",
    message_id: str = "message-a",
    source_event_id: str = "event-a",
    text: str = "answer",
) -> AgentEvent:
    return agent_event(
        kind="agent_message",
        source="acp",
        worker_id=binding.worker_id,
        payload={"schema_version": 1, "message_id": message_id, "text": text},
        source_session_id="session-a",
        source_turn_id=source_turn_id,
        source_message_id=message_id,
        source_event_id=source_event_id,
        observed_at=observed_at,
    )


def _content(
    *,
    complete: bool = False,
    source_turn_id: str = "turn-a",
    text: str = "answer",
) -> dict[str, object]:
    return {
        "source_turn_id": source_turn_id,
        "user_text": "",
        "assistant_stream_text": "" if complete else text,
        "assistant_final_text": text if complete else "",
        "complete": complete,
        "has_open_turn": not complete,
    }


def _counts(db_path: Path) -> tuple[int, int]:
    with sqlite3.connect(db_path) as conn:
        events = int(conn.execute("SELECT COUNT(*) FROM agent_events").fetchone()[0])
        turns = int(conn.execute("SELECT COUNT(*) FROM turns").fetchone()[0])
    return events, turns


@pytest.mark.parametrize(
    "boundary",
    (
        "after_binding_check",
        "after_event_append",
        "after_turn_projection",
        "before_commit",
    ),
)
def test_every_atomic_boundary_rolls_back_and_retry_succeeds(
    tmp_path: Path,
    boundary: str,
) -> None:
    config, binding = _store(tmp_path)
    event = _event(binding)

    def fail(current: str) -> None:
        if current == boundary:
            raise RuntimeError(boundary)

    with pytest.raises(RuntimeError, match=boundary):
        append_agent_event_and_apply_turn_for_binding(
            config.db_path,
            config.host_id,
            event,
            expected_binding=binding,
            content=_content(),
            _fault_inject=fail,
        )

    assert _counts(config.db_path) == (0, 0)
    retried = append_agent_event_and_apply_turn_for_binding(
        config.db_path,
        config.host_id,
        event,
        expected_binding=binding,
        content=_content(),
    )
    assert retried.event.status == "inserted"
    assert retried.turn is not None
    assert _counts(config.db_path) == (1, 1)


def test_stale_binding_writes_neither_journal_nor_projection(tmp_path: Path) -> None:
    config, binding = _store(tmp_path)
    stale = replace(binding, target_value="old-pane")
    result = append_agent_event_and_apply_turn_for_binding(
        config.db_path,
        config.host_id,
        _event(binding),
        expected_binding=stale,
        content=_content(),
    )

    assert result.event.status == "binding_changed"
    assert result.turn is None
    assert _counts(config.db_path) == (0, 0)


def test_tombstoned_replay_can_repair_projection_without_reinserting_event(
    tmp_path: Path,
) -> None:
    config, binding = _store(tmp_path)
    event = _event(binding, observed_at="2020-01-01T00:00:00+00:00")
    inserted = append_agent_event_and_apply_turn_for_binding(
        config.db_path,
        config.host_id,
        event,
        expected_binding=binding,
    )
    assert inserted.event.status == "inserted"
    cleanup = cleanup_agent_event_retention(
        config.db_path,
        config.host_id,
        retention_days=1,
        now="2026-07-31T00:00:00+00:00",
    )
    assert cleanup["tombstoned"] == 1
    assert len(list_agent_events(config.db_path, config.host_id)) == 0

    repaired = append_agent_event_and_apply_turn_for_binding(
        config.db_path,
        config.host_id,
        event,
        expected_binding=binding,
        content=_content(),
    )
    assert repaired.event.status == "replayed"
    assert repaired.turn is not None
    assert _counts(config.db_path) == (0, 1)


@pytest.mark.parametrize("retire_original", (False, True))
def test_replay_cannot_supersede_newer_turn_or_requeue_connector(
    tmp_path: Path,
    retire_original: bool,
) -> None:
    config, binding = _store(tmp_path, stable_owner=True)
    old = _event(
        binding,
        observed_at="2020-01-01T00:00:00+00:00",
        source_turn_id="turn-old",
        message_id="message-old",
        source_event_id="event-old",
        text="old",
    )
    first = append_agent_event_and_apply_turn_for_binding(
        config.db_path,
        config.host_id,
        old,
        expected_binding=binding,
        content=_content(complete=True, source_turn_id="turn-old", text="old"),
    )
    assert first.event.status == "inserted"
    if retire_original:
        assert cleanup_agent_event_retention(
            config.db_path,
            config.host_id,
            retention_days=1,
            now="2021-01-01T00:00:00+00:00",
        )["tombstoned"] == 1

    new = _event(
        binding,
        observed_at="2026-01-01T00:00:00+00:00",
        source_turn_id="turn-new",
        message_id="message-new",
        source_event_id="event-new",
        text="new",
    )
    second = append_agent_event_and_apply_turn_for_binding(
        config.db_path,
        config.host_id,
        new,
        expected_binding=binding,
        content=_content(complete=True, source_turn_id="turn-new", text="new"),
    )
    assert second.event.status == "inserted"
    turns_before = turns_payload_from_store(config.db_path, config.host_id)["turns"]
    assert turns_before[0]["assistant_final_text"] == "new"
    with sqlite3.connect(config.db_path) as conn:
        outbox_before = conn.execute(
            "SELECT turn_id, status FROM connector_outbox ORDER BY id"
        ).fetchall()

    replayed = append_agent_event_and_apply_turn_for_binding(
        config.db_path,
        config.host_id,
        replace(old, observed_at="2030-01-01T00:00:00+00:00"),
        expected_binding=binding,
        content=_content(complete=True, source_turn_id="turn-old", text="old"),
        observed_at="2040-01-01T00:00:00+00:00",
    )

    assert replayed.event.status == "replayed"
    assert replayed.turn is None
    turns = turns_payload_from_store(config.db_path, config.host_id)["turns"]
    assert turns == turns_before
    with sqlite3.connect(config.db_path) as conn:
        assert conn.execute(
            "SELECT turn_id, status FROM connector_outbox ORDER BY id"
        ).fetchall() == outbox_before


@pytest.mark.parametrize("retire_original", (False, True))
def test_replay_repairs_only_absent_projection_with_original_authority_time(
    tmp_path: Path,
    retire_original: bool,
) -> None:
    config, binding = _store(tmp_path, stable_owner=True)
    event = _event(binding, observed_at="2020-01-01T00:00:00+00:00")
    inserted = append_agent_event_and_apply_turn_for_binding(
        config.db_path,
        config.host_id,
        event,
        expected_binding=binding,
    )
    assert inserted.event.status == "inserted"
    if retire_original:
        assert cleanup_agent_event_retention(
            config.db_path,
            config.host_id,
            retention_days=1,
            now="2021-01-01T00:00:00+00:00",
        )["tombstoned"] == 1

    repaired = append_agent_event_and_apply_turn_for_binding(
        config.db_path,
        config.host_id,
        replace(event, observed_at="2030-01-01T00:00:00+00:00"),
        expected_binding=binding,
        content=_content(complete=True),
        observed_at="2040-01-01T00:00:00+00:00",
    )
    assert repaired.event.status == "replayed"
    assert repaired.turn is not None and repaired.turn.updated == 1
    with sqlite3.connect(config.db_path) as conn:
        turn_before = conn.execute(
            "SELECT payload_json, observed_at FROM turns"
        ).fetchone()
        outbox_before = conn.execute(
            "SELECT turn_id, status FROM connector_outbox ORDER BY id"
        ).fetchall()
    assert turn_before is not None
    assert turn_before[1] == "2020-01-01T00:00:00+00:00"

    ignored = append_agent_event_and_apply_turn_for_binding(
        config.db_path,
        config.host_id,
        replace(event, observed_at="2050-01-01T00:00:00+00:00"),
        expected_binding=binding,
        content=_content(complete=True, text="caller rewrite"),
    )
    assert ignored.event.status == "replayed"
    assert ignored.turn is None
    with sqlite3.connect(config.db_path) as conn:
        assert conn.execute(
            "SELECT payload_json, observed_at FROM turns"
        ).fetchone() == turn_before
        assert conn.execute(
            "SELECT turn_id, status FROM connector_outbox ORDER BY id"
        ).fetchall() == outbox_before


def test_legacy_tombstone_without_authority_time_cannot_repair(tmp_path: Path) -> None:
    config, binding = _store(tmp_path)
    event = _event(binding, observed_at="2020-01-01T00:00:00+00:00")
    append_agent_event_and_apply_turn_for_binding(
        config.db_path,
        config.host_id,
        event,
        expected_binding=binding,
    )
    cleanup_agent_event_retention(
        config.db_path,
        config.host_id,
        retention_days=1,
        now="2021-01-01T00:00:00+00:00",
    )
    with sqlite3.connect(config.db_path) as conn:
        conn.execute("UPDATE agent_event_tombstones SET observed_at = NULL")

    replayed = append_agent_event_and_apply_turn_for_binding(
        config.db_path,
        config.host_id,
        event,
        expected_binding=binding,
        content=_content(complete=True),
    )
    assert replayed.event.status == "replayed"
    assert replayed.turn is None
    assert _counts(config.db_path) == (0, 0)


def test_replay_repair_respects_binding_fence_and_rolls_back(tmp_path: Path) -> None:
    config, binding = _store(tmp_path)
    event = _event(binding)
    append_agent_event_and_apply_turn_for_binding(
        config.db_path,
        config.host_id,
        event,
        expected_binding=binding,
    )
    stale = replace(binding, target_value="old-pane")
    fenced = append_agent_event_and_apply_turn_for_binding(
        config.db_path,
        config.host_id,
        event,
        expected_binding=stale,
        content=_content(),
    )
    assert fenced.event.status == "binding_changed"
    assert fenced.turn is None
    assert _counts(config.db_path) == (1, 0)

    def fail(boundary: str) -> None:
        if boundary == "before_commit":
            raise RuntimeError("repair rollback")

    with pytest.raises(RuntimeError, match="repair rollback"):
        append_agent_event_and_apply_turn_for_binding(
            config.db_path,
            config.host_id,
            event,
            expected_binding=binding,
            content=_content(),
            _fault_inject=fail,
        )
    assert _counts(config.db_path) == (1, 0)
    repaired = append_agent_event_and_apply_turn_for_binding(
        config.db_path,
        config.host_id,
        event,
        expected_binding=binding,
        content=_content(),
    )
    assert repaired.event.status == "replayed"
    assert repaired.turn is not None
    assert _counts(config.db_path) == (1, 1)


def _update(text: str) -> dict[str, object]:
    return {
        "method": "session/update",
        "params": {
            "sessionId": "session-a",
            "update": {
                "sessionUpdate": "agent_message_chunk",
                "messageId": "message-a",
                "content": {"type": "text", "text": text},
            },
        },
    }


def test_process_reconstruction_replays_text_then_completes_once(
    tmp_path: Path,
) -> None:
    config, binding = _store(tmp_path)
    first = AcpSessionIngestor(
        config,
        session_id="session-a",
        stream_generation="generation-a",
        binding=binding,
    )
    first.start_turn(producer_turn_id="producer-a")
    first.ingest_update(_update("answer"), source_event_id="message-event-a")

    reconstructed = AcpSessionIngestor(
        config,
        session_id="session-a",
        stream_generation="generation-b",
        binding=binding,
    )
    reconstructed.start_turn(producer_turn_id="producer-a")
    replayed = reconstructed.ingest_update(
        _update("answer"), source_event_id="message-event-a", replay=True
    )
    completed = reconstructed.mark_prompt_complete()

    assert replayed.event is not None and replayed.event.status == "replayed"
    assert completed.event is not None and completed.event.status == "inserted"
    turns = turns_payload_from_store(config.db_path, config.host_id)["turns"]
    assert len(turns) == 1
    assert turns[0]["assistant_final_text"] == "answer"
    with sqlite3.connect(config.db_path) as conn:
        before = int(
            conn.execute("SELECT COUNT(*) FROM connector_outbox").fetchone()[0]
        )

    second_reconstruction = AcpSessionIngestor(
        config,
        session_id="session-a",
        stream_generation="generation-c",
        binding=binding,
    )
    second_reconstruction.start_turn(producer_turn_id="producer-a")
    second_reconstruction.ingest_update(
        _update("answer"), source_event_id="message-event-a", replay=True
    )
    completion_replay = second_reconstruction.mark_prompt_complete()
    assert completion_replay.event is not None
    assert completion_replay.event.status == "replayed"
    with sqlite3.connect(config.db_path) as conn:
        after = int(conn.execute("SELECT COUNT(*) FROM connector_outbox").fetchone()[0])
    assert after == before


def test_completion_failure_rolls_back_marker_and_final_then_retries(
    tmp_path: Path,
) -> None:
    config, binding = _store(tmp_path)
    fail_completion = True

    def persist(*args, **kwargs):
        def fault(boundary: str) -> None:
            if (
                fail_completion
                and args[2].kind == "extension"
                and boundary == "before_commit"
            ):
                raise RuntimeError("completion fault")

        kwargs["_fault_inject"] = fault
        return append_agent_event_and_apply_turn_for_binding(*args, **kwargs)

    ingestor = AcpSessionIngestor(
        config,
        session_id="session-a",
        stream_generation="generation-a",
        binding=binding,
        persist_event=persist,
    )
    ingestor.start_turn(producer_turn_id="producer-a")
    ingestor.ingest_update(_update("answer"), source_event_id="message-event-a")
    with pytest.raises(RuntimeError, match="completion fault"):
        ingestor.mark_prompt_complete()
    assert len(list_agent_events(config.db_path, config.host_id)) == 1

    fail_completion = False
    completed = ingestor.mark_prompt_complete()
    assert completed.event is not None and completed.event.status == "inserted"
    assert len(list_agent_events(config.db_path, config.host_id)) == 2
    turn = turns_payload_from_store(config.db_path, config.host_id)["turns"][0]
    assert turn["assistant_final_text"] == "answer"


def test_shadow_mode_journals_completion_without_projecting_turn(
    tmp_path: Path,
) -> None:
    config, binding = _store(tmp_path, source="acp_shadow")
    ingestor = AcpSessionIngestor(
        config,
        session_id="session-a",
        stream_generation="generation-a",
        binding=binding,
    )
    ingestor.start_turn(producer_turn_id="producer-a")
    ingestor.ingest_update(_update("answer"), source_event_id="message-event-a")
    ingestor.mark_prompt_complete()

    events = list_agent_events(config.db_path, config.host_id)
    assert [item.event.kind for item in events] == ["agent_message", "extension"]
    assert turns_payload_from_store(config.db_path, config.host_id)["turns"] == []
