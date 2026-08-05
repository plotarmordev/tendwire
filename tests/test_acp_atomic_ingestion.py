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
from .model_helpers import project_from_raw
from tendwire.store.events import list_agent_events
from tendwire.store.projection import save_snapshot, upsert_worker_bindings
from tendwire.store.schema import init_store
from tendwire.store.turns import (
    append_agent_event_and_apply_turn_for_binding,
    turns_payload_from_store,
)


def _store(
    tmp_path: Path,
    *,
    stable_owner: bool = False,
) -> tuple[Config, WorkerBinding]:
    db_path = tmp_path / "events.db"
    config = Config(host_id="host-a", db_path=db_path)
    snapshot = project_from_raw(
        config,
        workers=[
            {
                "id": "worker-a",
                "name": "Worker A",
                "meta": {
                    "stable_key": "wsk1_" + ("a" * 64),
                    "stable_key_version": 1,
                },
            }
        ],
    )
    init_store(db_path)
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
    persisted = save_snapshot(
        db_path,
        snapshot,
        worker_bindings=[binding],
        binding_backend="herdr",
    )
    return config, replace(binding, worker_fingerprint=persisted.workers[0].fingerprint)


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




def test_replay_cannot_supersede_newer_turn_or_requeue_connector(
    tmp_path: Path,
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
    )

    assert replayed.event.status == "replayed"
    assert replayed.turn is None
    turns = turns_payload_from_store(config.db_path, config.host_id)["turns"]
    assert turns == turns_before
    with sqlite3.connect(config.db_path) as conn:
        assert conn.execute(
            "SELECT turn_id, status FROM connector_outbox ORDER BY id"
        ).fetchall() == outbox_before


def test_replay_repairs_only_absent_projection_with_original_authority_time(
    tmp_path: Path,
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
    repaired = append_agent_event_and_apply_turn_for_binding(
        config.db_path,
        config.host_id,
        replace(event, observed_at="2030-01-01T00:00:00+00:00"),
        expected_binding=binding,
        content=_content(complete=True),
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
    assert turn_before[1] == "2020-01-01T00:00:00.000000Z"

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
