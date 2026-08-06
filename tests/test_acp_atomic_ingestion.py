"""Atomicity and recovery tests for ACP journal-to-turn ingestion."""

from __future__ import annotations

import json
import sqlite3
from dataclasses import replace
from pathlib import Path

import pytest

from tendwire.backends.acp_ingestion import AcpSessionIngestor
from tendwire.config import Config
from tendwire.connectors import ConnectorOutboxAPI
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


def test_acp_ingestion_projects_through_durable_herdr_route(tmp_path: Path) -> None:
    config, herdr_binding = _store(tmp_path)
    acp_binding = replace(
        herdr_binding,
        backend="acp",
        turn_target_kind="acp_session_id",
        turn_target_value="session-a",
        private_fingerprint="private-acp",
    )
    upsert_worker_bindings(config.db_path, [acp_binding])

    result = append_agent_event_and_apply_turn_for_binding(
        config.db_path,
        config.host_id,
        _event(acp_binding, source_turn_id="acpt_" + "a" * 24),
        expected_binding=acp_binding,
        content=_content(complete=True, source_turn_id="acpt_" + "a" * 24),
    )

    assert result.event.status == "inserted"
    with sqlite3.connect(config.db_path) as conn:
        generations = dict(
            conn.execute(
                "SELECT backend,route_generation FROM worker_bindings"
            ).fetchall()
        )
        turn_generation = conn.execute(
            "SELECT route_generation FROM turns"
        ).fetchone()[0]
        outbox_generation = conn.execute(
            "SELECT json_extract(payload_json,'$.worker.route_generation') "
            "FROM connector_outbox WHERE kind='final_ready'"
        ).fetchone()[0]
    assert generations["acp"] != generations["herdr"]
    assert turn_generation == generations["herdr"]
    assert outbox_generation == generations["herdr"]


def test_acp_content_fails_closed_without_durable_herdr_route(tmp_path: Path) -> None:
    config, herdr_binding = _store(tmp_path)
    acp_binding = replace(
        herdr_binding,
        backend="acp",
        turn_target_kind="acp_session_id",
        turn_target_value="session-a",
        private_fingerprint="private-acp",
    )
    upsert_worker_bindings(config.db_path, [acp_binding])
    with sqlite3.connect(config.db_path) as conn:
        conn.execute(
            "UPDATE worker_bindings SET expires_at='2020-01-01T00:00:00.000000Z' "
            "WHERE backend='herdr'"
        )

    with pytest.raises(RuntimeError, match="presentation route unavailable"):
        append_agent_event_and_apply_turn_for_binding(
            config.db_path,
            config.host_id,
            _event(acp_binding),
            expected_binding=acp_binding,
            content=_content(),
        )
    assert _counts(config.db_path) == (0, 0)


def test_stale_acp_lease_cannot_adopt_rotated_herdr_route(tmp_path: Path) -> None:
    config, first_herdr = _store(tmp_path)
    stale_acp = replace(
        first_herdr,
        backend="acp",
        turn_target_kind="acp_session_id",
        turn_target_value="session-a",
        private_fingerprint="private-acp",
    )
    upsert_worker_bindings(config.db_path, [stale_acp])
    replacement = replace(
        first_herdr,
        target_value="pane-b",
        private_fingerprint="private-herdr-b",
        observed_at="2099-08-06T00:00:00Z",
    )
    upsert_worker_bindings(config.db_path, [replacement])

    with pytest.raises(RuntimeError, match="presentation route unavailable"):
        append_agent_event_and_apply_turn_for_binding(
            config.db_path,
            config.host_id,
            _event(stale_acp),
            expected_binding=stale_acp,
            content=_content(),
        )
    assert _counts(config.db_path) == (0, 0)


def test_acp_replay_needs_no_presentation_route_after_projection(tmp_path: Path) -> None:
    config, herdr_binding = _store(tmp_path)
    acp_binding = replace(
        herdr_binding,
        backend="acp",
        turn_target_kind="acp_session_id",
        turn_target_value="session-a",
        private_fingerprint="private-acp",
    )
    upsert_worker_bindings(config.db_path, [acp_binding])
    event = _event(acp_binding)
    content = _content(complete=True)
    first = append_agent_event_and_apply_turn_for_binding(
        config.db_path,
        config.host_id,
        event,
        expected_binding=acp_binding,
        content=content,
    )
    assert first.turn is not None
    with sqlite3.connect(config.db_path) as conn:
        conn.execute(
            "UPDATE worker_bindings SET expires_at='2020-01-01T00:00:00.000000Z' "
            "WHERE backend='herdr'"
        )

    replay = append_agent_event_and_apply_turn_for_binding(
        config.db_path,
        config.host_id,
        event,
        expected_binding=acp_binding,
        content=content,
    )
    assert replay.event.status == "replayed"
    assert replay.turn is None


def test_current_owner_can_remove_turn_after_route_rotation(tmp_path: Path) -> None:
    config, first_binding = _store(tmp_path)
    inserted = append_agent_event_and_apply_turn_for_binding(
        config.db_path,
        config.host_id,
        _event(first_binding),
        expected_binding=first_binding,
        content=_content(complete=True),
    )
    assert inserted.turn is not None
    second_binding = replace(
        first_binding,
        target_value="pane-b",
        private_fingerprint="private-b",
        observed_at="2099-08-06T00:00:00Z",
    )
    upsert_worker_bindings(config.db_path, [second_binding])
    removed = append_agent_event_and_apply_turn_for_binding(
        config.db_path,
        config.host_id,
        _event(
            second_binding,
            observed_at="2099-08-06T00:00:01Z",
            source_event_id="remove-a",
        ),
        expected_binding=second_binding,
        content={"source_turn_id": "turn-a", "removed": True},
    )
    assert removed.turn is not None and removed.turn.updated == 1
    with sqlite3.connect(config.db_path) as conn:
        assert conn.execute(
            "SELECT state FROM turns WHERE turn_id='turn-a'"
        ).fetchone()[0] == "removed"


def test_different_stable_owner_cannot_remove_prior_turn(tmp_path: Path) -> None:
    config, first_binding = _store(tmp_path)
    inserted = append_agent_event_and_apply_turn_for_binding(
        config.db_path,
        config.host_id,
        _event(first_binding),
        expected_binding=first_binding,
        content=_content(complete=True),
    )
    assert inserted.turn is not None
    other = replace(
        first_binding,
        worker_id="worker-b",
        worker_fingerprint="worker-b-fingerprint",
        target_value="pane-b",
        private_fingerprint="private-b",
    )
    with sqlite3.connect(config.db_path) as conn:
        conn.execute(
            """INSERT INTO worker_bindings(
            host_id,worker_id,backend,private_fingerprint,private_binding_json,
            stable_key,stable_key_version,route_generation,partition_key,
            next_partition_sequence,route_retain_until,observed_at,expires_at)
            VALUES(?,?,?,?,?, ?,1,?,?,1,'2099-01-01T00:00:00.000000Z',?,NULL)""",
            (
                config.host_id,
                other.worker_id,
                other.backend,
                other.private_fingerprint,
                json.dumps(
                    {
                        "host_id": other.host_id,
                        "worker_id": other.worker_id,
                        "worker_fingerprint": other.worker_fingerprint,
                        "backend": other.backend,
                        "target_kind": other.target_kind,
                        "target_value": other.target_value,
                        "turn_target_kind": other.turn_target_kind,
                        "turn_target_value": other.turn_target_value,
                        "sendable": True,
                        "reason": None,
                        "observed_at": other.observed_at,
                        "expires_at": None,
                        "private_fingerprint": other.private_fingerprint,
                    },
                    sort_keys=True,
                    separators=(",", ":"),
                ),
                "wsk1_" + "b" * 64,
                "twroute1." + "b" * 43,
                "twpart1_" + "b" * 64,
                other.observed_at,
            ),
        )

    removed = append_agent_event_and_apply_turn_for_binding(
        config.db_path,
        config.host_id,
        _event(
            other,
            observed_at="2099-08-06T00:00:01Z",
            source_event_id="remove-other",
        ),
        expected_binding=other,
        content={"source_turn_id": "turn-a", "removed": True},
    )
    assert removed.turn is not None and removed.turn.updated == 0
    with sqlite3.connect(config.db_path) as conn:
        assert conn.execute(
            "SELECT state FROM turns WHERE turn_id='turn-a'"
        ).fetchone()[0] == "complete"


def test_acp_final_reaches_deliverable_plan_on_durable_route(tmp_path: Path) -> None:
    config, herdr_binding = _store(tmp_path)
    acp_binding = replace(
        herdr_binding,
        backend="acp",
        turn_target_kind="acp_session_id",
        turn_target_value="session-a",
        private_fingerprint="private-acp",
    )
    upsert_worker_bindings(config.db_path, [acp_binding])
    ingestor = AcpSessionIngestor(
        config,
        session_id="session-a",
        stream_generation="generation-a",
        binding=acp_binding,
    )
    turn_id = ingestor.start_turn(producer_turn_id="producer-a")
    assert turn_id.startswith("acpt_")
    ingestor.ingest_update(_update("answer"), source_event_id="message-event-a")
    completed = ingestor.mark_prompt_complete()
    assert completed.turn is not None

    api = ConnectorOutboxAPI(config.db_path, config.host_id)
    ready = api.poll({"name": "turn-final", "limit": 1})["items"][0]
    turn = ready["payload"]["turn"]
    assert turn["turn_id"] == turn_id
    begun = api.prepare(
        {
            "schema_version": 1,
            "action": "begin",
            "name": "turn-final",
            "turn_id": turn_id,
            "content_revision": turn["content_revision"],
            "presentation_version": "turn-present-v3",
            "part_count": 1,
            "source_ref": ready["ref"],
        }
    )
    assert begun["ok"] is True
    prepared = api.prepare(
        {
            "schema_version": 1,
            "action": "part",
            "name": "turn-final",
            "plan_token": begun["plan_token"],
            "ordinal": 0,
            "spans": [
                {
                    "field": "assistant_final_text",
                    "start_char": 0,
                    "end_char": len("answer"),
                }
            ],
        }
    )
    assert prepared["ok"] is True
    committed = api.prepare(
        {
            "schema_version": 1,
            "action": "commit",
            "name": "turn-final",
            "plan_token": begun["plan_token"],
            "source_ref": ready["ref"],
        }
    )
    assert committed["ok"] is True
    part = api.poll({"name": "turn-final", "limit": 1})["items"][0]
    assert part["payload"]["kind"] == "final_part"
    with sqlite3.connect(config.db_path) as conn:
        generations = dict(
            conn.execute(
                "SELECT backend,route_generation FROM worker_bindings"
            ).fetchall()
        )
    ready_generation = ready["payload"]["worker"]["route_generation"]
    part_generation = part["payload"]["worker"]["route_generation"]
    assert ready_generation == generations["herdr"]
    assert part_generation == generations["herdr"]
    assert generations["herdr"] != generations["acp"]




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
