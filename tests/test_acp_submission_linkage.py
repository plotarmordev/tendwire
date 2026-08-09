"""Exact ACP producer-submission linkage across prompt and steering paths."""

from __future__ import annotations

import json
import sqlite3
from dataclasses import replace
from pathlib import Path

import pytest

from tendwire.backends.acp_ingestion import AcpSessionIngestor
from tendwire.config import Config
from tendwire.core.commands import turn_submission_id
from tendwire.core.models import Snapshot, Worker, WorkerBinding
from tendwire.store.projection import save_snapshot, upsert_worker_bindings
from tendwire.store.receipts import (
    linked_turn_for_submission,
    mark_command_send_started,
    reserve_command_request,
    settle_submission_link_for_request,
)
from tendwire.store.schema import init_store


NOW = "2026-08-09T00:00:00.000000Z"


def _binding(worker: Worker, *, backend: str, fingerprint: str) -> WorkerBinding:
    return WorkerBinding(
        host_id="host-a",
        worker_id=worker.id,
        worker_fingerprint=worker.fingerprint,
        backend=backend,
        target_kind="terminal_id",
        target_value="terminal-private",
        turn_target_kind=("acp_session_id" if backend == "acp" else None),
        turn_target_value=("session-a" if backend == "acp" else None),
        sendable=True,
        observed_at=NOW,
        private_fingerprint=fingerprint,
    )


def _store(tmp_path: Path) -> tuple[Config, Worker, WorkerBinding]:
    config = Config(host_id="host-a", db_path=tmp_path / "link.db")
    init_store(config.db_path)
    worker = Worker(
        id="worker-a",
        name="codex",
        meta={"stable_key": "wsk1_" + "a" * 64, "stable_key_version": 1},
    )
    persisted = save_snapshot(
        config.db_path,
        Snapshot(host_id=config.host_id, updated_at=NOW, workers=[worker]),
        worker_bindings=[_binding(worker, backend="herdr", fingerprint="herdr-a")],
        binding_backend="herdr",
    ).workers[0]
    acp = _binding(persisted, backend="acp", fingerprint="acp-a")
    upsert_worker_bindings(config.db_path, [acp])
    return config, persisted, acp


def _submission(
    config: Config,
    worker: Worker,
    binding: WorkerBinding,
    request_id: str,
    *,
    instruction_text: str,
    now: str | None = NOW,
) -> str:
    reserved = reserve_command_request(
        config.db_path,
        host_id=config.host_id,
        request_id=request_id,
        action="send_instruction",
        canonical_version=1,
        canonical_fingerprint=f"fingerprint-{request_id}",
        canonical_request_json=json.dumps({"instruction": instruction_text}),
        public_worker_id=worker.id,
        pending_result_json='{"status":"pending"}',
        now=now,
    )
    started = mark_command_send_started(
        config.db_path,
        host_id=config.host_id,
        request_id=request_id,
        canonical_fingerprint=f"fingerprint-{request_id}",
        owner_token=reserved["owner_token"],
        binding_fingerprint=binding.private_fingerprint,
        submission_worker=worker,
        instruction_text=instruction_text,
        now=now,
    )
    assert started["status"] == "send_started"
    return str(started["submission_id"])


def _prompt(text: str) -> list[dict[str, str]]:
    return [{"type": "text", "text": text}]


def _submission_row(db_path: Path, request_id: str) -> tuple[str, str | None]:
    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            "SELECT state,turn_id FROM turn_submissions WHERE request_id=?",
            (request_id,),
        ).fetchone()
    assert row is not None
    return str(row[0]), row[1]


def test_injected_steering_links_exact_submission_to_current_turn(
    tmp_path: Path,
) -> None:
    config, worker, binding = _store(tmp_path)
    ingestor = AcpSessionIngestor(
        config,
        session_id="session-a",
        stream_generation="generation-a",
        binding=binding,
    )
    initial = ingestor.begin_prompt(_prompt("initial"), producer_turn_id="legacy-a")
    assert initial.event is not None
    current_turn = ingestor.source_turn_id
    submission_id = _submission(
        config,
        worker,
        binding,
        "steer-a",
        # Prove linkage does not depend on the composite projected user text.
        instruction_text="a deliberately different receipt instruction",
    )

    appended = ingestor.append_prompt(
        _prompt("exact steering prompt"), producer_turn_id=submission_id
    )

    assert appended.event is not None and appended.event.status == "inserted"
    assert ingestor.source_turn_id == current_turn
    assert _submission_row(config.db_path, "steer-a") == ("linked", current_turn)
    linked = linked_turn_for_submission(
        config.db_path, host_id=config.host_id, request_id="steer-a"
    )
    assert linked is not None and linked["turn_id"] == current_turn


def test_standard_prompt_links_exact_submission_to_its_new_turn(tmp_path: Path) -> None:
    config, worker, binding = _store(tmp_path)
    submission_id = turn_submission_id(config.host_id, "prompt-a")
    ingestor = AcpSessionIngestor(
        config,
        session_id="session-a",
        stream_generation="generation-a",
        binding=binding,
    )

    recorded = ingestor.begin_prompt(
        _prompt("new prompt"), producer_turn_id=submission_id
    )
    assert _submission(
        config,
        worker,
        binding,
        "prompt-a",
        instruction_text="new prompt",
        now=None,
    ) == submission_id
    settled = settle_submission_link_for_request(
        config.db_path,
        host_id=config.host_id,
        request_id="prompt-a",
    )

    assert recorded.event is not None and recorded.event.status == "inserted"
    assert settled is not None and settled["state"] == "linked"
    assert _submission_row(config.db_path, "prompt-a") == (
        "linked",
        ingestor.source_turn_id,
    )


def test_exact_submission_link_replay_is_idempotent(tmp_path: Path) -> None:
    config, worker, binding = _store(tmp_path)
    submission_id = _submission(
        config, worker, binding, "replay-a", instruction_text="once"
    )
    first = AcpSessionIngestor(
        config,
        session_id="session-a",
        stream_generation="generation-a",
        binding=binding,
    )
    first.begin_prompt(_prompt("once"), producer_turn_id=submission_id)
    turn_id = first.source_turn_id
    second = AcpSessionIngestor(
        config,
        session_id="session-a",
        stream_generation="generation-b",
        binding=binding,
    )

    replayed = second.begin_prompt(_prompt("once"), producer_turn_id=submission_id)

    assert replayed.event is not None and replayed.event.status == "replayed"
    assert second.source_turn_id == turn_id
    assert _submission_row(config.db_path, "replay-a") == ("linked", turn_id)
    with sqlite3.connect(config.db_path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM turns").fetchone() == (1,)
        assert conn.execute("SELECT COUNT(*) FROM agent_events").fetchone() == (1,)


def test_changed_acp_binding_cannot_link_or_project_submission(tmp_path: Path) -> None:
    config, worker, binding = _store(tmp_path)
    submission_id = _submission(
        config, worker, binding, "stale-a", instruction_text="must stay fenced"
    )
    changed = replace(binding, private_fingerprint="acp-b", observed_at="2026-08-09T00:00:01Z")
    upsert_worker_bindings(config.db_path, [changed])
    ingestor = AcpSessionIngestor(
        config,
        session_id="session-a",
        stream_generation="generation-b",
        binding=changed,
    )

    with pytest.raises(RuntimeError, match="receipt authority changed"):
        ingestor.begin_prompt(_prompt("must stay fenced"), producer_turn_id=submission_id)

    assert _submission_row(config.db_path, "stale-a") == ("send_started", None)
    with sqlite3.connect(config.db_path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM turns").fetchone() == (0,)
        assert conn.execute("SELECT COUNT(*) FROM agent_events").fetchone() == (0,)


def test_unknown_submission_id_cannot_project_prompt(tmp_path: Path) -> None:
    config, _worker, binding = _store(tmp_path)
    ingestor = AcpSessionIngestor(
        config,
        session_id="session-a",
        stream_generation="generation-a",
        binding=binding,
    )

    with pytest.raises(ValueError, match="malformed reserved namespace"):
        ingestor.begin_prompt(
            _prompt("unknown"), producer_turn_id="twsub1." + "z" * 43
        )

    with sqlite3.connect(config.db_path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM turns").fetchone() == (0,)
        assert conn.execute("SELECT COUNT(*) FROM agent_events").fetchone() == (0,)


def test_unknown_canonical_submission_cannot_project_steering(tmp_path: Path) -> None:
    config, _worker, binding = _store(tmp_path)
    ingestor = AcpSessionIngestor(
        config,
        session_id="session-a",
        stream_generation="generation-a",
        binding=binding,
    )
    ingestor.begin_prompt(_prompt("initial"), producer_turn_id="legacy-a")
    turn_id = ingestor.source_turn_id

    with pytest.raises(RuntimeError, match="submission authority unavailable"):
        ingestor.append_prompt(
            _prompt("unknown steering"),
            producer_turn_id="twsub1." + "a" * 64,
        )

    with sqlite3.connect(config.db_path) as conn:
        user_text = conn.execute(
            """SELECT user_text FROM turn_content_revisions
            WHERE host_id='host-a' AND turn_id=? AND is_current=1""",
            (turn_id,),
        ).fetchone()[0]
        assert user_text == "initial"
        assert conn.execute("SELECT COUNT(*) FROM agent_events").fetchone() == (1,)


def test_submission_id_cannot_be_reused_for_another_turn(tmp_path: Path) -> None:
    config, worker, binding = _store(tmp_path)
    submission_id = _submission(
        config, worker, binding, "duplicate-a", instruction_text="once"
    )
    first = AcpSessionIngestor(
        config,
        session_id="session-a",
        stream_generation="generation-a",
        binding=binding,
    )
    first.begin_prompt(_prompt("once"), producer_turn_id=submission_id)
    linked_turn = first.source_turn_id
    second = AcpSessionIngestor(
        config,
        session_id="session-a",
        stream_generation="generation-b",
        binding=binding,
    )
    second.begin_prompt(_prompt("another turn"), producer_turn_id="legacy-b")
    other_turn = second.source_turn_id

    with pytest.raises(RuntimeError, match="already linked elsewhere"):
        second.append_prompt(_prompt("duplicate"), producer_turn_id=submission_id)

    assert other_turn != linked_turn
    assert _submission_row(config.db_path, "duplicate-a") == ("linked", linked_turn)
    with sqlite3.connect(config.db_path) as conn:
        user_text = conn.execute(
            """SELECT r.user_text FROM turn_content_revisions r
            WHERE r.host_id='host-a' AND r.turn_id=? AND r.is_current=1""",
            (other_turn,),
        ).fetchone()[0]
    assert user_text == "another turn"


def test_linked_submission_replay_survives_removed_turn(tmp_path: Path) -> None:
    config, worker, binding = _store(tmp_path)
    submission_id = _submission(
        config, worker, binding, "removed-a", instruction_text="once"
    )
    first = AcpSessionIngestor(
        config,
        session_id="session-a",
        stream_generation="generation-a",
        binding=binding,
    )
    first.begin_prompt(_prompt("once"), producer_turn_id=submission_id)
    turn_id = first.source_turn_id
    with sqlite3.connect(config.db_path) as conn:
        conn.execute(
            "UPDATE turns SET removed_at=? WHERE host_id=? AND turn_id=?",
            (NOW, config.host_id, turn_id),
        )
        conn.commit()
    replay = AcpSessionIngestor(
        config,
        session_id="session-a",
        stream_generation="generation-b",
        binding=binding,
    )

    result = replay.begin_prompt(_prompt("once"), producer_turn_id=submission_id)

    assert result.event is not None and result.event.status == "replayed"
    assert _submission_row(config.db_path, "removed-a") == ("linked", turn_id)
