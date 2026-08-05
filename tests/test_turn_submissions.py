"""Command receipt and turn-submission state-machine behavior."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from tendwire.core.models import Snapshot, Worker, WorkerBinding
from tendwire.store.projection import save_snapshot, upsert_worker_bindings
from tendwire.store.retention import RetentionPolicy, run_retention_cycle
from tendwire.store.receipts import (
    finish_command_request,
    get_command_request,
    linked_turn_for_submission,
    mark_command_send_started,
    reserve_command_request,
    settle_submission_link_for_request,
)
from tendwire.store.schema import init_store
from tendwire.store.turns import apply_turn_refresh


NOW = "2026-08-05T00:00:00.000000Z"


def _acp_binding(worker: Worker) -> WorkerBinding:
    return WorkerBinding(
        host_id="host-a",
        worker_id=worker.id,
        worker_fingerprint=worker.fingerprint,
        backend="acp",
        target_kind="acp_session_id",
        target_value="session-private",
        turn_target_kind="acp_session_id",
        turn_target_value="session-private",
        sendable=True,
        observed_at=NOW,
        private_fingerprint="private-acp",
    )


def _seed(db_path: Path) -> Worker:
    init_store(db_path)
    worker = Worker(id="worker-a", name="codex", meta={"stable_key": "wsk1_" + "a" * 64, "stable_key_version": 1})
    binding = WorkerBinding(host_id="host-a", worker_id=worker.id, worker_fingerprint=worker.fingerprint, backend="herdr", target_kind="agent_id", target_value="private", observed_at=NOW, private_fingerprint="private-a")
    base = Snapshot(host_id="host-a", updated_at=NOW, workers=[worker])
    public = save_snapshot(
        db_path, base, worker_bindings=[binding], binding_backend="herdr"
    ).workers[0]
    upsert_worker_bindings(db_path, [_acp_binding(public)])
    return public


def _reserve(db_path: Path, request_id: str = "request-a") -> dict[str, object]:
    return reserve_command_request(
        db_path,
        host_id="host-a",
        request_id=request_id,
        action="send_instruction",
        canonical_version=1,
        canonical_fingerprint="fingerprint-a",
        canonical_request_json=json.dumps({"instruction": "do it"}),
        public_worker_id="worker-a",
        pending_result_json=json.dumps({"status": "pending"}),
        now=NOW,
    )


def test_send_started_requires_current_route_and_owner(tmp_path: Path) -> None:
    db_path = tmp_path / "submissions.db"
    worker = _seed(db_path)
    reserved = _reserve(db_path)
    wrong_owner = mark_command_send_started(
        db_path,
        host_id="host-a",
        request_id="request-a",
        canonical_fingerprint="fingerprint-a",
        owner_token="wrong",
        binding_fingerprint="private-acp",
        submission_worker=worker,
        instruction_text="do it",
        now=NOW,
    )
    assert wrong_owner["status"] == "not_owner"
    started = mark_command_send_started(
        db_path,
        host_id="host-a",
        request_id="request-a",
        canonical_fingerprint="fingerprint-a",
        owner_token=reserved["owner_token"],
        binding_fingerprint="private-acp",
        submission_worker=worker,
        instruction_text="do it",
        now=NOW,
    )
    assert started["status"] == "send_started"
    assert started["submission_id"]


def test_send_started_rejects_herdr_and_unsendable_acp_authority(
    tmp_path: Path,
) -> None:
    herdr_db = tmp_path / "herdr-route.db"
    worker = _seed(herdr_db)
    reserved = _reserve(herdr_db)
    herdr = mark_command_send_started(
        herdr_db,
        host_id="host-a",
        request_id="request-a",
        canonical_fingerprint="fingerprint-a",
        owner_token=reserved["owner_token"],
        binding_fingerprint="private-a",
        submission_worker=worker,
        instruction_text="do it",
        now=NOW,
    )
    assert herdr["status"] == "stale_route"
    assert get_command_request(herdr_db, "host-a", "request-a")["state"] == "reserved"

    unsendable_db = tmp_path / "unsendable-acp.db"
    worker = _seed(unsendable_db)
    unsendable = WorkerBinding(
        host_id="host-a",
        worker_id=worker.id,
        worker_fingerprint=worker.fingerprint,
        backend="acp",
        target_kind="acp_session_id",
        target_value="unsendable-session",
        turn_target_kind="acp_session_id",
        turn_target_value="unsendable-session",
        sendable=False,
        observed_at=NOW,
        private_fingerprint="private-unsendable",
    )
    worker = save_snapshot(
        unsendable_db,
        Snapshot(host_id="host-a", updated_at=NOW, workers=[worker]),
        worker_bindings=[unsendable],
        binding_backend="acp",
    ).workers[0]
    reserved = _reserve(unsendable_db)
    rejected = mark_command_send_started(
        unsendable_db,
        host_id="host-a",
        request_id="request-a",
        canonical_fingerprint="fingerprint-a",
        owner_token=reserved["owner_token"],
        binding_fingerprint="private-unsendable",
        submission_worker=worker,
        instruction_text="do it",
        now=NOW,
    )
    assert rejected["status"] == "stale_route"
    assert get_command_request(unsendable_db, "host-a", "request-a")["state"] == "reserved"


def test_terminal_receipt_is_immutable(tmp_path: Path) -> None:
    db_path = tmp_path / "submissions.db"
    _seed(db_path)
    reserved = _reserve(db_path)
    rejected = finish_command_request(
        db_path,
        host_id="host-a",
        request_id="request-a",
        canonical_fingerprint="fingerprint-a",
        owner_token=reserved["owner_token"],
        expected_state="reserved",
        terminal_state="rejected",
        status="rejected",
        result_json="{}",
        now=NOW,
    )
    assert rejected["status"] == "rejected"
    replay = finish_command_request(
        db_path,
        host_id="host-a",
        request_id="request-a",
        canonical_fingerprint="fingerprint-a",
        owner_token=reserved["owner_token"],
        expected_state="reserved",
        terminal_state="rejected",
        status="rejected",
        result_json='{"changed":true}',
        now=NOW,
    )
    assert replay["status"] == "terminal"
    assert get_command_request(db_path, "host-a", "request-a")["result_json"] == "{}"


def test_accepted_receipt_submission_can_link_later_turn(tmp_path: Path) -> None:
    db_path = tmp_path / "accepted-link.db"
    worker = _seed(db_path)
    reserved = _reserve(db_path)
    started = mark_command_send_started(
        db_path,
        host_id="host-a",
        request_id="request-a",
        canonical_fingerprint="fingerprint-a",
        owner_token=reserved["owner_token"],
        binding_fingerprint="private-acp",
        submission_worker=worker,
        instruction_text="do it",
        now=NOW,
    )
    accepted = finish_command_request(
        db_path,
        host_id="host-a",
        request_id="request-a",
        canonical_fingerprint="fingerprint-a",
        owner_token=started["owner_token"],
        expected_state="send_started",
        terminal_state="accepted",
        status="accepted",
        result_json='{"status":"accepted"}',
        now=NOW,
    )
    assert accepted["status"] == "accepted"
    with sqlite3.connect(db_path) as conn:
        assert conn.execute(
            "SELECT state FROM turn_submissions WHERE request_id='request-a'"
        ).fetchone() == ("submitted",)
    apply_turn_refresh(
        db_path,
        "host-a",
        worker.id,
        {"source_turn_id": "turn-after-ack", "user_text": "do it"},
        expected_binding=_acp_binding(worker),
        observed_at="2026-08-05T00:00:01Z",
    )
    assert settle_submission_link_for_request(
        db_path, host_id="host-a", request_id="request-a", now="2026-08-05T00:00:02Z"
    ) == {"state": "linked", "linked_turn_id": "turn-after-ack", "changed": True}


def test_retention_expires_unobserved_send_then_deletes_child_before_receipt(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "retention.db"
    worker = _seed(db_path)
    reserved = _reserve(db_path)
    mark_command_send_started(
        db_path,
        host_id="host-a",
        request_id="request-a",
        canonical_fingerprint="fingerprint-a",
        owner_token=reserved["owner_token"],
        binding_fingerprint="private-acp",
        submission_worker=worker,
        instruction_text="do it",
        submission_link_window_seconds=5,
        submission_hard_ttl_seconds=10,
        now=NOW,
    )
    first = run_retention_cycle(
        db_path,
        policy=RetentionPolicy(command_retention_days=1),
        now="2026-08-05T00:00:11Z",
    )
    assert first["submissions_settled"] == 1
    receipt = get_command_request(db_path, "host-a", "request-a")
    assert receipt is not None and receipt["state"] == "uncertain"
    assert json.loads(receipt["result_json"])["status"] == "request_state_uncertain"
    with sqlite3.connect(db_path) as conn:
        assert conn.execute(
            "SELECT state FROM turn_submissions WHERE request_id='request-a'"
        ).fetchone() == ("expired",)

    second = run_retention_cycle(
        db_path,
        policy=RetentionPolicy(command_retention_days=1),
        now="2026-08-07T00:00:12Z",
    )
    assert second["submissions"] == 1
    assert second["receipts"] == 1
    with sqlite3.connect(db_path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM turn_submissions").fetchone() == (0,)
        assert conn.execute("SELECT COUNT(*) FROM command_receipts").fetchone() == (0,)


def test_command_age_retention_removes_all_old_terminal_receipts_and_submissions(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "command-count-floor.db"
    init_store(db_path)
    with sqlite3.connect(db_path) as conn:
        for index in range(4):
            timestamp = f"2026-01-01T00:00:0{index}.000000Z"
            request_id = f"request-{index}"
            conn.execute(
                """INSERT INTO command_receipts(
                host_id,request_id,request_fingerprint,action,canonical_version,
                public_worker_id,state,status,selector_proof,request_json,result_json,
                created_at,updated_at)
                VALUES('host-a',?,?,'send_instruction',1,'worker-a',
                'accepted','accepted','','{}','{}',?,?)""",
                (request_id, f"fingerprint-{index}", timestamp, timestamp),
            )
            conn.execute(
                """INSERT INTO turn_submissions(
                host_id,submission_id,request_id,worker_id,route_generation,
                instruction_fingerprint,state,link_expires_at,hard_expires_at,
                created_at,updated_at)
                VALUES('host-a',?,?,'worker-a',?,?,'accepted',?,?,?,?)""",
                (
                    f"submission-{index}",
                    request_id,
                    f"twroute1.{index:043d}",
                    f"instruction-{index}",
                    timestamp,
                    timestamp,
                    timestamp,
                    timestamp,
                ),
            )

    result = run_retention_cycle(
        db_path,
        policy=RetentionPolicy(command_retention_days=1),
        now=NOW,
    )

    assert result["submissions"] == 4
    assert result["receipts"] == 4
    with sqlite3.connect(db_path) as conn:
        assert conn.execute(
            "SELECT request_id FROM command_receipts ORDER BY updated_at"
        ).fetchall() == []
        assert conn.execute(
            "SELECT submission_id FROM turn_submissions ORDER BY updated_at"
        ).fetchall() == []


@pytest.mark.parametrize(
    ("expected", "terminal", "status"),
    [
        ("reserved", "accepted", "accepted"),
        ("send_started", "reserved", "pending"),
        ("reserved", "uncertain", "wrong-status"),
    ],
)
def test_illegal_receipt_transitions_fail_closed(
    tmp_path: Path, expected: str, terminal: str, status: str
) -> None:
    db_path = tmp_path / "submissions.db"
    _seed(db_path)
    reserved = _reserve(db_path)
    with pytest.raises(ValueError):
        finish_command_request(
            db_path,
            host_id="host-a",
            request_id="request-a",
            canonical_fingerprint="fingerprint-a",
            owner_token=reserved["owner_token"],
            expected_state=expected,
            terminal_state=terminal,
            status=status,
            result_json="{}",
            now=NOW,
        )


def test_submission_links_only_one_matching_turn(tmp_path: Path) -> None:
    db_path = tmp_path / "submissions.db"
    worker = _seed(db_path)
    reserved = _reserve(db_path)
    started = mark_command_send_started(
        db_path,
        host_id="host-a",
        request_id="request-a",
        canonical_fingerprint="fingerprint-a",
        owner_token=reserved["owner_token"],
        binding_fingerprint="private-acp",
        submission_worker=worker,
        instruction_text="do it",
        now=NOW,
    )
    assert started["status"] == "send_started"
    apply_turn_refresh(
        db_path,
        "host-a",
        worker.id,
        {"source_turn_id": "turn-unrelated", "user_text": "different instruction"},
        expected_binding=_acp_binding(worker),
        observed_at="2026-08-05T00:00:01Z",
    )
    apply_turn_refresh(
        db_path,
        "host-a",
        worker.id,
        {
            "source_turn_id": "turn-a",
            "user_text": "do it",
            "assistant_final_text": "done",
            "complete": True,
        },
        expected_binding=_acp_binding(worker),
        observed_at=NOW,
    )
    linked = settle_submission_link_for_request(
        db_path, host_id="host-a", request_id="request-a", now="2026-08-05T00:00:03Z"
    )
    assert linked == {"state": "linked", "linked_turn_id": "turn-a", "changed": True}
    assert linked_turn_for_submission(
        db_path, host_id="host-a", request_id="request-a"
    )["id"] == "turn-a"


def test_submission_does_not_link_same_worker_on_another_route(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "submissions.db"
    worker = _seed(db_path)
    reserved = _reserve(db_path)
    started = mark_command_send_started(
        db_path,
        host_id="host-a",
        request_id="request-a",
        canonical_fingerprint="fingerprint-a",
        owner_token=reserved["owner_token"],
        binding_fingerprint="private-acp",
        submission_worker=worker,
        instruction_text="do it",
        now=NOW,
    )
    assert started["status"] == "send_started"
    herdr = WorkerBinding(
        host_id="host-a",
        worker_id=worker.id,
        worker_fingerprint=worker.fingerprint,
        backend="herdr",
        target_kind="agent_id",
        target_value="private",
        observed_at=NOW,
        private_fingerprint="private-a",
    )
    apply_turn_refresh(
        db_path,
        "host-a",
        worker.id,
        {"source_turn_id": "turn-other-route", "user_text": "do it"},
        expected_binding=herdr,
        observed_at="2026-08-05T00:00:01Z",
    )
    settled = settle_submission_link_for_request(
        db_path, host_id="host-a", request_id="request-a", now="2026-08-05T00:00:02Z"
    )
    assert settled == {
        "state": "send_started",
        "linked_turn_id": None,
        "changed": False,
    }


def test_submission_link_scan_fails_closed_at_ambiguity_bound(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "submissions.db"
    worker = _seed(db_path)
    reserved = _reserve(db_path)
    started = mark_command_send_started(
        db_path,
        host_id="host-a",
        request_id="request-a",
        canonical_fingerprint="fingerprint-a",
        owner_token=reserved["owner_token"],
        binding_fingerprint="private-acp",
        submission_worker=worker,
        instruction_text="do it",
        now=NOW,
    )
    assert started["status"] == "send_started"
    for index, text in enumerate(("do it", "other one", "other two"), 1):
        apply_turn_refresh(
            db_path,
            "host-a",
            worker.id,
            {"source_turn_id": f"turn-{index}", "user_text": text},
            expected_binding=_acp_binding(worker),
            observed_at=f"2026-08-05T00:00:0{index}Z",
        )
    settled = settle_submission_link_for_request(
        db_path, host_id="host-a", request_id="request-a", now="2026-08-05T00:00:04Z"
    )
    assert settled == {"state": "ambiguous", "linked_turn_id": None, "changed": True}


def test_linked_and_ambiguous_submission_states_are_terminal(
    tmp_path: Path,
) -> None:
    linked_db = tmp_path / "linked.db"
    worker = _seed(linked_db)
    reserved = _reserve(linked_db)
    mark_command_send_started(
        linked_db,
        host_id="host-a",
        request_id="request-a",
        canonical_fingerprint="fingerprint-a",
        owner_token=reserved["owner_token"],
        binding_fingerprint="private-acp",
        submission_worker=worker,
        instruction_text="do it",
        now=NOW,
    )
    apply_turn_refresh(
        linked_db,
        "host-a",
        worker.id,
        {"source_turn_id": "linked-turn", "user_text": "do it"},
        expected_binding=_acp_binding(worker),
        observed_at="2026-08-05T00:00:01Z",
    )
    assert settle_submission_link_for_request(
        linked_db, host_id="host-a", request_id="request-a", now="2026-08-05T00:00:02Z"
    )["state"] == "linked"
    apply_turn_refresh(
        linked_db,
        "host-a",
        worker.id,
        {"source_turn_id": "linked-turn", "removed": True},
        expected_binding=_acp_binding(worker),
        observed_at="2026-08-05T00:00:03Z",
    )
    assert settle_submission_link_for_request(
        linked_db, host_id="host-a", request_id="request-a", now="2026-08-05T00:00:04Z"
    ) == {"state": "linked", "linked_turn_id": "linked-turn", "changed": False}

    ambiguous_db = tmp_path / "ambiguous.db"
    worker = _seed(ambiguous_db)
    reserved = _reserve(ambiguous_db)
    mark_command_send_started(
        ambiguous_db,
        host_id="host-a",
        request_id="request-a",
        canonical_fingerprint="fingerprint-a",
        owner_token=reserved["owner_token"],
        binding_fingerprint="private-acp",
        submission_worker=worker,
        instruction_text="do it",
        now=NOW,
    )
    for index in (1, 2):
        apply_turn_refresh(
            ambiguous_db,
            "host-a",
            worker.id,
            {"source_turn_id": f"match-{index}", "user_text": "do it"},
            expected_binding=_acp_binding(worker),
            observed_at=f"2026-08-05T00:00:0{index}Z",
        )
    assert settle_submission_link_for_request(
        ambiguous_db, host_id="host-a", request_id="request-a", now="2026-08-05T00:00:03Z"
    )["state"] == "ambiguous"
    apply_turn_refresh(
        ambiguous_db,
        "host-a",
        worker.id,
        {"source_turn_id": "match-2", "removed": True},
        expected_binding=_acp_binding(worker),
        observed_at="2026-08-05T00:00:04Z",
    )
    assert settle_submission_link_for_request(
        ambiguous_db, host_id="host-a", request_id="request-a", now="2026-08-05T00:00:05Z"
    ) == {"state": "ambiguous", "linked_turn_id": None, "changed": False}


def test_submission_link_window_and_hard_deadline_are_enforced(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "windows.db"
    worker = _seed(db_path)
    reserved = _reserve(db_path)
    mark_command_send_started(
        db_path,
        host_id="host-a",
        request_id="request-a",
        canonical_fingerprint="fingerprint-a",
        owner_token=reserved["owner_token"],
        binding_fingerprint="private-acp",
        submission_worker=worker,
        instruction_text="do it",
        submission_link_window_seconds=10,
        submission_hard_ttl_seconds=20,
        now=NOW,
    )
    apply_turn_refresh(
        db_path,
        "host-a",
        worker.id,
        {"source_turn_id": "late-turn", "user_text": "do it"},
        expected_binding=_acp_binding(worker),
        observed_at="2026-08-05T00:00:11Z",
    )
    assert settle_submission_link_for_request(
        db_path, host_id="host-a", request_id="request-a", now="2026-08-05T00:00:12Z"
    ) == {"state": "send_started", "linked_turn_id": None, "changed": False}
    assert settle_submission_link_for_request(
        db_path, host_id="host-a", request_id="request-a", now="2026-08-05T00:00:20Z"
    ) == {"state": "expired", "linked_turn_id": None, "changed": True}
    assert settle_submission_link_for_request(
        db_path, host_id="host-a", request_id="request-a", now="2026-08-05T00:00:21Z"
    ) == {"state": "expired", "linked_turn_id": None, "changed": False}


@pytest.mark.parametrize(
    ("link_window", "hard_ttl"),
    [(0, 20), (True, 20), (10, 0), (10, True), (20, 10)],
)
def test_invalid_submission_windows_are_rejected(
    tmp_path: Path, link_window: object, hard_ttl: object
) -> None:
    db_path = tmp_path / "invalid-windows.db"
    worker = _seed(db_path)
    reserved = _reserve(db_path)
    with pytest.raises(ValueError, match="submission"):
        mark_command_send_started(
            db_path,
            host_id="host-a",
            request_id="request-a",
            canonical_fingerprint="fingerprint-a",
            owner_token=reserved["owner_token"],
            binding_fingerprint="private-acp",
            submission_worker=worker,
            instruction_text="do it",
            submission_link_window_seconds=link_window,  # type: ignore[arg-type]
            submission_hard_ttl_seconds=hard_ttl,  # type: ignore[arg-type]
            now=NOW,
        )
