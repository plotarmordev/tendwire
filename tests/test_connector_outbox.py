"""One-queue delivery, lease CAS, plan, and payload-boundary tests."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import replace
from pathlib import Path
from typing import Callable

import pytest

from tendwire.connectors import ConnectorOutboxAPI
from tendwire.core.models import Snapshot, Worker, WorkerBinding
from tendwire.core.turns import PendingObservation
from tendwire.store.db import write_transaction
from tendwire.store.outbox import (
    ack_connector_delivery,
    defer_connector_delivery,
    fail_connector_delivery,
    inspect_connector_outbox,
    poll_connector_outbox,
    prepare_connector_plan_begin,
    prepare_connector_plan_commit,
    prepare_connector_plan_part,
    prepare_connector_plan_recover,
    reclaim_expired_connector_leases,
    release_connector_delivery,
    renew_connector_delivery,
    retry_connector_dead_letter,
)
from tendwire.store.pending import apply_backend_pending_observation
from tendwire.store.projection import save_snapshot, upsert_worker_bindings
from tendwire.store.schema import init_store
from .store_helpers import append_test_turn


MAX_RESPONSE_BYTES = 850_000


def _generic(db_path: Path, *, key: str = "notice-a", payload: dict[str, object] | None = None) -> None:
    init_store(db_path)
    encoded = json.dumps(
        payload or {"schema_version": 1, "event_type": "notice", "body": "exact"},
        sort_keys=True,
        separators=(",", ":"),
    )
    with write_transaction(db_path) as conn:
        conn.execute(
            """INSERT INTO connector_outbox(
            host_id,connector,key,kind,payload_version,status,retry_generation,
            prior_attempt_count,payload_json,created_at,updated_at,available_at)
            VALUES('host-a','notice',?,'generic',1,'queued',1,0,?,
            '2026-08-05T00:00:00.000000Z','2026-08-05T00:00:00.000000Z',
            '2026-08-05T00:00:00.000000Z')""",
            (key, encoded),
        )


def _final(
    db_path: Path, *, text: str = "a", turn_id: str = "turn-a"
) -> dict[str, object]:
    init_store(db_path)
    worker = Worker(
        id="worker-a",
        name="codex",
        meta={"stable_key": "wsk1_" + "a" * 64, "stable_key_version": 1},
    )
    binding = WorkerBinding(
        host_id="host-a",
        worker_id=worker.id,
        worker_fingerprint=worker.fingerprint,
        backend="herdr",
        target_kind="agent_id",
        target_value="private",
        private_fingerprint="private-a",
    )
    save_snapshot(
        db_path,
        Snapshot(host_id="host-a", updated_at="2026-08-05T00:00:00Z", workers=[worker]),
        worker_bindings=[binding],
        binding_backend="herdr",
    )
    append_test_turn(
        db_path,
        "host-a",
        worker.id,
        {
            "source_turn_id": turn_id,
            "user_text": text,
            "assistant_final_text": text,
            "complete": True,
        },
        observed_at="2026-08-05T00:00:00Z",
    )
    leased = poll_connector_outbox(db_path, "host-a", "turn-final")["items"][0]
    assert leased["payload"]["kind"] == "final_ready"
    return leased


def test_acp_turn_id_can_begin_final_presentation_plan(tmp_path: Path) -> None:
    db_path = tmp_path / "acp-final.db"
    source = _final(db_path, turn_id="acpt_" + "a" * 24)
    turn = source["payload"]["turn"]

    result = ConnectorOutboxAPI(db_path, "host-a").prepare(
        {
            "schema_version": 1,
            "action": "begin",
            "name": "turn-final",
            "turn_id": turn["turn_id"],
            "content_revision": turn["content_revision"],
            "presentation_version": "turn-present-v3",
            "part_count": 1,
            "source_ref": source["ref"],
        }
    )

    assert result["ok"] is True
    assert result["status"] == "ok"


@pytest.mark.parametrize(
    "turn_id",
    [
        "acpt_",
        "acpt_" + "a" * 23,
        "acpt_" + "a" * 25,
        "acpt_" + "A" * 24,
        "acpt_" + "a" * 23 + ".",
        "acpt_" + "a" * 23 + "-",
    ],
)
def test_noncanonical_acp_turn_id_cannot_begin_plan(
    tmp_path: Path, turn_id: str
) -> None:
    db_path = tmp_path / "bad-acp-final.db"
    source = _final(db_path)
    turn = source["payload"]["turn"]

    result = ConnectorOutboxAPI(db_path, "host-a").prepare(
        {
            "schema_version": 1,
            "action": "begin",
            "name": "turn-final",
            "turn_id": turn_id,
            "content_revision": turn["content_revision"],
            "presentation_version": "turn-present-v3",
            "part_count": 1,
            "source_ref": source["ref"],
        }
    )

    assert result["ok"] is False
    assert result["status"] == "invalid_params"


def test_pending_decision_producer_emits_leaseable_canonical_identity(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "decision.db"
    worker = Worker(
        id="worker-a",
        name="codex",
        meta={"stable_key": "wsk1_" + "a" * 64, "stable_key_version": 1},
    )
    continuity = WorkerBinding(
        host_id="host-a",
        worker_id=worker.id,
        worker_fingerprint=worker.fingerprint,
        backend="herdr",
        target_kind="terminal_id",
        target_value="terminal-private",
        sendable=True,
        private_fingerprint="private-herdr",
    )
    save_snapshot(
        db_path,
        Snapshot(host_id="host-a", updated_at="2026-08-05T00:00:00Z", workers=[worker]),
        worker_bindings=[continuity],
        binding_backend="herdr",
    )
    binding = replace(
        continuity,
        backend="acp",
        turn_target_kind="acp_session_id",
        turn_target_value="session-private",
        private_fingerprint="private-a",
    )
    upsert_worker_bindings(db_path, [binding])
    assert apply_backend_pending_observation(
        db_path,
        "host-a",
        worker.id,
        PendingObservation(
            kind="open_prompt",
            question="Allow the tool?",
            pending_kind="approval",
            revision_digest="backend-revision-a",
            decision_kind="single",
            decision_options=("Allow", "Reject"),
            decision_question_count=1,
        ),
        observed_at="2026-08-05T00:00:01Z",
        binding_private_fingerprint="private-a",
    )

    leased = poll_connector_outbox(db_path, "host-a", "turn-final")["items"]
    assert len(leased) == 1
    decision = leased[0]["payload"]["decision"]
    expected_ref = "pending-" + hashlib.sha256(json.dumps(
        ["host-a", worker.id, decision["revision_digest"]],
        ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False,
    ).encode()).hexdigest()[:24]
    assert leased[0]["payload"]["kind"] == "decision"
    assert decision["decision_ref"] == expected_ref
    assert leased[0]["key"].startswith("turn-final:decision:twdecision1.")

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        pending = conn.execute(
            "SELECT decision_ref,revision_digest FROM backend_pending"
        ).fetchone()
        outbox = conn.execute(
            "SELECT decision_ref,status FROM connector_outbox WHERE kind='decision'"
        ).fetchone()
    assert pending is not None and outbox is not None
    assert pending["decision_ref"] == decision["decision_ref"]
    assert pending["revision_digest"] == decision["revision_digest"]
    assert outbox["decision_ref"] == decision["decision_ref"]
    assert outbox["status"] == "leased"


@pytest.mark.parametrize(
    "mutation", [lambda _payload: {}, lambda payload: (payload.pop("route"), payload)[1]],
)
def test_malformed_canonical_turn_final_payload_dead_letters_without_escaping(
    tmp_path: Path, mutation: Callable[[dict[str, object]], dict[str, object]],
) -> None:
    db_path = tmp_path / "malformed.db"
    leased = _final(db_path)
    assert release_connector_delivery(
        db_path, host_id="host-a", name="turn-final", ref=leased["ref"]
    )["status"] == "released"
    payload = json.loads(json.dumps(leased["payload"]))
    malformed = mutation(payload)
    with write_transaction(db_path) as conn:
        conn.execute(
            "UPDATE connector_outbox SET payload_json=? WHERE key=?",
            (json.dumps(malformed, sort_keys=True, separators=(",", ":")), leased["key"]),
        )
    assert poll_connector_outbox(db_path, "host-a", "turn-final")["items"] == []


def test_generic_poll_fail_retry_and_ack_preserve_outer_key(tmp_path: Path) -> None:
    db_path = tmp_path / "outbox.db"
    _generic(db_path)
    first = poll_connector_outbox(db_path, "host-a", "notice")["items"][0]
    failed = fail_connector_delivery(
        db_path,
        host_id="host-a",
        name="notice",
        ref=first["ref"],
        reason="temporary",
        delay_seconds=0,
    )
    assert failed["status"] == "retry_scheduled"
    second = poll_connector_outbox(db_path, "host-a", "notice")["items"][0]
    assert second["key"] == first["key"]
    assert second["attempt"] == 2
    assert ack_connector_delivery(
        db_path, host_id="host-a", name="notice", ref=second["ref"]
    )["status"] == "acknowledged"
    assert poll_connector_outbox(db_path, "host-a", "notice")["items"] == []


def test_nonfinite_canonical_store_payload_dead_letters_without_escaping(tmp_path: Path) -> None:
    db_path = tmp_path / "nonfinite.db"
    _generic(db_path, payload={"value": float("nan")})
    assert poll_connector_outbox(db_path, "host-a", "notice")["items"] == []


def test_old_ref_cannot_settle_new_attempt(tmp_path: Path) -> None:
    db_path = tmp_path / "outbox.db"
    _generic(db_path)
    first = poll_connector_outbox(db_path, "host-a", "notice")["items"][0]
    assert release_connector_delivery(
        db_path, host_id="host-a", name="notice", ref=first["ref"]
    )["status"] == "released"
    second = poll_connector_outbox(db_path, "host-a", "notice")["items"][0]
    stale = ack_connector_delivery(
        db_path, host_id="host-a", name="notice", ref=first["ref"]
    )
    assert stale["status"] in {"invalid_ref", "stale_ref"}
    assert ack_connector_delivery(
        db_path, host_id="host-a", name="notice", ref=second["ref"]
    )["status"] == "acknowledged"


def test_renew_and_defer_are_attempt_fenced(tmp_path: Path) -> None:
    db_path = tmp_path / "outbox.db"
    _generic(db_path)
    leased = poll_connector_outbox(db_path, "host-a", "notice")["items"][0]
    renewed = renew_connector_delivery(
        db_path,
        host_id="host-a",
        name="notice",
        ref=leased["ref"],
        lease_seconds=120,
    )
    assert renewed["status"] == "renewed"
    deferred = defer_connector_delivery(
        db_path,
        host_id="host-a",
        name="notice",
        ref=leased["ref"],
        reason="rate_limited",
        delay_seconds=1,
    )
    assert deferred["status"] == "deferred"
    assert renew_connector_delivery(
        db_path,
        host_id="host-a",
        name="notice",
        ref=leased["ref"],
        lease_seconds=120,
    )["status"] in {"invalid_ref", "stale_ref"}


def test_provider_uncertain_dead_letters_without_retrying(tmp_path: Path) -> None:
    db_path = tmp_path / "outbox.db"
    init_store(db_path)
    worker = Worker(id="worker-a", name="codex", meta={"stable_key": "wsk1_" + "a" * 64, "stable_key_version": 1})
    binding = WorkerBinding(host_id="host-a", worker_id=worker.id, worker_fingerprint=worker.fingerprint, backend="herdr", target_kind="agent_id", target_value="private", private_fingerprint="private-a")
    save_snapshot(db_path, Snapshot(host_id="host-a", updated_at="2026-08-05T00:00:00Z", workers=[worker]), worker_bindings=[binding], binding_backend="herdr")
    append_test_turn(db_path, "host-a", worker.id, {"source_turn_id": "turn-a", "assistant_stream_text": "work", "complete": False}, observed_at="2026-08-05T00:00:00Z")
    leased = poll_connector_outbox(db_path, "host-a", "turn-final")["items"][0]
    result = fail_connector_delivery(
        db_path,
        host_id="host-a",
        name="turn-final",
        ref=leased["ref"],
        reason="provider_uncertain",
    )
    assert result["status"] in {"dead_lettered", "attempts_exhausted"}
    inspected = inspect_connector_outbox(
        db_path, "host-a", name="turn-final", status="dead_letter", limit=10
    )
    assert inspected["total"] == 1
    assert inspected["items"][0]["reason"] == "provider_uncertain"


def test_plan_begin_part_commit_are_idempotent_after_response_loss(tmp_path: Path) -> None:
    db_path = tmp_path / "outbox.db"
    source = _final(db_path)
    turn = source["payload"]["turn"]
    begin_args = dict(
        name="turn-final",
        turn_id=turn["turn_id"],
        content_revision=turn["content_revision"],
        presentation_version="v1",
        part_count=1,
        source_ref=source["ref"],
    )
    first = prepare_connector_plan_begin(db_path, "host-a", **begin_args)
    replay = prepare_connector_plan_begin(db_path, "host-a", **begin_args)
    assert replay == first
    spans = [
        {"field": "user_text", "start_char": 0, "end_char": 1},
        {"field": "assistant_final_text", "start_char": 0, "end_char": 1},
    ]
    part = prepare_connector_plan_part(
        db_path,
        "host-a",
        name="turn-final",
        plan_token=first["plan_token"],
        ordinal=0,
        spans=spans,
    )
    assert prepare_connector_plan_part(
        db_path,
        "host-a",
        name="turn-final",
        plan_token=first["plan_token"],
        ordinal=0,
        spans=spans,
    ) == part
    committed = prepare_connector_plan_commit(
        db_path,
        "host-a",
        name="turn-final",
        plan_token=first["plan_token"],
        source_ref=source["ref"],
    )
    replay_commit = prepare_connector_plan_commit(
        db_path,
        "host-a",
        name="turn-final",
        plan_token=first["plan_token"],
        source_ref=source["ref"],
    )
    assert replay_commit == committed
    child = poll_connector_outbox(db_path, "host-a", "turn-final")["items"][0]
    assert child["payload"]["kind"] == "final_part"
    assert child["payload"]["plan"]["spans"] == spans


def test_failed_plan_suffix_recovery_is_request_idempotent_and_completes_root(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "outbox.db"
    source = _final(db_path, text="ab")
    turn = source["payload"]["turn"]
    begun = prepare_connector_plan_begin(
        db_path, "host-a", name="turn-final", turn_id=turn["turn_id"],
        content_revision=turn["content_revision"], presentation_version="v1",
        part_count=2, source_ref=source["ref"],
    )
    for ordinal, field in enumerate(("user_text", "assistant_final_text")):
        assert prepare_connector_plan_part(
            db_path, "host-a", name="turn-final", plan_token=begun["plan_token"],
            ordinal=ordinal, spans=[{"field": field, "start_char": 0, "end_char": 2}],
        )["status"] == "ok"
    assert prepare_connector_plan_commit(
        db_path, "host-a", name="turn-final", plan_token=begun["plan_token"],
        source_ref=source["ref"],
    )["state"] == "active"

    first = poll_connector_outbox(db_path, "host-a", "turn-final")["items"][0]
    assert ack_connector_delivery(
        db_path, host_id="host-a", name="turn-final", ref=first["ref"]
    )["status"] == "acknowledged"
    failed = poll_connector_outbox(db_path, "host-a", "turn-final")["items"][0]
    assert fail_connector_delivery(
        db_path, host_id="host-a", name="turn-final", ref=failed["ref"],
        reason="provider_rejected", max_attempts=1,
    )["status"] == "attempts_exhausted"
    with write_transaction(db_path) as conn:
        conn.execute("UPDATE connector_outbox SET status='delivered' WHERE key=?", (failed["key"],))
    assert prepare_connector_plan_recover(
        db_path, "host-a", name="turn-final", failed_plan_token=begun["plan_token"],
        request_id="corrupt-delivered-suffix",
    )["status"] == "not_recoverable"
    with write_transaction(db_path) as conn:
        conn.execute("UPDATE connector_outbox SET status='dead_letter' WHERE key=?", (failed["key"],))

    recovered = prepare_connector_plan_recover(
        db_path, "host-a", name="turn-final", failed_plan_token=begun["plan_token"],
        request_id="recovery-request-a",
    )
    replay = prepare_connector_plan_recover(
        db_path, "host-a", name="turn-final", failed_plan_token=begun["plan_token"],
        request_id="recovery-request-a",
    )
    assert recovered["status"] == replay["status"] == "recovered"
    assert recovered["plan_token"] == replay["plan_token"]
    assert recovered["generation"] == replay["generation"] == 2
    assert recovered["idempotent_replay"] is False
    assert replay["idempotent_replay"] is True

    suffix = poll_connector_outbox(db_path, "host-a", "turn-final")["items"][0]
    assert suffix["payload"]["plan"]["ordinal"] == 1
    assert ack_connector_delivery(
        db_path, host_id="host-a", name="turn-final", ref=suffix["ref"]
    )["status"] == "acknowledged"
    with write_transaction(db_path) as conn:
        assert conn.execute(
            "SELECT status FROM connector_outbox WHERE id=(SELECT source_outbox_id FROM connector_outbox WHERE plan_token=? LIMIT 1)",
            (recovered["plan_token"],),
        ).fetchone()[0] == "delivered"
        conn.execute(
            "UPDATE connector_outbox SET status='dead_letter' WHERE key=?",
            (source["key"],),
        )
    inspected = inspect_connector_outbox(
        db_path, "host-a", name="turn-final", status="dead_letter", limit=10
    )
    root_item = next(item for item in inspected["items"] if item["key"] == source["key"])
    assert root_item["retryable"] is False
    assert retry_connector_dead_letter(db_path, "host-a", key=source["key"])[
        "status"
    ] == "not_retryable"


def test_final_ready_dead_letter_retry_starts_new_generation(tmp_path: Path) -> None:
    db_path = tmp_path / "outbox.db"
    leased = _final(db_path)
    assert fail_connector_delivery(
        db_path,
        host_id="host-a",
        name="turn-final",
        ref=leased["ref"],
        reason="provider_rejected",
        max_attempts=1,
    )["status"] == "attempts_exhausted"
    retried = retry_connector_dead_letter(db_path, "host-a", key=leased["key"])
    assert retried["status"] == "requeued"
    assert retried["retry_generation"] == 2
    assert retried["prior_attempt_count"] == 1


def test_database_constraints_reject_illegal_kind_status_pair(tmp_path: Path) -> None:
    db_path = tmp_path / "outbox.db"
    init_store(db_path)
    with pytest.raises(sqlite3.IntegrityError), sqlite3.connect(db_path) as conn:
        conn.execute(
            """INSERT INTO connector_outbox(
            host_id,connector,key,kind,payload_version,status,partition_key,
            partition_sequence,retry_generation,prior_attempt_count,payload_json,
            created_at,updated_at,available_at)
            VALUES('host-a','turn-final','bad','working',1,'staged','twpart1_bad',1,1,0,
            '{}','2026-08-05T00:00:00Z','2026-08-05T00:00:00Z','2026-08-05T00:00:00Z')"""
        )


def test_oversized_first_polled_item_is_rejected_or_bounded(tmp_path: Path) -> None:
    """The response budget must cover the first item as well as later items."""
    db_path = tmp_path / "outbox.db"
    _generic(
        db_path,
        payload={"schema_version": 1, "body": "x" * (MAX_RESPONSE_BYTES + 1024)},
    )
    result = poll_connector_outbox(db_path, "host-a", "notice")
    encoded = json.dumps(result, separators=(",", ":")).encode()
    assert result.get("ok") is False or len(encoded) <= MAX_RESPONSE_BYTES


def test_reclaim_expired_lease_keeps_outer_key(tmp_path: Path) -> None:
    db_path = tmp_path / "outbox.db"
    _generic(db_path)
    leased = poll_connector_outbox(
        db_path,
        "host-a",
        "notice",
        lease_seconds=1,
        now="2026-08-05T00:00:00Z",
    )["items"][0]
    reclaimed = reclaim_expired_connector_leases(
        db_path,
        "host-a",
        "notice",
        now="2026-08-05T00:00:02Z",
    )
    assert reclaimed["reclaimed"] == 1
    replay = poll_connector_outbox(
        db_path, "host-a", "notice", now="2026-08-05T00:00:02Z"
    )["items"][0]
    assert replay["key"] == leased["key"]
