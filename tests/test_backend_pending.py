"""Backend-provided pending prompts: a REAL pane prompt (question + choices, captured by the herdres
pending hook through the turn adapter) flows adapter -> herdr_turns -> backend_pending -> pending.list,
superseding the worker's synthetic attention-derived row."""
from __future__ import annotations

import json
import sqlite3
import pytest
from pathlib import Path

from tendwire.backends.herdr_turns import _backend_pending_from_turn, _pop_backend_pending
from tendwire.backends.herdr_turns import _pending_observation_from_turn
from tendwire.config import Config, DEFAULT_PENDING_STALE_GRACE_SECONDS, load_config
from tendwire.core.projector import project_from_raw
from tendwire.core.models import Snapshot, WorkerBinding
from tendwire.core.turns import PendingObservation, pending_payload_from_snapshot
from tendwire.daemon import TendwireDaemon
from tendwire.store.sqlite import (
    STORE_SCHEMA_VERSION,
    abandon_backend_pending_choice_claim,
    apply_backend_pending_observation,
    backend_pending_health,
    claim_backend_pending_choice,
    finish_backend_pending_choice_send,
    init_store,
    list_backend_pending,
    merge_backend_pending,
    prune_backend_pending,
    pending_payload_from_store,
    save_snapshot,
    start_backend_pending_choice_send,
    upsert_worker_bindings,
)


def _decision_turn() -> dict:
    return {
        "available": True,
        "complete": False,
        "awaiting_input": True,
        "user_text": "which db?",
        "pending_decision": {
            "decision_id": "toolu_123",
            "prompt": "Which database should we use?",
            "mode": "buttons",
            "options": [
                {"id": "1", "label": "Postgres", "send_text": "Postgres"},
                {"id": "2", "label": "SQLite", "send_text": "SQLite"},
                {"id": "custom", "label": "Tell me differently", "send_text": ""},
            ],
        },
    }


def test_extract_pending_decision():
    pending = _backend_pending_from_turn(_decision_turn())
    assert pending["question"] == "Which database should we use?"
    assert pending["kind"] == "question"
    assert [c["label"] for c in pending["choices"]] == ["Postgres", "SQLite", "Tell me differently"]
    choice_ids = [c["choice_id"] for c in pending["choices"]]
    assert all(choice_id.startswith("choice-") for choice_id in choice_ids)
    assert choice_ids == [c["choice_id"] for c in _backend_pending_from_turn(_decision_turn())["choices"]]
    assert not ({"1", "2", "custom"} & set(choice_ids))
    # The machine-send payload (send_text) is not published; choices carry only id + label.
    assert all("value" not in c for c in pending["choices"])
    # decision_id (internal tool_use_id) is not published in public pending.
    assert "decision_id" not in pending["meta"]
    decision = pending["meta"]["decision"]
    assert decision == {
        "decision_ref": decision["decision_ref"],
        "kind": "single",
        "prompt": "Which database should we use?",
        "options": [
            {"ref": "1", "label": "Postgres"},
            {"ref": "2", "label": "SQLite"},
        ],
        "multi_select": False,
        "question_count": 1,
    }
    assert decision["decision_ref"].startswith("decision-")


def test_extract_plan_approval_kind():
    turn = {"pending_decision": {"decision_id": "t", "prompt": "Approve this plan?",
                                 "options": [{"id": "approve", "label": "Approve", "send_text": "1"},
                                             {"id": "revise", "label": "Revise", "send_text": ""}]}}
    assert _backend_pending_from_turn(turn)["kind"] == "approval"


def test_extract_none_without_pending():
    assert _backend_pending_from_turn({"complete": True, "assistant_final_text": "done"}) is None


def test_pop_backend_pending_splits():
    content, pending = _pop_backend_pending({"user_text": "hi", "_backend_pending": {"question": "q"}})
    assert content == {"user_text": "hi"} and pending == {"question": "q"}
    content, pending = _pop_backend_pending({"_backend_pending": {"question": "q"}})
    assert content is None and pending == {"question": "q"}


def test_merge_and_prune_backend_pending(tmp_path: Path):
    db = tmp_path / "p.db"
    init_store(db)
    pending = {"question": "Q?", "kind": "question", "choices": [], "meta": {"source": "backend"}}
    assert merge_backend_pending(db, "h", "w1", pending) is True
    assert merge_backend_pending(db, "h", "w1", pending) is False       # unchanged -> no write
    assert list_backend_pending(db, "h") == {"w1": pending}
    assert merge_backend_pending(db, "h", "w1", None) is True           # answered -> pruned
    assert list_backend_pending(db, "h") == {}


# --- Regression tests for the PR#3 review fixes ---------------------------------------------

_GOOGLE_API_KEY_SENTINEL = "AI" + "zaSyD-ExampleKey1234567890abcdefghijk"
_OPENAI_KEY_SENTINEL = "sk-" + "live-SENTINELSECRET123ABC"

_SENTINELS = [
    _OPENAI_KEY_SENTINEL,                        # sk- secret token
    "/run/user/1000/herdr/sock-abcdef123456",   # socket path
    "w4V:p1",                                    # pseudo pane id
    "/home/example/.ssh/id_rsa",                # absolute fs path
    "toolu_SENTINELDECISION01",                 # internal tool_use_id (dropped, not published)
    _GOOGLE_API_KEY_SENTINEL,                   # google api key
    "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjMifQ.SflKxwRJSMeKKF2QT4fwpMeJf",  # JWT
    "alice.jones@internal-corp.example",        # email / PII
    "10.4.2.9:5432",                            # internal ip:port
    "home/alice/.ssh/id_rsa",                  # home path without leading slash
]


def _leaky_decision_turn() -> dict:
    return {
        "pending_decision": {
            "decision_id": "toolu_SENTINELDECISION01",
            "prompt": (
                "Approve running against pane w4V:p1 at /home/example/.ssh/id_rsa via "
                f"/run/user/1000/herdr/sock-abcdef123456? Also rotate {_GOOGLE_API_KEY_SENTINEL} "
                "and eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjMifQ.SflKxwRJSMeKKF2QT4fwpMeJf; "
                "notify alice.jones@internal-corp.example on db 10.4.2.9:5432 at home/alice/.ssh/id_rsa"
            ),
            "options": [
                {
                    "id": "approve",
                    "label": f"Use secret {_OPENAI_KEY_SENTINEL}",
                    "send_text": _OPENAI_KEY_SENTINEL,
                },
                {"id": "postgres", "label": "Postgres", "send_text": "Postgres"},
                {"id": "run", "label": "Deploy to 10.4.2.9:5432", "send_text": "tmux send-keys -t w4V:p1 'rm -rf /home/example/.ssh'"},
                {"id": "shell", "label": "bash -lc 'echo untrusted option'", "send_text": "echo untrusted option"},
            ],
        }
    }


def test_ingestion_redacts_private_data_from_pending():
    """Blocker regression: no private path/pane-id/secret/tool-id survives ingestion."""
    pending = _backend_pending_from_turn(_leaky_decision_turn())
    blob = json.dumps(pending)
    for sentinel in _SENTINELS:
        assert sentinel not in blob, f"private sentinel leaked into ingested pending: {sentinel!r}"
    # A benign label/value is preserved verbatim.
    labels = [c["label"] for c in pending["choices"]]
    assert "Postgres" in labels
    assert "[redacted]" in pending["question"]
    assert "bash -lc" not in blob
    assert "echo untrusted option" not in blob


def test_get_pending_public_json_has_no_private_leak(tmp_path: Path):
    """End-to-end: the sentinels must not reach the PUBLIC pending.list payload either."""
    db = tmp_path / "leak.db"
    config = Config(host_id="h", db_path=db)
    snapshot = project_from_raw(config, workers=[{"id": "worker-1", "name": "claude", "status": "blocked", "space_id": "s1"}])
    init_store(db)
    save_snapshot(db, snapshot)
    pending = _backend_pending_from_turn(_leaky_decision_turn())
    merge_backend_pending(db, "h", "worker-1", pending)

    public = json.dumps(TendwireDaemon(config).get_pending())
    for sentinel in _SENTINELS:
        assert sentinel not in public, f"private sentinel leaked into public pending.list: {sentinel!r}"
    # The benign choice survives; the raw-command choice value is dropped by _public_choice_value.
    assert "Postgres" in public


def test_get_pending_recomputes_content_fingerprint_and_shows_choices(tmp_path: Path):
    db = tmp_path / "fp.db"
    config = Config(host_id="h", db_path=db)
    snapshot = project_from_raw(config, workers=[{"id": "worker-1", "name": "claude", "status": "blocked", "space_id": "s1"}])
    init_store(db)
    save_snapshot(db, snapshot)
    baseline_fp = pending_payload_from_snapshot(snapshot)["content_fingerprint"]

    merge_backend_pending(db, "h", "worker-1", _backend_pending_from_turn(_decision_turn()))
    payload = TendwireDaemon(config).get_pending()

    # The overlaid list moves the change-token (was left stale before the fix).
    assert payload["content_fingerprint"] != baseline_fp
    interactions = [p for p in payload["pending_interactions"] if p["worker_id"] == "worker-1"]
    assert interactions and interactions[0]["question"] == "Which database should we use?"
    assert [c["label"] for c in interactions[0]["choices"]] == ["Postgres", "SQLite", "Tell me differently"]


def test_prune_backend_pending_reaps_orphaned_workers(tmp_path: Path):
    db = tmp_path / "orphan.db"
    init_store(db)
    live = PendingObservation(
        "open_prompt",
        question="Q?",
        pending_kind="question",
        revision_digest="live-revision",
    )
    gone = PendingObservation(
        "open_prompt",
        question="Q?",
        pending_kind="question",
        revision_digest="gone-revision",
    )
    apply_backend_pending_observation(
        db, "h", "worker-live", live, binding_private_fingerprint="binding-live"
    )
    apply_backend_pending_observation(
        db, "h", "worker-gone", gone, binding_private_fingerprint="binding-gone"
    )
    assert set(list_backend_pending(db, "h")) == {"worker-live", "worker-gone"}

    reaped = prune_backend_pending(db, "h", {"binding-live"})
    assert reaped == 1
    assert set(list_backend_pending(db, "h")) == {"worker-live"}


def _pending_fixture(tmp_path: Path) -> tuple[Path, Config, object]:
    db = tmp_path / "pending-v10.db"
    config = Config(host_id="h", db_path=db)
    snapshot = project_from_raw(
        config,
        workers=[
            {
                "id": "worker-1",
                "name": "pane worker",
                "status": "blocked",
                "space_id": "space-1",
            }
        ],
    )
    init_store(db)
    save_snapshot(db, snapshot)
    return db, config, snapshot


def test_pending_stale_grace_config_default_env_and_validation(monkeypatch) -> None:
    monkeypatch.delenv("TENDWIRE_PENDING_STALE_GRACE_SECONDS", raising=False)
    assert DEFAULT_PENDING_STALE_GRACE_SECONDS == 30.0
    assert load_config().pending_stale_grace_seconds == 30.0
    monkeypatch.setenv("TENDWIRE_PENDING_STALE_GRACE_SECONDS", "12.5")
    assert load_config().pending_stale_grace_seconds == 12.5
    assert load_config(pending_stale_grace_seconds="4").pending_stale_grace_seconds == 4
    for invalid in (0, -1, "nan", "inf"):
        try:
            Config(pending_stale_grace_seconds=invalid)
        except ValueError as exc:
            assert "pending_stale_grace_seconds must be a finite positive number" in str(exc)
        else:
            raise AssertionError(f"accepted invalid grace: {invalid!r}")


def test_explicit_pending_transition_table_tombstone_stale_and_expiry(
    tmp_path: Path,
) -> None:
    db, _config, _snapshot = _pending_fixture(tmp_path)
    opened = _pending_observation_from_turn(_decision_turn())
    t0 = "2026-07-13T00:00:00+00:00"
    t1 = "2026-07-13T00:00:01+00:00"
    t2 = "2026-07-13T00:00:02+00:00"
    t20 = "2026-07-13T00:00:20+00:00"
    t32 = "2026-07-13T00:00:32+00:00"

    assert apply_backend_pending_observation(db, "h", "worker-1", opened, observed_at=t0)
    fresh = pending_payload_from_store(db, "h")
    backend_row = next(
        row for row in fresh["pending_interactions"] if row["worker_id"] == "worker-1"
    )
    assert backend_row["question"] == "Which database should we use?"
    assert backend_row["meta"]["freshness"] == "fresh"
    fresh_fingerprint = fresh["content_fingerprint"]

    assert not apply_backend_pending_observation(
        db, "h", "worker-1", opened, observed_at=t1
    )
    assert pending_payload_from_store(db, "h")["content_fingerprint"] == fresh_fingerprint
    with sqlite3.connect(db) as conn:
        assert conn.execute(
            "SELECT COUNT(*) FROM backend_pending WHERE host_id = 'h'"
        ).fetchone()[0] == 1
        assert conn.execute(
            "SELECT COUNT(*) FROM connector_outbox"
        ).fetchone()[0] == 0

    assert apply_backend_pending_observation(
        db, "h", "worker-1", PendingObservation("read_failed"), observed_at=t2
    )
    stale = pending_payload_from_store(db, "h")
    stale_row = next(
        row for row in stale["pending_interactions"] if row["worker_id"] == "worker-1"
    )
    assert stale_row["meta"]["freshness"] == "stale"
    assert stale["pending_health"] == {
        "status": "degraded",
        "counts": {"fresh": 0, "stale": 1, "total": 1},
    }
    assert stale["content_fingerprint"] != fresh_fingerprint
    with sqlite3.connect(db) as conn:
        deadline = conn.execute(
            "SELECT grace_deadline FROM backend_pending WHERE worker_id = 'worker-1'"
        ).fetchone()[0]

    assert not apply_backend_pending_observation(
        db, "h", "worker-1", PendingObservation("read_failed"), observed_at=t20
    )
    with sqlite3.connect(db) as conn:
        assert conn.execute(
            "SELECT grace_deadline FROM backend_pending WHERE worker_id = 'worker-1'"
        ).fetchone()[0] == deadline

    assert apply_backend_pending_observation(
        db, "h", "worker-1", PendingObservation("read_failed"), observed_at=t32
    )
    expired = pending_payload_from_store(db, "h")
    assert expired["pending_health"] == {
        "status": "degraded",
        "counts": {"fresh": 0, "stale": 1, "total": 1},
    }
    assert expired["content_fingerprint"] != stale["content_fingerprint"]
    assert any(
        row["worker_id"] == "worker-1"
        for row in expired["pending_interactions"]
    )

    assert apply_backend_pending_observation(
        db,
        "h",
        "worker-1",
        PendingObservation("read_succeeded_no_prompt"),
        observed_at="2026-07-13T00:01:00+00:00",
    )
    tombstoned = pending_payload_from_store(db, "h")
    assert not any(
        row["worker_id"] == "worker-1"
        for row in tombstoned["pending_interactions"]
    )
    assert tombstoned["pending_health"] == {
        "status": "healthy",
        "counts": {"fresh": 0, "stale": 0, "total": 0},
    }
    assert _pending_observation_from_turn(
        {"pending_decision": {"prompt": "Broken?", "options": "not-a-list"}}
    ).kind == "read_succeeded_invalid_prompt"
    with sqlite3.connect(db) as conn:
        assert conn.execute(
            "SELECT observation_state, freshness FROM backend_pending"
        ).fetchone() == ("none", "fresh")

    assert apply_backend_pending_observation(
        db,
        "h",
        "worker-1",
        PendingObservation("worker_authoritatively_absent"),
        observed_at="2026-07-13T00:01:01+00:00",
    )
    assert backend_pending_health(db, "h")["counts"]["total"] == 0


def test_malformed_source_prompt_exposes_snapshot_fallback_and_health(
    tmp_path: Path,
) -> None:
    db, _config, _snapshot = _pending_fixture(tmp_path)
    opened = _pending_observation_from_turn(_decision_turn())
    assert apply_backend_pending_observation(
        db,
        "h",
        "worker-1",
        opened,
        observed_at="2026-07-13T00:00:00+00:00",
    )
    malformed = _pending_observation_from_turn(
        {"pending_decision": {"prompt": "Broken?", "options": "not-a-list"}}
    )
    assert malformed.kind == "read_succeeded_invalid_prompt"
    assert apply_backend_pending_observation(
        db,
        "h",
        "worker-1",
        malformed,
        observed_at="2026-07-13T00:00:01+00:00",
    )
    fallback = pending_payload_from_store(db, "h")
    assert any(
        item["worker_id"] == "worker-1"
        for item in fallback["pending_interactions"]
    )
    assert all(
        item["question"] != "Which database should we use?"
        for item in fallback["pending_interactions"]
    )
    assert fallback["pending_health"] == {
        "status": "degraded",
        "counts": {"fresh": 0, "stale": 1, "total": 1},
    }
    fingerprint = fallback["content_fingerprint"]
    assert not apply_backend_pending_observation(
        db,
        "h",
        "worker-1",
        malformed,
        observed_at="2026-07-13T00:00:02+00:00",
    )
    assert (
        pending_payload_from_store(db, "h")["content_fingerprint"]
        == fingerprint
    )
    with sqlite3.connect(db) as conn:
        assert conn.execute(
            "SELECT observation_state, freshness FROM backend_pending"
        ).fetchone() == ("invalid", "stale")


def test_initial_read_failure_remains_degraded_until_success(
    tmp_path: Path,
) -> None:
    db, _config, _snapshot = _pending_fixture(tmp_path)
    failure = PendingObservation("read_failed")
    assert apply_backend_pending_observation(
        db,
        "h",
        "worker-1",
        failure,
        observed_at="2026-07-13T00:00:00+00:00",
    )
    failed = pending_payload_from_store(db, "h")
    assert failed["pending_health"] == {
        "status": "degraded",
        "counts": {"fresh": 0, "stale": 1, "total": 1},
    }
    assert any(
        item["worker_id"] == "worker-1"
        for item in failed["pending_interactions"]
    )
    fingerprint = failed["content_fingerprint"]
    assert not apply_backend_pending_observation(
        db,
        "h",
        "worker-1",
        failure,
        observed_at="2026-07-13T00:00:01+00:00",
    )
    assert (
        pending_payload_from_store(db, "h")["content_fingerprint"]
        == fingerprint
    )
    assert apply_backend_pending_observation(
        db,
        "h",
        "worker-1",
        PendingObservation("read_succeeded_no_prompt"),
        observed_at="2026-07-13T00:00:02+00:00",
    )
    recovered = pending_payload_from_store(db, "h")
    assert recovered["pending_health"] == {
        "status": "healthy",
        "counts": {"fresh": 0, "stale": 0, "total": 0},
    }
    assert not any(
        item["worker_id"] == "worker-1"
        for item in recovered["pending_interactions"]
    )


def test_older_pending_observations_cannot_regress_newer_state(
    tmp_path: Path,
) -> None:
    db, _config, _snapshot = _pending_fixture(tmp_path)
    opened = _pending_observation_from_turn(_decision_turn())
    assert apply_backend_pending_observation(
        db,
        "h",
        "worker-1",
        opened,
        observed_at="2026-07-13T00:00:10+00:00",
    )
    assert apply_backend_pending_observation(
        db,
        "h",
        "worker-1",
        PendingObservation("read_succeeded_no_prompt"),
        observed_at="2026-07-13T00:00:20+00:00",
    )
    assert not apply_backend_pending_observation(
        db,
        "h",
        "worker-1",
        opened,
        observed_at="2026-07-13T00:00:15+00:00",
    )
    assert not any(
        item["worker_id"] == "worker-1"
        for item in pending_payload_from_store(db, "h")[
            "pending_interactions"
        ]
    )
    assert apply_backend_pending_observation(
        db,
        "h",
        "worker-1",
        opened,
        observed_at="2026-07-13T00:00:30+00:00",
    )
    assert not apply_backend_pending_observation(
        db,
        "h",
        "worker-1",
        PendingObservation("read_failed"),
        observed_at="2026-07-13T00:00:25+00:00",
    )
    current = pending_payload_from_store(db, "h")
    row = next(
        item
        for item in current["pending_interactions"]
        if item["worker_id"] == "worker-1"
    )
    assert row["question"] == "Which database should we use?"
    assert row["meta"]["freshness"] == "fresh"


def test_revision_bound_opaque_handles_and_two_phase_claim(tmp_path: Path) -> None:
    db, config, snapshot = _pending_fixture(tmp_path)
    worker = snapshot.workers[0]
    binding = WorkerBinding(
        host_id=config.host_id,
        worker_id=worker.id,
        worker_fingerprint=worker.fingerprint,
        backend="herdr",
        target_kind="agent_id",
        target_value="agent-private",
        turn_target_kind="pane_id",
        turn_target_value="pane-private",
        sendable=True,
        observed_at="2026-07-13T00:00:00+00:00",
        expires_at="9999-12-31T23:59:59+00:00",
        private_fingerprint="binding-private",
    )
    upsert_worker_bindings(db, [binding])
    first = _pending_observation_from_turn(_decision_turn())
    changed_turn = _decision_turn()
    changed_turn["pending_decision"]["decision_id"] = "toolu_changed_private"
    second = _pending_observation_from_turn(changed_turn)
    assert [choice.choice_id for choice in first.choices] != [
        choice.choice_id for choice in second.choices
    ]
    public_blob = json.dumps(_backend_pending_from_turn(changed_turn))
    assert "toolu_changed_private" not in public_blob
    assert "send_text" not in public_blob

    assert apply_backend_pending_observation(
        db,
        "h",
        worker.id,
        first,
        observed_at="2026-07-13T00:00:00+00:00",
        binding_private_fingerprint=binding.private_fingerprint,
        observed_turn_target_value=binding.turn_target_value,
    )
    projected = pending_payload_from_store(db, "h")
    row = next(item for item in projected["pending_interactions"] if item["worker_id"] == worker.id)
    choice_id = row["choices"][1]["choice_id"]
    dry = claim_backend_pending_choice(
        db, "h", row["id"], row["fingerprint"], choice_id,
        claim=False, observed_at="2026-07-13T00:00:01+00:00",
    )
    assert dry.status == "validated"
    assert dry.claim_token is None
    assert dry.picker_ordinal == 2
    claimed = claim_backend_pending_choice(
        db, "h", row["id"], row["fingerprint"], choice_id,
        observed_at="2026-07-13T00:00:01+00:00",
    )
    assert claimed.status == "claimed"
    assert claimed.binding_private_fingerprint == "binding-private"
    assert claimed.turn_target_value == "pane-private"
    assert claimed.picker_ordinal == 2
    started = start_backend_pending_choice_send(
        db, "h", claimed.claim_token,
        observed_at="2026-07-13T00:00:02+00:00",
    )
    assert started.status == "started"
    assert started.turn_target_value == "pane-private"
    assert started.picker_ordinal == 2
    assert abandon_backend_pending_choice_claim(db, "h", claimed.claim_token) is False
    assert finish_backend_pending_choice_send(
        db, "h", claimed.claim_token, accepted=False
    ) is False
    with sqlite3.connect(db) as conn:
        assert conn.execute(
            """
            SELECT state FROM backend_pending_claims
            WHERE host_id = 'h' AND worker_id = ?
            """,
            (worker.id,),
        ).fetchone() == ("send_started",)
        assert conn.execute(
            """
            SELECT observation_state FROM backend_pending
            WHERE host_id = 'h' AND worker_id = ?
            """,
            (worker.id,),
        ).fetchone() == ("open",)
    assert finish_backend_pending_choice_send(
        db, "h", claimed.claim_token, accepted=True
    ) is True
    assert list_backend_pending(db, "h") == {}


def test_accepted_answer_immediately_tombstones_snapshot_fallback(
    tmp_path: Path,
) -> None:
    db, config, snapshot = _pending_fixture(tmp_path)
    worker = snapshot.workers[0]
    fallback = pending_payload_from_store(db, "h")
    assert any(
        item["worker_id"] == worker.id
        for item in fallback["pending_interactions"]
    )

    binding = WorkerBinding(
        host_id=config.host_id,
        worker_id=worker.id,
        worker_fingerprint=worker.fingerprint,
        backend="herdr",
        target_kind="agent_id",
        target_value="agent-private-answer",
        turn_target_kind="pane_id",
        turn_target_value="pane-private-answer",
        sendable=True,
        observed_at="2026-07-13T00:00:00+00:00",
        expires_at="9999-12-31T23:59:59+00:00",
        private_fingerprint="binding-private-answer",
    )
    upsert_worker_bindings(db, [binding])
    first = _pending_observation_from_turn(_decision_turn())
    assert apply_backend_pending_observation(
        db,
        "h",
        worker.id,
        first,
        observed_at="2026-07-13T00:00:00+00:00",
        binding_private_fingerprint=binding.private_fingerprint,
        observed_turn_target_value=binding.turn_target_value,
    )
    overlaid_payload = pending_payload_from_store(db, "h")
    overlaid = next(
        item
        for item in overlaid_payload["pending_interactions"]
        if item["worker_id"] == worker.id
    )
    assert overlaid["question"] == "Which database should we use?"
    assert binding.private_fingerprint not in json.dumps(overlaid_payload)
    assert binding.turn_target_value not in json.dumps(overlaid_payload)

    claim = claim_backend_pending_choice(
        db,
        "h",
        overlaid["id"],
        overlaid["fingerprint"],
        overlaid["choices"][0]["choice_id"],
        observed_at="2026-07-13T00:00:01+00:00",
    )
    assert claim.status == "claimed"
    assert start_backend_pending_choice_send(
        db,
        "h",
        claim.claim_token,
        observed_at="2026-07-13T00:00:02+00:00",
    ).status == "started"
    assert finish_backend_pending_choice_send(
        db,
        "h",
        claim.claim_token,
        accepted=True,
        observed_at="2026-07-13T00:00:03Z",
    )

    answered = pending_payload_from_store(db, "h")
    assert not any(
        item["worker_id"] == worker.id
        for item in answered["pending_interactions"]
    )
    answered_blob = json.dumps(answered)
    assert binding.private_fingerprint not in answered_blob
    assert binding.turn_target_value not in answered_blob
    with sqlite3.connect(db) as conn:
        tombstone = conn.execute(
            """
            SELECT payload_json, revision_digest, choice_routes_json,
                   binding_private_fingerprint, observed_turn_target_value,
                   observation_state, freshness, observed_at, last_success_at,
                   last_failure_at, grace_deadline, updated_at
            FROM backend_pending
            WHERE host_id = 'h' AND worker_id = ?
            """,
            (worker.id,),
        ).fetchone()
        assert tombstone == (
            "{}",
            "",
            "{}",
            binding.private_fingerprint,
            binding.turn_target_value,
            "none",
            "fresh",
            "2026-07-13T00:00:03+00:00",
            "2026-07-13T00:00:03+00:00",
            None,
            None,
            "2026-07-13T00:00:03+00:00",
        )
        assert conn.execute(
            """
            SELECT COUNT(*) FROM backend_pending_claims
            WHERE host_id = 'h' AND worker_id = ?
            """,
            (worker.id,),
        ).fetchone() == (0,)

    assert not finish_backend_pending_choice_send(
        db,
        "h",
        claim.claim_token,
        accepted=True,
        observed_at="2026-07-13T00:00:04+00:00",
    )
    with sqlite3.connect(db) as conn:
        assert conn.execute(
            """
            SELECT observation_state, freshness, observed_at
            FROM backend_pending
            WHERE host_id = 'h' AND worker_id = ?
            """,
            (worker.id,),
        ).fetchone() == (
            "none",
            "fresh",
            "2026-07-13T00:00:03+00:00",
        )

    later_turn = _decision_turn()
    later_turn["pending_decision"]["decision_id"] = "private-later-answer"
    assert apply_backend_pending_observation(
        db,
        "h",
        worker.id,
        _pending_observation_from_turn(later_turn),
        observed_at="2026-07-13T00:00:05+00:00",
        binding_private_fingerprint=binding.private_fingerprint,
        observed_turn_target_value=binding.turn_target_value,
    )
    reopened_payload = pending_payload_from_store(db, "h")
    reopened = next(
        item
        for item in reopened_payload["pending_interactions"]
        if item["worker_id"] == worker.id
    )
    assert reopened["id"] != overlaid["id"]
    assert reopened["fingerprint"] != overlaid["fingerprint"]
    assert (
        reopened["choices"][0]["choice_id"]
        != overlaid["choices"][0]["choice_id"]
    )
    reopened_blob = json.dumps(reopened_payload)
    assert binding.private_fingerprint not in reopened_blob
    assert binding.turn_target_value not in reopened_blob


def test_accepted_finish_tombstones_exact_prompt_after_stale_expiry(
    tmp_path: Path,
) -> None:
    db, config, snapshot = _pending_fixture(tmp_path)
    worker = snapshot.workers[0]
    binding = WorkerBinding(
        host_id=config.host_id,
        worker_id=worker.id,
        worker_fingerprint=worker.fingerprint,
        backend="herdr",
        target_kind="agent_id",
        target_value="agent-private-expiry",
        turn_target_kind="pane_id",
        turn_target_value="pane-private-expiry",
        sendable=True,
        observed_at="2026-07-13T00:00:00+00:00",
        expires_at="9999-12-31T23:59:59+00:00",
        private_fingerprint="binding-private-expiry",
    )
    upsert_worker_bindings(db, [binding])
    observation = _pending_observation_from_turn(_decision_turn())
    assert apply_backend_pending_observation(
        db,
        "h",
        worker.id,
        observation,
        observed_at="2026-07-13T00:00:00+00:00",
        binding_private_fingerprint=binding.private_fingerprint,
        observed_turn_target_value=binding.turn_target_value,
    )
    pending = next(
        item
        for item in pending_payload_from_store(db, "h")[
            "pending_interactions"
        ]
        if item["worker_id"] == worker.id
    )
    claim = claim_backend_pending_choice(
        db,
        "h",
        pending["id"],
        pending["fingerprint"],
        pending["choices"][0]["choice_id"],
        observed_at="2026-07-13T00:00:01+00:00",
    )
    assert claim.status == "claimed"
    assert start_backend_pending_choice_send(
        db,
        "h",
        claim.claim_token,
        observed_at="2026-07-13T00:00:02+00:00",
    ).status == "started"
    assert apply_backend_pending_observation(
        db,
        "h",
        worker.id,
        PendingObservation("read_failed"),
        observed_at="2026-07-13T00:00:03+00:00",
        stale_grace_seconds=1,
        binding_private_fingerprint=binding.private_fingerprint,
        observed_turn_target_value=binding.turn_target_value,
    )
    assert apply_backend_pending_observation(
        db,
        "h",
        worker.id,
        PendingObservation("read_failed"),
        observed_at="2026-07-13T00:00:04+00:00",
        stale_grace_seconds=1,
        binding_private_fingerprint=binding.private_fingerprint,
        observed_turn_target_value=binding.turn_target_value,
    )
    with sqlite3.connect(db) as conn:
        failed_row = conn.execute(
            """
            SELECT observation_state, freshness, revision_digest
            FROM backend_pending
            WHERE host_id = 'h' AND worker_id = ?
            """,
            (worker.id,),
        ).fetchone()
        assert failed_row[:2] == ("failed", "stale")
        assert failed_row[2]
        assert conn.execute(
            """
            SELECT state FROM backend_pending_claims
            WHERE host_id = 'h' AND worker_id = ?
            """,
            (worker.id,),
        ).fetchone() == ("send_started",)

    assert finish_backend_pending_choice_send(
        db,
        "h",
        claim.claim_token,
        accepted=True,
        observed_at="2026-07-13T00:00:05+00:00",
    )
    answered = pending_payload_from_store(db, "h")
    assert not any(
        item["worker_id"] == worker.id
        for item in answered["pending_interactions"]
    )
    with sqlite3.connect(db) as conn:
        assert conn.execute(
            """
            SELECT observation_state, freshness,
                   binding_private_fingerprint, observed_turn_target_value
            FROM backend_pending
            WHERE host_id = 'h' AND worker_id = ?
            """,
            (worker.id,),
        ).fetchone() == (
            "none",
            "fresh",
            binding.private_fingerprint,
            binding.turn_target_value,
        )
        assert conn.execute(
            """
            SELECT COUNT(*) FROM backend_pending_claims
            WHERE host_id = 'h' AND worker_id = ?
            """,
            (worker.id,),
        ).fetchone() == (0,)


def test_newer_prompt_racing_accepted_finish_is_not_erased(
    tmp_path: Path,
) -> None:
    db, config, snapshot = _pending_fixture(tmp_path)
    worker = snapshot.workers[0]
    binding = WorkerBinding(
        host_id=config.host_id,
        worker_id=worker.id,
        worker_fingerprint=worker.fingerprint,
        backend="herdr",
        target_kind="agent_id",
        target_value="agent-private-race",
        turn_target_kind="pane_id",
        turn_target_value="pane-private-race",
        sendable=True,
        observed_at="2026-07-13T00:00:00+00:00",
        expires_at="9999-12-31T23:59:59+00:00",
        private_fingerprint="binding-private-race",
    )
    upsert_worker_bindings(db, [binding])
    first = _pending_observation_from_turn(_decision_turn())
    assert apply_backend_pending_observation(
        db,
        "h",
        worker.id,
        first,
        observed_at="2026-07-13T00:00:00+00:00",
        binding_private_fingerprint=binding.private_fingerprint,
        observed_turn_target_value=binding.turn_target_value,
    )
    initial = next(
        item
        for item in pending_payload_from_store(db, "h")[
            "pending_interactions"
        ]
        if item["worker_id"] == worker.id
    )
    claim = claim_backend_pending_choice(
        db,
        "h",
        initial["id"],
        initial["fingerprint"],
        initial["choices"][0]["choice_id"],
        observed_at="2026-07-13T00:00:01+00:00",
    )
    assert claim.status == "claimed"
    assert start_backend_pending_choice_send(
        db,
        "h",
        claim.claim_token,
        observed_at="2026-07-13T00:00:02+00:00",
    ).status == "started"

    newer_turn = _decision_turn()
    newer_turn["pending_decision"]["decision_id"] = "private-racing-revision"
    assert apply_backend_pending_observation(
        db,
        "h",
        worker.id,
        _pending_observation_from_turn(newer_turn),
        observed_at="2026-07-13T00:00:03+00:00",
        binding_private_fingerprint=binding.private_fingerprint,
        observed_turn_target_value=binding.turn_target_value,
    )
    newer_before_finish = next(
        item
        for item in pending_payload_from_store(db, "h")[
            "pending_interactions"
        ]
        if item["worker_id"] == worker.id
    )
    assert newer_before_finish["id"] != initial["id"]
    assert not finish_backend_pending_choice_send(
        db,
        "h",
        claim.claim_token,
        accepted=True,
        observed_at="2026-07-13T00:00:04+00:00",
    )
    newer_after_finish_payload = pending_payload_from_store(db, "h")
    newer_after_finish = next(
        item
        for item in newer_after_finish_payload["pending_interactions"]
        if item["worker_id"] == worker.id
    )
    assert newer_after_finish == newer_before_finish
    private_blob = json.dumps(newer_after_finish_payload)
    assert binding.private_fingerprint not in private_blob
    assert binding.turn_target_value not in private_blob


def test_identical_prompt_on_new_source_mints_new_public_handles(
    tmp_path: Path,
) -> None:
    db, config, snapshot = _pending_fixture(tmp_path)
    worker = snapshot.workers[0]
    binding = WorkerBinding(
        host_id=config.host_id,
        worker_id=worker.id,
        worker_fingerprint=worker.fingerprint,
        backend="herdr",
        target_kind="agent_id",
        target_value="agent-private",
        turn_target_kind="pane_id",
        turn_target_value="pane-a-private",
        sendable=True,
        observed_at="2026-07-13T00:00:00+00:00",
        expires_at="9999-12-31T23:59:59+00:00",
        private_fingerprint="stable-binding-private",
    )
    upsert_worker_bindings(db, [binding])
    observation = _pending_observation_from_turn(_decision_turn())
    assert apply_backend_pending_observation(
        db,
        "h",
        worker.id,
        observation,
        observed_at="2026-07-13T00:00:00+00:00",
        binding_private_fingerprint=binding.private_fingerprint,
        observed_turn_target_value=binding.turn_target_value,
    )
    before = next(
        item
        for item in pending_payload_from_store(db, "h")[
            "pending_interactions"
        ]
        if item["worker_id"] == worker.id
    )
    with sqlite3.connect(db) as conn:
        conn.execute(
            """
            UPDATE worker_bindings
            SET turn_target_value = 'pane-b-private'
            WHERE private_fingerprint = ?
            """,
            (binding.private_fingerprint,),
        )
    assert apply_backend_pending_observation(
        db,
        "h",
        worker.id,
        observation,
        observed_at="2026-07-13T00:00:01+00:00",
        binding_private_fingerprint=binding.private_fingerprint,
        observed_turn_target_value="pane-b-private",
    )
    after_payload = pending_payload_from_store(db, "h")
    after = next(
        item
        for item in after_payload["pending_interactions"]
        if item["worker_id"] == worker.id
    )
    assert after["id"] != before["id"]
    assert after["fingerprint"] != before["fingerprint"]
    assert after["choices"][0]["choice_id"] != before["choices"][0]["choice_id"]
    assert "pane-a-private" not in json.dumps(after_payload)
    assert "pane-b-private" not in json.dumps(after_payload)
    assert claim_backend_pending_choice(
        db,
        "h",
        before["id"],
        before["fingerprint"],
        before["choices"][0]["choice_id"],
        observed_at="2026-07-13T00:00:02+00:00",
    ).status == "not_found"
    claimed = claim_backend_pending_choice(
        db,
        "h",
        after["id"],
        after["fingerprint"],
        after["choices"][0]["choice_id"],
        observed_at="2026-07-13T00:00:02+00:00",
    )
    assert claimed.status == "claimed"
    assert claimed.turn_target_value == "pane-b-private"


def test_claim_and_authoritative_prune_are_bound_to_exact_source_pane(
    tmp_path: Path,
) -> None:
    db, config, snapshot = _pending_fixture(tmp_path)
    worker = snapshot.workers[0]
    source = WorkerBinding(
        host_id=config.host_id,
        worker_id=worker.id,
        worker_fingerprint=worker.fingerprint,
        backend="herdr",
        target_kind="agent_id",
        target_value="agent-source",
        turn_target_kind="pane_id",
        turn_target_value="pane-source-private",
        sendable=True,
        observed_at="2026-07-13T00:00:00+00:00",
        expires_at="9999-12-31T23:59:59+00:00",
        private_fingerprint="z-source-binding",
    )
    decoy = WorkerBinding(
        host_id=config.host_id,
        worker_id=worker.id,
        worker_fingerprint=worker.fingerprint,
        backend="herdr",
        target_kind="agent_id",
        target_value="agent-decoy",
        turn_target_kind="pane_id",
        turn_target_value="pane-decoy-private",
        sendable=True,
        observed_at="2026-07-13T00:00:00+00:00",
        expires_at="9999-12-31T23:59:59+00:00",
        private_fingerprint="a-decoy-binding",
    )
    upsert_worker_bindings(db, [decoy, source])
    apply_backend_pending_observation(
        db,
        "h",
        worker.id,
        _pending_observation_from_turn(_decision_turn()),
        observed_at="2026-07-13T00:00:00+00:00",
        binding_private_fingerprint=source.private_fingerprint,
        observed_turn_target_value=source.turn_target_value,
    )
    payload = pending_payload_from_store(db, "h")
    row = next(
        item for item in payload["pending_interactions"]
        if item["worker_id"] == worker.id
    )
    with sqlite3.connect(db) as conn:
        conn.execute(
            """
            UPDATE worker_bindings
            SET turn_target_value = 'pane-moved-private'
            WHERE private_fingerprint = ?
            """,
            (source.private_fingerprint,),
        )
    assert claim_backend_pending_choice(
        db,
        "h",
        row["id"],
        row["fingerprint"],
        row["choices"][0]["choice_id"],
        observed_at="2026-07-13T00:00:00.500000+00:00",
    ).status == "not_found"
    with sqlite3.connect(db) as conn:
        conn.execute(
            """
            UPDATE worker_bindings
            SET turn_target_value = ?
            WHERE private_fingerprint = ?
            """,
            (source.turn_target_value, source.private_fingerprint),
        )
    claimed = claim_backend_pending_choice(
        db,
        "h",
        row["id"],
        row["fingerprint"],
        row["choices"][0]["choice_id"],
        observed_at="2026-07-13T00:00:01+00:00",
    )
    assert claimed.status == "claimed"
    assert claimed.binding_private_fingerprint == source.private_fingerprint
    assert claimed.turn_target_value == source.turn_target_value
    assert source.turn_target_value not in json.dumps(payload)
    with sqlite3.connect(db) as conn:
        conn.execute(
            """
            UPDATE worker_bindings
            SET turn_target_value = 'pane-moved-private'
            WHERE private_fingerprint = ?
            """,
            (source.private_fingerprint,),
        )
    assert start_backend_pending_choice_send(
        db,
        "h",
        claimed.claim_token,
        observed_at="2026-07-13T00:00:01.500000+00:00",
    ).status == "binding_changed"

    assert prune_backend_pending(db, "h", {decoy.private_fingerprint}) == 1
    assert list_backend_pending(db, "h") == {}
    assert start_backend_pending_choice_send(
        db,
        "h",
        claimed.claim_token,
        observed_at="2026-07-13T00:00:02+00:00",
    ).status == "not_found"


@pytest.mark.parametrize("terminal_status", ["closed", "failed"])
def test_start_send_rejects_latest_terminal_worker_status(
    tmp_path: Path,
    terminal_status: str,
) -> None:
    db, config, snapshot = _pending_fixture(tmp_path)
    worker = snapshot.workers[0]
    binding = WorkerBinding(
        host_id=config.host_id,
        worker_id=worker.id,
        worker_fingerprint=worker.fingerprint,
        backend="herdr",
        target_kind="agent_id",
        target_value="agent-private",
        turn_target_kind="pane_id",
        turn_target_value="pane-private",
        sendable=True,
        observed_at="2026-07-13T00:00:00+00:00",
        expires_at="9999-12-31T23:59:59+00:00",
        private_fingerprint="terminal-binding",
    )
    upsert_worker_bindings(db, [binding])
    apply_backend_pending_observation(
        db,
        "h",
        worker.id,
        _pending_observation_from_turn(_decision_turn()),
        observed_at="2026-07-13T00:00:00+00:00",
        binding_private_fingerprint=binding.private_fingerprint,
        observed_turn_target_value=binding.turn_target_value,
    )
    payload = pending_payload_from_store(db, "h")
    row = next(
        item for item in payload["pending_interactions"]
        if item["worker_id"] == worker.id
    )
    claimed = claim_backend_pending_choice(
        db,
        "h",
        row["id"],
        row["fingerprint"],
        row["choices"][0]["choice_id"],
        observed_at="2026-07-13T00:00:01+00:00",
    )
    assert claimed.status == "claimed"
    terminal = project_from_raw(
        config,
        workers=[
            {
                "id": worker.id,
                "name": worker.name,
                "status": terminal_status,
                "space_id": worker.space_id,
            }
        ],
    )
    terminal_payload = terminal.to_dict()
    terminal_payload["workers"][0]["fingerprint"] = worker.fingerprint
    terminal = Snapshot.from_dict(terminal_payload)
    assert terminal.workers[0].fingerprint == worker.fingerprint
    save_snapshot(db, terminal)
    assert start_backend_pending_choice_send(
        db,
        "h",
        claimed.claim_token,
        observed_at="2026-07-13T00:00:02+00:00",
    ).status == "binding_changed"


def test_presend_claim_lease_reclaims_only_unstarted_owner(tmp_path: Path) -> None:
    db, config, snapshot = _pending_fixture(tmp_path)
    worker = snapshot.workers[0]
    upsert_worker_bindings(
        db,
        [
            WorkerBinding(
                host_id=config.host_id,
                worker_id=worker.id,
                worker_fingerprint=worker.fingerprint,
                backend="herdr",
                target_kind="agent_id",
                target_value="agent",
                turn_target_kind="pane_id",
                turn_target_value="pane",
                sendable=True,
                observed_at="2026-07-13T00:00:00+00:00",
                expires_at="9999-12-31T23:59:59+00:00",
                private_fingerprint="lease-binding",
            )
        ],
    )
    observation = _pending_observation_from_turn(_decision_turn())
    apply_backend_pending_observation(
        db,
        "h",
        worker.id,
        observation,
        observed_at="2026-07-13T00:00:00+00:00",
        binding_private_fingerprint="lease-binding",
        observed_turn_target_value="pane",
    )
    projected = pending_payload_from_store(db, "h")
    row = next(item for item in projected["pending_interactions"] if item["worker_id"] == worker.id)
    args = (db, "h", row["id"], row["fingerprint"], row["choices"][0]["choice_id"])
    old = claim_backend_pending_choice(
        *args,
        observed_at="2026-07-13T00:00:01+00:00",
        claim_lease_seconds=30,
    )
    assert old.status == "claimed"
    assert claim_backend_pending_choice(
        *args,
        observed_at="2026-07-13T00:00:30+00:00",
        claim_lease_seconds=30,
    ).status == "already_claimed"
    replacement = claim_backend_pending_choice(
        *args,
        observed_at="2026-07-13T00:00:31+00:00",
        claim_lease_seconds=30,
    )
    assert replacement.status == "claimed"
    assert replacement.claim_token != old.claim_token
    assert start_backend_pending_choice_send(
        db,
        "h",
        old.claim_token,
        observed_at="2026-07-13T00:00:31+00:00",
        claim_lease_seconds=30,
    ).status == "not_found"
    assert start_backend_pending_choice_send(
        db,
        "h",
        replacement.claim_token,
        observed_at="2026-07-13T00:00:32+00:00",
        claim_lease_seconds=30,
    ).status == "started"
    assert claim_backend_pending_choice(
        *args,
        observed_at="2026-07-14T00:00:00+00:00",
        claim_lease_seconds=1,
    ).status == "already_claimed"


def test_new_revision_retires_uncertain_claim_and_malformed_overlay_falls_back(
    tmp_path: Path,
) -> None:
    db, config, snapshot = _pending_fixture(tmp_path)
    worker = snapshot.workers[0]
    upsert_worker_bindings(
        db,
        [
            WorkerBinding(
                host_id="h",
                worker_id=worker.id,
                worker_fingerprint=worker.fingerprint,
                backend="herdr",
                target_kind="agent_id",
                target_value="agent",
                turn_target_kind="pane_id",
                turn_target_value="pane",
                sendable=True,
                observed_at="2026-07-13T00:00:00+00:00",
                expires_at="9999-12-31T23:59:59+00:00",
                private_fingerprint="binding",
            )
        ],
    )
    first = _pending_observation_from_turn(_decision_turn())
    apply_backend_pending_observation(
        db,
        "h",
        worker.id,
        first,
        observed_at="2026-07-13T00:00:00+00:00",
        binding_private_fingerprint="binding",
        observed_turn_target_value="pane",
    )
    payload = pending_payload_from_store(db, "h")
    row = next(item for item in payload["pending_interactions"] if item["worker_id"] == worker.id)
    claim = claim_backend_pending_choice(
        db, "h", row["id"], row["fingerprint"], row["choices"][0]["choice_id"],
        observed_at="2026-07-13T00:00:01+00:00",
    )
    assert claim.status == "claimed"
    assert start_backend_pending_choice_send(
        db,
        "h",
        claim.claim_token,
        observed_at="2026-07-13T00:00:01.500000+00:00",
    ).status == "started"
    assert not finish_backend_pending_choice_send(
        db,
        "h",
        claim.claim_token,
        accepted=False,
    )
    changed = _decision_turn()
    changed["pending_decision"]["decision_id"] = "private-new-revision"
    apply_backend_pending_observation(
        db,
        "h",
        worker.id,
        _pending_observation_from_turn(changed),
        observed_at="2026-07-13T00:00:02+00:00",
        binding_private_fingerprint="binding",
        observed_turn_target_value="pane",
    )
    assert start_backend_pending_choice_send(
        db, "h", claim.claim_token,
        observed_at="2026-07-13T00:00:03+00:00",
    ).status == "not_found"
    changed_payload = pending_payload_from_store(db, "h")
    changed_row = next(
        item
        for item in changed_payload["pending_interactions"]
        if item["worker_id"] == worker.id
    )
    replacement = claim_backend_pending_choice(
        db,
        "h",
        changed_row["id"],
        changed_row["fingerprint"],
        changed_row["choices"][0]["choice_id"],
        observed_at="2026-07-13T00:00:03.500000+00:00",
    )
    assert replacement.status == "claimed"

    with sqlite3.connect(db) as conn:
        conn.execute(
            "UPDATE backend_pending SET payload_json = ? WHERE worker_id = ?",
            ('{"question":"broken","kind":"question","choices":"bad"}', worker.id),
        )
    fallback = pending_payload_from_store(db, "h")
    assert any(item["worker_id"] == worker.id for item in fallback["pending_interactions"])
    assert all(item["question"] != "broken" for item in fallback["pending_interactions"])


def test_cached_pending_projection_never_migrates_older_store(
    tmp_path: Path,
) -> None:
    db, _config, _snapshot = _pending_fixture(tmp_path)
    with sqlite3.connect(db) as conn:
        conn.execute("PRAGMA user_version = 9")
    result = pending_payload_from_store(db, "h")
    assert result["status"] == "store_unavailable"
    assert result["ok"] is False
    with sqlite3.connect(db) as conn:
        assert conn.execute("PRAGMA user_version").fetchone()[0] == 9


def test_current_schema_creation_has_exact_binding_and_claim_state(tmp_path: Path) -> None:
    db = tmp_path / "current-schema.db"
    init_store(db)
    with sqlite3.connect(db) as conn:
        assert conn.execute("PRAGMA user_version").fetchone()[0] == STORE_SCHEMA_VERSION == 21
        columns = {
            str(row[1])
            for row in conn.execute("PRAGMA table_info(backend_pending)").fetchall()
        }
        assert {
            "revision_digest",
            "choice_routes_json",
            "binding_private_fingerprint",
            "observed_turn_target_value",
            "observation_state",
            "freshness",
            "last_success_at",
            "last_failure_at",
            "grace_deadline",
            "updated_at",
        } <= columns
        claim_columns = {
            str(row[1])
            for row in conn.execute(
                "PRAGMA table_info(backend_pending_claims)"
            ).fetchall()
        }
        assert {
            "binding_private_fingerprint",
            "turn_target_value",
            "state",
            "send_started_at",
        } <= claim_columns
        assert "backend_pending_claims" in {
            str(row[0])
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }


def test_v9_pending_migration_preserves_public_row_but_leaves_it_unrouted(
    tmp_path: Path,
) -> None:
    db = tmp_path / "legacy-v9.db"
    init_store(db)
    config = Config(host_id="h", db_path=db)
    snapshot = project_from_raw(
        config,
        workers=[
            {
                "id": "worker-legacy",
                "name": "legacy worker",
                "status": "blocked",
            }
        ],
    )
    save_snapshot(db, snapshot)
    legacy_payload = json.dumps(
        {
            "question": "Legacy approval?",
            "kind": "approval",
            "choices": [{"choice_id": "choice-0123456789abcdef01234567", "label": "Approve"}],
            "meta": {"source": "backend"},
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    with sqlite3.connect(db) as conn:
        conn.execute("DROP TABLE backend_pending_claims")
        conn.execute("ALTER TABLE backend_pending RENAME TO backend_pending_v10")
        conn.execute(
            """
            CREATE TABLE backend_pending (
                host_id TEXT NOT NULL,
                worker_id TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                observed_at TEXT NOT NULL,
                PRIMARY KEY (host_id, worker_id)
            )
            """
        )
        conn.execute(
            "INSERT INTO backend_pending VALUES (?, ?, ?, ?)",
            ("h", "worker-legacy", legacy_payload, "2026-07-13T00:00:00+00:00"),
        )
        conn.execute("DROP TABLE backend_pending_v10")
        conn.execute("PRAGMA user_version = 9")
    init_store(db)
    with sqlite3.connect(db) as conn:
        assert conn.execute("PRAGMA user_version").fetchone()[0] == STORE_SCHEMA_VERSION
        migrated = conn.execute(
            """
            SELECT payload_json, observation_state, freshness,
                   choice_routes_json, binding_private_fingerprint,
                   observed_turn_target_value, last_success_at, updated_at
            FROM backend_pending
            """
        ).fetchone()
    assert migrated[0] == legacy_payload
    assert migrated[1:6] == ("open", "fresh", "{}", "", "")
    assert migrated[6:] == (
        "2026-07-13T00:00:00+00:00",
        "2026-07-13T00:00:00+00:00",
    )
    projected = pending_payload_from_store(db, "h")
    legacy = next(
        row
        for row in projected["pending_interactions"]
        if row["worker_id"] == "worker-legacy"
    )
    assert legacy["question"] == "Legacy approval?"
    assert legacy["choices"][0]["label"] == "Approve"
