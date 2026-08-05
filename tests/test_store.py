"""Focused public-contract tests for the concern-owned store modules."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from tendwire.core.agent_events import AgentEventIdentityConflict, agent_event
from tendwire.core.models import BackendHealth, Snapshot, Worker, WorkerBinding
from tendwire.store.db import StorePathError, read_transaction, write_transaction
from tendwire.store.events import _append as append_agent_event
from tendwire.store.events import list_agent_events
from tendwire.store.projection import (
    latest_snapshot,
    list_worker_bindings,
    save_snapshot,
)
from tendwire.store.receipts import (
    abandon_command_request_reservation,
    finish_command_request,
    get_command_request,
    reserve_command_request,
)
from tendwire.store.schema import STORE_SCHEMA_VERSION, init_store
from tendwire.store.turns import get_turn_content, turns_payload_from_store
from .store_helpers import append_test_turn


NOW = "2026-08-05T00:00:00.000000Z"


def _seed_route(db_path: Path) -> tuple[Worker, WorkerBinding]:
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
        target_value="private-agent",
        observed_at=NOW,
        private_fingerprint="private-binding-a",
    )
    persisted = save_snapshot(
        db_path,
        Snapshot(
            host_id="host-a",
            updated_at=NOW,
            workers=[worker],
            backend_health=[
                BackendHealth(
                    name="herdr",
                    status="healthy",
                    outcome="healthy_non_empty",
                    observed_at=NOW,
                )
            ],
        ),
        worker_bindings=[binding],
        binding_backend="herdr",
    )
    return persisted.workers[0], binding


def test_init_store_is_idempotent_and_exact_version(tmp_path: Path) -> None:
    db_path = tmp_path / "store.db"
    init_store(db_path)
    first = db_path.stat()
    init_store(db_path)
    with sqlite3.connect(db_path) as conn:
        assert conn.execute("PRAGMA user_version").fetchone()[0] == STORE_SCHEMA_VERSION
        assert conn.execute("PRAGMA foreign_key_check").fetchall() == []
    assert (db_path.stat().st_dev, db_path.stat().st_ino) == (first.st_dev, first.st_ino)


@pytest.mark.parametrize("path", [Path("relative.db"), Path(":memory:")])
def test_store_rejects_non_authoritative_paths(path: Path) -> None:
    with pytest.raises((StorePathError, ValueError)):
        init_store(path)


def test_snapshot_save_returns_persisted_route_enrichment(tmp_path: Path) -> None:
    db_path = tmp_path / "store.db"
    init_store(db_path)
    worker, _ = _seed_route(db_path)
    generation = worker.meta["route_generation"]
    assert generation.startswith("twroute1.") and len(generation) == 52
    latest = latest_snapshot(db_path, "host-a")
    assert latest is not None
    assert latest.workers[0].meta["route_generation"] == generation
    bindings = list_worker_bindings(db_path, "host-a", backend="herdr")
    assert len(bindings) == 1
    with read_transaction(db_path) as conn:
        route = conn.execute(
            "SELECT route_generation,partition_key FROM worker_bindings"
        ).fetchone()
    assert route[0] == generation
    assert str(route[1]).startswith("twpart1_")


def test_older_snapshot_cannot_replace_public_state_or_add_binding(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "store.db"
    init_store(db_path)
    stable = {"stable_key": "wsk1_" + "a" * 64, "stable_key_version": 1}
    newer_worker = Worker(id="worker-a", name="newer", meta=stable)
    newer = Snapshot(
        host_id="host-a",
        updated_at="2026-08-05T00:00:10Z",
        workers=[newer_worker],
    )
    save_snapshot(db_path, newer)
    older_worker = Worker(id="worker-a", name="older", meta=stable)
    older_binding = WorkerBinding(
        host_id="host-a",
        worker_id="worker-a",
        worker_fingerprint=older_worker.fingerprint,
        backend="herdr",
        target_kind="agent_id",
        target_value="stale-private",
        private_fingerprint="stale-binding",
    )
    returned = save_snapshot(
        db_path,
        Snapshot(
            host_id="host-a",
            updated_at="2026-08-05T00:00:09Z",
            workers=[older_worker],
        ),
        worker_bindings=[older_binding],
        binding_backend="herdr",
    )
    assert returned.workers[0].name == "newer"
    assert latest_snapshot(db_path, "host-a").workers[0].name == "newer"
    assert list_worker_bindings(db_path, "host-a") == []


def test_equal_time_snapshots_have_deterministic_authority_order(
    tmp_path: Path,
) -> None:
    stable = {"stable_key": "wsk1_" + "a" * 64, "stable_key_version": 1}
    candidates = [
        Snapshot(
            host_id="host-a",
            updated_at=NOW,
            workers=[Worker(id="worker-a", name=name, meta=stable)],
        )
        for name in ("alpha", "omega")
    ]
    expected = max(candidates, key=lambda item: item.content_fingerprint)
    observed: list[Snapshot] = []
    for ordinal, order in enumerate((candidates, list(reversed(candidates)))):
        db_path = tmp_path / f"equal-{ordinal}.db"
        init_store(db_path)
        for snapshot in order:
            save_snapshot(db_path, snapshot)
        latest = latest_snapshot(db_path, "host-a")
        assert latest is not None
        observed.append(latest)
    assert [item.workers[0].name for item in observed] == [
        expected.workers[0].name,
        expected.workers[0].name,
    ]
    assert observed[0].content_fingerprint == observed[1].content_fingerprint


def test_exact_public_snapshot_replay_keeps_herdr_and_acp_bindings(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "store.db"
    init_store(db_path)
    worker = Worker(
        id="worker-a",
        name="codex",
        meta={"stable_key": "wsk1_" + "a" * 64, "stable_key_version": 1},
    )
    snapshot = Snapshot(host_id="host-a", updated_at=NOW, workers=[worker])
    herdr = WorkerBinding(
        host_id="host-a",
        worker_id=worker.id,
        worker_fingerprint=worker.fingerprint,
        backend="herdr",
        target_kind="agent_id",
        target_value="herdr-private",
        private_fingerprint="herdr-binding",
    )
    acp = WorkerBinding(
        host_id="host-a",
        worker_id=worker.id,
        worker_fingerprint=worker.fingerprint,
        backend="acp",
        target_kind="acp_session_id",
        target_value="acp-private",
        turn_target_kind="acp_session_id",
        turn_target_value="acp-private",
        sendable=True,
        private_fingerprint="acp-binding",
    )
    first = save_snapshot(
        db_path, snapshot, worker_bindings=[herdr], binding_backend="herdr"
    )
    replay = save_snapshot(
        db_path, snapshot, worker_bindings=[acp], binding_backend="acp"
    )
    assert replay.content_fingerprint == first.content_fingerprint
    bindings = list_worker_bindings(db_path, "host-a")
    assert {(binding.backend, binding.private_fingerprint) for binding in bindings} == {
        ("herdr", "herdr-binding"),
        ("acp", "acp-binding"),
    }


def test_identical_route_reuses_generation_and_private_change_rotates(tmp_path: Path) -> None:
    db_path = tmp_path / "store.db"
    init_store(db_path)
    first_worker, binding = _seed_route(db_path)
    first_generation = first_worker.meta["route_generation"]
    public_worker = Worker(
        id="worker-a",
        name="codex",
        meta={"stable_key": "wsk1_" + "a" * 64, "stable_key_version": 1},
    )
    public_snapshot = Snapshot(
        host_id="host-a",
        updated_at=NOW,
        workers=[public_worker],
        backend_health=[
            BackendHealth(
                name="herdr",
                status="healthy",
                outcome="healthy_non_empty",
                observed_at=NOW,
            )
        ],
    )
    replay = save_snapshot(
        db_path,
        public_snapshot,
        worker_bindings=[binding],
        binding_backend="herdr",
    )
    assert replay.workers[0].meta["route_generation"] == first_generation
    changed = WorkerBinding(**{**binding.__dict__, "private_fingerprint": "private-binding-b"})
    rotated = save_snapshot(
        db_path,
        public_snapshot,
        worker_bindings=[changed],
        binding_backend="herdr",
    )
    assert rotated.workers[0].meta["route_generation"] == first_generation
    changed_route = next(
        item
        for item in list_worker_bindings(db_path, "host-a", backend="herdr")
        if item.private_fingerprint == "private-binding-b"
    )
    with read_transaction(db_path) as conn:
        changed_generation = conn.execute(
            """SELECT route_generation FROM worker_bindings
            WHERE host_id='host-a' AND backend='herdr' AND private_fingerprint='private-binding-b'"""
        ).fetchone()[0]
    assert changed_route.private_fingerprint == "private-binding-b"
    assert changed_generation != first_generation


def test_future_observation_rotates_route_without_future_dating_expiry(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "store.db"
    init_store(db_path)
    worker, binding = _seed_route(db_path)
    public_worker = Worker(
        id="worker-a",
        name="codex",
        meta={"stable_key": "wsk1_" + "a" * 64, "stable_key_version": 1},
    )
    future_binding = WorkerBinding(
        **{
            **binding.__dict__,
            "worker_fingerprint": public_worker.fingerprint,
            "private_fingerprint": "private-binding-future",
            "observed_at": "2099-01-01T00:00:00Z",
        }
    )
    save_snapshot(
        db_path,
        Snapshot(
            host_id="host-a",
            updated_at=NOW,
            workers=[public_worker],
            backend_health=[
                BackendHealth(
                    name="herdr",
                    status="healthy",
                    outcome="healthy_non_empty",
                    observed_at=NOW,
                )
            ],
        ),
        worker_bindings=[future_binding],
        binding_backend="herdr",
    )

    live = list_worker_bindings(db_path, "host-a", backend="herdr")
    assert [item.private_fingerprint for item in live] == ["private-binding-future"]
    expired = {
        item.private_fingerprint: item
        for item in list_worker_bindings(
            db_path, "host-a", backend="herdr", include_expired=True
        )
    }
    assert expired[binding.private_fingerprint].reason == "superseded_route"
    assert expired[binding.private_fingerprint].expires_at < "2099-01-01T00:00:00Z"
    assert worker.meta["route_generation"]


def test_future_authoritative_snapshot_prunes_stale_binding_immediately(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "store.db"
    init_store(db_path)
    _worker, binding = _seed_route(db_path)
    save_snapshot(
        db_path,
        Snapshot(host_id="host-a", updated_at="2099-01-01T00:00:00Z", workers=[]),
        worker_bindings=[],
        binding_backend="herdr",
        binding_observation_authoritative=True,
        binding_workers_present=False,
    )

    assert list_worker_bindings(db_path, "host-a", backend="herdr") == []
    expired = list_worker_bindings(
        db_path, "host-a", backend="herdr", include_expired=True
    )
    assert len(expired) == 1
    assert expired[0].private_fingerprint == binding.private_fingerprint
    assert expired[0].reason == "stale_observation"
    assert expired[0].expires_at < "2099-01-01T00:00:00Z"


def test_agent_event_authoritative_dedupe_and_conflict(tmp_path: Path) -> None:
    db_path = tmp_path / "store.db"
    init_store(db_path)
    event = agent_event(
        kind="agent_message",
        source="acp",
        worker_id="worker-a",
        payload={"text": "public"},
        source_session_id="session-a",
        source_event_id="event-a",
        observed_at=NOW,
    )
    fields = {
        "kind": event.kind,
        "source": event.source,
        "worker_id": event.worker_id,
        "payload": event.payload,
        "source_session_id": event.source_session_id,
        "source_turn_id": event.source_turn_id,
        "source_item_id": event.source_item_id,
        "source_message_id": event.source_message_id,
        "source_event_id": event.source_event_id,
        "source_sequence": event.source_sequence,
        "visibility": event.visibility,
        "observed_at": event.observed_at,
    }
    with write_transaction(db_path) as conn:
        first = append_agent_event(conn, "host-a", event)
    with write_transaction(db_path) as conn:
        replay = append_agent_event(conn, "host-a", event)
    assert first.inserted is True
    assert replay.inserted is False
    with pytest.raises(AgentEventIdentityConflict):
        with write_transaction(db_path) as conn:
            append_agent_event(
                conn,
                "host-a",
                agent_event(**{**fields, "payload": {"text": "changed"}}),
            )
    stored = list_agent_events(db_path, "host-a")
    assert len(stored) == 1 and stored[0].event.payload == {"text": "public"}


def test_turn_refresh_preserves_content_and_paging(tmp_path: Path) -> None:
    db_path = tmp_path / "store.db"
    init_store(db_path)
    _seed_route(db_path)
    result = append_test_turn(
        db_path,
        "host-a",
        "worker-a",
        {
            "source_turn_id": "turn-a",
            "user_text": "question",
            "assistant_final_text": "answer " * 20_000,
            "complete": True,
        },
        observed_at=NOW,
    )
    assert result.updated == 1
    payload = turns_payload_from_store(db_path, "host-a", schema_version=2)
    assert [turn["id"] for turn in payload["turns"]] == ["turn-a"]
    revision = payload["turns"][0]["content_revision"]
    page = get_turn_content(
        db_path,
        "host-a",
        turn_id="turn-a",
        content_revision=revision,
        field="assistant_final_text",
    )
    assert page["ok"] is True
    assert page["text"].startswith("answer")


def _reservation(db_path: Path, request_id: str = "request-a") -> dict[str, object]:
    return reserve_command_request(
        db_path,
        host_id="host-a",
        request_id=request_id,
        action="send_instruction",
        canonical_version=1,
        canonical_fingerprint="fingerprint-a",
        canonical_request_json=json.dumps({"action": "send_instruction"}),
        public_worker_id="worker-a",
        pending_result_json=json.dumps({"status": "pending"}),
        now=NOW,
    )


def test_command_reservation_replay_conflict_and_terminal_immutability(tmp_path: Path) -> None:
    db_path = tmp_path / "store.db"
    init_store(db_path)
    first = _reservation(db_path)
    assert first["status"] == "reserved"
    assert _reservation(db_path)["status"] == "in_progress"
    conflict = reserve_command_request(
        db_path,
        host_id="host-a",
        request_id="request-a",
        action="send_instruction",
        canonical_version=1,
        canonical_fingerprint="different",
        canonical_request_json=json.dumps({"action": "send_instruction"}),
        public_worker_id="worker-a",
        pending_result_json="{}",
        now=NOW,
    )
    assert conflict["status"] == "request_id_conflict"
    terminal = finish_command_request(
        db_path,
        host_id="host-a",
        request_id="request-a",
        canonical_fingerprint="fingerprint-a",
        owner_token=str(first["owner_token"]),
        expected_state="reserved",
        terminal_state="rejected",
        status="rejected",
        result_json=json.dumps({"status": "rejected"}),
        now=NOW,
    )
    assert terminal["status"] == "rejected"
    assert _reservation(db_path)["status"] == "terminal"
    assert get_command_request(db_path, "host-a", "request-a")["state"] == "rejected"


def test_reservation_atomically_fences_selector_proof_across_takeover(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "store.db"
    init_store(db_path)
    common = {
        "host_id": "host-a",
        "request_id": "request-a",
        "action": "send_instruction",
        "canonical_version": 1,
        "canonical_fingerprint": "fingerprint-a",
        "canonical_request_json": "{}",
        "public_worker_id": "worker-a",
        "pending_result_json": "{}",
    }
    first = reserve_command_request(
        db_path, **common, selector_proof="proof-a", now=NOW,
    )
    assert first["status"] == "reserved"
    assert reserve_command_request(
        db_path, **common, selector_proof="proof-b", now=NOW,
    )["status"] == "request_id_conflict"
    assert abandon_command_request_reservation(
        db_path,
        host_id="host-a",
        request_id="request-a",
        canonical_fingerprint="fingerprint-a",
        owner_token=str(first["owner_token"]),
        now=NOW,
    )
    assert reserve_command_request(
        db_path, **common, selector_proof="proof-b", now=NOW,
    )["status"] == "request_id_conflict"
    takeover = reserve_command_request(
        db_path, **common, selector_proof="proof-a", now=NOW,
    )
    assert takeover["status"] == "reserved"
    assert takeover["receipt"]["selector_proof"] == "proof-a"

    legacy = {**common, "request_id": "request-legacy"}
    empty = reserve_command_request(db_path, **legacy, selector_proof="", now=NOW)
    assert abandon_command_request_reservation(
        db_path,
        host_id="host-a",
        request_id="request-legacy",
        canonical_fingerprint="fingerprint-a",
        owner_token=str(empty["owner_token"]),
        now=NOW,
    )
    assert reserve_command_request(
        db_path, **legacy, selector_proof="", now=NOW,
    )["status"] == "reserved"
