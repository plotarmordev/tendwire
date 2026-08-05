"""Projection/content references survive retention as one atomic contract."""

from __future__ import annotations

import sqlite3
from pathlib import Path

from tendwire.core.models import Snapshot, Worker, WorkerBinding
from tendwire.store.projection import save_snapshot
from tendwire.store.retention import RetentionPolicy, run_retention_cycle
from tendwire.store.schema import init_store
from tendwire.store.turns import apply_turn_refresh, get_turn_content, turns_payload_from_store


def test_current_content_and_route_correlation_survive_old_cutoff(tmp_path: Path) -> None:
    db_path = tmp_path / "projection.db"
    init_store(db_path)
    worker = Worker(id="worker-a", name="codex", meta={"stable_key": "wsk1_" + "a" * 64, "stable_key_version": 1})
    binding = WorkerBinding(host_id="host-a", worker_id=worker.id, worker_fingerprint=worker.fingerprint, backend="herdr", target_kind="agent_id", target_value="private", observed_at="2026-01-01T00:00:00Z", private_fingerprint="private")
    persisted = save_snapshot(db_path, Snapshot(host_id="host-a", updated_at="2026-01-01T00:00:00Z", workers=[worker]), worker_bindings=[binding], binding_backend="herdr")
    result = apply_turn_refresh(db_path, "host-a", worker.id, {"source_turn_id": "turn-a", "user_text": "q", "assistant_final_text": "a", "complete": True}, observed_at="2026-01-01T00:00:00Z")
    assert result.updated == 1
    run_retention_cycle(db_path, policy=RetentionPolicy(), now="2026-08-05T00:00:00Z")
    turn = turns_payload_from_store(db_path, "host-a", schema_version=2)["turns"][0]
    assert turn["route_generation"] == persisted.workers[0].meta["route_generation"]
    content = get_turn_content(db_path, "host-a", turn_id="turn-a", content_revision=turn["content_revision"], field="assistant_final_text")
    assert content["ok"] is True and content["text"] == "a"
    with sqlite3.connect(db_path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM worker_bindings").fetchone()[0] == 1
