"""Retention floors, cutoff validation, and checkpoint hardening."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from tendwire.core.models import Snapshot, Worker
from tendwire.store.projection import latest_snapshot, save_snapshot
from tendwire.store.retention import (
    TURN_FINAL_ROUTE_CONTENT_RETENTION_DAYS,
    TURN_FINAL_TARGETABLE_RETENTION_DAYS,
    RetentionPolicy,
    run_retention_cycle,
)
from tendwire.store.schema import init_store


@pytest.mark.parametrize(
    "kwargs",
    [
        {"batch_size": 0},
        {"batch_size": 1001},
        {"batch_size": True},
        {"targetable_retention_days": TURN_FINAL_TARGETABLE_RETENTION_DAYS - 1},
        {"route_content_retention_days": TURN_FINAL_ROUTE_CONTENT_RETENTION_DAYS - 1},
    ],
)
def test_retention_policy_rejects_unbounded_or_shortened_values(kwargs: dict[str, object]) -> None:
    with pytest.raises(ValueError):
        RetentionPolicy(**kwargs)


def test_retention_reports_a_truncate_checkpoint(tmp_path: Path) -> None:
    db_path = tmp_path / "retention.db"
    init_store(db_path)
    result = run_retention_cycle(
        db_path,
        policy=RetentionPolicy(batch_size=3),
        now="2026-08-05T00:00:00Z",
    )
    assert set(result["checkpoint"]) == {"busy", "log", "checkpointed"}
    assert all(type(value) is int and value >= 0 for value in result["checkpoint"].values())
    with sqlite3.connect(db_path) as conn:
        assert conn.execute("PRAGMA integrity_check").fetchone()[0] == "ok"


def test_retention_rejects_noncanonical_now_without_mutation(tmp_path: Path) -> None:
    db_path = tmp_path / "retention.db"
    init_store(db_path)
    with pytest.raises(ValueError):
        run_retention_cycle(db_path, policy=RetentionPolicy(), now="not-a-time")


def test_retention_keeps_latest_snapshot_for_each_quiet_host(tmp_path: Path) -> None:
    db_path = tmp_path / "retention.db"
    init_store(db_path)
    for host_id, observed_at, worker_name in (
        ("host-a", "2026-01-01T00:00:00Z", "old"),
        ("host-a", "2026-01-01T00:00:01Z", "new"),
        ("host-b", "2026-01-01T00:00:00Z", "only"),
    ):
        save_snapshot(
            db_path,
            Snapshot(
                host_id=host_id,
                updated_at=observed_at,
                workers=[Worker(id="worker-a", name=worker_name)],
            ),
        )

    result = run_retention_cycle(
        db_path,
        policy=RetentionPolicy(snapshot_retention_days=14),
        now="2026-08-05T00:00:00Z",
    )

    assert result["snapshots"] == 1
    assert latest_snapshot(db_path, "host-a").updated_at == "2026-01-01T00:00:01Z"
    assert latest_snapshot(db_path, "host-b").updated_at == "2026-01-01T00:00:00Z"
