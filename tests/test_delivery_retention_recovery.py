"""Retention preserves retained prefixes and effective recovery correlations."""

from __future__ import annotations

import sqlite3
from pathlib import Path

from tendwire.store.retention import RetentionPolicy, run_retention_cycle
from tendwire.store.schema import init_store


OLD = "2026-01-01T00:00:00.000000Z"


def test_old_delivered_prefix_is_retained_while_fresh_suffix_references_it(tmp_path: Path) -> None:
    db_path = tmp_path / "recovery.db"
    init_store(db_path)
    with sqlite3.connect(db_path) as conn:
        prefix = conn.execute(
            """INSERT INTO connector_outbox(host_id,connector,key,kind,payload_version,status,
            retry_generation,prior_attempt_count,payload_json,created_at,updated_at,available_at)
            VALUES('host-a','notice','prefix','generic',1,'delivered',1,0,'{}',?,?,?) RETURNING id""",
            (OLD, OLD, OLD),
        ).fetchone()[0]
        conn.execute(
            """INSERT INTO connector_outbox(host_id,connector,key,kind,payload_version,status,
            predecessor_outbox_id,retry_generation,prior_attempt_count,payload_json,created_at,updated_at,available_at)
            VALUES('host-a','notice','suffix','generic',1,'queued',?,1,0,'{}',?,?,?)""",
            (prefix, OLD, OLD, OLD),
        )
    result = run_retention_cycle(
        db_path,
        policy=RetentionPolicy(),
        now="2026-08-05T00:00:00Z",
    )
    assert result["outbox"] == 0
    with sqlite3.connect(db_path) as conn:
        assert [row[0] for row in conn.execute("SELECT key FROM connector_outbox ORDER BY id")] == ["prefix", "suffix"]
