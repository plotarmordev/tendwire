"""Kind-aware retention of live work and terminal outbox rows."""

from __future__ import annotations

import sqlite3
from pathlib import Path

from tendwire.store.retention import RetentionPolicy, run_retention_cycle
from tendwire.store.schema import init_store


OLD = "2026-01-01T00:00:00.000000Z"
NOW = "2026-08-05T00:00:00.000000Z"


def _row(conn: sqlite3.Connection, key: str, status: str) -> int:
    return int(
        conn.execute(
            """INSERT INTO connector_outbox(
            host_id,connector,key,kind,payload_version,status,retry_generation,
            prior_attempt_count,payload_json,created_at,updated_at,available_at)
            VALUES('host-a','notice',?,'generic',1,?,1,0,'{}',?,?,?)
            RETURNING id""",
            (key, status, OLD, OLD, OLD),
        ).fetchone()[0]
    )


def test_retention_removes_old_unreferenced_terminal_rows_only(tmp_path: Path) -> None:
    db_path = tmp_path / "retention.db"
    init_store(db_path)
    with sqlite3.connect(db_path) as conn:
        _row(conn, "delivered", "delivered")
        _row(conn, "queued", "queued")
    result = run_retention_cycle(db_path, policy=RetentionPolicy(), now=NOW)
    assert result["outbox"] == 1
    with sqlite3.connect(db_path) as conn:
        assert conn.execute("SELECT key FROM connector_outbox").fetchall() == [("queued",)]


def test_live_lease_is_protected_regardless_of_age(tmp_path: Path) -> None:
    db_path = tmp_path / "retention.db"
    init_store(db_path)
    with sqlite3.connect(db_path) as conn:
        outbox_id = _row(conn, "leased", "queued")
        delivery_id = conn.execute(
            """INSERT INTO connector_deliveries(
            outbox_id,retry_generation,attempt,ref_hash,status,leased_at,leased_until,created_at)
            VALUES(?,1,1,'hash','leased',?,?,?) RETURNING id""",
            (outbox_id, OLD, "2027-01-01T00:00:00.000000Z", OLD),
        ).fetchone()[0]
        conn.execute(
            "UPDATE connector_outbox SET status='leased',current_delivery_id=? WHERE id=?",
            (delivery_id, outbox_id),
        )
    result = run_retention_cycle(db_path, policy=RetentionPolicy(), now=NOW)
    assert result["outbox"] == result["deliveries"] == 0
    with sqlite3.connect(db_path) as conn:
        assert conn.execute("SELECT status FROM connector_outbox").fetchone()[0] == "leased"


def test_referenced_terminal_parent_survives_child_first_retention(tmp_path: Path) -> None:
    db_path = tmp_path / "retention.db"
    init_store(db_path)
    with sqlite3.connect(db_path) as conn:
        parent = _row(conn, "parent", "delivered")
        child = _row(conn, "child", "queued")
        conn.execute(
            "UPDATE connector_outbox SET predecessor_outbox_id=? WHERE id=?",
            (parent, child),
        )
    result = run_retention_cycle(db_path, policy=RetentionPolicy(), now=NOW)
    assert result["outbox"] == 0


def test_age_retention_removes_all_old_terminal_outbox_and_deliveries(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "count-floor.db"
    init_store(db_path)
    with sqlite3.connect(db_path) as conn:
        for index in range(4):
            outbox_id = _row(conn, f"terminal-{index}", "delivered")
            conn.execute(
                """INSERT INTO connector_deliveries(
                outbox_id,retry_generation,attempt,ref_hash,status,
                leased_at,leased_until,created_at,settled_at)
                VALUES(?,1,1,?,'acknowledged',?,?,?,?)""",
                (outbox_id, f"hash-{index}", OLD, OLD, OLD, OLD),
            )

    result = run_retention_cycle(
        db_path,
        policy=RetentionPolicy(),
        now=NOW,
    )

    assert result["deliveries"] == 4
    assert result["outbox"] == 4
    with sqlite3.connect(db_path) as conn:
        assert conn.execute(
            "SELECT key FROM connector_outbox ORDER BY id"
        ).fetchall() == []
        assert conn.execute(
            "SELECT ref_hash FROM connector_deliveries ORDER BY id"
        ).fetchall() == []
