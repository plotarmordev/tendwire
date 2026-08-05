"""Static and cutover gates for the fresh concern-owned store."""

from __future__ import annotations

import ast
import os
import sqlite3
from pathlib import Path

import pytest

from tendwire.store.db import store_status
from tendwire.store.retention import RetentionPolicy, run_retention_cycle
from tendwire.store.schema import STORE_SCHEMA_VERSION, StoreSchemaError, init_store


ROOT = Path(__file__).resolve().parents[1]
SCAN_ROOTS = (ROOT / "src", ROOT / "scripts", ROOT / "tests")
DELETED_MAINTENANCE = {
    "CompactionOptions",
    "compact_store",
    "run_store_maintenance",
    "maybe_run_automatic_store_maintenance",
    "compact_turn_change_journal",
    "exhaust_connector_retries",
}
EXPECTED_TABLES = {
    "turns",
    "turn_content_revisions",
    "turn_content_page_boundaries",
    "attention_items",
    "pending_interactions",
    "snapshots",
    "agent_events",
    "command_receipts",
    "turn_submissions",
    "turn_supersessions",
    "backend_pending",
    "backend_pending_claims",
    "worker_bindings",
    "backend_health",
    "connector_outbox",
    "connector_deliveries",
}


def _python_files() -> list[Path]:
    return sorted(path for root in SCAN_ROOTS for path in root.rglob("*.py"))


def test_no_deleted_store_module_import_patch_or_reexport() -> None:
    forbidden = "tendwire." + "store" + "." + "sqlite"
    short = "store" + "." + "sqlite"
    findings: list[str] = []
    for path in _python_files():
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                if any(alias.name == forbidden for alias in node.names):
                    findings.append(f"{path.relative_to(ROOT)}:{node.lineno}:import")
            elif isinstance(node, ast.ImportFrom):
                if node.module in {forbidden, short}:
                    findings.append(f"{path.relative_to(ROOT)}:{node.lineno}:from")
                if node.module == "tendwire.store" and any(
                    alias.name == "sqlite" for alias in node.names
                ):
                    findings.append(f"{path.relative_to(ROOT)}:{node.lineno}:attribute")
            elif isinstance(node, ast.Constant) and isinstance(node.value, str):
                if forbidden in node.value or short in node.value:
                    findings.append(f"{path.relative_to(ROOT)}:{node.lineno}:string")
    assert findings == []


def test_deleted_public_maintenance_symbols_have_no_production_caller() -> None:
    findings: list[str] = []
    for path in sorted((ROOT / "src").rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            called = node.func.id if isinstance(node.func, ast.Name) else (
                node.func.attr if isinstance(node.func, ast.Attribute) else None
            )
            if called in DELETED_MAINTENANCE:
                findings.append(f"{path.relative_to(ROOT)}:{node.lineno}:{called}")
    assert findings == []


def test_fresh_schema_has_only_the_approved_application_tables(tmp_path: Path) -> None:
    db_path = tmp_path / "fresh.db"
    init_store(db_path)
    with sqlite3.connect(db_path) as conn:
        version = conn.execute("PRAGMA user_version").fetchone()[0]
        tables = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
            )
        }
        integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]
    assert version == STORE_SCHEMA_VERSION
    assert tables == EXPECTED_TABLES
    assert integrity == "ok"


@pytest.mark.parametrize("old_version", [1, STORE_SCHEMA_VERSION - 1, STORE_SCHEMA_VERSION + 1])
def test_version_mismatch_requires_explicit_discard_acknowledgement(
    tmp_path: Path, caplog: pytest.LogCaptureFixture, old_version: int
) -> None:
    db_path = tmp_path / f"old-{old_version}.db"
    with sqlite3.connect(db_path) as conn:
        conn.execute("CREATE TABLE legacy_payload(secret TEXT)")
        conn.execute("INSERT INTO legacy_payload VALUES ('discard-me')")
        conn.execute(f"PRAGMA user_version={old_version}")
    with pytest.raises(StoreSchemaError):
        init_store(db_path)
    with sqlite3.connect(db_path) as conn:
        assert conn.execute("SELECT secret FROM legacy_payload").fetchone() == (
            "discard-me",
        )
        assert conn.execute("PRAGMA user_version").fetchone()[0] == old_version

    caplog.set_level("WARNING", logger="tendwire.store.schema")
    init_store(db_path, discard_incompatible=True)
    with sqlite3.connect(db_path) as conn:
        tables = {
            row[0]
            for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
        version = conn.execute("PRAGMA user_version").fetchone()[0]
    assert "legacy_payload" not in tables
    assert version == STORE_SCHEMA_VERSION
    warning = "\n".join(record.getMessage() for record in caplog.records)
    assert f"old_user_version={old_version}" in warning
    assert f"new_user_version={STORE_SCHEMA_VERSION}" in warning
    assert "tables=legacy_payload" in warning
    assert "aggregate_rows=1" in warning


def test_database_is_owner_only_and_health_is_read_only(tmp_path: Path) -> None:
    db_path = tmp_path / "secure.db"
    init_store(db_path)
    before = db_path.stat()
    health = store_status(db_path, "host-a")
    after = db_path.stat()
    assert os.stat(db_path).st_mode & 0o777 == 0o600
    assert health["schema_version"] == 1
    assert health["store_schema_version"] == STORE_SCHEMA_VERSION
    assert (after.st_dev, after.st_ino) == (before.st_dev, before.st_ino)


@pytest.mark.parametrize("extra_kind", ["trigger", "view"])
def test_same_version_extra_schema_object_requires_explicit_discard(
    tmp_path: Path, extra_kind: str
) -> None:
    db_path = tmp_path / f"extra-{extra_kind}.db"
    init_store(db_path)
    with sqlite3.connect(db_path) as conn:
        if extra_kind == "trigger":
            conn.execute(
                """CREATE TRIGGER unexpected_trigger AFTER INSERT ON snapshots
                BEGIN SELECT 1; END"""
            )
            object_name = "unexpected_trigger"
        else:
            conn.execute("CREATE VIEW unexpected_view AS SELECT host_id FROM snapshots")
            object_name = "unexpected_view"

    with pytest.raises(StoreSchemaError):
        init_store(db_path)
    with sqlite3.connect(db_path) as conn:
        assert conn.execute(
            "SELECT type FROM sqlite_schema WHERE name=?", (object_name,)
        ).fetchone() == (extra_kind,)

    init_store(db_path, discard_incompatible=True)
    with sqlite3.connect(db_path) as conn:
        assert conn.execute(
            "SELECT 1 FROM sqlite_schema WHERE name=?", (object_name,)
        ).fetchone() is None
        tables = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%'"
            )
        }
    assert tables == EXPECTED_TABLES


def test_retention_is_bounded_and_never_runs_vacuum(tmp_path: Path) -> None:
    db_path = tmp_path / "retention.db"
    init_store(db_path)
    result = run_retention_cycle(
        db_path,
        policy=RetentionPolicy(batch_size=7),
        now="2026-08-05T00:00:00.000000Z",
    )
    assert sum(
        value for key, value in result.items() if key != "checkpoint"
    ) == 0
    assert set(result["checkpoint"]) == {"busy", "log", "checkpointed"}
