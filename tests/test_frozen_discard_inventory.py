from __future__ import annotations

import json
import os
import re
import sqlite3
import stat
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / ".deploy/capture-frozen-discard-inventory.py"
_SCRIPT_SOURCE = SCRIPT.read_text(encoding="utf-8")
TRANSACTION_ID = re.search(r'^TRANSACTION_ID = "([^"]+)"$', _SCRIPT_SOURCE, re.MULTILINE).group(1)  # type: ignore[union-attr]
AUTHORIZATION = re.search(r'^AUTHORIZATION = "([^"]+)"$', _SCRIPT_SOURCE, re.MULTILINE).group(1)  # type: ignore[union-attr]
TENDWIRE_TABLES = {
    "agent_event_tombstones",
    "agent_events",
    "attention_items",
    "attention_lifecycles",
    "backend_health",
    "backend_pending",
    "backend_pending_claims",
    "command_receipts",
    "commands",
    "connector_deliveries",
    "connector_outbox",
    "events",
    "herdr_turn_completions",
    "herdr_turn_refresh_retries",
    "herdr_turn_watermarks",
    "pending_interactions",
    "snapshots",
    "spaces",
    "store_maintenance_cursors",
    "store_maintenance_state",
    "turn_change_floor",
    "turn_change_journal",
    "turn_change_state",
    "turn_content_page_boundaries",
    "turn_content_revisions",
    "turn_list_hosts",
    "turn_list_state",
    "turn_presentation_jobs",
    "turn_presentation_plans",
    "turn_presentation_recoveries",
    "turn_submissions",
    "turn_supersessions",
    "turns",
    "worker_bindings",
    "workers",
}
LEGACY_STATE_COLLECTIONS = {
    "archived_legacy_direct_panes",
    "archived_source_spaces",
    "cleanup_topic_attempts",
    "decisions",
    "deleted_duplicate_topics",
    "deleted_orphan_topics",
    "deleted_tendwire_source_panes",
    "last_topic_cleanup",
    "panes",
    "pruned_tendwire_source_panes",
    "spaces",
    "telegram",
    "telegram_dead_topic_ids",
    "telegram_deleted_topics",
    "telegram_message_bindings",
    "telegram_partial_final_deliveries",
    "telegram_topic_cleanup_audit",
    "tendwire_command_submissions",
    "tendwire_delta_sync",
    "tendwire_ingress_command_requests",
    "tendwire_outbox",
    "tendwire_source_delivered_turns",
    "tendwire_turn_final_source_owners",
    "tendwire_turn_jobs",
    "tendwire_worker_rebind_audit",
}
ARRAY_COLLECTIONS = {
    "archived_legacy_direct_panes",
    "archived_source_spaces",
    "deleted_duplicate_topics",
    "deleted_orphan_topics",
    "deleted_tendwire_source_panes",
    "pruned_tendwire_source_panes",
    "telegram_dead_topic_ids",
    "telegram_deleted_topics",
    "telegram_topic_cleanup_audit",
    "tendwire_worker_rebind_audit",
}


@dataclass(frozen=True)
class LegacyFixture:
    database: Path
    state: Path
    missing_ingress: Path
    output: Path


def _create_tendwire_v28(path: Path) -> None:
    status_columns = {
        "backend_pending": ("observation_state", "none"),
        "backend_pending_claims": ("state", "settled"),
        "command_receipts": ("state", "accepted"),
        "commands": ("state", "accepted"),
        "connector_deliveries": ("status", "delivered"),
        "connector_outbox": ("status", "delivered"),
        "herdr_turn_refresh_retries": ("status", "escalated"),
        "pending_interactions": ("status", "closed"),
        "turn_submissions": ("state", "linked"),
    }
    connection = sqlite3.connect(path)
    try:
        for table in sorted(TENDWIRE_TABLES):
            status = status_columns.get(table)
            if status is None:
                connection.execute(f"CREATE TABLE {table} (id INTEGER)")
                connection.execute(f"INSERT INTO {table} VALUES (1)")
                continue
            column, value = status
            connection.execute(
                f"CREATE TABLE {table} (id INTEGER, {column} TEXT)"
            )
            connection.execute(f"INSERT INTO {table} VALUES (1, ?)", (value,))
        connection.execute("PRAGMA user_version=28")
        connection.commit()
    finally:
        connection.close()


def _create_legacy_state(path: Path) -> None:
    collections: dict[str, object] = {
        name: ([{}] if name in ARRAY_COLLECTIONS else {"fixture": {}})
        for name in LEGACY_STATE_COLLECTIONS
    }
    collections.update(
        {
            "decisions": {"active": {}, "accepted_artifacts": {}},
            "panes": {
                "fixture": {
                    "live_in_snapshot": False,
                    "partial_final_delivery": None,
                    "status": "closed",
                }
            },
            "telegram_partial_final_deliveries": {
                "fixture": {"status": "resolved"}
            },
            "tendwire_command_submissions": {
                "fixture": {"status": "accepted"}
            },
            "tendwire_ingress_command_requests": {
                "fixture": {"request_phase": "terminal", "state": "terminal"}
            },
            "tendwire_turn_jobs": {
                "fixture": {"substate": "acknowledged"}
            },
        }
    )
    path.write_text(
        json.dumps({"version": 1, **collections}, sort_keys=True),
        encoding="utf-8",
    )


@pytest.fixture
def legacy_fixture(tmp_path: Path) -> LegacyFixture:
    database = tmp_path / "tendwire-v28.db"
    state = tmp_path / "state-v1.json"
    output = tmp_path / TRANSACTION_ID / "discard-inventory.json"
    _create_tendwire_v28(database)
    _create_legacy_state(state)
    return LegacyFixture(
        database=database,
        state=state,
        missing_ingress=tmp_path / "missing-ingress.db",
        output=output,
    )


def _run(
    fixture: LegacyFixture, *, authorization: str | None = None
) -> subprocess.CompletedProcess[str]:
    environment = os.environ.copy()
    environment.pop("ACP_CUTOVER_DISCARD_AUTHORIZATION", None)
    if authorization is not None:
        environment["ACP_CUTOVER_DISCARD_AUTHORIZATION"] = authorization
    return subprocess.run(
        [
            sys.executable,
            SCRIPT,
            "--tendwire-db",
            fixture.database,
            "--herdres-ingress",
            fixture.missing_ingress,
            "--herdres-state",
            fixture.state,
            "--output",
            fixture.output,
        ],
        capture_output=True,
        check=False,
        env=environment,
        text=True,
    )


def _report(fixture: LegacyFixture) -> dict[str, object]:
    return json.loads(fixture.output.read_text(encoding="utf-8"))


def test_v28_and_legacy_v1_inventory_counts_every_collection(
    legacy_fixture: LegacyFixture,
) -> None:
    result = _run(legacy_fixture)
    assert result.returncode == 0, result.stderr
    report = _report(legacy_fixture)

    assert report["policy"] == "completed_drain"
    assert report["in_flight_category_total"] == 0
    assert report["authorization"] == "not_required"
    assert report["tendwire"]["schema_version"] == 28
    assert set(report["tendwire"]["table_rows"]) == TENDWIRE_TABLES
    assert set(report["tendwire"]["table_rows"].values()) == {1}
    assert report["tendwire"]["aggregate_record_total"] == len(
        TENDWIRE_TABLES
    )
    assert report["herdres_state"]["version"] == 1
    assert report["herdres_state"]["schema_version_present"] is False
    assert set(report["herdres_state"]["collection_rows"]) == (
        LEGACY_STATE_COLLECTIONS
    )
    assert report["herdres_state"]["aggregate_record_total"] == 26
    assert report["discard_aggregate_record_total"] == 61
    assert report["count_semantics"] == (
        "aggregate_category_counts_not_unique_records"
    )


def test_missing_ingress_is_absent_and_is_not_created(
    legacy_fixture: LegacyFixture,
) -> None:
    result = _run(legacy_fixture)
    assert result.returncode == 0, result.stderr
    report = _report(legacy_fixture)

    assert report["herdres_ingress"] == {
        "aggregate_record_total": 0,
        "cursor_rows": 0,
        "in_flight_category_total": 0,
        "present": False,
        "request_rows": 0,
        "schema_version": None,
    }
    assert not legacy_fixture.missing_ingress.exists()


def test_unknown_status_requires_loud_discard_authorization(
    legacy_fixture: LegacyFixture,
) -> None:
    connection = sqlite3.connect(legacy_fixture.database)
    try:
        connection.execute("UPDATE commands SET state='future_unclassified_state'")
        connection.commit()
    finally:
        connection.close()

    result = _run(legacy_fixture)
    assert result.returncode == 1
    assert "unknown legacy status blocks discard classification" in result.stderr
    assert "future_unclassified_state" not in result.stderr
    assert not legacy_fixture.output.exists()


def test_loud_discard_requires_the_exact_explicit_authorization(
    legacy_fixture: LegacyFixture,
) -> None:
    connection = sqlite3.connect(legacy_fixture.database)
    try:
        connection.execute("UPDATE commands SET state='reserved'")
        connection.execute("UPDATE command_receipts SET state='reserved'")
        connection.execute(
            "UPDATE herdr_turn_refresh_retries SET status='pending'"
        )
        connection.commit()
    finally:
        connection.close()

    missing = _run(legacy_fixture)
    assert missing.returncode == 3
    assert _report(legacy_fixture)["authorization"] == "required"

    wrong = _run(legacy_fixture, authorization=f"{AUTHORIZATION}_typo")
    assert wrong.returncode == 3
    assert _report(legacy_fixture)["permitted"] is False

    exact = _run(legacy_fixture, authorization=AUTHORIZATION)
    assert exact.returncode == 0, exact.stderr
    report = _report(legacy_fixture)
    assert report["authorization"] == "explicit"
    assert report["permitted"] is True
    assert report["policy"] == "loud_discard"
    assert report["tendwire"]["in_flight"]["reserved_commands"] == 1
    assert report["tendwire"]["in_flight"]["reserved_receipts"] == 1
    assert report["tendwire"]["in_flight"]["pending_refreshes"] == 1


def test_report_is_mode_0600_and_contains_only_aggregate_fixture_data(
    legacy_fixture: LegacyFixture,
) -> None:
    secret = "private-fixture-value-that-must-not-leak"
    state = json.loads(legacy_fixture.state.read_text(encoding="utf-8"))
    state["operator_note"] = secret
    state["panes"] = {
        secret: {
            "live_in_snapshot": False,
            "pane_id": secret,
            "partial_final_delivery": None,
            "status": "closed",
        }
    }
    state["archived_legacy_direct_panes"] = [{"pane_id": secret}]
    legacy_fixture.state.write_text(json.dumps(state), encoding="utf-8")

    result = _run(legacy_fixture)
    assert result.returncode == 0, result.stderr
    body = legacy_fixture.output.read_text(encoding="utf-8")
    report = json.loads(body)
    assert secret not in body
    assert report["aggregate_counts_only"] is True
    assert stat.S_IMODE(legacy_fixture.output.stat().st_mode) == 0o600
