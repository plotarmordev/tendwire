#!/usr/bin/python3 -I
"""Write a privacy-safe inventory of the legacy state frozen for cutover."""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import stat
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


RELEASE_ID = "9026d9bc-7446533-3659994"
TRANSACTION_ID = "frozen-7446533-6c0d0f5"
AUTHORIZATION = "LOUD_DISCARD_7446533_6c0d0f5"
MAX_STATE_BYTES = 16_777_216
TENDWIRE_TABLES = (
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
)
LEGACY_STATE_COLLECTIONS = (
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
)


def _utc_now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _regular_owned(path: Path, *, maximum: int | None = None) -> None:
    info = path.lstat()
    if (
        not stat.S_ISREG(info.st_mode)
        or info.st_uid != os.geteuid()
        or info.st_nlink != 1
        or (info.st_mode & stat.S_IWOTH)
        or (maximum is not None and info.st_size > maximum)
    ):
        raise RuntimeError("unsafe inventory input")


def _connect_read_only(path: Path) -> sqlite3.Connection:
    _regular_owned(path)
    connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=10)
    connection.execute("PRAGMA query_only=ON")
    return connection


def _count(connection: sqlite3.Connection, sql: str) -> int:
    value = connection.execute(sql).fetchone()
    return int(value[0])


def _known_status_counts(
    connection: sqlite3.Connection,
    *,
    table: str,
    column: str,
    statuses: tuple[str, ...],
) -> dict[str, int]:
    # Keep values out of SQL text while retaining a fixed, public status vocabulary.
    values: dict[str, int] = {}
    for status in statuses:
        row = connection.execute(
            f"SELECT COUNT(*) FROM {table} WHERE {column}=?",  # noqa: S608 - fixed identifiers
            (status,),
        ).fetchone()
        values[status] = int(row[0])
    known = sum(values.values())
    values["other"] = _count(connection, f"SELECT COUNT(*) FROM {table}") - known
    return values


def _require_known_statuses(*counts: dict[str, int]) -> None:
    if any(values.get("other") != 0 for values in counts):
        raise RuntimeError("unknown legacy status blocks discard classification")


def _tendwire_inventory(path: Path) -> tuple[dict[str, Any], int]:
    connection = _connect_read_only(path)
    try:
        if _count(connection, "PRAGMA user_version") != 28:
            raise RuntimeError("unexpected Tendwire schema")
        present = {
            str(row[0])
            for row in connection.execute(
                "SELECT name FROM sqlite_master "
                "WHERE type='table' AND name NOT LIKE 'sqlite_%'"
            )
        }
        if present != set(TENDWIRE_TABLES):
            raise RuntimeError("unexpected Tendwire table inventory")
        tables = {
            name: _count(connection, f"SELECT COUNT(*) FROM {name}")
            for name in TENDWIRE_TABLES
        }
        commands = _known_status_counts(
            connection,
            table="commands",
            column="state",
            statuses=("reserved", "send_started", "accepted", "rejected", "uncertain"),
        )
        outbox = _known_status_counts(
            connection,
            table="connector_outbox",
            column="status",
            statuses=(
                "staged", "blocked", "queued", "leased", "retry", "deferred",
                "awaiting_ack", "delivered", "superseded", "dead_letter",
            ),
        )
        deliveries = _known_status_counts(
            connection,
            table="connector_deliveries",
            column="status",
            statuses=(
                "leased", "awaiting_ack", "acknowledged", "failed", "deferred",
                "released", "expired", "delivered", "superseded",
            ),
        )
        receipts = _known_status_counts(
            connection,
            table="command_receipts",
            column="state",
            statuses=("reserved", "send_started", "accepted", "rejected", "uncertain"),
        )
        submissions = _known_status_counts(
            connection,
            table="turn_submissions",
            column="state",
            statuses=(
                "send_started", "submitted", "linked", "ambiguous", "expired",
                "rejected", "uncertain", "cancelled",
            ),
        )
        pending = _known_status_counts(
            connection,
            table="backend_pending",
            column="observation_state",
            statuses=("none", "open", "pending", "resolved", "closed"),
        )
        claims = _known_status_counts(
            connection,
            table="backend_pending_claims",
            column="state",
            statuses=("claimed", "send_started", "settled"),
        )
        interactions = _known_status_counts(
            connection,
            table="pending_interactions",
            column="status",
            statuses=("open", "pending", "resolved", "closed"),
        )
        refreshes = _known_status_counts(
            connection,
            table="herdr_turn_refresh_retries",
            column="status",
            statuses=("pending", "retry", "processing", "completed", "failed", "escalated"),
        )
        _require_known_statuses(
            commands, outbox, deliveries, receipts, submissions, pending,
            claims, interactions, refreshes,
        )
    finally:
        connection.close()
    in_flight_categories = {
        "reserved_commands": commands["reserved"] + commands["send_started"],
        "reserved_receipts": receipts["reserved"] + receipts["send_started"],
        "pending_refreshes": (
            refreshes["pending"] + refreshes["retry"] + refreshes["processing"]
        ),
        "connector_outbox": sum(
            outbox[name]
            for name in (
                "staged",
                "blocked",
                "queued",
                "leased",
                "retry",
                "deferred",
                "awaiting_ack",
            )
        ),
        "connector_deliveries": (
            deliveries["leased"]
            + deliveries["awaiting_ack"]
            + deliveries["deferred"]
        ),
        "turn_submissions": (
            submissions["send_started"]
            + submissions["submitted"]
            + submissions["ambiguous"]
            + submissions["uncertain"]
        ),
        "backend_pending": pending["open"] + pending["pending"],
        "backend_pending_claims": claims["claimed"] + claims["send_started"],
        "pending_interactions": interactions["open"] + interactions["pending"],
    }
    in_flight = sum(in_flight_categories.values())
    return {
        "schema_version": 28,
        "table_rows": tables,
        "command_state": commands,
        "outbox_status": outbox,
        "delivery_status": deliveries,
        "receipt_state": receipts,
        "submission_state": submissions,
        "backend_pending_observation_state": pending,
        "pending_claim_state": claims,
        "interaction_status": interactions,
        "refresh_status": refreshes,
        "in_flight": in_flight_categories,
        "aggregate_record_total": sum(tables.values()),
        "in_flight_category_total": in_flight,
    }, in_flight


def _ingress_inventory(path: Path) -> tuple[dict[str, Any], int]:
    if not path.exists():
        if path.is_symlink():
            raise RuntimeError("unsafe inventory input")
        return {
            "present": False,
            "schema_version": None,
            "cursor_rows": 0,
            "request_rows": 0,
            "aggregate_record_total": 0,
            "in_flight_category_total": 0,
        }, 0
    connection = _connect_read_only(path)
    try:
        if _count(connection, "PRAGMA user_version") != 1:
            raise RuntimeError("unexpected Herdres ingress schema")
        states = _known_status_counts(
            connection,
            table="requests",
            column="state",
            statuses=("pending", "processing", "retry", "terminal", "quarantine"),
        )
        notices = _known_status_counts(
            connection,
            table="requests",
            column="notify_state",
            statuses=("none", "pending", "claimed", "sent"),
        )
        phases = _known_status_counts(
            connection,
            table="requests",
            column="local_phase",
            statuses=(
                "checkpointed", "state_applied", "provider_ready", "provider_applied",
                "markup_recorded",
            ),
        )
        null_phases = _count(connection, "SELECT COUNT(*) FROM requests WHERE local_phase IS NULL")
        phases["none"] = null_phases
        phases["other"] -= null_phases
        _require_known_statuses(states, notices, phases)
        cursors = _count(connection, "SELECT COUNT(*) FROM receiver_cursors")
        requests = _count(connection, "SELECT COUNT(*) FROM requests")
    finally:
        connection.close()
    in_flight = (
        states["pending"]
        + states["processing"]
        + states["retry"]
        + notices["pending"]
        + notices["claimed"]
        + phases["state_applied"]
        + phases["provider_ready"]
        + phases["provider_applied"]
    )
    return {
        "present": True,
        "schema_version": 1,
        "cursor_rows": cursors,
        "request_rows": requests,
        "request_state": states,
        "notice_state": notices,
        "local_phase": phases,
        "aggregate_record_total": cursors + requests,
        "in_flight_category_total": in_flight,
    }, in_flight


def _state_status_counts(
    rows: list[Any], statuses: tuple[str, ...], *, field: str = "status",
) -> dict[str, int]:
    result = {status: 0 for status in statuses}
    result["other"] = 0
    for row in rows:
        status = row.get(field) if isinstance(row, dict) else None
        result[status if status in result else "other"] += 1
    return result


def _mapping_rows(value: dict[str, Any], name: str) -> list[Any]:
    rows = value.get(name, {})
    if not isinstance(rows, dict):
        raise RuntimeError("unexpected Herdres state collection")
    return list(rows.values())


def _matching_rows(rows: list[Any], predicate: Any) -> int:
    return sum(1 for row in rows if isinstance(row, dict) and predicate(row))


def _state_inventory(path: Path) -> tuple[dict[str, Any], int]:
    _regular_owned(path, maximum=MAX_STATE_BYTES)
    raw = path.read_bytes()
    if len(raw) > MAX_STATE_BYTES:
        raise RuntimeError("Herdres state exceeds inventory bound")
    value = json.loads(raw)
    if (
        not isinstance(value, dict)
        or value.get("version") != 1
        or "schema_version" in value
    ):
        raise RuntimeError("unexpected Herdres state schema")
    present_collections = {
        name for name, rows in value.items() if isinstance(rows, (dict, list))
    }
    if present_collections != set(LEGACY_STATE_COLLECTIONS):
        raise RuntimeError("unexpected Herdres state collection inventory")
    collection_rows = {name: len(value[name]) for name in LEGACY_STATE_COLLECTIONS}
    panes = _mapping_rows(value, "panes")
    jobs = _mapping_rows(value, "tendwire_turn_jobs")
    partial_finals = _mapping_rows(value, "telegram_partial_final_deliveries")
    ingress_requests = _mapping_rows(value, "tendwire_ingress_command_requests")
    command_submissions = _mapping_rows(value, "tendwire_command_submissions")
    pane_status = _state_status_counts(
        panes, ("working", "idle", "closed", "unknown")
    )
    job_substate = _state_status_counts(
        jobs,
        (
            "reserved",
            "retryable",
            "telegram_applied",
            "old_slot_retired",
            "suppressed",
            "acknowledged",
            "failed",
        ),
        field="substate",
    )
    partial_status = _state_status_counts(
        partial_finals, ("held", "retry_authorized", "resolved")
    )
    ingress_state = _state_status_counts(
        ingress_requests,
        ("resolving", "retryable", "terminal", "quarantined"),
        field="state",
    )
    ingress_phase = _state_status_counts(
        ingress_requests,
        (
            "resolving",
            "ready",
            "retryable",
            "retry_authorized",
            "accepted_unverified",
            "queued",
            "terminal",
        ),
        field="request_phase",
    )
    ingress_phase["none"] = _matching_rows(
        ingress_requests, lambda row: row.get("request_phase") is None
    )
    ingress_phase["other"] -= ingress_phase["none"]
    command_status = _state_status_counts(
        command_submissions,
        (
            "reserved",
            "pending",
            "accepted",
            "duplicate_instruction",
            "nonzero_exit",
            "request_state_uncertain",
            "stale_target",
            "rejected",
        ),
    )
    _require_known_statuses(
        pane_status, job_substate, partial_status, ingress_state,
        ingress_phase, command_status,
    )
    decisions = value.get("decisions", {})
    if not isinstance(decisions, dict):
        raise RuntimeError("unexpected Herdres state collection")
    active_decisions = decisions.get("active", {})
    accepted_decision_artifacts = decisions.get("accepted_artifacts", {})
    if not isinstance(active_decisions, dict) or not isinstance(
        accepted_decision_artifacts, dict
    ):
        raise RuntimeError("unexpected Herdres decision collection")
    live_panes = _matching_rows(panes, lambda row: row.get("live_in_snapshot") is True)
    pane_partial_finals = _matching_rows(
        panes,
        lambda row: isinstance(row.get("partial_final_delivery"), dict)
        and row["partial_final_delivery"].get("status") in {"held", "retry_authorized"},
    )
    ingress_in_flight = _matching_rows(
        ingress_requests,
        lambda row: row.get("state") in {"resolving", "retryable"}
        or row.get("request_phase")
        in {
            "resolving",
            "ready",
            "retryable",
            "retry_authorized",
            "accepted_unverified",
            "queued",
        },
    )
    in_flight_categories = {
        "live_panes": live_panes,
        "turn_jobs": sum(
            job_substate[name]
            for name in (
                "reserved",
                "retryable",
                "telegram_applied",
                "old_slot_retired",
                "suppressed",
            )
        ),
        "held_partial_finals": partial_status["held"] + partial_status["retry_authorized"],
        "pane_partial_finals": pane_partial_finals,
        "ingress_requests": ingress_in_flight,
        "active_decisions": len(active_decisions),
        "accepted_decision_artifacts": len(accepted_decision_artifacts),
        "command_submissions": (
            command_status["reserved"]
            + command_status["pending"]
            + command_status["request_state_uncertain"]
        ),
    }
    in_flight = sum(in_flight_categories.values())
    return {
        "version": 1,
        "schema_version_present": False,
        "collection_rows": collection_rows,
        "pane_status": pane_status,
        "turn_job_substate": job_substate,
        "partial_final_status": partial_status,
        "ingress_request_state": ingress_state,
        "ingress_request_phase": ingress_phase,
        "command_submission_status": command_status,
        "decision_active_rows": len(active_decisions),
        "decision_accepted_artifact_rows": len(accepted_decision_artifacts),
        "in_flight": in_flight_categories,
        "aggregate_record_total": sum(collection_rows.values()),
        "in_flight_category_total": in_flight,
    }, in_flight


def _write_atomic(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    if path.parent.name != TRANSACTION_ID or path.name != "discard-inventory.json":
        raise RuntimeError("inventory output is not transaction-bound")
    temporary = path.with_name(f".{path.name}.tmp.{os.getpid()}")
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC
    descriptor = os.open(temporary, flags, 0o600)
    try:
        body = (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()
        view = memoryview(body)
        while view:
            written = os.write(descriptor, view)
            if written < 1:
                raise RuntimeError("inventory write failed")
            view = view[written:]
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    os.replace(temporary, path)
    directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--tendwire-db", type=Path, required=True)
    parser.add_argument("--herdres-ingress", type=Path, required=True)
    parser.add_argument("--herdres-state", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    tendwire, tendwire_in_flight = _tendwire_inventory(args.tendwire_db)
    ingress, ingress_in_flight = _ingress_inventory(args.herdres_ingress)
    state, state_in_flight = _state_inventory(args.herdres_state)
    in_flight = tendwire_in_flight + ingress_in_flight + state_in_flight
    discarded = (
        tendwire["aggregate_record_total"]
        + ingress["aggregate_record_total"]
        + state["aggregate_record_total"]
    )
    authorized = os.environ.get("ACP_CUTOVER_DISCARD_AUTHORIZATION", "") == AUTHORIZATION
    policy = "completed_drain" if in_flight == 0 else "loud_discard"
    permitted = in_flight == 0 or authorized
    report = {
        "schema_version": 1,
        "release_id": RELEASE_ID,
        "transaction_id": TRANSACTION_ID,
        "captured_at": _utc_now(),
        "policy": policy,
        "authorization": (
            "explicit"
            if authorized
            else "not_required"
            if in_flight == 0
            else "required"
        ),
        "permitted": permitted,
        "aggregate_counts_only": True,
        "count_semantics": "aggregate_category_counts_not_unique_records",
        "discard_aggregate_record_total": discarded,
        "in_flight_category_total": in_flight,
        "tendwire": tendwire,
        "herdres_ingress": ingress,
        "herdres_state": state,
    }
    _write_atomic(args.output, report)
    if permitted and policy == "loud_discard":
        print(
            f"LOUD_DISCARD authorized in_flight_category_total={in_flight} "
            f"discard_aggregate_record_total={discarded}"
        )
        return 0
    if permitted:
        print(
            f"COMPLETED_DRAIN in_flight_category_total=0 "
            f"discard_aggregate_record_total={discarded}"
        )
        return 0
    print(
        f"LOUD_DISCARD authorization_required in_flight_category_total={in_flight} "
        f"discard_aggregate_record_total={discarded}",
        file=sys.stderr,
    )
    return 3


if __name__ == "__main__":
    raise SystemExit(main())
