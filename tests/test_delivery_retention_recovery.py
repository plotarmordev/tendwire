"""Transactional recovery contracts for final-delivery retention."""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest

from tendwire.config import Config
from tendwire.connectors import ConnectorOutboxAPI
from tendwire.core.projector import project_from_raw
from tendwire.store import sqlite as store_sqlite
from tendwire.store.sqlite import (
    cleanup_acknowledged_final_retention,
    init_store,
    merge_turn_content,
    save_snapshot,
)


HOST_ID = "recovery-host"
FINAL_NAME = "turn-final"
CREATED_AT = "2026-01-01T00:00:00+00:00"
STABLE_KEY = "wsk1_" + ("d" * 64)
RECOVERY_RAW_SOURCE = "legacy-recovery-backend-source"
RECOVERY_LEGACY_SOURCE_TOKEN = "turnsrc-422ef48fec1cfb0720da05bd"
PRIVATE_ROUTE_SENTINEL = "PRIVATE-RECOVERY-ROUTE-SENTINEL"


def _insert_revision(
    db_path: Path,
    *,
    turn_id: str,
    final_text: str | None,
    user_text: str | None = None,
    user_state: str | None = None,
    final_state: str | None = None,
    created_at: str = CREATED_AT,
) -> str:
    init_store(db_path)
    state = user_state or ("complete" if user_text is not None else "absent")
    resolved_final_state = final_state or (
        "complete" if final_text is not None else "absent"
    )
    revision = store_sqlite.content_revision(
        turn_id,
        user_text,
        final_text,
        state,
        resolved_final_state,
    )
    user_segments = (
        store_sqlite.segment_canonical_text(user_text)
        if state == "complete" and user_text
        else []
    )
    final_segments = (
        store_sqlite.segment_canonical_text(final_text)
        if resolved_final_state == "complete" and final_text
        else []
    )
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute(
            """
            INSERT OR IGNORE INTO turns (
                host_id,
                turn_id,
                worker_id,
                worker_fingerprint,
                space_id,
                status,
                kind,
                updated_at,
                fingerprint,
                snapshot_content_fingerprint,
                observed_at,
                payload_json,
                list_sequence
            ) VALUES (
                ?, ?, 'worker-recovery', NULL, NULL, 'complete', 'turn',
                ?, ?, ?, ?, ?,
                (SELECT COALESCE(MAX(list_sequence), 0) + 1 FROM turns WHERE host_id = ?)
            )
            """,
            (
                HOST_ID,
                turn_id,
                created_at,
                f"fingerprint-{turn_id}",
                f"snapshot-{turn_id}",
                created_at,
                json.dumps(
                    {
                        "source_turn_id": f"source-{turn_id}",
                        "complete": True,
                        "meta": {
                            "stable_key": STABLE_KEY,
                            "stable_key_version": 1,
                        },
                    }
                ),
                HOST_ID,
            ),
        )
        conn.execute(
            """
            UPDATE turn_content_revisions
            SET is_current = 0, superseded_at = ?
            WHERE host_id = ? AND turn_id = ? AND is_current = 1
            """,
            (created_at, HOST_ID, turn_id),
        )
        conn.execute(
            """
            INSERT INTO turn_content_revisions (
                host_id,
                turn_id,
                content_revision,
                user_text,
                assistant_final_text,
                user_state,
                final_state,
                user_char_length,
                user_byte_length,
                final_char_length,
                final_byte_length,
                user_page_count,
                final_page_count,
                is_current,
                created_at,
                superseded_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, NULL)
            """,
            (
                HOST_ID,
                turn_id,
                revision,
                user_text,
                final_text,
                state,
                resolved_final_state,
                len(user_text or ""),
                len((user_text or "").encode("utf-8")),
                len(final_text or ""),
                len((final_text or "").encode("utf-8")),
                len(user_segments),
                len(final_segments),
                created_at,
            ),
        )
        for field, segments in (
            ("user_text", user_segments),
            ("assistant_final_text", final_segments),
        ):
            conn.executemany(
                """
                INSERT INTO turn_content_page_boundaries (
                    host_id,
                    turn_id,
                    content_revision,
                    field,
                    page_index,
                    start_char,
                    start_byte
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    (
                        HOST_ID,
                        turn_id,
                        revision,
                        field,
                        int(segment.index),
                        int(segment.start_char),
                        int(segment.start_byte),
                    )
                    for segment in segments
                ),
            )
    return revision


def _ensure_anchor(db_path: Path, *, turn_id: str, revision: str) -> int:
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute("PRAGMA foreign_keys = ON")
        anchor_id = store_sqlite._ensure_final_ready_anchor_conn(
            conn,
            host_id=HOST_ID,
            turn_id=turn_id,
            content_revision_value=revision,
            now=CREATED_AT,
        )
        assert anchor_id is not None
        return anchor_id


def _final_key(turn_id: str, revision: str) -> str:
    identity = store_sqlite.turn_final_delivery_identity(HOST_ID, turn_id, revision)
    return f"{FINAL_NAME}:revision:{identity}"


def _prepare_plan(
    api: ConnectorOutboxAPI,
    *,
    turn_id: str,
    revision: str,
    parts: list[list[dict[str, Any]]],
    version: str,
    source_ref: str | None = None,
) -> dict[str, Any]:
    begin_request: dict[str, Any] = {
        "schema_version": 1,
        "action": "begin",
        "name": FINAL_NAME,
        "turn_id": turn_id,
        "content_revision": revision,
        "presentation_version": version,
        "part_count": len(parts),
    }
    if source_ref is not None:
        begin_request["source_ref"] = source_ref
    begun = api.prepare(begin_request)
    assert begun["ok"] is True
    token = begun["plan_token"]
    for ordinal, spans in enumerate(parts):
        staged = api.prepare(
            {
                "schema_version": 1,
                "action": "part",
                "name": FINAL_NAME,
                "plan_token": token,
                "ordinal": ordinal,
                "spans": spans,
            }
        )
        assert staged["ok"] is True
    commit_request: dict[str, Any] = {
        "schema_version": 1,
        "action": "commit",
        "name": FINAL_NAME,
        "plan_token": token,
    }
    if source_ref is not None:
        commit_request["source_ref"] = source_ref
    return api.prepare(commit_request)


def _poll_one(api: ConnectorOutboxAPI) -> dict[str, Any]:
    result = api.poll({"name": FINAL_NAME, "limit": 100, "lease_seconds": 60})
    assert result["ok"] is True
    assert len(result["items"]) == 1
    return result["items"][0]


def _ack(api: ConnectorOutboxAPI, item: dict[str, Any]) -> None:
    result = api.ack({"name": FINAL_NAME, "ref": item["ref"]})
    assert result["ok"] is True
    assert result["status"] == "acknowledged"


def _final_span(start: int, end: int) -> dict[str, Any]:
    return {
        "field": "assistant_final_text",
        "start_char": start,
        "end_char": end,
    }


def _user_span(start: int, end: int) -> dict[str, Any]:
    return {"field": "user_text", "start_char": start, "end_char": end}


def _owner_snapshot(
    db_path: Path,
    *,
    worker_id: str,
    worker_name: str,
    space_id: str,
    second: int,
    stable_key: str = STABLE_KEY,
) -> Any:
    return project_from_raw(
        Config(host_id=HOST_ID, db_path=db_path),
        workers=[
            {
                "id": worker_id,
                "name": worker_name,
                "status": "active",
                "space_id": space_id,
                "terminal_id": PRIVATE_ROUTE_SENTINEL,
                "backend_target": {
                    "kind": "agent_id",
                    "value": PRIVATE_ROUTE_SENTINEL,
                    "sendable": True,
                },
                "meta": {
                    "stable_key": stable_key,
                    "stable_key_version": 1,
                    "chat_id": PRIVATE_ROUTE_SENTINEL,
                    "topic_id": PRIVATE_ROUTE_SENTINEL,
                },
            }
        ],
        timestamp=datetime(2026, 1, 1, 0, 0, second, tzinfo=timezone.utc),
    )


def _turn_graph_snapshot(db_path: Path, turn_id: str) -> dict[str, Any]:
    with sqlite3.connect(str(db_path)) as conn:
        turn_row = conn.execute(
            """
            SELECT payload_json, list_sequence
            FROM turns
            WHERE host_id = ? AND turn_id = ?
            """,
            (HOST_ID, turn_id),
        ).fetchone()
        assert turn_row is not None
        payload = json.loads(str(turn_row[0]))
        return {
            "turn_identity": (
                turn_id,
                payload.get("id"),
                payload.get("source_turn_id"),
                int(turn_row[1]),
            ),
            "revisions": conn.execute(
                """
                SELECT content_revision, user_state, final_state, is_current,
                       created_at, superseded_at
                FROM turn_content_revisions
                WHERE host_id = ? AND turn_id = ?
                ORDER BY content_revision
                """,
                (HOST_ID, turn_id),
            ).fetchall(),
            "boundaries": conn.execute(
                """
                SELECT content_revision, field, page_index, start_char, start_byte
                FROM turn_content_page_boundaries
                WHERE host_id = ? AND turn_id = ?
                ORDER BY content_revision, field, page_index
                """,
                (HOST_ID, turn_id),
            ).fetchall(),
            "plans": conn.execute(
                """
                SELECT id, plan_token, content_revision, presentation_version,
                       generation, part_count, state, replaces_plan_token,
                       recovers_plan_token, source_outbox_id
                FROM turn_presentation_plans
                WHERE host_id = ? AND turn_id = ?
                ORDER BY id
                """,
                (HOST_ID, turn_id),
            ).fetchall(),
            "jobs": conn.execute(
                """
                SELECT jobs.id, jobs.plan_id, jobs.sequence_index, jobs.operation,
                       jobs.part_ordinal, jobs.spans_json, jobs.outbox_id
                FROM turn_presentation_jobs AS jobs
                JOIN turn_presentation_plans AS plans ON plans.id = jobs.plan_id
                WHERE plans.host_id = ? AND plans.turn_id = ?
                ORDER BY jobs.id
                """,
                (HOST_ID, turn_id),
            ).fetchall(),
            "recoveries": conn.execute(
                """
                SELECT audit.id, audit.request_id, audit.failed_plan_id,
                       audit.recovered_plan_id, audit.failed_plan_token,
                       audit.recovered_plan_token, audit.generation,
                       audit.source_job_count, audit.delivered_prefix_count,
                       audit.fresh_job_count, audit.retained_failed_job_count,
                       audit.prior_attempt_count, audit.outcome
                FROM turn_presentation_recoveries AS audit
                JOIN turn_presentation_plans AS failed
                  ON failed.id = audit.failed_plan_id
                WHERE audit.host_id = ? AND failed.turn_id = ?
                ORDER BY audit.id
                """,
                (HOST_ID, turn_id),
            ).fetchall(),
            "outbox": conn.execute(
                """
                SELECT id, delivery_key, delivery_kind, turn_id,
                       content_revision, status, next_attempt_at
                FROM connector_outbox
                WHERE host_id = ? AND turn_id = ?
                ORDER BY id
                """,
                (HOST_ID, turn_id),
            ).fetchall(),
            "attempts": conn.execute(
                """
                SELECT deliveries.id, deliveries.outbox_id,
                       deliveries.delivery_key, deliveries.attempt,
                       deliveries.status, deliveries.created_at,
                       deliveries.delivered_at
                FROM connector_deliveries AS deliveries
                JOIN connector_outbox AS outbox ON outbox.id = deliveries.outbox_id
                WHERE outbox.host_id = ? AND outbox.turn_id = ?
                ORDER BY deliveries.id
                """,
                (HOST_ID, turn_id),
            ).fetchall(),
        }


@pytest.mark.parametrize(
    ("included_field", "span"),
    [
        ("assistant_final_text", _final_span(0, 8)),
        ("user_text", _user_span(0, 8)),
    ],
)
def test_exact_coverage_rejects_omission_of_either_complete_positive_field(
    tmp_path: Path,
    included_field: str,
    span: dict[str, Any],
) -> None:
    db_path = tmp_path / f"omitted-{included_field}.db"
    turn_id = f"turn-omitted-{included_field}"
    revision = _insert_revision(
        db_path,
        turn_id=turn_id,
        user_text="usertext",
        final_text="finaltxt",
    )
    api = ConnectorOutboxAPI(db_path, HOST_ID)

    committed = _prepare_plan(
        api,
        turn_id=turn_id,
        revision=revision,
        parts=[[span]],
        version=f"coverage-{included_field}",
    )

    assert committed["ok"] is False
    assert committed["status"] == "plan_incomplete"
    with sqlite3.connect(str(db_path)) as conn:
        assert conn.execute(
            "SELECT COUNT(*) FROM connector_outbox WHERE connector = ?",
            (FINAL_NAME,),
        ).fetchone()[0] == 0


def test_source_less_user_only_prepare_fails_closed_at_every_gate(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "user-only-prepare.db"
    turn_id = "turn-user-only-prepare"
    revision = _insert_revision(
        db_path,
        turn_id=turn_id,
        user_text="user only",
        final_text=None,
        final_state="absent",
    )
    api = ConnectorOutboxAPI(db_path, HOST_ID)
    begin = api.prepare(
        {
            "schema_version": 1,
            "action": "begin",
            "name": FINAL_NAME,
            "turn_id": turn_id,
            "content_revision": revision,
            "presentation_version": "user-only-v1",
            "part_count": 1,
        }
    )
    assert begin["ok"] is False
    assert begin["status"] == "content_known_incomplete"

    token = "twplan1.legacy-user-only"
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            """
            INSERT INTO turn_presentation_plans (
                host_id,
                name,
                plan_token,
                turn_id,
                content_revision,
                presentation_version,
                generation,
                part_count,
                state,
                created_at
            ) VALUES (?, ?, ?, ?, ?, 'legacy-user-only-v1', 1, 1, 'preparing', ?)
            """,
            (HOST_ID, FINAL_NAME, token, turn_id, revision, CREATED_AT),
        )
    part = api.prepare(
        {
            "schema_version": 1,
            "action": "part",
            "name": FINAL_NAME,
            "plan_token": token,
            "ordinal": 0,
            "spans": [_user_span(0, len("user only"))],
        }
    )
    commit = api.prepare(
        {
            "schema_version": 1,
            "action": "commit",
            "name": FINAL_NAME,
            "plan_token": token,
        }
    )
    assert part["ok"] is commit["ok"] is False
    assert part["status"] == commit["status"] == "content_known_incomplete"
    with sqlite3.connect(str(db_path)) as conn:
        assert conn.execute(
            "SELECT COUNT(*) FROM turn_presentation_jobs"
        ).fetchone()[0] == 0
        assert conn.execute(
            "SELECT COUNT(*) FROM connector_outbox WHERE connector = ?",
            (FINAL_NAME,),
        ).fetchone()[0] == 0


def test_interleaved_multichunk_user_and_final_ranges_commit_and_ack(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "interleaved-herdres.db"
    turn_id = "turn-interleaved-ranges"
    revision = _insert_revision(
        db_path,
        turn_id=turn_id,
        user_text="abcdefgh",
        final_text="ABCDEFGH",
    )
    api = ConnectorOutboxAPI(db_path, HOST_ID)

    committed = _prepare_plan(
        api,
        turn_id=turn_id,
        revision=revision,
        parts=[
            [_user_span(0, 4), _final_span(0, 4)],
            [_user_span(4, 8), _final_span(4, 8)],
        ],
        version="interleaved-v1",
    )

    assert committed["ok"] is True
    assert committed["state"] == "active"
    assert committed["job_count"] == 2
    for expected_sequence in (0, 1):
        item = _poll_one(api)
        assert item["payload"]["sequence_index"] == expected_sequence
        assert [span["field"] for span in item["payload"]["spans"]] == [
            "user_text",
            "assistant_final_text",
        ]
        _ack(api, item)
    with sqlite3.connect(str(db_path)) as conn:
        assert conn.execute(
            "SELECT state FROM turn_presentation_plans WHERE plan_token = ?",
            (committed["plan_token"],),
        ).fetchone()[0] == "completed"


def test_recovered_completion_releases_source_and_retention_without_replay(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "recovered-retention.db"
    turn_id = "turn-recovered-retention"
    revision = _insert_revision(db_path, turn_id=turn_id, final_text="abcdefghijkl")
    _ensure_anchor(db_path, turn_id=turn_id, revision=revision)
    api = ConnectorOutboxAPI(db_path, HOST_ID, max_attempts=1)
    source = _poll_one(api)
    root = _prepare_plan(
        api,
        turn_id=turn_id,
        revision=revision,
        parts=[[_final_span(0, 4)], [_final_span(4, 8)], [_final_span(8, 12)]],
        version="recovered-retention-v1",
        source_ref=source["ref"],
    )
    assert root["state"] == "active"
    first = _poll_one(api)
    _ack(api, first)
    failed = _poll_one(api)
    assert api.fail(
        {"name": FINAL_NAME, "ref": failed["ref"], "delay_seconds": 0}
    )["status"] == "attempts_exhausted"

    recovered = api.prepare(
        {
            "schema_version": 1,
            "action": "recover",
            "name": FINAL_NAME,
            "failed_plan_token": root["plan_token"],
            "request_id": "recover-retention-root",
        }
    )
    assert recovered["ok"] is True
    assert recovered["prior_attempt_count"] == 2
    for expected_sequence in (1, 2):
        item = _poll_one(api)
        assert item["payload"]["sequence_index"] == expected_sequence
        _ack(api, item)
    assert api.poll({"name": FINAL_NAME, "limit": 100})["items"] == []

    with sqlite3.connect(str(db_path)) as conn:
        root_state = conn.execute(
            "SELECT state FROM turn_presentation_plans WHERE plan_token = ?",
            (root["plan_token"],),
        ).fetchone()[0]
        root_jobs = conn.execute(
            """
            SELECT outbox.status
            FROM turn_presentation_jobs AS jobs
            JOIN turn_presentation_plans AS plans ON plans.id = jobs.plan_id
            JOIN connector_outbox AS outbox ON outbox.id = jobs.outbox_id
            WHERE plans.plan_token = ?
            ORDER BY jobs.sequence_index
            """,
            (root["plan_token"],),
        ).fetchall()
        recovered_state = conn.execute(
            "SELECT state FROM turn_presentation_plans WHERE plan_token = ?",
            (recovered["plan_token"],),
        ).fetchone()[0]
        source_state = conn.execute(
            "SELECT status FROM connector_outbox WHERE delivery_key = ?",
            (source["key"],),
        ).fetchone()[0]
        source_attempts = conn.execute(
            """
            SELECT COUNT(*)
            FROM connector_deliveries AS deliveries
            JOIN connector_outbox AS outbox ON outbox.id = deliveries.outbox_id
            WHERE outbox.delivery_key = ?
            """,
            (source["key"],),
        ).fetchone()[0]
    assert root_state == "superseded"
    assert root_jobs == [("delivered",)]
    assert recovered_state == "completed"
    assert source_state == "delivered"
    assert source_attempts == 1

    cleanup = cleanup_acknowledged_final_retention(
        db_path,
        HOST_ID,
        acknowledged_final_retention_days=1,
        acknowledged_final_retention_count=1,
        batch_size=100,
        now="2099-01-01T00:00:00+00:00",
    )
    assert cleanup["ok"] is True
    assert cleanup["deleted"] == 1


def test_repeated_recovery_history_is_hard_bounded_and_cumulative(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "bounded-recovery.db"
    turn_id = "turn-bounded-recovery"
    revision = _insert_revision(db_path, turn_id=turn_id, final_text="abcdefgh")
    _ensure_anchor(db_path, turn_id=turn_id, revision=revision)
    api = ConnectorOutboxAPI(db_path, HOST_ID, max_attempts=1)
    source = _poll_one(api)
    root = _prepare_plan(
        api,
        turn_id=turn_id,
        revision=revision,
        parts=[[_final_span(0, 4)], [_final_span(4, 8)]],
        version="bounded-recovery-v1",
        source_ref=source["ref"],
    )
    prefix = _poll_one(api)
    _ack(api, prefix)
    suffix = _poll_one(api)
    assert api.fail(
        {"name": FINAL_NAME, "ref": suffix["ref"], "delay_seconds": 0}
    )["status"] == "attempts_exhausted"

    failed_token = root["plan_token"]
    final_generation = store_sqlite._PRESENTATION_RECOVERY_HISTORY_LIMIT + 4
    latest: dict[str, Any] | None = None
    for generation in range(2, final_generation + 1):
        latest = api.prepare(
            {
                "schema_version": 1,
                "action": "recover",
                "name": FINAL_NAME,
                "failed_plan_token": failed_token,
                "request_id": f"bounded-recovery-{generation}",
            }
        )
        assert latest["ok"] is True
        assert latest["generation"] == generation
        assert latest["prior_attempt_count"] == generation
        item = _poll_one(api)
        assert item["payload"]["sequence_index"] == 1
        assert item["payload"]["predecessor_job_key"] == prefix["key"]
        if generation == final_generation:
            _ack(api, item)
        else:
            assert api.fail(
                {"name": FINAL_NAME, "ref": item["ref"], "delay_seconds": 0}
            )["status"] == "attempts_exhausted"
            failed_token = latest["plan_token"]
    assert latest is not None
    assert api.poll({"name": FINAL_NAME, "limit": 100})["items"] == []

    with sqlite3.connect(str(db_path)) as conn:
        counts = {
            "audits": conn.execute(
                "SELECT COUNT(*) FROM turn_presentation_recoveries"
            ).fetchone()[0],
            "plans": conn.execute(
                "SELECT COUNT(*) FROM turn_presentation_plans"
            ).fetchone()[0],
            "jobs": conn.execute(
                "SELECT COUNT(*) FROM turn_presentation_jobs"
            ).fetchone()[0],
            "parts": conn.execute(
                "SELECT COUNT(*) FROM connector_outbox WHERE delivery_kind = 'final_part'"
            ).fetchone()[0],
            "deliveries": conn.execute(
                "SELECT COUNT(*) FROM connector_deliveries"
            ).fetchone()[0],
        }
        newest_audit = conn.execute(
            """
            SELECT generation, delivered_prefix_count, retained_failed_job_count,
                   prior_attempt_count
            FROM turn_presentation_recoveries
            ORDER BY generation DESC
            LIMIT 1
            """
        ).fetchone()
        plan_rows = conn.execute(
            "SELECT generation, part_count, state FROM turn_presentation_plans ORDER BY generation"
        ).fetchall()
        source_status = conn.execute(
            "SELECT status FROM connector_outbox WHERE delivery_key = ?",
            (source["key"],),
        ).fetchone()[0]
    assert counts == {
        "audits": store_sqlite._PRESENTATION_RECOVERY_HISTORY_LIMIT,
        "plans": store_sqlite._PRESENTATION_RECOVERY_HISTORY_LIMIT + 1,
        "jobs": 1,
        "parts": 2,
        "deliveries": 3,
    }
    assert newest_audit == (final_generation, 1, final_generation - 1, final_generation)
    assert [row[0] for row in plan_rows] == list(
        range(
            final_generation - store_sqlite._PRESENTATION_RECOVERY_HISTORY_LIMIT,
            final_generation + 1,
        )
    )
    assert {row[1] for row in plan_rows} == {2}
    assert plan_rows[-1][2] == "completed"
    assert source_status == "delivered"


def _seed_v10_recovered_lineage(db_path: Path) -> tuple[str, str, str]:
    turn_id = "turn-v10-recovered"
    revision = _insert_revision(db_path, turn_id=turn_id, final_text="abcdefghijkl")
    failed_token = "twplan1.legacy-failed"
    recovered_token = "twplan1.legacy-recovered"
    authoritative_route = {
        "schema_version": 2,
        "turn_id": turn_id,
        "content_revision": revision,
        "final_identity": store_sqlite.turn_final_delivery_identity(
            HOST_ID,
            turn_id,
            revision,
        ),
        "stable_key": STABLE_KEY,
        "stable_key_version": 1,
    }
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute(
            """
            UPDATE turns
            SET worker_id = 'worker-recovery-a',
                worker_fingerprint = 'fingerprint-recovery-a',
                space_id = 'space-recovery-a',
                payload_json = ?
            WHERE host_id = ? AND turn_id = ?
            """,
            (
                json.dumps(
                    {
                        "schema_version": 1,
                        "id": turn_id,
                        "host_id": HOST_ID,
                        "worker_id": "worker-recovery-a",
                        "worker_fingerprint": "fingerprint-recovery-a",
                        "space_id": "space-recovery-a",
                        "status": "complete",
                        "kind": "turn",
                        "source": "herdr-a",
                        "source_turn_id": RECOVERY_LEGACY_SOURCE_TOKEN,
                        "complete": True,
                        "has_open_turn": False,
                        "updated_at": CREATED_AT,
                        "meta": {
                            "stable_key": STABLE_KEY,
                            "stable_key_version": 1,
                        },
                        "chat_id": PRIVATE_ROUTE_SENTINEL,
                    },
                    sort_keys=True,
                ),
                HOST_ID,
                turn_id,
            ),
        )
        failed_cursor = conn.execute(
            """
            INSERT INTO turn_presentation_plans (
                host_id, name, plan_token, turn_id, content_revision,
                presentation_version, generation, part_count, state,
                created_at, activated_at
            ) VALUES (?, ?, ?, ?, ?, 'legacy-recovery-v10', 1, 3, 'failed', ?, ?)
            """,
            (HOST_ID, FINAL_NAME, failed_token, turn_id, revision, CREATED_AT, CREATED_AT),
        )
        failed_plan_id = int(failed_cursor.lastrowid)
        recovered_cursor = conn.execute(
            """
            INSERT INTO turn_presentation_plans (
                host_id, name, plan_token, turn_id, content_revision,
                presentation_version, generation, part_count, state,
                replaces_plan_token, recovers_plan_token,
                created_at, activated_at, completed_at
            ) VALUES (
                ?, ?, ?, ?, ?, 'legacy-recovery-v10', 2, 3, 'completed',
                ?, ?, ?, ?, ?
            )
            """,
            (
                HOST_ID,
                FINAL_NAME,
                recovered_token,
                turn_id,
                revision,
                failed_token,
                failed_token,
                CREATED_AT,
                CREATED_AT,
                CREATED_AT,
            ),
        )
        recovered_plan_id = int(recovered_cursor.lastrowid)

        def add_job(
            plan_id: int,
            sequence: int,
            status: str,
            key: str,
        ) -> None:
            outbox_cursor = conn.execute(
                """
                INSERT INTO connector_outbox (
                    host_id, connector, delivery_key, status, payload_json,
                    private_state_json, created_at, updated_at, next_attempt_at
                ) VALUES (?, ?, ?, ?, ?, '{}', ?, ?, NULL)
                """,
                (
                    HOST_ID,
                    FINAL_NAME,
                    key,
                    status,
                    json.dumps({"turn": authoritative_route}, sort_keys=True),
                    CREATED_AT,
                    CREATED_AT,
                ),
            )
            outbox_id = int(outbox_cursor.lastrowid)
            spans_json = json.dumps(
                [
                    {
                        "field": "assistant_final_text",
                        "start_char": sequence * 4,
                        "end_char": (sequence + 1) * 4,
                    }
                ],
                sort_keys=True,
            )
            conn.execute(
                """
                INSERT INTO turn_presentation_jobs (
                    plan_id, sequence_index, operation, part_ordinal,
                    spans_json, outbox_id, created_at
                ) VALUES (?, ?, 'upsert', ?, ?, ?, ?)
                """,
                (
                    plan_id,
                    sequence,
                    sequence,
                    spans_json,
                    outbox_id,
                    CREATED_AT,
                ),
            )
            if status in {"delivered", "dead_letter"}:
                conn.execute(
                    """
                    INSERT INTO connector_deliveries (
                        outbox_id, host_id, connector, delivery_key, attempt,
                        status, response_json, private_state_json,
                        created_at, delivered_at
                    ) VALUES (?, ?, ?, ?, 1, ?, '{}', '{}', ?, ?)
                    """,
                    (
                        outbox_id,
                        HOST_ID,
                        FINAL_NAME,
                        key,
                        "delivered" if status == "delivered" else "failed",
                        CREATED_AT,
                        CREATED_AT,
                    ),
                )

        add_job(failed_plan_id, 0, "delivered", "turn-final:legacy-root:000000")
        add_job(failed_plan_id, 1, "dead_letter", "turn-final:legacy-root:000001")
        add_job(failed_plan_id, 2, "queued", "turn-final:legacy-root:000002")
        add_job(recovered_plan_id, 1, "delivered", "turn-final:legacy-recovered:000001")
        add_job(recovered_plan_id, 2, "delivered", "turn-final:legacy-recovered:000002")
        conn.execute(
            """
            INSERT INTO turn_presentation_recoveries (
                host_id, name, request_id, failed_plan_id, recovered_plan_id,
                failed_plan_token, recovered_plan_token, generation,
                source_job_count, delivered_prefix_count, fresh_job_count,
                retained_failed_job_count, prior_attempt_count, outcome, created_at
            ) VALUES (?, ?, 'legacy-request', ?, ?, ?, ?, 2, 3, 1, 2, 1, 2,
                      'recovered', ?)
            """,
            (
                HOST_ID,
                FINAL_NAME,
                failed_plan_id,
                recovered_plan_id,
                failed_token,
                recovered_token,
                CREATED_AT,
            ),
        )
        conn.execute("PRAGMA user_version = 10")
    return turn_id, revision, failed_token


def test_v12_migration_uses_effective_recovery_lineage_without_repost_or_hold(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "v12-recovered-lineage.db"
    turn_id, revision, failed_token = _seed_v10_recovered_lineage(db_path)

    init_store(db_path)

    key = _final_key(turn_id, revision)
    api = ConnectorOutboxAPI(db_path, HOST_ID)
    assert api.poll({"name": FINAL_NAME, "limit": 100})["items"] == []
    with sqlite3.connect(str(db_path)) as conn:
        assert conn.execute("PRAGMA user_version").fetchone()[0] == store_sqlite.STORE_SCHEMA_VERSION == 21
        anchor = conn.execute(
            """
            SELECT delivery_kind, status
            FROM connector_outbox
            WHERE delivery_key = ?
            """,
            (key,),
        ).fetchone()
        failed_state = conn.execute(
            "SELECT state FROM turn_presentation_plans WHERE plan_token = ?",
            (failed_token,),
        ).fetchone()[0]
        linked_proof = conn.execute(
            """
            SELECT plans.state
            FROM turn_presentation_plans AS plans
            JOIN connector_outbox AS source ON source.id = plans.source_outbox_id
            WHERE source.delivery_key = ?
            """,
            (key,),
        ).fetchone()[0]
    assert anchor == ("final_ready", "delivered")
    assert failed_state == "superseded"
    assert linked_proof == "completed"

    cleanup = cleanup_acknowledged_final_retention(
        db_path,
        HOST_ID,
        acknowledged_final_retention_days=1,
        acknowledged_final_retention_count=1,
        batch_size=100,
        now="2099-01-01T00:00:00+00:00",
    )
    assert cleanup["deleted"] == 1


def test_v12_recovery_history_stays_immutable_when_observed_turn_arrives(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "v12-recovered-lineage-owner-churn.db"
    turn_id, revision, failed_token = _seed_v10_recovered_lineage(db_path)
    init_store(db_path)

    original_key = _final_key(turn_id, revision)
    before = _turn_graph_snapshot(db_path, turn_id)
    assert before["turn_identity"] == (
        turn_id,
        turn_id,
        RECOVERY_LEGACY_SOURCE_TOKEN,
        1,
    )
    assert len(before["attempts"]) == 3
    assert [row[4] for row in before["attempts"]] == [
        "delivered",
        "delivered",
        "delivered",
    ]
    recovered_plan = next(row for row in before["plans"] if row[6] == "completed")
    root = next(row for row in before["outbox"] if row[1] == original_key)
    assert recovered_plan[9] == root[0]
    assert before["recoveries"][0][4:6] == (
        failed_token,
        recovered_plan[1],
    )

    worker_b = _owner_snapshot(
        db_path,
        worker_id="worker-recovery-b",
        worker_name="Recovery Worker B",
        space_id="space-recovery-b",
        second=2,
    )
    assert save_snapshot(db_path, worker_b) is True
    assert merge_turn_content(
        db_path,
        HOST_ID,
        "worker-recovery-b",
        {
            "source_turn_id": RECOVERY_RAW_SOURCE,
            "assistant_final_text": "abcdefghijkl",
            "complete": True,
            "has_open_turn": False,
        },
        observed_at="2026-01-01T00:00:03+00:00",
    ) == 1

    after_churn = _turn_graph_snapshot(db_path, turn_id)
    assert after_churn == before
    api = ConnectorOutboxAPI(db_path, HOST_ID)
    observed_items = api.poll({"name": FINAL_NAME, "limit": 100})["items"]
    assert len(observed_items) == 1
    assert observed_items[0]["key"] != original_key
    with sqlite3.connect(str(db_path)) as conn:
        current = conn.execute(
            """
            SELECT worker_id, worker_fingerprint, space_id, payload_json
            FROM turns
            WHERE host_id = ? AND turn_id = ?
            """,
            (HOST_ID, turn_id),
        ).fetchone()
        assert current is not None
        current_payload = json.loads(str(current[3]))
        assert current[:3] == (
            "worker-recovery-a",
            "fingerprint-recovery-a",
            "space-recovery-a",
        )
        assert current_payload["id"] == turn_id
        assert current_payload["source_turn_id"] == RECOVERY_LEGACY_SOURCE_TOKEN
        observed = conn.execute(
            """
            SELECT turn_id, worker_id, worker_fingerprint, space_id, payload_json
            FROM turns
            WHERE host_id = ? AND turn_id != ?
            """,
            (HOST_ID, turn_id),
        ).fetchone()
        assert observed is not None
        observed_payload = json.loads(str(observed[4]))
        assert tuple(observed[1:4]) == (
            "worker-recovery-b",
            worker_b.workers[0].fingerprint,
            "space-recovery-b",
        )
        assert observed_payload["id"] == str(observed[0])
        assert observed_payload["source_turn_id"] != RECOVERY_LEGACY_SOURCE_TOKEN
        public_payloads = [
            str(row[0])
                for row in conn.execute(
                    """
                    SELECT payload_json FROM turns
                    WHERE host_id = ? AND turn_id != ?
                    UNION ALL
                    SELECT payload_json FROM connector_outbox
                    WHERE host_id = ? AND turn_id != ?
                    """,
                    (HOST_ID, turn_id, HOST_ID, turn_id),
                ).fetchall()
        ]
        assert conn.execute("PRAGMA foreign_key_check").fetchall() == []
    encoded_public = "\n".join(public_payloads)
    assert PRIVATE_ROUTE_SENTINEL not in encoded_public
    assert RECOVERY_RAW_SOURCE not in encoded_public

    init_store(db_path)
    assert save_snapshot(db_path, worker_b) is True
    assert merge_turn_content(
        db_path,
        HOST_ID,
        "worker-recovery-b",
        {
            "source_turn_id": RECOVERY_RAW_SOURCE,
            "assistant_final_text": "abcdefghijkl",
            "complete": True,
            "has_open_turn": False,
        },
        observed_at="2026-01-01T00:00:03+00:00",
    ) == 0
    assert ConnectorOutboxAPI(db_path, HOST_ID).poll(
        {"name": FINAL_NAME, "limit": 100}
    )["items"] == []
    assert _turn_graph_snapshot(db_path, turn_id) == before


def test_known_incomplete_final_is_hold_then_complete_revision_drains(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "known-incomplete-hold.db"
    turn_id = "turn-known-incomplete-hold"
    incomplete_revision = _insert_revision(
        db_path,
        turn_id=turn_id,
        user_text="partial",
        user_state="known_incomplete",
        final_text="complete final",
    )
    _ensure_anchor(db_path, turn_id=turn_id, revision=incomplete_revision)
    incomplete_key = _final_key(turn_id, incomplete_revision)
    api = ConnectorOutboxAPI(db_path, HOST_ID)

    assert api.poll({"name": FINAL_NAME, "limit": 100})["items"] == []
    inspected = api.inspect(
        {"schema_version": 1, "name": FINAL_NAME, "status": "dead_letter", "limit": 10}
    )
    assert inspected["total"] == 1
    assert inspected["items"][0]["key"] == incomplete_key
    retry = api.retry({"schema_version": 1, "name": FINAL_NAME, "key": incomplete_key})
    assert retry["ok"] is False
    assert retry["status"] == "not_retryable"
    with sqlite3.connect(str(db_path)) as conn:
        assert conn.execute(
            """
            SELECT delivery_kind, status
            FROM connector_outbox
            WHERE delivery_key = ?
            """,
            (incomplete_key,),
        ).fetchone() == ("final_migration_hold", "dead_letter")
        assert conn.execute(
            "SELECT COUNT(*) FROM connector_outbox WHERE delivery_kind = 'final_ready'"
        ).fetchone()[0] == 0

    complete_revision = _insert_revision(
        db_path,
        turn_id=turn_id,
        user_text="complete user",
        final_text="complete final",
        created_at="2026-01-02T00:00:00+00:00",
    )
    _ensure_anchor(db_path, turn_id=turn_id, revision=complete_revision)
    source = _poll_one(api)
    assert source["payload"]["content_revision"] == complete_revision
    committed = _prepare_plan(
        api,
        turn_id=turn_id,
        revision=complete_revision,
        parts=[[
            _user_span(0, len("complete user")),
            _final_span(0, len("complete final")),
        ]],
        version="complete-after-hold-v1",
        source_ref=source["ref"],
    )
    assert committed["state"] == "active"
    _ack(api, _poll_one(api))
    assert api.poll({"name": FINAL_NAME, "limit": 100})["items"] == []
    with sqlite3.connect(str(db_path)) as conn:
        rows = conn.execute(
            """
            SELECT content_revision, delivery_kind, status
            FROM connector_outbox
            WHERE delivery_key IN (?, ?)
            ORDER BY content_revision
            """,
            (incomplete_key, source["key"]),
        ).fetchall()
    assert {tuple(row) for row in rows} == {
        (incomplete_revision, "final_migration_hold", "superseded"),
        (complete_revision, "final_ready", "delivered"),
    }


def test_stale_hold_retry_is_stably_rejected_and_cannot_block_current(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "stale-hold-retry.db"
    turn_id = "turn-stale-hold-retry"
    old_revision = _insert_revision(
        db_path,
        turn_id=turn_id,
        user_text="old user",
        final_text="old final",
    )
    old_key = _final_key(turn_id, old_revision)
    with sqlite3.connect(str(db_path)) as conn:
        payload = store_sqlite._final_ready_payload_conn(
            conn,
            host_id=HOST_ID,
            turn_id=turn_id,
            content_revision_value=old_revision,
        )
        assert payload is not None
        conn.execute(
            """
            INSERT INTO connector_outbox (
                host_id, connector, delivery_key, delivery_kind,
                turn_id, content_revision, status, payload_json,
                private_state_json, created_at, updated_at, next_attempt_at
            ) VALUES (?, ?, ?, 'final_migration_hold', ?, ?, 'dead_letter', ?, '{}', ?, ?, NULL)
            """,
            (
                HOST_ID,
                FINAL_NAME,
                old_key,
                turn_id,
                old_revision,
                json.dumps(payload, sort_keys=True),
                CREATED_AT,
                CREATED_AT,
            ),
        )
    current_revision = _insert_revision(
        db_path,
        turn_id=turn_id,
        user_text="current user",
        final_text="current final",
        created_at="2026-01-02T00:00:00+00:00",
    )
    api = ConnectorOutboxAPI(db_path, HOST_ID)

    first_retry = api.retry({"schema_version": 1, "name": FINAL_NAME, "key": old_key})
    repeated_retry = api.retry({"schema_version": 1, "name": FINAL_NAME, "key": old_key})
    assert first_retry["ok"] is repeated_retry["ok"] is False
    assert first_retry["status"] == repeated_retry["status"] == "stale_revision"
    with sqlite3.connect(str(db_path)) as conn:
        assert conn.execute(
            "SELECT status FROM connector_outbox WHERE delivery_key = ?",
            (old_key,),
        ).fetchone()[0] == "superseded"

    _ensure_anchor(db_path, turn_id=turn_id, revision=current_revision)
    current = _poll_one(api)
    assert current["key"] == _final_key(turn_id, current_revision)
    assert current["payload"]["content_revision"] == current_revision


def _set_current_revision(
    db_path: Path,
    *,
    turn_id: str,
    revision: str,
    changed_at: str,
) -> None:
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            """
            UPDATE turn_content_revisions
            SET is_current = 0, superseded_at = ?
            WHERE host_id = ? AND turn_id = ? AND is_current = 1
            """,
            (changed_at, HOST_ID, turn_id),
        )
        conn.execute(
            """
            UPDATE turn_content_revisions
            SET is_current = 1, superseded_at = NULL
            WHERE host_id = ? AND turn_id = ? AND content_revision = ?
            """,
            (HOST_ID, turn_id, revision),
        )


def _deliver_one_part_source(
    api: ConnectorOutboxAPI,
    *,
    source: dict[str, Any],
    turn_id: str,
    revision: str,
    final_length: int,
    version: str,
) -> tuple[dict[str, Any], str]:
    committed = _prepare_plan(
        api,
        turn_id=turn_id,
        revision=revision,
        parts=[[_final_span(0, final_length)]],
        version=version,
        source_ref=source["ref"],
    )
    part = _poll_one(api)
    _ack(api, part)
    return committed, str(part["key"])


def test_final_root_reactivation_uses_fresh_epoch_only_after_effective_change(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "fresh-delivery-epoch.db"
    turn_id = "turn-fresh-root-cycle"
    first_revision = _insert_revision(
        db_path,
        turn_id=turn_id,
        final_text="first-final",
    )
    _ensure_anchor(db_path, turn_id=turn_id, revision=first_revision)
    api = ConnectorOutboxAPI(db_path, HOST_ID)
    first_source = _poll_one(api)
    first_plan, first_job_key = _deliver_one_part_source(
        api,
        source=first_source,
        turn_id=turn_id,
        revision=first_revision,
        final_length=len("first-final"),
        version="fresh-epoch-v1",
    )
    assert first_plan["generation"] == 1

    second_revision = _insert_revision(
        db_path,
        turn_id=turn_id,
        final_text="second-final",
        created_at="2026-01-02T00:00:00+00:00",
    )
    _ensure_anchor(db_path, turn_id=turn_id, revision=second_revision)
    second_source = _poll_one(api)
    second_plan, _ = _deliver_one_part_source(
        api,
        source=second_source,
        turn_id=turn_id,
        revision=second_revision,
        final_length=len("second-final"),
        version="fresh-epoch-v1",
    )
    assert second_plan["generation"] == 1

    _set_current_revision(
        db_path,
        turn_id=turn_id,
        revision=first_revision,
        changed_at="2026-01-03T00:00:00+00:00",
    )
    _ensure_anchor(db_path, turn_id=turn_id, revision=first_revision)
    reactivated_source = _poll_one(api)
    assert reactivated_source["key"] == first_source["key"]
    assert reactivated_source["attempt"] == 1
    reactivated_plan, reactivated_job_key = _deliver_one_part_source(
        api,
        source=reactivated_source,
        turn_id=turn_id,
        revision=first_revision,
        final_length=len("first-final"),
        version="fresh-epoch-v1",
    )
    assert reactivated_plan["generation"] == 2
    assert reactivated_plan["plan_token"] != first_plan["plan_token"]
    assert reactivated_job_key != first_job_key
    with sqlite3.connect(str(db_path)) as conn:
        first_revision_plans = conn.execute(
            """
            SELECT generation, plan_token, state
            FROM turn_presentation_plans
            WHERE host_id = ? AND turn_id = ? AND content_revision = ?
            """,
            (HOST_ID, turn_id, first_revision),
        ).fetchall()
        source_row = conn.execute(
            """
            SELECT status, private_state_json
            FROM connector_outbox
            WHERE delivery_key = ?
            """,
            (first_source["key"],),
        ).fetchone()
    assert first_revision_plans == [
        (2, reactivated_plan["plan_token"], "completed")
    ]
    assert source_row[0] == "delivered"
    source_private = json.loads(str(source_row[1]))
    assert source_private["presentation_generation"] == 2
    assert source_private["prior_attempt_count"] == 1

    noop_db = tmp_path / "unplanned-intervening-revision.db"
    noop_turn = "turn-unplanned-intervening"
    noop_first = _insert_revision(
        noop_db,
        turn_id=noop_turn,
        final_text="stable-first",
    )
    _ensure_anchor(noop_db, turn_id=noop_turn, revision=noop_first)
    noop_api = ConnectorOutboxAPI(noop_db, HOST_ID)
    noop_source = _poll_one(noop_api)
    noop_plan, _ = _deliver_one_part_source(
        noop_api,
        source=noop_source,
        turn_id=noop_turn,
        revision=noop_first,
        final_length=len("stable-first"),
        version="noop-epoch-v1",
    )
    noop_second = _insert_revision(
        noop_db,
        turn_id=noop_turn,
        final_text="never-activated",
        created_at="2026-01-02T00:00:00+00:00",
    )
    _ensure_anchor(noop_db, turn_id=noop_turn, revision=noop_second)
    _set_current_revision(
        noop_db,
        turn_id=noop_turn,
        revision=noop_first,
        changed_at="2026-01-03T00:00:00+00:00",
    )
    _ensure_anchor(noop_db, turn_id=noop_turn, revision=noop_first)
    assert noop_api.poll({"name": FINAL_NAME, "limit": 100})["items"] == []
    with sqlite3.connect(str(noop_db)) as conn:
        assert conn.execute(
            """
            SELECT generation, plan_token, state
            FROM turn_presentation_plans
            WHERE content_revision = ?
            """,
            (noop_first,),
        ).fetchall() == [(1, noop_plan["plan_token"], "completed")]

    unresolved_db = tmp_path / "unresolved-reactivation.db"
    unresolved_turn = "turn-unresolved-reactivation"
    unresolved_first = _insert_revision(
        unresolved_db,
        turn_id=unresolved_turn,
        final_text="unresolved-first",
    )
    _ensure_anchor(
        unresolved_db,
        turn_id=unresolved_turn,
        revision=unresolved_first,
    )
    unresolved_api = ConnectorOutboxAPI(unresolved_db, HOST_ID)
    unresolved_source = _poll_one(unresolved_api)
    unresolved_second = _insert_revision(
        unresolved_db,
        turn_id=unresolved_turn,
        final_text="unresolved-second",
        created_at="2026-01-02T00:00:00+00:00",
    )
    _ensure_anchor(
        unresolved_db,
        turn_id=unresolved_turn,
        revision=unresolved_second,
    )
    _set_current_revision(
        unresolved_db,
        turn_id=unresolved_turn,
        revision=unresolved_first,
        changed_at="2026-01-03T00:00:00+00:00",
    )
    _ensure_anchor(
        unresolved_db,
        turn_id=unresolved_turn,
        revision=unresolved_first,
    )
    requeued_unresolved = _poll_one(unresolved_api)
    assert requeued_unresolved["key"] == unresolved_source["key"]
    begun = unresolved_api.prepare(
        {
            "schema_version": 1,
            "action": "begin",
            "name": FINAL_NAME,
            "turn_id": unresolved_turn,
            "content_revision": unresolved_first,
            "presentation_version": "unresolved-epoch-v1",
            "part_count": 1,
            "source_ref": requeued_unresolved["ref"],
        }
    )
    assert begun["generation"] == 2


def test_failed_plan_inspect_and_final_identity_retry_recovers_lost_consumer(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "lost-consumer-recovery.db"
    turn_id = "turn-lost-consumer-recovery"
    final_text = "LOST-CONSUMER-PRIVATE-FINAL"
    revision = _insert_revision(
        db_path,
        turn_id=turn_id,
        final_text=final_text,
    )
    _ensure_anchor(db_path, turn_id=turn_id, revision=revision)
    api = ConnectorOutboxAPI(db_path, HOST_ID, max_attempts=1)
    source = _poll_one(api)
    root = _prepare_plan(
        api,
        turn_id=turn_id,
        revision=revision,
        parts=[
            [_final_span(0, 9)],
            [_final_span(9, 18)],
            [_final_span(18, len(final_text))],
        ],
        version="lost-consumer-v1",
        source_ref=source["ref"],
    )
    _ack(api, _poll_one(api))
    failed_part = _poll_one(api)
    assert api.fail(
        {
            "name": FINAL_NAME,
            "ref": failed_part["ref"],
            "delay_seconds": 0,
        }
    )["status"] == "attempts_exhausted"

    inspected = api.inspect(
        {
            "schema_version": 1,
            "name": FINAL_NAME,
            "status": "dead_letter",
            "limit": 10,
        }
    )
    assert inspected["total"] == 1
    assert len(inspected["items"]) == 1
    failed_item = inspected["items"][0]
    assert set(failed_item) == {
        "kind",
        "status",
        "plan_token",
        "final_identity",
        "key",
        "turn_id",
        "content_revision",
        "generation",
        "failed_job_count",
        "attempt_count",
    }
    assert failed_item == {
        "kind": "failed_plan",
        "status": "dead_letter",
        "plan_token": root["plan_token"],
        "final_identity": source["payload"]["final_identity"],
        "key": source["key"],
        "turn_id": turn_id,
        "content_revision": revision,
        "generation": 1,
        "failed_job_count": 1,
        "attempt_count": 2,
    }
    assert final_text not in json.dumps(inspected, sort_keys=True)
    recovered = api.retry(
        {
            "schema_version": 1,
            "name": FINAL_NAME,
            "final_identity": failed_item["final_identity"],
        }
    )
    assert recovered["ok"] is True
    assert recovered["status"] == "recovered"
    assert recovered["failed_plan_token"] == root["plan_token"]
    for expected_sequence in (1, 2):
        item = _poll_one(api)
        assert item["payload"]["sequence_index"] == expected_sequence
        _ack(api, item)
    assert api.poll({"name": FINAL_NAME, "limit": 100})["items"] == []
    with sqlite3.connect(str(db_path)) as conn:
        assert conn.execute(
            "SELECT status FROM connector_outbox WHERE delivery_key = ?",
            (source["key"],),
        ).fetchone()[0] == "delivered"


def test_committed_unattempted_intervening_revision_does_not_reactivate_delivered_root(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "unattempted-intervening-revision.db"
    turn_id = "turn-unattempted-intervening"
    first_revision = _insert_revision(
        db_path,
        turn_id=turn_id,
        final_text="stable-first",
    )
    _ensure_anchor(db_path, turn_id=turn_id, revision=first_revision)
    api = ConnectorOutboxAPI(db_path, HOST_ID)
    first_source = _poll_one(api)
    first_plan, _ = _deliver_one_part_source(
        api,
        source=first_source,
        turn_id=turn_id,
        revision=first_revision,
        final_length=len("stable-first"),
        version="unattempted-v1",
    )

    second_revision = _insert_revision(
        db_path,
        turn_id=turn_id,
        final_text="never-attempted-second",
        created_at="2026-01-02T00:00:00+00:00",
    )
    _ensure_anchor(db_path, turn_id=turn_id, revision=second_revision)
    second_source = _poll_one(api)
    second_plan = _prepare_plan(
        api,
        turn_id=turn_id,
        revision=second_revision,
        parts=[[_final_span(0, len("never-attempted-second"))]],
        version="unattempted-v1",
        source_ref=second_source["ref"],
    )

    _set_current_revision(
        db_path,
        turn_id=turn_id,
        revision=first_revision,
        changed_at="2026-01-03T00:00:00+00:00",
    )
    _ensure_anchor(db_path, turn_id=turn_id, revision=first_revision)

    assert api.poll({"name": FINAL_NAME, "limit": 100})["items"] == []
    with sqlite3.connect(str(db_path)) as conn:
        first_plans = conn.execute(
            """
            SELECT generation, plan_token, state
            FROM turn_presentation_plans
            WHERE host_id = ? AND turn_id = ? AND content_revision = ?
            """,
            (HOST_ID, turn_id, first_revision),
        ).fetchall()
        second_state = conn.execute(
            """
            SELECT state
            FROM turn_presentation_plans
            WHERE plan_token = ?
            """,
            (second_plan["plan_token"],),
        ).fetchone()
        second_root = conn.execute(
            """
            SELECT status
            FROM connector_outbox
            WHERE delivery_key = ?
            """,
            (second_source["key"],),
        ).fetchone()
        second_source_attempt = conn.execute(
            """
            SELECT deliveries.status
            FROM connector_deliveries AS deliveries
            JOIN connector_outbox AS source ON source.id = deliveries.outbox_id
            WHERE source.delivery_key = ?
            """,
            (second_source["key"],),
        ).fetchone()

    assert first_plans == [(1, first_plan["plan_token"], "superseded")]
    assert second_state == ("superseded",)
    assert second_root == ("superseded",)
    assert second_source_attempt == ("superseded",)


def test_source_less_failed_plan_is_rediscoverable_and_retryable_by_inspected_key(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "source-less-failed-plan.db"
    turn_id = "turn-source-less-recovery"
    final_text = "source-less replacement"
    revision = _insert_revision(
        db_path,
        turn_id=turn_id,
        final_text=final_text,
    )
    _ensure_anchor(db_path, turn_id=turn_id, revision=revision)
    api = ConnectorOutboxAPI(db_path, HOST_ID, max_attempts=1)
    source = _poll_one(api)
    _deliver_one_part_source(
        api,
        source=source,
        turn_id=turn_id,
        revision=revision,
        final_length=len(final_text),
        version="source-less-original-v1",
    )
    source_less = _prepare_plan(
        api,
        turn_id=turn_id,
        revision=revision,
        parts=[[_final_span(0, len(final_text))]],
        version="source-less-replacement-v2",
        source_ref=None,
    )
    failed_part = _poll_one(api)
    assert api.fail(
        {
            "name": FINAL_NAME,
            "ref": failed_part["ref"],
            "delay_seconds": 0,
        }
    )["status"] == "attempts_exhausted"

    restarted = ConnectorOutboxAPI(db_path, HOST_ID, max_attempts=1)
    inspected = restarted.inspect(
        {
            "schema_version": 1,
            "name": FINAL_NAME,
            "status": "dead_letter",
            "limit": 10,
        }
    )
    assert inspected["total"] == 1
    assert len(inspected["items"]) == 1
    failed_item = inspected["items"][0]
    assert failed_item["kind"] == "failed_plan"
    assert failed_item["plan_token"] == source_less["plan_token"]
    assert failed_item["key"] == source["key"]

    recovered = restarted.retry(
        {
            "schema_version": 1,
            "name": FINAL_NAME,
            "key": failed_item["key"],
        }
    )
    assert recovered["ok"] is True
    assert recovered["status"] == "recovered"
    _ack(restarted, _poll_one(restarted))
    assert restarted.poll({"name": FINAL_NAME, "limit": 100})["items"] == []


def test_source_less_recovery_ids_survive_same_owner_worker_churn_and_ack_loss(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "source-less-owner-churn-ack-loss.db"
    raw_source = "source-less-backend-id"
    final_text = "source-less continuity final"
    worker_a = _owner_snapshot(
        db_path,
        worker_id="worker-source-less-a",
        worker_name="Source-less Worker A",
        space_id="space-source-less-a",
        second=0,
    )
    init_store(db_path)
    assert save_snapshot(db_path, worker_a) is True
    assert merge_turn_content(
        db_path,
        HOST_ID,
        "worker-source-less-a",
        {
            "source_turn_id": raw_source,
            "assistant_final_text": final_text,
            "complete": True,
            "has_open_turn": False,
        },
        observed_at="2026-01-01T00:00:01+00:00",
    ) == 1

    api = ConnectorOutboxAPI(db_path, HOST_ID, max_attempts=1)
    source = _poll_one(api)
    turn_id = str(source["payload"]["turn_id"])
    revision = str(source["payload"]["content_revision"])
    original_key = str(source["key"])
    original_plan, _original_job_key = _deliver_one_part_source(
        api,
        source=source,
        turn_id=turn_id,
        revision=revision,
        final_length=len(final_text),
        version="source-less-churn-original-v1",
    )
    failed_plan = _prepare_plan(
        api,
        turn_id=turn_id,
        revision=revision,
        parts=[[_final_span(0, len(final_text))]],
        version="source-less-churn-replacement-v2",
        source_ref=None,
    )
    failed_job = _poll_one(api)
    assert api.fail(
        {
            "name": FINAL_NAME,
            "ref": failed_job["ref"],
            "delay_seconds": 0,
        }
    )["status"] == "attempts_exhausted"

    before_churn = _turn_graph_snapshot(db_path, turn_id)
    root = next(row for row in before_churn["outbox"] if row[1] == original_key)
    plan_links = {row[1]: row[9] for row in before_churn["plans"]}
    assert plan_links == {
        original_plan["plan_token"]: root[0],
        failed_plan["plan_token"]: None,
    }
    assert [row[4] for row in before_churn["attempts"]] == [
        "delivered",
        "delivered",
        "failed",
    ]

    worker_b = _owner_snapshot(
        db_path,
        worker_id="worker-source-less-b",
        worker_name="Source-less Worker B",
        space_id="space-source-less-b",
        second=2,
    )
    assert worker_b.workers[0].fingerprint != worker_a.workers[0].fingerprint
    assert save_snapshot(db_path, worker_b) is True
    assert merge_turn_content(
        db_path,
        HOST_ID,
        "worker-source-less-b",
        {
            "source_turn_id": raw_source,
            "assistant_final_text": final_text,
            "complete": True,
            "has_open_turn": False,
        },
        observed_at="2026-01-01T00:00:03+00:00",
    ) == 1
    assert _turn_graph_snapshot(db_path, turn_id) == before_churn

    churned = ConnectorOutboxAPI(db_path, HOST_ID, max_attempts=1)
    inspected = churned.inspect(
        {
            "schema_version": 1,
            "name": FINAL_NAME,
            "status": "dead_letter",
            "limit": 10,
        }
    )
    assert inspected["total"] == 1
    failed_item = inspected["items"][0]
    assert failed_item["plan_token"] == failed_plan["plan_token"]
    assert failed_item["key"] == original_key
    recovery_request = {
        "schema_version": 1,
        "action": "recover",
        "name": FINAL_NAME,
        "failed_plan_token": failed_plan["plan_token"],
        "request_id": "source-less-owner-churn-recovery",
    }
    recovered = churned.prepare(recovery_request)
    assert recovered["ok"] is True
    assert recovered["status"] == "recovered"
    assert recovered["failed_plan_token"] == failed_plan["plan_token"]
    assert recovered["idempotent_replay"] is False
    recovered_job = _poll_one(churned)
    acknowledged = churned.ack(
        {
            "name": FINAL_NAME,
            "ref": recovered_job["ref"],
            "response": {"accepted": True},
        }
    )
    assert acknowledged["ok"] is True
    assert acknowledged["status"] == "acknowledged"

    completed_graph = _turn_graph_snapshot(db_path, turn_id)
    completed_links = {row[1]: row[9] for row in completed_graph["plans"]}
    assert completed_links == {
        original_plan["plan_token"]: root[0],
        failed_plan["plan_token"]: None,
        recovered["plan_token"]: None,
    }
    recovery_row = completed_graph["recoveries"][0]
    failed_row = next(
        row for row in completed_graph["plans"] if row[1] == failed_plan["plan_token"]
    )
    recovered_row = next(
        row for row in completed_graph["plans"] if row[1] == recovered["plan_token"]
    )
    assert recovery_row[2:6] == (
        failed_row[0],
        recovered_row[0],
        failed_plan["plan_token"],
        recovered["plan_token"],
    )
    before_attempt_ids = [row[0] for row in before_churn["attempts"]]
    completed_attempt_ids = [row[0] for row in completed_graph["attempts"]]
    assert [row[4] for row in completed_graph["attempts"]] == [
        "delivered",
        "delivered",
        "delivered",
    ]
    assert completed_attempt_ids[:2] == before_attempt_ids[:2]
    assert before_attempt_ids[2] not in completed_attempt_ids
    assert len(set(completed_attempt_ids)) == 3
    assert len(
        [row for row in completed_graph["outbox"] if row[1] == original_key]
    ) == 1

    init_store(db_path)
    restarted = ConnectorOutboxAPI(db_path, HOST_ID, max_attempts=1)
    assert restarted.poll({"name": FINAL_NAME, "limit": 100})["items"] == []
    replayed_recovery = restarted.prepare(recovery_request)
    assert replayed_recovery["ok"] is True
    assert replayed_recovery["idempotent_replay"] is True
    assert replayed_recovery["failed_plan_token"] == failed_plan["plan_token"]
    assert replayed_recovery["plan_token"] == recovered["plan_token"]
    assert save_snapshot(db_path, worker_b) is True
    assert merge_turn_content(
        db_path,
        HOST_ID,
        "worker-source-less-b",
        {
            "source_turn_id": raw_source,
            "assistant_final_text": final_text,
            "complete": True,
            "has_open_turn": False,
        },
        observed_at="2026-01-01T00:00:03+00:00",
    ) == 0
    assert restarted.poll({"name": FINAL_NAME, "limit": 100})["items"] == []
    assert _turn_graph_snapshot(db_path, turn_id) == completed_graph

    with sqlite3.connect(str(db_path)) as conn:
        public_payloads = [
            str(row[0])
            for row in conn.execute(
                """
                SELECT payload_json FROM turns WHERE host_id = ?
                UNION ALL
                SELECT payload_json FROM connector_outbox WHERE host_id = ?
                """,
                (HOST_ID, HOST_ID),
            ).fetchall()
        ]
        assert conn.execute("PRAGMA foreign_key_check").fetchall() == []
    public_surface = json.dumps(
        {
            "inspect": inspected,
            "recovered": recovered,
            "acknowledged": acknowledged,
            "replayed_recovery": replayed_recovery,
            "payloads": public_payloads,
        },
        sort_keys=True,
    )
    assert PRIVATE_ROUTE_SENTINEL not in public_surface
    assert raw_source not in public_surface


def test_failed_plan_inspection_reserves_space_when_holds_fill_limit(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "failed-plan-inspect-fairness.db"
    for index in range(100):
        turn_id = f"turn-hold-{index:03d}"
        revision = _insert_revision(
            db_path,
            turn_id=turn_id,
            final_text=f"hold-{index}",
            user_state="known_incomplete",
            user_text=f"partial-{index}",
        )
        _ensure_anchor(db_path, turn_id=turn_id, revision=revision)

    failed_turn = "turn-failed-behind-holds"
    failed_text = "failed behind holds"
    failed_revision = _insert_revision(
        db_path,
        turn_id=failed_turn,
        final_text=failed_text,
    )
    _ensure_anchor(db_path, turn_id=failed_turn, revision=failed_revision)
    api = ConnectorOutboxAPI(db_path, HOST_ID, max_attempts=1)
    source = _poll_one(api)
    failed_plan = _prepare_plan(
        api,
        turn_id=failed_turn,
        revision=failed_revision,
        parts=[[_final_span(0, len(failed_text))]],
        version="inspect-fairness-v1",
        source_ref=source["ref"],
    )
    failed_part = _poll_one(api)
    assert api.fail(
        {
            "name": FINAL_NAME,
            "ref": failed_part["ref"],
            "delay_seconds": 0,
        }
    )["status"] == "attempts_exhausted"

    inspected = api.inspect(
        {
            "schema_version": 1,
            "name": FINAL_NAME,
            "status": "dead_letter",
            "limit": 100,
        }
    )
    failed_items = [
        item
        for item in inspected["items"]
        if item.get("kind") == "failed_plan"
    ]

    assert inspected["total"] == 101
    assert len(inspected["items"]) == 100
    assert len(failed_items) == 1
    assert failed_items[0]["plan_token"] == failed_plan["plan_token"]


def test_source_less_commit_uses_immutable_delivered_root_route(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "source-less-immutable-route.db"
    turn_id = "turn-source-less-immutable"
    final_text = "authoritative source-less final"
    revision = _insert_revision(db_path, turn_id=turn_id, final_text=final_text)
    _ensure_anchor(db_path, turn_id=turn_id, revision=revision)
    api = ConnectorOutboxAPI(db_path, HOST_ID)
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            """
            UPDATE connector_outbox
            SET status = 'delivered', updated_at = ?
            WHERE host_id = ? AND delivery_kind = 'final_ready'
            """,
            (CREATED_AT, HOST_ID),
        )

    begun = api.prepare(
        {
            "schema_version": 1,
            "action": "begin",
            "name": FINAL_NAME,
            "turn_id": turn_id,
            "content_revision": revision,
            "presentation_version": "render-v1",
            "part_count": 1,
        }
    )
    assert begun["ok"] is True, begun
    staged = api.prepare(
        {
            "schema_version": 1,
            "action": "part",
            "name": FINAL_NAME,
            "plan_token": begun["plan_token"],
            "ordinal": 0,
            "spans": [_final_span(0, len(final_text))],
        }
    )
    assert staged["ok"] is True

    replacement_key = "wsk1_" + ("e" * 64)
    with sqlite3.connect(str(db_path)) as conn:
        payload = json.loads(
            str(
                conn.execute(
                    """
                    SELECT payload_json
                    FROM turns
                    WHERE host_id = ? AND turn_id = ?
                    """,
                    (HOST_ID, turn_id),
                ).fetchone()[0]
            )
        )
        payload["meta"] = {
            "stable_key": replacement_key,
            "stable_key_version": 1,
        }
        conn.execute(
            """
            UPDATE turns
            SET payload_json = ?
            WHERE host_id = ? AND turn_id = ?
            """,
            (json.dumps(payload, sort_keys=True), HOST_ID, turn_id),
        )

    committed = api.prepare(
        {
            "schema_version": 1,
            "action": "commit",
            "name": FINAL_NAME,
            "plan_token": begun["plan_token"],
        }
    )
    assert committed["ok"] is True
    with sqlite3.connect(str(db_path)) as conn:
        part_payload = json.loads(
            str(
                conn.execute(
                    """
                    SELECT payload_json
                    FROM connector_outbox
                    WHERE host_id = ? AND delivery_kind = 'final_part'
                    """,
                    (HOST_ID,),
                ).fetchone()[0]
            )
        )
    assert part_payload["turn"]["stable_key"] == STABLE_KEY
    assert part_payload["turn"]["stable_key"] != replacement_key


def test_internal_automation_final_is_nonpollable_and_nonretryable(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "automation-final-hold.db"
    turn_id = "turn-internal-automation"
    revision = _insert_revision(
        db_path,
        turn_id=turn_id,
        final_text='{"acme_result":{"decision":"approved"}}',
    )
    _ensure_anchor(db_path, turn_id=turn_id, revision=revision)
    api = ConnectorOutboxAPI(db_path, HOST_ID)

    assert api.poll({"name": FINAL_NAME, "limit": 100})["items"] == []
    inspected = api.inspect(
        {
            "schema_version": 1,
            "name": FINAL_NAME,
            "status": "dead_letter",
            "limit": 100,
        }
    )
    assert len(inspected["items"]) == 1
    held = inspected["items"][0]
    assert held["final"]["schema_version"] == 2
    retried = api.retry(
        {
            "schema_version": 1,
            "name": FINAL_NAME,
            "final_identity": held["final"]["final_identity"],
        }
    )
    assert retried["ok"] is False
    assert retried["status"] == "not_retryable"
