"""Regression coverage for delivery retention isolation and health surfaces."""

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
from tendwire.core.turns import turn_final_delivery_identity
from tendwire.store.sqlite import (
    cleanup_acknowledged_final_retention,
    init_store,
    inspect_connector_outbox,
    merge_turn_content,
    maybe_run_automatic_store_maintenance,
    SnapshotRetentionPolicy,
    reclaim_expired_connector_leases,
    save_snapshot,
    store_status,
)


FINAL_NAME = "turn-final"
STABLE_KEY = "wsk1_" + ("b" * 64)


def _insert_expired_lease(
    conn: sqlite3.Connection,
    *,
    host_id: str,
    name: str,
    key: str,
) -> None:
    created_at = "2026-01-01T00:00:00+00:00"
    conn.execute(
        """
        INSERT INTO connector_outbox (
            host_id, connector, delivery_key, status, payload_json,
            private_state_json, created_at, updated_at
        ) VALUES (?, ?, ?, 'leased', '{}', '{}', ?, ?)
        """,
        (host_id, name, key, created_at, created_at),
    )
    outbox_id = int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])
    conn.execute(
        """
        INSERT INTO connector_deliveries (
            outbox_id, host_id, connector, delivery_key, attempt, status,
            response_json, private_state_json, created_at
        ) VALUES (?, ?, ?, ?, 1, 'leased', '{}', ?, ?)
        """,
        (
            outbox_id,
            host_id,
            name,
            key,
            json.dumps(
                {
                    "lease_expires_at": "2026-01-01T00:00:01+00:00",
                    "lease_token": f"private-{key}",
                },
                sort_keys=True,
            ),
            created_at,
        ),
    )
    delivery_id = int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])
    conn.execute(
        "UPDATE connector_outbox SET private_state_json = ? WHERE id = ?",
        (json.dumps({"current_delivery_id": delivery_id}), outbox_id),
    )


def test_expired_lease_reclaim_treats_empty_host_and_name_as_exact_scopes(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "empty-scope-isolation.db"
    init_store(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_expired_lease(conn, host_id="", name="", key="empty-both")
        _insert_expired_lease(conn, host_id="", name="attention", key="empty-host")
        _insert_expired_lease(conn, host_id="other-host", name="", key="empty-name")

    exact = reclaim_expired_connector_leases(
        db_path,
        "",
        "",
        now="2026-01-01T00:00:02+00:00",
    )
    with sqlite3.connect(str(db_path)) as conn:
        after_exact = conn.execute(
            "SELECT delivery_key, status FROM connector_outbox ORDER BY id"
        ).fetchall()

    assert exact["reclaimed"] == 1
    assert after_exact == [
        ("empty-both", "queued"),
        ("empty-host", "leased"),
        ("empty-name", "leased"),
    ]

    all_connectors_for_empty_host = reclaim_expired_connector_leases(
        db_path,
        "",
        None,
        now="2026-01-01T00:00:02+00:00",
    )
    with sqlite3.connect(str(db_path)) as conn:
        final_outbox = conn.execute(
            "SELECT delivery_key, status FROM connector_outbox ORDER BY id"
        ).fetchall()
        final_attempts = conn.execute(
            "SELECT delivery_key, status FROM connector_deliveries ORDER BY id"
        ).fetchall()

    assert all_connectors_for_empty_host["reclaimed"] == 1
    assert final_outbox == [
        ("empty-both", "queued"),
        ("empty-host", "queued"),
        ("empty-name", "leased"),
    ]
    assert final_attempts == [
        ("empty-both", "expired"),
        ("empty-host", "expired"),
        ("empty-name", "leased"),
    ]


def test_inspect_omits_malformed_typed_final_key_without_exposing_private_state(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "inspect-malformed-key.db"
    host_id = "inspect-host"
    valid_key = "turn-final:revision:" + turn_final_delivery_identity(
        host_id,
        "turn-visible",
        "twrev1.visible",
    )
    malformed_key = "turn-final:revision:twfinal1.PRIVATE/legacy-key-sentinel"
    private_sentinel = "PRIVATE-INSPECTION-STATE-SENTINEL"
    init_store(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        conn.executemany(
            """
            INSERT INTO connector_outbox (
                host_id, connector, delivery_key, delivery_kind, status,
                payload_json, private_state_json, created_at, updated_at
            ) VALUES (?, 'turn-final', ?, 'final_ready', 'dead_letter',
                      ?, ?, ?, ?)
            """,
            [
                (
                    host_id,
                    valid_key,
                    '{"operation":"materialize"}',
                    '{}',
                    "2026-01-01T00:00:00+00:00",
                    "2026-01-01T00:00:01+00:00",
                ),
                (
                    host_id,
                    malformed_key,
                    '{"operation":"materialize"}',
                    json.dumps({"prior_attempt_count": 3, "secret": private_sentinel}),
                    "2026-01-02T00:00:00+00:00",
                    "2026-01-02T00:00:01+00:00",
                ),
            ],
        )
        valid_outbox_id = conn.execute(
            "SELECT id FROM connector_outbox WHERE delivery_key = ?",
            (valid_key,),
        ).fetchone()[0]
        conn.execute(
            """
            INSERT INTO connector_deliveries (
                outbox_id, host_id, connector, delivery_key, attempt,
                status, response_json, private_state_json, created_at,
                delivered_at
            ) VALUES (
                ?, ?, 'turn-final', ?, 1, 'failed', 'not-json', '{}', ?, ?
            )
            """,
            (
                valid_outbox_id,
                host_id,
                valid_key,
                "2026-01-01T00:00:00+00:00",
                "2026-01-01T00:00:01+00:00",
            ),
        )

    inspected = inspect_connector_outbox(
        db_path,
        host_id,
        name=FINAL_NAME,
        status="dead_letter",
        limit=10,
    )
    encoded = json.dumps(inspected, sort_keys=True)

    assert inspected["ok"] is True
    assert inspected["total"] == 2
    assert inspected["dead_letter_rows"] == 2
    assert inspected["classifications"] == [
        {
            "kind": "final_anchor",
            "last_status": "missing",
            "last_reason": "missing",
            "count": 2,
            "recovery": "exact_key",
        }
    ]
    assert inspected["items"][0]["key"] == valid_key
    assert "key" not in inspected["items"][1]
    assert inspected["items"][1]["attempt_count"] == 3
    assert malformed_key not in encoded
    assert "PRIVATE/legacy-key-sentinel" not in encoded
    assert private_sentinel not in encoded
    assert "private_state_json" not in encoded
@pytest.mark.parametrize(
    "private_attempt_count",
    ["malformed", -7, 1.5, True, 1 << 80],
)
def test_inspect_rejects_malformed_private_attempt_counts(
    tmp_path: Path,
    private_attempt_count: object,
) -> None:
    db_path = tmp_path / "inspect-attempt-count.db"
    host_id = "inspect-attempt-host"
    key = "turn-final:revision:" + turn_final_delivery_identity(
        host_id,
        "turn-visible",
        "twrev1.visible",
    )
    init_store(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            """
            INSERT INTO connector_outbox (
                host_id, connector, delivery_key, delivery_kind, status,
                payload_json, private_state_json, created_at, updated_at
            ) VALUES (?, 'turn-final', ?, 'final_ready', 'dead_letter',
                      '{}', ?, ?, ?)
            """,
            (
                host_id,
                key,
                json.dumps({"prior_attempt_count": private_attempt_count}),
                "2026-01-01T00:00:00+00:00",
                "2026-01-01T00:00:01+00:00",
            ),
        )

    inspected = inspect_connector_outbox(
        db_path,
        host_id,
        name=FINAL_NAME,
        status="dead_letter",
        limit=10,
    )

    assert inspected["ok"] is True
    assert inspected["items"][0]["attempt_count"] == 0




def _new_delivery_store(db_path: Path, host_id: str) -> tuple[Any, ConnectorOutboxAPI]:
    snapshot = project_from_raw(
        Config(host_id=host_id, db_path=db_path),
        workers=[
            {
                "id": "worker-1",
                "name": "Retention Worker",
                "status": "active",
                "space_id": "space-1",
                "meta": {
                    "stable_key": STABLE_KEY,
                    "stable_key_version": 1,
                },
            }
        ],
    )
    init_store(db_path)
    save_snapshot(db_path, snapshot)
    return snapshot, ConnectorOutboxAPI(db_path, host_id)


def _deliver_final(
    db_path: Path,
    host_id: str,
    api: ConnectorOutboxAPI,
    *,
    source_turn_id: str,
    text: str,
    observed_at: str,
) -> str:
    assert merge_turn_content(
        db_path,
        host_id,
        "worker-1",
        {
            "assistant_final_text": text,
            "complete": True,
            "has_open_turn": False,
            "source_turn_id": source_turn_id,
        },
        observed_at=observed_at,
    ) == 1
    source_result = api.poll({"name": FINAL_NAME, "limit": 10, "lease_seconds": 60})
    assert source_result["ok"] is True
    assert len(source_result["items"]) == 1
    source = source_result["items"][0]
    payload = source["payload"]
    final_length = int(
        payload["content"]["fields"]["assistant_final_text"]["char_length"]
    )
    begun = api.prepare(
        {
            "schema_version": 1,
            "action": "begin",
            "name": FINAL_NAME,
            "turn_id": payload["turn_id"],
            "content_revision": payload["content_revision"],
            "presentation_version": "hardening-v1",
            "part_count": 1,
            "source_ref": source["ref"],
        }
    )
    assert begun["ok"] is True
    token = begun["plan_token"]
    part = api.prepare(
        {
            "schema_version": 1,
            "action": "part",
            "name": FINAL_NAME,
            "plan_token": token,
            "ordinal": 0,
            "spans": [
                {
                    "field": "assistant_final_text",
                    "start_char": 0,
                    "end_char": final_length,
                }
            ],
        }
    )
    assert part["ok"] is True
    committed = api.prepare(
        {
            "schema_version": 1,
            "action": "commit",
            "name": FINAL_NAME,
            "plan_token": token,
            "source_ref": source["ref"],
        }
    )
    assert committed["ok"] is True
    assert committed["job_count"] == 1
    job_result = api.poll({"name": FINAL_NAME, "limit": 10, "lease_seconds": 60})
    assert len(job_result["items"]) == 1
    job = job_result["items"][0]
    acknowledged = api.ack(
        {
            "name": FINAL_NAME,
            "ref": job["ref"],
            "response": {"accepted": True},
        }
    )
    assert acknowledged["status"] == "acknowledged"
    return str(source["key"])


@pytest.mark.parametrize(
    ("pressure_kind", "delivered_count", "retention_days", "retention_count", "acknowledged_at", "remaining"),
    [
        ("count", 2, 36_500, 1, "2099-01-01T00:00:00+00:00", 1),
        ("age", 1, 1, 100, "2000-01-01T00:00:00+00:00", 0),
    ],
)
def test_delivered_retention_eligibility_sets_pressure_and_backlog_until_cleanup(
    tmp_path: Path,
    pressure_kind: str,
    delivered_count: int,
    retention_days: int,
    retention_count: int,
    acknowledged_at: str,
    remaining: int,
) -> None:
    db_path = tmp_path / f"delivered-{pressure_kind}-pressure.db"
    host_id = f"delivered-{pressure_kind}-host"
    _snapshot, api = _new_delivery_store(db_path, host_id)
    keys = [
        _deliver_final(
            db_path,
            host_id,
            api,
            source_turn_id=f"source-{index}",
            text=f"retained final {index}",
            observed_at=f"2026-01-0{index + 1}T00:00:00+00:00",
        )
        for index in range(delivered_count)
    ]
    with sqlite3.connect(str(db_path)) as conn:
        conn.executemany(
            """
            UPDATE connector_outbox
            SET updated_at = ?
            WHERE host_id = ?
              AND connector = 'turn-final'
              AND delivery_kind = 'final_ready'
              AND delivery_key = ?
            """,
            [(acknowledged_at, host_id, key) for key in keys],
        )

    before = store_status(
        db_path,
        host_id,
        snapshot_retention_days=36_500,
        snapshot_retention_count=100,
        acknowledged_final_retention_days=retention_days,
        acknowledged_final_retention_count=retention_count,
    )

    assert before["final_retention"]["acknowledged"] == delivered_count
    assert before["final_retention"]["unresolved"] == 0
    assert before["final_retention"]["eligible"] == 1
    assert before["final_retention"]["storage_pressure"] is True
    assert before["maintenance"]["backlog"] is True

    cleanup = cleanup_acknowledged_final_retention(
        db_path,
        host_id,
        acknowledged_final_retention_days=retention_days,
        acknowledged_final_retention_count=retention_count,
        batch_size=10,
        now="2026-07-13T00:00:00+00:00",
    )
    after = store_status(
        db_path,
        host_id,
        snapshot_retention_days=36_500,
        snapshot_retention_count=100,
        acknowledged_final_retention_days=retention_days,
        acknowledged_final_retention_count=retention_count,
    )

    assert cleanup["deleted"] == 1
    assert cleanup["remaining_candidates"] is False
    assert after["final_retention"]["acknowledged"] == remaining
    assert after["final_retention"]["eligible"] == 0
    assert after["final_retention"]["storage_pressure"] is False
    assert after["maintenance"]["backlog"] is False


def test_automatic_maintenance_round_robins_eligible_final_hosts(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "automatic-final-host-fairness.db"
    _snapshot_a, api_a = _new_delivery_store(db_path, "fair-host-a")
    keys_a = [
        _deliver_final(
            db_path,
            "fair-host-a",
            api_a,
            source_turn_id=f"fair-a-{index}",
            text=f"fair host A final {index}",
            observed_at=f"2026-01-0{index + 1}T00:00:00+00:00",
        )
        for index in range(2)
    ]
    _snapshot_b, api_b = _new_delivery_store(db_path, "fair-host-b")
    key_b = _deliver_final(
        db_path,
        "fair-host-b",
        api_b,
        source_turn_id="fair-b-0",
        text="fair host B final",
        observed_at="2026-01-03T00:00:00+00:00",
    )
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            """
            UPDATE connector_outbox
            SET updated_at = '2000-01-01T00:00:00+00:00'
            WHERE delivery_key IN (?, ?, ?)
            """,
            (keys_a[0], keys_a[1], key_b),
        )

    policy = SnapshotRetentionPolicy(
        retention_days=36_500,
        retention_count=10_000,
        batch_size=1,
    )
    for second in (0, 2):
        maybe_run_automatic_store_maintenance(
            db_path,
            policy=policy,
            acknowledged_final_retention_days=1,
            acknowledged_final_retention_count=10_000,
            cadence_seconds=1,
            now=f"2026-07-13T00:00:0{second}+00:00",
        )

    status_a = store_status(
        db_path,
        "fair-host-a",
        acknowledged_final_retention_days=1,
        acknowledged_final_retention_count=10_000,
    )
    status_b = store_status(
        db_path,
        "fair-host-b",
        acknowledged_final_retention_days=1,
        acknowledged_final_retention_count=10_000,
    )
    assert status_a["final_retention"]["acknowledged"] == 1
    assert status_b["final_retention"]["acknowledged"] == 0


def test_turn_final_fail_and_defer_persist_only_allowlisted_reason_codes(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "turn-final-reason-codes.db"
    host_id = "reason-code-host"
    _snapshot, api = _new_delivery_store(db_path, host_id)
    assert merge_turn_content(
        db_path,
        host_id,
        "worker-1",
        {
            "assistant_final_text": "final awaiting delivery",
            "complete": True,
            "has_open_turn": False,
            "source_turn_id": "reason-source",
        },
        observed_at="2026-01-01T00:00:00+00:00",
    ) == 1
    unsafe_reason = (
        "telegram topic 98765 chat 12345 message 54321 "
        "/tmp/private bot-token:SECRET"
    )

    first = api.poll({"name": FINAL_NAME, "limit": 10})["items"][0]
    failed = api.fail(
        {
            "name": FINAL_NAME,
            "ref": first["ref"],
            "reason": unsafe_reason,
            "delay_seconds": 0,
        }
    )
    second = api.poll({"name": FINAL_NAME, "limit": 10})["items"][0]
    deferred = api.defer(
        {
            "name": FINAL_NAME,
            "ref": second["ref"],
            "reason": unsafe_reason,
            "delay_seconds": 0,
        }
    )
    third = api.poll({"name": FINAL_NAME, "limit": 10})["items"][0]
    allowlisted = api.fail(
        {
            "name": FINAL_NAME,
            "ref": third["ref"],
            "reason": "temporary",
            "delay_seconds": 0,
        }
    )

    with sqlite3.connect(str(db_path)) as conn:
        stored_json = [
            str(row[0])
            for row in conn.execute(
                """
                SELECT response_json
                FROM connector_deliveries
                WHERE host_id = ? AND connector = 'turn-final'
                ORDER BY id
                """,
                (host_id,),
            ).fetchall()
        ]
    stored = [json.loads(value) for value in stored_json]
    public_json = json.dumps([failed, deferred, allowlisted], sort_keys=True)

    assert [item["reason"] for item in stored] == [
        "unknown",
        "unknown",
        "temporary",
    ]
    assert failed["status"] == "retry_scheduled"
    assert deferred["status"] == "deferred"
    assert allowlisted["status"] == "retry_scheduled"
    for private_fragment in (
        unsafe_reason,
        "telegram",
        "98765",
        "12345",
        "54321",
        "/tmp/private",
        "bot-token",
        "SECRET",
    ):
        assert all(private_fragment not in value for value in stored_json)
        assert private_fragment not in public_json


def test_store_status_snapshot_pressure_is_exactly_host_scoped(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "host-scoped-snapshot-pressure.db"
    host_a = "host-a"
    host_b = "host-b"
    init_store(db_path)
    save_snapshot(
        db_path,
        project_from_raw(
            Config(host_id=host_a, db_path=db_path),
            workers=[],
            timestamp=datetime(2026, 1, 3, tzinfo=timezone.utc),
        ),
    )
    for day, status in ((1, "active"), (2, "waiting")):
        save_snapshot(
            db_path,
            project_from_raw(
                Config(host_id=host_b, db_path=db_path),
                workers=[
                    {
                        "id": "worker-b",
                        "name": "Worker B",
                        "status": status,
                    }
                ],
                timestamp=datetime(2026, 1, day, tzinfo=timezone.utc),
            ),
        )

    status_a = store_status(
        db_path,
        host_a,
        snapshot_retention_days=36_500,
        snapshot_retention_count=1,
    )
    status_b = store_status(
        db_path,
        host_b,
        snapshot_retention_days=36_500,
        snapshot_retention_count=1,
    )

    assert status_a["counts"]["snapshots"] == 1
    assert status_a["maintenance"]["snapshot_count"] == 1
    assert status_a["maintenance"]["backlog"] is False
    assert status_b["counts"]["snapshots"] == 2
    assert status_b["maintenance"]["snapshot_count"] == 2
    assert status_b["maintenance"]["backlog"] is True


@pytest.mark.parametrize(
    "proof_gap",
    [
        "declared_part_missing",
        "delivered_attempt_missing",
        "delivered_attempt_contradicted",
    ],
)
def test_cleanup_requires_exact_durable_all_part_ack_proof(
    tmp_path: Path,
    proof_gap: str,
) -> None:
    db_path = tmp_path / f"cleanup-proof-{proof_gap}.db"
    host_id = "cleanup-proof-host"
    _snapshot, api = _new_delivery_store(db_path, host_id)
    source_key = _deliver_final(
        db_path,
        host_id,
        api,
        source_turn_id="proof-source",
        text="proof final",
        observed_at="2026-01-01T00:00:00+00:00",
    )
    with sqlite3.connect(str(db_path)) as conn:
        part_id = int(
            conn.execute(
                """
                SELECT id
                FROM connector_outbox
                WHERE host_id = ? AND delivery_kind = 'final_part'
                """,
                (host_id,),
            ).fetchone()[0]
        )
        if proof_gap == "declared_part_missing":
            conn.execute(
                """
                UPDATE turn_presentation_plans
                SET part_count = 2
                WHERE host_id = ? AND state = 'completed'
                """,
                (host_id,),
            )
        elif proof_gap == "delivered_attempt_missing":
            conn.execute(
                "DELETE FROM connector_deliveries WHERE outbox_id = ?",
                (part_id,),
            )
        else:
            conn.execute(
                """
                UPDATE connector_deliveries
                SET status = 'failed', delivered_at = NULL
                WHERE outbox_id = ?
                """,
                (part_id,),
            )
        conn.execute(
            """
            UPDATE connector_outbox
            SET updated_at = '2000-01-01T00:00:00+00:00'
            WHERE host_id = ?
              AND delivery_kind = 'final_ready'
              AND delivery_key = ?
            """,
            (host_id, source_key),
        )

    cleanup = cleanup_acknowledged_final_retention(
        db_path,
        host_id,
        acknowledged_final_retention_days=1,
        acknowledged_final_retention_count=1,
        now="2026-07-13T00:00:00+00:00",
    )
    health = store_status(
        db_path,
        host_id,
        snapshot_retention_days=36_500,
        snapshot_retention_count=100,
        acknowledged_final_retention_days=1,
        acknowledged_final_retention_count=1,
    )
    with sqlite3.connect(str(db_path)) as conn:
        surviving_target = int(
            conn.execute(
                """
                SELECT COUNT(*)
                FROM turns
                WHERE host_id = ?
                  AND turn_id = (
                      SELECT turn_id
                      FROM connector_outbox
                      WHERE host_id = ? AND delivery_key = ?
                  )
                """,
                (host_id, host_id, source_key),
            ).fetchone()[0]
        )

    assert cleanup["deleted"] == 0
    assert surviving_target == 1
    assert health["final_retention"]["eligible"] == 0
    assert health["final_retention"]["unresolved"] == 1
    assert health["final_retention"]["storage_pressure"] is True


def test_unlinked_unresolved_final_part_blocks_graph_cleanup_and_surfaces_pressure(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "cleanup-orphan-part.db"
    host_id = "cleanup-orphan-host"
    _snapshot, api = _new_delivery_store(db_path, host_id)
    source_key = _deliver_final(
        db_path,
        host_id,
        api,
        source_turn_id="orphan-source",
        text="orphan protected final",
        observed_at="2026-01-01T00:00:00+00:00",
    )
    with sqlite3.connect(str(db_path)) as conn:
        turn_id, revision = conn.execute(
            """
            SELECT turn_id, content_revision
            FROM connector_outbox
            WHERE host_id = ? AND delivery_key = ?
            """,
            (host_id, source_key),
        ).fetchone()
        conn.execute(
            """
            INSERT INTO connector_outbox (
                host_id, connector, delivery_key, delivery_kind,
                turn_id, content_revision, status, payload_json,
                private_state_json, created_at, updated_at
            ) VALUES (
                ?, 'turn-final', 'turn-final:orphan-part', 'final_part',
                ?, ?, 'queued', '{}', '{}', ?, ?
            )
            """,
            (
                host_id,
                turn_id,
                revision,
                "2000-01-01T00:00:00+00:00",
                "2000-01-01T00:00:00+00:00",
            ),
        )
        conn.execute(
            """
            UPDATE connector_outbox
            SET updated_at = '2000-01-01T00:00:00+00:00'
            WHERE host_id = ? AND delivery_key = ?
            """,
            (host_id, source_key),
        )

    cleanup = cleanup_acknowledged_final_retention(
        db_path,
        host_id,
        acknowledged_final_retention_days=1,
        acknowledged_final_retention_count=1,
        now="2026-07-13T00:00:00+00:00",
    )
    health = store_status(
        db_path,
        host_id,
        snapshot_retention_days=36_500,
        snapshot_retention_count=100,
        acknowledged_final_retention_days=1,
        acknowledged_final_retention_count=1,
    )
    with sqlite3.connect(str(db_path)) as conn:
        orphan_status = conn.execute(
            """
            SELECT status
            FROM connector_outbox
            WHERE host_id = ? AND delivery_key = 'turn-final:orphan-part'
            """,
            (host_id,),
        ).fetchone()

    assert cleanup["deleted"] == 0
    assert orphan_status == ("queued",)
    assert health["final_retention"]["eligible"] == 0
    assert health["final_retention"]["queued"] == 1
    assert health["final_retention"]["unresolved"] == 1
    assert health["final_retention"]["storage_pressure"] is True


def test_acknowledged_graph_cleanup_deletes_matching_detached_tombstones(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "cleanup-detached-tombstone.db"
    host_id = "cleanup-tombstone-host"
    _snapshot, api = _new_delivery_store(db_path, host_id)
    source_key = _deliver_final(
        db_path,
        host_id,
        api,
        source_turn_id="tombstone-source",
        text="tombstone final",
        observed_at="2026-01-01T00:00:00+00:00",
    )
    with sqlite3.connect(str(db_path)) as conn:
        part_key = str(
            conn.execute(
                """
                SELECT delivery_key
                FROM connector_outbox
                WHERE host_id = ? AND delivery_kind = 'final_part'
                """,
                (host_id,),
            ).fetchone()[0]
        )
        conn.execute(
            """
            INSERT INTO connector_deliveries (
                outbox_id, host_id, connector, delivery_key, attempt,
                status, response_json, private_state_json, created_at
            ) VALUES (NULL, ?, 'turn-final', ?, 99, 'failed', '{}', '{}', ?)
            """,
            (host_id, part_key, "2026-01-01T00:00:00+00:00"),
        )
        conn.execute(
            """
            UPDATE connector_outbox
            SET updated_at = '2000-01-01T00:00:00+00:00'
            WHERE host_id = ? AND delivery_key = ?
            """,
            (host_id, source_key),
        )

    cleanup = cleanup_acknowledged_final_retention(
        db_path,
        host_id,
        acknowledged_final_retention_days=1,
        acknowledged_final_retention_count=1,
        now="2026-07-13T00:00:00+00:00",
    )
    with sqlite3.connect(str(db_path)) as conn:
        remaining_attempts = int(
            conn.execute(
                """
                SELECT COUNT(*)
                FROM connector_deliveries
                WHERE host_id = ? AND delivery_key = ?
                """,
                (host_id, part_key),
            ).fetchone()[0]
        )

    assert cleanup["deleted"] == 1
    assert cleanup["deleted_rows"]["attempts"] == 2
    assert remaining_attempts == 0


def test_same_owner_churn_preserves_detached_ack_tombstone(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "same-owner-detached-ack-tombstone.db"
    host_id = "detached-owner-churn-host"
    raw_source = "detached-backend-source-id"
    private_route = "PRIVATE-HARDENING-ROUTE-SENTINEL"
    stable_key_k2 = "wsk1_" + ("a" * 64)
    config = Config(host_id=host_id, db_path=db_path)

    def owner_snapshot(stable_key: str, worker_id: str, space_id: str, second: int) -> Any:
        return project_from_raw(
            config,
            workers=[
                {
                    "id": worker_id,
                    "name": f"Detached Worker {worker_id}",
                    "status": "active",
                    "space_id": space_id,
                    "terminal_id": private_route,
                    "backend_target": {
                        "kind": "agent_id",
                        "value": private_route,
                        "sendable": True,
                    },
                    "meta": {
                        "stable_key": stable_key,
                        "stable_key_version": 1,
                        "chat_id": private_route,
                        "topic_id": private_route,
                    },
                }
            ],
            timestamp=datetime(2026, 1, 1, 0, 0, second, tzinfo=timezone.utc),
        )

    worker_a = owner_snapshot(STABLE_KEY, "worker-1", "space-tombstone-a", 0)
    init_store(db_path)
    assert save_snapshot(db_path, worker_a) is True
    api = ConnectorOutboxAPI(db_path, host_id)
    original_key = _deliver_final(
        db_path,
        host_id,
        api,
        source_turn_id=raw_source,
        text="detached continuity final",
        observed_at="2026-01-01T00:00:01+00:00",
    )
    with sqlite3.connect(str(db_path)) as conn:
        original = conn.execute(
            """
            SELECT outbox.id, outbox.turn_id, outbox.content_revision,
                   turns.payload_json
            FROM connector_outbox AS outbox
            JOIN turns
              ON turns.host_id = outbox.host_id
             AND turns.turn_id = outbox.turn_id
            WHERE outbox.host_id = ?
              AND outbox.delivery_key = ?
              AND outbox.delivery_kind = 'final_ready'
            """,
            (host_id, original_key),
        ).fetchone()
        assert original is not None
        original_outbox_id = int(original[0])
        original_turn_id = str(original[1])
        original_revision = str(original[2])
        original_source_token = str(
            json.loads(str(original[3]))["source_turn_id"]
        )
        original_attempt = conn.execute(
            """
            SELECT id, outbox_id, delivery_key, attempt, status, delivered_at
            FROM connector_deliveries
            WHERE outbox_id = ?
            """,
            (original_outbox_id,),
        ).fetchone()
        assert original_attempt is not None
        assert original_attempt[4] == "delivered"
        conn.execute(
            """
            UPDATE connector_outbox
            SET updated_at = '2000-01-01T00:00:00+00:00'
            WHERE id = ?
            """,
            (original_outbox_id,),
        )
        conn.execute(
            """
            UPDATE connector_deliveries
            SET delivered_at = '2000-01-01T00:00:00+00:00'
            WHERE id = ?
            """,
            (int(original_attempt[0]),),
        )

    cleanup = cleanup_acknowledged_final_retention(
        db_path,
        host_id,
        acknowledged_final_retention_days=1,
        acknowledged_final_retention_count=1,
        batch_size=100,
        now="2026-07-13T00:00:00+00:00",
    )
    assert cleanup["deleted"] == 1
    with sqlite3.connect(str(db_path)) as conn:
        detached = conn.execute(
            """
            SELECT id, outbox_id, delivery_key, attempt, status, delivered_at
            FROM connector_deliveries
            WHERE host_id = ? AND delivery_key = ?
            """,
            (host_id, original_key),
        ).fetchone()
        assert detached == (
            int(original_attempt[0]),
            None,
            original_key,
            int(original_attempt[3]),
            "delivered",
            "2000-01-01T00:00:00+00:00",
        )
        assert conn.execute(
            "SELECT COUNT(*) FROM connector_outbox WHERE host_id = ?",
            (host_id,),
        ).fetchone()[0] == 0

    worker_b = owner_snapshot(STABLE_KEY, "worker-tombstone-b", "space-tombstone-b", 2)
    assert worker_b.workers[0].fingerprint != worker_a.workers[0].fingerprint
    assert save_snapshot(db_path, worker_b) is True
    assert merge_turn_content(
        db_path,
        host_id,
        "worker-tombstone-b",
        {
            "source_turn_id": raw_source,
            "assistant_final_text": "detached continuity final",
            "complete": True,
            "has_open_turn": False,
        },
        observed_at="2026-01-01T00:00:03+00:00",
    ) == 1
    assert api.poll({"name": FINAL_NAME, "limit": 10})["items"] == []

    with sqlite3.connect(str(db_path)) as conn:
        assert original_source_token.startswith("turnsrc-")
        assert conn.execute(
            """
            SELECT COUNT(*)
            FROM turns
            WHERE host_id = ?
              AND json_extract(payload_json, '$.source_turn_id') IS NOT NULL
            """,
            (host_id,),
        ).fetchone()[0] == 0
        assert conn.execute(
            "SELECT COUNT(*) FROM connector_outbox WHERE host_id = ?",
            (host_id,),
        ).fetchone()[0] == 0
        assert conn.execute(
            """
            SELECT id, outbox_id, delivery_key, attempt, status, delivered_at
            FROM connector_deliveries
            WHERE host_id = ? AND delivery_key = ?
            """,
            (host_id, original_key),
        ).fetchone() == detached

    init_store(db_path)
    assert save_snapshot(db_path, worker_b) is True
    assert merge_turn_content(
        db_path,
        host_id,
        "worker-tombstone-b",
        {
            "source_turn_id": raw_source,
            "assistant_final_text": "detached continuity final",
            "complete": True,
            "has_open_turn": False,
        },
        observed_at="2026-01-01T00:00:03+00:00",
    ) == 1
    restarted = ConnectorOutboxAPI(db_path, host_id)
    assert restarted.poll({"name": FINAL_NAME, "limit": 10})["items"] == []
    with sqlite3.connect(str(db_path)) as conn:
        assert conn.execute(
            """
            SELECT COUNT(*)
            FROM turns
            WHERE host_id = ?
              AND json_extract(payload_json, '$.source_turn_id') IS NOT NULL
            """,
            (host_id,),
        ).fetchone()[0] == 0
        assert conn.execute(
            "SELECT COUNT(*) FROM connector_outbox WHERE host_id = ?",
            (host_id,),
        ).fetchone()[0] == 0
        assert conn.execute(
            """
            SELECT id, outbox_id, delivery_key, attempt, status, delivered_at
            FROM connector_deliveries
            WHERE host_id = ? AND delivery_key = ?
            """,
            (host_id, original_key),
        ).fetchone() == detached

    owner_k2 = owner_snapshot(
        stable_key_k2,
        "worker-tombstone-b",
        "space-tombstone-b",
        4,
    )
    assert save_snapshot(db_path, owner_k2) is True
    assert merge_turn_content(
        db_path,
        host_id,
        "worker-tombstone-b",
        {
            "source_turn_id": raw_source,
            "assistant_final_text": "detached continuity final",
            "complete": True,
            "has_open_turn": False,
        },
        observed_at="2026-01-01T00:00:05+00:00",
    ) == 1
    k2_poll = restarted.poll({"name": FINAL_NAME, "limit": 10, "lease_seconds": 60})
    assert len(k2_poll["items"]) == 1
    k2_item = k2_poll["items"][0]
    assert k2_item["payload"]["stable_key"] == stable_key_k2
    assert k2_item["key"] != original_key
    assert k2_item["payload"]["turn_id"] != original_turn_id

    with sqlite3.connect(str(db_path)) as conn:
        assert conn.execute(
            """
            SELECT id, outbox_id, delivery_key, attempt, status, delivered_at
            FROM connector_deliveries
            WHERE host_id = ? AND delivery_key = ?
            """,
            (host_id, original_key),
        ).fetchone() == detached
        public_payloads = [
            str(row[0])
            for row in conn.execute(
                """
                SELECT payload_json FROM turns WHERE host_id = ?
                UNION ALL
                SELECT payload_json FROM connector_outbox WHERE host_id = ?
                """,
                (host_id, host_id),
            ).fetchall()
        ]
        assert conn.execute("PRAGMA foreign_key_check").fetchall() == []
    public_surface = json.dumps(
        {"poll": k2_poll, "payloads": public_payloads},
        sort_keys=True,
    )
    assert private_route not in public_surface
    assert raw_source not in public_surface
