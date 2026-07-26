"""Transactional retention contracts for authoritative snapshot projection."""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest

from tendwire.connectors import ConnectorOutboxAPI
from tendwire.config import Config
from tendwire.core.models import Snapshot, WorkerBinding
from tendwire.core.projector import project_from_raw
from tendwire.store import sqlite as store_sqlite
from tendwire.store.sqlite import (
    init_store,
    latest_snapshot,
    list_worker_bindings,
    merge_turn_content,
    save_snapshot,
    upsert_command_pending_turn,
    turns_payload_from_store,
    upsert_worker_bindings,
)


HOST_ID = "snapshot-retention-host"
WORKER_ID = "worker-1"
FINAL_TEXT = "authoritative snapshot final"
USER_TEXT = "authoritative snapshot prompt"
STABLE_KEY_A = "wsk1_" + ("e" * 64)
STABLE_KEY_B = "wsk1_" + ("f" * 64)
FINAL_NAME = "turn-final"
CONTINUITY_RAW_SOURCE = "backend-source-stable-42"
CONTINUITY_FINAL = "abcdefghijkl"
PRIVATE_ROUTE_A = "private-route-worker-a"
PRIVATE_ROUTE_B = "private-route-worker-b"
FROZEN_LEGACY_SOURCE_A = "turnsrc-840f2677167ab65aa0655a39"


def _owner_snapshot(
    db_path: Path,
    *,
    worker_id: str,
    stable_key: str | None,
    space_id: str,
    second: int,
) -> Snapshot:
    meta: dict[str, object] = {}
    if stable_key is not None:
        meta = {"stable_key": stable_key, "stable_key_version": 1}
    return project_from_raw(
        Config(host_id=HOST_ID, db_path=db_path),
        workers=[
            {
                "id": worker_id,
                "name": f"Continuity {worker_id}",
                "status": "active",
                "space_id": space_id,
                "meta": meta,
            }
        ],
        timestamp=datetime(2026, 1, 2, 0, 0, second, tzinfo=timezone.utc),
    )


def _private_binding(snapshot: Snapshot, route: str) -> WorkerBinding:
    worker = snapshot.workers[0]
    return WorkerBinding(
        host_id=HOST_ID,
        worker_id=worker.id,
        worker_fingerprint=worker.fingerprint,
        backend="herdr",
        target_kind="terminal_id",
        target_value=route,
        turn_target_kind="terminal_id",
        turn_target_value=route,
        sendable=True,
    )


def _source_turn_payloads(db_path: Path) -> list[tuple[str, dict[str, Any], int]]:
    with sqlite3.connect(str(db_path)) as conn:
        rows = conn.execute(
            """
            SELECT turn_id, payload_json, list_sequence
            FROM turns
            WHERE host_id = ?
            ORDER BY turn_id
            """,
            (HOST_ID,),
        ).fetchall()
    return [
        (str(turn_id), json.loads(str(raw_payload)), int(list_sequence))
        for turn_id, raw_payload, list_sequence in rows
        if json.loads(str(raw_payload)).get("source_turn_id")
    ]


def _continuity_graph_snapshot(db_path: Path) -> dict[str, Any]:
    graph_tables = (
        "turn_content_revisions",
        "turn_content_page_boundaries",
        "connector_outbox",
        "connector_deliveries",
        "turn_presentation_plans",
        "turn_presentation_jobs",
        "turn_presentation_recoveries",
    )
    with sqlite3.connect(str(db_path)) as conn:
        source_identities = []
        for turn_id, raw_payload, list_sequence in conn.execute(
            """
            SELECT turn_id, payload_json, list_sequence
            FROM turns
            WHERE host_id = ?
            ORDER BY turn_id
            """,
            (HOST_ID,),
        ).fetchall():
            payload = json.loads(str(raw_payload))
            if not payload.get("source_turn_id"):
                continue
            meta = payload.get("meta") if isinstance(payload.get("meta"), dict) else {}
            source_identities.append(
                (
                    str(turn_id),
                    str(payload.get("id") or ""),
                    str(payload.get("source_turn_id") or ""),
                    str(payload.get("origin_command_id") or ""),
                    str(meta.get("stable_key") or ""),
                    meta.get("stable_key_version"),
                    int(list_sequence),
                )
            )
        snapshot: dict[str, Any] = {
            "source_identities": tuple(source_identities),
            "list_state": tuple(conn.execute("SELECT * FROM turn_list_state").fetchall()),
            "list_hosts": tuple(
                conn.execute(
                    "SELECT * FROM turn_list_hosts WHERE host_id = ? ORDER BY host_id",
                    (HOST_ID,),
                ).fetchall()
            ),
        }
        for table in graph_tables:
            snapshot[table] = tuple(
                conn.execute(f"SELECT * FROM {table} ORDER BY rowid").fetchall()
            )
        return snapshot


def _turn_rows_snapshot(db_path: Path) -> tuple[tuple[Any, ...], ...]:
    with sqlite3.connect(str(db_path)) as conn:
        return tuple(
            conn.execute(
                "SELECT * FROM turns WHERE host_id = ? ORDER BY turn_id",
                (HOST_ID,),
            ).fetchall()
        )


def _assert_continuity_integrity(db_path: Path) -> None:
    with sqlite3.connect(str(db_path)) as conn:
        assert conn.execute("PRAGMA user_version").fetchone() == (
            store_sqlite.STORE_SCHEMA_VERSION,
        ) == (19,)
        assert conn.execute("PRAGMA foreign_key_check").fetchall() == []
        current_counts = conn.execute(
            """
            SELECT turn_id, SUM(is_current)
            FROM turn_content_revisions
            WHERE host_id = ?
            GROUP BY turn_id
            ORDER BY turn_id
            """,
            (HOST_ID,),
        ).fetchall()
        assert current_counts
        assert all(int(count) == 1 for _turn_id, count in current_counts)


def _merge_continuity_final(
    db_path: Path,
    snapshot: Snapshot,
    *,
    worker_id: str,
    observed_at: str,
) -> dict[str, Any]:
    assert merge_turn_content(
        db_path,
        HOST_ID,
        worker_id,
        {
            "source_turn_id": CONTINUITY_RAW_SOURCE,
            "assistant_final_text": CONTINUITY_FINAL,
            "complete": True,
            "has_open_turn": False,
        },
        observed_at=observed_at,
    ) == 1
    public_page = turns_payload_from_store(
        db_path,
        HOST_ID,
        snapshot=snapshot,
        schema_version=2,
        limit=250,
    )
    matches = [
        turn
        for turn in public_page["turns"]
        if turn.get("source_turn_id")
        and turn["content"]["fields"]["assistant_final_text"]["availability"]
        == "complete"
    ]
    assert len(matches) == 1
    return matches[0]


def _poll_one_final(api: ConnectorOutboxAPI) -> dict[str, Any]:
    response = api.poll({"name": FINAL_NAME, "limit": 100, "lease_seconds": 60})
    assert response["ok"] is True
    assert len(response["items"]) == 1
    return response["items"][0]


def _prepare_recoverable_graph(
    api: ConnectorOutboxAPI,
    source: dict[str, Any],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    payload = source["payload"]
    begun = api.prepare(
        {
            "schema_version": 1,
            "action": "begin",
            "name": FINAL_NAME,
            "turn_id": payload["turn_id"],
            "content_revision": payload["content_revision"],
            "presentation_version": "owner-continuity-v11",
            "part_count": 3,
            "source_ref": source["ref"],
        }
    )
    assert begun["ok"] is True
    token = begun["plan_token"]
    for ordinal, (start, end) in enumerate(((0, 4), (4, 8), (8, 12))):
        staged = api.prepare(
            {
                "schema_version": 1,
                "action": "part",
                "name": FINAL_NAME,
                "plan_token": token,
                "ordinal": ordinal,
                "spans": [
                    {
                        "field": "assistant_final_text",
                        "start_char": start,
                        "end_char": end,
                    }
                ],
            }
        )
        assert staged["ok"] is True
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
    assert committed["state"] == "active"

    public_responses: list[dict[str, Any]] = [begun, committed]
    first = _poll_one_final(api)
    first_ack = api.ack({"name": FINAL_NAME, "ref": first["ref"]})
    assert first_ack["status"] == "acknowledged"
    public_responses.extend((first, first_ack))

    failed = _poll_one_final(api)
    failed_response = api.fail(
        {
            "name": FINAL_NAME,
            "ref": failed["ref"],
            "reason": "continuity recovery fixture",
            "delay_seconds": 0,
        }
    )
    assert failed_response["status"] == "attempts_exhausted"
    public_responses.extend((failed, failed_response))

    recovered = api.prepare(
        {
            "schema_version": 1,
            "action": "recover",
            "name": FINAL_NAME,
            "failed_plan_token": token,
            "request_id": "owner-continuity-recovery",
        }
    )
    assert recovered["ok"] is True
    public_responses.append(recovered)
    for sequence_index in (1, 2):
        item = _poll_one_final(api)
        assert item["payload"]["sequence_index"] == sequence_index
        acknowledged = api.ack({"name": FINAL_NAME, "ref": item["ref"]})
        assert acknowledged["status"] == "acknowledged"
        public_responses.extend((item, acknowledged))
    assert api.poll({"name": FINAL_NAME, "limit": 100})["items"] == []
    return recovered, public_responses


def _freeze_source_token(db_path: Path, frozen_token: str) -> str:
    rows = _source_turn_payloads(db_path)
    assert len(rows) == 1
    turn_id, payload, _list_sequence = rows[0]
    payload["source_turn_id"] = frozen_token
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            """
            UPDATE turns
            SET payload_json = ?
            WHERE host_id = ? AND turn_id = ?
            """,
            (json.dumps(payload, sort_keys=True), HOST_ID, turn_id),
        )
    return turn_id


def _clone_ambiguous_source_alias(db_path: Path) -> str:
    duplicate_turn_id = "turn-existing-v11-legacy-alias"
    rows = _source_turn_payloads(db_path)
    assert len(rows) == 1
    original_turn_id, payload, _list_sequence = rows[0]
    assert payload.get("source") == "worker:worker-a"
    frozen_token = FROZEN_LEGACY_SOURCE_A
    payload["id"] = duplicate_turn_id
    payload["source_turn_id"] = frozen_token
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute("PRAGMA foreign_keys = ON")
        original = conn.execute(
            """
            SELECT worker_id, worker_fingerprint, space_id, status, kind,
                   updated_at, snapshot_content_fingerprint, observed_at
            FROM turns
            WHERE host_id = ? AND turn_id = ?
            """,
            (HOST_ID, original_turn_id),
        ).fetchone()
        current = conn.execute(
            """
            SELECT user_text, assistant_final_text, user_state, final_state
            FROM turn_content_revisions
            WHERE host_id = ? AND turn_id = ? AND is_current = 1
            """,
            (HOST_ID, original_turn_id),
        ).fetchone()
        assert original is not None
        assert current is not None
        list_sequence = store_sqlite._turn_list_sequence_conn(
            conn,
            HOST_ID,
            duplicate_turn_id,
        )
        conn.execute(
            """
            INSERT INTO turns (
                host_id, turn_id, worker_id, worker_fingerprint, space_id,
                status, kind, updated_at, fingerprint,
                snapshot_content_fingerprint, observed_at, payload_json,
                list_sequence
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                HOST_ID,
                duplicate_turn_id,
                *original[:6],
                f"frozen-fingerprint-{duplicate_turn_id}",
                original[6],
                original[7],
                json.dumps(payload, sort_keys=True),
                list_sequence,
            ),
        )
        revision = store_sqlite._insert_turn_content_revision_conn(
            conn,
            host_id=HOST_ID,
            turn_id=duplicate_turn_id,
            user_text=current[0],
            assistant_final_text=current[1],
            user_state=str(current[2]),
            final_state=str(current[3]),
            created_at="2026-01-02T00:00:05+00:00",
        )
        assert store_sqlite._ensure_final_ready_anchor_conn(
            conn,
            host_id=HOST_ID,
            turn_id=duplicate_turn_id,
            content_revision_value=revision,
            now="2026-01-02T00:00:05+00:00",
        ) is not None
    return duplicate_turn_id


def _snapshot(db_path: Path, *, status: str = "active", second: int = 0) -> Snapshot:
    return project_from_raw(
        Config(host_id=HOST_ID, db_path=db_path),
        workers=[
            {
                "id": WORKER_ID,
                "name": "Projection Worker",
                "status": status,
                "space_id": "space-1",
                "meta": {
                    "stable_key": STABLE_KEY_A,
                    "stable_key_version": 1,
                },
            }
        ],
        timestamp=datetime(2026, 1, 1, 0, 0, second, tzinfo=timezone.utc),
    )


def _empty_snapshot(db_path: Path, *, second: int) -> Snapshot:
    return project_from_raw(
        Config(host_id=HOST_ID, db_path=db_path),
        workers=[],
        timestamp=datetime(2026, 1, 1, 0, 0, second, tzinfo=timezone.utc),
    )


def _seed_complete_final(db_path: Path, snapshot: Snapshot) -> tuple[str, str]:
    init_store(db_path)
    save_snapshot(db_path, snapshot)
    assert merge_turn_content(
        db_path,
        HOST_ID,
        WORKER_ID,
        {
            "user_text": USER_TEXT,
            "assistant_final_text": FINAL_TEXT,
            "complete": True,
            "has_open_turn": False,
        },
        observed_at="2026-01-01T00:00:01+00:00",
    ) == 1
    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT turns.turn_id, revisions.content_revision
            FROM turns
            JOIN turn_content_revisions AS revisions
              ON revisions.host_id = turns.host_id
             AND revisions.turn_id = turns.turn_id
             AND revisions.is_current = 1
            WHERE turns.host_id = ? AND turns.worker_id = ?
            """,
            (HOST_ID, WORKER_ID),
        ).fetchone()
    assert row is not None
    return str(row[0]), str(row[1])


def test_snapshot_complete_final_has_one_neutral_idempotent_anchor(tmp_path: Path) -> None:
    db_path = tmp_path / "snapshot-final-anchor.db"
    original = _snapshot(db_path)
    turn_id, revision = _seed_complete_final(db_path, original)

    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "DELETE FROM connector_outbox WHERE host_id = ? AND turn_id = ?",
            (HOST_ID, turn_id),
        )
        conn.commit()

    rewritten = Snapshot.from_dict(
        {**original.to_dict(), "updated_at": "2026-01-01T00:00:02+00:00"}
    )
    save_snapshot(db_path, rewritten)
    save_snapshot(db_path, rewritten)

    with sqlite3.connect(db_path) as conn:
        anchors = conn.execute(
            """
            SELECT connector, delivery_kind, turn_id, content_revision, payload_json
            FROM connector_outbox
            WHERE host_id = ? AND delivery_kind = 'final_ready'
            """,
            (HOST_ID,),
        ).fetchall()

    assert len(anchors) == 1
    connector, delivery_kind, anchored_turn, anchored_revision, raw_payload = anchors[0]
    assert (connector, delivery_kind, anchored_turn, anchored_revision) == (
        "turn-final",
        "final_ready",
        turn_id,
        revision,
    )
    payload = json.loads(raw_payload)
    assert payload["operation"] == "materialize"
    assert payload["turn_id"] == turn_id
    assert payload["content_revision"] == revision
    assert payload["schema_version"] == 2
    assert payload["stable_key"] == STABLE_KEY_A
    assert payload["stable_key_version"] == 1
    encoded = json.dumps(payload, sort_keys=True)
    assert FINAL_TEXT not in encoded
    assert USER_TEXT not in encoded
    assert payload["content"]["fields"]["assistant_final_text"]["inline"] is False
    assert payload["content"]["fields"]["user_text"]["inline"] is False


@pytest.mark.parametrize(
    "invalid_meta",
    [
        {},
        {"stable_key": STABLE_KEY_A},
        {"stable_key": "wsk1_invalid", "stable_key_version": 1},
        {"stable_key": STABLE_KEY_A, "stable_key_version": True},
        {"stable_key": STABLE_KEY_A, "stable_key_version": 2},
    ],
)
def test_final_anchor_fails_closed_without_public_stable_key_pair(
    tmp_path: Path,
    invalid_meta: dict[str, object],
) -> None:
    db_path = tmp_path / "snapshot-final-stable-key.db"
    original = _snapshot(db_path)
    turn_id, revision = _seed_complete_final(db_path, original)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "DELETE FROM connector_outbox WHERE host_id = ? AND turn_id = ?",
            (HOST_ID, turn_id),
        )
        raw_payload = conn.execute(
            """
            SELECT payload_json
            FROM turns
            WHERE host_id = ? AND turn_id = ?
            """,
            (HOST_ID, turn_id),
        ).fetchone()[0]
        turn_payload = json.loads(str(raw_payload))
        turn_payload["meta"] = invalid_meta
        conn.execute(
            """
            UPDATE turns
            SET payload_json = ?
            WHERE host_id = ? AND turn_id = ?
            """,
            (json.dumps(turn_payload, sort_keys=True), HOST_ID, turn_id),
        )
        anchor_id = store_sqlite._ensure_final_ready_anchor_conn(
            conn,
            host_id=HOST_ID,
            turn_id=turn_id,
            content_revision_value=revision,
            now="2026-01-01T00:00:02+00:00",
        )
        anchor = conn.execute(
            """
            SELECT delivery_kind, status, payload_json
            FROM connector_outbox
            WHERE host_id = ? AND turn_id = ?
            """,
            (HOST_ID, turn_id),
        ).fetchone()

    assert anchor_id is not None
    assert anchor is not None
    assert anchor[0:2] == ("final_migration_hold", "dead_letter")
    hold_payload = json.loads(str(anchor[2]))
    assert hold_payload["schema_version"] == 1
    assert "stable_key" not in hold_payload
    assert "stable_key_version" not in hold_payload


def test_missing_identity_merge_persists_nonpollable_hold_on_current_placeholder(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "missing-identity-merge.db"
    config = Config(host_id=HOST_ID, db_path=db_path)
    snapshot = project_from_raw(
        config,
        workers=[
            {
                "id": WORKER_ID,
                "name": "Worker Missing Identity",
                "status": "active",
                "kind": "worker",
            }
        ],
    )
    init_store(db_path)
    assert save_snapshot(db_path, snapshot) is True

    changed = merge_turn_content(
        db_path,
        HOST_ID,
        WORKER_ID,
        {
            "source_turn_id": "turnsrc-missing-identity",
            "assistant_final_text": "must remain durably held",
        },
        observed_at="2026-01-01T00:00:01+00:00",
    )
    assert changed == 1
    with sqlite3.connect(str(db_path)) as conn:
        anchor = conn.execute(
            """
            SELECT outbox.delivery_kind, outbox.status, outbox.payload_json,
                   turns.payload_json
            FROM connector_outbox AS outbox
            JOIN turns
              ON turns.host_id = outbox.host_id
             AND turns.turn_id = outbox.turn_id
            WHERE outbox.host_id = ?
            """,
            (HOST_ID,),
        ).fetchone()

    assert anchor is not None
    assert anchor[0:2] == ("final_migration_hold", "dead_letter")
    hold_payload = json.loads(str(anchor[2]))
    turn_payload = json.loads(str(anchor[3]))
    assert hold_payload["schema_version"] == 1
    assert "stable_key" not in hold_payload
    assert turn_payload["source_turn_id"].startswith("turnsrc-")


def test_worker_id_reuse_binds_each_root_to_immutable_stable_key(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "snapshot-worker-reuse.db"
    config = Config(host_id=HOST_ID, db_path=db_path)
    first = project_from_raw(
        config,
        workers=[
            {
                "id": WORKER_ID,
                "name": "Worker Instance One",
                "status": "active",
                "space_id": "space-1",
                "meta": {
                    "stable_key": STABLE_KEY_A,
                    "stable_key_version": 1,
                },
            }
        ],
        timestamp=datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
    )
    init_store(db_path)
    save_snapshot(db_path, first)
    assert merge_turn_content(
        db_path,
        HOST_ID,
        WORKER_ID,
        {
            "assistant_final_text": "first instance final",
            "complete": True,
            "has_open_turn": False,
            "source_turn_id": "instance-one-source",
        },
        observed_at="2026-01-01T00:00:01+00:00",
    ) == 1

    second = project_from_raw(
        config,
        workers=[
            {
                "id": WORKER_ID,
                "name": "Worker Instance Two",
                "status": "active",
                "space_id": "space-1",
                "meta": {
                    "stable_key": STABLE_KEY_B,
                    "stable_key_version": 1,
                },
            }
        ],
        timestamp=datetime(2026, 1, 1, 0, 0, 2, tzinfo=timezone.utc),
    )
    assert second.workers[0].fingerprint != first.workers[0].fingerprint
    save_snapshot(db_path, second)
    assert merge_turn_content(
        db_path,
        HOST_ID,
        WORKER_ID,
        {
            "assistant_final_text": "second instance final",
            "complete": True,
            "has_open_turn": False,
            "source_turn_id": "instance-two-source",
        },
        observed_at="2026-01-01T00:00:03+00:00",
    ) == 1

    with sqlite3.connect(db_path) as conn:
        roots = [
            json.loads(str(row[0]))
            for row in conn.execute(
                """
                SELECT payload_json
                FROM connector_outbox
                WHERE host_id = ? AND delivery_kind = 'final_ready'
                ORDER BY id
                """,
                (HOST_ID,),
            ).fetchall()
        ]
        persisted = {
            str(turn_id): json.loads(str(payload_json)).get("meta", {}).get("stable_key")
            for turn_id, payload_json in conn.execute(
                """
                SELECT turn_id, payload_json
                FROM turns
                WHERE host_id = ?
                """,
                (HOST_ID,),
            ).fetchall()
        }

    assert len(roots) == 2
    assert {root["stable_key"] for root in roots} == {
        STABLE_KEY_A,
        STABLE_KEY_B,
    }
    assert all(root["schema_version"] == 2 for root in roots)
    assert all(root["stable_key_version"] == 1 for root in roots)
    for root in roots:
        assert root["stable_key"] == persisted[root["turn_id"]]


@pytest.mark.parametrize("old_status", ["queued", "delivered"])
def test_same_source_content_isolated_across_owner_change_and_restart(
    tmp_path: Path,
    old_status: str,
) -> None:
    db_path = tmp_path / f"same-source-owner-{old_status}.db"
    config = Config(host_id=HOST_ID, db_path=db_path)

    def owner_snapshot(stable_key: str, second: int) -> Snapshot:
        return project_from_raw(
            config,
            workers=[
                {
                    "id": WORKER_ID,
                    "name": "Reused Worker",
                    "status": "active",
                    "meta": {
                        "stable_key": stable_key,
                        "stable_key_version": 1,
                    },
                }
            ],
            timestamp=datetime(2026, 1, 1, 0, 0, second, tzinfo=timezone.utc),
        )

    first = owner_snapshot(STABLE_KEY_A, 0)
    init_store(db_path)
    assert save_snapshot(db_path, first) is True
    content = {
        "source_turn_id": "same-backend-source",
        "assistant_final_text": "identical owner-sensitive final",
        "complete": True,
        "has_open_turn": False,
    }
    assert merge_turn_content(
        db_path,
        HOST_ID,
        WORKER_ID,
        content,
        observed_at="2026-01-01T00:00:01+00:00",
    ) == 1
    with sqlite3.connect(str(db_path)) as conn:
        first_root = conn.execute(
            """
            SELECT id, turn_id
            FROM connector_outbox
            WHERE host_id = ? AND delivery_kind = 'final_ready'
            """,
            (HOST_ID,),
        ).fetchone()
        assert first_root is not None
        if old_status == "delivered":
            conn.execute(
                """
                UPDATE connector_outbox
                SET status = 'delivered'
                WHERE id = ?
                """,
                (int(first_root[0]),),
            )

    second = owner_snapshot(STABLE_KEY_B, 2)
    assert save_snapshot(db_path, second) is True
    assert merge_turn_content(
        db_path,
        HOST_ID,
        WORKER_ID,
        content,
        observed_at="2026-01-01T00:00:03+00:00",
    ) == 1
    init_store(db_path)
    assert save_snapshot(db_path, second) is True
    assert merge_turn_content(
        db_path,
        HOST_ID,
        WORKER_ID,
        content,
        observed_at="2026-01-01T00:00:04+00:00",
    ) == 0

    with sqlite3.connect(str(db_path)) as conn:
        roots = [
            (str(turn_id), str(status), json.loads(str(payload_json)))
            for turn_id, status, payload_json in conn.execute(
                """
                SELECT turn_id, status, payload_json
                FROM connector_outbox
                WHERE host_id = ? AND delivery_kind = 'final_ready'
                ORDER BY id
                """,
                (HOST_ID,),
            ).fetchall()
        ]
    assert len(roots) == 2
    assert roots[0][0] != roots[1][0]
    assert roots[0][1] == old_status
    assert roots[1][1] == "queued"
    assert [root[2]["stable_key"] for root in roots] == [
        STABLE_KEY_A,
        STABLE_KEY_B,
    ]


def test_missing_owner_replay_holds_without_overwriting_existing_owner_root(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "same-source-missing-owner.db"
    config = Config(host_id=HOST_ID, db_path=db_path)
    first = project_from_raw(
        config,
        workers=[
            {
                "id": WORKER_ID,
                "name": "Owned Worker",
                "status": "active",
                "meta": {
                    "stable_key": STABLE_KEY_A,
                    "stable_key_version": 1,
                },
            }
        ],
        timestamp=datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
    )
    init_store(db_path)
    assert save_snapshot(db_path, first) is True
    content = {
        "source_turn_id": "same-backend-source",
        "assistant_final_text": "same final during continuity loss",
        "complete": True,
        "has_open_turn": False,
    }
    assert merge_turn_content(
        db_path,
        HOST_ID,
        WORKER_ID,
        content,
        observed_at="2026-01-01T00:00:01+00:00",
    ) == 1

    missing = project_from_raw(
        config,
        workers=[
            {
                "id": WORKER_ID,
                "name": "Unowned Worker",
                "status": "active",
            }
        ],
        timestamp=datetime(2026, 1, 1, 0, 0, 2, tzinfo=timezone.utc),
    )
    assert save_snapshot(db_path, missing) is True
    assert merge_turn_content(
        db_path,
        HOST_ID,
        WORKER_ID,
        content,
        observed_at="2026-01-01T00:00:03+00:00",
    ) == 1
    init_store(db_path)
    assert save_snapshot(db_path, missing) is True
    assert merge_turn_content(
        db_path,
        HOST_ID,
        WORKER_ID,
        content,
        observed_at="2026-01-01T00:00:04+00:00",
    ) == 0

    with sqlite3.connect(str(db_path)) as conn:
        rows = conn.execute(
            """
            SELECT delivery_kind, status, payload_json
            FROM connector_outbox
            WHERE host_id = ?
            ORDER BY id
            """,
            (HOST_ID,),
        ).fetchall()
    assert [(str(row[0]), str(row[1])) for row in rows] == [
        ("final_ready", "queued"),
        ("final_migration_hold", "dead_letter"),
    ]
    assert json.loads(str(rows[0][2]))["stable_key"] == STABLE_KEY_A
    assert json.loads(str(rows[1][2]))["schema_version"] == 1


def test_equal_public_snapshot_replay_cannot_replace_private_binding_route(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "equal-public-private-route.db"
    config = Config(host_id=HOST_ID, db_path=db_path)
    snapshot = project_from_raw(
        config,
        workers=[
            {
                "id": WORKER_ID,
                "name": "Equal Public Worker",
                "status": "active",
                "meta": {
                    "stable_key": STABLE_KEY_A,
                    "stable_key_version": 1,
                },
            }
        ],
        timestamp=datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
    )
    private_identity = "binding-private-identity"
    first_binding = WorkerBinding(
        host_id=HOST_ID,
        worker_id=WORKER_ID,
        worker_fingerprint=snapshot.workers[0].fingerprint,
        backend="herdr",
        target_kind="terminal_id",
        target_value="first-private-target",
        sendable=True,
        observed_at=snapshot.updated_at,
        private_fingerprint=private_identity,
    )
    conflicting_binding = WorkerBinding(
        host_id=HOST_ID,
        worker_id=WORKER_ID,
        worker_fingerprint=snapshot.workers[0].fingerprint,
        backend="herdr",
        target_kind="terminal_id",
        target_value="conflicting-private-target",
        sendable=True,
        observed_at=snapshot.updated_at,
        private_fingerprint=private_identity,
    )
    init_store(db_path)
    assert save_snapshot(
        db_path,
        snapshot,
        worker_bindings=[first_binding],
        binding_backend="herdr",
        binding_observation_authoritative=True,
    ) is True
    assert save_snapshot(
        db_path,
        snapshot,
        worker_bindings=[conflicting_binding],
        binding_backend="herdr",
        binding_observation_authoritative=True,
    ) is True

    stored = list_worker_bindings(
        db_path,
        HOST_ID,
        backend="herdr",
        include_expired=True,
    )
    assert len(stored) == 1
    assert stored[0].target_value == "first-private-target"


def test_snapshot_projection_and_anchor_roll_back_together(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    db_path = tmp_path / "snapshot-projection-rollback.db"
    original = _snapshot(db_path)
    turn_id, _revision = _seed_complete_final(db_path, original)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "DELETE FROM connector_outbox WHERE host_id = ? AND turn_id = ?",
            (HOST_ID, turn_id),
        )
        conn.commit()

    def fail_after_projection(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError("fail after snapshot projection")

    monkeypatch.setattr(
        store_sqlite,
        "_apply_attention_observation_conn",
        fail_after_projection,
    )
    with pytest.raises(RuntimeError, match="fail after snapshot projection"):
        save_snapshot(db_path, _snapshot(db_path, status="waiting", second=3))

    with sqlite3.connect(db_path) as conn:
        status = conn.execute(
            "SELECT status FROM turns WHERE host_id = ? AND turn_id = ?",
            (HOST_ID, turn_id),
        ).fetchone()
        anchor_count = conn.execute(
            """
            SELECT COUNT(*)
            FROM connector_outbox
            WHERE host_id = ? AND turn_id = ? AND delivery_kind = 'final_ready'
            """,
            (HOST_ID, turn_id),
        ).fetchone()[0]
        snapshot_count = conn.execute(
            "SELECT COUNT(*) FROM snapshots WHERE host_id = ?", (HOST_ID,)
        ).fetchone()[0]

    assert status == ("active",)
    assert anchor_count == 0
    assert snapshot_count == 1


def test_snapshot_omission_preserves_real_command_and_source_provenance(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "snapshot-provenance.db"
    original = _snapshot(db_path)
    init_store(db_path)
    save_snapshot(db_path, original)
    pending = upsert_command_pending_turn(
        db_path,
        HOST_ID,
        original.workers[0],
        request_id="command-retained",
        instruction_text=USER_TEXT,
        observed_at="2026-01-01T00:00:01+00:00",
    )
    assert pending is not None
    assert merge_turn_content(
        db_path,
        HOST_ID,
        WORKER_ID,
        {
            "user_text": USER_TEXT,
            "assistant_final_text": FINAL_TEXT,
            "complete": True,
            "has_open_turn": False,
            "source_turn_id": "backend-source-retained",
        },
        observed_at="2026-01-01T00:00:02+00:00",
    ) == 1

    with sqlite3.connect(db_path) as conn:
        source_turns = [
            (str(turn_id), json.loads(str(raw_payload)))
            for turn_id, raw_payload in conn.execute(
                "SELECT turn_id, payload_json FROM turns WHERE host_id = ?",
                (HOST_ID,),
            ).fetchall()
            if json.loads(str(raw_payload)).get("source_turn_id")
        ]
    assert len(source_turns) == 1
    turn_id, source_payload = source_turns[0]
    assert source_payload["origin_command_id"] == "command-retained"
    source_identity = str(source_payload["source_turn_id"])
    assert source_identity.startswith("turnsrc-")

    save_snapshot(db_path, _snapshot(db_path, status="waiting", second=4))
    save_snapshot(db_path, _empty_snapshot(db_path, second=6))

    with sqlite3.connect(db_path) as conn:
        surviving = conn.execute(
            """
            SELECT payload_json
            FROM turns
            WHERE host_id = ? AND turn_id = ?
            """,
            (HOST_ID, turn_id),
        ).fetchone()
        anchor_count = conn.execute(
            """
            SELECT COUNT(*)
            FROM connector_outbox
            WHERE host_id = ?
              AND turn_id = ?
              AND delivery_kind = 'final_ready'
            """,
            (HOST_ID, turn_id),
        ).fetchone()[0]
    assert surviving is not None
    surviving_payload = json.loads(str(surviving[0]))
    assert surviving_payload["origin_command_id"] == "command-retained"
    assert surviving_payload["source_turn_id"] == source_identity
    assert anchor_count == 1


def test_stale_different_fingerprint_snapshot_cannot_regress_or_prune_projection(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "snapshot-stale-order.db"
    original = _snapshot(db_path, status="active", second=0)
    turn_id, _revision = _seed_complete_final(db_path, original)
    newer = _snapshot(db_path, status="waiting", second=8)
    save_snapshot(db_path, newer)

    save_snapshot(db_path, _snapshot(db_path, status="active", second=4))
    save_snapshot(db_path, _empty_snapshot(db_path, second=5))

    with sqlite3.connect(db_path) as conn:
        worker_status = conn.execute(
            "SELECT status FROM workers WHERE host_id = ?",
            (HOST_ID,),
        ).fetchone()
        turn_status = conn.execute(
            "SELECT status FROM turns WHERE host_id = ? AND turn_id = ?",
            (HOST_ID, turn_id),
        ).fetchone()
        snapshot_count = conn.execute(
            "SELECT COUNT(*) FROM snapshots WHERE host_id = ?",
            (HOST_ID,),
        ).fetchone()[0]
        anchor_count = conn.execute(
            """
            SELECT COUNT(*)
            FROM connector_outbox
            WHERE host_id = ?
              AND turn_id = ?
              AND delivery_kind = 'final_ready'
            """,
            (HOST_ID, turn_id),
        ).fetchone()[0]
    latest = latest_snapshot(db_path, HOST_ID)

    assert worker_status == ("waiting",)
    assert turn_status == ("waiting",)
    assert snapshot_count == 2
    assert anchor_count == 1
    assert latest is not None
    assert latest.updated_at == newer.updated_at


def test_same_owner_exact_source_survives_worker_fingerprint_space_and_source_churn_without_rekey_or_repost(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "existing-v12-owner-continuity.db"
    first = _owner_snapshot(
        db_path,
        worker_id="worker-a",
        stable_key=STABLE_KEY_A,
        space_id="space-a",
        second=0,
    )
    init_store(db_path)
    assert save_snapshot(db_path, first) is True
    upsert_worker_bindings(db_path, [_private_binding(first, PRIVATE_ROUTE_A)])
    public_turn = _merge_continuity_final(
        db_path,
        first,
        worker_id="worker-a",
        observed_at="2026-01-02T00:00:01+00:00",
    )
    stored_turn_id = str(public_turn["id"])
    stored_revision = str(public_turn["content"]["content_revision"])
    stored_source_token = str(public_turn["source_turn_id"])
    assert stored_source_token.startswith("turnsrc-")
    assert CONTINUITY_RAW_SOURCE not in stored_source_token

    api = ConnectorOutboxAPI(db_path, HOST_ID, max_attempts=1)
    source = _poll_one_final(api)
    assert source["payload"]["turn_id"] == stored_turn_id
    assert source["payload"]["content_revision"] == stored_revision
    assert source["payload"]["final_identity"].startswith("twfinal1.")
    recovered, delivery_surfaces = _prepare_recoverable_graph(api, source)
    assert recovered["plan_token"].startswith("twplan1.")

    before = _continuity_graph_snapshot(db_path)
    assert len(before["source_identities"]) == 1
    assert before["source_identities"][0][0:3] == (
        stored_turn_id,
        stored_turn_id,
        stored_source_token,
    )
    assert before["turn_presentation_plans"]
    assert before["turn_presentation_jobs"]
    assert len(before["turn_presentation_recoveries"]) == 1
    assert before["connector_deliveries"]
    _assert_continuity_integrity(db_path)

    second = _owner_snapshot(
        db_path,
        worker_id="worker-b",
        stable_key=STABLE_KEY_A,
        space_id="space-b",
        second=10,
    )
    assert first.workers[0].fingerprint != second.workers[0].fingerprint
    assert save_snapshot(db_path, second) is True
    upsert_worker_bindings(db_path, [_private_binding(second, PRIVATE_ROUTE_B)])
    assert merge_turn_content(
        db_path,
        HOST_ID,
        "worker-b",
        {
            "source_turn_id": CONTINUITY_RAW_SOURCE,
            "assistant_final_text": CONTINUITY_FINAL,
            "complete": True,
            "has_open_turn": False,
        },
        observed_at="2026-01-02T00:00:11+00:00",
    ) == 1

    listed = turns_payload_from_store(
        db_path,
        HOST_ID,
        snapshot=second,
        schema_version=2,
        limit=250,
    )
    current = next(turn for turn in listed["turns"] if turn["id"] == stored_turn_id)
    assert current["worker_id"] == "worker-b"
    assert current["worker_fingerprint"] == second.workers[0].fingerprint
    assert current["space_id"] == "space-b"
    assert current["source_turn_id"] == stored_source_token
    assert current["content"]["content_revision"] == stored_revision

    after = _continuity_graph_snapshot(db_path)
    assert after == before
    assert api.poll({"name": FINAL_NAME, "limit": 100})["items"] == []
    public_encoded = json.dumps(
        {
            "source": source,
            "delivery": delivery_surfaces,
            "listed": listed,
        },
        sort_keys=True,
    )
    for private_value in (
        CONTINUITY_RAW_SOURCE,
        PRIVATE_ROUTE_A,
        PRIVATE_ROUTE_B,
    ):
        assert private_value not in public_encoded
    _assert_continuity_integrity(db_path)

    init_store(db_path)
    assert save_snapshot(db_path, second) is True
    assert merge_turn_content(
        db_path,
        HOST_ID,
        "worker-b",
        {
            "source_turn_id": CONTINUITY_RAW_SOURCE,
            "assistant_final_text": CONTINUITY_FINAL,
            "complete": True,
            "has_open_turn": False,
        },
        observed_at="2026-01-02T00:00:11+00:00",
    ) == 0
    restarted = ConnectorOutboxAPI(db_path, HOST_ID, max_attempts=1)
    assert restarted.poll({"name": FINAL_NAME, "limit": 100})["items"] == []
    assert _continuity_graph_snapshot(db_path) == before
    with sqlite3.connect(str(db_path)) as conn:
        assert conn.execute(
            """
            SELECT COUNT(*)
            FROM connector_outbox
            WHERE host_id = ? AND turn_id = ? AND delivery_kind = 'final_ready'
            """,
            (HOST_ID, stored_turn_id),
        ).fetchone() == (1,)
        assert conn.execute(
            """
            SELECT COUNT(*)
            FROM connector_outbox
            WHERE host_id = ?
              AND delivery_kind = 'final_ready'
              AND status IN ('queued', 'deferred', 'retry')
            """,
            (HOST_ID,),
        ).fetchone() == (0,)
    _assert_continuity_integrity(db_path)


@pytest.mark.parametrize(
    ("replacement_key", "expected_source_count", "expected_root_kinds"),
    [
        pytest.param(
            STABLE_KEY_A,
            1,
            ("final_ready",),
            id="exact-owner",
        ),
        pytest.param(
            STABLE_KEY_B,
            2,
            ("final_ready", "final_ready"),
            id="different-owner",
        ),
        pytest.param(
            None,
            2,
            ("final_ready", "final_migration_hold"),
            id="missing-owner",
        ),
    ],
)
def test_existing_v12_legacy_source_token_dual_matches_only_exact_owner(
    tmp_path: Path,
    replacement_key: str | None,
    expected_source_count: int,
    expected_root_kinds: tuple[str, ...],
) -> None:
    label = replacement_key[-1] if replacement_key is not None else "missing"
    db_path = tmp_path / f"existing-v12-legacy-token-{label}.db"
    first = _owner_snapshot(
        db_path,
        worker_id="worker-a",
        stable_key=STABLE_KEY_A,
        space_id="space-a",
        second=0,
    )
    init_store(db_path)
    assert save_snapshot(db_path, first) is True
    _merge_continuity_final(
        db_path,
        first,
        worker_id="worker-a",
        observed_at="2026-01-02T00:00:01+00:00",
    )
    historical_turn_id = _freeze_source_token(db_path, FROZEN_LEGACY_SOURCE_A)
    before = _continuity_graph_snapshot(db_path)

    replacement = _owner_snapshot(
        db_path,
        worker_id="worker-b",
        stable_key=replacement_key,
        space_id="space-b",
        second=10,
    )
    assert save_snapshot(db_path, replacement) is True
    assert merge_turn_content(
        db_path,
        HOST_ID,
        "worker-b",
        {
            "source_turn_id": CONTINUITY_RAW_SOURCE,
            "assistant_final_text": CONTINUITY_FINAL,
            "complete": True,
            "has_open_turn": False,
        },
        observed_at="2026-01-02T00:00:11+00:00",
    ) == 1

    source_rows = _source_turn_payloads(db_path)
    assert len(source_rows) == expected_source_count
    historical = next(row for row in source_rows if row[0] == historical_turn_id)
    assert historical[1]["id"] == historical_turn_id
    assert historical[1]["source_turn_id"] == FROZEN_LEGACY_SOURCE_A
    root_rows: list[tuple[str, str, dict[str, Any]]] = []
    with sqlite3.connect(str(db_path)) as conn:
        root_rows = [
            (str(turn_id), str(kind), json.loads(str(raw_payload)))
            for turn_id, kind, raw_payload in conn.execute(
                """
                SELECT turn_id, delivery_kind, payload_json
                FROM connector_outbox
                WHERE host_id = ?
                  AND delivery_kind IN ('final_ready', 'final_migration_hold')
                ORDER BY id
                """,
                (HOST_ID,),
            ).fetchall()
        ]
    assert tuple(row[1] for row in root_rows) == expected_root_kinds

    if replacement_key == STABLE_KEY_A:
        assert _continuity_graph_snapshot(db_path) == before
        assert root_rows[0][0] == historical_turn_id
        assert root_rows[0][2]["stable_key"] == STABLE_KEY_A
    elif replacement_key == STABLE_KEY_B:
        assert {row[2]["stable_key"] for row in root_rows} == {
            STABLE_KEY_A,
            STABLE_KEY_B,
        }
        assert len({row[0] for row in root_rows}) == 2
    else:
        assert root_rows[0][2]["stable_key"] == STABLE_KEY_A
        assert root_rows[1][2]["schema_version"] == 1
        assert "stable_key" not in root_rows[1][2]
        assert len({row[0] for row in root_rows}) == 2

    after_first_replay = _continuity_graph_snapshot(db_path)
    init_store(db_path)
    assert save_snapshot(db_path, replacement) is True
    assert merge_turn_content(
        db_path,
        HOST_ID,
        "worker-b",
        {
            "source_turn_id": CONTINUITY_RAW_SOURCE,
            "assistant_final_text": CONTINUITY_FINAL,
            "complete": True,
            "has_open_turn": False,
        },
        observed_at="2026-01-02T00:00:11+00:00",
    ) == 0
    assert _continuity_graph_snapshot(db_path) == after_first_replay
    _assert_continuity_integrity(db_path)


def test_exact_owner_source_ambiguity_rolls_back_turn_pending_and_graph(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "existing-v12-source-ambiguity.db"
    first = _owner_snapshot(
        db_path,
        worker_id="worker-a",
        stable_key=STABLE_KEY_A,
        space_id="space-a",
        second=0,
    )
    init_store(db_path)
    assert save_snapshot(db_path, first) is True
    _merge_continuity_final(
        db_path,
        first,
        worker_id="worker-a",
        observed_at="2026-01-02T00:00:01+00:00",
    )

    second = _owner_snapshot(
        db_path,
        worker_id="worker-b",
        stable_key=STABLE_KEY_A,
        space_id="space-b",
        second=10,
    )
    assert save_snapshot(db_path, second) is True
    binding = _private_binding(second, PRIVATE_ROUTE_B)
    upsert_worker_bindings(db_path, [binding])
    duplicate_turn_id = _clone_ambiguous_source_alias(db_path)
    before = _continuity_graph_snapshot(db_path)
    before_turns = _turn_rows_snapshot(db_path)
    assert len(before["source_identities"]) == 2
    assert duplicate_turn_id in {row[0] for row in before["source_identities"]}

    with pytest.raises(
        store_sqlite.StoreSchemaError,
        match="turn_owner_source_ambiguous",
    ):
        store_sqlite.apply_turn_refresh(
            db_path,
            HOST_ID,
            "worker-b",
            {
                "source_turn_id": CONTINUITY_RAW_SOURCE,
                "assistant_final_text": CONTINUITY_FINAL,
                "complete": True,
                "has_open_turn": False,
            },
            backend_pending={
                "question": "must roll back with ambiguous source",
                "choices": [{"choice_id": "rollback-choice", "label": "Rollback"}],
            },
            expected_binding=binding,
            observed_at="2026-01-02T00:00:11+00:00",
        )

    assert _continuity_graph_snapshot(db_path) == before
    assert _turn_rows_snapshot(db_path) == before_turns
    assert store_sqlite.list_backend_pending(db_path, HOST_ID) == {}
    with sqlite3.connect(str(db_path)) as conn:
        assert conn.execute(
            """
            SELECT COUNT(*)
            FROM connector_outbox
            WHERE host_id = ?
              AND delivery_kind = 'final_ready'
              AND status IN ('queued', 'deferred', 'retry')
            """,
            (HOST_ID,),
        ).fetchone() == (2,)
        assert conn.execute(
            "SELECT COUNT(*) FROM turns WHERE host_id = ?",
            (HOST_ID,),
        ).fetchone()[0] == len(before["turn_content_revisions"])
    _assert_continuity_integrity(db_path)

    init_store(db_path)
    assert _continuity_graph_snapshot(db_path) == before
    assert _turn_rows_snapshot(db_path) == before_turns
    assert store_sqlite.list_backend_pending(db_path, HOST_ID) == {}
    _assert_continuity_integrity(db_path)
