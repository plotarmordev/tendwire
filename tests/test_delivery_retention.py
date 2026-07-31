"""Behavioral contracts for delivery-aware final-turn retention."""

from __future__ import annotations

import json
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Barrier
from typing import Any

import pytest

from tendwire.config import Config
from tendwire.connectors import ConnectorOutboxAPI
from tendwire.core.projector import project_from_raw
from tendwire.core.turns import turn_final_delivery_identity
from tendwire.store import sqlite as store_sqlite
from tendwire.store.sqlite import (
    cleanup_acknowledged_final_retention,
    init_store,
    merge_turn_content,
    reclaim_expired_connector_leases,
    save_snapshot,
    store_status,
    turns_payload_from_store,
)


HOST_ID = "retention-host"
WORKER_ID = "worker-1"
FINAL_NAME = "turn-final"
STABLE_KEY = "wsk1_" + ("a" * 64)


@pytest.fixture
def generated_long_outage_finals() -> list[tuple[str, str, str]]:
    return [
        (
            f"offline-source-{index:02d}",
            f"offline final {index:02d}",
            f"2026-01-01T00:{index:02d}:00+00:00",
        )
        for index in range(20)
    ]


def _new_store(db_path: Path, *, host_id: str = HOST_ID) -> Any:
    snapshot = project_from_raw(
        Config(host_id=host_id, db_path=db_path),
        workers=[
            {
                "id": WORKER_ID,
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
    return snapshot


def _source_turns(db_path: Path, snapshot: Any, *, host_id: str = HOST_ID) -> list[dict[str, Any]]:
    payload = turns_payload_from_store(
        db_path,
        host_id,
        snapshot=snapshot,
        schema_version=2,
        limit=250,
        turn_refresh_interval_seconds=1_000_000_000,
    )
    return [
        turn
        for turn in payload["turns"]
        if str(turn.get("source_turn_id") or "")
    ]


def _merge_final(
    db_path: Path,
    snapshot: Any,
    *,
    source_turn_id: str,
    final_text: str,
    observed_at: str,
    host_id: str = HOST_ID,
) -> dict[str, Any]:
    assert merge_turn_content(
        db_path,
        host_id,
        WORKER_ID,
        {
            "assistant_final_text": final_text,
            "complete": True,
            "has_open_turn": False,
            "source_turn_id": source_turn_id,
        },
        observed_at=observed_at,
    ) == 1
    matches = []
    for turn in _source_turns(db_path, snapshot, host_id=host_id):
        if _reconstruct_final(
            db_path,
            host_id=host_id,
            turn_id=turn["id"],
            revision=turn["content"]["content_revision"],
        ) == final_text:
            matches.append(turn)
    assert len(matches) == 1
    return matches[0]


def _poll_one_source(api: ConnectorOutboxAPI) -> dict[str, Any]:
    polled = api.poll({"name": FINAL_NAME, "limit": 100, "lease_seconds": 60})
    assert polled["ok"] is True
    assert len(polled["items"]) == 1
    item = polled["items"][0]
    assert item["key"].startswith("turn-final:revision:twfinal1.")
    assert item["payload"]["operation"] == "materialize"
    assert item["payload"]["final_identity"].startswith("twfinal1.")
    assert item["payload"]["schema_version"] == 2
    assert item["payload"]["stable_key"] == STABLE_KEY
    assert item["payload"]["stable_key_version"] == 1
    return item


def _begin_source_plan(
    api: ConnectorOutboxAPI,
    source: dict[str, Any],
    ranges: list[tuple[int, int]],
    *,
    version: str = "retention-v1",
) -> dict[str, Any]:
    payload = source["payload"]
    begun = api.prepare(
        {
            "schema_version": 1,
            "action": "begin",
            "name": FINAL_NAME,
            "turn_id": payload["turn_id"],
            "content_revision": payload["content_revision"],
            "presentation_version": version,
            "part_count": len(ranges),
            "source_ref": source["ref"],
        }
    )
    assert begun["ok"] is True
    assert begun["state"] == "preparing"
    token = begun["plan_token"]
    for ordinal, (start, end) in enumerate(ranges):
        added = api.prepare(
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
        assert added["ok"] is True
        assert added["accepted_parts"] == ordinal + 1
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
    assert committed["job_count"] == len(ranges)
    return committed


def _stage_whole_source(
    api: ConnectorOutboxAPI,
    source: dict[str, Any],
    *,
    version: str = "retention-v1",
) -> dict[str, Any]:
    length = int(
        source["payload"]["content"]["fields"]["assistant_final_text"][
            "char_length"
        ]
    )
    assert length > 0
    return _begin_source_plan(api, source, [(0, length)], version=version)


def _poll_one_part(api: ConnectorOutboxAPI) -> dict[str, Any]:
    polled = api.poll({"name": FINAL_NAME, "limit": 100})
    assert polled["ok"] is True
    assert len(polled["items"]) == 1
    item = polled["items"][0]
    assert item["payload"]["operation"] in {"upsert", "retire"}
    return item


def _ack_part(api: ConnectorOutboxAPI, item: dict[str, Any]) -> dict[str, Any]:
    result = api.ack(
        {
            "name": FINAL_NAME,
            "ref": item["ref"],
            "response": {"accepted": True},
        }
    )
    assert result["ok"] is True
    assert result["status"] == "acknowledged"
    return result


def _finish_source(
    api: ConnectorOutboxAPI,
    source: dict[str, Any],
    *,
    version: str = "retention-v1",
) -> list[dict[str, Any]]:
    committed = _stage_whole_source(api, source, version=version)
    jobs: list[dict[str, Any]] = []
    for _ in range(int(committed["job_count"])):
        job = _poll_one_part(api)
        jobs.append(job)
        _ack_part(api, job)
    return jobs


def _anchor_state(db_path: Path, key: str) -> tuple[str, str, str, str] | None:
    with sqlite3.connect(str(db_path)) as conn:
        row = conn.execute(
            """
            SELECT status, delivery_kind, turn_id, content_revision
            FROM connector_outbox
            WHERE host_id = ? AND connector = ? AND delivery_key = ?
            """,
            (HOST_ID, FINAL_NAME, key),
        ).fetchone()
    if row is None:
        return None
    return tuple(str(value) for value in row)


def _attempt_states(db_path: Path, key: str) -> list[tuple[int, str]]:
    with sqlite3.connect(str(db_path)) as conn:
        return [
            (int(row[0]), str(row[1]))
            for row in conn.execute(
                """
                SELECT deliveries.attempt, deliveries.status
                FROM connector_deliveries AS deliveries
                JOIN connector_outbox AS outbox ON outbox.id = deliveries.outbox_id
                WHERE outbox.host_id = ?
                  AND outbox.connector = ?
                  AND outbox.delivery_key = ?
                ORDER BY deliveries.id
                """,
                (HOST_ID, FINAL_NAME, key),
            ).fetchall()
        ]


def _reconstruct_final(
    db_path: Path,
    *,
    turn_id: str,
    revision: str,
    host_id: str = HOST_ID,
) -> str:
    cursor: str | None = None
    chunks: list[str] = []
    while True:
        page = store_sqlite.get_turn_content(
            db_path,
            host_id,
            turn_id=turn_id,
            content_revision=revision,
            field="assistant_final_text",
            cursor=cursor,
        )
        assert page.get("status") is None
        chunks.append(str(page["text"]))
        cursor = page["next_cursor"]
        if cursor is None:
            return "".join(chunks)


def _aggressive_cleanup(db_path: Path, *, batch_size: int = 100) -> dict[str, Any]:
    return cleanup_acknowledged_final_retention(
        db_path,
        HOST_ID,
        acknowledged_final_retention_days=1,
        acknowledged_final_retention_count=1,
        batch_size=batch_size,
        now="2099-01-01T00:00:00+00:00",
    )


def test_dead_letter_final_blocks_no_later_finals(tmp_path: Path) -> None:
    # dead_letter is terminal for FIFO ordering: it must not block ANY later
    # final — not other owners (the original intent of this test) and not its
    # own owner either (a poisoned reply must never wedge its worker; recovery
    # goes through retry_final_ready_delivery/supersede instead).
    db_path = tmp_path / "dead-letter-owner-isolation.db"
    second_worker_id = "worker-2"
    second_stable_key = "wsk1_" + ("b" * 64)
    snapshot = project_from_raw(
        Config(host_id=HOST_ID, db_path=db_path),
        workers=[
            {
                "id": WORKER_ID,
                "name": "First Worker",
                "status": "active",
                "space_id": "space-1",
                "meta": {
                    "stable_key": STABLE_KEY,
                    "stable_key_version": 1,
                },
            },
            {
                "id": second_worker_id,
                "name": "Second Worker",
                "status": "active",
                "space_id": "space-2",
                "meta": {
                    "stable_key": second_stable_key,
                    "stable_key_version": 1,
                },
            },
        ],
    )
    init_store(db_path)
    save_snapshot(db_path, snapshot)
    _merge_final(
        db_path,
        snapshot,
        source_turn_id="first-owner-failed",
        final_text="first owner failed final",
        observed_at="2026-01-01T00:00:00+00:00",
    )

    api = ConnectorOutboxAPI(db_path, HOST_ID, max_attempts=1)
    failed = _poll_one_source(api)
    assert api.fail(
        {
            "name": FINAL_NAME,
            "ref": failed["ref"],
            "delay_seconds": 0,
        }
    )["ok"] is True

    _merge_final(
        db_path,
        snapshot,
        source_turn_id="first-owner-later",
        final_text="first owner later final",
        observed_at="2026-01-01T00:01:00+00:00",
    )
    assert merge_turn_content(
        db_path,
        HOST_ID,
        second_worker_id,
        {
            "assistant_final_text": "second owner final",
            "complete": True,
            "has_open_turn": False,
            "source_turn_id": "second-owner-ready",
        },
        observed_at="2026-01-01T00:02:00+00:00",
    ) == 1

    unrelated = api.poll(
        {"name": FINAL_NAME, "limit": 100, "lease_seconds": 60}
    )
    assert unrelated["ok"] is True
    assert len(unrelated["items"]) == 2
    polled_keys = {item["payload"]["stable_key"] for item in unrelated["items"]}
    assert polled_keys == {STABLE_KEY, second_stable_key}

    with sqlite3.connect(str(db_path)) as conn:
        first_owner_states = conn.execute(
            """
            SELECT status
            FROM connector_outbox
            WHERE connector = ?
              AND delivery_kind = 'final_ready'
              AND json_extract(payload_json, '$.stable_key') = ?
            ORDER BY id
            """,
            (FINAL_NAME, STABLE_KEY),
        ).fetchall()
    assert first_owner_states == [("dead_letter",), ("leased",)]


def test_acknowledged_prefix_is_cleaned_but_failed_suffix_and_exact_final_survive(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "ack-prefix-failed-suffix.db"
    snapshot = _new_store(db_path)
    texts = {
        "source-0": "final-zero!",
        "source-1": "final-one!!",
        "source-2": "abcdefghijkl",
    }
    turns = {
        source_id: _merge_final(
            db_path,
            snapshot,
            source_turn_id=source_id,
            final_text=text,
            observed_at=f"2026-01-01T00:0{index}:00+00:00",
        )
        for index, (source_id, text) in enumerate(texts.items())
    }
    api = ConnectorOutboxAPI(db_path, HOST_ID, max_attempts=1)

    delivered_keys: list[str] = []
    for index in range(2):
        source = _poll_one_source(api)
        delivered_keys.append(source["key"])
        _finish_source(api, source, version=f"retention-prefix-{index}")
        assert _anchor_state(db_path, source["key"])[0] == "delivered"

    failed_source = _poll_one_source(api)
    committed = _begin_source_plan(
        api,
        failed_source,
        [(0, 4), (4, 8), (8, 12)],
        version="retention-failed-suffix",
    )
    assert committed["job_count"] == 3
    for expected_sequence in (0, 1):
        part = _poll_one_part(api)
        assert part["payload"]["sequence_index"] == expected_sequence
        _ack_part(api, part)
    failed_part = _poll_one_part(api)
    assert failed_part["payload"]["sequence_index"] == 2
    failed = api.fail(
        {
            "name": FINAL_NAME,
            "ref": failed_part["ref"],
            "delay_seconds": 0,
            "reason": "temporary",
        }
    )
    assert failed["status"] == "attempts_exhausted"
    assert _anchor_state(db_path, failed_source["key"])[0] == "awaiting_ack"

    cleanup = _aggressive_cleanup(db_path)

    assert cleanup["ok"] is True
    assert cleanup["deleted"] == 2
    assert cleanup["deleted_rows"]["turns"] == 2
    assert all(_anchor_state(db_path, key) is None for key in delivered_keys)
    failed_state = _anchor_state(db_path, failed_source["key"])
    assert failed_state is not None
    assert failed_state[:2] == ("awaiting_ack", "final_ready")
    failed_turn = turns["source-2"]
    assert _reconstruct_final(
        db_path,
        turn_id=failed_turn["id"],
        revision=failed_turn["content"]["content_revision"],
    ) == texts["source-2"]
    remaining_sources = _source_turns(db_path, snapshot)
    assert len(remaining_sources) == 1
    assert remaining_sources[0]["id"] == failed_turn["id"]


def test_lease_expiry_defer_transient_failure_restart_and_stale_ref_preserve_final(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "lease-failure-restart.db"
    snapshot = _new_store(db_path)
    final_text = "durable final through retries"
    turn = _merge_final(
        db_path,
        snapshot,
        source_turn_id="retry-source",
        final_text=final_text,
        observed_at="2026-01-01T00:00:00+00:00",
    )
    api = ConnectorOutboxAPI(db_path, HOST_ID, max_attempts=4)

    expired_ref = _poll_one_source(api)["ref"]
    reclaimed = reclaim_expired_connector_leases(
        db_path,
        HOST_ID,
        FINAL_NAME,
        now="9999-01-01T00:00:00+00:00",
    )
    assert reclaimed["reclaimed"] == 1
    second = _poll_one_source(api)
    stale_ack = api.ack({"name": FINAL_NAME, "ref": expired_ref})
    assert stale_ack["ok"] is False
    assert stale_ack["status"] in {"invalid_ref", "stale_ref", "expired_ref"}
    assert _anchor_state(db_path, second["key"])[0] == "leased"

    deferred = api.defer(
        {"name": FINAL_NAME, "ref": second["ref"], "delay_seconds": 0}
    )
    assert deferred["status"] == "deferred"
    restarted = ConnectorOutboxAPI(db_path, HOST_ID, max_attempts=4)
    third = _poll_one_source(restarted)
    assert third["attempt"] == 3
    transient = restarted.fail(
        {"name": FINAL_NAME, "ref": third["ref"], "delay_seconds": 0}
    )
    assert transient["status"] == "retry_scheduled"

    restarted_again = ConnectorOutboxAPI(db_path, HOST_ID, max_attempts=4)
    fourth = _poll_one_source(restarted_again)
    assert fourth["attempt"] == 4
    exhausted = restarted_again.fail(
        {"name": FINAL_NAME, "ref": fourth["ref"], "delay_seconds": 0}
    )
    assert exhausted["status"] == "attempts_exhausted"
    assert _anchor_state(db_path, fourth["key"])[0] == "dead_letter"
    assert restarted_again.poll({"name": FINAL_NAME, "limit": 100})["items"] == []

    cleanup = _aggressive_cleanup(db_path)
    assert cleanup["deleted"] == 0
    assert _reconstruct_final(
        db_path,
        turn_id=turn["id"],
        revision=turn["content"]["content_revision"],
    ) == final_text
    assert _attempt_states(db_path, fourth["key"]) == [
        (1, "expired"),
        (2, "deferred"),
        (3, "failed"),
        (4, "failed"),
    ]


def test_multipart_source_anchor_is_delivered_only_after_every_part_ack(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "multipart-anchor.db"
    snapshot = _new_store(db_path)
    _merge_final(
        db_path,
        snapshot,
        source_turn_id="multipart-source",
        final_text="abcdefghijkl",
        observed_at="2026-01-01T00:00:00+00:00",
    )
    api = ConnectorOutboxAPI(db_path, HOST_ID)
    source = _poll_one_source(api)
    _begin_source_plan(
        api,
        source,
        [(0, 4), (4, 8), (8, 12)],
        version="retention-multipart",
    )

    assert _anchor_state(db_path, source["key"])[0] == "awaiting_ack"
    for expected_sequence in (0, 1):
        part = _poll_one_part(api)
        assert part["payload"]["sequence_index"] == expected_sequence
        _ack_part(api, part)
        assert _anchor_state(db_path, source["key"])[0] == "awaiting_ack"
        assert _aggressive_cleanup(db_path)["deleted"] == 0

    last = _poll_one_part(api)
    assert last["payload"]["sequence_index"] == 2
    _ack_part(api, last)
    assert _anchor_state(db_path, source["key"])[0] == "delivered"
    assert api.poll({"name": FINAL_NAME, "limit": 100})["items"] == []

    cleanup = _aggressive_cleanup(db_path)
    assert cleanup["deleted"] == 1
    assert _anchor_state(db_path, source["key"]) is None


def test_explicit_dead_letter_retry_resets_budget_and_bounds_attempt_rows(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "explicit-retry.db"
    snapshot = _new_store(db_path)
    _merge_final(
        db_path,
        snapshot,
        source_turn_id="dead-letter-source",
        final_text="retry me exactly",
        observed_at="2026-01-01T00:00:00+00:00",
    )
    api = ConnectorOutboxAPI(db_path, HOST_ID, max_attempts=2)
    first = _poll_one_source(api)
    key = first["key"]
    assert api.fail(
        {"name": FINAL_NAME, "ref": first["ref"], "delay_seconds": 0}
    )["status"] == "retry_scheduled"
    second = _poll_one_source(api)
    assert second["attempt"] == 2
    assert api.fail(
        {"name": FINAL_NAME, "ref": second["ref"], "delay_seconds": 0}
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
    assert inspected["items"][0]["key"] == key
    assert inspected["items"][0]["attempt_count"] == 2
    assert len(_attempt_states(db_path, key)) == 2

    retried = api.retry(
        {"schema_version": 1, "name": FINAL_NAME, "key": key}
    )
    assert retried["status"] == "requeued"
    assert retried["prior_attempt_count"] == 2
    assert _attempt_states(db_path, key) == []
    fresh_first = _poll_one_source(api)
    assert fresh_first["attempt"] == 1
    assert api.fail(
        {"name": FINAL_NAME, "ref": fresh_first["ref"], "delay_seconds": 0}
    )["status"] == "retry_scheduled"
    fresh_second = _poll_one_source(api)
    assert fresh_second["attempt"] == 2
    assert api.fail(
        {"name": FINAL_NAME, "ref": fresh_second["ref"], "delay_seconds": 0}
    )["status"] == "attempts_exhausted"
    assert len(_attempt_states(db_path, key)) == 2

    retried_again = api.retry(
        {
            "schema_version": 1,
            "name": FINAL_NAME,
            "final_identity": retried["final_identity"],
        }
    )
    assert retried_again["status"] == "requeued"
    assert retried_again["prior_attempt_count"] == 4
    assert _attempt_states(db_path, key) == []
    deliverable = _poll_one_source(api)
    assert deliverable["attempt"] == 1
    _finish_source(api, deliverable, version="retention-after-retry")
    assert _anchor_state(db_path, key)[0] == "delivered"
    source_attempts = _attempt_states(db_path, key)
    assert source_attempts == [(1, "delivered")]


def test_new_authoritative_revision_supersedes_stale_lease_without_double_send(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "revision-supersede.db"
    snapshot = _new_store(db_path)
    old_turn = _merge_final(
        db_path,
        snapshot,
        source_turn_id="revision-source",
        final_text="old authoritative final",
        observed_at="2026-01-01T00:00:00+00:00",
    )
    api = ConnectorOutboxAPI(db_path, HOST_ID)
    old_source = _poll_one_source(api)

    new_turn = _merge_final(
        db_path,
        snapshot,
        source_turn_id="revision-source",
        final_text="new authoritative final",
        observed_at="2026-01-02T00:00:00+00:00",
    )
    assert old_turn["id"] == new_turn["id"]
    assert old_turn["content"]["content_revision"] != new_turn["content"][
        "content_revision"
    ]
    assert _anchor_state(db_path, old_source["key"])[0] == "superseded"
    stale = api.ack({"name": FINAL_NAME, "ref": old_source["ref"]})
    assert stale["ok"] is False
    assert stale["status"] in {"invalid_ref", "stale_ref", "expired_ref"}

    current_source = _poll_one_source(api)
    assert current_source["payload"]["content_revision"] == new_turn["content"][
        "content_revision"
    ]
    _finish_source(api, current_source, version="retention-current-revision")
    assert _anchor_state(db_path, current_source["key"])[0] == "delivered"

    assert merge_turn_content(
        db_path,
        HOST_ID,
        WORKER_ID,
        {
            "assistant_final_text": "new authoritative final",
            "complete": True,
            "has_open_turn": False,
            "source_turn_id": "revision-source",
        },
        observed_at="2026-01-03T00:00:00+00:00",
    ) == 0
    assert merge_turn_content(
        db_path,
        HOST_ID,
        WORKER_ID,
        {
            "assistant_final_text": "old authoritative final",
            "complete": True,
            "has_open_turn": False,
            "source_turn_id": "revision-source",
        },
        observed_at="2025-12-31T23:59:59+00:00",
    ) == 0
    assert api.poll({"name": FINAL_NAME, "limit": 100})["items"] == []

    with sqlite3.connect(str(db_path)) as conn:
        anchors = conn.execute(
            """
            SELECT delivery_key, status
            FROM connector_outbox
            WHERE host_id = ?
              AND connector = ?
              AND delivery_kind = 'final_ready'
            ORDER BY id
            """,
            (HOST_ID, FINAL_NAME),
        ).fetchall()
    assert anchors == [
        (old_source["key"], "superseded"),
        (current_source["key"], "delivered"),
    ]
    assert current_source["key"] != old_source["key"]


def test_storage_pressure_surface_is_aggregate_and_omits_final_content_and_ids(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "storage-pressure.db"
    snapshot = _new_store(db_path)
    sentinels = [
        "UNPUBLISHED_FINAL_SENTINEL_ALPHA",
        "UNPUBLISHED_FINAL_SENTINEL_BRAVO",
        "UNPUBLISHED_FINAL_SENTINEL_CHARLIE",
    ]
    for index, sentinel in enumerate(sentinels):
        _merge_final(
            db_path,
            snapshot,
            source_turn_id=f"pressure-source-{index}",
            final_text=sentinel,
            observed_at=f"2020-01-01T00:00:0{index}+00:00",
        )

    status = store_status(
        db_path,
        HOST_ID,
        acknowledged_final_retention_days=1,
        acknowledged_final_retention_count=2,
    )
    retention = status["final_retention"]
    encoded = json.dumps(status, sort_keys=True)

    assert status["ok"] is True
    assert retention == {
        "acknowledged": 0,
        "unresolved": 3,
        "queued": 3,
        "leased": 0,
        "deferred": 0,
        "retry": 0,
        "dead_letter": 0,
        "awaiting_ack": 0,
        "eligible": 0,
        "acknowledged_final_retention_days": 1,
        "acknowledged_final_retention_count": 2,
        "storage_pressure": True,
    }
    assert all(sentinel not in encoded for sentinel in sentinels)
    assert "pressure-source" not in encoded
    assert "twrev1." not in encoded
    assert "twfinal1." not in encoded
    assert "turn-final:revision:" not in encoded
    assert "private_state_json" not in encoded



def test_concurrent_final_merges_create_unique_anchors_drained_in_durable_order(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "concurrent-ordered-anchors.db"
    snapshot = _new_store(db_path)
    count = 8
    barrier = Barrier(count)

    def merge_one(index: int) -> int:
        barrier.wait(timeout=10)
        return merge_turn_content(
            db_path,
            HOST_ID,
            WORKER_ID,
            {
                "assistant_final_text": f"concurrent final {index}",
                "complete": True,
                "has_open_turn": False,
                "source_turn_id": f"concurrent-source-{index}",
            },
            observed_at=f"2026-01-01T00:00:{index:02d}+00:00",
        )

    with ThreadPoolExecutor(max_workers=count) as executor:
        results = list(executor.map(merge_one, range(count)))
    assert results == [1] * count

    source_turns = _source_turns(db_path, snapshot)
    assert len(source_turns) == count
    expected_keys = {
        "turn-final:revision:"
        + turn_final_delivery_identity(
            HOST_ID,
            turn["id"],
            turn["content"]["content_revision"],
        )
        for turn in source_turns
    }
    api = ConnectorOutboxAPI(db_path, HOST_ID)
    drained_keys: list[str] = []
    for index in range(count):
        source = _poll_one_source(api)
        drained_keys.append(source["key"])
        assert api.poll({"name": FINAL_NAME, "limit": 100})["items"] == []
        _stage_whole_source(
            api,
            source,
            version=f"retention-concurrent-{index}",
        )
        part = _poll_one_part(api)
        assert part["payload"]["turn"]["final_identity"] == source["payload"][
            "final_identity"
        ]
        assert part["payload"]["turn"]["stable_key"] == source["payload"]["stable_key"]
        assert part["payload"]["turn"]["stable_key_version"] == source["payload"][
            "stable_key_version"
        ]
        assert api.poll({"name": FINAL_NAME, "limit": 100})["items"] == []
        _ack_part(api, part)
        assert _anchor_state(db_path, source["key"])[0] == "delivered"

    assert api.poll({"name": FINAL_NAME, "limit": 100})["items"] == []
    assert len(drained_keys) == len(set(drained_keys)) == count
    assert set(drained_keys) == expected_keys
    with sqlite3.connect(str(db_path)) as conn:
        durable_order = [
            str(row[0])
            for row in conn.execute(
                """
                SELECT delivery_key
                FROM connector_outbox
                WHERE host_id = ?
                  AND connector = ?
                  AND delivery_kind = 'final_ready'
                ORDER BY id
                """,
                (HOST_ID, FINAL_NAME),
            ).fetchall()
        ]
        source_states = conn.execute(
            """
            SELECT DISTINCT status
            FROM connector_outbox
            WHERE host_id = ?
              AND connector = ?
              AND delivery_kind = 'final_ready'
            """,
            (HOST_ID, FINAL_NAME),
        ).fetchall()
    assert drained_keys == durable_order
    assert source_states == [("delivered",)]


def test_twenty_turn_outage_restart_delivers_each_final_exactly_once(
    tmp_path: Path,
    generated_long_outage_finals: list[tuple[str, str, str]],
) -> None:
    db_path = tmp_path / "twenty-turn-outage.db"
    snapshot = _new_store(db_path)
    expected: dict[str, str] = {}
    for source_turn_id, final_text, observed_at in generated_long_outage_finals:
        turn = _merge_final(
            db_path,
            snapshot,
            source_turn_id=source_turn_id,
            final_text=final_text,
            observed_at=observed_at,
        )
        expected[str(turn["id"])] = final_text

    delivered: dict[str, str] = {}
    api = ConnectorOutboxAPI(db_path, HOST_ID)
    for index in range(20):
        if index in {7, 14}:
            api = ConnectorOutboxAPI(db_path, HOST_ID)
        source = _poll_one_source(api)
        payload = source["payload"]
        turn_id = str(payload["turn_id"])
        revision = str(payload["content_revision"])
        assert turn_id not in delivered
        delivered[turn_id] = _reconstruct_final(
            db_path,
            turn_id=turn_id,
            revision=revision,
        )
        _finish_source(api, source, version=f"retention-outage-{index}")

    assert delivered == expected
    restarted = ConnectorOutboxAPI(db_path, HOST_ID)
    assert restarted.poll({"name": FINAL_NAME, "limit": 100})["items"] == []
    save_snapshot(db_path, snapshot)
    assert restarted.poll({"name": FINAL_NAME, "limit": 100})["items"] == []

    with sqlite3.connect(str(db_path)) as conn:
        sources = conn.execute(
            """
            SELECT COUNT(*), COUNT(DISTINCT delivery_key), MIN(status), MAX(status)
            FROM connector_outbox
            WHERE host_id = ? AND connector = ?
              AND delivery_kind = 'final_ready'
            """,
            (HOST_ID, FINAL_NAME),
        ).fetchone()
        completed_plans = conn.execute(
            """
            SELECT COUNT(*), COUNT(DISTINCT content_revision)
            FROM turn_presentation_plans
            WHERE host_id = ? AND name = ? AND state = 'completed'
            """,
            (HOST_ID, FINAL_NAME),
        ).fetchone()
        delivered_jobs = conn.execute(
            """
            SELECT COUNT(*), COUNT(DISTINCT delivery_key)
            FROM connector_outbox
            WHERE host_id = ? AND connector = ?
              AND delivery_kind = 'final_part'
              AND status = 'delivered'
            """,
            (HOST_ID, FINAL_NAME),
        ).fetchone()
        integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]
        foreign_keys = conn.execute("PRAGMA foreign_key_check").fetchall()

    assert sources == (20, 20, "delivered", "delivered")
    assert completed_plans == (20, 20)
    assert delivered_jobs == (20, 20)
    assert integrity == "ok"
    assert foreign_keys == []
