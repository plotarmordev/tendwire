"""Goal 13 cache-only turn delta capture, paging, safety, and latency tests."""

from __future__ import annotations

import json
import math
import sqlite3
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping

import pytest

from tendwire.cli import main
from tendwire.config import Config, load_config
from tendwire.core import turns as turns_core
from tendwire.core.models import stable_json_dumps
from tendwire.core.projector import project_from_raw
from tendwire.core.turns import decode_turn_delta_watermark
from tendwire.daemon_api import (
    DaemonAPIClient,
    MAX_RESPONSE_BYTES,
    REQUIRED_METHODS,
    TendwireDaemonAPI,
    UnixSocketJSONServer,
    _serialized_response,
    success_response,
)
from tendwire.daemon import TendwireDaemon
from tendwire.store import sqlite as store_sqlite
from tendwire.store.sqlite import (
    TurnDeltaWorkCounters,
    compact_turn_change_journal,
    init_store,
    turn_delta_payload_from_store,
)


HOST = "delta-host"
TS = "2026-07-18T12:00:00+00:00"
OLD_TS = "2025-01-01T00:00:00+00:00"


def _payload(
    turn_id: str,
    *,
    host_id: str = HOST,
    worker_id: str = "worker-0",
    status: str = "working",
    summary: str | None = None,
    updated_at: str = TS,
    extra: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    value: dict[str, Any] = {
        "schema_version": 1,
        "id": turn_id,
        "host_id": host_id,
        "worker_id": worker_id,
        "status": status,
        "kind": "prompt",
        "source": "snapshot",
        "updated_at": updated_at,
    }
    if summary is not None:
        value["summary"] = summary
    if extra:
        value.update(extra)
    return value


def _insert_turn(
    conn: sqlite3.Connection,
    turn_id: str,
    sequence: int,
    *,
    host_id: str = HOST,
    worker_id: str = "worker-0",
    status: str = "working",
    summary: str | None = None,
    updated_at: str = TS,
    extra: Mapping[str, Any] | None = None,
) -> None:
    payload = _payload(
        turn_id,
        host_id=host_id,
        worker_id=worker_id,
        status=status,
        summary=summary,
        updated_at=updated_at,
        extra=extra,
    )
    conn.execute(
        """
        INSERT INTO turns (
            host_id, turn_id, worker_id, worker_fingerprint, space_id,
            status, kind, updated_at, fingerprint,
            snapshot_content_fingerprint, observed_at, payload_json,
            list_sequence
        ) VALUES (?, ?, ?, NULL, NULL, ?, 'prompt', ?, ?, ?, ?, ?, ?)
        """,
        (
            host_id,
            turn_id,
            worker_id,
            status,
            updated_at,
            f"fingerprint-{turn_id}",
            f"snapshot-{turn_id}",
            updated_at,
            stable_json_dumps(payload),
            sequence,
        ),
    )


def _mutate_turn(
    db_path: Path,
    turn_id: str,
    *,
    summary: str,
    status: str = "working",
    updated_at: str = TS,
) -> None:
    with sqlite3.connect(str(db_path)) as conn:
        row = conn.execute(
            "SELECT payload_json FROM turns WHERE host_id = ? AND turn_id = ?",
            (HOST, turn_id),
        ).fetchone()
        assert row is not None
        payload = json.loads(str(row[0]))
        payload.update({"summary": summary, "status": status, "updated_at": updated_at})
        conn.execute(
            """
            UPDATE turns
            SET status = ?, updated_at = ?, payload_json = ?
            WHERE host_id = ? AND turn_id = ?
            """,
            (status, updated_at, stable_json_dumps(payload), HOST, turn_id),
        )
        conn.commit()


def _tombstone_turn(db_path: Path, turn_id: str, replacement: str | None = None) -> None:
    with sqlite3.connect(str(db_path)) as conn:
        row = conn.execute(
            "SELECT payload_json FROM turns WHERE host_id = ? AND turn_id = ?",
            (HOST, turn_id),
        ).fetchone()
        assert row is not None
        payload = json.loads(str(row[0]))
        payload["superseded_at"] = TS
        if replacement is not None:
            payload["superseded_by_turn_id"] = replacement
        conn.execute(
            "UPDATE turns SET payload_json = ? WHERE host_id = ? AND turn_id = ?",
            (stable_json_dumps(payload), HOST, turn_id),
        )
        conn.commit()


def _seed_pre_v18_store(db_path: Path, count: int) -> None:
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute("PRAGMA foreign_keys=ON")
        store_sqlite._run_migrations(conn, target_version=17)
        rows = []
        for index in range(count):
            turn_id = f"historical-{index:05d}"
            worker_id = f"worker-{index % 8}"
            status = "working" if index < 8 else "complete"
            payload = _payload(
                turn_id,
                worker_id=worker_id,
                status=status,
                summary=f"retained public result {index}",
            )
            rows.append(
                (
                    HOST,
                    turn_id,
                    worker_id,
                    status,
                    TS,
                    f"fingerprint-{index}",
                    f"snapshot-{index}",
                    TS,
                    stable_json_dumps(payload),
                    index + 1,
                )
            )
        conn.executemany(
            """
            INSERT INTO turns (
                host_id, turn_id, worker_id, status, kind, updated_at,
                fingerprint, snapshot_content_fingerprint, observed_at,
                payload_json, list_sequence
            ) VALUES (?, ?, ?, ?, 'prompt', ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        conn.commit()
    init_store(db_path)


def _bootstrap_checkpoint(db_path: Path, *, limit: int = 100) -> str:
    cursor: str | None = None
    while True:
        page = turn_delta_payload_from_store(
            db_path,
            HOST,
            cursor=cursor,
            limit=limit,
        )
        assert page.get("ok") is not False
        if not page["has_more"]:
            assert isinstance(page["checkpoint"], str)
            return str(page["checkpoint"])
        assert page["checkpoint"] is None
        cursor = str(page["next_cursor"])


def _incompatible_watermark(valid: str) -> str:
    decoded = decode_turn_delta_watermark(valid, host_id=HOST)
    schema = 99
    projection = decoded.projection_schema_version
    host_digest = turns_core._turn_delta_host_digest(HOST)
    material = {
        "host_digest": host_digest,
        "projection_schema_version": projection,
        "schema_version": schema,
        "sequence": decoded.sequence,
        "store_epoch": decoded.store_epoch,
        "token_version": 1,
    }
    body = stable_json_dumps(
        {
            "h": turns_core._domain_digest(
                "tendwire.turn-delta-watermark-integrity.v1", material
            ),
            "p": projection,
            "q": decoded.store_epoch,
            "s": decoded.sequence,
            "v": 1,
            "x": schema,
            "z": host_digest,
        }
    ).encode("utf-8")
    return f"twdelta1.{turns_core._base64url(body)}"


def _nearest_rank_p95(samples: list[float]) -> float:
    return sorted(samples)[math.ceil(0.95 * len(samples)) - 1]


def test_goal13_acceptance_1_to_3_ten_thousand_bootstrap_and_unchanged_polls(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """10k bootstrap is stable/bounded; unchanged polls traverse no list/content."""
    db_path = tmp_path / "ten-thousand.db"
    _seed_pre_v18_store(db_path, 10_000)
    with sqlite3.connect(str(db_path)) as conn:
        assert conn.execute("SELECT COUNT(*) FROM turn_change_journal").fetchone() == (0,)

    reader_calls = {"list": 0, "content": 0}
    real_list_reader = store_sqlite.turns_payload_from_store
    real_content_reader = store_sqlite.get_turn_content

    def list_reader_spy(*args: Any, **kwargs: Any) -> dict[str, Any]:
        reader_calls["list"] += 1
        return real_list_reader(*args, **kwargs)

    def content_reader_spy(*args: Any, **kwargs: Any) -> dict[str, Any]:
        reader_calls["content"] += 1
        return real_content_reader(*args, **kwargs)

    monkeypatch.setattr(store_sqlite, "turns_payload_from_store", list_reader_spy)
    monkeypatch.setattr(store_sqlite, "get_turn_content", content_reader_spy)

    cursor: str | None = None
    seen: set[str] = set()
    active_workers: set[str] = set()
    pages = 0
    checkpoint: str | None = None
    while True:
        counters = TurnDeltaWorkCounters()
        page = turn_delta_payload_from_store(
            db_path,
            HOST,
            cursor=cursor,
            limit=500,
            work_counters=counters,
        )
        pages += 1
        assert page["mode"] == "bootstrap"
        assert len(_serialized_response(success_response(page))) < MAX_RESPONSE_BYTES
        seen.update(str(change["turn_id"]) for change in page["changes"])
        active_workers.update(
            str(change["turn"]["worker_id"])
            for change in page["changes"]
            if change["turn"]["status"] == "active"
        )
        if not page["has_more"]:
            checkpoint = str(page["checkpoint"])
            break
        assert page["checkpoint"] is None
        assert page["next_cursor"].startswith("twdeltac1.")
        cursor = str(page["next_cursor"])

    assert len(seen) == 10_000
    assert active_workers == {f"worker-{index}" for index in range(8)}
    assert pages == 20
    assert checkpoint.startswith("twdelta1.")
    for _ in range(2):
        counters = TurnDeltaWorkCounters()
        unchanged = turn_delta_payload_from_store(
            db_path,
            HOST,
            watermark=checkpoint,
            work_counters=counters,
        )
        assert unchanged["mode"] == "changes"
        assert unchanged["changes"] == []
        assert unchanged["has_more"] is False
        assert counters.journal_rows_scanned == 0
        assert counters.projection_rows_read == 0
        checkpoint = str(unchanged["checkpoint"])
    assert reader_calls == {"list": 0, "content": 0}


def test_bootstrap_size_gate_is_independent_of_client_limit_two(tmp_path: Path) -> None:
    db_path = tmp_path / "limit-two-bootstrap.db"
    _seed_pre_v18_store(db_path, 5_000)

    first = turn_delta_payload_from_store(
        db_path,
        HOST,
        limit=2,
        bootstrap_max_pages=10,
    )
    assert first.get("ok") is not False
    assert first["mode"] == "bootstrap"
    assert first["has_more"] is True
    assert len(first["changes"]) == 2

    second = turn_delta_payload_from_store(
        db_path,
        HOST,
        cursor=first["next_cursor"],
        limit=2,
        bootstrap_max_pages=10,
    )
    assert second.get("ok") is not False
    assert second["mode"] == "bootstrap"
    assert second["has_more"] is True
    assert len(second["changes"]) == 2


def test_goal13_acceptance_4_working_mutation_is_one_upsert_and_revision_only_changes(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "working.db"
    init_store(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_turn(conn, "working-turn", 1, summary="first working text")
        conn.commit()
    checkpoint = _bootstrap_checkpoint(db_path)

    _mutate_turn(db_path, "working-turn", summary="intermediate working text")
    _mutate_turn(db_path, "working-turn", summary="second working text")
    changed = turn_delta_payload_from_store(db_path, HOST, watermark=checkpoint)
    assert [(item["op"], item["turn_id"]) for item in changed["changes"]] == [
        ("upsert", "working-turn")
    ]
    assert changed["changes"][0]["turn"]["summary"] == "second working text"

    with sqlite3.connect(str(db_path)) as conn:
        before = conn.execute("SELECT MAX(seq) FROM turn_change_journal").fetchone()[0]
        store_sqlite._insert_turn_content_revision_conn(
            conn,
            host_id=HOST,
            turn_id="working-turn",
            user_text="prompt",
            assistant_final_text="revision-only public text",
            user_state="complete",
            final_state="complete",
            created_at=TS,
            is_current=False,
        )
        revision = conn.execute(
            """
            SELECT content_revision FROM turn_content_revisions
            WHERE host_id = ? AND turn_id = ? ORDER BY rowid DESC LIMIT 1
            """,
            (HOST, "working-turn"),
        ).fetchone()[0]
        conn.execute(
            """
            UPDATE turn_content_revisions SET is_current = 1
            WHERE host_id = ? AND turn_id = ? AND content_revision = ?
            """,
            (HOST, "working-turn", revision),
        )
        conn.commit()
        after = conn.execute("SELECT MAX(seq) FROM turn_change_journal").fetchone()[0]
    assert int(after) == int(before) + 1

    revision_delta = turn_delta_payload_from_store(
        db_path,
        HOST,
        watermark=changed["checkpoint"],
    )
    assert len(revision_delta["changes"]) == 1
    turn = revision_delta["changes"][0]["turn"]
    assert turn["assistant_final_text"] == "revision-only public text"


def test_current_revision_insert_alone_emits_one_upsert(tmp_path: Path) -> None:
    """Installing the first current revision closes the content-only change gap."""
    db_path = tmp_path / "revision-insert.db"
    init_store(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_turn(conn, "revision-insert", 1)
        conn.commit()
    checkpoint = _bootstrap_checkpoint(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        store_sqlite._insert_turn_content_revision_conn(
            conn,
            host_id=HOST,
            turn_id="revision-insert",
            user_text="prompt",
            assistant_final_text="new current content",
            user_state="complete",
            final_state="complete",
            created_at=TS,
            is_current=True,
        )
        conn.commit()

    delta = turn_delta_payload_from_store(db_path, HOST, watermark=checkpoint)
    assert [(item["op"], item["turn_id"]) for item in delta["changes"]] == [
        ("upsert", "revision-insert")
    ]
    assert delta["changes"][0]["turn"]["assistant_final_text"] == (
        "new current content"
    )


def test_public_turn_reclassified_as_internal_emits_remove(tmp_path: Path) -> None:
    db_path = tmp_path / "hidden-reclassification.db"
    init_store(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_turn(conn, "public-then-hidden", 1, summary="visible work")
        conn.commit()
    checkpoint = _bootstrap_checkpoint(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        row = conn.execute(
            "SELECT payload_json FROM turns WHERE host_id = ? AND turn_id = ?",
            (HOST, "public-then-hidden"),
        ).fetchone()
        assert row is not None
        payload = json.loads(str(row[0]))
        payload["assistant_stream_text"] = stable_json_dumps(
            {"gitmoot_result": {"decision": "approved"}}
        )
        conn.execute(
            "UPDATE turns SET payload_json = ? WHERE host_id = ? AND turn_id = ?",
            (stable_json_dumps(payload), HOST, "public-then-hidden"),
        )
        conn.commit()

    delta = turn_delta_payload_from_store(db_path, HOST, watermark=checkpoint)
    assert [(item["op"], item["turn_id"]) for item in delta["changes"]] == [
        ("remove", "public-then-hidden")
    ]


def test_single_oversized_change_degrades_and_advances_checkpoint(tmp_path: Path) -> None:
    db_path = tmp_path / "oversized-single-change.db"
    init_store(db_path)
    checkpoint = _bootstrap_checkpoint(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_turn(
            conn,
            "oversized",
            1,
            summary="large public descriptor",
            extra={"meta": {"items": ["x" * 12_000 for _ in range(100)]}},
        )
        store_sqlite._insert_turn_content_revision_conn(
            conn,
            host_id=HOST,
            turn_id="oversized",
            user_text="prompt",
            assistant_final_text="answer",
            user_state="complete",
            final_state="complete",
            created_at=TS,
            is_current=True,
        )
        conn.commit()

    page = turn_delta_payload_from_store(
        db_path,
        HOST,
        watermark=checkpoint,
        limit=1,
    )
    encoded = _serialized_response(success_response(page))
    assert len(encoded) < MAX_RESPONSE_BYTES
    assert page["has_more"] is False
    assert page["checkpoint"] is not None
    assert [(item["op"], item["turn_id"]) for item in page["changes"]] == [
        ("upsert", "oversized")
    ]
    projected = page["changes"][0]["turn"]
    assert "meta" not in projected
    assert projected["content"]["fields"]["user_text"]["inline"] is False
    assert projected["content"]["fields"]["user_text"]["first_cursor"]


def test_goal13_acceptance_6_tombstone_once_and_physical_delete_not_repeated(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "remove.db"
    init_store(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_turn(conn, "removed-turn", 1)
        conn.commit()
    checkpoint = _bootstrap_checkpoint(db_path)

    _tombstone_turn(db_path, "removed-turn", replacement="replacement-turn")
    removed = turn_delta_payload_from_store(db_path, HOST, watermark=checkpoint)
    assert removed["changes"] == [
        {
            "op": "remove",
            "turn_id": "removed-turn",
            "removed_at": TS,
            "superseded_by_turn_id": "replacement-turn",
        }
    ]
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            "DELETE FROM turns WHERE host_id = ? AND turn_id = ?",
            (HOST, "removed-turn"),
        )
        conn.commit()
    unchanged = turn_delta_payload_from_store(
        db_path,
        HOST,
        watermark=removed["checkpoint"],
    )
    assert unchanged["changes"] == []


def test_goal13_acceptance_7_concurrent_changes_are_frozen_then_safely_repeated(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "race.db"
    init_store(db_path)
    checkpoint = _bootstrap_checkpoint(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        for index, turn_id in enumerate(("a", "b", "c"), start=1):
            _insert_turn(conn, turn_id, index)
        conn.commit()

    first = turn_delta_payload_from_store(
        db_path,
        HOST,
        watermark=checkpoint,
        limit=1,
    )
    assert first["has_more"] is True
    first_id = str(first["changes"][0]["turn_id"])

    inserted = threading.Event()
    continue_writer = threading.Event()

    def race_writer() -> None:
        with sqlite3.connect(str(db_path)) as conn:
            _insert_turn(conn, "d", 4)
            conn.commit()
        inserted.set()
        assert continue_writer.wait(timeout=5)
        _mutate_turn(db_path, first_id, summary="mutated after its frozen page")

    writer = threading.Thread(target=race_writer)
    writer.start()
    assert inserted.wait(timeout=5)

    frozen_ids = [first_id]
    cursor = str(first["next_cursor"])
    page = turn_delta_payload_from_store(db_path, HOST, cursor=cursor, limit=1)
    frozen_ids.extend(str(change["turn_id"]) for change in page["changes"])
    continue_writer.set()
    cursor = str(page["next_cursor"])
    while True:
        page = turn_delta_payload_from_store(db_path, HOST, cursor=cursor, limit=1)
        frozen_ids.extend(str(change["turn_id"]) for change in page["changes"])
        if not page["has_more"]:
            frozen_checkpoint = str(page["checkpoint"])
            break
        cursor = str(page["next_cursor"])
    writer.join(timeout=5)
    assert not writer.is_alive()
    assert set(frozen_ids) == {"a", "b", "c"}
    assert len(frozen_ids) == 3

    next_batch = turn_delta_payload_from_store(
        db_path,
        HOST,
        watermark=frozen_checkpoint,
        limit=10,
    )
    assert {change["turn_id"] for change in next_batch["changes"]} == {"d", first_id}
    assert len(next_batch["changes"]) == 2


def test_journal_rows_scanned_counts_raw_rows_before_collapse(tmp_path: Path) -> None:
    db_path = tmp_path / "raw-journal-count.db"
    init_store(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_turn(conn, "collapsed", 1)
        conn.commit()
    checkpoint = _bootstrap_checkpoint(db_path)
    for index in range(3):
        _mutate_turn(db_path, "collapsed", summary=f"revision {index}")

    counters = TurnDeltaWorkCounters()
    page = turn_delta_payload_from_store(
        db_path,
        HOST,
        watermark=checkpoint,
        work_counters=counters,
    )
    assert len(page["changes"]) == 1
    assert page["aggregate"]["projection_rows_read"] == 1
    assert page["aggregate"]["journal_rows_scanned"] == 3
    assert counters.journal_rows_scanned == 3


def test_lost_final_page_response_is_safely_repeatable_without_delivery_authority(
    tmp_path: Path,
) -> None:
    """An old checkpoint repeats source changes but cannot authorize a send."""
    db_path = tmp_path / "lost-checkpoint.db"
    init_store(db_path)
    checkpoint = _bootstrap_checkpoint(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_turn(conn, "repeat-a", 1)
        _insert_turn(conn, "repeat-b", 2)
        conn.commit()

    first = turn_delta_payload_from_store(
        db_path, HOST, watermark=checkpoint, limit=10, now=1_800_000_000
    )
    assert first["has_more"] is False
    assert first["checkpoint"].startswith("twdelta1.")
    repeated = turn_delta_payload_from_store(
        db_path, HOST, watermark=checkpoint, limit=10, now=1_800_000_001
    )
    assert repeated["changes"] == first["changes"]
    assert repeated["checkpoint"] == first["checkpoint"]


def test_goal13_acceptance_9_token_outcomes_compaction_and_store_epoch_rebuild(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "tokens.db"
    init_store(db_path)
    checkpoint = _bootstrap_checkpoint(db_path)
    assert turn_delta_payload_from_store(
        db_path, HOST, watermark="twdelta1.not-json"
    )["status"] == "invalid_watermark"
    assert turn_delta_payload_from_store(
        db_path, "different-host", watermark=checkpoint
    )["status"] == "cross_host_watermark"
    assert turn_delta_payload_from_store(
        db_path, HOST, watermark=_incompatible_watermark(checkpoint)
    )["status"] == "incompatible_schema"
    assert turn_delta_payload_from_store(
        tmp_path / "missing.db", HOST
    )["status"] == "store_unavailable"
    future_store = tmp_path / "future.db"
    with sqlite3.connect(str(future_store)) as conn:
        conn.execute("PRAGMA user_version=99")
    assert turn_delta_payload_from_store(
        future_store, HOST
    )["status"] == "store_unavailable"

    with sqlite3.connect(str(db_path)) as conn:
        for index in range(3):
            _insert_turn(
                conn,
                f"old-{index}",
                index + 1,
                updated_at=OLD_TS,
            )
        conn.commit()
    compacted = compact_turn_change_journal(
        db_path,
        HOST,
        retention_days=1,
        retention_count=1,
        batch_size=10,
        now="2030-07-18T12:00:00+00:00",
    )
    assert compacted["deleted"] == 2
    assert turn_delta_payload_from_store(
        db_path, HOST, watermark=checkpoint
    )["status"] == "expired_watermark"

    page = turn_delta_payload_from_store(db_path, HOST, limit=1, now=1_800_000_000)
    assert page["has_more"] is True
    resumed_bootstrap = turn_delta_payload_from_store(
        db_path,
        HOST,
        cursor=page["next_cursor"],
        limit=1,
        now=1_800_000_001,
    )
    assert resumed_bootstrap.get("ok") is not False
    assert resumed_bootstrap["mode"] == "bootstrap"
    assert turn_delta_payload_from_store(
        db_path,
        HOST,
        cursor=page["next_cursor"],
        limit=1,
        now=1_800_000_301,
    )["status"] == "expired_cursor"
    assert turn_delta_payload_from_store(
        db_path, HOST, cursor="twdeltac1.bad", limit=1
    )["status"] == "invalid_cursor"
    assert turn_delta_payload_from_store(
        db_path, HOST, bootstrap_max_rows=1
    )["status"] == "bootstrap_too_large"

    rebuilt = tmp_path / "rebuilt.db"
    init_store(rebuilt)
    assert turn_delta_payload_from_store(
        rebuilt, HOST, watermark=checkpoint
    )["status"] == "invalid_watermark"


@pytest.mark.parametrize("source_version", range(18))
def test_goal13_acceptance_11_every_prior_migration_installs_empty_v18_journal(
    tmp_path: Path,
    source_version: int,
) -> None:
    db_path = tmp_path / f"migration-{source_version}.db"
    with sqlite3.connect(str(db_path)) as conn:
        store_sqlite._run_migrations(conn, target_version=source_version)
        assert conn.execute("PRAGMA user_version").fetchone() == (source_version,)
        store_sqlite._run_migrations(conn, target_version=18)
        assert conn.execute("PRAGMA user_version").fetchone() == (18,)
        assert conn.execute("SELECT COUNT(*) FROM turn_change_journal").fetchone() == (0,)
        columns = tuple(
            row[1] for row in conn.execute("PRAGMA table_info(turn_change_journal)")
        )
        assert columns == ("seq", "host_id", "turn_id", "op", "changed_at")
        epoch = conn.execute(
            "SELECT store_epoch FROM turn_change_state WHERE scope = 'turn-delta'"
        ).fetchone()
        assert epoch is not None and len(str(epoch[0])) >= 32
        trigger_names = {
            str(row[0])
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'trigger'"
            )
        }
        assert {
            "trg_turn_change_after_insert",
            "trg_turn_change_after_update",
            "trg_turn_change_after_delete",
            "trg_turn_change_revision_current",
            "trg_turn_change_revision_insert_current",
            "trg_turn_change_journal_no_update",
        } <= trigger_names
        assert conn.execute("PRAGMA integrity_check").fetchone() == ("ok",)
        assert conn.execute("PRAGMA foreign_key_check").fetchall() == []


def test_goal13_capture_is_trigger_backed_immutable_and_public_minimal(tmp_path: Path) -> None:
    db_path = tmp_path / "capture.db"
    init_store(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_turn(
            conn,
            "safe-turn",
            1,
            extra={
                "summary": "public summary",
                "pane_id": "private-pane-sentinel",
                "auth_token": "private-token-sentinel",
            },
        )
        conn.commit()
        row = conn.execute(
            "SELECT host_id, turn_id, op, changed_at FROM turn_change_journal"
        ).fetchone()
        assert row is not None
        assert row[:3] == (HOST, "safe-turn", "upsert")
        assert str(row[3]).endswith("+00:00")
        with pytest.raises(sqlite3.IntegrityError, match="immutable"):
            conn.execute("UPDATE turn_change_journal SET op = 'remove'")
        conn.rollback()

    bootstrap = turn_delta_payload_from_store(db_path, HOST)
    serialized = stable_json_dumps(bootstrap)
    assert "private-pane-sentinel" not in serialized
    assert "private-token-sentinel" not in serialized
    assert bootstrap["changes"][0]["turn"]["summary"] == "public summary"


def test_delta_page_bytes_do_not_change_when_submission_sweep_fires_mid_request(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "sweep-byte-stability.db"
    init_store(db_path)
    worker = {"id": "worker-0"}
    with sqlite3.connect(str(db_path)) as conn:
        store_sqlite._insert_turn_submission_conn(
            conn,
            host_id=HOST,
            request_id="submission-to-expire",
            worker=worker,
            instruction_text="unobserved instruction",
            current=TS,
            link_window_seconds=1,
            hard_ttl_seconds=2,
        )
        assert store_sqlite._terminalize_turn_submission_conn(
            conn,
            host_id=HOST,
            request_id="submission-to-expire",
            terminal_state="accepted",
            current=TS,
        )
        _insert_turn(conn, "stable-turn", 1, summary="stable page")
        conn.commit()

    sweep_due = iter((False, True))

    def reserve_sweep(*_args: Any, **_kwargs: Any) -> tuple[tuple[str, str, str], bool]:
        return ((str(db_path), HOST, "submission_links"), next(sweep_due))

    monkeypatch.setattr(
        store_sqlite,
        "_reserve_lazy_submission_link_sweep",
        reserve_sweep,
    )
    monkeypatch.setattr(store_sqlite.time, "perf_counter", lambda: 100.0)
    current = datetime.fromisoformat(TS).timestamp() + 3

    before = stable_json_dumps(
        turn_delta_payload_from_store(
            db_path,
            HOST,
            now=current,
            turn_model="observed",
        )
    ).encode("utf-8")
    during = stable_json_dumps(
        turn_delta_payload_from_store(
            db_path,
            HOST,
            now=current,
            turn_model="observed",
        )
    ).encode("utf-8")

    assert during == before
    with sqlite3.connect(str(db_path)) as conn:
        assert conn.execute(
            "SELECT state FROM turn_submissions WHERE host_id = ? AND request_id = ?",
            (HOST, "submission-to-expire"),
        ).fetchone() == ("expired",)


def test_real_turn_writers_all_capture_journal_changes(tmp_path: Path) -> None:
    db_path = tmp_path / "real-writer-capture.db"
    snapshot = project_from_raw(
        Config(host_id=HOST, db_path=db_path),
        workers=[
            {
                "id": "worker-0",
                "name": "Journal Worker",
                "status": "active",
                "meta": {
                    "stable_key": "wsk1_" + ("a" * 64),
                    "stable_key_version": 1,
                },
            }
        ],
    )
    init_store(db_path)
    store_sqlite.save_snapshot(db_path, snapshot)

    def journal_high() -> int:
        with sqlite3.connect(str(db_path)) as conn:
            return int(
                conn.execute(
                    "SELECT COALESCE(MAX(seq), 0) FROM turn_change_journal"
                ).fetchone()[0]
            )

    def journal_since(sequence: int) -> list[tuple[str, str]]:
        with sqlite3.connect(str(db_path)) as conn:
            return [
                (str(row[0]), str(row[1]))
                for row in conn.execute(
                    """
                    SELECT turn_id, op FROM turn_change_journal
                    WHERE seq > ? ORDER BY seq
                    """,
                    (sequence,),
                ).fetchall()
            ]

    before = journal_high()
    assert store_sqlite.merge_turn_content(
        db_path,
        HOST,
        "worker-0",
        {
            "source_turn_id": "journal-final-source",
            "user_text": "capture this observed turn",
            "assistant_stream_text": "working through the request",
            "complete": False,
            "has_open_turn": True,
        },
        observed_at="2026-01-01T00:01:00+00:00",
    ) == 1
    first_rows = journal_since(before)
    assert first_rows and all(op == "upsert" for _turn_id, op in first_rows)

    before = journal_high()
    applied = store_sqlite.apply_turn_refresh(
        db_path,
        HOST,
        "worker-0",
        {
            "source_turn_id": "journal-final-source",
            "assistant_final_text": "captured final",
            "complete": True,
            "has_open_turn": False,
        },
        observed_at="2026-01-01T00:02:00+00:00",
    )
    assert applied.updated == 1
    applied_rows = journal_since(before)
    assert applied_rows and all(op == "upsert" for _turn_id, op in applied_rows)


def test_turn_delta_rpc_advertises_feature_and_cannot_invoke_delivery(tmp_path: Path) -> None:
    db_path = tmp_path / "authority.db"
    init_store(db_path)
    delivery_calls: list[Any] = []
    api = TendwireDaemonAPI(
        get_snapshot=lambda: None,  # Not reached by this method.
        get_health=lambda: {},
        submit_command=lambda request: delivery_calls.append(request) or {},
        get_turn_delta=lambda **params: turn_delta_payload_from_store(
            db_path, HOST, **params
        ),
        connector_call=lambda method, params: delivery_calls.append((method, params)) or {},
    )
    response = api.dispatch({"method": "turn.delta", "params": {"limit": 10}})
    assert "turn.delta" in REQUIRED_METHODS
    assert response["ok"] is True
    assert response["result"]["mode"] == "bootstrap"
    assert delivery_calls == []


def test_turn_delta_cli_bootstrap_and_incremental_read(tmp_path: Path, capsys) -> None:
    db_path = tmp_path / "cli.db"
    socket_path = tmp_path / "missing.sock"
    init_store(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_turn(conn, "cli-turn", 1, summary="first CLI projection")
        conn.commit()

    base_args = [
        "--host-id",
        HOST,
        "--socket-path",
        str(socket_path),
        "turn",
        "delta",
        "--db-path",
        str(db_path),
    ]
    assert main(base_args) == 0
    bootstrap = json.loads(capsys.readouterr().out)
    assert bootstrap["changes"][0]["turn"]["summary"] == "first CLI projection"
    checkpoint = str(bootstrap["checkpoint"])

    _mutate_turn(db_path, "cli-turn", summary="second CLI projection")
    assert main([*base_args, "--watermark", checkpoint]) == 0
    changed = json.loads(capsys.readouterr().out)
    assert changed["changes"][0]["turn"]["summary"] == "second CLI projection"


def test_turn_change_retention_config_defaults_env_and_bounds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    defaults = Config()
    assert defaults.turn_change_retention_days == 7
    assert defaults.turn_change_retention_count == 100_000
    assert defaults.turn_change_compaction_batch_size == 1_000
    monkeypatch.setenv("TENDWIRE_TURN_CHANGE_RETENTION_DAYS", "11")
    monkeypatch.setenv("TENDWIRE_TURN_CHANGE_RETENTION_COUNT", "1234")
    monkeypatch.setenv("TENDWIRE_TURN_CHANGE_COMPACTION_BATCH_SIZE", "77")
    configured = load_config()
    assert configured.turn_change_retention_days == 11
    assert configured.turn_change_retention_count == 1234
    assert configured.turn_change_compaction_batch_size == 77
    with pytest.raises(ValueError, match="turn_change_compaction_batch_size"):
        Config(turn_change_compaction_batch_size=10_001)


def test_due_daemon_maintenance_compacts_delta_journal_with_configured_policy(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "maintenance.db"
    init_store(db_path)
    calls: list[tuple[Any, ...]] = []

    monkeypatch.setattr(
        store_sqlite,
        "maybe_run_automatic_store_maintenance",
        lambda *_args, **_kwargs: {
            "ok": True,
            "status": "ok",
            "due": True,
            "snapshot": {
                "examined": 0,
                "deleted": 0,
                "remaining_candidates": False,
            },
        },
    )

    def compact(path: Path, host_id: str, **kwargs: Any) -> dict[str, Any]:
        calls.append((path, host_id, kwargs))
        return {"ok": True}

    monkeypatch.setattr(store_sqlite, "compact_turn_change_journal", compact)
    daemon = TendwireDaemon(
        Config(
            host_id=HOST,
            db_path=db_path,
            turn_change_retention_days=13,
            turn_change_retention_count=2345,
            turn_change_compaction_batch_size=89,
        )
    )
    daemon._after_snapshot_saved()
    assert calls == [
        (
            db_path,
            HOST,
            {"retention_days": 13, "retention_count": 2345, "batch_size": 89},
        )
    ]


@pytest.mark.skipif(not hasattr(__import__("socket"), "AF_UNIX"), reason="Unix sockets required")
def test_goal13_acceptance_10_unix_socket_noop_and_one_update_p95_under_350ms(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "latency.db"
    init_store(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_turn(conn, "live", 1)
        conn.commit()
    checkpoint = _bootstrap_checkpoint(db_path)

    api = TendwireDaemonAPI(
        get_snapshot=lambda: None,
        get_health=lambda: {},
        submit_command=lambda _request: {},
        get_turn_delta=lambda **params: turn_delta_payload_from_store(
            db_path, HOST, **params
        ),
    )
    socket_path = tmp_path / "delta.sock"
    stop_event = threading.Event()
    server = UnixSocketJSONServer(socket_path, api.dispatch, stop_event=stop_event)
    server.start()
    thread = threading.Thread(target=server.serve_forever)
    thread.start()
    client = DaemonAPIClient(socket_path, timeout_seconds=2)
    try:
        for _ in range(5):
            response = client.request(
                "turn.delta", {"watermark": checkpoint, "limit": 10}
            )
            assert response["result"]["changes"] == []
            checkpoint = str(response["result"]["checkpoint"])

        noop_ms: list[float] = []
        for _ in range(51):
            started = time.perf_counter()
            response = client.request(
                "turn.delta", {"watermark": checkpoint, "limit": 10}
            )
            noop_ms.append((time.perf_counter() - started) * 1000)
            assert response["result"]["changes"] == []
            checkpoint = str(response["result"]["checkpoint"])

        update_ms: list[float] = []
        for index in range(51):
            _mutate_turn(db_path, "live", summary=f"working update {index}")
            started = time.perf_counter()
            response = client.request(
                "turn.delta", {"watermark": checkpoint, "limit": 10}
            )
            update_ms.append((time.perf_counter() - started) * 1000)
            changes = response["result"]["changes"]
            assert [(item["op"], item["turn_id"]) for item in changes] == [
                ("upsert", "live")
            ]
            checkpoint = str(response["result"]["checkpoint"])

        assert _nearest_rank_p95(noop_ms) <= 350
        assert _nearest_rank_p95(update_ms) <= 350
    finally:
        stop_event.set()
        server.close()
        thread.join(timeout=2)
    assert not thread.is_alive()
