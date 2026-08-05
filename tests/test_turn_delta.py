"""Turn list/delta paging, retained-floor, and sequence-allocation contracts."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from tendwire.core.models import Snapshot, Worker, WorkerBinding
from tendwire.store.projection import save_snapshot
from tendwire.store.retention import RetentionPolicy, run_retention_cycle
from tendwire.store.schema import init_store
from tendwire.store.turns import (
    get_turn_content,
    turn_delta_payload_from_store,
    turns_payload_from_store,
)
from .store_helpers import append_test_turn


NOW = "2026-08-05T00:00:00.000000Z"
MAX_RESPONSE_BYTES = 850_000


def _seed(db_path: Path) -> None:
    init_store(db_path)
    worker = Worker(
        id="worker-a",
        name="codex",
        meta={"stable_key": "wsk1_" + "a" * 64, "stable_key_version": 1},
    )
    binding = WorkerBinding(
        host_id="host-a",
        worker_id=worker.id,
        worker_fingerprint=worker.fingerprint,
        backend="herdr",
        target_kind="agent_id",
        target_value="private-a",
        observed_at=NOW,
        private_fingerprint="private-a",
    )
    save_snapshot(
        db_path,
        Snapshot(host_id="host-a", updated_at=NOW, workers=[worker]),
        worker_bindings=[binding],
        binding_backend="herdr",
    )


def _turn(
    db_path: Path,
    turn_id: str,
    text: str = "answer",
    *,
    observed_at: str = "2026-01-01T00:00:00Z",
) -> None:
    result = append_test_turn(
        db_path,
        "host-a",
        "worker-a",
        {
            "source_turn_id": turn_id,
            "user_text": "question",
            "assistant_final_text": text,
            "complete": True,
        },
        observed_at=observed_at,
    )
    assert result.updated == 1


def _remove(db_path: Path, turn_id: str, observed_at: str) -> None:
    result = append_test_turn(
        db_path,
        "host-a",
        "worker-a",
        {"source_turn_id": turn_id, "removed": True},
        observed_at=observed_at,
    )
    assert result.updated == 1


def _terminalize_turn_work(db_path: Path, turn_id: str) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """UPDATE connector_outbox
            SET status='delivered',updated_at='2026-01-03T00:00:00.000000Z'
            WHERE turn_id=? AND status IN('queued','retry','deferred')""",
            (turn_id,),
        )


def _retention(db_path: Path) -> dict[str, object]:
    return run_retention_cycle(
        db_path,
        policy=RetentionPolicy(batch_size=100),
        now="2026-08-05T00:00:00Z",
    )


def test_bootstrap_delta_then_change_watermark(tmp_path: Path) -> None:
    db_path = tmp_path / "turns.db"
    _seed(db_path)
    _turn(db_path, "turn-a", "first")
    bootstrap = turn_delta_payload_from_store(db_path, "host-a")
    assert bootstrap["mode"] == "bootstrap"
    assert bootstrap["changes"][0]["turn_id"] == "turn-a"
    assert bootstrap["checkpoint"]
    _turn(
        db_path,
        "turn-a",
        "second",
        observed_at="2026-01-01T00:00:01Z",
    )
    changed = turn_delta_payload_from_store(
        db_path, "host-a", watermark=bootstrap["checkpoint"]
    )
    assert changed["mode"] == "changes"
    assert [item["turn_id"] for item in changed["changes"]] == ["turn-a"]


@pytest.mark.parametrize(
    ("field", "complete"),
    [("assistant_final_text", True), ("assistant_stream_text", False)],
)
def test_older_projection_cannot_replace_newer_turn_or_outbox(
    tmp_path: Path, field: str, complete: bool
) -> None:
    db_path = tmp_path / "turns.db"
    _seed(db_path)
    fresh = append_test_turn(
        db_path,
        "host-a",
        "worker-a",
        {"source_turn_id": "turn-a", field: "newer", "complete": complete},
        observed_at="2026-08-05T00:00:10Z",
    )
    assert fresh.updated == 1
    with sqlite3.connect(db_path) as conn:
        before_turn = conn.execute(
            "SELECT payload_json,change_sequence,state,observed_at FROM turns"
        ).fetchall()
        before_outbox = conn.execute(
            """SELECT key,status,payload_json,partition_sequence
            FROM connector_outbox ORDER BY id"""
        ).fetchall()
    stale = append_test_turn(
        db_path,
        "host-a",
        "worker-a",
        {"source_turn_id": "turn-a", field: "older", "complete": complete},
        observed_at="2026-08-05T00:00:09Z",
        expected_updated=0,
    )
    assert stale.updated == 0
    with sqlite3.connect(db_path) as conn:
        assert conn.execute(
            "SELECT payload_json,change_sequence,state,observed_at FROM turns"
        ).fetchall() == before_turn
        assert conn.execute(
            """SELECT key,status,payload_json,partition_sequence
            FROM connector_outbox ORDER BY id"""
        ).fetchall() == before_outbox


def test_stale_removal_cannot_mutate_newer_turn_or_outbox(tmp_path: Path) -> None:
    db_path = tmp_path / "turns.db"
    _seed(db_path)
    assert append_test_turn(
        db_path,
        "host-a",
        "worker-a",
        {"source_turn_id": "turn-a", "assistant_final_text": "newer", "complete": True},
        observed_at="2026-08-05T00:00:10Z",
    ).updated == 1
    with sqlite3.connect(db_path) as conn:
        before = conn.execute(
            "SELECT payload_json,change_sequence,state,observed_at,removed_at FROM turns"
        ).fetchall()
        outbox = conn.execute(
            "SELECT key,status,payload_json FROM connector_outbox ORDER BY id"
        ).fetchall()
    assert append_test_turn(
        db_path,
        "host-a",
        "worker-a",
        {"source_turn_id": "turn-a", "removed": True},
        observed_at="2026-08-05T00:00:09Z",
        expected_updated=0,
    ).updated == 0
    with sqlite3.connect(db_path) as conn:
        assert conn.execute(
            "SELECT payload_json,change_sequence,state,observed_at,removed_at FROM turns"
        ).fetchall() == before
        assert conn.execute(
            "SELECT key,status,payload_json FROM connector_outbox ORDER BY id"
        ).fetchall() == outbox


def test_list_and_delta_cursors_page_without_duplicates(tmp_path: Path) -> None:
    db_path = tmp_path / "turns.db"
    _seed(db_path)
    for index in range(3):
        _turn(db_path, f"turn-{index}")
    first = turns_payload_from_store(db_path, "host-a", schema_version=2, limit=1)
    second = turns_payload_from_store(
        db_path,
        "host-a",
        schema_version=2,
        limit=1,
        cursor=first["next_cursor"],
    )
    assert first["has_more"] and second["turns"]
    assert first["turns"][0]["id"] != second["turns"][0]["id"]
    delta_first = turn_delta_payload_from_store(db_path, "host-a", limit=1)
    delta_second = turn_delta_payload_from_store(
        db_path, "host-a", limit=1, cursor=delta_first["next_cursor"]
    )
    assert delta_first["changes"][0]["turn_id"] != delta_second["changes"][0]["turn_id"]


def test_pruning_does_not_reuse_published_insertion_or_change_sequences(tmp_path: Path) -> None:
    """A retained allocator/sentinel must survive deletion of every visible turn."""
    db_path = tmp_path / "turns.db"
    _seed(db_path)
    _turn(db_path, "turn-a")
    _turn(db_path, "turn-b")
    _remove(db_path, "turn-a", "2026-01-02T00:00:00Z")
    _remove(db_path, "turn-b", "2026-01-02T00:00:01Z")
    with sqlite3.connect(db_path) as conn:
        published = conn.execute(
            "SELECT MAX(insertion_sequence),MAX(change_sequence) FROM turns"
        ).fetchone()
    _terminalize_turn_work(db_path, "turn-a")
    _terminalize_turn_work(db_path, "turn-b")
    _retention(db_path)
    with sqlite3.connect(db_path) as conn:
        retained = conn.execute(
            "SELECT turn_id,insertion_sequence,change_sequence,removed_at FROM turns"
        ).fetchall()
    assert retained == [("turn-b", published[0], published[1], "2026-01-02T00:00:01.000000Z")]
    _turn(db_path, "turn-c")
    with sqlite3.connect(db_path) as conn:
        allocated = conn.execute(
            "SELECT insertion_sequence,change_sequence FROM turns WHERE turn_id='turn-c'"
        ).fetchone()
    assert allocated == (published[0] + 1, published[1] + 1)


def test_retention_keeps_removed_turn_referenced_by_live_outbox(tmp_path: Path) -> None:
    db_path = tmp_path / "turns.db"
    _seed(db_path)
    _turn(db_path, "turn-a")
    _remove(db_path, "turn-a", "2026-01-02T00:00:00Z")
    _retention(db_path)
    with sqlite3.connect(db_path) as conn:
        assert conn.execute(
            "SELECT removed_at FROM turns WHERE turn_id='turn-a'"
        ).fetchone() == ("2026-01-02T00:00:00.000000Z",)


def test_retention_does_not_use_fresh_max_removed_turn_as_floor_marker(
    tmp_path: Path,
) -> None:
    """An ineligible fresh max row keeps its payload and canonical content."""
    db_path = tmp_path / "turns.db"
    _seed(db_path)
    _turn(db_path, "turn-old", "old candidate")
    _turn(db_path, "turn-fresh", "fresh protected content")
    _remove(db_path, "turn-old", "2026-01-02T00:00:00Z")
    _remove(db_path, "turn-fresh", "2026-08-04T00:00:00Z")
    _terminalize_turn_work(db_path, "turn-old")
    _terminalize_turn_work(db_path, "turn-fresh")
    with sqlite3.connect(db_path) as conn:
        before = conn.execute(
            "SELECT state,content_revision,payload_json FROM turns WHERE turn_id='turn-fresh'"
        ).fetchone()
        revision_count = conn.execute(
            "SELECT COUNT(*) FROM turn_content_revisions WHERE turn_id='turn-fresh'"
        ).fetchone()[0]
    assert before is not None and before[1] is not None and revision_count == 1

    _retention(db_path)

    with sqlite3.connect(db_path) as conn:
        after = conn.execute(
            "SELECT state,content_revision,payload_json FROM turns WHERE turn_id='turn-fresh'"
        ).fetchone()
        revision_count = conn.execute(
            "SELECT COUNT(*) FROM turn_content_revisions WHERE turn_id='turn-fresh'"
        ).fetchone()[0]
    assert after == before
    assert revision_count == 1


def test_seven_day_turn_pruning_preserves_content_until_45_day_floor(
    tmp_path: Path,
) -> None:
    """A previously issued content descriptor remains usable for 45 days."""
    db_path = tmp_path / "turns.db"
    _seed(db_path)
    _turn(db_path, "turn-eight-days", "content still targetable")
    listed = turns_payload_from_store(db_path, "host-a", schema_version=2)
    descriptor = listed["turns"][0]["content"]
    revision = descriptor["content_revision"]
    _turn(db_path, "turn-old-sentinel", "expired sentinel content")
    _remove(db_path, "turn-eight-days", "2026-07-28T00:00:00Z")
    _remove(db_path, "turn-old-sentinel", "2026-01-02T00:00:00Z")
    _terminalize_turn_work(db_path, "turn-eight-days")
    _terminalize_turn_work(db_path, "turn-old-sentinel")

    _retention(db_path)

    page = get_turn_content(
        db_path,
        "host-a",
        turn_id="turn-eight-days",
        content_revision=revision,
        field="assistant_final_text",
    )
    assert page.get("status") is None
    assert page["text"] == "content still targetable"


def test_pruned_list_since_token_expires_instead_of_missing_new_turn(tmp_path: Path) -> None:
    db_path = tmp_path / "turns.db"
    _seed(db_path)
    _turn(db_path, "turn-a")
    since = turns_payload_from_store(db_path, "host-a", schema_version=2)["since"]
    _turn(db_path, "turn-b")
    _remove(db_path, "turn-a", "2026-01-02T00:00:00Z")
    _remove(db_path, "turn-b", "2026-01-02T00:00:01Z")
    _terminalize_turn_work(db_path, "turn-a")
    _terminalize_turn_work(db_path, "turn-b")
    _retention(db_path)
    _turn(db_path, "turn-c")
    result = turns_payload_from_store(
        db_path, "host-a", schema_version=2, since=since
    )
    assert result.get("status") == "since_expired" or [
        turn["id"] for turn in result.get("turns", [])
    ] == ["turn-c"]


def test_pruned_delta_watermark_expires_instead_of_missing_new_change(tmp_path: Path) -> None:
    db_path = tmp_path / "turns.db"
    _seed(db_path)
    _turn(db_path, "turn-a")
    watermark = turn_delta_payload_from_store(db_path, "host-a")["checkpoint"]
    _turn(db_path, "turn-b")
    _remove(db_path, "turn-a", "2026-01-02T00:00:00Z")
    _remove(db_path, "turn-b", "2026-01-02T00:00:01Z")
    _terminalize_turn_work(db_path, "turn-a")
    _terminalize_turn_work(db_path, "turn-b")
    _retention(db_path)
    _turn(db_path, "turn-c")
    result = turn_delta_payload_from_store(db_path, "host-a", watermark=watermark)
    assert result.get("status") in {"expired_watermark", "invalid_watermark"} or [
        change["turn_id"] for change in result.get("changes", [])
    ] == ["turn-c"]


def test_cursor_anchor_pruned_returns_cursor_expired(tmp_path: Path) -> None:
    db_path = tmp_path / "turns.db"
    _seed(db_path)
    for turn_id in ("turn-a", "turn-b", "turn-c"):
        _turn(db_path, turn_id)
    first = turns_payload_from_store(db_path, "host-a", schema_version=2, limit=1)
    anchor = first["turns"][0]["id"]
    _remove(db_path, anchor, "2026-01-02T00:00:00Z")
    # Publish a newer allocator position so the removed anchor is not the
    # retained max-sequence sentinel.
    _turn(db_path, "turn-d")
    _remove(db_path, "turn-d", "2026-01-02T00:00:01Z")
    _terminalize_turn_work(db_path, anchor)
    _terminalize_turn_work(db_path, "turn-d")
    _retention(db_path)
    resumed = turns_payload_from_store(
        db_path,
        "host-a",
        schema_version=2,
        limit=1,
        cursor=first["next_cursor"],
    )
    assert resumed.get("status") == "cursor_expired"


def test_v1_upgrade_required_precedes_oversized_v2_error(tmp_path: Path) -> None:
    db_path = tmp_path / "turns.db"
    _seed(db_path)
    _turn(db_path, "turn-a", "x" * 20_000)
    assert turns_payload_from_store(db_path, "host-a", schema_version=1)["status"] == "upgrade_required"
    with sqlite3.connect(db_path) as conn:
        payload = json.loads(conn.execute("SELECT payload_json FROM turns").fetchone()[0])
        payload["oversized_public_field"] = "x" * (MAX_RESPONSE_BYTES + 1024)
        conn.execute("UPDATE turns SET payload_json=?", (json.dumps(payload, separators=(",", ":")),))
    v2 = turns_payload_from_store(db_path, "host-a", schema_version=2)
    assert v2.get("ok") is False or len(json.dumps(v2, separators=(",", ":")).encode()) <= MAX_RESPONSE_BYTES


@pytest.mark.parametrize("surface", ["list", "delta"])
def test_oversized_first_row_is_rejected_or_bounded(
    tmp_path: Path, surface: str
) -> None:
    """The byte budget applies to the first selected row, not only later rows."""
    db_path = tmp_path / "turns.db"
    _seed(db_path)
    _turn(db_path, "turn-a")
    with sqlite3.connect(db_path) as conn:
        payload = json.loads(
            conn.execute("SELECT payload_json FROM turns WHERE turn_id='turn-a'").fetchone()[0]
        )
        payload["oversized_public_field"] = "x" * (MAX_RESPONSE_BYTES + 1024)
        conn.execute(
            "UPDATE turns SET payload_json=? WHERE turn_id='turn-a'",
            (json.dumps(payload, separators=(",", ":")),),
        )
    result = (
        turns_payload_from_store(db_path, "host-a", schema_version=2)
        if surface == "list"
        else turn_delta_payload_from_store(db_path, "host-a")
    )
    encoded = json.dumps(result, separators=(",", ":")).encode()
    assert result.get("ok") is False or len(encoded) <= MAX_RESPONSE_BYTES
