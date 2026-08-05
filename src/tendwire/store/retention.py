"""Bounded child-first retention and WAL checkpoint."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from .db import _connect, canonical_utc, utc_now, write_transaction
from .receipts import settle_due_submission_links_conn

TURN_FINAL_TARGETABLE_RETENTION_DAYS = 30
TURN_FINAL_ROUTE_CONTENT_RETENTION_DAYS = 45


@dataclass(frozen=True)
class RetentionPolicy:
    event_retention_days: int = 7
    snapshot_retention_days: int = 14
    targetable_retention_days: int = TURN_FINAL_TARGETABLE_RETENTION_DAYS
    route_content_retention_days: int = TURN_FINAL_ROUTE_CONTENT_RETENTION_DAYS
    command_retention_days: int = 30
    batch_size: int = 100
    turn_change_retention_days: int = 7
    turn_change_batch_size: int = 1_000

    def __post_init__(self) -> None:
        values = (
            self.event_retention_days,
            self.snapshot_retention_days,
            self.targetable_retention_days,
            self.route_content_retention_days,
            self.command_retention_days,
            self.batch_size,
            self.turn_change_retention_days,
            self.turn_change_batch_size,
        )
        if any(isinstance(value, bool) or not isinstance(value, int) or value < 1 for value in values):
            raise ValueError("retention values must be positive integers")
        if self.targetable_retention_days < TURN_FINAL_TARGETABLE_RETENTION_DAYS:
            raise ValueError("targetable retention cannot be shorter than 30 days")
        if self.route_content_retention_days < TURN_FINAL_ROUTE_CONTENT_RETENTION_DAYS:
            raise ValueError("route/content retention cannot be shorter than 45 days")
        if self.batch_size > 1000:
            raise ValueError("retention batch_size must not exceed 1000")
        if self.turn_change_batch_size > 10_000:
            raise ValueError("turn_change_batch_size must not exceed 10000")


def _cutoff(now: str, days: int) -> str:
    parsed = datetime.fromisoformat(canonical_utc(now).replace("Z", "+00:00"))
    return canonical_utc(parsed - timedelta(days=days))


def _delete_ids(conn: Any, table: str, ids: list[int]) -> int:
    if not ids:
        return 0
    marks = ",".join("?" for _ in ids)
    return int(conn.execute(f"DELETE FROM {table} WHERE id IN ({marks})", ids).rowcount)


def _events(conn: Any, cutoff: str, batch: int) -> int:
    ids = [int(row[0]) for row in conn.execute(
        "SELECT id FROM agent_events WHERE observed_at<? ORDER BY id LIMIT ?",
        (cutoff, batch),
    ).fetchall()]
    return _delete_ids(conn, "agent_events", ids)


def _snapshots(conn: Any, cutoff: str, batch: int) -> int:
    ids = [int(row[0]) for row in conn.execute(
        """SELECT id FROM snapshots candidate WHERE observed_at<?
        AND id<>(SELECT id FROM snapshots newest
          WHERE newest.host_id=candidate.host_id
          ORDER BY observed_at DESC,authority_fingerprint DESC,id DESC LIMIT 1)
        ORDER BY id LIMIT ?""",
        (cutoff, batch),
    ).fetchall()]
    return _delete_ids(conn, "snapshots", ids)


def _deliveries(conn: Any, cutoff: str, batch: int) -> int:
    ids = [int(row[0]) for row in conn.execute(
        """SELECT d.id FROM connector_deliveries d JOIN connector_outbox o ON o.id=d.outbox_id
        WHERE d.status NOT IN('leased','awaiting_ack') AND d.settled_at<?
          AND o.status IN('delivered','superseded','dead_letter')
          AND NOT (o.kind='retire' AND o.status='dead_letter')
          AND o.updated_at<? AND o.current_delivery_id IS NULL
          AND NOT EXISTS(
            SELECT 1 FROM connector_outbox child
            WHERE child.predecessor_outbox_id=o.id
               OR child.replaces_outbox_id=o.id
               OR child.target_outbox_id=o.id
               OR child.source_outbox_id=o.id
          )
        ORDER BY d.id LIMIT ?""",
        (cutoff, cutoff, batch),
    ).fetchall()]
    return _delete_ids(conn, "connector_deliveries", ids)


def _outbox(conn: Any, cutoff: str, batch: int) -> int:
    ids = [int(row[0]) for row in conn.execute(
        """SELECT o.id FROM connector_outbox o
        WHERE o.status IN('delivered','superseded','dead_letter') AND o.updated_at<?
          AND NOT (o.kind='retire' AND o.status='dead_letter')
          AND o.current_delivery_id IS NULL
          AND NOT EXISTS(SELECT 1 FROM connector_outbox child WHERE
            child.predecessor_outbox_id=o.id OR child.replaces_outbox_id=o.id
            OR child.target_outbox_id=o.id OR child.source_outbox_id=o.id)
          AND NOT EXISTS(SELECT 1 FROM connector_deliveries d WHERE d.outbox_id=o.id)
        ORDER BY o.id LIMIT ?""",
        (cutoff, batch),
    ).fetchall()]
    return _delete_ids(conn, "connector_outbox", ids)


def _receipts(conn: Any, cutoff: str, batch: int) -> int:
    rows = conn.execute(
        """SELECT host_id,request_id FROM command_receipts r WHERE state IN('accepted','rejected','uncertain')
        AND updated_at<? AND NOT EXISTS(SELECT 1 FROM turn_submissions s WHERE s.host_id=r.host_id AND s.request_id=r.request_id)
        ORDER BY updated_at LIMIT ?""",
        (cutoff, batch),
    ).fetchall()
    for row in rows:
        conn.execute("DELETE FROM command_receipts WHERE host_id=? AND request_id=?", (row[0], row[1]))
    return len(rows)


def _submissions(conn: Any, cutoff: str, batch: int) -> int:
    rows = conn.execute(
        """SELECT host_id,submission_id FROM turn_submissions
        WHERE state IN('linked','ambiguous','expired','accepted','rejected','uncertain')
          AND updated_at<?
          ORDER BY updated_at LIMIT ?""",
        (cutoff, batch),
    ).fetchall()
    for row in rows:
        conn.execute(
            "DELETE FROM turn_submissions WHERE host_id=? AND submission_id=?",
            (row[0], row[1]),
        )
    return len(rows)


def _pending_history(conn: Any, cutoff: str, batch: int) -> tuple[int, int]:
    claims = conn.execute(
        """SELECT c.host_id,c.decision_ref FROM backend_pending_claims c
        JOIN backend_pending p USING(host_id,decision_ref)
        WHERE (c.state='settled' AND c.settled_at<?)
           OR (c.state='claimed' AND c.claimed_until<?)
           OR (c.state='send_started' AND c.send_started_at<?
               AND p.state IN('closed','resolved'))
        ORDER BY COALESCE(c.settled_at,c.send_started_at,c.claimed_until) LIMIT ?""",
        (cutoff, cutoff, cutoff, batch),
    ).fetchall()
    for row in claims:
        conn.execute(
            "DELETE FROM backend_pending_claims WHERE host_id=? AND decision_ref=?",
            (row[0], row[1]),
        )
    parents = conn.execute(
        """SELECT host_id,decision_ref FROM backend_pending p
        WHERE state IN('closed','resolved') AND observed_at<?
          AND NOT EXISTS(SELECT 1 FROM backend_pending_claims c
            WHERE c.host_id=p.host_id AND c.decision_ref=p.decision_ref)
        ORDER BY observed_at LIMIT ?""",
        (cutoff, batch),
    ).fetchall()
    for row in parents:
        conn.execute(
            "DELETE FROM pending_interactions WHERE host_id=? AND decision_ref=?",
            (row[0], row[1]),
        )
        conn.execute(
            "DELETE FROM backend_pending WHERE host_id=? AND decision_ref=?",
            (row[0], row[1]),
        )
    return len(claims), len(parents)


def _content(conn: Any, cutoff: str, batch: int) -> int:
    ids = [int(row[0]) for row in conn.execute(
        """SELECT r.id FROM turn_content_revisions r WHERE r.is_current=0 AND r.created_at<?
        AND NOT EXISTS(SELECT 1 FROM connector_outbox o WHERE o.host_id=r.host_id
          AND o.turn_id=r.turn_id AND o.content_revision=r.content_revision)
        ORDER BY r.id LIMIT ?""",
        (cutoff, batch),
    ).fetchall()]
    return _delete_ids(conn, "turn_content_revisions", ids)


def _bindings(conn: Any, cutoff: str, batch: int) -> int:
    rows = conn.execute(
        """SELECT host_id,worker_id,route_generation FROM worker_bindings b WHERE expires_at IS NOT NULL
        AND expires_at<? AND route_retain_until<?
        AND NOT EXISTS(SELECT 1 FROM turns t WHERE t.host_id=b.host_id AND t.route_generation=b.route_generation)
        AND NOT EXISTS(SELECT 1 FROM connector_outbox o WHERE o.host_id=b.host_id AND o.partition_key=b.partition_key)
        ORDER BY expires_at LIMIT ?""",
        (cutoff, cutoff, batch),
    ).fetchall()
    for row in rows:
        conn.execute(
            """DELETE FROM worker_bindings
            WHERE host_id=? AND worker_id=? AND route_generation=?""",
            (row[0], row[1], row[2]),
        )
    return len(rows)


_TURN_CANDIDATE_CTE = """
WITH ranked AS (
 SELECT t.*,
  MAX(insertion_sequence) OVER(PARTITION BY host_id) max_insertion,
  MAX(change_sequence) OVER(PARTITION BY host_id) max_change
 FROM turns t
)
"""


_TURN_UNREFERENCED = """
AND NOT EXISTS(SELECT 1 FROM connector_outbox o
 WHERE o.host_id=r.host_id AND o.turn_id=r.turn_id)
AND NOT EXISTS(SELECT 1 FROM turn_submissions s
 WHERE s.host_id=r.host_id AND s.turn_id=r.turn_id)
AND NOT EXISTS(SELECT 1 FROM turn_supersessions s
 WHERE s.host_id=r.host_id AND (s.predecessor_turn_id=r.turn_id OR s.replacement_turn_id=r.turn_id))
AND NOT EXISTS(SELECT 1 FROM agent_events e
 WHERE e.host_id=r.host_id AND e.source_turn_id=r.turn_id)
"""


def _turn_candidate_host(conn: Any, cutoff: str) -> str | None:
    row = conn.execute(
        _TURN_CANDIDATE_CTE + """
        SELECT r.host_id FROM ranked r
        WHERE r.removed_at IS NOT NULL AND r.state<>'removed_floor'
          AND r.removed_at<?
          AND r.insertion_sequence<>r.max_insertion AND r.change_sequence<>r.max_change
        """ + _TURN_UNREFERENCED + " ORDER BY r.removed_at,r.host_id LIMIT 1",
        (cutoff,),
    ).fetchone()
    return None if row is None else str(row[0])


def _turn_candidates(
    conn: Any, host_id: str, cutoff: str, batch: int, *, floor: bool,
) -> list[Any]:
    state = "=" if floor else "<>"
    return conn.execute(
        _TURN_CANDIDATE_CTE + f"""
        SELECT r.host_id,r.turn_id,r.insertion_sequence,r.change_sequence,r.payload_json
        FROM ranked r WHERE r.host_id=? AND r.removed_at IS NOT NULL
          AND r.state{state}'removed_floor' AND r.removed_at<?
          AND r.insertion_sequence<>r.max_insertion AND r.change_sequence<>r.max_change
        """ + _TURN_UNREFERENCED + " ORDER BY r.change_sequence DESC LIMIT ?",
        (host_id, cutoff, batch),
    ).fetchall()


def _turn_marker(conn: Any, host_id: str, cutoff: str) -> Any:
    return conn.execute(
        """SELECT r.host_id,r.turn_id,r.insertion_sequence,r.change_sequence,r.payload_json
        FROM turns r WHERE r.host_id=? AND r.removed_at IS NOT NULL
          AND r.removed_at<?
        """ + _TURN_UNREFERENCED + " ORDER BY r.change_sequence DESC LIMIT 1",
        (host_id, cutoff),
    ).fetchone()


def _floor_values(row: Any) -> tuple[int, int]:
    try:
        floor = json.loads(str(row[4])).get("_retention_floor", {})
        return int(floor.get("insertion_sequence", 0)), int(floor.get("change_sequence", 0))
    except (AttributeError, TypeError, ValueError, json.JSONDecodeError):
        return 0, 0


def _delete_turn_content(conn: Any, row: Any) -> None:
    revision_ids = [int(item[0]) for item in conn.execute(
        "SELECT id FROM turn_content_revisions WHERE host_id=? AND turn_id=?",
        (row[0], row[1]),
    ).fetchall()]
    if revision_ids:
        marks = ",".join("?" for _ in revision_ids)
        conn.execute(
            f"DELETE FROM turn_content_page_boundaries WHERE revision_id IN ({marks})",
            revision_ids,
        )
    conn.execute(
        "DELETE FROM turn_content_revisions WHERE host_id=? AND turn_id=?",
        (row[0], row[1]),
    )


def _delete_turn(conn: Any, row: Any) -> int:
    _delete_turn_content(conn, row)
    return int(conn.execute(
        "DELETE FROM turns WHERE host_id=? AND turn_id=?",
        (row[0], row[1]),
    ).rowcount)


def _turns(conn: Any, cutoff: str, batch: int) -> tuple[int, int]:
    host_id = _turn_candidate_host(conn, cutoff)
    if host_id is None:
        return 0, 0
    rows = _turn_candidates(conn, host_id, cutoff, batch, floor=False)
    prior_floor = _turn_candidates(conn, host_id, cutoff, 1, floor=True)
    marker = _turn_marker(conn, host_id, cutoff)
    if marker is None:
        return 0, 0
    considered = list({(str(row[0]), str(row[1])): row for row in [*rows, *prior_floor, marker]}.values())
    marker_key = (str(marker[0]), str(marker[1]))
    deleted_rows = [
        row for row in [*rows, *prior_floor]
        if (str(row[0]), str(row[1])) != marker_key
    ]
    insertion_floor = max(
        [int(row[2]) for row in deleted_rows]
        + [value for row in considered for value in (_floor_values(row)[0],)],
        default=0,
    )
    change_floor = max(
        [int(row[3]) for row in deleted_rows]
        + [value for row in considered for value in (_floor_values(row)[1],)],
        default=0,
    )
    deleted = sum(_delete_turn(conn, row) for row in deleted_rows)
    _delete_turn_content(conn, marker)
    payload = {
        "id": str(marker[1]),
        "turn_id": str(marker[1]),
        "_retention_floor": {
            "insertion_sequence": insertion_floor,
            "change_sequence": change_floor,
        },
    }
    conn.execute(
        """UPDATE turns SET state='removed_floor',content_revision=NULL,payload_json=?
        WHERE host_id=? AND turn_id=?""",
        (json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")), marker[0], marker[1]),
    )
    return deleted, 1


def _floor_content(conn: Any, cutoff: str, batch: int) -> int:
    rows = conn.execute(
        """SELECT r.host_id,r.turn_id,r.insertion_sequence,r.change_sequence,r.payload_json
        FROM turns r WHERE r.state='removed_floor' AND r.content_revision IS NOT NULL
          AND r.removed_at<?
        """ + _TURN_UNREFERENCED + " ORDER BY r.removed_at LIMIT ?",
        (cutoff, batch),
    ).fetchall()
    for row in rows:
        insertion_floor, change_floor = _floor_values(row)
        _delete_turn_content(conn, row)
        payload = {
            "id": str(row[1]),
            "turn_id": str(row[1]),
            "_retention_floor": {
                "insertion_sequence": insertion_floor,
                "change_sequence": change_floor,
            },
        }
        conn.execute(
            """UPDATE turns SET content_revision=NULL,payload_json=?
            WHERE host_id=? AND turn_id=?""",
            (json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")), row[0], row[1]),
        )
    return len(rows)


def run_retention_cycle(db_path: Path, *, policy: RetentionPolicy, now: str | None = None) -> dict[str, Any]:
    current = canonical_utc(now) if now is not None else utc_now()
    with write_transaction(db_path) as conn:
        submissions_settled = settle_due_submission_links_conn(
            conn, now=current, limit=policy.batch_size
        )
        agent_events = _events(conn, _cutoff(current, policy.event_retention_days), policy.batch_size)
        snapshots = _snapshots(conn, _cutoff(current, policy.snapshot_retention_days), policy.batch_size)
        deliveries = _deliveries(
            conn, _cutoff(current, policy.targetable_retention_days),
            policy.batch_size,
        )
        outbox = _outbox(
            conn, _cutoff(current, policy.targetable_retention_days),
            policy.batch_size,
        )
        command_cutoff = _cutoff(current, policy.command_retention_days)
        submissions = _submissions(conn, command_cutoff, policy.batch_size)
        pending_claims, pending_prompts = _pending_history(
            conn, command_cutoff, policy.batch_size
        )
        receipts = _receipts(conn, command_cutoff, policy.batch_size)
        content_cutoff = _cutoff(current, policy.route_content_retention_days)
        content = _content(conn, content_cutoff, policy.batch_size)
        content += _floor_content(conn, content_cutoff, policy.batch_size)
        turn_cutoff = min(
            _cutoff(current, policy.turn_change_retention_days),
            content_cutoff,
        )
        turns_deleted, turn_floors = _turns(
            conn,
            turn_cutoff,
            policy.turn_change_batch_size,
        )
        bindings = _bindings(conn, _cutoff(current, policy.route_content_retention_days), policy.batch_size)
        result = {
            "agent_events": agent_events,
            "snapshots": snapshots,
            "deliveries": deliveries,
            "outbox": outbox,
            "receipts": receipts,
            "submissions": submissions,
            "submissions_settled": submissions_settled,
            "pending_claims": pending_claims,
            "pending_prompts": pending_prompts,
            "content": content,
            "bindings": bindings,
            "turns": turns_deleted,
            "turn_floors": turn_floors,
        }
    conn = _connect(db_path, writable=True)
    try:
        checkpoint = conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
        result["checkpoint"] = {"busy": int(checkpoint[0]), "log": int(checkpoint[1]), "checkpointed": int(checkpoint[2])}
    finally:
        conn.close()
    return result


__all__ = ("RetentionPolicy", "run_retention_cycle")
