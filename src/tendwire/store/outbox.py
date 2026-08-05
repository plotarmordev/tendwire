"""One durable connector queue with FIFO, fenced leases, and plan lineage."""

from __future__ import annotations

import base64
import hashlib
import json
import re
import secrets
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from ..connectors.protocol import (
    valid_generic_payload, valid_part_spans, valid_turn_final_delivery,
)
from ..core.turns import TURN_CONTENT_PAGE_MAX_UTF8_BYTES, content_cursor
from .db import add_seconds, canonical_utc, read_transaction, utc_now, write_transaction

TURN_FINAL_CONNECTOR = "turn-final"

_PRIVATE_REASONS = frozenset(
    {
        "temporary",
        "rate_limited",
        "provider_rejected",
        "provider_uncertain",
        "invalid_payload",
        "content_unavailable",
        "route_unavailable",
        "provider_binding_unknown",
        "lease_expired",
        "ack_deadline_expired",
        "superseded",
        "attempts_exhausted",
        "operator_recovery",
    }
)
_REF_RE = re.compile(r"^twref1\.[A-Za-z0-9_-]{43}$")
_PLAN_RE = re.compile(r"^twplan1\.[A-Za-z0-9_-]{1,256}$")
_PUBLIC_RESPONSE_MAX_BYTES = 16 * 1024


class OutboxInvariantError(RuntimeError):
    pass


def _matches(pattern: re.Pattern[str], value: Any) -> bool:
    return isinstance(value, str) and pattern.fullmatch(value) is not None


def _hash_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _decision_identity(
    host_id: str,
    worker_id: str,
    revision_digest: str,
    route_generation: str,
) -> tuple[str, str]:
    decision_ref = "pending-" + _hash_text(_json([host_id, worker_id, revision_digest]))[:24]
    material = _json({
        "domain": "tendwire.decision.v1", "host_id": host_id,
        "decision_ref": decision_ref, "revision_digest": revision_digest,
        "route_generation": route_generation,
    }).encode("utf-8")
    digest = base64.urlsafe_b64encode(hashlib.sha256(material).digest())
    key = "turn-final:decision:twdecision1." + digest.rstrip(b"=").decode("ascii")
    return decision_ref, key


def _decision_retire_key(
    target_key: str,
    resolving_revision: str,
    route_generation: str,
) -> str:
    material = _json({
        "domain": "tendwire.decision-retire.v1", "target_key": target_key,
        "reason": "decision_resolved", "resolving_revision": resolving_revision,
        "route_generation": route_generation,
    }).encode("utf-8")
    digest = base64.urlsafe_b64encode(hashlib.sha256(material).digest())
    return "turn-final:retire:twretire1." + digest.rstrip(b"=").decode("ascii")


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _now(value: str | None = None) -> str:
    return canonical_utc(value) if value is not None else utc_now()


def _check_integer(
    value: Any,
    field: str,
    *,
    minimum: int,
    maximum: int,
) -> int:
    if not _valid_integer(value, minimum, maximum):
        raise ValueError(f"{field} is invalid")
    return value


def _valid_integer(value: Any, minimum: int, maximum: int) -> bool:
    return type(value) is int and minimum <= value <= maximum


def _success(host_id: str, name: str, status: str, **values: Any) -> dict[str, Any]:
    return {
        "schema_version": 1, "ok": True, "status": status,
        "host_id": host_id, "name": name, **values,
    }


def _error(host_id: str, name: str, status: str) -> dict[str, Any]:
    return {
        "schema_version": 1, "ok": False, "status": status,
        "host_id": host_id, "name": name, "message": status,
    }


def _lease_response(
    host_id: str,
    name: str,
    status: str,
    *,
    ref: str,
    row: Any,
    available_at: str | None = None,
    leased_until: str | None = None,
) -> dict[str, Any]:
    values: dict[str, Any] = {
        "ref": ref, "key": str(row["key"]), "attempt": int(row["attempt"]),
    }
    if available_at is not None:
        values["available_at"] = available_at
    if leased_until is not None:
        values["leased_until"] = leased_until
    return _success(host_id, name, status, **values)


def _public_response(value: Mapping[str, Any] | None) -> str | None:
    if value is None:
        return None
    encoded = _json(dict(value))
    if len(encoded.encode("utf-8")) > _PUBLIC_RESPONSE_MAX_BYTES:
        raise ValueError("response is too large")
    return encoded


def _live_delivery(
    conn: Any,
    host_id: str,
    name: str,
    ref: str,
    current: str,
    *,
    outbox_status: str = "leased",
    delivery_status: str = "leased",
) -> Any:
    if not _matches(_REF_RE, ref):
        return None
    return conn.execute(
        """SELECT o.*,d.id AS delivery_id,d.status AS delivery_status,
        d.leased_until,d.ack_deadline_at,d.attempt,d.retry_generation AS delivery_generation
        FROM connector_outbox o
        JOIN connector_deliveries d ON d.id=o.current_delivery_id
        WHERE o.host_id=? AND o.connector=? AND o.status=?
          AND d.outbox_id=o.id AND d.status=? AND d.ref_hash=?
          AND d.leased_until>?""",
        (
            host_id,
            name,
            outbox_status,
            delivery_status,
            _hash_text(ref),
            current,
        ),
    ).fetchone()


def _attempt_count(conn: Any, outbox_id: int, retry_generation: int) -> int:
    return int(
        conn.execute(
            """SELECT COUNT(*) FROM connector_deliveries
            WHERE outbox_id=? AND retry_generation=?""",
            (outbox_id, retry_generation),
        ).fetchone()[0]
    )


def _allocate_partition_sequence(conn: Any, host_id: str, partition_key: str) -> int:
    row = conn.execute(
        """UPDATE worker_bindings
        SET next_partition_sequence=next_partition_sequence+1
        WHERE host_id=? AND partition_key=?
        RETURNING next_partition_sequence-1""",
        (host_id, partition_key),
    ).fetchone()
    if row is None:
        raise OutboxInvariantError("route allocator unavailable")
    sequence = int(row[0])
    if sequence < 1:
        raise OutboxInvariantError("route allocator produced invalid sequence")
    return sequence


def _payload_row_correlates(row: Any, payload: Mapping[str, Any]) -> bool:
    kind = str(row["kind"])
    versions = {"working": 1, "final_ready": 3, "final_part": 2, "retire": 1, "decision": 1}
    route = payload["route"]
    return bool(
        kind in versions and row["payload_version"] == versions[kind]
        and payload["kind"] == kind and payload["created_at"] == row["created_at"]
        and route["partition_key"] == row["partition_key"]
        and route["partition_sequence"] == row["partition_sequence"]
    )


def _validate_generic(row: Any, payload: Mapping[str, Any]) -> bool:
    correlation_fields = [
        "partition_key",
        "partition_sequence",
        "turn_id",
        "final_identity",
        "decision_ref",
        "content_revision",
        "presentation_version",
        "plan_token",
        "plan_generation",
        "logical_sequence",
        "logical_ordinal",
        "predecessor_outbox_id",
        "replaces_outbox_id",
        "target_outbox_id",
        "source_outbox_id",
        "active_lineage_generation",
        "recovery_request_digest",
        "recovered_from_plan_token",
    ]
    return bool(
        row["connector"] != TURN_FINAL_CONNECTOR
        and row["kind"] == "generic"
        and row["payload_version"] == 1
        and all(row[field] is None for field in correlation_fields)
        and isinstance(payload, Mapping)
    )


def _final_content_matches_revision(conn: Any, row: Any, payload: Mapping[str, Any]) -> bool:
    revision = conn.execute(
        """SELECT user_text,assistant_final_text FROM turn_content_revisions
        WHERE host_id=? AND turn_id=? AND content_revision=?""",
        (row["host_id"], row["turn_id"], row["content_revision"]),
    ).fetchone()
    if revision is None:
        return False
    fields = payload["turn"]["content"]["fields"]
    for field in ("user_text", "assistant_final_text"):
        text = revision[field]
        descriptor = fields[field]
        if text is None:
            if descriptor["availability"] != "absent":
                return False
            continue
        char_length = len(text)
        byte_length = len(text.encode("utf-8"))
        if (
            descriptor["availability"] != "complete"
            or descriptor["char_length"] != char_length
            or descriptor["byte_length"] != byte_length
        ):
            return False
        if byte_length <= TURN_CONTENT_PAGE_MAX_UTF8_BYTES:
            if descriptor["inline"] != text or descriptor["page_count"] != 0 or descriptor["first_cursor"] is not None:
                return False
            continue
        page_count = int(
            conn.execute(
                """SELECT COUNT(*) FROM turn_content_page_boundaries
                WHERE revision_id=(SELECT id FROM turn_content_revisions
                  WHERE host_id=? AND turn_id=? AND content_revision=?) AND field=?""",
                (row["host_id"], row["turn_id"], row["content_revision"], field),
            ).fetchone()[0]
        )
        if (
            descriptor["inline"] is not None
            or descriptor["page_count"] != page_count
            or descriptor["first_cursor"] != content_cursor(str(row["content_revision"]), field, 0)
        ):
            return False
    return True


def _referenced_row(conn: Any, row_id: Any) -> Any:
    if row_id is None:
        return None
    return conn.execute(
        "SELECT * FROM connector_outbox WHERE id=?",
        (row_id,),
    ).fetchone()


def _current_revision_exists(conn: Any, host_id: str, turn_id: Any, revision: Any) -> bool:
    return conn.execute(
        """SELECT 1 FROM turn_content_revisions
        WHERE host_id=? AND turn_id=? AND content_revision=? AND is_current=1""",
        (host_id, turn_id, revision),
    ).fetchone() is not None


def _validate_persisted_correlations(
    conn: Any,
    row: Any,
    payload: Mapping[str, Any],
) -> bool:
    worker = payload["worker"]
    binding = conn.execute(
        """SELECT 1 FROM worker_bindings
        WHERE host_id=? AND worker_id=? AND partition_key=? AND stable_key_version=1
          AND stable_key=? AND route_generation=?""",
        (
            row["host_id"],
            worker["worker_id"],
            row["partition_key"],
            worker["stable_key"],
            worker["route_generation"],
        ),
    ).fetchone()
    if binding is None:
        return False
    if payload["created_at"] != row["created_at"]:
        return False
    if row["kind"] != "decision":
        turn = payload["turn"]
        if turn["turn_id"] != row["turn_id"] or turn["content_revision"] != row["content_revision"]:
            return False
        if row["kind"] != "working" and turn["final_identity"] != row["final_identity"]:
            return False
    predecessor = _referenced_row(conn, row["predecessor_outbox_id"])
    replacement = _referenced_row(conn, row["replaces_outbox_id"])
    target = _referenced_row(conn, row["target_outbox_id"])
    source = _referenced_row(conn, row["source_outbox_id"])
    references = (predecessor, replacement, target, source)
    if any(
        ref is not None
        and (ref["host_id"] != row["host_id"] or ref["connector"] != row["connector"])
        for ref in references
    ):
        return False
    if row["kind"] == "final_ready":
        if not _final_content_matches_revision(conn, row, payload):
            return False
    elif row["kind"] == "final_part":
        plan = payload["plan"]
        if (
            plan["plan_token"] != row["plan_token"]
            or plan["generation"] != row["plan_generation"]
            or plan["presentation_version"] != row["presentation_version"]
            or plan["ordinal"] != row["logical_ordinal"]
        ):
            return False
        if source is None or source["kind"] != "final_ready":
            return False
        if any(
            source[field] != row[field]
            for field in ("turn_id", "final_identity", "content_revision", "partition_key")
        ):
            return False
        if row["key"] != f"turn-final:{row['plan_token']}:{int(row['logical_sequence']):06d}":
            return False
        lineage = payload["lineage"]
        if lineage["predecessor_key"] != (predecessor["key"] if predecessor else None):
            return False
        if lineage["replaces_key"] != (replacement["key"] if replacement else None):
            return False
    elif row["kind"] == "retire":
        retire = payload["retire"]
        if target is None or target["key"] != retire["target_key"]:
            return False
        if predecessor is None or predecessor["key"] != retire["predecessor_key"]:
            return False
        if row["source_outbox_id"] is None:
            if target["kind"] != "decision" or predecessor["id"] != target["id"]:
                return False
            try:
                target_payload = json.loads(str(target["payload_json"]))
            except (TypeError, json.JSONDecodeError):
                return False
            if not isinstance(target_payload, Mapping):
                return False
            if target_payload.get("worker") != worker:
                return False
            if target["partition_key"] != row["partition_key"]:
                return False
            resolving_revision = row["decision_ref"]
            if not isinstance(resolving_revision, str) or not resolving_revision:
                return False
            expected_key = _decision_retire_key(
                str(target["key"]),
                resolving_revision,
                str(worker["route_generation"]),
            )
            if row["key"] != expected_key:
                return False
        else:
            if source is None or source["kind"] != "final_ready":
                return False
            if retire["plan_token"] != row["plan_token"] or retire["generation"] != row["plan_generation"]:
                return False
            expected_kind = "final_part" if target["kind"] == "final_part" else target["kind"]
            if expected_kind != retire["target_kind"]:
                return False
    elif row["kind"] == "decision":
        decision = payload["decision"]
        decision_ref, key = _decision_identity(
            str(row["host_id"]),
            str(worker["worker_id"]),
            str(decision["revision_digest"]),
            str(worker["route_generation"]),
        )
        if row["decision_ref"] != decision_ref or row["key"] != key:
            return False
    return True


def _validate_polled_payload(conn: Any, row: Any) -> dict[str, Any]:
    try:
        payload = json.loads(str(row["payload_json"]))
        if not isinstance(payload, Mapping) or _json(payload) != row["payload_json"]:
            raise ValueError("noncanonical payload")
    except (TypeError, ValueError, UnicodeError, OverflowError, RecursionError) as exc:
        raise OutboxInvariantError("outbox payload is not canonical") from exc
    if row["kind"] == "generic":
        if not _validate_generic(row, payload) or not valid_generic_payload(payload):
            raise OutboxInvariantError("generic outbox payload correlation failed")
        return dict(payload)
    if (
        not valid_turn_final_delivery(payload, row["key"], row["host_id"])
        or not _payload_row_correlates(row, payload)
        or not _validate_persisted_correlations(conn, row, payload)
    ):
        raise OutboxInvariantError("outbox payload correlation failed")
    return dict(payload)


def _expire_leases(conn: Any, host_id: str, name: str | None, current: str) -> int:
    clauses = ["o.host_id=?", "o.status='leased'", "d.status='leased'", "d.leased_until<=?"]
    values: list[Any] = [host_id, current]
    if name is not None:
        clauses.append("o.connector=?")
        values.append(name)
    rows = conn.execute(
        f"""SELECT o.id,o.terminal_after_lease,d.id AS delivery_id
        FROM connector_outbox o
        JOIN connector_deliveries d ON d.id=o.current_delivery_id
        WHERE {' AND '.join(clauses)}""",
        values,
    ).fetchall()
    for row in rows:
        conn.execute(
            """UPDATE connector_deliveries
            SET status='expired',private_reason_enum='lease_expired',settled_at=?
            WHERE id=? AND status='leased'""",
            (current, row["delivery_id"]),
        )
        status = "superseded" if bool(row["terminal_after_lease"]) else "queued"
        conn.execute(
            """UPDATE connector_outbox
            SET status=?,current_delivery_id=NULL,available_at=?,updated_at=?
            WHERE id=? AND status='leased'""",
            (status, current, current, row["id"]),
        )
    return len(rows)


def _expire_ack_roots(conn: Any, host_id: str, name: str | None, current: str) -> int:
    clauses = [
        "o.host_id=?",
        "o.status='awaiting_ack'",
        "d.status='awaiting_ack'",
        "d.ack_deadline_at<=?",
    ]
    values: list[Any] = [host_id, current]
    if name is not None:
        clauses.append("o.connector=?")
        values.append(name)
    rows = conn.execute(
        f"""SELECT o.id,d.id AS delivery_id,o.active_lineage_generation
        FROM connector_outbox o
        JOIN connector_deliveries d ON d.id=o.current_delivery_id
        WHERE {' AND '.join(clauses)}
          AND NOT EXISTS(
            SELECT 1 FROM connector_outbox child
            JOIN connector_deliveries live ON live.id=child.current_delivery_id
            WHERE child.source_outbox_id=o.id
              AND child.active_lineage_generation=o.active_lineage_generation
              AND live.status='leased'
          )""",
        values,
    ).fetchall()
    for row in rows:
        conn.execute(
            """UPDATE connector_deliveries
            SET status='failed',ack_deadline_at=NULL,
                private_reason_enum='ack_deadline_expired',settled_at=?
            WHERE id=? AND status='awaiting_ack'""",
            (current, row["delivery_id"]),
        )
        conn.execute(
            """UPDATE connector_outbox
            SET status='dead_letter',current_delivery_id=NULL,updated_at=?
            WHERE id=? AND status='awaiting_ack'""",
            (current, row["id"]),
        )
        conn.execute(
            """UPDATE connector_outbox
            SET status='dead_letter',current_delivery_id=NULL,updated_at=?
            WHERE source_outbox_id=? AND active_lineage_generation=?
              AND status NOT IN('delivered','superseded','dead_letter')""",
            (current, row["id"], row["active_lineage_generation"]),
        )
    return len(rows)


def reclaim_expired_connector_leases(
    db_path: Path | str,
    host_id: str,
    name: str | None,
    *,
    now: str | None = None,
) -> dict[str, Any]:
    current = _now(now)
    with write_transaction(db_path) as conn:
        leases = _expire_leases(conn, host_id, name, current)
        roots = _expire_ack_roots(conn, host_id, name, current)
    return _success(host_id, name or "", "ok", reclaimed=leases + roots)


def connector_reclaim_due(
    db_path: Path | str,
    host_id: str,
    name: str | None,
    *,
    now: str | None = None,
) -> bool:
    current = _now(now)
    try:
        with read_transaction(db_path) as conn:
            values: list[Any] = [host_id, current, current]
            connector = ""
            if name is not None:
                connector = " AND o.connector=?"
                values.append(name)
            row = conn.execute(
                f"""SELECT 1 FROM connector_outbox o
                JOIN connector_deliveries d ON d.id=o.current_delivery_id
                WHERE o.host_id=?
                  AND ((d.status='leased' AND d.leased_until<=?)
                    OR (d.status='awaiting_ack' AND d.ack_deadline_at<=?))
                  {connector} LIMIT 1""",
                values,
            ).fetchone()
        return row is not None
    except Exception:
        return False


def _activate_decision_retires(conn: Any, host_id: str, name: str, current: str) -> None:
    conn.execute(
        """UPDATE connector_outbox AS retire SET status='queued',updated_at=?
        WHERE retire.host_id=? AND retire.connector=? AND retire.kind='retire'
          AND retire.status='blocked' AND retire.source_outbox_id IS NULL
          AND EXISTS(
            SELECT 1 FROM connector_outbox target
            WHERE target.id=retire.target_outbox_id AND target.kind='decision'
              AND target.status IN('delivered','superseded','dead_letter')
              AND target.current_delivery_id IS NULL
              AND EXISTS(SELECT 1 FROM connector_deliveries d WHERE d.outbox_id=target.id)
          )""",
        (current, host_id, name),
    )


def _dead_letter_poll_failure(conn: Any, row: Any, current: str) -> None:
    """Terminalize one unleaseable row and its effective non-delivered suffix."""
    conn.execute(
        """UPDATE connector_outbox SET status='dead_letter',updated_at=?
        WHERE id=? AND status IN('queued','retry','deferred')""",
        (current, row["id"]),
    )
    if row["source_outbox_id"] is None:
        return
    conn.execute(
        """UPDATE connector_outbox
        SET status='dead_letter',current_delivery_id=NULL,updated_at=?
        WHERE source_outbox_id=? AND active_lineage_generation=?
          AND logical_sequence>=?
          AND status IN('staged','blocked','queued','retry','deferred')""",
        (
            current,
            row["source_outbox_id"],
            row["active_lineage_generation"],
            row["logical_sequence"],
        ),
    )


def poll_connector_outbox(
    db_path: Path | str,
    host_id: str,
    name: str,
    *,
    limit: int = 1,
    lease_seconds: int = 60,
    max_attempts: int = 10,
    now: str | None = None,
) -> dict[str, Any]:
    _check_integer(limit, "limit", minimum=1, maximum=100)
    _check_integer(lease_seconds, "lease_seconds", minimum=1, maximum=86_400)
    _check_integer(max_attempts, "max_attempts", minimum=1, maximum=1_000_000)
    current = _now(now)
    leased_until = add_seconds(current, lease_seconds)
    items: list[dict[str, Any]] = []
    with write_transaction(db_path) as conn:
        _expire_leases(conn, host_id, name, current)
        _expire_ack_roots(conn, host_id, name, current)
        _activate_decision_retires(conn, host_id, name, current)
        rows = conn.execute(
            """SELECT o.* FROM connector_outbox o
            WHERE o.host_id=? AND o.connector=?
              AND o.status IN('queued','retry','deferred') AND o.available_at<=?
              AND (
                o.predecessor_outbox_id IS NULL
                OR EXISTS(
                  SELECT 1 FROM connector_outbox predecessor
                  WHERE predecessor.id=o.predecessor_outbox_id
                    AND predecessor.host_id=o.host_id
                    AND predecessor.connector=o.connector
                    AND predecessor.status='delivered'
                )
                OR (
                  o.kind='retire' AND o.source_outbox_id IS NULL
                  AND EXISTS(
                    SELECT 1 FROM connector_outbox target
                    WHERE target.id=o.target_outbox_id
                      AND target.status IN('delivered','superseded','dead_letter')
                      AND target.current_delivery_id IS NULL
                      AND EXISTS(SELECT 1 FROM connector_deliveries d WHERE d.outbox_id=target.id)
                  )
                )
              )
              AND NOT EXISTS(
                SELECT 1 FROM connector_outbox earlier
                WHERE earlier.host_id=o.host_id AND earlier.connector=o.connector
                  AND earlier.partition_key=o.partition_key
                  AND earlier.partition_sequence<o.partition_sequence
                  AND earlier.status IN('blocked','queued','leased','retry','deferred')
              )
            ORDER BY o.available_at,o.partition_key,o.partition_sequence,o.id
            LIMIT ?""",
            (host_id, name, current, limit),
        ).fetchall()
        for row in rows:
            attempt = _attempt_count(conn, int(row["id"]), int(row["retry_generation"])) + 1
            if attempt > max_attempts:
                _dead_letter_poll_failure(conn, row, current)
                continue
            try:
                payload = _validate_polled_payload(conn, row)
            except OutboxInvariantError:
                _dead_letter_poll_failure(conn, row, current)
                continue
            ref = "twref1." + secrets.token_urlsafe(32)
            item = {
                "key": row["key"],
                "ref": ref,
                "attempt": attempt,
                "leased_until": leased_until,
                "available_at": row["available_at"],
                "created_at": row["created_at"],
                "payload": payload,
            }
            prospective = {"ok": True, "status": "ok", "items": [*items, item]}
            if len(_json(prospective).encode("utf-8")) > 850_000:
                if items:
                    break
                _dead_letter_poll_failure(conn, row, current)
                continue
            delivery = conn.execute(
                """INSERT INTO connector_deliveries(
                outbox_id,retry_generation,attempt,ref_hash,status,
                leased_at,leased_until,created_at)
                VALUES(?,?,?,?,'leased',?,?,?)""",
                (
                    row["id"],
                    row["retry_generation"],
                    attempt,
                    _hash_text(ref),
                    current,
                    leased_until,
                    current,
                ),
            )
            changed = conn.execute(
                """UPDATE connector_outbox
                SET status='leased',current_delivery_id=?,updated_at=?
                WHERE id=? AND status IN('queued','retry','deferred')
                  AND current_delivery_id IS NULL""",
                (delivery.lastrowid, current, row["id"]),
            ).rowcount
            if changed != 1:
                raise OutboxInvariantError("poll lease CAS failed")
            items.append(item)
    return {"ok": True, "status": "ok", "items": items}


def renew_connector_delivery(
    db_path: Path | str,
    *,
    host_id: str,
    name: str,
    ref: str,
    lease_seconds: int,
    now: str | None = None,
) -> dict[str, Any]:
    _check_integer(lease_seconds, "lease_seconds", minimum=1, maximum=86_400)
    current = _now(now)
    leased_until = add_seconds(current, lease_seconds)
    with write_transaction(db_path) as conn:
        row = _live_delivery(conn, host_id, name, ref, current)
        if row is None:
            return _error(host_id, name, "stale_ref")
        changed = conn.execute(
            """UPDATE connector_deliveries SET leased_until=?
            WHERE id=? AND outbox_id=? AND status='leased'
              AND ref_hash=? AND leased_until>?""",
            (
                leased_until,
                row["delivery_id"],
                row["id"],
                _hash_text(ref),
                current,
            ),
        ).rowcount
        if changed != 1:
            return _error(host_id, name, "stale_ref")
    return _lease_response(
        host_id,
        name,
        "renewed",
        ref=ref,
        row=row,
        leased_until=leased_until,
    )


def _unblock_successor(conn: Any, row: Any, current: str) -> None:
    successor = conn.execute(
        """SELECT id FROM connector_outbox
        WHERE predecessor_outbox_id=? AND status='blocked'
        ORDER BY logical_sequence,id LIMIT 2""",
        (row["id"],),
    ).fetchall()
    if len(successor) > 1:
        raise OutboxInvariantError("lineage branches are forbidden")
    if successor:
        conn.execute(
            """UPDATE connector_outbox SET status='queued',updated_at=?
            WHERE id=? AND status='blocked'""",
            (current, successor[0][0]),
        )


def _complete_source_root(conn: Any, source_id: int, current: str) -> None:
    root = conn.execute(
        """SELECT o.id,o.active_lineage_generation,o.current_delivery_id,
        d.ack_deadline_at,d.status AS delivery_status
        FROM connector_outbox o
        JOIN connector_deliveries d ON d.id=o.current_delivery_id
        WHERE o.id=? AND o.status='awaiting_ack' AND d.status='awaiting_ack'""",
        (source_id,),
    ).fetchone()
    if root is None:
        return
    pending = conn.execute(
        """SELECT 1 FROM connector_outbox
        WHERE source_outbox_id=? AND active_lineage_generation=?
          AND status<>'delivered' LIMIT 1""",
        (source_id, root["active_lineage_generation"]),
    ).fetchone()
    if pending is not None:
        return
    deadline = root["ack_deadline_at"]
    if not isinstance(deadline, str) or deadline <= current:
        return
    changed = conn.execute(
        """UPDATE connector_deliveries
        SET status='acknowledged',ack_deadline_at=NULL,settled_at=?
        WHERE id=? AND outbox_id=? AND status='awaiting_ack'
          AND ack_deadline_at>?""",
        (current, root["current_delivery_id"], source_id, current),
    ).rowcount
    if changed != 1:
        return
    conn.execute(
        """UPDATE connector_outbox
        SET status='delivered',current_delivery_id=NULL,updated_at=?
        WHERE id=? AND status='awaiting_ack'""",
        (current, source_id),
    )


def _validate_failure_reason(kind: str, reason: str | None) -> bool:
    if reason not in _PRIVATE_REASONS:
        return False
    if reason == "provider_uncertain" and kind not in {"working", "final_part", "decision"}:
        return False
    if reason == "provider_binding_unknown" and kind != "retire":
        return False
    return True


def _settle(
    db_path: Path | str,
    host_id: str,
    name: str,
    ref: str,
    action: str,
    *,
    response: Mapping[str, Any] | None = None,
    reason: str | None = None,
    delay_seconds: int | None = None,
    available_at: str | None = None,
    max_attempts: int = 10,
    now: str | None = None,
) -> dict[str, Any]:
    current = _now(now)
    if action in {"fail", "defer"} and not _validate_failure_reason("retire" if reason == "provider_binding_unknown" else "working", reason):
        if reason not in _PRIVATE_REASONS:
            return _error(host_id, name, "invalid_params")
    if delay_seconds is not None and not _valid_integer(delay_seconds, 0, 31_536_000):
        return _error(host_id, name, "invalid_params")
    if available_at is not None:
        try:
            available_at = canonical_utc(available_at)
        except (TypeError, ValueError):
            return _error(host_id, name, "invalid_params")
    if available_at is not None and delay_seconds is not None:
        return _error(host_id, name, "invalid_params")
    try:
        public_response = _public_response(response)
    except (TypeError, ValueError):
        return _error(host_id, name, "invalid_params")
    with write_transaction(db_path) as conn:
        row = _live_delivery(conn, host_id, name, ref, current)
        if row is None:
            return _error(host_id, name, "stale_ref")
        if action in {"fail", "defer"} and not _validate_failure_reason(str(row["kind"]), reason):
            return _error(host_id, name, "invalid_params")
        if action == "ack" and row["kind"] == "final_ready":
            return _error(host_id, name, "invalid_params")
        delivery_status = {
            "ack": "acknowledged",
            "fail": "failed",
            "defer": "deferred",
            "release": "released",
        }[action]
        changed = conn.execute(
            """UPDATE connector_deliveries
            SET status=?,public_response_json=?,private_reason_enum=?,settled_at=?
            WHERE id=? AND outbox_id=? AND status='leased'
              AND ref_hash=? AND leased_until>?""",
            (
                delivery_status,
                public_response,
                reason,
                current,
                row["delivery_id"],
                row["id"],
                _hash_text(ref),
                current,
            ),
        ).rowcount
        if changed != 1:
            return _error(host_id, name, "stale_ref")
        due = available_at or add_seconds(current, delay_seconds or 0)
        if action == "ack":
            next_status = "delivered"
            public_status = "acknowledged"
        elif action == "release":
            next_status = "superseded" if bool(row["terminal_after_lease"]) else "queued"
            public_status = "superseded" if next_status == "superseded" else "released"
        elif action == "defer":
            next_status = "superseded" if bool(row["terminal_after_lease"]) else "deferred"
            public_status = "superseded" if next_status == "superseded" else "deferred"
        else:
            immediate = reason in {"provider_uncertain", "provider_binding_unknown"}
            exhausted = int(row["attempt"]) >= max_attempts
            if bool(row["terminal_after_lease"]):
                next_status = "superseded"
                public_status = "superseded"
            elif immediate or exhausted:
                next_status = "dead_letter"
                public_status = "attempts_exhausted"
            else:
                next_status = "retry"
                public_status = "retry_scheduled"
        changed = conn.execute(
            """UPDATE connector_outbox
            SET status=?,current_delivery_id=NULL,updated_at=?,available_at=?
            WHERE id=? AND status='leased' AND current_delivery_id=?""",
            (next_status, current, due, row["id"], row["delivery_id"]),
        ).rowcount
        if changed != 1:
            raise OutboxInvariantError("settlement outbox CAS failed")
        if action == "ack":
            _unblock_successor(conn, row, current)
            if row["source_outbox_id"] is not None:
                _complete_source_root(conn, int(row["source_outbox_id"]), current)
        if action == "fail" and row["source_outbox_id"] is not None and next_status == "dead_letter":
            conn.execute(
                """UPDATE connector_outbox
                SET status='dead_letter',current_delivery_id=NULL,updated_at=?
                WHERE source_outbox_id=? AND active_lineage_generation=?
                  AND logical_sequence>=? AND status NOT IN('delivered','dead_letter')
                  AND current_delivery_id IS NULL""",
                (
                    current,
                    row["source_outbox_id"],
                    row["active_lineage_generation"],
                    row["logical_sequence"],
                ),
            )
    response_due = due if public_status in {"retry_scheduled", "deferred"} else None
    return _lease_response(
        host_id,
        name,
        public_status,
        ref=ref,
        row=row,
        available_at=response_due,
    )


def ack_connector_delivery(
    db_path: Path | str,
    *,
    host_id: str, name: str, ref: str,
    response: Mapping[str, Any] | None = None,
    **kwargs: Any,
) -> dict[str, Any]:
    return _settle(db_path, host_id, name, ref, "ack", response=response, **kwargs)


def fail_connector_delivery(
    db_path: Path | str,
    *,
    host_id: str, name: str, ref: str,
    reason: str | None = None,
    delay_seconds: int | None = None,
    max_attempts: int = 10,
    **kwargs: Any,
) -> dict[str, Any]:
    return _settle(db_path, host_id, name, ref, "fail", reason=reason,
                   delay_seconds=delay_seconds, max_attempts=max_attempts, **kwargs)


def defer_connector_delivery(
    db_path: Path | str,
    *,
    host_id: str, name: str, ref: str,
    reason: str | None = None,
    delay_seconds: int | None = None,
    **kwargs: Any,
) -> dict[str, Any]:
    return _settle(db_path, host_id, name, ref, "defer", reason=reason,
                   delay_seconds=delay_seconds, **kwargs)


def release_connector_delivery(
    db_path: Path | str,
    *,
    host_id: str, name: str, ref: str,
    **kwargs: Any,
) -> dict[str, Any]:
    return _settle(db_path, host_id, name, ref, "release", **kwargs)


def _plan_rows(conn: Any, host_id: str, name: str, token: str) -> list[Any]:
    return conn.execute(
        """SELECT * FROM connector_outbox
        WHERE host_id=? AND connector=? AND plan_token=? AND kind='final_part'
        ORDER BY logical_ordinal""",
        (host_id, name, token),
    ).fetchall()


def _accepted_ordinals(rows: Sequence[Any]) -> list[int]:
    return [
        int(row["logical_ordinal"])
        for row in rows
        if str(row["payload_json"]) != "{}"
    ]


def _plan_state(rows: Sequence[Any]) -> str:
    statuses = {str(row["status"]) for row in rows}
    if statuses == {"staged"}:
        return "preparing"
    if statuses <= {"queued", "leased", "retry", "deferred", "blocked"}:
        return "active" if "queued" in statuses or "leased" in statuses else "waiting_predecessor"
    if statuses == {"delivered"}:
        return "completed"
    if "dead_letter" in statuses:
        return "failed"
    if statuses <= {"superseded", "delivered"}:
        return "superseded"
    raise OutboxInvariantError("plan has an invalid mixed state")


def _plan_success(
    host_id: str, name: str, token: str, state: str, generation: int,
    part_count: int, **values: Any,
) -> dict[str, Any]:
    return _success(host_id, name, "ok", plan_token=token, state=state,
                    generation=generation, part_count=part_count, **values)


def prepare_connector_plan_begin(
    db_path: Path | str,
    host_id: str,
    *,
    name: str,
    turn_id: str,
    content_revision: str,
    presentation_version: str,
    part_count: int,
    source_ref: str | None,
    **_: Any,
) -> dict[str, Any]:
    if not _matches(_REF_RE, source_ref):
        return _error(host_id, name, "invalid_ref")
    if not _valid_integer(part_count, 1, 10_000):
        return _error(host_id, name, "invalid_params")
    current = _now()
    with write_transaction(db_path) as conn:
        root = _live_delivery(conn, host_id, name, source_ref, current)
        if root is None:
            return _error(host_id, name, "stale_ref")
        if root["kind"] != "final_ready":
            return _error(host_id, name, "invalid_params")
        if root["turn_id"] != turn_id or root["content_revision"] != content_revision:
            return _error(host_id, name, "stale_revision")
        if not _current_revision_exists(conn, host_id, turn_id, content_revision):
            return _error(host_id, name, "stale_revision")
        existing = conn.execute(
            """SELECT DISTINCT plan_token,plan_generation,presentation_version
            FROM connector_outbox
            WHERE source_outbox_id=? AND kind='final_part' AND plan_generation=1""",
            (root["id"],),
        ).fetchall()
        if existing:
            if len(existing) != 1:
                raise OutboxInvariantError("source has ambiguous generation-one plans")
            token = str(existing[0]["plan_token"])
            generation = int(existing[0]["plan_generation"])
            rows = _plan_rows(conn, host_id, name, token)
            if len(rows) != part_count or existing[0]["presentation_version"] != presentation_version:
                return _error(host_id, name, "plan_conflict")
        else:
            token = "twplan1." + secrets.token_urlsafe(32)
            generation = 1
            created_at = current
            for ordinal in range(part_count):
                partition_sequence = _allocate_partition_sequence(
                    conn,
                    host_id,
                    str(root["partition_key"]),
                )
                key = f"turn-final:{token}:{ordinal + 1:06d}"
                conn.execute(
                    """INSERT INTO connector_outbox(
                    host_id,connector,key,kind,payload_version,status,
                    partition_key,partition_sequence,turn_id,final_identity,
                    content_revision,presentation_version,plan_token,plan_generation,
                    logical_sequence,logical_ordinal,source_outbox_id,
                    active_lineage_generation,retry_generation,prior_attempt_count,
                    payload_json,created_at,updated_at,available_at)
                    VALUES(?,?,?,?,2,'staged',?,?,?,?,?,?,?,?,?,?,?,?,1,0,'{}',?,?,?)""",
                    (
                        host_id,
                        name,
                        key,
                        "final_part",
                        root["partition_key"],
                        partition_sequence,
                        turn_id,
                        root["final_identity"],
                        content_revision,
                        presentation_version,
                        token,
                        generation,
                        ordinal + 1,
                        ordinal,
                        root["id"],
                        generation,
                        created_at,
                        created_at,
                        created_at,
                    ),
                )
            rows = _plan_rows(conn, host_id, name, token)
        accepted = _accepted_ordinals(rows)
        state = _plan_state(rows)
    return _plan_success(host_id, name, token, state, generation, len(rows),
                         accepted_ordinals=accepted)


def _span_content_lengths(conn: Any, row: Any) -> dict[str, int] | None:
    revision = conn.execute(
        """SELECT user_text,assistant_final_text FROM turn_content_revisions
        WHERE host_id=? AND turn_id=? AND content_revision=?""",
        (row["host_id"], row["turn_id"], row["content_revision"]),
    ).fetchone()
    if revision is None:
        return None
    return {
        "user_text": len(str(revision[0] or "")),
        "assistant_final_text": len(str(revision[1] or "")),
    }


def _validated_part_spans(conn: Any, row: Any, spans: Any) -> list[dict[str, Any]] | None:
    if not valid_part_spans(spans):
        return None
    lengths = _span_content_lengths(conn, row)
    if lengths is None:
        return None
    normalized: list[dict[str, Any]] = []
    last_field = ""
    last_end = -1
    for raw in spans:
        field = str(raw["field"])
        start = int(raw["start_char"])
        end = int(raw["end_char"])
        if end > lengths[field]:
            return None
        if field == last_field and start < last_end:
            return None
        last_field = field
        last_end = end
        normalized.append({"field": field, "start_char": start, "end_char": end})
    return normalized


def prepare_connector_plan_part(
    db_path: Path | str,
    host_id: str,
    *,
    name: str,
    plan_token: str,
    ordinal: int,
    spans: list[Mapping[str, Any]],
    **_: Any,
) -> dict[str, Any]:
    if not _matches(_PLAN_RE, plan_token) or not _valid_integer(ordinal, 0, 9_999):
        return _error(host_id, name, "invalid_params")
    current = _now()
    with write_transaction(db_path) as conn:
        row = conn.execute(
            """SELECT * FROM connector_outbox
            WHERE host_id=? AND connector=? AND plan_token=?
              AND kind='final_part' AND logical_ordinal=?""",
            (host_id, name, plan_token, ordinal),
        ).fetchone()
        if row is None:
            return _error(host_id, name, "plan_not_found")
        root = conn.execute(
            """SELECT o.*,d.status AS delivery_status,d.leased_until
            FROM connector_outbox o
            LEFT JOIN connector_deliveries d ON d.id=o.current_delivery_id
            WHERE o.id=?""",
            (row["source_outbox_id"],),
        ).fetchone()
        if root is None:
            return _error(host_id, name, "plan_not_found")
        if root["status"] == "leased" and (
            root["delivery_status"] != "leased" or root["leased_until"] <= current
        ):
            return _error(host_id, name, "stale_ref")
        if root["status"] not in {"leased", "awaiting_ack", "delivered"}:
            return _error(host_id, name, "stale_ref")
        normalized = _validated_part_spans(conn, row, spans)
        if normalized is None:
            return _error(host_id, name, "invalid_params")
        root_payload = _validate_polled_payload(conn, root)
        rows = _plan_rows(conn, host_id, name, plan_token)
        payload = {
            "schema_version": 2, "kind": "final_part", "created_at": row["created_at"],
            "worker": root_payload["worker"],
            "route": {
                "partition_key": row["partition_key"],
                "partition_sequence": row["partition_sequence"],
            },
            "turn": {
                "turn_id": row["turn_id"], "final_identity": row["final_identity"],
                "content_revision": row["content_revision"],
            },
            "plan": {
                "plan_token": plan_token, "generation": row["plan_generation"],
                "presentation_version": row["presentation_version"], "ordinal": ordinal,
                "part_count": len(rows), "spans": normalized,
            },
            "lineage": {
                "recovered_from_plan_token": row["recovered_from_plan_token"],
                "predecessor_key": None, "replaces_key": None,
            },
        }
        encoded = _json(payload)
        if row["payload_json"] not in {"{}", encoded}:
            return _error(host_id, name, "part_conflict")
        if row["status"] != "staged" and row["payload_json"] != encoded:
            return _error(host_id, name, "part_conflict")
        conn.execute(
            """UPDATE connector_outbox SET payload_json=?,updated_at=?
            WHERE id=? AND payload_json IN('{}',?)""",
            (encoded, current, row["id"], encoded),
        )
        rows = _plan_rows(conn, host_id, name, plan_token)
        accepted = _accepted_ordinals(rows)
        state = _plan_state(rows)
    return _plan_success(
        host_id, name, plan_token, state, int(row["plan_generation"]), len(rows),
        ordinal=ordinal, accepted_ordinals=accepted,
    )


def _exact_plan_coverage(conn: Any, rows: Sequence[Any]) -> bool:
    if not rows or [int(row["logical_ordinal"]) for row in rows] != list(range(len(rows))):
        return False
    lengths = _span_content_lengths(conn, rows[0])
    if lengths is None:
        return False
    coordinates: dict[str, list[tuple[int, int]]] = {
        "user_text": [],
        "assistant_final_text": [],
    }
    for row in rows:
        try:
            payload = json.loads(row["payload_json"])
        except (TypeError, ValueError, json.JSONDecodeError):
            return False
        if not valid_turn_final_delivery(payload, row["key"], row["host_id"]):
            return False
        plan, turn = payload["plan"], payload["turn"]
        if (
            (turn["turn_id"], turn["final_identity"], turn["content_revision"])
            != (row["turn_id"], row["final_identity"], row["content_revision"])
            or (plan["plan_token"], plan["generation"], plan["presentation_version"], plan["ordinal"])
            != (row["plan_token"], row["plan_generation"], row["presentation_version"], row["logical_ordinal"])
        ):
            return False
        for span in payload["plan"]["spans"]:
            coordinates[span["field"]].append((span["start_char"], span["end_char"]))
    for field, expected_length in lengths.items():
        expected_start = 0
        for start, end in coordinates[field]:
            if start != expected_start:
                return False
            expected_start = end
        if expected_start != expected_length:
            return False
    return True


def _retire_payload(
    root_payload: Mapping[str, Any],
    *,
    row: Any,
    target: Any,
    predecessor_key: str,
    reason: str,
) -> dict[str, Any]:
    target_kind = "final_part" if target["kind"] == "final_ready" else target["kind"]
    return {
        "schema_version": 1, "kind": "retire", "created_at": row["created_at"],
        "worker": root_payload["worker"],
        "route": {
            "partition_key": row["partition_key"],
            "partition_sequence": row["partition_sequence"],
        },
        "turn": {
            "turn_id": row["turn_id"], "final_identity": row["final_identity"],
            "content_revision": row["content_revision"],
        },
        "retire": {
            "target_key": target["key"], "target_kind": target_kind,
            "target_ordinal": target["logical_ordinal"], "predecessor_key": predecessor_key,
            "plan_token": row["plan_token"], "generation": row["plan_generation"],
            "reason": reason,
        },
    }


def _effective_delivered_parts(conn: Any, source_id: int) -> dict[int, Any]:
    rows = conn.execute(
        """SELECT * FROM connector_outbox
        WHERE source_outbox_id=? AND kind='final_part' AND status='delivered'
        ORDER BY logical_ordinal,plan_generation DESC,id DESC""",
        (source_id,),
    ).fetchall()
    effective: dict[int, Any] = {}
    for row in rows:
        ordinal = int(row["logical_ordinal"])
        effective.setdefault(ordinal, row)
    return effective


def _replacement_targets(
    conn: Any, root: Any
) -> tuple[Any | None, dict[int, Any], list[Any]]:
    previous = _referenced_row(conn, root["replaces_outbox_id"])
    if previous is None:
        return None, {}, []
    if previous["kind"] == "working":
        if previous["partition_key"] == root["partition_key"]:
            return previous, {}, []
        attempted = conn.execute(
            "SELECT 1 FROM connector_deliveries WHERE outbox_id=? LIMIT 1",
            (previous["id"],),
        ).fetchone()
        return None, {}, [previous] if attempted is not None else []
    if previous["kind"] != "final_ready":
        raise OutboxInvariantError("final root replacement target is invalid")
    old_parts = _effective_delivered_parts(conn, int(previous["id"]))
    if previous["partition_key"] != root["partition_key"]:
        return None, {}, [old_parts[index] for index in sorted(old_parts)]
    return None, old_parts, []


def _append_replacement_retires(
    conn: Any,
    root: Any,
    rows: list[Any],
    root_payload: Mapping[str, Any],
    current: str,
    targets: list[Any],
) -> list[Any]:
    if not targets:
        return rows
    lineage = list(rows)
    tail = lineage[-1]
    for target in targets:
        partition_sequence = _allocate_partition_sequence(
            conn,
            str(root["host_id"]),
            str(root["partition_key"]),
        )
        logical_sequence = int(tail["logical_sequence"]) + 1
        key = f"turn-final:{tail['plan_token']}:{logical_sequence:06d}"
        row_data = {
            **dict(tail),
            "key": key,
            "kind": "retire",
            "created_at": current,
            "payload_version": 1,
            "partition_sequence": partition_sequence,
            "logical_sequence": logical_sequence,
            "logical_ordinal": None,
            "target_outbox_id": target["id"],
            "predecessor_outbox_id": tail["id"],
        }
        reason = "working_replaced" if target["kind"] == "working" else "excess_part"
        payload = _retire_payload(
            root_payload,
            row=row_data,
            target=target,
            predecessor_key=str(tail["key"]),
            reason=reason,
        )
        cursor = conn.execute(
            """INSERT INTO connector_outbox(
            host_id,connector,key,kind,payload_version,status,partition_key,
            partition_sequence,turn_id,final_identity,content_revision,
            presentation_version,plan_token,plan_generation,logical_sequence,
            predecessor_outbox_id,target_outbox_id,source_outbox_id,
            active_lineage_generation,retry_generation,prior_attempt_count,
            payload_json,created_at,updated_at,available_at)
            VALUES(?,?,?,?,1,'blocked',?,?,?,?,?,?,?,?,?,?,?,?,?,1,0,?,?,?,?)""",
            (
                root["host_id"], root["connector"], key, "retire",
                root["partition_key"], partition_sequence, root["turn_id"],
                root["final_identity"], root["content_revision"],
                tail["presentation_version"], tail["plan_token"],
                tail["plan_generation"], logical_sequence, tail["id"],
                target["id"], root["id"], tail["active_lineage_generation"],
                _json(payload), current, current, current,
            ),
        )
        tail = conn.execute(
            "SELECT * FROM connector_outbox WHERE id=?", (cursor.lastrowid,)
        ).fetchone()
        lineage.append(tail)
    return lineage


def prepare_connector_plan_commit(
    db_path: Path | str,
    host_id: str,
    *,
    name: str,
    plan_token: str,
    source_ref: str | None,
    ack_ttl_seconds: int = 60,
    **_: Any,
) -> dict[str, Any]:
    if not _valid_integer(ack_ttl_seconds, 1, 86_400):
        return _error(host_id, name, "invalid_params")
    if not _matches(_PLAN_RE, plan_token):
        return _error(host_id, name, "invalid_params")
    current = _now()
    with write_transaction(db_path) as conn:
        rows = _plan_rows(conn, host_id, name, plan_token)
        if not rows:
            return _error(host_id, name, "plan_not_found")
        root = _referenced_row(conn, rows[0]["source_outbox_id"])
        if root is None or any(row["source_outbox_id"] != root["id"] for row in rows):
            return _error(host_id, name, "plan_conflict")
        if root["status"] in {"awaiting_ack", "delivered"}:
            state = _plan_state(rows)
            return _plan_success(
                host_id, name, plan_token, state, int(rows[0]["plan_generation"]), len(rows),
                job_count=int(
                    conn.execute(
                        """SELECT COUNT(*) FROM connector_outbox
                        WHERE source_outbox_id=? AND plan_token=?""",
                        (root["id"], plan_token),
                    ).fetchone()[0]
                ),
                accepted_ordinals=list(range(len(rows))),
            )
        if not _matches(_REF_RE, source_ref):
            return _error(host_id, name, "invalid_ref")
        live = _live_delivery(conn, host_id, name, source_ref, current)
        if live is None or live["id"] != root["id"]:
            return _error(host_id, name, "stale_ref")
        if any(row["payload_json"] == "{}" for row in rows):
            return _error(host_id, name, "plan_incomplete")
        if not _exact_plan_coverage(conn, rows):
            return _error(host_id, name, "plan_incomplete")
        if not _current_revision_exists(
            conn, host_id, root["turn_id"], root["content_revision"]
        ):
            return _error(host_id, name, "stale_revision")
        root_payload = _validate_polled_payload(conn, root)
        working_target, old_parts, rotated_targets = _replacement_targets(conn, root)
        predecessor: Any = None
        for index, row in enumerate(rows):
            payload = json.loads(row["payload_json"])
            payload["lineage"]["predecessor_key"] = predecessor["key"] if predecessor else None
            replacement = working_target if index == 0 and working_target is not None else old_parts.get(index)
            payload["lineage"]["replaces_key"] = replacement["key"] if replacement else None
            conn.execute(
                """UPDATE connector_outbox
                SET status=?,predecessor_outbox_id=?,replaces_outbox_id=?,payload_json=?,updated_at=?
                WHERE id=? AND status='staged'""",
                (
                    "queued" if index == 0 else "blocked",
                    predecessor["id"] if predecessor else None,
                    replacement["id"] if replacement else None,
                    _json(payload),
                    current,
                    row["id"],
                ),
            )
            predecessor = row
        rows = _plan_rows(conn, host_id, name, plan_token)
        excess = [old_parts[index] for index in sorted(old_parts) if index >= len(rows)]
        retire_targets = (
            [working_target]
            if working_target is not None
            else [*excess, *rotated_targets]
        )
        lineage = _append_replacement_retires(
            conn, root, rows, root_payload, current, retire_targets
        )
        deadline = add_seconds(current, ack_ttl_seconds)
        changed = conn.execute(
            """UPDATE connector_deliveries
            SET status='awaiting_ack',ack_deadline_at=?
            WHERE id=? AND outbox_id=? AND status='leased'
              AND ref_hash=? AND leased_until>?""",
            (
                deadline,
                live["delivery_id"],
                root["id"],
                _hash_text(source_ref),
                current,
            ),
        ).rowcount
        if changed != 1:
            return _error(host_id, name, "stale_ref")
        changed = conn.execute(
            """UPDATE connector_outbox
            SET status='awaiting_ack',active_lineage_generation=?,updated_at=?
            WHERE id=? AND status='leased' AND current_delivery_id=?""",
            (rows[0]["plan_generation"], current, root["id"], live["delivery_id"]),
        ).rowcount
        if changed != 1:
            raise OutboxInvariantError("source commit CAS failed")
    return _plan_success(
        host_id, name, plan_token, "active", int(rows[0]["plan_generation"]), len(rows),
        job_count=len(lineage), accepted_ordinals=list(range(len(rows))),
    )


def _recovery_replay_result(
    conn: Any,
    host_id: str,
    name: str,
    failed_plan_token: str,
    head: Any,
) -> dict[str, Any]:
    source = _referenced_row(conn, head["source_outbox_id"])
    rows = conn.execute(
        """SELECT * FROM connector_outbox
        WHERE source_outbox_id=? AND plan_token=? ORDER BY logical_sequence""",
        (source["id"], head["plan_token"]),
    ).fetchall()
    prefix_count = int(
        conn.execute(
            """SELECT COUNT(*) FROM connector_outbox
            WHERE source_outbox_id=? AND plan_generation<? AND status='delivered'""",
            (source["id"], head["plan_generation"]),
        ).fetchone()[0]
    )
    return _success(
        host_id, name, "recovered", failed_plan_token=failed_plan_token,
        plan_token=head["plan_token"], generation=int(head["plan_generation"]),
        content_revision=source["content_revision"], state="active",
        acknowledged_prefix_count=prefix_count, executable_job_count=len(rows),
        retained_failed_job_count=int(
            conn.execute(
                """SELECT COUNT(*) FROM connector_outbox
                WHERE source_outbox_id=? AND recovered_from_plan_token=?
                  AND kind='retire' AND status='dead_letter'""",
                (source["id"], failed_plan_token),
            ).fetchone()[0]
        ),
        prior_attempt_count=sum(int(row["prior_attempt_count"]) for row in rows),
        idempotent_replay=True,
    )


def _materialize_recovery_suffix(
    conn: Any,
    *,
    host_id: str,
    name: str,
    root: Any,
    prefix: Sequence[Any],
    suffix: Sequence[Any],
    failed_plan_token: str,
    token: str,
    generation: int,
    request_digest: str,
    current: str,
) -> tuple[list[Any], int, int]:
    predecessor_id = prefix[-1]["id"] if prefix else None
    predecessor_key = prefix[-1]["key"] if prefix else None
    retained_failed = 0
    prior_attempts = 0
    created: list[Any] = []
    for index, prior in enumerate(suffix):
        attempts = _attempt_count(conn, int(prior["id"]), int(prior["retry_generation"]))
        accumulated = attempts + int(prior["prior_attempt_count"])
        prior_attempts += accumulated
        mandatory_retire = prior["kind"] == "retire" and attempts > 0
        if mandatory_retire:
            retained_failed += 1
        else:
            changed = conn.execute(
                """UPDATE connector_outbox
                SET status='superseded',current_delivery_id=NULL,updated_at=?
                WHERE id=? AND status IN('dead_letter','superseded')""",
                (current, prior["id"]),
            ).rowcount
            if changed != 1:
                raise OutboxInvariantError("recovery supersession CAS failed")
        partition_sequence = _allocate_partition_sequence(conn, host_id, str(root["partition_key"]))
        logical_sequence = int(prior["logical_sequence"])
        key = f"turn-final:{token}:{logical_sequence:06d}"
        payload = json.loads(prior["payload_json"])
        payload["created_at"] = current
        payload["route"]["partition_sequence"] = partition_sequence
        if "plan" in payload:
            payload["plan"].update(plan_token=token, generation=generation)
        if "retire" in payload:
            payload["retire"].update(plan_token=token, generation=generation,
                                     predecessor_key=predecessor_key)
        if "lineage" in payload:
            payload["lineage"].update(
                recovered_from_plan_token=failed_plan_token,
                predecessor_key=predecessor_key, replaces_key=prior["key"],
            )
        cursor = conn.execute(
            """INSERT INTO connector_outbox(
            host_id,connector,key,kind,payload_version,status,partition_key,
            partition_sequence,turn_id,final_identity,content_revision,
            presentation_version,plan_token,plan_generation,logical_sequence,
            logical_ordinal,predecessor_outbox_id,replaces_outbox_id,
            target_outbox_id,source_outbox_id,active_lineage_generation,
            recovery_request_digest,recovered_from_plan_token,retry_generation,
            prior_attempt_count,payload_json,created_at,updated_at,available_at)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                host_id, name, key, prior["kind"], prior["payload_version"],
                "queued" if index == 0 else "blocked", root["partition_key"], partition_sequence,
                prior["turn_id"], prior["final_identity"], prior["content_revision"],
                prior["presentation_version"], token, generation, logical_sequence,
                prior["logical_ordinal"], predecessor_id, prior["id"], prior["target_outbox_id"],
                root["id"], generation, request_digest if index == 0 else None,
                failed_plan_token, 1, accumulated, _json(payload), current, current, current,
            ),
        )
        predecessor_id = int(cursor.lastrowid)
        predecessor_key = key
        created.append(_referenced_row(conn, cursor.lastrowid))
    return created, retained_failed, prior_attempts


def prepare_connector_plan_recover(
    db_path: Path | str,
    host_id: str,
    *,
    name: str,
    failed_plan_token: str,
    request_id: str,
    ack_ttl_seconds: int = 60,
    **_: Any,
) -> dict[str, Any]:
    if (
        not _matches(_PLAN_RE, failed_plan_token)
        or not isinstance(request_id, str) or not 1 <= len(request_id) <= 128
        or not _valid_integer(ack_ttl_seconds, 1, 86_400)
    ):
        return _error(host_id, name, "invalid_params")
    request_digest = _hash_text(_json({
        "domain": "tendwire.turn-final.recover.v1", "host_id": host_id, "name": name,
        "failed_plan_token": failed_plan_token, "request_id": request_id,
    }))
    current = _now()
    with write_transaction(db_path) as conn:
        replay = conn.execute(
            """SELECT * FROM connector_outbox
            WHERE host_id=? AND connector=? AND recovery_request_digest=?""",
            (host_id, name, request_digest),
        ).fetchone()
        if replay is not None:
            return _recovery_replay_result(conn, host_id, name, failed_plan_token, replay)
        old = conn.execute(
            """SELECT * FROM connector_outbox
            WHERE host_id=? AND connector=? AND plan_token=?
            ORDER BY logical_sequence""",
            (host_id, name, failed_plan_token),
        ).fetchall()
        if not old:
            return _error(host_id, name, "plan_not_found")
        source_ids = {row["source_outbox_id"] for row in old}
        if len(source_ids) != 1 or None in source_ids:
            return _error(host_id, name, "not_recoverable")
        source_id = int(next(iter(source_ids)))
        root = conn.execute(
            """SELECT o.*,d.id AS delivery_id,d.status AS delivery_status,
            d.ack_deadline_at FROM connector_outbox o
            JOIN connector_deliveries d ON d.id=o.current_delivery_id
            WHERE o.id=?""",
            (source_id,),
        ).fetchone()
        if root is None or root["status"] != "awaiting_ack" or root["delivery_status"] != "awaiting_ack":
            return _error(host_id, name, "not_recoverable")
        if root["ack_deadline_at"] <= current:
            return _error(host_id, name, "ack_deadline_expired")
        prefix: list[Any] = []
        suffix: list[Any] = []
        failed_seen = False
        for row in old:
            if not failed_seen and row["status"] == "delivered":
                prefix.append(row)
            else:
                failed_seen = True
                suffix.append(row)
        if not suffix or any(row["status"] not in {"dead_letter", "superseded"} for row in suffix):
            return _error(host_id, name, "not_recoverable")
        generation = max(int(row["plan_generation"] or 1) for row in old) + 1
        token = "twplan1." + secrets.token_urlsafe(32)
        created, retained_failed, prior_attempts = _materialize_recovery_suffix(
            conn,
            host_id=host_id,
            name=name,
            root=root,
            prefix=prefix,
            suffix=suffix,
            failed_plan_token=failed_plan_token,
            token=token,
            generation=generation,
            request_digest=request_digest,
            current=current,
        )
        deadline = add_seconds(current, ack_ttl_seconds)
        changed = conn.execute(
            """UPDATE connector_deliveries SET ack_deadline_at=?
            WHERE id=? AND outbox_id=? AND status='awaiting_ack'
              AND ack_deadline_at>?""",
            (deadline, root["delivery_id"], source_id, current),
        ).rowcount
        if changed != 1:
            raise OutboxInvariantError("recovery ack-deadline CAS failed")
        changed = conn.execute(
            """UPDATE connector_outbox
            SET active_lineage_generation=?,updated_at=?
            WHERE id=? AND status='awaiting_ack'""",
            (generation, current, source_id),
        ).rowcount
        if changed != 1:
            raise OutboxInvariantError("recovery source CAS failed")
    return _success(
        host_id, name, "recovered", failed_plan_token=failed_plan_token,
        plan_token=token, generation=generation, content_revision=root["content_revision"],
        state="active", acknowledged_prefix_count=len(prefix),
        executable_job_count=len(created), retained_failed_job_count=retained_failed,
        prior_attempt_count=prior_attempts, idempotent_replay=False,
    )


def _latest_reason(conn: Any, outbox_id: int) -> tuple[str, int]:
    row = conn.execute(
        """SELECT private_reason_enum,attempt FROM connector_deliveries
        WHERE outbox_id=? ORDER BY retry_generation DESC,attempt DESC LIMIT 1""",
        (outbox_id,),
    ).fetchone()
    if row is None:
        return "attempts_exhausted", 0
    reason = str(row[0] or "attempts_exhausted")
    return reason if reason in _PRIVATE_REASONS else "attempts_exhausted", int(row[1])


def _standalone_retryable(row: Any) -> bool:
    return bool(
        row["source_outbox_id"] is None
        and row["kind"] in {"final_ready", "decision", "retire"}
        and not (row["kind"] == "final_ready" and row["active_lineage_generation"] is not None)
    )


def inspect_connector_outbox(
    db_path: Path | str,
    host_id: str,
    *,
    name: str,
    status: str,
    limit: int,
) -> dict[str, Any]:
    if name != TURN_FINAL_CONNECTOR or status != "dead_letter":
        return _error(host_id, name, "invalid_params")
    if not _valid_integer(limit, 1, 100):
        return _error(host_id, name, "invalid_params")
    current = _now()
    with read_transaction(db_path) as conn:
        raw = conn.execute(
            """SELECT * FROM connector_outbox
            WHERE host_id=? AND connector=? AND status='dead_letter'
            ORDER BY updated_at DESC,id DESC""",
            (host_id, name),
        ).fetchall()
        grouped: list[Any] = []
        seen: set[tuple[str, Any]] = set()
        for row in raw:
            identity = (("plan", row["source_outbox_id"], row["plan_token"])
                        if row["source_outbox_id"] is not None else ("root", row["id"]))
            if identity in seen:
                continue
            seen.add(identity)
            grouped.append(row)
        total = len(grouped)
        items: list[dict[str, Any]] = []
        for row in grouped[:limit]:
            reason, current_attempt = _latest_reason(conn, int(row["id"]))
            attempts = _attempt_count(conn, int(row["id"]), int(row["retry_generation"]))
            retryable = _standalone_retryable(row)
            root = None
            if row["source_outbox_id"] is not None:
                root = conn.execute(
                    """SELECT o.status,d.ack_deadline_at FROM connector_outbox o
                    LEFT JOIN connector_deliveries d ON d.id=o.current_delivery_id
                    WHERE o.id=?""",
                    (row["source_outbox_id"],),
                ).fetchone()
            recoverable = bool(
                root is not None
                and root["status"] == "awaiting_ack"
                and isinstance(root["ack_deadline_at"], str)
                and root["ack_deadline_at"] > current
            )
            target = None
            if row["target_outbox_id"] is not None:
                target_row = conn.execute(
                    "SELECT key FROM connector_outbox WHERE id=?",
                    (row["target_outbox_id"],),
                ).fetchone()
                target = target_row[0] if target_row else None
            items.append(
                {
                    "kind": row["kind"], "key": row["key"],
                    "final_identity": row["final_identity"],
                    "failed_plan_token": row["plan_token"] if row["source_outbox_id"] is not None else None,
                    "decision_ref": row["decision_ref"], "target_key": target, "reason": reason,
                    "attempt_count": max(attempts, current_attempt),
                    "prior_attempt_count": int(row["prior_attempt_count"]),
                    "created_at": row["created_at"], "terminal_at": row["updated_at"],
                    "retryable": retryable, "recoverable": recoverable,
                }
            )
    return _success(host_id, name, "ok", total=total, items=items)


def retry_connector_dead_letter(
    db_path: Path | str,
    host_id: str,
    *,
    key: str | None = None,
    final_identity: str | None = None,
) -> dict[str, Any]:
    if (key is None) == (final_identity is None):
        return _error(host_id, TURN_FINAL_CONNECTOR, "invalid_params")
    current = _now()
    with write_transaction(db_path) as conn:
        if key is not None:
            rows = conn.execute(
                """SELECT * FROM connector_outbox
                WHERE host_id=? AND connector=? AND key=?""",
                (host_id, TURN_FINAL_CONNECTOR, key),
            ).fetchall()
        else:
            rows = conn.execute(
                """SELECT * FROM connector_outbox
                WHERE host_id=? AND connector=? AND final_identity=? AND kind='final_ready'""",
                (host_id, TURN_FINAL_CONNECTOR, final_identity),
            ).fetchall()
        if len(rows) != 1:
            return _error(host_id, TURN_FINAL_CONNECTOR, "not_retryable")
        row = rows[0]
        if row["status"] != "dead_letter" or not _standalone_retryable(row):
            return _error(host_id, TURN_FINAL_CONNECTOR, "not_retryable")
        attempts = _attempt_count(conn, int(row["id"]), int(row["retry_generation"]))
        changed = conn.execute(
            """UPDATE connector_outbox
            SET status='queued',retry_generation=retry_generation+1,
                prior_attempt_count=prior_attempt_count+?,available_at=?,updated_at=?
            WHERE id=? AND status='dead_letter' AND current_delivery_id IS NULL""",
            (attempts, current, current, row["id"]),
        ).rowcount
        if changed != 1:
            return _error(host_id, TURN_FINAL_CONNECTOR, "not_retryable")
    return _success(
        host_id, TURN_FINAL_CONNECTOR, "requeued", key=row["key"],
        retry_generation=int(row["retry_generation"]) + 1,
        prior_attempt_count=int(row["prior_attempt_count"]) + attempts,
        warning=(
            "provider_acceptance_may_have_occurred"
            if reason_for_retry_requires_warning(db_path, int(row["id"]))
            else None
        ),
    )


def reason_for_retry_requires_warning(db_path: Path | str, outbox_id: int) -> bool:
    with read_transaction(db_path) as conn:
        reason, _attempt = _latest_reason(conn, outbox_id)
    return reason == "provider_uncertain"


__all__ = tuple(
    (
        "ack_connector_delivery",
        "connector_reclaim_due",
        "defer_connector_delivery",
        "fail_connector_delivery",
        "inspect_connector_outbox",
        "poll_connector_outbox",
        "prepare_connector_plan_begin",
        "prepare_connector_plan_commit",
        "prepare_connector_plan_part",
        "prepare_connector_plan_recover",
        "reclaim_expired_connector_leases",
        "release_connector_delivery",
        "renew_connector_delivery",
        "retry_connector_dead_letter",
    )
)
