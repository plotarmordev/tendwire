"""Command receipts, submission fences, replay, and turn linkage."""

from __future__ import annotations

import hashlib
import json
import math
import secrets
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Mapping

from ..core.commands import CommandEnvelope, instruction_fingerprint, turn_submission_id
from .db import add_seconds, canonical_utc, read_transaction, utc_now, write_transaction
from .projection import presentation_binding_row


_TERMINAL_STATES = frozenset({"accepted", "rejected", "uncertain"})
_OPEN_SUBMISSION_STATES = frozenset({"send_started", "submitted"})
_TRANSITIONS = {
    "reserved": frozenset({"rejected", "uncertain"}),
    "send_started": frozenset({"accepted", "rejected", "uncertain"}),
}


def _hash(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def _now(value: str | None) -> str:
    return utc_now() if value is None else canonical_utc(value)


def _validate_request_identity(
    *,
    host_id: str,
    request_id: str,
    action: str,
    canonical_version: int,
    canonical_fingerprint: str,
    canonical_request_json: str,
    public_worker_id: str,
) -> None:
    if any(
        not isinstance(value, str) or not value.strip()
        for value in (host_id, request_id, action, canonical_fingerprint, public_worker_id)
    ):
        raise ValueError("command request identity fields must be non-empty")
    if type(canonical_version) is not int or canonical_version < 1:
        raise ValueError("canonical_version must be an integer >= 1")
    try:
        request = json.loads(canonical_request_json)
    except (TypeError, json.JSONDecodeError) as exc:
        raise ValueError("canonical_request_json must be JSON") from exc
    if not isinstance(request, Mapping):
        raise ValueError("canonical_request_json must be a JSON object")


def _row(row: Any) -> dict[str, Any]:
    return {
        "host_id": row["host_id"],
        "request_id": row["request_id"],
        "action": row["action"],
        "canonical_version": row["canonical_version"],
        "canonical_fingerprint": row["request_fingerprint"],
        "canonical_request_json": row["request_json"],
        "public_worker_id": row["public_worker_id"],
        "state": row["state"],
        "status": row["status"],
        "result_json": row["result_json"],
        "selector_proof": row["selector_proof"],
        "owner_expires_at": row["owner_until"],
        "binding_fingerprint": row["binding_fingerprint"],
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }


def _response(
    status: str,
    row: Any | None,
    *,
    owner_token: str | None = None,
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "status": status,
        "receipt": None if row is None else _row(row),
    }
    if owner_token is not None:
        result["owner_token"] = owner_token
    return result


def _same(
    row: Any,
    *,
    action: str,
    canonical_version: int,
    canonical_fingerprint: str,
    canonical_request_json: str,
    public_worker_id: str,
    selector_proof: str,
) -> bool:
    return (
        row["action"],
        row["canonical_version"],
        row["request_fingerprint"],
        row["request_json"],
        row["public_worker_id"],
    ) == (
        action,
        canonical_version,
        canonical_fingerprint,
        canonical_request_json,
        public_worker_id,
    ) and str(row["selector_proof"] or "") == selector_proof


def get_command_request(
    db_path: Path,
    host_id: str,
    request_id: str,
) -> dict[str, Any] | None:
    try:
        with read_transaction(db_path) as conn:
            row = conn.execute(
                """SELECT * FROM command_receipts
                WHERE host_id=? AND request_id=?""",
                (host_id, request_id),
            ).fetchone()
    except Exception:
        return None
    return None if row is None else _row(row)


def command_reservation_is_live(
    receipt: Mapping[str, Any],
    *,
    now: str | None = None,
) -> bool:
    if receipt.get("state") not in {"reserved", "send_started"}:
        return False
    try:
        expiry = canonical_utc(receipt.get("owner_expires_at"))
        current = _now(now)
    except (TypeError, ValueError):
        return False
    return datetime.fromisoformat(expiry) > datetime.fromisoformat(current)


def reserve_command_request(
    db_path: Path,
    *,
    host_id: str,
    request_id: str,
    action: str,
    canonical_version: int,
    canonical_fingerprint: str,
    canonical_request_json: str,
    public_worker_id: str,
    pending_result_json: str,
    selector_proof: str = "",
    owner_lease_seconds: float = 30.0,
    now: str | None = None,
) -> dict[str, Any]:
    _validate_request_identity(
        host_id=host_id,
        request_id=request_id,
        action=action,
        canonical_version=canonical_version,
        canonical_fingerprint=canonical_fingerprint,
        canonical_request_json=canonical_request_json,
        public_worker_id=public_worker_id,
    )
    if isinstance(owner_lease_seconds, bool):
        raise ValueError("owner lease must be positive and finite")
    seconds = float(owner_lease_seconds)
    if not math.isfinite(seconds) or not 0 < seconds <= 86_400:
        raise ValueError("owner lease must be positive and finite")
    current = _now(now)
    until = add_seconds(current, seconds)
    token = secrets.token_urlsafe(32)
    digest = _hash(token)
    with write_transaction(db_path) as conn:
        row = conn.execute(
            """SELECT * FROM command_receipts
            WHERE host_id=? AND request_id=?""",
            (host_id, request_id),
        ).fetchone()
        if row is not None:
            if not _same(
                row,
                action=action,
                canonical_version=canonical_version,
                canonical_fingerprint=canonical_fingerprint,
                canonical_request_json=canonical_request_json,
                public_worker_id=public_worker_id,
                selector_proof=selector_proof,
            ):
                return _response("request_id_conflict", row)
            if row["state"] in _TERMINAL_STATES:
                return _response("terminal", row)
            if row["state"] == "send_started" or command_reservation_is_live(
                _row(row),
                now=current,
            ):
                return _response("in_progress", row)
            conn.execute(
                """UPDATE command_receipts
                SET owner_hash=?,owner_until=?,status='pending',result_json=?,updated_at=?
                WHERE host_id=? AND request_id=? AND state='reserved'""",
                (
                    digest,
                    until,
                    pending_result_json,
                    current,
                    host_id,
                    request_id,
                ),
            )
        else:
            conn.execute(
                """INSERT INTO command_receipts(
                host_id,request_id,request_fingerprint,action,canonical_version,
                public_worker_id,state,status,owner_hash,owner_until,selector_proof,
                request_json,result_json,created_at,updated_at)
                VALUES(?,?,?,?,?,?,'reserved','pending',?,?,?,?,?,?,?)""",
                (
                    host_id,
                    request_id,
                    canonical_fingerprint,
                    action,
                    canonical_version,
                    public_worker_id,
                    digest,
                    until,
                    selector_proof,
                    canonical_request_json,
                    pending_result_json,
                    current,
                    current,
                ),
            )
        row = conn.execute(
            """SELECT * FROM command_receipts
            WHERE host_id=? AND request_id=?""",
            (host_id, request_id),
        ).fetchone()
    return _response("reserved", row, owner_token=token)


def abandon_command_request_reservation(
    db_path: Path,
    *,
    host_id: str,
    request_id: str,
    canonical_fingerprint: str,
    owner_token: str,
    now: str | None = None,
) -> bool:
    current = _now(now)
    with write_transaction(db_path) as conn:
        return (
            conn.execute(
                """UPDATE command_receipts SET owner_until=?,updated_at=?
                WHERE host_id=? AND request_id=? AND request_fingerprint=?
                  AND state='reserved' AND owner_hash=?""",
                (
                    current,
                    current,
                    host_id,
                    request_id,
                    canonical_fingerprint,
                    _hash(owner_token),
                ),
            ).rowcount
            == 1
        )


def mark_command_send_started(
    db_path: Path,
    *,
    host_id: str,
    request_id: str,
    canonical_fingerprint: str,
    owner_token: str,
    binding_fingerprint: str,
    send_started_effect: Callable[[Any], Any] | None = None,
    submission_worker: Any | None = None,
    instruction_text: str | None = None,
    submission_link_window_seconds: int = 60,
    submission_hard_ttl_seconds: int = 86_400,
    now: str | None = None,
    **_: Any,
) -> dict[str, Any]:
    if not isinstance(binding_fingerprint, str) or not binding_fingerprint:
        raise ValueError("binding_fingerprint must be non-empty")
    if (submission_worker is None) is not (instruction_text is None):
        raise ValueError("submission worker and instruction must be paired")
    windows = (submission_link_window_seconds, submission_hard_ttl_seconds)
    if any(type(value) is not int or value <= 0 for value in windows):
        raise ValueError("submission windows must be positive integers")
    if submission_hard_ttl_seconds < submission_link_window_seconds:
        raise ValueError("submission hard TTL must cover the link window")
    current = _now(now)
    with write_transaction(db_path) as conn:
        row = conn.execute(
            """SELECT * FROM command_receipts
            WHERE host_id=? AND request_id=?""",
            (host_id, request_id),
        ).fetchone()
        if row is None:
            return _response("not_found", None)
        if row["request_fingerprint"] != canonical_fingerprint:
            return _response("request_id_conflict", row)
        if row["state"] in _TERMINAL_STATES:
            return _response("terminal", row)
        if row["state"] != "reserved" or not secrets.compare_digest(
            str(row["owner_hash"] or ""),
            _hash(owner_token),
        ):
            return _response("not_owner", row)
        binding = conn.execute(
            """SELECT * FROM worker_bindings
            WHERE host_id=? AND worker_id=? AND private_fingerprint=?
              AND backend='acp' AND stable_key_version=1
              AND (expires_at IS NULL OR expires_at>?)""",
            (host_id, row["public_worker_id"], binding_fingerprint, current),
        ).fetchone()
        if binding is None:
            return _response("stale_route", row)
        try:
            private_binding = json.loads(str(binding["private_binding_json"]))
        except (TypeError, json.JSONDecodeError) as exc:
            raise ValueError("private binding is corrupt") from exc
        if (
            private_binding.get("host_id") != binding["host_id"]
            or private_binding.get("worker_id") != binding["worker_id"]
            or private_binding.get("backend") != "acp"
            or private_binding.get("private_fingerprint") != binding["private_fingerprint"]
            or private_binding.get("sendable") is not True
        ):
            return _response("stale_route", row)
        presentation = presentation_binding_row(
            conn, host_id, str(binding["worker_id"]), binding
        )
        if presentation is None:
            return _response("stale_route", row)
        authority = {
            "worker_id": binding["worker_id"],
            "worker_fingerprint": private_binding.get("worker_fingerprint"),
            "stable_key": binding["stable_key"],
            "stable_key_version": binding["stable_key_version"],
            "route_generation": binding["route_generation"],
            "private_fingerprint": binding["private_fingerprint"],
        }
        if submission_worker is not None:
            meta = getattr(submission_worker, "meta", {})
            snapshot_row = conn.execute(
                """SELECT payload_json FROM snapshots WHERE host_id=?
                ORDER BY observed_at DESC,authority_fingerprint DESC LIMIT 1""",
                (host_id,),
            ).fetchone()
            try:
                snapshot_workers = json.loads(snapshot_row["payload_json"])["workers"]
            except (TypeError, KeyError, json.JSONDecodeError):
                return _response("stale_route", row)
            public = [
                worker for worker in snapshot_workers
                if isinstance(worker, Mapping)
                and worker.get("id") == getattr(submission_worker, "id", None)
            ]
            public_meta = public[0].get("meta") if len(public) == 1 else None
            if (
                len(public) != 1
                or getattr(submission_worker, "id", None) != binding["worker_id"]
                or public[0].get("fingerprint")
                != getattr(submission_worker, "fingerprint", None)
                or not isinstance(meta, Mapping)
                or not isinstance(public_meta, Mapping)
                or meta.get("stable_key") != binding["stable_key"]
                or public_meta.get("stable_key") != binding["stable_key"]
                or meta.get("stable_key_version") != binding["stable_key_version"]
                or public_meta.get("stable_key_version") != binding["stable_key_version"]
            ):
                return _response("stale_route", row)
        authority_fingerprint = _hash(
            json.dumps(
                authority,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            )
        )
        changed = conn.execute(
            """UPDATE command_receipts
            SET state='send_started',binding_fingerprint=?,updated_at=?
            WHERE host_id=? AND request_id=? AND state='reserved'
              AND request_fingerprint=? AND owner_hash=?""",
            (
                authority_fingerprint,
                current,
                host_id,
                request_id,
                canonical_fingerprint,
                _hash(owner_token),
            ),
        ).rowcount
        if changed != 1:
            return _response("not_owner", row)
        submission_id = None
        if submission_worker is not None and instruction_text is not None:
            stable = submission_worker.meta.get("stable_key")
            generation = str(presentation["route_generation"])
            if not isinstance(stable, str) or not generation:
                raise ValueError("submission worker route is incomplete")
            fingerprint = instruction_fingerprint(instruction_text)
            submission_id = turn_submission_id(host_id, request_id)
            conn.execute(
                """INSERT INTO turn_submissions(
                host_id,submission_id,request_id,worker_id,route_generation,
                instruction_fingerprint,state,link_expires_at,hard_expires_at,
                created_at,updated_at)
                VALUES(?,?,?,?,?,?,'send_started',?,?,?,?)""",
                (
                    host_id,
                    submission_id,
                    request_id,
                    submission_worker.id,
                    generation,
                    fingerprint,
                    add_seconds(current, submission_link_window_seconds),
                    add_seconds(current, submission_hard_ttl_seconds),
                    current,
                    current,
                ),
            )
        effect = send_started_effect(conn) if send_started_effect else None
        row = conn.execute(
            """SELECT * FROM command_receipts
            WHERE host_id=? AND request_id=?""",
            (host_id, request_id),
        ).fetchone()
    result = _response("send_started", row, owner_token=owner_token)
    if submission_id:
        result["submission_id"] = submission_id
    if send_started_effect:
        result["effect_result"] = effect
    return result


def finish_command_request(
    db_path: Path,
    *,
    host_id: str,
    request_id: str,
    canonical_fingerprint: str,
    owner_token: str,
    expected_state: str,
    terminal_state: str,
    status: str,
    result_json: str,
    terminal_effect: Callable[[Any], Any] | None = None,
    now: str | None = None,
    **_: Any,
) -> dict[str, Any]:
    if terminal_state not in _TRANSITIONS.get(expected_state, frozenset()):
        raise ValueError("illegal command receipt transition")
    if terminal_state == "accepted" and status != "accepted":
        raise ValueError("accepted receipt requires accepted status")
    if terminal_state == "uncertain" and status != "request_state_uncertain":
        raise ValueError("uncertain receipt requires uncertain status")
    current = _now(now)
    with write_transaction(db_path) as conn:
        row = conn.execute(
            """SELECT * FROM command_receipts
            WHERE host_id=? AND request_id=?""",
            (host_id, request_id),
        ).fetchone()
        if row is None:
            return _response("not_found", None)
        if row["request_fingerprint"] != canonical_fingerprint:
            return _response("request_id_conflict", row)
        if row["state"] in _TERMINAL_STATES:
            return _response("terminal", row)
        if row["state"] != expected_state or not secrets.compare_digest(
            str(row["owner_hash"] or ""),
            _hash(owner_token),
        ):
            return _response("not_owner", row)
        if terminal_effect:
            terminal_effect(conn)
        conn.execute(
            """UPDATE command_receipts
            SET state=?,status=?,result_json=?,owner_hash='',owner_until=NULL,updated_at=?
            WHERE host_id=? AND request_id=?""",
            (
                terminal_state,
                status,
                result_json,
                current,
                host_id,
                request_id,
            ),
        )
        if terminal_state == "accepted":
            conn.execute(
                """UPDATE turn_submissions SET state='submitted',updated_at=?
                WHERE host_id=? AND request_id=? AND state='send_started'""",
                (current, host_id, request_id),
            )
        else:
            conn.execute(
                """UPDATE turn_submissions SET state=?,updated_at=?
                WHERE host_id=? AND request_id=?
                  AND state NOT IN('linked','ambiguous','expired')""",
                (terminal_state, current, host_id, request_id),
            )
        row = conn.execute(
            """SELECT * FROM command_receipts
            WHERE host_id=? AND request_id=?""",
            (host_id, request_id),
        ).fetchone()
    return _response(terminal_state, row)


def linked_turn_for_submission(
    db_path: Path,
    *,
    host_id: str,
    request_id: str,
) -> dict[str, Any] | None:
    with read_transaction(db_path) as conn:
        row = conn.execute(
            """SELECT t.payload_json FROM turn_submissions s
            JOIN turns t ON t.host_id=s.host_id AND t.turn_id=s.turn_id
            WHERE s.host_id=? AND s.request_id=? AND s.state='linked'""",
            (host_id, request_id),
        ).fetchone()
    return None if row is None else json.loads(row[0])


def _settle_submission_conn(conn: Any, submission: Any, current: str) -> dict[str, Any]:
    if submission["state"] not in _OPEN_SUBMISSION_STATES:
        return {
            "state": submission["state"],
            "linked_turn_id": submission["turn_id"],
            "changed": False,
        }
    rows = conn.execute(
        """SELECT t.turn_id,r.user_text FROM turns t
        LEFT JOIN turn_content_revisions r ON r.host_id=t.host_id
          AND r.turn_id=t.turn_id AND r.is_current=1
        WHERE t.host_id=? AND t.worker_id=? AND t.route_generation=?
          AND t.observed_at>=? AND t.observed_at<=? AND t.observed_at<=?
          AND t.removed_at IS NULL
        ORDER BY t.observed_at,t.turn_id LIMIT 3""",
        (
            submission["host_id"],
            submission["worker_id"],
            submission["route_generation"],
            submission["created_at"],
            submission["link_expires_at"],
            current,
        ),
    ).fetchall()
    candidates = [
        row
        for row in rows
        if instruction_fingerprint(row["user_text"])
        == submission["instruction_fingerprint"]
    ]
    state = (
        "linked" if len(candidates) == 1 and len(rows) < 3
        else "ambiguous" if len(candidates) > 1 or len(rows) >= 3
        else "expired" if current >= submission["hard_expires_at"]
        else submission["state"]
    )
    turn_id = candidates[0][0] if state == "linked" else None
    changed = state != submission["state"] or turn_id != submission["turn_id"]
    if changed:
        conn.execute(
            """UPDATE turn_submissions SET state=?,turn_id=?,updated_at=?
            WHERE host_id=? AND request_id=? AND state IN('send_started','submitted')""",
            (
                state,
                turn_id,
                current,
                submission["host_id"],
                submission["request_id"],
            ),
        )
    return {"state": state, "linked_turn_id": turn_id, "changed": changed}


def settle_submission_link_for_request(
    db_path: Path,
    *,
    host_id: str,
    request_id: str,
    now: str | None = None,
) -> dict[str, Any] | None:
    current = _now(now)
    with write_transaction(db_path) as conn:
        submission = conn.execute(
            """SELECT * FROM turn_submissions
            WHERE host_id=? AND request_id=?""",
            (host_id, request_id),
        ).fetchone()
        return None if submission is None else _settle_submission_conn(conn, submission, current)


def settle_due_submission_links_conn(conn: Any, *, now: str, limit: int) -> int:
    """Settle a bounded maintenance batch and terminalize expired uncertain sends."""
    current = _now(now)
    rows = conn.execute(
        """SELECT * FROM turn_submissions
        WHERE state IN('send_started','submitted') AND link_expires_at<=?
        ORDER BY hard_expires_at,host_id,submission_id LIMIT ?""",
        (current, limit),
    ).fetchall()
    changed = 0
    for submission in rows:
        result = _settle_submission_conn(conn, submission, current)
        changed += int(result["changed"])
        if result["state"] != "expired":
            continue
        receipt = conn.execute(
            """SELECT state,status,result_json FROM command_receipts
            WHERE host_id=? AND request_id=?""",
            (submission["host_id"], submission["request_id"]),
        ).fetchone()
        if receipt is None or receipt["state"] != "send_started":
            continue
        try:
            envelope = json.loads(receipt["result_json"])
            result_payload = dict(envelope.get("result") or {})
            result_payload.update(
                {
                    "delivery_state": "unknown",
                    "transport_state": "unknown",
                    "submission_verdict": "unknown",
                }
            )
            envelope.update(
                {
                    "ok": False,
                    "status": "request_state_uncertain",
                    "disposition": "terminal_uncertain",
                    "result": result_payload,
                    "error": {
                        "code": "request_state_uncertain",
                        "message": (
                            "submission observation window expired; "
                            "not retrying mutation"
                        ),
                    },
                }
            )
            result_json = json.dumps(
                envelope,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            )
        except (TypeError, json.JSONDecodeError):
            continue
        conn.execute(
            """UPDATE command_receipts SET state='uncertain',status='request_state_uncertain',
            result_json=?,owner_hash='',owner_until=NULL,updated_at=?
            WHERE host_id=? AND request_id=? AND state='send_started'""",
            (
                result_json,
                current,
                submission["host_id"],
                submission["request_id"],
            ),
        )
    return changed


def _terminal_without_owner(
    db_path: Path,
    *,
    state: str,
    status: str,
    result_key: str,
    required_submission_states: frozenset[str] | None = None,
    **kwargs: Any,
) -> dict[str, Any]:
    host_id = kwargs["host_id"]
    request_id = kwargs["request_id"]
    with write_transaction(db_path) as conn:
        row = conn.execute(
            """SELECT * FROM command_receipts
            WHERE host_id=? AND request_id=?""",
            (host_id, request_id),
        ).fetchone()
        if row is None:
            return _response("not_found", None)
        if row["request_fingerprint"] != kwargs["canonical_fingerprint"]:
            return _response("request_id_conflict", row)
        if row["state"] in _TERMINAL_STATES:
            return _response("terminal", row)
        queued_result = kwargs.get("queued_result_json", kwargs.get("unresolved_result_json"))
        if (
            row["state"] != "send_started"
            or row["status"] != "pending"
            or queued_result is None
            or row["result_json"] != queued_result
        ):
            return _response("in_progress", row)
        if (
            not command_reservation_is_live(_row(row), now=kwargs.get("now"))
            and result_key == "uncertain_result_json"
            and required_submission_states is None
        ):
            pass
        elif required_submission_states is not None:
            submission = conn.execute(
                """SELECT state,turn_id FROM turn_submissions
                WHERE host_id=? AND request_id=?""",
                (host_id, request_id),
            ).fetchone()
            if submission is None or submission["state"] not in required_submission_states:
                return _response("in_progress", row)
            if state == "accepted" and submission["turn_id"] is None:
                return _response("in_progress", row)
        elif command_reservation_is_live(_row(row), now=kwargs.get("now")):
            return _response("in_progress", row)
        current = _now(kwargs.get("now"))
        changed = conn.execute(
            """UPDATE command_receipts
            SET state=?,status=?,result_json=?,owner_hash='',owner_until=NULL,updated_at=?
            WHERE host_id=? AND request_id=? AND state='send_started'
              AND request_fingerprint=? AND status='pending' AND result_json=?""",
            (
                state,
                status,
                kwargs[result_key],
                current,
                host_id,
                request_id,
                kwargs["canonical_fingerprint"],
                queued_result,
            ),
        ).rowcount
        if changed != 1:
            return _response("in_progress", row)
        conn.execute(
            """UPDATE turn_submissions SET state=?,updated_at=?
            WHERE host_id=? AND request_id=? AND state<>'linked'""",
            (state, current, host_id, request_id),
        )
        row = conn.execute(
            """SELECT * FROM command_receipts
            WHERE host_id=? AND request_id=?""",
            (host_id, request_id),
        ).fetchone()
    return _response(state, row)


def recover_unresolved_command_send(db_path: Path, **kwargs: Any) -> dict[str, Any]:
    return _terminal_without_owner(
        db_path,
        state="uncertain",
        status="request_state_uncertain",
        result_key="uncertain_result_json",
        **kwargs,
    )


def envelope_to_receipt_json(envelope: CommandEnvelope) -> str:
    return json.dumps(
        envelope.to_dict(),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


__all__ = tuple(
    name
    for name in globals()
    if name.startswith(
        (
            "get_command",
            "reserve_",
            "command_",
            "abandon_",
            "mark_",
            "record_",
            "finish_",
            "recover_",
            "linked_",
            "settle_",
            "envelope_",
        )
    )
)
