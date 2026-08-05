"""Pending observations and fenced decision claims."""

from __future__ import annotations

import base64
import hashlib
import json
import secrets
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Literal, Mapping

from ..core.models import sanitize_canonical_turn_text, stable_fingerprint
from ..core.turns import (
    InteractionChoice,
    PendingInteraction,
    PendingObservation,
    TURN_SCHEMA_VERSION,
)
from .db import add_seconds, canonical_utc, read_transaction, utc_now, write_transaction


@dataclass(frozen=True)
class BackendPendingDecisionClaim:
    status: str
    claim_token: str | None = None
    worker_id: str | None = None
    worker_fingerprint: str | None = None
    binding_private_fingerprint: str | None = None
    turn_target_value: str | None = None
    decision_ref: str | None = None
    decision_kind: Literal["single", "multi", "plan"] | None = None
    option_count: int | None = None
    option_refs: tuple[str, ...] = ()
    text: str | None = None


@dataclass(frozen=True)
class BackendPendingDecisionSend:
    status: str
    worker_id: str | None = None
    worker_fingerprint: str | None = None
    binding_private_fingerprint: str | None = None
    turn_target_value: str | None = None
    decision_ref: str | None = None
    decision_kind: Literal["single", "multi", "plan"] | None = None
    option_count: int | None = None
    option_refs: tuple[str, ...] = ()
    text: str | None = None


def _json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def backend_pending_decision_ref(
    host_id: str, worker_id: str, revision_digest: str,
) -> str:
    material = _json([host_id, worker_id, revision_digest]).encode("utf-8")
    return "pending-" + hashlib.sha256(material).hexdigest()[:24]


def _opaque_digest(prefix: str, domain: str, value: Mapping[str, Any]) -> str:
    encoded = _json({"domain": domain, **value}).encode("utf-8")
    digest = base64.urlsafe_b64encode(hashlib.sha256(encoded).digest())
    return prefix + digest.rstrip(b"=").decode("ascii")


def _decision_coordinates(
    host_id: str,
    decision_ref: str,
    revision_digest: str,
) -> tuple[str, str, str]:
    material = {
        "host_id": host_id,
        "decision_ref": decision_ref,
        "revision_digest": revision_digest,
    }
    turn_suffix = hashlib.sha256(_json(material).encode("utf-8")).hexdigest()[:24]
    content_revision = _opaque_digest(
        "twrev1.",
        "tendwire.decision-content.v1",
        material,
    )
    final_identity = _opaque_digest(
        "twfinal1.",
        "tendwire.decision-final.v1",
        {**material, "content_revision": content_revision},
    )
    return f"turn-decision-{turn_suffix}", final_identity, content_revision


def _binding_route(
    conn: Any, host_id: str, worker_id: str, private_fingerprint: str | None = None,
) -> Any:
    rows = conn.execute(
        """SELECT * FROM worker_bindings
        WHERE host_id=? AND worker_id=? AND (expires_at IS NULL OR expires_at>?)
          AND (? IS NULL OR private_fingerprint=?)""",
        (host_id, worker_id, utc_now(), private_fingerprint, private_fingerprint),
    ).fetchall()
    return rows[0] if len(rows) == 1 else None


def _public_worker(conn: Any, host_id: str, worker_id: str, binding: Any) -> dict[str, Any]:
    row = conn.execute(
        """SELECT payload_json FROM snapshots
        WHERE host_id=? ORDER BY id DESC LIMIT 1""",
        (host_id,),
    ).fetchone()
    if row is None:
        raise RuntimeError("snapshot route unavailable")
    snapshot = json.loads(row[0])
    worker = next(
        (item for item in snapshot.get("workers", []) if item.get("id") == worker_id),
        None,
    )
    if not isinstance(worker, Mapping):
        raise RuntimeError("worker projection unavailable")
    return {
        "worker_id": worker_id,
        "stable_key": binding["stable_key"],
        "stable_key_version": 1,
        "route_generation": binding["route_generation"],
    }


def _allocate_sequence(conn: Any, host_id: str, partition_key: str) -> int:
    row = conn.execute(
        """UPDATE worker_bindings
        SET next_partition_sequence=next_partition_sequence+1
        WHERE host_id=? AND partition_key=?
        RETURNING next_partition_sequence-1""",
        (host_id, partition_key),
    ).fetchone()
    if row is None or int(row[0]) < 1:
        raise RuntimeError("route allocator unavailable")
    return int(row[0])


def _enqueue_decision(
    conn: Any,
    host_id: str,
    worker_id: str,
    decision_ref: str,
    revision_digest: str,
    observation: PendingObservation,
    now: str,
    *,
    binding: Any = None,
) -> None:
    if observation.decision_kind is None:
        return
    binding = binding if binding is not None else _binding_route(conn, host_id, worker_id)
    if binding is None:
        raise RuntimeError("decision route unavailable")
    worker = _public_worker(conn, host_id, worker_id, binding)
    key_token = _opaque_digest(
        "twdecision1.",
        "tendwire.decision.v1",
        {
            "host_id": host_id,
            "decision_ref": decision_ref,
            "revision_digest": revision_digest,
            "route_generation": binding["route_generation"],
        },
    )
    key = f"turn-final:decision:{key_token}"
    turn_id, final_identity, content_revision = _decision_coordinates(
        host_id,
        decision_ref,
        revision_digest,
    )
    public = _public(observation)
    choices = [
        {"ordinal": index, "option_ref": str(index + 1), "label": label}
        for index, label in enumerate(public["decision_options"])
    ]
    existing = conn.execute(
        """SELECT * FROM connector_outbox
        WHERE host_id=? AND connector='turn-final' AND key=?""",
        (host_id, key),
    ).fetchone()
    if existing is not None:
        replay_payload = json.loads(existing["payload_json"])
        replay_decision = replay_payload.get("decision")
        expected_decision = {
            "decision_ref": decision_ref,
            "revision_digest": revision_digest,
            "mode": observation.decision_kind,
            "title": public["kind"] or "Decision",
            "body": public["question"] or "",
            "choices": choices,
        }
        if replay_decision != expected_decision:
            raise RuntimeError("decision producer identity conflict")
        return
    sequence = _allocate_sequence(conn, host_id, str(binding["partition_key"]))
    payload = {
        "schema_version": 1,
        "kind": "decision",
        "created_at": now,
        "worker": worker,
        "route": {
            "partition_key": binding["partition_key"],
            "partition_sequence": sequence,
        },
        "decision": {
            "decision_ref": decision_ref,
            "revision_digest": revision_digest,
            "mode": observation.decision_kind,
            "title": public["kind"] or "Decision",
            "body": public["question"] or "",
            "choices": choices,
        },
    }
    conn.execute(
        """UPDATE connector_outbox
        SET status='superseded',updated_at=?
        WHERE host_id=? AND connector='turn-final' AND kind='decision'
          AND decision_ref=? AND status IN('queued','retry','deferred')
          AND NOT EXISTS(
            SELECT 1 FROM connector_deliveries d
            WHERE d.outbox_id=connector_outbox.id
          )""",
        (now, host_id, decision_ref),
    )
    conn.execute(
        """UPDATE connector_outbox
        SET terminal_after_lease=1,updated_at=?
        WHERE host_id=? AND connector='turn-final' AND kind='decision'
          AND decision_ref=? AND status='leased'""",
        (now, host_id, decision_ref),
    )
    conn.execute(
        """INSERT INTO connector_outbox(
        host_id,connector,key,kind,payload_version,status,partition_key,
        partition_sequence,turn_id,final_identity,decision_ref,content_revision,
        retry_generation,prior_attempt_count,payload_json,created_at,updated_at,available_at)
        VALUES(?,'turn-final',?,'decision',1,'queued',?,?,?,?,?,?,1,0,?,?,?,?)""",
        (
            host_id,
            key,
            binding["partition_key"],
            sequence,
            turn_id,
            final_identity,
            decision_ref,
            content_revision,
            _json(payload),
            now,
            now,
            now,
        ),
    )


def _decision_retire_payload(
    target: Any,
    target_payload: Mapping[str, Any],
    sequence: int,
    now: str,
) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "kind": "retire",
        "created_at": now,
        "worker": dict(target_payload["worker"]),
        "route": {
            "partition_key": target["partition_key"],
            "partition_sequence": sequence,
        },
        "turn": {
            "turn_id": target["turn_id"],
            "final_identity": target["final_identity"],
            "content_revision": target["content_revision"],
        },
        "retire": {
            "target_key": target["key"],
            "target_kind": "decision",
            "target_ordinal": None,
            "predecessor_key": target["key"],
            "plan_token": None,
            "generation": None,
            "reason": "decision_resolved",
        },
    }


def _resolve_decision_rows(
    conn: Any,
    host_id: str,
    worker_id: str,
    decision_ref: str,
    resolving_revision: str,
    now: str,
) -> None:
    rows = conn.execute(
        """SELECT * FROM connector_outbox
        WHERE host_id=? AND connector='turn-final' AND kind='decision'
          AND decision_ref=? ORDER BY id""",
        (host_id, decision_ref),
    ).fetchall()
    for target in rows:
        target_payload = json.loads(target["payload_json"])
        target_worker = target_payload.get("worker")
        if not isinstance(target_worker, Mapping):
            raise RuntimeError("decision target payload is malformed")
        binding = conn.execute(
            """SELECT * FROM worker_bindings
            WHERE host_id=? AND partition_key=? AND stable_key=?
              AND route_generation=?""",
            (
                host_id,
                target["partition_key"],
                target_worker.get("stable_key"),
                target_worker.get("route_generation"),
            ),
        ).fetchone()
        if binding is None:
            raise RuntimeError("decision target route is unavailable")
        attempts = int(
            conn.execute(
                "SELECT COUNT(*) FROM connector_deliveries WHERE outbox_id=?",
                (target["id"],),
            ).fetchone()[0]
        )
        if attempts == 0:
            if target["status"] in {"queued", "retry", "deferred"}:
                conn.execute(
                    """UPDATE connector_outbox SET status='superseded',updated_at=?
                    WHERE id=? AND status IN('queued','retry','deferred')""",
                    (now, target["id"]),
                )
            continue
        if target["status"] == "leased":
            conn.execute(
                """UPDATE connector_outbox
                SET terminal_after_lease=1,updated_at=? WHERE id=? AND status='leased'""",
                (now, target["id"]),
            )
        token = _opaque_digest(
            "twretire1.",
            "tendwire.decision-retire.v1",
            {
                "target_key": target["key"],
                "reason": "decision_resolved",
                "resolving_revision": resolving_revision,
                "route_generation": binding["route_generation"],
            },
        )
        key = f"turn-final:retire:{token}"
        existing = conn.execute(
            """SELECT payload_json FROM connector_outbox
            WHERE host_id=? AND connector='turn-final' AND key=?""",
            (host_id, key),
        ).fetchone()
        if existing is not None:
            continue
        sequence = _allocate_sequence(conn, host_id, str(binding["partition_key"]))
        payload = _decision_retire_payload(target, target_payload, sequence, now)
        conn.execute(
            """INSERT INTO connector_outbox(
            host_id,connector,key,kind,payload_version,status,partition_key,
            partition_sequence,turn_id,final_identity,decision_ref,content_revision,
            predecessor_outbox_id,target_outbox_id,retry_generation,
            prior_attempt_count,payload_json,created_at,updated_at,available_at)
            VALUES(?,'turn-final',?,'retire',1,'blocked',?,?,?,?,?,?,?,?,1,0,?,?,?,?)""",
            (
                host_id,
                key,
                binding["partition_key"],
                sequence,
                target["turn_id"],
                target["final_identity"],
                resolving_revision,
                target["content_revision"],
                target["id"],
                target["id"],
                _json(payload),
                now,
                now,
                now,
            ),
        )


def _public(
    observation: PendingObservation, decision_ref: str | None = None,
) -> dict[str, Any]:
    choices = []
    for index, choice in enumerate(observation.choices):
        choices.append(
            {
                "choice_id": choice.choice_id,
                "label": sanitize_canonical_turn_text(choice.label) or f"Option {index + 1}",
                "ordinal": choice.picker_ordinal,
            }
        )
    public = {
        "question": sanitize_canonical_turn_text(observation.question),
        "kind": sanitize_canonical_turn_text(observation.pending_kind) or "question",
        "choices": choices,
        "decision_kind": observation.decision_kind,
        "decision_options": [
            sanitize_canonical_turn_text(option) or f"Option {index + 1}"
            for index, option in enumerate(observation.decision_options)
        ],
    }
    if observation.decision_kind is not None and decision_ref is not None:
        public["meta"] = {
            "source": "backend",
            "decision": {
                "decision_ref": decision_ref,
                "kind": observation.decision_kind,
                "prompt": public["question"],
                "options": [
                    {"ref": str(index + 1), "label": label}
                    for index, label in enumerate(public["decision_options"])
                ],
                "multi_select": observation.decision_multi_select,
                "question_count": observation.decision_question_count,
            },
        }
    return public


def apply_backend_pending_observation(
    db_path: Path | str,
    host_id: str,
    worker_id: str,
    observation: PendingObservation,
    *,
    observed_at: str | None = None,
    stale_grace_seconds: float = 30.0,
    binding_private_fingerprint: str | None = None,
    observed_turn_target_value: str | None = None,
    binding_authoritative: bool = False,
) -> bool:
    now = canonical_utc(observed_at) if observed_at else utc_now()
    with write_transaction(db_path) as conn:
        existing_rows = conn.execute(
            """SELECT decision_ref,revision_digest,state,observed_at FROM backend_pending
            WHERE host_id=? AND worker_id=? ORDER BY observed_at DESC,decision_ref DESC""",
            (host_id, worker_id),
        ).fetchall()
        existing = existing_rows[0] if existing_rows else None
        if existing is not None and now < existing["observed_at"]:
            return False
        if observation.kind != "open_prompt":
            if not existing_rows:
                return False
            for row in existing_rows:
                if row["state"] != "open":
                    continue
                _resolve_decision_rows(
                    conn,
                    host_id,
                    worker_id,
                    str(row["decision_ref"]),
                    str(row["revision_digest"]),
                    now,
                )
            conn.execute(
                """UPDATE backend_pending SET state='closed',observed_at=?
                WHERE host_id=? AND worker_id=? AND state='open'""",
                (now, host_id, worker_id),
            )
            conn.execute(
                """UPDATE pending_interactions SET status='closed',observed_at=?
                WHERE host_id=? AND worker_id=? AND status='open'""",
                (now, host_id, worker_id),
            )
            return True
        revision = stable_fingerprint(
            {
                "decision_revision": str(observation.revision_digest),
                "binding_private_fingerprint": binding_private_fingerprint or "",
                "observed_turn_target_value": observed_turn_target_value or "",
            }
        )
        ref = backend_pending_decision_ref(host_id, worker_id, revision)
        for prior in existing_rows:
            if prior["decision_ref"] == ref or prior["state"] != "open":
                continue
            _resolve_decision_rows(
                conn,
                host_id,
                worker_id,
                str(prior["decision_ref"]),
                revision,
                now,
            )
        private = {
            "public": _public(observation, ref),
            "binding_private_fingerprint": binding_private_fingerprint or "",
            "turn_target_value": observed_turn_target_value or "",
            "choice_routes": {c.choice_id: c.picker_ordinal for c in observation.choices},
        }
        route = _binding_route(
            conn,
            host_id,
            worker_id,
            binding_private_fingerprint or None,
        )
        if route is None:
            return False
        conn.execute(
            """DELETE FROM backend_pending_claims
            WHERE host_id=? AND decision_ref IN(
              SELECT decision_ref FROM backend_pending
              WHERE host_id=? AND worker_id=? AND decision_ref<>?
            ) AND state='claimed' AND claimed_until<=?""",
            (host_id, host_id, worker_id, ref, now),
        )
        conn.execute(
            """UPDATE backend_pending SET state='closed',observed_at=?
            WHERE host_id=? AND worker_id=? AND decision_ref<>?""",
            (now, host_id, worker_id, ref),
        )
        conn.execute(
            """DELETE FROM backend_pending
            WHERE host_id=? AND worker_id=? AND decision_ref<>?
              AND NOT EXISTS(
                SELECT 1 FROM backend_pending_claims c
                WHERE c.host_id=backend_pending.host_id
                  AND c.decision_ref=backend_pending.decision_ref
              )""",
            (host_id, worker_id, ref),
        )
        conn.execute(
            """INSERT INTO backend_pending(
            host_id,decision_ref,revision_digest,worker_id,route_generation,
            private_payload_json,state,observed_at)
            VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(host_id,decision_ref) DO UPDATE SET
            private_payload_json=excluded.private_payload_json,state='open',
            observed_at=excluded.observed_at""",
            (
                host_id,
                ref,
                revision,
                worker_id,
                route["route_generation"],
                _json(private),
                "open",
                now,
            ),
        )
        public_payload = _public(observation, ref)
        conn.execute(
            """INSERT INTO pending_interactions(
            host_id,decision_ref,revision_digest,worker_id,route_generation,
            payload_json,status,observed_at)
            VALUES(?,?,?,?,?,?,'open',?)
            ON CONFLICT(host_id,decision_ref) DO UPDATE SET
            payload_json=excluded.payload_json,status='open',observed_at=excluded.observed_at""",
            (
                host_id,
                ref,
                revision,
                worker_id,
                route["route_generation"],
                _json(public_payload),
                now,
            ),
        )
        _enqueue_decision(
            conn,
            host_id,
            worker_id,
            ref,
            revision,
            observation,
            now,
            binding=route,
        )
        return (
            existing is None
            or str(existing["revision_digest"]) != revision
            or str(existing["state"]) != "open"
        )


def pending_payload_from_store(db_path: Path | str, host_id: str) -> dict[str, Any]:
    try:
        with read_transaction(db_path) as conn:
            snapshot = conn.execute(
                """SELECT payload_json FROM snapshots
                WHERE host_id=? ORDER BY id DESC LIMIT 1""",
                (host_id,),
            ).fetchone()
            rows = conn.execute(
                """SELECT p.*,b.private_binding_json FROM backend_pending p
                JOIN worker_bindings b ON b.host_id=p.host_id AND b.worker_id=p.worker_id
                  AND b.route_generation=p.route_generation
                WHERE p.host_id=? AND p.state='open' ORDER BY p.worker_id""",
                (host_id,),
            ).fetchall()
            health = conn.execute(
                """SELECT payload_json FROM backend_health
                WHERE host_id=? ORDER BY backend""",
                (host_id,),
            ).fetchall()
    except Exception:
        return {
            "schema_version": TURN_SCHEMA_VERSION,
            "host_id": host_id,
            "ok": False,
            "status": "store_unavailable",
            "pending_interactions": [],
            "backend_health": [],
        }
    if snapshot is None:
        return {
            "schema_version": TURN_SCHEMA_VERSION,
            "host_id": host_id,
            "ok": False,
            "status": "store_unavailable",
            "pending_interactions": [],
            "backend_health": [],
        }
    interactions = []
    for row in rows:
        private = json.loads(row["private_payload_json"])
        public = private["public"]
        binding = json.loads(row["private_binding_json"])
        interactions.append(
            PendingInteraction(
                id=str(row["decision_ref"]),
                host_id=host_id,
                worker_id=str(row["worker_id"]),
                worker_fingerprint=binding.get("worker_fingerprint"),
                question=str(public.get("question") or "Action requires attention"),
                kind=str(public.get("kind") or "question"),
                choices=[
                    InteractionChoice.from_dict(choice)
                    for choice in public.get("choices", [])
                ],
                status="open",
                updated_at=str(row["observed_at"]),
                meta=dict(public.get("meta") or {}),
            ).to_dict()
        )
    backend_health = [json.loads(r[0]) for r in health]
    pending_health = {
        "status": "healthy",
        "counts": {"fresh": len(rows), "stale": 0, "total": len(rows)},
    }
    result = {
        "schema_version": TURN_SCHEMA_VERSION,
        "host_id": host_id,
        "ok": True,
        "status": "ok",
        "pending_interactions": interactions,
        "backend_health": backend_health,
        "pending_health": pending_health,
    }
    result["content_fingerprint"] = stable_fingerprint(
        {
            key: result[key]
            for key in (
                "schema_version",
                "host_id",
                "pending_interactions",
                "backend_health",
                "pending_health",
            )
        }
    )
    return result


def _claim_fields(
    row: Any,
    private: Mapping[str, Any],
    *,
    text: str | None = None,
    option_refs: tuple[str, ...] = (),
) -> dict[str, Any]:
    public = private["public"]
    binding = json.loads(row["private_binding_json"])
    return {
        "worker_id": row["worker_id"],
        "worker_fingerprint": binding["worker_fingerprint"],
        "binding_private_fingerprint": private["binding_private_fingerprint"],
        "turn_target_value": private["turn_target_value"],
        "decision_ref": row["decision_ref"],
        "decision_kind": public.get("decision_kind"),
        "option_count": len(public.get("decision_options") or ()),
        "option_refs": option_refs,
        "text": text,
    }


def claim_backend_pending_decision(
    db_path: Path | str,
    host_id: str,
    worker_id: str,
    decision_ref: str,
    selection: Mapping[str, Any],
    *,
    claim: bool = True,
    observed_at: str | None = None,
    claim_lease_seconds: float = 30.0,
) -> BackendPendingDecisionClaim:
    if isinstance(claim_lease_seconds, bool):
        raise ValueError("claim lease must be positive")
    lease_seconds = float(claim_lease_seconds)
    if not 0 < lease_seconds <= 86_400:
        raise ValueError("claim lease must be positive")
    now = canonical_utc(observed_at) if observed_at else utc_now()
    with write_transaction(db_path) as conn:
        row = conn.execute(
            """SELECT p.*,b.private_binding_json FROM backend_pending p
            JOIN worker_bindings b ON b.host_id=p.host_id AND b.worker_id=p.worker_id
              AND b.route_generation=p.route_generation
            WHERE p.host_id=? AND p.worker_id=? AND p.decision_ref=?
              AND p.state='open'""",
            (host_id, worker_id, decision_ref),
        ).fetchone()
        if row is None:
            return BackendPendingDecisionClaim("decision_not_pending")
        private = json.loads(row["private_payload_json"])
        options = private["public"].get("decision_options") or []
        refs = tuple(str(item) for item in selection.get("option_refs", ()))
        text = selection.get("text") if isinstance(selection.get("text"), str) else None
        if refs and any(not ref.isdigit() or not 1 <= int(ref) <= len(options) for ref in refs):
            return BackendPendingDecisionClaim("invalid_selection")
        fields = _claim_fields(row, private, text=text, option_refs=refs)
        if not claim:
            return BackendPendingDecisionClaim("validated", **fields)
        token = secrets.token_urlsafe(32)
        digest = hashlib.sha256(token.encode()).hexdigest()
        existing_claim = conn.execute(
            """SELECT fence,state,claimed_until FROM backend_pending_claims
            WHERE host_id=? AND decision_ref=?""",
            (host_id, decision_ref),
        ).fetchone()
        if existing_claim is None:
            conn.execute(
                """INSERT INTO backend_pending_claims(
                host_id,decision_ref,claim_token_hash,fence,state,selection_json,claimed_until)
                VALUES(?,?,?,1,'claimed',?,?)""",
                (host_id, decision_ref, digest, _json(selection), add_seconds(now, lease_seconds)),
            )
        elif (
            existing_claim["state"] == "claimed"
            and existing_claim["claimed_until"] <= now
        ):
            changed = conn.execute(
                """UPDATE backend_pending_claims SET
                claim_token_hash=?,fence=fence+1,state='claimed',selection_json=?,
                claimed_until=?,send_started_at=NULL,settled_at=NULL
                WHERE host_id=? AND decision_ref=? AND state='claimed'
                  AND claimed_until<=?""",
                (digest, _json(selection), add_seconds(now, lease_seconds),
                 host_id, decision_ref, now),
            ).rowcount
            if changed != 1:
                return BackendPendingDecisionClaim("already_claimed")
        else:
            return BackendPendingDecisionClaim("already_claimed")
        return BackendPendingDecisionClaim("claimed", claim_token=token, **fields)


def start_backend_pending_decision_send(
    db_path: Path | str,
    host_id: str,
    claim_token: str,
    *,
    observed_at: str | None = None,
    **_: Any,
) -> BackendPendingDecisionSend:
    digest = hashlib.sha256(claim_token.encode()).hexdigest()
    now = canonical_utc(observed_at) if observed_at else utc_now()
    with write_transaction(db_path) as conn:
        row = conn.execute(
            """SELECT p.*,b.private_binding_json,
            c.state AS claim_state,c.selection_json,c.claimed_until
            FROM backend_pending_claims c
            JOIN backend_pending p USING(host_id,decision_ref)
            JOIN worker_bindings b ON b.host_id=p.host_id AND b.worker_id=p.worker_id
              AND b.route_generation=p.route_generation
            WHERE c.host_id=? AND c.claim_token_hash=?""",
            (host_id, digest),
        ).fetchone()
        if row is None:
            return BackendPendingDecisionSend("not_found")
        selection = json.loads(row["selection_json"] or "{}")
        selection_fields = {
            "text": selection.get("text") if isinstance(selection.get("text"), str) else None,
            "option_refs": tuple(str(item) for item in selection.get("option_refs", ())),
        }
        if row["claim_state"] == "send_started":
            return BackendPendingDecisionSend(
                "already_started",
                **_claim_fields(
                    row,
                    json.loads(row["private_payload_json"]),
                    **selection_fields,
                ),
            )
        changed = conn.execute(
            """UPDATE backend_pending_claims SET state='send_started',send_started_at=?
            WHERE host_id=? AND claim_token_hash=? AND state='claimed'
              AND claimed_until>?""",
            (now, host_id, digest, now),
        ).rowcount
        fields = _claim_fields(
            row,
            json.loads(row["private_payload_json"]),
            **selection_fields,
        )
        return BackendPendingDecisionSend("started" if changed else "changed", **fields)


def abandon_backend_pending_decision_claim(
    db_path: Path | str,
    host_id: str,
    claim_token: str,
) -> bool:
    digest = hashlib.sha256(claim_token.encode()).hexdigest()
    with write_transaction(db_path) as conn:
        return (
            conn.execute(
                """DELETE FROM backend_pending_claims
                WHERE host_id=? AND claim_token_hash=? AND state='claimed'""",
                (host_id, digest),
            ).rowcount
            == 1
        )


def _terminal(conn: Any, host_id: str, claim_token: str, accepted: bool) -> None:
    if not accepted:
        return
    digest = hashlib.sha256(claim_token.encode()).hexdigest()
    row = conn.execute(
        """SELECT p.decision_ref,p.worker_id,p.revision_digest
        FROM backend_pending_claims c
        JOIN backend_pending p USING(host_id,decision_ref)
        WHERE c.host_id=? AND c.claim_token_hash=? AND c.state='send_started'""",
        (host_id, digest),
    ).fetchone()
    if row is None:
        raise RuntimeError("pending claim is not send-started")
    now = utc_now()
    _resolve_decision_rows(
        conn,
        host_id,
        str(row["worker_id"]),
        str(row["decision_ref"]),
        str(row["revision_digest"]),
        now,
    )
    conn.execute(
        """UPDATE backend_pending SET state='resolved',observed_at=?
        WHERE host_id=? AND decision_ref=?""",
        (now, host_id, row["decision_ref"]),
    )
    conn.execute(
        """UPDATE pending_interactions SET status='resolved',observed_at=?
        WHERE host_id=? AND decision_ref=?""",
        (now, host_id, row["decision_ref"]),
    )
    conn.execute(
        """UPDATE backend_pending_claims SET state='settled',settled_at=?
        WHERE host_id=? AND claim_token_hash=?""",
        (now, host_id, digest),
    )


def backend_pending_decision_terminal_effect(
    *,
    host_id: str,
    claim_token: str,
    accepted: bool,
) -> Callable[[Any], None]:
    return lambda conn: _terminal(conn, host_id, claim_token, accepted)


__all__ = tuple(
    name
    for name in globals()
    if name.startswith(
        (
            "BackendPending",
            "apply_",
            "pending_",
            "claim_",
            "start_",
            "abandon_",
            "backend_",
        )
    )
)
