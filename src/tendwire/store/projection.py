"""Snapshots, attention, route bindings, and projection health."""

from __future__ import annotations

import hashlib
import json
import re
import secrets
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Literal, Mapping

from ..core.models import Snapshot, Worker, WorkerBinding, separate_duplicate_worker_bindings
from ..worker_identity import is_stable_worker_key
from .db import canonical_utc, read_transaction, utc_now, write_transaction
from .schema import init_store

_ROUTE_PREFIX = "twroute1."
_ROUTE_RETAIN_DAYS = 45
_ROUTE_RE = re.compile(r"^twroute1\.[A-Za-z0-9_-]{43}$")


@dataclass(frozen=True)
class SnapshotObservationContext:
    authority: Literal["none", "positive", "complete"] = "none"
    observed_at: str | None = None


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _binding_dict(binding: WorkerBinding) -> dict[str, Any]:
    return {
        "host_id": binding.host_id, "worker_id": binding.worker_id,
        "worker_fingerprint": binding.worker_fingerprint, "backend": binding.backend,
        "target_kind": binding.target_kind, "target_value": binding.target_value,
        "turn_target_kind": binding.turn_target_kind, "turn_target_value": binding.turn_target_value,
        "sendable": binding.sendable, "reason": binding.reason,
        "observed_at": binding.observed_at, "expires_at": binding.expires_at,
        "private_fingerprint": binding.private_fingerprint,
    }


def _partition(host_id: str, stable_key: str, generation: str) -> str:
    material = _json({
        "domain": "tendwire.turn-final.partition.v1", "host_id": host_id,
        "route_generation": generation, "stable_key": stable_key,
        "stable_key_version": 1,
    })
    return "twpart1_" + hashlib.sha256(material.encode()).hexdigest()


def _route(
    conn: Any,
    binding: WorkerBinding,
    stable_key: str,
    now: str,
) -> tuple[str, str, str | None]:
    if not is_stable_worker_key(stable_key):
        raise ValueError("worker binding requires exact stable key")
    rows = conn.execute(
        """SELECT worker_id,route_generation,partition_key FROM worker_bindings
        WHERE host_id=? AND stable_key_version=1 AND stable_key=? AND backend=?
          AND private_fingerprint=?""",
        (binding.host_id, stable_key, binding.backend, binding.private_fingerprint),
    ).fetchall()
    if len(rows) > 1:
        raise ValueError("ambiguous stable route")
    if rows:
        generation = str(rows[0][1])
        partition = str(rows[0][2])
        if not _ROUTE_RE.fullmatch(generation):
            raise ValueError("stored route generation is malformed")
        if partition != _partition(binding.host_id, stable_key, generation):
            raise ValueError("stored route partition is malformed")
        return generation, partition, str(rows[0][0])
    collisions = conn.execute(
        """SELECT worker_id FROM worker_bindings
        WHERE host_id=? AND stable_key_version=1 AND stable_key=?
          AND worker_id<>? AND (expires_at IS NULL OR expires_at>?)""",
        (binding.host_id, stable_key, binding.worker_id, now),
    ).fetchall()
    if len(collisions) > 1:
        raise ValueError("multiple live routes claim one stable identity")
    generation = _ROUTE_PREFIX + secrets.token_urlsafe(32)
    if not _ROUTE_RE.fullmatch(generation):
        raise RuntimeError("route generation allocation failed")
    prior_worker_id = str(collisions[0][0]) if collisions else None
    return generation, _partition(binding.host_id, stable_key, generation), prior_worker_id


def _upsert_binding(conn: Any, binding: WorkerBinding, stable_key: str) -> tuple[str, str]:
    observed_at = canonical_utc(binding.observed_at) if binding.observed_at else utc_now()
    actual = utc_now()
    generation, partition, existing_worker_id = _route(
        conn,
        binding,
        stable_key,
        actual,
    )
    retain = canonical_utc(
        datetime.fromisoformat(actual.replace("Z", "+00:00"))
        + timedelta(days=_ROUTE_RETAIN_DAYS)
    )
    expires_at = canonical_utc(binding.expires_at) if binding.expires_at else None
    if expires_at is None or expires_at > actual:
        superseded = conn.execute(
            """SELECT * FROM worker_bindings
            WHERE host_id=? AND worker_id=? AND backend=? AND route_generation<>?
              AND observed_at<=? AND (expires_at IS NULL OR expires_at>?)""",
            (
                binding.host_id,
                binding.worker_id,
                binding.backend,
                generation,
                observed_at,
                actual,
            ),
        ).fetchall()
        for row in superseded:
            _expire_binding_row(
                conn,
                row,
                now=actual,
                observed_before=observed_at,
                reason="superseded_route",
            )
    if existing_worker_id is not None and existing_worker_id != binding.worker_id:
        occupied = conn.execute(
            """SELECT 1 FROM worker_bindings
            WHERE host_id=? AND worker_id=? AND route_generation=?""",
            (binding.host_id, binding.worker_id, generation),
        ).fetchone()
        if occupied is not None:
            raise ValueError("worker binding identity collision")
        conn.execute(
            """UPDATE worker_bindings SET worker_id=?
            WHERE host_id=? AND worker_id=? AND route_generation=?""",
            (binding.worker_id, binding.host_id, existing_worker_id, generation),
        )
    conn.execute(
        """INSERT INTO worker_bindings(
        host_id,worker_id,backend,private_fingerprint,private_binding_json,
        stable_key,stable_key_version,route_generation,partition_key,
        next_partition_sequence,route_retain_until,observed_at,expires_at)
        VALUES(?,?,?,?,?,?,1,?,?,1,?,?,?)
        ON CONFLICT(host_id,worker_id,route_generation) DO UPDATE SET
        backend=excluded.backend,private_fingerprint=excluded.private_fingerprint,
        private_binding_json=excluded.private_binding_json,stable_key=excluded.stable_key,
        partition_key=excluded.partition_key,
        route_retain_until=excluded.route_retain_until,observed_at=excluded.observed_at,
        expires_at=excluded.expires_at
        WHERE excluded.observed_at>=worker_bindings.observed_at""",
        (binding.host_id, binding.worker_id, binding.backend, binding.private_fingerprint,
         _json(_binding_dict(binding)), stable_key, generation, partition, retain, observed_at, expires_at),
    )
    return generation, partition


def _enrich(snapshot: Snapshot, bindings: tuple[WorkerBinding, ...], conn: Any) -> Snapshot:
    by_worker = {binding.worker_id: binding for binding in bindings}
    stable_keys = [
        worker.meta.get("stable_key")
        for worker in snapshot.workers
        if worker.id in by_worker
    ]
    if len(stable_keys) != len(set(stable_keys)):
        raise ValueError("multiple observed workers claim one stable identity")
    workers: list[Worker] = []
    for worker in snapshot.workers:
        binding = by_worker.get(worker.id)
        if binding is None:
            workers.append(worker)
            continue
        stable_key = worker.meta.get("stable_key")
        stable_version = worker.meta.get("stable_key_version")
        if stable_version != 1 or not isinstance(stable_key, str):
            raise ValueError("snapshot worker lacks stable identity")
        generation, _partition_key = _upsert_binding(conn, binding, stable_key)
        meta = dict(worker.meta)
        meta.update(
            {
                "stable_key": stable_key,
                "stable_key_version": 1,
                "route_generation": generation,
            }
        )
        enriched_worker = replace(worker, meta=meta, fingerprint="")
        _upsert_binding(
            conn,
            replace(binding, worker_fingerprint=enriched_worker.fingerprint),
            stable_key,
        )
        workers.append(enriched_worker)
    return Snapshot(
        host_id=snapshot.host_id, updated_at=snapshot.updated_at, spaces=snapshot.spaces,
        workers=workers, attention=snapshot.attention, backend_health=snapshot.backend_health,
    )


def save_snapshot(
    db_path: Path, snapshot: Snapshot, *,
    observation: SnapshotObservationContext | None = None,
    worker_bindings: Iterable[WorkerBinding] | None = None,
    binding_backend: str | None = None,
    binding_observation_authoritative: bool = False,
    binding_workers_present: bool = True,
) -> Snapshot:
    bindings = tuple(separate_duplicate_worker_bindings(worker_bindings or ()))
    if bindings and (not binding_backend or any(b.host_id != snapshot.host_id or b.backend != binding_backend for b in bindings)):
        raise ValueError("snapshot binding scope mismatch")
    if not db_path.exists():
        init_store(db_path)
    with write_transaction(db_path) as conn:
        observed_at = canonical_utc(snapshot.updated_at)
        actual = utc_now()
        authority_fingerprint = snapshot.content_fingerprint
        current = conn.execute(
            """SELECT observed_at,authority_fingerprint,payload_json FROM snapshots
            WHERE host_id=? ORDER BY observed_at DESC,authority_fingerprint DESC LIMIT 1""",
            (snapshot.host_id,),
        ).fetchone()
        incoming_authority = (observed_at, authority_fingerprint)
        if current is not None:
            current_authority = (
                current["observed_at"], current["authority_fingerprint"]
            )
            if incoming_authority < current_authority:
                return Snapshot.from_dict(json.loads(current["payload_json"]))
            if incoming_authority == current_authority:
                # The same public observation may arrive once through Herdr
                # continuity and once with its derived ACP route. Persist the
                # private binding without reapplying public projections.
                current_snapshot = Snapshot.from_dict(json.loads(current["payload_json"]))
                current_workers = {worker.id: worker for worker in current_snapshot.workers}
                incoming_workers = {worker.id: worker for worker in snapshot.workers}
                for binding in bindings:
                    worker = current_workers.get(binding.worker_id)
                    incoming_worker = incoming_workers.get(binding.worker_id)
                    stable_key = None if worker is None else worker.meta.get("stable_key")
                    if (
                        worker is None
                        or incoming_worker is None
                        or binding.worker_fingerprint != incoming_worker.fingerprint
                        or worker.meta.get("stable_key_version") != 1
                        or not isinstance(stable_key, str)
                    ):
                        raise ValueError("replayed binding does not match current worker authority")
                    _upsert_binding(
                        conn,
                        replace(binding, worker_fingerprint=worker.fingerprint),
                        stable_key,
                    )
                return current_snapshot
        enriched = _enrich(snapshot, bindings, conn)
        payload = enriched.to_dict()
        conn.execute(
            """INSERT INTO snapshots(host_id,observed_at,authority_fingerprint,content_fingerprint,payload_json)
            VALUES(?,?,?,?,?) ON CONFLICT(host_id,content_fingerprint) DO UPDATE SET
            observed_at=excluded.observed_at,authority_fingerprint=excluded.authority_fingerprint,
            payload_json=excluded.payload_json""",
            (enriched.host_id, observed_at, authority_fingerprint,
             enriched.content_fingerprint, _json(payload)),
        )
        conn.execute("DELETE FROM attention_items WHERE host_id=?", (enriched.host_id,))
        for item in payload.get("attention", []):
            conn.execute(
                "INSERT INTO attention_items(host_id,attention_id,payload_json,state,observed_at) VALUES(?,?,?,?,?)",
                (enriched.host_id, str(item.get("id")), _json(item), "open", observed_at),
            )
        for health in enriched.backend_health:
            conn.execute(
                """INSERT INTO backend_health(host_id,backend,payload_json,observed_at) VALUES(?,?,?,?)
                ON CONFLICT(host_id,backend) DO UPDATE SET payload_json=excluded.payload_json,observed_at=excluded.observed_at""",
                (enriched.host_id, health.name, _json(health.to_dict()), observed_at),
            )
        if binding_observation_authoritative and (bindings or not binding_workers_present):
            keep = {b.private_fingerprint for b in bindings}
            rows = conn.execute(
                """SELECT * FROM worker_bindings
                WHERE host_id=? AND backend=? AND observed_at<=?
                  AND (expires_at IS NULL OR expires_at>?)""",
                (
                    enriched.host_id,
                    binding_backend,
                    observed_at,
                    actual,
                ),
            ).fetchall()
            for row in rows:
                if str(row["private_fingerprint"]) not in keep:
                    _expire_binding_row(
                        conn,
                        row,
                        now=actual,
                        observed_before=observed_at,
                        reason="stale_observation",
                    )
    return enriched


def latest_snapshot(db_path: Path, host_id: str | None = None) -> Snapshot | None:
    try:
        with read_transaction(db_path) as conn:
            if host_id is None:
                row = conn.execute("SELECT payload_json FROM snapshots ORDER BY observed_at DESC,authority_fingerprint DESC LIMIT 1").fetchone()
            else:
                row = conn.execute("SELECT payload_json FROM snapshots WHERE host_id=? ORDER BY observed_at DESC,authority_fingerprint DESC LIMIT 1", (host_id,)).fetchone()
    except Exception:
        return None
    return None if row is None else Snapshot.from_dict(json.loads(row[0]))


def attention_payload_from_store(db_path: Path, host_id: str, *, include_resolved: bool = False) -> dict[str, Any] | None:
    snapshot = latest_snapshot(db_path, host_id)
    if snapshot is None:
        return None
    with read_transaction(db_path) as conn:
        rows = conn.execute("SELECT payload_json FROM attention_items WHERE host_id=? AND (? OR state='open') ORDER BY observed_at DESC,attention_id", (host_id, int(include_resolved))).fetchall()
    return {"schema_version": 1, "host_id": host_id, "updated_at": snapshot.updated_at, "attention": [json.loads(row[0]) for row in rows], "backend_health": [h.to_dict() for h in snapshot.backend_health]}


def upsert_worker_bindings(db_path: Path, bindings: Iterable[WorkerBinding]) -> int:
    values = tuple(separate_duplicate_worker_bindings(bindings))
    with write_transaction(db_path) as conn:
        for binding in values:
            prior = conn.execute(
                """SELECT stable_key FROM worker_bindings
                WHERE host_id=? AND backend=? AND private_fingerprint=?""",
                (
                    binding.host_id,
                    binding.backend,
                    binding.private_fingerprint,
                ),
            ).fetchone()
            stable_key = str(prior[0]) if prior is not None else None
            if stable_key is None:
                row = conn.execute(
                    """SELECT payload_json FROM snapshots
                    WHERE host_id=? ORDER BY id DESC LIMIT 1""",
                    (binding.host_id,),
                ).fetchone()
                snapshot = Snapshot.from_dict(json.loads(row[0])) if row else None
                worker = (
                    next(
                        (w for w in snapshot.workers if w.id == binding.worker_id),
                        None,
                    )
                    if snapshot
                    else None
                )
                stable_key = worker.meta.get("stable_key") if worker else None
            if not isinstance(stable_key, str):
                raise ValueError("binding has no authoritative stable identity")
            _upsert_binding(conn, binding, stable_key)
    return len(values)


def _binding(row: Any) -> WorkerBinding:
    binding = WorkerBinding(**json.loads(row["private_binding_json"]))
    return replace(binding, worker_id=str(row["worker_id"]))


def list_worker_bindings(db_path: Path, host_id: str, *, backend: str | None = None, include_expired: bool = False, now: str | None = None) -> list[WorkerBinding]:
    clauses, values = ["host_id=?"], [host_id]
    if backend is not None:
        clauses.append("backend=?"); values.append(backend)
    if not include_expired:
        clauses.append("(expires_at IS NULL OR expires_at>?)")
        values.append(canonical_utc(now) if now else utc_now())
    with read_transaction(db_path) as conn:
        rows = conn.execute(f"SELECT * FROM worker_bindings WHERE {' AND '.join(clauses)} ORDER BY worker_id", values).fetchall()
    return [_binding(row) for row in rows]


def _expire_binding_row(
    conn: Any,
    row: Any,
    *,
    now: str,
    observed_before: str | None = None,
    reason: str,
) -> int:
    observation_cutoff = observed_before or now
    binding = replace(
        _binding(row),
        sendable=False,
        reason=reason,
        expires_at=now,
    )
    cursor = conn.execute(
        """UPDATE worker_bindings
        SET private_binding_json=?,expires_at=?
        WHERE host_id=? AND worker_id=? AND route_generation=?
          AND observed_at<=? AND (expires_at IS NULL OR expires_at>?)""",
        (
            _json(_binding_dict(binding)),
            now,
            str(row["host_id"]),
            str(row["worker_id"]),
            str(row["route_generation"]),
            observation_cutoff,
            now,
        ),
    )
    return int(cursor.rowcount or 0)


def expire_worker_bindings(
    db_path: Path,
    host_id: str,
    *,
    backend: str,
    worker_id: str | None = None,
    private_fingerprints: Iterable[str] | None = None,
    now: str | None = None,
    observed_before: str | None = None,
    reason: str = "expired",
) -> int:
    current = canonical_utc(now) if now else utc_now()
    observation_cutoff = (
        canonical_utc(observed_before) if observed_before else current
    )
    fingerprints = {str(value) for value in (private_fingerprints or ())}
    with write_transaction(db_path) as conn:
        clauses = [
            "host_id=?",
            "backend=?",
            "observed_at<=?",
            "(expires_at IS NULL OR expires_at>?)",
        ]
        values: list[Any] = [host_id, backend, observation_cutoff, current]
        if worker_id is not None:
            clauses.append("worker_id=?")
            values.append(worker_id)
        if fingerprints:
            placeholders = ",".join("?" for _ in fingerprints)
            clauses.append(f"private_fingerprint IN ({placeholders})")
            values.extend(sorted(fingerprints))
        rows = conn.execute(
            f"SELECT * FROM worker_bindings WHERE {' AND '.join(clauses)}",
            values,
        ).fetchall()
        return sum(
            _expire_binding_row(
                conn,
                row,
                now=current,
                observed_before=observation_cutoff,
                reason=reason,
            )
            for row in rows
        )


def backend_pending_health(db_path: Path, host_id: str) -> dict[str, Any]:
    try:
        with read_transaction(db_path) as conn:
            total = int(conn.execute("SELECT COUNT(*) FROM backend_pending WHERE host_id=? AND state='open'", (host_id,)).fetchone()[0])
    except Exception:
        return {"status": "store_unavailable", "counts": {"fresh": 0, "stale": 0, "total": 0}}
    return {"status": "healthy", "counts": {"fresh": total, "stale": 0, "total": total}}


__all__ = ("SnapshotObservationContext", "save_snapshot", "latest_snapshot", "attention_payload_from_store", "upsert_worker_bindings", "list_worker_bindings", "expire_worker_bindings", "backend_pending_health")
