"""Pure deterministic attention helpers and rules.

This module performs no I/O and depends only on stdlib and sibling core models.
"""

from __future__ import annotations

import re
from typing import Any

from .models import AttentionSignal, Snapshot, Worker, normalize_status


_HUMAN_NEEDED_META_KEYS = frozenset(
    {
        "needs_human",
        "human_input_required",
        "requires_human",
        "approval_required",
        "requires_approval",
        "needs_input",
        "awaiting_input",
    }
)

_HUMAN_NEEDED_TEXT_PATTERNS = (
    re.compile(r"\b(?:requires?|needs?|await(?:s|ing)?)\s+(?:human|user|manual)\s+(?:input|review|approval)\b"),
    re.compile(r"\b(?:human|user|manual)\s+(?:input|review|approval)\s+(?:required|needed|requested)\b"),
    re.compile(r"\b(?:requires?|needs?|await(?:s|ing)?)\s+approval\b"),
    re.compile(r"\bapproval\s+(?:required|needed|requested)\b"),
    re.compile(r"\bmanual\s+review\s+(?:required|needed|requested)\b"),
)


def _normalized_meta_key(key: Any) -> str:
    return str(key).strip().lower().replace("-", "_")


def _truthy_signal(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, (int, float)):
        return value != 0
    text = str(value).strip().lower()
    return text not in {"", "0", "false", "no", "off", "none", "null"}


def _worker_needs_human(worker: Worker) -> bool:
    for key, value in worker.meta.items():
        if _normalized_meta_key(key) in _HUMAN_NEEDED_META_KEYS and _truthy_signal(value):
            return True

    summary = (worker.summary or "").strip().lower()
    return bool(summary) and any(pattern.search(summary) for pattern in _HUMAN_NEEDED_TEXT_PATTERNS)


def _worker_meta_value(worker: Worker, normalized_key: str) -> Any:
    for key, value in worker.meta.items():
        if _normalized_meta_key(key) == normalized_key:
            return value
    return None


def _worker_attention_updated_at(worker: Worker) -> Any | None:
    for value in (
        worker.last_seen_at,
        _worker_meta_value(worker, "updated_at"),
        _worker_meta_value(worker, "observed_at"),
        _worker_meta_value(worker, "timestamp"),
    ):
        if value is not None:
            return value
    return None


def _worker_attention_signal(
    worker: Worker,
    *,
    host_id: str,
    severity: str,
    reason: str,
    updated_at: Any | None = None,
) -> AttentionSignal:
    status = normalize_status(worker.status)
    meta = {
        "worker_id": worker.id,
    }
    if worker.space_id is not None:
        meta["space_id"] = worker.space_id
    needs_human = _worker_meta_value(worker, "needs_human")
    if needs_human is not None:
        meta["needs_human"] = needs_human
    elif severity in {"warning", "critical"}:
        meta["needs_human"] = True
    return AttentionSignal(
        kind="worker_status",
        severity=severity,
        status=status,
        reason=reason,
        source=f"worker:{worker.id}",
        updated_at=updated_at,
        meta=meta,
        host_id=host_id,
    )


def attention_for_worker(
    worker: Worker,
    *,
    host_id: str = "",
    updated_at: str | None = None,
) -> list[AttentionSignal]:
    """Return deterministic attention signals for a single worker."""
    status = normalize_status(worker.status)
    source_updated_at = _worker_attention_updated_at(worker)

    if status == "failed":
        return [
            _worker_attention_signal(
                worker,
                host_id=host_id,
                severity="critical",
                reason=f"Worker {worker.name!r} reports status {status!r}",
                updated_at=source_updated_at,
            )
        ]

    if status in {"blocked", "warning"}:
        return [
            _worker_attention_signal(
                worker,
                host_id=host_id,
                severity="warning",
                reason=f"Worker {worker.name!r} may need attention: {status!r}",
                updated_at=source_updated_at,
            )
        ]

    if status == "waiting" and _worker_needs_human(worker):
        return [
            _worker_attention_signal(
                worker,
                host_id=host_id,
                severity="warning",
                reason=f"Worker {worker.name!r} is waiting for human input or approval",
                updated_at=source_updated_at,
            )
        ]

    return []


def attention_from_snapshot(snapshot: Snapshot) -> list[AttentionSignal]:
    """Return deterministic attention signals for an entire snapshot."""
    signals: list[AttentionSignal] = []

    for worker in snapshot.workers:
        signals.extend(attention_for_worker(worker, host_id=snapshot.host_id))

    return sorted(signals, key=lambda signal: (signal.id, signal.fingerprint))


def update_snapshot_attention(snapshot: Snapshot) -> Snapshot:
    """Return a new snapshot with attention recomputed from its workers."""
    return Snapshot(
        host_id=snapshot.host_id,
        updated_at=snapshot.updated_at,
        spaces=list(snapshot.spaces),
        workers=list(snapshot.workers),
        attention=attention_from_snapshot(snapshot),
        backend_health=list(snapshot.backend_health),
    )
