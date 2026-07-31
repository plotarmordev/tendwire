"""Thin adapter boundary around the Herdr CLI.

This module shells out read-only to a `herdr` binary when available and parses
output on a best-effort basis. If the binary is missing or fails, it returns
empty neutral data rather than blocking the snapshot contract.

This module must not import Herdres code or leak delivery/routing state into
core models.
"""

from __future__ import annotations

import hashlib
import json
import shutil
import subprocess
import time
from collections import Counter
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta, timezone
from typing import Any

from ..config import Config
from ..core.models import (
    BackendHealth,
    Space,
    Worker,
    WorkerBinding,
    normalize_status,
    separate_duplicate_worker_bindings,
    sanitize_public_text,
    stable_fingerprint,
    utc_timestamp,
    worker_binding_private_fingerprint,
)
from ..local_state import (
    ConfigStateReport,
    LocalStateErrorCode,
    LocalStateKind,
    PermissionState,
    inspect_config_state,
)
from ..worker_identity import (
    InstallationKeyError,
    STABLE_KEY_VERSION,
    canonical_herdr_pane_identity,
    load_or_create_installation_key,
    stable_worker_key,
)


_HERDR_TIMEOUT_SECONDS = 5.0
_BACKEND_NAME = "herdr"
_AMBIGUOUS_BINDING_REASONS = frozenset(
    {"ambiguous_pane_match", "duplicate_backend_target", "not_unique"}
)


class HerdrContinuityUnavailableError(RuntimeError):
    """A healthy-looking Herdr observation cannot authenticate worker continuity."""


@dataclass(frozen=True)
class _WorkerRecord:
    worker: Worker
    private_fingerprint: str
    turn_target_kind: str | None = None
    turn_target_value: str | None = None
    # Canonical public Herdr identity used exclusively for continuity. Raw
    # observations remain private and separate so routing compatibility can
    # never accidentally feed stable-key derivation.
    workspace_id: str | None = None
    pane_id: str | None = None
    observed_workspace_id: str | None = None
    observed_pane_id: str | None = None
    identity_source: str = "unknown"
    terminal_id: str | None = None
    agent_session_id: str | None = None
    # Continuity is authorized only by a PaneInfo observation, never by
    # workspace/pane-shaped fields reported by agent.list.
    pane_info_observed: bool = False
    unmatched_agent_observation: bool = False

_FORBIDDEN_CONNECTOR_FIELDS = {
    "telegram",
    "chat_id",
    "topic_id",
    "message_id",
    "thread_id",
    "token",
    "bot_token",
    "delivery",
    "route",
    "herdres_delivery",
    "backend_target",
}

_STATUS_KEYS = (
    "agent_status",
    "status",
    "state",
    "phase",
    "lifecycle",
    "lifecycle_state",
    "raw_status",
)

_BACKEND_TARGET_KINDS = frozenset(
    {"agent_id", "terminal_id", "pane_id", "agent", "name", "label"}
)
_AGENT_SCOPED_BACKEND_TARGET_KINDS = frozenset({"agent_id", "agent"})
_SESSION_SCOPED_TURN_TARGET_KINDS = frozenset(
    {"codex_session_id", "omp_session_path"}
)
_DEADLINE_EXHAUSTED_OUTCOMES = frozenset({"timeout", "deadline_exhausted"})
_UNAVAILABLE_HEALTH_OUTCOMES = frozenset({"missing_binary", "launch_error", "socket_disconnected"})
_DEGRADED_HEALTH_OUTCOMES = frozenset(
    {
        "timeout",
        "deadline_exhausted",
        "nonzero",
        "malformed_json",
        "protocol_error",
        "worker_cap_exceeded",
        "continuity_unavailable",
    }
)

_HEALTH_MESSAGES = {
    "healthy_non_empty": "Herdr observation is healthy",
    "empty_healthy": "Herdr observation is healthy but empty",
    "missing_binary": "Herdr binary is unavailable",
    "launch_error": "Herdr launch failed",
    "timeout": "Herdr observation timed out",
    "deadline_exhausted": "Herdr observation deadline was exhausted",
    "nonzero": "Herdr command returned nonzero status",
    "malformed_json": "Herdr command returned malformed JSON",
    "protocol_error": "Herdr protocol returned an invalid envelope",
    "socket_disconnected": "Herdr socket disconnected",
    "worker_cap_exceeded": "Herdr observation exceeded the configured worker cap",
    "continuity_unavailable": "Herdr continuity identity is unavailable",
    "unknown": "Herdr observation state is unknown",
}


@dataclass(frozen=True)
class _ProbeBudget:
    """Aggregate deadline for a read-only Herdr observation chain."""

    started_at: float
    per_probe_timeout_seconds: float
    aggregate_deadline_seconds: float

    @classmethod
    def from_config(cls, config: Config, *, planned_probes: int) -> "_ProbeBudget":
        per_probe = float(config.herdr_timeout_seconds)
        planned = max(1, int(planned_probes))
        return cls(
            started_at=time.monotonic(),
            per_probe_timeout_seconds=per_probe,
            aggregate_deadline_seconds=per_probe * planned,
        )

    def remaining_seconds(self) -> float:
        return self.aggregate_deadline_seconds - (time.monotonic() - self.started_at)

    def subprocess_timeout_seconds(self) -> float | None:
        remaining = self.remaining_seconds()
        if remaining <= 0:
            return None
        if remaining >= self.per_probe_timeout_seconds:
            return self.per_probe_timeout_seconds
        return max(0.001, remaining)


def herdr_health_status_for_outcome(outcome: str) -> str:
    """Map a Herdr adapter outcome into the public backend health status."""
    normalized = str(outcome or "unknown").strip().lower().replace("-", "_")
    if normalized in {"healthy_non_empty", "empty_healthy"}:
        return "healthy"
    if normalized in _UNAVAILABLE_HEALTH_OUTCOMES:
        return "unavailable"
    if normalized in _DEGRADED_HEALTH_OUTCOMES:
        return "degraded"
    return "unknown"


def herdr_backend_health(
    outcome: str,
    *,
    observed_at: str | None = None,
    message: str | None = None,
    spaces: Sequence[Space] | None = None,
    workers: Sequence[Worker] | None = None,
) -> BackendHealth:
    """Return the fixed public-safe health object for a Herdr observation."""
    normalized_outcome = str(outcome or "unknown").strip().lower().replace("-", "_")
    if normalized_outcome == "ok":
        normalized_outcome = (
            "healthy_non_empty"
            if (spaces and len(spaces) > 0) or (workers and len(workers) > 0)
            else "empty_healthy"
        )
    if normalized_outcome not in {
        "healthy_non_empty",
        "empty_healthy",
        "missing_binary",
        "launch_error",
        "timeout",
        "deadline_exhausted",
        "nonzero",
        "malformed_json",
        "protocol_error",
        "socket_disconnected",
        "worker_cap_exceeded",
        "continuity_unavailable",
        "unknown",
    }:
        normalized_outcome = "unknown"
    counts = {
        "spaces": len(spaces or []),
        "workers": len(workers or []),
    }
    return BackendHealth(
        name=_BACKEND_NAME,
        status=herdr_health_status_for_outcome(normalized_outcome),
        outcome=normalized_outcome,
        observed_at=observed_at or utc_timestamp(),
        message=message if message is not None else _HEALTH_MESSAGES[normalized_outcome],
        counts=counts,
    )


@dataclass(frozen=True)
class HerdrSnapshotObservation:
    """Snapshot observation plus public backend health and private bindings."""

    spaces: list[Space]
    workers: list[Worker]
    bindings: list[WorkerBinding] = field(default_factory=list)
    backend_health: list[BackendHealth] = field(default_factory=list)

    def __post_init__(self) -> None:
        spaces = list(self.spaces)
        workers = list(self.workers)
        bindings = list(self.bindings)
        backend_health = list(self.backend_health)
        if not backend_health:
            outcome = "healthy_non_empty" if spaces or workers else "empty_healthy"
            backend_health = [herdr_backend_health(outcome, spaces=spaces, workers=workers)]
        object.__setattr__(self, "spaces", spaces)
        object.__setattr__(self, "workers", workers)
        object.__setattr__(self, "bindings", bindings)
        object.__setattr__(self, "backend_health", backend_health)

    @property
    def health(self) -> BackendHealth:
        for item in self.backend_health:
            if item.name == _BACKEND_NAME:
                return item
        return herdr_backend_health("unknown", spaces=self.spaces, workers=self.workers)

    @property
    def authoritative(self) -> bool:
        return self.health.status == "healthy"


@dataclass(frozen=True)
class HerdrCommandObservation:
    """Command execution observation with health metadata."""

    spaces: list[Space]
    workers: list[Worker]
    status: str
    outcome: str
    message: str = ""
    bindings: list[WorkerBinding] = field(default_factory=list)
    backend_health: list[BackendHealth] = field(default_factory=list)

    def __post_init__(self) -> None:
        spaces = list(self.spaces)
        workers = list(self.workers)
        bindings = list(self.bindings)
        backend_health = list(self.backend_health)
        if not backend_health:
            backend_health = [
                herdr_backend_health(
                    self.outcome,
                    message=self.message or None,
                    spaces=spaces,
                    workers=workers,
                )
            ]
        object.__setattr__(self, "spaces", spaces)
        object.__setattr__(self, "workers", workers)
        object.__setattr__(self, "bindings", bindings)
        object.__setattr__(self, "backend_health", backend_health)

    @property
    def healthy(self) -> bool:
        return self.status == "healthy" and self.health.status == "healthy"

    @property
    def health(self) -> BackendHealth:
        for item in self.backend_health:
            if item.name == _BACKEND_NAME:
                return item
        return herdr_backend_health(self.outcome, message=self.message or None, spaces=self.spaces, workers=self.workers)


_FORBIDDEN_CONNECTOR_FIELDS_COMPACT = {field.replace("_", "") for field in _FORBIDDEN_CONNECTOR_FIELDS}


def _compact_field_name(key: object) -> str:
    """Normalize field names for conservative connector/status-key matching."""
    return str(key).lower().replace("-", "_").replace(".", "_").replace("_", "")


def _field_matches(key: object, expected: str) -> bool:
    """Return True when a payload key matches snake_case or camelCase spelling."""
    return _compact_field_name(key) == _compact_field_name(expected)

def _is_reserved_stable_key_field(key: object) -> bool:
    """Match the entire current and future normalized stable-key family."""
    compact = str(key).lower().replace("_", "").replace("-", "").replace(".", "")
    return compact.startswith("stablekey")


def _strip_stable_key_fields(value: Any) -> Any:
    """Recursively remove every source-controlled stable-key family field."""
    if isinstance(value, Mapping):
        return {
            str(key): _strip_stable_key_fields(child)
            for key, child in value.items()
            if not _is_reserved_stable_key_field(key)
        }
    if isinstance(value, list):
        return [_strip_stable_key_fields(item) for item in value]
    if isinstance(value, tuple):
        return [_strip_stable_key_fields(item) for item in value]
    return value


_TURN_OBSERVATION_FIELD_NAMES = frozenset(
    {
        "turn",
        "turnepoch",
        "lastcompletedturn",
        "outcome",
    }
)


def _strip_turn_observation_fields(value: Any) -> Any:
    """Remove turn counters and outcomes from every Worker identity surface."""
    if isinstance(value, Mapping):
        return {
            str(key): _strip_turn_observation_fields(child)
            for key, child in value.items()
            if _compact_field_name(key) not in _TURN_OBSERVATION_FIELD_NAMES
        }
    if isinstance(value, list):
        return [_strip_turn_observation_fields(item) for item in value]
    if isinstance(value, tuple):
        return tuple(_strip_turn_observation_fields(item) for item in value)
    return value


def _private_fingerprint(value: Any) -> str:
    """Return a private adapter-only fingerprint without public sanitization."""
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()[:24]


def _is_forbidden_connector_field(key: object) -> bool:
    """Return True for forbidden connector fields, including common case variants."""
    normalized = str(key).lower().replace("-", "_").replace(".", "_")
    compact = _compact_field_name(key)
    segments = {part for part in normalized.split("_") if part}
    return (
        normalized in _FORBIDDEN_CONNECTOR_FIELDS
        or compact in _FORBIDDEN_CONNECTOR_FIELDS_COMPACT
        or bool(segments & _FORBIDDEN_CONNECTOR_FIELDS)
    )


def _contains_forbidden_connector_text(value: object) -> bool:
    """Return True when diagnostic text names connector/private delivery fields."""
    normalized = str(value).lower().replace("-", "_").replace(".", "_")
    compact = normalized.replace("_", "")
    return any(field in normalized for field in _FORBIDDEN_CONNECTOR_FIELDS) or any(
        field in compact for field in _FORBIDDEN_CONNECTOR_FIELDS_COMPACT
    )


def _run_herdr(
    args: Sequence[str],
    config: Config,
    *,
    timeout_seconds: float | None = None,
) -> subprocess.CompletedProcess[str] | None:
    """Run the Herdr CLI with read-only arguments; return None on launch failure."""
    timeout = config.herdr_timeout_seconds if timeout_seconds is None else timeout_seconds
    try:
        return subprocess.run(
            [config.herdr_bin, *args],
            capture_output=True,
            text=True,
            check=False,
            timeout=timeout,
        )
    except (OSError, UnicodeDecodeError, ValueError, TypeError):
        return None


def _run_herdr_probe(
    args: Sequence[str],
    config: Config,
    timeout_seconds: float | None,
) -> subprocess.CompletedProcess[str] | None:
    """Call _run_herdr with timeout support while preserving simple test fakes."""
    if timeout_seconds is None:
        return _run_herdr(args, config)
    try:
        return _run_herdr(args, config, timeout_seconds=timeout_seconds)
    except TypeError:
        return _run_herdr(args, config)


def _probe_herdr(
    args: Sequence[str],
    config: Config,
    budget: _ProbeBudget | None = None,
) -> tuple[str, Any]:
    """Run a read-only Herdr command and retain failure class for mutations."""
    timeout_seconds: float | None = None
    if budget is not None:
        timeout_seconds = budget.subprocess_timeout_seconds()
        if timeout_seconds is None:
            return "deadline_exhausted", None
    try:
        completed = _run_herdr_probe(args, config, timeout_seconds)
    except subprocess.TimeoutExpired:
        return "timeout", None
    if completed is None:
        return "launch_error", None

    if completed.returncode != 0:
        return "nonzero", None
    payload = _parse_json_output(completed.stdout)
    if payload is None:
        return "malformed_json", None
    return "ok", payload


def _parse_json_output(stdout: str | None) -> Any:
    """Best-effort parse of herdr JSON output; None on failure."""
    if not stdout or not stdout.strip():
        return None
    try:
        return json.loads(stdout)
    except (json.JSONDecodeError, TypeError, ValueError):
        return None


def _command_payload(args: Sequence[str], config: Config) -> Any:
    """Return parsed JSON for a single herdr command, or None on any bad output."""
    outcome, payload = _probe_herdr(args, config)
    if outcome != "ok":
        return None
    return payload


def _command_payload_variants(variants: Sequence[Sequence[str]], config: Config) -> Any:
    """Try a sequence of herdr arg lists in order; return first successful payload."""
    outcome, payload = _probe_payload_variants(variants, config)
    return payload if outcome == "ok" else None


def _safe_text_sample(value: str | None) -> str | None:
    """Return a short diagnostic sample through the shared public sanitizer."""
    if not value or _contains_forbidden_connector_text(value):
        return None
    sanitized = sanitize_public_text(
        value,
        max_chars=200,
        collapse_whitespace=True,
    )
    return sanitized or None


_LOCAL_STATE_COMPLIANT_REMEDIATION = "No action required."
_LOCAL_STATE_UNINITIALIZED_REMEDIATION = (
    "No action required while local state is uninitialized."
)
_LOCAL_STATE_STOPPED_REMEDIATION = "No action required while the daemon is stopped."
_LOCAL_STATE_REPAIR_REMEDIATION = (
    "Restart Tendwire to repair local state permissions."
)
_LOCAL_STATE_UNSAFE_REMEDIATION = (
    "Move unsafe local state aside and restore from a trusted backup."
)
_LOCAL_STATE_CHECK_GROUPS = (
    (
        "state_directory_permissions",
        frozenset({LocalStateKind.STATE_DIRECTORY}),
        "not_initialized",
        _LOCAL_STATE_UNINITIALIZED_REMEDIATION,
    ),
    (
        "database_permissions",
        frozenset(
            {
                LocalStateKind.DATABASE,
                LocalStateKind.DATABASE_WAL,
                LocalStateKind.DATABASE_SHM,
                LocalStateKind.DATABASE_JOURNAL,
            }
        ),
        "not_initialized",
        _LOCAL_STATE_UNINITIALIZED_REMEDIATION,
    ),
    (
        "identity_permissions",
        frozenset({LocalStateKind.PRIVATE_FILE}),
        "not_initialized",
        _LOCAL_STATE_UNINITIALIZED_REMEDIATION,
    ),
    (
        "daemon_socket_permissions",
        frozenset({LocalStateKind.SOCKET, LocalStateKind.SOCKET_GROUP}),
        "not_running",
        _LOCAL_STATE_STOPPED_REMEDIATION,
    ),
)


def _local_state_check(
    name: str,
    kinds: frozenset[LocalStateKind],
    neutral_outcome: str,
    neutral_remediation: str,
    *,
    entries: Sequence[Any],
    issues: Sequence[Any],
) -> dict[str, Any]:
    """Fold one fixed local-state category into a path-free doctor record."""
    category_entries = [entry for entry in entries if entry.kind in kinds]
    category_issues = [issue for issue in issues if issue.kind in kinds]
    issue_codes = {issue.code for issue in category_issues}
    if issue_codes - {LocalStateErrorCode.INSECURE_MODE}:
        return {
            "name": name,
            "ok": False,
            "outcome": "unsafe",
            "remediation": _LOCAL_STATE_UNSAFE_REMEDIATION,
        }
    if (
        LocalStateErrorCode.INSECURE_MODE in issue_codes
        or any(
            entry.state is PermissionState.REPAIR_REQUIRED
            for entry in category_entries
        )
    ):
        return {
            "name": name,
            "ok": False,
            "outcome": "repair_required",
            "remediation": _LOCAL_STATE_REPAIR_REMEDIATION,
        }
    if not category_entries or all(
        entry.state is PermissionState.ABSENT for entry in category_entries
    ):
        return {
            "name": name,
            "ok": True,
            "outcome": neutral_outcome,
            "remediation": neutral_remediation,
        }
    return {
        "name": name,
        "ok": True,
        "outcome": "compliant",
        "remediation": _LOCAL_STATE_COMPLIANT_REMEDIATION,
    }


def _inspect_local_state(config: Config) -> ConfigStateReport | None:
    """Inspect configured local state once without creating or repairing it."""
    try:
        if config.db_path is None:
            raise ValueError
        socket_path = config.socket_path or config.data_dir / "tendwire.sock"
        return inspect_config_state(
            config.data_dir,
            config.db_path,
            socket_path=socket_path,
            private_files=(
                config.installation_key_path,
                config.installation_key_marker_path,
                config.installation_key_sentinel_path,
            ),
            socket_group=config.socket_group,
        )
    except Exception:
        return None


def _local_state_checks(
    report: ConfigStateReport | None,
) -> tuple[list[dict[str, Any]], bool]:
    """Fold a path-free inspection report into the fixed local-state checks."""
    inspected = report
    if inspected is None:
        checks = [
            {
                "name": name,
                "ok": False,
                "outcome": "unsafe",
                "remediation": _LOCAL_STATE_UNSAFE_REMEDIATION,
            }
            for name, _kinds, _neutral, _remediation in _LOCAL_STATE_CHECK_GROUPS
        ]
        return checks, False

    checks = [
        _local_state_check(
            name,
            kinds,
            neutral_outcome,
            neutral_remediation,
            entries=inspected.entries,
            issues=inspected.issues,
        )
        for name, kinds, neutral_outcome, neutral_remediation in _LOCAL_STATE_CHECK_GROUPS
    ]
    return checks, inspected.ok


def _public_herdr_bin(value: str) -> str:
    """Retain the doctor key while redacting configured path-shaped values."""
    sanitized = sanitize_public_text(
        value,
        max_chars=200,
        collapse_whitespace=True,
    )
    return sanitized or "[redacted]"


_STORE_MAINTENANCE_REMEDIATION = {
    "ok": "No action required.",
    "overdue": "Keep Tendwire running to resume automatic store maintenance.",
    "backlog": "Keep Tendwire running to drain the maintenance backlog.",
    "not_initialized": "No action required while the store is uninitialized.",
    "unsafe": "Move unsafe local state aside and restore from a trusted backup.",
    "unavailable": "Check store availability before retrying diagnostics.",
}
_STORE_KINDS = frozenset(
    {
        LocalStateKind.STATE_DIRECTORY,
        LocalStateKind.DATABASE,
        LocalStateKind.DATABASE_WAL,
        LocalStateKind.DATABASE_SHM,
        LocalStateKind.DATABASE_JOURNAL,
    }
)


def _store_maintenance_record(
    config: Config,
    outcome: str,
    *,
    snapshot_count: int = 0,
    last_completed_at: str | None = None,
) -> dict[str, Any]:
    """Build the fixed public-safe maintenance doctor record."""
    return {
        "name": "store_maintenance",
        "ok": outcome in {"ok", "not_initialized"},
        "outcome": outcome,
        "remediation": _STORE_MAINTENANCE_REMEDIATION[outcome],
        "snapshot_retention_days": config.snapshot_retention_days,
        "snapshot_retention_count": config.snapshot_retention_count,
        "maintenance_batch_size": config.snapshot_maintenance_batch_size,
        "maintenance_cadence_seconds": config.store_maintenance_cadence_seconds,
        "snapshot_count": snapshot_count,
        "last_completed_at": last_completed_at,
    }


def _public_utc_timestamp(value: Any) -> tuple[str | None, datetime | None]:
    """Return a normalized public UTC timestamp, rejecting malformed/private text."""
    if not isinstance(value, str) or not value.strip():
        return None, None
    raw = value.strip()
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(raw)
    except (TypeError, ValueError):
        return None, None
    if parsed.tzinfo is None:
        return None, None
    normalized = parsed.astimezone(timezone.utc)
    return normalized.isoformat(), normalized




def _store_maintenance_check(
    config: Config,
    report: ConfigStateReport | None,
) -> dict[str, Any]:
    """Return one fixed, path-free, non-mutating store-maintenance check."""
    if report is None:
        return _store_maintenance_record(config, "unsafe")
    relevant_issues = [issue for issue in report.issues if issue.kind in _STORE_KINDS]
    relevant_entries = [entry for entry in report.entries if entry.kind in _STORE_KINDS]
    if relevant_issues or any(
        entry.state is PermissionState.REPAIR_REQUIRED for entry in relevant_entries
    ):
        return _store_maintenance_record(config, "unsafe")
    database = next(
        (entry for entry in relevant_entries if entry.kind is LocalStateKind.DATABASE),
        None,
    )
    if database is None or database.state is PermissionState.ABSENT:
        return _store_maintenance_record(config, "not_initialized")

    try:
        from ..store.sqlite import store_status

        status = store_status(
            config.db_path,
            config.host_id,
            snapshot_retention_days=config.snapshot_retention_days,
            snapshot_retention_count=config.snapshot_retention_count,
            maintenance_batch_size=config.snapshot_maintenance_batch_size,
            maintenance_cadence_seconds=config.store_maintenance_cadence_seconds,
            require_current_schema=True,
        )
        maintenance = status.get("maintenance")
        if (
            status.get("ok") is not True
            or not isinstance(maintenance, Mapping)
            or isinstance(maintenance.get("snapshot_count"), bool)
            or not isinstance(maintenance.get("snapshot_count"), int)
            or int(maintenance["snapshot_count"]) < 0
            or not isinstance(maintenance.get("backlog"), bool)
            or maintenance.get("status") not in {"never", "ok", "failed"}
        ):
            return _store_maintenance_record(config, "unavailable")
        snapshot_count = int(maintenance["snapshot_count"])
        maintenance_status = str(maintenance["status"])
        last_value = maintenance.get("last_completed_at")
        if maintenance_status == "failed" or (
            maintenance_status == "never" and last_value is not None
        ):
            return _store_maintenance_record(config, "unavailable")
        if last_value is None:
            last_completed_at = None
            completed = None
        else:
            last_completed_at, completed = _public_utc_timestamp(last_value)
            if completed is None:
                return _store_maintenance_record(config, "unavailable")
        if maintenance["backlog"]:
            outcome = "backlog"
        elif completed is None:
            outcome = "overdue"
        else:
            _now_value, now = _public_utc_timestamp(utc_timestamp())
            if now is None:
                return _store_maintenance_record(config, "unavailable")
            due_at = completed + timedelta(
                seconds=config.store_maintenance_cadence_seconds
            )
            outcome = "overdue" if now >= due_at else "ok"
        return _store_maintenance_record(
            config,
            outcome,
            snapshot_count=snapshot_count,
            last_completed_at=last_completed_at,
        )
    except Exception:
        return _store_maintenance_record(config, "unavailable")


def _pending_ingestion_check(config: Config) -> dict[str, Any]:
    """Return one fixed non-mutating check from durable pending state only."""
    unavailable = {
        "status": "store_unavailable",
        "counts": {"fresh": 0, "stale": 0, "total": 0},
    }
    try:
        from ..store.sqlite import backend_pending_health

        value = backend_pending_health(config.db_path, config.host_id)
    except Exception:
        value = unavailable
    raw = value if isinstance(value, Mapping) else unavailable
    raw_counts = raw.get("counts")
    status = raw.get("status")
    count_values = (
        tuple(raw_counts.get(key) for key in ("fresh", "stale", "total"))
        if isinstance(raw_counts, Mapping)
        else ()
    )
    valid_counts = len(count_values) == 3 and all(
        isinstance(item, int) and not isinstance(item, bool) and item >= 0
        for item in count_values
    )
    if (
        status not in {"healthy", "degraded", "store_unavailable"}
        or not valid_counts
        or count_values[2] != count_values[0] + count_values[1]
        or (status == "healthy" and count_values[1] != 0)
        or (status == "degraded" and count_values[1] == 0)
        or (status == "store_unavailable" and count_values != (0, 0, 0))
    ):
        status = "store_unavailable"
        counts = dict(unavailable["counts"])
    else:
        counts = dict(zip(("fresh", "stale", "total"), count_values, strict=True))
    return {
        "name": "pending_ingestion",
        "ok": status == "healthy",
        "outcome": status,
        "counts": counts,
        "stale_grace_seconds": config.pending_stale_grace_seconds,
    }


def _finish_diagnostics(result: dict[str, Any], config: Config) -> dict[str, Any]:
    report = _inspect_local_state(config)
    local_checks, local_ok = _local_state_checks(report)
    maintenance = _store_maintenance_check(config, report)
    pending_ingestion = _pending_ingestion_check(config)
    result["checks"].extend(local_checks)
    result["checks"].append(maintenance)
    result["checks"].append(pending_ingestion)
    if (
        (not local_ok or not maintenance["ok"] or not pending_ingestion["ok"])
        and result["status"] == "ok"
    ):
        result["status"] = "degraded"
    return result


def _diagnostic_item_count(payload: Any, keys: Sequence[str]) -> int:
    return len(_payload_items(payload, keys))


def _diagnostic_check(
    name: str,
    args: Sequence[str],
    config: Config,
    keys: Sequence[str],
    budget: _ProbeBudget,
) -> dict[str, Any]:
    """Run one read-only Herdr command and return a sanitized diagnostic record."""
    check: dict[str, Any] = {
        "name": name,
        "ok": False,
        "outcome": "unknown",
        "timeout_seconds": config.herdr_timeout_seconds,
        "aggregate_deadline_seconds": budget.aggregate_deadline_seconds,
    }
    timeout_seconds = budget.subprocess_timeout_seconds()
    if timeout_seconds is None:
        check["outcome"] = "deadline_exhausted"
        return check
    try:
        completed = subprocess.run(
            [config.herdr_bin, *args],
            capture_output=True,
            text=True,
            check=False,
            timeout=timeout_seconds,
        )
    except subprocess.TimeoutExpired:
        check["outcome"] = "timeout"
        return check
    except (OSError, UnicodeDecodeError, ValueError, TypeError):
        check["outcome"] = "launch_error"
        return check

    check["exit_code"] = int(completed.returncode)
    if completed.returncode != 0:
        check["outcome"] = "nonzero"
        stdout_sample = _safe_text_sample(completed.stdout)
        stderr_sample = _safe_text_sample(completed.stderr)
        if stdout_sample is not None:
            check["stdout_sample"] = stdout_sample
        if stderr_sample is not None:
            check["stderr_sample"] = stderr_sample
        return check

    payload = _parse_json_output(completed.stdout)
    if payload is None:
        check["outcome"] = "malformed_json"
        stdout_sample = _safe_text_sample(completed.stdout)
        if stdout_sample is not None:
            check["stdout_sample"] = stdout_sample
        return check

    item_count = _diagnostic_item_count(payload, keys)
    check["ok"] = True
    check["item_count"] = item_count
    check["outcome"] = "healthy_non_empty" if item_count else "empty_healthy"
    return check


def diagnose_herdr(config: Config) -> dict[str, Any]:
    """Return JSON-serializable read-only Herdr CLI diagnostics."""
    groups = [
        [
            ("workspace_list", ["workspace", "list"], ("workspaces", "spaces", "data", "items", "results", "result")),
            ("workspace_list_json", ["workspace", "list", "--json"], ("workspaces", "spaces", "data", "items", "results", "result")),
        ],
        [
            ("agent_list", ["agent", "list"], ("agents", "workers", "data", "items", "results", "result")),
            ("agent_list_json", ["agent", "list", "--json"], ("agents", "workers", "data", "items", "results", "result")),
        ],
        [
            ("pane_list", ["pane", "list"], ("panes", "items", "data", "results", "result")),
            ("pane_list_json", ["pane", "list", "--json"], ("panes", "items", "data", "results", "result")),
        ],
    ]
    planned = [check for group in groups for check in group]
    result: dict[str, Any] = {
        "schema_version": 1,
        "command": "doctor",
        "herdr_bin": _public_herdr_bin(config.herdr_bin),
        "timeout_seconds": config.herdr_timeout_seconds,
        "aggregate_deadline_seconds": config.herdr_timeout_seconds * len(planned),
        "status": "ok",
        "checks": [],
    }
    try:
        binary_path = shutil.which(config.herdr_bin)
    except (TypeError, ValueError, OSError):
        binary_path = None

    if binary_path is None:
        result["status"] = "unavailable"
        result["checks"] = [
            {
                "name": name,
                "ok": False,
                "outcome": "missing_binary",
                "timeout_seconds": config.herdr_timeout_seconds,
                "aggregate_deadline_seconds": result["aggregate_deadline_seconds"],
            }
            for name, args, _keys in planned
        ]
        return _finish_diagnostics(result, config)

    budget = _ProbeBudget.from_config(config, planned_probes=len(planned))
    checks: list[dict[str, Any]] = []
    stop_after_timeout = False
    stop_after_outcome = ""
    for group in groups:
        if stop_after_timeout:
            break
        for index, (name, args, keys) in enumerate(group):
            if index > 0 and checks[-1]["ok"]:
                break
            check = _diagnostic_check(name, args, config, keys, budget)
            checks.append(check)
            if check["outcome"] in _DEADLINE_EXHAUSTED_OUTCOMES:
                stop_after_timeout = True
                stop_after_outcome = str(check["outcome"])
                break

    names_seen = {str(check["name"]) for check in checks}
    remaining_planned = [
        (name, args, keys)
        for name, args, keys in planned
        if name not in names_seen
    ]
    if stop_after_timeout:
        skipped_outcome = (
            "skipped_after_deadline"
            if stop_after_outcome == "deadline_exhausted"
            else "skipped_after_timeout"
        )
        for name, args, _keys in remaining_planned:
            checks.append(
                {
                    "name": name,
                    "ok": False,
                    "outcome": skipped_outcome,
                    "timeout_seconds": config.herdr_timeout_seconds,
                    "aggregate_deadline_seconds": budget.aggregate_deadline_seconds,
                }
            )
    else:
        for name, args, _keys in remaining_planned:
            checks.append(
                {
                    "name": name,
                    "ok": True,
                    "outcome": "skipped_not_needed",
                    "timeout_seconds": config.herdr_timeout_seconds,
                    "aggregate_deadline_seconds": budget.aggregate_deadline_seconds,
                }
            )

    result["checks"] = checks
    if any(check["outcome"] in _DEADLINE_EXHAUSTED_OUTCOMES for check in checks):
        result["status"] = "timeout"
    elif any(not check["ok"] for check in checks):
        result["status"] = "degraded"
    return _finish_diagnostics(result, config)


def _strip_connector_fields(value: Any) -> Any:
    """Recursively drop connector/delivery fields from arbitrary JSON-like values."""
    if isinstance(value, Mapping):
        clean: dict[str, Any] = {}
        for key, child in value.items():
            key_text = str(key)
            if _is_forbidden_connector_field(key):
                continue
            clean[key_text] = _strip_connector_fields(child)
        return clean
    if isinstance(value, list):
        return [_strip_connector_fields(item) for item in value]
    if isinstance(value, tuple):
        return [_strip_connector_fields(item) for item in value]
    return value


def _strip_status_fields(value: Any) -> Any:
    """Recursively drop raw status fields from metadata values."""
    if isinstance(value, Mapping):
        clean: dict[str, Any] = {}
        for key, child in value.items():
            if any(_field_matches(key, status_key) for status_key in _STATUS_KEYS):
                continue
            clean[str(key)] = _strip_status_fields(child)
        return clean
    if isinstance(value, list):
        return [_strip_status_fields(item) for item in value]
    if isinstance(value, tuple):
        return [_strip_status_fields(item) for item in value]
    return value


def _payload_items(payload: Any, keys: Sequence[str]) -> list[dict[str, Any]]:
    """Extract object records from conservative herdr list payload shapes."""
    if isinstance(payload, list):
        candidates: Iterable[Any] = payload
    elif isinstance(payload, Mapping):
        candidates = ()
        for key in keys:
            value = _value_for_key(payload, key)
            if isinstance(value, list):
                candidates = value
                break
            if isinstance(value, Mapping):
                nested = _payload_items(value, keys)
                if nested:
                    return nested
    else:
        return []

    items: list[dict[str, Any]] = []
    for item in candidates:
        if isinstance(item, Mapping):
            stripped = _strip_connector_fields(item)
            if isinstance(stripped, dict):
                items.append(stripped)
    return items


def _first_text(item: Mapping[str, Any], keys: Sequence[str]) -> str | None:
    """Return the first scalar string value for any key."""
    for key in keys:
        value = _value_for_key(item, key)
        if value is None:
            continue
        if isinstance(value, (str, int, float, bool)):
            return str(value)
    return None


def _nested_text(item: Mapping[str, Any], *path: str) -> str | None:
    """Return the first scalar string value reachable via a dotted key path."""
    current: Any = item
    for key in path:
        if not isinstance(current, Mapping):
            return None
        current = _value_for_key(current, key)
    if current is None:
        return None
    if isinstance(current, (str, int, float, bool)):
        return str(current)
    if isinstance(current, Mapping):
        return _first_text(current, ("id", "value", "name", "label"))
    return None


def _related_id(value: Any) -> str | None:
    """Return a neutral related-object id from a scalar or mapping."""
    if value is None:
        return None
    if isinstance(value, Mapping):
        return _first_text(value, ("id", "workspace_id", "space_id", "slug", "name"))
    if isinstance(value, (str, int, float, bool)):
        return str(value)
    return None


def _normalize_status(raw_status: Any) -> tuple[str, str | None]:
    """Return canonical status plus original raw string when normalization changed it."""
    if raw_status is None:
        return "unknown", None

    raw_text = str(raw_status)
    canonical = normalize_status(raw_text)
    raw_key = raw_text.strip().lower().replace("_", "-")
    raw_meta = raw_text if raw_text and raw_key != canonical else None
    return canonical, raw_meta


def _value_for_key(item: Mapping[str, Any], expected_key: str) -> Any:
    """Return a value by exact key or snake/camel-case equivalent."""
    if expected_key in item:
        return item[expected_key]
    for key, value in item.items():
        if _field_matches(key, expected_key):
            return value
    return None


def _status_from_item(item: Mapping[str, Any]) -> tuple[str, str | None]:
    """Extract and normalize status-like fields from a herdr record."""
    for key in _STATUS_KEYS:
        value = _value_for_key(item, key)
        if value is not None:
            return _normalize_status(value)
    return "unknown", None


def _meta_from_item(item: Mapping[str, Any], excluded_keys: set[str], raw_status: str | None) -> dict[str, Any]:
    """Build sanitized neutral metadata for a projected model."""
    item = _strip_stable_key_fields(item)
    explicit_meta = _value_for_key(item, "meta")
    meta = {
        str(key): _strip_status_fields(value)
        for key, value in item.items()
        if not _field_matches(key, "meta")
        and not any(_field_matches(key, excluded_key) for excluded_key in excluded_keys)
        and not any(_field_matches(key, status_key) for status_key in _STATUS_KEYS)
    }
    if isinstance(explicit_meta, Mapping):
        for key, value in explicit_meta.items():
            if _is_forbidden_connector_field(key):
                continue
            if any(_field_matches(key, status_key) for status_key in _STATUS_KEYS):
                continue
            meta[str(key)] = _strip_status_fields(value)
    if raw_status is not None:
        meta["raw_status"] = raw_status
    return meta


def _space_id_from_item(item: Mapping[str, Any]) -> str:
    """Resolve a stable space id, preferring explicit Herdr workspace_id."""
    return _first_text(item, ("workspace_id", "space_id", "id", "slug", "name")) or "unknown"


def _space_name_from_item(item: Mapping[str, Any], space_id: str) -> str:
    """Resolve a space name, preferring label then workspace_id."""
    return _first_text(item, ("label", "name", "title", "workspace_id", "space_id")) or space_id


def _public_worker_id_base_from_item(item: Mapping[str, Any]) -> str:
    """Resolve a neutral public worker id without terminal/session handles."""
    return (
        _first_text(item, ("worker_id", "id", "slug", "agent_id"))
        or _first_text(item, ("agent", "name", "label", "title"))
        or "unknown"
    )


def _worker_id_from_item(item: Mapping[str, Any]) -> str:
    """Resolve a stable public worker id."""
    return _public_worker_id_base_from_item(item)


def _private_identity_material_from_item(item: Mapping[str, Any]) -> dict[str, Any]:
    """Return private Herdr identity material that is never serialized publicly."""
    agent_id = _first_text(item, ("agent_id",))
    agent_session = _nested_text(item, "agent_session", "value")
    session_id = _first_text(item, ("session_id",))
    if agent_id or agent_session or session_id:
        return {
            "agent_id": agent_id,
            "agent_session": agent_session,
            "session_id": session_id,
            "space_id": _worker_space_id_from_item(item),
        }
    base = {
        "public_id": _public_worker_id_base_from_item(item),
        "name": _first_text(item, ("agent", "name", "label", "title")),
        "space_id": _worker_space_id_from_item(item),
    }
    base["terminal_id"] = _first_text(item, ("terminal_id",))
    base["pane_id"] = _first_text(item, ("pane_id",))
    return base


def _private_identity_from_item(item: Mapping[str, Any], config: Config | None = None) -> str:
    """Return a private identity used only to avoid collapsing distinct workers."""
    material = _private_identity_material_from_item(item)
    if config is None:
        return _private_fingerprint(material)
    return worker_binding_private_fingerprint(
        host_id=config.host_id,
        backend=_BACKEND_NAME,
        identity_material=material,
    )


def _private_backend_target(kind: str, value: str, *, sendable: bool = True, reason: str | None = None) -> dict[str, Any]:
    """Return the internal backend target shape."""
    return {
        "kind": kind,
        "value": value,
        "sendable": bool(sendable),
        "reason": reason,
    }


def _backend_target_from_item(item: Mapping[str, Any]) -> dict[str, Any] | None:
    """Resolve the private Herdr send target from backend-observed fields."""
    candidates = (
        ("agent_id", _first_text(item, ("agent_id",))),
        ("terminal_id", _first_text(item, ("terminal_id",))),
        ("pane_id", _first_text(item, ("pane_id",))),
        ("agent", _first_text(item, ("agent",))),
        ("name", _first_text(item, ("name",))),
        ("label", _first_text(item, ("label",))),
    )
    for kind, value in candidates:
        if value:
            return _private_backend_target(kind, value)
    return None


def _turn_target_from_item(item: Mapping[str, Any]) -> tuple[str, str] | None:
    """Resolve the private Herdr structured-turn target from backend-observed fields."""
    agent_name = (_first_text(item, ("agent", "name")) or "").strip().lower()
    agent_session = _nested_text(item, "agent_session", "value")
    if agent_name == "codex" and agent_session:
        return "codex_session_id", agent_session
    if agent_name == "omp" and agent_session:
        # oh-my-pi reports its native session file path (herdr:omp source).
        return "omp_session_path", agent_session
    pane_id = _first_text(item, ("pane_id",))
    if pane_id:
        return "pane_id", pane_id
    return None


def _worker_with_id(worker: Worker, worker_id: str) -> Worker:
    """Return a worker copy with a disambiguated public id."""
    return Worker(
        id=worker_id,
        name=worker.name,
        status=worker.status,
        space_id=worker.space_id,
        meta=worker.meta,
        last_seen_at=worker.last_seen_at,
        summary=worker.summary,
        backend_target=worker.backend_target,
    )


def _worker_name_from_item(item: Mapping[str, Any], worker_id: str) -> str:
    """Resolve a worker display name, preferring agent then label."""
    return _first_text(item, ("agent", "label", "name", "title")) or worker_id


def _worker_space_id_from_item(item: Mapping[str, Any]) -> str | None:
    """Resolve a worker's parent space id, preferring workspace_id."""
    return (
        _first_text(item, ("workspace_id", "space_id", "spaceId", "workspaceId"))
        or _related_id(_value_for_key(item, "space"))
        or _related_id(_value_for_key(item, "workspace"))
    )


def _spaces_from_payload(payload: Any) -> list[Space]:
    """Extract neutral Space objects from a herdr workspace-list payload."""
    spaces: list[Space] = []
    for item in _payload_items(payload, ("workspaces", "spaces", "data", "items", "results", "result")):
        space_id = _space_id_from_item(item)
        name = _space_name_from_item(item, space_id)
        status, raw_status = _status_from_item(item)
        updated_at = _first_text(item, ("updated_at", "last_seen_at", "observed_at", "timestamp"))
        status_line = _first_text(item, ("status_line", "summary", "description"))
        meta = _meta_from_item(
            item,
            {
                "id",
                "workspace_id",
                "space_id",
                "slug",
                "name",
                "title",
                "label",
                "meta",
                "updated_at",
                "last_seen_at",
                "observed_at",
                "timestamp",
                "status_line",
                "summary",
                "description",
                "fingerprint",
                "agent_status",
            },
            raw_status,
        )
        spaces.append(
            Space(
                id=space_id,
                name=name,
                status=status,
                meta=meta,
                updated_at=updated_at,
                status_line=status_line,
            )
        )
    return spaces


def _worker_from_item(item: Mapping[str, Any]) -> Worker:
    """Build a neutral Worker from a single herdr agent/pane record."""
    worker_id = _worker_id_from_item(item)
    name = _worker_name_from_item(item, worker_id)
    status, raw_status = _status_from_item(item)
    last_seen_at = _first_text(item, ("last_seen_at", "updated_at", "observed_at", "timestamp"))
    summary = _first_text(item, ("summary", "status_line", "description"))
    space_id = _worker_space_id_from_item(item)
    meta = _meta_from_item(
        item,
        {
            "id",
            "agent_id",
            "worker_id",
            "slug",
            "name",
            "title",
            "agent",
            "meta",
            "space_id",
            "workspace_id",
            "spaceId",
            "workspaceId",
            "space",
            "workspace",
            "last_seen_at",
            "updated_at",
            "observed_at",
            "timestamp",
            "summary",
            "status_line",
            "description",
            "fingerprint",
            "agent_status",
            "backend_target",
            "terminal_id",
            "pane_id",
            "agent_session",
            "session_id",
        },
        raw_status,
    )
    return Worker(
        id=worker_id,
        name=name,
        status=status,
        space_id=space_id,
        meta=meta,
        last_seen_at=last_seen_at,
        summary=summary,
        backend_target=_backend_target_from_item(item),
    )


def _output_excerpt_limit(config: Config | None) -> int | None:
    if config is None:
        return None
    try:
        limit = int(getattr(config, "output_excerpt_chars"))
    except (TypeError, ValueError):
        return None
    return max(1, limit)


def _bounded_excerpt(value: str | None, limit: int | None) -> str | None:
    if value is None or limit is None:
        return value
    text = str(value)
    if len(text) <= limit:
        return text
    if limit <= 3:
        return text[:limit]
    return text[: limit - 3] + "..."


def _worker_with_summary(worker: Worker, summary: str | None) -> Worker:
    if summary == worker.summary:
        return worker
    return Worker(
        id=worker.id,
        name=worker.name,
        status=worker.status,
        space_id=worker.space_id,
        meta=worker.meta,
        last_seen_at=worker.last_seen_at,
        summary=summary,
        backend_target=worker.backend_target,
    )


def _worker_record_from_item(
    item: Mapping[str, Any],
    config: Config | None = None,
    *,
    pane_info_observed: bool = False,
    identity_source: str = "unknown",
) -> _WorkerRecord:
    item = _strip_turn_observation_fields(item)
    worker = _worker_from_item(item)
    worker = _worker_with_summary(worker, _bounded_excerpt(worker.summary, _output_excerpt_limit(config)))
    turn_target = _turn_target_from_item(item)
    observed_workspace_id = _first_text(item, ("workspace_id", "workspaceId"))
    observed_pane_id = _first_text(item, ("pane_id", "paneId"))
    canonical_identity = canonical_herdr_pane_identity(
        observed_workspace_id,
        observed_pane_id,
    )
    workspace_id, pane_id = canonical_identity or (None, None)
    return _WorkerRecord(
        worker=worker,
        private_fingerprint=_private_identity_from_item(item, config),
        turn_target_kind=turn_target[0] if turn_target is not None else None,
        turn_target_value=turn_target[1] if turn_target is not None else None,
        workspace_id=workspace_id,
        pane_id=pane_id,
        observed_workspace_id=observed_workspace_id,
        observed_pane_id=observed_pane_id,
        identity_source=identity_source,
        terminal_id=_first_text(item, ("terminal_id", "terminalId")),
        agent_session_id=(
            _nested_text(item, "agent_session", "value")
            or _first_text(item, ("session_id", "sessionId"))
        ),
        pane_info_observed=pane_info_observed,
    )


def _worker_with_backend_target(worker: Worker, backend_target: dict[str, Any] | None) -> Worker:
    return Worker(
        id=worker.id,
        name=worker.name,
        status=worker.status,
        space_id=worker.space_id,
        meta=worker.meta,
        last_seen_at=worker.last_seen_at,
        summary=worker.summary,
        fingerprint=worker.fingerprint,
        backend_target=backend_target,
    )


def _agent_observation_record(
    item: Mapping[str, Any],
    config: Config | None = None,
) -> _WorkerRecord:
    """Record agent.list provenance without authorizing pane continuity."""
    return replace(
        _worker_record_from_item(
            item,
            config,
            pane_info_observed=False,
            identity_source="agent.list",
        ),
        unmatched_agent_observation=True,
    )


def _stable_pane_identity(record: _WorkerRecord) -> tuple[str, str] | None:
    """Return a PaneInfo-verified workspace/public-pane pair, never a runtime id."""
    if not record.pane_info_observed:
        return None
    return canonical_herdr_pane_identity(record.workspace_id, record.pane_id)


def _worker_with_stable_key(
    config: Config,
    record: _WorkerRecord,
    installation_key: bytes | None,
) -> Worker:
    """Replace source continuity metadata with a locally authenticated key."""
    worker = record.worker
    meta = _strip_stable_key_fields(worker.meta)
    identity = _stable_pane_identity(record)
    if installation_key is not None and identity is not None:
        workspace_id, pane_id = identity
        meta["stable_key"] = stable_worker_key(
            installation_key,
            backend=_BACKEND_NAME,
            host_id=config.host_id,
            workspace_id=workspace_id,
            pane_id=pane_id,
        )
        meta["stable_key_version"] = STABLE_KEY_VERSION
    return Worker(
        id=worker.id,
        name=worker.name,
        status=worker.status,
        space_id=worker.space_id,
        meta=meta,
        last_seen_at=worker.last_seen_at,
        summary=worker.summary,
        backend_target=worker.backend_target,
    )


def _backend_target_send_token(target: Mapping[str, Any] | None) -> str:
    """Return the exact value passed as herdr agent send's target argv token."""
    if not isinstance(target, Mapping):
        return ""
    return str(target.get("value") or "")


def _mark_backend_sendability(workers: list[Worker]) -> list[Worker]:
    """Mark unsupported or duplicate final Herdr send tokens as not sendable."""
    marked: list[Worker] = []
    for worker in workers:
        target = worker.backend_target
        if not isinstance(target, dict):
            marked.append(worker)
            continue
        kind = str(target.get("kind") or "")
        value = _backend_target_send_token(target)
        if target.get("sendable") is False:
            marked.append(worker)
            continue
        if kind not in _BACKEND_TARGET_KINDS or not value:
            marked.append(
                _worker_with_backend_target(
                    worker,
                    _private_backend_target(kind or "agent", value, sendable=False, reason="backend_unsupported"),
                )
            )
            continue
        marked.append(_worker_with_backend_target(worker, _private_backend_target(kind, value)))

    send_token_counts = Counter(
        _backend_target_send_token(worker.backend_target)
        for worker in marked
        if isinstance(worker.backend_target, dict) and worker.backend_target.get("sendable") is True
    )
    final: list[Worker] = []
    for worker in marked:
        target = worker.backend_target
        if not isinstance(target, dict) or target.get("sendable") is not True:
            final.append(worker)
            continue
        kind = str(target.get("kind") or "")
        value = _backend_target_send_token(target)
        if send_token_counts[value] > 1:
            final.append(
                _worker_with_backend_target(
                    worker,
                    _private_backend_target(kind, value, sendable=False, reason="duplicate_backend_target"),
                )
            )
            continue
        final.append(worker)
    return final


def assert_unique_sendable_backend_targets(workers: Iterable[Worker]) -> bool:
    """Prove no sendable workers share the same final Herdr argv target token."""
    seen: set[str] = set()
    for worker in workers:
        target = worker.backend_target
        if not isinstance(target, Mapping) or target.get("sendable") is not True:
            continue
        token = _backend_target_send_token(target)
        if not token:
            raise AssertionError("sendable backend target is missing a send token")
        if token in seen:
            raise AssertionError("duplicate sendable backend target token")
        seen.add(token)
    return True


def _binding_target_key(binding: WorkerBinding) -> tuple[str, str]:
    return (binding.target_kind, binding.target_value)


def _safe_stored_binding_for_reuse(binding: WorkerBinding) -> bool:
    return bool(binding.worker_id) and (binding.reason or "") not in _AMBIGUOUS_BINDING_REASONS


def _worker_target_key(worker: Worker) -> tuple[str, str] | None:
    target = worker.backend_target
    if not isinstance(target, Mapping):
        return None
    kind = str(target.get("kind") or "")
    value = str(target.get("value") or "")
    if not kind or not value:
        return None
    return kind, value


def _record_with_worker(record: _WorkerRecord, worker: Worker) -> _WorkerRecord:
    return _WorkerRecord(
        worker=worker,
        private_fingerprint=record.private_fingerprint,
        turn_target_kind=record.turn_target_kind,
        turn_target_value=record.turn_target_value,
        workspace_id=record.workspace_id,
        pane_id=record.pane_id,
        observed_workspace_id=record.observed_workspace_id,
        observed_pane_id=record.observed_pane_id,
        identity_source=record.identity_source,
        terminal_id=record.terminal_id,
        agent_session_id=record.agent_session_id,
        pane_info_observed=record.pane_info_observed,
        unmatched_agent_observation=record.unmatched_agent_observation,
    )


def _reuse_worker_ids_from_bindings(
    records: list[_WorkerRecord],
    stored_bindings: Sequence[WorkerBinding] | None,
) -> list[_WorkerRecord]:
    """Reuse stable public ids from private binding matches when safe."""
    if not stored_bindings:
        return records

    by_private: dict[str, list[WorkerBinding]] = {}
    by_target: dict[tuple[str, str], list[WorkerBinding]] = {}
    for binding in stored_bindings:
        if binding.backend != _BACKEND_NAME:
            continue
        by_private.setdefault(binding.private_fingerprint, []).append(binding)
        if _safe_stored_binding_for_reuse(binding):
            key = _binding_target_key(binding)
            if key[0] and key[1]:
                by_target.setdefault(key, []).append(binding)

    current_private_counts = Counter(record.private_fingerprint for record in records)
    current_target_counts = Counter(
        key
        for key in (_worker_target_key(record.worker) for record in records)
        if key is not None
    )

    reused: list[_WorkerRecord] = []
    for record in records:
        worker = record.worker
        private_fingerprint = record.private_fingerprint
        matched = None
        private_candidates = [
            binding
            for binding in by_private.get(private_fingerprint, [])
            if _safe_stored_binding_for_reuse(binding)
        ]
        if current_private_counts[private_fingerprint] == 1 and len(private_candidates) == 1:
            matched = private_candidates[0]
        if matched is None:
            key = _worker_target_key(worker)
            candidates = by_target.get(key or ("", ""), [])
            if key is not None and current_target_counts[key] == 1 and len(candidates) == 1:
                matched = candidates[0]
        if matched is not None and matched.worker_id:
            reused.append(_record_with_worker(record, _worker_with_id(worker, matched.worker_id)))
        else:
            reused.append(record)
    return reused


def _deduplicated_worker_records(
    records: list[_WorkerRecord],
    stored_bindings: Sequence[WorkerBinding] | None = None,
) -> list[_WorkerRecord]:
    """Drop exact duplicates, then disambiguate duplicate public ids."""
    records = _reuse_worker_ids_from_bindings(records, stored_bindings)
    seen: set[tuple[str, str, str | None, str, str, str]] = set()
    unique: list[_WorkerRecord] = []
    for record in records:
        worker = record.worker
        backend_kind = ""
        backend_value = ""
        if worker.backend_target:
            backend_kind = str(worker.backend_target.get("kind", ""))
            backend_value = str(worker.backend_target.get("value", ""))
        key = (worker.id, worker.name, worker.space_id, backend_kind, backend_value, record.private_fingerprint)
        if key in seen:
            continue
        seen.add(key)
        unique.append(record)

    groups: dict[str, list[_WorkerRecord]] = {}
    for record in unique:
        groups.setdefault(record.worker.id, []).append(record)

    disambiguated: list[_WorkerRecord] = []
    for worker_id, group in groups.items():
        if len(group) == 1:
            disambiguated.append(group[0])
            continue
        ordered = sorted(
            group,
            key=lambda record: (
                record.worker.name,
                record.worker.space_id or "",
                str((record.worker.backend_target or {}).get("kind", "")),
                str((record.worker.backend_target or {}).get("value", "")),
                record.private_fingerprint,
                stable_fingerprint(
                    {
                        "id": record.worker.id,
                        "name": record.worker.name,
                        "space_id": record.worker.space_id,
                        "status": record.worker.status,
                        "summary": record.worker.summary,
                    }
                ),
            ),
        )
        for index, record in enumerate(ordered, start=1):
            disambiguated.append(_record_with_worker(record, _worker_with_id(record.worker, f"{worker_id}-{index}")))

    disambiguated = sorted(disambiguated, key=lambda record: record.worker.id)
    workers = _mark_backend_sendability([record.worker for record in disambiguated])
    assert_unique_sendable_backend_targets(workers)
    return [
        _record_with_worker(record, worker)
        for record, worker in zip(disambiguated, workers, strict=True)
    ]


def _deduplicate_worker_records(
    records: list[_WorkerRecord],
    stored_bindings: Sequence[WorkerBinding] | None = None,
) -> list[Worker]:
    return [record.worker for record in _deduplicated_worker_records(records, stored_bindings)]


def _deduplicate_workers(workers: list[Worker]) -> list[Worker]:
    return _deduplicate_worker_records(
        [_WorkerRecord(worker=worker, private_fingerprint=worker.fingerprint) for worker in workers]
    )


def _binding_from_worker_record(
    config: Config,
    record: _WorkerRecord,
    observed_at: str,
) -> WorkerBinding | None:
    worker = record.worker
    target = worker.backend_target
    if not isinstance(target, Mapping):
        return None
    target_kind = str(target.get("kind") or "")
    target_value = str(target.get("value") or "")
    if not target_kind or not target_value:
        return None
    reason = target.get("reason")
    return WorkerBinding(
        host_id=config.host_id,
        worker_id=worker.id,
        worker_fingerprint=worker.fingerprint,
        backend=_BACKEND_NAME,
        target_kind=target_kind,
        target_value=target_value,
        turn_target_kind=record.turn_target_kind,
        turn_target_value=record.turn_target_value,
        sendable=target.get("sendable") is True,
        reason=str(reason) if reason is not None else None,
        observed_at=observed_at,
        expires_at=None,
        private_fingerprint=record.private_fingerprint,
    )


def _workers_and_bindings_from_records(
    config: Config,
    records: list[_WorkerRecord],
    *,
    stored_bindings: Sequence[WorkerBinding] | None = None,
    require_authenticated_continuity: bool = False,
) -> tuple[list[Worker], list[WorkerBinding]]:
    observed_at = utc_timestamp()
    deduplicated = _deduplicated_worker_records(records, stored_bindings)
    if require_authenticated_continuity and any(
        record.unmatched_agent_observation for record in deduplicated
    ):
        raise HerdrContinuityUnavailableError(
            "Herdr agent observation has no authoritative pane owner"
        )
    installation_key = None
    if any(
        _stable_pane_identity(record) is not None
        or (require_authenticated_continuity and record.pane_info_observed)
        for record in deduplicated
    ):
        try:
            installation_key = load_or_create_installation_key(config.data_dir)
        except InstallationKeyError:
            if require_authenticated_continuity:
                raise
            installation_key = None
    finalized = [
        _record_with_worker(
            record,
            _worker_with_stable_key(config, record, installation_key),
        )
        for record in deduplicated
    ]
    workers = [record.worker for record in finalized]
    bindings = [
        binding
        for record in finalized
        if (binding := _binding_from_worker_record(config, record, observed_at)) is not None
    ]
    return workers, separate_duplicate_worker_bindings(bindings)


def bindings_from_workers(
    config: Config,
    workers: Sequence[Worker],
    *,
    observed_at: str | None = None,
) -> list[WorkerBinding]:
    """Build private Herdr bindings from in-memory workers when raw records are absent."""
    timestamp = observed_at or utc_timestamp()
    workers = _mark_backend_sendability(list(workers))
    bindings: list[WorkerBinding] = []
    for worker in workers:
        target = worker.backend_target
        if not isinstance(target, Mapping):
            continue
        target_kind = str(target.get("kind") or "")
        target_value = str(target.get("value") or "")
        if not target_kind or not target_value:
            continue
        private_fingerprint = worker_binding_private_fingerprint(
            host_id=config.host_id,
            backend=_BACKEND_NAME,
            identity_material={
                "worker_id": worker.id,
                "worker_fingerprint": worker.fingerprint,
                "target_kind": target_kind,
                "target_value": target_value,
            },
        )
        record = _WorkerRecord(worker=worker, private_fingerprint=private_fingerprint)
        binding = _binding_from_worker_record(config, record, timestamp)
        if binding is not None:
            bindings.append(binding)
    return separate_duplicate_worker_bindings(bindings)


def _binding_for_worker(worker: Worker, bindings: Sequence[WorkerBinding]) -> WorkerBinding | None:
    candidates = [binding for binding in bindings if binding.worker_id == worker.id]
    if not candidates:
        return None
    exact = [binding for binding in candidates if binding.worker_fingerprint == worker.fingerprint]
    if len(exact) == 1:
        return exact[0]
    if exact:
        return None
    if len(candidates) == 1:
        return candidates[0]
    return None


def rehydrate_workers_from_bindings(
    workers: Sequence[Worker],
    current_bindings: Sequence[WorkerBinding] | None = None,
    stored_bindings: Sequence[WorkerBinding] | None = None,
) -> list[Worker]:
    """Attach private backend targets to workers from current, then stored bindings."""
    current = list(current_bindings or [])
    stored = list(stored_bindings or [])
    rehydrated: list[Worker] = []
    for worker in workers:
        binding = _binding_for_worker(worker, current)
        if binding is not None:
            rehydrated.append(_worker_with_backend_target(worker, binding.backend_target()))
            continue
        if isinstance(worker.backend_target, Mapping):
            rehydrated.append(worker)
            continue
        binding = _binding_for_worker(worker, stored)
        if binding is None:
            rehydrated.append(worker)
        else:
            rehydrated.append(_worker_with_backend_target(worker, binding.backend_target()))
    return rehydrated


def _workers_from_payload(
    payload: Any,
    config: Config | None = None,
    stored_bindings: Sequence[WorkerBinding] | None = None,
) -> list[Worker]:
    """Extract neutral Worker objects from a herdr agent-list payload."""
    records: list[_WorkerRecord] = []
    for item in _payload_items(payload, ("agents", "workers", "data", "items", "results", "result")):
        records.append(_agent_observation_record(item, config))
    return _deduplicate_worker_records(records, stored_bindings)


def _workers_and_bindings_from_payload(
    payload: Any,
    config: Config,
    stored_bindings: Sequence[WorkerBinding] | None = None,
) -> tuple[list[Worker], list[WorkerBinding]]:
    records: list[_WorkerRecord] = []
    for item in _payload_items(payload, ("agents", "workers", "data", "items", "results", "result")):
        records.append(_agent_observation_record(item, config))
    return _workers_and_bindings_from_records(config, records, stored_bindings=stored_bindings)


def _pane_has_agent(item: Mapping[str, Any]) -> bool:
    """Return True when a pane record carries an agent or explicit agent marker."""
    if _value_for_key(item, "agent") is not None:
        return True
    if _value_for_key(item, "agent_session") is not None:
        return True
    if _nested_text(item, "agent_session", "value"):
        return True
    markers = _value_for_key(item, "state_labels") or _value_for_key(item, "labels") or []
    if isinstance(markers, list):
        for marker in markers:
            if isinstance(marker, str) and "agent" in marker.lower():
                return True
    return False


def _record_match_keys(
    item: Mapping[str, Any],
    record: _WorkerRecord,
    *,
    pane_shaped: bool = False,
) -> list[tuple[str, str]]:
    keys: list[tuple[str, str]] = []
    pane_id_keys = ("pane_id", "paneId", "id") if pane_shaped else ("pane_id", "paneId")
    agent_session = (
        _nested_text(item, "agent_session", "value")
        or _first_text(item, ("session_id", "sessionId"))
    )
    for kind, value in (
        ("pane_id", _first_text(item, pane_id_keys)),
        ("terminal_id", _first_text(item, ("terminal_id", "terminalId"))),
        ("agent_session", agent_session),
        ("private_fingerprint", str(record.private_fingerprint)),
    ):
        if value and (kind, value) not in keys:
            keys.append((kind, value))
    return keys


def _backend_target_present(target: Any) -> bool:
    return (
        isinstance(target, Mapping)
        and bool(str(target.get("kind") or ""))
        and bool(str(target.get("value") or ""))
    )


def _compatible_backend_target(
    agent_record: _WorkerRecord,
    pane_record: _WorkerRecord,
) -> dict[str, Any] | None:
    """Retain only verified agent-scoped targets; PaneInfo owns pane targets."""
    agent_target = agent_record.worker.backend_target
    pane_target = pane_record.worker.backend_target
    if not _backend_target_present(agent_target):
        return pane_target if _backend_target_present(pane_target) else None

    kind = str(agent_target.get("kind") or "")
    value = str(agent_target.get("value") or "")
    if (
        kind == "agent_id"
        and _backend_target_present(pane_target)
        and str(pane_target.get("kind") or "") == "agent_id"
        and str(pane_target.get("value") or "") != value
    ):
        return pane_target
    if kind in _AGENT_SCOPED_BACKEND_TARGET_KINDS and (
        kind != "agent" or value == pane_record.worker.name
    ):
        return agent_target
    return pane_target if _backend_target_present(pane_target) else None


def _compatible_turn_target(
    agent_record: _WorkerRecord,
    pane_record: _WorkerRecord,
) -> tuple[str | None, str | None]:
    """Prefer PaneInfo targets unless a compatible session target is more precise."""
    agent_kind = agent_record.turn_target_kind
    agent_value = agent_record.turn_target_value
    pane_kind = pane_record.turn_target_kind
    pane_value = pane_record.turn_target_value
    if agent_kind in _SESSION_SCOPED_TURN_TARGET_KINDS and agent_value:
        pane_session_id = pane_record.agent_session_id
        if pane_session_id and pane_session_id != agent_value:
            if pane_kind and pane_value:
                return pane_kind, pane_value
            return None, None
        if pane_kind in _SESSION_SCOPED_TURN_TARGET_KINDS and pane_value:
            return pane_kind, pane_value
        return agent_kind, agent_value
    if pane_kind and pane_value:
        return pane_kind, pane_value
    return None, None


def _ambiguous_agent_record(
    record: _WorkerRecord,
    *,
    config: Config | None = None,
    observation: Mapping[str, Any] | None = None,
) -> _WorkerRecord:
    """Fail closed and retain a unique auditable row for ambiguous ownership."""
    worker = record.worker
    target = worker.backend_target
    if _backend_target_present(target):
        worker = _worker_with_backend_target(
            worker,
            _private_backend_target(
                str(target.get("kind") or ""),
                str(target.get("value") or ""),
                sendable=False,
                reason="ambiguous_pane_match",
            ),
        )
    private_fingerprint = record.private_fingerprint
    if config is not None and observation is not None:
        private_fingerprint = worker_binding_private_fingerprint(
            host_id=config.host_id,
            backend=_BACKEND_NAME,
            identity_material={
                "ambiguous_pane_ownership": dict(observation),
                "source_private_fingerprint": record.private_fingerprint,
            },
        )
    return _WorkerRecord(
        worker=worker,
        private_fingerprint=private_fingerprint,
        workspace_id=record.workspace_id,
        pane_id=record.pane_id,
        observed_workspace_id=record.observed_workspace_id,
        observed_pane_id=record.observed_pane_id,
        identity_source=record.identity_source,
        terminal_id=record.terminal_id,
        agent_session_id=record.agent_session_id,
        pane_info_observed=False,
    )


def _merge_agent_pane_record(
    agent_record: _WorkerRecord,
    pane_record: _WorkerRecord | None,
) -> _WorkerRecord:
    if pane_record is None:
        return agent_record
    pane_meta = pane_record.worker.meta
    merged_meta = dict(agent_record.worker.meta)
    label = pane_meta.get("label")
    if isinstance(label, str) and label.strip():
        merged_meta["label"] = label
    for key in ("foreground_cwd", "cwd"):
        value = pane_meta.get(key)
        if isinstance(value, str) and value.strip() and not merged_meta.get(key):
            merged_meta[key] = value

    backend_target = _compatible_backend_target(agent_record, pane_record)
    worker = agent_record.worker
    pane_space_id = pane_record.worker.space_id
    if (
        merged_meta != worker.meta
        or backend_target != worker.backend_target
        or pane_space_id != worker.space_id
    ):
        worker = Worker(
            id=worker.id,
            name=worker.name,
            status=worker.status,
            space_id=pane_space_id,
            meta=merged_meta,
            last_seen_at=worker.last_seen_at,
            summary=worker.summary,
            backend_target=backend_target,
        )

    turn_target_kind, turn_target_value = _compatible_turn_target(
        agent_record,
        pane_record,
    )

    workspace_id = pane_record.workspace_id
    pane_id = pane_record.pane_id
    terminal_id = pane_record.terminal_id
    if (
        worker == agent_record.worker
        and turn_target_kind == agent_record.turn_target_kind
        and turn_target_value == agent_record.turn_target_value
        and workspace_id == agent_record.workspace_id
        and pane_id == agent_record.pane_id
        and terminal_id == agent_record.terminal_id
        and agent_record.pane_info_observed
    ):
        return agent_record
    return _WorkerRecord(
        worker=worker,
        private_fingerprint=agent_record.private_fingerprint,
        turn_target_kind=turn_target_kind,
        turn_target_value=turn_target_value,
        workspace_id=workspace_id,
        pane_id=pane_id,
        observed_workspace_id=pane_record.observed_workspace_id,
        observed_pane_id=pane_record.observed_pane_id,
        identity_source=pane_record.identity_source,
        terminal_id=terminal_id,
        agent_session_id=pane_record.agent_session_id,
        pane_info_observed=True,
    )


def _collapse_exact_observation_items(
    items: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Collapse byte-equivalent JSON observations before ownership cardinality."""
    seen: set[str] = set()
    unique: list[dict[str, Any]] = []
    for item in items:
        signature = json.dumps(
            item,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        )
        if signature in seen:
            continue
        seen.add(signature)
        unique.append(dict(item))
    return unique


def _record_ownership_keys(
    item: Mapping[str, Any],
    record: _WorkerRecord,
    *,
    pane_shaped: bool = False,
) -> set[tuple[str, str]]:
    keys = set(
        _record_match_keys(
            item,
            record,
            pane_shaped=pane_shaped,
        )
    )
    backend_target = record.worker.backend_target
    if _backend_target_present(backend_target):
        keys.add(
            (
                f"backend:{backend_target.get('kind')}",
                str(backend_target.get("value") or ""),
            )
        )
        keys.add(
            (
                "backend_send_token",
                str(backend_target.get("value") or ""),
            )
        )
    if record.turn_target_kind and record.turn_target_value:
        keys.add(
            (
                f"turn:{record.turn_target_kind}",
                record.turn_target_value,
            )
        )
    return keys


def _pane_ownership_graph(
    agent_items: Sequence[Mapping[str, Any]],
    agent_records: Sequence[_WorkerRecord],
    pane_items: Sequence[Mapping[str, Any]],
    pane_records: Sequence[_WorkerRecord],
) -> tuple[list[set[int]], set[int], set[int]]:
    """Return agent-to-pane edges and every ambiguous ownership component."""
    agent_nodes = [("agent", index) for index in range(len(agent_records))]
    pane_nodes = [("pane", index) for index in range(len(pane_records))]
    adjacency: dict[tuple[str, int], set[tuple[str, int]]] = {
        node: set() for node in [*agent_nodes, *pane_nodes]
    }
    ambiguous_seeds: set[tuple[str, int]] = set()

    pane_indices_by_match: dict[tuple[str, str], set[int]] = {}
    pane_indices_by_owner: dict[tuple[str, str], set[int]] = {}
    for pane_index, (item, record) in enumerate(
        zip(pane_items, pane_records, strict=True)
    ):
        match_keys = _record_match_keys(item, record, pane_shaped=True)
        for key in match_keys:
            pane_indices_by_match.setdefault(key, set()).add(pane_index)
        for key in _record_ownership_keys(item, record, pane_shaped=True):
            pane_indices_by_owner.setdefault(key, set()).add(pane_index)

    # Each private pane, terminal, session, or agent identity may own only one
    # PaneInfo row. Exact duplicate rows were already collapsed above.
    for pane_indices in pane_indices_by_owner.values():
        if len(pane_indices) < 2:
            continue
        ordered = sorted(pane_indices)
        first_node = ("pane", ordered[0])
        ambiguous_seeds.update(("pane", index) for index in ordered)
        for pane_index in ordered[1:]:
            pane_node = ("pane", pane_index)
            adjacency[first_node].add(pane_node)
            adjacency[pane_node].add(first_node)

    agent_matches: list[set[int]] = []
    pane_claimants: dict[int, set[int]] = {}
    agent_indices_by_owner: dict[tuple[str, str], set[int]] = {}
    for agent_index, (item, record) in enumerate(
        zip(agent_items, agent_records, strict=True)
    ):
        matches: set[int] = set()
        for key in _record_match_keys(item, record):
            matches.update(pane_indices_by_match.get(key, ()))
        agent_matches.append(matches)
        agent_node = ("agent", agent_index)
        owner_keys = _record_ownership_keys(item, record)
        for key in owner_keys:
            agent_indices_by_owner.setdefault(key, set()).add(agent_index)
            if key[0] != "backend_send_token":
                continue
            for pane_index in pane_indices_by_owner.get(key, ()):
                if pane_index in matches:
                    continue
                pane_node = ("pane", pane_index)
                adjacency[agent_node].add(pane_node)
                adjacency[pane_node].add(agent_node)
                ambiguous_seeds.add(agent_node)
                ambiguous_seeds.add(pane_node)
        for pane_index in matches:
            pane_node = ("pane", pane_index)
            adjacency[agent_node].add(pane_node)
            adjacency[pane_node].add(agent_node)
            pane_claimants.setdefault(pane_index, set()).add(agent_index)
        if len(matches) > 1:
            ambiguous_seeds.add(agent_node)
            ambiguous_seeds.update(("pane", index) for index in matches)

    for agent_indices in agent_indices_by_owner.values():
        if len(agent_indices) < 2:
            continue
        if not any(agent_matches[index] for index in agent_indices):
            continue
        ordered = sorted(agent_indices)
        first_node = ("agent", ordered[0])
        ambiguous_seeds.update(("agent", index) for index in ordered)
        for agent_index in ordered[1:]:
            agent_node = ("agent", agent_index)
            adjacency[first_node].add(agent_node)
            adjacency[agent_node].add(first_node)

    for pane_index, claimants in pane_claimants.items():
        if len(claimants) < 2:
            continue
        ambiguous_seeds.add(("pane", pane_index))
        ambiguous_seeds.update(("agent", index) for index in claimants)

    ambiguous_nodes = set(ambiguous_seeds)
    pending = list(ambiguous_seeds)
    while pending:
        node = pending.pop()
        for adjacent in adjacency[node]:
            if adjacent in ambiguous_nodes:
                continue
            ambiguous_nodes.add(adjacent)
            pending.append(adjacent)

    return (
        agent_matches,
        {index for kind, index in ambiguous_nodes if kind == "agent"},
        {index for kind, index in ambiguous_nodes if kind == "pane"},
    )


def _records_from_agent_and_pane_payloads(
    config: Config | None,
    agent_payload: Any,
    pane_payload: Any,
    *,
    include_unmatched_panes: bool = True,
) -> list[_WorkerRecord]:
    """Merge PaneInfo identity only across a one-to-one ownership graph."""
    pane_items = _collapse_exact_observation_items(
        [
            item
            for item in _payload_items(
                pane_payload,
                ("panes", "items", "data", "results", "result"),
            )
            if _pane_has_agent(item)
        ]
    )
    pane_records = [
        _worker_record_from_item(
            item,
            config,
            pane_info_observed=True,
            identity_source="pane.list",
        )
        for item in pane_items
    ]
    agent_items = _collapse_exact_observation_items(
        _payload_items(
            agent_payload,
            ("agents", "workers", "data", "items", "results", "result"),
        )
    )
    agent_records = [
        _agent_observation_record(item, config)
        for item in agent_items
    ]
    agent_matches, ambiguous_agents, ambiguous_panes = _pane_ownership_graph(
        agent_items,
        agent_records,
        pane_items,
        pane_records,
    )

    records: list[_WorkerRecord] = []
    consumed_pane_indices: set[int] = set()
    for agent_index, record in enumerate(agent_records):
        matched_pane_indices = agent_matches[agent_index]
        consumed_pane_indices.update(matched_pane_indices)
        if agent_index in ambiguous_agents:
            records.append(
                _ambiguous_agent_record(
                    record,
                    config=config,
                    observation=agent_items[agent_index],
                )
            )
        elif len(matched_pane_indices) == 1:
            matched_pane_index = next(iter(matched_pane_indices))
            records.append(
                _merge_agent_pane_record(record, pane_records[matched_pane_index])
            )
        else:
            records.append(record)
    if include_unmatched_panes:
        records.extend(
            _ambiguous_agent_record(
                record,
                config=config,
                observation=pane_items[index],
            )
            if index in ambiguous_panes
            else record
            for index, record in enumerate(pane_records)
            if index not in consumed_pane_indices
        )
    return records


def _workers_from_pane_payload(
    payload: Any,
    config: Config | None = None,
    stored_bindings: Sequence[WorkerBinding] | None = None,
) -> list[Worker]:
    """Extract pane workers through the ownership graph."""
    records = _records_from_agent_and_pane_payloads(
        config,
        None,
        payload,
    )
    return _deduplicate_worker_records(records, stored_bindings)


def _workers_and_bindings_from_pane_payload(
    payload: Any,
    config: Config,
    stored_bindings: Sequence[WorkerBinding] | None = None,
) -> tuple[list[Worker], list[WorkerBinding]]:
    records = _records_from_agent_and_pane_payloads(
        config,
        None,
        payload,
    )
    return _workers_and_bindings_from_records(
        config,
        records,
        stored_bindings=stored_bindings,
    )


def _probe_payload_variants(
    variants: Sequence[Sequence[str]],
    config: Config,
    budget: _ProbeBudget | None = None,
) -> tuple[str, Any]:
    outcomes: list[str] = []
    for args in variants:
        if budget is None:
            outcome, payload = _probe_herdr(args, config)
        else:
            try:
                outcome, payload = _probe_herdr(args, config, budget)
            except TypeError:
                outcome, payload = _probe_herdr(args, config)
        if outcome == "ok":
            return outcome, payload
        if outcome in _DEADLINE_EXHAUSTED_OUTCOMES:
            return outcome, None
        outcomes.append(outcome)
    if outcomes and all(outcome == "launch_error" for outcome in outcomes):
        return "launch_error", None
    if "malformed_json" in outcomes:
        return "malformed_json", None
    if "nonzero" in outcomes:
        return "nonzero", None
    return outcomes[-1] if outcomes else "nonzero", None


def _degraded_observation(
    outcome: str,
    message: str,
    *,
    spaces: Sequence[Space] | None = None,
    workers: Sequence[Worker] | None = None,
) -> HerdrCommandObservation:
    observed_spaces = list(spaces or [])
    observed_workers = list(workers or [])
    health = herdr_backend_health(
        outcome,
        message=message,
        spaces=observed_spaces,
        workers=observed_workers,
    )
    return HerdrCommandObservation(
        spaces=observed_spaces,
        workers=observed_workers,
        status=health.status,
        outcome=health.outcome,
        message=health.message,
        backend_health=[health],
    )


def _snapshot_observation(
    spaces: list[Space],
    workers: list[Worker],
    bindings: list[WorkerBinding],
    outcome: str,
    *,
    message: str | None = None,
) -> HerdrSnapshotObservation:
    health = herdr_backend_health(
        outcome,
        message=message,
        spaces=spaces,
        workers=workers,
    )
    return HerdrSnapshotObservation(
        spaces=spaces,
        workers=workers,
        bindings=bindings,
        backend_health=[health],
    )


def fetch_herdr_command_observation(
    config: Config,
    stored_bindings: Sequence[WorkerBinding] | None = None,
) -> HerdrCommandObservation:
    """Return Herdr observations plus health metadata for mutation safety."""
    try:
        if shutil.which(config.herdr_bin) is None:
            return _degraded_observation("missing_binary", "Herdr binary is unavailable")
    except (TypeError, ValueError, OSError):
        return _degraded_observation("launch_error", "Herdr binary could not be inspected")

    budget = _ProbeBudget.from_config(config, planned_probes=5)
    workspace_outcome, workspace_payload = _probe_payload_variants(
        [
            ["workspace", "list"],
            ["workspace", "list", "--json"],
        ],
        config,
        budget,
    )
    if workspace_outcome != "ok":
        return _degraded_observation(
            workspace_outcome,
            "Herdr workspace observation is not healthy",
        )

    agent_outcome, agent_payload = _probe_payload_variants(
        [
            ["agent", "list"],
            ["agent", "list", "--json"],
        ],
        config,
        budget,
    )
    if agent_outcome != "ok":
        return _degraded_observation(
            agent_outcome,
            "Herdr agent observation is not healthy",
        )

    spaces = _spaces_from_payload(workspace_payload)
    try:
        pane_outcome, pane_payload = _probe_herdr(["pane", "list"], config, budget)
    except TypeError:
        pane_outcome, pane_payload = _probe_herdr(["pane", "list"], config)
    records = _records_from_agent_and_pane_payloads(
        config,
        agent_payload,
        pane_payload if pane_outcome == "ok" else None,
        include_unmatched_panes=not bool(
            _payload_items(
                agent_payload,
                ("agents", "workers", "data", "items", "results", "result"),
            )
        ),
    )
    try:
        workers, bindings = _workers_and_bindings_from_records(
            config,
            records,
            stored_bindings=stored_bindings,
            require_authenticated_continuity=pane_outcome == "ok",
        )
    except (HerdrContinuityUnavailableError, InstallationKeyError):
        return _degraded_observation(
            "continuity_unavailable",
            _HEALTH_MESSAGES["continuity_unavailable"],
            spaces=spaces,
        )
    if pane_outcome != "ok":
        return _degraded_observation(
            pane_outcome,
            "Herdr pane continuity observation is not healthy",
            spaces=spaces,
            workers=workers,
        )

    return HerdrCommandObservation(
        spaces=spaces,
        workers=workers,
        status="healthy",
        outcome="healthy_non_empty" if spaces or workers else "empty_healthy",
        bindings=bindings,
        backend_health=[
            herdr_backend_health(
                "healthy_non_empty" if spaces or workers else "empty_healthy",
                spaces=spaces,
                workers=workers,
            )
        ],
    )


def _state_result(
    spaces: list[Space],
    workers: list[Worker],
    bindings: list[WorkerBinding],
    include_bindings: bool,
) -> tuple[list[Space], list[Worker]] | tuple[list[Space], list[Worker], list[WorkerBinding]]:
    if include_bindings:
        return spaces, workers, bindings
    return spaces, workers


def fetch_herdr_snapshot_observation(
    config: Config,
    stored_bindings: Sequence[WorkerBinding] | None = None,
    *,
    require_authenticated_continuity: bool = True,
) -> HerdrSnapshotObservation:
    """Return Herdr snapshot observations plus public backend health."""
    try:
        if shutil.which(config.herdr_bin) is None:
            return _snapshot_observation(
                [],
                [],
                [],
                "missing_binary",
                message=_HEALTH_MESSAGES["missing_binary"],
            )
    except (TypeError, ValueError, OSError):
        return _snapshot_observation(
            [],
            [],
            [],
            "launch_error",
            message="Herdr binary could not be inspected",
        )

    budget = _ProbeBudget.from_config(config, planned_probes=5)
    workspace_outcome, workspace_payload = _probe_payload_variants(
        [
            ["workspace", "list"],
            ["workspace", "list", "--json"],
        ],
        config,
        budget,
    )
    if workspace_outcome in _DEADLINE_EXHAUSTED_OUTCOMES:
        return _snapshot_observation(
            [],
            [],
            [],
            workspace_outcome,
            message=_HEALTH_MESSAGES[workspace_outcome],
        )

    agent_outcome, agent_payload = _probe_payload_variants(
        [
            ["agent", "list"],
            ["agent", "list", "--json"],
        ],
        config,
        budget,
    )

    spaces = _spaces_from_payload(workspace_payload)
    if agent_outcome in _DEADLINE_EXHAUSTED_OUTCOMES:
        return _snapshot_observation(
            spaces,
            [],
            [],
            agent_outcome,
            message=_HEALTH_MESSAGES[agent_outcome],
        )

    try:
        pane_outcome, pane_payload = _probe_herdr(["pane", "list"], config, budget)
    except TypeError:
        pane_outcome, pane_payload = _probe_herdr(["pane", "list"], config)
    records = _records_from_agent_and_pane_payloads(
        config,
        agent_payload,
        pane_payload if pane_outcome == "ok" else None,
        include_unmatched_panes=not bool(
            _payload_items(
                agent_payload,
                ("agents", "workers", "data", "items", "results", "result"),
            )
        ),
    )
    try:
        workers, bindings = _workers_and_bindings_from_records(
            config,
            records,
            stored_bindings=stored_bindings,
            require_authenticated_continuity=(
                require_authenticated_continuity and pane_outcome == "ok"
            ),
        )
    except (HerdrContinuityUnavailableError, InstallationKeyError):
        return _snapshot_observation(
            spaces,
            [],
            [],
            "continuity_unavailable",
            message=_HEALTH_MESSAGES["continuity_unavailable"],
        )

    failed_outcomes = [
        outcome
        for outcome in (workspace_outcome, agent_outcome, pane_outcome)
        if outcome not in {"ok"}
    ]
    if failed_outcomes:
        outcome = failed_outcomes[0]
        return _snapshot_observation(
            spaces,
            workers,
            bindings,
            outcome,
            message=_HEALTH_MESSAGES.get(outcome, _HEALTH_MESSAGES["unknown"]),
        )

    return _snapshot_observation(
        spaces,
        workers,
        bindings,
        "healthy_non_empty" if spaces or workers else "empty_healthy",
    )


def fetch_herdr_state(
    config: Config,
    stored_bindings: Sequence[WorkerBinding] | None = None,
    *,
    include_bindings: bool = False,
) -> tuple[list[Space], list[Worker]] | tuple[list[Space], list[Worker], list[WorkerBinding]]:
    """Return neutral spaces and workers from the Herdr CLI, or empty lists."""
    observation = fetch_herdr_snapshot_observation(
        config,
        stored_bindings=stored_bindings,
        require_authenticated_continuity=False,
    )
    return _state_result(observation.spaces, observation.workers, observation.bindings, include_bindings)
