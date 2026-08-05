"""Tendwire runtime configuration.

Loads settings from a simple defaults + optional environment override set.
No external config-file parser is required.
"""

from __future__ import annotations

import math
import os
import platform
import socket
from dataclasses import dataclass, field
from pathlib import Path

ACP_THOUGHT_POLICIES = frozenset({"disabled", "private_summary", "private_all"})
DEFAULT_ACP_THOUGHT_POLICY = "disabled"
DEFAULT_ACP_REQUEST_TIMEOUT_SECONDS = 30.0
DEFAULT_ACP_SHUTDOWN_TIMEOUT_SECONDS = 5.0
DEFAULT_ACP_MAX_FRAME_BYTES = 8 * 1024 * 1024
DEFAULT_RECONCILE_INTERVAL_SECONDS = 15.0
DEFAULT_EVENT_RETENTION_DAYS = 7
DEFAULT_SUBMISSION_LINK_WINDOW_SECONDS = 60
DEFAULT_SUBMISSION_HARD_TTL_SECONDS = 86_400
DEFAULT_MAX_OUTBOX_ATTEMPTS = 10
DEFAULT_CONNECTOR_CLAIM_TTL_SECONDS = 60
DEFAULT_CONNECTOR_MAX_CLAIM_TTL_SECONDS = 300
DEFAULT_CONNECTOR_ACK_TTL_SECONDS = 60
DEFAULT_ACKNOWLEDGED_FINAL_RETENTION_DAYS = 30
DEFAULT_COMMAND_RECEIPT_RETENTION_SECONDS = 2_592_000
MIN_COMMAND_RECEIPT_RETENTION_SECONDS = 691_200
DEFAULT_SNAPSHOT_RETENTION_DAYS = 14
DEFAULT_SNAPSHOT_MAINTENANCE_BATCH_SIZE = 100
DEFAULT_STORE_MAINTENANCE_CADENCE_SECONDS = 3600
DEFAULT_TURN_CHANGE_RETENTION_DAYS = 7
DEFAULT_TURN_CHANGE_COMPACTION_BATCH_SIZE = 1_000
MAX_SNAPSHOT_MAINTENANCE_BATCH_SIZE = 1000
MAX_RETENTION_DAYS = 365_000
MAX_MAINTENANCE_CADENCE_SECONDS = MAX_RETENTION_DAYS * 24 * 60 * 60

_POSITIVE_FLOAT_FIELDS = (
    "acp_request_timeout_seconds",
    "acp_shutdown_timeout_seconds",
    "reconcile_interval_seconds",
)
_POSITIVE_INT_FIELDS = (
    "max_outbox_attempts",
    "connector_claim_ttl_seconds",
    "connector_max_claim_ttl_seconds",
)
_BOUNDED_INT_FIELDS = {
    "acp_max_frame_bytes": 64 * 1024 * 1024,
    "event_retention_days": MAX_RETENTION_DAYS,
    "submission_link_window_seconds": MAX_MAINTENANCE_CADENCE_SECONDS,
    "submission_hard_ttl_seconds": MAX_MAINTENANCE_CADENCE_SECONDS,
    "connector_ack_ttl_seconds": 86_400,
    "acknowledged_final_retention_days": MAX_RETENTION_DAYS,
    "command_receipt_retention_seconds": MAX_MAINTENANCE_CADENCE_SECONDS,
    "snapshot_retention_days": MAX_RETENTION_DAYS,
    "snapshot_maintenance_batch_size": MAX_SNAPSHOT_MAINTENANCE_BATCH_SIZE,
    "store_maintenance_cadence_seconds": MAX_MAINTENANCE_CADENCE_SECONDS,
    "turn_change_retention_days": MAX_RETENTION_DAYS,
    "turn_change_compaction_batch_size": 10_000,
}
_LOAD_DEFAULTS = {
    "acp_thought_policy": DEFAULT_ACP_THOUGHT_POLICY,
    "acp_request_timeout_seconds": DEFAULT_ACP_REQUEST_TIMEOUT_SECONDS,
    "acp_shutdown_timeout_seconds": DEFAULT_ACP_SHUTDOWN_TIMEOUT_SECONDS,
    "acp_max_frame_bytes": DEFAULT_ACP_MAX_FRAME_BYTES,
    "reconcile_interval_seconds": DEFAULT_RECONCILE_INTERVAL_SECONDS,
    "event_retention_days": DEFAULT_EVENT_RETENTION_DAYS,
    "submission_link_window_seconds": DEFAULT_SUBMISSION_LINK_WINDOW_SECONDS,
    "submission_hard_ttl_seconds": DEFAULT_SUBMISSION_HARD_TTL_SECONDS,
    "max_outbox_attempts": DEFAULT_MAX_OUTBOX_ATTEMPTS,
    "connector_claim_ttl_seconds": DEFAULT_CONNECTOR_CLAIM_TTL_SECONDS,
    "connector_max_claim_ttl_seconds": DEFAULT_CONNECTOR_MAX_CLAIM_TTL_SECONDS,
    "connector_ack_ttl_seconds": DEFAULT_CONNECTOR_ACK_TTL_SECONDS,
    "acknowledged_final_retention_days": DEFAULT_ACKNOWLEDGED_FINAL_RETENTION_DAYS,
    "command_receipt_retention_seconds": DEFAULT_COMMAND_RECEIPT_RETENTION_SECONDS,
    "snapshot_retention_days": DEFAULT_SNAPSHOT_RETENTION_DAYS,
    "snapshot_maintenance_batch_size": DEFAULT_SNAPSHOT_MAINTENANCE_BATCH_SIZE,
    "store_maintenance_cadence_seconds": DEFAULT_STORE_MAINTENANCE_CADENCE_SECONDS,
    "turn_change_retention_days": DEFAULT_TURN_CHANGE_RETENTION_DAYS,
    "turn_change_compaction_batch_size": DEFAULT_TURN_CHANGE_COMPACTION_BATCH_SIZE,
}


@dataclass(frozen=True)
class Config:
    """Neutral runtime configuration for Tendwire."""

    host_id: str = field(default_factory=lambda: platform.node() or "unknown")
    herdr_bin: str = "herdr"
    data_dir: Path = field(default_factory=lambda: Path.home() / ".local" / "share" / "tendwire")
    db_path: Path | None = None
    socket_path: Path | None = None
    herdr_timeout_seconds: float = 5.0
    acp_thought_policy: str = DEFAULT_ACP_THOUGHT_POLICY
    acp_request_timeout_seconds: float = DEFAULT_ACP_REQUEST_TIMEOUT_SECONDS
    acp_shutdown_timeout_seconds: float = DEFAULT_ACP_SHUTDOWN_TIMEOUT_SECONDS
    acp_max_frame_bytes: int = DEFAULT_ACP_MAX_FRAME_BYTES
    reconcile_interval_seconds: float = DEFAULT_RECONCILE_INTERVAL_SECONDS
    event_retention_days: int = DEFAULT_EVENT_RETENTION_DAYS
    submission_link_window_seconds: int = DEFAULT_SUBMISSION_LINK_WINDOW_SECONDS
    submission_hard_ttl_seconds: int = DEFAULT_SUBMISSION_HARD_TTL_SECONDS
    max_outbox_attempts: int = DEFAULT_MAX_OUTBOX_ATTEMPTS
    connector_claim_ttl_seconds: int = DEFAULT_CONNECTOR_CLAIM_TTL_SECONDS
    connector_max_claim_ttl_seconds: int = DEFAULT_CONNECTOR_MAX_CLAIM_TTL_SECONDS
    connector_ack_ttl_seconds: int = DEFAULT_CONNECTOR_ACK_TTL_SECONDS
    acknowledged_final_retention_days: int = DEFAULT_ACKNOWLEDGED_FINAL_RETENTION_DAYS
    command_receipt_retention_seconds: int = DEFAULT_COMMAND_RECEIPT_RETENTION_SECONDS
    snapshot_retention_days: int = DEFAULT_SNAPSHOT_RETENTION_DAYS
    snapshot_maintenance_batch_size: int = DEFAULT_SNAPSHOT_MAINTENANCE_BATCH_SIZE
    store_maintenance_cadence_seconds: int = DEFAULT_STORE_MAINTENANCE_CADENCE_SECONDS
    turn_change_retention_days: int = DEFAULT_TURN_CHANGE_RETENTION_DAYS
    turn_change_compaction_batch_size: int = DEFAULT_TURN_CHANGE_COMPACTION_BATCH_SIZE
    socket_group: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "herdr_bin", os.path.expanduser(self.herdr_bin))
        object.__setattr__(self, "data_dir", Path(self.data_dir).expanduser())
        if self.db_path is None:
            object.__setattr__(
                self,
                "db_path",
                self.data_dir / "tendwire.db",
            )
        else:
            object.__setattr__(self, "db_path", Path(self.db_path).expanduser())
        if self.socket_path is not None:
            object.__setattr__(self, "socket_path", Path(self.socket_path).expanduser())
        if self.socket_group is not None:
            normalized_socket_group = str(self.socket_group).strip()
            object.__setattr__(self, "socket_group", normalized_socket_group or None)

        object.__setattr__(
            self,
            "herdr_timeout_seconds",
            _positive_finite_float(
                self.herdr_timeout_seconds,
                "herdr_timeout_seconds",
            ),
        )
        acp_thought_policy = str(self.acp_thought_policy or "").strip().lower()
        if acp_thought_policy not in ACP_THOUGHT_POLICIES:
            allowed = ", ".join(sorted(ACP_THOUGHT_POLICIES))
            raise ValueError(f"acp_thought_policy must be one of: {allowed}")
        object.__setattr__(self, "acp_thought_policy", acp_thought_policy)
        for name in _POSITIVE_FLOAT_FIELDS:
            object.__setattr__(
                self,
                name,
                _positive_finite_float(getattr(self, name), name),
            )
        for name in _POSITIVE_INT_FIELDS:
            object.__setattr__(
                self,
                name,
                _positive_int(getattr(self, name), name, minimum=1),
            )
        for name, maximum in _BOUNDED_INT_FIELDS.items():
            object.__setattr__(
                self,
                name,
                _bounded_positive_int(getattr(self, name), name, maximum=maximum),
            )
        if self.submission_hard_ttl_seconds < self.submission_link_window_seconds:
            raise ValueError(
                "submission_hard_ttl_seconds must be >= submission_link_window_seconds"
            )
        if (
            self.command_receipt_retention_seconds
            < MIN_COMMAND_RECEIPT_RETENTION_SECONDS
        ):
            raise ValueError(
                "command_receipt_retention_seconds must be >= "
                f"{MIN_COMMAND_RECEIPT_RETENTION_SECONDS}"
            )

    @property
    def installation_key_path(self) -> Path:
        """Private stable-worker installation key path."""
        return self.data_dir / "installation.key"

    @property
    def installation_key_marker_path(self) -> Path:
        """Nonsecret digest marker used to detect installation key loss."""
        return self.data_dir / "installation.key.sha256"

    @property
    def installation_key_sentinel_path(self) -> Path:
        """Nonsecret durable marker that the installation identity was initialized."""
        return self.data_dir / "installation.key.initialized"


def _positive_finite_float(value: float | str, name: str) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be a finite positive number") from exc
    if not math.isfinite(parsed) or parsed <= 0:
        raise ValueError(f"{name} must be a finite positive number")
    return parsed


def _positive_int(value: int | str, name: str, *, minimum: int = 1) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be an integer >= {minimum}") from exc
    if parsed < minimum:
        raise ValueError(f"{name} must be >= {minimum}")
    return parsed


def _bounded_positive_int(
    value: int | str,
    name: str,
    *,
    maximum: int | None = None,
) -> int:
    if isinstance(value, bool) or not isinstance(value, (int, str)):
        raise ValueError(f"{name} must be an integer >= 1")
    try:
        parsed = int(value)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer >= 1") from exc
    if parsed < 1:
        raise ValueError(f"{name} must be >= 1")
    if maximum is not None and parsed > maximum:
        raise ValueError(f"{name} must be <= {maximum}")
    return parsed


def _resolve_value(explicit: object, env_name: str, default: object) -> object:
    if explicit is not None:
        return explicit
    env_value = os.environ.get(env_name)
    if env_value is not None:
        return env_value
    return default


def load_config(
    *,
    host_id: str | None = None,
    herdr_bin: str | None = None,
    data_dir: str | Path | None = None,
    db_path: str | Path | None = None,
    socket_path: str | Path | None = None,
    socket_group: str | None = None,
    herdr_timeout_seconds: float | str | None = None,
    acp_thought_policy: str | None = None,
    acp_request_timeout_seconds: float | str | None = None,
    acp_shutdown_timeout_seconds: float | str | None = None,
    acp_max_frame_bytes: int | str | None = None,
    reconcile_interval_seconds: float | str | None = None,
    event_retention_days: int | str | None = None,
    submission_link_window_seconds: int | str | None = None,
    submission_hard_ttl_seconds: int | str | None = None,
    max_outbox_attempts: int | str | None = None,
    connector_claim_ttl_seconds: int | str | None = None,
    connector_max_claim_ttl_seconds: int | str | None = None,
    connector_ack_ttl_seconds: int | str | None = None,
    acknowledged_final_retention_days: int | str | None = None,
    command_receipt_retention_seconds: int | str | None = None,
    snapshot_retention_days: int | str | None = None,
    snapshot_maintenance_batch_size: int | str | None = None,
    store_maintenance_cadence_seconds: int | str | None = None,
    turn_change_retention_days: int | str | None = None,
    turn_change_compaction_batch_size: int | str | None = None,
) -> Config:
    """Build a Config from explicit args, then environment, then defaults."""
    explicit = locals()
    resolved: dict[str, object] = {
        name: _resolve_value(
            explicit[name],
            f"TENDWIRE_{name.upper()}",
            default,
        )
        for name, default in _LOAD_DEFAULTS.items()
    }
    resolved["host_id"] = host_id or os.environ.get("TENDWIRE_HOST_ID") or (
        platform.node() or "unknown"
    )
    resolved["herdr_bin"] = herdr_bin or os.environ.get("TENDWIRE_HERDR_BIN") or "herdr"
    resolved["data_dir"] = Path(
        _resolve_value(
            data_dir,
            "TENDWIRE_DATA_DIR",
            Path.home() / ".local" / "share" / "tendwire",
        )
    )
    for name, explicit_path in (("db_path", db_path), ("socket_path", socket_path)):
        raw_path = _resolve_value(explicit_path, f"TENDWIRE_{name.upper()}", None)
        resolved[name] = Path(raw_path) if raw_path is not None else None
    resolved["socket_group"] = _resolve_value(
        socket_group,
        "TENDWIRE_SOCKET_GROUP",
        None,
    )
    try:
        resolved["herdr_timeout_seconds"] = float(
            _resolve_value(
                herdr_timeout_seconds,
                "TENDWIRE_HERDR_TIMEOUT_SECONDS",
                5.0,
            )
        )
    except (TypeError, ValueError) as exc:
        raise ValueError("herdr timeout must be a positive number") from exc
    return Config(**resolved)  # type: ignore[arg-type]
