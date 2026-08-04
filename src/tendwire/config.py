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
ACP_CONSOLE_INPUT_POLICIES = frozenset({"preserve", "live_only"})
DEFAULT_TURN_MODEL = "observed"
DEFAULT_ACP_THOUGHT_POLICY = "disabled"
DEFAULT_ACP_CONSOLE_INPUT_POLICY = "preserve"
DEFAULT_ACP_REQUEST_TIMEOUT_SECONDS = 30.0
DEFAULT_ACP_SHUTDOWN_TIMEOUT_SECONDS = 5.0
DEFAULT_ACP_MAX_FRAME_BYTES = 8 * 1024 * 1024
DEFAULT_RECONCILE_INTERVAL_SECONDS = 15.0
DEFAULT_EVENT_RETENTION_DAYS = 7
DEFAULT_MAX_WORKERS = 512
DEFAULT_SUBMISSION_LINK_WINDOW_SECONDS = 60
DEFAULT_SUBMISSION_HARD_TTL_SECONDS = 86_400
DEFAULT_PENDING_STALE_GRACE_SECONDS = 30.0
DEFAULT_MAX_OUTBOX_ATTEMPTS = 10
DEFAULT_CONNECTOR_CLAIM_TTL_SECONDS = 60
DEFAULT_CONNECTOR_MAX_CLAIM_TTL_SECONDS = 300
DEFAULT_CONNECTOR_ACK_TTL_SECONDS = 60
DEFAULT_ACKNOWLEDGED_FINAL_RETENTION_DAYS = 30
DEFAULT_ACKNOWLEDGED_FINAL_RETENTION_COUNT = 4096
DEFAULT_COMMAND_RETRY_HORIZON_SECONDS = 604_800
DEFAULT_COMMAND_RECEIPT_RETENTION_SECONDS = 2_592_000
DEFAULT_COMMAND_RECEIPT_RETENTION_COUNT = 4096
MAX_COMMAND_RETRY_HORIZON_SECONDS = 604_800
MIN_COMMAND_RECEIPT_RETENTION_SECONDS = 691_200
DEFAULT_SNAPSHOT_RETENTION_DAYS = 14
DEFAULT_SNAPSHOT_RETENTION_COUNT = 4096
DEFAULT_SNAPSHOT_MAINTENANCE_BATCH_SIZE = 100
DEFAULT_STORE_MAINTENANCE_CADENCE_SECONDS = 3600
DEFAULT_TURN_CHANGE_RETENTION_DAYS = 7
DEFAULT_TURN_CHANGE_RETENTION_COUNT = 100_000
DEFAULT_TURN_CHANGE_COMPACTION_BATCH_SIZE = 1_000
MAX_SNAPSHOT_MAINTENANCE_BATCH_SIZE = 1000
MAX_RETENTION_DAYS = 365_000
MAX_SQLITE_INTEGER = (1 << 63) - 1
MAX_MAINTENANCE_CADENCE_SECONDS = MAX_RETENTION_DAYS * 24 * 60 * 60


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
    acp_console_input_policy: str = DEFAULT_ACP_CONSOLE_INPUT_POLICY
    acp_request_timeout_seconds: float = DEFAULT_ACP_REQUEST_TIMEOUT_SECONDS
    acp_shutdown_timeout_seconds: float = DEFAULT_ACP_SHUTDOWN_TIMEOUT_SECONDS
    acp_max_frame_bytes: int = DEFAULT_ACP_MAX_FRAME_BYTES
    reconcile_interval_seconds: float = DEFAULT_RECONCILE_INTERVAL_SECONDS
    event_retention_days: int = DEFAULT_EVENT_RETENTION_DAYS
    max_workers: int = DEFAULT_MAX_WORKERS
    submission_link_window_seconds: int = DEFAULT_SUBMISSION_LINK_WINDOW_SECONDS
    submission_hard_ttl_seconds: int = DEFAULT_SUBMISSION_HARD_TTL_SECONDS
    pending_stale_grace_seconds: float = DEFAULT_PENDING_STALE_GRACE_SECONDS
    max_outbox_attempts: int = DEFAULT_MAX_OUTBOX_ATTEMPTS
    connector_claim_ttl_seconds: int = DEFAULT_CONNECTOR_CLAIM_TTL_SECONDS
    connector_max_claim_ttl_seconds: int = DEFAULT_CONNECTOR_MAX_CLAIM_TTL_SECONDS
    connector_ack_ttl_seconds: int = DEFAULT_CONNECTOR_ACK_TTL_SECONDS
    acknowledged_final_retention_days: int = DEFAULT_ACKNOWLEDGED_FINAL_RETENTION_DAYS
    acknowledged_final_retention_count: int = DEFAULT_ACKNOWLEDGED_FINAL_RETENTION_COUNT
    command_retry_horizon_seconds: int = DEFAULT_COMMAND_RETRY_HORIZON_SECONDS
    command_receipt_retention_seconds: int = DEFAULT_COMMAND_RECEIPT_RETENTION_SECONDS
    command_receipt_retention_count: int = DEFAULT_COMMAND_RECEIPT_RETENTION_COUNT
    snapshot_retention_days: int = DEFAULT_SNAPSHOT_RETENTION_DAYS
    snapshot_retention_count: int = DEFAULT_SNAPSHOT_RETENTION_COUNT
    snapshot_maintenance_batch_size: int = DEFAULT_SNAPSHOT_MAINTENANCE_BATCH_SIZE
    store_maintenance_cadence_seconds: int = DEFAULT_STORE_MAINTENANCE_CADENCE_SECONDS
    turn_change_retention_days: int = DEFAULT_TURN_CHANGE_RETENTION_DAYS
    turn_change_retention_count: int = DEFAULT_TURN_CHANGE_RETENTION_COUNT
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
        acp_console_input_policy = str(
            self.acp_console_input_policy or ""
        ).strip().lower()
        if acp_console_input_policy not in ACP_CONSOLE_INPUT_POLICIES:
            allowed = ", ".join(sorted(ACP_CONSOLE_INPUT_POLICIES))
            raise ValueError(
                f"acp_console_input_policy must be one of: {allowed}"
            )
        object.__setattr__(
            self,
            "acp_console_input_policy",
            acp_console_input_policy,
        )
        object.__setattr__(
            self,
            "acp_request_timeout_seconds",
            _positive_finite_float(
                self.acp_request_timeout_seconds,
                "acp_request_timeout_seconds",
            ),
        )
        object.__setattr__(
            self,
            "acp_shutdown_timeout_seconds",
            _positive_finite_float(
                self.acp_shutdown_timeout_seconds,
                "acp_shutdown_timeout_seconds",
            ),
        )
        object.__setattr__(
            self,
            "acp_max_frame_bytes",
            _bounded_positive_int(
                self.acp_max_frame_bytes,
                "acp_max_frame_bytes",
                maximum=64 * 1024 * 1024,
            ),
        )
        object.__setattr__(
            self,
            "reconcile_interval_seconds",
            _non_negative_float(self.reconcile_interval_seconds, "reconcile_interval_seconds"),
        )
        object.__setattr__(
            self,
            "event_retention_days",
            _bounded_positive_int(
                self.event_retention_days,
                "event_retention_days",
                maximum=MAX_RETENTION_DAYS,
            ),
        )
        object.__setattr__(
            self,
            "max_workers",
            _positive_int(self.max_workers, "max_workers", minimum=1),
        )
        object.__setattr__(
            self,
            "submission_link_window_seconds",
            _bounded_positive_int(
                self.submission_link_window_seconds,
                "submission_link_window_seconds",
                maximum=MAX_MAINTENANCE_CADENCE_SECONDS,
            ),
        )
        object.__setattr__(
            self,
            "submission_hard_ttl_seconds",
            _bounded_positive_int(
                self.submission_hard_ttl_seconds,
                "submission_hard_ttl_seconds",
                maximum=MAX_MAINTENANCE_CADENCE_SECONDS,
            ),
        )
        if self.submission_hard_ttl_seconds < self.submission_link_window_seconds:
            raise ValueError(
                "submission_hard_ttl_seconds must be >= submission_link_window_seconds"
            )
        object.__setattr__(
            self,
            "pending_stale_grace_seconds",
            _positive_finite_float(
                self.pending_stale_grace_seconds,
                "pending_stale_grace_seconds",
            ),
        )
        object.__setattr__(
            self,
            "max_outbox_attempts",
            _positive_int(self.max_outbox_attempts, "max_outbox_attempts", minimum=1),
        )
        object.__setattr__(
            self,
            "connector_claim_ttl_seconds",
            _positive_int(
                self.connector_claim_ttl_seconds,
                "connector_claim_ttl_seconds",
                minimum=1,
            ),
        )
        object.__setattr__(
            self,
            "connector_max_claim_ttl_seconds",
            _positive_int(
                self.connector_max_claim_ttl_seconds,
                "connector_max_claim_ttl_seconds",
                minimum=1,
            ),
        )
        object.__setattr__(
            self,
            "connector_ack_ttl_seconds",
            _positive_int(
                self.connector_ack_ttl_seconds,
                "connector_ack_ttl_seconds",
                minimum=1,
            ),
        )
        object.__setattr__(
            self,
            "acknowledged_final_retention_days",
            _bounded_positive_int(
                self.acknowledged_final_retention_days,
                "acknowledged_final_retention_days",
                maximum=MAX_RETENTION_DAYS,
            ),
        )
        object.__setattr__(
            self,
            "acknowledged_final_retention_count",
            _bounded_positive_int(
                self.acknowledged_final_retention_count,
                "acknowledged_final_retention_count",
                maximum=MAX_SQLITE_INTEGER,
            ),
        )
        object.__setattr__(
            self,
            "command_retry_horizon_seconds",
            _bounded_positive_int(
                self.command_retry_horizon_seconds,
                "command_retry_horizon_seconds",
                maximum=MAX_COMMAND_RETRY_HORIZON_SECONDS,
            ),
        )
        object.__setattr__(
            self,
            "command_receipt_retention_seconds",
            _bounded_positive_int(
                self.command_receipt_retention_seconds,
                "command_receipt_retention_seconds",
                maximum=MAX_MAINTENANCE_CADENCE_SECONDS,
            ),
        )
        object.__setattr__(
            self,
            "command_receipt_retention_count",
            _bounded_positive_int(
                self.command_receipt_retention_count,
                "command_receipt_retention_count",
                maximum=MAX_SQLITE_INTEGER,
            ),
        )
        if self.command_receipt_retention_seconds <= self.command_retry_horizon_seconds:
            raise ValueError(
                "command_receipt_retention_seconds must be greater than "
                "command_retry_horizon_seconds"
            )
        if (
            self.command_receipt_retention_seconds
            < MIN_COMMAND_RECEIPT_RETENTION_SECONDS
        ):
            raise ValueError(
                "command_receipt_retention_seconds must be >= "
                f"{MIN_COMMAND_RECEIPT_RETENTION_SECONDS}"
            )
        object.__setattr__(
            self,
            "snapshot_retention_days",
            _bounded_positive_int(
                self.snapshot_retention_days,
                "snapshot_retention_days",
                maximum=MAX_RETENTION_DAYS,
            ),
        )
        object.__setattr__(
            self,
            "snapshot_retention_count",
            _bounded_positive_int(
                self.snapshot_retention_count,
                "snapshot_retention_count",
                maximum=MAX_SQLITE_INTEGER,
            ),
        )
        object.__setattr__(
            self,
            "snapshot_maintenance_batch_size",
            _bounded_positive_int(
                self.snapshot_maintenance_batch_size,
                "snapshot_maintenance_batch_size",
                maximum=MAX_SNAPSHOT_MAINTENANCE_BATCH_SIZE,
            ),
        )
        object.__setattr__(
            self,
            "store_maintenance_cadence_seconds",
            _bounded_positive_int(
                self.store_maintenance_cadence_seconds,
                "store_maintenance_cadence_seconds",
                maximum=MAX_MAINTENANCE_CADENCE_SECONDS,
            ),
        )
        object.__setattr__(
            self,
            "turn_change_retention_days",
            _bounded_positive_int(
                self.turn_change_retention_days,
                "turn_change_retention_days",
                maximum=MAX_RETENTION_DAYS,
            ),
        )
        object.__setattr__(
            self,
            "turn_change_retention_count",
            _bounded_positive_int(
                self.turn_change_retention_count,
                "turn_change_retention_count",
                maximum=MAX_SQLITE_INTEGER,
            ),
        )
        object.__setattr__(
            self,
            "turn_change_compaction_batch_size",
            _bounded_positive_int(
                self.turn_change_compaction_batch_size,
                "turn_change_compaction_batch_size",
                maximum=10_000,
            ),
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


def _non_negative_float(value: float | str, name: str) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be a non-negative number") from exc
    if parsed < 0:
        raise ValueError(f"{name} must be non-negative")
    return parsed


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
    acp_console_input_policy: str | None = None,
    acp_request_timeout_seconds: float | str | None = None,
    acp_shutdown_timeout_seconds: float | str | None = None,
    acp_max_frame_bytes: int | str | None = None,
    reconcile_interval_seconds: float | str | None = None,
    event_retention_days: int | str | None = None,
    max_workers: int | str | None = None,
    submission_link_window_seconds: int | str | None = None,
    submission_hard_ttl_seconds: int | str | None = None,
    pending_stale_grace_seconds: float | str | None = None,
    max_outbox_attempts: int | str | None = None,
    connector_claim_ttl_seconds: int | str | None = None,
    connector_max_claim_ttl_seconds: int | str | None = None,
    connector_ack_ttl_seconds: int | str | None = None,
    acknowledged_final_retention_days: int | str | None = None,
    acknowledged_final_retention_count: int | str | None = None,
    command_retry_horizon_seconds: int | str | None = None,
    command_receipt_retention_seconds: int | str | None = None,
    command_receipt_retention_count: int | str | None = None,
    snapshot_retention_days: int | str | None = None,
    snapshot_retention_count: int | str | None = None,
    snapshot_maintenance_batch_size: int | str | None = None,
    store_maintenance_cadence_seconds: int | str | None = None,
    turn_change_retention_days: int | str | None = None,
    turn_change_retention_count: int | str | None = None,
    turn_change_compaction_batch_size: int | str | None = None,
) -> Config:
    """Build a Config from explicit args, then environment, then defaults."""
    env_host_id = os.environ.get("TENDWIRE_HOST_ID")
    env_herdr_bin = os.environ.get("TENDWIRE_HERDR_BIN")
    env_data_dir = os.environ.get("TENDWIRE_DATA_DIR")
    env_db_path = os.environ.get("TENDWIRE_DB_PATH")
    env_socket_path = os.environ.get("TENDWIRE_SOCKET_PATH")
    env_socket_group = os.environ.get("TENDWIRE_SOCKET_GROUP")
    env_herdr_timeout_seconds = os.environ.get("TENDWIRE_HERDR_TIMEOUT_SECONDS")

    resolved_host_id = host_id or env_host_id or (platform.node() or "unknown")
    resolved_herdr_bin = herdr_bin or env_herdr_bin or "herdr"

    if db_path is not None:
        resolved_db_path = Path(db_path)
    elif env_db_path is not None:
        resolved_db_path = Path(env_db_path)
    else:
        resolved_db_path = None

    if data_dir is not None:
        resolved_data_dir = Path(data_dir)
    elif env_data_dir is not None:
        resolved_data_dir = Path(env_data_dir)
    else:
        resolved_data_dir = Path.home() / ".local" / "share" / "tendwire"

    if socket_path is not None:
        resolved_socket_path = Path(socket_path)
    elif env_socket_path is not None:
        resolved_socket_path = Path(env_socket_path)
    else:
        resolved_socket_path = None
    if socket_group is not None:
        resolved_socket_group = socket_group
    else:
        resolved_socket_group = env_socket_group

    raw_timeout = herdr_timeout_seconds
    if raw_timeout is None:
        raw_timeout = env_herdr_timeout_seconds
    if raw_timeout is None:
        resolved_herdr_timeout_seconds = 5.0
    else:
        try:
            resolved_herdr_timeout_seconds = float(raw_timeout)
        except (TypeError, ValueError) as exc:
            raise ValueError("herdr timeout must be a positive number") from exc

    return Config(
        host_id=resolved_host_id,
        herdr_bin=resolved_herdr_bin,
        data_dir=resolved_data_dir,
        db_path=resolved_db_path,
        socket_path=resolved_socket_path,
        herdr_timeout_seconds=resolved_herdr_timeout_seconds,
        acp_thought_policy=_resolve_value(
            acp_thought_policy,
            "TENDWIRE_ACP_THOUGHT_POLICY",
            DEFAULT_ACP_THOUGHT_POLICY,
        ),
        acp_console_input_policy=_resolve_value(
            acp_console_input_policy,
            "TENDWIRE_ACP_CONSOLE_INPUT_POLICY",
            DEFAULT_ACP_CONSOLE_INPUT_POLICY,
        ),
        acp_request_timeout_seconds=_resolve_value(
            acp_request_timeout_seconds,
            "TENDWIRE_ACP_REQUEST_TIMEOUT_SECONDS",
            DEFAULT_ACP_REQUEST_TIMEOUT_SECONDS,
        ),
        acp_shutdown_timeout_seconds=_resolve_value(
            acp_shutdown_timeout_seconds,
            "TENDWIRE_ACP_SHUTDOWN_TIMEOUT_SECONDS",
            DEFAULT_ACP_SHUTDOWN_TIMEOUT_SECONDS,
        ),
        acp_max_frame_bytes=_resolve_value(
            acp_max_frame_bytes,
            "TENDWIRE_ACP_MAX_FRAME_BYTES",
            DEFAULT_ACP_MAX_FRAME_BYTES,
        ),
        reconcile_interval_seconds=_resolve_value(
            reconcile_interval_seconds,
            "TENDWIRE_RECONCILE_INTERVAL_SECONDS",
            DEFAULT_RECONCILE_INTERVAL_SECONDS,
        ),
        event_retention_days=_resolve_value(
            event_retention_days,
            "TENDWIRE_EVENT_RETENTION_DAYS",
            DEFAULT_EVENT_RETENTION_DAYS,
        ),
        max_workers=_resolve_value(
            max_workers,
            "TENDWIRE_MAX_WORKERS",
            DEFAULT_MAX_WORKERS,
        ),
        submission_link_window_seconds=_resolve_value(
            submission_link_window_seconds,
            "TENDWIRE_SUBMISSION_LINK_WINDOW_SECONDS",
            DEFAULT_SUBMISSION_LINK_WINDOW_SECONDS,
        ),
        submission_hard_ttl_seconds=_resolve_value(
            submission_hard_ttl_seconds,
            "TENDWIRE_SUBMISSION_HARD_TTL_SECONDS",
            DEFAULT_SUBMISSION_HARD_TTL_SECONDS,
        ),
        pending_stale_grace_seconds=_resolve_value(
            pending_stale_grace_seconds,
            "TENDWIRE_PENDING_STALE_GRACE_SECONDS",
            DEFAULT_PENDING_STALE_GRACE_SECONDS,
        ),
        max_outbox_attempts=_resolve_value(
            max_outbox_attempts,
            "TENDWIRE_MAX_OUTBOX_ATTEMPTS",
            DEFAULT_MAX_OUTBOX_ATTEMPTS,
        ),
        connector_claim_ttl_seconds=_resolve_value(
            connector_claim_ttl_seconds,
            "TENDWIRE_CONNECTOR_CLAIM_TTL_SECONDS",
            DEFAULT_CONNECTOR_CLAIM_TTL_SECONDS,
        ),
        connector_max_claim_ttl_seconds=_resolve_value(
            connector_max_claim_ttl_seconds,
            "TENDWIRE_CONNECTOR_MAX_CLAIM_TTL_SECONDS",
            DEFAULT_CONNECTOR_MAX_CLAIM_TTL_SECONDS,
        ),
        connector_ack_ttl_seconds=_resolve_value(
            connector_ack_ttl_seconds,
            "TENDWIRE_CONNECTOR_ACK_TTL_SECONDS",
            DEFAULT_CONNECTOR_ACK_TTL_SECONDS,
        ),
        acknowledged_final_retention_days=_resolve_value(
            acknowledged_final_retention_days,
            "TENDWIRE_ACKNOWLEDGED_FINAL_RETENTION_DAYS",
            DEFAULT_ACKNOWLEDGED_FINAL_RETENTION_DAYS,
        ),
        acknowledged_final_retention_count=_resolve_value(
            acknowledged_final_retention_count,
            "TENDWIRE_ACKNOWLEDGED_FINAL_RETENTION_COUNT",
            DEFAULT_ACKNOWLEDGED_FINAL_RETENTION_COUNT,
        ),
        command_retry_horizon_seconds=_resolve_value(
            command_retry_horizon_seconds,
            "TENDWIRE_COMMAND_RETRY_HORIZON_SECONDS",
            DEFAULT_COMMAND_RETRY_HORIZON_SECONDS,
        ),
        command_receipt_retention_seconds=_resolve_value(
            command_receipt_retention_seconds,
            "TENDWIRE_COMMAND_RECEIPT_RETENTION_SECONDS",
            DEFAULT_COMMAND_RECEIPT_RETENTION_SECONDS,
        ),
        command_receipt_retention_count=_resolve_value(
            command_receipt_retention_count,
            "TENDWIRE_COMMAND_RECEIPT_RETENTION_COUNT",
            DEFAULT_COMMAND_RECEIPT_RETENTION_COUNT,
        ),
        snapshot_retention_days=_resolve_value(
            snapshot_retention_days,
            "TENDWIRE_SNAPSHOT_RETENTION_DAYS",
            DEFAULT_SNAPSHOT_RETENTION_DAYS,
        ),
        snapshot_retention_count=_resolve_value(
            snapshot_retention_count,
            "TENDWIRE_SNAPSHOT_RETENTION_COUNT",
            DEFAULT_SNAPSHOT_RETENTION_COUNT,
        ),
        snapshot_maintenance_batch_size=_resolve_value(
            snapshot_maintenance_batch_size,
            "TENDWIRE_SNAPSHOT_MAINTENANCE_BATCH_SIZE",
            DEFAULT_SNAPSHOT_MAINTENANCE_BATCH_SIZE,
        ),
        store_maintenance_cadence_seconds=_resolve_value(
            store_maintenance_cadence_seconds,
            "TENDWIRE_STORE_MAINTENANCE_CADENCE_SECONDS",
            DEFAULT_STORE_MAINTENANCE_CADENCE_SECONDS,
        ),
        turn_change_retention_days=_resolve_value(
            turn_change_retention_days,
            "TENDWIRE_TURN_CHANGE_RETENTION_DAYS",
            DEFAULT_TURN_CHANGE_RETENTION_DAYS,
        ),
        turn_change_retention_count=_resolve_value(
            turn_change_retention_count,
            "TENDWIRE_TURN_CHANGE_RETENTION_COUNT",
            DEFAULT_TURN_CHANGE_RETENTION_COUNT,
        ),
        turn_change_compaction_batch_size=_resolve_value(
            turn_change_compaction_batch_size,
            "TENDWIRE_TURN_CHANGE_COMPACTION_BATCH_SIZE",
            DEFAULT_TURN_CHANGE_COMPACTION_BATCH_SIZE,
        ),
        socket_group=resolved_socket_group,
    )
