"""Local-first sqlite persistence for canonical Tendwire snapshots.

The CLI snapshot path works without requiring a live store. This module is
provided for optional persistence and is kept intentionally stdlib-only.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import logging
import math
import os
import secrets
import sqlite3
import stat
import time
import weakref
import threading
from collections.abc import Callable, Collection, Iterable, Iterator, Mapping
from contextlib import contextmanager, nullcontext
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Literal
from urllib.parse import parse_qsl, quote, urlsplit

from ..config import (
    DEFAULT_CONNECTOR_ACK_TTL_SECONDS,
    DEFAULT_PENDING_STALE_GRACE_SECONDS,
    DEFAULT_SUBMISSION_HARD_TTL_SECONDS,
    DEFAULT_SUBMISSION_LINK_WINDOW_SECONDS,
)
from ..local_state import (
    EntryIdentity,
    EntryType,
    LocalStateError,
    LocalStateErrorCode,
    PermissionState,
    _SQLiteFamilyMemberSnapshot,
    _snapshot_sqlite_family_at,
    canonical_path_from_fd,
    cleanup_private_sqlite_replacement_at,
    create_private_file_at,
    entry_identity,
    identity_matches,
    inspect_private_file_at,
    local_state_error,
    open_resolved_parent,
    prepare_private_sqlite_replacement_at,
    prepare_resolved_private_sqlite_parent,
    prepare_sqlite_family_at,
    private_file_creation_umask,
    publish_private_sqlite_replacement_at,
    release_private_sqlite_replacement_at,
    sqlite_parent_available_bytes_at,
    validate_owned_directory_stat,
    validate_owned_regular_stat,
    verify_created_private_sqlite_replacement_at,
    verify_entry_identity,
)
from ..core.agent_events import (
    AGENT_EVENT_KINDS,
    AGENT_EVENT_QUERY_DEFAULT_LIMIT,
    AGENT_EVENT_QUERY_MAX_LIMIT,
    AgentEvent,
    AgentEventIdentityConflict,
    AppendAgentEventResult,
    AppendBoundAgentEventResult,
    StoredAgentEvent,
    agent_event,
    normalize_agent_event_identifier,
)
from ..core.commands import (
    CommandEnvelope,
    instruction_fingerprint,
    is_selector_proof,
    normalize_instruction_text,
    turn_submission_id,
    validate_instruction_text,
)
from ..core.models import (
    FINGERPRINT_HEX_CHARS,
    SCHEMA_VERSION,
    Snapshot,
    WorkerBinding,
    normalize_severity,
    separate_duplicate_worker_bindings,
    sanitize_canonical_turn_text,
    sanitize_public_mapping,
    sanitize_public_text,
    sanitize_public_value,
    stable_fingerprint,
    utc_timestamp,
)
from ..core.turns import (
    PendingInteraction,
    PendingObservation,
    PendingObservedChoice,
    Turn,
    TURN_CONTENT_PAGE_MAX_UTF8_BYTES,
    TURN_CONTENT_PREVIEW_MAX_CHARS,
    TURN_DELTA_BOOTSTRAP_MAX_PAGES,
    TURN_DELTA_BOOTSTRAP_MAX_ROWS,
    TURN_DELTA_CURSOR_TTL_SECONDS,
    TURN_DELTA_DEFAULT_LIMIT,
    TURN_DELTA_MAX_BATCH_SEQUENCES,
    TURN_DELTA_MAX_LIMIT,
    TURN_DELTA_PROJECTION_SCHEMA_VERSION,
    TURN_DELTA_SCHEMA_VERSION,
    TURN_LIST_CURSOR_TTL_SECONDS,
    TURN_LIST_DEFAULT_LIMIT,
    TURN_LIST_MAX_LIMIT,
    TURN_LIST_SCHEMA_VERSION,
    TURN_SCHEMA_VERSION,
    TURN_TEXT_MAX_CHARS,
    ContentCursorPosition,
    content_cursor,
    content_revision,
    content_segment_id,
    decode_content_cursor,
    decode_turn_delta_cursor,
    decode_turn_delta_watermark,
    decode_turn_list_cursor,
    decode_turn_since_token,
    is_internal_automation_turn_payload,
    pending_from_snapshot,
    pending_payload_from_snapshot,
    project_persisted_turn_content,
    project_turn_content,
    recompute_pending_content_fingerprint,
    segment_canonical_text,
    turn_final_delivery_identity,
    turn_delta_cursor,
    turn_delta_watermark,
    turn_list_cursor,
    turn_since_token,
    turn_source_id_candidates,
)


FINGERPRINT_HEX_LENGTH = FINGERPRINT_HEX_CHARS
STORE_SCHEMA_VERSION = 29
CONNECTOR_ACK_TTL_SECONDS = DEFAULT_CONNECTOR_ACK_TTL_SECONDS
TURN_CHANGE_RETENTION_DAYS = 7
TURN_CHANGE_RETENTION_COUNT = 100_000
TURN_CHANGE_COMPACTION_BATCH_SIZE = 1_000
TURN_SUBMISSION_OBSERVATION_ADOPTION_WINDOW_SECONDS = 60.0
SUBMISSION_LINK_WINDOW_SECONDS = DEFAULT_SUBMISSION_LINK_WINDOW_SECONDS
SUBMISSION_HARD_TTL_SECONDS = DEFAULT_SUBMISSION_HARD_TTL_SECONDS
# A send that has not produced an authoritative accepted/uncertain receipt
# within the backend command timeout is no longer safe for instant linking.
# Keep this classification local to the observation linker: receipts and the
# submission lifecycle remain authoritative and unchanged.
SUBMISSION_SEND_ACK_TIMEOUT_SECONDS = 5.0
TURN_LEDGER_BACKFILL_BATCH_SIZE = 500
ACKNOWLEDGED_FINAL_RETENTION_DAYS = 30
ACKNOWLEDGED_FINAL_RETENTION_COUNT = 4096
COMMAND_RETRY_HORIZON_SECONDS = 604_800
COMMAND_RECEIPT_RETENTION_SECONDS = 2_592_000
COMMAND_RECEIPT_RETENTION_MIN_SECONDS = 691_200
COMMAND_RECEIPT_RETENTION_COUNT = 4096
COMMAND_RECEIPT_OWNER_LEASE_SECONDS = 30.0
COMMAND_RECEIPT_RETENTION_BATCH_SIZE = 100
_COMMAND_RECEIPT_RETENTION_BATCH_MAX = 1000
_COMMAND_REQUEST_UNCERTAIN_RESULT_JSON = (
    '{"ok":false,"status":"request_state_uncertain"}'
)
_COMMAND_REQUEST_STATES = frozenset(
    {"reserved", "send_started", "accepted", "rejected", "uncertain"}
)
_COMMAND_REQUEST_ACTIVE_STATES = frozenset({"reserved", "send_started"})
_COMMAND_REQUEST_TERMINAL_STATES = frozenset({"accepted", "rejected", "uncertain"})
TURN_SUBMISSION_STATE_TRANSITIONS = {
    "send_started": frozenset(
        {"submitted", "uncertain", "linked", "ambiguous", "expired", "cancelled"}
    ),
    "submitted": frozenset({"linked", "ambiguous", "expired", "cancelled"}),
    "uncertain": frozenset({"linked", "ambiguous", "expired", "cancelled"}),
    "linked": frozenset(),
    "ambiguous": frozenset(),
    "expired": frozenset(),
    "cancelled": frozenset(),
}
_SQLITE_MAX_INTEGER = (1 << 63) - 1
_MAX_RETENTION_DAYS = 365_000
_MAX_TIMEDELTA_SECONDS = _MAX_RETENTION_DAYS * 24 * 60 * 60
ATTENTION_LIFECYCLE_OPEN = "open"
ATTENTION_LIFECYCLE_RESOLVED = "resolved"
ATTENTION_RESOLVED_REASON_GONE = "gone"
ATTENTION_RESOLVED_REASON_SUPERSEDED = "superseded"
ATTENTION_OUTBOX_CONNECTOR = "attention"
BACKEND_PENDING_CLAIM_LEASE_SECONDS = 30.0
ATTENTION_MISSING_REQUIRED = 2
ATTENTION_MISSING_GRACE_SECONDS = 120
_ATTENTION_SEVERITY_RANK = {"info": 0, "warning": 1, "critical": 2}
_LOGGER = logging.getLogger(__name__)
_SUBMISSION_LINK_SWEEP_LAST_AT: dict[tuple[str, str, str], float] = {}
_SUBMISSION_LINK_SWEEP_LOCK = threading.Lock()
DEFAULT_SUBMISSION_LINK_SWEEP_INTERVAL_SECONDS = 2.0
_SUBMISSION_LINK_BACKOFF: dict[
    tuple[str, str, str, str], datetime | None
] = {}
_SUBMISSION_LINK_BACKOFF_LOCK = threading.Lock()
_SUBMISSION_LINK_BACKOFF_MISSING = object()

# A component that settles with no candidate observations at all gets a
# BOUNDED deferral, never an indefinite one: the matching observation may
# arrive through a path that cannot re-arm the key (for example, a worker
# payload missing its version marker), so an unbounded skip would wedge the
# link until process restart or hard TTL.
_SUBMISSION_LINK_EMPTY_COMPONENT_RECHECK_SECONDS = 30.0


def _submission_link_backoff_key(
    db_path: Path | str,
    host_id: str,
    owner_key: str,
    instruction_fingerprint_value: str,
) -> tuple[str, str, str, str]:
    return (
        str(Path(db_path).absolute()),
        str(host_id),
        str(owner_key),
        str(instruction_fingerprint_value),
    )


def _submission_link_component_is_due(
    key: tuple[str, str, str, str],
    current: datetime,
) -> bool:
    with _SUBMISSION_LINK_BACKOFF_LOCK:
        next_eligible = _SUBMISSION_LINK_BACKOFF.get(
            key,
            _SUBMISSION_LINK_BACKOFF_MISSING,
        )
        if next_eligible is _SUBMISSION_LINK_BACKOFF_MISSING:
            return True
        if next_eligible is not None and current < next_eligible:
            return False
        _SUBMISSION_LINK_BACKOFF.pop(key, None)
        return True


def _backoff_submission_link_component(
    key: tuple[str, str, str, str],
    next_eligible_at: datetime | None,
    *,
    current: datetime,
) -> None:
    if next_eligible_at is None:
        next_eligible_at = current + timedelta(
            seconds=_SUBMISSION_LINK_EMPTY_COMPONENT_RECHECK_SECONDS
        )
    with _SUBMISSION_LINK_BACKOFF_LOCK:
        _SUBMISSION_LINK_BACKOFF[key] = next_eligible_at


def _rearm_submission_link_component(
    db_path: Path | str,
    host_id: str,
    owner_key: str,
    instruction_fingerprint_value: str,
) -> None:
    key = _submission_link_backoff_key(
        db_path,
        host_id,
        owner_key,
        instruction_fingerprint_value,
    )
    with _SUBMISSION_LINK_BACKOFF_LOCK:
        _SUBMISSION_LINK_BACKOFF.pop(key, None)


def _rearm_submission_link_component_conn(
    conn: sqlite3.Connection,
    host_id: str,
    owner_key: str,
    instruction_fingerprint_value: str,
) -> None:
    database_row = next(
        (
            row
            for row in conn.execute("PRAGMA database_list").fetchall()
            if str(row[1]) == "main"
        ),
        None,
    )
    if database_row is None or not str(database_row[2] or ""):
        return
    _rearm_submission_link_component(
        str(database_row[2]),
        host_id,
        owner_key,
        instruction_fingerprint_value,
    )


def _prune_submission_link_backoff(
    db_path: Path | str,
    host_id: str | None,
    active_keys: set[tuple[str, str, str, str]],
) -> None:
    path = str(Path(db_path).absolute())
    host = None if host_id is None else str(host_id)
    with _SUBMISSION_LINK_BACKOFF_LOCK:
        stale = [
            key
            for key in _SUBMISSION_LINK_BACKOFF
            if key[0] == path
            and (host is None or key[1] == host)
            and key not in active_keys
        ]
        for key in stale:
            _SUBMISSION_LINK_BACKOFF.pop(key, None)


class StoreSchemaError(RuntimeError):
    """Raised when a store schema cannot be opened safely."""

    def __init__(self, status: str) -> None:
        self.status = str(status)
        super().__init__(self.status)


def is_valid_turn_submission_state_transition(
    current_state: object,
    next_state: object,
) -> bool:
    """Return whether a turn-submission state change is allowed."""
    if not isinstance(current_state, str) or not isinstance(next_state, str):
        return False
    return next_state in TURN_SUBMISSION_STATE_TRANSITIONS.get(
        current_state,
        frozenset(),
    )


@dataclass(frozen=True)
class BackendPendingChoiceClaim:
    status: Literal[
        "claimed",
        "validated",
        "not_found",
        "stale",
        "changed",
        "unknown_choice",
        "already_claimed",
    ]
    claim_token: str | None = None
    worker_id: str | None = None
    worker_fingerprint: str | None = None
    binding_private_fingerprint: str | None = None
    turn_target_value: str | None = None
    picker_ordinal: int | None = None


@dataclass(frozen=True)
class BackendPendingChoiceSend:
    status: Literal[
        "started",
        "not_found",
        "stale",
        "changed",
        "binding_changed",
        "already_started",
    ]
    worker_id: str | None = None
    worker_fingerprint: str | None = None
    binding_private_fingerprint: str | None = None
    turn_target_value: str | None = None
    picker_ordinal: int | None = None


@dataclass(frozen=True)
class BackendPendingDecisionClaim:
    status: Literal[
        "claimed",
        "validated",
        "unknown_worker",
        "decision_not_pending",
        "invalid_selection",
        "unsupported_decision",
        "acp_authority_unavailable",
        "already_claimed",
    ]
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
    status: Literal[
        "started",
        "not_found",
        "stale",
        "changed",
        "binding_changed",
        "already_started",
    ]
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
class SnapshotRetentionPolicy:
    """Database-wide snapshot history bounds."""

    retention_days: int = 14
    retention_count: int = 4096
    batch_size: int = 100

    def __post_init__(self) -> None:
        for name in ("retention_days", "retention_count", "batch_size"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise ValueError(f"{name} must be a positive integer")
        if self.retention_days > _MAX_RETENTION_DAYS:
            raise ValueError("retention_days is too large")
        if self.retention_count > _SQLITE_MAX_INTEGER:
            raise ValueError("retention_count is too large")
        if self.batch_size > _SQLITE_MAX_INTEGER:
            raise ValueError("batch_size is too large")


@dataclass(frozen=True)
class CompactionOptions:
    """Policy and explicit offline authority for one compaction."""

    dry_run: bool
    acknowledge_offline: bool = False
    backup_path: Path | None = None
    snapshot_retention_days: int = 14
    snapshot_retention_count: int = 4096
    batch_size: int = 100

    def __post_init__(self) -> None:
        if not isinstance(self.dry_run, bool):
            raise ValueError("dry_run must be a boolean")
        if not isinstance(self.acknowledge_offline, bool):
            raise ValueError("acknowledge_offline must be a boolean")
        if self.backup_path is not None:
            object.__setattr__(self, "backup_path", Path(self.backup_path))


@dataclass(frozen=True)
class SnapshotObservationContext:
    authority: Literal["none", "positive", "complete"] = "none"
    observed_at: str | None = None

@dataclass(frozen=True)
class TurnRefreshApplyResult:
    """Atomic turn and backend-pending persistence outcome."""

    updated: int
    pending_changed: bool
    stale_binding: bool = False
    cancelled: bool = False


@dataclass(frozen=True)
class AppendProjectedAgentEventResult:
    """One binding-fenced journal append and optional turn projection outcome."""

    event: AppendBoundAgentEventResult
    turn: TurnRefreshApplyResult | None = None

@dataclass(frozen=True)
class _TurnContentMergeResult:
    """Observation merge outcome and optional submission settlement key."""

    updated: int
    submission_link: tuple[str, str] | None = None
    submission_link_rearm: tuple[str, str] | None = None


_UNSET = object()


@dataclass
class TurnContentWorkCounters:
    """Deterministic canonical bytes and SQL work observed by list/page operations."""

    list_sql_queries: int = 0
    list_descriptor_rows: int = 0
    list_preview_chars_examined: int = 0
    list_inline_chars_examined: int = 0
    page_sql_queries: int = 0
    page_blob_reads: int = 0
    page_bytes_examined: int = 0
    page_chars_examined: int = 0
    max_response_utf8_bytes: int = 0

    def to_dict(self) -> dict[str, int]:
        return {
            "list_sql_queries": self.list_sql_queries,
            "list_descriptor_rows": self.list_descriptor_rows,
            "list_preview_chars_examined": self.list_preview_chars_examined,
            "list_inline_chars_examined": self.list_inline_chars_examined,
            "page_sql_queries": self.page_sql_queries,
            "page_blob_reads": self.page_blob_reads,
            "page_bytes_examined": self.page_bytes_examined,
            "page_chars_examined": self.page_chars_examined,
            "max_response_utf8_bytes": self.max_response_utf8_bytes,
        }


@dataclass
class TurnDeltaWorkCounters:
    """Bounded aggregate work observed by turn.delta; never carries identities."""

    journal_queries: int = 0
    journal_rows_scanned: int = 0
    projection_queries: int = 0
    projection_rows_read: int = 0
    max_response_utf8_bytes: int = 0

    def to_dict(self) -> dict[str, int]:
        return {
            "journal_queries": self.journal_queries,
            "journal_rows_scanned": self.journal_rows_scanned,
            "projection_queries": self.projection_queries,
            "projection_rows_read": self.projection_rows_read,
            "max_response_utf8_bytes": self.max_response_utf8_bytes,
        }


def _record_response_size(
    counters: TurnContentWorkCounters | None,
    payload: Mapping[str, Any],
) -> None:
    if counters is not None:
        counters.max_response_utf8_bytes = max(
            counters.max_response_utf8_bytes,
            len(_canonical_json(payload).encode("utf-8")),
        )

CREATE_SNAPSHOTS_TABLE = """
CREATE TABLE IF NOT EXISTS snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    host_id TEXT NOT NULL,
    created_at TEXT NOT NULL,
    content_fingerprint TEXT NOT NULL DEFAULT '',
    payload TEXT NOT NULL
);
"""


CREATE_SNAPSHOT_INDEXES = (
    (
        "CREATE INDEX IF NOT EXISTS idx_snapshots_host_newest "
        "ON snapshots(host_id, id DESC)"
    ),
    (
        "CREATE INDEX IF NOT EXISTS idx_snapshots_created_host_id "
        "ON snapshots(created_at, host_id, id)"
    ),
)
# Leading "~" sorts after every canonical timestamp under SQLite BINARY
# collation, so raw indexed age comparisons never delete unknown history.
_SNAPSHOT_CREATED_AT_QUARANTINE = "~invalid-snapshot-created-at"

CREATE_STORE_MAINTENANCE_STATE_TABLE = """
CREATE TABLE IF NOT EXISTS store_maintenance_state (
    scope TEXT PRIMARY KEY CHECK (scope = 'automatic'),
    last_started_at TEXT,
    last_completed_at TEXT,
    last_status TEXT NOT NULL DEFAULT 'never'
        CHECK (last_status IN ('never', 'ok', 'failed')),
    last_examined INTEGER NOT NULL DEFAULT 0 CHECK (last_examined >= 0),
    last_deleted INTEGER NOT NULL DEFAULT 0 CHECK (last_deleted >= 0),
    last_examined_id INTEGER
);
"""

CREATE_STORE_MAINTENANCE_CURSORS_TABLE = """
CREATE TABLE IF NOT EXISTS store_maintenance_cursors (
    scope TEXT PRIMARY KEY,
    last_completed_at TEXT NOT NULL
);
"""

INSERT_STORE_MAINTENANCE_STATE = """
INSERT INTO store_maintenance_state (
    scope, last_started_at, last_completed_at, last_status,
    last_examined, last_deleted, last_examined_id
) VALUES ('automatic', NULL, NULL, 'never', 0, 0, NULL)
ON CONFLICT(scope) DO NOTHING
"""


CREATE_COMMAND_RECEIPTS_TABLE = """
CREATE TABLE IF NOT EXISTS command_receipts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    host_id TEXT NOT NULL,
    request_id TEXT NOT NULL,
    action TEXT NOT NULL,
    canonical_version INTEGER NOT NULL CHECK (canonical_version >= 0),
    canonical_fingerprint TEXT NOT NULL,
    canonical_request_json TEXT NOT NULL,
    public_worker_id TEXT NOT NULL,
    state TEXT NOT NULL CHECK (
        state IN ('reserved', 'send_started', 'accepted', 'rejected', 'uncertain')
    ),
    status TEXT NOT NULL,
    result_json TEXT NOT NULL,
    owner_token_hash TEXT NOT NULL DEFAULT '',
    owner_expires_at TEXT,
    binding_fingerprint TEXT,
    created_at TEXT NOT NULL,
    reserved_at TEXT NOT NULL,
    send_started_at TEXT,
    terminal_at TEXT,
    updated_at TEXT NOT NULL,
    -- Private evidence of the immutable selector this request was spelled with.
    -- Empty is malformed fail-safe evidence that cannot prove an alias retry.
    selector_proof TEXT NOT NULL DEFAULT '',
    CHECK (
        (
            state IN ('reserved', 'send_started')
            AND terminal_at IS NULL
            AND owner_token_hash <> ''
        )
        OR (
            state IN ('accepted', 'rejected', 'uncertain')
            AND terminal_at IS NOT NULL
            AND owner_token_hash = ''
            AND owner_expires_at IS NULL
        )
    ),
    CHECK (state NOT IN ('reserved', 'send_started') OR status = 'pending'),
    CHECK (
        state != 'accepted'
        OR (status = 'accepted' AND send_started_at IS NOT NULL)
    ),
    CHECK (state != 'uncertain' OR status = 'request_state_uncertain'),
    CHECK (
        state != 'rejected'
        OR status NOT IN ('pending', 'accepted', 'request_state_uncertain')
    )
);
"""

CREATE_COMMAND_RECEIPT_INDEXES = (
    "CREATE UNIQUE INDEX IF NOT EXISTS ux_command_receipts_host_request "
    "ON command_receipts(host_id, request_id)",
    "CREATE INDEX IF NOT EXISTS idx_command_receipts_host_state_terminal "
    "ON command_receipts(host_id, state, terminal_at, id)",
)

CREATE_WORKER_BINDINGS_TABLE = """
CREATE TABLE IF NOT EXISTS worker_bindings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    host_id TEXT NOT NULL,
    worker_id TEXT NOT NULL,
    worker_fingerprint TEXT NOT NULL,
    backend TEXT NOT NULL,
    target_kind TEXT NOT NULL,
    target_value TEXT NOT NULL,
    turn_target_kind TEXT,
    turn_target_value TEXT,
    sendable INTEGER NOT NULL DEFAULT 0,
    reason TEXT,
    observed_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    private_fingerprint TEXT NOT NULL
);
"""

CREATE_WORKER_BINDING_INDEXES = (
    "CREATE INDEX IF NOT EXISTS idx_worker_bindings_host_worker_id "
    "ON worker_bindings(host_id, worker_id)",
    "CREATE INDEX IF NOT EXISTS idx_worker_bindings_host_worker_fingerprint "
    "ON worker_bindings(host_id, worker_fingerprint)",
    "CREATE INDEX IF NOT EXISTS idx_worker_bindings_host_private_fingerprint "
    "ON worker_bindings(host_id, backend, private_fingerprint)",
    "CREATE INDEX IF NOT EXISTS idx_worker_bindings_host_backend_target "
    "ON worker_bindings(host_id, backend, target_kind, target_value)",
    "CREATE INDEX IF NOT EXISTS idx_worker_bindings_host_expires_at "
    "ON worker_bindings(host_id, expires_at)",
)
CREATE_WORKER_BINDING_UNIQUE_INDEX = (
    "CREATE UNIQUE INDEX IF NOT EXISTS ux_worker_bindings_host_backend_private "
    "ON worker_bindings(host_id, backend, private_fingerprint)"
)

CREATE_EVENTS_TABLE = """
CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    host_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    aggregate_type TEXT NOT NULL DEFAULT '',
    aggregate_id TEXT NOT NULL DEFAULT '',
    observed_at TEXT NOT NULL,
    content_fingerprint TEXT NOT NULL,
    payload_json TEXT NOT NULL
);
"""

CREATE_SPACES_TABLE = """
CREATE TABLE IF NOT EXISTS spaces (
    host_id TEXT NOT NULL,
    space_id TEXT NOT NULL,
    name TEXT NOT NULL,
    status TEXT NOT NULL,
    updated_at TEXT,
    fingerprint TEXT NOT NULL,
    snapshot_content_fingerprint TEXT NOT NULL,
    observed_at TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    PRIMARY KEY (host_id, space_id)
);
"""

CREATE_WORKERS_TABLE = """
CREATE TABLE IF NOT EXISTS workers (
    host_id TEXT NOT NULL,
    worker_id TEXT NOT NULL,
    worker_fingerprint TEXT NOT NULL,
    space_id TEXT,
    name TEXT NOT NULL,
    status TEXT NOT NULL,
    last_seen_at TEXT,
    snapshot_content_fingerprint TEXT NOT NULL,
    observed_at TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    PRIMARY KEY (host_id, worker_id)
);
"""

CREATE_TURNS_TABLE = """
CREATE TABLE IF NOT EXISTS turns (
    host_id TEXT NOT NULL,
    turn_id TEXT NOT NULL,
    worker_id TEXT NOT NULL,
    worker_fingerprint TEXT,
    space_id TEXT,
    status TEXT NOT NULL,
    kind TEXT NOT NULL,
    updated_at TEXT,
    fingerprint TEXT NOT NULL,
    snapshot_content_fingerprint TEXT NOT NULL,
    observed_at TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    list_sequence INTEGER NOT NULL CHECK (list_sequence > 0),
    PRIMARY KEY (host_id, turn_id)
);
"""

CREATE_TURN_SUBMISSIONS_TABLE = """
CREATE TABLE IF NOT EXISTS turn_submissions (
    host_id TEXT NOT NULL,
    submission_id TEXT NOT NULL,
    request_id TEXT NOT NULL,
    owner_key TEXT NOT NULL,
    owner_key_version INTEGER NOT NULL,
    instruction_fingerprint TEXT NOT NULL,
    state TEXT NOT NULL CHECK (
        state IN (
            'send_started', 'submitted', 'uncertain', 'linked',
            'ambiguous', 'expired', 'cancelled'
        )
    ),
    linked_turn_id TEXT,
    link_not_before TEXT NOT NULL,
    link_expires_at TEXT NOT NULL,
    hard_expires_at TEXT NOT NULL,
    linked_at TEXT,
    terminal_at TEXT,
    submitted_at TEXT,
    send_started_at TEXT,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (host_id, submission_id),
    UNIQUE (host_id, request_id)
);
"""

CREATE_TURN_SUBMISSION_INDEXES = (
    (
        "CREATE INDEX IF NOT EXISTS idx_turn_submissions_link_candidates "
        "ON turn_submissions("
        "host_id, owner_key, instruction_fingerprint, state)"
    ),
    (
        "CREATE UNIQUE INDEX IF NOT EXISTS ux_turn_submissions_linked_turn "
        "ON turn_submissions(host_id, linked_turn_id) "
        "WHERE linked_turn_id IS NOT NULL"
    ),
)

CREATE_TURN_SUPERSESSIONS_TABLE = """
CREATE TABLE IF NOT EXISTS turn_supersessions (
    host_id TEXT NOT NULL,
    superseded_turn_id TEXT NOT NULL,
    canonical_turn_id TEXT NOT NULL,
    reason TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (host_id, superseded_turn_id)
);
"""

CREATE_TURN_SUPERSESSION_INDEXES = (
    (
        "CREATE INDEX IF NOT EXISTS idx_turn_supersessions_canonical "
        "ON turn_supersessions(host_id, canonical_turn_id)"
    ),
)

CREATE_TURN_LIST_STATE_TABLE = """
CREATE TABLE IF NOT EXISTS turn_list_state (
    scope TEXT PRIMARY KEY CHECK (scope = 'turn-list'),
    store_epoch TEXT NOT NULL
);
"""

CREATE_TURN_LIST_HOSTS_TABLE = """
CREATE TABLE IF NOT EXISTS turn_list_hosts (
    host_id TEXT PRIMARY KEY,
    next_sequence INTEGER NOT NULL CHECK (next_sequence > 0),
    traversal_generation INTEGER NOT NULL CHECK (traversal_generation > 0)
);
"""

CREATE_TURN_LIST_INDEXES = (
    (
        "CREATE UNIQUE INDEX IF NOT EXISTS ux_turns_host_list_sequence "
        "ON turns(host_id, list_sequence)"
    ),
    (
        "CREATE INDEX IF NOT EXISTS idx_turns_host_worker_list_sequence "
        "ON turns(host_id, worker_id, list_sequence DESC, turn_id)"
    ),
)

CREATE_TURN_LIST_SEQUENCE_TRIGGERS = (
    """
    CREATE TRIGGER IF NOT EXISTS trg_turns_positive_list_sequence_insert
    BEFORE INSERT ON turns
    WHEN NEW.list_sequence <= 0
    BEGIN
        SELECT RAISE(ABORT, 'invalid turn list sequence');
    END
    """,
    """
    CREATE TRIGGER IF NOT EXISTS trg_turns_positive_list_sequence_update
    BEFORE UPDATE OF list_sequence ON turns
    WHEN NEW.list_sequence <= 0
    BEGIN
        SELECT RAISE(ABORT, 'invalid turn list sequence');
    END
    """,
)

CREATE_TURN_CHANGE_JOURNAL_TABLE = """
CREATE TABLE IF NOT EXISTS turn_change_journal (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    host_id TEXT NOT NULL,
    turn_id TEXT NOT NULL,
    op TEXT NOT NULL CHECK (op IN ('upsert', 'remove')),
    changed_at TEXT NOT NULL
);
"""

CREATE_TURN_CHANGE_FLOOR_TABLE = """
CREATE TABLE IF NOT EXISTS turn_change_floor (
    host_id TEXT PRIMARY KEY,
    floor_seq INTEGER NOT NULL CHECK (floor_seq >= 0)
);
"""

CREATE_TURN_CHANGE_STATE_TABLE = """
CREATE TABLE IF NOT EXISTS turn_change_state (
    scope TEXT PRIMARY KEY CHECK (scope = 'turn-delta'),
    store_epoch TEXT NOT NULL
);
"""

CREATE_TURN_CHANGE_INDEXES = (
    "CREATE INDEX IF NOT EXISTS idx_turn_change_journal_host_seq "
    "ON turn_change_journal(host_id, seq)",
    "CREATE INDEX IF NOT EXISTS idx_turn_change_journal_host_turn_seq "
    "ON turn_change_journal(host_id, turn_id, seq DESC)",
)

CREATE_TURN_CHANGE_TRIGGERS = (
    """
    CREATE TRIGGER IF NOT EXISTS trg_turn_change_after_insert
    AFTER INSERT ON turns
    BEGIN
        INSERT INTO turn_change_journal(host_id, turn_id, op, changed_at)
        VALUES (
            NEW.host_id,
            NEW.turn_id,
            CASE WHEN COALESCE(
                json_extract(NEW.payload_json, '$.superseded_at'), ''
            ) = '' THEN 'upsert' ELSE 'remove' END,
            strftime('%Y-%m-%dT%H:%M:%f+00:00', 'now')
        );
    END
    """,
    """
    CREATE TRIGGER IF NOT EXISTS trg_turn_change_after_update
    AFTER UPDATE ON turns
    WHEN OLD.worker_id IS NOT NEW.worker_id
      OR OLD.worker_fingerprint IS NOT NEW.worker_fingerprint
      OR OLD.space_id IS NOT NEW.space_id
      OR OLD.status IS NOT NEW.status
      OR OLD.kind IS NOT NEW.kind
      OR OLD.updated_at IS NOT NEW.updated_at
      OR OLD.fingerprint IS NOT NEW.fingerprint
      OR OLD.payload_json IS NOT NEW.payload_json
    BEGIN
        INSERT INTO turn_change_journal(host_id, turn_id, op, changed_at)
        VALUES (
            NEW.host_id,
            NEW.turn_id,
            CASE WHEN COALESCE(
                json_extract(NEW.payload_json, '$.superseded_at'), ''
            ) = '' THEN 'upsert' ELSE 'remove' END,
            strftime('%Y-%m-%dT%H:%M:%f+00:00', 'now')
        );
    END
    """,
    """
    CREATE TRIGGER IF NOT EXISTS trg_turn_change_after_delete
    AFTER DELETE ON turns
    WHEN COALESCE(json_extract(OLD.payload_json, '$.superseded_at'), '') = ''
    BEGIN
        INSERT INTO turn_change_journal(host_id, turn_id, op, changed_at)
        VALUES (
            OLD.host_id,
            OLD.turn_id,
            'remove',
            strftime('%Y-%m-%dT%H:%M:%f+00:00', 'now')
        );
    END
    """,
    """
    CREATE TRIGGER IF NOT EXISTS trg_turn_change_revision_current
    AFTER UPDATE OF is_current ON turn_content_revisions
    WHEN NEW.is_current = 1
    BEGIN
        INSERT INTO turn_change_journal(host_id, turn_id, op, changed_at)
        SELECT
            NEW.host_id,
            NEW.turn_id,
            CASE WHEN COALESCE(
                json_extract(turns.payload_json, '$.superseded_at'), ''
            ) = '' THEN 'upsert' ELSE 'remove' END,
            strftime('%Y-%m-%dT%H:%M:%f+00:00', 'now')
        FROM turns
        WHERE turns.host_id = NEW.host_id
          AND turns.turn_id = NEW.turn_id;
    END
    """,
    """
    CREATE TRIGGER IF NOT EXISTS trg_turn_change_revision_insert_current
    AFTER INSERT ON turn_content_revisions
    WHEN NEW.is_current = 1
    BEGIN
        INSERT INTO turn_change_journal(host_id, turn_id, op, changed_at)
        SELECT
            NEW.host_id,
            NEW.turn_id,
            CASE WHEN COALESCE(
                json_extract(turns.payload_json, '$.superseded_at'), ''
            ) = '' THEN 'upsert' ELSE 'remove' END,
            strftime('%Y-%m-%dT%H:%M:%f+00:00', 'now')
        FROM turns
        WHERE turns.host_id = NEW.host_id
          AND turns.turn_id = NEW.turn_id;
    END
    """,
    """
    CREATE TRIGGER IF NOT EXISTS trg_turn_change_journal_no_update
    BEFORE UPDATE ON turn_change_journal
    BEGIN
        SELECT RAISE(ABORT, 'turn change journal rows are immutable');
    END
    """,
)

CREATE_TURN_CONTENT_REVISIONS_TABLE = """
CREATE TABLE IF NOT EXISTS turn_content_revisions (
    host_id TEXT NOT NULL,
    turn_id TEXT NOT NULL,
    content_revision TEXT NOT NULL,
    user_text TEXT,
    assistant_final_text TEXT,
    user_state TEXT NOT NULL
        CHECK (user_state IN ('absent', 'complete', 'known_incomplete')),
    final_state TEXT NOT NULL
        CHECK (final_state IN ('absent', 'complete', 'known_incomplete')),
    user_char_length INTEGER NOT NULL CHECK (user_char_length >= 0),
    user_byte_length INTEGER NOT NULL CHECK (user_byte_length >= 0),
    final_char_length INTEGER NOT NULL CHECK (final_char_length >= 0),
    final_byte_length INTEGER NOT NULL CHECK (final_byte_length >= 0),
    user_page_count INTEGER NOT NULL CHECK (user_page_count >= 0),
    final_page_count INTEGER NOT NULL CHECK (final_page_count >= 0),
    is_current INTEGER NOT NULL CHECK (is_current IN (0, 1)),
    created_at TEXT NOT NULL,
    superseded_at TEXT,
    PRIMARY KEY (host_id, turn_id, content_revision),
    FOREIGN KEY (host_id, turn_id)
        REFERENCES turns(host_id, turn_id) ON DELETE RESTRICT
);
"""

CREATE_TURN_CONTENT_PAGE_BOUNDARIES_TABLE = """
CREATE TABLE IF NOT EXISTS turn_content_page_boundaries (
    host_id TEXT NOT NULL,
    turn_id TEXT NOT NULL,
    content_revision TEXT NOT NULL,
    field TEXT NOT NULL
        CHECK (field IN ('user_text', 'assistant_final_text')),
    page_index INTEGER NOT NULL CHECK (page_index >= 0),
    start_char INTEGER NOT NULL CHECK (start_char >= 0),
    start_byte INTEGER NOT NULL CHECK (start_byte >= 0),
    PRIMARY KEY (
        host_id,
        turn_id,
        content_revision,
        field,
        page_index
    ),
    UNIQUE (
        host_id,
        turn_id,
        content_revision,
        field,
        start_char
    ),
    UNIQUE (
        host_id,
        turn_id,
        content_revision,
        field,
        start_byte
    ),
    FOREIGN KEY (host_id, turn_id, content_revision)
        REFERENCES turn_content_revisions(host_id, turn_id, content_revision)
        ON DELETE CASCADE
);
"""

CREATE_TURN_CONTENT_REVISION_INDEXES = (
    (
        "CREATE UNIQUE INDEX IF NOT EXISTS ux_turn_content_current "
        "ON turn_content_revisions(host_id, turn_id) WHERE is_current = 1"
    ),
    (
        "CREATE INDEX IF NOT EXISTS idx_turn_content_cleanup "
        "ON turn_content_revisions(host_id, is_current, superseded_at)"
    ),
)

CREATE_FINAL_DELIVERY_INDEXES = (
    (
        "CREATE INDEX IF NOT EXISTS idx_connector_outbox_final_state "
        "ON connector_outbox(host_id, connector, delivery_kind, status, id)"
    ),
    (
        "CREATE INDEX IF NOT EXISTS idx_connector_outbox_turn_revision "
        "ON connector_outbox(host_id, turn_id, content_revision, delivery_kind)"
    ),
    (
        "CREATE INDEX IF NOT EXISTS idx_turn_presentation_plans_source "
        "ON turn_presentation_plans(source_outbox_id)"
    ),
)

CREATE_CONNECTOR_ORDERING_INDEX = (
    "CREATE INDEX IF NOT EXISTS idx_connector_outbox_final_ordering "
    "ON connector_outbox(host_id, connector, ordering_key, delivery_kind, status, id)"
)


CREATE_TURN_PRESENTATION_PLANS_TABLE = """
CREATE TABLE IF NOT EXISTS turn_presentation_plans (
    id INTEGER PRIMARY KEY,
    host_id TEXT NOT NULL,
    name TEXT NOT NULL,
    plan_token TEXT NOT NULL,
    turn_id TEXT NOT NULL,
    content_revision TEXT NOT NULL,
    source_outbox_id INTEGER,
    presentation_version TEXT NOT NULL,
    generation INTEGER NOT NULL DEFAULT 1 CHECK (generation >= 1),
    part_count INTEGER NOT NULL CHECK (part_count > 0),
    state TEXT NOT NULL
        CHECK (state IN (
            'preparing',
            'waiting_predecessor',
            'active',
            'completed',
            'superseded',
            'failed'
        )),
    replaces_plan_token TEXT,
    recovers_plan_token TEXT,
    created_at TEXT NOT NULL,
    activated_at TEXT,
    completed_at TEXT,
    UNIQUE (host_id, name, plan_token),
    UNIQUE (
        host_id,
        name,
        turn_id,
        content_revision,
        presentation_version,
        generation
    ),
    FOREIGN KEY (host_id, turn_id, content_revision)
        REFERENCES turn_content_revisions(host_id, turn_id, content_revision)
        ON DELETE RESTRICT,
    FOREIGN KEY (source_outbox_id)
        REFERENCES connector_outbox(id) ON DELETE RESTRICT
);
"""

CREATE_TURN_PRESENTATION_JOBS_TABLE = """
CREATE TABLE IF NOT EXISTS turn_presentation_jobs (
    id INTEGER PRIMARY KEY,
    plan_id INTEGER NOT NULL,
    sequence_index INTEGER NOT NULL CHECK (sequence_index >= 0),
    operation TEXT NOT NULL CHECK (operation IN ('upsert', 'retire')),
    part_ordinal INTEGER NOT NULL CHECK (part_ordinal >= 0),
    spans_json TEXT NOT NULL,
    outbox_id INTEGER UNIQUE,
    created_at TEXT NOT NULL,
    UNIQUE (plan_id, sequence_index),
    UNIQUE (plan_id, operation, part_ordinal),
    FOREIGN KEY (plan_id) REFERENCES turn_presentation_plans(id) ON DELETE CASCADE,
    FOREIGN KEY (outbox_id) REFERENCES connector_outbox(id) ON DELETE RESTRICT
);
"""

CREATE_TURN_PRESENTATION_RECOVERIES_TABLE = """
CREATE TABLE IF NOT EXISTS turn_presentation_recoveries (
    id INTEGER PRIMARY KEY,
    host_id TEXT NOT NULL,
    name TEXT NOT NULL,
    request_id TEXT NOT NULL,
    failed_plan_id INTEGER NOT NULL,
    recovered_plan_id INTEGER NOT NULL,
    failed_plan_token TEXT NOT NULL,
    recovered_plan_token TEXT NOT NULL,
    generation INTEGER NOT NULL CHECK (generation >= 2),
    source_job_count INTEGER NOT NULL CHECK (source_job_count > 0),
    delivered_prefix_count INTEGER NOT NULL CHECK (delivered_prefix_count >= 0),
    fresh_job_count INTEGER NOT NULL CHECK (fresh_job_count > 0),
    retained_failed_job_count INTEGER NOT NULL CHECK (retained_failed_job_count > 0),
    prior_attempt_count INTEGER NOT NULL CHECK (prior_attempt_count > 0),
    outcome TEXT NOT NULL CHECK (outcome = 'recovered'),
    created_at TEXT NOT NULL,
    UNIQUE (host_id, name, request_id),
    UNIQUE (failed_plan_id),
    FOREIGN KEY (failed_plan_id)
        REFERENCES turn_presentation_plans(id) ON DELETE RESTRICT,
    FOREIGN KEY (recovered_plan_id)
        REFERENCES turn_presentation_plans(id) ON DELETE RESTRICT
);
"""

CREATE_TURN_PRESENTATION_INDEXES = (
    (
        "CREATE INDEX IF NOT EXISTS idx_turn_presentation_jobs_plan_sequence "
        "ON turn_presentation_jobs(plan_id, sequence_index)"
    ),
    (
        "CREATE INDEX IF NOT EXISTS idx_turn_presentation_jobs_outbox "
        "ON turn_presentation_jobs(outbox_id)"
    ),
    (
        "CREATE INDEX IF NOT EXISTS idx_turn_presentation_recoveries_recovered "
        "ON turn_presentation_recoveries(recovered_plan_id)"
    ),
)

CREATE_PENDING_INTERACTIONS_TABLE = """
CREATE TABLE IF NOT EXISTS pending_interactions (
    host_id TEXT NOT NULL,
    pending_id TEXT NOT NULL,
    worker_id TEXT NOT NULL,
    worker_fingerprint TEXT,
    space_id TEXT,
    kind TEXT NOT NULL,
    status TEXT NOT NULL,
    updated_at TEXT,
    fingerprint TEXT NOT NULL,
    snapshot_content_fingerprint TEXT NOT NULL,
    observed_at TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    PRIMARY KEY (host_id, pending_id)
);
"""

CREATE_ATTENTION_ITEMS_TABLE = """
CREATE TABLE IF NOT EXISTS attention_items (
    host_id TEXT NOT NULL,
    attention_id TEXT NOT NULL,
    source TEXT NOT NULL,
    kind TEXT NOT NULL,
    severity TEXT NOT NULL,
    status TEXT NOT NULL,
    updated_at TEXT,
    fingerprint TEXT NOT NULL,
    snapshot_content_fingerprint TEXT NOT NULL,
    observed_at TEXT NOT NULL,
    first_seen_at TEXT NOT NULL DEFAULT '',
    last_seen_at TEXT NOT NULL DEFAULT '',
    last_changed_at TEXT NOT NULL DEFAULT '',
    resolved_at TEXT,
    lifecycle_status TEXT NOT NULL DEFAULT 'open',
    resolved_reason TEXT,
    signal_count INTEGER NOT NULL DEFAULT 1,
    payload_json TEXT NOT NULL,
    PRIMARY KEY (host_id, attention_id)
);
"""

CREATE_ATTENTION_LIFECYCLES_TABLE = """
CREATE TABLE IF NOT EXISTS attention_lifecycles (
    host_id TEXT NOT NULL,
    family_key TEXT NOT NULL,
    generation INTEGER NOT NULL CHECK (generation >= 1),
    lifecycle_status TEXT NOT NULL CHECK (lifecycle_status IN ('open','resolved')),
    current_attention_id TEXT,
    first_seen_at TEXT NOT NULL,
    last_positive_at TEXT NOT NULL,
    first_missing_at TEXT,
    missing_observation_count INTEGER NOT NULL DEFAULT 0 CHECK (missing_observation_count >= 0),
    last_accepted_at TEXT NOT NULL,
    last_observation_key TEXT NOT NULL,
    max_notified_severity_rank INTEGER NOT NULL DEFAULT -1,
    PRIMARY KEY (host_id, family_key),
    CHECK (
        (lifecycle_status = 'open' AND current_attention_id IS NOT NULL)
        OR (lifecycle_status = 'resolved' AND current_attention_id IS NULL)
    ),
    CHECK (
        (missing_observation_count = 0 AND first_missing_at IS NULL)
        OR (missing_observation_count > 0 AND first_missing_at IS NOT NULL)
    )
);
"""

CREATE_ATTENTION_LIFECYCLE_INDEXES = (
    (
        "CREATE INDEX IF NOT EXISTS idx_attention_lifecycles_host_status "
        "ON attention_lifecycles(host_id, lifecycle_status)"
    ),
    (
        "CREATE INDEX IF NOT EXISTS idx_attention_lifecycles_host_current "
        "ON attention_lifecycles(host_id, current_attention_id)"
    ),
)


CREATE_COMMANDS_TABLE = """
CREATE TABLE IF NOT EXISTS commands (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    host_id TEXT NOT NULL,
    request_id TEXT NOT NULL,
    action TEXT NOT NULL,
    canonical_version INTEGER NOT NULL CHECK (canonical_version >= 0),
    canonical_fingerprint TEXT NOT NULL,
    public_worker_id TEXT NOT NULL,
    state TEXT NOT NULL CHECK (
        state IN ('reserved', 'send_started', 'accepted', 'rejected', 'uncertain')
    ),
    status TEXT NOT NULL,
    request_json TEXT NOT NULL,
    result_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    reserved_at TEXT NOT NULL,
    send_started_at TEXT,
    terminal_at TEXT,
    updated_at TEXT NOT NULL,
    CHECK (
        (state IN ('reserved', 'send_started') AND terminal_at IS NULL)
        OR (state IN ('accepted', 'rejected', 'uncertain') AND terminal_at IS NOT NULL)
    ),
    CHECK (state NOT IN ('reserved', 'send_started') OR status = 'pending'),
    CHECK (
        state != 'accepted'
        OR (status = 'accepted' AND send_started_at IS NOT NULL)
    ),
    CHECK (state != 'uncertain' OR status = 'request_state_uncertain'),
    CHECK (
        state != 'rejected'
        OR status NOT IN ('pending', 'accepted', 'request_state_uncertain')
    )
);
"""

CREATE_CONNECTOR_OUTBOX_TABLE = """
CREATE TABLE IF NOT EXISTS connector_outbox (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    host_id TEXT NOT NULL,
    connector TEXT NOT NULL,
    delivery_key TEXT NOT NULL,
    delivery_kind TEXT NOT NULL DEFAULT 'generic',
    turn_id TEXT,
    content_revision TEXT,
    ordering_key TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    private_state_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    next_attempt_at TEXT
);
"""

CREATE_CONNECTOR_DELIVERIES_TABLE = """
CREATE TABLE IF NOT EXISTS connector_deliveries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    outbox_id INTEGER,
    host_id TEXT NOT NULL,
    connector TEXT NOT NULL,
    delivery_key TEXT NOT NULL,
    attempt INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL,
    response_json TEXT NOT NULL DEFAULT '{}',
    private_state_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    delivered_at TEXT,
    FOREIGN KEY (outbox_id) REFERENCES connector_outbox(id) ON DELETE SET NULL
);
"""

CREATE_BACKEND_HEALTH_TABLE = """
CREATE TABLE IF NOT EXISTS backend_health (
    host_id TEXT NOT NULL,
    backend_name TEXT NOT NULL,
    status TEXT NOT NULL,
    outcome TEXT NOT NULL,
    observed_at TEXT,
    snapshot_content_fingerprint TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    PRIMARY KEY (host_id, backend_name)
);
"""


CREATE_BACKEND_PENDING_TABLE = """
CREATE TABLE IF NOT EXISTS backend_pending (
    host_id TEXT NOT NULL,
    worker_id TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    observed_at TEXT NOT NULL,
    revision_digest TEXT NOT NULL DEFAULT '',
    choice_routes_json TEXT NOT NULL DEFAULT '{}',
    binding_private_fingerprint TEXT NOT NULL DEFAULT '',
    observed_turn_target_value TEXT NOT NULL DEFAULT '',
    observation_state TEXT NOT NULL DEFAULT 'open',
    freshness TEXT NOT NULL DEFAULT 'fresh',
    last_success_at TEXT,
    last_failure_at TEXT,
    grace_deadline TEXT,
    updated_at TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (host_id, worker_id)
);
"""


CREATE_BACKEND_PENDING_CLAIMS_TABLE = """
CREATE TABLE IF NOT EXISTS backend_pending_claims (
    host_id TEXT NOT NULL,
    worker_id TEXT NOT NULL,
    claim_token TEXT NOT NULL UNIQUE,
    revision_digest TEXT NOT NULL,
    choice_id TEXT NOT NULL,
    picker_ordinal INTEGER NOT NULL CHECK (picker_ordinal >= 1),
    worker_fingerprint TEXT NOT NULL,
    binding_private_fingerprint TEXT NOT NULL,
    turn_target_value TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('claimed', 'send_started')),
    claimed_at TEXT NOT NULL,
    send_started_at TEXT,
    PRIMARY KEY (host_id, worker_id)
);
"""






CREATE_AGENT_EVENTS_TABLE = """
CREATE TABLE IF NOT EXISTS agent_events (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    host_id TEXT NOT NULL,
    event_id TEXT NOT NULL,
    kind TEXT NOT NULL CHECK (
        kind IN (
            'user_message', 'agent_message', 'thought', 'tool_call',
            'tool_call_update', 'plan', 'usage', 'session_info', 'extension'
        )
    ),
    source TEXT NOT NULL,
    worker_id TEXT NOT NULL,
    visibility TEXT NOT NULL CHECK (visibility IN ('private', 'public')),
    source_session_id TEXT,
    source_turn_id TEXT,
    source_item_id TEXT,
    source_message_id TEXT,
    source_event_id TEXT,
    source_sequence INTEGER CHECK (source_sequence >= 0),
    observed_at TEXT NOT NULL,
    payload_fingerprint TEXT NOT NULL,
    private_payload_json TEXT NOT NULL,
    public_payload_json TEXT NOT NULL,
    UNIQUE (host_id, event_id),
    CHECK (length(host_id) BETWEEN 1 AND 2048),
    CHECK (instr(host_id, char(0)) = 0),
    CHECK (length(event_id) = 64),
    CHECK (length(source) BETWEEN 1 AND 2048),
    CHECK (instr(source, char(0)) = 0),
    CHECK (length(worker_id) BETWEEN 1 AND 2048),
    CHECK (instr(worker_id, char(0)) = 0),
    CHECK (
        source_session_id IS NULL OR (
            length(source_session_id) BETWEEN 1 AND 2048
            AND instr(source_session_id, char(0)) = 0
        )
    ),
    CHECK (
        source_turn_id IS NULL OR (
            length(source_turn_id) BETWEEN 1 AND 2048
            AND instr(source_turn_id, char(0)) = 0
        )
    ),
    CHECK (
        source_item_id IS NULL OR (
            length(source_item_id) BETWEEN 1 AND 2048
            AND instr(source_item_id, char(0)) = 0
        )
    ),
    CHECK (
        source_message_id IS NULL OR (
            length(source_message_id) BETWEEN 1 AND 2048
            AND instr(source_message_id, char(0)) = 0
        )
    ),
    CHECK (
        source_event_id IS NULL OR (
            length(source_event_id) BETWEEN 1 AND 2048
            AND instr(source_event_id, char(0)) = 0
        )
    ),
    CHECK (length(observed_at) BETWEEN 20 AND 40),
    CHECK (length(payload_fingerprint) = 64),
    CHECK (
        CASE WHEN json_valid(private_payload_json)
        THEN json_type(private_payload_json) = 'object' ELSE 0 END
    ),
    CHECK (
        length(CAST(private_payload_json AS BLOB)) <= 16777216
    ),
    CHECK (
        CASE WHEN json_valid(public_payload_json)
        THEN json_type(public_payload_json) = 'object' ELSE 0 END
    ),
    CHECK (
        length(CAST(public_payload_json AS BLOB)) <= 65536
    ),
    CHECK (visibility != 'private' OR public_payload_json = '{}'),
    CHECK (source_event_id IS NOT NULL OR source_sequence IS NOT NULL),
    CHECK (
        source_event_id IS NOT NULL
        OR (source_session_id IS NOT NULL AND source_sequence IS NOT NULL)
    ),
    CHECK (
        kind NOT IN (
            'thought', 'tool_call', 'tool_call_update', 'plan', 'extension'
        ) OR visibility = 'private'
    )
);
"""

CREATE_AGENT_EVENT_INDEXES = (
    (
        "CREATE INDEX IF NOT EXISTS idx_agent_events_host_worker_sequence "
        "ON agent_events(host_id, worker_id, sequence)"
    ),
    (
        "CREATE INDEX IF NOT EXISTS idx_agent_events_host_session_sequence "
        "ON agent_events(host_id, source_session_id, sequence)"
    ),
    (
        "CREATE INDEX IF NOT EXISTS idx_agent_events_host_turn_sequence "
        "ON agent_events(host_id, source_turn_id, sequence)"
    ),
    (
        "CREATE INDEX IF NOT EXISTS idx_agent_events_host_source_sequence "
        "ON agent_events(host_id, source, sequence)"
    ),
    (
        "CREATE INDEX IF NOT EXISTS idx_agent_events_host_visibility_sequence "
        "ON agent_events(host_id, visibility, sequence)"
    ),
    (
        "CREATE INDEX IF NOT EXISTS idx_agent_events_host_observed_sequence "
        "ON agent_events(host_id, observed_at, sequence)"
    ),
    (
        "CREATE UNIQUE INDEX IF NOT EXISTS "
        "idx_agent_events_source_event_identity "
        "ON agent_events("
        "host_id, source, COALESCE(source_session_id, ''), source_event_id"
        ") WHERE source_event_id IS NOT NULL"
    ),
    (
        "CREATE UNIQUE INDEX IF NOT EXISTS "
        "idx_agent_events_source_sequence_identity "
        "ON agent_events(host_id, source, source_session_id, source_sequence) "
        "WHERE source_event_id IS NULL"
    ),
)


CREATE_CURRENT_PR6_TABLES = (
    CREATE_EVENTS_TABLE,
    CREATE_SPACES_TABLE,
    CREATE_WORKERS_TABLE,
    CREATE_TURNS_TABLE,
    CREATE_PENDING_INTERACTIONS_TABLE,
    CREATE_ATTENTION_ITEMS_TABLE,
    CREATE_COMMANDS_TABLE,
    CREATE_CONNECTOR_OUTBOX_TABLE,
    CREATE_CONNECTOR_DELIVERIES_TABLE,
    CREATE_BACKEND_HEALTH_TABLE,
)

CREATE_PR6_INDEXES = (
    "CREATE INDEX IF NOT EXISTS idx_events_host_observed_at ON events(host_id, observed_at)",
    "CREATE INDEX IF NOT EXISTS idx_events_host_type ON events(host_id, event_type)",
    (
        "CREATE INDEX IF NOT EXISTS idx_events_host_aggregate "
        "ON events(host_id, aggregate_type, aggregate_id)"
    ),
    "CREATE INDEX IF NOT EXISTS idx_spaces_host_status ON spaces(host_id, status)",
    "CREATE INDEX IF NOT EXISTS idx_workers_host_status ON workers(host_id, status)",
    "CREATE INDEX IF NOT EXISTS idx_workers_host_space ON workers(host_id, space_id)",
    "CREATE INDEX IF NOT EXISTS idx_turns_host_worker ON turns(host_id, worker_id)",
    "CREATE INDEX IF NOT EXISTS idx_turns_host_status ON turns(host_id, status)",
    (
        "CREATE INDEX IF NOT EXISTS idx_pending_interactions_host_worker "
        "ON pending_interactions(host_id, worker_id)"
    ),
    (
        "CREATE INDEX IF NOT EXISTS idx_pending_interactions_host_status "
        "ON pending_interactions(host_id, status)"
    ),
    (
        "CREATE INDEX IF NOT EXISTS idx_attention_items_host_source "
        "ON attention_items(host_id, source)"
    ),
    (
        "CREATE INDEX IF NOT EXISTS idx_attention_items_host_status "
        "ON attention_items(host_id, status)"
    ),
    (
        "CREATE INDEX IF NOT EXISTS idx_attention_items_host_lifecycle_status "
        "ON attention_items(host_id, lifecycle_status)"
    ),
    (
        "CREATE INDEX IF NOT EXISTS idx_attention_items_host_last_seen "
        "ON attention_items(host_id, last_seen_at)"
    ),
    (
        "CREATE INDEX IF NOT EXISTS idx_attention_items_host_fingerprint "
        "ON attention_items(host_id, fingerprint)"
    ),
    (
        "CREATE UNIQUE INDEX IF NOT EXISTS ux_connector_outbox_host_connector_key "
        "ON connector_outbox(host_id, connector, delivery_key)"
    ),
    "CREATE INDEX IF NOT EXISTS idx_connector_outbox_status ON connector_outbox(status)",
    (
        "CREATE INDEX IF NOT EXISTS idx_connector_deliveries_outbox "
        "ON connector_deliveries(outbox_id)"
    ),
    (
        "CREATE INDEX IF NOT EXISTS idx_connector_deliveries_host_connector "
        "ON connector_deliveries(host_id, connector, delivery_key)"
    ),
    "CREATE INDEX IF NOT EXISTS idx_backend_health_host_status ON backend_health(host_id, status)",
)
CREATE_COMMAND_INDEXES = (
    "CREATE UNIQUE INDEX IF NOT EXISTS ux_commands_host_request "
    "ON commands(host_id, request_id)",
    "CREATE INDEX IF NOT EXISTS idx_commands_host_state_updated "
    "ON commands(host_id, state, updated_at, id)",
)
CREATE_CURRENT_PR6_INDEXES = (
    CREATE_PR6_INDEXES + CREATE_COMMAND_INDEXES
)


_SQLITE_FAMILY_SUFFIXES = ("", "-wal", "-shm", "-journal")


def _is_memory_db(db_path: Path | str) -> bool:
    raw = str(db_path)
    if raw == ":memory:":
        return True
    if not raw.startswith("file:"):
        return False
    try:
        query = urlsplit(raw).query
    except ValueError:
        return False
    modes = [
        value
        for name, value in parse_qsl(query, keep_blank_values=True)
        if name == "mode"
    ]
    return modes == ["memory"]


def _has_duplicate_sqlite_uri_mode(db_path: Path | str) -> bool:
    raw = str(db_path)
    if not raw.startswith("file:"):
        return False
    try:
        query = urlsplit(raw).query
    except ValueError:
        return False
    return sum(
        1
        for name, _value in parse_qsl(query, keep_blank_values=True)
        if name == "mode"
    ) > 1


def _validate_parent_fd(parent_fd: int, *, private: bool) -> None:
    try:
        current = os.fstat(parent_fd)
    except OSError:
        raise local_state_error(LocalStateErrorCode.OPERATION_FAILED) from None
    validate_owned_directory_stat(current)
    forbidden = ~0o700 if private else stat.S_IWGRP | stat.S_IWOTH
    if stat.S_IMODE(current.st_mode) & forbidden:
        raise local_state_error(LocalStateErrorCode.INSECURE_MODE) from None


def _bare_relative_parent(db_path: Path | str) -> bool:
    try:
        raw = os.fspath(db_path)
        return isinstance(raw, str) and not raw.startswith(os.sep) and Path(raw).parent == Path(".")
    except (TypeError, ValueError):
        return False


def _open_filesystem_db(
    db_path: Path | str,
    *,
    prepare: bool,
    retain_parent_shared_lock: bool,
) -> tuple[int, str]:
    shared_lock_held = False
    if prepare and not _bare_relative_parent(db_path):
        parent_fd, leaf, _result = prepare_resolved_private_sqlite_parent(
            db_path,
            retain_parent_shared_lock=retain_parent_shared_lock,
        )
        shared_lock_held = retain_parent_shared_lock
    else:
        parent_fd, leaf = open_resolved_parent(db_path)
        try:
            _validate_parent_fd(parent_fd, private=not prepare)
        except Exception:
            os.close(parent_fd)
            raise
    try:
        if retain_parent_shared_lock and not shared_lock_held:
            try:
                fcntl.flock(parent_fd, fcntl.LOCK_SH | fcntl.LOCK_NB)
            except (BlockingIOError, OSError):
                raise local_state_error(LocalStateErrorCode.OPERATION_FAILED) from None
        if prepare:
            prepare_sqlite_family_at(
                parent_fd,
                leaf,
                retain_parent_shared_lock=retain_parent_shared_lock,
            )
        _validate_sqlite_family_at(parent_fd, leaf)
        return parent_fd, leaf
    except Exception:
        os.close(parent_fd)
        raise


def _validate_sqlite_family_at(parent_fd: int, leaf: str) -> None:
    inspected = _snapshot_sqlite_family_at(
        parent_fd,
        leaf,
        require_main=True,
    )
    for result in inspected:
        if result.state is PermissionState.REPAIR_REQUIRED:
            raise local_state_error(LocalStateErrorCode.INSECURE_MODE)


def _sqlite_store_exists(db_path: Path | str) -> bool:
    if _is_memory_db(db_path):
        return False
    try:
        parent_fd, leaf = open_resolved_parent(db_path)
    except LocalStateError as exc:
        if exc.code is LocalStateErrorCode.MISSING_ENTRY:
            return False
        raise
    try:
        inspected = _snapshot_sqlite_family_at(
            parent_fd,
            leaf,
            require_main=False,
        )
        return inspected[0].state is not PermissionState.ABSENT
    finally:
        os.close(parent_fd)


def _apply_connection_pragmas(conn: sqlite3.Connection, db_path: Path | str) -> None:
    """Apply cheap, connection-local safety settings."""
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA synchronous=NORMAL")


def _configure_persistent_database_conn(conn: sqlite3.Connection) -> None:
    """Negotiate persistent WAL once, only while initializing or migrating."""
    database_row = conn.execute("PRAGMA database_list").fetchone()
    if database_row is None or not str(database_row[2] or ""):
        return
    mode_row = conn.execute("PRAGMA journal_mode=WAL").fetchone()
    if mode_row is None or str(mode_row[0]).lower() != "wal":
        raise StoreSchemaError("wal_unavailable")


@dataclass(frozen=True)
class _SchemaConnectionAuthority:
    parent_fd: int | None
    parent_identity: EntryIdentity | None
    leaf: str | None
    selected_identity: EntryIdentity | None
    retains_parent_shared_lock: bool


_SCHEMA_CONNECTION_AUTHORITIES: dict[
    int, tuple[weakref.ReferenceType[Any], _SchemaConnectionAuthority]
] = {}
_SCHEMA_CONNECTION_AUTHORITIES_LOCK = threading.RLock()
_SCHEMA_PARENT_SHARED_LOCK_RECOVERY_ATTEMPTS = 3


def _parent_directory_identity(parent_fd: int) -> EntryIdentity:
    try:
        return entry_identity(os.fstat(parent_fd))
    except OSError:
        raise local_state_error(LocalStateErrorCode.OPERATION_FAILED) from None


def _close_schema_authority(authority: _SchemaConnectionAuthority) -> None:
    if authority.parent_fd is not None:
        try:
            os.close(authority.parent_fd)
        except OSError:
            pass


def _release_abandoned_schema_connection_authority(
    connection_id: int,
    reference: weakref.ReferenceType[Any],
) -> None:
    authority: _SchemaConnectionAuthority | None = None
    with _SCHEMA_CONNECTION_AUTHORITIES_LOCK:
        registered = _SCHEMA_CONNECTION_AUTHORITIES.get(connection_id)
        if registered is None or registered[0] is not reference:
            return
        _SCHEMA_CONNECTION_AUTHORITIES.pop(connection_id)
        authority = registered[1]
    _close_schema_authority(authority)


def _register_schema_connection_authority(
    conn: sqlite3.Connection,
    *,
    parent_fd: int | None = None,
    leaf: str | None = None,
    selected_identity: EntryIdentity | None = None,
    retains_parent_shared_lock: bool = False,
) -> None:
    parent_identity = (
        _parent_directory_identity(parent_fd) if parent_fd is not None else None
    )
    connection_id = id(conn)

    def release(reference: weakref.ReferenceType[Any]) -> None:
        _release_abandoned_schema_connection_authority(connection_id, reference)

    authority = _SchemaConnectionAuthority(
        parent_fd=parent_fd,
        parent_identity=parent_identity,
        leaf=leaf,
        selected_identity=selected_identity,
        retains_parent_shared_lock=retains_parent_shared_lock,
    )
    stale_authority: _SchemaConnectionAuthority | None = None
    with _SCHEMA_CONNECTION_AUTHORITIES_LOCK:
        registered = _SCHEMA_CONNECTION_AUTHORITIES.get(connection_id)
        if registered is not None:
            existing = registered[0]()
            if existing is conn:
                return
            if existing is not None:
                raise local_state_error(LocalStateErrorCode.OPERATION_FAILED)
            _SCHEMA_CONNECTION_AUTHORITIES.pop(connection_id)
            stale_authority = registered[1]
        _SCHEMA_CONNECTION_AUTHORITIES[connection_id] = (
            weakref.ref(conn, release),
            authority,
        )
    if stale_authority is not None:
        _close_schema_authority(stale_authority)


def _schema_connection_authority(
    conn: sqlite3.Connection,
) -> _SchemaConnectionAuthority:
    with _SCHEMA_CONNECTION_AUTHORITIES_LOCK:
        registered = _SCHEMA_CONNECTION_AUTHORITIES.get(id(conn))
        if registered is None or registered[0]() is not conn:
            raise local_state_error(LocalStateErrorCode.OPERATION_FAILED)
        return registered[1]


def _release_schema_connection_authority(
    conn: sqlite3.Connection,
) -> _SchemaConnectionAuthority | None:
    with _SCHEMA_CONNECTION_AUTHORITIES_LOCK:
        registered = _SCHEMA_CONNECTION_AUTHORITIES.get(id(conn))
        if registered is None or registered[0]() is not conn:
            return None
        _SCHEMA_CONNECTION_AUTHORITIES.pop(id(conn))
        return registered[1]


def _has_other_shared_parent_schema_authority(
    conn: sqlite3.Connection,
    authority: _SchemaConnectionAuthority,
) -> bool:
    if authority.parent_identity is None:
        return False
    stale_authorities: list[_SchemaConnectionAuthority] = []
    has_other = False
    with _SCHEMA_CONNECTION_AUTHORITIES_LOCK:
        for connection_id, (reference, candidate) in tuple(
            _SCHEMA_CONNECTION_AUTHORITIES.items()
        ):
            registered_conn = reference()
            if registered_conn is None:
                registered = _SCHEMA_CONNECTION_AUTHORITIES.get(connection_id)
                if (
                    registered is not None
                    and registered[0] is reference
                    and registered[1] is candidate
                ):
                    _SCHEMA_CONNECTION_AUTHORITIES.pop(connection_id)
                    stale_authorities.append(candidate)
                continue
            if registered_conn is conn:
                continue
            if (
                candidate.retains_parent_shared_lock
                and candidate.parent_identity == authority.parent_identity
            ):
                has_other = True
                break
    for stale_authority in stale_authorities:
        _close_schema_authority(stale_authority)
    return has_other


def _restore_schema_parent_lock(authority: _SchemaConnectionAuthority) -> bool:
    parent_fd = authority.parent_fd
    if parent_fd is None:
        return False
    if not authority.retains_parent_shared_lock:
        try:
            fcntl.flock(parent_fd, fcntl.LOCK_UN)
        except (BlockingIOError, OSError):
            raise local_state_error(LocalStateErrorCode.OPERATION_FAILED) from None
        return False
    retried = False
    for _attempt in range(_SCHEMA_PARENT_SHARED_LOCK_RECOVERY_ATTEMPTS):
        try:
            fcntl.flock(parent_fd, fcntl.LOCK_SH | fcntl.LOCK_NB)
        except (BlockingIOError, OSError):
            retried = True
            continue
        return retried
    raise local_state_error(LocalStateErrorCode.OPERATION_FAILED)


def _fail_closed_schema_connection(conn: sqlite3.Connection) -> None:
    try:
        conn.close()
    except Exception:
        authority = _release_schema_connection_authority(conn)
        if authority is not None:
            _close_schema_authority(authority)


class _ClosingConnection(sqlite3.Connection):
    """Connection that owns its registered schema authority until close."""

    def close(self) -> None:
        super().close()
        authority = _release_schema_connection_authority(self)
        if authority is not None:
            _close_schema_authority(authority)

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> Any:
        try:
            return super().__exit__(exc_type, exc, tb)
        finally:
            self.close()


@contextmanager
def _filesystem_schema_mutation_authority(
    conn: sqlite3.Connection,
) -> Iterator[None]:
    """Hold exclusive authority for one non-current filesystem schema branch."""

    authority = _schema_connection_authority(conn)
    parent_fd = authority.parent_fd
    leaf = authority.leaf
    selected_identity = authority.selected_identity
    if parent_fd is None or leaf is None or selected_identity is None:
        raise local_state_error(LocalStateErrorCode.OPERATION_FAILED)
    with _SCHEMA_CONNECTION_AUTHORITIES_LOCK:
        if _has_other_shared_parent_schema_authority(conn, authority):
            raise local_state_error(LocalStateErrorCode.OPERATION_FAILED)
        try:
            fcntl.flock(parent_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except (BlockingIOError, OSError):
            try:
                restored_after_contention = _restore_schema_parent_lock(authority)
            except LocalStateError:
                _fail_closed_schema_connection(conn)
            else:
                if restored_after_contention:
                    _fail_closed_schema_connection(conn)
            raise local_state_error(LocalStateErrorCode.OPERATION_FAILED) from None
    try:
        _validate_parent_fd(parent_fd, private=True)
        _verify_expected_identity_at(parent_fd, leaf, selected_identity)
        _validate_sqlite_family_at(parent_fd, leaf)
        try:
            yield
        finally:
            prepare_sqlite_family_at(
                parent_fd,
                leaf,
                retain_parent_shared_lock=authority.retains_parent_shared_lock,
                _parent_exclusive_lock_held=True,
                _expected_main_identity=selected_identity,
            )
            _validate_parent_fd(parent_fd, private=True)
            _verify_expected_identity_at(parent_fd, leaf, selected_identity)
            _validate_sqlite_family_at(parent_fd, leaf)
            _verify_expected_identity_at(parent_fd, leaf, selected_identity)
    finally:
        try:
            restored_after_contention = _restore_schema_parent_lock(authority)
        except LocalStateError:
            _fail_closed_schema_connection(conn)
            raise
        if restored_after_contention:
            _fail_closed_schema_connection(conn)
            raise local_state_error(LocalStateErrorCode.OPERATION_FAILED)


def _verify_expected_identity_at(
    parent_fd: int,
    leaf: str,
    expected_identity: EntryIdentity,
) -> os.stat_result:
    return verify_entry_identity(
        parent_fd,
        leaf,
        expected_identity,
        expected_type=EntryType.REGULAR_FILE,
    )


def _connect(
    db_path: Path | str,
    *,
    isolation_level: str | None = "",
    prepare: bool = False,
    read_only: bool = False,
    _store_lock_held: bool = False,
    _expected_db_identity: EntryIdentity | None = None,
) -> sqlite3.Connection:
    with _SCHEMA_CONNECTION_AUTHORITIES_LOCK:
        return _connect_unlocked(
            db_path,
            isolation_level=isolation_level,
            prepare=prepare,
            read_only=read_only,
            _store_lock_held=_store_lock_held,
            _expected_db_identity=_expected_db_identity,
        )


def _connect_unlocked(
    db_path: Path | str,
    *,
    isolation_level: str | None = "",
    prepare: bool = False,
    read_only: bool = False,
    _store_lock_held: bool = False,
    _expected_db_identity: EntryIdentity | None = None,
) -> sqlite3.Connection:
    if prepare and read_only:
        raise ValueError("read-only connections cannot prepare store state")
    raw_db_path = str(db_path)
    if _has_duplicate_sqlite_uri_mode(raw_db_path):
        raise ValueError("sqlite URI must have at most one mode parameter")
    memory_db = _is_memory_db(raw_db_path)
    if memory_db and _expected_db_identity is not None:
        raise ValueError("memory databases do not have filesystem identities")
    parent_fd: int | None = None
    leaf: str | None = None
    selected_identity: EntryIdentity | None = None
    if memory_db:
        connect_target = raw_db_path
        connect_uri = raw_db_path.startswith("file:")
    else:
        try:
            parent_fd, leaf = _open_filesystem_db(
                db_path,
                prepare=prepare,
                retain_parent_shared_lock=not _store_lock_held,
            )
        except LocalStateError as exc:
            if (
                _expected_db_identity is not None
                and exc.code is LocalStateErrorCode.MISSING_ENTRY
            ):
                raise local_state_error(LocalStateErrorCode.ENTRY_CHANGED) from None
            raise
        try:
            selected_family = _snapshot_sqlite_family_at(
                parent_fd,
                leaf,
                require_main=True,
            )
            selected = selected_family[0]
            if selected.identity is None:
                raise local_state_error(LocalStateErrorCode.ENTRY_CHANGED)
            selected_identity = selected.identity
            if (
                _expected_db_identity is not None
                and selected_identity != _expected_db_identity
            ):
                raise local_state_error(LocalStateErrorCode.ENTRY_CHANGED)
            canonical_path = canonical_path_from_fd(parent_fd, leaf)
            mode = "ro" if read_only else "rw"
            immutable_query = ""
            if read_only:
                wal = selected_family[1]
                shm = selected_family[2]
                settled = (
                    wal.state is PermissionState.ABSENT
                    or wal.size == 0
                )
                if settled:
                    immutable_query = "&immutable=1"
                elif shm.state is PermissionState.ABSENT:
                    raise local_state_error(
                        LocalStateErrorCode.OPERATION_FAILED
                    )
            connect_target = (
                f"file:{quote(canonical_path, safe='/')}?mode={mode}"
                f"{immutable_query}"
            )
        except LocalStateError as exc:
            os.close(parent_fd)
            if (
                _expected_db_identity is not None
                and exc.code is LocalStateErrorCode.MISSING_ENTRY
            ):
                raise local_state_error(LocalStateErrorCode.ENTRY_CHANGED) from None
            raise
        except Exception:
            os.close(parent_fd)
            raise
        connect_uri = True
    try:
        with private_file_creation_umask():
            conn = sqlite3.connect(
                connect_target,
                timeout=30.0,
                isolation_level=isolation_level,
                factory=_ClosingConnection,
                uri=connect_uri,
            )
    except Exception:
        if parent_fd is not None:
            os.close(parent_fd)
        raise
    if not isinstance(conn, _ClosingConnection):
        try:
            conn.close()
        finally:
            if parent_fd is not None:
                os.close(parent_fd)
        raise local_state_error(LocalStateErrorCode.OPERATION_FAILED) from None
    if parent_fd is not None and leaf is not None and selected_identity is not None:
        try:
            # Catch path substitution across sqlite3.connect before any pragma
            # can mutate a database other than the securely resolved one.
            canonical_path_from_fd(parent_fd, leaf)
            _verify_expected_identity_at(parent_fd, leaf, selected_identity)
        except Exception:
            conn.close()
            os.close(parent_fd)
            raise
    if parent_fd is not None:
        assert leaf is not None
        assert selected_identity is not None
        _register_schema_connection_authority(
            conn,
            parent_fd=parent_fd,
            leaf=leaf,
            selected_identity=selected_identity,
            retains_parent_shared_lock=not _store_lock_held,
        )
        parent_fd = None
    else:
        assert memory_db
        _register_schema_connection_authority(conn)
    try:
        with private_file_creation_umask():
            _apply_connection_pragmas(conn, db_path)
            if leaf is not None:
                authority = _schema_connection_authority(conn)
                assert authority.parent_fd is not None
                assert authority.selected_identity is not None
                _verify_expected_identity_at(
                    authority.parent_fd,
                    leaf,
                    authority.selected_identity,
                )
                if not read_only:
                    # Activate new sidecars only on normal store connections.
                    conn.execute("PRAGMA user_version").fetchone()
                _validate_sqlite_family_at(authority.parent_fd, leaf)
                _verify_expected_identity_at(
                    authority.parent_fd,
                    leaf,
                    authority.selected_identity,
                )
        return conn
    except Exception:
        conn.close()
        raise


def _canonical_json(data: Any) -> str:
    """Serialize private or pre-sanitized data without silently dropping fields."""
    return json.dumps(
        data,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )

_CONNECTOR_LEASE_STATUS = "leased"
_CONNECTOR_AWAITING_ACK_STATUS = "awaiting_ack"
_CONNECTOR_POLLABLE_STATUSES = frozenset({"queued", "deferred", "retry"})
_CONNECTOR_TERMINAL_OUTBOX_STATUS = "delivered"
_CONNECTOR_EXHAUSTED_OUTBOX_STATUS = "dead_letter"
_CONNECTOR_SUPERSEDED_OUTBOX_STATUS = "superseded"
_CONNECTOR_ACK_RETRY_BACKOFF_MAX_SECONDS = 30
_CONNECTOR_DRAIN_TARGET_SECONDS = 30
_TURN_FINAL_REASON_DETAIL_MAX_CHARS = 240
_CONNECTOR_TERMINAL_DIAGNOSTIC_KEY = "terminal_diagnostic"
_TURN_FINAL_PUBLIC_REASONS = frozenset(
    {
        "backpressure",
        "content_fetch_failed",
        "content_known_incomplete",
        "content_revision_not_found",
        "delivery_rejected",
        "delivery_uncertain",
        "invalid_content_page",
        "invalid_content_schema",
        "invalid_pending_plan",
        "invalid_prepare_response",
        "invalid_presentation_plan",
        "invalid_recovery_predecessor_receipt",
        "invalid_turn_final_job",
        "missing_message_owner_token",
        "operation_budget_exhausted",
        "predecessor_pending",
        "prepare_failed",
        "rate_limited",
        "rejected",
        "revision_conflict",
        "stale_or_unavailable_content_revision",
        "stale_or_unroutable_turn_plan",
        "stale_ref",
        "stale_revision",
        "temporary",
        "timeout",
        "transient_delivery",
        "unavailable",
        "unroutable_final_ready",
        "upgrade_required",
    }
)
_CONNECTOR_PUBLIC_OUTBOX_STATUSES = frozenset(
    {
        _CONNECTOR_LEASE_STATUS,
        _CONNECTOR_AWAITING_ACK_STATUS,
        _CONNECTOR_TERMINAL_OUTBOX_STATUS,
        _CONNECTOR_EXHAUSTED_OUTBOX_STATUS,
        _CONNECTOR_SUPERSEDED_OUTBOX_STATUS,
        *_CONNECTOR_POLLABLE_STATUSES,
    }
)
_CONNECTOR_REF_PREFIX = "twref1."


def _json_object(raw: Any) -> dict[str, Any]:
    if isinstance(raw, Mapping):
        return dict(raw)
    if not isinstance(raw, str) or not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except (TypeError, json.JSONDecodeError):
        return {}
    return dict(parsed) if isinstance(parsed, Mapping) else {}


def _connector_datetime(value: str) -> datetime:
    raw = str(value or "").strip()
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        parsed = datetime.now(timezone.utc)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _connector_strict_datetime(value: Any) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(raw)
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _connector_deadline_due(*values: Any, now_dt: datetime) -> bool:
    """Use the first valid deadline; malformed-only state is already overdue."""
    for value in values:
        parsed = _connector_strict_datetime(value)
        if parsed is not None:
            return parsed <= now_dt
    return True


def _connector_iso(value: str | datetime) -> str:
    parsed = value if isinstance(value, datetime) else _connector_datetime(str(value))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).isoformat(timespec="seconds")


def _connector_now(value: str | None = None) -> str:
    return _connector_iso(value or utc_timestamp())


def _connector_add_seconds(now: str, seconds: int) -> str:
    bounded = max(0, min(int(seconds), _MAX_TIMEDELTA_SECONDS))
    return _connector_iso(_connector_datetime(now) + timedelta(seconds=bounded))


def _utc_cutoff(*, retention_days: int, now: str | None = None) -> str:
    current = _connector_datetime(now or utc_timestamp())
    bounded_days = max(1, min(int(retention_days), _MAX_RETENTION_DAYS))
    cutoff = current - timedelta(days=bounded_days)
    return _connector_iso(cutoff)


def _connector_public_ref() -> str:
    return f"{_CONNECTOR_REF_PREFIX}{secrets.token_hex(32)}"



def _connector_public_reason(value: Any) -> str:
    clean = sanitize_public_mapping(
        {"reason": str(value or "").strip()},
        backend_neutral=True,
    ).get("reason")
    return clean if isinstance(clean, str) else ""


def _store_public_label(value: Any, *, allowed: Collection[str] | None = None) -> str:
    lowered = str(value or "").strip().lower().replace("-", "_")
    label = "".join(
        char if char.isalnum() or char in {"_", "."} else "_"
        for char in lowered
    )
    label = "_".join(part for part in label.split("_") if part).strip("._")[:64]
    if not label or (allowed is not None and label not in allowed):
        return "unknown"
    clean = sanitize_public_value(label, backend_neutral=True)
    return clean if isinstance(clean, str) and clean == label else "unknown"


def _store_public_text(
    value: Any,
    *,
    default: str = "",
    free_text: bool = False,
) -> str:
    text = str(value or "").strip()
    if free_text:
        clean = sanitize_public_mapping(
            {"reason": text},
            backend_neutral=True,
        ).get("reason")
    else:
        clean = sanitize_public_value(text, backend_neutral=True)
    return clean if isinstance(clean, str) and clean else default


def _store_private_diagnostic_text(value: Any) -> str:
    return sanitize_public_text(
        str(value or ""),
        max_chars=_TURN_FINAL_REASON_DETAIL_MAX_CHARS,
    )


def _turn_final_reason_diagnostics(value: Any) -> tuple[str, str]:
    public_reason = _store_public_label(
        value,
        allowed=_TURN_FINAL_PUBLIC_REASONS,
    )
    if public_reason != "unknown":
        return public_reason, ""
    return public_reason, _store_private_diagnostic_text(value)


def _connector_private_with_terminal_diagnostic(
    raw: Any,
    *,
    public_reason: str,
    reason_detail: str,
    attempt_count: int,
    classification: str,
    terminalized_at: str,
) -> str:
    state = _json_object(_connector_private_clear_current(raw))
    terminal = {
        "reason": _store_public_label(
            public_reason,
            allowed={*_TURN_FINAL_PUBLIC_REASONS, "missing", "unknown"},
        ),
        "attempt_count": max(0, min(int(attempt_count), (1 << 63) - 1)),
        "classification": _store_public_label(classification),
        "terminalized_at": str(terminalized_at),
    }
    if reason_detail:
        terminal["reason_detail"] = _store_private_diagnostic_text(reason_detail)
    state[_CONNECTOR_TERMINAL_DIAGNOSTIC_KEY] = terminal
    return _canonical_json(state)


def _connector_private_clear_terminal_diagnostic(raw: Any) -> str:
    state = _json_object(raw)
    state.pop(_CONNECTOR_TERMINAL_DIAGNOSTIC_KEY, None)
    return _canonical_json(state)


def _connector_attempt_count_conn(
    conn: sqlite3.Connection,
    *,
    outbox_id: int,
    private_state_json: Any,
) -> int:
    state = _json_object(private_state_json)
    prior_attempts = state.get("prior_attempt_count")
    local_attempts = conn.execute(
        """
        SELECT COALESCE(MAX(attempt), 0)
        FROM connector_deliveries
        WHERE outbox_id = ?
        """,
        (int(outbox_id),),
    ).fetchone()
    values = (
        prior_attempts
        if isinstance(prior_attempts, int) and not isinstance(prior_attempts, bool)
        else 0,
        int(local_attempts[0] or 0) if local_attempts is not None else 0,
    )
    return min(sum(value for value in values if value >= 0), (1 << 63) - 1)


def _turn_final_latest_reason_conn(
    conn: sqlite3.Connection,
    *,
    outbox_id: int,
) -> tuple[str, str]:
    row = conn.execute(
        """
        SELECT response_json
        FROM connector_deliveries
        WHERE outbox_id = ?
        ORDER BY id DESC
        LIMIT 1
        """,
        (int(outbox_id),),
    ).fetchone()
    response = _json_object(row[0]) if row is not None else {}
    if "reason" not in response:
        return "missing", ""
    public_reason = _store_public_label(
        response.get("reason"),
        allowed={*_TURN_FINAL_PUBLIC_REASONS, "missing", "unknown"},
    )
    detail = (
        _store_private_diagnostic_text(response.get("reason_detail"))
        if public_reason == "unknown"
        else ""
    )
    return public_reason, detail


def _connector_private_with_lease(
    raw: Any,
    *,
    delivery_id: int | None,
    attempt: int,
    lease_token: str,
    lease_expires_at: str,
    public_ref: str,
) -> str:
    state = _json_object(raw)
    state["current_delivery_id"] = delivery_id
    state["current_attempt"] = int(attempt)
    state["lease_token"] = str(lease_token)
    state["lease_expires_at"] = str(lease_expires_at)
    state["public_ref"] = str(public_ref)
    return _canonical_json(state)


def _connector_private_clear_current(raw: Any) -> str:
    state = _json_object(raw)
    for key in ("current_delivery_id", "current_attempt", "lease_token", "lease_expires_at", "public_ref"):
        state.pop(key, None)
    return _canonical_json(state)


def _connector_ack_retry_state(
    raw: Any,
    *,
    now: str,
) -> tuple[dict[str, Any], str]:
    """Record one ACK timeout and return its bounded exponential retry time."""
    state = _json_object(_connector_private_clear_current(raw))
    previous = state.get("ack_timeout_count")
    timeout_count = (
        int(previous) + 1
        if (
            isinstance(previous, int)
            and not isinstance(previous, bool)
            and previous >= 0
        )
        else 1
    )
    state["ack_timeout_count"] = timeout_count
    state.pop("ack_deadline_at", None)
    delay_seconds = min(
        1 << min(timeout_count - 1, 30),
        _CONNECTOR_ACK_RETRY_BACKOFF_MAX_SECONDS,
    )
    return state, _connector_add_seconds(str(now), delay_seconds)


def _connector_response(
    *,
    ok: bool,
    status: str,
    host_id: str,
    name: str,
    ref: str | None = None,
    key: str | None = None,
    attempt: int | None = None,
    available_at: str | None = None,
    leased_until: str | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "schema_version": 1,
        "ok": bool(ok),
        "status": str(status),
        "host_id": str(host_id),
        "name": str(name),
    }
    if ref is not None:
        payload["ref"] = str(ref)
    if key is not None:
        payload["key"] = str(key)
    if attempt is not None:
        payload["attempt"] = int(attempt)
    if available_at is not None:
        payload["available_at"] = str(available_at)
    if leased_until is not None:
        payload["leased_until"] = str(leased_until)
    return sanitize_public_value(payload)


def _connector_error_response(
    *,
    status: str,
    host_id: str,
    name: str,
    ref: str | None = None,
) -> dict[str, Any]:
    payload = _connector_response(ok=False, status=status, host_id=host_id, name=name, ref=ref)
    payload["error"] = {
        "code": str(status),
        "message": "reference is not valid for the requested operation",
    }
    return sanitize_public_value(payload)


def _connector_reclaim_expired_leases_conn(
    conn: sqlite3.Connection,
    *,
    host_id: str,
    name: str | None,
    now: str,
) -> int:
    clauses = ["d.status = ?", "d.host_id = ?"]
    params: list[Any] = [_CONNECTOR_LEASE_STATUS, str(host_id)]
    if name is not None:
        clauses.append("d.connector = ?")
        params.append(str(name))
    rows = conn.execute(
        f"""
        SELECT
            d.id,
            d.outbox_id,
            d.private_state_json,
            o.status,
            o.private_state_json
        FROM connector_deliveries d
        LEFT JOIN connector_outbox o ON o.id = d.outbox_id
        WHERE {" AND ".join(clauses)}
        """,
        params,
    ).fetchall()
    reclaimed = 0
    now_dt = _connector_datetime(now)
    for delivery_id, outbox_id, delivery_private, outbox_status, outbox_private in rows:
        state = _json_object(delivery_private)
        lease_expires_at = state.get("lease_expires_at")
        if not _connector_deadline_due(lease_expires_at, now_dt=now_dt):
            continue
        conn.execute(
            """
            UPDATE connector_deliveries
            SET status = ?, response_json = ?, delivered_at = ?
            WHERE id = ? AND status = ?
            """,
            (
                "expired",
                _canonical_json(
                    sanitize_public_mapping({"schema_version": 1, "status": "expired"})
                ),
                now,
                int(delivery_id),
                _CONNECTOR_LEASE_STATUS,
            ),
        )
        outbox_state = _json_object(outbox_private)
        current_delivery_id = outbox_state.get("current_delivery_id")
        if int(outbox_id or 0) > 0 and (
            current_delivery_id is None or int(current_delivery_id or 0) == int(delivery_id)
        ) and str(outbox_status or "") == _CONNECTOR_LEASE_STATUS:
            terminal_after_lease = bool(outbox_state.get("terminal_after_lease"))
            conn.execute(
                """
                UPDATE connector_outbox
                SET status = ?, next_attempt_at = NULL, updated_at = ?,
                    private_state_json = ?
                WHERE id = ? AND status = ?
                """,
                (
                    (
                        _CONNECTOR_SUPERSEDED_OUTBOX_STATUS
                        if terminal_after_lease
                        else "queued"
                    ),
                    now,
                    _connector_private_clear_current(outbox_private),
                    int(outbox_id),
                    _CONNECTOR_LEASE_STATUS,
                ),
            )
            terminal_status = (
                _CONNECTOR_SUPERSEDED_OUTBOX_STATUS
                if terminal_after_lease
                else "queued"
            )
            _update_presentation_plan_after_outbox_conn(
                conn,
                outbox_id=int(outbox_id),
                outbox_status=terminal_status,
                now=now,
            )
        reclaimed += 1
    reclaimed += _connector_reclaim_expired_awaiting_ack_conn(
        conn,
        host_id=str(host_id),
        name=str(name) if name is not None else None,
        now=str(now),
    )
    return reclaimed


def _connector_reclaim_expired_awaiting_ack_conn(
    conn: sqlite3.Connection,
    *,
    host_id: str,
    name: str | None,
    now: str,
) -> int:
    clauses = ["outbox.host_id = ?", "outbox.status = 'awaiting_ack'"]
    params: list[Any] = [str(host_id)]
    if name is not None:
        clauses.append("outbox.connector = ?")
        params.append(str(name))
    rows = conn.execute(
        f"""
        SELECT
            outbox.id,
            outbox.connector,
            outbox.private_state_json,
            outbox.next_attempt_at
        FROM connector_outbox AS outbox
        WHERE {" AND ".join(clauses)}
        ORDER BY outbox.id
        """,
        params,
    ).fetchall()
    reclaimed = 0
    now_dt = _connector_datetime(now)
    for outbox_id, connector, private_state_json, next_attempt_at in rows:
        outbox_state = _json_object(private_state_json)
        deadline = outbox_state.get("ack_deadline_at")
        delivery = conn.execute(
            """
            SELECT id, private_state_json
            FROM connector_deliveries
            WHERE outbox_id = ? AND status = 'awaiting_ack'
            ORDER BY id DESC
            LIMIT 1
            """,
            (int(outbox_id),),
        ).fetchone()
        delivery_deadline = (
            _json_object(delivery[1]).get("ack_deadline_at")
            if delivery is not None
            else None
        )
        # A malformed awaiting_ack row without a deadline is already overdue.
        # Every non-terminal state must be swept rather than wedged.
        if not _connector_deadline_due(
            deadline,
            next_attempt_at,
            delivery_deadline,
            now_dt=now_dt,
        ):
            continue

        plan = conn.execute(
            """
            SELECT id, state, generation
            FROM turn_presentation_plans
            WHERE source_outbox_id = ?
            ORDER BY id DESC
            LIMIT 1
            """,
            (int(outbox_id),),
        ).fetchone()
        if plan is not None and str(plan[1]) == "completed":
            if delivery is not None:
                conn.execute(
                    """
                    UPDATE connector_deliveries
                    SET status = 'delivered', delivered_at = COALESCE(delivered_at, ?)
                    WHERE id = ? AND status = 'awaiting_ack'
                    """,
                    (str(now), int(delivery[0])),
                )
            conn.execute(
                """
                UPDATE connector_outbox
                SET status = 'delivered', next_attempt_at = NULL,
                    updated_at = ?, private_state_json = ?
                WHERE id = ? AND status = 'awaiting_ack'
                """,
                (
                    str(now),
                    _connector_private_clear_current(private_state_json),
                    int(outbox_id),
                ),
            )
            reclaimed += 1
            continue

        recoverable = plan is not None and str(plan[1]) in {
            "active",
            "waiting_predecessor",
            "failed",
        }
        if recoverable:
            plan_id = int(plan[0])
            if _connector_plan_has_live_job_lease_conn(
                conn,
                plan_id=plan_id,
                now_dt=now_dt,
            ):
                continue
            conn.execute(
                "UPDATE turn_presentation_plans SET state = 'failed' WHERE id = ?",
                (plan_id,),
            )
            job_rows = conn.execute(
                """
                SELECT outbox.id, outbox.private_state_json
                FROM turn_presentation_jobs AS jobs
                JOIN connector_outbox AS outbox ON outbox.id = jobs.outbox_id
                WHERE jobs.plan_id = ?
                  AND outbox.status NOT IN ('delivered', 'superseded', 'dead_letter')
                """,
                (plan_id,),
            ).fetchall()
            for job_outbox_id, job_private in job_rows:
                job_reason, job_reason_detail = _turn_final_latest_reason_conn(
                    conn,
                    outbox_id=int(job_outbox_id),
                )
                job_attempt_count = _connector_attempt_count_conn(
                    conn,
                    outbox_id=int(job_outbox_id),
                    private_state_json=job_private,
                )
                conn.execute(
                    """
                    UPDATE connector_deliveries
                    SET status = 'failed', response_json = ?, delivered_at = ?
                    WHERE outbox_id = ? AND status = 'leased'
                    """,
                    (
                        _canonical_json(
                            {"schema_version": 1, "status": "ack_deadline_expired"}
                        ),
                        str(now),
                        int(job_outbox_id),
                    ),
                )
                conn.execute(
                    """
                    UPDATE connector_outbox
                    SET status = 'dead_letter', next_attempt_at = NULL,
                        updated_at = ?, private_state_json = ?
                    WHERE id = ?
                    """,
                    (
                        str(now),
                        _connector_private_with_terminal_diagnostic(
                            job_private,
                            public_reason=job_reason,
                            reason_detail=job_reason_detail,
                            attempt_count=job_attempt_count,
                            classification="ack_deadline_expired",
                            terminalized_at=str(now),
                        ),
                        int(job_outbox_id),
                    ),
                )
            source_state, retry_at = _connector_ack_retry_state(
                private_state_json,
                now=str(now),
            )
            source_state["presentation_generation"] = max(
                int(source_state.get("presentation_generation") or 1),
                int(plan[2] or 1) + 1,
            )
            source_status = "retry"
        else:
            source_reason, source_reason_detail = _turn_final_latest_reason_conn(
                conn,
                outbox_id=int(outbox_id),
            )
            source_state = _json_object(
                _connector_private_with_terminal_diagnostic(
                    private_state_json,
                    public_reason=source_reason,
                    reason_detail=source_reason_detail,
                    attempt_count=_connector_attempt_count_conn(
                        conn,
                        outbox_id=int(outbox_id),
                        private_state_json=private_state_json,
                    ),
                    classification="plan_unrecoverable",
                    terminalized_at=str(now),
                )
            )
            source_status = _CONNECTOR_EXHAUSTED_OUTBOX_STATUS
            retry_at = None

        if delivery is not None:
            conn.execute(
                """
                UPDATE connector_deliveries
                SET status = 'failed', response_json = ?, delivered_at = ?
                WHERE id = ? AND status = 'awaiting_ack'
                """,
                (
                    _canonical_json(
                        {
                            "schema_version": 1,
                            "status": (
                                "ack_deadline_expired"
                                if recoverable
                                else "plan_unrecoverable"
                            ),
                        }
                    ),
                    str(now),
                    int(delivery[0]),
                ),
            )
        conn.execute(
            """
            UPDATE connector_outbox
            SET status = ?, next_attempt_at = ?, updated_at = ?,
                private_state_json = ?
            WHERE id = ? AND status = 'awaiting_ack'
            """,
            (
                source_status,
                retry_at,
                str(now),
                _canonical_json(source_state),
                int(outbox_id),
            ),
        )
        _activate_waiting_presentation_plans_conn(
            conn,
            host_id=str(host_id),
            name=str(connector),
            now=str(now),
        )
        reclaimed += 1
    return reclaimed


def _connector_plan_has_live_job_lease_conn(
    conn: sqlite3.Connection,
    *,
    plan_id: int,
    now_dt: datetime,
) -> bool:
    live_job_leases = conn.execute(
        """
        SELECT deliveries.private_state_json
        FROM turn_presentation_jobs AS jobs
        JOIN connector_outbox AS outbox
          ON outbox.id = jobs.outbox_id
        JOIN connector_deliveries AS deliveries
          ON deliveries.outbox_id = outbox.id
        WHERE jobs.plan_id = ?
          AND outbox.status = 'leased'
          AND deliveries.status = 'leased'
        """,
        (int(plan_id),),
    ).fetchall()
    return any(
        not _connector_deadline_due(
            _json_object(row[0]).get("lease_expires_at"),
            now_dt=now_dt,
        )
        for row in live_job_leases
    )


def _connector_exhaust_retryable_conn(
    conn: sqlite3.Connection,
    *,
    host_id: str,
    name: str | None = None,
    max_attempts: int,
    now: str,
    dry_run: bool = False,
) -> int:
    clauses = [
        "host_id = ?",
        "status IN ('queued', 'deferred', 'retry')",
        """
        (
            SELECT
                COALESCE(MAX(d.attempt), 0)
                - COALESCE(
                    SUM(
                        CASE
                            WHEN d.status = 'failed'
                             AND COALESCE(
                                 json_extract(d.response_json, '$.status'),
                                 ''
                             ) = 'ack_deadline_expired'
                            THEN 1
                            ELSE 0
                        END
                    ),
                    0
                )
            FROM connector_deliveries d
            WHERE d.outbox_id = connector_outbox.id
        ) >= ?
        """,
    ]
    params: list[Any] = [str(host_id), max(1, int(max_attempts))]
    if name is not None:
        clauses.insert(1, "connector = ?")
        params.insert(1, str(name))
    where_sql = " AND ".join(clauses)
    if dry_run:
        row = conn.execute(
            f"SELECT COUNT(*) FROM connector_outbox WHERE {where_sql}",
            params,
        ).fetchone()
        return int(row[0] or 0)

    rows = conn.execute(
        f"""
        SELECT id, connector, private_state_json
        FROM connector_outbox
        WHERE {where_sql}
        ORDER BY id
        """,
        params,
    ).fetchall()
    for outbox_id, connector, private_state_json in rows:
        next_private_state = "{}"
        if str(connector) == _TURN_FINAL_NAME:
            public_reason, reason_detail = _turn_final_latest_reason_conn(
                conn,
                outbox_id=int(outbox_id),
            )
            next_private_state = _connector_private_with_terminal_diagnostic(
                private_state_json,
                public_reason=public_reason,
                reason_detail=reason_detail,
                attempt_count=_connector_attempt_count_conn(
                    conn,
                    outbox_id=int(outbox_id),
                    private_state_json=private_state_json,
                ),
                classification="attempts_exhausted",
                terminalized_at=str(now),
            )
        conn.execute(
            """
            UPDATE connector_outbox
            SET status = ?,
                next_attempt_at = NULL,
                updated_at = ?,
                private_state_json = ?
            WHERE id = ?
            """,
            (
                _CONNECTOR_EXHAUSTED_OUTBOX_STATUS,
                str(now),
                next_private_state,
                int(outbox_id),
            ),
        )
    _mark_exhausted_presentation_plans_conn(
        conn,
        host_id=str(host_id),
        name=str(name) if name is not None else None,
        now=str(now),
    )
    return len(rows)


def connector_reclaim_due(
    db_path: Path,
    host_id: str,
    name: str | None = None,
    *,
    now: str | None = None,
) -> bool:
    """Return whether connector work is due for reclaim without taking a write lock."""
    if not _sqlite_store_exists(db_path):
        return False
    current_time = _connector_now(now)
    now_dt = _connector_datetime(current_time)
    delivery_clauses = ["host_id = ?", "status = 'leased'"]
    outbox_clauses = ["host_id = ?", "status = 'awaiting_ack'"]
    params: list[Any] = [str(host_id)]
    if name is not None:
        delivery_clauses.append("connector = ?")
        outbox_clauses.append("connector = ?")
        params.append(str(name))
    with _connect(db_path) as conn:
        _ensure_schema(conn)
        lease_rows = conn.execute(
            f"""
            SELECT private_state_json
            FROM connector_deliveries
            WHERE {' AND '.join(delivery_clauses)}
            """,
            params,
        ).fetchall()
        for (private_state_json,) in lease_rows:
            expires_at = _json_object(private_state_json).get("lease_expires_at")
            if _connector_deadline_due(expires_at, now_dt=now_dt):
                return True
        awaiting_rows = conn.execute(
            f"""
            SELECT
                outbox.id,
                outbox.private_state_json,
                outbox.next_attempt_at,
                (
                    SELECT deliveries.private_state_json
                    FROM connector_deliveries AS deliveries
                    WHERE deliveries.outbox_id = outbox.id
                      AND deliveries.status = 'awaiting_ack'
                    ORDER BY deliveries.id DESC
                    LIMIT 1
                )
            FROM connector_outbox AS outbox
            WHERE {' AND '.join(outbox_clauses)}
            """,
            params,
        ).fetchall()
        for (
            outbox_id,
            outbox_private,
            next_attempt_at,
            delivery_private,
        ) in awaiting_rows:
            if not _connector_deadline_due(
                _json_object(outbox_private).get("ack_deadline_at"),
                next_attempt_at,
                _json_object(delivery_private).get("ack_deadline_at"),
                now_dt=now_dt,
            ):
                continue
            plan = conn.execute(
                """
                SELECT id, state
                FROM turn_presentation_plans
                WHERE source_outbox_id = ?
                ORDER BY id DESC
                LIMIT 1
                """,
                (int(outbox_id),),
            ).fetchone()
            if (
                plan is not None
                and str(plan[1]) in {"active", "waiting_predecessor", "failed"}
                and _connector_plan_has_live_job_lease_conn(
                    conn,
                    plan_id=int(plan[0]),
                    now_dt=now_dt,
                )
            ):
                continue
            return True
    return False


def reclaim_expired_connector_leases(
    db_path: Path,
    host_id: str,
    name: str | None = None,
    *,
    now: str | None = None,
) -> dict[str, Any]:
    """Expire stale connector leases and return their outbox rows to polling."""
    if not _sqlite_store_exists(db_path):
        return sanitize_public_value({
            "schema_version": 1,
            "ok": False,
            "status": "store_unavailable",
            "host_id": str(host_id),
            "name": str(name or ""),
            "reclaimed": 0,
        })
    current_time = _connector_now(now)
    with _connect(db_path, isolation_level=None) as conn:
        _ensure_schema(conn)
        conn.execute("BEGIN IMMEDIATE")
        try:
            reclaimed = _connector_reclaim_expired_leases_conn(
                conn,
                host_id=str(host_id),
                name=str(name) if name is not None else None,
                now=current_time,
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    return sanitize_public_value({
        "schema_version": 1,
        "ok": True,
        "status": "ok",
        "host_id": str(host_id),
        "name": str(name or ""),
        "reclaimed": int(reclaimed),
    })


def exhaust_connector_retries(
    db_path: Path,
    host_id: str,
    *,
    max_attempts: int,
    now: str | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Move host-scoped retryable outbox rows beyond max attempts to a neutral terminal state."""
    if not _sqlite_store_exists(db_path):
        return sanitize_public_value({
            "schema_version": 1,
            "ok": False,
            "status": "store_unavailable",
            "host_id": str(host_id),
            "dry_run": bool(dry_run),
            "updated": 0,
        })
    current_time = _connector_now(now)
    attempt_limit = max(1, int(max_attempts))
    with _connect(db_path, isolation_level=None) as conn:
        _ensure_schema(conn)
        conn.execute("BEGIN IMMEDIATE")
        try:
            if dry_run:
                conn.execute("SAVEPOINT dry_run_exhaust_connector_retries")
                try:
                    _connector_reclaim_expired_leases_conn(
                        conn,
                        host_id=str(host_id),
                        name=None,
                        now=current_time,
                    )
                    updated = _connector_exhaust_retryable_conn(
                        conn,
                        host_id=str(host_id),
                        max_attempts=attempt_limit,
                        now=current_time,
                    )
                finally:
                    conn.execute("ROLLBACK TO dry_run_exhaust_connector_retries")
                    conn.execute("RELEASE dry_run_exhaust_connector_retries")
            else:
                _connector_reclaim_expired_leases_conn(
                    conn,
                    host_id=str(host_id),
                    name=None,
                    now=current_time,
                )
                updated = _connector_exhaust_retryable_conn(
                    conn,
                    host_id=str(host_id),
                    max_attempts=attempt_limit,
                    now=current_time,
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    return sanitize_public_value({
        "schema_version": 1,
        "ok": True,
        "status": "ok",
        "host_id": str(host_id),
        "dry_run": bool(dry_run),
        "max_attempts": attempt_limit,
        "updated": int(updated),
    })

_TURN_FINAL_NAME = "turn-final"
_PRESENTATION_SCHEMA_VERSION = 1
_PRESENTATION_MAX_PARTS = 10_000
_PRESENTATION_MAX_SPANS_PER_PART = 64
_PRESENTATION_SEQUENCE_WIDTH = 6
_PRESENTATION_RECOVERY_HISTORY_LIMIT = 4
_PRESENTATION_FIELDS = ("user_text", "assistant_final_text")
_PRESENTATION_FIELD_RANK = {
    field: index for index, field in enumerate(_PRESENTATION_FIELDS)
}
_PRESENTATION_TOKEN_CHARS = frozenset(
    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_"
)
_PRESENTATION_LABEL_CHARS = frozenset(
    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-"
)


def _valid_final_stable_key(value: Any) -> bool:
    return (
        isinstance(value, str)
        and value.startswith("wsk1_")
        and len(value) == 69
        and all(char in "0123456789abcdef" for char in value[5:])
    )


def _turn_ordering_key_conn(
    conn: sqlite3.Connection,
    *,
    host_id: str,
    turn_id: str,
) -> str:
    row = conn.execute(
        "SELECT worker_id, payload_json FROM turns WHERE host_id = ? AND turn_id = ?",
        (str(host_id), str(turn_id)),
    ).fetchone()
    if row is None:
        return f"orphan:{turn_id}"
    payload = _json_object(row[1])
    meta = _json_object(payload.get("meta"))
    stable_key = meta.get("stable_key")
    if (
        _valid_final_stable_key(stable_key)
        and type(meta.get("stable_key_version")) is int
        and meta.get("stable_key_version") == 1
    ):
        return str(stable_key)
    return str(row[0] or f"orphan:{turn_id}")


def _final_content_field_descriptor(
    *,
    revision: str,
    field: str,
    availability: str,
    char_length: int,
    byte_length: int,
    page_count: int,
) -> dict[str, Any]:
    complete = str(availability) == "complete"
    pageable = complete and int(char_length) > 0 and int(page_count) > 0
    return {
        "availability": str(availability),
        "inline": False,
        "char_length": int(char_length),
        "byte_length": int(byte_length),
        "page_count": int(page_count) if complete else 0,
        "first_cursor": (
            content_cursor(
                str(revision),
                str(field),
                0,
                start_char=0,
                start_byte=0,
            )
            if pageable
            else None
        ),
    }


def _final_ready_payload_conn(
    conn: sqlite3.Connection,
    *,
    host_id: str,
    turn_id: str,
    content_revision_value: str,
    allow_unroutable: bool = False,
) -> dict[str, Any] | None:
    canonical_turn_id = _resolve_canonical_turn_id_conn(
        conn,
        str(host_id),
        turn_id,
    )
    if canonical_turn_id is None:
        return None
    turn_id = canonical_turn_id
    row = conn.execute(
        """
        SELECT
            turns.worker_id,
            turns.space_id,
            turns.payload_json,
            revisions.user_state,
            revisions.final_state,
            revisions.user_char_length,
            revisions.user_byte_length,
            revisions.final_char_length,
            revisions.final_byte_length,
            revisions.user_page_count,
            revisions.final_page_count,
            revisions.is_current
        FROM turns
        JOIN turn_content_revisions AS revisions
          ON revisions.host_id = turns.host_id
         AND revisions.turn_id = turns.turn_id
         AND revisions.content_revision = ?
        WHERE turns.host_id = ?
          AND turns.turn_id = ?
        """,
        (
            str(content_revision_value),
            str(host_id),
            str(turn_id),
        ),
    ).fetchone()
    if (
        row is None
        or int(row[11] or 0) != 1
        or str(row[4]) != "complete"
    ):
        return None
    turn_payload = _json_object(row[2])
    turn_meta = _json_object(turn_payload.get("meta"))
    stable_key = turn_meta.get("stable_key")
    stable_key_version = turn_meta.get("stable_key_version")
    routable = (
        _valid_final_stable_key(stable_key)
        and type(stable_key_version) is int
        and stable_key_version == 1
    )
    if not routable and not allow_unroutable:
        return None
    final_identity = turn_final_delivery_identity(
        str(host_id),
        str(turn_id),
        str(content_revision_value),
    )
    content = {
        "schema_version": 1,
        "content_revision": str(content_revision_value),
        "known_incomplete": str(row[3]) == "known_incomplete",
        "fields": {
            "user_text": _final_content_field_descriptor(
                revision=str(content_revision_value),
                field="user_text",
                availability=str(row[3]),
                char_length=int(row[5] or 0),
                byte_length=int(row[6] or 0),
                page_count=int(row[9] or 0),
            ),
            "assistant_final_text": _final_content_field_descriptor(
                revision=str(content_revision_value),
                field="assistant_final_text",
                availability=str(row[4]),
                char_length=int(row[7] or 0),
                byte_length=int(row[8] or 0),
                page_count=int(row[10] or 0),
            ),
        },
    }
    payload = {
        "schema_version": 2 if routable else 1,
        "operation": "materialize",
        "final_identity": final_identity,
        "turn_id": str(turn_id),
        "worker_id": str(row[0]),
        "space_id": str(row[1]) if row[1] is not None else None,
        "content_revision": str(content_revision_value),
        "content": content,
    }
    if routable:
        payload["stable_key"] = str(stable_key)
        payload["stable_key_version"] = 1
    return payload


def _final_revision_is_internal_automation_conn(
    conn: sqlite3.Connection,
    *,
    host_id: str,
    turn_id: str,
    content_revision_value: str,
) -> bool:
    row = conn.execute(
        """
        SELECT turns.payload_json,
               revisions.user_text,
               revisions.assistant_final_text
        FROM turns
        JOIN turn_content_revisions AS revisions
          ON revisions.host_id = turns.host_id
         AND revisions.turn_id = turns.turn_id
        WHERE turns.host_id = ?
          AND turns.turn_id = ?
          AND revisions.content_revision = ?
        """,
        (str(host_id), str(turn_id), str(content_revision_value)),
    ).fetchone()
    if row is None:
        return True
    payload = _json_object(row[0])
    payload["user_text"] = row[1]
    payload["assistant_final_text"] = row[2]
    return is_internal_automation_turn_payload(payload)


def _source_less_authoritative_route_conn(
    conn: sqlite3.Connection,
    *,
    host_id: str,
    turn_id: str,
    content_revision_value: str,
) -> dict[str, Any] | None:
    existing = conn.execute(
        """
        SELECT delivery_kind, status, payload_json
        FROM connector_outbox
        WHERE host_id = ?
          AND connector = ?
          AND turn_id = ?
          AND content_revision = ?
          AND delivery_kind IN ('final_ready', 'final_migration_hold')
        ORDER BY id DESC
        LIMIT 1
        """,
        (
            str(host_id),
            _TURN_FINAL_NAME,
            str(turn_id),
            str(content_revision_value),
        ),
    ).fetchone()
    if existing is not None:
        if str(existing[0]) != "final_ready" or str(existing[1]) != "delivered":
            return None
        route = _json_object(existing[2])
    else:
        candidate = _final_ready_payload_conn(
            conn,
            host_id=str(host_id),
            turn_id=str(turn_id),
            content_revision_value=str(content_revision_value),
        )
        route = dict(candidate) if candidate is not None else {}
    expected_identity = turn_final_delivery_identity(
        str(host_id),
        str(turn_id),
        str(content_revision_value),
    )
    if (
        route.get("schema_version") != 2
        or str(route.get("turn_id") or "") != str(turn_id)
        or str(route.get("content_revision") or "") != str(content_revision_value)
        or str(route.get("final_identity") or "") != expected_identity
        or not _valid_final_stable_key(route.get("stable_key"))
        or type(route.get("stable_key_version")) is not int
        or route.get("stable_key_version") != 1
        or _final_revision_is_internal_automation_conn(
            conn,
            host_id=str(host_id),
            turn_id=str(turn_id),
            content_revision_value=str(content_revision_value),
        )
    ):
        return None
    return route


def _supersede_stale_final_work_conn(
    conn: sqlite3.Connection,
    *,
    host_id: str,
    turn_id: str,
    content_revision_value: str,
    now: str,
) -> None:
    stale_outbox_ids = [
        int(row[0])
        for row in conn.execute(
            """
            SELECT id
            FROM connector_outbox
            WHERE host_id = ?
              AND connector = ?
              AND turn_id = ?
              AND delivery_kind IN ('final_ready', 'final_migration_hold')
              AND content_revision != ?
              AND status NOT IN ('delivered', 'superseded')
            """,
            (
                str(host_id),
                _TURN_FINAL_NAME,
                str(turn_id),
                str(content_revision_value),
            ),
        ).fetchall()
    ]
    stale_plan_ids = [
        int(row[0])
        for row in conn.execute(
            """
            SELECT id
            FROM turn_presentation_plans
            WHERE host_id = ?
              AND name = ?
              AND turn_id = ?
              AND content_revision != ?
              AND state IN (
                  'preparing',
                  'waiting_predecessor',
                  'active',
                  'failed'
              )
            """,
            (
                str(host_id),
                _TURN_FINAL_NAME,
                str(turn_id),
                str(content_revision_value),
            ),
        ).fetchall()
    ]
    if stale_plan_ids:
        placeholders = ",".join("?" for _ in stale_plan_ids)
        conn.execute(
            f"UPDATE turn_presentation_plans SET state = 'superseded' "
            f"WHERE id IN ({placeholders})",
            stale_plan_ids,
        )
        stale_outbox_ids.extend(
            int(row[0])
            for row in conn.execute(
                f"""
                SELECT outbox_id
                FROM turn_presentation_jobs
                WHERE plan_id IN ({placeholders})
                  AND outbox_id IS NOT NULL
                """,
                stale_plan_ids,
            ).fetchall()
        )
    stale_outbox_ids = sorted(set(stale_outbox_ids))
    if not stale_outbox_ids:
        return
    placeholders = ",".join("?" for _ in stale_outbox_ids)
    conn.execute(
        f"""
        UPDATE connector_deliveries
        SET status = 'superseded',
            response_json = ?,
            delivered_at = COALESCE(delivered_at, ?)
        WHERE outbox_id IN ({placeholders}) AND status != 'delivered'
        """,
        (
            _canonical_json(
                {
                    "schema_version": 1,
                    "status": "superseded",
                }
            ),
            str(now),
            *stale_outbox_ids,
        ),
    )
    for outbox_id, private_state_json in conn.execute(
        f"""
        SELECT id, private_state_json
        FROM connector_outbox
        WHERE id IN ({placeholders})
          AND status NOT IN ('delivered', 'superseded')
        """,
        stale_outbox_ids,
    ).fetchall():
        conn.execute(
            """
            UPDATE connector_outbox
            SET status = 'superseded',
                next_attempt_at = NULL,
                updated_at = ?,
                private_state_json = ?
            WHERE id = ?
            """,
            (
                str(now),
                _connector_private_clear_current(private_state_json),
                int(outbox_id),
            ),
        )


def _reactivate_final_root_conn(
    conn: sqlite3.Connection,
    *,
    host_id: str,
    turn_id: str,
    content_revision_value: str,
    outbox_id: int,
    private_state_json: Any,
    latest_activated_plan_id: int | None,
    delivery_kind: str,
    initial_status: str,
    payload_json: str,
    now: str,
) -> None:
    plan_rows = conn.execute(
        """
        SELECT id, generation, part_count
        FROM turn_presentation_plans
        WHERE host_id = ?
          AND name = ?
          AND turn_id = ?
          AND content_revision = ?
        """,
        (
            str(host_id),
            _TURN_FINAL_NAME,
            str(turn_id),
            str(content_revision_value),
        ),
    ).fetchall()
    plan_ids = [int(row[0]) for row in plan_rows]
    max_generation = max(
        (int(row[1] or 0) for row in plan_rows),
        default=0,
    )
    max_part_count = max(
        (int(row[2] or 0) for row in plan_rows),
        default=0,
    )
    state = _json_object(private_state_json)
    root_generation = max(
        1,
        int(state.get("presentation_generation") or 1),
        max_generation,
    ) + 1
    retained_footprint = max(
        int(state.get("presentation_max_part_count") or 0),
        max_part_count,
    )
    prior_attempt_count = int(state.get("prior_attempt_count") or 0) + int(
        conn.execute(
            """
            SELECT COUNT(*)
            FROM connector_deliveries
            WHERE outbox_id = ?
            """,
            (int(outbox_id),),
        ).fetchone()[0]
        or 0
    )
    if plan_ids:
        placeholders = ",".join("?" for _ in plan_ids)
        conn.execute(
            f"""
            UPDATE turn_presentation_plans
            SET state = 'superseded'
            WHERE id IN ({placeholders})
              AND state IN (
                  'preparing',
                  'waiting_predecessor',
                  'active',
                  'failed'
              )
            """,
            plan_ids,
        )
        job_outbox_ids = [
            int(row[0])
            for row in conn.execute(
                f"""
                SELECT outbox_id
                FROM turn_presentation_jobs
                WHERE plan_id IN ({placeholders})
                  AND outbox_id IS NOT NULL
                """,
                plan_ids,
            ).fetchall()
        ]
        conn.execute(
            f"""
            DELETE FROM turn_presentation_recoveries
            WHERE failed_plan_id IN ({placeholders})
               OR recovered_plan_id IN ({placeholders})
            """,
            (*plan_ids, *plan_ids),
        )
        conn.execute(
            f"""
            DELETE FROM turn_presentation_plans
            WHERE id IN ({placeholders})
              AND state IN ('completed', 'superseded')
            """,
            plan_ids,
        )
        if job_outbox_ids:
            outbox_placeholders = ",".join("?" for _ in job_outbox_ids)
            conn.execute(
                f"""
                DELETE FROM connector_deliveries
                WHERE outbox_id IN ({outbox_placeholders})
                """,
                job_outbox_ids,
            )
            conn.execute(
                f"""
                DELETE FROM connector_outbox
                WHERE id IN ({outbox_placeholders})
                """,
                job_outbox_ids,
            )
    conn.execute(
        "DELETE FROM connector_deliveries WHERE outbox_id = ?",
        (int(outbox_id),),
    )
    state = _json_object(
        _connector_private_clear_terminal_diagnostic(
            _connector_private_clear_current(state)
        )
    )
    state["presentation_generation"] = root_generation
    state["presentation_max_part_count"] = retained_footprint
    state["prior_attempt_count"] = prior_attempt_count
    if latest_activated_plan_id is not None:
        state["reactivated_after_plan_id"] = int(latest_activated_plan_id)
    conn.execute(
        """
        UPDATE connector_outbox
        SET delivery_kind = ?,
            status = ?,
            payload_json = ?,
            private_state_json = ?,
            updated_at = ?,
            next_attempt_at = NULL
        WHERE id = ?
        """,
        (
            str(delivery_kind),
            str(initial_status),
            str(payload_json),
            _canonical_json(state),
            str(now),
            int(outbox_id),
        ),
    )


def _discard_acknowledged_final_replay_conn(
    conn: sqlite3.Connection,
    *,
    host_id: str,
    turn_id: str,
    content_revision_value: str,
) -> None:
    live_graph = conn.execute(
        """
        SELECT 1
        WHERE EXISTS (
            SELECT 1
            FROM connector_outbox
            WHERE host_id = ? AND turn_id = ?
        )
           OR EXISTS (
            SELECT 1
            FROM turn_presentation_plans
            WHERE host_id = ? AND turn_id = ?
        )
        """,
        (str(host_id), str(turn_id), str(host_id), str(turn_id)),
    ).fetchone()
    if live_graph is not None:
        return
    conn.execute(
        """
        DELETE FROM turn_content_page_boundaries
        WHERE host_id = ? AND turn_id = ? AND content_revision = ?
        """,
        (str(host_id), str(turn_id), str(content_revision_value)),
    )
    conn.execute(
        """
        DELETE FROM turn_content_revisions
        WHERE host_id = ? AND turn_id = ? AND content_revision = ?
        """,
        (str(host_id), str(turn_id), str(content_revision_value)),
    )
    deleted = conn.execute(
        """
        DELETE FROM turns
        WHERE host_id = ? AND turn_id = ?
          AND NOT EXISTS (
              SELECT 1
              FROM turn_content_revisions
              WHERE host_id = ? AND turn_id = ?
          )
        """,
        (str(host_id), str(turn_id), str(host_id), str(turn_id)),
    )
    if deleted.rowcount:
        _increment_turn_list_generation_conn(conn, str(host_id))


def _ensure_final_ready_anchor_conn(
    conn: sqlite3.Connection,
    *,
    host_id: str,
    turn_id: str,
    content_revision_value: str,
    now: str,
) -> int | None:
    payload = _final_ready_payload_conn(
        conn,
        host_id=str(host_id),
        turn_id=str(turn_id),
        content_revision_value=str(content_revision_value),
        allow_unroutable=True,
    )
    if payload is None:
        return None
    final_identity = str(payload["final_identity"])
    delivery_key = f"{_TURN_FINAL_NAME}:revision:{final_identity}"
    existing = conn.execute(
        """
        SELECT id, status, private_state_json, payload_json
        FROM connector_outbox
        WHERE host_id = ? AND connector = ? AND delivery_key = ?
        """,
        (str(host_id), _TURN_FINAL_NAME, delivery_key),
    ).fetchone()
    if existing is None:
        source_less_delivery = conn.execute(
            """
            SELECT 1
            FROM turn_presentation_plans AS plan
            WHERE plan.host_id = ?
              AND plan.name = ?
              AND plan.turn_id = ?
              AND plan.content_revision = ?
              AND plan.source_outbox_id IS NULL
              AND plan.state IN ('waiting_predecessor', 'active', 'completed')
              AND EXISTS (
                  SELECT 1
                  FROM turn_presentation_jobs AS job
                  WHERE job.plan_id = plan.id
                    AND job.outbox_id IS NOT NULL
              )
            LIMIT 1
            """,
            (
                str(host_id),
                _TURN_FINAL_NAME,
                str(turn_id),
                str(content_revision_value),
            ),
        ).fetchone()
        if source_less_delivery is not None:
            return None
        acknowledged_tombstone = conn.execute(
            """
            SELECT 1
            FROM connector_deliveries
            WHERE outbox_id IS NULL
              AND host_id = ?
              AND connector = ?
              AND delivery_key = ?
              AND status = 'delivered'
              AND delivered_at IS NOT NULL
            LIMIT 1
            """,
            (str(host_id), _TURN_FINAL_NAME, delivery_key),
        ).fetchone()
        if acknowledged_tombstone is not None:
            _discard_acknowledged_final_replay_conn(
                conn,
                host_id=str(host_id),
                turn_id=str(turn_id),
                content_revision_value=str(content_revision_value),
            )
            return None
    latest_activated = conn.execute(
        """
        SELECT plans.id, plans.content_revision
        FROM turn_presentation_plans AS plans
        WHERE plans.host_id = ?
          AND plans.name = ?
          AND plans.turn_id = ?
          AND plans.activated_at IS NOT NULL
          AND EXISTS (
              SELECT 1
              FROM turn_presentation_jobs AS jobs
              JOIN connector_deliveries AS attempts
                ON attempts.outbox_id = jobs.outbox_id
              WHERE jobs.plan_id = plans.id
          )
        ORDER BY plans.id DESC
        LIMIT 1
        """,
        (str(host_id), _TURN_FINAL_NAME, str(turn_id)),
    ).fetchone()
    known_incomplete = bool(payload["content"]["known_incomplete"])
    unroutable = payload.get("schema_version") != 2
    internal_automation = _final_revision_is_internal_automation_conn(
        conn,
        host_id=str(host_id),
        turn_id=str(turn_id),
        content_revision_value=str(content_revision_value),
    )
    delivery_kind = (
        "final_migration_hold"
        if known_incomplete or unroutable or internal_automation
        else "final_ready"
    )
    initial_status = (
        _CONNECTOR_EXHAUSTED_OUTBOX_STATUS
        if known_incomplete or unroutable or internal_automation
        else "queued"
    )
    initial_private_state = "{}"
    if initial_status == _CONNECTOR_EXHAUSTED_OUTBOX_STATUS:
        hold_reason = (
            "content_known_incomplete"
            if known_incomplete
            else "unroutable_final_ready"
            if unroutable
            else "internal_automation"
        )
        public_reason, reason_detail = _turn_final_reason_diagnostics(hold_reason)
        initial_private_state = _connector_private_with_terminal_diagnostic(
            {},
            public_reason=public_reason,
            reason_detail=reason_detail,
            attempt_count=0,
            classification="migration_hold",
            terminalized_at=str(now),
        )
    payload_json = _canonical_json(payload)
    ordering_key = _turn_ordering_key_conn(
        conn,
        host_id=str(host_id),
        turn_id=str(turn_id),
    )
    if existing is not None:
        existing_state = _json_object(existing[2])
        activated_after = (
            int(latest_activated[0]) if latest_activated is not None else None
        )
        different_activated_revision = bool(
            latest_activated is not None
            and str(latest_activated[1]) != str(content_revision_value)
            and int(existing_state.get("reactivated_after_plan_id") or 0)
            != int(latest_activated[0])
        )
        reactivate = (
            str(existing[1]) == _CONNECTOR_SUPERSEDED_OUTBOX_STATUS
            or (
                str(existing[1]) == _CONNECTOR_TERMINAL_OUTBOX_STATUS
                and different_activated_revision
            )
        )
        if reactivate and str(existing[3]) == payload_json:
            _reactivate_final_root_conn(
                conn,
                host_id=str(host_id),
                turn_id=str(turn_id),
                content_revision_value=str(content_revision_value),
                outbox_id=int(existing[0]),
                private_state_json=existing[2],
                latest_activated_plan_id=activated_after,
                delivery_kind=delivery_kind,
                initial_status=initial_status,
                payload_json=payload_json,
                now=str(now),
            )
    _supersede_stale_final_work_conn(
        conn,
        host_id=str(host_id),
        turn_id=str(turn_id),
        content_revision_value=str(content_revision_value),
        now=str(now),
    )
    conn.execute(
        """
        INSERT INTO connector_outbox (
            host_id,
            connector,
            delivery_key,
            delivery_kind,
            turn_id,
            content_revision,
            ordering_key,
            status,
            payload_json,
            private_state_json,
            created_at,
            updated_at,
            next_attempt_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)
        ON CONFLICT(host_id, connector, delivery_key) DO NOTHING
        """,
        (
            str(host_id),
            _TURN_FINAL_NAME,
            delivery_key,
            delivery_kind,
            str(turn_id),
            str(content_revision_value),
            ordering_key,
            initial_status,
            payload_json,
            initial_private_state,
            str(now),
            str(now),
        ),
    )
    row = conn.execute(
        """
        SELECT id
        FROM connector_outbox
        WHERE host_id = ? AND connector = ? AND delivery_key = ?
        """,
        (str(host_id), _TURN_FINAL_NAME, delivery_key),
    ).fetchone()
    return int(row[0]) if row is not None else None


def _valid_presentation_opaque(value: Any, prefix: str) -> bool:
    if not isinstance(value, str) or not value.startswith(prefix):
        return False
    body = value[len(prefix) :]
    return bool(body) and all(char in _PRESENTATION_TOKEN_CHARS for char in body)


def _valid_presentation_label(value: Any, *, prefix: str | None = None) -> bool:
    if not isinstance(value, str) or not value or len(value) > 128:
        return False
    if prefix is not None and not value.startswith(prefix):
        return False
    if any(char not in _PRESENTATION_LABEL_CHARS for char in value):
        return False
    return sanitize_public_value(value, backend_neutral=True) == value




def _presentation_plan_token(
    *,
    host_id: str,
    name: str,
    turn_id: str,
    content_revision_value: str,
    presentation_version: str,
    part_count: int,
    generation: int = 1,
) -> str:
    identity: dict[str, Any] = {
        "domain": "tendwire.connector.prepare.v1",
        "host_id": str(host_id),
        "name": str(name),
        "turn_id": str(turn_id),
        "content_revision": str(content_revision_value),
        "presentation_version": str(presentation_version),
        "part_count": int(part_count),
    }
    if int(generation) > 1:
        identity["generation"] = int(generation)
    digest = stable_fingerprint(identity, length=64)
    return f"twplan1.{digest}"


def _presentation_recovery_token(
    *,
    host_id: str,
    name: str,
    failed_plan_token: str,
    request_id: str,
    generation: int,
) -> str:
    digest = stable_fingerprint(
        {
            "domain": "tendwire.connector.prepare.recover.v1",
            "host_id": str(host_id),
            "name": str(name),
            "failed_plan_token": str(failed_plan_token),
            "request_id": str(request_id),
            "generation": int(generation),
        },
        length=64,
    )
    return f"twplan1.{digest}"


def _restore_presentation_tokens(
    sanitized: dict[str, Any],
    original: Mapping[str, Any],
) -> dict[str, Any]:
    for key in (
        "plan_token",
        "replaces_plan_token",
        "recovers_plan_token",
        "failed_plan_token",
        "recovered_plan_token",
    ):
        value = original.get(key)
        if value is None and key in original:
            sanitized[key] = None
        elif (
            isinstance(value, str)
            and value.startswith("twplan1.")
            and value[8:]
            and all(char.isalnum() or char in "-_" for char in value[8:])
        ):
            sanitized[key] = value
    final_identity = original.get("final_identity")
    if _valid_presentation_opaque(final_identity, "twfinal1."):
        sanitized["final_identity"] = str(final_identity)
    content_revision_value = original.get("content_revision")
    if _valid_presentation_opaque(content_revision_value, "twrev1."):
        sanitized["content_revision"] = str(content_revision_value)
    turn_id = original.get("turn_id")
    if _valid_presentation_label(turn_id, prefix="turn-"):
        sanitized["turn_id"] = str(turn_id)
    for nested_key in ("turn", "content"):
        nested_turn = original.get(nested_key)
        if not isinstance(nested_turn, Mapping):
            continue
        clean_nested = sanitized.get(nested_key)
        if not isinstance(clean_nested, dict):
            clean_nested = dict(
                sanitize_public_mapping(
                    nested_turn,
                    backend_neutral=True,
                )
            )
            sanitized[nested_key] = clean_nested
        _restore_presentation_tokens(clean_nested, nested_turn)
    return sanitized


def _presentation_response(payload: Mapping[str, Any]) -> dict[str, Any]:
    return _restore_presentation_tokens(
        dict(sanitize_public_value(dict(payload))),
        payload,
    )


def _presentation_error(
    status: str,
    *,
    host_id: str,
    name: str,
    plan_token: str | None = None,
    failed_plan_token: str | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "schema_version": _PRESENTATION_SCHEMA_VERSION,
        "ok": False,
        "status": str(status),
        "host_id": str(host_id),
        "name": str(name),
        "error": {
            "code": str(status),
            "message": "presentation plan request could not be applied",
        },
    }
    if plan_token is not None:
        payload["plan_token"] = str(plan_token)
    if failed_plan_token is not None:
        payload["failed_plan_token"] = str(failed_plan_token)
    return _presentation_response(payload)


def _current_presentation_revision_conn(
    conn: sqlite3.Connection,
    *,
    host_id: str,
    turn_id: str,
    content_revision_value: str,
) -> tuple[Any | None, str | None]:
    row = conn.execute(
        """
        SELECT
            user_state,
            final_state,
            user_char_length,
            final_char_length,
            is_current
        FROM turn_content_revisions
        WHERE host_id = ? AND turn_id = ? AND content_revision = ?
        """,
        (str(host_id), str(turn_id), str(content_revision_value)),
    ).fetchone()
    if row is None:
        return None, "content_revision_not_found"
    if int(row[4] or 0) != 1:
        return row, "revision_conflict"
    states = (str(row[0]), str(row[1]))
    if states[1] != "complete" or states[0] not in {"absent", "complete"}:
        return row, "content_known_incomplete"
    return row, None


def _presentation_plan_row_conn(
    conn: sqlite3.Connection,
    *,
    host_id: str,
    name: str,
    plan_token: str,
) -> Any | None:
    return conn.execute(
        """
        SELECT
            id,
            turn_id,
            content_revision,
            presentation_version,
            part_count,
            state,
            replaces_plan_token,
            generation,
            recovers_plan_token,
            source_outbox_id
        FROM turn_presentation_plans
        WHERE host_id = ? AND name = ? AND plan_token = ?
        """,
        (str(host_id), str(name), str(plan_token)),
    ).fetchone()


def _presentation_accepted_parts_conn(
    conn: sqlite3.Connection,
    plan_id: int,
) -> int:
    row = conn.execute(
        """
        SELECT COUNT(*)
        FROM turn_presentation_jobs
        WHERE plan_id = ? AND operation = 'upsert'
        """,
        (int(plan_id),),
    ).fetchone()
    return int(row[0] or 0)


def prepare_connector_plan_begin(
    db_path: Path,
    host_id: str,
    *,
    name: str,
    turn_id: str,
    content_revision: str,
    presentation_version: str,
    part_count: int,
    source_ref: str | None = None,
    now: str | None = None,
) -> dict[str, Any]:
    """Idempotently begin one bounded range-only presentation plan."""
    if not _sqlite_store_exists(db_path):
        return _presentation_error(
            "store_unavailable",
            host_id=host_id,
            name=name,
        )
    if (
        str(name) != _TURN_FINAL_NAME
        or isinstance(part_count, bool)
        or not isinstance(part_count, int)
        or part_count < 1
        or part_count > _PRESENTATION_MAX_PARTS
        or not _valid_presentation_label(turn_id, prefix="turn-")
        or not _valid_presentation_opaque(content_revision, "twrev1.")
        or not _valid_presentation_label(presentation_version)
        or (
            source_ref is not None
            and not _valid_presentation_opaque(source_ref, _CONNECTOR_REF_PREFIX)
        )
    ):
        return _presentation_error("invalid_params", host_id=host_id, name=name)
    count = part_count
    created_at = _connector_now(now)
    with _connect(db_path, isolation_level=None) as conn:
        _ensure_schema(conn)
        conn.execute("BEGIN IMMEDIATE")
        try:
            canonical_turn_id = _resolve_canonical_turn_id_conn(
                conn,
                str(host_id),
                turn_id,
            )
            if canonical_turn_id is None:
                conn.rollback()
                return _presentation_error(
                    "content_revision_not_found",
                    host_id=host_id,
                    name=name,
                )
            turn_id = canonical_turn_id
            _, revision_error = _current_presentation_revision_conn(
                conn,
                host_id=str(host_id),
                turn_id=str(turn_id),
                content_revision_value=str(content_revision),
            )
            if revision_error is not None:
                conn.rollback()
                return _presentation_error(
                    revision_error,
                    host_id=host_id,
                    name=name,
                )
            source_outbox_id: int | None = None
            generation = 1
            if source_ref is None:
                authoritative_route = _source_less_authoritative_route_conn(
                    conn,
                    host_id=str(host_id),
                    turn_id=str(turn_id),
                    content_revision_value=str(content_revision),
                )
                if authoritative_route is None:
                    conn.rollback()
                    return _presentation_error(
                        "invalid_ref",
                        host_id=host_id,
                        name=name,
                    )
            if source_ref is not None:
                source_row, source_error = _connector_validate_live_ref_conn(
                    conn,
                    host_id=str(host_id),
                    name=str(name),
                    ref=str(source_ref),
                    now=created_at,
                )
                if source_error is not None or source_row is None:
                    conn.rollback()
                    return _presentation_error(
                        source_error or "invalid_ref",
                        host_id=host_id,
                        name=name,
                    )
                if (
                    str(source_row[10]) != "final_ready"
                    or str(source_row[11]) != str(turn_id)
                    or str(source_row[12]) != str(content_revision)
                ):
                    conn.rollback()
                    return _presentation_error(
                        "stale_ref",
                        host_id=host_id,
                        name=name,
                    )
                source_outbox_id = int(source_row[1])
                source_state = _json_object(source_row[9])
                generation = max(
                    1,
                    int(source_state.get("presentation_generation") or 1),
                )
            token = _presentation_plan_token(
                host_id=str(host_id),
                name=str(name),
                turn_id=str(turn_id),
                content_revision_value=str(content_revision),
                presentation_version=str(presentation_version),
                part_count=count,
                generation=generation,
            )
            existing = conn.execute(
                """
                SELECT
                    id,
                    plan_token,
                    part_count,
                    state,
                    turn_id,
                    content_revision,
                    presentation_version,
                    source_outbox_id
                FROM turn_presentation_plans
                WHERE host_id = ?
                  AND name = ?
                  AND turn_id = ?
                  AND content_revision = ?
                  AND presentation_version = ?
                  AND generation = ?
                """,
                (
                    str(host_id),
                    str(name),
                    str(turn_id),
                    str(content_revision),
                    str(presentation_version),
                    generation,
                ),
            ).fetchone()
            if existing is not None:
                if (
                    int(existing[2]) != count
                    or str(existing[1]) != token
                    or str(existing[4]) != str(turn_id)
                    or str(existing[5]) != str(content_revision)
                    or str(existing[6]) != str(presentation_version)
                ):
                    conn.rollback()
                    return _presentation_error(
                        "plan_conflict",
                        host_id=host_id,
                        name=name,
                    )
                existing_source_outbox_id = (
                    int(existing[7]) if existing[7] is not None else None
                )
                if (
                    source_outbox_id is not None
                    and existing_source_outbox_id not in {
                        None,
                        source_outbox_id,
                    }
                ):
                    conn.rollback()
                    return _presentation_error(
                        "stale_ref",
                        host_id=host_id,
                        name=name,
                    )
                if (
                    source_outbox_id is not None
                    and existing_source_outbox_id is None
                    and str(existing[3]) == "preparing"
                ):
                    conn.execute(
                        """
                        UPDATE turn_presentation_plans
                        SET source_outbox_id = ?
                        WHERE id = ? AND source_outbox_id IS NULL
                        """,
                        (source_outbox_id, int(existing[0])),
                    )
                accepted = _presentation_accepted_parts_conn(conn, int(existing[0]))
                conn.commit()
                return _presentation_response(
                    {
                        "schema_version": _PRESENTATION_SCHEMA_VERSION,
                        "ok": True,
                        "status": "ok",
                        "host_id": str(host_id),
                        "name": str(name),
                        "plan_token": token,
                        "state": str(existing[3]),
                        "part_count": count,
                        "accepted_parts": accepted,
                        "generation": generation,
                    }
                )
            cursor = conn.execute(
                """
                INSERT INTO turn_presentation_plans (
                    host_id,
                    name,
                    plan_token,
                    turn_id,
                    content_revision,
                    source_outbox_id,
                    presentation_version,
                    generation,
                    part_count,
                    state,
                    created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'preparing', ?)
                """,
                (
                    str(host_id),
                    str(name),
                    token,
                    str(turn_id),
                    str(content_revision),
                    source_outbox_id,
                    str(presentation_version),
                    generation,
                    count,
                    created_at,
                ),
            )
            plan_id = int(cursor.lastrowid)
            conn.commit()
            return _presentation_response(
                {
                    "schema_version": _PRESENTATION_SCHEMA_VERSION,
                    "ok": True,
                    "status": "ok",
                    "host_id": str(host_id),
                    "name": str(name),
                    "plan_token": token,
                    "state": "preparing",
                    "part_count": count,
                    "accepted_parts": 0,
                    "generation": generation,
                }
            )
        except Exception:
            conn.rollback()
            raise


def _validate_presentation_spans(
    spans: Iterable[Mapping[str, Any]],
    *,
    revision_row: Any,
) -> list[dict[str, Any]] | None:
    normalized: list[dict[str, Any]] = []
    prior_rank = -1
    prior_end_by_field: dict[str, int] = {}
    states = {
        "user_text": str(revision_row[0]),
        "assistant_final_text": str(revision_row[1]),
    }
    lengths = {
        "user_text": int(revision_row[2] or 0),
        "assistant_final_text": int(revision_row[3] or 0),
    }
    for raw in spans:
        if not isinstance(raw, Mapping):
            return None
        field = str(raw.get("field") or "")
        start = raw.get("start_char")
        end = raw.get("end_char")
        if (
            field not in _PRESENTATION_FIELD_RANK
            or states[field] != "complete"
            or isinstance(start, bool)
            or isinstance(end, bool)
            or not isinstance(start, int)
            or not isinstance(end, int)
            or start < 0
            or end <= start
            or end > lengths[field]
        ):
            return None
        rank = _PRESENTATION_FIELD_RANK[field]
        if rank < prior_rank or start < prior_end_by_field.get(field, 0):
            return None
        prior_rank = rank
        prior_end_by_field[field] = end
        normalized.append(
            {
                "field": field,
                "start_char": int(start),
                "end_char": int(end),
            }
        )
    if not normalized or len(normalized) > _PRESENTATION_MAX_SPANS_PER_PART:
        return None
    return normalized


def prepare_connector_plan_part(
    db_path: Path,
    host_id: str,
    *,
    name: str,
    plan_token: str,
    ordinal: int,
    spans: Iterable[Mapping[str, Any]],
    now: str | None = None,
) -> dict[str, Any]:
    """Idempotently stage one ordinal's bounded canonical coordinate ranges."""
    if not _sqlite_store_exists(db_path):
        return _presentation_error(
            "store_unavailable",
            host_id=host_id,
            name=name,
            plan_token=plan_token,
        )
    if (
        str(name) != _TURN_FINAL_NAME
        or not _valid_presentation_opaque(plan_token, "twplan1.")
        or isinstance(ordinal, bool)
        or not isinstance(ordinal, int)
        or not isinstance(spans, list | tuple)
        or not spans
        or len(spans) > _PRESENTATION_MAX_SPANS_PER_PART
    ):
        return _presentation_error(
            "invalid_params",
            host_id=host_id,
            name=name,
        )
    created_at = _connector_now(now)
    with _connect(db_path, isolation_level=None) as conn:
        _ensure_schema(conn)
        conn.execute("BEGIN IMMEDIATE")
        try:
            plan = _presentation_plan_row_conn(
                conn,
                host_id=str(host_id),
                name=str(name),
                plan_token=str(plan_token),
            )
            if plan is None:
                conn.rollback()
                return _presentation_error(
                    "plan_not_found",
                    host_id=host_id,
                    name=name,
                    plan_token=plan_token,
                )
            plan_id = int(plan[0])
            part_count = int(plan[4])
            if ordinal < 0 or ordinal >= part_count:
                conn.rollback()
                return _presentation_error(
                    "invalid_params",
                    host_id=host_id,
                    name=name,
                    plan_token=plan_token,
                )
            revision_row, revision_error = _current_presentation_revision_conn(
                conn,
                host_id=str(host_id),
                turn_id=str(plan[1]),
                content_revision_value=str(plan[2]),
            )
            if revision_error is not None or revision_row is None:
                conn.rollback()
                return _presentation_error(
                    revision_error or "content_revision_not_found",
                    host_id=host_id,
                    name=name,
                    plan_token=plan_token,
                )
            normalized = _validate_presentation_spans(spans, revision_row=revision_row)
            if normalized is None:
                conn.rollback()
                return _presentation_error(
                    "invalid_params",
                    host_id=host_id,
                    name=name,
                    plan_token=plan_token,
                )
            encoded = _canonical_json(normalized)
            existing = conn.execute(
                """
                SELECT spans_json
                FROM turn_presentation_jobs
                WHERE plan_id = ? AND operation = 'upsert' AND part_ordinal = ?
                """,
                (plan_id, int(ordinal)),
            ).fetchone()
            if existing is not None:
                if str(existing[0]) != encoded:
                    conn.rollback()
                    return _presentation_error(
                        "plan_conflict",
                        host_id=host_id,
                        name=name,
                        plan_token=plan_token,
                    )
            elif str(plan[5]) != "preparing":
                conn.rollback()
                return _presentation_error(
                    "plan_conflict",
                    host_id=host_id,
                    name=name,
                    plan_token=plan_token,
                )
            else:
                conn.execute(
                    """
                    INSERT INTO turn_presentation_jobs (
                        plan_id,
                        sequence_index,
                        operation,
                        part_ordinal,
                        spans_json,
                        created_at
                    ) VALUES (?, ?, 'upsert', ?, ?, ?)
                    """,
                    (
                        plan_id,
                        int(ordinal),
                        int(ordinal),
                        encoded,
                        created_at,
                    ),
                )
            accepted = _presentation_accepted_parts_conn(conn, plan_id)
            conn.commit()
            return _presentation_response(
                {
                    "schema_version": _PRESENTATION_SCHEMA_VERSION,
                    "ok": True,
                    "status": "ok",
                    "host_id": str(host_id),
                    "name": str(name),
                    "plan_token": str(plan_token),
                    "ordinal": int(ordinal),
                    "accepted_parts": accepted,
                }
            )
        except Exception:
            conn.rollback()
            raise


def _presentation_exact_coverage(
    staged_rows: Iterable[Any],
    *,
    revision_row: Any,
) -> bool:
    states = {
        "user_text": str(revision_row[0]),
        "assistant_final_text": str(revision_row[1]),
    }
    lengths = {
        "user_text": int(revision_row[2] or 0),
        "assistant_final_text": int(revision_row[3] or 0),
    }
    cursors: dict[str, int] = {}
    for row in staged_rows:
        spans = json.loads(str(row[2]))
        for span in spans:
            field = str(span["field"])
            start = int(span["start_char"])
            end = int(span["end_char"])
            if states[field] != "complete" or start != cursors.get(field, 0):
                return False
            cursors[field] = end
    required = {
        field: lengths[field]
        for field in _PRESENTATION_FIELDS
        if states[field] == "complete" and lengths[field] > 0
    }
    return cursors == required


def _materialize_connector_plan_job_conn(
    conn: sqlite3.Connection,
    *,
    plan_id: int,
    job_id: int,
    host_id: str,
    name: str,
    delivery_key: str,
    payload: Mapping[str, Any],
    created_at: str,
) -> int:
    plan_identity = conn.execute(
        """
        SELECT plans.turn_id, plans.content_revision, source.ordering_key
        FROM turn_presentation_plans AS plans
        LEFT JOIN connector_outbox AS source
          ON source.id = plans.source_outbox_id
        WHERE plans.id = ?
        """,
        (int(plan_id),),
    ).fetchone()
    if plan_identity is None:
        raise StoreSchemaError("presentation_plan_not_found")
    ordering_key = (
        str(plan_identity[2])
        if plan_identity[2] is not None
        else _turn_ordering_key_conn(
            conn,
            host_id=str(host_id),
            turn_id=str(plan_identity[0]),
        )
    )
    cursor = conn.execute(
        """
        INSERT INTO connector_outbox (
            host_id,
            connector,
            delivery_key,
            delivery_kind,
            turn_id,
            content_revision,
            ordering_key,
            status,
            payload_json,
            private_state_json,
            created_at,
            updated_at,
            next_attempt_at
        ) VALUES (?, ?, ?, 'final_part', ?, ?, ?, 'queued', ?, '{}', ?, ?, NULL)
        """,
        (
            str(host_id),
            str(name),
            str(delivery_key),
            str(plan_identity[0]),
            str(plan_identity[1]),
            ordering_key,
            _canonical_json(dict(payload)),
            str(created_at),
            str(created_at),
        ),
    )
    outbox_id = int(cursor.lastrowid)
    conn.execute(
        """
        UPDATE turn_presentation_jobs
        SET outbox_id = ?
        WHERE id = ? AND plan_id = ? AND outbox_id IS NULL
        """,
        (outbox_id, int(job_id), int(plan_id)),
    )
    return outbox_id


def _finalize_recovered_plan_materialization_conn(
    conn: sqlite3.Connection,
    *,
    failed_plan_id: int,
    recovered_plan_id: int,
    now: str,
) -> None:
    recovered = conn.execute(
        """
        SELECT host_id, name, turn_id, content_revision, presentation_version
        FROM turn_presentation_plans
        WHERE id = ?
        """,
        (int(recovered_plan_id),),
    ).fetchone()
    if recovered is None:
        return
    lineage_ids = [
        int(row[0])
        for row in conn.execute(
            """
            WITH RECURSIVE lineage (
                id,
                host_id,
                name,
                recovers_plan_token
            ) AS (
                SELECT id, host_id, name, recovers_plan_token
                FROM turn_presentation_plans
                WHERE id = ?
                UNION
                SELECT
                    predecessor.id,
                    predecessor.host_id,
                    predecessor.name,
                    predecessor.recovers_plan_token
                FROM turn_presentation_plans AS predecessor
                JOIN lineage AS successor
                  ON predecessor.host_id = successor.host_id
                 AND predecessor.name = successor.name
                 AND predecessor.plan_token = successor.recovers_plan_token
            )
            SELECT id
            FROM lineage
            """,
            (int(failed_plan_id),),
        ).fetchall()
    ]
    if not lineage_ids:
        raise StoreSchemaError("presentation_recovery_lineage_missing")
    placeholders = ",".join("?" for _ in lineage_ids)
    conn.execute(
        f"""
        UPDATE turn_presentation_plans
        SET state = 'superseded'
        WHERE id IN ({placeholders}) AND state = 'failed'
        """,
        lineage_ids,
    )
    obsolete_outbox_ids = [
        int(row[0])
        for row in conn.execute(
            f"""
            SELECT DISTINCT outbox.id
            FROM turn_presentation_jobs AS jobs
            JOIN connector_outbox AS outbox ON outbox.id = jobs.outbox_id
            WHERE jobs.plan_id IN ({placeholders})
              AND outbox.status != 'delivered'
            """,
            lineage_ids,
        ).fetchall()
    ]
    if obsolete_outbox_ids:
        outbox_placeholders = ",".join("?" for _ in obsolete_outbox_ids)
        conn.execute(
            f"""
            UPDATE connector_deliveries
            SET status = 'superseded',
                delivered_at = COALESCE(delivered_at, ?)
            WHERE outbox_id IN ({outbox_placeholders})
              AND status != 'delivered'
            """,
            (str(now), *obsolete_outbox_ids),
        )
        for outbox_id, private_state_json in conn.execute(
            f"""
            SELECT id, private_state_json
            FROM connector_outbox
            WHERE id IN ({outbox_placeholders})
              AND status NOT IN ('delivered', 'superseded')
            """,
            obsolete_outbox_ids,
        ).fetchall():
            conn.execute(
                """
                UPDATE connector_outbox
                SET status = 'superseded',
                    next_attempt_at = NULL,
                    updated_at = ?,
                    private_state_json = ?
                WHERE id = ?
                """,
                (
                    str(now),
                    _connector_private_clear_current(private_state_json),
                    int(outbox_id),
                ),
            )
        conn.execute(
            f"""
            DELETE FROM connector_deliveries
            WHERE outbox_id IN ({outbox_placeholders})
            """,
            obsolete_outbox_ids,
        )
        conn.execute(
            f"""
            DELETE FROM turn_presentation_jobs
            WHERE outbox_id IN ({outbox_placeholders})
            """,
            obsolete_outbox_ids,
        )
        conn.execute(
            f"""
            DELETE FROM connector_outbox
            WHERE id IN ({outbox_placeholders})
            """,
            obsolete_outbox_ids,
        )

    audits = conn.execute(
        """
        SELECT
            audit.id,
            audit.failed_plan_id,
            audit.recovered_plan_id
        FROM turn_presentation_recoveries AS audit
        JOIN turn_presentation_plans AS plan
          ON plan.id = audit.recovered_plan_id
        WHERE audit.host_id = ?
          AND audit.name = ?
          AND plan.turn_id = ?
          AND plan.content_revision = ?
          AND plan.presentation_version = ?
        ORDER BY audit.generation DESC, audit.id DESC
        """,
        (
            str(recovered[0]),
            str(recovered[1]),
            str(recovered[2]),
            str(recovered[3]),
            str(recovered[4]),
        ),
    ).fetchall()
    retained_audits = audits[:_PRESENTATION_RECOVERY_HISTORY_LIMIT]
    obsolete_audit_ids = [
        int(row[0])
        for row in audits[_PRESENTATION_RECOVERY_HISTORY_LIMIT:]
    ]
    if obsolete_audit_ids:
        audit_placeholders = ",".join("?" for _ in obsolete_audit_ids)
        conn.execute(
            f"""
            DELETE FROM turn_presentation_recoveries
            WHERE id IN ({audit_placeholders})
            """,
            obsolete_audit_ids,
        )
    retained_plan_ids = sorted(
        {
            int(plan_id)
            for row in retained_audits
            for plan_id in (row[1], row[2])
        }
    )
    if retained_plan_ids:
        plan_placeholders = ",".join("?" for _ in retained_plan_ids)
        conn.execute(
            f"""
            DELETE FROM turn_presentation_plans
            WHERE host_id = ?
              AND name = ?
              AND turn_id = ?
              AND content_revision = ?
              AND presentation_version = ?
              AND state = 'superseded'
              AND id NOT IN ({plan_placeholders})
            """,
            (
                str(recovered[0]),
                str(recovered[1]),
                str(recovered[2]),
                str(recovered[3]),
                str(recovered[4]),
                *retained_plan_ids,
            ),
        )


def _mark_obsolete_presentation_plans_conn(
    conn: sqlite3.Connection,
    *,
    host_id: str,
    name: str,
    turn_id: str,
    keep_plan_id: int,
    now: str,
) -> bool:
    obsolete = conn.execute(
        """
        SELECT id
        FROM turn_presentation_plans
        WHERE host_id = ?
          AND name = ?
          AND turn_id = ?
          AND id != ?
          AND state IN ('preparing', 'waiting_predecessor', 'active', 'completed')
        """,
        (str(host_id), str(name), str(turn_id), int(keep_plan_id)),
    ).fetchall()
    obsolete_ids = [int(row[0]) for row in obsolete]
    if not obsolete_ids:
        return False
    placeholders = ",".join("?" for _ in obsolete_ids)
    conn.execute(
        f"""
        UPDATE turn_presentation_plans
        SET state = 'superseded'
        WHERE id IN ({placeholders})
        """,
        obsolete_ids,
    )
    conn.execute(
        f"""
        UPDATE connector_outbox
        SET status = 'superseded',
            next_attempt_at = NULL,
            updated_at = ?
        WHERE id IN (
            SELECT outbox_id
            FROM turn_presentation_jobs
            WHERE plan_id IN ({placeholders}) AND outbox_id IS NOT NULL
        )
          AND status IN ('queued', 'retry', 'deferred')
        """,
        (str(now), *obsolete_ids),
    )
    leased = conn.execute(
        f"""
        SELECT outbox.id, outbox.private_state_json
        FROM connector_outbox AS outbox
        JOIN turn_presentation_jobs AS jobs ON jobs.outbox_id = outbox.id
        WHERE jobs.plan_id IN ({placeholders}) AND outbox.status = 'leased'
        """,
        obsolete_ids,
    ).fetchall()
    for outbox_id, private_state_json in leased:
        state = _json_object(private_state_json)
        state["terminal_after_lease"] = True
        conn.execute(
            "UPDATE connector_outbox SET private_state_json = ? WHERE id = ?",
            (_canonical_json(state), int(outbox_id)),
        )
    return bool(leased)


def _activate_waiting_presentation_plans_conn(
    conn: sqlite3.Connection,
    *,
    host_id: str,
    name: str,
    now: str,
) -> int:
    cursor = conn.execute(
        """
        UPDATE turn_presentation_plans AS waiting
        SET state = 'active', activated_at = COALESCE(activated_at, ?)
        WHERE waiting.host_id = ?
          AND waiting.name = ?
          AND waiting.state = 'waiting_predecessor'
          AND NOT EXISTS (
              SELECT 1
              FROM turn_presentation_plans AS older
              JOIN turn_presentation_jobs AS jobs ON jobs.plan_id = older.id
              JOIN connector_outbox AS outbox ON outbox.id = jobs.outbox_id
              WHERE older.host_id = waiting.host_id
                AND older.name = waiting.name
                AND older.turn_id = waiting.turn_id
                AND older.id != waiting.id
                AND outbox.status = 'leased'
          )
        """,
        (str(now), str(host_id), str(name)),
    )
    return int(cursor.rowcount or 0)


def _update_presentation_plan_after_outbox_conn(
    conn: sqlite3.Connection,
    *,
    outbox_id: int,
    outbox_status: str,
    now: str,
    ack_ttl_seconds: int | None = None,
) -> None:
    plan = conn.execute(
        """
        SELECT
            plans.id,
            plans.host_id,
            plans.name,
            plans.state,
            plans.source_outbox_id,
            plans.turn_id,
            plans.content_revision
        FROM turn_presentation_jobs AS jobs
        JOIN turn_presentation_plans AS plans ON plans.id = jobs.plan_id
        WHERE jobs.outbox_id = ?
        """,
        (int(outbox_id),),
    ).fetchone()
    if plan is not None:
        plan_id = int(plan[0])
        if outbox_status == _CONNECTOR_EXHAUSTED_OUTBOX_STATUS and str(plan[3]) in {
            "active",
            "waiting_predecessor",
        }:
            conn.execute(
                """
                UPDATE turn_presentation_plans
                SET state = 'failed'
                WHERE id = ?
                """,
                (plan_id,),
            )
        elif outbox_status == _CONNECTOR_TERMINAL_OUTBOX_STATUS and str(plan[3]) == "active":
            remaining = conn.execute(
                """
                SELECT COUNT(*)
                FROM turn_presentation_jobs AS jobs
                JOIN connector_outbox AS outbox ON outbox.id = jobs.outbox_id
                WHERE jobs.plan_id = ? AND outbox.status != 'delivered'
                """,
                (plan_id,),
            ).fetchone()
            remaining_count = int(remaining[0] or 0)
            source_outbox_id = (
                int(plan[4]) if plan[4] is not None else None
            )
            if (
                remaining_count > 0
                and source_outbox_id is not None
                and ack_ttl_seconds is not None
            ):
                deadline = _connector_add_seconds(
                    str(now),
                    max(1, int(ack_ttl_seconds)),
                )
                source_row = conn.execute(
                    """
                    SELECT private_state_json
                    FROM connector_outbox
                    WHERE id = ? AND status = 'awaiting_ack'
                    """,
                    (source_outbox_id,),
                ).fetchone()
                if source_row is not None:
                    source_state = _json_object(source_row[0])
                    source_state["ack_deadline_at"] = deadline
                    conn.execute(
                        """
                        UPDATE connector_outbox
                        SET private_state_json = ?, next_attempt_at = ?,
                            updated_at = ?
                        WHERE id = ? AND status = 'awaiting_ack'
                        """,
                        (
                            _canonical_json(source_state),
                            deadline,
                            str(now),
                            source_outbox_id,
                        ),
                    )
                    delivery_row = conn.execute(
                        """
                        SELECT id, private_state_json
                        FROM connector_deliveries
                        WHERE outbox_id = ? AND status = 'awaiting_ack'
                        ORDER BY id DESC
                        LIMIT 1
                        """,
                        (source_outbox_id,),
                    ).fetchone()
                    if delivery_row is not None:
                        delivery_state = _json_object(delivery_row[1])
                        delivery_state["ack_deadline_at"] = deadline
                        conn.execute(
                            """
                            UPDATE connector_deliveries
                            SET private_state_json = ?
                            WHERE id = ? AND status = 'awaiting_ack'
                            """,
                            (
                                _canonical_json(delivery_state),
                                int(delivery_row[0]),
                            ),
                        )
            if remaining_count == 0:
                conn.execute(
                    """
                    UPDATE turn_presentation_plans
                    SET state = 'completed', completed_at = COALESCE(completed_at, ?)
                    WHERE id = ? AND state = 'active'
                    """,
                    (str(now), plan_id),
                )
                if source_outbox_id is not None:
                    conn.execute(
                        """
                        UPDATE connector_deliveries
                        SET status = 'delivered', delivered_at = ?
                        WHERE outbox_id = ? AND status = 'awaiting_ack'
                        """,
                        (str(now), source_outbox_id),
                    )
                    source_row = conn.execute(
                        """
                        SELECT private_state_json
                        FROM connector_outbox
                        WHERE id = ?
                        """,
                        (source_outbox_id,),
                    ).fetchone()
                    if source_row is not None:
                        conn.execute(
                            """
                            UPDATE connector_outbox
                            SET status = 'delivered',
                                next_attempt_at = NULL,
                                updated_at = ?,
                                private_state_json = ?
                            WHERE id = ? AND status = 'awaiting_ack'
                            """,
                            (
                                str(now),
                                _connector_private_clear_current(source_row[0]),
                                source_outbox_id,
                            ),
                        )
                else:
                    final_identity = turn_final_delivery_identity(
                        str(plan[1]),
                        str(plan[5]),
                        str(plan[6]),
                    )
                    delivery_key = (
                        f"{_TURN_FINAL_NAME}:revision:{final_identity}"
                    )
                    conn.execute(
                        """
                        INSERT INTO connector_deliveries (
                            outbox_id, host_id, connector, delivery_key,
                            attempt, status, response_json, private_state_json,
                            created_at, delivered_at
                        )
                        SELECT NULL, ?, ?, ?, 0, 'delivered', '{}', '{}', ?, ?
                        WHERE NOT EXISTS (
                            SELECT 1
                            FROM connector_deliveries
                            WHERE outbox_id IS NULL
                              AND host_id = ?
                              AND connector = ?
                              AND delivery_key = ?
                              AND status = 'delivered'
                              AND delivered_at IS NOT NULL
                        )
                        """,
                        (
                            str(plan[1]),
                            str(plan[2]),
                            delivery_key,
                            str(now),
                            str(now),
                            str(plan[1]),
                            str(plan[2]),
                            delivery_key,
                        ),
                    )
        _activate_waiting_presentation_plans_conn(
            conn,
            host_id=str(plan[1]),
            name=str(plan[2]),
            now=str(now),
        )


def _mark_exhausted_presentation_plans_conn(
    conn: sqlite3.Connection,
    *,
    host_id: str,
    name: str | None,
    now: str,
) -> None:
    params: list[Any] = [str(host_id)]
    connector_clause = ""
    if name is not None:
        connector_clause = "AND plans.name = ?"
        params.append(str(name))
    conn.execute(
        f"""
        UPDATE turn_presentation_plans AS plans
        SET state = 'failed'
        WHERE plans.host_id = ?
          {connector_clause}
          AND (
              (
                  plans.state IN ('active', 'waiting_predecessor')
                  AND EXISTS (
                      SELECT 1
                      FROM turn_presentation_jobs AS jobs
                      JOIN connector_outbox AS outbox
                        ON outbox.id = jobs.outbox_id
                      WHERE jobs.plan_id = plans.id
                        AND outbox.status = 'dead_letter'
                  )
              )
              OR (
                  plans.state = 'preparing'
                  AND EXISTS (
                      SELECT 1
                      FROM connector_outbox AS source
                      WHERE source.id = plans.source_outbox_id
                        AND source.status = 'dead_letter'
                  )
              )
          )
        """,
        params,
    )


def _validate_plan_source_ref_conn(
    conn: sqlite3.Connection,
    *,
    host_id: str,
    name: str,
    source_outbox_id: int,
    turn_id: str,
    content_revision_value: str,
    source_ref: str,
    now: str,
    require_live: bool,
) -> tuple[Any | None, str | None]:
    if require_live:
        row, error = _connector_validate_live_ref_conn(
            conn,
            host_id=str(host_id),
            name=str(name),
            ref=str(source_ref),
            now=str(now),
        )
        if error is not None or row is None:
            return row, error or "invalid_ref"
        if (
            int(row[1] or 0) != int(source_outbox_id)
            or str(row[10]) != "final_ready"
            or str(row[11]) != str(turn_id)
            or str(row[12]) != str(content_revision_value)
        ):
            return row, "stale_ref"
        return row, None
    rows = conn.execute(
        """
        SELECT
            deliveries.id,
            deliveries.private_state_json,
            deliveries.status,
            outbox.status,
            outbox.delivery_kind,
            outbox.turn_id,
            outbox.content_revision
        FROM connector_deliveries AS deliveries
        JOIN connector_outbox AS outbox ON outbox.id = deliveries.outbox_id
        WHERE deliveries.outbox_id = ?
          AND deliveries.host_id = ?
          AND deliveries.connector = ?
        ORDER BY deliveries.id DESC
        """,
        (int(source_outbox_id), str(host_id), str(name)),
    ).fetchall()
    for row in rows:
        state = _json_object(row[1])
        if str(state.get("public_ref") or "") != str(source_ref):
            continue
        if (
            str(row[2]) not in {"awaiting_ack", "delivered"}
            or str(row[3]) not in {"awaiting_ack", "delivered"}
            or str(row[4]) != "final_ready"
            or str(row[5]) != str(turn_id)
            or str(row[6]) != str(content_revision_value)
        ):
            return row, "stale_ref"
        return row, None
    return None, "invalid_ref"


def prepare_connector_plan_commit(
    db_path: Path,
    host_id: str,
    *,
    name: str,
    plan_token: str,
    source_ref: str | None = None,
    ack_ttl_seconds: int = CONNECTOR_ACK_TTL_SECONDS,
    now: str | None = None,
) -> dict[str, Any]:
    """Atomically validate exact coverage and materialize one plan's ordered jobs."""
    if not _sqlite_store_exists(db_path):
        return _presentation_error(
            "store_unavailable",
            host_id=host_id,
            name=name,
            plan_token=plan_token,
        )
    if (
        str(name) != _TURN_FINAL_NAME
        or not _valid_presentation_opaque(plan_token, "twplan1.")
        or (
            source_ref is not None
            and not _valid_presentation_opaque(source_ref, _CONNECTOR_REF_PREFIX)
        )
    ):
        return _presentation_error(
            "invalid_params",
            host_id=host_id,
            name=name,
        )
    current_time = _connector_now(now)
    ack_deadline_at = _connector_add_seconds(
        current_time,
        max(1, int(ack_ttl_seconds)),
    )
    with _connect(db_path, isolation_level=None) as conn:
        _ensure_schema(conn)
        conn.execute("BEGIN IMMEDIATE")
        try:
            plan = _presentation_plan_row_conn(
                conn,
                host_id=str(host_id),
                name=str(name),
                plan_token=str(plan_token),
            )
            if plan is None:
                conn.rollback()
                return _presentation_error(
                    "plan_not_found",
                    host_id=host_id,
                    name=name,
                    plan_token=plan_token,
                )
            plan_id = int(plan[0])
            plan_state = str(plan[5])
            job_count_row = conn.execute(
                """
                SELECT COUNT(*)
                FROM turn_presentation_jobs
                WHERE plan_id = ? AND outbox_id IS NOT NULL
                """,
                (plan_id,),
            ).fetchone()
            materialized_job_count = int(job_count_row[0] or 0)
            if materialized_job_count > 0:
                conn.commit()
                return _presentation_response(
                    {
                        "schema_version": _PRESENTATION_SCHEMA_VERSION,
                        "ok": True,
                        "status": "ok",
                        "host_id": str(host_id),
                        "name": str(name),
                        "plan_token": str(plan_token),
                        "state": plan_state,
                        "job_count": materialized_job_count,
                        "generation": int(plan[7]),
                    }
                )
            if plan_state in {"failed", "superseded"}:
                # A source final can exhaust before commit materializes any
                # child jobs. Preserve terminal idempotency so a recovering
                # connector can abandon that zero-job plan instead of retrying
                # a now-stale source reference forever.
                conn.commit()
                return _presentation_response(
                    {
                        "schema_version": _PRESENTATION_SCHEMA_VERSION,
                        "ok": True,
                        "status": "ok",
                        "host_id": str(host_id),
                        "name": str(name),
                        "plan_token": str(plan_token),
                        "state": plan_state,
                        "job_count": 0,
                        "generation": int(plan[7]),
                    }
                )
            _, early_revision_error = _current_presentation_revision_conn(
                conn,
                host_id=str(host_id),
                turn_id=str(plan[1]),
                content_revision_value=str(plan[2]),
            )
            if early_revision_error is not None:
                conn.rollback()
                return _presentation_error(
                    early_revision_error,
                    host_id=host_id,
                    name=name,
                    plan_token=plan_token,
                )
            source_outbox_id = int(plan[9]) if plan[9] is not None else None
            source_delivery: Any | None = None
            if source_outbox_id is not None:
                if source_ref is None:
                    if materialized_job_count == 0:
                        conn.rollback()
                        return _presentation_error(
                            "invalid_ref",
                            host_id=host_id,
                            name=name,
                            plan_token=plan_token,
                        )
                else:
                    source_delivery, source_error = _validate_plan_source_ref_conn(
                        conn,
                        host_id=str(host_id),
                        name=str(name),
                        source_outbox_id=source_outbox_id,
                        turn_id=str(plan[1]),
                        content_revision_value=str(plan[2]),
                        source_ref=str(source_ref),
                        now=current_time,
                        require_live=materialized_job_count == 0,
                    )
                    if source_error is not None or source_delivery is None:
                        conn.rollback()
                        return _presentation_error(
                            source_error or "invalid_ref",
                            host_id=host_id,
                            name=name,
                            plan_token=plan_token,
                        )
            if source_outbox_id is None:
                authoritative_route = _source_less_authoritative_route_conn(
                    conn,
                    host_id=str(host_id),
                    turn_id=str(plan[1]),
                    content_revision_value=str(plan[2]),
                )
                if authoritative_route is None:
                    conn.rollback()
                    return _presentation_error(
                        "invalid_ref",
                        host_id=host_id,
                        name=name,
                        plan_token=plan_token,
                    )
            if source_outbox_id is None and source_ref is not None:
                conn.rollback()
                return _presentation_error(
                    "stale_ref",
                    host_id=host_id,
                    name=name,
                    plan_token=plan_token,
                )
            if plan_state != "preparing":
                conn.rollback()
                return _presentation_error(
                    "plan_conflict",
                    host_id=host_id,
                    name=name,
                    plan_token=plan_token,
                )
            revision_row, revision_error = _current_presentation_revision_conn(
                conn,
                host_id=str(host_id),
                turn_id=str(plan[1]),
                content_revision_value=str(plan[2]),
            )
            if revision_error is not None or revision_row is None:
                conn.rollback()
                return _presentation_error(
                    revision_error or "revision_conflict",
                    host_id=host_id,
                    name=name,
                    plan_token=plan_token,
                )
            staged = conn.execute(
                """
                SELECT id, part_ordinal, spans_json
                FROM turn_presentation_jobs
                WHERE plan_id = ? AND operation = 'upsert'
                ORDER BY part_ordinal
                """,
                (plan_id,),
            ).fetchall()
            expected_count = int(plan[4])
            if (
                len(staged) != expected_count
                or [int(row[1]) for row in staged] != list(range(expected_count))
                or not _presentation_exact_coverage(staged, revision_row=revision_row)
            ):
                conn.rollback()
                return _presentation_error(
                    "plan_incomplete",
                    host_id=host_id,
                    name=name,
                    plan_token=plan_token,
                )
            predecessor = conn.execute(
                """
                SELECT id, plan_token
                FROM turn_presentation_plans
                WHERE host_id = ?
                  AND name = ?
                  AND turn_id = ?
                  AND id != ?
                  AND activated_at IS NOT NULL
                ORDER BY id DESC
                LIMIT 1
                """,
                (str(host_id), str(name), str(plan[1]), plan_id),
            ).fetchone()
            replaces_token = str(predecessor[1]) if predecessor is not None else None
            completed_baseline = conn.execute(
                """
                SELECT COALESCE(MAX(id), 0)
                FROM turn_presentation_plans
                WHERE host_id = ?
                  AND name = ?
                  AND turn_id = ?
                  AND id != ?
                  AND completed_at IS NOT NULL
                """,
                (str(host_id), str(name), str(plan[1]), plan_id),
            ).fetchone()
            baseline_id = int(completed_baseline[0] or 0)
            footprint = conn.execute(
                """
                SELECT COALESCE(MAX(part_count), 0)
                FROM turn_presentation_plans
                WHERE host_id = ?
                  AND name = ?
                  AND turn_id = ?
                  AND id != ?
                  AND activated_at IS NOT NULL
                  AND id >= ?
                """,
                (
                    str(host_id),
                    str(name),
                    str(plan[1]),
                    plan_id,
                    baseline_id,
                ),
            ).fetchone()
            prior_part_count = int(footprint[0] or 0)
            common = {
                "schema_version": _PRESENTATION_SCHEMA_VERSION,
                "plan_token": str(plan_token),
                "content_revision": str(plan[2]),
                "presentation_version": str(plan[3]),
                "part_count": expected_count,
                "replaces_plan_token": replaces_token,
            }
            if source_outbox_id is None:
                if authoritative_route is None:
                    raise StoreSchemaError("presentation_route_unavailable")
                common["turn"] = authoritative_route
            if source_outbox_id is not None:
                source_payload_row = conn.execute(
                    """
                    SELECT payload_json, private_state_json
                    FROM connector_outbox
                    WHERE id = ?
                      AND delivery_kind = 'final_ready'
                      AND turn_id = ?
                      AND content_revision = ?
                    """,
                    (
                        source_outbox_id,
                        str(plan[1]),
                        str(plan[2]),
                    ),
                ).fetchone()
                if source_payload_row is None:
                    conn.rollback()
                    return _presentation_error(
                        "stale_ref",
                        host_id=host_id,
                        name=name,
                        plan_token=plan_token,
                    )
                source_plan_state = _json_object(source_payload_row[1])
                prior_part_count = max(
                    prior_part_count,
                    int(
                        source_plan_state.get(
                            "presentation_max_part_count"
                        )
                        or 0
                    ),
                )
                common["turn"] = _json_object(source_payload_row[0])
            for job_id, part_ordinal, spans_json in staged:
                sequence = int(part_ordinal)
                payload = {
                    **common,
                    "operation": "upsert",
                    "sequence_index": sequence,
                    "part_ordinal": int(part_ordinal),
                    "spans": json.loads(str(spans_json)),
                }
                _materialize_connector_plan_job_conn(
                    conn,
                    plan_id=plan_id,
                    job_id=int(job_id),
                    host_id=str(host_id),
                    name=str(name),
                    delivery_key=(
                        f"{name}:{plan_token}:"
                        f"{sequence:0{_PRESENTATION_SEQUENCE_WIDTH}d}"
                    ),
                    payload=payload,
                    created_at=current_time,
                )
            sequence = expected_count
            for old_ordinal in range(prior_part_count - 1, expected_count - 1, -1):
                cursor = conn.execute(
                    """
                    INSERT INTO turn_presentation_jobs (
                        plan_id,
                        sequence_index,
                        operation,
                        part_ordinal,
                        spans_json,
                        created_at
                    ) VALUES (?, ?, 'retire', ?, '[]', ?)
                    """,
                    (plan_id, sequence, int(old_ordinal), current_time),
                )
                payload = {
                    **common,
                    "operation": "retire",
                    "sequence_index": sequence,
                    "part_ordinal": int(old_ordinal),
                    "spans": [],
                }
                _materialize_connector_plan_job_conn(
                    conn,
                    plan_id=plan_id,
                    job_id=int(cursor.lastrowid),
                    host_id=str(host_id),
                    name=str(name),
                    delivery_key=(
                        f"{name}:{plan_token}:"
                        f"{sequence:0{_PRESENTATION_SEQUENCE_WIDTH}d}"
                    ),
                    payload=payload,
                    created_at=current_time,
                )
                sequence += 1
            leased_predecessor = _mark_obsolete_presentation_plans_conn(
                conn,
                host_id=str(host_id),
                name=str(name),
                turn_id=str(plan[1]),
                keep_plan_id=plan_id,
                now=current_time,
            )
            next_state = "waiting_predecessor" if leased_predecessor else "active"
            conn.execute(
                """
                UPDATE turn_presentation_plans
                SET state = ?,
                    replaces_plan_token = ?,
                    activated_at = CASE WHEN ? = 'active' THEN ? ELSE NULL END
                WHERE id = ? AND state = 'preparing'
                """,
                (
                    next_state,
                    replaces_token,
                    next_state,
                    current_time,
                    plan_id,
                ),
            )
            if source_outbox_id is not None:
                if source_delivery is None:
                    raise StoreSchemaError("presentation_source_not_live")
                source_private = conn.execute(
                    """
                    SELECT private_state_json
                    FROM connector_outbox
                    WHERE id = ? AND status = 'leased'
                    """,
                    (source_outbox_id,),
                ).fetchone()
                if source_private is None:
                    raise StoreSchemaError("presentation_source_not_live")
                source_delivery_private = conn.execute(
                    "SELECT private_state_json FROM connector_deliveries WHERE id = ?",
                    (int(source_delivery[0]),),
                ).fetchone()
                if source_delivery_private is None:
                    raise StoreSchemaError("presentation_source_not_live")
                delivery_cursor = conn.execute(
                    """
                    UPDATE connector_deliveries
                    SET status = 'awaiting_ack',
                        response_json = ?,
                        private_state_json = ?,
                        delivered_at = NULL
                    WHERE id = ? AND outbox_id = ? AND status = 'leased'
                    """,
                    (
                        _canonical_json(
                            {
                                "schema_version": 1,
                                "status": "prepared",
                            }
                        ),
                        _canonical_json(
                            {
                                **_json_object(source_delivery_private[0]),
                                "ack_deadline_at": ack_deadline_at,
                            }
                        ),
                        int(source_delivery[0]),
                        source_outbox_id,
                    ),
                )
                next_source_state = _json_object(
                    _connector_private_clear_current(source_private[0])
                )
                next_source_state["presentation_generation"] = int(plan[7])
                next_source_state["ack_deadline_at"] = ack_deadline_at
                next_source_state["presentation_max_part_count"] = max(
                    int(
                        next_source_state.get(
                            "presentation_max_part_count"
                        )
                        or 0
                    ),
                    prior_part_count,
                    expected_count,
                )
                source_cursor = conn.execute(
                    """
                    UPDATE connector_outbox
                    SET status = 'awaiting_ack',
                        next_attempt_at = ?,
                        updated_at = ?,
                        private_state_json = ?
                    WHERE id = ? AND status = 'leased'
                    """,
                    (
                        ack_deadline_at,
                        current_time,
                        _canonical_json(next_source_state),
                        source_outbox_id,
                    ),
                )
                if not delivery_cursor.rowcount or not source_cursor.rowcount:
                    raise StoreSchemaError("presentation_source_not_live")
            conn.commit()
            return _presentation_response(
                {
                    "schema_version": _PRESENTATION_SCHEMA_VERSION,
                    "ok": True,
                    "status": "ok",
                    "host_id": str(host_id),
                    "name": str(name),
                    "plan_token": str(plan_token),
                    "state": next_state,
                    "job_count": sequence,
                    "generation": int(plan[7]),
                }
            )
        except Exception:
            conn.rollback()
            raise



def _presentation_recovery_result(
    *,
    failed_plan_token: str,
    plan_token: str,
    generation: int,
    content_revision: str,
    acknowledged_prefix_count: int,
    executable_job_count: int,
    retained_failed_job_count: int,
    prior_attempt_count: int,
    idempotent_replay: bool,
) -> dict[str, Any]:
    return _presentation_response(
        {
            "schema_version": _PRESENTATION_SCHEMA_VERSION,
            "ok": True,
            "status": "recovered",
            "failed_plan_token": str(failed_plan_token),
            "plan_token": str(plan_token),
            "generation": int(generation),
            "content_revision": str(content_revision),
            "state": "active",
            "acknowledged_prefix_count": int(acknowledged_prefix_count),
            "executable_job_count": int(executable_job_count),
            "retained_failed_job_count": int(retained_failed_job_count),
            "prior_attempt_count": int(prior_attempt_count),
            "idempotent_replay": bool(idempotent_replay),
        }
    )


def prepare_connector_plan_recover(
    db_path: Path,
    host_id: str,
    *,
    name: str,
    failed_plan_token: str,
    request_id: str,
    now: str | None = None,
) -> dict[str, Any]:
    """Explicitly replace one failed immutable plan with its unfinished suffix."""
    if not _sqlite_store_exists(db_path):
        return _presentation_error(
            "store_unavailable",
            host_id=host_id,
            name=name,
            failed_plan_token=failed_plan_token,
        )
    if (
        str(name) != _TURN_FINAL_NAME
        or not _valid_presentation_opaque(failed_plan_token, "twplan1.")
        or not _valid_presentation_label(request_id)
    ):
        return _presentation_error(
            "invalid_params",
            host_id=host_id,
            name=name,
            failed_plan_token=failed_plan_token,
        )
    current_time = _connector_now(now)
    with _connect(db_path, isolation_level=None) as conn:
        _ensure_schema(conn)
        conn.execute("BEGIN IMMEDIATE")
        try:
            prior_request = conn.execute(
                """
                SELECT
                    audit.failed_plan_token,
                    audit.recovered_plan_token,
                    audit.generation,
                    plans.content_revision,
                    audit.delivered_prefix_count,
                    audit.fresh_job_count,
                    audit.retained_failed_job_count,
                    audit.prior_attempt_count
                FROM turn_presentation_recoveries AS audit
                JOIN turn_presentation_plans AS plans
                  ON plans.id = audit.recovered_plan_id
                WHERE audit.host_id = ? AND audit.name = ? AND audit.request_id = ?
                """,
                (str(host_id), str(name), str(request_id)),
            ).fetchone()
            if prior_request is not None:
                if str(prior_request[0]) != str(failed_plan_token):
                    conn.rollback()
                    return _presentation_error(
                        "request_conflict",
                        host_id=host_id,
                        name=name,
                        failed_plan_token=failed_plan_token,
                    )
                conn.commit()
                return _presentation_recovery_result(
                    failed_plan_token=str(prior_request[0]),
                    plan_token=str(prior_request[1]),
                    generation=int(prior_request[2]),
                    content_revision=str(prior_request[3]),
                    acknowledged_prefix_count=int(prior_request[4]),
                    executable_job_count=int(prior_request[5]),
                    retained_failed_job_count=int(prior_request[6]),
                    prior_attempt_count=int(prior_request[7]),
                    idempotent_replay=True,
                )
            failed = conn.execute(
                """
                SELECT
                    id,
                    turn_id,
                    content_revision,
                    presentation_version,
                    generation,
                    part_count,
                    state,
                    source_outbox_id
                FROM turn_presentation_plans
                WHERE host_id = ? AND name = ? AND plan_token = ?
                """,
                (str(host_id), str(name), str(failed_plan_token)),
            ).fetchone()
            if failed is None:
                conn.rollback()
                return _presentation_error(
                    "plan_not_found",
                    host_id=host_id,
                    name=name,
                    failed_plan_token=failed_plan_token,
                )
            prior_recovery = conn.execute(
                """
                SELECT 1
                FROM turn_presentation_recoveries
                WHERE failed_plan_id = ?
                """,
                (int(failed[0]),),
            ).fetchone()
            if prior_recovery is not None:
                conn.rollback()
                return _presentation_error(
                    "plan_conflict",
                    host_id=host_id,
                    name=name,
                    failed_plan_token=failed_plan_token,
                )
            if str(failed[6]) != "failed":
                conn.rollback()
                return _presentation_error(
                    "plan_not_failed",
                    host_id=host_id,
                    name=name,
                    failed_plan_token=failed_plan_token,
                )
            _, revision_error = _current_presentation_revision_conn(
                conn,
                host_id=str(host_id),
                turn_id=str(failed[1]),
                content_revision_value=str(failed[2]),
            )
            if revision_error is not None:
                conn.rollback()
                return _presentation_error(
                    revision_error,
                    host_id=host_id,
                    name=name,
                    failed_plan_token=failed_plan_token,
                )
            latest_generation = conn.execute(
                """
                SELECT MAX(generation)
                FROM turn_presentation_plans
                WHERE host_id = ?
                  AND name = ?
                  AND turn_id = ?
                  AND content_revision = ?
                  AND presentation_version = ?
                """,
                (
                    str(host_id),
                    str(name),
                    str(failed[1]),
                    str(failed[2]),
                    str(failed[3]),
                ),
            ).fetchone()
            if int(latest_generation[0] or 0) != int(failed[4]):
                conn.rollback()
                return _presentation_error(
                    "plan_conflict",
                    host_id=host_id,
                    name=name,
                    failed_plan_token=failed_plan_token,
                )
            inherited_audit = conn.execute(
                """
                SELECT
                    delivered_prefix_count,
                    retained_failed_job_count,
                    prior_attempt_count
                FROM turn_presentation_recoveries
                WHERE recovered_plan_id = ?
                """,
                (int(failed[0]),),
            ).fetchone()
            inherited_prefix_count = (
                int(inherited_audit[0]) if inherited_audit is not None else 0
            )
            inherited_failed_count = (
                int(inherited_audit[1]) if inherited_audit is not None else 0
            )
            inherited_attempt_count = (
                int(inherited_audit[2]) if inherited_audit is not None else 0
            )
            source_jobs = conn.execute(
                """
                SELECT
                    jobs.sequence_index,
                    jobs.operation,
                    jobs.part_ordinal,
                    jobs.spans_json,
                    outbox.delivery_key,
                    outbox.status,
                    outbox.payload_json
                FROM turn_presentation_jobs AS jobs
                JOIN connector_outbox AS outbox ON outbox.id = jobs.outbox_id
                WHERE jobs.plan_id = ?
                ORDER BY jobs.sequence_index
                """,
                (int(failed[0]),),
            ).fetchall()
            if (
                not source_jobs
                or [int(row[0]) for row in source_jobs]
                != list(
                    range(
                        inherited_prefix_count,
                        inherited_prefix_count + len(source_jobs),
                    )
                )
            ):
                conn.rollback()
                return _presentation_error(
                    "plan_conflict",
                    host_id=host_id,
                    name=name,
                    failed_plan_token=failed_plan_token,
                )
            local_acknowledged_count = 0
            for source_job in source_jobs:
                if str(source_job[5]) != "delivered":
                    break
                local_acknowledged_count += 1
            acknowledged_prefix_count = (
                inherited_prefix_count + local_acknowledged_count
            )
            suffix = source_jobs[local_acknowledged_count:]
            retained_failed_job_count = inherited_failed_count + sum(
                str(row[5]) == "dead_letter" for row in suffix
            )
            if (
                not suffix
                or retained_failed_job_count <= inherited_failed_count
                or any(str(row[5]) in {"delivered", "leased"} for row in suffix)
            ):
                conn.rollback()
                return _presentation_error(
                    "plan_conflict",
                    host_id=host_id,
                    name=name,
                    failed_plan_token=failed_plan_token,
                )
            current_attempt_count = int(
                conn.execute(
                    """
                    SELECT COUNT(*)
                    FROM connector_deliveries AS deliveries
                    JOIN turn_presentation_jobs AS jobs
                      ON jobs.outbox_id = deliveries.outbox_id
                    WHERE jobs.plan_id = ?
                    """,
                    (int(failed[0]),),
                ).fetchone()[0]
                or 0
            )
            prior_attempt_count = inherited_attempt_count + current_attempt_count
            if current_attempt_count < 1:
                conn.rollback()
                return _presentation_error(
                    "plan_conflict",
                    host_id=host_id,
                    name=name,
                    failed_plan_token=failed_plan_token,
                )
            generation = int(failed[4]) + 1
            recovered_token = _presentation_recovery_token(
                host_id=str(host_id),
                name=str(name),
                failed_plan_token=str(failed_plan_token),
                request_id=str(request_id),
                generation=generation,
            )
            plan_cursor = conn.execute(
                """
                INSERT INTO turn_presentation_plans (
                    host_id,
                    name,
                    plan_token,
                    turn_id,
                    content_revision,
                    source_outbox_id,
                    presentation_version,
                    generation,
                    part_count,
                    state,
                    replaces_plan_token,
                    recovers_plan_token,
                    created_at,
                    activated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'active', ?, ?, ?, ?)
                """,
                (
                    str(host_id),
                    str(name),
                    recovered_token,
                    str(failed[1]),
                    str(failed[2]),
                    failed[7],
                    str(failed[3]),
                    generation,
                    int(failed[5]),
                    str(failed_plan_token),
                    str(failed_plan_token),
                    current_time,
                    current_time,
                ),
            )
            recovered_plan_id = int(plan_cursor.lastrowid)
            common = {
                "schema_version": _PRESENTATION_SCHEMA_VERSION,
                "plan_token": recovered_token,
                "content_revision": str(failed[2]),
                "presentation_version": str(failed[3]),
                "part_count": int(failed[5]),
                "replaces_plan_token": str(failed_plan_token),
            }
            if failed[7] is not None:
                source_payload = conn.execute(
                    """
                    SELECT payload_json
                    FROM connector_outbox
                    WHERE id = ? AND delivery_kind = 'final_ready'
                    """,
                    (int(failed[7]),),
                ).fetchone()
                if source_payload is None:
                    conn.rollback()
                    return _presentation_error(
                        "plan_conflict",
                        host_id=host_id,
                        name=name,
                        failed_plan_token=failed_plan_token,
                    )
                common["turn"] = _json_object(source_payload[0])
            else:
                route_payloads: dict[str, dict[str, Any]] = {}
                for source_job in source_jobs:
                    job_payload = _json_object(source_job[6])
                    route = job_payload.get("turn")
                    if not isinstance(route, Mapping):
                        continue
                    route_data = dict(route)
                    if (
                        route_data.get("schema_version") != 2
                        or str(route_data.get("turn_id") or "") != str(failed[1])
                        or str(route_data.get("content_revision") or "")
                        != str(failed[2])
                        or not _valid_final_stable_key(route_data.get("stable_key"))
                        or type(route_data.get("stable_key_version")) is not int
                        or route_data.get("stable_key_version") != 1
                    ):
                        continue
                    route_payloads[_canonical_json(route_data)] = route_data
                if (
                    len(route_payloads) != 1
                    or _final_revision_is_internal_automation_conn(
                        conn,
                        host_id=str(host_id),
                        turn_id=str(failed[1]),
                        content_revision_value=str(failed[2]),
                    )
                ):
                    conn.rollback()
                    return _presentation_error(
                        "plan_conflict",
                        host_id=host_id,
                        name=name,
                        failed_plan_token=failed_plan_token,
                    )
                common["turn"] = next(iter(route_payloads.values()))
            if local_acknowledged_count:
                common["predecessor_job_key"] = str(
                    source_jobs[local_acknowledged_count - 1][4]
                )
            elif inherited_prefix_count:
                inherited_payload = _json_object(source_jobs[0][6])
                predecessor_job_key = str(
                    inherited_payload.get("predecessor_job_key") or ""
                )
                if not predecessor_job_key:
                    conn.rollback()
                    return _presentation_error(
                        "plan_conflict",
                        host_id=host_id,
                        name=name,
                        failed_plan_token=failed_plan_token,
                    )
                common["predecessor_job_key"] = predecessor_job_key
            for (
                sequence_index,
                operation,
                part_ordinal,
                spans_json,
                _key,
                _status,
                _payload_json,
            ) in suffix:
                job_cursor = conn.execute(
                    """
                    INSERT INTO turn_presentation_jobs (
                        plan_id,
                        sequence_index,
                        operation,
                        part_ordinal,
                        spans_json,
                        created_at
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        recovered_plan_id,
                        int(sequence_index),
                        str(operation),
                        int(part_ordinal),
                        str(spans_json),
                        current_time,
                    ),
                )
                payload = {
                    **common,
                    "operation": str(operation),
                    "sequence_index": int(sequence_index),
                    "part_ordinal": int(part_ordinal),
                    "spans": json.loads(str(spans_json)),
                }
                _materialize_connector_plan_job_conn(
                    conn,
                    plan_id=recovered_plan_id,
                    job_id=int(job_cursor.lastrowid),
                    host_id=str(host_id),
                    name=str(name),
                    delivery_key=(
                        f"{name}:{recovered_token}:"
                        f"{int(sequence_index):0{_PRESENTATION_SEQUENCE_WIDTH}d}"
                    ),
                    payload=payload,
                    created_at=current_time,
                )
            conn.execute(
                """
                INSERT INTO turn_presentation_recoveries (
                    host_id,
                    name,
                    request_id,
                    failed_plan_id,
                    recovered_plan_id,
                    failed_plan_token,
                    recovered_plan_token,
                    generation,
                    source_job_count,
                    delivered_prefix_count,
                    fresh_job_count,
                    retained_failed_job_count,
                    prior_attempt_count,
                    outcome,
                    created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'recovered', ?)
                """,
                (
                    str(host_id),
                    str(name),
                    str(request_id),
                    int(failed[0]),
                    recovered_plan_id,
                    str(failed_plan_token),
                    recovered_token,
                    generation,
                    len(source_jobs),
                    acknowledged_prefix_count,
                    len(suffix),
                    retained_failed_job_count,
                    prior_attempt_count,
                    current_time,
                ),
            )
            _finalize_recovered_plan_materialization_conn(
                conn,
                failed_plan_id=int(failed[0]),
                recovered_plan_id=recovered_plan_id,
                now=current_time,
            )
            conn.commit()
            return _presentation_recovery_result(
                failed_plan_token=str(failed_plan_token),
                plan_token=recovered_token,
                generation=generation,
                content_revision=str(failed[2]),
                acknowledged_prefix_count=acknowledged_prefix_count,
                executable_job_count=len(suffix),
                retained_failed_job_count=retained_failed_job_count,
                prior_attempt_count=prior_attempt_count,
                idempotent_replay=False,
            )
        except Exception:
            conn.rollback()
            raise


def poll_connector_outbox(
    db_path: Path,
    host_id: str,
    name: str,
    *,
    limit: int = 1,
    lease_seconds: int = 60,
    max_attempts: int | None = None,
    now: str | None = None,
) -> dict[str, Any]:
    """Atomically lease due connector outbox rows for one neutral queue name."""
    if not _sqlite_store_exists(db_path):
        return sanitize_public_value({
            "schema_version": 1,
            "ok": False,
            "status": "store_unavailable",
            "host_id": str(host_id),
            "name": str(name),
            "items": [],
        })
    current_time = _connector_now(now)
    lease_expires_at = _connector_add_seconds(current_time, max(1, int(lease_seconds)))
    row_limit = max(1, min(int(limit), 100))
    items: list[dict[str, Any]] = []
    with _connect(db_path, isolation_level=None) as conn:
        _ensure_schema(conn)
        conn.execute("BEGIN IMMEDIATE")
        try:
            _connector_reclaim_expired_leases_conn(
                conn,
                host_id=str(host_id),
                name=str(name),
                now=current_time,
            )
            if max_attempts is not None:
                _connector_exhaust_retryable_conn(
                    conn,
                    host_id=str(host_id),
                    name=str(name),
                    max_attempts=max_attempts,
                    now=current_time,
                )
                _mark_exhausted_presentation_plans_conn(
                    conn,
                    host_id=str(host_id),
                    name=str(name),
                    now=current_time,
                )
            rows = conn.execute(
                """
                SELECT
                    outbox.id,
                    outbox.delivery_key,
                    outbox.payload_json,
                    outbox.private_state_json,
                    outbox.created_at
                FROM connector_outbox AS outbox
                WHERE outbox.host_id = ?
                  AND outbox.connector = ?
                  AND outbox.status IN ('queued', 'deferred', 'retry')
                  AND outbox.delivery_kind != 'final_migration_hold'
                  AND (
                      outbox.next_attempt_at IS NULL
                      OR outbox.next_attempt_at = ''
                      OR julianday(outbox.next_attempt_at) IS NULL
                      OR julianday(outbox.next_attempt_at) <= julianday(?)
                  )
                  AND (
                      outbox.delivery_kind != 'final_ready'
                      OR (
                          NOT EXISTS (
                              SELECT 1
                              FROM connector_outbox AS earlier_final
                              WHERE earlier_final.host_id = outbox.host_id
                                AND earlier_final.connector = outbox.connector
                                AND earlier_final.delivery_kind = 'final_ready'
                                AND earlier_final.ordering_key = outbox.ordering_key
                                AND earlier_final.id < outbox.id
                                AND earlier_final.status NOT IN (
                                    'delivered',
                                    'superseded',
                                    'dead_letter',
                                    'awaiting_ack'
                                )
                          )
                          AND NOT EXISTS (
                              SELECT 1
                              FROM turn_presentation_plans AS active_plan
                              JOIN turn_presentation_jobs AS active_job
                                ON active_job.plan_id = active_plan.id
                              LEFT JOIN connector_outbox AS active_outbox
                                ON active_outbox.id = active_job.outbox_id
                              WHERE active_plan.host_id = outbox.host_id
                                AND active_plan.name = outbox.connector
                                AND active_plan.turn_id = outbox.turn_id
                                AND active_plan.content_revision = outbox.content_revision
                                AND active_plan.state = 'active'
                                AND (
                                    active_outbox.id IS NULL
                                    OR active_outbox.status != 'delivered'
                                )
                          )
                          AND NOT EXISTS (
                              SELECT 1
                              FROM turn_presentation_plans AS earlier_plan
                              LEFT JOIN connector_outbox AS earlier_source
                                ON earlier_source.id = earlier_plan.source_outbox_id
                              WHERE earlier_plan.host_id = outbox.host_id
                                AND earlier_plan.name = outbox.connector
                                AND earlier_plan.state IN (
                                    'active',
                                    'waiting_predecessor'
                                )
                                AND (
                                    (
                                        earlier_source.ordering_key
                                            = outbox.ordering_key
                                        AND earlier_source.id < outbox.id
                                    )
                                    OR (
                                        earlier_plan.source_outbox_id IS NULL
                                        AND EXISTS (
                                            SELECT 1
                                            FROM turn_presentation_jobs
                                                AS ordering_job
                                            JOIN connector_outbox
                                                AS ordering_outbox
                                              ON ordering_outbox.id
                                                = ordering_job.outbox_id
                                            WHERE ordering_job.plan_id
                                                = earlier_plan.id
                                              AND ordering_outbox.ordering_key
                                                = outbox.ordering_key
                                              AND ordering_outbox.id < outbox.id
                                        )
                                    )
                                )
                                AND EXISTS (
                                    SELECT 1
                                    FROM turn_presentation_jobs AS earlier_job
                                    LEFT JOIN connector_outbox AS earlier_job_outbox
                                      ON earlier_job_outbox.id = earlier_job.outbox_id
                                    WHERE earlier_job.plan_id = earlier_plan.id
                                      AND (
                                          earlier_job_outbox.id IS NULL
                                          OR earlier_job_outbox.status NOT IN (
                                              'delivered',
                                              'superseded',
                                              'dead_letter'
                                          )
                                      )
                                )
                          )
                      )
                  )
                  AND (
                      outbox.delivery_kind != 'final_part'
                      OR NOT EXISTS (
                          SELECT 1
                          FROM connector_outbox AS earlier_part
                          WHERE earlier_part.host_id = outbox.host_id
                            AND earlier_part.connector = outbox.connector
                            AND earlier_part.delivery_kind = 'final_part'
                            AND earlier_part.ordering_key = outbox.ordering_key
                            AND earlier_part.id < outbox.id
                            AND earlier_part.status NOT IN (
                                'delivered',
                                'superseded',
                                'dead_letter'
                            )
                      )
                  )
                  AND (
                      NOT EXISTS (
                          SELECT 1
                          FROM turn_presentation_jobs AS linked
                          WHERE linked.outbox_id = outbox.id
                      )
                      OR EXISTS (
                          SELECT 1
                          FROM turn_presentation_jobs AS current_job
                          JOIN turn_presentation_plans AS current_plan
                            ON current_plan.id = current_job.plan_id
                          WHERE current_job.outbox_id = outbox.id
                            AND current_plan.state = 'active'
                            AND NOT EXISTS (
                                SELECT 1
                                FROM turn_presentation_jobs AS predecessor
                                JOIN connector_outbox AS predecessor_outbox
                                  ON predecessor_outbox.id = predecessor.outbox_id
                                WHERE predecessor.plan_id = current_job.plan_id
                                  AND predecessor.sequence_index
                                      < current_job.sequence_index
                                  AND predecessor_outbox.status != 'delivered'
                            )
                      )
                  )
                ORDER BY
                    CASE
                        WHEN EXISTS (
                            SELECT 1
                            FROM turn_presentation_jobs AS priority_job
                            JOIN turn_presentation_plans AS priority_plan
                              ON priority_plan.id = priority_job.plan_id
                            WHERE priority_job.outbox_id = outbox.id
                              AND priority_plan.state = 'active'
                        ) THEN 0
                        ELSE 1
                    END,
                    outbox.id
                LIMIT ?
                """,
                (str(host_id), str(name), current_time, row_limit),
            ).fetchall()
            for row in rows:
                outbox_id = int(row[0])
                attempt_row = conn.execute(
                    """
                    SELECT COALESCE(MAX(attempt), 0)
                    FROM connector_deliveries
                    WHERE outbox_id = ?
                    """,
                    (outbox_id,),
                ).fetchone()
                attempt = int(attempt_row[0] or 0) + 1
                lease_token = secrets.token_urlsafe(24)
                public_ref = _connector_public_ref()
                cursor = conn.execute(
                    """
                    INSERT INTO connector_deliveries (
                        outbox_id,
                        host_id,
                        connector,
                        delivery_key,
                        attempt,
                        status,
                        response_json,
                        private_state_json,
                        created_at,
                        delivered_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        outbox_id,
                        str(host_id),
                        str(name),
                        str(row[1]),
                        attempt,
                        _CONNECTOR_LEASE_STATUS,
                        _canonical_json(sanitize_public_mapping({})),
                        _connector_private_with_lease(
                            {},
                            delivery_id=None,
                            attempt=attempt,
                            lease_token=lease_token,
                            lease_expires_at=lease_expires_at,
                            public_ref=public_ref,
                        ),
                        current_time,
                        None,
                    ),
                )
                delivery_id = int(cursor.lastrowid)
                conn.execute(
                    """
                    UPDATE connector_deliveries
                    SET private_state_json = ?
                    WHERE id = ?
                    """,
                    (
                        _connector_private_with_lease(
                            {},
                            delivery_id=delivery_id,
                            attempt=attempt,
                            lease_token=lease_token,
                            lease_expires_at=lease_expires_at,
                            public_ref=public_ref,
                        ),
                        delivery_id,
                    ),
                )
                conn.execute(
                    """
                    UPDATE connector_outbox
                    SET status = ?, updated_at = ?, private_state_json = ?
                    WHERE id = ? AND status IN ('queued', 'deferred', 'retry')
                    """,
                    (
                        _CONNECTOR_LEASE_STATUS,
                        current_time,
                        _connector_private_with_lease(
                            row[3],
                            delivery_id=delivery_id,
                            attempt=attempt,
                            lease_token=lease_token,
                            lease_expires_at=lease_expires_at,
                            public_ref=public_ref,
                        ),
                        outbox_id,
                    ),
                )
                items.append(
                    {
                        "outbox_id": outbox_id,
                        "delivery_id": delivery_id,
                        "host_id": str(host_id),
                        "name": str(name),
                        "key": str(row[1]),
                        "attempt": attempt,
                        "lease_token": lease_token,
                        "leased_until": lease_expires_at,
                        "ref": public_ref,
                        "available_at": current_time,
                        "created_at": str(row[4] or ""),
                        "payload": _restore_presentation_tokens(
                            sanitize_public_mapping(
                                _json_object(row[2]),
                                backend_neutral=True,
                            ),
                            _json_object(row[2]),
                        ),
                    }
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    result = dict(sanitize_public_value({
        "schema_version": 1,
        "ok": True,
        "status": "ok",
        "host_id": str(host_id),
        "name": str(name),
        "items": items,
    }))
    sanitized_items = result.get("items")
    if isinstance(sanitized_items, list):
        for sanitized_item, original_item in zip(sanitized_items, items, strict=True):
            if not isinstance(sanitized_item, dict):
                continue
            original_key = original_item.get("key")
            if _valid_final_ready_key(original_key):
                sanitized_item["key"] = str(original_key)
            sanitized_payload = sanitized_item.get("payload")
            original_payload = original_item.get("payload")
            if isinstance(sanitized_payload, dict) and isinstance(original_payload, Mapping):
                _restore_presentation_tokens(sanitized_payload, original_payload)
    return result


def _connector_validate_live_ref_conn(
    conn: sqlite3.Connection,
    *,
    host_id: str,
    name: str,
    ref: str,
    now: str,
) -> tuple[Any | None, str | None]:
    rows = conn.execute(
        """
        SELECT
            d.id,
            d.outbox_id,
            d.host_id,
            d.connector,
            d.delivery_key,
            d.attempt,
            d.status,
            d.private_state_json,
            o.status,
            o.private_state_json,
            o.delivery_kind,
            o.turn_id,
            o.content_revision
        FROM connector_deliveries d
        LEFT JOIN connector_outbox o ON o.id = d.outbox_id
        WHERE d.host_id = ? AND d.connector = ? AND d.status = ?
        ORDER BY d.id DESC
        """,
        (str(host_id), str(name), _CONNECTOR_LEASE_STATUS),
    ).fetchall()
    for row in rows:
        delivery_state = _json_object(row[7])
        if str(delivery_state.get("public_ref") or "") != str(ref):
            continue
        if str(row[6] or "") != _CONNECTOR_LEASE_STATUS:
            return row, "stale_ref"
        outbox_state = _json_object(row[9])
        if int(outbox_state.get("current_delivery_id") or 0) != int(row[0]):
            return row, "stale_ref"
        if str(row[8] or "") != _CONNECTOR_LEASE_STATUS:
            return row, "stale_ref"
        lease_expires_at = delivery_state.get("lease_expires_at")
        if _connector_deadline_due(
            lease_expires_at,
            now_dt=_connector_datetime(now),
        ):
            return row, "expired_ref"
        return row, None
    return None, "invalid_ref"


def renew_connector_delivery(
    db_path: Path,
    *,
    host_id: str,
    name: str,
    ref: str,
    lease_seconds: int,
    now: str | None = None,
) -> dict[str, Any]:
    """Extend one live connector lease without creating another attempt."""
    if not _sqlite_store_exists(db_path):
        return _connector_error_response(
            status="store_unavailable", host_id=host_id, name=name, ref=ref
        )
    current_time = _connector_now(now)
    with _connect(db_path, isolation_level=None) as conn:
        _ensure_schema(conn)
        conn.execute("BEGIN IMMEDIATE")
        try:
            _connector_reclaim_expired_leases_conn(
                conn,
                host_id=str(host_id),
                name=str(name),
                now=current_time,
            )
            row, error = _connector_validate_live_ref_conn(
                conn,
                host_id=str(host_id),
                name=str(name),
                ref=str(ref),
                now=current_time,
            )
            if error is not None or row is None:
                conn.rollback()
                return _connector_error_response(
                    status=error or "invalid_ref",
                    host_id=host_id,
                    name=name,
                    ref=ref,
                )
            delivery_state = _json_object(row[7])
            outbox_state = _json_object(row[9])
            current_expiry = str(delivery_state.get("lease_expires_at") or current_time)
            requested_expiry = _connector_datetime(
                _connector_add_seconds(current_time, max(1, int(lease_seconds)))
            )
            leased_until = _connector_iso(
                max(
                    _connector_datetime(current_expiry),
                    requested_expiry,
                )
            )
            delivery_state["lease_expires_at"] = leased_until
            outbox_state["lease_expires_at"] = leased_until
            conn.execute(
                "UPDATE connector_deliveries SET private_state_json = ? WHERE id = ?",
                (_canonical_json(delivery_state), int(row[0])),
            )
            conn.execute(
                """
                UPDATE connector_outbox
                SET private_state_json = ?, updated_at = ?
                WHERE id = ? AND status = 'leased'
                """,
                (_canonical_json(outbox_state), current_time, int(row[1])),
            )
            conn.commit()
            return _connector_response(
                ok=True,
                status="renewed",
                host_id=host_id,
                name=name,
                ref=ref,
                key=str(row[4]),
                attempt=int(row[5] or 0),
                leased_until=leased_until,
            )
        except Exception:
            conn.rollback()
            raise


def release_connector_delivery(
    db_path: Path,
    *,
    host_id: str,
    name: str,
    ref: str,
    now: str | None = None,
) -> dict[str, Any]:
    """Release one live lease and make its outbox row immediately available."""
    if not _sqlite_store_exists(db_path):
        return _connector_error_response(
            status="store_unavailable", host_id=host_id, name=name, ref=ref
        )
    current_time = _connector_now(now)
    with _connect(db_path, isolation_level=None) as conn:
        _ensure_schema(conn)
        conn.execute("BEGIN IMMEDIATE")
        try:
            _connector_reclaim_expired_leases_conn(
                conn,
                host_id=str(host_id),
                name=str(name),
                now=current_time,
            )
            row, error = _connector_validate_live_ref_conn(
                conn,
                host_id=str(host_id),
                name=str(name),
                ref=str(ref),
                now=current_time,
            )
            if error is not None or row is None:
                conn.rollback()
                return _connector_error_response(
                    status=error or "invalid_ref",
                    host_id=host_id,
                    name=name,
                    ref=ref,
                )
            terminal_after_lease = bool(
                _json_object(row[9]).get("terminal_after_lease")
            )
            next_status = (
                _CONNECTOR_SUPERSEDED_OUTBOX_STATUS
                if terminal_after_lease
                else "queued"
            )
            result_status = "superseded" if terminal_after_lease else "released"
            conn.execute(
                """
                UPDATE connector_deliveries
                SET status = ?, response_json = ?, delivered_at = ?
                WHERE id = ? AND status = 'leased'
                """,
                (
                    result_status,
                    _canonical_json(
                        {"schema_version": 1, "status": result_status}
                    ),
                    current_time,
                    int(row[0]),
                ),
            )
            conn.execute(
                """
                UPDATE connector_outbox
                SET status = ?, next_attempt_at = ?, updated_at = ?,
                    private_state_json = ?
                WHERE id = ? AND status = 'leased'
                """,
                (
                    next_status,
                    None if terminal_after_lease else current_time,
                    current_time,
                    _connector_private_clear_current(row[9]),
                    int(row[1]),
                ),
            )
            _update_presentation_plan_after_outbox_conn(
                conn,
                outbox_id=int(row[1]),
                outbox_status=next_status,
                now=current_time,
            )
            conn.commit()
            return _connector_response(
                ok=True,
                status=result_status,
                host_id=host_id,
                name=name,
                ref=ref,
                key=str(row[4]),
                attempt=int(row[5] or 0),
                available_at=None if terminal_after_lease else current_time,
            )
        except Exception:
            conn.rollback()
            raise


def _connector_update_ref(
    db_path: Path,
    *,
    action: str,
    host_id: str,
    name: str,
    ref: str,
    response: Mapping[str, Any] | None = None,
    reason: str | None = None,
    available_at: str | None = None,
    delay_seconds: int | None = None,
    max_attempts: int | None = None,
    ack_ttl_seconds: int | None = None,
    now: str | None = None,
) -> dict[str, Any]:
    if not _sqlite_store_exists(db_path):
        return _connector_error_response(status="store_unavailable", host_id=host_id, name=name, ref=ref)
    current_time = _connector_now(now)
    sanitized_response = sanitize_public_mapping(response or {}, backend_neutral=True)
    if str(name) == _TURN_FINAL_NAME:
        sanitized_reason, reason_detail = _turn_final_reason_diagnostics(reason)
    else:
        sanitized_reason = _connector_public_reason(reason)
        reason_detail = ""
    with _connect(db_path, isolation_level=None) as conn:
        _ensure_schema(conn)
        conn.execute("BEGIN IMMEDIATE")
        try:
            _connector_reclaim_expired_leases_conn(
                conn,
                host_id=str(host_id),
                name=str(name),
                now=current_time,
            )
            row, error = _connector_validate_live_ref_conn(
                conn,
                host_id=str(host_id),
                name=str(name),
                ref=str(ref),
                now=current_time,
            )
            if error is not None or row is None:
                conn.rollback()
                return _connector_error_response(status=error or "invalid_ref", host_id=host_id, name=name, ref=ref)

            delivery_id = int(row[0])
            outbox_id = int(row[1] or 0)
            delivery_key = str(row[4])
            attempt = int(row[5] or 0)
            if action == "ack" and str(row[10]) == "final_ready":
                conn.rollback()
                return _connector_error_response(
                    status="prepare_required",
                    host_id=host_id,
                    name=name,
                    ref=ref,
                )
            if action == "ack":
                response_json = _canonical_json(
                    sanitize_public_value({
                        "schema_version": 1,
                        "status": "acknowledged",
                        "response": dict(sanitized_response),
                    })
                )
                conn.execute(
                    """
                    UPDATE connector_deliveries
                    SET status = ?, response_json = ?, delivered_at = ?
                    WHERE id = ?
                    """,
                    ("delivered", response_json, current_time, int(delivery_id)),
                )
                conn.execute(
                    """
                    UPDATE connector_outbox
                    SET status = ?, next_attempt_at = NULL, updated_at = ?, private_state_json = ?
                    WHERE id = ?
                    """,
                    (
                        _CONNECTOR_TERMINAL_OUTBOX_STATUS,
                        current_time,
                        _connector_private_clear_current(row[9]),
                        int(outbox_id),
                    ),
                )
                migration_group = str(
                    _json_object(row[9]).get("migration_group") or ""
                )
                if migration_group:
                    conn.execute(
                        """
                        UPDATE connector_outbox
                        SET status = ?, next_attempt_at = NULL, updated_at = ?
                        WHERE id != ? AND status IN ('queued', 'retry', 'deferred')
                          AND json_extract(private_state_json, '$.migration_group') = ?
                        """,
                        (
                            _CONNECTOR_SUPERSEDED_OUTBOX_STATUS,
                            current_time,
                            outbox_id,
                            migration_group,
                        ),
                    )
                    leased_siblings = conn.execute(
                        """
                        SELECT id, private_state_json
                        FROM connector_outbox
                        WHERE id != ? AND status = 'leased'
                          AND json_extract(
                              private_state_json, '$.migration_group'
                          ) = ?
                        """,
                        (outbox_id, migration_group),
                    ).fetchall()
                    for sibling_id, sibling_private in leased_siblings:
                        conn.execute(
                            """
                            UPDATE connector_outbox
                            SET private_state_json = ?
                            WHERE id = ? AND status = 'leased'
                            """,
                            (
                                _connector_group_private_state(
                                    sibling_private,
                                    group=migration_group,
                                    canonical=False,
                                    terminal_after_lease=True,
                                ),
                                int(sibling_id),
                            ),
                        )
                _update_presentation_plan_after_outbox_conn(
                    conn,
                    outbox_id=outbox_id,
                    outbox_status=_CONNECTOR_TERMINAL_OUTBOX_STATUS,
                    now=current_time,
                    ack_ttl_seconds=ack_ttl_seconds,
                )
                conn.commit()
                return _connector_response(
                    ok=True,
                    status="acknowledged",
                    host_id=host_id,
                    name=name,
                    ref=ref,
                    key=delivery_key,
                    attempt=attempt,
                )

            if available_at is None:
                available_at = _connector_add_seconds(
                    current_time,
                    60 if delay_seconds is None else int(delay_seconds),
                )
            else:
                available_at = _connector_iso(available_at)
            attempt_limit = max(1, int(max_attempts)) if max_attempts is not None else None
            attempts_used = int(
                conn.execute(
                    """
                    SELECT
                        COALESCE(MAX(attempt), 0)
                        - COALESCE(
                            SUM(
                                CASE
                                    WHEN status = 'failed'
                                     AND COALESCE(
                                         json_extract(
                                             response_json,
                                             '$.status'
                                         ),
                                         ''
                                     ) = 'ack_deadline_expired'
                                    THEN 1
                                    ELSE 0
                                END
                            ),
                            0
                        )
                    FROM connector_deliveries
                    WHERE outbox_id = ?
                    """,
                    (outbox_id,),
                ).fetchone()[0]
                or 0
            )
            exhausted = (
                action == "fail"
                and attempt_limit is not None
                and attempts_used >= attempt_limit
            )
            result_status = "attempts_exhausted" if exhausted else ("retry_scheduled" if action == "fail" else "deferred")
            delivery_status = "failed" if action == "fail" else "deferred"
            outbox_status = (
                _CONNECTOR_EXHAUSTED_OUTBOX_STATUS
                if exhausted
                else ("retry" if action == "fail" else "deferred")
            )
            terminal_after_lease = bool(
                _json_object(row[9]).get("terminal_after_lease")
            )
            if terminal_after_lease:
                result_status = "superseded"
                outbox_status = _CONNECTOR_SUPERSEDED_OUTBOX_STATUS
            response_payload = dict(
                sanitize_public_value(
                    {
                        "schema_version": 1,
                        "status": result_status,
                        "reason": sanitized_reason,
                        "available_at": (
                            None if terminal_after_lease else available_at
                        ),
                        "response": dict(sanitized_response),
                    }
                )
            )
            if reason_detail:
                response_payload["reason_detail"] = reason_detail
            response_json = _canonical_json(response_payload)
            conn.execute(
                """
                UPDATE connector_deliveries
                SET status = ?, response_json = ?, delivered_at = ?
                WHERE id = ?
                """,
                (delivery_status, response_json, current_time, int(delivery_id)),
            )
            next_private_state = _connector_private_clear_current(row[9])
            if (
                str(name) == _TURN_FINAL_NAME
                and outbox_status == _CONNECTOR_EXHAUSTED_OUTBOX_STATUS
            ):
                next_private_state = _connector_private_with_terminal_diagnostic(
                    row[9],
                    public_reason=sanitized_reason,
                    reason_detail=reason_detail,
                    attempt_count=_connector_attempt_count_conn(
                        conn,
                        outbox_id=outbox_id,
                        private_state_json=row[9],
                    ),
                    classification=result_status,
                    terminalized_at=current_time,
                )
            conn.execute(
                """
                UPDATE connector_outbox
                SET status = ?, next_attempt_at = ?, updated_at = ?, private_state_json = ?
                WHERE id = ?
                """,
                (
                    outbox_status,
                    None if exhausted or terminal_after_lease else available_at,
                    current_time,
                    next_private_state,
                    int(outbox_id),
                ),
            )
            _update_presentation_plan_after_outbox_conn(
                conn,
                outbox_id=outbox_id,
                outbox_status=outbox_status,
                now=current_time,
            )
            if outbox_status == _CONNECTOR_EXHAUSTED_OUTBOX_STATUS:
                # The exhausted row can be either a materialized plan job or
                # the final-ready source anchor of a still-preparing plan.
                _mark_exhausted_presentation_plans_conn(
                    conn,
                    host_id=str(host_id),
                    name=str(name),
                    now=current_time,
                )
            conn.commit()
            return _connector_response(
                ok=True,
                status=result_status,
                host_id=host_id,
                name=name,
                ref=ref,
                key=delivery_key,
                attempt=attempt,
                available_at=None if exhausted or terminal_after_lease else available_at,
            )
        except Exception:
            conn.rollback()
            raise


def ack_connector_delivery(
    db_path: Path,
    *,
    host_id: str,
    name: str,
    ref: str,
    response: Mapping[str, Any] | None = None,
    ack_ttl_seconds: int = CONNECTOR_ACK_TTL_SECONDS,
    now: str | None = None,
) -> dict[str, Any]:
    """Acknowledge a live connector lease and make the outbox item terminal."""
    return _connector_update_ref(
        db_path,
        action="ack",
        host_id=host_id,
        name=name,
        ref=ref,
        response=response,
        ack_ttl_seconds=max(1, int(ack_ttl_seconds)),
        now=now,
    )


def fail_connector_delivery(
    db_path: Path,
    *,
    host_id: str,
    name: str,
    ref: str,
    reason: str | None = None,
    response: Mapping[str, Any] | None = None,
    available_at: str | None = None,
    delay_seconds: int | None = None,
    max_attempts: int | None = None,
    now: str | None = None,
) -> dict[str, Any]:
    """Record a connector failure and schedule the outbox item for retry."""
    return _connector_update_ref(
        db_path,
        action="fail",
        host_id=host_id,
        name=name,
        ref=ref,
        reason=reason,
        response=response,
        available_at=available_at,
        delay_seconds=delay_seconds,
        max_attempts=max_attempts,
        now=now,
    )


def defer_connector_delivery(
    db_path: Path,
    *,
    host_id: str,
    name: str,
    ref: str,
    reason: str | None = None,
    response: Mapping[str, Any] | None = None,
    available_at: str | None = None,
    delay_seconds: int | None = None,
    now: str | None = None,
) -> dict[str, Any]:
    """Record a connector deferral and make the outbox item available later."""
    return _connector_update_ref(
        db_path,
        action="defer",
        host_id=host_id,
        name=name,
        ref=ref,
        reason=reason,
        response=response,
        available_at=available_at,
        delay_seconds=delay_seconds,
        now=now,
    )


def _valid_final_ready_key(value: Any) -> bool:
    prefix = f"{_TURN_FINAL_NAME}:revision:twfinal1."
    return (
        isinstance(value, str)
        and value.startswith(prefix)
        and _valid_presentation_opaque(value[len(f"{_TURN_FINAL_NAME}:revision:"):], "twfinal1.")
    )


def retry_final_ready_delivery(
    db_path: Path,
    host_id: str,
    *,
    key: str | None = None,
    final_identity: str | None = None,
    now: str | None = None,
) -> dict[str, Any]:
    """Explicitly requeue one exact dead-letter final anchor with a fresh budget."""
    if (key is None) == (final_identity is None):
        return sanitize_public_value(
            {
                "schema_version": 1,
                "ok": False,
                "status": "invalid_params",
                "host_id": str(host_id),
                "name": _TURN_FINAL_NAME,
            }
        )
    if final_identity is not None:
        if not _valid_presentation_opaque(final_identity, "twfinal1."):
            return sanitize_public_value(
                {
                    "schema_version": 1,
                    "ok": False,
                    "status": "invalid_params",
                    "host_id": str(host_id),
                    "name": _TURN_FINAL_NAME,
                }
            )
        key = f"{_TURN_FINAL_NAME}:revision:{final_identity}"
    assert key is not None
    if not _valid_final_ready_key(key):
        return sanitize_public_value(
            {
                "schema_version": 1,
                "ok": False,
                "status": "invalid_params",
                "host_id": str(host_id),
                "name": _TURN_FINAL_NAME,
            }
        )
    if not _sqlite_store_exists(db_path):
        return sanitize_public_value(
            {
                "schema_version": 1,
                "ok": False,
                "status": "store_unavailable",
                "host_id": str(host_id),
                "name": _TURN_FINAL_NAME,
                "key": str(key),
            }
        )
    current_time = _connector_now(now)
    with _connect(db_path, isolation_level=None) as conn:
        _ensure_schema(conn)
        conn.execute("BEGIN IMMEDIATE")
        try:
            row = conn.execute(
                """
                SELECT
                    id,
                    delivery_kind,
                    status,
                    payload_json,
                    private_state_json,
                    turn_id,
                    content_revision
                FROM connector_outbox
                WHERE host_id = ? AND connector = ? AND delivery_key = ?
                """,
                (str(host_id), _TURN_FINAL_NAME, str(key)),
            ).fetchone()
            if row is None:
                conn.rollback()
                return sanitize_public_value(
                    {
                        "schema_version": 1,
                        "ok": False,
                        "status": "not_found",
                        "host_id": str(host_id),
                        "name": _TURN_FINAL_NAME,
                        "key": str(key),
                    }
                )
            if str(row[1]) not in {"final_ready", "final_migration_hold"}:
                conn.rollback()
                return sanitize_public_value(
                    {
                        "schema_version": 1,
                        "ok": False,
                        "status": "not_retryable",
                        "host_id": str(host_id),
                        "name": _TURN_FINAL_NAME,
                        "key": str(key),
                    }
                )
            typed_turn_id = str(row[5] or "")
            typed_revision = str(row[6] or "")
            authoritative = (
                _final_ready_payload_conn(
                    conn,
                    host_id=str(host_id),
                    turn_id=typed_turn_id,
                    content_revision_value=typed_revision,
                )
                if typed_turn_id and typed_revision
                else None
            )
            expected_key = (
                f"{_TURN_FINAL_NAME}:revision:{authoritative['final_identity']}"
                if authoritative is not None
                else None
            )
            if authoritative is None or expected_key != str(key):
                outbox_id = int(row[0])
                conn.execute(
                    """
                    UPDATE connector_deliveries
                    SET status = 'superseded',
                        delivered_at = COALESCE(delivered_at, ?)
                    WHERE outbox_id = ? AND status != 'delivered'
                    """,
                    (current_time, outbox_id),
                )
                conn.execute(
                    """
                    UPDATE connector_outbox
                    SET status = 'superseded',
                        next_attempt_at = NULL,
                        updated_at = ?,
                        private_state_json = ?
                    WHERE id = ? AND status != 'delivered'
                    """,
                    (
                        current_time,
                        _connector_private_clear_current(row[4]),
                        outbox_id,
                    ),
                )
                conn.commit()
                return sanitize_public_value(
                    {
                        "schema_version": 1,
                        "ok": False,
                        "status": "stale_revision",
                        "host_id": str(host_id),
                        "name": _TURN_FINAL_NAME,
                        "key": str(key),
                    }
                )
            if (
                bool(authoritative["content"]["known_incomplete"])
                or _final_revision_is_internal_automation_conn(
                    conn,
                    host_id=str(host_id),
                    turn_id=typed_turn_id,
                    content_revision_value=typed_revision,
                )
            ):
                conn.rollback()
                return sanitize_public_value(
                    {
                        "schema_version": 1,
                        "ok": False,
                        "status": "not_retryable",
                        "host_id": str(host_id),
                        "name": _TURN_FINAL_NAME,
                        "key": str(key),
                    }
                )
            failed_plans = conn.execute(
                """
                SELECT plans.plan_token
                FROM turn_presentation_plans AS plans
                WHERE plans.host_id = ?
                  AND plans.name = ?
                  AND plans.turn_id = ?
                  AND plans.content_revision = ?
                  AND (
                      plans.source_outbox_id = ?
                      OR plans.source_outbox_id IS NULL
                  )
                  AND (
                      plans.source_outbox_id IS NULL
                      OR ? = 'awaiting_ack'
                  )
                  AND plans.state = 'failed'
                  AND NOT EXISTS (
                      SELECT 1
                      FROM turn_presentation_recoveries AS recovery
                      WHERE recovery.failed_plan_id = plans.id
                  )
                ORDER BY plans.generation DESC, plans.id DESC
                LIMIT 2
                """,
                (
                    str(host_id),
                    _TURN_FINAL_NAME,
                    typed_turn_id,
                    typed_revision,
                    int(row[0]),
                    str(row[2]),
                ),
            ).fetchall()
            if (
                len(failed_plans) == 1
                and _valid_presentation_opaque(
                    failed_plans[0][0],
                    "twplan1.",
                )
            ):
                failed_plan_token = str(failed_plans[0][0])
                request_id = (
                    "recover-request-"
                    + stable_fingerprint(
                        {
                            "domain": (
                                "tendwire.connector.final-retry-recovery.v1"
                            ),
                            "host_id": str(host_id),
                            "failed_plan_token": failed_plan_token,
                        },
                        length=64,
                    )
                )
                conn.rollback()
                return prepare_connector_plan_recover(
                    db_path,
                    str(host_id),
                    name=_TURN_FINAL_NAME,
                    failed_plan_token=failed_plan_token,
                    request_id=request_id,
                    now=current_time,
                )
            if str(row[2]) != _CONNECTOR_EXHAUSTED_OUTBOX_STATUS:
                conn.rollback()
                return sanitize_public_value(
                    {
                        "schema_version": 1,
                        "ok": False,
                        "status": "not_retryable",
                        "host_id": str(host_id),
                        "name": _TURN_FINAL_NAME,
                        "key": str(key),
                    }
                )
            outbox_id = int(row[0])
            prior_state = _json_object(row[4])
            prior_attempts = int(prior_state.get("prior_attempt_count") or 0)
            local_attempts = int(
                conn.execute(
                    """
                    SELECT COUNT(*)
                    FROM connector_deliveries
                    WHERE outbox_id = ?
                    """,
                    (outbox_id,),
                ).fetchone()[0]
                or 0
            )
            if str(row[1]) == "final_migration_hold" and local_attempts:
                conn.rollback()
                return sanitize_public_value(
                    {
                        "schema_version": 1,
                        "ok": False,
                        "status": "not_retryable",
                        "host_id": str(host_id),
                        "name": _TURN_FINAL_NAME,
                        "key": str(key),
                    }
                )
            total_attempts = prior_attempts + local_attempts
            next_private_state = _json_object(
                _connector_private_clear_terminal_diagnostic(
                    _connector_private_clear_current(prior_state)
                )
            )
            next_private_state["prior_attempt_count"] = total_attempts
            conn.execute(
                "DELETE FROM connector_deliveries WHERE outbox_id = ?",
                (outbox_id,),
            )
            final_identity = str(authoritative["final_identity"])
            cursor = conn.execute(
                """
                UPDATE connector_outbox
                SET delivery_kind = 'final_ready',
                    status = 'queued',
                    payload_json = CASE
                        WHEN delivery_kind = 'final_migration_hold' THEN ?
                        ELSE payload_json
                    END,
                    next_attempt_at = NULL,
                    updated_at = ?,
                    private_state_json = ?
                WHERE id = ? AND status = 'dead_letter'
                """,
                (
                    _canonical_json(authoritative),
                    current_time,
                    _canonical_json(next_private_state),
                    outbox_id,
                ),
            )
            if not cursor.rowcount:
                raise StoreSchemaError("final_retry_conflict")
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    result = dict(
        sanitize_public_value(
            {
                "schema_version": 1,
                "ok": True,
                "status": "requeued",
                "host_id": str(host_id),
                "name": _TURN_FINAL_NAME,
                "key": str(key),
                "final_identity": final_identity,
                "prior_attempt_count": total_attempts,
            }
        )
    )
    result["key"] = str(key)
    if _valid_presentation_opaque(final_identity, "twfinal1."):
        result["final_identity"] = final_identity
    return result


def _bounded_connector_attempt_count(*values: Any) -> int:
    """Return a fail-closed SQLite-sized sum of nonnegative attempt counters."""
    maximum = (1 << 63) - 1
    total = 0
    for value in values:
        if (
            not isinstance(value, int)
            or isinstance(value, bool)
            or value < 0
            or value > maximum
        ):
            continue
        total = min(maximum, total + value)
    return total


def inspect_connector_outbox(
    db_path: Path,
    host_id: str,
    *,
    name: str,
    status: str,
    limit: int = 20,
) -> dict[str, Any]:
    """Return one bounded public-safe view of neutral final outbox pressure."""
    bounded_limit = max(1, min(int(limit), 100))
    if str(name) != _TURN_FINAL_NAME or str(status) != "dead_letter":
        return sanitize_public_value(
            {
                "schema_version": 1,
                "ok": False,
                "status": "invalid_params",
                "host_id": str(host_id),
                "name": str(name),
                "items": [],
            }
        )
    if not _sqlite_store_exists(db_path):
        return sanitize_public_value(
            {
                "schema_version": 1,
                "ok": False,
                "status": "store_unavailable",
                "host_id": str(host_id),
                "name": str(name),
                "items": [],
            }
        )
    with _connect(db_path) as conn:
        _ensure_schema(conn)
        anchor_total = int(
            conn.execute(
                """
                SELECT COUNT(*)
                FROM connector_outbox
                WHERE host_id = ?
                  AND connector = ?
                  AND status = ?
                  AND delivery_kind IN (
                      'final_ready',
                      'final_migration_hold'
                  )
                """,
                (str(host_id), str(name), str(status)),
            ).fetchone()[0]
            or 0
        )
        rows = conn.execute(
            """
            SELECT
                outbox.delivery_key,
                outbox.status,
                outbox.created_at,
                outbox.updated_at,
                outbox.payload_json,
                outbox.private_state_json,
                (
                    SELECT COUNT(*)
                    FROM connector_deliveries AS attempts
                    WHERE attempts.outbox_id = outbox.id
                )
            FROM connector_outbox AS outbox
            WHERE outbox.host_id = ?
              AND outbox.connector = ?
              AND outbox.status = ?
              AND outbox.delivery_kind IN (
                  'final_ready',
                  'final_migration_hold'
              )
            ORDER BY outbox.id
            LIMIT ?
            """,
            (
                str(host_id),
                str(name),
                str(status),
                bounded_limit,
            ),
        ).fetchall()
        failed_total = int(
            conn.execute(
                """
                SELECT COUNT(*)
                FROM turn_presentation_plans AS plans
                JOIN connector_outbox AS source
                  ON source.host_id = plans.host_id
                 AND source.connector = plans.name
                 AND source.turn_id = plans.turn_id
                 AND source.content_revision = plans.content_revision
                 AND source.delivery_kind = 'final_ready'
                 AND (
                     source.id = plans.source_outbox_id
                     OR plans.source_outbox_id IS NULL
                 )
                JOIN turn_content_revisions AS revisions
                  ON revisions.host_id = plans.host_id
                 AND revisions.turn_id = plans.turn_id
                 AND revisions.content_revision = plans.content_revision
                WHERE plans.host_id = ?
                  AND plans.name = ?
                  AND plans.state = 'failed'
                  AND (
                      (
                          plans.source_outbox_id IS NOT NULL
                          AND source.status = 'awaiting_ack'
                      )
                      OR (
                          plans.source_outbox_id IS NULL
                          AND source.status != 'superseded'
                      )
                  )
                  AND revisions.is_current = 1
                  AND revisions.final_state = 'complete'
                  AND revisions.user_state IN ('absent', 'complete')
                  AND NOT EXISTS (
                      SELECT 1
                      FROM turn_presentation_recoveries AS recovery
                      WHERE recovery.failed_plan_id = plans.id
                  )
                """,
                (str(host_id), str(name)),
            ).fetchone()[0]
            or 0
        )
        if failed_total and len(rows) >= bounded_limit:
            rows = rows[: max(0, bounded_limit - 1)]
        failed_rows = conn.execute(
            """
            SELECT
                plans.plan_token,
                source.delivery_key,
                source.payload_json,
                plans.turn_id,
                plans.content_revision,
                plans.generation,
                (
                    SELECT COUNT(*)
                    FROM turn_presentation_jobs AS failed_jobs
                    JOIN connector_outbox AS failed_outbox
                      ON failed_outbox.id = failed_jobs.outbox_id
                    WHERE failed_jobs.plan_id = plans.id
                      AND failed_outbox.status = 'dead_letter'
                ),
                COALESCE(
                    (
                        SELECT recovery.prior_attempt_count
                        FROM turn_presentation_recoveries AS recovery
                        WHERE recovery.recovered_plan_id = plans.id
                    ),
                    0
                ) + (
                    SELECT COUNT(*)
                    FROM turn_presentation_jobs AS attempt_jobs
                    JOIN connector_deliveries AS attempts
                      ON attempts.outbox_id = attempt_jobs.outbox_id
                    WHERE attempt_jobs.plan_id = plans.id
                )
            FROM turn_presentation_plans AS plans
            JOIN connector_outbox AS source
              ON source.host_id = plans.host_id
             AND source.connector = plans.name
             AND source.turn_id = plans.turn_id
             AND source.content_revision = plans.content_revision
             AND source.delivery_kind = 'final_ready'
             AND (
                 source.id = plans.source_outbox_id
                 OR plans.source_outbox_id IS NULL
             )
            JOIN turn_content_revisions AS revisions
              ON revisions.host_id = plans.host_id
             AND revisions.turn_id = plans.turn_id
             AND revisions.content_revision = plans.content_revision
            WHERE plans.host_id = ?
              AND plans.name = ?
              AND plans.state = 'failed'
              AND (
                  (
                      plans.source_outbox_id IS NOT NULL
                      AND source.status = 'awaiting_ack'
                  )
                  OR (
                      plans.source_outbox_id IS NULL
                      AND source.status != 'superseded'
                  )
              )
              AND revisions.is_current = 1
              AND revisions.final_state = 'complete'
              AND revisions.user_state IN ('absent', 'complete')
              AND NOT EXISTS (
                  SELECT 1
                  FROM turn_presentation_recoveries AS recovery
                  WHERE recovery.failed_plan_id = plans.id
              )
            ORDER BY plans.generation DESC, plans.id DESC
            LIMIT ?
            """,
            (
                str(host_id),
                str(name),
                max(0, bounded_limit - len(rows)),
            ),
        ).fetchall()
        classification_rows = conn.execute(
            """
            SELECT
                CASE
                    WHEN jobs.id IS NOT NULL THEN 'failed_plan_job'
                    WHEN outbox.delivery_kind = 'final_ready'
                        THEN 'final_anchor'
                    WHEN outbox.delivery_kind = 'final_migration_hold'
                        THEN 'migration_hold'
                    ELSE 'other'
                END,
                COALESCE(
                    CASE
                        WHEN json_valid(deliveries.response_json)
                        THEN json_extract(deliveries.response_json, '$.status')
                    END,
                    'missing'
                ),
                COALESCE(
                    CASE
                        WHEN json_valid(deliveries.response_json)
                        THEN json_extract(deliveries.response_json, '$.reason')
                    END,
                    'missing'
                ),
                COUNT(*)
            FROM connector_outbox AS outbox
            LEFT JOIN turn_presentation_jobs AS jobs
              ON jobs.outbox_id = outbox.id
            LEFT JOIN connector_deliveries AS deliveries
              ON deliveries.id = (
                  SELECT latest.id
                  FROM connector_deliveries AS latest
                  WHERE latest.outbox_id = outbox.id
                  ORDER BY latest.id DESC
                  LIMIT 1
              )
            WHERE outbox.host_id = ?
              AND outbox.connector = ?
              AND outbox.status = 'dead_letter'
            GROUP BY 1, 2, 3
            ORDER BY COUNT(*) DESC, 1, 2, 3
            """,
            (str(host_id), str(name)),
        ).fetchall()
        total = anchor_total + failed_total
    items: list[dict[str, Any]] = []
    for (
        key_value,
        status_value,
        created_at,
        updated_at,
        payload_json,
        private_json,
        attempts,
    ) in rows:
        original_payload = _json_object(payload_json)
        payload = _restore_presentation_tokens(
            dict(
                sanitize_public_mapping(
                    original_payload,
                    backend_neutral=True,
                )
            ),
            original_payload,
        )
        prior_attempts = _bounded_connector_attempt_count(
            _json_object(private_json).get("prior_attempt_count")
        )
        item = {
            "status": _store_public_label(
                status_value,
                allowed={_CONNECTOR_EXHAUSTED_OUTBOX_STATUS},
            ),
            "created_at": str(created_at),
            "updated_at": str(updated_at),
            "attempt_count": _bounded_connector_attempt_count(prior_attempts, attempts),
            "final": payload,
        }
        if _valid_final_ready_key(key_value):
            item["key"] = str(key_value)
        items.append(item)
    for (
        plan_token,
        key_value,
        payload_json,
        turn_id,
        content_revision_value,
        generation,
        failed_job_count,
        attempt_count,
    ) in failed_rows:
        payload = _json_object(payload_json)
        final_identity_value = payload.get("final_identity")
        if (
            not _valid_presentation_opaque(plan_token, "twplan1.")
            or not _valid_final_ready_key(key_value)
            or not _valid_presentation_opaque(
                final_identity_value,
                "twfinal1.",
            )
            or not _valid_presentation_label(turn_id, prefix="turn-")
            or not _valid_presentation_opaque(
                content_revision_value,
                "twrev1.",
            )
        ):
            continue
        items.append(
            {
                "kind": "failed_plan",
                "status": _CONNECTOR_EXHAUSTED_OUTBOX_STATUS,
                "plan_token": str(plan_token),
                "final_identity": str(final_identity_value),
                "key": str(key_value),
                "turn_id": str(turn_id),
                "content_revision": str(content_revision_value),
                "generation": int(generation),
                "failed_job_count": int(failed_job_count or 0),
                "attempt_count": _bounded_connector_attempt_count(attempt_count),
            }
        )
    classifications: list[dict[str, Any]] = []
    classification_statuses = {
        "ack_deadline_expired",
        "attempts_exhausted",
        "deferred",
        "expired",
        "missing",
        "plan_unrecoverable",
        "retry_scheduled",
    }
    classification_reasons = {*_TURN_FINAL_PUBLIC_REASONS, "missing", "unknown"}
    for kind_value, status_value, reason_value, count_value in classification_rows:
        kind = _store_public_label(
            kind_value,
            allowed={
                "failed_plan_job",
                "final_anchor",
                "migration_hold",
                "other",
            },
        )
        recovery = (
            "source_retry"
            if kind == "failed_plan_job"
            else "exact_key"
            if kind in {"final_anchor", "migration_hold"}
            else "none"
        )
        classifications.append(
            {
                "kind": kind,
                "last_status": _store_public_label(
                    status_value,
                    allowed=classification_statuses,
                ),
                "last_reason": _store_public_label(
                    reason_value,
                    allowed=classification_reasons,
                ),
                "count": max(0, int(count_value or 0)),
                "recovery": recovery,
            }
        )
    dead_letter_rows = sum(item["count"] for item in classifications)
    result = dict(
        sanitize_public_value(
            {
                "schema_version": 1,
                "ok": True,
                "status": "ok",
                "host_id": str(host_id),
                "name": str(name),
                "filter_status": str(status),
                "total": total,
                "dead_letter_rows": dead_letter_rows,
                "classifications": classifications,
                "items": items,
            }
        )
    )
    clean_items = result.get("items")
    if isinstance(clean_items, list):
        for clean_item, original_item in zip(clean_items, items, strict=True):
            if not isinstance(clean_item, dict):
                continue
            original_key = original_item.get("key")
            if _valid_final_ready_key(original_key):
                clean_item["key"] = str(original_key)
            original_plan_token = original_item.get("plan_token")
            if _valid_presentation_opaque(original_plan_token, "twplan1."):
                clean_item["plan_token"] = str(original_plan_token)
            original_final_identity = original_item.get("final_identity")
            if _valid_presentation_opaque(
                original_final_identity,
                "twfinal1.",
            ):
                clean_item["final_identity"] = str(original_final_identity)
            original_turn_id = original_item.get("turn_id")
            if _valid_presentation_label(original_turn_id, prefix="turn-"):
                clean_item["turn_id"] = str(original_turn_id)
            clean_final = clean_item.get("final")
            original_final = original_item.get("final")
            if isinstance(clean_final, dict) and isinstance(original_final, Mapping):
                _restore_presentation_tokens(clean_final, original_final)
    return result


def _snapshot_dict(snapshot: Snapshot) -> dict[str, Any]:
    if hasattr(snapshot, "to_dict"):
        data = snapshot.to_dict()
    else:
        data = json.loads(snapshot.to_json())
    return dict(data)


def _sort_observations(value: Any) -> Any:
    if not isinstance(value, list):
        return value
    if not all(isinstance(item, Mapping) for item in value):
        return value
    return sorted(
        (dict(item) for item in value),
        key=lambda item: (
            str(item.get("id") or item.get("fingerprint") or ""),
            str(item.get("fingerprint") or ""),
            _canonical_json(item),
        ),
    )


def _strip_content_volatile(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {
            str(key): _strip_content_volatile(item)
            for key, item in value.items()
            if str(key).lower() not in {"updated_at", "observed_at", "content_fingerprint"}
        }
    if isinstance(value, list | tuple):
        return [_strip_content_volatile(item) for item in value]
    return value


def _fingerprint_input(data: Mapping[str, Any]) -> dict[str, Any]:
    fingerprint_data = dict(_strip_content_volatile(data))
    for collection in ("spaces", "workers", "attention"):
        if collection in fingerprint_data:
            fingerprint_data[collection] = _sort_observations(
                fingerprint_data[collection]
            )
    return fingerprint_data


def _content_fingerprint(data: Mapping[str, Any]) -> str:
    raw = data.get("content_fingerprint")
    if isinstance(raw, str) and raw:
        return raw
    return stable_fingerprint(
        _fingerprint_input(data),
        length=FINGERPRINT_HEX_LENGTH,
    )


def _command_receipt_from_row(row: Any) -> dict[str, Any]:
    """Return the authoritative command receipt, excluding owner-token and binding evidence.

    ``selector_proof`` and ``owner_expires_at`` are private submission-path
    evidence. They let a caller decide a retry from stored truth alone, and they
    must never reach an envelope, event, audit row, or connector payload.
    """
    return {
        "host_id": str(row[1]),
        "request_id": str(row[2]),
        "action": str(row[3]),
        "canonical_version": int(row[4]),
        "canonical_fingerprint": str(row[5]),
        "canonical_request_json": str(row[6]),
        "public_worker_id": str(row[7]),
        "state": str(row[8]),
        "status": str(row[9]),
        "result_json": str(row[10]),
        "owner_expires_at": row[12],
        "created_at": str(row[14]),
        "reserved_at": str(row[15]),
        "send_started_at": row[16],
        "terminal_at": row[17],
        "updated_at": str(row[18]),
        "selector_proof": str(row[19]),
    }


def command_reservation_is_live(
    receipt: Mapping[str, Any],
    *,
    now: str | None = None,
) -> bool:
    """Return whether a reservation still has an unexpired mutation owner.

    A live reservation means another caller may still be sending, so a retry
    must replay in-progress rather than re-drive the mutation. An unreadable or
    absent deadline is treated as live: refusing to re-send is always the safe
    direction.
    """
    if not isinstance(receipt, Mapping):
        return True
    if str(receipt.get("state") or "") not in {"reserved", "send_started"}:
        return False
    expires_at = receipt.get("owner_expires_at")
    if not isinstance(expires_at, str) or not expires_at.strip():
        return True
    try:
        deadline = datetime.fromisoformat(_command_request_now(expires_at))
        current = datetime.fromisoformat(_command_request_now(now))
    except ValueError:
        return True
    return deadline > current


def _owner_token_hash(owner_token: str) -> str:
    token = str(owner_token)
    if not token:
        return ""
    return hashlib.sha256(
        b"tendwire.command-owner.v1\x00" + token.encode("utf-8")
    ).hexdigest()


def _worker_binding_from_row(row: Any) -> WorkerBinding:
    return WorkerBinding(
        host_id=row[0],
        worker_id=row[1],
        worker_fingerprint=row[2],
        backend=row[3],
        target_kind=row[4],
        target_value=row[5],
        turn_target_kind=row[6],
        turn_target_value=row[7],
        sendable=bool(row[8]),
        reason=row[9],
        observed_at=row[10],
        expires_at=row[11],
        private_fingerprint=row[12],
    )






def _command_request_row(
    conn: sqlite3.Connection,
    host_id: str,
    request_id: str,
) -> Any:
    return conn.execute(
        """
        SELECT
            id,
            host_id,
            request_id,
            action,
            canonical_version,
            canonical_fingerprint,
            canonical_request_json,
            public_worker_id,
            state,
            status,
            result_json,
            owner_token_hash,
            owner_expires_at,
            binding_fingerprint,
            created_at,
            reserved_at,
            send_started_at,
            terminal_at,
            updated_at,
            selector_proof
        FROM command_receipts
        WHERE host_id = ? AND request_id = ?
        LIMIT 1
        """,
        (str(host_id), str(request_id)),
    ).fetchone()


def _snapshot_payload(data: Mapping[str, Any]) -> tuple[dict[str, Any], str]:
    payload_data = sanitize_public_mapping(data)
    payload_data.setdefault("schema_version", SCHEMA_VERSION)
    fingerprint = _content_fingerprint(payload_data)
    payload_data["content_fingerprint"] = fingerprint
    return payload_data, fingerprint














def _append_event_conn(
    conn: sqlite3.Connection,
    *,
    host_id: str,
    event_type: str,
    payload: Mapping[str, Any],
    aggregate_type: str = "",
    aggregate_id: str = "",
    observed_at: str | None = None,
    content_fingerprint: str | None = None,
) -> int:
    payload_json = _canonical_json(payload)
    fingerprint = content_fingerprint or stable_fingerprint(
        {"event_type": event_type, "payload": payload}
    )
    cursor = conn.execute(
        """
        INSERT INTO events (
            host_id,
            event_type,
            aggregate_type,
            aggregate_id,
            observed_at,
            content_fingerprint,
            payload_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            str(host_id),
            str(event_type),
            str(aggregate_type),
            str(aggregate_id),
            observed_at or utc_timestamp(),
            str(fingerprint),
            payload_json,
        ),
    )
    return int(cursor.lastrowid)


def _repair_missing_final_ready_anchors_conn(
    conn: sqlite3.Connection,
    *,
    host_id: str,
    now: str,
) -> None:
    """Repair missing final anchors without decoding retained turn payloads."""
    rows = conn.execute(
        """
        SELECT revisions.turn_id, revisions.content_revision
        FROM turn_content_revisions AS revisions
        WHERE revisions.host_id = ?
          AND revisions.is_current = 1
          AND revisions.final_state = 'complete'
          AND NOT EXISTS (
              SELECT 1
              FROM connector_outbox AS outbox
              WHERE outbox.host_id = revisions.host_id
                AND outbox.turn_id = revisions.turn_id
                AND outbox.content_revision = revisions.content_revision
                AND outbox.delivery_kind = 'final_ready'
          )
        """,
        (str(host_id),),
    ).fetchall()
    for turn_id, content_revision_value in rows:
        _ensure_final_ready_anchor_conn(
            conn,
            host_id=str(host_id),
            turn_id=str(turn_id),
            content_revision_value=str(content_revision_value),
            now=str(now),
        )




def _prune_host_projection(
    conn: sqlite3.Connection,
    table: str,
    key_column: str,
    host_id: str,
    keep_ids: Iterable[str],
) -> None:
    ids = sorted({str(value) for value in keep_ids})
    if ids:
        placeholders = ",".join("?" for _ in ids)
        conn.execute(
            f"DELETE FROM {table} WHERE host_id = ? AND {key_column} NOT IN ({placeholders})",
            [str(host_id), *ids],
        )
    else:
        conn.execute(f"DELETE FROM {table} WHERE host_id = ?", (str(host_id),))


def _turn_payload_is_prune_protected(payload_json: Any) -> bool:
    """Source-observed turns outlive snapshot projection rewrites."""
    try:
        payload = json.loads(str(payload_json or "{}"))
    except json.JSONDecodeError:
        return False
    if not isinstance(payload, Mapping):
        return False
    return bool(str(payload.get("source_turn_id") or "").strip())


def _typed_final_reference_exists_conn(
    conn: sqlite3.Connection,
    *,
    host_id: str,
    turn_id: str,
    content_revision_value: str | None = None,
) -> bool:
    revision_clause = (
        "AND content_revision = ?"
        if content_revision_value is not None
        else "AND content_revision IS NOT NULL"
    )
    params: list[Any] = [str(host_id), str(turn_id)]
    if content_revision_value is not None:
        params.append(str(content_revision_value))
    return bool(
        conn.execute(
            f"""
            SELECT 1
            FROM connector_outbox
            WHERE host_id = ? AND turn_id = ?
              AND delivery_kind GLOB 'final_*'
              {revision_clause}
            LIMIT 1
            """,
            params,
        ).fetchone()
    )


def _delete_turn_if_unreferenced_conn(
    conn: sqlite3.Connection,
    host_id: str,
    turn_id: str,
) -> bool:
    """Delete one historical turn and invalidate active list traversals."""
    if _typed_final_reference_exists_conn(
        conn,
        host_id=str(host_id),
        turn_id=str(turn_id),
    ):
        return False
    protected = conn.execute(
        """
        SELECT 1
        FROM turn_content_revisions AS revisions
        WHERE revisions.host_id = ? AND revisions.turn_id = ?
          AND (
              EXISTS (
                  SELECT 1
                  FROM turn_presentation_plans AS plans
                  WHERE plans.host_id = revisions.host_id
                    AND plans.turn_id = revisions.turn_id
                    AND plans.content_revision = revisions.content_revision
              )
              OR EXISTS (
                  SELECT 1
                  FROM connector_outbox AS outbox
                  WHERE outbox.host_id = revisions.host_id
                    AND outbox.connector = ?
                    AND json_valid(outbox.payload_json)
                    AND json_extract(
                        outbox.payload_json,
                        '$.content_revision'
                    ) = revisions.content_revision
              )
          )
        LIMIT 1
        """,
        (str(host_id), str(turn_id), _TURN_FINAL_NAME),
    ).fetchone()
    if protected is not None:
        return False
    _ensure_turn_list_host_state_conn(conn, host_id)
    conn.execute(
        "DELETE FROM turn_content_revisions WHERE host_id = ? AND turn_id = ?",
        (str(host_id), str(turn_id)),
    )
    cursor = conn.execute(
        "DELETE FROM turns WHERE host_id = ? AND turn_id = ?",
        (str(host_id), str(turn_id)),
    )
    deleted = cursor.rowcount > 0
    if deleted:
        _increment_turn_list_generation_conn(conn, host_id)
    return deleted


def _prune_turn_projection(
    conn: sqlite3.Connection,
    host_id: str,
    keep_ids: Iterable[str],
) -> None:
    ids = sorted({str(value) for value in keep_ids})
    if ids:
        placeholders = ",".join("?" for _ in ids)
        rows = conn.execute(
            f"""
            SELECT turn_id, payload_json
            FROM turns
            WHERE host_id = ? AND turn_id NOT IN ({placeholders})
            """,
            [str(host_id), *ids],
        ).fetchall()
    else:
        rows = conn.execute(
            """
            SELECT turn_id, payload_json
            FROM turns
            WHERE host_id = ?
            """,
            (str(host_id),),
        ).fetchall()
    for turn_id, payload_json in rows:
        if _turn_payload_is_prune_protected(payload_json):
            continue
        _delete_turn_if_unreferenced_conn(conn, str(host_id), str(turn_id))


def _attention_id_from_item(item: Mapping[str, Any]) -> str:
    return str(item.get("id") or item.get("fingerprint") or "unknown")


def _attention_lifecycle_payload(
    item: Mapping[str, Any],
    *,
    attention_id: str,
    observed_at: str,
    first_seen_at: str,
    last_seen_at: str,
    last_changed_at: str,
    lifecycle_status: str,
    signal_count: int,
    resolved_at: str | None = None,
    resolved_reason: str | None = None,
) -> dict[str, Any]:
    payload = dict(item)
    payload.setdefault("id", attention_id)
    payload.setdefault("source", "")
    payload.setdefault("kind", "unknown")
    payload.setdefault("severity", "info")
    payload.setdefault("status", "unknown")
    payload.setdefault("fingerprint", "")
    payload["observed_at"] = observed_at
    payload["first_seen_at"] = first_seen_at
    payload["last_seen_at"] = last_seen_at
    payload["last_changed_at"] = last_changed_at
    payload["lifecycle_status"] = lifecycle_status
    payload["resolved_at"] = resolved_at
    if resolved_reason is not None:
        payload["resolved_reason"] = resolved_reason
    payload["signal_count"] = max(1, int(signal_count))
    return sanitize_public_value(payload)


def _attention_severity_rank(value: Any) -> int:
    return _ATTENTION_SEVERITY_RANK.get(normalize_severity(value), 0)


def _strict_utc_timestamp(value: Any) -> str | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(raw)
    except (TypeError, ValueError, OverflowError):
        return None
    if parsed.tzinfo is None:
        return None
    try:
        return parsed.astimezone(timezone.utc).isoformat()
    except (OverflowError, ValueError):
        return None


def _attention_family_key(host_id: str, item: Mapping[str, Any]) -> str:
    source = _store_public_text(item.get("source"), default="unknown")
    kind = _store_public_label(item.get("kind"))
    return stable_fingerprint(
        {
            "domain": "tendwire.attention.lifecycle-family.v1",
            "host_id": str(host_id),
            "source": source,
            "kind": kind,
        }
    )


def _attention_observation_key(
    *,
    host_id: str,
    authority: str,
    observed_at: str,
    content_fingerprint: str,
) -> str:
    return stable_fingerprint(
        {
            "domain": "tendwire.attention.observation.v1",
            "host_id": str(host_id),
            "authority": str(authority),
            "observed_at": observed_at,
            "snapshot_content_fingerprint": str(content_fingerprint),
        }
    )


@dataclass(frozen=True)
class _AttentionLifecycleState:
    host_id: str
    family_key: str
    generation: int
    lifecycle_status: str
    current_attention_id: str | None
    first_seen_at: str
    last_positive_at: str
    first_missing_at: str | None
    missing_observation_count: int
    last_accepted_at: str
    last_observation_key: str
    max_notified_severity_rank: int


@dataclass(frozen=True)
class _AttentionObservation:
    host_id: str
    family_key: str
    authority: str
    observed_at: str
    observation_key: str
    signal: Mapping[str, Any] | None


@dataclass(frozen=True)
class _AttentionTransition:
    action: str
    next_state: _AttentionLifecycleState | None
    upsert_signal: Mapping[str, Any] | None = None
    superseded_attention_id: str | None = None
    resolve_attention_id: str | None = None
    delivery: tuple[str, str] | None = None


def _plan_attention_transition(
    state: _AttentionLifecycleState | None,
    observation: _AttentionObservation,
) -> _AttentionTransition:
    signal = observation.signal
    if state is not None:
        if observation.observed_at < state.last_accepted_at:
            return _AttentionTransition("no-op", state)
        if observation.observed_at == state.last_accepted_at:
            return _AttentionTransition("no-op", state)

    if signal is not None:
        attention_id = _attention_id_from_item(signal)
        severity = normalize_severity(signal.get("severity"))
        severity_rank = _attention_severity_rank(severity)
        if state is None:
            next_state = _AttentionLifecycleState(
                host_id=observation.host_id,
                family_key=observation.family_key,
                generation=1,
                lifecycle_status=ATTENTION_LIFECYCLE_OPEN,
                current_attention_id=attention_id,
                first_seen_at=observation.observed_at,
                last_positive_at=observation.observed_at,
                first_missing_at=None,
                missing_observation_count=0,
                last_accepted_at=observation.observed_at,
                last_observation_key=observation.observation_key,
                max_notified_severity_rank=severity_rank,
            )
            return _AttentionTransition(
                "open",
                next_state,
                upsert_signal=signal,
                delivery=("attention_created", "initial"),
            )

        if state.lifecycle_status == ATTENTION_LIFECYCLE_RESOLVED:
            next_state = _AttentionLifecycleState(
                host_id=state.host_id,
                family_key=state.family_key,
                generation=state.generation + 1,
                lifecycle_status=ATTENTION_LIFECYCLE_OPEN,
                current_attention_id=attention_id,
                first_seen_at=observation.observed_at,
                last_positive_at=observation.observed_at,
                first_missing_at=None,
                missing_observation_count=0,
                last_accepted_at=observation.observed_at,
                last_observation_key=observation.observation_key,
                max_notified_severity_rank=severity_rank,
            )
            return _AttentionTransition(
                "open",
                next_state,
                upsert_signal=signal,
                delivery=("attention_created", "initial"),
            )

        escalated = severity_rank > state.max_notified_severity_rank
        next_state = _AttentionLifecycleState(
            host_id=state.host_id,
            family_key=state.family_key,
            generation=state.generation,
            lifecycle_status=ATTENTION_LIFECYCLE_OPEN,
            current_attention_id=attention_id,
            first_seen_at=state.first_seen_at,
            last_positive_at=observation.observed_at,
            first_missing_at=None,
            missing_observation_count=0,
            last_accepted_at=observation.observed_at,
            last_observation_key=observation.observation_key,
            max_notified_severity_rank=max(
                state.max_notified_severity_rank, severity_rank
            ),
        )
        return _AttentionTransition(
            "escalate" if escalated else "update",
            next_state,
            upsert_signal=signal,
            superseded_attention_id=(
                state.current_attention_id
                if state.current_attention_id != attention_id
                else None
            ),
            delivery=(
                ("attention_escalated", f"severity:{severity}")
                if escalated
                else None
            ),
        )

    if state is None or state.lifecycle_status != ATTENTION_LIFECYCLE_OPEN:
        return _AttentionTransition("no-op", state)
    if observation.authority != "complete":
        return _AttentionTransition("no-op", state)

    first_missing_at = state.first_missing_at or observation.observed_at
    missing_count = state.missing_observation_count + 1
    elapsed = (
        datetime.fromisoformat(observation.observed_at)
        - datetime.fromisoformat(first_missing_at)
    ).total_seconds()
    resolves = (
        missing_count >= ATTENTION_MISSING_REQUIRED
        and elapsed >= ATTENTION_MISSING_GRACE_SECONDS
    )
    next_state = _AttentionLifecycleState(
        host_id=state.host_id,
        family_key=state.family_key,
        generation=state.generation,
        lifecycle_status=(
            ATTENTION_LIFECYCLE_RESOLVED
            if resolves
            else ATTENTION_LIFECYCLE_OPEN
        ),
        current_attention_id=None if resolves else state.current_attention_id,
        first_seen_at=state.first_seen_at,
        last_positive_at=state.last_positive_at,
        first_missing_at=first_missing_at,
        missing_observation_count=missing_count,
        last_accepted_at=observation.observed_at,
        last_observation_key=observation.observation_key,
        max_notified_severity_rank=state.max_notified_severity_rank,
    )
    return _AttentionTransition(
        "resolve" if resolves else (
            "start-missing" if state.first_missing_at is None else "advance-missing"
        ),
        next_state,
        resolve_attention_id=state.current_attention_id if resolves else None,
    )


def _enqueue_attention_lifecycle_job_conn(
    conn: sqlite3.Connection,
    *,
    state: _AttentionLifecycleState,
    event_type: str,
    stage: str,
    attention_payload: Mapping[str, Any],
    transition_at: str,
) -> None:
    transition_key = stable_fingerprint(
        {
            "domain": "tendwire.attention.transition.v1",
            "host_id": state.host_id,
            "family_key": state.family_key,
            "generation": state.generation,
            "event_type": event_type,
            "stage": stage,
        }
    )
    delivery_key = f"attention:{event_type}:{transition_key}"
    payload = sanitize_public_value(
        {
            "schema_version": 1,
            "event_type": event_type,
            "host_id": state.host_id,
            "attention": dict(attention_payload),
            "transition_at": transition_at,
        }
    )
    conn.execute(
        """
        INSERT INTO connector_outbox (
            host_id, connector, delivery_key, status, payload_json,
            private_state_json, created_at, updated_at, next_attempt_at
        ) VALUES (?, ?, ?, 'queued', ?, '{}', ?, ?, NULL)
        ON CONFLICT(host_id, connector, delivery_key) DO NOTHING
        """,
        (
            state.host_id,
            ATTENTION_OUTBOX_CONNECTOR,
            delivery_key,
            _canonical_json(payload),
            transition_at,
            transition_at,
        ),
    )


def _upsert_attention_projection_conn(
    conn: sqlite3.Connection,
    *,
    state: _AttentionLifecycleState,
    item: Mapping[str, Any],
    content_fingerprint: str,
    observed_at: str,
    prior_signal_count: int = 0,
) -> dict[str, Any]:
    item = sanitize_public_mapping(item)
    attention_id = _attention_id_from_item(item)
    source = _store_public_text(item.get("source"), default="unknown")
    kind = _store_public_label(item.get("kind"))
    severity = normalize_severity(item.get("severity"))
    signal_status = str(item.get("status") or "unknown")
    fingerprint = str(item.get("fingerprint") or "")
    existing = conn.execute(
        """
        SELECT fingerprint, lifecycle_status, signal_count, last_changed_at,
               severity, status
        FROM attention_items
        WHERE host_id = ? AND attention_id = ?
        """,
        (state.host_id, attention_id),
    ).fetchone()
    signal_count = max(
        max(0, int(prior_signal_count)),
        0 if existing is None else max(0, int(existing[2] or 0)),
    ) + 1
    changed = (
        existing is None
        or str(existing[0] or "") != fingerprint
        or str(existing[1] or "") != ATTENTION_LIFECYCLE_OPEN
        or normalize_severity(existing[4]) != severity
        or str(existing[5] or "") != signal_status
    )
    last_changed_at = (
        observed_at if changed else str(existing[3] or observed_at)
    )
    conn.execute(
        """
        INSERT INTO attention_items (
            host_id, attention_id, source, kind, severity, status, updated_at,
            fingerprint, snapshot_content_fingerprint, observed_at,
            first_seen_at, last_seen_at, last_changed_at, resolved_at,
            lifecycle_status, resolved_reason, signal_count, payload_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, 'open', NULL, ?, ?)
        ON CONFLICT(host_id, attention_id) DO UPDATE SET
            source = excluded.source,
            kind = excluded.kind,
            severity = excluded.severity,
            status = excluded.status,
            updated_at = excluded.updated_at,
            fingerprint = excluded.fingerprint,
            snapshot_content_fingerprint = excluded.snapshot_content_fingerprint,
            observed_at = excluded.observed_at,
            first_seen_at = excluded.first_seen_at,
            last_seen_at = excluded.last_seen_at,
            last_changed_at = excluded.last_changed_at,
            resolved_at = NULL,
            lifecycle_status = 'open',
            resolved_reason = NULL,
            signal_count = excluded.signal_count,
            payload_json = excluded.payload_json
        """,
        (
            state.host_id,
            attention_id,
            source,
            kind,
            severity,
            signal_status,
            item.get("updated_at"),
            fingerprint,
            str(content_fingerprint),
            observed_at,
            state.first_seen_at,
            observed_at,
            last_changed_at,
            signal_count,
            _canonical_json(dict(item)),
        ),
    )
    return _attention_lifecycle_payload(
        item,
        attention_id=attention_id,
        observed_at=observed_at,
        first_seen_at=state.first_seen_at,
        last_seen_at=observed_at,
        last_changed_at=last_changed_at,
        lifecycle_status=ATTENTION_LIFECYCLE_OPEN,
        signal_count=signal_count,
    )


def _attention_state_from_row(row: Any) -> _AttentionLifecycleState:
    return _AttentionLifecycleState(
        host_id=str(row[0]),
        family_key=str(row[1]),
        generation=int(row[2]),
        lifecycle_status=str(row[3]),
        current_attention_id=str(row[4]) if row[4] is not None else None,
        first_seen_at=str(row[5]),
        last_positive_at=str(row[6]),
        first_missing_at=str(row[7]) if row[7] is not None else None,
        missing_observation_count=int(row[8]),
        last_accepted_at=str(row[9]),
        last_observation_key=str(row[10]),
        max_notified_severity_rank=int(row[11]),
    )


def _apply_attention_observation_conn(
    conn: sqlite3.Connection,
    *,
    snapshot: Snapshot,
    payload_data: Mapping[str, Any],
    content_fingerprint: str,
    observation: SnapshotObservationContext,
) -> None:
    authority = (
        observation.authority
        if observation.authority in {"none", "positive", "complete"}
        else "none"
    )
    if authority == "none":
        return
    observed_at = _strict_utc_timestamp(observation.observed_at)
    if observed_at is None:
        return
    host_id = str(snapshot.host_id)
    observation_key = _attention_observation_key(
        host_id=host_id,
        authority=authority,
        observed_at=observed_at,
        content_fingerprint=content_fingerprint,
    )

    grouped: dict[str, list[Mapping[str, Any]]] = {}
    for raw_item in payload_data.get("attention", []):
        if not isinstance(raw_item, Mapping):
            continue
        item = sanitize_public_mapping(raw_item)
        family_key = _attention_family_key(host_id, item)
        grouped.setdefault(family_key, []).append(item)

    def candidate_rank(item: Mapping[str, Any]) -> tuple[int, str, str]:
        updated_at = _strict_utc_timestamp(item.get("updated_at")) or ""
        return (
            _attention_severity_rank(item.get("severity")),
            updated_at,
            "".join(chr(0x10FFFF - ord(ch)) for ch in _attention_id_from_item(item)),
        )

    selected = {
        family_key: max(items, key=candidate_rank)
        for family_key, items in grouped.items()
    }
    rows = conn.execute(
        """
        SELECT host_id, family_key, generation, lifecycle_status,
               current_attention_id, first_seen_at, last_positive_at,
               first_missing_at, missing_observation_count, last_accepted_at,
               last_observation_key, max_notified_severity_rank
        FROM attention_lifecycles
        WHERE host_id = ?
        """,
        (host_id,),
    ).fetchall()
    states = {
        state.family_key: state
        for state in (_attention_state_from_row(row) for row in rows)
    }
    family_keys = set(selected)
    if authority == "complete":
        family_keys.update(
            key
            for key, state in states.items()
            if state.lifecycle_status == ATTENTION_LIFECYCLE_OPEN
        )

    for family_key in sorted(family_keys):
        state = states.get(family_key)
        signal = selected.get(family_key)
        transition = _plan_attention_transition(
            state,
            _AttentionObservation(
                host_id=host_id,
                family_key=family_key,
                authority=authority,
                observed_at=observed_at,
                observation_key=observation_key,
                signal=signal,
            ),
        )
        next_state = transition.next_state
        if transition.action == "no-op" or next_state is None:
            continue
        if state is None:
            cursor = conn.execute(
                """
                INSERT OR IGNORE INTO attention_lifecycles (
                    host_id, family_key, generation, lifecycle_status,
                    current_attention_id, first_seen_at, last_positive_at,
                    first_missing_at, missing_observation_count, last_accepted_at,
                    last_observation_key, max_notified_severity_rank
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    next_state.host_id,
                    next_state.family_key,
                    next_state.generation,
                    next_state.lifecycle_status,
                    next_state.current_attention_id,
                    next_state.first_seen_at,
                    next_state.last_positive_at,
                    next_state.first_missing_at,
                    next_state.missing_observation_count,
                    next_state.last_accepted_at,
                    next_state.last_observation_key,
                    next_state.max_notified_severity_rank,
                ),
            )
        else:
            cursor = conn.execute(
                """
                UPDATE attention_lifecycles
                SET generation = ?, lifecycle_status = ?,
                    current_attention_id = ?, first_seen_at = ?,
                    last_positive_at = ?, first_missing_at = ?,
                    missing_observation_count = ?, last_accepted_at = ?,
                    last_observation_key = ?, max_notified_severity_rank = ?
                WHERE host_id = ? AND family_key = ? AND generation = ?
                  AND lifecycle_status = ? AND last_accepted_at < ?
                """,
                (
                    next_state.generation,
                    next_state.lifecycle_status,
                    next_state.current_attention_id,
                    next_state.first_seen_at,
                    next_state.last_positive_at,
                    next_state.first_missing_at,
                    next_state.missing_observation_count,
                    next_state.last_accepted_at,
                    next_state.last_observation_key,
                    next_state.max_notified_severity_rank,
                    state.host_id,
                    state.family_key,
                    state.generation,
                    state.lifecycle_status,
                    observed_at,
                ),
            )
        if int(cursor.rowcount or 0) != 1:
            continue

        prior_signal_count = 0
        if transition.superseded_attention_id is not None:
            prior_row = conn.execute(
                """
                SELECT signal_count FROM attention_items
                WHERE host_id = ? AND attention_id = ?
                """,
                (host_id, transition.superseded_attention_id),
            ).fetchone()
            prior_signal_count = int(prior_row[0] or 0) if prior_row else 0
        if transition.superseded_attention_id is not None:
            conn.execute(
                """
                UPDATE attention_items
                SET lifecycle_status = 'resolved', resolved_at = ?,
                    resolved_reason = ?, last_changed_at = ?
                WHERE host_id = ? AND attention_id = ? AND lifecycle_status = 'open'
                """,
                (
                    observed_at,
                    ATTENTION_RESOLVED_REASON_SUPERSEDED,
                    observed_at,
                    host_id,
                    transition.superseded_attention_id,
                ),
            )
        public_payload: dict[str, Any] | None = None
        if transition.upsert_signal is not None:
            public_payload = _upsert_attention_projection_conn(
                conn,
                state=next_state,
                item=transition.upsert_signal,
                content_fingerprint=content_fingerprint,
                observed_at=observed_at,
                prior_signal_count=prior_signal_count,
            )
        if transition.resolve_attention_id is not None:
            conn.execute(
                """
                UPDATE attention_items
                SET lifecycle_status = 'resolved', resolved_at = ?,
                    resolved_reason = ?, last_changed_at = ?,
                    snapshot_content_fingerprint = ?
                WHERE host_id = ? AND attention_id = ? AND lifecycle_status = 'open'
                """,
                (
                    observed_at,
                    ATTENTION_RESOLVED_REASON_GONE,
                    observed_at,
                    str(content_fingerprint),
                    host_id,
                    transition.resolve_attention_id,
                ),
            )
        if transition.delivery is not None and public_payload is not None:
            _enqueue_attention_lifecycle_job_conn(
                conn,
                state=next_state,
                event_type=transition.delivery[0],
                stage=transition.delivery[1],
                attention_payload=public_payload,
                transition_at=observed_at,
            )
        states[family_key] = next_state


def _append_snapshot_saved_event_conn(
    conn: sqlite3.Connection,
    snapshot: Snapshot,
    *,
    snapshot_id: int,
    content_fingerprint: str,
    private_snapshot_data: Mapping[str, Any],
) -> None:
    _append_event_conn(
        conn,
        host_id=str(snapshot.host_id),
        event_type="snapshot.saved",
        aggregate_type="snapshot",
        aggregate_id=str(content_fingerprint),
        observed_at=str(snapshot.updated_at),
        content_fingerprint=str(content_fingerprint),
        payload={
            "snapshot_id": int(snapshot_id),
            "content_fingerprint": str(content_fingerprint),
            "snapshot": dict(private_snapshot_data),
        },
    )


def _ensure_turn_list_host_state_conn(
    conn: sqlite3.Connection,
    host_id: str,
) -> None:
    conn.execute(
        """
        INSERT INTO turn_list_hosts (
            host_id,
            next_sequence,
            traversal_generation
        )
        SELECT ?, COALESCE(MAX(list_sequence), 0) + 1, 1
        FROM turns
        WHERE host_id = ?
        ON CONFLICT(host_id) DO NOTHING
        """,
        (str(host_id), str(host_id)),
    )


def _turn_list_host_state_conn(
    conn: sqlite3.Connection,
    host_id: str,
) -> tuple[int, int]:
    row = conn.execute(
        """
        SELECT next_sequence, traversal_generation
        FROM turn_list_hosts
        WHERE host_id = ?
        """,
        (str(host_id),),
    ).fetchone()
    if row is not None:
        return max(0, int(row[0]) - 1), int(row[1])
    row = conn.execute(
        "SELECT COALESCE(MAX(list_sequence), 0) FROM turns WHERE host_id = ?",
        (str(host_id),),
    ).fetchone()
    return int(row[0] if row is not None else 0), 1


def _turn_list_sequence_conn(
    conn: sqlite3.Connection,
    host_id: str,
    turn_id: str,
) -> int:
    """Return an existing immutable sequence or consume one durable host counter."""
    row = conn.execute(
        """
        SELECT list_sequence
        FROM turns
        WHERE host_id = ? AND turn_id = ?
        """,
        (str(host_id), str(turn_id)),
    ).fetchone()
    if row is not None:
        return int(row[0])
    _ensure_turn_list_host_state_conn(conn, host_id)
    row = conn.execute(
        "SELECT next_sequence FROM turn_list_hosts WHERE host_id = ?",
        (str(host_id),),
    ).fetchone()
    if row is None:
        raise StoreSchemaError("turn_list_host_state_unavailable")
    sequence = int(row[0])
    conn.execute(
        """
        UPDATE turn_list_hosts
        SET next_sequence = next_sequence + 1
        WHERE host_id = ?
        """,
        (str(host_id),),
    )
    return sequence


def _increment_turn_list_generation_conn(
    conn: sqlite3.Connection,
    host_id: str,
) -> None:
    _ensure_turn_list_host_state_conn(conn, host_id)
    conn.execute(
        """
        UPDATE turn_list_hosts
        SET traversal_generation = traversal_generation + 1
        WHERE host_id = ?
        """,
        (str(host_id),),
    )


def _refresh_snapshot_projections_conn(
    conn: sqlite3.Connection,
    snapshot: Snapshot,
    payload_data: Mapping[str, Any],
    *,
    content_fingerprint: str,
    turn_items: Iterable[Mapping[str, Any]],
    pending_items: Iterable[Mapping[str, Any]] | None = None,
) -> None:
    host_id = str(snapshot.host_id)
    observed_at = str(snapshot.updated_at)

    space_ids: set[str] = set()
    for item in payload_data.get("spaces", []):
        if not isinstance(item, Mapping):
            continue
        space_id = str(item.get("id") or "unknown")
        space_ids.add(space_id)
        conn.execute(
            """
            INSERT INTO spaces (
                host_id,
                space_id,
                name,
                status,
                updated_at,
                fingerprint,
                snapshot_content_fingerprint,
                observed_at,
                payload_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(host_id, space_id) DO UPDATE SET
                name = excluded.name,
                status = excluded.status,
                updated_at = excluded.updated_at,
                fingerprint = excluded.fingerprint,
                snapshot_content_fingerprint = excluded.snapshot_content_fingerprint,
                observed_at = excluded.observed_at,
                payload_json = excluded.payload_json
            """,
            (
                host_id,
                space_id,
                str(item.get("name") or space_id),
                str(item.get("status") or "unknown"),
                item.get("updated_at"),
                str(item.get("fingerprint") or ""),
                str(content_fingerprint),
                observed_at,
                _canonical_json(dict(item)),
            ),
        )
    _prune_host_projection(conn, "spaces", "space_id", host_id, space_ids)

    worker_ids: set[str] = set()
    for item in payload_data.get("workers", []):
        if not isinstance(item, Mapping):
            continue
        worker_id = str(item.get("id") or "unknown")
        worker_ids.add(worker_id)
        conn.execute(
            """
            INSERT INTO workers (
                host_id,
                worker_id,
                worker_fingerprint,
                space_id,
                name,
                status,
                last_seen_at,
                snapshot_content_fingerprint,
                observed_at,
                payload_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(host_id, worker_id) DO UPDATE SET
                worker_fingerprint = excluded.worker_fingerprint,
                space_id = excluded.space_id,
                name = excluded.name,
                status = excluded.status,
                last_seen_at = excluded.last_seen_at,
                snapshot_content_fingerprint = excluded.snapshot_content_fingerprint,
                observed_at = excluded.observed_at,
                payload_json = excluded.payload_json
            """,
            (
                host_id,
                worker_id,
                str(item.get("fingerprint") or ""),
                item.get("space_id"),
                str(item.get("name") or worker_id),
                str(item.get("status") or "unknown"),
                item.get("last_seen_at"),
                str(content_fingerprint),
                observed_at,
                _canonical_json(dict(item)),
            ),
        )
    _prune_host_projection(conn, "workers", "worker_id", host_id, worker_ids)


    turn_ids: set[str] = set()
    prepared_turn_items = [dict(item) for item in turn_items]
    for item in prepared_turn_items:
        turn_id = str(item.get("id") or "unknown")
        existing_turn = conn.execute(
            """
            SELECT payload_json, worker_fingerprint, updated_at
            FROM turns
            WHERE host_id = ? AND turn_id = ?
            """,
            (host_id, turn_id),
        ).fetchone()
        if (
            existing_turn is not None
            and str(existing_turn[1] or "")
            == str(item.get("worker_fingerprint") or "")
            and str(existing_turn[2] or "") == str(item.get("updated_at") or "")
        ):
            turn_ids.add(turn_id)
            continue
        if existing_turn is not None:
            existing_payload = _json_object(existing_turn[0])
            for provenance_key in ("source_turn_id",):
                if not str(item.get(provenance_key) or "").strip():
                    retained = existing_payload.get(provenance_key)
                    if str(retained or "").strip():
                        item[provenance_key] = retained
        turn_ids.add(turn_id)
        list_sequence = _turn_list_sequence_conn(conn, host_id, turn_id)
        conn.execute(
            """
            INSERT INTO turns (
                host_id,
                turn_id,
                worker_id,
                worker_fingerprint,
                space_id,
                status,
                kind,
                updated_at,
                fingerprint,
                snapshot_content_fingerprint,
                observed_at,
                payload_json,
                list_sequence
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(host_id, turn_id) DO UPDATE SET
                worker_id = excluded.worker_id,
                worker_fingerprint = excluded.worker_fingerprint,
                space_id = excluded.space_id,
                status = excluded.status,
                kind = excluded.kind,
                updated_at = excluded.updated_at,
                fingerprint = excluded.fingerprint,
                snapshot_content_fingerprint = excluded.snapshot_content_fingerprint,
                observed_at = excluded.observed_at,
                payload_json = excluded.payload_json
            """,
            (
                host_id,
                turn_id,
                str(item.get("worker_id") or ""),
                item.get("worker_fingerprint"),
                item.get("space_id"),
                str(item.get("status") or "unknown"),
                str(item.get("kind") or "unknown"),
                item.get("updated_at"),
                str(item.get("fingerprint") or ""),
                str(content_fingerprint),
                observed_at,
                _canonical_json(item),
                list_sequence,
            ),
        )
        _ensure_payload_turn_content_revision_conn(
            conn,
            host_id=str(host_id),
            turn_id=turn_id,
            payload=item,
            observed_at=str(observed_at) if observed_at else None,
        )
        current_revision = conn.execute(
            """
            SELECT content_revision
            FROM turn_content_revisions
            WHERE host_id = ? AND turn_id = ? AND is_current = 1
            """,
            (host_id, turn_id),
        ).fetchone()
        if current_revision is not None:
            _ensure_final_ready_anchor_conn(
                conn,
                host_id=host_id,
                turn_id=turn_id,
                content_revision_value=str(current_revision[0]),
                now=observed_at,
            )
    _prune_turn_projection(conn, host_id, turn_ids)

    pending_ids: set[str] = set()
    prepared_pending_items = (
        [dict(item) for item in pending_items]
        if pending_items is not None
        else [pending.to_dict() for pending in pending_from_snapshot(snapshot)]
    )
    for item in prepared_pending_items:
        pending_id = str(item.get("id") or "unknown")
        pending_ids.add(pending_id)
        conn.execute(
            """
            INSERT INTO pending_interactions (
                host_id,
                pending_id,
                worker_id,
                worker_fingerprint,
                space_id,
                kind,
                status,
                updated_at,
                fingerprint,
                snapshot_content_fingerprint,
                observed_at,
                payload_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(host_id, pending_id) DO UPDATE SET
                worker_id = excluded.worker_id,
                worker_fingerprint = excluded.worker_fingerprint,
                space_id = excluded.space_id,
                kind = excluded.kind,
                status = excluded.status,
                updated_at = excluded.updated_at,
                fingerprint = excluded.fingerprint,
                snapshot_content_fingerprint = excluded.snapshot_content_fingerprint,
                observed_at = excluded.observed_at,
                payload_json = excluded.payload_json
            """,
            (
                host_id,
                pending_id,
                str(item.get("worker_id") or ""),
                item.get("worker_fingerprint"),
                item.get("space_id"),
                str(item.get("kind") or "unknown"),
                str(item.get("status") or "unknown"),
                item.get("updated_at"),
                str(item.get("fingerprint") or ""),
                str(content_fingerprint),
                observed_at,
                _canonical_json(item),
            ),
        )
    _prune_host_projection(
        conn,
        "pending_interactions",
        "pending_id",
        host_id,
        pending_ids,
    )

    backend_names: set[str] = set()
    for item in payload_data.get("backend_health", []):
        if not isinstance(item, Mapping):
            continue
        backend_name = str(item.get("name") or "unknown")
        backend_names.add(backend_name)
        conn.execute(
            """
            INSERT INTO backend_health (
                host_id,
                backend_name,
                status,
                outcome,
                observed_at,
                snapshot_content_fingerprint,
                payload_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(host_id, backend_name) DO UPDATE SET
                status = excluded.status,
                outcome = excluded.outcome,
                observed_at = excluded.observed_at,
                snapshot_content_fingerprint = excluded.snapshot_content_fingerprint,
                payload_json = excluded.payload_json
            """,
            (
                host_id,
                backend_name,
                str(item.get("status") or "unknown"),
                str(item.get("outcome") or "unknown"),
                item.get("observed_at"),
                str(content_fingerprint),
                _canonical_json(dict(item)),
            ),
        )
    _prune_host_projection(
        conn,
        "backend_health",
        "backend_name",
        host_id,
        backend_names,
    )


def _upsert_command_audit(
    conn: sqlite3.Connection,
    *,
    host_id: str,
    request_id: str,
    action: str,
    payload_fingerprint: str,
    status: str,
    result_json: str,
    created_at: str | None = None,
    reserved_at: str | None = None,
    completed_at: str | None = None,
    uncertain: bool = False,
    dry_run: bool = False,
    request_json: str = "{}",
    updated_at: str | None = None,
) -> None:
    if not str(request_id):
        return
    now = utc_timestamp()
    created = created_at or now
    updated = updated_at or now
    conn.execute(
        """
        INSERT INTO commands (
            host_id,
            request_id,
            action,
            payload_fingerprint,
            status,
            dry_run,
            uncertain,
            request_json,
            result_json,
            created_at,
            reserved_at,
            completed_at,
            updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(host_id, request_id, action) DO UPDATE SET
            payload_fingerprint = excluded.payload_fingerprint,
            status = excluded.status,
            uncertain = excluded.uncertain,
            result_json = excluded.result_json,
            completed_at = excluded.completed_at,
            updated_at = excluded.updated_at
        """,
        (
            str(host_id),
            str(request_id),
            str(action),
            str(payload_fingerprint),
            str(status),
            int(dry_run),
            int(uncertain),
            str(request_json),
            str(result_json),
            created,
            reserved_at,
            completed_at,
            updated,
        ),
    )


def _command_audit_exists(
    conn: sqlite3.Connection,
    *,
    host_id: str,
    request_id: str,
    action: str,
) -> bool:
    row = conn.execute(
        """
        SELECT 1
        FROM commands
        WHERE host_id = ? AND request_id = ? AND action = ?
        LIMIT 1
        """,
        (str(host_id), str(request_id), str(action)),
    ).fetchone()
    return row is not None


def _upsert_command_audit_from_receipt_row(
    conn: sqlite3.Connection,
    row: Any,
) -> None:
    if _command_audit_exists(
        conn,
        host_id=str(row[0]),
        request_id=str(row[1]),
        action=str(row[2]),
    ):
        return
    created_at = str(row[6] or utc_timestamp())
    completed_at = row[7]
    _upsert_command_audit(
        conn,
        host_id=str(row[0]),
        request_id=str(row[1]),
        action=str(row[2]),
        payload_fingerprint=str(row[3]),
        status=str(row[4]),
        result_json=str(row[5]),
        created_at=created_at,
        reserved_at=created_at,
        completed_at=completed_at,
        uncertain=bool(row[8]),
        updated_at=str(completed_at or created_at),
    )




def _project_command_request_conn(conn: sqlite3.Connection, row: Any) -> None:
    """Replace the non-authoritative audit row from one authoritative receipt."""
    conn.execute(
        """
        INSERT INTO commands (
            host_id,
            request_id,
            action,
            canonical_version,
            canonical_fingerprint,
            public_worker_id,
            state,
            status,
            request_json,
            result_json,
            created_at,
            reserved_at,
            send_started_at,
            terminal_at,
            updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(host_id, request_id) DO UPDATE SET
            action = excluded.action,
            canonical_version = excluded.canonical_version,
            canonical_fingerprint = excluded.canonical_fingerprint,
            public_worker_id = excluded.public_worker_id,
            state = excluded.state,
            status = excluded.status,
            request_json = excluded.request_json,
            result_json = excluded.result_json,
            created_at = excluded.created_at,
            reserved_at = excluded.reserved_at,
            send_started_at = excluded.send_started_at,
            terminal_at = excluded.terminal_at,
            updated_at = excluded.updated_at
        """,
        (
            str(row[1]),
            str(row[2]),
            str(row[3]),
            int(row[4]),
            str(row[5]),
            str(row[7]),
            str(row[8]),
            str(row[9]),
            str(row[6]),
            str(row[10]),
            str(row[14]),
            str(row[15]),
            row[16],
            row[17],
            str(row[18]),
        ),
    )






def _connector_group_private_state(
    raw: Any,
    *,
    group: str,
    canonical: bool,
    terminal_after_lease: bool = False,
) -> str:
    state = _json_object(raw)
    state["migration_group"] = group
    state["migration_canonical"] = bool(canonical)
    if terminal_after_lease:
        state["terminal_after_lease"] = True
    else:
        state.pop("terminal_after_lease", None)
    return _canonical_json(state)


def _insert_turn_content_page_boundaries_conn(
    conn: sqlite3.Connection,
    *,
    host_id: str,
    turn_id: str,
    content_revision_value: str,
    field: str,
    segments: Iterable[Any],
) -> None:
    conn.executemany(
        """
        INSERT OR IGNORE INTO turn_content_page_boundaries (
            host_id, turn_id, content_revision, field,
            page_index, start_char, start_byte
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            (
                str(host_id),
                str(turn_id),
                str(content_revision_value),
                str(field),
                int(segment.index),
                int(segment.start_char),
                int(segment.start_byte),
            )
            for segment in segments
        ),
    )


def _insert_turn_content_revision_conn(
    conn: sqlite3.Connection,
    *,
    host_id: str,
    turn_id: str,
    user_text: str | None,
    assistant_final_text: str | None,
    user_state: str,
    final_state: str,
    created_at: str,
    is_current: bool = True,
) -> str:
    revision = content_revision(
        str(turn_id),
        user_text,
        assistant_final_text,
        user_state,
        final_state,
    )
    user_segments = segment_canonical_text(user_text or "") if user_state == "complete" else ()
    final_segments = (
        segment_canonical_text(assistant_final_text or "")
        if final_state == "complete"
        else ()
    )
    conn.execute(
        """
        INSERT INTO turn_content_revisions (
            host_id, turn_id, content_revision, user_text, assistant_final_text,
            user_state, final_state, user_char_length, user_byte_length,
            final_char_length, final_byte_length, user_page_count,
            final_page_count, is_current, created_at, superseded_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)
        ON CONFLICT(host_id, turn_id, content_revision) DO NOTHING
        """,
        (
            str(host_id),
            str(turn_id),
            revision,
            user_text,
            assistant_final_text,
            user_state,
            final_state,
            len(user_text or ""),
            len((user_text or "").encode("utf-8")),
            len(assistant_final_text or ""),
            len((assistant_final_text or "").encode("utf-8")),
            len(user_segments),
            len(final_segments),
            int(bool(is_current)),
            str(created_at),
        ),
    )
    _insert_turn_content_page_boundaries_conn(
        conn,
        host_id=str(host_id),
        turn_id=str(turn_id),
        content_revision_value=revision,
        field="user_text",
        segments=user_segments,
    )
    _insert_turn_content_page_boundaries_conn(
        conn,
        host_id=str(host_id),
        turn_id=str(turn_id),
        content_revision_value=revision,
        field="assistant_final_text",
        segments=final_segments,
    )
    return revision

def _ensure_payload_turn_content_revision_conn(
    conn: sqlite3.Connection,
    *,
    host_id: str,
    turn_id: str,
    payload: Mapping[str, Any],
    observed_at: str | None,
) -> bool:
    current = conn.execute(
        """
        SELECT 1
        FROM turn_content_revisions
        WHERE host_id = ? AND turn_id = ? AND is_current = 1
        LIMIT 1
        """,
        (str(host_id), str(turn_id)),
    ).fetchone()
    if current is not None:
        return False
    user_text = sanitize_canonical_turn_text(payload.get("user_text"))
    final_text = sanitize_canonical_turn_text(
        payload.get("assistant_final_text")
    )
    if user_text == "":
        user_text = None
    if final_text == "":
        final_text = None
    user_state = "complete" if user_text else "absent"
    final_state = "complete" if final_text else "absent"
    if user_state == "absent" and final_state == "absent":
        return _ensure_absent_turn_content_revision_conn(
            conn,
            host_id=str(host_id),
            turn_id=str(turn_id),
            observed_at=observed_at,
        )
    _insert_turn_content_revision_conn(
        conn,
        host_id=str(host_id),
        turn_id=str(turn_id),
        user_text=user_text,
        assistant_final_text=final_text,
        user_state=user_state,
        final_state=final_state,
        created_at=str(observed_at or "1970-01-01T00:00:00+00:00"),
    )
    return True

def _ensure_absent_turn_content_revision_conn(
    conn: sqlite3.Connection,
    *,
    host_id: str,
    turn_id: str,
    observed_at: str | None,
) -> bool:
    current = conn.execute(
        """
        SELECT 1
        FROM turn_content_revisions
        WHERE host_id = ? AND turn_id = ? AND is_current = 1
        LIMIT 1
        """,
        (str(host_id), str(turn_id)),
    ).fetchone()
    if current is not None:
        return False
    revision = _insert_turn_content_revision_conn(
        conn,
        host_id=str(host_id),
        turn_id=str(turn_id),
        user_text=None,
        assistant_final_text=None,
        user_state="absent",
        final_state="absent",
        created_at=str(observed_at or "1970-01-01T00:00:00+00:00"),
    )
    cursor = conn.execute(
        """
        UPDATE turn_content_revisions
        SET is_current = 1, superseded_at = NULL
        WHERE host_id = ?
          AND turn_id = ?
          AND content_revision = ?
          AND NOT EXISTS (
              SELECT 1
              FROM turn_content_revisions AS current_revision
              WHERE current_revision.host_id = ?
                AND current_revision.turn_id = ?
                AND current_revision.is_current = 1
          )
        """,
        (
            str(host_id),
            str(turn_id),
            revision,
            str(host_id),
            str(turn_id),
        ),
    )
    return bool(cursor.rowcount)

def _resolve_canonical_turn_id_conn(
    conn: sqlite3.Connection,
    host_id: str,
    turn_id: Any,
) -> str | None:
    """Resolve a superseded public turn alias without guessing through cycles."""
    current = str(turn_id or "").strip()
    if not current:
        return None
    seen: set[str] = set()
    while current and current not in seen:
        seen.add(current)
        row = conn.execute(
            """
            SELECT canonical_turn_id
            FROM turn_supersessions
            WHERE host_id = ? AND superseded_turn_id = ?
            """,
            (str(host_id), current),
        ).fetchone()
        if row is None:
            return current
        current = str(row[0] or "").strip()
    return None



def _create_current_schema_objects_conn(conn: sqlite3.Connection) -> None:
    table_statements = (
        CREATE_SNAPSHOTS_TABLE,
        CREATE_COMMAND_RECEIPTS_TABLE,
        CREATE_WORKER_BINDINGS_TABLE,
        *CREATE_CURRENT_PR6_TABLES,
        CREATE_BACKEND_PENDING_TABLE,
        CREATE_BACKEND_PENDING_CLAIMS_TABLE,
        CREATE_ATTENTION_LIFECYCLES_TABLE,
        CREATE_TURN_CONTENT_REVISIONS_TABLE,
        CREATE_TURN_CONTENT_PAGE_BOUNDARIES_TABLE,
        CREATE_TURN_PRESENTATION_PLANS_TABLE,
        CREATE_TURN_PRESENTATION_JOBS_TABLE,
        CREATE_TURN_PRESENTATION_RECOVERIES_TABLE,
        CREATE_STORE_MAINTENANCE_STATE_TABLE,
        CREATE_STORE_MAINTENANCE_CURSORS_TABLE,
        CREATE_TURN_LIST_STATE_TABLE,
        CREATE_TURN_LIST_HOSTS_TABLE,
        CREATE_TURN_CHANGE_JOURNAL_TABLE,
        CREATE_TURN_CHANGE_FLOOR_TABLE,
        CREATE_TURN_CHANGE_STATE_TABLE,
        CREATE_TURN_SUBMISSIONS_TABLE,
        CREATE_TURN_SUPERSESSIONS_TABLE,
        CREATE_AGENT_EVENTS_TABLE,
    )
    index_statements = (
        *CREATE_COMMAND_RECEIPT_INDEXES,
        *CREATE_WORKER_BINDING_INDEXES,
        CREATE_WORKER_BINDING_UNIQUE_INDEX,
        *CREATE_CURRENT_PR6_INDEXES,
        *CREATE_TURN_LIST_INDEXES,
        *CREATE_TURN_LIST_SEQUENCE_TRIGGERS,
        *CREATE_TURN_CHANGE_INDEXES,
        *CREATE_TURN_CHANGE_TRIGGERS,
        *CREATE_TURN_SUBMISSION_INDEXES,
        *CREATE_TURN_SUPERSESSION_INDEXES,
        *CREATE_AGENT_EVENT_INDEXES,
        *CREATE_ATTENTION_LIFECYCLE_INDEXES,
        *CREATE_TURN_CONTENT_REVISION_INDEXES,
        *CREATE_TURN_PRESENTATION_INDEXES,
        *CREATE_FINAL_DELIVERY_INDEXES,
        CREATE_CONNECTOR_ORDERING_INDEX,
        *CREATE_SNAPSHOT_INDEXES,
    )
    for statement in (*table_statements, *index_statements):
        conn.execute(statement)
    conn.execute(INSERT_STORE_MAINTENANCE_STATE)
    conn.execute(
        "INSERT INTO turn_list_state(scope, store_epoch) VALUES (?, ?)",
        ("turn-list", secrets.token_urlsafe(32)),
    )
    conn.execute(
        "INSERT INTO turn_change_state(scope, store_epoch) VALUES (?, ?)",
        ("turn-delta", secrets.token_urlsafe(32)),
    )
    conn.execute(f"PRAGMA user_version = {STORE_SCHEMA_VERSION}")


def _create_current_schema_conn(conn: sqlite3.Connection) -> None:
    """Create an empty database directly at the current schema."""
    if conn.in_transaction:
        raise StoreSchemaError("schema_rebuild_in_transaction")
    conn.execute("BEGIN IMMEDIATE")
    try:
        _create_current_schema_objects_conn(conn)
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def _application_schema_objects(
    conn: sqlite3.Connection,
) -> tuple[tuple[str, str], ...]:
    rows = conn.execute(
        """
        SELECT type, name
        FROM sqlite_master
        WHERE name NOT LIKE 'sqlite_%'
          AND type IN ('view', 'trigger', 'index', 'table')
        ORDER BY CASE type
            WHEN 'view' THEN 0
            WHEN 'trigger' THEN 1
            WHEN 'index' THEN 2
            ELSE 3
        END, name
        """
    ).fetchall()
    return tuple((str(row[0]), str(row[1])) for row in rows)


def _quote_sqlite_identifier(value: str) -> str:
    return '"' + str(value).replace('"', '""') + '"'


def _rebuild_current_schema_conn(
    conn: sqlite3.Connection,
    *,
    previous_version: int,
) -> None:
    if conn.in_transaction:
        raise StoreSchemaError("schema_rebuild_in_transaction")
    objects = _application_schema_objects(conn)
    _LOGGER.warning(
        "store schema mismatch; discarding and recreating database "
        "previous_version=%d target_version=%d discarded_objects=%s",
        int(previous_version),
        STORE_SCHEMA_VERSION,
        ",".join(f"{kind}:{name}" for kind, name in objects) or "none",
    )
    conn.execute("PRAGMA foreign_keys=OFF")
    try:
        conn.execute("BEGIN IMMEDIATE")
        try:
            for kind, name in objects:
                conn.execute(
                    f"DROP {kind.upper()} IF EXISTS "
                    f"{_quote_sqlite_identifier(name)}"
                )
            _create_current_schema_objects_conn(conn)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    finally:
        conn.execute("PRAGMA foreign_keys=ON")


def ensure_schema(conn: sqlite3.Connection) -> None:
    """Open v29 unchanged or explicitly replace any other schema."""
    version = int(conn.execute("PRAGMA user_version").fetchone()[0])
    if version == STORE_SCHEMA_VERSION:
        return
    if not isinstance(conn, _ClosingConnection):
        raise local_state_error(LocalStateErrorCode.OPERATION_FAILED)
    schema_authority = _schema_connection_authority(conn)
    authority = (
        nullcontext()
        if schema_authority.parent_fd is None
        else _filesystem_schema_mutation_authority(conn)
    )
    with authority:
        version = int(conn.execute("PRAGMA user_version").fetchone()[0])
        if version == STORE_SCHEMA_VERSION:
            return
        with private_file_creation_umask():
            _configure_persistent_database_conn(conn)
            objects = _application_schema_objects(conn)
            if version == 0 and not objects:
                _create_current_schema_conn(conn)
                return
            _rebuild_current_schema_conn(
                conn, previous_version=version
            )


_ensure_schema = ensure_schema


def _turn_list_store_epoch_conn(conn: sqlite3.Connection) -> str:
    row = conn.execute(
        "SELECT store_epoch FROM turn_list_state WHERE scope = 'turn-list'"
    ).fetchone()
    if row is None or not str(row[0]):
        raise StoreSchemaError("turn_list_state_unavailable")
    return str(row[0])


def _turn_change_store_epoch_conn(conn: sqlite3.Connection) -> str:
    row = conn.execute(
        "SELECT store_epoch FROM turn_change_state WHERE scope = 'turn-delta'"
    ).fetchone()
    if row is None or not str(row[0]):
        raise StoreSchemaError("turn_change_state_unavailable")
    return str(row[0])


def init_store(
    db_path: Path,
) -> None:
    """Initialize the sqlite store at the current schema."""
    with _connect(db_path, prepare=True) as conn:
        ensure_schema(conn)


def _agent_event_from_row(row: tuple[Any, ...]) -> StoredAgentEvent:
    private_payload = _json_object(row[15])
    public_payload = _json_object(row[16])
    kind = str(row[3])
    if kind not in AGENT_EVENT_KINDS:
        raise StoreSchemaError("invalid_agent_event_kind")
    try:
        normalized_host = normalize_agent_event_identifier(
            row[1], "host_id", required=True
        )
        stored_event = AgentEvent(
            event_id=str(row[2]),
            kind=kind,  # type: ignore[arg-type]
            source=str(row[4]),
            worker_id=str(row[5]),
            visibility=str(row[6]),  # type: ignore[arg-type]
            source_session_id=str(row[7]) if row[7] is not None else None,
            source_turn_id=str(row[8]) if row[8] is not None else None,
            source_item_id=str(row[9]) if row[9] is not None else None,
            source_message_id=str(row[10]) if row[10] is not None else None,
            source_event_id=str(row[11]) if row[11] is not None else None,
            source_sequence=int(row[12]) if row[12] is not None else None,
            observed_at=str(row[13]),
            payload_fingerprint=str(row[14]),
            payload=private_payload,
            public_payload=public_payload,
        )
        canonical = agent_event(
            kind=stored_event.kind,
            source=stored_event.source,
            worker_id=stored_event.worker_id,
            payload=stored_event.payload,
            source_session_id=stored_event.source_session_id,
            source_turn_id=stored_event.source_turn_id,
            source_item_id=stored_event.source_item_id,
            source_message_id=stored_event.source_message_id,
            source_event_id=stored_event.source_event_id,
            source_sequence=stored_event.source_sequence,
            visibility=stored_event.visibility,
            observed_at=stored_event.observed_at,
        )
    except (TypeError, ValueError, OverflowError) as exc:
        raise StoreSchemaError("invalid_agent_event_row") from exc
    if canonical != stored_event:
        raise StoreSchemaError("invalid_agent_event_row")
    return StoredAgentEvent(
        sequence=int(row[0]),
        host_id=normalized_host or "",
        event=stored_event,
    )


_AGENT_EVENT_SELECT = """
SELECT
    sequence, host_id, event_id, kind, source, worker_id, visibility,
    source_session_id, source_turn_id, source_item_id, source_message_id,
    source_event_id, source_sequence, observed_at, payload_fingerprint,
    private_payload_json, public_payload_json
FROM agent_events
"""


def _agent_event_conflicts(existing: StoredAgentEvent, incoming: AgentEvent) -> bool:
    stored = existing.event
    return (
        stored.kind != incoming.kind
        or stored.source != incoming.source
        or stored.worker_id != incoming.worker_id
        or stored.visibility != incoming.visibility
        or stored.source_session_id != incoming.source_session_id
        or stored.source_turn_id != incoming.source_turn_id
        or stored.source_item_id != incoming.source_item_id
        or stored.source_message_id != incoming.source_message_id
        or stored.source_event_id != incoming.source_event_id
        or stored.source_sequence != incoming.source_sequence
        or stored.payload_fingerprint != incoming.payload_fingerprint
        or stored.payload != incoming.payload
        or stored.public_payload != incoming.public_payload
    )






def _canonical_agent_event_for_append(event: AgentEvent) -> AgentEvent:
    if not isinstance(event, AgentEvent):
        raise ValueError("event must be an AgentEvent")
    canonical_event = agent_event(
        kind=event.kind,
        source=event.source,
        worker_id=event.worker_id,
        payload=event.payload,
        source_session_id=event.source_session_id,
        source_turn_id=event.source_turn_id,
        source_item_id=event.source_item_id,
        source_message_id=event.source_message_id,
        source_event_id=event.source_event_id,
        source_sequence=event.source_sequence,
        visibility=event.visibility,
        observed_at=event.observed_at,
    )
    if canonical_event != event:
        raise ValueError("event must use the canonical agent event contract")
    return canonical_event


def _append_agent_event_conn(
    conn: sqlite3.Connection,
    host_id: str,
    event: AgentEvent,
) -> AppendAgentEventResult:
    private_json = _canonical_json(event.payload)
    public_json = _canonical_json(event.public_payload)
    cursor = conn.execute(
        """
        INSERT INTO agent_events (
            host_id, event_id, kind, source, worker_id, visibility,
            source_session_id, source_turn_id, source_item_id,
            source_message_id, source_event_id, source_sequence,
            observed_at, payload_fingerprint, private_payload_json,
            public_payload_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(host_id, event_id) DO NOTHING
        """,
        (
            host_id,
            event.event_id,
            event.kind,
            event.source,
            event.worker_id,
            event.visibility,
            event.source_session_id,
            event.source_turn_id,
            event.source_item_id,
            event.source_message_id,
            event.source_event_id,
            event.source_sequence,
            event.observed_at,
            event.payload_fingerprint,
            private_json,
            public_json,
        ),
    )
    inserted = cursor.rowcount == 1
    row = conn.execute(
        _AGENT_EVENT_SELECT + " WHERE host_id = ? AND event_id = ?",
        (host_id, event.event_id),
    ).fetchone()
    if row is None:
        raise StoreSchemaError("agent_event_append_failed")
    stored = _agent_event_from_row(row)
    if _agent_event_conflicts(stored, event):
        raise AgentEventIdentityConflict(event.event_id)
    return AppendAgentEventResult(
        sequence=stored.sequence,
        event_id=event.event_id,
        inserted=inserted,
    )






def record_agent_event(
    db_path: Path | str,
    host_id: str,
    **event_fields: Any,
) -> AppendAgentEventResult:
    """Validate, normalize, and durably append one source event."""
    normalized_host = normalize_agent_event_identifier(
        host_id, "host_id", required=True
    )
    event = agent_event(**event_fields)
    _canonical_agent_event_for_append(event)
    with _connect(db_path, prepare=True) as conn:
        _ensure_schema(conn)
        conn.execute("BEGIN IMMEDIATE")
        try:
            result = _append_agent_event_conn(
                conn, normalized_host or "", event
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    return result


def list_agent_events(
    db_path: Path | str,
    host_id: str,
    *,
    worker_id: str | None = None,
    source: str | None = None,
    session_id: str | None = None,
    turn_id: str | None = None,
    visibility: Literal["private", "public"] | None = None,
    after_sequence: int = 0,
    limit: int = AGENT_EVENT_QUERY_DEFAULT_LIMIT,
) -> tuple[StoredAgentEvent, ...]:
    """List private structured events in append order with bounded work."""
    if (
        isinstance(after_sequence, bool)
        or not isinstance(after_sequence, int)
        or after_sequence < 0
        or after_sequence > (1 << 63) - 1
    ):
        raise ValueError("after_sequence must be a nonnegative integer")
    if (
        isinstance(limit, bool)
        or not isinstance(limit, int)
        or not 1 <= limit <= AGENT_EVENT_QUERY_MAX_LIMIT
    ):
        raise ValueError(
            f"limit must be between 1 and {AGENT_EVENT_QUERY_MAX_LIMIT}"
        )
    normalized_host = normalize_agent_event_identifier(
        host_id, "host_id", required=True
    )
    normalized_filters: list[tuple[str, str | None]] = []
    for column, value, field in (
        ("worker_id", worker_id, "worker_id"),
        ("source", source, "source"),
        ("source_session_id", session_id, "source_session_id"),
        ("source_turn_id", turn_id, "source_turn_id"),
    ):
        normalized_filters.append(
            (column, normalize_agent_event_identifier(value, field))
        )
    if visibility is not None and visibility not in {"private", "public"}:
        raise ValueError("visibility must be private, public, or None")
    if not _sqlite_store_exists(db_path):
        return ()
    clauses = ["host_id = ?", "sequence > ?"]
    parameters: list[Any] = [normalized_host, int(after_sequence)]
    for column, value in (*normalized_filters, ("visibility", visibility)):
        if value is not None:
            clauses.append(f"{column} = ?")
            parameters.append(value)
    parameters.append(int(limit))
    with _connect(db_path) as conn:
        _ensure_schema(conn)
        rows = conn.execute(
            _AGENT_EVENT_SELECT
            + " WHERE "
            + " AND ".join(clauses)
            + " ORDER BY sequence ASC LIMIT ?",
            parameters,
        ).fetchall()
    return tuple(_agent_event_from_row(row) for row in rows)




def _normalized_command_request_policy(
    retry_horizon_seconds: Any,
    retention_seconds: Any,
    retention_count: Any,
) -> tuple[int, int, int]:
    values = (retry_horizon_seconds, retention_seconds, retention_count)
    valid = all(
        isinstance(value, int) and not isinstance(value, bool) and value > 0
        for value in values
    )
    valid = (
        valid
        and int(retry_horizon_seconds) <= COMMAND_RETRY_HORIZON_SECONDS
        and int(retention_seconds) >= COMMAND_RECEIPT_RETENTION_MIN_SECONDS
        and int(retention_seconds) <= _MAX_TIMEDELTA_SECONDS
        and int(retention_seconds) > int(retry_horizon_seconds)
        and int(retention_count) <= _SQLITE_MAX_INTEGER
    )
    if not valid:
        return (
            COMMAND_RETRY_HORIZON_SECONDS,
            COMMAND_RECEIPT_RETENTION_SECONDS,
            COMMAND_RECEIPT_RETENTION_COUNT,
        )
    return (
        int(retry_horizon_seconds),
        int(retention_seconds),
        int(retention_count),
    )


def _command_request_status_empty(
    *,
    retry_horizon_seconds: int,
    retention_seconds: int,
    retention_count: int,
) -> dict[str, Any]:
    return {
        "total": 0,
        "states": {
            "reserved": 0,
            "send_started": 0,
            "accepted": 0,
            "rejected": 0,
            "uncertain": 0,
        },
        "stale_active": 0,
        "eligible": 0,
        "retry_horizon_seconds": int(retry_horizon_seconds),
        "retention_seconds": int(retention_seconds),
        "retention_count": int(retention_count),
        "storage_pressure": False,
    }


def _command_request_status_conn(
    conn: sqlite3.Connection,
    *,
    host_id: str,
    retry_horizon_seconds: int,
    retention_seconds: int,
    retention_count: int,
) -> dict[str, Any]:
    result = _command_request_status_empty(
        retry_horizon_seconds=retry_horizon_seconds,
        retention_seconds=retention_seconds,
        retention_count=retention_count,
    )
    states = dict(result["states"])
    for state, count in conn.execute(
        """
        SELECT state, COUNT(*)
        FROM command_receipts
        WHERE host_id = ?
        GROUP BY state
        """,
        (str(host_id),),
    ).fetchall():
        if str(state) in states:
            states[str(state)] = int(count or 0)
    current = datetime.now(timezone.utc)
    current_at = current.isoformat(timespec="seconds")
    retry_cutoff = (
        current - timedelta(seconds=int(retry_horizon_seconds))
    ).isoformat(timespec="seconds")
    retention_cutoff = (
        current - timedelta(seconds=int(retention_seconds))
    ).isoformat(timespec="seconds")
    stale_active = int(
        conn.execute(
            """
            SELECT COUNT(*)
            FROM command_receipts
            WHERE host_id = ?
              AND state = 'send_started'
              AND COALESCE(send_started_at, updated_at) < ?
            """,
            (str(host_id), retry_cutoff),
        ).fetchone()[0]
    )
    eligible = int(
        conn.execute(
            """
            WITH ranked AS (
                SELECT
                    COALESCE(terminal_at, updated_at) AS retention_at,
                    ROW_NUMBER() OVER (
                        ORDER BY COALESCE(terminal_at, updated_at) DESC, id DESC
                    ) AS retention_rank
                FROM command_receipts
                WHERE host_id = ?
                  AND (
                      state IN ('accepted', 'rejected', 'uncertain')
                      OR (
                          state = 'reserved'
                          AND owner_expires_at IS NOT NULL
                          AND owner_expires_at <= ?
                      )
                  )
            )
            SELECT COUNT(*)
            FROM ranked
            WHERE retention_at < ?
              AND retention_rank > ?
            """,
            (
                str(host_id),
                current_at,
                retention_cutoff,
                int(retention_count),
            ),
        ).fetchone()[0]
    )
    result.update(
        {
            "total": sum(states.values()),
            "states": states,
            "stale_active": stale_active,
            "eligible": eligible,
            "storage_pressure": bool(stale_active or eligible),
        }
    )
    return result


def store_status(
    db_path: Path,
    host_id: str,
    *,
    snapshot_retention_days: int = 14,
    snapshot_retention_count: int = 4096,
    acknowledged_final_retention_days: int = ACKNOWLEDGED_FINAL_RETENTION_DAYS,
    acknowledged_final_retention_count: int = ACKNOWLEDGED_FINAL_RETENTION_COUNT,
    command_retry_horizon_seconds: int = COMMAND_RETRY_HORIZON_SECONDS,
    command_receipt_retention_seconds: int = COMMAND_RECEIPT_RETENTION_SECONDS,
    command_receipt_retention_count: int = COMMAND_RECEIPT_RETENTION_COUNT,
    maintenance_batch_size: int = 100,
    maintenance_cadence_seconds: int = 3600,
    require_current_schema: bool = False,
) -> dict[str, Any]:
    """Return bounded public-safe host state and database maintenance aggregates."""
    if (
        isinstance(acknowledged_final_retention_days, bool)
        or not isinstance(acknowledged_final_retention_days, int)
        or acknowledged_final_retention_days <= 0
        or acknowledged_final_retention_days > _MAX_RETENTION_DAYS
    ):
        acknowledged_final_retention_days = ACKNOWLEDGED_FINAL_RETENTION_DAYS
    if (
        isinstance(acknowledged_final_retention_count, bool)
        or not isinstance(acknowledged_final_retention_count, int)
        or acknowledged_final_retention_count <= 0
        or acknowledged_final_retention_count > _SQLITE_MAX_INTEGER
    ):
        acknowledged_final_retention_count = ACKNOWLEDGED_FINAL_RETENTION_COUNT
    policy = SnapshotRetentionPolicy(
        retention_days=snapshot_retention_days,
        retention_count=snapshot_retention_count,
        batch_size=maintenance_batch_size,
    )
    (
        command_retry_horizon_seconds,
        command_receipt_retention_seconds,
        command_receipt_retention_count,
    ) = _normalized_command_request_policy(
        command_retry_horizon_seconds,
        command_receipt_retention_seconds,
        command_receipt_retention_count,
    )
    maintenance_empty = {
        "last_completed_at": None,
        "status": "not_initialized",
        "snapshot_count": 0,
        "snapshot_retention_days": policy.retention_days,
        "snapshot_retention_count": policy.retention_count,
        "maintenance_batch_size": policy.batch_size,
        "maintenance_cadence_seconds": int(maintenance_cadence_seconds),
        "backlog": False,
    }
    final_retention_empty = {
        "acknowledged": 0,
        "unresolved": 0,
        "queued": 0,
        "leased": 0,
        "deferred": 0,
        "retry": 0,
        "dead_letter": 0,
        "awaiting_ack": 0,
        "eligible": 0,
        "acknowledged_final_retention_days": int(
            acknowledged_final_retention_days
        ),
        "acknowledged_final_retention_count": int(
            acknowledged_final_retention_count
        ),
        "storage_pressure": False,
    }
    command_requests_empty = _command_request_status_empty(
        retry_horizon_seconds=command_retry_horizon_seconds,
        retention_seconds=command_receipt_retention_seconds,
        retention_count=command_receipt_retention_count,
    )
    def unavailable(status: str) -> dict[str, Any]:
        return dict(sanitize_public_value({
            "schema_version": 1,
            "ok": False,
            "status": status,
            "host_id": str(host_id),
            "counts": {},
            "outbox": {
                "pending": 0,
                "leased": 0,
                "completed": 0,
                "by_status": {},
                "due": 0,
                "oldest_due_at": None,
                "overdue_awaiting_ack": 0,
                "drain_target_seconds": _CONNECTOR_DRAIN_TARGET_SECONDS,
                "starved": False,
            },
            "final_retention": final_retention_empty,
            "command_requests": command_requests_empty,
            "maintenance": maintenance_empty,
        }))

    if not require_current_schema and not _sqlite_store_exists(db_path):
        return unavailable("store_unavailable")
    tables = (
        "snapshots",
        "events",
        "spaces",
        "workers",
        "turns",
        "pending_interactions",
        "attention_items",
        "commands",
        "command_receipts",
        "backend_health",
    )
    try:
        with _connect(db_path, read_only=require_current_schema) as conn:
            if require_current_schema:
                conn.execute("PRAGMA query_only=ON")
                version = int(conn.execute("PRAGMA user_version").fetchone()[0])
                if version != STORE_SCHEMA_VERSION:
                    return unavailable("schema_not_current")
            else:
                _ensure_schema(conn)
            counts = {
                table: int(
                    conn.execute(
                        f"SELECT COUNT(*) FROM {table} WHERE host_id = ?",
                        (str(host_id),),
                    ).fetchone()[0]
                )
                for table in tables
            }
            last_event_row = conn.execute(
                """
                SELECT observed_at
                FROM events
                WHERE host_id = ?
                ORDER BY observed_at DESC, id DESC
                LIMIT 1
                """,
                (str(host_id),),
            ).fetchone()
            last_snapshot_row = conn.execute(
                """
                SELECT created_at
                FROM snapshots
                WHERE host_id = ?
                ORDER BY id DESC
                LIMIT 1
                """,
                (str(host_id),),
            ).fetchone()
            outbox_rows = conn.execute(
                """
                SELECT status, COUNT(*)
                FROM connector_outbox
                WHERE host_id = ?
                GROUP BY status
                """,
                (str(host_id),),
            ).fetchall()
            drain_now = utc_timestamp()
            due_row = conn.execute(
                """
                SELECT
                    COUNT(*),
                    MIN(
                        CASE
                            WHEN next_attempt_at IS NOT NULL
                             AND next_attempt_at != ''
                             AND julianday(next_attempt_at) IS NOT NULL
                             AND julianday(next_attempt_at) > julianday(created_at)
                            THEN next_attempt_at
                            ELSE created_at
                        END
                    )
                FROM connector_outbox
                WHERE host_id = ?
                  AND status IN ('queued', 'deferred', 'retry')
                  AND (
                      next_attempt_at IS NULL
                      OR next_attempt_at = ''
                      OR julianday(next_attempt_at) IS NULL
                      OR julianday(next_attempt_at) <= julianday(?)
                  )
                """,
                (str(host_id), drain_now),
            ).fetchone()
            overdue_ack_row = conn.execute(
                """
                SELECT COUNT(*)
                FROM connector_outbox
                WHERE host_id = ?
                  AND status = 'awaiting_ack'
                  AND COALESCE(
                      CASE
                          WHEN json_valid(
                              connector_outbox.private_state_json
                          )
                          THEN julianday(
                              json_extract(
                                  connector_outbox.private_state_json,
                                  '$.ack_deadline_at'
                              )
                          )
                      END,
                      julianday(connector_outbox.next_attempt_at),
                      (
                          SELECT CASE
                              WHEN json_valid(
                                  deliveries.private_state_json
                              )
                              THEN julianday(
                                  json_extract(
                                      deliveries.private_state_json,
                                      '$.ack_deadline_at'
                                  )
                              )
                          END
                          FROM connector_deliveries AS deliveries
                          WHERE deliveries.outbox_id = connector_outbox.id
                            AND deliveries.status = 'awaiting_ack'
                          ORDER BY deliveries.id DESC
                          LIMIT 1
                      ),
                      -1
                  ) <= julianday(?)
                """,
                (str(host_id), drain_now),
            ).fetchone()
            maintenance_row = conn.execute(
                """
                SELECT last_completed_at, last_status
                FROM store_maintenance_state
                WHERE scope = 'automatic'
                """
            ).fetchone()
            snapshot_count = int(
                conn.execute(
                    "SELECT COUNT(*) FROM snapshots WHERE host_id = ?",
                    (str(host_id),),
                ).fetchone()[0]
            )
            backlog_ids, _ = _snapshot_retention_candidates_conn(
                conn,
                cutoff_at=_utc_cutoff(retention_days=policy.retention_days),
                retention_count=policy.retention_count,
                batch_size=1,
                host_id=str(host_id),
            )
            final_retention = {
                **_acknowledged_final_retention_metrics_conn(
                    conn,
                    host_id=str(host_id),
                    cutoff_at=_utc_cutoff(
                        retention_days=acknowledged_final_retention_days
                    ),
                    retention_count=acknowledged_final_retention_count,
                ),
                "acknowledged_final_retention_days": int(
                    acknowledged_final_retention_days
                ),
                "acknowledged_final_retention_count": int(
                    acknowledged_final_retention_count
                ),
            }
            command_requests = _command_request_status_conn(
                conn,
                host_id=str(host_id),
                retry_horizon_seconds=command_retry_horizon_seconds,
                retention_seconds=command_receipt_retention_seconds,
                retention_count=command_receipt_retention_count,
            )
    except (LocalStateError, StoreSchemaError, sqlite3.Error):
        if require_current_schema:
            return unavailable("store_unavailable")
        raise
    by_status: dict[str, int] = {}
    for row in outbox_rows:
        status = _store_public_label(row[0], allowed=_CONNECTOR_PUBLIC_OUTBOX_STATUSES)
        by_status[status] = by_status.get(status, 0) + int(row[1] or 0)
    pending_statuses = _CONNECTOR_POLLABLE_STATUSES
    terminal_statuses = {
        _CONNECTOR_TERMINAL_OUTBOX_STATUS,
        _CONNECTOR_SUPERSEDED_OUTBOX_STATUS,
    }
    outbox = {
        "pending": sum(
            count for status, count in by_status.items() if status in pending_statuses
        ),
        "leased": int(by_status.get(_CONNECTOR_LEASE_STATUS, 0)),
        "completed": sum(
            count for status, count in by_status.items() if status in terminal_statuses
        ),
        "by_status": by_status,
        "due": int(due_row[0] or 0),
        "oldest_due_at": _strict_utc_timestamp(due_row[1]),
        "overdue_awaiting_ack": int(overdue_ack_row[0] or 0),
        "drain_target_seconds": _CONNECTOR_DRAIN_TARGET_SECONDS,
        "starved": bool(
            due_row[1] is not None
            and _connector_datetime(str(due_row[1]))
            <= _connector_datetime(drain_now)
            - timedelta(seconds=_CONNECTOR_DRAIN_TARGET_SECONDS)
        ),
    }
    maintenance = {
        **maintenance_empty,
        "last_completed_at": (
            _strict_utc_timestamp(maintenance_row[0])
            if maintenance_row is not None
            else None
        ),
        "status": (
            _store_public_label(
                maintenance_row[1],
                allowed={"never", "ok", "failed"},
            )
            if maintenance_row is not None
            else "not_initialized"
        ),
        "snapshot_count": snapshot_count,
        "backlog": (
            bool(backlog_ids)
            or bool(final_retention["storage_pressure"])
            or bool(command_requests["storage_pressure"])
        ),
    }
    return sanitize_public_value({
        "schema_version": 1,
        "ok": True,
        "status": "ok",
        "host_id": str(host_id),
        "counts": counts,
        "outbox": outbox,
        "last_event_at": last_event_row[0] if last_event_row is not None else None,
        "last_snapshot_at": last_snapshot_row[0] if last_snapshot_row is not None else None,
        "final_retention": final_retention,
        "command_requests": command_requests,
        "maintenance": maintenance,
    })


def tail_event_metadata(
    db_path: Path,
    host_id: str,
    *,
    limit: int = 20,
) -> dict[str, Any]:
    """Return bounded event/history metadata without raw payloads."""
    row_limit = max(1, min(int(limit), 100))
    if not _sqlite_store_exists(db_path):
        return sanitize_public_value({
            "schema_version": 1,
            "ok": False,
            "status": "store_unavailable",
            "host_id": str(host_id),
            "limit": row_limit,
            "events": [],
        })
    with _connect(db_path) as conn:
        _ensure_schema(conn)
        rows = conn.execute(
            """
            SELECT id, event_type, aggregate_type, observed_at, content_fingerprint
            FROM events
            WHERE host_id = ?
            ORDER BY observed_at DESC, id DESC
            LIMIT ?
            """,
            (str(host_id), row_limit),
        ).fetchall()
    events = [
        {
            "row_id": int(row[0]),
            "event_type": _store_public_label(row[1]),
            "aggregate_type": _store_public_label(row[2]),
            "observed_at": str(row[3] or ""),
            "content_fingerprint": str(row[4] or ""),
        }
        for row in rows
    ]
    return sanitize_public_value({
        "schema_version": 1,
        "ok": True,
        "status": "ok",
        "host_id": str(host_id),
        "limit": row_limit,
        "events": events,
    })


_TURN_CONTENT_MAINTENANCE_BATCH = 100
_TURN_CONTENT_MAINTENANCE_BATCH_MAX = 1_000
_TURN_CONTENT_TERMINAL_PLAN_STATES = frozenset(
    {"completed", "superseded"}
)
_TURN_CONTENT_TERMINAL_OUTBOX_STATES = frozenset(
    {
        _CONNECTOR_TERMINAL_OUTBOX_STATUS,
        _CONNECTOR_SUPERSEDED_OUTBOX_STATUS,
    }
)
_TURN_CONTENT_TERMINAL_ATTEMPT_STATES = frozenset(
    {"delivered", "superseded", "failed", "deferred", "expired"}
)




_ACKNOWLEDGED_FINAL_ELIGIBILITY_CTE = """
WITH proven AS (
    SELECT
        source.id AS source_outbox_id,
        source.host_id,
        source.turn_id,
        source.content_revision,
        source.updated_at AS acknowledged_at,
        revisions.is_current,
        (
            NOT EXISTS (
                SELECT 1
                FROM connector_outbox AS unresolved_outbox
                WHERE unresolved_outbox.host_id = source.host_id
                  AND unresolved_outbox.connector = source.connector
                  AND unresolved_outbox.turn_id = source.turn_id
                  AND unresolved_outbox.status NOT IN (
                      'delivered',
                      'superseded'
                  )
            )
            AND NOT EXISTS (
                SELECT 1
                FROM turn_presentation_plans AS unresolved_plan
                WHERE unresolved_plan.host_id = source.host_id
                  AND unresolved_plan.name = source.connector
                  AND unresolved_plan.turn_id = source.turn_id
                  AND unresolved_plan.state IN (
                      'preparing',
                      'waiting_predecessor',
                      'active',
                      'failed'
                  )
            )
            AND NOT EXISTS (
                SELECT 1
                FROM connector_deliveries AS unsafe_attempt
                WHERE unsafe_attempt.host_id = source.host_id
                  AND unsafe_attempt.connector = source.connector
                  AND unsafe_attempt.delivery_key IN (
                      SELECT tracked_outbox.delivery_key
                      FROM connector_outbox AS tracked_outbox
                      WHERE tracked_outbox.host_id = source.host_id
                        AND tracked_outbox.connector = source.connector
                        AND tracked_outbox.turn_id = source.turn_id
                  )
                  AND unsafe_attempt.status NOT IN (
                      'delivered',
                      'superseded',
                      'failed',
                      'deferred',
                      'expired'
                  )
            )
        ) AS whole_turn_resolved
    FROM connector_outbox AS source
    JOIN turn_content_revisions AS revisions
      ON revisions.host_id = source.host_id
     AND revisions.turn_id = source.turn_id
     AND revisions.content_revision = source.content_revision
     AND revisions.final_state = 'complete'
    JOIN turns
      ON turns.host_id = source.host_id
     AND turns.turn_id = source.turn_id
    WHERE source.host_id = :host_id
      AND source.connector = 'turn-final'
      AND source.delivery_kind = 'final_ready'
      AND source.status = 'delivered'
      AND source.updated_at IS NOT NULL
      AND EXISTS (
          SELECT 1
          FROM turn_presentation_plans AS proof_plan
          WHERE proof_plan.source_outbox_id = source.id
            AND proof_plan.state IN ('completed', 'superseded')
            AND proof_plan.completed_at IS NOT NULL
            AND (
                SELECT COUNT(DISTINCT proof_job.part_ordinal)
                FROM turn_presentation_plans AS proof_lineage
                JOIN turn_presentation_jobs AS proof_job
                  ON proof_job.plan_id = proof_lineage.id
                WHERE proof_lineage.host_id = source.host_id
                  AND proof_lineage.name = source.connector
                  AND proof_lineage.turn_id = source.turn_id
                  AND proof_lineage.content_revision = source.content_revision
                  AND proof_lineage.presentation_version
                      = proof_plan.presentation_version
                  AND proof_lineage.generation <= proof_plan.generation
                  AND proof_lineage.state IN ('completed', 'superseded')
                  AND proof_job.operation = 'upsert'
            ) = proof_plan.part_count
            AND NOT EXISTS (
                SELECT 1
                FROM turn_presentation_plans AS proof_lineage
                JOIN turn_presentation_jobs AS proof_job
                  ON proof_job.plan_id = proof_lineage.id
                WHERE proof_lineage.host_id = source.host_id
                  AND proof_lineage.name = source.connector
                  AND proof_lineage.turn_id = source.turn_id
                  AND proof_lineage.content_revision = source.content_revision
                  AND proof_lineage.presentation_version
                      = proof_plan.presentation_version
                  AND proof_lineage.generation <= proof_plan.generation
                  AND proof_lineage.state IN ('completed', 'superseded')
                  AND proof_job.operation = 'upsert'
                  AND proof_job.part_ordinal >= proof_plan.part_count
            )
            AND NOT EXISTS (
                SELECT 1
                FROM turn_presentation_plans AS proof_lineage
                JOIN turn_presentation_jobs AS proof_job
                  ON proof_job.plan_id = proof_lineage.id
                LEFT JOIN connector_outbox AS proof_outbox
                  ON proof_outbox.id = proof_job.outbox_id
                WHERE proof_lineage.host_id = source.host_id
                  AND proof_lineage.name = source.connector
                  AND proof_lineage.turn_id = source.turn_id
                  AND proof_lineage.content_revision = source.content_revision
                  AND proof_lineage.presentation_version
                      = proof_plan.presentation_version
                  AND proof_lineage.generation <= proof_plan.generation
                  AND proof_lineage.state IN ('completed', 'superseded')
                  AND (
                      proof_outbox.id IS NULL
                      OR proof_outbox.host_id != source.host_id
                      OR proof_outbox.connector != source.connector
                      OR proof_outbox.turn_id != source.turn_id
                      OR proof_outbox.content_revision != source.content_revision
                      OR proof_outbox.delivery_kind != 'final_part'
                      OR proof_outbox.status != 'delivered'
                      OR NOT EXISTS (
                          SELECT 1
                          FROM connector_deliveries AS delivered_attempt
                          WHERE delivered_attempt.outbox_id = proof_outbox.id
                            AND delivered_attempt.host_id = proof_outbox.host_id
                            AND delivered_attempt.connector
                                = proof_outbox.connector
                            AND delivered_attempt.delivery_key
                                = proof_outbox.delivery_key
                            AND delivered_attempt.status = 'delivered'
                            AND delivered_attempt.delivered_at IS NOT NULL
                      )
                  )
            )
      )
),
ranked AS (
    SELECT
        proven.*,
        ROW_NUMBER() OVER (
            ORDER BY acknowledged_at DESC, source_outbox_id DESC
        ) AS acknowledged_rank,
        ROW_NUMBER() OVER (
            PARTITION BY host_id, turn_id, is_current
            ORDER BY acknowledged_at DESC, source_outbox_id DESC
        ) AS turn_revision_rank
    FROM proven
),
candidate_ranked AS (
    SELECT
        ranked.*,
        ROW_NUMBER() OVER (
            ORDER BY acknowledged_at DESC, source_outbox_id DESC
        ) AS retention_rank
    FROM ranked
    WHERE is_current = 1
      AND whole_turn_resolved = 1
      AND turn_revision_rank = 1
)
"""


def _acknowledged_final_retention_candidates_conn(
    conn: sqlite3.Connection,
    *,
    host_id: str,
    cutoff_at: str,
    retention_count: int,
    batch_size: int,
) -> list[tuple[int, str]]:
    rows = conn.execute(
        _ACKNOWLEDGED_FINAL_ELIGIBILITY_CTE
        + """
        SELECT source_outbox_id, turn_id
        FROM candidate_ranked
        WHERE acknowledged_at < :cutoff_at
           OR retention_rank > :retention_count
        ORDER BY acknowledged_at, source_outbox_id
        LIMIT :batch_size
        """,
        {
            "host_id": str(host_id),
            "cutoff_at": str(cutoff_at),
            "retention_count": int(retention_count),
            "batch_size": int(batch_size),
        },
    ).fetchall()
    return [(int(row[0]), str(row[1])) for row in rows]


def _acknowledged_final_retention_metrics_conn(
    conn: sqlite3.Connection,
    *,
    host_id: str,
    cutoff_at: str,
    retention_count: int,
) -> dict[str, Any]:
    status_rows = conn.execute(
        """
        SELECT status, COUNT(*), MIN(updated_at)
        FROM connector_outbox
        WHERE host_id = ?
          AND connector = 'turn-final'
          AND delivery_kind IN (
              'final_ready',
              'final_migration_hold',
              'final_part'
          )
        GROUP BY status
        """,
        (str(host_id),),
    ).fetchall()
    by_status = {
        str(row[0]): int(row[1] or 0)
        for row in status_rows
    }
    acknowledged = int(
        conn.execute(
            _ACKNOWLEDGED_FINAL_ELIGIBILITY_CTE
            + "SELECT COUNT(*) FROM proven",
            {"host_id": str(host_id)},
        ).fetchone()[0]
        or 0
    )
    unproven_delivered_row = conn.execute(
        _ACKNOWLEDGED_FINAL_ELIGIBILITY_CTE
        + """
        SELECT COUNT(*), MIN(anchor.updated_at)
        FROM connector_outbox AS anchor
        WHERE anchor.host_id = :host_id
          AND anchor.connector = 'turn-final'
          AND anchor.delivery_kind IN ('final_ready', 'final_migration_hold')
          AND anchor.status = 'delivered'
          AND NOT EXISTS (
              SELECT 1
              FROM proven
              WHERE proven.source_outbox_id = anchor.id
          )
        """,
        {"host_id": str(host_id)},
    ).fetchone()
    unproven_delivered = int(unproven_delivered_row[0] or 0)
    unresolved = unproven_delivered + sum(
        count
        for status, count in by_status.items()
        if status not in {"delivered", "superseded"}
    )
    unresolved_times = [
        str(row[2])
        for row in status_rows
        if str(row[0]) not in {"delivered", "superseded"}
        and row[2] is not None
    ]
    if unproven_delivered_row[1] is not None:
        unresolved_times.append(str(unproven_delivered_row[1]))
    oldest_unresolved = min(unresolved_times, default=None)
    eligible = int(
        conn.execute(
            _ACKNOWLEDGED_FINAL_ELIGIBILITY_CTE
            + """
            SELECT COUNT(*)
            FROM candidate_ranked
            WHERE acknowledged_at < :cutoff_at
               OR retention_rank > :retention_count
            """,
            {
                "host_id": str(host_id),
                "cutoff_at": str(cutoff_at),
                "retention_count": int(retention_count),
            },
        ).fetchone()[0]
        or 0
    )
    return {
        "acknowledged": acknowledged,
        "unresolved": unresolved,
        "queued": int(by_status.get("queued", 0)),
        "leased": int(by_status.get("leased", 0)),
        "deferred": int(by_status.get("deferred", 0)),
        "retry": int(by_status.get("retry", 0)),
        "dead_letter": int(by_status.get("dead_letter", 0)),
        "awaiting_ack": int(by_status.get("awaiting_ack", 0)),
        "eligible": eligible,
        "storage_pressure": bool(
            eligible > 0
            or unresolved > int(retention_count)
            or (
                oldest_unresolved is not None
                and oldest_unresolved < str(cutoff_at)
            )
        ),
    }


def _delete_acknowledged_final_turn_conn(
    conn: sqlite3.Connection,
    *,
    host_id: str,
    turn_id: str,
    tombstone_retention_count: int = ACKNOWLEDGED_FINAL_RETENTION_COUNT,
) -> dict[str, int]:
    graph_params = (str(host_id), str(turn_id))
    recoveries_deleted = int(
        conn.execute(
            """
            DELETE FROM turn_presentation_recoveries
            WHERE failed_plan_id IN (
                    SELECT id
                    FROM turn_presentation_plans
                    WHERE host_id = ? AND turn_id = ?
                )
               OR recovered_plan_id IN (
                    SELECT id
                    FROM turn_presentation_plans
                    WHERE host_id = ? AND turn_id = ?
                )
            """,
            (*graph_params, *graph_params),
        ).rowcount
        or 0
    )
    attempts_deleted = int(
        conn.execute(
            """
            DELETE FROM connector_deliveries AS attempt
            WHERE attempt.host_id = ?
              AND attempt.connector = 'turn-final'
              AND (
                  attempt.outbox_id IN (
                      SELECT id
                      FROM connector_outbox
                      WHERE host_id = ?
                        AND connector = 'turn-final'
                        AND turn_id = ?
                  )
                  OR (
                      attempt.outbox_id IS NULL
                      AND attempt.delivery_key IN (
                          SELECT delivery_key
                          FROM connector_outbox
                          WHERE host_id = ?
                            AND connector = 'turn-final'
                            AND turn_id = ?
                      )
                  )
              )
              AND NOT (
                  attempt.status = 'delivered'
                  AND attempt.delivered_at IS NOT NULL
                  AND attempt.delivery_key IN (
                      SELECT delivery_key
                      FROM connector_outbox
                      WHERE host_id = ?
                        AND connector = 'turn-final'
                        AND turn_id = ?
                        AND delivery_kind = 'final_ready'
                        AND status = 'delivered'
                  )
              )
            """,
            (
                str(host_id),
                str(host_id),
                str(turn_id),
                str(host_id),
                str(turn_id),
                str(host_id),
                str(turn_id),
            ),
        ).rowcount
        or 0
    )
    jobs_deleted = int(
        conn.execute(
            """
            DELETE FROM turn_presentation_jobs
            WHERE plan_id IN (
                SELECT id
                FROM turn_presentation_plans
                WHERE host_id = ? AND turn_id = ?
            )
            """,
            graph_params,
        ).rowcount
        or 0
    )
    plans_deleted = int(
        conn.execute(
            """
            DELETE FROM turn_presentation_plans
            WHERE host_id = ? AND turn_id = ?
            """,
            graph_params,
        ).rowcount
        or 0
    )
    anchors_deleted = int(
        conn.execute(
            """
            DELETE FROM connector_outbox
            WHERE host_id = ?
              AND connector = 'turn-final'
              AND turn_id = ?
            """,
            graph_params,
        ).rowcount
        or 0
    )
    attempts_deleted += int(
        conn.execute(
            f"""
            DELETE FROM connector_deliveries
            WHERE id IN (
                SELECT id
                FROM (
                    SELECT
                        id,
                        ROW_NUMBER() OVER (
                            PARTITION BY delivery_key
                            ORDER BY delivered_at DESC, id DESC
                        ) AS delivery_rank
                    FROM connector_deliveries
                    WHERE outbox_id IS NULL
                      AND host_id = ?
                      AND connector = 'turn-final'
                      AND delivery_key LIKE ?
                      AND status = 'delivered'
                      AND delivered_at IS NOT NULL
                )
                WHERE delivery_rank > 1
            )
            """,
            (str(host_id), f"{_TURN_FINAL_NAME}:revision:twfinal1.%"),
        ).rowcount
        or 0
    )
    attempts_deleted += int(
        conn.execute(
            """
            DELETE FROM connector_deliveries
            WHERE id IN (
                SELECT id
                FROM connector_deliveries
                WHERE outbox_id IS NULL
                  AND host_id = ?
                  AND connector = 'turn-final'
                  AND delivery_key LIKE ?
                  AND status = 'delivered'
                  AND delivered_at IS NOT NULL
                ORDER BY delivered_at DESC, id DESC
                LIMIT -1 OFFSET ?
            )
            """,
            (
                str(host_id),
                f"{_TURN_FINAL_NAME}:revision:twfinal1.%",
                max(1, int(tombstone_retention_count)),
            ),
        ).rowcount
        or 0
    )
    revisions_deleted = int(
        conn.execute(
            """
            DELETE FROM turn_content_revisions
            WHERE host_id = ? AND turn_id = ?
            """,
            graph_params,
        ).rowcount
        or 0
    )
    turns_deleted = int(
        conn.execute(
            "DELETE FROM turns WHERE host_id = ? AND turn_id = ?",
            graph_params,
        ).rowcount
        or 0
    )
    if turns_deleted:
        _increment_turn_list_generation_conn(conn, str(host_id))
    return {
        "recoveries": recoveries_deleted,
        "attempts": attempts_deleted,
        "jobs": jobs_deleted,
        "plans": plans_deleted,
        "anchors": anchors_deleted,
        "revisions": revisions_deleted,
        "turns": turns_deleted,
    }


def cleanup_acknowledged_final_retention(
    db_path: Path,
    host_id: str,
    *,
    acknowledged_final_retention_days: int = ACKNOWLEDGED_FINAL_RETENTION_DAYS,
    acknowledged_final_retention_count: int = ACKNOWLEDGED_FINAL_RETENTION_COUNT,
    now: str | None = None,
    dry_run: bool = False,
    batch_size: int = _TURN_CONTENT_MAINTENANCE_BATCH,
) -> dict[str, Any]:
    """Delete only bounded final-turn graphs with exact all-part ACK proof."""
    valid_policy = not any(
        isinstance(value, bool) or not isinstance(value, int) or value <= 0
        for value in (
            acknowledged_final_retention_days,
            acknowledged_final_retention_count,
            batch_size,
        )
    )
    valid_policy = (
        valid_policy
        and acknowledged_final_retention_days <= _MAX_RETENTION_DAYS
        and acknowledged_final_retention_count <= _SQLITE_MAX_INTEGER
        and batch_size <= _SQLITE_MAX_INTEGER
    )
    bounded_batch = max(
        1,
        min(
            int(batch_size) if valid_policy else _TURN_CONTENT_MAINTENANCE_BATCH,
            _TURN_CONTENT_MAINTENANCE_BATCH_MAX,
        ),
    )
    days = (
        int(acknowledged_final_retention_days)
        if valid_policy
        else ACKNOWLEDGED_FINAL_RETENTION_DAYS
    )
    count = (
        int(acknowledged_final_retention_count)
        if valid_policy
        else ACKNOWLEDGED_FINAL_RETENTION_COUNT
    )
    cutoff_at = _utc_cutoff(retention_days=days, now=now)
    empty_rows = {
        "recoveries": 0,
        "attempts": 0,
        "jobs": 0,
        "plans": 0,
        "anchors": 0,
        "revisions": 0,
        "turns": 0,
    }
    if not valid_policy:
        return sanitize_public_value(
            {
                "schema_version": 1,
                "ok": False,
                "status": "invalid_policy",
                "host_id": str(host_id),
                "dry_run": bool(dry_run),
                "acknowledged_final_retention_days": days,
                "acknowledged_final_retention_count": count,
                "cutoff_at": cutoff_at,
                "batch_size": bounded_batch,
                "examined": 0,
                "deleted": 0,
                "remaining_candidates": False,
                "deleted_rows": empty_rows,
            }
        )
    if not _sqlite_store_exists(db_path):
        return sanitize_public_value(
            {
                "schema_version": 1,
                "ok": False,
                "status": "store_unavailable",
                "host_id": str(host_id),
                "dry_run": bool(dry_run),
                "acknowledged_final_retention_days": days,
                "acknowledged_final_retention_count": count,
                "cutoff_at": cutoff_at,
                "batch_size": bounded_batch,
                "examined": 0,
                "deleted": 0,
                "remaining_candidates": False,
                "deleted_rows": empty_rows,
            }
        )
    deleted_rows = dict(empty_rows)
    with _connect(db_path, isolation_level=None) as conn:
        _ensure_schema(conn)
        conn.execute("BEGIN IMMEDIATE")
        try:
            scanned = _acknowledged_final_retention_candidates_conn(
                conn,
                host_id=str(host_id),
                cutoff_at=cutoff_at,
                retention_count=count,
                batch_size=bounded_batch + 1,
            )
            candidates = scanned[:bounded_batch]
            remaining = len(scanned) > bounded_batch
            if not dry_run:
                for _source_outbox_id, turn_id in candidates:
                    counts = _delete_acknowledged_final_turn_conn(
                        conn,
                        host_id=str(host_id),
                        turn_id=str(turn_id),
                        tombstone_retention_count=count,
                    )
                    for key, value in counts.items():
                        deleted_rows[key] += int(value)
                if not remaining:
                    remaining = bool(
                        _acknowledged_final_retention_candidates_conn(
                            conn,
                            host_id=str(host_id),
                            cutoff_at=cutoff_at,
                            retention_count=count,
                            batch_size=1,
                        )
                    )
                conn.commit()
            else:
                conn.rollback()
        except Exception:
            conn.rollback()
            raise
    return sanitize_public_value(
        {
            "schema_version": 1,
            "ok": True,
            "status": "ok",
            "host_id": str(host_id),
            "dry_run": bool(dry_run),
            "acknowledged_final_retention_days": days,
            "acknowledged_final_retention_count": count,
            "cutoff_at": cutoff_at,
            "batch_size": bounded_batch,
            "examined": len(candidates),
            "deleted": (
                len(candidates)
                if dry_run
                else int(deleted_rows["turns"])
            ),
            "remaining_candidates": bool(remaining),
            "deleted_rows": deleted_rows,
        }
    )


_SNAPSHOT_AGE_CANDIDATE_SQL = """
SELECT candidate.id
FROM snapshots AS candidate INDEXED BY idx_snapshots_created_host_id
WHERE candidate.created_at < :cutoff_at
  AND candidate.id <> (
      SELECT newest.id
      FROM snapshots AS newest INDEXED BY idx_snapshots_host_newest
      WHERE newest.host_id = candidate.host_id
      ORDER BY newest.id DESC
      LIMIT 1
  )
ORDER BY candidate.created_at, candidate.host_id, candidate.id
LIMIT :candidate_limit
"""

_SNAPSHOT_COUNT_CANDIDATE_SQL = """
WITH RECURSIVE
hosts(host_id) AS (
    SELECT MIN(first_host.host_id)
    FROM snapshots AS first_host INDEXED BY idx_snapshots_host_newest
    UNION ALL
    SELECT (
        SELECT MIN(next_host.host_id)
        FROM snapshots AS next_host INDEXED BY idx_snapshots_host_newest
        WHERE next_host.host_id > hosts.host_id
    )
    FROM hosts
    WHERE hosts.host_id IS NOT NULL
),
boundaries(host_id, boundary_id) AS MATERIALIZED (
    SELECT
        hosts.host_id,
        (
            SELECT boundary.id
            FROM snapshots AS boundary INDEXED BY idx_snapshots_host_newest
            WHERE boundary.host_id = hosts.host_id
            ORDER BY boundary.id DESC
            LIMIT 1 OFFSET :retention_offset
        )
    FROM hosts
    WHERE hosts.host_id IS NOT NULL
)
SELECT candidate.id
FROM boundaries
JOIN snapshots AS candidate INDEXED BY idx_snapshots_host_newest
  ON candidate.host_id = boundaries.host_id
 AND candidate.id < boundaries.boundary_id
WHERE boundaries.boundary_id IS NOT NULL
LIMIT :candidate_limit
"""


def _snapshot_retention_candidates_conn(
    conn: sqlite3.Connection,
    *,
    cutoff_at: str,
    retention_count: int,
    batch_size: int,
    host_id: str | None = None,
) -> tuple[list[int], bool]:
    candidate_limit = int(batch_size) + 1
    if host_id is None:
        age_sql = _SNAPSHOT_AGE_CANDIDATE_SQL
        age_params = {
            "cutoff_at": str(cutoff_at),
            "candidate_limit": candidate_limit,
        }
        count_sql = _SNAPSHOT_COUNT_CANDIDATE_SQL
        count_params = {
            "retention_offset": int(retention_count) - 1,
            "candidate_limit": candidate_limit,
        }
    else:
        age_sql = """
            SELECT candidate.id
            FROM snapshots AS candidate
            WHERE candidate.host_id = :host_id
              AND candidate.created_at < :cutoff_at
              AND candidate.id <> (
                  SELECT newest.id
                  FROM snapshots AS newest
                  WHERE newest.host_id = :host_id
                  ORDER BY newest.id DESC
                  LIMIT 1
              )
            ORDER BY candidate.created_at, candidate.id
            LIMIT :candidate_limit
        """
        age_params = {
            "host_id": str(host_id),
            "cutoff_at": str(cutoff_at),
            "candidate_limit": candidate_limit,
        }
        count_sql = """
            SELECT candidate.id
            FROM snapshots AS candidate
            WHERE candidate.host_id = :host_id
              AND candidate.id < (
                  SELECT boundary.id
                  FROM snapshots AS boundary
                  WHERE boundary.host_id = :host_id
                  ORDER BY boundary.id DESC
                  LIMIT 1 OFFSET :retention_offset
              )
            ORDER BY candidate.id
            LIMIT :candidate_limit
        """
        count_params = {
            "host_id": str(host_id),
            "retention_offset": int(retention_count) - 1,
            "candidate_limit": candidate_limit,
        }
    age_ids = [
        int(row[0])
        for row in conn.execute(age_sql, age_params).fetchall()
    ]
    if len(age_ids) == candidate_limit:
        return sorted(set(age_ids))[: int(batch_size)], True
    count_ids = [
        int(row[0])
        for row in conn.execute(count_sql, count_params).fetchall()
    ]
    merged = sorted(set(age_ids).union(count_ids))
    candidates = merged[: int(batch_size)]
    saturated = (
        len(merged) > int(batch_size)
        or len(count_ids) == candidate_limit
    )
    return candidates, saturated


def _delete_snapshot_candidates_conn(
    conn: sqlite3.Connection,
    candidate_ids: Iterable[int],
) -> int:
    ids = tuple(int(candidate_id) for candidate_id in candidate_ids)
    if not ids:
        return 0
    placeholders = ",".join("?" for _ in ids)
    return int(
        conn.execute(
            f"DELETE FROM snapshots WHERE id IN ({placeholders})",
            ids,
        ).rowcount
        or 0
    )


def _snapshot_retention_result(
    *,
    ok: bool,
    status: str,
    dry_run: bool,
    policy: SnapshotRetentionPolicy,
    cutoff_at: str,
    examined: int,
    deleted: int,
    eligible: int,
    remaining_candidates: bool,
    latest_hosts_retained: int,
) -> dict[str, Any]:
    return dict(sanitize_public_value({
        "schema_version": 1,
        "ok": bool(ok),
        "status": str(status),
        "scope": "database",
        "dry_run": bool(dry_run),
        "retention_days": policy.retention_days,
        "retention_count": policy.retention_count,
        "cutoff_at": str(cutoff_at),
        "batch_size": policy.batch_size,
        "examined": int(examined),
        "deleted": int(deleted),
        "eligible": int(eligible),
        "remaining_candidates": bool(remaining_candidates),
        "latest_hosts_retained": int(latest_hosts_retained),
    }))


def cleanup_snapshot_retention(
    db_path: Path,
    *,
    retention_days: int,
    retention_count: int,
    batch_size: int = 100,
    now: str | None = None,
    dry_run: bool = False,
    _store_lock_held: bool = False,
    _expected_db_identity: EntryIdentity | None = None,
) -> dict[str, Any]:
    """Apply one bounded database-wide snapshot retention batch."""
    try:
        policy = SnapshotRetentionPolicy(
            retention_days=retention_days,
            retention_count=retention_count,
            batch_size=batch_size,
        )
    except ValueError:
        fallback = SnapshotRetentionPolicy()
        return _snapshot_retention_result(
            ok=False,
            status="invalid_policy",
            dry_run=bool(dry_run),
            policy=fallback,
            cutoff_at=_utc_cutoff(retention_days=fallback.retention_days, now=now),
            examined=0,
            deleted=0,
            eligible=0,
            remaining_candidates=False,
            latest_hosts_retained=0,
        )
    cutoff_at = _utc_cutoff(retention_days=policy.retention_days, now=now)
    if not _sqlite_store_exists(db_path):
        return _snapshot_retention_result(
            ok=False,
            status="store_unavailable",
            dry_run=bool(dry_run),
            policy=policy,
            cutoff_at=cutoff_at,
            examined=0,
            deleted=0,
            eligible=0,
            remaining_candidates=False,
            latest_hosts_retained=0,
        )
    with _connect(
        db_path,
        isolation_level=None,
        _store_lock_held=_store_lock_held,
        _expected_db_identity=_expected_db_identity,
    ) as conn:
        _ensure_schema(conn)
        conn.execute("BEGIN IMMEDIATE")
        try:
            candidates, saturated = _snapshot_retention_candidates_conn(
                conn,
                cutoff_at=cutoff_at,
                retention_count=policy.retention_count,
                batch_size=policy.batch_size,
            )
            deleted = (
                0
                if dry_run
                else _delete_snapshot_candidates_conn(conn, candidates)
            )
            if dry_run:
                remaining = saturated
                conn.rollback()
            else:
                remaining_ids, _ = _snapshot_retention_candidates_conn(
                    conn,
                    cutoff_at=cutoff_at,
                    retention_count=policy.retention_count,
                    batch_size=1,
                )
                remaining = bool(remaining_ids)
                conn.commit()
            latest_hosts_retained = int(
                conn.execute(
                    "SELECT COUNT(DISTINCT host_id) FROM snapshots"
                ).fetchone()[0]
            )
        except Exception:
            conn.rollback()
            raise
    return _snapshot_retention_result(
        ok=True,
        status="ok",
        dry_run=bool(dry_run),
        policy=policy,
        cutoff_at=cutoff_at,
        examined=len(candidates),
        deleted=deleted,
        eligible=len(candidates),
        remaining_candidates=remaining,
        latest_hosts_retained=latest_hosts_retained,
    )


_COMPACTION_STATUSES = frozenset(
    {
        "dry_run",
        "completed",
        "invalid_request",
        "store_unavailable",
        "schema_not_current",
        "permissions_failed",
        "offline_required",
        "integrity_failed",
        "insufficient_space",
        "backup_failed",
        "maintenance_failed",
        "checkpoint_failed",
        "replacement_failed",
        "rollback_completed",
        "rollback_failed",
    }
)


class _CompactionAbort(RuntimeError):
    def __init__(self, status: str) -> None:
        if status not in _COMPACTION_STATUSES:
            status = "replacement_failed"
        self.status = status
        super().__init__(status)


def _compaction_report(options: CompactionOptions) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "ok": False,
        "status": "invalid_request",
        "command": "store.compact",
        "scope": "database",
        "dry_run": bool(options.dry_run),
        "maintenance_window_acknowledged": bool(options.acknowledge_offline),
        "permissions": {"ok": False, "outcome": "unsafe"},
        "integrity": {
            "before": "not_run",
            "backup": "not_run",
            "replacement": "not_run",
            "after": "not_run",
        },
        "space": {
            "available_bytes": 0,
            "required_bytes": 0,
            "headroom_ok": False,
        },
        "snapshots": {
            "before": 0,
            "retained": 0,
            "eligible": 0,
            "examined": 0,
            "deleted": 0,
            "remaining": 0,
            "latest_hosts_retained": 0,
        },
        "storage": {
            "before_bytes": 0,
            "estimated_reclaimable_bytes": 0,
            "after_bytes": None,
        },
        "backup": {
            "required": True,
            "created": False,
            "verified": False,
        },
        "checkpoint": {"status": "not_run"},
        "replacement": {"status": "not_run"},
        "rollback": {"status": "not_needed"},
    }


def _public_compaction_report(
    report: dict[str, Any], *, status: str, ok: bool = False
) -> dict[str, Any]:
    report["status"] = status if status in _COMPACTION_STATUSES else "replacement_failed"
    report["ok"] = bool(ok)
    public = dict(sanitize_public_value(report))
    public["command"] = "store.compact"
    return public


def _call_compaction_phase(
    phase_hook: Callable[[str], None] | None,
    phase: str,
    *,
    failure_status: str,
) -> None:
    if phase_hook is None:
        return
    try:
        phase_hook(phase)
    except Exception:
        raise _CompactionAbort(failure_status) from None


def _readonly_sqlite_at(
    parent_fd: int,
    leaf: str,
    *,
    immutable: bool = False,
    expected_identity: EntryIdentity | None = None,
) -> sqlite3.Connection:
    if expected_identity is not None:
        _verify_expected_identity_at(parent_fd, leaf, expected_identity)
    canonical_path = canonical_path_from_fd(parent_fd, leaf)
    immutable_query = "&immutable=1" if immutable else ""
    target = f"file:{quote(canonical_path, safe='/')}?mode=ro{immutable_query}"
    conn = sqlite3.connect(target, timeout=0.0, isolation_level=None, uri=True)
    try:
        if expected_identity is not None:
            _verify_expected_identity_at(parent_fd, leaf, expected_identity)
        conn.execute("PRAGMA busy_timeout=0")
        conn.execute("PRAGMA query_only=ON")
        conn.execute("PRAGMA foreign_keys=ON")
        return conn
    except Exception:
        conn.close()
        raise


def _writable_sqlite_at(
    parent_fd: int,
    leaf: str,
    *,
    expected_identity: EntryIdentity | None = None,
) -> sqlite3.Connection:
    if expected_identity is not None:
        _verify_expected_identity_at(parent_fd, leaf, expected_identity)
    canonical_path = canonical_path_from_fd(parent_fd, leaf)
    target = f"file:{quote(canonical_path, safe='/')}?mode=rw"
    with private_file_creation_umask():
        conn = sqlite3.connect(target, timeout=0.0, isolation_level=None, uri=True)
    try:
        if expected_identity is not None:
            _verify_expected_identity_at(parent_fd, leaf, expected_identity)
        conn.execute("PRAGMA busy_timeout=0")
        conn.execute("PRAGMA foreign_keys=ON")
        return conn
    except Exception:
        conn.close()
        raise


def _quick_check_ok(conn: sqlite3.Connection) -> bool:
    try:
        rows = conn.execute("PRAGMA quick_check").fetchall()
    except sqlite3.Error:
        return False
    return len(rows) == 1 and str(rows[0][0]).lower() == "ok"


def _foreign_key_check_ok(conn: sqlite3.Connection) -> bool:
    try:
        return conn.execute("PRAGMA foreign_key_check").fetchone() is None
    except sqlite3.Error:
        return False


def _compaction_snapshot_metrics(
    conn: sqlite3.Connection,
    *,
    cutoff_at: str,
    retention_count: int,
) -> tuple[int, int, int, int]:
    row = conn.execute(
        """
        WITH ranked AS (
            SELECT
                host_id,
                created_at,
                payload,
                content_fingerprint,
                ROW_NUMBER() OVER (
                    PARTITION BY host_id
                    ORDER BY id DESC
                ) AS newest_rank
            FROM snapshots
        )
        SELECT
            COUNT(*),
            COALESCE(SUM(
                CASE
                    WHEN newest_rank > 1
                     AND (created_at < ? OR newest_rank > ?)
                    THEN 1
                    ELSE 0
                END
            ), 0),
            COUNT(DISTINCT host_id),
            COALESCE(SUM(
                CASE
                    WHEN newest_rank > 1
                     AND (created_at < ? OR newest_rank > ?)
                    THEN LENGTH(payload)
                       + LENGTH(host_id)
                       + LENGTH(created_at)
                       + LENGTH(content_fingerprint)
                       + 64
                    ELSE 0
                END
            ), 0)
        FROM ranked
        """,
        (
            str(cutoff_at),
            int(retention_count),
            str(cutoff_at),
            int(retention_count),
        ),
    ).fetchone()
    if row is None:
        raise _CompactionAbort("store_unavailable")
    return int(row[0]), int(row[1]), int(row[2]), int(row[3])


def _compaction_page_metrics(conn: sqlite3.Connection) -> tuple[int, int]:
    page_size = int(conn.execute("PRAGMA page_size").fetchone()[0])
    page_count = int(conn.execute("PRAGMA page_count").fetchone()[0])
    freelist_count = int(conn.execute("PRAGMA freelist_count").fetchone()[0])
    live_bytes = max(page_size, max(0, page_count - freelist_count) * page_size)
    reclaimable = max(0, freelist_count * page_size)
    return live_bytes, reclaimable


def _compaction_family_metrics(
    members: tuple[_SQLiteFamilyMemberSnapshot, ...],
) -> tuple[int, int, int, EntryIdentity]:
    if len(members) != len(_SQLITE_FAMILY_SUFFIXES):
        raise _CompactionAbort("permissions_failed")
    main = members[0]
    if main.state is PermissionState.ABSENT:
        raise _CompactionAbort("store_unavailable")
    if any(
        member.state is PermissionState.REPAIR_REQUIRED
        for member in members
    ):
        raise _CompactionAbort("permissions_failed")

    identities: set[EntryIdentity] = set()
    family_bytes = 0
    for member in members:
        if member.state is PermissionState.ABSENT:
            if any(
                value is not None
                for value in (
                    member.mode,
                    member.identity,
                    member.size,
                    member.link_count,
                )
            ):
                raise _CompactionAbort("permissions_failed")
            continue
        if (
            member.mode is None
            or member.identity is None
            or member.size is None
            or member.link_count is None
            or member.link_count != 1
            or member.size < 0
        ):
            raise _CompactionAbort("permissions_failed")
        if member.identity in identities:
            raise _CompactionAbort("permissions_failed")
        identities.add(member.identity)
        family_bytes += member.size

    if (
        main.mode is None
        or main.identity is None
        or main.size is None
    ):
        raise _CompactionAbort("store_unavailable")
    return family_bytes, main.size, main.mode, main.identity


def _sqlite_family_bytes_and_identity(
    parent_fd: int,
    leaf: str,
) -> tuple[int, int, int, EntryIdentity]:
    members = _snapshot_sqlite_family_at(
        parent_fd,
        leaf,
        require_main=False,
    )
    return _compaction_family_metrics(members)


def _compaction_family_permissions_ok(
    members: tuple[_SQLiteFamilyMemberSnapshot, ...],
    *,
    expected_main: EntryIdentity,
) -> bool:
    if len(members) != len(_SQLITE_FAMILY_SUFFIXES):
        return False
    main = members[0]
    if (
        main.state is not PermissionState.PRIVATE
        or main.identity != expected_main
    ):
        return False
    return all(
        member.state in {PermissionState.PRIVATE, PermissionState.ABSENT}
        for member in members
    )


def _create_verified_compaction_backup(
    source_parent_fd: int,
    source_leaf: str,
    backup_parent_fd: int,
    backup_leaf: str,
    *,
    retained_mode: int,
    source_identity: EntryIdentity,
) -> EntryIdentity:
    if inspect_private_file_at(backup_parent_fd, backup_leaf).state is not PermissionState.ABSENT:
        raise _CompactionAbort("invalid_request")
    backup_fd = create_private_file_at(backup_parent_fd, backup_leaf)
    try:
        created = os.fstat(backup_fd)
        validate_owned_regular_stat(created)
        backup_identity = entry_identity(created)
        if backup_identity == source_identity:
            raise _CompactionAbort("invalid_request")
        source_conn = _readonly_sqlite_at(
            source_parent_fd,
            source_leaf,
            expected_identity=source_identity,
        )
        destination = _writable_sqlite_at(
            backup_parent_fd,
            backup_leaf,
            expected_identity=backup_identity,
        )
        try:
            source_conn.backup(destination)
            mode_row = destination.execute("PRAGMA journal_mode=DELETE").fetchone()
            if mode_row is None or str(mode_row[0]).lower() != "delete":
                raise _CompactionAbort("backup_failed")
        finally:
            destination.close()
            source_conn.close()
        current = verify_entry_identity(
            backup_parent_fd,
            backup_leaf,
            backup_identity,
            expected_type=EntryType.REGULAR_FILE,
        )
        if int(current.st_nlink) != 1 or identity_matches(source_identity, current):
            raise _CompactionAbort("backup_failed")
        os.fchmod(backup_fd, retained_mode)
        os.fsync(backup_fd)
        current = os.fstat(backup_fd)
        if stat.S_IMODE(current.st_mode) != retained_mode:
            raise _CompactionAbort("backup_failed")
    except _CompactionAbort:
        raise
    except Exception:
        raise _CompactionAbort("backup_failed") from None
    finally:
        os.close(backup_fd)
    backup_conn = _readonly_sqlite_at(
        backup_parent_fd,
        backup_leaf,
        expected_identity=backup_identity,
    )
    try:
        if not _quick_check_ok(backup_conn):
            raise _CompactionAbort("backup_failed")
    finally:
        backup_conn.close()
    return backup_identity


def _checkpoint_truncate(
    db_path: Path,
    *,
    expected_identity: EntryIdentity,
    store_lock_held: bool = False,
) -> bool:
    try:
        with _connect(
            db_path,
            isolation_level=None,
            _store_lock_held=store_lock_held,
            _expected_db_identity=expected_identity,
        ) as conn:
            conn.execute("PRAGMA busy_timeout=0")
            row = conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
    except (LocalStateError, sqlite3.Error, StoreSchemaError):
        return False
    return (
        row is not None
        and len(row) >= 2
        and int(row[0]) == 0
        and int(row[1]) == 0
    )


def _activate_compacted_wal(
    db_path: Path,
    *,
    expected_identity: EntryIdentity,
    store_lock_held: bool = False,
) -> bool:
    try:
        with _connect(
            db_path,
            isolation_level=None,
            _store_lock_held=store_lock_held,
            _expected_db_identity=expected_identity,
        ) as conn:
            conn.execute("PRAGMA busy_timeout=0")
            with private_file_creation_umask():
                _configure_persistent_database_conn(conn)
    except (LocalStateError, sqlite3.Error, StoreSchemaError):
        return False
    return True


def _vacuum_into_replacement(
    db_path: Path,
    handle: Any,
    phase_hook: Callable[[str], None] | None,
    *,
    source_identity: EntryIdentity,
    store_lock_held: bool = False,
) -> Any:
    released_handle: Any = handle
    try:
        with release_private_sqlite_replacement_at(handle) as (
            released_handle,
            replacement_target,
        ):
            _call_compaction_phase(
                phase_hook,
                "during_replacement",
                failure_status="replacement_failed",
            )
            with _connect(
                db_path,
                isolation_level=None,
                _store_lock_held=store_lock_held,
                _expected_db_identity=source_identity,
            ) as source:
                source.execute("PRAGMA busy_timeout=0")
                source.execute("VACUUM INTO ?", (replacement_target,))
        return verify_created_private_sqlite_replacement_at(released_handle)
    except Exception:
        try:
            created_handle = verify_created_private_sqlite_replacement_at(
                released_handle
            )
            cleanup_private_sqlite_replacement_at(created_handle)
        except LocalStateError:
            pass
        raise _CompactionAbort("replacement_failed") from None


def _copy_backup_into_replacement(
    backup_parent_fd: int,
    backup_leaf: str,
    handle: Any,
    *,
    backup_identity: EntryIdentity,
) -> Any:
    released_handle: Any = handle
    try:
        with release_private_sqlite_replacement_at(handle) as (
            released_handle,
            replacement_target,
        ):
            source = _readonly_sqlite_at(
                backup_parent_fd,
                backup_leaf,
                expected_identity=backup_identity,
            )
            with private_file_creation_umask():
                destination = sqlite3.connect(
                    replacement_target,
                    timeout=0.0,
                    isolation_level=None,
                )
            try:
                source.backup(destination)
                mode_row = destination.execute(
                    "PRAGMA journal_mode=DELETE"
                ).fetchone()
                if mode_row is None or str(mode_row[0]).lower() != "delete":
                    raise _CompactionAbort("rollback_failed")
            finally:
                destination.close()
                source.close()
        return verify_created_private_sqlite_replacement_at(released_handle)
    except Exception:
        try:
            created_handle = verify_created_private_sqlite_replacement_at(
                released_handle
            )
            cleanup_private_sqlite_replacement_at(created_handle)
        except LocalStateError:
            pass
        raise


def _replacement_integrity_ok(
    parent_fd: int,
    replacement_leaf: str,
    *,
    expected_identity: EntryIdentity,
) -> bool:
    conn = _readonly_sqlite_at(
        parent_fd,
        replacement_leaf,
        expected_identity=expected_identity,
    )
    try:
        return _quick_check_ok(conn) and _foreign_key_check_ok(conn)
    finally:
        conn.close()


def _restore_verified_compaction_backup(
    db_path: Path,
    source_parent_fd: int,
    source_leaf: str,
    backup_parent_fd: int,
    backup_leaf: str,
    *,
    backup_identity: EntryIdentity,
    retained_mode: int,
    expected_source_identity: EntryIdentity,
    store_lock_held: bool = False,
) -> bool:
    rollback_handle: Any = None
    published = False
    try:
        backup_stat = verify_entry_identity(
            backup_parent_fd,
            backup_leaf,
            backup_identity,
            expected_type=EntryType.REGULAR_FILE,
        )
        if int(backup_stat.st_nlink) != 1:
            return False
        backup_conn = _readonly_sqlite_at(
            backup_parent_fd,
            backup_leaf,
            expected_identity=backup_identity,
        )
        try:
            if not _quick_check_ok(backup_conn):
                return False
        finally:
            backup_conn.close()

        current_members = _snapshot_sqlite_family_at(
            source_parent_fd,
            source_leaf,
            require_main=True,
        )
        _family_bytes, _main_bytes, _main_mode, current_identity = (
            _compaction_family_metrics(current_members)
        )
        if current_identity != expected_source_identity:
            return False
        if not _checkpoint_truncate(
            db_path,
            expected_identity=expected_source_identity,
            store_lock_held=store_lock_held,
        ):
            return False
        rollback_handle = prepare_private_sqlite_replacement_at(
            source_parent_fd,
            basename=source_leaf,
            retained_mode=retained_mode,
        )
        rollback_handle = _copy_backup_into_replacement(
            backup_parent_fd,
            backup_leaf,
            rollback_handle,
            backup_identity=backup_identity,
        )
        replacement_identity = rollback_handle._replacement_identity
        if replacement_identity is None or not _replacement_integrity_ok(
            source_parent_fd,
            rollback_handle._replacement_name,
            expected_identity=replacement_identity,
        ):
            return False
        restored_identity = publish_private_sqlite_replacement_at(
            rollback_handle,
            expected_source=expected_source_identity,
        )
        published = True
        if not _activate_compacted_wal(
            db_path,
            expected_identity=restored_identity,
            store_lock_held=store_lock_held,
        ):
            return False
        restored = _readonly_sqlite_at(
            source_parent_fd,
            source_leaf,
            expected_identity=restored_identity,
        )
        try:
            restored_ok = (
                _quick_check_ok(restored)
                and _foreign_key_check_ok(restored)
            )
        finally:
            restored.close()
        restored_members = _snapshot_sqlite_family_at(
            source_parent_fd,
            source_leaf,
            require_main=True,
        )
        return restored_ok and _compaction_family_permissions_ok(
            restored_members,
            expected_main=restored_identity,
        )
    except Exception:
        return False
    finally:
        if rollback_handle is not None and not published:
            try:
                cleanup_private_sqlite_replacement_at(rollback_handle)
            except LocalStateError:
                pass


def compact_store(
    db_path: Path,
    *,
    options: CompactionOptions,
    now: str | None = None,
    phase_hook: Callable[[str], None] | None = None,
) -> dict[str, Any]:
    """Inspect or explicitly compact one current-v9 store while offline."""
    report = _compaction_report(options)
    try:
        policy = SnapshotRetentionPolicy(
            retention_days=options.snapshot_retention_days,
            retention_count=options.snapshot_retention_count,
            batch_size=options.batch_size,
        )
        cutoff_at = _utc_cutoff(
            retention_days=policy.retention_days,
            now=now,
        )
    except (TypeError, ValueError):
        return _public_compaction_report(report, status="invalid_request")
    source_parent_fd = -1
    backup_parent_fd = -1
    backup_leaf = ""
    backup_identity: EntryIdentity | None = None
    replacement_handle: Any = None
    replacement_published = False
    source_mutated = False
    stable_lock_held = False
    retained_mode = 0o600
    authoritative_source_identity: EntryIdentity | None = None
    stage = "preflight"
    try:
        if options.dry_run and (
            options.acknowledge_offline or options.backup_path is not None
        ):
            raise _CompactionAbort("invalid_request")
        if _is_memory_db(db_path):
            raise _CompactionAbort("invalid_request")
        if not options.dry_run and (
            options.acknowledge_offline is not True or options.backup_path is None
        ):
            raise _CompactionAbort("invalid_request")

        source_parent_fd, source_leaf = open_resolved_parent(db_path)
        _validate_parent_fd(source_parent_fd, private=True)
        source_members = _snapshot_sqlite_family_at(
            source_parent_fd,
            source_leaf,
            require_main=False,
        )
        try:
            (
                family_bytes,
                source_bytes,
                retained_mode,
                source_identity,
            ) = _compaction_family_metrics(source_members)
        except _CompactionAbort as exc:
            if exc.status == "permissions_failed":
                report["permissions"]["outcome"] = (
                    "repair_required"
                    if any(
                        member.state is PermissionState.REPAIR_REQUIRED
                        for member in source_members
                    )
                    else "unsafe"
                )
            raise
        if not options.dry_run:
            try:
                fcntl.flock(
                    source_parent_fd,
                    fcntl.LOCK_EX | fcntl.LOCK_NB,
                )
            except (BlockingIOError, OSError):
                raise _CompactionAbort("offline_required") from None
            stable_lock_held = True
            stage = "offline"
            locked_members = _snapshot_sqlite_family_at(
                source_parent_fd,
                source_leaf,
                require_main=False,
            )
            if locked_members[0].identity != source_identity:
                raise local_state_error(LocalStateErrorCode.ENTRY_CHANGED)
            source_members = locked_members
            try:
                (
                    family_bytes,
                    source_bytes,
                    retained_mode,
                    source_identity,
                ) = _compaction_family_metrics(source_members)
            except _CompactionAbort as exc:
                if exc.status == "permissions_failed":
                    report["permissions"]["outcome"] = (
                        "repair_required"
                        if any(
                            member.state is PermissionState.REPAIR_REQUIRED
                            for member in source_members
                        )
                        else "unsafe"
                    )
                raise
        report["permissions"] = {"ok": True, "outcome": "compliant"}
        authoritative_source_identity = source_identity
        report["storage"]["before_bytes"] = family_bytes

        wal = source_members[1]
        shm = source_members[2]
        preflight_immutable = (
            wal.state is PermissionState.ABSENT
            or wal.size == 0
        )
        if not preflight_immutable and shm.state is PermissionState.ABSENT:
            raise _CompactionAbort(
                "store_unavailable" if options.dry_run else "offline_required"
            )
        source = _readonly_sqlite_at(
            source_parent_fd,
            source_leaf,
            immutable=preflight_immutable,
            expected_identity=source_identity,
        )
        try:
            version_row = source.execute("PRAGMA user_version").fetchone()
            if version_row is None or int(version_row[0]) != STORE_SCHEMA_VERSION:
                raise _CompactionAbort("schema_not_current")
            if not _quick_check_ok(source):
                report["integrity"]["before"] = "failed"
                raise _CompactionAbort("integrity_failed")
            report["integrity"]["before"] = "ok"
            (
                before,
                eligible,
                latest_hosts,
                eligible_logical_bytes,
            ) = _compaction_snapshot_metrics(
                source,
                cutoff_at=cutoff_at,
                retention_count=policy.retention_count,
            )
            live_bytes, reclaimable_bytes = _compaction_page_metrics(source)
        except sqlite3.Error:
            raise _CompactionAbort("store_unavailable") from None
        finally:
            source.close()
        report["snapshots"].update(
            {
                "before": before,
                "retained": before - eligible,
                "eligible": eligible,
                "remaining": eligible,
                "latest_hosts_retained": latest_hosts,
            }
        )
        report["storage"]["estimated_reclaimable_bytes"] = min(
            source_bytes,
            reclaimable_bytes + eligible_logical_bytes,
        )
        required_bytes = family_bytes * 2 + max(source_bytes, live_bytes)
        available_bytes = sqlite_parent_available_bytes_at(source_parent_fd)
        report["space"] = {
            "available_bytes": available_bytes,
            "required_bytes": required_bytes,
            "headroom_ok": available_bytes >= required_bytes,
        }
        if available_bytes < required_bytes and not options.dry_run:
            raise _CompactionAbort("insufficient_space")

        if options.dry_run:
            return _public_compaction_report(report, status="dry_run", ok=True)

        assert options.backup_path is not None
        backup_parent_fd, backup_leaf = open_resolved_parent(options.backup_path)
        _validate_parent_fd(backup_parent_fd, private=True)
        source_parent_stat = os.fstat(source_parent_fd)
        backup_parent_stat = os.fstat(backup_parent_fd)
        if (
            identity_matches(entry_identity(source_parent_stat), backup_parent_stat)
            and source_leaf == backup_leaf
        ):
            raise _CompactionAbort("invalid_request")
        if inspect_private_file_at(
            backup_parent_fd, backup_leaf
        ).state is not PermissionState.ABSENT:
            raise _CompactionAbort("invalid_request")
        backup_available = sqlite_parent_available_bytes_at(backup_parent_fd)
        if (
            source_parent_stat.st_dev != backup_parent_stat.st_dev
            and backup_available < family_bytes
        ):
            report["space"]["available_bytes"] = min(
                int(report["space"]["available_bytes"]),
                backup_available,
            )
            report["space"]["headroom_ok"] = False
            raise _CompactionAbort("insufficient_space")

        stage = "offline"
        try:
            with _connect(
                db_path,
                isolation_level=None,
                _store_lock_held=True,
                _expected_db_identity=source_identity,
            ) as exclusive:
                exclusive.execute("PRAGMA busy_timeout=0")
                exclusive.execute("BEGIN EXCLUSIVE")
                if int(exclusive.execute("PRAGMA user_version").fetchone()[0]) != STORE_SCHEMA_VERSION:
                    exclusive.rollback()
                    raise _CompactionAbort("schema_not_current")
                if not _quick_check_ok(exclusive):
                    report["integrity"]["before"] = "failed"
                    exclusive.rollback()
                    raise _CompactionAbort("integrity_failed")
                exclusive.commit()
        except sqlite3.OperationalError:
            raise _CompactionAbort("offline_required") from None
        _call_compaction_phase(
            phase_hook,
            "after_precheck",
            failure_status="backup_failed",
        )

        stage = "backup"
        _call_compaction_phase(
            phase_hook,
            "before_backup",
            failure_status="backup_failed",
        )
        backup_identity = _create_verified_compaction_backup(
            source_parent_fd,
            source_leaf,
            backup_parent_fd,
            backup_leaf,
            retained_mode=retained_mode,
            source_identity=source_identity,
        )
        report["backup"] = {
            "required": True,
            "created": True,
            "verified": True,
        }
        report["integrity"]["backup"] = "ok"
        _call_compaction_phase(
            phase_hook,
            "after_backup",
            failure_status="maintenance_failed",
        )

        stage = "maintenance"
        while True:
            batch = cleanup_snapshot_retention(
                db_path,
                retention_days=policy.retention_days,
                retention_count=policy.retention_count,
                batch_size=policy.batch_size,
                now=now,
                dry_run=False,
                _store_lock_held=True,
                _expected_db_identity=source_identity,
            )
            if not batch.get("ok"):
                raise _CompactionAbort("maintenance_failed")
            examined = int(batch.get("examined") or 0)
            deleted = int(batch.get("deleted") or 0)
            source_mutated = source_mutated or deleted > 0
            report["snapshots"]["examined"] += examined
            report["snapshots"]["deleted"] += deleted
            if not batch.get("remaining_candidates"):
                break
            if examined <= 0 or deleted <= 0:
                raise _CompactionAbort("maintenance_failed")
        report["snapshots"]["remaining"] = 0
        report["snapshots"]["retained"] = (
            report["snapshots"]["before"] - report["snapshots"]["deleted"]
        )

        stage = "checkpoint"
        source_mutated = True
        if not _checkpoint_truncate(
            db_path,
            store_lock_held=True,
            expected_identity=source_identity,
        ):
            report["checkpoint"]["status"] = "failed"
            raise _CompactionAbort("checkpoint_failed")
        report["checkpoint"]["status"] = "completed"

        stage = "replacement"
        replacement_handle = prepare_private_sqlite_replacement_at(
            source_parent_fd,
            basename=source_leaf,
            retained_mode=retained_mode,
        )
        replacement_handle = _vacuum_into_replacement(
            db_path,
            replacement_handle,
            phase_hook,
            store_lock_held=True,
            source_identity=source_identity,
        )
        report["replacement"]["status"] = "built"
        replacement_identity = replacement_handle._replacement_identity
        if replacement_identity is None or not _replacement_integrity_ok(
            source_parent_fd,
            replacement_handle._replacement_name,
            expected_identity=replacement_identity,
        ):
            report["integrity"]["replacement"] = "failed"
            raise _CompactionAbort("replacement_failed")
        report["integrity"]["replacement"] = "ok"
        _call_compaction_phase(
            phase_hook,
            "after_replacement_check",
            failure_status="replacement_failed",
        )
        _call_compaction_phase(
            phase_hook,
            "before_publish",
            failure_status="replacement_failed",
        )
        published_identity = publish_private_sqlite_replacement_at(
            replacement_handle,
            expected_source=source_identity,
        )
        authoritative_source_identity = published_identity
        replacement_published = True
        report["replacement"]["status"] = "published"
        if not _activate_compacted_wal(
            db_path,
            store_lock_held=True,
            expected_identity=published_identity,
        ):
            raise _CompactionAbort("replacement_failed")
        _call_compaction_phase(
            phase_hook,
            "publication_failed",
            failure_status="replacement_failed",
        )

        stage = "post_publish"
        post = _readonly_sqlite_at(
            source_parent_fd,
            source_leaf,
            expected_identity=published_identity,
        )
        try:
            post_ok = _quick_check_ok(post) and _foreign_key_check_ok(post)
        finally:
            post.close()
        post_members = _snapshot_sqlite_family_at(
            source_parent_fd,
            source_leaf,
            require_main=True,
        )
        post_ok = post_ok and _compaction_family_permissions_ok(
            post_members,
            expected_main=published_identity,
        )
        report["integrity"]["after"] = "ok" if post_ok else "failed"
        if not post_ok:
            raise _CompactionAbort("replacement_failed")
        _call_compaction_phase(
            phase_hook,
            "after_publish_check",
            failure_status="replacement_failed",
        )
        after_family, _after_main, _mode, after_identity = (
            _compaction_family_metrics(post_members)
        )
        if after_identity != published_identity:
            raise _CompactionAbort("replacement_failed")
        report["storage"]["after_bytes"] = after_family
        return _public_compaction_report(report, status="completed", ok=True)
    except _CompactionAbort as exc:
        if replacement_published or source_mutated:
            report["rollback"]["status"] = "failed"
            if (
                backup_identity is not None
                and backup_parent_fd >= 0
                and authoritative_source_identity is not None
                and _restore_verified_compaction_backup(
                    db_path,
                    source_parent_fd,
                    source_leaf,
                    backup_parent_fd,
                    backup_leaf,
                    backup_identity=backup_identity,
                    expected_source_identity=authoritative_source_identity,
                    retained_mode=retained_mode,
                    store_lock_held=stable_lock_held,
                )
            ):
                report["rollback"]["status"] = "completed"
                report["integrity"]["after"] = "ok"
                return _public_compaction_report(
                    report,
                    status="rollback_completed",
                )
            return _public_compaction_report(report, status="rollback_failed")
        if stage == "backup" and report["backup"]["created"]:
            report["integrity"]["backup"] = "failed"
        if stage == "replacement":
            report["replacement"]["status"] = "failed"
        return _public_compaction_report(report, status=exc.status)
    except LocalStateError:
        if replacement_published or source_mutated:
            report["rollback"]["status"] = "failed"
            if (
                backup_identity is not None
                and backup_parent_fd >= 0
                and authoritative_source_identity is not None
                and _restore_verified_compaction_backup(
                    db_path,
                    source_parent_fd,
                    source_leaf,
                    backup_parent_fd,
                    backup_leaf,
                    backup_identity=backup_identity,
                    expected_source_identity=authoritative_source_identity,
                    retained_mode=retained_mode,
                    store_lock_held=stable_lock_held,
                )
            ):
                report["rollback"]["status"] = "completed"
                report["integrity"]["after"] = "ok"
                return _public_compaction_report(
                    report,
                    status="rollback_completed",
                )
            return _public_compaction_report(report, status="rollback_failed")
        status = (
            "permissions_failed"
            if stage in {"preflight", "offline"}
            else "backup_failed"
            if stage == "backup"
            else "checkpoint_failed"
            if stage == "checkpoint"
            else "replacement_failed"
        )
        return _public_compaction_report(report, status=status)
    except Exception:
        if replacement_published or source_mutated:
            report["rollback"]["status"] = "failed"
            if (
                backup_identity is not None
                and backup_parent_fd >= 0
                and authoritative_source_identity is not None
                and _restore_verified_compaction_backup(
                    db_path,
                    source_parent_fd,
                    source_leaf,
                    backup_parent_fd,
                    backup_leaf,
                    backup_identity=backup_identity,
                    expected_source_identity=authoritative_source_identity,
                    retained_mode=retained_mode,
                    store_lock_held=stable_lock_held,
                )
            ):
                report["rollback"]["status"] = "completed"
                report["integrity"]["after"] = "ok"
                return _public_compaction_report(
                    report,
                    status="rollback_completed",
                )
            return _public_compaction_report(report, status="rollback_failed")
        status = {
            "preflight": "store_unavailable",
            "offline": "offline_required",
            "backup": "backup_failed",
            "maintenance": "maintenance_failed",
            "checkpoint": "checkpoint_failed",
            "replacement": "replacement_failed",
        }.get(stage, "replacement_failed")
        return _public_compaction_report(report, status=status)
    finally:
        if replacement_handle is not None and not replacement_published:
            try:
                cleanup_private_sqlite_replacement_at(replacement_handle)
            except LocalStateError:
                pass
        if backup_parent_fd >= 0:
            os.close(backup_parent_fd)
        if source_parent_fd >= 0:
            os.close(source_parent_fd)


def _command_request_maintenance_summary(
    result: Mapping[str, Any] | None,
    *,
    retry_horizon_seconds: int,
    retention_seconds: int,
    retention_count: int,
    batch_size: int,
) -> dict[str, Any]:
    data = result or {}
    return {
        "ok": bool(data.get("ok")) if result is not None else True,
        "status": str(data.get("status") or ("ok" if result is None else "unknown")),
        "retry_horizon_seconds": int(
            data.get("retry_horizon_seconds") or retry_horizon_seconds
        ),
        "retention_seconds": int(data.get("retention_seconds") or retention_seconds),
        "retention_count": int(data.get("retention_count") or retention_count),
        "batch_size": int(data.get("batch_size") or batch_size),
        "retry_cutoff_at": data.get("retry_cutoff_at"),
        "cutoff_at": data.get("cutoff_at"),
        "examined": int(data.get("examined") or 0),
        "stale_active": int(data.get("stale_active") or 0),
        "deleted": int(data.get("deleted") or 0),
        "remaining_candidates": bool(data.get("remaining_candidates")),
    }


def maybe_run_automatic_store_maintenance(
    db_path: Path,
    *,
    policy: SnapshotRetentionPolicy,
    agent_event_host_id: str | None = None,
    agent_event_retention_days: int | None = None,
    acknowledged_final_retention_days: int = ACKNOWLEDGED_FINAL_RETENTION_DAYS,
    acknowledged_final_retention_count: int = ACKNOWLEDGED_FINAL_RETENTION_COUNT,
    command_retry_horizon_seconds: int = COMMAND_RETRY_HORIZON_SECONDS,
    command_receipt_retention_seconds: int = COMMAND_RECEIPT_RETENTION_SECONDS,
    command_receipt_retention_count: int = COMMAND_RECEIPT_RETENTION_COUNT,
    cadence_seconds: int = 3600,
    now: str | None = None,
) -> dict[str, Any]:
    """Run one serialized automatic batch when the persisted cadence is due."""
    if (
        isinstance(cadence_seconds, bool)
        or not isinstance(cadence_seconds, int)
        or cadence_seconds <= 0
        or cadence_seconds > _MAX_TIMEDELTA_SECONDS
    ):
        raise ValueError("cadence_seconds must be a positive integer")
    if any(
        isinstance(value, bool) or not isinstance(value, int) or value <= 0
        for value in (
            acknowledged_final_retention_days,
            acknowledged_final_retention_count,
        )
    ):
        raise ValueError(
            "acknowledged final retention values must be positive integers"
        )
    if (
        acknowledged_final_retention_days > _MAX_RETENTION_DAYS
        or acknowledged_final_retention_count > _SQLITE_MAX_INTEGER
    ):
        raise ValueError("acknowledged final retention values are too large")
    if agent_event_host_id is None and agent_event_retention_days is not None:
        raise ValueError("agent_event_host_id is required for event retention")
    normalized_agent_event_host = (
        normalize_agent_event_identifier(
            agent_event_host_id,
            "agent_event_host_id",
            required=True,
        )
        if agent_event_host_id is not None
        else None
    )
    agent_event_days = (
        policy.retention_days
        if agent_event_retention_days is None
        else agent_event_retention_days
    )
    if (
        normalized_agent_event_host is not None
        and (
            isinstance(agent_event_days, bool)
            or not isinstance(agent_event_days, int)
            or not 1 <= agent_event_days <= _MAX_RETENTION_DAYS
        )
    ):
        raise ValueError("agent_event_retention_days must be positive")
    current_at = _connector_now(now)
    agent_event_cutoff_at = _utc_cutoff(
        retention_days=agent_event_days,
        now=current_at,
    )
    empty_agent_events = {
        "host_id": normalized_agent_event_host,
        "retention_days": agent_event_days,
        "cutoff_at": agent_event_cutoff_at,
        "batch_size": policy.batch_size,
        "examined": 0,
        "deleted": 0,
        "remaining_candidates": False,
    }
    empty_command_requests = _command_request_maintenance_summary(
        None,
        retry_horizon_seconds=command_retry_horizon_seconds,
        retention_seconds=command_receipt_retention_seconds,
        retention_count=command_receipt_retention_count,
        batch_size=policy.batch_size,
    )
    if not _sqlite_store_exists(db_path):
        return dict(sanitize_public_value({
            "schema_version": 1,
            "ok": False,
            "status": "store_unavailable",
            "due": False,
            "last_completed_at": None,
            "next_due_at": None,
            "snapshot": {
                "examined": 0,
                "deleted": 0,
                "remaining_candidates": False,
            },
            "agent_events": empty_agent_events,
            "final_retention": {
                "examined": 0,
                "deleted": 0,
                "remaining_candidates": False,
                "acknowledged_final_retention_days": int(
                    acknowledged_final_retention_days
                ),
                "acknowledged_final_retention_count": int(
                    acknowledged_final_retention_count
                ),
            },
            "command_requests": empty_command_requests,
            "batch_size": policy.batch_size,
        }))
    cutoff_at = _utc_cutoff(retention_days=policy.retention_days, now=current_at)
    final_cutoff_at = _utc_cutoff(
        retention_days=acknowledged_final_retention_days,
        now=current_at,
    )
    with _connect(db_path, isolation_level=None) as conn:
        _ensure_schema(conn)
        if normalized_agent_event_host is not None:
            conn.execute("PRAGMA secure_delete=ON")
        conn.execute("BEGIN IMMEDIATE")
        try:
            state = conn.execute(
                """
                SELECT last_completed_at
                FROM store_maintenance_state
                WHERE scope = 'automatic'
                """
            ).fetchone()
            last_completed_at = (
                str(state[0]) if state is not None and state[0] is not None else None
            )
            next_due_at = (
                _connector_add_seconds(last_completed_at, cadence_seconds)
                if last_completed_at is not None
                else None
            )
            due = (
                next_due_at is None
                or _connector_datetime(current_at) >= _connector_datetime(next_due_at)
            )
            if not due:
                conn.rollback()
                return dict(sanitize_public_value({
                    "schema_version": 1,
                    "ok": True,
                    "status": "not_due",
                    "due": False,
                    "last_completed_at": last_completed_at,
                    "next_due_at": next_due_at,
                    "snapshot": {
                        "examined": 0,
                        "deleted": 0,
                        "remaining_candidates": False,
                    },
                    "agent_events": empty_agent_events,
                    "final_retention": {
                        "examined": 0,
                        "deleted": 0,
                        "remaining_candidates": False,
                        "acknowledged_final_retention_days": int(
                            acknowledged_final_retention_days
                        ),
                        "acknowledged_final_retention_count": int(
                            acknowledged_final_retention_count
                        ),
                    },
                    "command_requests": empty_command_requests,
                    "batch_size": policy.batch_size,
                }))
            _settle_due_submission_links_conn(
                conn,
                db_path=db_path,
                now=current_at,
            )
            _expire_turn_submissions_conn(
                conn,
                current=current_at,
            )
            agent_events = dict(empty_agent_events)
            if normalized_agent_event_host is not None:
                agent_events.update(
                    _cleanup_agent_event_retention_conn(
                        conn,
                        normalized_agent_event_host,
                        cutoff_at=agent_event_cutoff_at,
                        batch_size=policy.batch_size,
                        dry_run=False,
                    )
                )
            candidates, _ = _snapshot_retention_candidates_conn(
                conn,
                cutoff_at=cutoff_at,
                retention_count=policy.retention_count,
                batch_size=policy.batch_size,
            )
            deleted = _delete_snapshot_candidates_conn(conn, candidates)
            remaining_ids, _ = _snapshot_retention_candidates_conn(
                conn,
                cutoff_at=cutoff_at,
                retention_count=policy.retention_count,
                batch_size=1,
            )
            final_host_ids = [
                str(row[0])
                for row in conn.execute(
                    """
                    SELECT roots.host_id
                    FROM (
                        SELECT DISTINCT host_id
                        FROM connector_outbox
                        WHERE connector = 'turn-final'
                          AND delivery_kind = 'final_ready'
                    ) AS roots
                    LEFT JOIN store_maintenance_cursors AS serviced
                      ON serviced.scope = roots.host_id
                    ORDER BY
                        serviced.last_completed_at IS NOT NULL,
                        serviced.last_completed_at,
                        roots.host_id
                    """
                ).fetchall()
            ]
            final_examined = 0
            final_deleted = 0
            final_remaining = False
            final_budget = int(policy.batch_size)
            for final_host_id in final_host_ids:
                if final_budget <= 0:
                    final_remaining = True
                    break
                final_candidates = _acknowledged_final_retention_candidates_conn(
                    conn,
                    host_id=final_host_id,
                    cutoff_at=final_cutoff_at,
                    retention_count=acknowledged_final_retention_count,
                    batch_size=final_budget + 1,
                )
                selected_final_candidates = final_candidates[:final_budget]
                final_remaining = final_remaining or (
                    len(final_candidates) > final_budget
                )
                final_examined += len(selected_final_candidates)
                for _source_outbox_id, final_turn_id in selected_final_candidates:
                    deleted_rows = _delete_acknowledged_final_turn_conn(
                        conn,
                        host_id=final_host_id,
                        turn_id=final_turn_id,
                        tombstone_retention_count=acknowledged_final_retention_count,
                    )
                    final_deleted += int(deleted_rows["turns"])
                final_budget -= len(selected_final_candidates)
                conn.execute(
                    """
                    INSERT INTO store_maintenance_cursors (
                        scope, last_completed_at
                    ) VALUES (?, ?)
                    ON CONFLICT(scope) DO UPDATE SET
                        last_completed_at = excluded.last_completed_at
                    """,
                    (str(final_host_id), current_at),
                )
            if not final_remaining:
                final_remaining = any(
                    _acknowledged_final_retention_candidates_conn(
                        conn,
                        host_id=final_host_id,
                        cutoff_at=final_cutoff_at,
                        retention_count=acknowledged_final_retention_count,
                        batch_size=1,
                    )
                    for final_host_id in final_host_ids
                )
            conn.execute(
                """
                UPDATE store_maintenance_state
                SET last_started_at = ?,
                    last_completed_at = ?,
                    last_status = 'ok',
                    last_examined = ?,
                    last_deleted = ?,
                    last_examined_id = ?
                WHERE scope = 'automatic'
                """,
                (
                    current_at,
                    current_at,
                    len(candidates),
                    deleted,
                    max(candidates) if candidates else None,
                ),
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    command_request_result = cleanup_command_request_retention(
        db_path,
        retry_horizon_seconds=command_retry_horizon_seconds,
        retention_seconds=command_receipt_retention_seconds,
        retention_count=command_receipt_retention_count,
        now=current_at,
        batch_size=policy.batch_size,
    )
    command_requests = _command_request_maintenance_summary(
        command_request_result,
        retry_horizon_seconds=command_retry_horizon_seconds,
        retention_seconds=command_receipt_retention_seconds,
        retention_count=command_receipt_retention_count,
        batch_size=policy.batch_size,
    )
    return dict(sanitize_public_value({
        "schema_version": 1,
        "ok": bool(command_requests["ok"]),
        "status": "ok" if command_requests["ok"] else command_requests["status"],
        "due": True,
        "last_completed_at": current_at,
        "next_due_at": _connector_add_seconds(current_at, cadence_seconds),
        "snapshot": {
            "examined": len(candidates),
            "deleted": deleted,
            "remaining_candidates": bool(remaining_ids),
        },
        "agent_events": agent_events,
        "final_retention": {
            "examined": final_examined,
            "deleted": final_deleted,
            "remaining_candidates": bool(final_remaining),
            "acknowledged_final_retention_days": int(
                acknowledged_final_retention_days
            ),
            "acknowledged_final_retention_count": int(
                acknowledged_final_retention_count
            ),
        },
        "command_requests": command_requests,
        "batch_size": policy.batch_size,
    }))


def cleanup_event_retention(
    db_path: Path,
    host_id: str,
    *,
    retention_days: int,
    now: str | None = None,
    dry_run: bool = False,
    batch_size: int = 100,
) -> dict[str, Any]:
    """Delete one bounded host-scoped batch from event history."""
    days = max(1, int(retention_days))
    bounded_batch = max(1, min(int(batch_size), 1_000))
    cutoff_at = _utc_cutoff(retention_days=days, now=now)
    base = {
        "schema_version": 1,
        "host_id": str(host_id),
        "dry_run": bool(dry_run),
        "retention_days": days,
        "cutoff_at": cutoff_at,
        "batch_size": bounded_batch,
    }
    if not _sqlite_store_exists(db_path):
        return dict(sanitize_public_value({
            **base,
            "ok": False,
            "status": "store_unavailable",
            "examined": 0,
            "deleted": 0,
            "remaining_candidates": False,
        }))
    with _connect(db_path, isolation_level=None) as conn:
        _ensure_schema(conn)
        conn.execute("BEGIN IMMEDIATE")
        try:
            candidate_pool = [
                int(row[0])
                for row in conn.execute(
                    """
                    SELECT id
                    FROM events
                    WHERE host_id = ? AND observed_at < ?
                    ORDER BY observed_at, id
                    LIMIT ?
                    """,
                    (str(host_id), cutoff_at, bounded_batch + 1),
                ).fetchall()
            ]
            candidate_ids = candidate_pool[:bounded_batch]
            deleted = 0
            if candidate_ids and not dry_run:
                placeholders = ",".join("?" for _ in candidate_ids)
                deleted = int(
                    conn.execute(
                        f"DELETE FROM events WHERE id IN ({placeholders})",
                        candidate_ids,
                    ).rowcount
                    or 0
                )
            if dry_run:
                remaining = len(candidate_pool) > bounded_batch
            else:
                remaining = bool(
                    conn.execute(
                        """
                        SELECT 1
                        FROM events
                        WHERE host_id = ? AND observed_at < ?
                        LIMIT 1
                        """,
                        (str(host_id), cutoff_at),
                    ).fetchone()
                )
            if dry_run:
                conn.rollback()
            else:
                conn.commit()
        except Exception:
            conn.rollback()
            raise
    return dict(sanitize_public_value({
        **base,
        "ok": True,
        "status": "ok",
        "examined": len(candidate_ids),
        "deleted": len(candidate_ids) if dry_run else deleted,
        "remaining_candidates": remaining,
    }))


_AGENT_EVENT_RETENTION_SELECT = """
SELECT sequence
FROM agent_events
"""


def _cleanup_agent_event_retention_conn(
    conn: sqlite3.Connection,
    host_id: str,
    *,
    cutoff_at: str,
    batch_size: int,
    dry_run: bool,
) -> dict[str, Any]:
    """Delete one bounded batch of expired journal rows."""
    rows = conn.execute(
        _AGENT_EVENT_RETENTION_SELECT
        + " WHERE host_id = ? AND observed_at < ?"
        + " ORDER BY observed_at, sequence LIMIT ?",
        (str(host_id), cutoff_at, int(batch_size) + 1),
    ).fetchall()
    sequences = [int(row[0]) for row in rows[:batch_size]]
    remaining = len(rows) > len(sequences)
    deleted = 0
    if sequences and not dry_run:
        placeholders = ",".join("?" for _ in sequences)
        deleted = int(
            conn.execute(
                f"DELETE FROM agent_events WHERE host_id = ? "
                f"AND sequence IN ({placeholders})",
                (str(host_id), *sequences),
            ).rowcount
            or 0
        )
        if deleted != len(sequences):
            raise StoreSchemaError("agent_event_retention_delete_mismatch")
    return {
        "examined": len(sequences),
        "deleted": len(sequences) if dry_run else deleted,
        "remaining_candidates": remaining,
    }


def cleanup_agent_event_retention(
    db_path: Path,
    host_id: str,
    *,
    retention_days: int,
    now: str | None = None,
    dry_run: bool = False,
    batch_size: int = 100,
) -> dict[str, Any]:
    """Delete a bounded batch of expired structured agent events."""
    days = max(1, int(retention_days))
    bounded_batch = max(1, min(int(batch_size), 1_000))
    cutoff_at = _utc_cutoff(retention_days=days, now=now)
    base = {
        "schema_version": 1,
        "host_id": str(host_id),
        "dry_run": bool(dry_run),
        "retention_days": days,
        "cutoff_at": cutoff_at,
        "batch_size": bounded_batch,
    }
    if not _sqlite_store_exists(db_path):
        return dict(sanitize_public_value({
            **base,
            "ok": False,
            "status": "store_unavailable",
            "examined": 0,
            "deleted": 0,
            "remaining_candidates": False,
        }))
    with _connect(db_path, isolation_level=None) as conn:
        _ensure_schema(conn)
        # Ask SQLite to scrub deleted cells in modified pages. WAL frames,
        # checkpoints, filesystem snapshots, and backups have independent
        # lifecycles, so this is not an immediate physical-erasure guarantee.
        conn.execute("PRAGMA secure_delete=ON")
        conn.execute("BEGIN IMMEDIATE")
        try:
            result = _cleanup_agent_event_retention_conn(
                conn,
                str(host_id),
                cutoff_at=cutoff_at,
                batch_size=bounded_batch,
                dry_run=bool(dry_run),
            )
            if dry_run:
                conn.rollback()
            else:
                conn.commit()
        except Exception:
            conn.rollback()
            raise
    return dict(sanitize_public_value({
        **base,
        "ok": True,
        "status": "ok",
        **result,
    }))


def _turn_content_retention_candidates_conn(
    conn: sqlite3.Connection,
    *,
    host_id: str,
    cutoff_at: str,
    batch_size: int,
    retention_count: int,
) -> list[tuple[str, int]]:
    rows = conn.execute(
        _ACKNOWLEDGED_FINAL_ELIGIBILITY_CTE
        + """
        SELECT candidate_type, candidate_id
        FROM (
            SELECT
                'plan' AS candidate_type,
                plans.id AS candidate_id,
                CASE
                    WHEN plans.state = 'preparing' THEN plans.created_at
                    ELSE COALESCE(
                        plans.completed_at,
                        plans.activated_at,
                        plans.created_at
                    )
                END AS eligible_at
            FROM turn_presentation_plans AS plans
            WHERE plans.host_id = :host_id
              AND plans.source_outbox_id IS NULL
              AND (
                  (
                      plans.state = 'preparing'
                      AND plans.created_at < :cutoff_at
                  )
                  OR (
                      plans.state IN ('completed', 'superseded')
                      AND COALESCE(
                          plans.completed_at,
                          plans.activated_at,
                          plans.created_at
                      ) < :cutoff_at
                  )
              )
              AND NOT EXISTS (
                  SELECT 1
                  FROM turn_presentation_plans AS replacement
                  WHERE replacement.host_id = plans.host_id
                    AND replacement.name = plans.name
                    AND replacement.turn_id = plans.turn_id
                    AND replacement.replaces_plan_token = plans.plan_token
                    AND replacement.state IN (
                        'preparing',
                        'waiting_predecessor',
                        'active'
                    )
              )
              AND (
                  plans.state = 'preparing'
                  OR plans.activated_at IS NULL
                  OR plans.id < COALESCE(
                      (
                          SELECT MAX(completed.id)
                          FROM turn_presentation_plans AS completed
                          WHERE completed.host_id = plans.host_id
                            AND completed.name = plans.name
                            AND completed.turn_id = plans.turn_id
                            AND completed.completed_at IS NOT NULL
                      ),
                      0
                  )
              )
              AND NOT EXISTS (
                  SELECT 1
                  FROM turn_presentation_jobs AS jobs
                  LEFT JOIN connector_outbox AS outbox
                    ON outbox.id = jobs.outbox_id
                  LEFT JOIN connector_deliveries AS deliveries
                    ON deliveries.outbox_id = outbox.id
                  WHERE jobs.plan_id = plans.id
                    AND (
                        (
                            outbox.id IS NOT NULL
                            AND (
                                plans.state = 'preparing'
                                OR outbox.status NOT IN (
                                    'delivered',
                                    'superseded'
                                )
                                OR outbox.updated_at IS NULL
                                OR outbox.updated_at >= :cutoff_at
                            )
                        )
                        OR (
                            deliveries.id IS NOT NULL
                            AND (
                                deliveries.status NOT IN (
                                    'delivered',
                                    'superseded',
                                    'failed',
                                    'deferred',
                                    'expired'
                                )
                                OR COALESCE(
                                    deliveries.delivered_at,
                                    deliveries.created_at
                                ) >= :cutoff_at
                            )
                        )
                    )
              )
            UNION ALL
            SELECT
                'plan' AS candidate_type,
                plans.id AS candidate_id,
                COALESCE(plans.activated_at, plans.created_at) AS eligible_at
            FROM turn_presentation_plans AS plans
            JOIN connector_outbox AS source
              ON source.id = plans.source_outbox_id
            WHERE plans.host_id = :host_id
              AND plans.state = 'superseded'
              AND COALESCE(plans.activated_at, plans.created_at) < :cutoff_at
              AND (
                  plans.activated_at IS NULL
                  OR plans.id = COALESCE(
                      (
                          SELECT MAX(latest_superseded.id)
                          FROM turn_presentation_plans AS latest_superseded
                          WHERE latest_superseded.host_id = plans.host_id
                            AND latest_superseded.name = plans.name
                            AND latest_superseded.turn_id = plans.turn_id
                            AND latest_superseded.state = 'superseded'
                            AND latest_superseded.activated_at IS NOT NULL
                      ),
                      0
                  )
                  OR EXISTS (
                      SELECT 1
                      FROM turn_presentation_plans AS summary
                      WHERE summary.host_id = plans.host_id
                        AND summary.name = plans.name
                        AND summary.turn_id = plans.turn_id
                        AND summary.id > plans.id
                        AND summary.state = 'superseded'
                        AND summary.activated_at IS NOT NULL
                        AND summary.source_outbox_id IS NULL
                  )
                  OR EXISTS (
                      SELECT 1
                      FROM turn_presentation_plans AS completed
                      WHERE completed.host_id = plans.host_id
                        AND completed.name = plans.name
                        AND completed.turn_id = plans.turn_id
                        AND completed.id > plans.id
                        AND completed.state = 'completed'
                        AND completed.completed_at IS NOT NULL
                  )
              )
              AND source.host_id = plans.host_id
              AND source.connector = :turn_final_name
              AND source.delivery_kind = 'final_ready'
              AND source.turn_id = plans.turn_id
              AND source.content_revision = plans.content_revision
              AND source.status = 'superseded'
              AND source.updated_at IS NOT NULL
              AND source.updated_at < :cutoff_at
              AND NOT EXISTS (
                  SELECT 1
                  FROM connector_deliveries AS source_attempts
                  WHERE source_attempts.outbox_id = source.id
                    AND (
                        source_attempts.status NOT IN (
                            'delivered',
                            'superseded',
                            'failed',
                            'deferred',
                            'expired'
                        )
                        OR COALESCE(
                            source_attempts.delivered_at,
                            source_attempts.created_at
                        ) >= :cutoff_at
                    )
              )
              AND NOT EXISTS (
                  SELECT 1
                  FROM turn_presentation_jobs AS jobs
                  LEFT JOIN connector_outbox AS outbox
                    ON outbox.id = jobs.outbox_id
                  LEFT JOIN connector_deliveries AS deliveries
                    ON deliveries.outbox_id = outbox.id
                  WHERE jobs.plan_id = plans.id
                    AND (
                        (
                            outbox.id IS NOT NULL
                            AND (
                                outbox.status NOT IN (
                                    'delivered',
                                    'superseded'
                                )
                                OR outbox.updated_at IS NULL
                                OR outbox.updated_at >= :cutoff_at
                            )
                        )
                        OR (
                            deliveries.id IS NOT NULL
                            AND (
                                deliveries.status NOT IN (
                                    'delivered',
                                    'superseded',
                                    'failed',
                                    'deferred',
                                    'expired'
                                )
                                OR COALESCE(
                                    deliveries.delivered_at,
                                    deliveries.created_at
                                ) >= :cutoff_at
                            )
                        )
                    )
              )
            UNION ALL
            SELECT
                'plan' AS candidate_type,
                plans.id AS candidate_id,
                source.updated_at AS eligible_at
            FROM turn_presentation_plans AS plans
            JOIN connector_outbox AS source
              ON source.id = plans.source_outbox_id
            JOIN ranked AS delivered_rank
              ON delivered_rank.source_outbox_id = source.id
             AND delivered_rank.is_current = 0
            WHERE plans.host_id = :host_id
              AND plans.state IN ('completed', 'superseded')
              AND plans.completed_at IS NOT NULL
              AND (
                  plans.completed_at < :cutoff_at
                  OR delivered_rank.acknowledged_rank > :retention_count
              )
              AND EXISTS (
                  SELECT 1
                  FROM turn_presentation_plans AS completed
                  WHERE completed.host_id = plans.host_id
                    AND completed.name = plans.name
                    AND completed.turn_id = plans.turn_id
                    AND completed.id > plans.id
                    AND completed.state = 'completed'
                    AND completed.completed_at IS NOT NULL
              )
              AND source.host_id = plans.host_id
              AND source.connector = :turn_final_name
              AND source.delivery_kind = 'final_ready'
              AND source.turn_id = plans.turn_id
              AND source.content_revision = plans.content_revision
              AND source.status = 'delivered'
              AND source.updated_at IS NOT NULL
              AND (
                  source.updated_at < :cutoff_at
                  OR delivered_rank.acknowledged_rank > :retention_count
              )
              AND NOT EXISTS (
                  SELECT 1
                  FROM connector_deliveries AS source_attempts
                  WHERE source_attempts.outbox_id = source.id
                    AND (
                        source_attempts.status NOT IN (
                            'delivered',
                            'superseded',
                            'failed',
                            'deferred',
                            'expired'
                        )
                        OR (
                            COALESCE(
                                source_attempts.delivered_at,
                                source_attempts.created_at
                            ) >= :cutoff_at
                            AND delivered_rank.acknowledged_rank
                                <= :retention_count
                        )
                    )
              )
              AND NOT EXISTS (
                  SELECT 1
                  FROM turn_presentation_jobs AS jobs
                  LEFT JOIN connector_outbox AS outbox
                    ON outbox.id = jobs.outbox_id
                  LEFT JOIN connector_deliveries AS deliveries
                    ON deliveries.outbox_id = outbox.id
                  WHERE jobs.plan_id = plans.id
                    AND (
                        (
                            outbox.id IS NULL
                            OR outbox.status != 'delivered'
                            OR outbox.updated_at IS NULL
                            OR (
                                outbox.updated_at >= :cutoff_at
                                AND delivered_rank.acknowledged_rank
                                    <= :retention_count
                            )
                        )
                        OR (
                            deliveries.id IS NOT NULL
                            AND (
                                deliveries.status NOT IN (
                                    'delivered',
                                    'superseded',
                                    'failed',
                                    'deferred',
                                    'expired'
                                )
                                OR (
                                    COALESCE(
                                        deliveries.delivered_at,
                                        deliveries.created_at
                                    ) >= :cutoff_at
                                    AND delivered_rank.acknowledged_rank
                                        <= :retention_count
                                )
                            )
                        )
                    )
              )
            UNION ALL
            SELECT
                'source' AS candidate_type,
                source.id AS candidate_id,
                source.updated_at AS eligible_at
            FROM connector_outbox AS source
            WHERE source.host_id = :host_id
              AND source.connector = :turn_final_name
              AND source.delivery_kind = 'final_ready'
              AND source.status = 'superseded'
              AND source.updated_at IS NOT NULL
              AND source.updated_at < :cutoff_at
              AND NOT EXISTS (
                  SELECT 1
                  FROM turn_presentation_plans AS plans
                  WHERE plans.source_outbox_id = source.id
              )
              AND NOT EXISTS (
                  SELECT 1
                  FROM connector_deliveries AS attempts
                  WHERE attempts.outbox_id = source.id
                    AND (
                        attempts.status NOT IN (
                            'delivered',
                            'superseded',
                            'failed',
                            'deferred',
                            'expired'
                        )
                        OR COALESCE(
                            attempts.delivered_at,
                            attempts.created_at
                        ) >= :cutoff_at
                    )
              )
            UNION ALL
            SELECT
                'revision' AS candidate_type,
                revisions.rowid AS candidate_id,
                revisions.superseded_at AS eligible_at
            FROM turn_content_revisions AS revisions
            WHERE revisions.host_id = :host_id
              AND revisions.is_current = 0
              AND revisions.superseded_at IS NOT NULL
              AND revisions.superseded_at < :cutoff_at
              AND NOT EXISTS (
                  SELECT 1
                  FROM turn_presentation_plans AS plans
                  WHERE plans.host_id = revisions.host_id
                    AND plans.turn_id = revisions.turn_id
                    AND plans.content_revision = revisions.content_revision
              )
              AND NOT EXISTS (
                  SELECT 1
                  FROM connector_outbox AS typed_final
                  WHERE typed_final.host_id = revisions.host_id
                    AND typed_final.turn_id = revisions.turn_id
                    AND typed_final.content_revision = revisions.content_revision
                    AND typed_final.delivery_kind GLOB 'final_*'
              )
              AND NOT EXISTS (
                  SELECT 1
                  FROM connector_outbox AS outbox
                  WHERE outbox.host_id = revisions.host_id
                    AND outbox.connector = :turn_final_name
                    AND json_valid(outbox.payload_json)
                    AND json_extract(
                        outbox.payload_json,
                        '$.content_revision'
                    ) = revisions.content_revision
              )
              AND NOT EXISTS (
                  SELECT 1
                  FROM turns
                  WHERE turns.host_id = revisions.host_id
                    AND turns.turn_id = revisions.turn_id
                    AND json_valid(turns.payload_json)
                    AND (
                        json_extract(
                            turns.payload_json,
                            '$.content_revision'
                        ) = revisions.content_revision
                        OR json_extract(
                            turns.payload_json,
                            '$.content.content_revision'
                        ) = revisions.content_revision
                    )
              )
        )
        ORDER BY eligible_at, candidate_type, candidate_id
        LIMIT :batch_size
        """,
        {
            "host_id": str(host_id),
            "cutoff_at": str(cutoff_at),
            "turn_final_name": _TURN_FINAL_NAME,
            "retention_count": int(retention_count),
            "batch_size": int(batch_size),
        },
    ).fetchall()
    return [(str(row[0]), int(row[1])) for row in rows]


def _delivered_final_rank_conn(
    conn: sqlite3.Connection,
    *,
    host_id: str,
    source_outbox_id: int,
) -> tuple[int, int] | None:
    row = conn.execute(
        _ACKNOWLEDGED_FINAL_ELIGIBILITY_CTE
        + """
        SELECT acknowledged_rank, is_current
        FROM ranked
        WHERE source_outbox_id = :source_outbox_id
        """,
        {
            "host_id": str(host_id),
            "source_outbox_id": int(source_outbox_id),
        },
    ).fetchone()
    if row is None:
        return None
    return int(row[0]), int(row[1])


def _terminal_source_anchor_reference_reason_conn(
    conn: sqlite3.Connection,
    *,
    source_outbox_id: int,
    host_id: str,
    turn_id: str,
    content_revision_value: str,
    source_status: str,
    cutoff_at: str,
    allow_young: bool = False,
) -> str | None:
    source = conn.execute(
        """
        SELECT
            host_id, connector, delivery_kind, turn_id, content_revision,
            status, updated_at
        FROM connector_outbox
        WHERE id = ?
        """,
        (int(source_outbox_id),),
    ).fetchone()
    if source is None:
        return "source_anchor"
    if (
        str(source[0]) != str(host_id)
        or str(source[1]) != _TURN_FINAL_NAME
        or str(source[2]) != "final_ready"
        or str(source[3]) != str(turn_id)
        or str(source[4]) != str(content_revision_value)
        or str(source[5]) != str(source_status)
        or not source[6]
        or (not allow_young and str(source[6]) >= str(cutoff_at))
    ):
        return "source_anchor"
    if str(source_status) == _CONNECTOR_TERMINAL_OUTBOX_STATUS:
        revision = conn.execute(
            """
            SELECT is_current
            FROM turn_content_revisions
            WHERE host_id = ? AND turn_id = ? AND content_revision = ?
            """,
            (str(host_id), str(turn_id), str(content_revision_value)),
        ).fetchone()
        if revision is None or int(revision[0]) != 0:
            return "current_source_anchor"
    unsafe_attempt = conn.execute(
        """
        SELECT 1
        FROM connector_deliveries
        WHERE outbox_id = ?
          AND (
              status NOT IN ('delivered', 'superseded', 'failed', 'deferred', 'expired')
              OR (
                  ? = 0
                  AND COALESCE(delivered_at, created_at) >= ?
              )
          )
        LIMIT 1
        """,
        (int(source_outbox_id), int(bool(allow_young)), str(cutoff_at)),
    ).fetchone()
    return "source_attempt" if unsafe_attempt is not None else None


def _terminal_plan_reference_reason_conn(
    conn: sqlite3.Connection,
    *,
    plan: sqlite3.Row | tuple[Any, ...],
    cutoff_at: str,
    retention_count: int,
) -> str | None:
    plan_id = int(plan[0])
    host_id = str(plan[1])
    name = str(plan[2])
    plan_token = str(plan[3])
    turn_id = str(plan[4])
    content_revision_value = str(plan[5])
    state = str(plan[6])
    activated_at = plan[8]
    source_outbox_id = int(plan[10]) if plan[10] is not None else None
    was_completed = state == "completed" or (
        state == "superseded" and plan[9] is not None
    )
    allow_young = False
    if source_outbox_id is not None:
        if state == "superseded" and not was_completed:
            source_status = _CONNECTOR_SUPERSEDED_OUTBOX_STATUS
        elif was_completed:
            newer_completed = conn.execute(
                """
                SELECT 1
                FROM turn_presentation_plans
                WHERE host_id = ? AND name = ? AND turn_id = ?
                  AND id > ? AND state = 'completed'
                  AND completed_at IS NOT NULL
                LIMIT 1
                """,
                (host_id, name, turn_id, plan_id),
            ).fetchone()
            rank = _delivered_final_rank_conn(
                conn,
                host_id=host_id,
                source_outbox_id=source_outbox_id,
            )
            if (
                newer_completed is None
                or rank is None
                or int(rank[1]) != 0
            ):
                return "current_completed_baseline"
            allow_young = int(rank[0]) > max(1, int(retention_count))
            source_status = _CONNECTOR_TERMINAL_OUTBOX_STATUS
        else:
            return "source_anchor"
        source_reason = _terminal_source_anchor_reference_reason_conn(
            conn,
            source_outbox_id=source_outbox_id,
            host_id=host_id,
            turn_id=turn_id,
            content_revision_value=content_revision_value,
            source_status=source_status,
            cutoff_at=cutoff_at,
            allow_young=allow_young,
        )
        if source_reason is not None:
            return source_reason
    if state == "preparing":
        unexpected_anchor = conn.execute(
            """
            SELECT 1
            FROM turn_presentation_jobs AS jobs
            LEFT JOIN connector_outbox AS outbox ON outbox.id = jobs.outbox_id
            LEFT JOIN connector_deliveries AS deliveries
              ON deliveries.outbox_id = outbox.id
            WHERE jobs.plan_id = ?
              AND (jobs.outbox_id IS NOT NULL OR deliveries.id IS NOT NULL)
            LIMIT 1
            """,
            (plan_id,),
        ).fetchone()
        return "reference" if unexpected_anchor is not None else None
    if state not in _TURN_CONTENT_TERMINAL_PLAN_STATES:
        return "reference"
    if source_outbox_id is None:
        live_replacement = conn.execute(
            """
            SELECT 1
            FROM turn_presentation_plans
            WHERE host_id = ? AND name = ? AND turn_id = ?
              AND replaces_plan_token = ?
              AND state IN ('preparing', 'waiting_predecessor', 'active')
            LIMIT 1
            """,
            (host_id, name, turn_id, plan_token),
        ).fetchone()
        if live_replacement is not None:
            return "replacement"
        latest_completed = conn.execute(
            """
            SELECT COALESCE(MAX(id), 0)
            FROM turn_presentation_plans
            WHERE host_id = ? AND name = ? AND turn_id = ?
              AND completed_at IS NOT NULL
            """,
            (host_id, name, turn_id),
        ).fetchone()
        baseline_id = int(latest_completed[0] or 0)
        if activated_at is not None and (baseline_id == 0 or plan_id >= baseline_id):
            return "failed_prefix_or_current_baseline"
    anchors = conn.execute(
        """
        SELECT
            outbox.id,
            outbox.status,
            outbox.updated_at,
            deliveries.id,
            deliveries.status,
            COALESCE(deliveries.delivered_at, deliveries.created_at)
        FROM turn_presentation_jobs AS jobs
        LEFT JOIN connector_outbox AS outbox ON outbox.id = jobs.outbox_id
        LEFT JOIN connector_deliveries AS deliveries
          ON deliveries.outbox_id = outbox.id
        WHERE jobs.plan_id = ?
        """,
        (plan_id,),
    ).fetchall()
    allowed_outbox_states = (
        {_CONNECTOR_TERMINAL_OUTBOX_STATUS}
        if was_completed
        else _TURN_CONTENT_TERMINAL_OUTBOX_STATES
    )
    for outbox_id, outbox_status, updated_at, delivery_id, delivery_status, audit_at in anchors:
        if (
            outbox_id is None
            or str(outbox_status) not in allowed_outbox_states
            or not updated_at
            or (not allow_young and str(updated_at) >= str(cutoff_at))
        ):
            return "outbox"
        if delivery_id is not None and (
            str(delivery_status) not in _TURN_CONTENT_TERMINAL_ATTEMPT_STATES
            or not audit_at
            or (not allow_young and str(audit_at) >= str(cutoff_at))
        ):
            return "delivery"
    return None


def _superseded_plan_summary_conn(
    conn: sqlite3.Connection,
    *,
    plan: sqlite3.Row | tuple[Any, ...],
) -> tuple[bool, int]:
    plan_id = int(plan[0])
    host_id = str(plan[1])
    name = str(plan[2])
    turn_id = str(plan[4])
    state = str(plan[6])
    activated_at = plan[8]
    source_outbox_id = int(plan[10]) if plan[10] is not None else None
    part_count = int(plan[11])
    if (
        source_outbox_id is None
        or state != "superseded"
        or activated_at is None
    ):
        return False, part_count
    newer_completed = conn.execute(
        """
        SELECT 1
        FROM turn_presentation_plans
        WHERE host_id = ? AND name = ? AND turn_id = ?
          AND id > ? AND state = 'completed' AND completed_at IS NOT NULL
        LIMIT 1
        """,
        (host_id, name, turn_id, plan_id),
    ).fetchone()
    if newer_completed is not None:
        return False, part_count
    latest_superseded = conn.execute(
        """
        SELECT COALESCE(MAX(id), 0)
        FROM turn_presentation_plans
        WHERE host_id = ? AND name = ? AND turn_id = ?
          AND state = 'superseded' AND activated_at IS NOT NULL
        """,
        (host_id, name, turn_id),
    ).fetchone()
    if int(latest_superseded[0] or 0) != plan_id:
        return False, part_count
    completed_baseline = conn.execute(
        """
        SELECT COALESCE(MAX(id), 0)
        FROM turn_presentation_plans
        WHERE host_id = ? AND name = ? AND turn_id = ?
          AND id < ? AND state = 'completed' AND completed_at IS NOT NULL
        """,
        (host_id, name, turn_id, plan_id),
    ).fetchone()
    baseline_id = int(completed_baseline[0] or 0)
    footprint = conn.execute(
        """
        SELECT COALESCE(MAX(part_count), 0)
        FROM turn_presentation_plans
        WHERE host_id = ? AND name = ? AND turn_id = ?
          AND activated_at IS NOT NULL AND id >= ?
        """,
        (host_id, name, turn_id, baseline_id),
    ).fetchone()
    return True, max(part_count, int(footprint[0] or 0))


def _delete_terminal_source_anchor_conn(
    conn: sqlite3.Connection,
    *,
    source_outbox_id: int,
    host_id: str,
    turn_id: str,
    content_revision_value: str,
    cutoff_at: str,
    source_status: str,
    allow_young: bool = False,
) -> dict[str, int]:
    reason = _terminal_source_anchor_reference_reason_conn(
        conn,
        source_outbox_id=int(source_outbox_id),
        host_id=str(host_id),
        turn_id=str(turn_id),
        content_revision_value=str(content_revision_value),
        source_status=str(source_status),
        cutoff_at=str(cutoff_at),
        allow_young=bool(allow_young),
    )
    if reason is not None:
        return {"queue_anchors": 0, "attempts": 0}
    remaining_reference = conn.execute(
        """
        SELECT 1
        FROM turn_presentation_plans
        WHERE source_outbox_id = ?
        LIMIT 1
        """,
        (int(source_outbox_id),),
    ).fetchone()
    if remaining_reference is not None:
        return {"queue_anchors": 0, "attempts": 0}
    attempts_deleted = int(
        conn.execute(
            "DELETE FROM connector_deliveries WHERE outbox_id = ?",
            (int(source_outbox_id),),
        ).rowcount
        or 0
    )
    anchor_deleted = int(
        conn.execute(
            """
            DELETE FROM connector_outbox
            WHERE id = ? AND host_id = ? AND connector = ?
              AND delivery_kind = 'final_ready' AND turn_id = ?
              AND content_revision = ? AND status = ?
              AND updated_at IS NOT NULL
              AND (? = 1 OR updated_at < ?)
              AND NOT EXISTS (
                  SELECT 1
                  FROM turn_presentation_plans
                  WHERE source_outbox_id = connector_outbox.id
              )
            """,
            (
                int(source_outbox_id),
                str(host_id),
                _TURN_FINAL_NAME,
                str(turn_id),
                str(content_revision_value),
                str(source_status),
                int(bool(allow_young)),
                str(cutoff_at),
            ),
        ).rowcount
        or 0
    )
    if anchor_deleted != 1:
        raise StoreSchemaError("turn_content_source_cleanup_race")
    return {
        "queue_anchors": anchor_deleted,
        "attempts": attempts_deleted,
    }


def _delete_retained_plan_conn(
    conn: sqlite3.Connection,
    *,
    plan: sqlite3.Row | tuple[Any, ...],
    cutoff_at: str,
    retain_summary: bool,
    summary_part_count: int,
    retention_count: int,
) -> tuple[dict[str, int], bool]:
    plan_id = int(plan[0])
    host_id = str(plan[1])
    name = str(plan[2])
    turn_id = str(plan[4])
    content_revision_value = str(plan[5])
    state = str(plan[6])
    was_completed = state == "completed" or (
        state == "superseded" and plan[9] is not None
    )
    source_status = (
        _CONNECTOR_TERMINAL_OUTBOX_STATUS
        if was_completed
        else _CONNECTOR_SUPERSEDED_OUTBOX_STATUS
    )
    delivered_rank = (
        _delivered_final_rank_conn(
            conn,
            host_id=host_id,
            source_outbox_id=int(plan[10]),
        )
        if was_completed and plan[10] is not None
        else None
    )
    allow_young = bool(
        delivered_rank is not None
        and int(delivered_rank[0]) > max(1, int(retention_count))
    )
    source_outbox_id = int(plan[10]) if plan[10] is not None else None
    outbox_ids = [
        int(row[0])
        for row in conn.execute(
            """
            SELECT outbox_id
            FROM turn_presentation_jobs
            WHERE plan_id = ? AND outbox_id IS NOT NULL
            """,
            (plan_id,),
        ).fetchall()
    ]
    recoveries_deleted = int(
        conn.execute(
            """
            DELETE FROM turn_presentation_recoveries
            WHERE failed_plan_id = ? OR recovered_plan_id = ?
            """,
            (plan_id, plan_id),
        ).rowcount
        or 0
    )
    attempts_deleted = 0
    queue_anchors_deleted = 0
    if outbox_ids:
        placeholders = ",".join("?" for _ in outbox_ids)
        attempts_deleted = int(
            conn.execute(
                f"DELETE FROM connector_deliveries WHERE outbox_id IN ({placeholders})",
                outbox_ids,
            ).rowcount
            or 0
        )
    jobs_deleted = int(
        conn.execute(
            "DELETE FROM turn_presentation_jobs WHERE plan_id = ?",
            (plan_id,),
        ).rowcount
        or 0
    )
    if outbox_ids:
        placeholders = ",".join("?" for _ in outbox_ids)
        queue_anchors_deleted = int(
            conn.execute(
                f"DELETE FROM connector_outbox WHERE id IN ({placeholders})",
                outbox_ids,
            ).rowcount
            or 0
        )
    if retain_summary:
        summary_updated = int(
            conn.execute(
                """
                UPDATE turn_presentation_plans
                SET source_outbox_id = NULL, part_count = ?
                WHERE id = ? AND state = 'superseded'
                """,
                (int(summary_part_count), plan_id),
            ).rowcount
            or 0
        )
        if summary_updated != 1:
            raise StoreSchemaError("turn_content_summary_cleanup_race")
        collapsed_plans_deleted = int(
            conn.execute(
                """
                DELETE FROM turn_presentation_plans
                WHERE host_id = ? AND name = ? AND turn_id = ?
                  AND id != ? AND state = 'superseded'
                  AND source_outbox_id IS NULL
                  AND activated_at IS NOT NULL AND activated_at < ?
                  AND NOT EXISTS (
                      SELECT 1
                      FROM turn_presentation_jobs AS jobs
                      WHERE jobs.plan_id = turn_presentation_plans.id
                  )
                  AND NOT EXISTS (
                      SELECT 1
                      FROM turn_presentation_recoveries AS recoveries
                      WHERE recoveries.failed_plan_id = turn_presentation_plans.id
                         OR recoveries.recovered_plan_id = turn_presentation_plans.id
                  )
                """,
                (host_id, name, turn_id, plan_id, str(cutoff_at)),
            ).rowcount
            or 0
        )
        plan_deleted = collapsed_plans_deleted
        changed = True
    else:
        plan_deleted = int(
            conn.execute(
                "DELETE FROM turn_presentation_plans WHERE id = ?",
                (plan_id,),
            ).rowcount
            or 0
        )
        changed = bool(plan_deleted)
    if source_outbox_id is not None:
        source_counts = _delete_terminal_source_anchor_conn(
            conn,
            source_outbox_id=source_outbox_id,
            host_id=host_id,
            turn_id=turn_id,
            content_revision_value=content_revision_value,
            source_status=source_status,
            allow_young=allow_young,
            cutoff_at=cutoff_at,
        )
        queue_anchors_deleted += int(source_counts["queue_anchors"])
        attempts_deleted += int(source_counts["attempts"])
    revision_deleted = 0
    revision = conn.execute(
        """
        SELECT rowid
        FROM turn_content_revisions
        WHERE host_id = ? AND turn_id = ? AND content_revision = ?
        """,
        (host_id, turn_id, content_revision_value),
    ).fetchone()
    if revision is not None:
        revision_deleted = int(
            _delete_superseded_revision_conn(
                conn,
                revision_rowid=int(revision[0]),
                host_id=host_id,
                cutoff_at=cutoff_at,
                allow_young=allow_young,
            )
        )
    return (
        {
            "plans": plan_deleted,
            "recoveries": recoveries_deleted,
            "jobs": jobs_deleted,
            "queue_anchors": queue_anchors_deleted,
            "attempts": attempts_deleted,
            "revisions": revision_deleted,
        },
        changed,
    )


def _delete_superseded_revision_conn(
    conn: sqlite3.Connection,
    *,
    revision_rowid: int,
    host_id: str,
    cutoff_at: str,
    allow_young: bool = False,
) -> bool:
    identity = conn.execute(
        """
        SELECT turn_id, content_revision
        FROM turn_content_revisions
        WHERE rowid = ? AND host_id = ?
        """,
        (int(revision_rowid), str(host_id)),
    ).fetchone()
    if identity is None or _typed_final_reference_exists_conn(
        conn,
        host_id=str(host_id),
        turn_id=str(identity[0]),
        content_revision_value=str(identity[1]),
    ):
        return False
    cursor = conn.execute(
        """
        DELETE FROM turn_content_revisions AS revisions
        WHERE revisions.rowid = ?
          AND revisions.host_id = ?
          AND revisions.is_current = 0
          AND revisions.superseded_at IS NOT NULL
          AND (? = 1 OR revisions.superseded_at < ?)
          AND NOT EXISTS (
              SELECT 1
              FROM turn_presentation_plans AS plans
              WHERE plans.host_id = revisions.host_id
                AND plans.turn_id = revisions.turn_id
                AND plans.content_revision = revisions.content_revision
          )
          AND NOT EXISTS (
              SELECT 1
              FROM connector_outbox AS outbox
              WHERE outbox.host_id = revisions.host_id
                AND outbox.connector = ?
                AND json_valid(outbox.payload_json)
                AND json_extract(
                    outbox.payload_json,
                    '$.content_revision'
                ) = revisions.content_revision
          )
          AND NOT EXISTS (
              SELECT 1
              FROM turns
              WHERE turns.host_id = revisions.host_id
                AND turns.turn_id = revisions.turn_id
                AND json_valid(turns.payload_json)
                AND (
                    json_extract(
                        turns.payload_json,
                        '$.content_revision'
                    ) = revisions.content_revision
                    OR json_extract(
                        turns.payload_json,
                        '$.content.content_revision'
                    ) = revisions.content_revision
                )
          )
        """,
        (
            int(revision_rowid),
            str(host_id),
            int(bool(allow_young)),
            str(cutoff_at),
            _TURN_FINAL_NAME,
        ),
    )
    deleted = bool(cursor.rowcount)
    if deleted:
        conn.execute(
            """
            DELETE FROM turn_content_page_boundaries
            WHERE host_id = ? AND turn_id = ? AND content_revision = ?
            """,
            (str(host_id), str(identity[0]), str(identity[1])),
        )
    return deleted


def cleanup_turn_content_retention(
    db_path: Path,
    host_id: str,
    *,
    retention_days: int,
    acknowledged_final_retention_count: int = ACKNOWLEDGED_FINAL_RETENTION_COUNT,
    now: str | None = None,
    dry_run: bool = False,
    batch_size: int = _TURN_CONTENT_MAINTENANCE_BATCH,
) -> dict[str, Any]:
    """Remove old presentation anchors, then superseded canonical revisions, in one bounded batch."""
    days = max(1, int(retention_days))
    bounded_batch = max(
        1,
        min(int(batch_size), _TURN_CONTENT_MAINTENANCE_BATCH_MAX),
    )
    bounded_retention_count = max(
        1,
        int(acknowledged_final_retention_count),
    )
    cutoff_at = _utc_cutoff(retention_days=days, now=now)
    empty_counts = {
        "plans": 0,
        "recoveries": 0,
        "jobs": 0,
        "queue_anchors": 0,
        "attempts": 0,
        "revisions": 0,
    }
    if not _sqlite_store_exists(db_path):
        return sanitize_public_value({
            "schema_version": 1,
            "ok": False,
            "status": "store_unavailable",
            "host_id": str(host_id),
            "dry_run": bool(dry_run),
            "retention_days": days,
            "cutoff_at": cutoff_at,
            "batch_size": bounded_batch,
            "examined": 0,
            "deleted": 0,
            "skipped_reference": 0,
            "deleted_rows": empty_counts,
        })
    deleted_rows = dict(empty_counts)
    skipped_reference = 0
    deleted = 0
    with _connect(db_path, isolation_level=None) as conn:
        _ensure_schema(conn)
        conn.execute("BEGIN IMMEDIATE")
        try:
            candidates = _turn_content_retention_candidates_conn(
                conn,
                host_id=str(host_id),
                cutoff_at=cutoff_at,
                retention_count=bounded_retention_count,
                batch_size=bounded_batch,
            )
            if not conn.in_transaction:
                conn.execute("BEGIN IMMEDIATE")
            for candidate_type, candidate_id in candidates:
                if candidate_type == "plan":
                    plan = conn.execute(
                        """
                        SELECT
                            id, host_id, name, plan_token, turn_id,
                            content_revision, state, created_at, activated_at,
                            completed_at, source_outbox_id, part_count
                        FROM turn_presentation_plans
                        WHERE id = ? AND host_id = ?
                        """,
                        (int(candidate_id), str(host_id)),
                    ).fetchone()
                    if plan is None:
                        skipped_reference += 1
                        continue
                    if _terminal_plan_reference_reason_conn(
                        conn,
                        plan=plan,
                        cutoff_at=cutoff_at,
                        retention_count=bounded_retention_count,
                    ) is not None:
                        skipped_reference += 1
                        continue
                    retain_summary, summary_part_count = (
                        _superseded_plan_summary_conn(conn, plan=plan)
                    )
                    plan_counts, changed = _delete_retained_plan_conn(
                        conn,
                        plan=plan,
                        cutoff_at=cutoff_at,
                        retain_summary=retain_summary,
                        summary_part_count=summary_part_count,
                        retention_count=bounded_retention_count,
                    )
                    if not changed:
                        skipped_reference += 1
                        continue
                    deleted += 1
                    for key, count in plan_counts.items():
                        deleted_rows[key] += int(count)
                    continue
                if candidate_type == "source":
                    source = conn.execute(
                        """
                        SELECT host_id, turn_id, content_revision
                        FROM connector_outbox
                        WHERE id = ?
                        """,
                        (int(candidate_id),),
                    ).fetchone()
                    if (
                        source is None
                        or source[1] is None
                        or source[2] is None
                    ):
                        skipped_reference += 1
                        continue
                    source_counts = _delete_terminal_source_anchor_conn(
                        conn,
                        source_outbox_id=int(candidate_id),
                        host_id=str(source[0]),
                        turn_id=str(source[1]),
                        content_revision_value=str(source[2]),
                        source_status=_CONNECTOR_SUPERSEDED_OUTBOX_STATUS,
                        cutoff_at=cutoff_at,
                    )
                    if not source_counts["queue_anchors"]:
                        skipped_reference += 1
                        continue
                    deleted += 1
                    deleted_rows["queue_anchors"] += int(
                        source_counts["queue_anchors"]
                    )
                    deleted_rows["attempts"] += int(
                        source_counts["attempts"]
                    )
                    continue
                if _delete_superseded_revision_conn(
                    conn,
                    revision_rowid=int(candidate_id),
                    host_id=str(host_id),
                    cutoff_at=cutoff_at,
                ):
                    deleted += 1
                    deleted_rows["revisions"] += 1
                else:
                    skipped_reference += 1
            if dry_run:
                conn.rollback()
            else:
                conn.commit()
        except Exception:
            conn.rollback()
            raise
    return sanitize_public_value({
        "schema_version": 1,
        "ok": True,
        "status": "ok",
        "host_id": str(host_id),
        "dry_run": bool(dry_run),
        "retention_days": days,
        "cutoff_at": cutoff_at,
        "stale_preparing_before": cutoff_at,
        "batch_size": bounded_batch,
        "examined": len(candidates),
        "deleted": deleted,
        "skipped_reference": skipped_reference,
        "deleted_rows": deleted_rows,
    })


def compact_turn_change_journal(
    db_path: Path | str,
    host_id: str,
    *,
    retention_days: int = TURN_CHANGE_RETENTION_DAYS,
    retention_count: int = TURN_CHANGE_RETENTION_COUNT,
    batch_size: int = TURN_CHANGE_COMPACTION_BATCH_SIZE,
    now: str | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Delete one bounded host batch older than both retention guarantees."""
    days = max(1, int(retention_days))
    count = max(1, int(retention_count))
    bounded_batch = min(10_000, max(1, int(batch_size)))
    current = _connector_datetime(now or utc_timestamp())
    cutoff_at = (current - timedelta(days=days)).isoformat()
    if not _sqlite_store_exists(db_path):
        return {
            "schema_version": 1, "ok": False, "status": "store_unavailable",
            "host_id": str(host_id), "examined": 0, "deleted": 0,
        }
    with _connect(db_path, isolation_level=None) as conn:
        _ensure_schema(conn)
        conn.execute("BEGIN IMMEDIATE")
        try:
            boundary = conn.execute(
                """
                SELECT seq FROM turn_change_journal
                WHERE host_id = ? ORDER BY seq DESC LIMIT 1 OFFSET ?
                """,
                (str(host_id), count - 1),
            ).fetchone()
            candidates = [] if boundary is None else conn.execute(
                """
                SELECT seq FROM turn_change_journal
                WHERE host_id = ? AND seq < ?
                  AND julianday(changed_at) IS NOT NULL
                  AND julianday(changed_at) < julianday(?)
                ORDER BY seq ASC LIMIT ?
                """,
                (str(host_id), int(boundary[0]), cutoff_at, bounded_batch),
            ).fetchall()
            sequences = [int(row[0]) for row in candidates]
            if sequences:
                placeholders = ",".join("?" for _ in sequences)
                conn.execute(
                    f"DELETE FROM turn_change_journal WHERE host_id = ? AND seq IN ({placeholders})",
                    (str(host_id), *sequences),
                )
                conn.execute(
                    """
                    INSERT INTO turn_change_floor(host_id, floor_seq) VALUES (?, ?)
                    ON CONFLICT(host_id) DO UPDATE SET
                        floor_seq = MAX(turn_change_floor.floor_seq, excluded.floor_seq)
                    """,
                    (str(host_id), max(sequences)),
                )
            remaining = bool(boundary is not None and conn.execute(
                """
                SELECT 1 FROM turn_change_journal
                WHERE host_id = ? AND seq < ?
                  AND julianday(changed_at) IS NOT NULL
                  AND julianday(changed_at) < julianday(?) LIMIT 1
                """,
                (str(host_id), int(boundary[0]), cutoff_at),
            ).fetchone())
            if dry_run:
                conn.rollback()
            else:
                conn.commit()
        except Exception:
            conn.rollback()
            raise
    return {
        "schema_version": 1, "ok": True, "status": "ok",
        "host_id": str(host_id), "dry_run": bool(dry_run),
        "retention_days": days, "retention_count": count,
        "batch_size": bounded_batch, "examined": len(sequences),
        "deleted": len(sequences), "remaining_candidates": remaining,
    }




def run_store_maintenance(
    db_path: Path,
    host_id: str,
    *,
    retention_days: int,
    max_outbox_attempts: int,
    acknowledged_final_retention_days: int = ACKNOWLEDGED_FINAL_RETENTION_DAYS,
    acknowledged_final_retention_count: int = ACKNOWLEDGED_FINAL_RETENTION_COUNT,
    command_retry_horizon_seconds: int = COMMAND_RETRY_HORIZON_SECONDS,
    command_receipt_retention_seconds: int = COMMAND_RECEIPT_RETENTION_SECONDS,
    command_receipt_retention_count: int = COMMAND_RECEIPT_RETENTION_COUNT,
    now: str | None = None,
    dry_run: bool = False,
    content_batch_size: int = _TURN_CONTENT_MAINTENANCE_BATCH,
    event_batch_size: int = 100,
    snapshot_retention_days: int = 14,
    snapshot_retention_count: int = 4096,
    snapshot_batch_size: int = 100,
    turn_change_retention_days: int = TURN_CHANGE_RETENTION_DAYS,
    turn_change_retention_count: int = TURN_CHANGE_RETENTION_COUNT,
    turn_change_batch_size: int = TURN_CHANGE_COMPACTION_BATCH_SIZE,
) -> dict[str, Any]:
    """Run one bounded batch for every online store-maintenance class."""
    retention = cleanup_event_retention(
        db_path,
        host_id,
        retention_days=retention_days,
        now=now,
        dry_run=dry_run,
        batch_size=event_batch_size,
    )
    agent_events = cleanup_agent_event_retention(
        db_path,
        host_id,
        retention_days=retention_days,
        now=now,
        dry_run=dry_run,
        batch_size=event_batch_size,
    )
    snapshots = cleanup_snapshot_retention(
        db_path,
        retention_days=snapshot_retention_days,
        retention_count=snapshot_retention_count,
        batch_size=snapshot_batch_size,
        now=now,
        dry_run=dry_run,
    )
    outbox = exhaust_connector_retries(
        db_path,
        host_id,
        max_attempts=max_outbox_attempts,
        now=now,
        dry_run=dry_run,
    )
    final_retention = cleanup_acknowledged_final_retention(
        db_path,
        host_id,
        acknowledged_final_retention_days=acknowledged_final_retention_days,
        acknowledged_final_retention_count=acknowledged_final_retention_count,
        now=now,
        dry_run=dry_run,
        batch_size=content_batch_size,
    )
    turn_content = cleanup_turn_content_retention(
        db_path,
        host_id,
        retention_days=retention_days,
        acknowledged_final_retention_count=acknowledged_final_retention_count,
        now=now,
        dry_run=dry_run,
        batch_size=content_batch_size,
    )
    command_request_result = cleanup_command_request_retention(
        db_path,
        retry_horizon_seconds=command_retry_horizon_seconds,
        retention_seconds=command_receipt_retention_seconds,
        retention_count=command_receipt_retention_count,
        host_id=str(host_id),
        now=now,
        dry_run=dry_run,
        batch_size=content_batch_size,
    )
    command_requests = _command_request_maintenance_summary(
        command_request_result,
        retry_horizon_seconds=command_retry_horizon_seconds,
        retention_seconds=command_receipt_retention_seconds,
        retention_count=command_receipt_retention_count,
        batch_size=content_batch_size,
    )
    turn_changes = compact_turn_change_journal(
        db_path,
        host_id,
        retention_days=turn_change_retention_days,
        retention_count=turn_change_retention_count,
        batch_size=turn_change_batch_size,
        now=now,
        dry_run=dry_run,
    )
    ok = (
        bool(retention.get("ok"))
        and bool(agent_events.get("ok"))
        and bool(snapshots.get("ok"))
        and bool(outbox.get("ok"))
        and bool(final_retention.get("ok"))
        and bool(turn_content.get("ok"))
        and bool(command_requests.get("ok"))
        and bool(turn_changes.get("ok"))
    )
    return sanitize_public_value({
        "schema_version": 1,
        "ok": ok,
        "status": "ok" if ok else "store_unavailable",
        "host_id": str(host_id),
        "dry_run": bool(dry_run),
        "retention": {
            "retention_days": int(retention.get("retention_days") or retention_days),
            "cutoff_at": retention.get("cutoff_at"),
            "batch_size": int(retention.get("batch_size") or event_batch_size),
            "examined": int(retention.get("examined") or 0),
            "deleted": int(retention.get("deleted") or 0),
            "remaining_candidates": bool(
                retention.get("remaining_candidates")
            ),
        },
        "agent_events": {
            "retention_days": int(
                agent_events.get("retention_days") or retention_days
            ),
            "cutoff_at": agent_events.get("cutoff_at"),
            "batch_size": int(
                agent_events.get("batch_size") or event_batch_size
            ),
            "examined": int(agent_events.get("examined") or 0),
            "deleted": int(agent_events.get("deleted") or 0),
            "remaining_candidates": bool(
                agent_events.get("remaining_candidates")
            ),
        },
        "snapshots": {
            "scope": "database",
            "retention_days": int(
                snapshots.get("retention_days") or snapshot_retention_days
            ),
            "retention_count": int(
                snapshots.get("retention_count") or snapshot_retention_count
            ),
            "cutoff_at": snapshots.get("cutoff_at"),
            "batch_size": int(
                snapshots.get("batch_size") or snapshot_batch_size
            ),
            "examined": int(snapshots.get("examined") or 0),
            "deleted": int(snapshots.get("deleted") or 0),
            "eligible": int(snapshots.get("eligible") or 0),
            "remaining_candidates": bool(
                snapshots.get("remaining_candidates")
            ),
            "latest_hosts_retained": int(
                snapshots.get("latest_hosts_retained") or 0
            ),
        },
        "outbox": {
            "max_attempts": int(outbox.get("max_attempts") or max_outbox_attempts),
            "updated": int(outbox.get("updated") or 0),
        },
        "final_retention": {
            "dry_run": bool(final_retention.get("dry_run")),
            "acknowledged_final_retention_days": int(
                final_retention.get("acknowledged_final_retention_days")
                or acknowledged_final_retention_days
            ),
            "acknowledged_final_retention_count": int(
                final_retention.get("acknowledged_final_retention_count")
                or acknowledged_final_retention_count
            ),
            "cutoff_at": final_retention.get("cutoff_at"),
            "batch_size": int(
                final_retention.get("batch_size") or content_batch_size
            ),
            "examined": int(final_retention.get("examined") or 0),
            "deleted": int(final_retention.get("deleted") or 0),
            "remaining_candidates": bool(
                final_retention.get("remaining_candidates")
            ),
            "deleted_rows": dict(
                final_retention.get("deleted_rows")
                or {
                    "recoveries": 0,
                    "attempts": 0,
                    "jobs": 0,
                    "plans": 0,
                    "anchors": 0,
                    "revisions": 0,
                    "turns": 0,
                }
            ),
        },
        "command_requests": command_requests,
        "turn_changes": {
            "dry_run": bool(turn_changes.get("dry_run")),
            "retention_days": int(turn_changes.get("retention_days") or turn_change_retention_days),
            "retention_count": int(turn_changes.get("retention_count") or turn_change_retention_count),
            "batch_size": int(turn_changes.get("batch_size") or turn_change_batch_size),
            "examined": int(turn_changes.get("examined") or 0),
            "deleted": int(turn_changes.get("deleted") or 0),
            "remaining_candidates": bool(turn_changes.get("remaining_candidates")),
        },
        "turn_content": {
            "dry_run": bool(turn_content.get("dry_run")),
            "retention_days": int(
                turn_content.get("retention_days") or retention_days
            ),
            "cutoff_at": turn_content.get("cutoff_at"),
            "stale_preparing_before": turn_content.get(
                "stale_preparing_before"
            ),
            "batch_size": int(
                turn_content.get("batch_size") or content_batch_size
            ),
            "examined": int(turn_content.get("examined") or 0),
            "deleted": int(turn_content.get("deleted") or 0),
            "skipped_reference": int(
                turn_content.get("skipped_reference") or 0
            ),
            "deleted_rows": dict(
                turn_content.get("deleted_rows")
                or {
                    "plans": 0,
                    "recoveries": 0,
                    "jobs": 0,
                    "queue_anchors": 0,
                    "attempts": 0,
                    "revisions": 0,
                }
            ),
        },
    })


_TURN_CONTENT_FIELDS = frozenset(
    {
        "user_text",
        "assistant_final_text",
        "assistant_stream_text",
        "model",
        "complete",
        "has_open_turn",
        "awaiting_input",
        "pending_decision",
        "source_turn_id",
    }
)


_TURN_IDENTITY_SEED_FIELDS = (
    "schema_version",
    "host_id",
    "worker_id",
    "worker_fingerprint",
    "space_id",
    "status",
    "kind",
    "source",
    "title",
    "summary",
    "meta",
)


def _turn_merge_match_text(value: Any) -> str:
    return normalize_instruction_text(value)


def _turn_continuity_identity(payload: Mapping[str, Any]) -> tuple[str, str, int] | None:
    meta = payload.get("meta")
    if isinstance(meta, Mapping):
        stable_key = meta.get("stable_key")
        stable_key_version = meta.get("stable_key_version")
        if (
            _valid_final_stable_key(stable_key)
            and type(stable_key_version) is int
            and stable_key_version == 1
        ):
            return ("stable_key", str(stable_key), 1)
    return None


def _turn_submission_owner_identity(worker: Any) -> tuple[str, int] | None:
    """Extract the stable continuity owner required for submission linking."""
    meta = getattr(worker, "meta", None)
    if meta is None and isinstance(worker, Mapping):
        meta = worker.get("meta")
    identity = _turn_continuity_identity(
        {"meta": meta if isinstance(meta, Mapping) else {}}
    )
    if identity is not None:
        _kind, owner_key, owner_key_version = identity
        return owner_key, owner_key_version
    return None


def _turn_submission_policy(
    link_window_seconds: Any,
    hard_ttl_seconds: Any,
) -> tuple[int, int]:
    values = (link_window_seconds, hard_ttl_seconds)
    if any(
        isinstance(value, bool) or not isinstance(value, int) or value <= 0
        for value in values
    ):
        raise ValueError("turn submission windows must be positive integers")
    link_window = int(link_window_seconds)
    hard_ttl = int(hard_ttl_seconds)
    if hard_ttl < link_window:
        raise ValueError("turn submission hard TTL must cover the link window")
    return link_window, hard_ttl


def _insert_turn_submission_conn(
    conn: sqlite3.Connection,
    *,
    host_id: str,
    request_id: str,
    worker: Any,
    instruction_text: str,
    current: str,
    link_window_seconds: int,
    hard_ttl_seconds: int,
) -> str:
    """Insert the send-started submission row in the caller transaction."""
    link_window, hard_ttl = _turn_submission_policy(
        link_window_seconds,
        hard_ttl_seconds,
    )
    owner_identity = _turn_submission_owner_identity(worker)
    if owner_identity is None:
        raise StoreSchemaError("turn_submission_stable_owner_missing")
    owner_key, owner_key_version = owner_identity
    submission_id = turn_submission_id(host_id, request_id)
    current_time = datetime.fromisoformat(current)
    link_not_before = (current_time - timedelta(seconds=link_window)).isoformat(
        timespec="seconds"
    )
    link_expires_at = (current_time + timedelta(seconds=link_window)).isoformat(
        timespec="seconds"
    )
    hard_expires_at = (current_time + timedelta(seconds=hard_ttl)).isoformat(
        timespec="seconds"
    )
    fingerprint = instruction_fingerprint(instruction_text)
    conn.execute(
        """
        INSERT INTO turn_submissions (
            host_id, submission_id, request_id, owner_key,
            owner_key_version, instruction_fingerprint, state,
            linked_turn_id, link_not_before, link_expires_at,
            hard_expires_at, linked_at, terminal_at, submitted_at,
            send_started_at, updated_at
        ) VALUES (
            ?, ?, ?, ?, ?, ?, 'send_started', NULL, ?, ?, ?,
            NULL, NULL, NULL, ?, ?
        )
        ON CONFLICT DO NOTHING
        """,
        (
            str(host_id),
            submission_id,
            str(request_id),
            owner_key,
            owner_key_version,
            fingerprint,
            link_not_before,
            link_expires_at,
            hard_expires_at,
            current,
            current,
        ),
    )
    _rearm_submission_link_component_conn(
        conn,
        str(host_id),
        owner_key,
        fingerprint,
    )
    return submission_id


def _turn_submission_transition_sources(next_state: str) -> tuple[str, ...]:
    """Return states that may advance to ``next_state`` under the contract."""
    return tuple(
        current_state
        for current_state in TURN_SUBMISSION_STATE_TRANSITIONS
        if is_valid_turn_submission_state_transition(current_state, next_state)
    )


def _terminalize_turn_submission_conn(
    conn: sqlite3.Connection,
    *,
    host_id: str,
    request_id: str,
    terminal_state: str,
    current: str,
) -> bool:
    """Advance a submission row with its command receipt transaction."""
    next_state = {
        "accepted": "submitted",
        "rejected": "cancelled",
        "uncertain": "uncertain",
    }.get(str(terminal_state))
    if next_state is None:
        return False
    row = conn.execute(
        """
        SELECT state
        FROM turn_submissions
        WHERE host_id = ? AND request_id = ?
        """,
        (str(host_id), str(request_id)),
    ).fetchone()
    if row is None:
        # Answer commands and receipts created before Stage 2 have no ledger row.
        return False
    current_state = str(row[0])
    if not is_valid_turn_submission_state_transition(current_state, next_state):
        # Expiry/cancellation/linkage may have won before a late receipt write.
        return False
    updated = conn.execute(
        """
        UPDATE turn_submissions
        SET state = ?,
            terminal_at = ?,
            submitted_at = CASE WHEN ? = 'submitted' THEN ? ELSE submitted_at END,
            updated_at = ?
        WHERE host_id = ? AND request_id = ? AND state = ?
        """,
        (
            next_state,
            current,
            next_state,
            current,
            current,
            str(host_id),
            str(request_id),
            current_state,
        ),
    )
    if int(updated.rowcount or 0) != 1:
        raise StoreSchemaError("turn_submission_terminal_transition_failed")
    return True


def _expire_turn_submissions_conn(
    conn: sqlite3.Connection,
    *,
    current: str,
    host_id: str | None = None,
) -> int:
    source_states = _turn_submission_transition_sources("expired")
    if not source_states:
        return 0
    state_params = {
        f"transition_state_{index}": state
        for index, state in enumerate(source_states)
    }
    state_placeholders = ", ".join(
        f":{name}" for name in state_params
    )
    scope_sql = "" if host_id is None else "AND host_id = :host_id"
    updated = conn.execute(
        f"""
        UPDATE turn_submissions
        SET state = 'expired', terminal_at = :current, updated_at = :current
        WHERE linked_turn_id IS NULL
          AND state IN ({state_placeholders})
          AND julianday(hard_expires_at) <= julianday(:current)
          {scope_sql}
        """,
        {"current": current, "host_id": host_id} | state_params,
    )
    return int(updated.rowcount or 0)


def _submission_link_candidate_turns_conn(
    conn: sqlite3.Connection,
    *,
    host_id: str,
    owner_key: str,
    instruction_fingerprint_value: str,
) -> list[tuple[str, int, datetime]]:
    """Return unlinked, source-identity observations for one match component."""
    rows = conn.execute(
        """
        SELECT turns.turn_id,
               turns.worker_id,
               json_extract(turns.payload_json, '$.meta.stable_key'),
               json_extract(turns.payload_json, '$.meta.stable_key_version'),
               json_extract(turns.payload_json, '$.source_turn_id'),
               revisions.user_text,
               MIN(history.created_at)
        FROM turns
        JOIN turn_content_revisions AS revisions
          ON revisions.host_id = turns.host_id
         AND revisions.turn_id = turns.turn_id
         AND revisions.is_current = 1
        JOIN turn_content_revisions AS history
          ON history.host_id = turns.host_id
         AND history.turn_id = turns.turn_id
        WHERE turns.host_id = ?
          AND json_valid(turns.payload_json)
          AND json_type(turns.payload_json, '$.id') = 'text'
          AND json_extract(turns.payload_json, '$.id') = turns.turn_id
          AND COALESCE(json_extract(turns.payload_json, '$.source_turn_id'), '') != ''
          AND COALESCE(json_extract(turns.payload_json, '$.superseded_at'), '') = ''
          AND NOT EXISTS (
              SELECT 1
              FROM turn_submissions AS linked
              WHERE linked.host_id = turns.host_id
                AND linked.linked_turn_id = turns.turn_id
          )
        GROUP BY turns.host_id, turns.turn_id, turns.worker_id,
                 json_extract(turns.payload_json, '$.meta.stable_key'),
                 json_extract(turns.payload_json, '$.meta.stable_key_version'),
                 json_extract(turns.payload_json, '$.source_turn_id'),
                 revisions.user_text
        ORDER BY turns.turn_id
        """,
        (str(host_id),),
    ).fetchall()
    candidates: list[tuple[str, int, datetime]] = []
    for (
        turn_id,
        worker_id,
        stable_key,
        stable_key_version,
        source_turn_id,
        user_text,
        first_observed_at,
    ) in rows:
        if not isinstance(source_turn_id, str) or not source_turn_id.strip():
            continue
        owner_identity = _turn_submission_owner_identity(
            {
                "id": str(worker_id),
                "meta": {
                    "stable_key": stable_key,
                    "stable_key_version": stable_key_version,
                },
            }
        )
        if owner_identity is None:
            continue
        candidate_owner, owner_version = owner_identity
        if owner_version != 1 or candidate_owner != str(owner_key):
            continue
        if user_text is None:
            continue
        if instruction_fingerprint(user_text) != instruction_fingerprint_value:
            continue
        canonical_observed_at = _strict_utc_timestamp(first_observed_at)
        if canonical_observed_at is None:
            continue
        candidates.append(
            (
                str(turn_id),
                int(owner_version),
                datetime.fromisoformat(canonical_observed_at),
            )
        )
    return candidates


def _settle_submission_links_conn(
    conn: sqlite3.Connection,
    host_id: str,
    owner_key: str,
    instruction_fingerprint_value: str,
    *,
    now: str | None = None,
    _next_eligible_out: list[datetime | None] | None = None,
) -> int:
    """Settle unambiguous live links and fail closed for degraded components.

    The caller owns the surrounding ``BEGIN IMMEDIATE`` transaction. Candidate
    edges exist only when an observation falls inside a submission's symmetric
    link window. A sole fresh open submission may link immediately to its first
    post-send observation. Multiple same-fingerprint submissions, uncertain
    submissions, and stale send-started submissions retain the window-close
    graph settlement. Disconnected windows settle independently; any connected
    component larger than 1x1 terminalizes its submissions as ambiguous.
    """
    current, current_dt = _pending_observed_time(now)
    open_states = ("send_started", "submitted", "uncertain")
    submissions = []
    for row in conn.execute(
        """
        SELECT submission_id, owner_key_version, state,
               link_not_before, link_expires_at,
               submitted_at, send_started_at
        FROM turn_submissions
        WHERE host_id = ?
          AND owner_key = ?
          AND instruction_fingerprint = ?
          AND linked_turn_id IS NULL
          AND state IN (?, ?, ?)
        ORDER BY submission_id
        """,
        (
            str(host_id),
            str(owner_key),
            str(instruction_fingerprint_value),
            *open_states,
        ),
    ).fetchall():
        lower = _strict_utc_timestamp(row[3])
        upper = _strict_utc_timestamp(row[4])
        if lower is None or upper is None:
            continue
        submitted_at = _strict_utc_timestamp(row[5])
        send_started_at = _strict_utc_timestamp(row[6])
        submissions.append(
            (
                str(row[0]),
                int(row[1]),
                str(row[2]),
                datetime.fromisoformat(lower),
                datetime.fromisoformat(upper),
                (
                    None
                    if submitted_at is None
                    else datetime.fromisoformat(submitted_at)
                ),
                (
                    None
                    if send_started_at is None
                    else datetime.fromisoformat(send_started_at)
                ),
            )
        )
    if not submissions:
        if _next_eligible_out is not None:
            _next_eligible_out.append(None)
        return 0

    turns = _submission_link_candidate_turns_conn(
        conn,
        host_id=str(host_id),
        owner_key=str(owner_key),
        instruction_fingerprint_value=str(instruction_fingerprint_value),
    )
    submission_edges: dict[str, set[str]] = {
        submission_id: set()
        for (
            submission_id,
            _owner_version,
            _state,
            _lower,
            _upper,
            _submitted_at,
            _send_started_at,
        ) in submissions
    }
    turn_edges: dict[str, set[str]] = {
        turn_id: set() for turn_id, _owner_version, _at in turns
    }
    submission_expires = {
        submission_id: upper
        for (
            submission_id,
            _owner_version,
            _state,
            _lower,
            upper,
            _submitted_at,
            _send_started_at,
        ) in submissions
    }
    submission_details = {
        submission_id: (state, submitted_at, send_started_at)
        for (
            submission_id,
            _owner_version,
            state,
            _lower,
            _upper,
            submitted_at,
            send_started_at,
        ) in submissions
    }
    turn_observed_at = {
        turn_id: observed_at
        for turn_id, _owner_version, observed_at in turns
    }
    for (
        submission_id,
        owner_version,
        state,
        lower,
        upper,
        submitted_at,
        _send_started_at,
    ) in submissions:
        # Turn ingestion records when an observation first reached Tendwire,
        # not when the underlying agent turn started. record_command_send_queued
        # stamps submitted_at while retaining send_started, which marks a
        # written_to_pty prompt that cannot safely use observations as evidence.
        # A pre-verdict send_started row has submitted_at NULL and may still be
        # linked provisionally; the authoritative queued verdict clears it.
        if state == "send_started" and submitted_at is not None:
            continue
        for turn_id, turn_owner_version, observed_at in turns:
            if owner_version == turn_owner_version and lower <= observed_at <= upper:
                submission_edges[submission_id].add(turn_id)
                turn_edges[turn_id].add(submission_id)

    changed = 0
    waiting_boundaries: list[datetime] = []

    visited_submissions: set[str] = set()
    for (
        initial_submission,
        _owner_version,
        _state,
        _lower,
        _upper,
        _submitted_at,
        _send_started_at,
    ) in submissions:
        if initial_submission in visited_submissions:
            continue
        if not submission_edges[initial_submission]:
            # A no-edge submission is its own disconnected component. Surface
            # the missed link as soon as its window closes even when another
            # same-fingerprint component remains open.
            boundary = submission_expires[initial_submission]
            if current_dt < boundary:
                waiting_boundaries.append(boundary)
                continue
            updated = conn.execute(
                """
                UPDATE turn_submissions
                SET state = 'expired', terminal_at = ?, updated_at = ?
                WHERE host_id = ? AND submission_id = ?
                  AND linked_turn_id IS NULL
                  AND state IN (?, ?, ?)
                """,
                (
                    current,
                    current,
                    str(host_id),
                    initial_submission,
                    *open_states,
                ),
            )
            changed += int(updated.rowcount or 0)
            continue
        component_submissions: set[str] = set()
        component_turns: set[str] = set()
        pending_submissions = [initial_submission]
        pending_turns: list[str] = []
        while pending_submissions or pending_turns:
            while pending_submissions:
                submission_id = pending_submissions.pop()
                if submission_id in component_submissions:
                    continue
                component_submissions.add(submission_id)
                pending_turns.extend(submission_edges[submission_id])
            while pending_turns:
                turn_id = pending_turns.pop()
                if turn_id in component_turns:
                    continue
                component_turns.add(turn_id)
                pending_submissions.extend(turn_edges[turn_id])
        visited_submissions.update(component_submissions)

        # A 1:1 decision is irreversible. Wait until every submission window in
        # the connected component is closed so a later in-window observation
        # cannot turn an apparent match into an ambiguity.
        component_boundary = max(
            submission_expires[submission_id]
            for submission_id in component_submissions
        )
        if current_dt < component_boundary:
            # Only a live 1x1 component is provably unambiguous. Any second
            # same-fingerprint submission or candidate retains the existing
            # fail-closed window settlement. Uncertain and stale send-started
            # submissions stay on that degraded path too.
            if len(component_submissions) == len(component_turns) == 1:
                submission_id = next(iter(component_submissions))
                turn_id = next(iter(component_turns))
                state, submitted_at, send_started_at = submission_details[
                    submission_id
                ]
                stale_send_started = state == "send_started" and (
                    send_started_at is None
                    or current_dt
                    > send_started_at
                    + timedelta(seconds=SUBMISSION_SEND_ACK_TIMEOUT_SECONDS)
                )
                instant_anchor = (
                    send_started_at if state == "send_started" else submitted_at
                )
                if (
                    state != "uncertain"
                    and not stale_send_started
                    and instant_anchor is not None
                    and turn_observed_at[turn_id] <= current_dt
                    # Timestamps are stored to whole seconds. Equality cannot
                    # prove the observation followed the send boundary.
                    and turn_observed_at[turn_id] > instant_anchor
                ):
                    updated = conn.execute(
                        """
                        UPDATE turn_submissions
                        SET linked_turn_id = ?, state = 'linked', linked_at = ?,
                            updated_at = ?
                        WHERE host_id = ? AND submission_id = ?
                          AND linked_turn_id IS NULL
                          AND state IN (?, ?, ?)
                        """,
                        (
                            turn_id,
                            current,
                            current,
                            str(host_id),
                            submission_id,
                            *open_states,
                        ),
                    )
                    changed += int(updated.rowcount or 0)
                    if int(updated.rowcount or 0) == 1:
                        continue
            waiting_boundaries.append(component_boundary)
            continue

        if len(component_submissions) == len(component_turns) == 1:
            submission_id = next(iter(component_submissions))
            turn_id = next(iter(component_turns))
            updated = conn.execute(
                """
                UPDATE turn_submissions
                SET linked_turn_id = ?, state = 'linked', linked_at = ?,
                    updated_at = ?
                WHERE host_id = ? AND submission_id = ?
                  AND linked_turn_id IS NULL
                  AND state IN (?, ?, ?)
                """,
                (
                    turn_id,
                    current,
                    current,
                    str(host_id),
                    submission_id,
                    *open_states,
                ),
            )
            if int(updated.rowcount or 0) == 1:
                changed += 1
            continue

        placeholders = ", ".join("?" for _ in component_submissions)
        updated = conn.execute(
            f"""
            UPDATE turn_submissions
            SET state = 'ambiguous', terminal_at = ?, updated_at = ?
            WHERE host_id = ?
              AND submission_id IN ({placeholders})
              AND linked_turn_id IS NULL
              AND state IN (?, ?, ?)
            """,
            (
                current,
                current,
                str(host_id),
                *sorted(component_submissions),
                *open_states,
            ),
        )
        changed += int(updated.rowcount or 0)
    if _next_eligible_out is not None:
        _next_eligible_out.append(
            min(waiting_boundaries) if waiting_boundaries else None
        )
    return changed


def _settle_due_submission_links_conn(
    conn: sqlite3.Connection,
    *,
    db_path: Path | str,
    host_id: str | None = None,
    now: str | None = None,
) -> int:
    """Settle every due open owner/fingerprint component in one transaction."""
    current, current_dt = _pending_observed_time(now)
    scope_sql = "" if host_id is None else "AND host_id = :host_id"
    rows = conn.execute(
        f"""
        SELECT DISTINCT host_id, owner_key, instruction_fingerprint
        FROM turn_submissions
        WHERE linked_turn_id IS NULL
          AND state IN ('send_started', 'submitted', 'uncertain')
          AND julianday(link_not_before) <= julianday(:current)
          {scope_sql}
        ORDER BY host_id, owner_key, instruction_fingerprint
        """,
        {"current": current, "host_id": host_id},
    ).fetchall()
    active_keys = {
        _submission_link_backoff_key(
            db_path,
            str(candidate_host),
            str(owner_key),
            str(fingerprint),
        )
        for candidate_host, owner_key, fingerprint in rows
    }
    _prune_submission_link_backoff(db_path, host_id, active_keys)
    changed = 0
    for candidate_host, owner_key, fingerprint in rows:
        key = _submission_link_backoff_key(
            db_path,
            str(candidate_host),
            str(owner_key),
            str(fingerprint),
        )
        if not _submission_link_component_is_due(key, current_dt):
            continue
        next_eligible: list[datetime | None] = []
        component_changed = _settle_submission_links_conn(
            conn,
            str(candidate_host),
            str(owner_key),
            str(fingerprint),
            now=current,
            _next_eligible_out=next_eligible,
        )
        changed += component_changed
        if component_changed:
            _rearm_submission_link_component(
                db_path,
                str(candidate_host),
                str(owner_key),
                str(fingerprint),
            )
        elif next_eligible:
            _backoff_submission_link_component(key, next_eligible[0], current=current_dt)
    return changed


def _turn_uses_current_canonical_identity(
    turn_id: str,
    payload: Mapping[str, Any],
) -> bool:
    try:
        return Turn.from_dict(payload).id == str(turn_id)
    except (TypeError, ValueError):
        return False


def _turn_content_matches_origin(payload: Mapping[str, Any], content: Mapping[str, Any]) -> bool:
    incoming_user = _turn_merge_match_text(content.get("user_text"))
    if not incoming_user:
        return False
    return incoming_user == _turn_merge_match_text(payload.get("user_text"))


def _current_worker_turn_projection(
    host_id: str,
    worker_id: str,
    worker_payload: Mapping[str, Any],
) -> dict[str, Any]:
    meta = worker_payload.get("meta")
    clean_meta = dict(meta) if isinstance(meta, Mapping) else {}
    origin_command_id = clean_meta.get("origin_command_id")
    return sanitize_public_mapping(
        Turn(
            host_id=str(host_id),
            worker_id=str(worker_id),
            worker_fingerprint=str(worker_payload.get("fingerprint") or ""),
            space_id=worker_payload.get("space_id"),
            status=str(worker_payload.get("status") or "unknown"),
            kind="task",
            source=f"worker:{worker_id}",
            title=worker_payload.get("name"),
            summary=worker_payload.get("summary"),
            updated_at=worker_payload.get("last_seen_at"),
            origin_command_id=(
                str(origin_command_id)
                if str(origin_command_id or "").strip()
                else None
            ),
            meta=clean_meta,
        ).to_dict()
    )


def _observed_turn_payload(
    turn_id: str,
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    """Sanitize one source-observed row without command-derived identity."""
    normalized = Turn.from_dict(payload).to_dict()
    normalized["id"] = str(turn_id)
    normalized.pop("origin_command_id", None)
    normalized.update(_public_pending_turn_extension(payload))
    return _strip_canonical_turn_payload(normalized)


def _public_pending_turn_extension(
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    """Keep only the safe, opt-in pending projection outside the frozen Turn model."""
    if payload.get("awaiting_input") is not True:
        return {}
    decision = sanitize_public_mapping(payload.get("pending_decision"))
    if not decision:
        return {}
    return {
        "awaiting_input": True,
        "pending_decision": decision,
    }


def _update_observed_turn_row(
    conn: sqlite3.Connection,
    host_id: str,
    turn_id: str,
    payload: Mapping[str, Any],
    current_time: str,
    *,
    snapshot_content_fingerprint: str | None = None,
) -> tuple[bool, dict[str, Any]]:
    """Update an existing source-observed turn row."""
    item = _observed_turn_payload(str(turn_id), payload)
    encoded = _canonical_json(item)
    row = conn.execute(
        """
        SELECT
            worker_id,
            worker_fingerprint,
            space_id,
            status,
            kind,
            updated_at,
            fingerprint,
            snapshot_content_fingerprint,
            observed_at,
            payload_json
        FROM turns
        WHERE host_id = ? AND turn_id = ?
        """,
        (str(host_id), str(turn_id)),
    ).fetchone()
    if row is None:
        raise StoreSchemaError("turn_persisted_identity_missing")
    next_snapshot_fingerprint = (
        str(snapshot_content_fingerprint)
        if snapshot_content_fingerprint is not None
        else str(row[7] or "")
    )
    stored_observed_at = str(row[8] or "")
    next_observed_at = (
        str(current_time)
        if _turn_observation_is_newer(
            str(current_time),
            stored_observed_at,
        )
        else stored_observed_at
    )
    values = (
        str(item.get("worker_id") or ""),
        item.get("worker_fingerprint"),
        item.get("space_id"),
        str(item.get("status") or "unknown"),
        str(item.get("kind") or "unknown"),
        item.get("updated_at") or row[5] or current_time,
        str(item.get("fingerprint") or ""),
        next_snapshot_fingerprint,
        encoded,
    )
    material_row = (*tuple(row[:8]), row[9])
    material_changed = material_row != values
    if not material_changed and stored_observed_at == next_observed_at:
        return False, item
    conn.execute(
        """
        UPDATE turns
        SET worker_id = ?,
            worker_fingerprint = ?,
            space_id = ?,
            status = ?,
            kind = ?,
            updated_at = ?,
            fingerprint = ?,
            snapshot_content_fingerprint = ?,
            observed_at = ?,
            payload_json = ?
        WHERE host_id = ? AND turn_id = ?
        """,
        (
            values[0],
            values[1],
            values[2],
            values[3],
            values[4],
            values[5],
            values[6],
            values[7],
            next_observed_at,
            values[8],
            str(host_id),
            str(turn_id),
        ),
    )
    return material_changed, item


def _normalize_pending_decision_meta(value: Any) -> dict[str, Any]:
    """Validate the connector-facing semantic decision contract."""
    if not isinstance(value, Mapping) or set(value) != {
        "decision_ref",
        "kind",
        "prompt",
        "options",
        "multi_select",
        "question_count",
    }:
        raise ValueError("invalid backend pending decision")
    decision_ref = value.get("decision_ref")
    kind = value.get("kind")
    prompt = value.get("prompt")
    options = value.get("options")
    multi_select = value.get("multi_select")
    question_count = value.get("question_count")
    if (
        type(decision_ref) is not str
        or not decision_ref.strip()
        or kind not in {"single", "multi", "plan"}
        or type(prompt) is not str
        or not prompt.strip()
        or not isinstance(options, list)
        or not options
        or not isinstance(multi_select, bool)
        or multi_select is not (kind == "multi")
        or not isinstance(question_count, int)
        or isinstance(question_count, bool)
        or question_count < 1
    ):
        raise ValueError("invalid backend pending decision")
    normalized_options: list[dict[str, str]] = []
    for ordinal, option in enumerate(options, 1):
        if not isinstance(option, Mapping) or set(option) != {"ref", "label"}:
            raise ValueError("invalid backend pending decision option")
        ref = option.get("ref")
        label = option.get("label")
        if ref != str(ordinal) or type(label) is not str or not label.strip():
            raise ValueError("invalid backend pending decision option")
        normalized_options.append({"ref": ref, "label": label})
    return {
        "decision_ref": decision_ref,
        "kind": kind,
        "prompt": prompt,
        "options": normalized_options,
        "multi_select": multi_select,
        "question_count": question_count,
    }


def _normalize_backend_pending_payload(
    pending: Mapping[str, Any],
    choice_routes: tuple[tuple[str, int], ...],
) -> tuple[dict[str, Any], str]:
    """Validate and canonicalize one complete public overlay before persistence."""
    clean = sanitize_public_mapping(pending)
    if not isinstance(clean, dict) or not set(clean) <= {
        "question",
        "kind",
        "choices",
        "meta",
    }:
        raise ValueError("invalid backend pending payload")
    question = clean.get("question")
    kind = clean.get("kind")
    choices = clean.get("choices", [])
    meta = clean.get("meta", {"source": "backend"})
    if (
        not isinstance(question, str)
        or not question.strip()
        or not isinstance(kind, str)
        or not kind.strip()
        or not isinstance(choices, list)
        or not isinstance(meta, Mapping)
    ):
        raise ValueError("invalid backend pending payload")
    normalized_choices: list[dict[str, str]] = []
    seen: set[str] = set()
    for choice in choices:
        if not isinstance(choice, Mapping) or set(choice) != {"choice_id", "label"}:
            raise ValueError("invalid backend pending choice")
        choice_id = choice.get("choice_id")
        label = choice.get("label")
        if (
            type(choice_id) is not str
            or not choice_id
            or choice_id in seen
            or type(label) is not str
            or not label.strip()
        ):
            raise ValueError("invalid backend pending choice")
        seen.add(choice_id)
        normalized_choices.append({"choice_id": choice_id, "label": label})
    route_map = dict(choice_routes)
    if set(route_map) != seen or any(
        type(ordinal) is not int or ordinal < 1 for ordinal in route_map.values()
    ):
        raise ValueError("backend pending choices do not match private routes")
    normalized_meta = sanitize_public_mapping(meta)
    if "decision" in normalized_meta:
        normalized_meta["decision"] = _normalize_pending_decision_meta(
            normalized_meta["decision"]
        )
    normalized = {
        "question": question,
        "kind": kind,
        "choices": normalized_choices,
        "meta": normalized_meta,
    }
    return normalized, _canonical_json(route_map)


def _pending_observed_time(observed_at: str | None) -> tuple[str, datetime]:
    value = observed_at or utc_timestamp()
    canonical = _strict_utc_timestamp(value)
    if canonical is None:
        raise ValueError("observed_at must be a UTC timestamp")
    return canonical, datetime.fromisoformat(canonical)


def _apply_backend_pending_observation_conn(
    conn: sqlite3.Connection,
    host_id: str,
    worker_id: str,
    observation: PendingObservation,
    *,
    observed_at: str,
    stale_grace_seconds: float,
    binding_private_fingerprint: str = "",
    observed_turn_target_value: str = "",
    binding_authoritative: bool = False,
) -> bool:
    """Apply one binding-scoped durable backend-pending transition."""
    current_time, current_dt = _pending_observed_time(observed_at)
    if (
        isinstance(stale_grace_seconds, bool)
        or not isinstance(stale_grace_seconds, (int, float))
        or not float(stale_grace_seconds) > 0
    ):
        raise ValueError("stale_grace_seconds must be positive")
    source_binding = str(binding_private_fingerprint or "")
    source_target = str(observed_turn_target_value or "")
    key = (str(host_id), str(worker_id))
    row = conn.execute(
        """
        SELECT payload_json, revision_digest, choice_routes_json,
               binding_private_fingerprint, observed_turn_target_value,
               observation_state, freshness, grace_deadline, observed_at
        FROM backend_pending
        WHERE host_id = ? AND worker_id = ?
        """,
        key,
    ).fetchone()
    stored_binding = str(row[3]) if row is not None else ""
    stored_target = str(row[4]) if row is not None else ""
    stored_observed_at = (
        _strict_utc_timestamp(row[8]) if row is not None else None
    )
    if (
        stored_observed_at is not None
        and current_dt < datetime.fromisoformat(stored_observed_at)
    ):
        return False

    if observation.kind == "worker_authoritatively_absent":
        if source_binding:
            cursor = conn.execute(
                """
                DELETE FROM backend_pending
                WHERE host_id = ? AND worker_id = ?
                  AND binding_private_fingerprint = ?
                """,
                (*key, source_binding),
            )
        else:
            cursor = conn.execute(
                "DELETE FROM backend_pending WHERE host_id = ? AND worker_id = ?",
                key,
            )
        if cursor.rowcount:
            conn.execute(
                "DELETE FROM backend_pending_claims "
                "WHERE host_id = ? AND worker_id = ?",
                key,
            )
        return bool(cursor.rowcount)

    binding_changed = (
        row is not None
        and source_binding
        and stored_binding
        and source_binding != stored_binding
    )
    if binding_changed and not binding_authoritative:
        return False
    effective_binding = source_binding or stored_binding
    effective_target = source_target or stored_target

    if observation.kind == "read_failed":
        if row is None:
            conn.execute(
                """
                INSERT INTO backend_pending (
                    host_id, worker_id, payload_json, observed_at,
                    revision_digest, choice_routes_json,
                    binding_private_fingerprint, observed_turn_target_value,
                    observation_state, freshness, last_success_at,
                    last_failure_at, grace_deadline, updated_at
                ) VALUES (?, ?, '{}', ?, '', '{}', ?, ?, 'failed', 'stale',
                          NULL, ?, NULL, ?)
                """,
                (
                    *key,
                    current_time,
                    effective_binding,
                    effective_target,
                    current_time,
                    current_time,
                ),
            )
            return True
        state = str(row[5])
        freshness = str(row[6])
        if state == "open":
            deadline = _strict_utc_timestamp(row[7])
            if (
                deadline is not None
                and current_dt >= datetime.fromisoformat(deadline)
            ):
                conn.execute(
                    """
                    UPDATE backend_pending
                    SET observed_at = ?,
                        binding_private_fingerprint = ?,
                        observed_turn_target_value = ?,
                        observation_state = 'failed', freshness = 'stale',
                        last_failure_at = ?, grace_deadline = NULL,
                        updated_at = ?
                    WHERE host_id = ? AND worker_id = ?
                    """,
                    (
                        current_time,
                        effective_binding,
                        effective_target,
                        current_time,
                        current_time,
                        *key,
                    ),
                )
                conn.execute(
                    """
                    DELETE FROM backend_pending_claims
                    WHERE host_id = ? AND worker_id = ? AND state = 'claimed'
                    """,
                    key,
                )
                return True
            if freshness == "stale":
                conn.execute(
                    """
                    UPDATE backend_pending
                    SET observed_at = ?, last_failure_at = ?
                    WHERE host_id = ? AND worker_id = ?
                    """,
                    (current_time, current_time, *key),
                )
                return False
            grace_deadline = (
                current_dt + timedelta(seconds=float(stale_grace_seconds))
            ).isoformat(timespec="microseconds")
            conn.execute(
                """
                UPDATE backend_pending
                SET observed_at = ?, binding_private_fingerprint = ?,
                    observed_turn_target_value = ?, freshness = 'stale',
                    last_failure_at = ?, grace_deadline = ?, updated_at = ?
                WHERE host_id = ? AND worker_id = ?
                """,
                (
                    current_time,
                    effective_binding,
                    effective_target,
                    current_time,
                    grace_deadline,
                    current_time,
                    *key,
                ),
            )
            conn.execute(
                """
                DELETE FROM backend_pending_claims
                WHERE host_id = ? AND worker_id = ? AND state = 'claimed'
                """,
                key,
            )
            return True
        if freshness == "stale":
            conn.execute(
                """
                UPDATE backend_pending
                SET observed_at = ?, last_failure_at = ?
                WHERE host_id = ? AND worker_id = ?
                """,
                (current_time, current_time, *key),
            )
            return False
        conn.execute(
            """
            UPDATE backend_pending
            SET observed_at = ?, binding_private_fingerprint = ?,
                observed_turn_target_value = ?, freshness = 'stale',
                last_failure_at = ?, grace_deadline = NULL, updated_at = ?
            WHERE host_id = ? AND worker_id = ?
            """,
            (
                current_time,
                effective_binding,
                effective_target,
                current_time,
                current_time,
                *key,
            ),
        )
        return True

    if observation.kind == "read_succeeded_unsupported_decision":
        unsupported_payload = _canonical_json({"unsupported_decision": True})
        changed = row is None or (
            str(row[0]),
            str(row[5]),
            str(row[6]),
            stored_binding,
            stored_target,
        ) != (
            unsupported_payload,
            "invalid",
            "fresh",
            effective_binding,
            effective_target,
        )
        conn.execute(
            """
            INSERT INTO backend_pending (
                host_id, worker_id, payload_json, observed_at, revision_digest,
                choice_routes_json, binding_private_fingerprint,
                observed_turn_target_value, observation_state, freshness,
                last_success_at, last_failure_at, grace_deadline, updated_at
            ) VALUES (?, ?, ?, ?, '', '{}', ?, ?, 'invalid', 'fresh',
                      ?, NULL, NULL, ?)
            ON CONFLICT(host_id, worker_id) DO UPDATE SET
                payload_json = excluded.payload_json,
                observed_at = excluded.observed_at,
                revision_digest = '', choice_routes_json = '{}',
                binding_private_fingerprint =
                    excluded.binding_private_fingerprint,
                observed_turn_target_value =
                    excluded.observed_turn_target_value,
                observation_state = 'invalid', freshness = 'fresh',
                last_success_at = excluded.last_success_at,
                last_failure_at = NULL, grace_deadline = NULL,
                updated_at = excluded.updated_at
            """,
            (
                *key,
                unsupported_payload,
                current_time,
                effective_binding,
                effective_target,
                current_time,
                current_time,
            ),
        )
        conn.execute(
            "DELETE FROM backend_pending_claims "
            "WHERE host_id = ? AND worker_id = ?",
            key,
        )
        return changed

    if observation.kind == "read_succeeded_invalid_prompt":
        changed = row is None or (
            str(row[5]),
            str(row[6]),
            stored_binding,
            stored_target,
        ) != ("invalid", "stale", effective_binding, effective_target)
        conn.execute(
            """
            INSERT INTO backend_pending (
                host_id, worker_id, payload_json, observed_at, revision_digest,
                choice_routes_json, binding_private_fingerprint,
                observed_turn_target_value, observation_state, freshness,
                last_success_at, last_failure_at, grace_deadline, updated_at
            ) VALUES (?, ?, '{}', ?, '', '{}', ?, ?, 'invalid', 'stale',
                      NULL, ?, NULL, ?)
            ON CONFLICT(host_id, worker_id) DO UPDATE SET
                payload_json = '{}', observed_at = excluded.observed_at,
                revision_digest = '', choice_routes_json = '{}',
                binding_private_fingerprint =
                    excluded.binding_private_fingerprint,
                observed_turn_target_value =
                    excluded.observed_turn_target_value,
                observation_state = 'invalid', freshness = 'stale',
                last_success_at = NULL,
                last_failure_at = excluded.last_failure_at,
                grace_deadline = NULL, updated_at = excluded.updated_at
            """,
            (
                *key,
                current_time,
                effective_binding,
                effective_target,
                current_time,
                current_time,
            ),
        )
        conn.execute(
            "DELETE FROM backend_pending_claims "
            "WHERE host_id = ? AND worker_id = ?",
            key,
        )
        return changed

    if observation.kind == "read_succeeded_no_prompt":
        changed = row is None or (
            str(row[5]),
            str(row[6]),
            stored_binding,
            stored_target,
        ) != ("none", "fresh", effective_binding, effective_target)
        conn.execute(
            """
            INSERT INTO backend_pending (
                host_id, worker_id, payload_json, observed_at, revision_digest,
                choice_routes_json, binding_private_fingerprint,
                observed_turn_target_value, observation_state, freshness,
                last_success_at, last_failure_at, grace_deadline, updated_at
            ) VALUES (?, ?, '{}', ?, '', '{}', ?, ?, 'none', 'fresh',
                      ?, NULL, NULL, ?)
            ON CONFLICT(host_id, worker_id) DO UPDATE SET
                payload_json = '{}', observed_at = excluded.observed_at,
                revision_digest = '', choice_routes_json = '{}',
                binding_private_fingerprint =
                    excluded.binding_private_fingerprint,
                observed_turn_target_value =
                    excluded.observed_turn_target_value,
                observation_state = 'none', freshness = 'fresh',
                last_success_at = excluded.last_success_at,
                last_failure_at = NULL, grace_deadline = NULL,
                updated_at = excluded.updated_at
            """,
            (
                *key,
                current_time,
                effective_binding,
                effective_target,
                current_time,
                current_time,
            ),
        )
        conn.execute(
            "DELETE FROM backend_pending_claims "
            "WHERE host_id = ? AND worker_id = ?",
            key,
        )
        return changed

    revision_digest = stable_fingerprint(
        {
            "decision_revision": str(observation.revision_digest),
            "binding_private_fingerprint": source_binding,
            "observed_turn_target_value": source_target,
        }
    )
    persisted_choices = tuple(
        (
            "choice-"
            + stable_fingerprint(
                {
                    "revision": revision_digest,
                    "ordinal": choice.picker_ordinal,
                    "label": choice.label,
                }
            ),
            choice.label,
            choice.picker_ordinal,
        )
        for choice in observation.choices
    )
    public_meta: dict[str, Any] = {"source": "backend"}
    if observation.decision_kind is not None:
        public_meta["decision"] = {
            "decision_ref": f"decision-{revision_digest}",
            "kind": observation.decision_kind,
            "prompt": observation.question,
            "options": [
                {"ref": str(ordinal), "label": label}
                for ordinal, label in enumerate(observation.decision_options, 1)
            ],
            "multi_select": observation.decision_multi_select,
            "question_count": observation.decision_question_count,
        }
    public_payload = {
        "question": observation.question,
        "kind": observation.pending_kind or "question",
        "choices": [
            {"choice_id": choice_id, "label": label}
            for choice_id, label, _ordinal in persisted_choices
        ],
        "meta": public_meta,
    }
    routes = tuple(
        (choice_id, ordinal)
        for choice_id, _label, ordinal in persisted_choices
    )
    normalized, routes_json = _normalize_backend_pending_payload(
        public_payload,
        routes,
    )
    payload_json = _canonical_json(normalized)
    changed = row is None or (
        str(row[0]),
        str(row[1]),
        str(row[2]),
        stored_binding,
        stored_target,
        str(row[5]),
        str(row[6]),
    ) != (
        payload_json,
        revision_digest,
        routes_json,
        source_binding,
        source_target,
        "open",
        "fresh",
    )
    conn.execute(
        """
        DELETE FROM backend_pending_claims
        WHERE host_id = ? AND worker_id = ?
          AND (
               revision_digest != ?
            OR binding_private_fingerprint != ?
            OR turn_target_value != ?
          )
        """,
        (*key, revision_digest, source_binding, source_target),
    )
    conn.execute(
        """
        INSERT INTO backend_pending (
            host_id, worker_id, payload_json, observed_at, revision_digest,
            choice_routes_json, binding_private_fingerprint,
            observed_turn_target_value, observation_state, freshness,
            last_success_at, last_failure_at, grace_deadline, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'open', 'fresh', ?, NULL, NULL, ?)
        ON CONFLICT(host_id, worker_id) DO UPDATE SET
            payload_json = excluded.payload_json,
            observed_at = excluded.observed_at,
            revision_digest = excluded.revision_digest,
            choice_routes_json = excluded.choice_routes_json,
            binding_private_fingerprint =
                excluded.binding_private_fingerprint,
            observed_turn_target_value =
                excluded.observed_turn_target_value,
            observation_state = 'open', freshness = 'fresh',
            last_success_at = excluded.last_success_at,
            last_failure_at = NULL, grace_deadline = NULL,
            updated_at = excluded.updated_at
        """,
        (
            *key,
            payload_json,
            current_time,
            revision_digest,
            routes_json,
            source_binding,
            source_target,
            current_time,
            current_time,
        ),
    )
    return changed


def apply_backend_pending_observation(
    db_path: Path | str,
    host_id: str,
    worker_id: str,
    observation: PendingObservation,
    *,
    observed_at: str | None = None,
    stale_grace_seconds: float = DEFAULT_PENDING_STALE_GRACE_SECONDS,
    binding_private_fingerprint: str | None = None,
    observed_turn_target_value: str | None = None,
    binding_authoritative: bool = False,
) -> bool:
    """Apply one explicit observation in a short writer transaction."""
    if not _sqlite_store_exists(db_path):
        return False
    current_time, _ = _pending_observed_time(observed_at)
    with _connect(db_path, isolation_level=None) as conn:
        _ensure_schema(conn)
        conn.execute("BEGIN IMMEDIATE")
        try:
            changed = _apply_backend_pending_observation_conn(
                conn,
                host_id,
                worker_id,
                observation,
                observed_at=current_time,
                stale_grace_seconds=stale_grace_seconds,
                binding_private_fingerprint=str(
                    binding_private_fingerprint or ""
                ),
                observed_turn_target_value=str(
                    observed_turn_target_value or ""
                ),
                binding_authoritative=bool(binding_authoritative),
            )
            conn.commit()
            return changed
        except Exception:
            conn.rollback()
            raise






def _backend_pending_health_from_rows(
    freshness_values: Iterable[Any],
) -> dict[str, Any]:
    fresh = 0
    stale = 0
    for value in freshness_values:
        if str(value) == "stale":
            stale += 1
        else:
            fresh += 1
    return {
        "status": "degraded" if stale else "healthy",
        "counts": {"fresh": fresh, "stale": stale, "total": fresh + stale},
    }


def backend_pending_health(
    db_path: Path | str,
    host_id: str,
) -> dict[str, Any]:
    """Return only aggregate pending freshness; never transitions state."""
    unavailable = {
        "status": "store_unavailable",
        "counts": {"fresh": 0, "stale": 0, "total": 0},
    }
    if not _sqlite_store_exists(db_path):
        return unavailable
    try:
        with _connect(db_path) as conn:
            conn.execute("PRAGMA query_only=ON")
            if (
                int(conn.execute("PRAGMA user_version").fetchone()[0])
                != STORE_SCHEMA_VERSION
            ):
                return unavailable
            rows = conn.execute(
                """
                SELECT freshness
                FROM backend_pending
                WHERE host_id = ?
                  AND (observation_state = 'open' OR freshness = 'stale')
                """,
                (str(host_id),),
            ).fetchall()
    except Exception:
        return unavailable
    return _backend_pending_health_from_rows(row[0] for row in rows)


def _backend_pending_interaction(
    *,
    host_id: str,
    worker_id: str,
    pending: Mapping[str, Any],
    routes_json: Any,
    revision_digest: Any,
    freshness: Any,
    last_success_at: Any,
    grace_deadline: Any,
    updated_at: Any,
    worker: Any,
) -> dict[str, Any]:
    routes = json.loads(routes_json)
    if not isinstance(routes, Mapping) or not all(
        type(key) is str
        and key
        and type(value) is int
        and value >= 1
        for key, value in routes.items()
    ):
        raise ValueError("invalid backend pending routes")
    validation_routes = tuple(
        (str(key), int(value)) for key, value in routes.items()
    )
    if not validation_routes:
        raw_choices = pending.get("choices", [])
        if isinstance(raw_choices, list):
            validation_routes = tuple(
                (str(choice.get("choice_id")), ordinal)
                for ordinal, choice in enumerate(raw_choices, 1)
                if isinstance(choice, Mapping) and choice.get("choice_id")
            )
    normalized, _ = _normalize_backend_pending_payload(
        pending,
        validation_routes,
    )
    if type(revision_digest) is not str or not revision_digest:
        raise ValueError("invalid backend pending revision")
    state = str(freshness)
    if state not in {"fresh", "stale"}:
        raise ValueError("invalid backend pending freshness")
    meta = dict(normalized["meta"])
    meta["freshness"] = state
    interaction = PendingInteraction.from_dict(
        {
            "id": f"pending-{stable_fingerprint({'revision': revision_digest, 'worker_id': worker_id})}",
            "host_id": host_id,
            "worker_id": worker_id,
            "question": normalized["question"],
            "kind": normalized["kind"],
            "choices": normalized["choices"],
            "status": "open",
            "worker_fingerprint": (
                worker.fingerprint if worker is not None else None
            ),
            "space_id": worker.space_id if worker is not None else None,
            "created_at": last_success_at,
            "updated_at": updated_at,
            "expires_at": grace_deadline if state == "stale" else None,
            "meta": meta,
        }
    )
    return interaction.to_dict()


def pending_payload_from_store(
    db_path: Path | str,
    host_id: str,
) -> dict[str, Any]:
    """Project one coherent snapshot plus a strictly validated pending overlay."""
    unavailable = {
        "schema_version": TURN_SCHEMA_VERSION,
        "host_id": str(host_id),
        "ok": False,
        "status": "store_unavailable",
        "pending_interactions": [],
        "backend_health": [],
        "pending_health": {
            "status": "store_unavailable",
            "counts": {"fresh": 0, "stale": 0, "total": 0},
        },
    }
    if not _sqlite_store_exists(db_path):
        return unavailable

    try:
        with _connect(db_path) as conn:
            conn.execute("PRAGMA query_only=ON")
            if (
                int(conn.execute("PRAGMA user_version").fetchone()[0])
                != STORE_SCHEMA_VERSION
            ):
                return unavailable
            conn.execute("BEGIN")
            try:
                snapshot_row = conn.execute(
                    """
                    SELECT payload
                    FROM snapshots
                    WHERE host_id = ?
                    ORDER BY id DESC
                    LIMIT 1
                    """,
                    (str(host_id),),
                ).fetchone()
                backend_rows = conn.execute(
                    """
                    SELECT worker_id, payload_json, choice_routes_json,
                           revision_digest, freshness, last_success_at,
                           grace_deadline, updated_at, observation_state
                    FROM backend_pending
                    WHERE host_id = ?
                    ORDER BY worker_id
                    """,
                    (str(host_id),),
                ).fetchall()
            finally:
                conn.rollback()
    except Exception:
        return unavailable

    if snapshot_row is None:
        return unavailable
    raw_snapshot = snapshot_row[0]
    if not isinstance(raw_snapshot, str) or not raw_snapshot:
        return unavailable
    try:
        decoded_snapshot = json.loads(raw_snapshot)
    except (TypeError, ValueError):
        return unavailable
    if not isinstance(decoded_snapshot, Mapping):
        return unavailable
    if decoded_snapshot.get("host_id") != str(host_id):
        return unavailable
    if _strict_utc_timestamp(decoded_snapshot.get("updated_at")) is None:
        return unavailable
    for collection_name in ("spaces", "workers", "attention", "backend_health"):
        collection = decoded_snapshot.get(collection_name, [])
        if not isinstance(collection, list) or not all(
            isinstance(item, Mapping) for item in collection
        ):
            return unavailable
    for collection_name in ("spaces", "workers", "attention"):
        for item in decoded_snapshot.get(collection_name, []):
            if "meta" in item and not isinstance(item.get("meta"), Mapping):
                return unavailable
    for item in decoded_snapshot.get("attention", []):
        actions = item.get("suggested_actions", item.get("actions", []))
        if not isinstance(actions, list) or not all(
            isinstance(action, Mapping) for action in actions
        ):
            return unavailable
        for action in actions:
            if "params" in action and not isinstance(action.get("params"), Mapping):
                return unavailable
    for item in decoded_snapshot.get("backend_health", []):
        if "counts" in item and not isinstance(item.get("counts"), Mapping):
            return unavailable
    try:
        stored_snapshot = Snapshot.from_dict(
            sanitize_public_mapping(decoded_snapshot)
        )
    except Exception:
        return unavailable
    if stored_snapshot.host_id != str(host_id):
        return unavailable

    payload = dict(pending_payload_from_snapshot(stored_snapshot))
    workers = {worker.id: worker for worker in stored_snapshot.workers}
    built: dict[str, dict[str, Any]] = {}
    suppressed_worker_ids: set[str] = set()
    for (
        worker_id,
        payload_json,
        routes_json,
        revision_digest,
        freshness,
        last_success_at,
        grace_deadline,
        updated_at,
        observation_state,
    ) in backend_rows:
        state = str(observation_state)
        if state == "none":
            suppressed_worker_ids.add(str(worker_id))
            continue
        if state == "invalid":
            try:
                invalid_payload = json.loads(payload_json)
            except (TypeError, ValueError):
                invalid_payload = None
            if (
                isinstance(invalid_payload, Mapping)
                and invalid_payload.get("unsupported_decision") is True
            ):
                suppressed_worker_ids.add(str(worker_id))
            continue
        if state != "open":
            continue
        try:
            pending = json.loads(payload_json)
            if not isinstance(pending, Mapping):
                continue
            built[str(worker_id)] = _backend_pending_interaction(
                host_id=str(host_id),
                worker_id=str(worker_id),
                pending=pending,
                routes_json=routes_json,
                revision_digest=revision_digest,
                freshness=freshness,
                last_success_at=last_success_at,
                grace_deadline=grace_deadline,
                updated_at=updated_at,
                worker=workers.get(str(worker_id)),
            )
        except (TypeError, ValueError):
            continue

    rows = [
        row
        for row in payload.get("pending_interactions", [])
        if row.get("worker_id") not in built
        and row.get("worker_id") not in suppressed_worker_ids
    ]
    rows.extend(built.values())
    rows.sort(
        key=lambda row: (
            str(row.get("id") or ""),
            str(row.get("fingerprint") or ""),
        )
    )
    payload["pending_interactions"] = rows
    payload["pending_health"] = _backend_pending_health_from_rows(
        row[4]
        for row in backend_rows
        if str(row[8]) == "open" or str(row[4]) == "stale"
    )
    payload["content_fingerprint"] = recompute_pending_content_fingerprint(payload)
    return sanitize_public_mapping(payload)


def _backend_pending_claim_context_conn(
    conn: sqlite3.Connection,
    host_id: str,
    worker_id: str,
    binding_private_fingerprint: str,
    observed_turn_target_value: str,
    *,
    observed_at: str,
) -> tuple[Any, str, str, str] | None:
    if (
        not str(binding_private_fingerprint)
        or not str(observed_turn_target_value)
    ):
        return None
    snapshot_row = conn.execute(
        "SELECT payload FROM snapshots WHERE host_id = ? ORDER BY id DESC LIMIT 1",
        (str(host_id),),
    ).fetchone()
    if snapshot_row is None:
        return None
    try:
        snapshot = Snapshot.from_dict(json.loads(snapshot_row[0]))
    except Exception:
        return None
    worker = next(
        (item for item in snapshot.workers if item.id == str(worker_id)),
        None,
    )
    if worker is None or worker.status in {"closed", "failed", "unknown"}:
        return None
    binding_row = conn.execute(
        """
        SELECT private_fingerprint, worker_fingerprint, turn_target_value
        FROM worker_bindings
        WHERE host_id = ? AND worker_id = ? AND worker_fingerprint = ?
          AND ((backend = 'herdr' AND turn_target_kind = 'pane_id')
            OR (backend = 'acp' AND turn_target_kind = 'acp_session_id'))
          AND private_fingerprint = ? AND turn_target_value = ?
          AND sendable = 1 AND (expires_at IS NULL OR expires_at > ?)
        """,
        (
            str(host_id),
            str(worker_id),
            str(worker.fingerprint),
            str(binding_private_fingerprint),
            str(observed_turn_target_value),
            str(observed_at),
        ),
    ).fetchone()
    if binding_row is None:
        return None
    return (
        worker,
        str(binding_row[0]),
        str(binding_row[1]),
        str(binding_row[2]),
    )


def _backend_pending_claim_expired(
    claimed_at: Any,
    current_time: str,
    lease_seconds: float,
) -> bool:
    if (
        isinstance(lease_seconds, bool)
        or not isinstance(lease_seconds, (int, float))
        or not float(lease_seconds) > 0
    ):
        raise ValueError("claim_lease_seconds must be positive")
    claimed = _strict_utc_timestamp(claimed_at)
    if claimed is None:
        return False
    return datetime.fromisoformat(current_time) >= (
        datetime.fromisoformat(claimed)
        + timedelta(seconds=float(lease_seconds))
    )


def claim_backend_pending_choice(
    db_path: Path | str,
    host_id: str,
    pending_id: str,
    pending_fingerprint: str,
    choice_id: str,
    *,
    claim: bool = True,
    observed_at: str | None = None,
    claim_lease_seconds: float = BACKEND_PENDING_CLAIM_LEASE_SECONDS,
) -> BackendPendingChoiceClaim:
    """Validate, and optionally durably claim, one exact fresh public choice."""
    if not _sqlite_store_exists(db_path):
        return BackendPendingChoiceClaim("not_found")
    current_time, _ = _pending_observed_time(observed_at)
    with _connect(db_path, isolation_level=None) as conn:
        _ensure_schema(conn)
        conn.execute("BEGIN IMMEDIATE" if claim else "BEGIN")
        try:
            candidates = conn.execute(
                """
                SELECT worker_id, payload_json, choice_routes_json,
                       revision_digest, freshness, last_success_at,
                       grace_deadline, updated_at,
                       binding_private_fingerprint,
                       observed_turn_target_value
                FROM backend_pending
                WHERE host_id = ? AND observation_state = 'open'
                ORDER BY worker_id
                """,
                (str(host_id),),
            ).fetchall()
            matched: tuple[Any, ...] | None = None
            interaction: dict[str, Any] | None = None
            context: tuple[Any, str, str, str] | None = None
            for candidate in candidates:
                candidate_context = _backend_pending_claim_context_conn(
                    conn,
                    str(host_id),
                    str(candidate[0]),
                    str(candidate[8]),
                    str(candidate[9]),
                    observed_at=current_time,
                )
                if candidate_context is None:
                    continue
                try:
                    candidate_interaction = _backend_pending_interaction(
                        host_id=str(host_id),
                        worker_id=str(candidate[0]),
                        pending=json.loads(candidate[1]),
                        routes_json=candidate[2],
                        revision_digest=candidate[3],
                        freshness=candidate[4],
                        last_success_at=candidate[5],
                        grace_deadline=candidate[6],
                        updated_at=candidate[7],
                        worker=candidate_context[0],
                    )
                except Exception:
                    continue
                if candidate_interaction["id"] == str(pending_id):
                    matched = candidate
                    interaction = candidate_interaction
                    context = candidate_context
                    break
            if matched is None or interaction is None or context is None:
                conn.rollback()
                return BackendPendingChoiceClaim("not_found")
            if str(matched[4]) != "fresh":
                conn.rollback()
                return BackendPendingChoiceClaim("stale")
            if interaction["fingerprint"] != str(pending_fingerprint):
                conn.rollback()
                return BackendPendingChoiceClaim("changed")
            routes = json.loads(matched[2])
            ordinal = routes.get(str(choice_id)) if isinstance(routes, Mapping) else None
            if type(ordinal) is not int or ordinal < 1:
                conn.rollback()
                return BackendPendingChoiceClaim("unknown_choice")
            existing = conn.execute(
                """
                SELECT state, claimed_at
                FROM backend_pending_claims
                WHERE host_id = ? AND worker_id = ?
                """,
                (str(host_id), str(matched[0])),
            ).fetchone()
            if existing is not None:
                reclaimable = (
                    str(existing[0]) == "claimed"
                    and _backend_pending_claim_expired(
                        existing[1],
                        current_time,
                        claim_lease_seconds,
                    )
                )
                if reclaimable and claim:
                    conn.execute(
                        """
                        DELETE FROM backend_pending_claims
                        WHERE host_id = ? AND worker_id = ? AND state = 'claimed'
                          AND claimed_at = ?
                        """,
                        (str(host_id), str(matched[0]), str(existing[1])),
                    )
                elif not reclaimable:
                    conn.rollback()
                    return BackendPendingChoiceClaim("already_claimed")
            fields = {
                "worker_id": str(matched[0]),
                "worker_fingerprint": context[2],
                "binding_private_fingerprint": context[1],
                "turn_target_value": context[3],
                "picker_ordinal": ordinal,
            }
            if not claim:
                conn.rollback()
                return BackendPendingChoiceClaim("validated", **fields)
            token = secrets.token_urlsafe(32)
            conn.execute(
                """
                INSERT INTO backend_pending_claims (
                    host_id, worker_id, claim_token, revision_digest, choice_id,
                    picker_ordinal, worker_fingerprint, binding_private_fingerprint,
                    turn_target_value, state, claimed_at, send_started_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'claimed', ?, NULL)
                """,
                (
                    str(host_id), str(matched[0]), token, str(matched[3]),
                    str(choice_id), ordinal, context[2], context[1], context[3],
                    current_time,
                ),
            )
            conn.commit()
            return BackendPendingChoiceClaim("claimed", claim_token=token, **fields)
        except Exception:
            conn.rollback()
            raise


_DECISION_CLAIM_PREFIX = "decision:"


def _decision_from_pending_row(
    payload_json: Any,
    routes_json: Any,
    revision_digest: Any,
) -> dict[str, Any] | None:
    try:
        payload = json.loads(payload_json)
        routes = json.loads(routes_json)
        if not isinstance(payload, Mapping) or not isinstance(routes, Mapping):
            return None
        normalized, _ = _normalize_backend_pending_payload(
            payload,
            tuple((str(key), int(value)) for key, value in routes.items()),
        )
        meta = normalized.get("meta")
        decision = meta.get("decision") if isinstance(meta, Mapping) else None
        if not isinstance(decision, Mapping):
            return None
        normalized_decision = _normalize_pending_decision_meta(decision)
        if normalized_decision["decision_ref"] != f"decision-{revision_digest}":
            return None
        return normalized_decision
    except (TypeError, ValueError):
        return None


def _validated_decision_selection(
    decision: Mapping[str, Any],
    selection: Any,
) -> tuple[tuple[str, ...], str | None] | None:
    if not isinstance(selection, Mapping) or len(selection) != 1:
        return None
    kind = decision.get("kind")
    valid_refs = {
        str(option.get("ref"))
        for option in decision.get("options", [])
        if isinstance(option, Mapping)
    }
    if set(selection) == {"option_refs"}:
        raw_refs = selection.get("option_refs")
        if not isinstance(raw_refs, list) or not raw_refs or any(
            type(ref) is not str for ref in raw_refs
        ):
            return None
        refs = tuple(raw_refs)
        if len(refs) != len(set(refs)) or any(ref not in valid_refs for ref in refs):
            return None
        if kind in {"single", "plan"} and len(refs) != 1:
            return None
        if kind == "multi" and len(refs) < 1:
            return None
        return refs, None
    if set(selection) == {"text"}:
        text = selection.get("text")
        if kind != "single" or validate_instruction_text(text) is not None:
            return None
        return (), str(text)
    return None


def _encode_decision_claim_selection(
    option_refs: tuple[str, ...],
    text: str | None,
) -> str:
    selection: dict[str, Any]
    if text is None:
        selection = {"option_refs": list(option_refs)}
    else:
        selection = {"text": text}
    return _DECISION_CLAIM_PREFIX + _canonical_json(selection)


def _decode_decision_claim_selection(value: Any) -> Mapping[str, Any] | None:
    if not isinstance(value, str) or not value.startswith(_DECISION_CLAIM_PREFIX):
        return None
    try:
        decoded = json.loads(value[len(_DECISION_CLAIM_PREFIX) :])
    except (TypeError, ValueError):
        return None
    return decoded if isinstance(decoded, Mapping) else None


def claim_backend_pending_decision(
    db_path: Path | str,
    host_id: str,
    worker_id: str,
    decision_ref: str,
    selection: Mapping[str, Any],
    *,
    claim: bool = True,
    observed_at: str | None = None,
    claim_lease_seconds: float = BACKEND_PENDING_CLAIM_LEASE_SECONDS,
) -> BackendPendingDecisionClaim:
    """Validate and optionally claim one worker's exact current decision."""
    if not _sqlite_store_exists(db_path):
        return BackendPendingDecisionClaim("decision_not_pending")
    current_time, _ = _pending_observed_time(observed_at)
    with _connect(db_path, isolation_level=None) as conn:
        _ensure_schema(conn)
        conn.execute("BEGIN IMMEDIATE" if claim else "BEGIN")
        try:
            snapshot_row = conn.execute(
                "SELECT payload FROM snapshots WHERE host_id = ? ORDER BY id DESC LIMIT 1",
                (str(host_id),),
            ).fetchone()
            try:
                snapshot = (
                    Snapshot.from_dict(json.loads(snapshot_row[0]))
                    if snapshot_row is not None
                    else None
                )
            except Exception:
                snapshot = None
            worker = next(
                (
                    item
                    for item in (snapshot.workers if snapshot is not None else [])
                    if item.id == str(worker_id)
                ),
                None,
            )
            if worker is None or worker.status in {"closed", "failed", "unknown"}:
                conn.rollback()
                return BackendPendingDecisionClaim("unknown_worker")
            row = conn.execute(
                """
                SELECT payload_json, choice_routes_json, revision_digest,
                       freshness, binding_private_fingerprint,
                       observed_turn_target_value, observation_state
                FROM backend_pending
                WHERE host_id = ? AND worker_id = ?
                """,
                (str(host_id), str(worker_id)),
            ).fetchone()
            if row is not None and str(row[6]) == "invalid":
                try:
                    unsupported = json.loads(str(row[0]))
                except (TypeError, ValueError):
                    unsupported = None
                if (
                    isinstance(unsupported, Mapping)
                    and unsupported.get("unsupported_decision") is True
                ):
                    conn.rollback()
                    return BackendPendingDecisionClaim("unsupported_decision")
            if row is None or str(row[6]) != "open" or str(row[3]) != "fresh":
                conn.rollback()
                return BackendPendingDecisionClaim("decision_not_pending")
            context = _backend_pending_claim_context_conn(
                conn,
                str(host_id),
                str(worker_id),
                str(row[4]),
                str(row[5]),
                observed_at=current_time,
            )
            decision = _decision_from_pending_row(row[0], row[1], row[2])
            if decision is None or decision.get("decision_ref") != str(decision_ref):
                conn.rollback()
                return BackendPendingDecisionClaim("decision_not_pending")
            if context is None:
                conn.rollback()
                return BackendPendingDecisionClaim("acp_authority_unavailable")
            if int(decision["question_count"]) > 1:
                conn.rollback()
                return BackendPendingDecisionClaim("unsupported_decision")
            validated_selection = _validated_decision_selection(decision, selection)
            if validated_selection is None:
                conn.rollback()
                return BackendPendingDecisionClaim("invalid_selection")
            option_refs, text = validated_selection
            existing = conn.execute(
                """
                SELECT state, claimed_at
                FROM backend_pending_claims
                WHERE host_id = ? AND worker_id = ?
                """,
                (str(host_id), str(worker_id)),
            ).fetchone()
            if existing is not None:
                reclaimable = (
                    str(existing[0]) == "claimed"
                    and _backend_pending_claim_expired(
                        existing[1], current_time, claim_lease_seconds
                    )
                )
                if reclaimable and claim:
                    conn.execute(
                        """
                        DELETE FROM backend_pending_claims
                        WHERE host_id = ? AND worker_id = ? AND state = 'claimed'
                          AND claimed_at = ?
                        """,
                        (str(host_id), str(worker_id), str(existing[1])),
                    )
                elif not reclaimable:
                    conn.rollback()
                    return BackendPendingDecisionClaim("already_claimed")
            option_count = len(decision["options"])
            picker_ordinal = int(option_refs[0]) if option_refs else option_count + 1
            fields = {
                "worker_id": str(worker_id),
                "worker_fingerprint": context[2],
                "binding_private_fingerprint": context[1],
                "turn_target_value": context[3],
                "decision_ref": str(decision_ref),
                "decision_kind": decision["kind"],
                "option_count": option_count,
                "option_refs": option_refs,
                "text": text,
            }
            if not claim:
                conn.rollback()
                return BackendPendingDecisionClaim("validated", **fields)
            token = secrets.token_urlsafe(32)
            conn.execute(
                """
                INSERT INTO backend_pending_claims (
                    host_id, worker_id, claim_token, revision_digest, choice_id,
                    picker_ordinal, worker_fingerprint,
                    binding_private_fingerprint, turn_target_value, state,
                    claimed_at, send_started_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'claimed', ?, NULL)
                """,
                (
                    str(host_id), str(worker_id), token, str(row[2]),
                    _encode_decision_claim_selection(option_refs, text),
                    picker_ordinal, context[2], context[1], context[3], current_time,
                ),
            )
            conn.commit()
            return BackendPendingDecisionClaim(
                "claimed", claim_token=token, **fields
            )
        except Exception:
            conn.rollback()
            raise


def start_backend_pending_decision_send(
    db_path: Path | str,
    host_id: str,
    claim_token: str,
    *,
    observed_at: str | None = None,
    claim_lease_seconds: float = BACKEND_PENDING_CLAIM_LEASE_SECONDS,
) -> BackendPendingDecisionSend:
    """CAS a decision claim against its current prompt and private binding."""
    if not _sqlite_store_exists(db_path):
        return BackendPendingDecisionSend("not_found")
    current_time, _ = _pending_observed_time(observed_at)
    with _connect(db_path, isolation_level=None) as conn:
        _ensure_schema(conn)
        conn.execute("BEGIN IMMEDIATE")
        try:
            row = conn.execute(
                """
                SELECT worker_id, revision_digest, choice_id,
                       worker_fingerprint, binding_private_fingerprint,
                       turn_target_value, state, claimed_at
                FROM backend_pending_claims
                WHERE host_id = ? AND claim_token = ?
                """,
                (str(host_id), str(claim_token)),
            ).fetchone()
            if row is None:
                conn.rollback()
                return BackendPendingDecisionSend("not_found")
            if (
                str(row[6]) == "claimed"
                and _backend_pending_claim_expired(
                    row[7], current_time, claim_lease_seconds
                )
            ):
                conn.execute(
                    """
                    DELETE FROM backend_pending_claims
                    WHERE host_id = ? AND claim_token = ? AND state = 'claimed'
                    """,
                    (str(host_id), str(claim_token)),
                )
                conn.commit()
                return BackendPendingDecisionSend("not_found")
            current = conn.execute(
                """
                SELECT payload_json, choice_routes_json, revision_digest,
                       freshness, binding_private_fingerprint,
                       observed_turn_target_value
                FROM backend_pending
                WHERE host_id = ? AND worker_id = ?
                  AND observation_state = 'open'
                """,
                (str(host_id), str(row[0])),
            ).fetchone()
            if current is None:
                conn.rollback()
                return BackendPendingDecisionSend("changed")
            if str(current[3]) != "fresh":
                conn.rollback()
                return BackendPendingDecisionSend("stale")
            decision = _decision_from_pending_row(current[0], current[1], current[2])
            selection = _decode_decision_claim_selection(row[2])
            validated_selection = (
                _validated_decision_selection(decision, selection)
                if decision is not None and selection is not None
                else None
            )
            if (
                str(current[2]) != str(row[1])
                or str(current[4]) != str(row[4])
                or str(current[5]) != str(row[5])
                or decision is None
                or validated_selection is None
            ):
                conn.rollback()
                return BackendPendingDecisionSend("changed")
            option_refs, text = validated_selection
            fields = {
                "worker_id": str(row[0]),
                "worker_fingerprint": str(row[3]),
                "binding_private_fingerprint": str(row[4]),
                "turn_target_value": str(row[5]),
                "decision_ref": str(decision["decision_ref"]),
                "decision_kind": decision["kind"],
                "option_count": len(decision["options"]),
                "option_refs": option_refs,
                "text": text,
            }
            if str(row[6]) == "send_started":
                conn.rollback()
                return BackendPendingDecisionSend("already_started", **fields)
            context = _backend_pending_claim_context_conn(
                conn,
                str(host_id),
                str(row[0]),
                str(row[4]),
                str(row[5]),
                observed_at=current_time,
            )
            if context is None or (context[1], context[2], context[3]) != (
                str(row[4]), str(row[3]), str(row[5])
            ):
                conn.rollback()
                return BackendPendingDecisionSend("binding_changed")
            cursor = conn.execute(
                """
                UPDATE backend_pending_claims
                SET state = 'send_started', send_started_at = ?
                WHERE host_id = ? AND claim_token = ? AND state = 'claimed'
                """,
                (current_time, str(host_id), str(claim_token)),
            )
            if cursor.rowcount != 1:
                conn.rollback()
                return BackendPendingDecisionSend("changed")
            conn.commit()
            return BackendPendingDecisionSend("started", **fields)
        except Exception:
            conn.rollback()
            raise


def start_backend_pending_choice_send(
    db_path: Path | str,
    host_id: str,
    claim_token: str,
    *,
    observed_at: str | None = None,
    claim_lease_seconds: float = BACKEND_PENDING_CLAIM_LEASE_SECONDS,
) -> BackendPendingChoiceSend:
    """CAS a pre-send claim against the current revision and exact binding."""
    if not _sqlite_store_exists(db_path):
        return BackendPendingChoiceSend("not_found")
    current_time, _ = _pending_observed_time(observed_at)
    with _connect(db_path, isolation_level=None) as conn:
        _ensure_schema(conn)
        conn.execute("BEGIN IMMEDIATE")
        try:
            row = conn.execute(
                """
                SELECT worker_id, revision_digest, choice_id, picker_ordinal,
                       worker_fingerprint, binding_private_fingerprint,
                       turn_target_value, state, claimed_at
                FROM backend_pending_claims
                WHERE host_id = ? AND claim_token = ?
                """,
                (str(host_id), str(claim_token)),
            ).fetchone()
            if row is None:
                conn.rollback()
                return BackendPendingChoiceSend("not_found")
            fields = {
                "worker_id": str(row[0]),
                "worker_fingerprint": str(row[4]),
                "binding_private_fingerprint": str(row[5]),
                "turn_target_value": str(row[6]),
                "picker_ordinal": int(row[3]),
            }
            if (
                str(row[7]) == "claimed"
                and _backend_pending_claim_expired(
                    row[8],
                    current_time,
                    claim_lease_seconds,
                )
            ):
                conn.execute(
                    """
                    DELETE FROM backend_pending_claims
                    WHERE host_id = ? AND claim_token = ? AND state = 'claimed'
                    """,
                    (str(host_id), str(claim_token)),
                )
                conn.commit()
                return BackendPendingChoiceSend("not_found")
            if str(row[7]) == "send_started":
                conn.rollback()
                return BackendPendingChoiceSend("already_started", **fields)
            current = conn.execute(
                """
                SELECT revision_digest, choice_routes_json, freshness,
                       binding_private_fingerprint,
                       observed_turn_target_value
                FROM backend_pending
                WHERE host_id = ? AND worker_id = ? AND observation_state = 'open'
                """,
                (str(host_id), str(row[0])),
            ).fetchone()
            if current is None:
                conn.rollback()
                return BackendPendingChoiceSend("changed")
            if str(current[2]) != "fresh":
                conn.rollback()
                return BackendPendingChoiceSend("stale")
            try:
                routes = json.loads(current[1])
            except (TypeError, ValueError):
                routes = {}
            if (
                str(current[0]) != str(row[1])
                or str(current[3]) != str(row[5])
                or str(current[4]) != str(row[6])
                or not isinstance(routes, Mapping)
                or routes.get(str(row[2])) != int(row[3])
            ):
                conn.rollback()
                return BackendPendingChoiceSend("changed")
            context = _backend_pending_claim_context_conn(
                conn,
                str(host_id),
                str(row[0]),
                str(row[5]),
                str(row[6]),
                observed_at=current_time,
            )
            if context is None or (context[1], context[2], context[3]) != (
                str(row[5]),
                str(row[4]),
                str(row[6]),
            ):
                conn.rollback()
                return BackendPendingChoiceSend("binding_changed")
            cursor = conn.execute(
                """
                UPDATE backend_pending_claims
                SET state = 'send_started', send_started_at = ?
                WHERE host_id = ? AND claim_token = ? AND state = 'claimed'
                """,
                (current_time, str(host_id), str(claim_token)),
            )
            if cursor.rowcount != 1:
                conn.rollback()
                return BackendPendingChoiceSend("changed")
            conn.commit()
            return BackendPendingChoiceSend("started", **fields)
        except Exception:
            conn.rollback()
            raise


def abandon_backend_pending_choice_claim(
    db_path: Path | str,
    host_id: str,
    claim_token: str,
) -> bool:
    """Release only a claim that has not crossed the pane-I/O boundary."""
    if not _sqlite_store_exists(db_path):
        return False
    with _connect(db_path, isolation_level=None) as conn:
        _ensure_schema(conn)
        conn.execute("BEGIN IMMEDIATE")
        try:
            cursor = conn.execute(
                """
                DELETE FROM backend_pending_claims
                WHERE host_id = ? AND claim_token = ? AND state = 'claimed'
                """,
                (str(host_id), str(claim_token)),
            )
            conn.commit()
            return cursor.rowcount == 1
        except Exception:
            conn.rollback()
            raise


def _finish_backend_pending_choice_send_conn(
    conn: sqlite3.Connection,
    host_id: str,
    claim_token: str,
    *,
    accepted: bool,
    observed_at: str | None = None,
) -> bool:
    if not accepted:
        return False
    current_time, _ = _pending_observed_time(observed_at)
    row = conn.execute(
        """
        SELECT worker_id, revision_digest,
               binding_private_fingerprint, turn_target_value
        FROM backend_pending_claims
        WHERE host_id = ? AND claim_token = ? AND state = 'send_started'
        """,
        (str(host_id), str(claim_token)),
    ).fetchone()
    if row is None:
        return False
    updated = conn.execute(
        """
        UPDATE backend_pending
        SET payload_json = '{}',
            observed_at = ?,
            revision_digest = '',
            choice_routes_json = '{}',
            observation_state = 'none',
            freshness = 'fresh',
            last_success_at = ?,
            last_failure_at = NULL,
            grace_deadline = NULL,
            updated_at = ?
        WHERE host_id = ?
          AND worker_id = ?
          AND revision_digest = ?
          AND binding_private_fingerprint = ?
          AND observed_turn_target_value = ?
          AND observation_state IN ('open', 'failed')
        """,
        (
            current_time,
            current_time,
            current_time,
            str(host_id),
            str(row[0]),
            str(row[1]),
            str(row[2]),
            str(row[3]),
        ),
    )
    deleted = conn.execute(
        """
        DELETE FROM backend_pending_claims
        WHERE host_id = ? AND claim_token = ? AND state = 'send_started'
        """,
        (str(host_id), str(claim_token)),
    )
    return updated.rowcount == 1 and deleted.rowcount == 1


def finish_backend_pending_choice_send(
    db_path: Path | str,
    host_id: str,
    claim_token: str,
    *,
    accepted: bool,
    observed_at: str | None = None,
) -> bool:
    """Tombstone an accepted exact revision; retain failed sends as uncertain."""
    if not accepted or not _sqlite_store_exists(db_path):
        return False
    with _connect(db_path, isolation_level=None) as conn:
        _ensure_schema(conn)
        conn.execute("BEGIN IMMEDIATE")
        try:
            finished = _finish_backend_pending_choice_send_conn(
                conn,
                host_id,
                claim_token,
                accepted=accepted,
                observed_at=observed_at,
            )
            conn.commit()
            return finished
        except Exception:
            conn.rollback()
            raise


def backend_pending_choice_terminal_effect(
    *,
    host_id: str,
    claim_token: str,
    accepted: bool,
) -> Callable[[sqlite3.Connection], None]:
    """Build an accepted-choice effect for a command terminal transaction."""
    def effect(conn: sqlite3.Connection) -> None:
        if not accepted:
            return
        if not _finish_backend_pending_choice_send_conn(
            conn,
            host_id,
            claim_token,
            accepted=True,
            observed_at=utc_timestamp(),
        ):
            raise StoreSchemaError("backend_pending_choice_terminal_effect_failed")

    return effect




def _decode_turn_content_rows(
    rows: Iterable[tuple[Any, ...]],
) -> list[tuple[Any, dict[str, Any], dict[str, Any] | None, str]]:
    decoded: list[tuple[Any, dict[str, Any], dict[str, Any] | None, str]] = []
    for (
        turn_id,
        payload_json,
        revision,
        user_text,
        final_text,
        user_state,
        final_state,
        stored_observed_at,
    ) in rows:
        try:
            loaded = json.loads(str(payload_json or "{}"))
        except (TypeError, json.JSONDecodeError):
            loaded = {}
        sanitized_payload = sanitize_public_value(
            loaded if isinstance(loaded, Mapping) else {}
        )
        payload = (
            dict(sanitized_payload)
            if isinstance(sanitized_payload, Mapping)
            else {}
        )
        current = (
            {
                "content_revision": str(revision),
                "user_text": user_text,
                "assistant_final_text": final_text,
                "user_state": str(user_state),
                "final_state": str(final_state),
            }
            if revision is not None
            else None
        )
        decoded.append(
            (turn_id, payload, current, str(stored_observed_at or ""))
        )
    return decoded


def _current_turn_content_row_by_id_conn(
    conn: sqlite3.Connection,
    host_id: str,
    turn_id: str,
) -> tuple[Any, dict[str, Any], dict[str, Any] | None, str] | None:
    row = conn.execute(
        """
        SELECT
            turns.turn_id,
            turns.payload_json,
            revisions.content_revision,
            revisions.user_text,
            revisions.assistant_final_text,
            revisions.user_state,
            revisions.final_state,
            turns.observed_at
        FROM turns
        LEFT JOIN turn_content_revisions AS revisions
          ON revisions.host_id = turns.host_id
         AND revisions.turn_id = turns.turn_id
         AND revisions.is_current = 1
        WHERE turns.host_id = ? AND turns.turn_id = ?
        """,
        (str(host_id), str(turn_id)),
    ).fetchone()
    if row is None:
        return None
    decoded = _decode_turn_content_rows((row,))
    return decoded[0] if decoded else None


def _current_turn_content_rows_conn(
    conn: sqlite3.Connection,
    host_id: str,
    worker_id: str,
) -> list[tuple[Any, dict[str, Any], dict[str, Any] | None, str]]:
    rows = conn.execute(
        """
        SELECT
            turns.turn_id,
            turns.payload_json,
            revisions.content_revision,
            revisions.user_text,
            revisions.assistant_final_text,
            revisions.user_state,
            revisions.final_state,
            turns.observed_at
        FROM turns
        LEFT JOIN turn_content_revisions AS revisions
          ON revisions.host_id = turns.host_id
         AND revisions.turn_id = turns.turn_id
         AND revisions.is_current = 1
        WHERE turns.host_id = ? AND turns.worker_id = ?
        """,
        (str(host_id), str(worker_id)),
    ).fetchall()
    return [
        row
        for row in _decode_turn_content_rows(rows)
        if not _turn_is_tombstoned(row[1])
    ]


def _turn_with_current_content(
    payload: Mapping[str, Any],
    current: Mapping[str, Any] | None,
) -> dict[str, Any]:
    merged = dict(payload)
    if current is not None:
        merged["user_text"] = current.get("user_text")
        merged["assistant_final_text"] = current.get("assistant_final_text")
    return merged


def _turn_is_tombstoned(payload: Mapping[str, Any]) -> bool:
    return bool(str(payload.get("superseded_at") or "").strip())


def _migrate_tombstone_command_turn_conn(
    conn: sqlite3.Connection,
    host_id: str,
    turn_id: str,
    *,
    superseded_by_turn_id: str | None,
    superseded_at: str,
) -> bool:
    row = conn.execute(
        "SELECT payload_json FROM turns WHERE host_id = ? AND turn_id = ?",
        (str(host_id), str(turn_id)),
    ).fetchone()
    if row is None:
        return False
    stored = _json_object(row[0])
    if _turn_is_tombstoned(stored):
        return False
    payload = dict(stored)
    payload.update(
        {
            "status": "closed",
            "complete": True,
            "has_open_turn": False,
            "assistant_stream_text": None,
            "completed_at": payload.get("completed_at") or superseded_at,
            "updated_at": superseded_at,
            "superseded_by_turn_id": (
                str(superseded_by_turn_id)
                if str(superseded_by_turn_id or "").strip()
                else None
            ),
            "superseded_at": superseded_at,
        }
    )
    _update_turn_row(
        conn,
        str(host_id),
        str(turn_id),
        payload,
        superseded_at,
    )
    return True


def _turn_row_time(payload: Mapping[str, Any], observed_at: str) -> datetime | None:
    for value in (
        payload.get("started_at"),
        payload.get("updated_at"),
        observed_at,
    ):
        timestamp = _strict_utc_timestamp(value)
        if timestamp is not None:
            return datetime.fromisoformat(timestamp)
    return None


def _reserve_lazy_submission_link_sweep(
    db_path: Path | str,
    host_id: str,
    *,
    purpose: str,
    current_clock: float,
    refresh_interval_seconds: float,
) -> tuple[tuple[str, str, str], bool]:
    key = (str(Path(db_path).absolute()), str(host_id), str(purpose))
    with _SUBMISSION_LINK_SWEEP_LOCK:
        last = _SUBMISSION_LINK_SWEEP_LAST_AT.get(key)
        if (
            last is not None
            and current_clock >= last
            and current_clock - last < refresh_interval_seconds
        ):
            return key, False
        _SUBMISSION_LINK_SWEEP_LAST_AT[key] = current_clock
    return key, True


def _release_failed_submission_link_sweep(
    key: tuple[str, str, str],
    *,
    current_clock: float,
) -> None:
    with _SUBMISSION_LINK_SWEEP_LOCK:
        if _SUBMISSION_LINK_SWEEP_LAST_AT.get(key) == current_clock:
            _SUBMISSION_LINK_SWEEP_LAST_AT.pop(key, None)


def _source_turn_matches(payload: Mapping[str, Any], incoming_source_turn: str) -> bool:
    stored = str(payload.get("source_turn_id") or "").strip()
    if not stored or not incoming_source_turn:
        return False
    candidate = Turn.from_dict({**dict(payload), "source_turn_id": incoming_source_turn})
    return candidate.source_turn_id == stored


def _owned_source_turn_matches(
    payload: Mapping[str, Any],
    incoming_source_turn: str,
) -> bool:
    stored = str(payload.get("source_turn_id") or "").strip()
    meta = payload.get("meta")
    if (
        not stored
        or not incoming_source_turn
        or not isinstance(meta, Mapping)
    ):
        return False
    candidates = turn_source_id_candidates(
        incoming_source_turn,
        meta=meta,
        source=payload.get("source"),
        kind=payload.get("kind"),
    )
    return stored in candidates


def _raw_owned_source_turn_candidates(
    raw_value: Any,
    *,
    owner_key: str,
    kind: str,
) -> tuple[str, ...]:
    """Derive exact-source lookup tokens without invoking the public sanitizer.

    This is only a speculative no-op lookup. A mismatch falls through to the
    canonical sanitizer and merge path, so non-canonical or unsafe input can
    never be persisted through this shortcut.
    """
    if not isinstance(raw_value, str):
        return ()
    raw = raw_value.strip()
    if not raw:
        return ()
    if (
        raw.startswith("turnsrc-")
        and len(raw) == len("turnsrc-") + FINGERPRINT_HEX_LENGTH
        and all(char in "0123456789abcdef" for char in raw[len("turnsrc-") :])
    ):
        return (raw,)
    owner_token = "turnsrc-" + stable_fingerprint(
        {
            "seed": raw,
            "public": {
                "identity_domain": "stable-owner-source-v1",
                "stable_key": str(owner_key),
                "stable_key_version": 1,
                "kind": str(kind),
            },
        }
    )
    return (owner_token,)


def _canonical_reobservation_text_matches(
    content: Mapping[str, Any],
    key: str,
    stored_text: Any,
    stored_state: Any,
) -> bool:
    if key not in content or content.get(key) in (None, ""):
        return True
    raw = content.get(key)
    return (
        isinstance(raw, str)
        and raw == stored_text
        and str(stored_state) == "complete"
    )


def _unchanged_owned_turn_reobservation_conn(
    conn: sqlite3.Connection,
    host_id: str,
    worker_id: str,
    worker_payload: Mapping[str, Any],
    content: Mapping[str, Any],
    *,
    observed_at: str,
) -> _TurnContentMergeResult | None:
    """Return a no-op merge result without decoding or sanitizing turn payloads.

    The fast path is deliberately exact and fail-closed. It accepts only a
    canonical re-observation whose persisted projection, metadata, and content
    revision already equal the incoming values. Anything uncertain uses the
    normal public-sanitizing merge path.
    """
    owner_identity = _turn_continuity_identity(worker_payload)
    if owner_identity is None:
        return None
    _owner_kind, owner_key, owner_version = owner_identity
    if owner_version != 1:
        return None
    if not isinstance(content.get("source_turn_id"), str):
        return None
    rows = conn.execute(
        """
        SELECT
            turns.turn_id,
            turns.payload_json,
            turns.observed_at,
            revisions.content_revision,
            revisions.user_text,
            revisions.assistant_final_text,
            revisions.user_state,
            revisions.final_state
        FROM turns
        LEFT JOIN turn_content_revisions AS revisions
          ON revisions.host_id = turns.host_id
         AND revisions.turn_id = turns.turn_id
         AND revisions.is_current = 1
        WHERE turns.host_id = ?
          AND json_valid(turns.payload_json)
          AND json_type(turns.payload_json, '$.meta.stable_key') = 'text'
          AND json_extract(turns.payload_json, '$.meta.stable_key') = ?
          AND json_type(
                turns.payload_json,
                '$.meta.stable_key_version'
              ) = 'integer'
          AND json_extract(
                turns.payload_json,
                '$.meta.stable_key_version'
              ) = ?
          AND COALESCE(json_extract(turns.payload_json, '$.source_turn_id'), '') != ''
          AND COALESCE(json_extract(turns.payload_json, '$.superseded_at'), '') = ''
        """,
        (
            str(host_id),
            str(owner_key),
            int(owner_version),
        ),
    ).fetchall()
    matching_rows: list[tuple[tuple[Any, ...], dict[str, Any], tuple[str, ...]]] = []
    for row in rows:
        payload = _json_object(row[1])
        source_candidates = _raw_owned_source_turn_candidates(
            content.get("source_turn_id"),
            owner_key=owner_key,
            kind=str(payload.get("kind") or "unknown"),
        )
        if str(payload.get("source_turn_id") or "") in source_candidates:
            matching_rows.append((row, payload, source_candidates))
    if len(matching_rows) != 1:
        return None
    row, payload, source_candidates = matching_rows[0]
    (
        turn_id,
        _payload_json,
        stored_observed_at,
        content_revision_value,
        user_text,
        final_text,
        user_state,
        final_state,
    ) = row
    if (
        content_revision_value is None
        or _turn_continuity_identity(payload) != owner_identity
        or str(payload.get("source_turn_id") or "") not in source_candidates
        or not _canonical_reobservation_text_matches(
            content,
            "user_text",
            user_text,
            user_state,
        )
        or not _canonical_reobservation_text_matches(
            content,
            "assistant_final_text",
            final_text,
            final_state,
        )
    ):
        return None

    worker_meta = worker_payload.get("meta")
    projection_values = {
        "host_id": str(host_id),
        "worker_id": str(worker_id),
        "worker_fingerprint": worker_payload.get("fingerprint") or None,
        "space_id": worker_payload.get("space_id"),
        "status": str(worker_payload.get("status") or "unknown"),
        "kind": "task",
        "title": worker_payload.get("name"),
        "summary": worker_payload.get("summary"),
        "updated_at": worker_payload.get("last_seen_at"),
        "meta": dict(worker_meta) if isinstance(worker_meta, Mapping) else {},
    }
    if any(payload.get(key) != value for key, value in projection_values.items()):
        return None

    incoming_final = content.get("assistant_final_text")
    terminal = (
        payload.get("complete") is True
        or str(final_state or "") == "complete"
    )
    completes_now = content.get("complete") is True or (
        isinstance(incoming_final, str) and bool(incoming_final)
    )
    expected_updates: dict[str, Any] = {}
    for key in ("model", "assistant_stream_text"):
        if key in content:
            raw = content.get(key)
            if raw is not None and not isinstance(raw, str):
                return None
            expected_updates[key] = raw
    for key in ("complete", "has_open_turn"):
        if key in content:
            raw = content.get(key)
            if raw is not None and not isinstance(raw, bool):
                return None
            expected_updates[key] = raw
    if terminal or completes_now:
        if not payload.get("completed_at"):
            return None
        expected_updates.update(
            {
                "complete": True,
                "has_open_turn": False,
                "assistant_stream_text": None,
            }
        )
    if any(payload.get(key) != value for key, value in expected_updates.items()):
        return None

    if _turn_observation_is_newer(str(observed_at), str(stored_observed_at or "")):
        conn.execute(
            """
            UPDATE turns SET observed_at = ?
            WHERE host_id = ? AND turn_id = ?
            """,
            (str(observed_at), str(host_id), str(turn_id)),
        )
    incoming_user = content.get("user_text")
    submission_link = (
        str(owner_key),
        instruction_fingerprint(
            incoming_user if isinstance(incoming_user, str) else None
        ),
    )
    return _TurnContentMergeResult(0, submission_link)


def _merge_canonical_field(
    incoming: str | None,
    current_text: Any,
    current_state: Any,
) -> tuple[str | None, str]:
    if incoming is None or incoming == "":
        state = str(current_state or "absent")
        if state not in {"absent", "complete", "known_incomplete"}:
            state = "absent"
        return (
            str(current_text) if current_text is not None and state != "absent" else None,
            state,
        )
    return incoming, "complete"


def _retain_authoritative_completion(
    metadata: Mapping[str, Any],
    current: Mapping[str, Any] | None,
    existing: Mapping[str, Any],
    *,
    incoming_final: str | None,
    observed_at: str,
) -> dict[str, Any]:
    merged = dict(metadata)
    terminal = (
        existing.get("complete") is True
        or (current is not None and str(current.get("final_state") or "") == "complete")
    )
    completes_now = merged.get("complete") is True or bool(incoming_final)
    if terminal or completes_now:
        merged["complete"] = True
        merged["has_open_turn"] = False
        merged["assistant_stream_text"] = None
        merged["completed_at"] = existing.get("completed_at") or observed_at
    return merged


def _turn_observation_is_newer(incoming: str, stored: str) -> bool:
    if not stored:
        return True
    incoming_timestamp = _strict_utc_timestamp(incoming)
    stored_timestamp = _strict_utc_timestamp(stored)
    if incoming_timestamp is not None and stored_timestamp is not None:
        return _connector_datetime(incoming_timestamp) > _connector_datetime(stored_timestamp)
    return str(incoming) > str(stored)

def _turn_has_authoritative_observation(
    payload: Mapping[str, Any],
    current: Mapping[str, Any] | None,
) -> bool:
    if str(payload.get("source_turn_id") or "").strip():
        return True
    if any(
        payload.get(key) not in (None, "", False)
        for key in ("assistant_stream_text", "complete", "has_open_turn")
    ):
        return True
    return current is not None and any(
        str(current.get(key) or "") != "absent"
        for key in ("user_state", "final_state")
    )


def _replace_current_turn_content_conn(
    conn: sqlite3.Connection,
    *,
    host_id: str,
    turn_id: str,
    current: Mapping[str, Any] | None,
    incoming_user: str | None,
    incoming_final: str | None,
    current_time: str,
) -> bool:
    user_text, user_state = _merge_canonical_field(
        incoming_user,
        current.get("user_text") if current else None,
        current.get("user_state") if current else None,
    )
    final_text, final_state = _merge_canonical_field(
        incoming_final,
        current.get("assistant_final_text") if current else None,
        current.get("final_state") if current else None,
    )
    if user_state == "absent" and final_state == "absent":
        return False
    revision = content_revision(
        str(turn_id),
        user_text,
        final_text,
        user_state,
        final_state,
    )
    if current is not None and str(current.get("content_revision") or "") == revision:
        return False
    conn.execute(
        """
        UPDATE turn_content_revisions
        SET is_current = 0, superseded_at = ?
        WHERE host_id = ? AND turn_id = ? AND is_current = 1
        """,
        (current_time, str(host_id), str(turn_id)),
    )
    existing = conn.execute(
        """
        SELECT 1
        FROM turn_content_revisions
        WHERE host_id = ? AND turn_id = ? AND content_revision = ?
        """,
        (str(host_id), str(turn_id), revision),
    ).fetchone()
    if existing is None:
        _insert_turn_content_revision_conn(
            conn,
            host_id=str(host_id),
            turn_id=str(turn_id),
            user_text=user_text,
            assistant_final_text=final_text,
            user_state=user_state,
            final_state=final_state,
            created_at=current_time,
            is_current=False,
        )
    conn.execute(
        """
        UPDATE turn_content_revisions
        SET is_current = 1, superseded_at = NULL
        WHERE host_id = ? AND turn_id = ? AND content_revision = ?
        """,
        (str(host_id), str(turn_id), revision),
    )
    return True


def _strip_canonical_turn_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    lightweight = dict(payload)
    for key in (
        "user_text",
        "assistant_final_text",
        "user_preview",
        "assistant_final_preview",
        "content",
    ):
        lightweight.pop(key, None)
    return lightweight


def _insert_owned_turn_conn(
    conn: sqlite3.Connection,
    *,
    host_id: str,
    item: Mapping[str, Any],
    snapshot_content_fingerprint: str,
    observed_at: str,
) -> str:
    turn_id = str(item.get("id") or "")
    if not turn_id:
        raise StoreSchemaError("turn_owned_identity_missing")
    collision = conn.execute(
        """
        SELECT payload_json
        FROM turns
        WHERE host_id = ? AND turn_id = ?
        """,
        (str(host_id), turn_id),
    ).fetchone()
    if collision is not None:
        raise StoreSchemaError("turn_owner_source_identity_conflict")
    item = _observed_turn_payload(turn_id, item)
    list_sequence = _turn_list_sequence_conn(conn, host_id, turn_id)
    conn.execute(
        """
        INSERT INTO turns (
            host_id,
            turn_id,
            worker_id,
            worker_fingerprint,
            space_id,
            status,
            kind,
            updated_at,
            fingerprint,
            snapshot_content_fingerprint,
            observed_at,
            payload_json,
            list_sequence
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            str(host_id),
            turn_id,
            str(item.get("worker_id") or ""),
            item.get("worker_fingerprint"),
            item.get("space_id"),
            str(item.get("status") or "unknown"),
            str(item.get("kind") or "unknown"),
            item.get("updated_at") or observed_at,
            str(item.get("fingerprint") or ""),
            str(snapshot_content_fingerprint),
            str(observed_at),
            _canonical_json(_strip_canonical_turn_payload(item)),
            list_sequence,
        ),
    )
    return turn_id


def _merge_observed_turn_content_conn(
    conn: sqlite3.Connection,
    host_id: str,
    current_worker_payload: Mapping[str, Any],
    current_projection: Mapping[str, Any],
    clean_content: Mapping[str, Any],
    *,
    incoming_user: str | None,
    incoming_final: str | None,
    observed_at: str,
    snapshot_content_fingerprint: str,
) -> _TurnContentMergeResult:
    """Apply the observation-authoritative path without command adoption."""
    incoming_source_turn = str(
        clean_content.get("source_turn_id") or ""
    ).strip()
    if not incoming_source_turn:
        return _TurnContentMergeResult(0)

    seed = dict(current_projection)
    seed.pop("origin_command_id", None)
    if str(seed.get("source") or "") == "command":
        seed["source"] = "snapshot"
    seed.update(
        _retain_authoritative_completion(
            clean_content,
            None,
            {},
            incoming_final=incoming_final,
            observed_at=observed_at,
        )
    )
    seed["source_turn_id"] = incoming_source_turn
    item = _strip_canonical_turn_payload(Turn.from_dict(seed).to_dict())
    item.update(_public_pending_turn_extension(seed))
    item.pop("origin_command_id", None)
    turn_id = str(item.get("id") or "")
    if not turn_id:
        raise StoreSchemaError("turn_observed_identity_missing")

    current_row = _current_turn_content_row_by_id_conn(
        conn,
        str(host_id),
        turn_id,
    )
    changed = False
    if current_row is None:
        _insert_owned_turn_conn(
            conn,
            host_id=str(host_id),
            item=item,
            snapshot_content_fingerprint=snapshot_content_fingerprint,
            observed_at=observed_at,
        )
        _replace_current_turn_content_conn(
            conn,
            host_id=str(host_id),
            turn_id=turn_id,
            current=None,
            incoming_user=incoming_user,
            incoming_final=incoming_final,
            current_time=observed_at,
        )
        _ensure_absent_turn_content_revision_conn(
            conn,
            host_id=str(host_id),
            turn_id=turn_id,
            observed_at=observed_at,
        )
        changed = True
    else:
        (
            _persisted_turn_id,
            stored_payload,
            current_content,
            stored_observed_at,
        ) = current_row
        if (
            _turn_is_tombstoned(stored_payload)
            or not _owned_source_turn_matches(
                stored_payload,
                incoming_source_turn,
            )
            and not _source_turn_matches(
                stored_payload,
                incoming_source_turn,
            )
        ):
            raise StoreSchemaError("turn_observed_identity_conflict")
        payload = dict(stored_payload)
        for key in (
            "worker_id",
            "worker_fingerprint",
            "space_id",
            "status",
            "kind",
            "title",
            "summary",
            "updated_at",
            "meta",
        ):
            if key in item:
                payload[key] = item.get(key)
        payload.pop("origin_command_id", None)
        observation_is_newer = _turn_observation_is_newer(
            observed_at,
            stored_observed_at,
        )
        if observation_is_newer:
            payload.update(
                _retain_authoritative_completion(
                    clean_content,
                    current_content,
                    payload,
                    incoming_final=incoming_final,
                    observed_at=observed_at,
                )
            )
        metadata_changed, _persisted_item = _update_observed_turn_row(
            conn,
            str(host_id),
            turn_id,
            payload,
            observed_at,
            snapshot_content_fingerprint=snapshot_content_fingerprint,
        )
        if observation_is_newer:
            revision_changed = _replace_current_turn_content_conn(
                conn,
                host_id=str(host_id),
                turn_id=turn_id,
                current=current_content,
                incoming_user=incoming_user,
                incoming_final=incoming_final,
                current_time=observed_at,
            )
            revision_repaired = _ensure_absent_turn_content_revision_conn(
                conn,
                host_id=str(host_id),
                turn_id=turn_id,
                observed_at=observed_at,
            )
        else:
            revision_changed = False
            revision_repaired = False
        changed = metadata_changed or revision_changed or revision_repaired

    owner_identity = _turn_submission_owner_identity(current_worker_payload)
    submission_link = (
        (owner_identity[0], instruction_fingerprint(incoming_user))
        if owner_identity is not None and owner_identity[1] == 1
        else None
    )
    submission_link_rearm = submission_link
    current_revision = conn.execute(
        """
        SELECT content_revision
        FROM turn_content_revisions
        WHERE host_id = ? AND turn_id = ? AND is_current = 1
        """,
        (str(host_id), turn_id),
    ).fetchone()
    if current_revision is not None:
        _ensure_final_ready_anchor_conn(
            conn,
            host_id=str(host_id),
            turn_id=turn_id,
            content_revision_value=str(current_revision[0]),
            now=str(observed_at),
        )
    return _TurnContentMergeResult(
        int(changed),
        submission_link,
        submission_link_rearm,
    )



def _merge_turn_content_conn(
    conn: sqlite3.Connection,
    host_id: str,
    worker_id: str,
    content: Mapping[str, Any],
    *,
    observed_at: str,
) -> _TurnContentMergeResult:
    if not any(key in content for key in _TURN_CONTENT_FIELDS):
        return _TurnContentMergeResult(0)
    current_worker_row = conn.execute(
        """
        SELECT payload_json, snapshot_content_fingerprint
        FROM workers
        WHERE host_id = ? AND worker_id = ?
        """,
        (str(host_id), str(worker_id)),
    ).fetchone()
    if current_worker_row is None:
        return _TurnContentMergeResult(0)
    current_worker_payload = _json_object(current_worker_row[0])
    unchanged = _unchanged_owned_turn_reobservation_conn(
        conn,
        str(host_id),
        str(worker_id),
        current_worker_payload,
        content,
        observed_at=observed_at,
    )
    if unchanged is not None:
        return unchanged
    incoming_user = (
        sanitize_canonical_turn_text(content.get("user_text"))
        if "user_text" in content
        else None
    )
    incoming_final = (
        sanitize_canonical_turn_text(content.get("assistant_final_text"))
        if "assistant_final_text" in content
        else None
    )
    clean_content = sanitize_public_mapping(
        {
            key: content.get(key)
            for key in _TURN_CONTENT_FIELDS
            if key in content and key not in {"user_text", "assistant_final_text"}
        }
    )
    automation_probe = {
        **clean_content,
        "user_text": incoming_user,
        "assistant_final_text": incoming_final,
    }
    if is_internal_automation_turn_payload(automation_probe):
        return _TurnContentMergeResult(0)
    current_projection = _current_worker_turn_projection(
        str(host_id),
        str(worker_id),
        current_worker_payload,
    )
    return _merge_observed_turn_content_conn(
        conn,
        str(host_id),
        current_worker_payload,
        current_projection,
        clean_content,
        incoming_user=incoming_user,
        incoming_final=incoming_final,
        observed_at=observed_at,
        snapshot_content_fingerprint=str(current_worker_row[1] or ""),
    )


def _turn_refresh_binding_matches_conn(
    conn: sqlite3.Connection,
    host_id: str,
    worker_id: str,
    expected: WorkerBinding,
) -> bool:
    if (
        str(expected.host_id) != str(host_id)
        or str(expected.worker_id) != str(worker_id)
    ):
        return False
    row = conn.execute(
        """
        SELECT
            worker_id,
            worker_fingerprint,
            backend,
            turn_target_kind,
            turn_target_value
        FROM worker_bindings
        WHERE host_id = ?
          AND backend = ?
          AND private_fingerprint = ?
          AND expires_at > ?
        """,
        (
            str(host_id),
            str(expected.backend),
            str(expected.private_fingerprint),
            utc_timestamp(),
        ),
    ).fetchone()
    return row is not None and tuple(row) == (
        str(expected.worker_id),
        str(expected.worker_fingerprint),
        str(expected.backend),
        expected.turn_target_kind,
        expected.turn_target_value,
    )


def _agent_event_binding_matches_conn(
    conn: sqlite3.Connection,
    host_id: str,
    worker_id: str,
    expected: WorkerBinding,
) -> bool:
    """Match the complete routing generation used by an agent event stream."""
    if (
        str(expected.host_id) != str(host_id)
        or str(expected.worker_id) != str(worker_id)
    ):
        return False
    row = conn.execute(
        """
        SELECT
            worker_id,
            worker_fingerprint,
            backend,
            target_kind,
            target_value,
            turn_target_kind,
            turn_target_value
        FROM worker_bindings
        WHERE host_id = ?
          AND backend = ?
          AND private_fingerprint = ?
          AND expires_at > ?
        """,
        (
            str(host_id),
            str(expected.backend),
            str(expected.private_fingerprint),
            utc_timestamp(),
        ),
    ).fetchone()
    return row is not None and tuple(row) == (
        str(expected.worker_id),
        str(expected.worker_fingerprint),
        str(expected.backend),
        str(expected.target_kind),
        str(expected.target_value),
        expected.turn_target_kind,
        expected.turn_target_value,
    )


def _agent_event_authority_observed_at_conn(
    conn: sqlite3.Connection,
    host_id: str,
    event_id: str,
) -> str | None:
    """Return the immutable authority time for a retained event."""
    row = conn.execute(
        "SELECT observed_at FROM agent_events WHERE host_id = ? AND event_id = ?",
        (str(host_id), str(event_id)),
    ).fetchone()
    if row is None or row[0] is None:
        return None
    observed_at = _strict_utc_timestamp(row[0])
    if observed_at is None:
        raise StoreSchemaError("invalid_agent_event_authority_time")
    return observed_at


def _owned_turn_projection_exists_conn(
    conn: sqlite3.Connection,
    host_id: str,
    worker_id: str,
    source_turn_id: str,
) -> bool:
    """Fail closed when any live or superseded owned projection still exists."""
    rows = conn.execute(
        "SELECT payload_json FROM turns WHERE host_id = ? AND worker_id = ?",
        (str(host_id), str(worker_id)),
    ).fetchall()
    for row in rows:
        payload = _json_object(row[0])
        if _owned_source_turn_matches(payload, source_turn_id) or _source_turn_matches(
            payload, source_turn_id
        ):
            return True
    return False


def _turn_refresh_is_cancelled(
    *,
    deadline_monotonic: float | None,
    cancelled: Callable[[], bool] | None,
) -> bool:
    if cancelled is not None:
        try:
            if cancelled():
                return True
        except Exception:
            return True
    return (
        deadline_monotonic is not None
        and time.monotonic() >= float(deadline_monotonic)
    )


def _begin_turn_refresh_transaction(
    conn: sqlite3.Connection,
    *,
    deadline_monotonic: float | None,
    cancelled: Callable[[], bool] | None,
) -> bool:
    if deadline_monotonic is None and cancelled is None:
        conn.execute("BEGIN IMMEDIATE")
        return True
    conn.execute("PRAGMA busy_timeout=50")
    while True:
        if _turn_refresh_is_cancelled(
            deadline_monotonic=deadline_monotonic,
            cancelled=cancelled,
        ):
            return False
        try:
            conn.execute("BEGIN IMMEDIATE")
            return True
        except sqlite3.OperationalError as exc:
            message = str(exc).lower()
            if "locked" not in message and "busy" not in message:
                raise


def append_agent_event_and_apply_turn_for_binding(
    db_path: Path | str,
    host_id: str,
    event: AgentEvent,
    *,
    expected_binding: WorkerBinding,
    content: Mapping[str, Any] | None = None,
    _fault_inject: Callable[[str], None] | None = None,
) -> AppendProjectedAgentEventResult:
    """Atomically journal an agent event and apply its text-only turn projection.

    A replay may repair a provably absent projection exactly once, using the
    immutable observation time retained with the original event.  It never
    re-merges caller data into a live or superseded owned projection.
    """

    normalized_host = normalize_agent_event_identifier(
        host_id, "host_id", required=True
    )
    _canonical_agent_event_for_append(event)
    if not isinstance(expected_binding, WorkerBinding):
        raise ValueError("expected_binding must be a WorkerBinding")
    if content is not None:
        if not isinstance(content, Mapping):
            raise ValueError("content must be a mapping or None")
        if not str(content.get("source_turn_id") or "").strip():
            raise ValueError("projected agent content requires source_turn_id")
    if _fault_inject is not None and not callable(_fault_inject):
        raise TypeError("_fault_inject must be callable or None")
    rearm_key: tuple[str, str] | None = None

    def fault(boundary: str) -> None:
        if _fault_inject is not None:
            _fault_inject(boundary)

    with _connect(db_path, prepare=True, isolation_level=None) as conn:
        _ensure_schema(conn)
        conn.execute("BEGIN IMMEDIATE")
        try:
            if not _agent_event_binding_matches_conn(
                conn,
                normalized_host or "",
                event.worker_id,
                expected_binding,
            ):
                conn.rollback()
                return AppendProjectedAgentEventResult(
                    event=AppendBoundAgentEventResult(
                        status="binding_changed",
                        event_id=event.event_id,
                    )
                )
            fault("after_binding_check")
            appended = _append_agent_event_conn(
                conn,
                normalized_host or "",
                event,
            )
            fault("after_event_append")
            turn: TurnRefreshApplyResult | None = None
            authority_time = _agent_event_authority_observed_at_conn(
                conn,
                normalized_host or "",
                event.event_id,
            )
            projection_allowed = content is not None
            if not appended.inserted and content is not None:
                durable_source_turn = str(event.source_turn_id or "").strip()
                caller_source_turn = str(content.get("source_turn_id") or "").strip()
                projection_allowed = bool(
                    authority_time is not None
                    and durable_source_turn
                    and caller_source_turn == durable_source_turn
                    and not _owned_turn_projection_exists_conn(
                        conn,
                        normalized_host or "",
                        event.worker_id,
                        durable_source_turn,
                    )
                )
            if projection_allowed and content is not None:
                if authority_time is None:
                    raise StoreSchemaError("agent_event_authority_time_missing")
                worker_exists = conn.execute(
                    "SELECT 1 FROM workers WHERE host_id = ? AND worker_id = ?",
                    (normalized_host or "", event.worker_id),
                ).fetchone()
                if worker_exists is None:
                    raise StoreSchemaError("agent_event_projection_worker_missing")
                merge_result = _merge_turn_content_conn(
                    conn,
                    normalized_host or "",
                    event.worker_id,
                    content,
                    observed_at=authority_time,
                )
                rearm_key = (
                    merge_result.submission_link_rearm
                    or merge_result.submission_link
                )
                if merge_result.submission_link is not None:
                    owner_key, fingerprint = merge_result.submission_link
                    _settle_submission_links_conn(
                        conn,
                        normalized_host or "",
                        owner_key,
                        fingerprint,
                        now=authority_time,
                    )
                turn = TurnRefreshApplyResult(merge_result.updated, False)
            fault("after_turn_projection")
            fault("before_commit")
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    if rearm_key is not None:
        _rearm_submission_link_component(
            db_path,
            normalized_host or "",
            rearm_key[0],
            rearm_key[1],
        )
    return AppendProjectedAgentEventResult(
        event=AppendBoundAgentEventResult(
            status="inserted" if appended.inserted else "replayed",
            event_id=appended.event_id,
            sequence=appended.sequence,
        ),
        turn=turn,
    )


def apply_turn_refresh(
    db_path: Path | str,
    host_id: str,
    worker_id: str,
    content: Mapping[str, Any],
    *,
    backend_pending_observation: PendingObservation | object = _UNSET,
    expected_binding: WorkerBinding | None = None,
    deadline_monotonic: float | None = None,
    cancelled: Callable[[], bool] | None = None,
    observed_at: str | None = None,
    pending_stale_grace_seconds: float = DEFAULT_PENDING_STALE_GRACE_SECONDS,
) -> TurnRefreshApplyResult:
    """Atomically apply one binding's turn observation and optional pending state."""
    if not _sqlite_store_exists(db_path):
        return TurnRefreshApplyResult(0, False)
    current_time, _ = _pending_observed_time(observed_at)
    with _connect(db_path, isolation_level=None) as conn:
        _ensure_schema(conn)
        # Transaction acquisition must precede every merge-base read. Use
        # cancellable short waits for daemon-owned refresh work so a stopped
        # scheduler cannot commit after its deadline.
        if not _begin_turn_refresh_transaction(
            conn,
            deadline_monotonic=deadline_monotonic,
            cancelled=cancelled,
        ):
            return TurnRefreshApplyResult(0, False, False, True)
        try:
            if _turn_refresh_is_cancelled(
                deadline_monotonic=deadline_monotonic,
                cancelled=cancelled,
            ):
                conn.rollback()
                return TurnRefreshApplyResult(0, False, False, True)
            if expected_binding is not None and not _turn_refresh_binding_matches_conn(
                conn,
                str(host_id),
                str(worker_id),
                expected_binding,
            ):
                conn.rollback()
                return TurnRefreshApplyResult(0, False, True)
            merge_result = _merge_turn_content_conn(
                conn,
                str(host_id),
                str(worker_id),
                content,
                observed_at=current_time,
            )
            updated = merge_result.updated
            rearm_key = (
                merge_result.submission_link_rearm
                or merge_result.submission_link
            )
            if rearm_key is not None:
                owner_key, fingerprint = rearm_key
                _rearm_submission_link_component(
                    db_path,
                    str(host_id),
                    owner_key,
                    fingerprint,
                )
            if merge_result.submission_link is not None:
                owner_key, fingerprint = merge_result.submission_link
                conn.execute("SAVEPOINT settle_submission_links")
                try:
                    _settle_submission_links_conn(
                        conn,
                        str(host_id),
                        owner_key,
                        fingerprint,
                        now=current_time,
                    )
                except Exception:
                    conn.execute("ROLLBACK TO SAVEPOINT settle_submission_links")
                    _LOGGER.warning(
                        "turn_submission_link_settlement_failed",
                        extra={
                            "tendwire_diagnostic": {
                                "code": "turn_submission_link_settlement_failed",
                                "host_id": str(host_id),
                            }
                        },
                    )
                finally:
                    conn.execute("RELEASE SAVEPOINT settle_submission_links")
            if backend_pending_observation is not _UNSET:
                if not isinstance(
                    backend_pending_observation,
                    PendingObservation,
                ):
                    raise TypeError("invalid backend pending observation")
                pending_changed = _apply_backend_pending_observation_conn(
                    conn,
                    str(host_id),
                    str(worker_id),
                    backend_pending_observation,
                    observed_at=current_time,
                    stale_grace_seconds=pending_stale_grace_seconds,
                    binding_private_fingerprint=(
                        str(expected_binding.private_fingerprint)
                        if expected_binding is not None
                        else ""
                    ),
                    observed_turn_target_value=(
                        str(expected_binding.turn_target_value or "")
                        if expected_binding is not None
                        else ""
                    ),
                    # The expected binding was checked against the active row
                    # above in this same transaction.  It may therefore take
                    # ownership from stale pending state left by an expired
                    # pane binding for the same stable worker.
                    binding_authoritative=expected_binding is not None,
                )
            else:
                pending_changed = False
            if _turn_refresh_is_cancelled(
                deadline_monotonic=deadline_monotonic,
                cancelled=cancelled,
            ):
                conn.rollback()
                return TurnRefreshApplyResult(0, False, False, True)
            conn.commit()
            return TurnRefreshApplyResult(updated, pending_changed)
        except Exception:
            conn.rollback()
            raise




def _update_turn_row(
    conn: sqlite3.Connection,
    host_id: str,
    turn_id: Any,
    payload: dict[str, Any],
    current_time: str,
) -> bool:
    item = _strip_canonical_turn_payload(Turn.from_dict(payload).to_dict())
    encoded = _canonical_json(item)
    row = conn.execute(
        """
        SELECT status, kind, updated_at, fingerprint, payload_json
        FROM turns
        WHERE host_id = ? AND turn_id = ?
        """,
        (str(host_id), str(turn_id)),
    ).fetchone()
    values = (
        str(item.get("status") or "unknown"),
        str(item.get("kind") or "unknown"),
        item.get("updated_at") or (row[2] if row is not None else None) or current_time,
        str(item.get("fingerprint") or ""),
        encoded,
    )
    if row is not None and tuple(row) == values:
        return False
    conn.execute(
        """
        UPDATE turns
        SET status = ?,
            kind = ?,
            updated_at = ?,
            fingerprint = ?,
            observed_at = ?,
            payload_json = ?
        WHERE host_id = ? AND turn_id = ?
        """,
        (
            values[0],
            values[1],
            values[2],
            values[3],
            current_time,
            values[4],
            str(host_id),
            str(turn_id),
        ),
    )
    return True



_TURN_DELTA_PROJECTION_SELECT = """
    turns.payload_json, turns.observed_at, turns.turn_id, turns.worker_id,
    turns.list_sequence, revisions.content_revision, revisions.user_state,
    revisions.user_char_length, revisions.user_byte_length,
    CASE WHEN revisions.user_state = 'complete' THEN revisions.user_page_count ELSE 0 END,
    CASE WHEN revisions.user_state = 'complete'
              AND revisions.user_char_length BETWEEN 1 AND :text_max
         THEN revisions.user_text END,
    CASE WHEN revisions.user_state != 'absent'
              AND NOT (revisions.user_state = 'complete'
                       AND revisions.user_char_length BETWEEN 1 AND :text_max)
         THEN substr(revisions.user_text, 1, :preview_max) END,
    revisions.final_state, revisions.final_char_length, revisions.final_byte_length,
    CASE WHEN revisions.final_state = 'complete' THEN revisions.final_page_count ELSE 0 END,
    CASE WHEN revisions.final_state = 'complete'
              AND revisions.final_char_length BETWEEN 1 AND :text_max
         THEN revisions.assistant_final_text END,
    CASE WHEN revisions.final_state != 'absent'
              AND NOT (revisions.final_state = 'complete'
                       AND revisions.final_char_length BETWEEN 1 AND :text_max)
         THEN substr(revisions.assistant_final_text, 1, :preview_max) END,
    linked_submission.submission_id, linked_submission.state
"""


def _turn_delta_projection(
    row: tuple[Any, ...],
) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    """Apply the same sanitizer and schema-v2 content projector as turn.list."""
    (
        payload_json, _observed_at, turn_id, _worker_id, _list_sequence,
        revision, user_state, user_char_length, user_byte_length, user_page_count,
        user_inline, user_preview, final_state, final_char_length,
        final_byte_length, final_page_count, final_inline, final_preview,
        submission_id, submission_state,
    ) = row[:20]
    try:
        loaded = json.loads(str(payload_json or "{}"))
    except (TypeError, json.JSONDecodeError):
        loaded = {}
    if not isinstance(loaded, Mapping):
        loaded = {}
    turn_payload = sanitize_public_mapping(loaded)
    if not turn_payload or is_internal_automation_turn_payload(turn_payload):
        return None, dict(loaded)
    serialized = Turn.from_dict(turn_payload).to_dict()
    item = _strip_canonical_turn_payload(serialized)
    item["id"] = str(turn_id)
    source_turn_id = str(turn_payload.get("source_turn_id") or "").strip()
    stored_meta = turn_payload.get("meta")
    if source_turn_id and isinstance(stored_meta, Mapping):
        if source_turn_id in turn_source_id_candidates(
            source_turn_id,
            meta=stored_meta,
            source=turn_payload.get("source"),
            kind=turn_payload.get("kind"),
        ):
            item["source_turn_id"] = source_turn_id
    if revision is not None:
        item.update(project_persisted_turn_content(
            str(revision),
            user_state=str(user_state), user_char_length=int(user_char_length),
            user_byte_length=int(user_byte_length), user_page_count=int(user_page_count),
            user_inline=user_inline, user_preview=user_preview,
            final_state=str(final_state), final_char_length=int(final_char_length),
            final_byte_length=int(final_byte_length), final_page_count=int(final_page_count),
            final_inline=final_inline, final_preview=final_preview,
        ))
    if submission_id is not None and submission_state == "linked":
        item["submission_id"] = str(submission_id)
        item["submission_state"] = "linked"
    item["schema_version"] = TURN_DELTA_PROJECTION_SCHEMA_VERSION
    return item, dict(loaded)


def _turn_delta_remove(
    turn_id: str,
    *,
    changed_at: str,
    payload: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    try:
        clean_turn = Turn.from_dict(payload or {})
    except (TypeError, ValueError):
        clean_turn = None
    change: dict[str, Any] = {
        "op": "remove",
        "turn_id": str(turn_id),
        "removed_at": str(
            (clean_turn.superseded_at if clean_turn is not None else None)
            or changed_at
            or utc_timestamp()
        ),
    }
    replacement = (
        clean_turn.superseded_by_turn_id if clean_turn is not None else None
    )
    if replacement:
        change["superseded_by_turn_id"] = replacement
    return change


def _turn_delta_descriptor_only(projected: Mapping[str, Any]) -> dict[str, Any]:
    """Bound one turn change while preserving its content-page descriptor."""
    descriptor_keys = (
        "schema_version",
        "id",
        "host_id",
        "worker_id",
        "worker_fingerprint",
        "space_id",
        "status",
        "kind",
        "complete",
        "has_open_turn",
        "started_at",
        "updated_at",
        "completed_at",
        "source",
        "origin_command_id",
        "source_turn_id",
        "superseded_by_turn_id",
        "superseded_at",
        "fingerprint",
        "submission_id",
        "submission_state",
    )
    bounded = {key: projected[key] for key in descriptor_keys if key in projected}
    raw_content = projected.get("content")
    if isinstance(raw_content, Mapping):
        content = dict(raw_content)
        revision = str(content.get("content_revision") or "")
        raw_fields = content.get("fields")
        if revision and isinstance(raw_fields, Mapping):
            fields: dict[str, Any] = {}
            for field in ("user_text", "assistant_final_text"):
                raw_field = raw_fields.get(field)
                if not isinstance(raw_field, Mapping):
                    continue
                field_descriptor = dict(raw_field)
                field_descriptor["inline"] = False
                if (
                    field_descriptor.get("availability") == "complete"
                    and int(field_descriptor.get("page_count") or 0) > 0
                ):
                    field_descriptor["first_cursor"] = content_cursor(
                        revision,
                        field,
                        0,
                        start_char=0,
                        start_byte=0,
                    )
                fields[field] = field_descriptor
            content["fields"] = fields
        bounded["content"] = content
    return bounded


def _turn_delta_payload_from_store(
    db_path: Path | str,
    host_id: str,
    *,
    watermark: str | None = None,
    cursor: str | None = None,
    limit: int = TURN_DELTA_DEFAULT_LIMIT,
    now: float | int | None = None,
    work_counters: TurnDeltaWorkCounters | None = None,
    batch_sequence_ceiling: int = TURN_DELTA_MAX_BATCH_SEQUENCES,
    bootstrap_max_rows: int = TURN_DELTA_BOOTSTRAP_MAX_ROWS,
    bootstrap_max_pages: int = TURN_DELTA_BOOTSTRAP_MAX_PAGES,
) -> dict[str, Any]:
    """Return one atomic, frozen, byte-bounded public turn-delta page."""
    started = time.perf_counter()
    host = str(host_id)
    error_base = {
        "schema_version": TURN_DELTA_SCHEMA_VERSION,
        "projection_schema_version": TURN_DELTA_PROJECTION_SCHEMA_VERSION,
        "host_id": host,
        "ok": False,
    }
    if (
        not isinstance(limit, int) or isinstance(limit, bool)
        or not 1 <= limit <= TURN_DELTA_MAX_LIMIT
        or watermark is not None and cursor is not None
        or watermark is not None and (not isinstance(watermark, str) or not watermark)
        or cursor is not None and (not isinstance(cursor, str) or not cursor)
    ):
        return {
            **error_base,
            "status": "invalid_cursor" if cursor is not None else "invalid_watermark",
        }
    if not _sqlite_store_exists(db_path):
        return {**error_base, "status": "store_unavailable"}
    clock = time.time() if now is None else float(now)
    try:
        decoded_watermark = (
            decode_turn_delta_watermark(watermark, host_id=host)
            if watermark is not None else None
        )
        decoded_cursor = (
            decode_turn_delta_cursor(cursor, host_id=host, limit=limit, now=clock)
            if cursor is not None else None
        )
    except ValueError as exc:
        status = str(exc)
        allowed = {
            "invalid_watermark", "expired_watermark", "cross_host_watermark",
            "incompatible_schema", "invalid_cursor", "expired_cursor",
        }
        return {
            **error_base,
            "status": status if status in allowed else (
                "invalid_cursor" if cursor is not None else "invalid_watermark"
            ),
        }

    sweep_key, sweep_due = _reserve_lazy_submission_link_sweep(
        db_path,
        host,
        purpose="submission_links",
        current_clock=clock,
        refresh_interval_seconds=DEFAULT_SUBMISSION_LINK_SWEEP_INTERVAL_SECONDS,
    )
    if sweep_due:
        try:
            _sweep_submission_links(
                Path(db_path),
                host_id=host,
                now=datetime.fromtimestamp(clock, tz=timezone.utc).isoformat(),
            )
        except Exception:
            _release_failed_submission_link_sweep(
                sweep_key,
                current_clock=clock,
            )
            # Delta remains available if opportunistic linkage is contended.
            pass

    with _connect(db_path, isolation_level=None) as conn:
        _ensure_schema(conn)
        conn.execute("BEGIN")
        try:
            store_epoch = _turn_change_store_epoch_conn(conn)
            floor_row = conn.execute(
                "SELECT floor_seq FROM turn_change_floor WHERE host_id = ?", (host,)
            ).fetchone()
            floor = int(floor_row[0]) if floor_row is not None else 0
            journal_high = int(conn.execute(
                "SELECT COALESCE(MAX(seq), 0) FROM turn_change_journal WHERE host_id = ?",
                (host,),
            ).fetchone()[0] or 0)
            insertion_high, _generation = _turn_list_host_state_conn(conn, host)
            if decoded_cursor is not None:
                mode = decoded_cursor.mode
                if decoded_cursor.store_epoch != store_epoch:
                    conn.rollback()
                    return {**error_base, "status": "invalid_cursor"}
                if (
                    decoded_cursor.mode == "changes"
                    and decoded_cursor.accepted_sequence < floor
                ):
                    conn.rollback()
                    return {**error_base, "status": "expired_cursor"}
                accepted = decoded_cursor.accepted_sequence
                batch_high = decoded_cursor.batch_high
                insertion_high = decoded_cursor.insertion_high
                expires_at = decoded_cursor.expires_at
                page_number = decoded_cursor.page_number + 1
            elif decoded_watermark is not None:
                mode = "changes"
                if (
                    decoded_watermark.store_epoch != store_epoch
                    or decoded_watermark.sequence > journal_high
                ):
                    conn.rollback()
                    return {**error_base, "status": "invalid_watermark"}
                if decoded_watermark.sequence < floor:
                    conn.rollback()
                    return {**error_base, "status": "expired_watermark"}
                accepted = decoded_watermark.sequence
                batch_high = min(
                    journal_high,
                    accepted + max(1, int(batch_sequence_ceiling)),
                )
                expires_at = int(clock) + TURN_DELTA_CURSOR_TTL_SECONDS
                page_number = 1
            else:
                mode = "bootstrap"
                accepted = 0
                batch_high = journal_high
                expires_at = int(clock) + TURN_DELTA_CURSOR_TTL_SECONDS
                page_number = 1
                bootstrap_count = int(conn.execute(
                    """
                    SELECT COUNT(*) FROM turns
                    WHERE host_id = ? AND list_sequence <= ?
                      AND COALESCE(json_extract(payload_json, '$.superseded_at'), '') = ''
                    """,
                    (host, insertion_high),
                ).fetchone()[0] or 0)
                if (
                    bootstrap_count > max(1, int(bootstrap_max_rows))
                    or math.ceil(bootstrap_count / TURN_DELTA_MAX_LIMIT)
                    > max(1, int(bootstrap_max_pages))
                ):
                    conn.rollback()
                    return {**error_base, "status": "bootstrap_too_large"}

            params: dict[str, Any] = {
                "host_id": host,
                "text_max": TURN_TEXT_MAX_CHARS,
                "preview_max": TURN_CONTENT_PREVIEW_MAX_CHARS,
                "limit": limit + 1,
            }
            if mode == "bootstrap":
                continuation = ""
                if decoded_cursor is not None:
                    continuation = """
                      AND (turns.worker_id > :position_worker
                           OR (turns.worker_id = :position_worker AND (
                               turns.list_sequence < :position_sequence
                               OR (turns.list_sequence = :position_sequence
                                   AND turns.turn_id > :position_turn))))
                    """
                    params.update({
                        "position_worker": decoded_cursor.position_worker_id,
                        "position_sequence": decoded_cursor.position_sequence,
                        "position_turn": decoded_cursor.position_turn_id,
                    })
                params["insertion_high"] = insertion_high
                rows = conn.execute(f"""
                    SELECT {_TURN_DELTA_PROJECTION_SELECT}
                    FROM turns
                    LEFT JOIN turn_content_revisions AS revisions
                      ON revisions.host_id = turns.host_id
                     AND revisions.turn_id = turns.turn_id
                     AND revisions.is_current = 1
                    LEFT JOIN turn_submissions AS linked_submission
                      ON linked_submission.host_id = turns.host_id
                     AND linked_submission.linked_turn_id = turns.turn_id
                     AND linked_submission.state = 'linked'
                    WHERE turns.host_id = :host_id
                      AND turns.list_sequence <= :insertion_high
                      AND COALESCE(json_extract(turns.payload_json, '$.superseded_at'), '') = ''
                      {continuation}
                    ORDER BY turns.worker_id, turns.list_sequence DESC, turns.turn_id
                    LIMIT :limit
                """, params).fetchall()
            else:
                continuation = ""
                if decoded_cursor is not None:
                    continuation = """
                    WHERE collapsed.seq > :position_sequence
                       OR (collapsed.seq = :position_sequence
                           AND collapsed.turn_id > :position_turn)
                    """
                    params.update({
                        "position_sequence": decoded_cursor.position_sequence,
                        "position_turn": decoded_cursor.position_turn_id,
                    })
                params.update({"accepted": accepted, "batch_high": batch_high})
                rows = conn.execute(f"""
                    WITH collapsed AS (
                        SELECT turn_id, MAX(seq) AS seq,
                               SUM(COUNT(*)) OVER () AS raw_count
                        FROM turn_change_journal
                        WHERE host_id = :host_id
                          AND seq > :accepted AND seq <= :batch_high
                        GROUP BY turn_id
                    ), positioned AS (
                        SELECT collapsed.turn_id, collapsed.seq, collapsed.raw_count
                        FROM collapsed
                        {continuation}
                        ORDER BY collapsed.seq, collapsed.turn_id
                        LIMIT :limit
                    )
                    SELECT {_TURN_DELTA_PROJECTION_SELECT},
                           positioned.seq, positioned.turn_id, journal.changed_at,
                           positioned.raw_count
                    FROM positioned
                    JOIN turn_change_journal AS journal
                      ON journal.host_id = :host_id
                     AND journal.turn_id = positioned.turn_id
                     AND journal.seq = positioned.seq
                    LEFT JOIN turns
                      ON turns.host_id = :host_id
                     AND turns.turn_id = positioned.turn_id
                    LEFT JOIN turn_content_revisions AS revisions
                      ON revisions.host_id = turns.host_id
                     AND revisions.turn_id = turns.turn_id
                     AND revisions.is_current = 1
                    LEFT JOIN turn_submissions AS linked_submission
                      ON linked_submission.host_id = turns.host_id
                     AND linked_submission.linked_turn_id = turns.turn_id
                     AND linked_submission.state = 'linked'
                    ORDER BY positioned.seq, positioned.turn_id
                """, params).fetchall()
            if work_counters is not None:
                if mode == "changes":
                    work_counters.journal_queries += 1
                    work_counters.journal_rows_scanned += (
                        int(rows[0][23]) if rows else 0
                    )
                work_counters.projection_queries += 1
                work_counters.projection_rows_read += len(rows)
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    changes: list[dict[str, Any]] = []
    positions: list[tuple[str, int, str]] = []
    accumulated_bytes = 0
    has_more = False
    last_scanned: tuple[str, int, str] | None = None
    for index, row in enumerate(rows):
        if index >= limit:
            has_more = True
            break
        if mode == "bootstrap":
            worker, sequence, turn_id = str(row[3]), int(row[4]), str(row[2])
            changed_at = str(row[1] or utc_timestamp())
        else:
            worker, sequence, turn_id = "", int(row[20]), str(row[21])
            changed_at = str(row[22] or utc_timestamp())
        projected, raw_payload = _turn_delta_projection(tuple(row[:20]))
        tombstoned = bool(str(raw_payload.get("superseded_at") or "").strip())
        if mode == "changes" and (
            row[2] is None or tombstoned or projected is None
        ):
            change = _turn_delta_remove(turn_id, changed_at=changed_at, payload=raw_payload)
        elif projected is None:
            last_scanned = (worker, sequence, turn_id)
            continue
        else:
            change = {
                "op": "upsert", "turn_id": turn_id,
                "changed_at": changed_at, "turn": projected,
            }
        item_bytes = len(_canonical_json(change).encode("utf-8")) + 1
        if item_bytes > 850_000 and change["op"] == "upsert":
            change["turn"] = _turn_delta_descriptor_only(change["turn"])
            item_bytes = len(_canonical_json(change).encode("utf-8")) + 1
        if changes and accumulated_bytes + item_bytes > 850_000:
            has_more = True
            break
        changes.append(change)
        positions.append((worker, sequence, turn_id))
        last_scanned = (worker, sequence, turn_id)
        accumulated_bytes += item_bytes

    next_cursor: str | None = None
    if has_more and last_scanned is not None:
        next_cursor = turn_delta_cursor(
            host, mode=mode, limit=limit, accepted_sequence=accepted,
            batch_high=batch_high, insertion_high=insertion_high,
            page_number=page_number, position_worker_id=last_scanned[0],
            position_sequence=last_scanned[1], position_turn_id=last_scanned[2],
            store_epoch=store_epoch, expires_at=expires_at,
        )
    else:
        has_more = False
    checkpoint = None if has_more else turn_delta_watermark(
        host, sequence=batch_high, store_epoch=store_epoch
    )
    duration_ms = min(
        2_147_483_647,
        max(0, int((time.perf_counter() - started) * 1000)),
    )
    payload: dict[str, Any] = {
        "schema_version": TURN_DELTA_SCHEMA_VERSION,
        "projection_schema_version": TURN_DELTA_PROJECTION_SCHEMA_VERSION,
        "host_id": host,
        "mode": mode,
        "changes": changes,
        "has_more": has_more,
        "next_cursor": next_cursor,
        "checkpoint": checkpoint,
        "aggregate": {
            "journal_rows_scanned": (
                int(rows[0][23]) if mode == "changes" and rows else 0
            ),
            "projection_rows_read": len(rows),
            "changes_returned": len(changes),
            "duration_ms": duration_ms,
        },
    }
    while len(_canonical_json(payload).encode("utf-8")) >= 1024 * 1024 and len(changes) > 1:
        changes.pop()
        last_worker, last_sequence, last_turn = positions[len(changes) - 1]
        payload["has_more"] = True
        payload["checkpoint"] = None
        payload["next_cursor"] = turn_delta_cursor(
            host, mode=mode, limit=limit, accepted_sequence=accepted,
            batch_high=batch_high, insertion_high=insertion_high,
            page_number=page_number, position_worker_id=last_worker,
            position_sequence=last_sequence, position_turn_id=last_turn,
            store_epoch=store_epoch, expires_at=expires_at,
        )
        payload["aggregate"]["changes_returned"] = len(changes)
    if work_counters is not None:
        work_counters.max_response_utf8_bytes = max(
            work_counters.max_response_utf8_bytes,
            len(_canonical_json(payload).encode("utf-8")),
        )
    return payload


def turn_delta_payload_from_store(
    db_path: Path | str,
    host_id: str,
    *,
    watermark: str | None = None,
    cursor: str | None = None,
    limit: int = TURN_DELTA_DEFAULT_LIMIT,
    now: float | int | None = None,
    work_counters: TurnDeltaWorkCounters | None = None,
    batch_sequence_ceiling: int = TURN_DELTA_MAX_BATCH_SEQUENCES,
    bootstrap_max_rows: int = TURN_DELTA_BOOTSTRAP_MAX_ROWS,
    bootstrap_max_pages: int = TURN_DELTA_BOOTSTRAP_MAX_PAGES,
) -> dict[str, Any]:
    """Fail closed to the documented public outcome for unavailable stores."""
    try:
        return _turn_delta_payload_from_store(
            db_path,
            host_id,
            watermark=watermark,
            cursor=cursor,
            limit=limit,
            now=now,
            work_counters=work_counters,
            batch_sequence_ceiling=batch_sequence_ceiling,
            bootstrap_max_rows=bootstrap_max_rows,
            bootstrap_max_pages=bootstrap_max_pages,
        )
    except (sqlite3.Error, StoreSchemaError, LocalStateError, OSError):
        return {
            "schema_version": TURN_DELTA_SCHEMA_VERSION,
            "projection_schema_version": TURN_DELTA_PROJECTION_SCHEMA_VERSION,
            "host_id": str(host_id),
            "ok": False,
            "status": "store_unavailable",
        }


def turns_payload_from_store(
    db_path: Path | str,
    host_id: str,
    *,
    snapshot: Snapshot | None = None,
    schema_version: int = 1,
    limit: int = TURN_LIST_DEFAULT_LIMIT,
    cursor: str | None = None,
    since: str | None = None,
    now: float | int | None = None,
    work_counters: TurnContentWorkCounters | None = None,
    turn_refresh_interval_seconds: float = 2.0,
) -> dict[str, Any]:
    """Return one insertion-stable, byte-bounded turn-list page."""
    requested_schema = int(schema_version)
    if requested_schema not in {1, TURN_LIST_SCHEMA_VERSION}:
        return {
            "schema_version": requested_schema,
            "ok": False,
            "status": "unsupported_turn_schema_version",
            "required_turn_schema_version": TURN_LIST_SCHEMA_VERSION,
        }
    if (
        not isinstance(limit, int)
        or isinstance(limit, bool)
        or not 1 <= limit <= TURN_LIST_MAX_LIMIT
        or cursor is not None and since is not None
        or cursor is not None and (not isinstance(cursor, str) or not cursor)
        or since is not None and (not isinstance(since, str) or not since)
    ):
        return {
            "schema_version": requested_schema,
            "ok": False,
            "status": "invalid_cursor",
        }
    if not _sqlite_store_exists(db_path):
        return {
            "schema_version": requested_schema,
            "host_id": str(host_id),
            "ok": False,
            "status": "store_unavailable",
        }
    current_clock = time.time() if now is None else float(now)
    refresh_interval = float(turn_refresh_interval_seconds)
    submission_sweep_key, submission_sweep_due = _reserve_lazy_submission_link_sweep(
        db_path,
        str(host_id),
        purpose="submission_links",
        current_clock=current_clock,
        refresh_interval_seconds=refresh_interval,
    )
    if submission_sweep_due:
        try:
            _sweep_submission_links(
                Path(db_path),
                host_id=str(host_id),
                now=datetime.fromtimestamp(
                    current_clock,
                    tz=timezone.utc,
                ).isoformat(),
            )
        except Exception:
            _release_failed_submission_link_sweep(
                submission_sweep_key,
                current_clock=current_clock,
            )
            # Listing remains available if opportunistic maintenance is contended.
            pass
    try:
        decoded_cursor = (
            decode_turn_list_cursor(
                cursor,
                host_id=str(host_id),
                schema_version=requested_schema,
                limit=limit,
                now=current_clock,
            )
            if cursor is not None
            else None
        )
        decoded_since = (
            decode_turn_since_token(
                since,
                host_id=str(host_id),
                schema_version=requested_schema,
            )
            if since is not None
            else None
        )
    except ValueError as exc:
        status = "cursor_expired" if str(exc) == "cursor_expired" else "invalid_cursor"
        return {
            "schema_version": requested_schema,
            "ok": False,
            "status": status,
        }
    with _connect(db_path, isolation_level=None) as conn:
        _ensure_schema(conn)
        conn.execute("BEGIN")
        try:
            store_epoch = _turn_list_store_epoch_conn(conn)
            row = conn.execute(
                """
                SELECT COALESCE(MIN(list_sequence), 0)
                FROM turns
                WHERE host_id = ?
                """,
                (str(host_id),),
            ).fetchone()
            current_floor = int(row[0] if row is not None else 0)
            current_high, current_generation = _turn_list_host_state_conn(
                conn,
                str(host_id),
            )
            if decoded_cursor is not None:
                if (
                    decoded_cursor.store_epoch != store_epoch
                    or decoded_cursor.watermark > current_high
                    or decoded_cursor.traversal_generation != current_generation
                    or (
                        decoded_cursor.floor_sequence
                        and current_floor > decoded_cursor.floor_sequence
                    )
                ):
                    conn.rollback()
                    return {
                        "schema_version": requested_schema,
                        "ok": False,
                        "status": "cursor_expired",
                    }
                anchor = conn.execute(
                    """
                    SELECT 1
                    FROM turns
                    WHERE host_id = ?
                      AND worker_id = ?
                      AND list_sequence = ?
                      AND turn_id = ?
                    """,
                    (
                        str(host_id),
                        decoded_cursor.worker_id,
                        decoded_cursor.list_sequence,
                        decoded_cursor.turn_id,
                    ),
                ).fetchone()
                if anchor is None:
                    conn.rollback()
                    return {
                        "schema_version": requested_schema,
                        "ok": False,
                        "status": "cursor_expired",
                    }
                original_since = decoded_cursor.since_sequence
                watermark = decoded_cursor.watermark
                floor_sequence = decoded_cursor.floor_sequence
                traversal_generation = decoded_cursor.traversal_generation
                expires_at = decoded_cursor.expires_at
            else:
                if decoded_since is not None:
                    if (
                        decoded_since.store_epoch != store_epoch
                        or decoded_since.watermark > current_high
                    ):
                        conn.rollback()
                        return {
                            "schema_version": requested_schema,
                            "ok": False,
                            "status": "since_expired",
                        }
                    original_since = decoded_since.watermark
                else:
                    original_since = 0
                watermark = current_high
                floor_sequence = current_floor
                traversal_generation = current_generation
                expires_at = int(current_clock) + TURN_LIST_CURSOR_TTL_SECONDS
            parameters: list[Any] = [
                TURN_TEXT_MAX_CHARS,
                TURN_TEXT_MAX_CHARS,
                TURN_CONTENT_PREVIEW_MAX_CHARS,
                TURN_TEXT_MAX_CHARS,
                TURN_TEXT_MAX_CHARS,
                TURN_CONTENT_PREVIEW_MAX_CHARS,
                str(host_id),
                original_since,
                watermark,
            ]
            continuation = ""
            if decoded_cursor is not None:
                continuation = """
                  AND (
                        turns.worker_id > ?
                     OR (
                            turns.worker_id = ?
                        AND (
                               turns.list_sequence < ?
                            OR (
                                   turns.list_sequence = ?
                               AND turns.turn_id > ?
                            )
                        )
                     )
                  )
                """
                parameters.extend(
                    [
                        decoded_cursor.worker_id,
                        decoded_cursor.worker_id,
                        decoded_cursor.list_sequence,
                        decoded_cursor.list_sequence,
                        decoded_cursor.turn_id,
                    ]
                )
            parameters.append(limit + 1)
            rows = conn.execute(
                f"""
                SELECT
                    turns.payload_json,
                    turns.observed_at,
                    turns.turn_id,
                    turns.worker_id,
                    turns.list_sequence,
                    revisions.content_revision,
                    revisions.user_state,
                    revisions.user_char_length,
                    revisions.user_byte_length,
                    CASE WHEN revisions.user_state = 'complete'
                         THEN revisions.user_page_count ELSE 0 END,
                    CASE
                        WHEN revisions.user_state = 'complete'
                         AND revisions.user_char_length BETWEEN 1 AND ?
                        THEN revisions.user_text
                    END,
                    CASE
                        WHEN revisions.user_state != 'absent'
                         AND NOT (
                             revisions.user_state = 'complete'
                             AND revisions.user_char_length BETWEEN 1 AND ?
                         )
                        THEN substr(revisions.user_text, 1, ?)
                    END,
                    revisions.final_state,
                    revisions.final_char_length,
                    revisions.final_byte_length,
                    CASE WHEN revisions.final_state = 'complete'
                         THEN revisions.final_page_count ELSE 0 END,
                    CASE
                        WHEN revisions.final_state = 'complete'
                         AND revisions.final_char_length BETWEEN 1 AND ?
                        THEN revisions.assistant_final_text
                    END,
                    CASE
                        WHEN revisions.final_state != 'absent'
                         AND NOT (
                             revisions.final_state = 'complete'
                             AND revisions.final_char_length BETWEEN 1 AND ?
                         )
                        THEN substr(revisions.assistant_final_text, 1, ?)
                    END
                FROM turns
                LEFT JOIN turn_content_revisions AS revisions
                  ON revisions.host_id = turns.host_id
                 AND revisions.turn_id = turns.turn_id
                 AND revisions.is_current = 1
                WHERE turns.host_id = ?
                  AND turns.list_sequence > ?
                  AND turns.list_sequence <= ?
                  AND COALESCE(
                        json_extract(turns.payload_json, '$.superseded_at'),
                        ''
                      ) = ''
                  {continuation}
                ORDER BY
                    turns.worker_id ASC,
                    turns.list_sequence DESC,
                    turns.turn_id ASC
                LIMIT ?
                """,
                tuple(parameters),
            ).fetchall()
            if work_counters is not None:
                work_counters.list_sql_queries += 1
                work_counters.list_descriptor_rows += len(rows)
                work_counters.list_inline_chars_examined += sum(
                    len(value)
                    for row in rows
                    for value in (row[10], row[16])
                    if isinstance(value, str)
                )
                work_counters.list_preview_chars_examined += sum(
                    len(value)
                    for row in rows
                    for value in (row[11], row[17])
                    if isinstance(value, str)
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    turns: list[dict[str, Any]] = []
    positions: list[tuple[str, int, str]] = []
    observed_values: list[str] = []
    incompatible_v1 = False
    accumulated_row_bytes = 0
    more_unprocessed = False
    last_scanned_position: tuple[str, int, str] | None = None
    for row_index, row in enumerate(rows):
        if row_index >= limit:
            more_unprocessed = True
            break
        (
            payload_json,
            observed_at,
            turn_id,
            worker_id,
            list_sequence,
            revision,
            user_state,
            user_char_length,
            user_byte_length,
            user_page_count,
            user_inline,
            user_preview,
            final_state,
            final_char_length,
            final_byte_length,
            final_page_count,
            final_inline,
            final_preview,
        ) = row
        current_position = (str(worker_id), int(list_sequence), str(turn_id))
        try:
            loaded = json.loads(str(payload_json or "{}"))
        except (TypeError, json.JSONDecodeError):
            loaded = {}
        if not isinstance(loaded, Mapping):
            loaded = {}
        turn_payload = sanitize_public_mapping(loaded)
        if not turn_payload or is_internal_automation_turn_payload(turn_payload):
            last_scanned_position = current_position
            continue
        serialized = Turn.from_dict(turn_payload).to_dict()
        item = _strip_canonical_turn_payload(serialized)
        item.update(_public_pending_turn_extension(turn_payload))
        item["id"] = str(turn_id)
        stored_source_turn_id = str(
            turn_payload.get("source_turn_id") or ""
        ).strip()
        stored_meta = turn_payload.get("meta")
        if stored_source_turn_id and isinstance(stored_meta, Mapping):
            source_candidates = turn_source_id_candidates(
                stored_source_turn_id,
                meta=stored_meta,
                source=turn_payload.get("source"),
                kind=turn_payload.get("kind"),
            )
            if stored_source_turn_id in source_candidates:
                item["source_turn_id"] = stored_source_turn_id
        if revision is not None:
            projection = project_persisted_turn_content(
                str(revision),
                user_state=str(user_state),
                user_char_length=int(user_char_length),
                user_byte_length=int(user_byte_length),
                user_page_count=int(user_page_count),
                user_inline=user_inline,
                user_preview=user_preview,
                final_state=str(final_state),
                final_char_length=int(final_char_length),
                final_byte_length=int(final_byte_length),
                final_page_count=int(final_page_count),
                final_inline=final_inline,
                final_preview=final_preview,
            )
            fields = projection["content"]["fields"]
            incompatible_v1 = incompatible_v1 or any(
                descriptor["availability"] != "absent"
                and not descriptor["inline"]
                for descriptor in fields.values()
            )
            item.update(projection)
        if requested_schema == 1:
            item["schema_version"] = 1
            item.pop("content", None)
            item.pop("user_preview", None)
            item.pop("assistant_final_preview", None)
            item.setdefault("user_text", None)
            item.setdefault("assistant_final_text", None)
        item_bytes = len(_canonical_json(item).encode("utf-8")) + 1
        if turns and accumulated_row_bytes + item_bytes > 850_000:
            more_unprocessed = True
            break
        turns.append(item)
        positions.append((str(worker_id), int(list_sequence), str(turn_id)))
        last_scanned_position = current_position
        accumulated_row_bytes += item_bytes
        if observed_at:
            observed_values.append(str(observed_at))
    if requested_schema == 1 and incompatible_v1:
        return {
            "schema_version": 1,
            "ok": False,
            "status": "upgrade_required",
            "required_turn_schema_version": TURN_LIST_SCHEMA_VERSION,
        }
    has_more = more_unprocessed
    if has_more and last_scanned_position is not None:
        last_worker, last_sequence, last_turn = last_scanned_position
        next_cursor = turn_list_cursor(
            str(host_id),
            schema_version=requested_schema,
            limit=limit,
            since_sequence=original_since,
            watermark=watermark,
            floor_sequence=floor_sequence,
            traversal_generation=traversal_generation,
            worker_id=last_worker,
            list_sequence=last_sequence,
            turn_id=last_turn,
            store_epoch=store_epoch,
            expires_at=expires_at,
        )
    else:
        has_more = False
        next_cursor = None
    watermark_token = turn_since_token(
        str(host_id),
        schema_version=requested_schema,
        watermark=watermark,
        store_epoch=store_epoch,
    )
    backend_health = sanitize_public_value(
        [health.to_dict() for health in snapshot.backend_health]
        if snapshot is not None
        else []
    )
    if not isinstance(backend_health, list):
        backend_health = []
    payload = {
        "schema_version": requested_schema,
        "host_id": str(host_id),
        "updated_at": (
            max(observed_values)
            if observed_values
            else (snapshot.updated_at if snapshot is not None else None)
        ),
        "turns": turns,
        "backend_health": backend_health,
        "next_cursor": next_cursor,
        "has_more": has_more,
        "as_of": watermark_token,
        "since": watermark_token,
    }
    payload["content_fingerprint"] = stable_fingerprint(
        {
            "schema_version": requested_schema,
            "host_id": str(host_id),
            "turns": turns,
            "backend_health": backend_health,
            "has_more": has_more,
            "as_of": watermark_token,
        }
    )
    while (
        len(_canonical_json(payload).encode("utf-8")) >= 1024 * 1024
        and len(turns) > 1
    ):
        turns.pop()
        last_worker, last_sequence, last_turn = positions[len(turns) - 1]
        payload["has_more"] = True
        payload["next_cursor"] = turn_list_cursor(
            str(host_id),
            schema_version=requested_schema,
            limit=limit,
            since_sequence=original_since,
            watermark=watermark,
            floor_sequence=floor_sequence,
            traversal_generation=traversal_generation,
            worker_id=last_worker,
            list_sequence=last_sequence,
            turn_id=last_turn,
            store_epoch=store_epoch,
            expires_at=expires_at,
        )
        payload["content_fingerprint"] = stable_fingerprint(
            {
                "schema_version": requested_schema,
                "host_id": str(host_id),
                "turns": turns,
                "backend_health": backend_health,
                "has_more": True,
                "as_of": watermark_token,
            }
        )
    _record_response_size(work_counters, payload)
    return payload


def _bounded_utf8_blob_page(raw: bytes) -> str:
    """Decode the longest code-point-complete prefix of one bounded byte window."""
    end = len(raw)
    minimum = max(0, end - 3)
    while end >= minimum:
        try:
            return raw[:end].decode("utf-8")
        except UnicodeDecodeError as exc:
            if exc.end != end:
                raise ValueError("invalid_canonical_utf8") from None
            end -= 1
    raise ValueError("invalid_canonical_utf8")


def _ensure_turn_content_page_boundaries_conn(
    conn: sqlite3.Connection,
    *,
    rowid: int,
    host_id: str,
    turn_id: str,
    content_revision_value: str,
    field: str,
    column: str,
    total_char_length: int,
    total_byte_length: int,
    page_count: int,
    work_counters: TurnContentWorkCounters | None,
    allow_rebuild: bool = True,
) -> None:
    existing_count = int(
        conn.execute(
            """
            SELECT COUNT(*)
            FROM turn_content_page_boundaries
            WHERE host_id = ?
              AND turn_id = ?
              AND content_revision = ?
              AND field = ?
            """,
            (
                str(host_id),
                str(turn_id),
                str(content_revision_value),
                str(field),
            ),
        ).fetchone()[0]
        or 0
    )
    if existing_count == page_count:
        return
    if existing_count:
        raise ValueError("invalid_content_metadata")
    if not allow_rebuild:
        raise ValueError("content_not_available")
    blob = conn.blobopen(
        "turn_content_revisions",
        str(column),
        int(rowid),
        readonly=True,
    )
    try:
        if len(blob) != total_byte_length:
            raise ValueError("invalid_content_metadata")
        start_byte = 0
        start_char = 0
        page_index = 0
        while start_byte < total_byte_length:
            blob.seek(start_byte)
            raw = blob.read(
                min(
                    TURN_CONTENT_PAGE_MAX_UTF8_BYTES,
                    total_byte_length - start_byte,
                )
            )
            text = _bounded_utf8_blob_page(raw)
            segment_byte_length = len(text.encode("utf-8"))
            segment_char_length = len(text)
            if (
                not segment_byte_length
                or segment_byte_length > TURN_CONTENT_PAGE_MAX_UTF8_BYTES
            ):
                raise ValueError("invalid_content_metadata")
            conn.execute(
                """
                INSERT INTO turn_content_page_boundaries (
                    host_id,
                    turn_id,
                    content_revision,
                    field,
                    page_index,
                    start_char,
                    start_byte
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    str(host_id),
                    str(turn_id),
                    str(content_revision_value),
                    str(field),
                    page_index,
                    start_char,
                    start_byte,
                ),
            )
            page_index += 1
            start_byte += segment_byte_length
            start_char += segment_char_length
            if work_counters is not None:
                work_counters.page_blob_reads += 1
                work_counters.page_bytes_examined += len(raw)
                work_counters.page_chars_examined += segment_char_length
        if (
            page_index != page_count
            or start_byte != total_byte_length
            or start_char != total_char_length
        ):
            raise ValueError("invalid_content_metadata")
    finally:
        blob.close()


def _backfill_missing_turn_content_page_boundaries_conn(
    conn: sqlite3.Connection,
) -> int:
    """Repair missing page boundaries for complete current revisions."""
    repaired_fields = 0
    cursor = conn.execute(
        """
        SELECT
            rowid,
            host_id,
            turn_id,
            content_revision,
            user_state,
            user_char_length,
            user_byte_length,
            user_page_count,
            final_state,
            final_char_length,
            final_byte_length,
            final_page_count
        FROM turn_content_revisions
        WHERE (user_state = 'complete' AND user_page_count > 0)
           OR (final_state = 'complete' AND final_page_count > 0)
        ORDER BY host_id, turn_id, content_revision
        """
    )
    while True:
        rows = cursor.fetchmany(64)
        if not rows:
            return repaired_fields
        for row in rows:
            fields = (
                (
                    "user_text",
                    "user_text",
                    str(row[4]),
                    int(row[5]),
                    int(row[6]),
                    int(row[7]),
                ),
                (
                    "assistant_final_text",
                    "assistant_final_text",
                    str(row[8]),
                    int(row[9]),
                    int(row[10]),
                    int(row[11]),
                ),
            )
            for (
                field,
                column,
                state,
                total_char_length,
                total_byte_length,
                page_count,
            ) in fields:
                if state != "complete" or page_count < 1:
                    continue
                existing_count = int(
                    conn.execute(
                        """
                        SELECT COUNT(*)
                        FROM turn_content_page_boundaries
                        WHERE host_id = ?
                          AND turn_id = ?
                          AND content_revision = ?
                          AND field = ?
                        """,
                        (
                            str(row[1]),
                            str(row[2]),
                            str(row[3]),
                            field,
                        ),
                    ).fetchone()[0]
                    or 0
                )
                if existing_count == page_count:
                    continue
                if existing_count:
                    conn.execute(
                        """
                        DELETE FROM turn_content_page_boundaries
                        WHERE host_id = ?
                          AND turn_id = ?
                          AND content_revision = ?
                          AND field = ?
                        """,
                        (
                            str(row[1]),
                            str(row[2]),
                            str(row[3]),
                            field,
                        ),
                    )
                _ensure_turn_content_page_boundaries_conn(
                    conn,
                    rowid=int(row[0]),
                    host_id=str(row[1]),
                    turn_id=str(row[2]),
                    content_revision_value=str(row[3]),
                    field=field,
                    column=column,
                    total_char_length=total_char_length,
                    total_byte_length=total_byte_length,
                    page_count=page_count,
                    work_counters=None,
                )
                repaired_fields += 1


def get_turn_content(
    db_path: Path,
    host_id: str,
    *,
    turn_id: str,
    content_revision: str,
    field: str,
    cursor: str | None = None,
    schema_version: int = 1,
    work_counters: TurnContentWorkCounters | None = None,
) -> dict[str, Any]:
    """Read one bounded page directly from the canonical SQLite value."""
    if schema_version != 1:
        return {
            "schema_version": int(schema_version),
            "ok": False,
            "status": "unsupported_content_schema_version",
            "required_content_schema_version": 1,
        }
    field_columns = {
        "user_text": (1, 3, 4, 5, "user_text"),
        "assistant_final_text": (2, 6, 7, 8, "assistant_final_text"),
    }
    if field not in field_columns:
        return {"schema_version": 1, "ok": False, "status": "invalid_content_field"}
    if not _sqlite_store_exists(db_path):
        return {
            "schema_version": 1,
            "ok": False,
            "status": "content_revision_not_found",
        }
    try:
        with _connect(db_path) as conn:
            _ensure_schema(conn)
            canonical_turn_id = _resolve_canonical_turn_id_conn(
                conn,
                str(host_id),
                turn_id,
            )
            if canonical_turn_id is None:
                return {
                    "schema_version": 1,
                    "ok": False,
                    "status": "content_revision_not_found",
                }
            turn_id = canonical_turn_id
            row = conn.execute(
                """
                SELECT
                    revisions.rowid,
                    revisions.user_state,
                    revisions.final_state,
                    revisions.user_char_length,
                    revisions.user_byte_length,
                    revisions.user_page_count,
                    revisions.final_char_length,
                    revisions.final_byte_length,
                    revisions.final_page_count,
                    revisions.is_current,
                    EXISTS (
                        SELECT 1
                        FROM turn_presentation_plans AS plans
                        WHERE plans.host_id = revisions.host_id
                          AND plans.turn_id = revisions.turn_id
                          AND plans.content_revision = revisions.content_revision
                    )
                FROM turn_content_revisions AS revisions
                WHERE host_id = ? AND turn_id = ? AND content_revision = ?
                """,
                (str(host_id), str(turn_id), str(content_revision)),
            ).fetchone()
            if work_counters is not None:
                work_counters.page_sql_queries += 1
            if row is None or (not bool(row[9]) and not bool(row[10])):
                return {
                    "schema_version": 1,
                    "ok": False,
                    "status": "content_revision_not_found",
                }
            state_index, char_index, byte_index, count_index, column = field_columns[field]
            availability = str(row[state_index])
            if availability == "known_incomplete":
                return {
                    "schema_version": 1,
                    "ok": False,
                    "status": "content_known_incomplete",
                }
            total_char_length = int(row[char_index])
            total_byte_length = int(row[byte_index])
            count = int(row[count_index])
            if (
                availability != "complete"
                or total_char_length < 1
                or total_byte_length < 1
                or count < 1
            ):
                return {
                    "schema_version": 1,
                    "ok": False,
                    "status": "content_not_available",
                }
            position = (
                ContentCursorPosition(
                    index=0,
                    segment_id=content_segment_id(content_revision, field, 0),
                    start_char=0,
                    start_byte=0,
                )
                if cursor is None
                else decode_content_cursor(
                    cursor,
                    revision=content_revision,
                    field=field,
                    count=count,
                )
            )
            _ensure_turn_content_page_boundaries_conn(
                conn,
                rowid=int(row[0]),
                host_id=str(host_id),
                turn_id=str(turn_id),
                content_revision_value=str(content_revision),
                field=str(field),
                column=column,
                total_char_length=total_char_length,
                total_byte_length=total_byte_length,
                page_count=count,
                work_counters=work_counters,
                allow_rebuild=False,
            )
            expected_boundary = conn.execute(
                """
                SELECT start_char, start_byte
                FROM turn_content_page_boundaries
                WHERE host_id = ?
                  AND turn_id = ?
                  AND content_revision = ?
                  AND field = ?
                  AND page_index = ?
                """,
                (
                    str(host_id),
                    str(turn_id),
                    str(content_revision),
                    str(field),
                    position.index,
                ),
            ).fetchone()
            if expected_boundary is None:
                raise ValueError("invalid_content_metadata")
            if (
                position.start_char != int(expected_boundary[0])
                or position.start_byte != int(expected_boundary[1])
            ):
                raise ValueError("invalid_cursor")
            blob = conn.blobopen(
                "turn_content_revisions",
                column,
                int(row[0]),
                readonly=True,
            )
            try:
                if len(blob) != total_byte_length:
                    raise ValueError("invalid_content_metadata")
                blob.seek(position.start_byte)
                raw = blob.read(
                    min(
                        TURN_CONTENT_PAGE_MAX_UTF8_BYTES,
                        total_byte_length - position.start_byte,
                    )
                )
            finally:
                blob.close()
            text = _bounded_utf8_blob_page(raw)
            segment_byte_length = len(text.encode("utf-8"))
            segment_char_length = len(text)
            if not segment_byte_length or segment_byte_length > TURN_CONTENT_PAGE_MAX_UTF8_BYTES:
                raise ValueError("invalid_content_metadata")
            end_byte = position.start_byte + segment_byte_length
            end_char = position.start_char + segment_char_length
            has_next = position.index + 1 < count
            if has_next:
                next_boundary = conn.execute(
                    """
                    SELECT start_char, start_byte
                    FROM turn_content_page_boundaries
                    WHERE host_id = ?
                      AND turn_id = ?
                      AND content_revision = ?
                      AND field = ?
                      AND page_index = ?
                    """,
                    (
                        str(host_id),
                        str(turn_id),
                        str(content_revision),
                        str(field),
                        position.index + 1,
                    ),
                ).fetchone()
                if (
                    next_boundary is None
                    or end_char != int(next_boundary[0])
                    or end_byte != int(next_boundary[1])
                ):
                    raise ValueError("invalid_content_metadata")
            elif (
                end_byte != total_byte_length
                or end_char != total_char_length
            ):
                raise ValueError("invalid_content_metadata")
            payload = {
                "schema_version": 1,
                "turn_id": str(turn_id),
                "content_revision": str(content_revision),
                "field": field,
                "availability": "complete",
                "segment_id": position.segment_id,
                "index": position.index,
                "count": count,
                "text": text,
                "segment_char_length": segment_char_length,
                "segment_byte_length": segment_byte_length,
                "total_char_length": total_char_length,
                "total_byte_length": total_byte_length,
                "next_cursor": (
                    content_cursor(
                        content_revision,
                        field,
                        position.index + 1,
                        start_char=end_char,
                        start_byte=end_byte,
                    )
                    if has_next
                    else None
                ),
            }
            if work_counters is not None:
                work_counters.page_blob_reads += 1
                work_counters.page_bytes_examined += len(raw)
                work_counters.page_chars_examined += segment_char_length
            _record_response_size(work_counters, payload)
            return payload
    except ValueError as exc:
        status = "invalid_cursor" if str(exc) == "invalid_cursor" else "content_not_available"
        return {"schema_version": 1, "ok": False, "status": status}
    except sqlite3.Error:
        return {
            "schema_version": 1,
            "ok": False,
            "status": "content_not_available",
        }


def _upsert_worker_bindings_conn(
    conn: sqlite3.Connection,
    bindings: Iterable[WorkerBinding],
) -> int:
    binding_list = list(bindings)
    if not binding_list:
        return 0
    conn.executemany(
        """
        INSERT INTO worker_bindings (
            host_id,
            worker_id,
            worker_fingerprint,
            backend,
            target_kind,
            target_value,
            turn_target_kind,
            turn_target_value,
            sendable,
            reason,
            observed_at,
            expires_at,
            private_fingerprint
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(host_id, backend, private_fingerprint) DO UPDATE SET
            worker_id = excluded.worker_id,
            worker_fingerprint = excluded.worker_fingerprint,
            target_kind = excluded.target_kind,
            target_value = excluded.target_value,
            turn_target_kind = excluded.turn_target_kind,
            turn_target_value = excluded.turn_target_value,
            sendable = excluded.sendable,
            reason = excluded.reason,
            observed_at = excluded.observed_at,
            expires_at = excluded.expires_at
        WHERE excluded.observed_at >= worker_bindings.observed_at
        """,
        [
            (
                binding.host_id,
                binding.worker_id,
                binding.worker_fingerprint,
                binding.backend,
                binding.target_kind,
                binding.target_value,
                binding.turn_target_kind,
                binding.turn_target_value,
                int(binding.sendable),
                binding.reason,
                binding.observed_at,
                binding.expires_at,
                binding.private_fingerprint,
            )
            for binding in binding_list
        ],
    )
    return len(binding_list)


def _expire_stale_worker_bindings_conn(
    conn: sqlite3.Connection,
    host_id: str,
    *,
    backend: str,
    current_private_fingerprints: Iterable[str],
    now: str,
    reason: str = "stale_observation",
) -> int:
    current = {str(value) for value in current_private_fingerprints}
    if current:
        placeholders = ",".join("?" for _ in current)
        cursor = conn.execute(
            f"""
            UPDATE worker_bindings
            SET sendable = 0,
                reason = ?,
                expires_at = ?
            WHERE host_id = ?
              AND backend = ?
              AND expires_at > ?
              AND observed_at <= ?
              AND private_fingerprint NOT IN ({placeholders})
            """,
            [
                str(reason),
                str(now),
                str(host_id),
                str(backend),
                str(now),
                str(now),
                *sorted(current),
            ],
        )
    else:
        cursor = conn.execute(
            """
            UPDATE worker_bindings
            SET sendable = 0,
                reason = ?,
                expires_at = ?
            WHERE host_id = ?
              AND backend = ?
              AND expires_at > ?
              AND observed_at <= ?
            """,
            (
                str(reason),
                str(now),
                str(host_id),
                str(backend),
                str(now),
                str(now),
            ),
        )
    return int(cursor.rowcount or 0)


def _snapshot_projection_freshness(
    payload_data: Mapping[str, Any],
) -> tuple[tuple[str, str, str], ...]:
    """Return timestamp fields that projections persist outside snapshots."""
    freshness: list[tuple[str, str, str]] = []
    for collection, timestamp_key in (
        ("spaces", "updated_at"),
        ("workers", "last_seen_at"),
        ("attention", "updated_at"),
    ):
        values = payload_data.get(collection, [])
        if not isinstance(values, list | tuple):
            continue
        for item in values:
            if not isinstance(item, Mapping):
                continue
            freshness.append(
                (
                    collection,
                    str(item.get("id") or ""),
                    str(item.get(timestamp_key) or ""),
                )
            )
    return tuple(sorted(freshness))


def _snapshot_projection_refresh_required(
    latest: sqlite3.Row | tuple[Any, ...] | None,
    content_fingerprint: str,
    payload_data: Mapping[str, Any],
) -> bool:
    if latest is None or str(latest[1]) != str(content_fingerprint):
        return True
    retained_payload = _json_object(latest[3])
    if _snapshot_projection_freshness(retained_payload) != (
        _snapshot_projection_freshness(payload_data)
    ):
        return True
    return _strict_utc_timestamp(latest[2]) is None


def save_snapshot(
    db_path: Path,
    snapshot: Snapshot,
    *,
    observation: SnapshotObservationContext | None = None,
    worker_bindings: Iterable[WorkerBinding] | None = None,
    binding_backend: str | None = None,
    binding_observation_authoritative: bool = False,
    binding_workers_present: bool = True,
) -> bool:
    """Persist a canonical snapshot; return whether it became the host projection."""
    context = observation or SnapshotObservationContext()
    binding_list = (
        None
        if worker_bindings is None
        else separate_duplicate_worker_bindings(
            binding
            if isinstance(binding, WorkerBinding)
            else WorkerBinding(**binding)
            for binding in worker_bindings
        )
    )
    if binding_list is not None:
        if not binding_backend:
            raise ValueError("binding_backend is required")
        if any(
            binding.host_id != snapshot.host_id
            or binding.backend != str(binding_backend)
            for binding in binding_list
        ):
            raise ValueError("snapshot binding scope mismatch")
    private_snapshot_data = _snapshot_dict(snapshot)
    created_at = _strict_utc_timestamp(private_snapshot_data.get("updated_at"))
    if created_at is None:
        raise ValueError("invalid snapshot updated_at")
    public_snapshot = Snapshot.from_dict(
        sanitize_public_mapping(private_snapshot_data)
    )
    data, fingerprint = _snapshot_payload(public_snapshot.to_dict())
    payload = _canonical_json(data)
    with _connect(db_path, prepare=True, isolation_level=None) as conn:
        _ensure_schema(conn)
        prepared_turn_items: tuple[dict[str, Any], ...] | None = None
        prepared_pending_items: tuple[dict[str, Any], ...] | None = None
        while True:
            preflight_latest = conn.execute(
                """
                SELECT id, content_fingerprint, created_at, payload
                FROM snapshots
                WHERE host_id = ?
                ORDER BY id DESC
                LIMIT 1
                """,
                (str(public_snapshot.host_id),),
            ).fetchone()
            preflight_projection_refresh_required = (
                _snapshot_projection_refresh_required(
                    preflight_latest,
                    fingerprint,
                    data,
                )
            )
            if (
                preflight_projection_refresh_required
                and prepared_turn_items is None
            ):
                # Turn model construction and pending serialization can recurse
                # through the public sanitizer. Keep that CPU work outside the
                # SQLite writer transaction.
                prepared_turn_items = ()
                prepared_pending_items = tuple(
                    pending.to_dict()
                    for pending in pending_from_snapshot(public_snapshot)
                )
            conn.execute("BEGIN IMMEDIATE")
            latest = conn.execute(
                """
                SELECT id, content_fingerprint, created_at, payload
                FROM snapshots
                WHERE host_id = ?
                ORDER BY id DESC
                LIMIT 1
                """,
                (str(public_snapshot.host_id),),
            ).fetchone()
            projection_refresh_required = _snapshot_projection_refresh_required(
                latest,
                fingerprint,
                data,
            )
            if projection_refresh_required and prepared_turn_items is None:
                # Another writer changed the retained snapshot between the
                # read-only preflight and BEGIN IMMEDIATE. Retry after preparing
                # the now-required projection inputs without holding the lock.
                conn.rollback()
                prepared_turn_items = ()
                prepared_pending_items = tuple(
                    pending.to_dict()
                    for pending in pending_from_snapshot(public_snapshot)
                )
                continue
            break
        try:
            retained_created_at = str(latest[2]) if latest is not None else ""
            retained_at = (
                _strict_utc_timestamp(retained_created_at)
                if latest is not None
                else None
            )
            retained_is_unknown = latest is None or retained_at is None
            refresh_current = retained_is_unknown
            exact_replay = False
            if retained_at is not None and not retained_is_unknown:
                incoming_order = _connector_datetime(created_at)
                retained_order = _connector_datetime(retained_at)
                refresh_current = incoming_order > retained_order or (
                    incoming_order == retained_order
                    and fingerprint > str(latest[1])
                )
                exact_replay = (
                    incoming_order == retained_order
                    and fingerprint == str(latest[1])
                )
            if latest is not None and str(latest[1]) == fingerprint:
                snapshot_id = int(latest[0])
                if refresh_current:
                    conn.execute(
                        """
                        UPDATE snapshots
                        SET created_at = ?, content_fingerprint = ?, payload = ?
                        WHERE id = ?
                        """,
                        (
                            created_at,
                            fingerprint,
                            payload,
                            snapshot_id,
                        ),
                    )
            elif refresh_current:
                cursor = conn.execute(
                    """
                    INSERT INTO snapshots (
                        host_id, created_at, content_fingerprint, payload
                    ) VALUES (?, ?, ?, ?)
                    """,
                    (
                        public_snapshot.host_id,
                        created_at,
                        fingerprint,
                        payload,
                    ),
                )
                snapshot_id = int(cursor.lastrowid)
                _append_snapshot_saved_event_conn(
                    conn,
                    public_snapshot,
                    snapshot_id=snapshot_id,
                    content_fingerprint=fingerprint,
                    private_snapshot_data=private_snapshot_data,
                )
            if refresh_current and projection_refresh_required:
                _refresh_snapshot_projections_conn(
                    conn,
                    public_snapshot,
                    data,
                    content_fingerprint=fingerprint,
                    turn_items=prepared_turn_items,
                    pending_items=prepared_pending_items,
                )
            elif refresh_current:
                _repair_missing_final_ready_anchors_conn(
                    conn,
                    host_id=str(public_snapshot.host_id),
                    now=created_at,
                )
            if refresh_current:
                if binding_list is not None:
                    _upsert_worker_bindings_conn(conn, binding_list)
                    if binding_observation_authoritative and (
                        binding_list or not binding_workers_present
                    ):
                        _expire_stale_worker_bindings_conn(
                            conn,
                            str(public_snapshot.host_id),
                            backend=str(binding_backend),
                            current_private_fingerprints=[
                                binding.private_fingerprint
                                for binding in binding_list
                            ],
                            now=created_at,
                        )
            _apply_attention_observation_conn(
                conn,
                snapshot=public_snapshot,
                payload_data=data,
                content_fingerprint=fingerprint,
                observation=context,
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    return bool(refresh_current or exact_replay)


def latest_snapshot(db_path: Path, host_id: str | None = None) -> Snapshot | None:
    """Return the latest snapshot globally, or scoped to host_id when provided."""
    if not _sqlite_store_exists(db_path):
        return None
    with _connect(db_path) as conn:
        _ensure_schema(conn)
        if host_id is None:
            row = conn.execute(
                "SELECT payload FROM snapshots ORDER BY id DESC LIMIT 1"
            ).fetchone()
        else:
            row = conn.execute(
                """
                SELECT payload
                FROM snapshots
                WHERE host_id = ?
                ORDER BY id DESC
                LIMIT 1
                """,
                (host_id,),
            ).fetchone()
    if row is None:
        return None
    return Snapshot.from_dict(sanitize_public_mapping(_json_object(row[0])))


def latest_healthy_backend_snapshot(
    db_path: Path,
    host_id: str,
    *,
    backend: str,
) -> Snapshot | None:
    """Return the newest snapshot reporting a healthy named backend."""
    if not _sqlite_store_exists(db_path):
        return None
    with _connect(db_path) as conn:
        _ensure_schema(conn)
        row = conn.execute(
            """
            SELECT snapshot.payload
            FROM snapshots AS snapshot
            WHERE snapshot.host_id = ?
              AND EXISTS (
                  SELECT 1
                  FROM json_each(snapshot.payload, '$.backend_health') AS health
                  WHERE json_extract(health.value, '$.name') = ?
                    AND json_extract(health.value, '$.status') = 'healthy'
              )
            ORDER BY snapshot.id DESC
            LIMIT 1
            """,
            (str(host_id), str(backend)),
        ).fetchone()
    if row is None:
        return None
    return Snapshot.from_dict(sanitize_public_mapping(_json_object(row[0])))


def _attention_rows_conn(
    conn: sqlite3.Connection,
    host_id: str,
    *,
    include_resolved: bool = False,
) -> list[Any]:
    columns = """
        i.attention_id,
        i.source,
        i.kind,
        i.severity,
        i.status,
        i.updated_at,
        i.fingerprint,
        i.snapshot_content_fingerprint,
        i.observed_at,
        i.payload_json,
        i.first_seen_at,
        i.last_seen_at,
        i.last_changed_at,
        i.resolved_at,
        i.lifecycle_status,
        i.resolved_reason,
        i.signal_count
    """
    if not include_resolved:
        return conn.execute(
            f"""
            SELECT {columns}
            FROM attention_lifecycles l
            JOIN attention_items i
              ON i.host_id = l.host_id
             AND i.attention_id = l.current_attention_id
            WHERE l.host_id = ? AND l.lifecycle_status = 'open'
            ORDER BY i.last_changed_at DESC, i.attention_id
            """,
            (str(host_id),),
        ).fetchall()
    return conn.execute(
        f"""
        SELECT {columns}, 0 AS sort_group
        FROM attention_lifecycles l
        JOIN attention_items i
          ON i.host_id = l.host_id
         AND i.attention_id = l.current_attention_id
        WHERE l.host_id = ? AND l.lifecycle_status = 'open'
        UNION ALL
        SELECT {columns}, 1 AS sort_group
        FROM attention_items i
        WHERE i.host_id = ?
          AND i.lifecycle_status != 'open'
          AND NOT EXISTS (
              SELECT 1 FROM attention_lifecycles l
              WHERE l.host_id = i.host_id
                AND l.current_attention_id = i.attention_id
          )
        ORDER BY sort_group, last_changed_at DESC, attention_id
        """,
        (str(host_id), str(host_id)),
    ).fetchall()


def _attention_item_from_row(row: Any) -> dict[str, Any]:
    try:
        parsed = json.loads(row[9] or "{}")
    except (TypeError, json.JSONDecodeError):
        parsed = {}
    payload = sanitize_public_mapping(parsed)
    payload.update(
        {
            "id": str(row[0] or ""),
            "source": _store_public_text(row[1], default="unknown"),
            "kind": _store_public_label(row[2]),
            "severity": str(row[3] or "info"),
            "status": str(row[4] or "unknown"),
            "updated_at": row[5],
            "fingerprint": str(row[6] or ""),
        }
    )
    payload["reason"] = _store_public_text(
        payload.get("reason"),
        default="",
        free_text=True,
    )
    return _attention_lifecycle_payload(
        payload,
        attention_id=str(row[0] or ""),
        observed_at=str(row[8] or row[11] or ""),
        first_seen_at=str(row[10] or row[8] or ""),
        last_seen_at=str(row[11] or row[8] or ""),
        last_changed_at=str(row[12] or row[8] or ""),
        resolved_at=row[13],
        lifecycle_status=str(row[14] or ATTENTION_LIFECYCLE_OPEN),
        resolved_reason=_store_public_text(
            row[15],
            default="",
            free_text=True,
        ) or None,
        signal_count=int(row[16] or 1),
    )




def attention_payload_from_store(
    db_path: Path,
    host_id: str,
    *,
    include_resolved: bool = False,
) -> dict[str, Any] | None:
    """Return a public attention.list payload from lifecycle rows or snapshot fallback."""
    if not _sqlite_store_exists(db_path):
        return None
    with _connect(db_path) as conn:
        _ensure_schema(conn)
        rows = _attention_rows_conn(
            conn,
            host_id,
            include_resolved=include_resolved,
        )
        snapshot_row = conn.execute(
            """
            SELECT payload
            FROM snapshots
            WHERE host_id = ?
            ORDER BY id DESC
            LIMIT 1
            """,
            (str(host_id),),
        ).fetchone()
        attention_row_count = int(
            conn.execute(
                "SELECT COUNT(*) FROM attention_items WHERE host_id = ?",
                (str(host_id),),
            ).fetchone()[0]
        )
        lifecycle_row_count = int(
            conn.execute(
                "SELECT COUNT(*) FROM attention_lifecycles WHERE host_id = ?",
                (str(host_id),),
            ).fetchone()[0]
        )

    if snapshot_row is None and not rows:
        return None

    snapshot: Snapshot | None = None
    if snapshot_row is not None:
        try:
            snapshot = Snapshot.from_dict(
                sanitize_public_mapping(_json_object(snapshot_row[0]))
            )
        except (TypeError, ValueError, json.JSONDecodeError):
            snapshot = None

    attention = [_attention_item_from_row(row) for row in rows]
    backend_health = [health.to_dict() for health in snapshot.backend_health] if snapshot is not None else []
    updated_at = snapshot.updated_at if snapshot is not None else utc_timestamp()
    if (
        not attention
        and attention_row_count == 0
        and lifecycle_row_count == 0
        and snapshot is not None
        and snapshot.attention
    ):
        attention = []
        for signal in snapshot.attention:
            item = signal.to_dict()
            attention.append(
                _attention_lifecycle_payload(
                    item,
                    attention_id=_attention_id_from_item(item),
                    observed_at=updated_at,
                    first_seen_at=updated_at,
                    last_seen_at=updated_at,
                    last_changed_at=updated_at,
                    lifecycle_status=ATTENTION_LIFECYCLE_OPEN,
                    signal_count=1,
                )
            )
    if snapshot is None and attention:
        updated_at = str(attention[0].get("last_seen_at") or attention[0].get("observed_at") or updated_at)
    payload = {
        "schema_version": SCHEMA_VERSION,
        "host_id": str(host_id),
        "updated_at": updated_at,
        "attention": attention,
        "backend_health": backend_health,
    }
    payload["content_fingerprint"] = stable_fingerprint(
        {
            "schema_version": payload["schema_version"],
            "host_id": payload["host_id"],
            "attention": attention,
            "backend_health": backend_health,
        }
    )
    return sanitize_public_value(payload)




def upsert_worker_bindings(db_path: Path, bindings: Iterable[WorkerBinding]) -> int:
    """Persist observed private worker bindings by private identity.

    The upsert key is host/backend/private_fingerprint so a moved pane or
    changed backend target updates the private routing record while preserving
    the public worker identity associated with that private Herdr identity.
    """
    binding_list = separate_duplicate_worker_bindings(
        binding if isinstance(binding, WorkerBinding) else WorkerBinding(**binding)
        for binding in bindings
    )
    if not binding_list:
        return 0
    with _connect(db_path, prepare=True) as conn:
        _ensure_schema(conn)
        return _upsert_worker_bindings_conn(conn, binding_list)


def list_worker_bindings(
    db_path: Path,
    host_id: str,
    *,
    backend: str | None = None,
    include_expired: bool = False,
    now: str | None = None,
) -> list[WorkerBinding]:
    """Return private worker bindings for a host, current/unexpired by default."""
    if not _sqlite_store_exists(db_path):
        return []
    current_time = now or utc_timestamp()
    clauses = ["host_id = ?"]
    params: list[Any] = [str(host_id)]
    if backend is not None:
        clauses.append("backend = ?")
        params.append(str(backend))
    if not include_expired:
        clauses.append("expires_at > ?")
        params.append(current_time)
    where = " AND ".join(clauses)
    with _connect(db_path) as conn:
        _ensure_schema(conn)
        rows = conn.execute(
            f"""
            SELECT
                host_id,
                worker_id,
                worker_fingerprint,
                backend,
                target_kind,
                target_value,
                turn_target_kind,
                turn_target_value,
                sendable,
                reason,
                observed_at,
                expires_at,
                private_fingerprint
            FROM worker_bindings
            WHERE {where}
            ORDER BY observed_at DESC, id DESC
            """,
            params,
        ).fetchall()
    return [_worker_binding_from_row(row) for row in rows]




def expire_worker_bindings(
    db_path: Path,
    host_id: str,
    *,
    backend: str | None = None,
    worker_id: str | None = None,
    private_fingerprints: Iterable[str] | None = None,
    now: str | None = None,
    reason: str = "expired",
) -> int:
    """Mark matching private bindings expired and unsendable without deleting rows."""
    current_time = now or utc_timestamp()
    fingerprints = [str(value) for value in (private_fingerprints or [])]
    clauses = ["host_id = ?", "expires_at > ?", "observed_at <= ?"]
    params: list[Any] = [str(host_id), current_time, current_time]
    if backend is not None:
        clauses.append("backend = ?")
        params.append(str(backend))
    if worker_id is not None:
        clauses.append("worker_id = ?")
        params.append(str(worker_id))
    if fingerprints:
        placeholders = ",".join("?" for _ in fingerprints)
        clauses.append(f"private_fingerprint IN ({placeholders})")
        params.extend(fingerprints)
    where = " AND ".join(clauses)
    with _connect(db_path, prepare=True) as conn:
        _ensure_schema(conn)
        cursor = conn.execute(
            f"""
            UPDATE worker_bindings
            SET sendable = 0,
                reason = ?,
                expires_at = ?
            WHERE {where}
            """,
            [str(reason), current_time, *params],
        )
        return int(cursor.rowcount or 0)


def expire_stale_worker_bindings(
    db_path: Path,
    host_id: str,
    *,
    backend: str,
    current_private_fingerprints: Iterable[str],
    now: str | None = None,
    reason: str = "stale_observation",
) -> int:
    """Expire host/backend bindings absent from a fresh successful observation."""
    current_time = now or utc_timestamp()
    with _connect(db_path, prepare=True) as conn:
        _ensure_schema(conn)
        return _expire_stale_worker_bindings_conn(
            conn,
            str(host_id),
            backend=str(backend),
            current_private_fingerprints=current_private_fingerprints,
            now=current_time,
            reason=str(reason),
        )


def _command_request_now(value: str | None = None) -> str:
    raw = str(value or utc_timestamp()).strip()
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError as exc:
        raise ValueError("now must be an ISO-8601 timestamp") from exc
    if parsed.tzinfo is None:
        raise ValueError("now must include a timezone")
    return parsed.astimezone(timezone.utc).isoformat(timespec="seconds")


def _command_request_add_seconds(now: str, seconds: float) -> str:
    return (
        datetime.fromisoformat(now) + timedelta(seconds=float(seconds))
    ).isoformat(timespec="seconds")


def _command_request_response(
    status: str,
    row: Any,
    *,
    owner_token: str | None = None,
) -> dict[str, Any]:
    return {
        "status": str(status),
        "owner_token": owner_token,
        "receipt": None if row is None else _command_receipt_from_row(row),
    }


def _command_transition_event_conn(
    conn: sqlite3.Connection,
    row: Any,
    *,
    observed_at: str,
    event_payload: Mapping[str, Any] | None = None,
) -> None:
    payload: dict[str, Any] = {
        "schema_version": 1,
        "request_id": str(row[2]),
        "action": str(row[3]),
        "state": str(row[8]),
        "status": str(row[9]),
        "canonical_fingerprint": str(row[5]),
        "public_worker_id": str(row[7]),
    }
    if event_payload is not None:
        payload["detail"] = sanitize_public_mapping(event_payload)
    _append_event_conn(
        conn,
        host_id=str(row[1]),
        event_type=f"command.request.{row[8]}",
        aggregate_type="command_request",
        aggregate_id=str(row[2]),
        observed_at=observed_at,
        content_fingerprint=str(row[5]),
        payload=payload,
    )


def _canonical_request_matches(
    row: Any,
    *,
    action: str,
    canonical_version: int,
    canonical_fingerprint: str,
    canonical_request_json: str,
    public_worker_id: str,
) -> bool:
    if str(row[3]) != str(action):
        return False
    return (
        int(row[4]) == int(canonical_version)
        and str(row[5]) == str(canonical_fingerprint)
        and str(row[6]) == str(canonical_request_json)
        and str(row[7]) == str(public_worker_id)
    )


def get_command_request(
    db_path: Path,
    host_id: str,
    request_id: str,
) -> dict[str, Any] | None:
    """Return the authoritative public receipt for a host-wide request id."""
    if not _sqlite_store_exists(db_path):
        return None
    with _connect(db_path) as conn:
        _ensure_schema(conn)
        row = _command_request_row(conn, host_id, request_id)
    return None if row is None else _command_receipt_from_row(row)


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
    owner_lease_seconds: float = COMMAND_RECEIPT_OWNER_LEASE_SECONDS,
    now: str | None = None,
) -> dict[str, Any]:
    """Reserve or replay exactly one authoritative host/request mutation."""
    values = {
        "host_id": str(host_id).strip(),
        "request_id": str(request_id).strip(),
        "action": str(action).strip(),
        "canonical_fingerprint": str(canonical_fingerprint).strip(),
    }
    if any(not value for value in values.values()):
        raise ValueError("command request identity fields must be non-empty")
    if isinstance(canonical_version, bool) or int(canonical_version) < 1:
        raise ValueError("canonical_version must be an integer >= 1")
    proof = str(selector_proof or "")
    if proof and not is_selector_proof(proof):
        raise ValueError("selector_proof must be a supported selector proof")
    try:
        canonical_payload = json.loads(str(canonical_request_json))
    except (TypeError, json.JSONDecodeError) as exc:
        raise ValueError("canonical_request_json must be a JSON object") from exc
    if not isinstance(canonical_payload, Mapping):
        raise ValueError("canonical_request_json must be a JSON object")
    try:
        lease_seconds = float(owner_lease_seconds)
    except (TypeError, ValueError) as exc:
        raise ValueError("owner_lease_seconds must be positive and finite") from exc
    if (
        isinstance(owner_lease_seconds, bool)
        or not math.isfinite(lease_seconds)
        or lease_seconds <= 0
        or lease_seconds > COMMAND_RETRY_HORIZON_SECONDS
    ):
        raise ValueError("owner_lease_seconds must be positive and finite")
    current = _command_request_now(now)
    owner_token = secrets.token_urlsafe(32)
    owner_hash = _owner_token_hash(owner_token)
    owner_expires_at = _command_request_add_seconds(current, lease_seconds)
    conn = _connect(db_path, isolation_level=None, prepare=True)
    try:
        _ensure_schema(conn)
        conn.execute("BEGIN IMMEDIATE")
        row = _command_request_row(conn, values["host_id"], values["request_id"])
        if row is not None:
            if not _canonical_request_matches(
                row,
                action=values["action"],
                canonical_version=int(canonical_version),
                canonical_fingerprint=values["canonical_fingerprint"],
                canonical_request_json=str(canonical_request_json),
                public_worker_id=str(public_worker_id),
            ):
                conn.commit()
                return _command_request_response("request_id_conflict", row)
            if str(row[8]) in _COMMAND_REQUEST_TERMINAL_STATES:
                conn.commit()
                return _command_request_response("terminal", row)
            if str(row[8]) == "send_started":
                conn.commit()
                return _command_request_response("in_progress", row)
            expires_at = str(row[12] or "")
            if expires_at and datetime.fromisoformat(expires_at) > datetime.fromisoformat(current):
                conn.commit()
                return _command_request_response("in_progress", row)
            # Re-drive an abandoned reservation without rewriting selector_proof:
            # the original spelling stays the evidence, even when this caller
            # proved equivalence with a different one.
            updated = conn.execute(
                """
                UPDATE command_receipts
                SET owner_token_hash = ?,
                    owner_expires_at = ?,
                    binding_fingerprint = NULL,
                    status = 'pending',
                    result_json = ?,
                    reserved_at = ?,
                    send_started_at = NULL,
                    updated_at = ?
                WHERE id = ? AND state = 'reserved'
                """,
                (
                    owner_hash,
                    owner_expires_at,
                    str(pending_result_json),
                    current,
                    current,
                    int(row[0]),
                ),
            )
            if int(updated.rowcount or 0) != 1:
                row = _command_request_row(
                    conn, values["host_id"], values["request_id"]
                )
                conn.commit()
                return _command_request_response("in_progress", row)
        else:
            conn.execute(
                """
                INSERT INTO command_receipts (
                    host_id, request_id, action, canonical_version,
                    canonical_fingerprint, canonical_request_json,
                    public_worker_id, state, status, result_json,
                    owner_token_hash, owner_expires_at, binding_fingerprint,
                    created_at, reserved_at, send_started_at, terminal_at,
                    updated_at, selector_proof
                ) VALUES (
                    ?, ?, ?, ?, ?, ?, ?, 'reserved', 'pending', ?, ?, ?, NULL,
                    ?, ?, NULL, NULL, ?, ?
                )
                """,
                (
                    values["host_id"],
                    values["request_id"],
                    values["action"],
                    int(canonical_version),
                    values["canonical_fingerprint"],
                    str(canonical_request_json),
                    str(public_worker_id),
                    str(pending_result_json),
                    owner_hash,
                    owner_expires_at,
                    current,
                    current,
                    current,
                    proof,
                ),
            )
        row = _command_request_row(conn, values["host_id"], values["request_id"])
        if row is None:
            raise RuntimeError("command request reservation disappeared")
        _project_command_request_conn(conn, row)
        _command_transition_event_conn(conn, row, observed_at=current)
        conn.commit()
        return _command_request_response("reserved", row, owner_token=owner_token)
    except Exception:
        if conn.in_transaction:
            conn.rollback()
        raise
    finally:
        conn.close()


def abandon_command_request_reservation(
    db_path: Path,
    *,
    host_id: str,
    request_id: str,
    canonical_fingerprint: str,
    owner_token: str,
    now: str | None = None,
) -> bool:
    """Release only the caller's unsent reservation for immediate takeover."""
    if not _sqlite_store_exists(db_path):
        return False
    current = _command_request_now(now)
    owner_hash = _owner_token_hash(owner_token)
    if not owner_hash:
        return False
    with _connect(db_path, isolation_level=None) as conn:
        _ensure_schema(conn)
        conn.execute("BEGIN IMMEDIATE")
        try:
            updated = conn.execute(
                """
                UPDATE command_receipts
                SET owner_expires_at = ?, updated_at = ?
                WHERE host_id = ? AND request_id = ?
                  AND canonical_fingerprint = ?
                  AND state = 'reserved' AND owner_token_hash = ?
                """,
                (
                    current,
                    current,
                    str(host_id),
                    str(request_id),
                    str(canonical_fingerprint),
                    owner_hash,
                ),
            )
            conn.commit()
            return int(updated.rowcount or 0) == 1
        except Exception:
            conn.rollback()
            raise


def reserve_terminal_command_replay(
    db_path: Path,
    *,
    host_id: str,
    request_id: str,
    action: str,
    canonical_version: int,
    canonical_fingerprint: str,
    canonical_request_json: str,
    public_worker_id: str,
    terminal_state: str,
    status: str,
    result_json: str,
    selector_proof: str = "",
    event_payload: Mapping[str, Any] | None = None,
    now: str | None = None,
) -> dict[str, Any]:
    """Atomically preserve known terminal evidence when retention removed its row."""
    values = {
        "host_id": str(host_id).strip(),
        "request_id": str(request_id).strip(),
        "action": str(action).strip(),
        "canonical_fingerprint": str(canonical_fingerprint).strip(),
    }
    if any(not value for value in values.values()):
        raise ValueError("command request identity fields must be non-empty")
    if isinstance(canonical_version, bool) or int(canonical_version) < 1:
        raise ValueError("canonical_version must be an integer >= 1")
    proof = str(selector_proof or "")
    if proof and not is_selector_proof(proof):
        raise ValueError("selector_proof must be a supported selector proof")
    try:
        canonical_payload = json.loads(str(canonical_request_json))
    except (TypeError, json.JSONDecodeError) as exc:
        raise ValueError("canonical_request_json must be a JSON object") from exc
    if not isinstance(canonical_payload, Mapping):
        raise ValueError("canonical_request_json must be a JSON object")

    terminal = str(terminal_state)
    terminal_status = str(status)
    if terminal not in {"rejected", "uncertain"}:
        raise ValueError("terminal replay state must be rejected or uncertain")
    if terminal == "uncertain" and terminal_status != "request_state_uncertain":
        raise ValueError(
            "uncertain terminal state requires request_state_uncertain status"
        )
    if terminal == "rejected" and terminal_status in {
        "pending",
        "accepted",
        "request_state_uncertain",
    }:
        raise ValueError("rejected terminal state requires a rejection status")

    current = _command_request_now(now)
    conn = _connect(db_path, isolation_level=None, prepare=True)
    try:
        _ensure_schema(conn)
        conn.execute("BEGIN IMMEDIATE")
        row = _command_request_row(conn, values["host_id"], values["request_id"])
        if row is not None:
            if not _canonical_request_matches(
                row,
                action=values["action"],
                canonical_version=int(canonical_version),
                canonical_fingerprint=values["canonical_fingerprint"],
                canonical_request_json=str(canonical_request_json),
                public_worker_id=str(public_worker_id),
            ):
                conn.commit()
                return _command_request_response("request_id_conflict", row)
            if str(row[8]) in _COMMAND_REQUEST_TERMINAL_STATES:
                conn.commit()
                return _command_request_response("terminal", row)
            conn.commit()
            return _command_request_response("in_progress", row)

        conn.execute(
            """
            INSERT INTO command_receipts (
                host_id, request_id, action, canonical_version,
                canonical_fingerprint, canonical_request_json,
                public_worker_id, state, status, result_json,
                owner_token_hash, owner_expires_at, binding_fingerprint,
                created_at, reserved_at, send_started_at, terminal_at,
                updated_at, selector_proof
            ) VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '', NULL, NULL,
                ?, ?, NULL, ?, ?, ?
            )
            """,
            (
                values["host_id"],
                values["request_id"],
                values["action"],
                int(canonical_version),
                values["canonical_fingerprint"],
                str(canonical_request_json),
                str(public_worker_id),
                terminal,
                terminal_status,
                str(result_json),
                current,
                current,
                current,
                current,
                proof,
            ),
        )
        row = _command_request_row(conn, values["host_id"], values["request_id"])
        if row is None:
            raise RuntimeError("terminal command replay disappeared")
        _project_command_request_conn(conn, row)
        _command_transition_event_conn(
            conn,
            row,
            observed_at=current,
            event_payload=event_payload,
        )
        conn.commit()
        return _command_request_response(terminal, row)
    except Exception:
        if conn.in_transaction:
            conn.rollback()
        raise
    finally:
        conn.close()




def _sweep_submission_links(
    db_path: Path,
    *,
    host_id: str | None = None,
    now: str | None = None,
) -> int:
    """Settle due submission components and expire their hard-TTL stragglers."""
    if not _sqlite_store_exists(db_path):
        return 0
    current = _command_request_now(now)
    with _connect(db_path, isolation_level=None) as conn:
        _ensure_schema(conn)
        conn.execute("BEGIN IMMEDIATE")
        try:
            changed = _settle_due_submission_links_conn(
                conn,
                db_path=db_path,
                host_id=None if host_id is None else str(host_id),
                now=current,
            )
            _expire_turn_submissions_conn(
                conn,
                current=current,
                host_id=None if host_id is None else str(host_id),
            )
            conn.commit()
            return changed
        except Exception:
            conn.rollback()
            raise


def settle_submission_link_for_request(
    db_path: Path,
    *,
    host_id: str,
    request_id: str,
    now: str | None = None,
) -> dict[str, Any] | None:
    """Settle only the owner/fingerprint component containing one request."""
    if not _sqlite_store_exists(db_path):
        return None
    current = _command_request_now(now)
    with _connect(db_path, isolation_level=None) as conn:
        _ensure_schema(conn)
        conn.execute("BEGIN IMMEDIATE")
        try:
            component = conn.execute(
                """
                SELECT owner_key, instruction_fingerprint
                FROM turn_submissions
                WHERE host_id = ? AND request_id = ?
                """,
                (str(host_id), str(request_id)),
            ).fetchone()
            if component is None:
                conn.commit()
                return None
            owner_key, fingerprint = map(str, component)
            changed = _settle_submission_links_conn(
                conn,
                str(host_id),
                owner_key,
                fingerprint,
                now=current,
            )
            row = conn.execute(
                """
                SELECT state, linked_turn_id
                FROM turn_submissions
                WHERE host_id = ? AND request_id = ?
                """,
                (str(host_id), str(request_id)),
            ).fetchone()
            conn.commit()
            if row is None:
                return None
            return {
                "state": str(row[0]),
                "linked_turn_id": (
                    None if row[1] is None else str(row[1])
                ),
                "changed": changed,
            }
        except Exception:
            conn.rollback()
            raise


def linked_turn_for_submission(
    db_path: Path,
    *,
    host_id: str,
    request_id: str,
) -> dict[str, Any] | None:
    """Return only the canonical observed turn proven by the durable link."""
    if not _sqlite_store_exists(db_path):
        return None
    with _connect(db_path) as conn:
        _ensure_schema(conn)
        row = conn.execute(
            """
            SELECT linked_turn_id
            FROM turn_submissions
            WHERE host_id = ? AND request_id = ?
              AND state = 'linked' AND linked_turn_id IS NOT NULL
            """,
            (str(host_id), str(request_id)),
        ).fetchone()
        if row is None:
            return None
        canonical_turn_id = _resolve_canonical_turn_id_conn(
            conn,
            str(host_id),
            row[0],
        )
        if canonical_turn_id is None:
            return None
        turn_row = conn.execute(
            """
            SELECT payload_json
            FROM turns
            WHERE host_id = ? AND turn_id = ?
            """,
            (str(host_id), canonical_turn_id),
        ).fetchone()
        if turn_row is None:
            return None
        payload = sanitize_public_mapping(_json_object(turn_row[0]))
        if (
            not payload
            or _turn_is_tombstoned(payload)
            or not str(payload.get("source_turn_id") or "").strip()
        ):
            return None
        payload["id"] = canonical_turn_id
        return payload




def mark_command_send_started(
    db_path: Path,
    *,
    host_id: str,
    request_id: str,
    canonical_fingerprint: str,
    owner_token: str,
    binding_fingerprint: str,
    event_payload: Mapping[str, Any] | None = None,
    send_started_effect: Callable[[sqlite3.Connection], Any] | None = None,
    submission_worker: Any | None = None,
    instruction_text: str | None = None,
    submission_link_window_seconds: int = SUBMISSION_LINK_WINDOW_SECONDS,
    submission_hard_ttl_seconds: int = SUBMISSION_HARD_TTL_SECONDS,
    now: str | None = None,
) -> dict[str, Any]:
    """CAS the exact reserved owner to send_started before external mutation."""
    if not str(binding_fingerprint).strip():
        raise ValueError("binding_fingerprint must be non-empty")
    if (submission_worker is None) is not (instruction_text is None):
        raise ValueError(
            "submission_worker and instruction_text must be provided together"
        )
    current = _command_request_now(now)
    owner_hash = _owner_token_hash(owner_token)
    conn = _connect(db_path, isolation_level=None, prepare=True)
    try:
        _ensure_schema(conn)
        conn.execute("BEGIN IMMEDIATE")
        row = _command_request_row(conn, host_id, request_id)
        if row is None:
            conn.commit()
            return _command_request_response("not_found", None)
        if str(row[5]) != str(canonical_fingerprint):
            conn.commit()
            return _command_request_response("request_id_conflict", row)
        if str(row[8]) in _COMMAND_REQUEST_TERMINAL_STATES:
            conn.commit()
            return _command_request_response("terminal", row)
        if str(row[8]) != "reserved":
            conn.commit()
            return _command_request_response("invalid_state", row)
        if not owner_hash or not secrets.compare_digest(str(row[11]), owner_hash):
            conn.commit()
            return _command_request_response("not_owner", row)
        updated = conn.execute(
            """
            UPDATE command_receipts
            SET state = 'send_started',
                binding_fingerprint = ?,
                send_started_at = ?,
                updated_at = ?
            WHERE id = ?
              AND state = 'reserved'
              AND canonical_fingerprint = ?
              AND owner_token_hash = ?
            """,
            (
                str(binding_fingerprint),
                current,
                current,
                int(row[0]),
                str(canonical_fingerprint),
                owner_hash,
            ),
        )
        if int(updated.rowcount or 0) != 1:
            row = _command_request_row(conn, host_id, request_id)
            conn.commit()
            return _command_request_response("not_owner", row)
        submission_id = (
            _insert_turn_submission_conn(
                conn,
                host_id=str(host_id),
                request_id=str(request_id),
                worker=submission_worker,
                instruction_text=str(instruction_text),
                current=current,
                link_window_seconds=submission_link_window_seconds,
                hard_ttl_seconds=submission_hard_ttl_seconds,
            )
            if submission_worker is not None and instruction_text is not None
            else None
        )
        effect_result = (
            send_started_effect(conn)
            if send_started_effect is not None
            else None
        )
        row = _command_request_row(conn, host_id, request_id)
        if row is None:
            raise RuntimeError("command request send-start disappeared")
        _project_command_request_conn(conn, row)
        _command_transition_event_conn(
            conn,
            row,
            observed_at=current,
            event_payload=event_payload,
        )
        conn.commit()
        response = _command_request_response(
            "send_started", row, owner_token=str(owner_token)
        )
        if send_started_effect is not None:
            response["effect_result"] = effect_result
        if submission_id is not None:
            response["submission_id"] = submission_id
        return response
    except Exception:
        if conn.in_transaction:
            conn.rollback()
        raise
    finally:
        conn.close()


def record_command_send_queued(
    db_path: Path,
    *,
    host_id: str,
    request_id: str,
    canonical_fingerprint: str,
    owner_token: str,
    result_json: str,
    event_payload: Mapping[str, Any] | None = None,
    now: str | None = None,
) -> dict[str, Any]:
    """Persist a verified queued send without permitting another mutation."""
    current = _command_request_now(now)
    owner_hash = _owner_token_hash(owner_token)
    conn = _connect(db_path, isolation_level=None, prepare=True)
    try:
        _ensure_schema(conn)
        conn.execute("BEGIN IMMEDIATE")
        row = _command_request_row(conn, host_id, request_id)
        if row is None:
            conn.commit()
            return _command_request_response("not_found", None)
        if str(row[5]) != str(canonical_fingerprint):
            conn.commit()
            return _command_request_response("request_id_conflict", row)
        if str(row[8]) in _COMMAND_REQUEST_TERMINAL_STATES:
            conn.commit()
            return _command_request_response("terminal", row)
        if str(row[8]) != "send_started":
            conn.commit()
            return _command_request_response("invalid_state", row)
        if not owner_hash or not secrets.compare_digest(str(row[11]), owner_hash):
            conn.commit()
            return _command_request_response("not_owner", row)
        updated = conn.execute(
            """
            UPDATE command_receipts
            SET status = 'pending',
                result_json = ?,
                updated_at = ?
            WHERE id = ?
              AND state = 'send_started'
              AND canonical_fingerprint = ?
              AND owner_token_hash = ?
            """,
            (
                str(result_json),
                current,
                int(row[0]),
                str(canonical_fingerprint),
                owner_hash,
            ),
        )
        if int(updated.rowcount or 0) != 1:
            row = _command_request_row(conn, host_id, request_id)
            conn.commit()
            return _command_request_response("not_owner", row)
        submission_updated = conn.execute(
            """
            UPDATE turn_submissions
            SET state = 'send_started',
                linked_turn_id = NULL,
                linked_at = NULL,
                submitted_at = ?,
                link_expires_at = hard_expires_at,
                updated_at = ?
            WHERE host_id = ? AND request_id = ?
              AND state IN ('send_started', 'linked')
            """,
            (current, current, str(host_id), str(request_id)),
        )
        if int(submission_updated.rowcount or 0) != 1:
            raise StoreSchemaError("queued turn submission transition failed")
        row = _command_request_row(conn, host_id, request_id)
        if row is None:
            raise RuntimeError("queued command request disappeared")
        _project_command_request_conn(conn, row)
        _command_transition_event_conn(
            conn,
            row,
            observed_at=current,
            event_payload=event_payload,
        )
        conn.commit()
        return _command_request_response("queued", row)
    except Exception:
        if conn.in_transaction:
            conn.rollback()
        raise
    finally:
        conn.close()


def finish_queued_command_request(
    db_path: Path,
    *,
    host_id: str,
    request_id: str,
    canonical_fingerprint: str,
    queued_result_json: str,
    accepted_result_json: str,
    event_payload: Mapping[str, Any] | None = None,
    now: str | None = None,
) -> dict[str, Any]:
    """Accept a queued request only after its exact observed turn is linked."""
    current = _command_request_now(now)
    conn = _connect(db_path, isolation_level=None, prepare=True)
    try:
        _ensure_schema(conn)
        conn.execute("BEGIN IMMEDIATE")
        row = _command_request_row(conn, host_id, request_id)
        if row is None:
            conn.commit()
            return _command_request_response("not_found", None)
        if str(row[5]) != str(canonical_fingerprint):
            conn.commit()
            return _command_request_response("request_id_conflict", row)
        if str(row[8]) in _COMMAND_REQUEST_TERMINAL_STATES:
            conn.commit()
            return _command_request_response("terminal", row)
        if (
            str(row[8]) != "send_started"
            or str(row[9]) != "pending"
            or str(row[10]) != str(queued_result_json)
        ):
            conn.commit()
            return _command_request_response("invalid_state", row)
        linked = conn.execute(
            """
            SELECT 1
            FROM turn_submissions
            WHERE host_id = ? AND request_id = ?
              AND state = 'linked' AND linked_turn_id IS NOT NULL
            LIMIT 1
            """,
            (str(host_id), str(request_id)),
        ).fetchone()
        if linked is None:
            conn.commit()
            return _command_request_response("in_progress", row)
        updated = conn.execute(
            """
            UPDATE command_receipts
            SET state = 'accepted',
                status = 'accepted',
                result_json = ?,
                owner_token_hash = '',
                owner_expires_at = NULL,
                terminal_at = ?,
                updated_at = ?
            WHERE id = ?
              AND state = 'send_started'
              AND canonical_fingerprint = ?
              AND status = 'pending'
              AND result_json = ?
            """,
            (
                str(accepted_result_json),
                current,
                current,
                int(row[0]),
                str(canonical_fingerprint),
                str(queued_result_json),
            ),
        )
        if int(updated.rowcount or 0) != 1:
            row = _command_request_row(conn, host_id, request_id)
            conn.commit()
            return _command_request_response("in_progress", row)
        row = _command_request_row(conn, host_id, request_id)
        if row is None:
            raise RuntimeError("accepted queued command request disappeared")
        _project_command_request_conn(conn, row)
        _command_transition_event_conn(
            conn,
            row,
            observed_at=current,
            event_payload=event_payload,
        )
        conn.commit()
        return _command_request_response("accepted", row)
    except Exception:
        if conn.in_transaction:
            conn.rollback()
        raise
    finally:
        conn.close()


def finish_unverified_queued_command_request(
    db_path: Path,
    *,
    host_id: str,
    request_id: str,
    canonical_fingerprint: str,
    queued_result_json: str,
    uncertain_result_json: str,
    event_payload: Mapping[str, Any] | None = None,
    now: str | None = None,
) -> dict[str, Any]:
    """Terminalize a queued receipt after its ledger can no longer verify it."""
    current = _command_request_now(now)
    conn = _connect(db_path, isolation_level=None, prepare=True)
    try:
        _ensure_schema(conn)
        conn.execute("BEGIN IMMEDIATE")
        row = _command_request_row(conn, host_id, request_id)
        if row is None:
            conn.commit()
            return _command_request_response("not_found", None)
        if str(row[5]) != str(canonical_fingerprint):
            conn.commit()
            return _command_request_response("request_id_conflict", row)
        if str(row[8]) in _COMMAND_REQUEST_TERMINAL_STATES:
            conn.commit()
            return _command_request_response("terminal", row)
        if (
            str(row[8]) != "send_started"
            or str(row[9]) != "pending"
            or str(row[10]) != str(queued_result_json)
        ):
            conn.commit()
            return _command_request_response("in_progress", row)
        ledger = conn.execute(
            """
            SELECT state, linked_turn_id
            FROM turn_submissions
            WHERE host_id = ? AND request_id = ?
            """,
            (str(host_id), str(request_id)),
        ).fetchone()
        if (
            ledger is None
            or str(ledger[0]) not in {"ambiguous", "expired"}
            or ledger[1] is not None
        ):
            conn.commit()
            return _command_request_response("in_progress", row)
        updated = conn.execute(
            """
            UPDATE command_receipts
            SET state = 'uncertain',
                status = 'request_state_uncertain',
                result_json = ?,
                owner_token_hash = '',
                owner_expires_at = NULL,
                terminal_at = ?,
                updated_at = ?
            WHERE id = ?
              AND state = 'send_started'
              AND canonical_fingerprint = ?
              AND status = 'pending'
              AND result_json = ?
            """,
            (
                str(uncertain_result_json),
                current,
                current,
                int(row[0]),
                str(canonical_fingerprint),
                str(queued_result_json),
            ),
        )
        if int(updated.rowcount or 0) != 1:
            row = _command_request_row(conn, host_id, request_id)
            conn.commit()
            return _command_request_response("in_progress", row)
        row = _command_request_row(conn, host_id, request_id)
        if row is None:
            raise RuntimeError("uncertain queued command request disappeared")
        _project_command_request_conn(conn, row)
        _command_transition_event_conn(
            conn,
            row,
            observed_at=current,
            event_payload=event_payload,
        )
        conn.commit()
        return _command_request_response("uncertain", row)
    except Exception:
        if conn.in_transaction:
            conn.rollback()
        raise
    finally:
        conn.close()


def recover_unresolved_command_send(
    db_path: Path,
    *,
    host_id: str,
    request_id: str,
    canonical_fingerprint: str,
    unresolved_result_json: str,
    uncertain_result_json: str,
    event_payload: Mapping[str, Any] | None = None,
    now: str | None = None,
) -> dict[str, Any]:
    """Resolve an abandoned, verdict-less send-start as terminal uncertainty."""
    current = _command_request_now(now)
    conn = _connect(db_path, isolation_level=None, prepare=True)
    try:
        _ensure_schema(conn)
        conn.execute("BEGIN IMMEDIATE")
        row = _command_request_row(conn, host_id, request_id)
        if row is None:
            conn.commit()
            return _command_request_response("not_found", None)
        if str(row[5]) != str(canonical_fingerprint):
            conn.commit()
            return _command_request_response("request_id_conflict", row)
        if str(row[8]) in _COMMAND_REQUEST_TERMINAL_STATES:
            conn.commit()
            return _command_request_response("terminal", row)
        if (
            str(row[8]) != "send_started"
            or str(row[9]) != "pending"
            or str(row[10]) != str(unresolved_result_json)
        ):
            conn.commit()
            return _command_request_response("in_progress", row)
        expires_at = str(row[12] or "")
        if (
            not expires_at
            or datetime.fromisoformat(expires_at)
            > datetime.fromisoformat(current)
        ):
            conn.commit()
            return _command_request_response("in_progress", row)
        updated = conn.execute(
            """
            UPDATE command_receipts
            SET state = 'uncertain',
                status = 'request_state_uncertain',
                result_json = ?,
                owner_token_hash = '',
                owner_expires_at = NULL,
                terminal_at = ?,
                updated_at = ?
            WHERE id = ?
              AND state = 'send_started'
              AND canonical_fingerprint = ?
              AND status = 'pending'
              AND result_json = ?
            """,
            (
                str(uncertain_result_json),
                current,
                current,
                int(row[0]),
                str(canonical_fingerprint),
                str(unresolved_result_json),
            ),
        )
        if int(updated.rowcount or 0) != 1:
            row = _command_request_row(conn, host_id, request_id)
            conn.commit()
            return _command_request_response("in_progress", row)
        _terminalize_turn_submission_conn(
            conn,
            host_id=str(host_id),
            request_id=str(request_id),
            terminal_state="uncertain",
            current=current,
        )
        row = _command_request_row(conn, host_id, request_id)
        if row is None:
            raise RuntimeError("uncertain recovered command request disappeared")
        _project_command_request_conn(conn, row)
        _command_transition_event_conn(
            conn,
            row,
            observed_at=current,
            event_payload=event_payload,
        )
        conn.commit()
        return _command_request_response("uncertain", row)
    except Exception:
        if conn.in_transaction:
            conn.rollback()
        raise
    finally:
        conn.close()


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
    event_payload: Mapping[str, Any] | None = None,
    terminal_effect: Callable[[sqlite3.Connection], Any] | None = None,
    now: str | None = None,
) -> dict[str, Any]:
    """CAS the sole terminal writer without inserting or rewriting evidence."""
    expected = str(expected_state)
    terminal = str(terminal_state)
    allowed = {
        "reserved": {"rejected", "uncertain"},
        "send_started": {"accepted", "rejected", "uncertain"},
    }
    if expected not in allowed or terminal not in allowed[expected]:
        raise ValueError("illegal command request transition")
    if terminal == "accepted" and str(status) != "accepted":
        raise ValueError("accepted terminal state requires accepted status")
    if terminal == "uncertain" and str(status) != "request_state_uncertain":
        raise ValueError(
            "uncertain terminal state requires request_state_uncertain status"
        )
    if terminal == "rejected" and str(status) in {
        "pending",
        "answer_in_progress",
        "accepted",
        "request_state_uncertain",
    }:
        raise ValueError("rejected terminal state requires a rejection status")
    current = _command_request_now(now)
    owner_hash = _owner_token_hash(owner_token)
    conn = _connect(db_path, isolation_level=None, prepare=True)
    try:
        _ensure_schema(conn)
        conn.execute("BEGIN IMMEDIATE")
        row = _command_request_row(conn, host_id, request_id)
        if row is None:
            conn.commit()
            return _command_request_response("not_found", None)
        if str(row[5]) != str(canonical_fingerprint):
            conn.commit()
            return _command_request_response("request_id_conflict", row)
        if str(row[8]) in _COMMAND_REQUEST_TERMINAL_STATES:
            conn.commit()
            return _command_request_response("terminal", row)
        if str(row[8]) != expected:
            conn.commit()
            return _command_request_response("invalid_state", row)
        if not owner_hash or not secrets.compare_digest(str(row[11]), owner_hash):
            conn.commit()
            return _command_request_response("not_owner", row)
        updated = conn.execute(
            """
            UPDATE command_receipts
            SET state = ?,
                status = ?,
                result_json = ?,
                owner_token_hash = '',
                owner_expires_at = NULL,
                terminal_at = ?,
                updated_at = ?
            WHERE id = ?
              AND state = ?
              AND canonical_fingerprint = ?
              AND owner_token_hash = ?
            """,
            (
                terminal,
                str(status),
                str(result_json),
                current,
                current,
                int(row[0]),
                expected,
                str(canonical_fingerprint),
                owner_hash,
            ),
        )
        if int(updated.rowcount or 0) != 1:
            row = _command_request_row(conn, host_id, request_id)
            conn.commit()
            return _command_request_response("not_owner", row)
        if terminal_effect is not None:
            terminal_effect(conn)
        _terminalize_turn_submission_conn(
            conn,
            host_id=str(host_id),
            request_id=str(request_id),
            terminal_state=terminal,
            current=current,
        )
        row = _command_request_row(conn, host_id, request_id)
        if row is None:
            raise RuntimeError("command request terminal row disappeared")
        _project_command_request_conn(conn, row)
        _command_transition_event_conn(
            conn,
            row,
            observed_at=current,
            event_payload=event_payload,
        )
        conn.commit()
        return _command_request_response(terminal, row)
    except Exception:
        if conn.in_transaction:
            conn.rollback()
        raise
    finally:
        conn.close()


def cleanup_command_request_retention(
    db_path: Path,
    *,
    retry_horizon_seconds: int = COMMAND_RETRY_HORIZON_SECONDS,
    retention_seconds: int = COMMAND_RECEIPT_RETENTION_SECONDS,
    retention_count: int = COMMAND_RECEIPT_RETENTION_COUNT,
    host_id: str | None = None,
    now: str | None = None,
    dry_run: bool = False,
    batch_size: int = COMMAND_RECEIPT_RETENTION_BATCH_SIZE,
) -> dict[str, Any]:
    """Bound all inactive request evidence beyond both age and count floors."""
    policy_values = (
        retry_horizon_seconds,
        retention_seconds,
        retention_count,
        batch_size,
    )
    valid = all(
        isinstance(value, int) and not isinstance(value, bool) and value > 0
        for value in policy_values
    )
    valid = (
        valid
        and retry_horizon_seconds <= COMMAND_RETRY_HORIZON_SECONDS
        and retention_seconds >= COMMAND_RECEIPT_RETENTION_MIN_SECONDS
        and retention_seconds <= _MAX_TIMEDELTA_SECONDS
        and retention_seconds > retry_horizon_seconds
        and retention_count <= _SQLITE_MAX_INTEGER
        and batch_size <= _COMMAND_RECEIPT_RETENTION_BATCH_MAX
    )
    current = _command_request_now(now)
    cutoff_at = (
        datetime.fromisoformat(current)
        - timedelta(
            seconds=(
                retention_seconds
                if valid
                else COMMAND_RECEIPT_RETENTION_SECONDS
            )
        )
    ).isoformat(timespec="seconds")
    retry_cutoff_at = (
        datetime.fromisoformat(current)
        - timedelta(
            seconds=(
                retry_horizon_seconds
                if valid
                else COMMAND_RETRY_HORIZON_SECONDS
            )
        )
    ).isoformat(timespec="seconds")
    base_result = {
        "schema_version": 1,
        "host_id": None if host_id is None else str(host_id),
        "dry_run": bool(dry_run),
        "retry_horizon_seconds": (
            int(retry_horizon_seconds)
            if valid
            else COMMAND_RETRY_HORIZON_SECONDS
        ),
        "retention_seconds": (
            int(retention_seconds)
            if valid
            else COMMAND_RECEIPT_RETENTION_SECONDS
        ),
        "retention_count": (
            int(retention_count)
            if valid
            else COMMAND_RECEIPT_RETENTION_COUNT
        ),
        "batch_size": (
            int(batch_size)
            if valid
            else COMMAND_RECEIPT_RETENTION_BATCH_SIZE
        ),
        "cutoff_at": cutoff_at,
        "retry_cutoff_at": retry_cutoff_at,
    }
    if not valid:
        return dict(
            base_result,
            ok=False,
            status="invalid_policy",
            examined=0,
            stale_active=0,
            deleted=0,
            remaining_candidates=False,
        )
    if not _sqlite_store_exists(db_path):
        return dict(
            base_result,
            ok=False,
            status="store_unavailable",
            examined=0,
            stale_active=0,
            deleted=0,
            remaining_candidates=False,
        )
    scope_sql = "" if host_id is None else "AND host_id = :host_id"
    params: dict[str, Any] = {
        "cutoff_at": cutoff_at,
        "current": current,
        "retry_cutoff_at": retry_cutoff_at,
        "retention_count": int(retention_count),
        "host_id": None if host_id is None else str(host_id),
    }
    conn = _connect(db_path, isolation_level=None)
    try:
        _ensure_schema(conn)
        conn.execute("BEGIN IMMEDIATE")
        if not dry_run:
            _expire_turn_submissions_conn(
                conn,
                current=current,
                host_id=None if host_id is None else str(host_id),
            )
        stale_rows = conn.execute(
            f"""
            SELECT id
            FROM command_receipts
            WHERE state = 'send_started'
              AND COALESCE(send_started_at, updated_at) < :retry_cutoff_at
              {scope_sql}
            ORDER BY updated_at, host_id, id
            LIMIT :candidate_limit
            """,
            dict(params, candidate_limit=int(batch_size) + 1),
        ).fetchall()
        stale_ids = [int(row[0]) for row in stale_rows[: int(batch_size)]]
        remaining_capacity = int(batch_size) - len(stale_ids)
        stale_overflow = len(stale_rows) > len(stale_ids)
        if not dry_run:
            for receipt_id in stale_ids:
                conn.execute(
                    """
                    UPDATE command_receipts
                    SET state = 'uncertain',
                        status = 'request_state_uncertain',
                        result_json = ?,
                        owner_token_hash = '',
                        owner_expires_at = NULL,
                        terminal_at = ?,
                        updated_at = ?
                    WHERE id = ? AND state = 'send_started'
                    """,
                    (
                        _COMMAND_REQUEST_UNCERTAIN_RESULT_JSON,
                        current,
                        current,
                        receipt_id,
                    ),
                )
                row = conn.execute(
                    """
                    SELECT
                        id, host_id, request_id, action, canonical_version,
                        canonical_fingerprint, canonical_request_json,
                        public_worker_id, state, status, result_json,
                        owner_token_hash, owner_expires_at, binding_fingerprint,
                        created_at, reserved_at, send_started_at, terminal_at,
                        updated_at, selector_proof
                    FROM command_receipts
                    WHERE id = ?
                    """,
                    (receipt_id,),
                ).fetchone()
                if row is not None:
                    _terminalize_turn_submission_conn(
                        conn,
                        host_id=str(row[1]),
                        request_id=str(row[2]),
                        terminal_state="uncertain",
                        current=current,
                    )
                    _project_command_request_conn(conn, row)
                    _command_transition_event_conn(
                        conn,
                        row,
                        observed_at=current,
                        event_payload={"reason": "retention_stale_active"},
                    )
        deletion_rows: list[Any] = []
        deletion_overflow = False
        if remaining_capacity > 0 and not stale_overflow:
            deletion_rows = conn.execute(
                f"""
                WITH ranked AS (
                    SELECT
                        id,
                        host_id,
                        request_id,
                        state,
                        COALESCE(terminal_at, updated_at) AS retention_at,
                        ROW_NUMBER() OVER (
                            PARTITION BY host_id
                            ORDER BY COALESCE(terminal_at, updated_at) DESC, id DESC
                        ) AS retention_rank
                    FROM command_receipts
                    WHERE (
                        state IN ('accepted', 'rejected', 'uncertain')
                        OR (
                            state = 'reserved'
                            AND owner_expires_at IS NOT NULL
                            AND owner_expires_at <= :current
                        )
                    )
                    {scope_sql}
                )
                SELECT id, host_id, request_id
                FROM ranked
                WHERE retention_at < :cutoff_at
                  AND retention_rank > :retention_count
                ORDER BY retention_at, host_id, id
                LIMIT :candidate_limit
                """,
                dict(params, candidate_limit=remaining_capacity + 1),
            ).fetchall()
            deletion_overflow = len(deletion_rows) > remaining_capacity
            deletion_rows = deletion_rows[:remaining_capacity]
        if remaining_capacity == 0 and not stale_overflow:
            deletion_overflow = bool(
                conn.execute(
                    f"""
                    WITH ranked AS (
                        SELECT
                            id,
                            COALESCE(terminal_at, updated_at) AS retention_at,
                            ROW_NUMBER() OVER (
                                PARTITION BY host_id
                                ORDER BY COALESCE(terminal_at, updated_at) DESC, id DESC
                            ) AS retention_rank
                        FROM command_receipts
                        WHERE (
                            state IN ('accepted', 'rejected', 'uncertain')
                            OR (
                                state = 'reserved'
                                AND owner_expires_at IS NOT NULL
                                AND owner_expires_at <= :current
                            )
                        )
                        {scope_sql}
                    )
                    SELECT 1
                    FROM ranked
                    WHERE retention_at < :cutoff_at
                      AND retention_rank > :retention_count
                    LIMIT 1
                    """,
                    params,
                ).fetchone()
            )
        if not dry_run:
            for receipt_id, receipt_host_id, receipt_request_id in deletion_rows:
                conn.execute(
                    "DELETE FROM turn_submissions "
                    "WHERE host_id = ? AND request_id = ?",
                    (str(receipt_host_id), str(receipt_request_id)),
                )
                conn.execute(
                    "DELETE FROM commands WHERE host_id = ? AND request_id = ?",
                    (str(receipt_host_id), str(receipt_request_id)),
                )
                conn.execute(
                    """
                    DELETE FROM command_receipts
                    WHERE id = ?
                      AND (
                          state IN ('accepted', 'rejected', 'uncertain')
                          OR (
                              state = 'reserved'
                              AND owner_expires_at IS NOT NULL
                              AND owner_expires_at <= ?
                          )
                      )
                    """,
                    (int(receipt_id), current),
                )
            conn.commit()
        else:
            conn.rollback()
        remaining = stale_overflow or deletion_overflow
    except Exception:
        if conn.in_transaction:
            conn.rollback()
        raise
    finally:
        conn.close()
    return dict(
        base_result,
        ok=True,
        status="ok",
        examined=len(stale_ids) + len(deletion_rows),
        stale_active=len(stale_ids),
        deleted=len(deletion_rows),
        remaining_candidates=bool(remaining),
    )

def envelope_to_receipt_json(envelope: CommandEnvelope) -> str:
    """Serialize a command envelope for storage in a receipt."""
    return _canonical_json(envelope.to_dict())
