"""Secure SQLite connection and transaction authority."""

from __future__ import annotations

import os
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from ..local_state import (
    EntryType,
    entry_identity,
    identity_matches,
    lstat_at,
    prepare_resolved_private_parent,
    prepare_sqlite_family_at,
    proc_fd_path,
    validate_owned_regular_stat,
    verify_entry_identity,
)


class StorePathError(RuntimeError):
    """The configured store path is not an owner-private regular leaf."""


class StoreTimestampError(ValueError):
    """A persisted timestamp was not canonical UTC with a trailing ``Z``."""


def canonical_utc(value: str | datetime) -> str:
    """Return an exact microsecond UTC timestamp, rejecting naive values."""
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str) and value:
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise StoreTimestampError("invalid timestamp") from exc
    else:
        raise StoreTimestampError("timestamp must be a nonempty string or datetime")
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise StoreTimestampError("timestamp must include an offset")
    return (
        parsed.astimezone(timezone.utc)
        .isoformat(timespec="microseconds")
        .replace("+00:00", "Z")
    )


def utc_now() -> str:
    """Return the one persisted timestamp representation used by the store."""
    return canonical_utc(datetime.now(timezone.utc))


def add_seconds(value: str, seconds: int | float) -> str:
    """Add a finite nonnegative duration to a canonical timestamp."""
    if isinstance(seconds, bool) or not isinstance(seconds, (int, float)):
        raise ValueError("seconds must be numeric")
    if seconds < 0 or seconds == float("inf") or seconds != seconds:
        raise ValueError("seconds must be finite and nonnegative")
    parsed = datetime.fromisoformat(canonical_utc(value).replace("Z", "+00:00"))
    return canonical_utc(parsed + timedelta(seconds=float(seconds)))


def _path(path: Path | str) -> Path:
    value = Path(path)
    if not value.is_absolute() or str(value) == ":memory:" or ".." in value.parts:
        raise StorePathError("store path must be an absolute local leaf")
    if value.name in {"", ".", ".."} or "?" in value.name or "#" in value.name:
        raise StorePathError("store path must not be a URI")
    return value


class _PinnedConnection(sqlite3.Connection):
    """Connection retaining the exact securely walked parent descriptor."""

    _parent_fd: int = -1
    _leaf: str = ""
    _main_identity: Any = None

    def close(self) -> None:
        parent_fd = self._parent_fd
        if parent_fd < 0:
            return super().close()
        try:
            prepare_sqlite_family_at(
                parent_fd,
                self._leaf,
                _expected_main_identity=self._main_identity,
            )
            verify_entry_identity(
                parent_fd,
                self._leaf,
                self._main_identity,
                expected_type=EntryType.REGULAR_FILE,
            )
        finally:
            try:
                super().close()
            finally:
                os.close(parent_fd)
                self._parent_fd = -1


def _pragmas(conn: sqlite3.Connection, *, writable: bool) -> None:
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA trusted_schema = OFF")
    conn.execute("PRAGMA busy_timeout = 5000")
    if writable:
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA synchronous = FULL")


def _connect(path: Path | str, *, writable: bool = True, create: bool = False) -> sqlite3.Connection:
    db_path = _path(path)
    if not create and not db_path.exists():
        raise StorePathError("store does not exist")
    parent_fd, leaf, _ = prepare_resolved_private_parent(db_path)
    conn: _PinnedConnection | None = None
    try:
        prepare_sqlite_family_at(parent_fd, leaf, create_main=create)
        main = lstat_at(parent_fd, leaf)
        if main is None:
            raise StorePathError("store leaf could not be created")
        validate_owned_regular_stat(main)
        if main.st_nlink != 1:
            raise StorePathError("store leaf must have one link")
        main_identity = entry_identity(main)
        anchored = proc_fd_path(parent_fd, leaf)
        uri = f"file:{anchored}?mode={'rw' if writable else 'ro'}"
        conn = sqlite3.connect(
            uri,
            uri=True,
            isolation_level=None,
            timeout=5.0,
            factory=_PinnedConnection,
        )
        conn._parent_fd = parent_fd
        conn._leaf = leaf
        conn._main_identity = main_identity
        current = lstat_at(parent_fd, leaf)
        if current is None or not identity_matches(main_identity, current):
            raise StorePathError("store leaf identity changed during connect")
    except Exception:
        if conn is not None:
            conn._parent_fd = -1
            conn.close()
        os.close(parent_fd)
        raise
    conn.row_factory = sqlite3.Row
    try:
        _pragmas(conn, writable=writable)
        prepare_sqlite_family_at(
            parent_fd,
            leaf,
            _expected_main_identity=main_identity,
        )
        verify_entry_identity(
            parent_fd,
            leaf,
            main_identity,
            expected_type=EntryType.REGULAR_FILE,
        )
    except Exception:
        conn.close()
        raise
    return conn


def connect_read(path: Path | str) -> sqlite3.Connection:
    """Open one bounded read-only connection."""
    return _connect(path, writable=False)


@contextmanager
def read_transaction(path: Path | str) -> Iterator[sqlite3.Connection]:
    conn = connect_read(path)
    try:
        conn.execute("BEGIN")
        yield conn
        conn.execute("COMMIT")
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    finally:
        conn.close()


@contextmanager
def write_transaction(path: Path | str) -> Iterator[sqlite3.Connection]:
    conn = _connect(path, writable=True)
    try:
        conn.execute("BEGIN IMMEDIATE")
        yield conn
        conn.execute("COMMIT")
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    finally:
        conn.close()


def store_status(path: Path | str, host_id: str, **_: Any) -> dict[str, Any]:
    """Return bounded read-only health without repairing the store."""
    try:
        with read_transaction(path) as conn:
            version = int(conn.execute("PRAGMA user_version").fetchone()[0])
            integrity = str(conn.execute("PRAGMA quick_check(1)").fetchone()[0])
            counts = {
                table: int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
                for table in ("turns", "agent_events", "connector_outbox")
            }
    except Exception:
        return {"schema_version": 1, "ok": False, "status": "store_unavailable", "host_id": host_id, "counts": {}}
    return {
        "schema_version": 1,
        "ok": integrity == "ok",
        "status": "ok" if integrity == "ok" else "corrupt",
        "host_id": host_id,
        "store_schema_version": version,
        "counts": counts,
    }
