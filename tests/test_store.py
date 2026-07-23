"""Tests for the sqlite store contract."""

from __future__ import annotations

import fcntl
import gc
import hashlib
import json
import multiprocessing
import os
import sqlite3
import stat
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest

from tendwire.local_state import (
    EntryIdentity,
    entry_identity,
    LocalStateError,
    LocalStateErrorCode,
    LocalStateKind,
    PermissionState,
    prepare_sqlite_family,
    repair_sqlite_family,
)

from tendwire.core.commands import STATUS_ACCEPTED
from tendwire.config import Config
from tendwire.core.models import Snapshot, Worker, WorkerBinding
from tendwire.core.turns import Turn, content_cursor, turn_final_delivery_identity
from tendwire.core.projector import project_empty, project_from_raw
from tendwire.store import sqlite as store_sqlite
from tendwire.store.sqlite import (
    SnapshotObservationContext,
    CompactionOptions,
    ack_connector_delivery,
    backend_pending_choice_terminal_effect,
    append_event,
    attention_payload_from_store,
    cleanup_event_retention,
    cleanup_acknowledged_final_retention,
    command_pending_turn_terminal_effect,
    compact_store,
    defer_connector_delivery,
    exhaust_connector_retries,
    expire_stale_worker_bindings,
    expire_worker_bindings,
    fail_connector_delivery,
    cleanup_command_request_retention,
    finish_command_request,
    get_command_request,
    init_store,
    latest_snapshot,
    list_attention_items,
    list_hosts,
    list_worker_bindings,
    reclaim_expired_connector_leases,
    poll_connector_outbox,
    mark_command_send_started,
    reserve_command_request,
    reserve_terminal_command_replay,
    resolve_worker_binding,
    run_store_maintenance,
    save_snapshot,
    store_status,
    tail_event_metadata,
    merge_turn_content,
    turns_payload_from_store,
    upsert_worker_bindings,
)


_PR6_TABLES = {
    "events",
    "spaces",
    "workers",
    "worker_bindings",
    "turns",
    "pending_interactions",
    "attention_items",
    "attention_lifecycles",
    "commands",
    "command_receipts",
    "connector_outbox",
    "connector_deliveries",
    "backend_health",
    "turn_content_revisions",
    "turn_presentation_plans",
    "turn_presentation_jobs",
}


def _user_version(conn: sqlite3.Connection) -> int:
    return int(conn.execute("PRAGMA user_version").fetchone()[0])


def _table_names(conn: sqlite3.Connection) -> set[str]:
    return {
        str(row[0])
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        ).fetchall()
    }


def _indexed_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    columns: set[str] = set()
    for row in conn.execute(f"PRAGMA index_list({table})").fetchall():
        index_name = row[1]
        for index_row in conn.execute(f"PRAGMA index_info({index_name})").fetchall():
            columns.add(index_row[2])
    return columns


def _unique_index_columns(conn: sqlite3.Connection, table: str) -> dict[str, tuple[str, ...]]:
    indexes: dict[str, tuple[str, ...]] = {}
    for row in conn.execute(f"PRAGMA index_list({table})").fetchall():
        if int(row[2]) != 1:
            continue
        index_name = str(row[1])
        columns = tuple(
            str(index_row[2])
            for index_row in conn.execute(f"PRAGMA index_info({index_name})").fetchall()
        )
        indexes[index_name] = columns
    return indexes


def _cross_process_store_writer(
    db_path: str,
    barrier: Any,
    results: Any,
    actor: str,
    padding_fd_count: int,
) -> None:
    padding_fds: list[int] = []
    try:
        padding_fds = [
            os.open(os.devnull, os.O_RDONLY) for _ in range(padding_fd_count)
        ]
        with store_sqlite._connect(Path(db_path), prepare=True) as conn:
            barrier.wait(timeout=15)
            for sequence in range(80):
                conn.execute(
                    "INSERT INTO cross_process_writes (actor, sequence) VALUES (?, ?)",
                    (actor, sequence),
                )
                conn.commit()
        results.put(None)
    except BaseException as exc:
        results.put(f"{type(exc).__name__}: {exc}")
    finally:
        for fd in padding_fds:
            os.close(fd)


def _cross_process_try_immediate(db_path: str, results: Any) -> None:
    try:
        with sqlite3.connect(db_path, timeout=0) as conn:
            conn.execute("PRAGMA busy_timeout=0")
            conn.execute("BEGIN IMMEDIATE")
        results.put("acquired")
    except sqlite3.OperationalError as exc:
        results.put("locked" if "locked" in str(exc).lower() else type(exc).__name__)



def _cross_process_hold_parent_lock(
    parent_path: str,
    operation: int,
    acquired: Any,
    release: Any,
) -> None:
    parent_fd = -1
    try:
        parent_fd = os.open(
            parent_path,
            os.O_RDONLY | os.O_DIRECTORY | getattr(os, "O_CLOEXEC", 0),
        )
        fcntl.flock(parent_fd, operation)
        acquired.put(None)
        if not release.wait(timeout=15):
            raise TimeoutError("parent lock release was not signaled")
    except BaseException as exc:
        acquired.put(f"{type(exc).__name__}: {exc}")
    finally:
        if parent_fd >= 0:
            try:
                fcntl.flock(parent_fd, fcntl.LOCK_UN)
            finally:
                os.close(parent_fd)

def _assert_cross_process_immediate_is_locked(db_path: Path) -> None:
    context = multiprocessing.get_context("spawn")
    results = context.Queue()
    process = context.Process(
        target=_cross_process_try_immediate,
        args=(str(db_path), results),
    )
    started = False
    try:
        process.start()
        started = True
        process.join(timeout=15)
        if process.is_alive():
            process.terminate()
            process.join(timeout=5)
            pytest.fail("SQLite lock probe did not terminate")
        assert process.exitcode == 0
        assert results.get(timeout=5) == "locked"
    finally:
        if started and process.is_alive():
            process.terminate()
            process.join(timeout=5)
        if started and not process.is_alive():
            process.close()
        results.close()
        results.join_thread()



def _mode(path: Path) -> int:
    return stat.S_IMODE(path.lstat().st_mode)


def _create_private_empty_file(path: Path) -> None:
    fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    os.close(fd)


def _tree_metadata(root: Path) -> tuple[tuple[str, int, int, int, int], ...]:
    return tuple(
        sorted(
            (
                str(path.relative_to(root)),
                stat.S_IFMT(current.st_mode),
                stat.S_IMODE(current.st_mode),
                current.st_size,
                current.st_mtime_ns,
            )
            for path in root.rglob("*")
            for current in (path.lstat(),)
        )
    )


def _invoke_creation_capable_store_path(name: str, db_path: Path) -> None:
    config = Config(host_id="host-a", db_path=db_path)
    snapshot = project_from_raw(
        config,
        workers=[{"id": "worker-1", "name": "Worker One"}],
    )
    if name == "init_store":
        init_store(db_path)
    elif name == "append_event":
        append_event(db_path, "host-a", "store.test", {"safe": True})
    elif name == "upsert_command_pending_turn":
        store_sqlite.upsert_command_pending_turn(
            db_path,
            "host-a",
            snapshot.workers[0],
            request_id="request-1",
            instruction_text="Continue.",
        )
    elif name == "save_snapshot":
        save_snapshot(db_path, snapshot)
    elif name == "upsert_worker_bindings":
        upsert_worker_bindings(db_path, [_worker_binding()])
    elif name == "expire_worker_bindings":
        expire_worker_bindings(db_path, "host-a")
    elif name == "expire_stale_worker_bindings":
        expire_stale_worker_bindings(
            db_path,
            "host-a",
            backend="herdr",
            current_private_fingerprints=[],
        )
    elif name == "reserve_command_request":
        reserve_command_request(
            db_path,
            host_id="host-a",
            request_id="request-1",
            action="send_instruction",
            canonical_version=1,
            canonical_fingerprint="payload-fingerprint",
            canonical_request_json='{"action":"send_instruction"}',
            public_worker_id="worker-1",
            pending_result_json='{"status":"pending"}',
        )
    else:
        raise AssertionError(f"unknown creation-capable path: {name}")


def test_store_secure_creation_ignores_permissive_umask_for_live_sqlite_family(
    tmp_path: Path,
) -> None:
    state_dir = tmp_path / "private-state"
    db_path = state_dir / "tendwire.db"
    previous_umask = os.umask(0)
    try:
        init_store(db_path)
        with store_sqlite._connect(db_path) as conn:
            assert _user_version(conn) == store_sqlite.STORE_SCHEMA_VERSION
            assert _mode(Path(f"{db_path}-wal")) == 0o600
            assert _mode(Path(f"{db_path}-shm")) == 0o600
    finally:
        os.umask(previous_umask)

    assert _mode(state_dir) == 0o700
    assert _mode(db_path) == 0o600


def test_creation_boundary_rejects_live_sidecar_change_after_wal_activation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "tendwire.db"
    init_store(db_path)
    original_validate = store_sqlite._validate_sqlite_family_at
    validated_live_family = False

    def broaden_then_validate(parent_fd: int, leaf: str) -> None:
        nonlocal validated_live_family
        try:
            os.stat(f"{leaf}-wal", dir_fd=parent_fd, follow_symlinks=False)
            os.stat(f"{leaf}-shm", dir_fd=parent_fd, follow_symlinks=False)
        except FileNotFoundError:
            pass
        else:
            os.chmod(f"{leaf}-wal", 0o644, dir_fd=parent_fd)
            os.chmod(f"{leaf}-shm", 0o644, dir_fd=parent_fd)
            validated_live_family = True
        original_validate(parent_fd, leaf)

    monkeypatch.setattr(
        store_sqlite,
        "_validate_sqlite_family_at",
        broaden_then_validate,
    )

    with pytest.raises(LocalStateError) as caught:
        store_sqlite._connect(db_path, prepare=True)

    assert caught.value.code is LocalStateErrorCode.INSECURE_MODE
    assert validated_live_family is True


def test_store_startup_repairs_broad_modes_idempotently_and_preserves_data(
    tmp_path: Path,
) -> None:
    state_dir = tmp_path / "broad-state"
    state_dir.mkdir()
    os.chmod(state_dir, 0o755)
    db_path = state_dir / "tendwire.db"
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute("CREATE TABLE preserved (value TEXT NOT NULL)")
        conn.execute("INSERT INTO preserved (value) VALUES ('kept')")
    conn.close()
    os.chmod(db_path, 0o644)
    inode = db_path.stat().st_ino

    init_store(db_path)
    first_modes = (_mode(state_dir), _mode(db_path))
    with sqlite3.connect(str(db_path)) as conn:
        first_value = conn.execute("SELECT value FROM preserved").fetchone()[0]
        first_version = _user_version(conn)
    conn.close()

    init_store(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        second_value = conn.execute("SELECT value FROM preserved").fetchone()[0]
        second_version = _user_version(conn)
    conn.close()

    assert first_modes == (0o700, 0o600)
    assert (_mode(state_dir), _mode(db_path)) == first_modes
    assert db_path.stat().st_ino == inode
    assert (first_value, second_value) == ("kept", "kept")
    assert (first_version, second_version) == (
        store_sqlite.STORE_SCHEMA_VERSION,
        store_sqlite.STORE_SCHEMA_VERSION,
    )


def test_sqlite_family_preparation_does_not_widen_stricter_modes(tmp_path: Path) -> None:
    state_dir = tmp_path / "strict-state"
    state_dir.mkdir()
    db_path = state_dir / "tendwire.db"
    _create_private_empty_file(db_path)
    os.chmod(state_dir, 0o500)
    os.chmod(db_path, 0o400)

    prepare_sqlite_family(db_path)

    assert _mode(state_dir) == 0o500
    assert _mode(db_path) == 0o400


def test_sqlite_backup_and_vacuum_preserve_private_modes_under_permissive_umask(
    tmp_path: Path,
) -> None:
    source_path = tmp_path / "state" / "source.db"
    backup_path = tmp_path / "state" / "backup.db"
    init_store(source_path)
    with store_sqlite._connect(source_path) as conn:
        conn.execute("CREATE TABLE backup_probe (value TEXT NOT NULL)")
        conn.execute("INSERT INTO backup_probe (value) VALUES ('kept')")

    previous_umask = os.umask(0)
    try:
        prepare_sqlite_family(backup_path)
        with store_sqlite._connect(source_path) as source:
            with sqlite3.connect(str(backup_path)) as destination:
                source.backup(destination)
        with store_sqlite._connect(source_path, isolation_level=None) as conn:
            conn.execute("VACUUM")
            for suffix in ("", "-wal", "-shm"):
                member = Path(f"{source_path}{suffix}")
                if member.exists():
                    assert _mode(member) == 0o600
    finally:
        os.umask(previous_umask)

    for database_path in (source_path, backup_path):
        assert _mode(database_path) == 0o600
        for suffix in ("-wal", "-shm", "-journal"):
            member = Path(f"{database_path}{suffix}")
            if member.exists():
                assert _mode(member) == 0o600
    with sqlite3.connect(str(backup_path)) as conn:
        assert conn.execute("SELECT value FROM backup_probe").fetchone()[0] == "kept"


@pytest.mark.parametrize("suffix", ["", "-wal", "-shm", "-journal"])
@pytest.mark.parametrize("entry_type", ["symlink", "directory"])
def test_store_refuses_wrong_type_or_symlink_for_every_sqlite_family_member(
    tmp_path: Path,
    suffix: str,
    entry_type: str,
) -> None:
    state_dir = tmp_path / "private-state"
    state_dir.mkdir(mode=0o700)
    os.chmod(state_dir, 0o700)
    db_path = state_dir / "secret.db"
    family_path = Path(f"{db_path}{suffix}")
    if suffix:
        _create_private_empty_file(db_path)
    if entry_type == "symlink":
        target = tmp_path / "outside-target"
        _create_private_empty_file(target)
        family_path.symlink_to(target)
    else:
        family_path.mkdir()

    with pytest.raises(LocalStateError) as caught:
        init_store(db_path)

    assert caught.value.code is LocalStateErrorCode.WRONG_TYPE
    assert str(db_path) not in str(caught.value)
    if entry_type == "symlink":
        assert family_path.is_symlink()
    else:
        assert family_path.is_dir()


@pytest.mark.parametrize(
    ("state_mode", "db_mode"),
    [(0o755, 0o600), (0o700, 0o644)],
)
def test_store_read_refuses_broad_state_without_repairing(
    tmp_path: Path,
    state_mode: int,
    db_mode: int,
) -> None:
    state_dir = tmp_path / "private-state"
    db_path = state_dir / "tendwire.db"
    init_store(db_path)
    os.chmod(state_dir, state_mode)
    os.chmod(db_path, db_mode)

    with pytest.raises(LocalStateError) as caught:
        latest_snapshot(db_path)

    assert caught.value.code is LocalStateErrorCode.INSECURE_MODE
    assert (_mode(state_dir), _mode(db_path)) == (state_mode, db_mode)


def test_store_broad_parent_error_has_actionable_path_free_remediation(
    tmp_path: Path,
) -> None:
    state_dir = tmp_path / "private-state"
    db_path = state_dir / "tendwire.db"
    init_store(db_path)
    os.chmod(state_dir, 0o755)

    with pytest.raises(LocalStateError) as caught:
        latest_snapshot(db_path)

    message = str(caught.value)
    assert caught.value.code is LocalStateErrorCode.INSECURE_MODE
    assert "`tendwire daemon`" in message
    assert "restrict permissions manually" in message
    assert "doctor --fix-permissions" not in message
    assert str(state_dir) not in message
    assert str(db_path) not in message


@pytest.mark.parametrize(
    "operation",
    ["connect", "latest_snapshot", "store_status"],
)
def test_store_reads_refuse_broad_sidecar_without_repairing(
    tmp_path: Path,
    operation: str,
) -> None:
    state_dir = tmp_path / "private-state"
    db_path = state_dir / "tendwire.db"
    init_store(db_path)
    journal_path = Path(f"{db_path}-journal")
    _create_private_empty_file(journal_path)
    os.chmod(journal_path, 0o644)

    if operation == "connect":
        with pytest.raises(LocalStateError) as caught:
            store_sqlite._connect(db_path, read_only=True)
        assert caught.value.code is LocalStateErrorCode.INSECURE_MODE
    elif operation == "latest_snapshot":
        with pytest.raises(LocalStateError) as caught:
            latest_snapshot(db_path)
        assert caught.value.code is LocalStateErrorCode.INSECURE_MODE
    else:
        result = store_status(
            db_path,
            "readonly-host",
            require_current_schema=True,
        )
        assert result["ok"] is False
        assert result["status"] == "store_unavailable"

    assert _mode(journal_path) == 0o644


@pytest.mark.parametrize(
    "operation",
    ["validate", "connect", "latest_snapshot", "store_status"],
)
def test_store_reads_do_not_invoke_local_state_mutation_for_absent_sidecars(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    operation: str,
) -> None:
    db_path = tmp_path / "validation-only" / "tendwire.db"
    init_store(db_path)
    sidecars = tuple(Path(f"{db_path}{suffix}") for suffix in ("-wal", "-shm", "-journal"))
    for sidecar in sidecars:
        sidecar.unlink(missing_ok=True)
    before_main = db_path.stat()

    def reject_mutation(*_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError("ordinary store reads must not prepare or create state")

    monkeypatch.setattr(store_sqlite, "prepare_sqlite_family_at", reject_mutation)
    monkeypatch.setattr(store_sqlite, "create_private_file_at", reject_mutation)

    if operation == "validate":
        parent_fd, leaf = store_sqlite.open_resolved_parent(db_path)
        try:
            store_sqlite._validate_sqlite_family_at(parent_fd, leaf)
        finally:
            os.close(parent_fd)
    elif operation == "connect":
        with store_sqlite._connect(db_path, read_only=True) as conn:
            assert conn.execute("PRAGMA quick_check").fetchone() == ("ok",)
    elif operation == "latest_snapshot":
        assert latest_snapshot(db_path) is None
    else:
        result = store_status(
            db_path,
            "readonly-host",
            require_current_schema=True,
        )
        assert result["ok"] is True

    after_main = db_path.stat()
    assert (after_main.st_dev, after_main.st_ino, after_main.st_size) == (
        before_main.st_dev,
        before_main.st_ino,
        before_main.st_size,
    )
    assert not sidecars[2].exists()
    if operation in {"validate", "latest_snapshot"}:
        assert not any(sidecar.exists() for sidecar in sidecars)


@pytest.mark.parametrize(
    "entrypoint",
    [
        "init_store",
        "append_event",
        "upsert_command_pending_turn",
        "save_snapshot",
        "upsert_worker_bindings",
        "expire_worker_bindings",
        "expire_stale_worker_bindings",
        "reserve_command_request",
    ],
)
def test_every_creation_capable_store_path_prepares_private_sqlite_state(
    tmp_path: Path,
    entrypoint: str,
) -> None:
    state_dir = tmp_path / entrypoint
    db_path = state_dir / "tendwire.db"

    _invoke_creation_capable_store_path(entrypoint, db_path)

    assert _mode(state_dir) == 0o700
    assert _mode(db_path) == 0o600
    with sqlite3.connect(str(db_path)) as conn:
        assert _user_version(conn) == store_sqlite.STORE_SCHEMA_VERSION


@pytest.mark.parametrize("operation", ["init", "read"])
@pytest.mark.parametrize("configured_kind", ["absolute", "controlled-relative"])
def test_store_refuses_intermediate_symlink_without_mutating_target(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    operation: str,
    configured_kind: str,
) -> None:
    target_parent = tmp_path / "target" / "private"
    target_parent.mkdir(parents=True, mode=0o700)
    os.chmod(target_parent, 0o700)
    leaf = f"tendwire-{os.getpid()}-{tmp_path.name}.db"
    target_db = target_parent / leaf
    if operation == "read":
        init_store(target_db)
    else:
        (target_parent / "sentinel").write_text("unchanged", encoding="utf-8")

    configured_root = tmp_path / "configured"
    configured_root.mkdir(mode=0o700)
    linked_parent = configured_root / "linked"
    linked_parent.symlink_to(target_parent, target_is_directory=True)
    configured_db = linked_parent / leaf
    before = _tree_metadata(target_parent)
    root_artifact = Path(os.sep) / leaf
    assert not root_artifact.exists()

    if configured_kind == "controlled-relative":
        monkeypatch.chdir(configured_root)
        requested_db = Path("linked") / leaf
    else:
        requested_db = configured_db

    with pytest.raises(LocalStateError) as caught:
        if operation == "init":
            init_store(requested_db)
        else:
            latest_snapshot(requested_db)

    assert caught.value.code is LocalStateErrorCode.WRONG_TYPE
    assert _tree_metadata(target_parent) == before
    assert linked_parent.is_symlink()
    assert not root_artifact.exists()
    for private_path in (configured_db, linked_parent, target_parent, target_db):
        assert str(private_path) not in str(caught.value)


@pytest.mark.parametrize("configured_kind", ["absolute", "controlled-relative"])
def test_store_read_treats_absent_nested_parent_as_missing_without_creation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    configured_kind: str,
) -> None:
    absolute_db = tmp_path / "absent" / "nested" / "tendwire.db"
    if configured_kind == "controlled-relative":
        monkeypatch.chdir(tmp_path)
        requested_db = Path("absent/nested/tendwire.db")
    else:
        requested_db = absolute_db
    before = set(os.listdir("/proc/self/fd"))

    assert latest_snapshot(requested_db) is None

    assert not (tmp_path / "absent").exists()
    assert set(os.listdir("/proc/self/fd")) == before


def test_store_initializes_bare_relative_db_in_controlled_cwd_without_root_artifact(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    controlled = tmp_path / "controlled"
    controlled.mkdir(mode=0o700)
    os.chmod(controlled, 0o700)
    monkeypatch.chdir(controlled)
    relative_db = Path("controlled-relative.db")
    root_artifact = Path("/controlled-relative.db")
    assert not root_artifact.exists()

    init_store(relative_db)

    assert relative_db.is_file()
    assert _mode(relative_db) == 0o600
    assert not root_artifact.exists()
    with store_sqlite._connect(relative_db) as conn:
        assert _user_version(conn) == store_sqlite.STORE_SCHEMA_VERSION


def test_store_rejects_bare_relative_db_from_writable_cwd_without_creation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    writable = tmp_path / "writable"
    writable.mkdir(mode=0o700)
    monkeypatch.chdir(writable)
    os.chmod(writable, 0o777)
    try:
        with pytest.raises(LocalStateError) as caught:
            init_store(Path("unsafe-relative.db"))
    finally:
        os.chmod(writable, 0o700)

    assert caught.value.code is LocalStateErrorCode.INSECURE_MODE
    assert not (writable / "unsafe-relative.db").exists()
    assert str(writable) not in str(caught.value)


def test_store_uri_quotes_pinned_database_leaf(tmp_path: Path) -> None:
    state_dir = tmp_path / "quoted"
    db_path = state_dir / "question?hash#percent%.db"

    init_store(db_path)

    with store_sqlite._connect(db_path) as conn:
        assert _user_version(conn) == store_sqlite.STORE_SCHEMA_VERSION
    assert db_path.is_file()
    assert [path.name for path in state_dir.iterdir()] == [
        "question?hash#percent%.db",
    ]


def test_store_wal_is_shared_safely_across_independent_processes(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "cross-process" / "tendwire.db"
    init_store(db_path)
    with store_sqlite._connect(db_path, prepare=True) as conn:
        conn.execute(
            "CREATE TABLE cross_process_writes ("
            "actor TEXT NOT NULL, sequence INTEGER NOT NULL, "
            "PRIMARY KEY (actor, sequence))"
        )

    context = multiprocessing.get_context("spawn")
    barrier = context.Barrier(2)
    results = context.Queue()
    processes = [
        context.Process(
            target=_cross_process_store_writer,
            args=(str(db_path), barrier, results, "first", 0),
        ),
        context.Process(
            target=_cross_process_store_writer,
            args=(str(db_path), barrier, results, "second", 11),
        ),
    ]
    started: list[Any] = []
    try:
        for process in processes:
            process.start()
            started.append(process)
        for process in started:
            process.join(timeout=30)
            if process.is_alive():
                process.terminate()
                process.join(timeout=5)
                pytest.fail("cross-process SQLite writer did not terminate")
            assert process.exitcode == 0

        assert [results.get(timeout=5) for _ in processes] == [None, None]
        with store_sqlite._connect(db_path) as conn:
            assert (
                conn.execute("SELECT COUNT(*) FROM cross_process_writes").fetchone()[0]
                == 160
            )
            assert conn.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
    finally:
        for process in started:
            if process.is_alive():
                process.terminate()
                process.join(timeout=5)
            if not process.is_alive():
                process.close()
        results.close()
        results.join_thread()


def test_sqlite_family_preparation_survives_fixed_real_wal_close_churn(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "bounded-wal-churn.db"
    init_store(db_path)
    with store_sqlite._connect(db_path, isolation_level=None) as conn:
        conn.execute(
            "CREATE TABLE wal_churn (sequence INTEGER PRIMARY KEY, value TEXT NOT NULL)"
        )
    main_stat = db_path.stat()
    before_fds = set(os.listdir("/proc/self/fd"))
    before_threads = {id(thread) for thread in threading.enumerate()}
    before_children = {process.pid for process in multiprocessing.active_children()}
    cycles = 64
    ready = threading.Barrier(2)
    settled = threading.Barrier(2)
    errors: list[BaseException] = []
    writes: list[int] = []
    preparations: list[tuple[PermissionState, ...]] = []

    def churn_wal() -> None:
        try:
            for sequence in range(cycles):
                conn = sqlite3.connect(str(db_path), timeout=5, isolation_level=None)
                try:
                    conn.execute("PRAGMA busy_timeout=5000")
                    conn.execute("BEGIN IMMEDIATE")
                    conn.execute(
                        "INSERT INTO wal_churn (sequence, value) VALUES (?, ?)",
                        (sequence, f"value-{sequence}"),
                    )
                    conn.execute("COMMIT")
                    checkpoint = conn.execute(
                        "PRAGMA wal_checkpoint(TRUNCATE)"
                    ).fetchone()
                    assert checkpoint is not None
                    assert int(checkpoint[0]) == 0
                    writes.append(sequence)
                    ready.wait(timeout=10)
                finally:
                    conn.close()
                settled.wait(timeout=10)
        except BaseException as exc:
            errors.append(exc)
            ready.abort()
            settled.abort()

    def prepare_family() -> None:
        try:
            for _sequence in range(cycles):
                ready.wait(timeout=10)
                results = prepare_sqlite_family(db_path)
                preparations.append(tuple(result.state for result in results))
                settled.wait(timeout=10)
        except BaseException as exc:
            errors.append(exc)
            ready.abort()
            settled.abort()

    workers = [
        threading.Thread(target=churn_wal),
        threading.Thread(target=prepare_family),
    ]
    for worker in workers:
        worker.start()
    for worker in workers:
        worker.join(timeout=20)
    if any(worker.is_alive() for worker in workers):
        ready.abort()
        settled.abort()
        for worker in workers:
            worker.join(timeout=2)

    assert not any(worker.is_alive() for worker in workers)
    assert errors == []
    assert writes == list(range(cycles))
    assert len(preparations) == cycles
    assert all(
        states[0] in {PermissionState.PRIVATE, PermissionState.REPAIRED}
        and all(
            state in {
                PermissionState.PRIVATE,
                PermissionState.REPAIRED,
                PermissionState.ABSENT,
            }
            for state in states[1:]
        )
        for states in preparations
    )
    assert not any(
        isinstance(exc, LocalStateError)
        and exc.code is LocalStateErrorCode.MISSING_ENTRY
        for exc in errors
    )
    integrity = sqlite3.connect(str(db_path))
    try:
        assert integrity.execute("SELECT COUNT(*) FROM wal_churn").fetchone() == (cycles,)
        assert integrity.execute("PRAGMA integrity_check").fetchone() == ("ok",)
    finally:
        integrity.close()
    after_stat = db_path.stat()
    assert (after_stat.st_dev, after_stat.st_ino) == (
        main_stat.st_dev,
        main_stat.st_ino,
    )
    assert {id(thread) for thread in threading.enumerate()} == before_threads
    assert {process.pid for process in multiprocessing.active_children()} == before_children
    assert set(os.listdir("/proc/self/fd")) == before_fds


def test_prepare_sqlite_family_preserves_existing_immediate_lock(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "same-process-lock" / "tendwire.db"
    init_store(db_path)
    from multiprocessing import resource_tracker

    resource_tracker.ensure_running()
    before_fds = set(os.listdir("/proc/self/fd"))
    before_threads = {id(thread) for thread in threading.enumerate()}
    before_children = {process.pid for process in multiprocessing.active_children()}
    holder = store_sqlite._connect(db_path, prepare=True)
    try:
        holder.execute("BEGIN IMMEDIATE")
        prepare_sqlite_family(db_path)
        _assert_cross_process_immediate_is_locked(db_path)
    finally:
        holder.rollback()
        holder.close()
    assert set(os.listdir("/proc/self/fd")) == before_fds
    assert {id(thread) for thread in threading.enumerate()} == before_threads
    assert {process.pid for process in multiprocessing.active_children()} == before_children


@pytest.mark.parametrize("suffix", ["", "-wal", "-shm"])
@pytest.mark.parametrize(
    "repair",
    [prepare_sqlite_family, repair_sqlite_family],
)
def test_broad_sqlite_member_repair_is_excluded_by_active_store_lock(
    tmp_path: Path,
    suffix: str,
    repair: Any,
) -> None:
    db_path = tmp_path / "active-repair-lock" / "tendwire.db"
    init_store(db_path)
    holder = store_sqlite._connect(db_path, prepare=True)
    member = Path(f"{db_path}{suffix}")
    try:
        holder.execute("BEGIN IMMEDIATE")
        assert member.is_file()
        os.chmod(member, 0o644)

        with pytest.raises(LocalStateError) as caught:
            repair(db_path)

        assert caught.value.code is LocalStateErrorCode.OPERATION_FAILED
        assert _mode(member) == 0o644
        _assert_cross_process_immediate_is_locked(db_path)
    finally:
        holder.rollback()
        holder.close()


def test_prepare_existing_broad_parent_requires_exclusive_store_authority(
    tmp_path: Path,
) -> None:
    from multiprocessing import resource_tracker

    db_path = tmp_path / "active-parent-repair-lock" / "tendwire.db"
    init_store(db_path)
    parent = db_path.parent
    parent_identity = entry_identity(os.lstat(parent))
    database_identity = entry_identity(os.lstat(db_path))
    resource_tracker.ensure_running()
    before_fds = set(os.listdir("/proc/self/fd"))
    before_threads = {id(thread) for thread in threading.enumerate()}
    before_children = {process.pid for process in multiprocessing.active_children()}
    holder = store_sqlite._connect(db_path, prepare=True)
    try:
        holder.execute("BEGIN IMMEDIATE")
        assert _mode(parent) == 0o700
        os.chmod(parent, 0o755)

        with pytest.raises(LocalStateError) as caught:
            init_store(db_path)

        assert caught.value.code is LocalStateErrorCode.OPERATION_FAILED
        assert _mode(parent) == 0o755
        assert _mode(db_path) == 0o600
        assert entry_identity(os.lstat(parent)) == parent_identity
        assert entry_identity(os.lstat(db_path)) == database_identity
        _assert_cross_process_immediate_is_locked(db_path)
    finally:
        holder.rollback()
        holder.close()
    assert set(os.listdir("/proc/self/fd")) == before_fds
    assert {id(thread) for thread in threading.enumerate()} == before_threads
    assert {process.pid for process in multiprocessing.active_children()} == before_children


def test_direct_prepare_broad_parent_requires_exclusive_store_authority(
    tmp_path: Path,
) -> None:
    from multiprocessing import resource_tracker

    db_path = tmp_path / "active-direct-parent-lock" / "tendwire.db"
    init_store(db_path)
    parent = db_path.parent
    parent_identity = entry_identity(os.lstat(parent))
    database_identity = entry_identity(os.lstat(db_path))
    resource_tracker.ensure_running()
    before_fds = set(os.listdir("/proc/self/fd"))
    before_threads = {id(thread) for thread in threading.enumerate()}
    before_children = {process.pid for process in multiprocessing.active_children()}
    holder = store_sqlite._connect(db_path, prepare=True)
    try:
        holder.execute("BEGIN IMMEDIATE")
        assert _mode(parent) == 0o700
        os.chmod(parent, 0o755)

        with pytest.raises(LocalStateError) as caught:
            prepare_sqlite_family(db_path)

        assert caught.value.code is LocalStateErrorCode.OPERATION_FAILED
        assert _mode(parent) == 0o755
        assert _mode(db_path) == 0o600
        assert entry_identity(os.lstat(parent)) == parent_identity
        assert entry_identity(os.lstat(db_path)) == database_identity
        _assert_cross_process_immediate_is_locked(db_path)
    finally:
        holder.rollback()
        holder.close()
    assert set(os.listdir("/proc/self/fd")) == before_fds
    assert {id(thread) for thread in threading.enumerate()} == before_threads
    assert {process.pid for process in multiprocessing.active_children()} == before_children


@pytest.mark.parametrize("suffix", ["", "-wal", "-shm"])
def test_private_sqlite_member_preparation_preserves_active_store_lock(
    tmp_path: Path,
    suffix: str,
) -> None:
    db_path = tmp_path / "private-active-lock" / "tendwire.db"
    init_store(db_path)
    holder = store_sqlite._connect(db_path, prepare=True)
    member = Path(f"{db_path}{suffix}")
    try:
        holder.execute("BEGIN IMMEDIATE")
        assert member.is_file()
        assert _mode(member) == 0o600

        prepare_sqlite_family(db_path)

        assert _mode(member) == 0o600
        _assert_cross_process_immediate_is_locked(db_path)
    finally:
        holder.rollback()
        holder.close()


@pytest.mark.parametrize("prepare", [False, True])
def test_store_connection_stays_on_pinned_parent_after_ancestor_substitution(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    prepare: bool,
) -> None:
    current_root = tmp_path / "current"
    replacement_root = tmp_path / "replacement"
    current_db = current_root / "state" / "anchored.db"
    replacement_db = replacement_root / "state" / "anchored.db"
    for db_path, marker in ((current_db, "pinned"), (replacement_db, "replacement")):
        init_store(db_path)
        with store_sqlite._connect(db_path, prepare=True) as conn:
            conn.execute("CREATE TABLE anchor_marker (value TEXT NOT NULL)")
            conn.execute("INSERT INTO anchor_marker (value) VALUES (?)", (marker,))
        for suffix in ("-wal", "-shm", "-journal"):
            member = Path(f"{db_path}{suffix}")
            if member.exists():
                member.unlink()

    displaced_root = tmp_path / "displaced"
    original_canonical_path = store_sqlite.canonical_path_from_fd
    substituted = False

    def substitute_ancestor(parent_fd: int, leaf: str) -> str:
        nonlocal substituted
        if not substituted:
            current_root.rename(displaced_root)
            replacement_root.rename(current_root)
            substituted = True
        return original_canonical_path(parent_fd, leaf)

    monkeypatch.setattr(store_sqlite, "canonical_path_from_fd", substitute_ancestor)

    with store_sqlite._connect(current_db, prepare=prepare) as conn:
        assert conn.execute("SELECT value FROM anchor_marker").fetchone()[0] == "pinned"
        conn.execute("CREATE TABLE anchored_write (value TEXT NOT NULL)")
        conn.execute("INSERT INTO anchored_write (value) VALUES ('retained-fd')")
        conn.commit()
        pinned_db = displaced_root / "state" / "anchored.db"
        assert Path(f"{pinned_db}-wal").is_file()
        assert Path(f"{pinned_db}-shm").is_file()
        assert not Path(f"{current_db}-wal").exists()
        assert not Path(f"{current_db}-shm").exists()

    pinned_db = displaced_root / "state" / "anchored.db"
    with sqlite3.connect(str(pinned_db)) as conn:
        assert conn.execute("SELECT value FROM anchored_write").fetchone()[0] == "retained-fd"
    with sqlite3.connect(str(current_db)) as conn:
        assert conn.execute("SELECT value FROM anchor_marker").fetchone()[0] == "replacement"
        assert (
            conn.execute(
                "SELECT COUNT(*) FROM sqlite_master WHERE name = 'anchored_write'"
            ).fetchone()[0]
            == 0
        )


def test_memory_store_does_not_create_filesystem_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    def reject_filesystem_resolution(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError("memory databases must not resolve filesystem paths")

    monkeypatch.setattr(store_sqlite, "open_resolved_parent", reject_filesystem_resolution)
    monkeypatch.setattr(
        store_sqlite, "prepare_resolved_private_sqlite_parent", reject_filesystem_resolution
    )
    monkeypatch.setattr(
        store_sqlite, "canonical_path_from_fd", reject_filesystem_resolution
    )

    init_store(Path(":memory:"))

    assert not (tmp_path / ":memory:").exists()


def test_shared_named_memory_uri_uses_sqlite_uri_without_creating_a_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    def reject_filesystem_resolution(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError("memory URI must not resolve filesystem paths")

    monkeypatch.setattr(store_sqlite, "open_resolved_parent", reject_filesystem_resolution)
    monkeypatch.setattr(
        store_sqlite, "prepare_resolved_private_sqlite_parent", reject_filesystem_resolution
    )
    monkeypatch.setattr(
        store_sqlite, "canonical_path_from_fd", reject_filesystem_resolution
    )
    db_uri = "file:tendwire-shared?mode=memory&cache=shared"

    with store_sqlite._connect(db_uri) as writer:
        writer.execute("CREATE TABLE shared_values (value TEXT NOT NULL)")
        writer.execute("INSERT INTO shared_values (value) VALUES ('visible')")
        writer.commit()
        with store_sqlite._connect(db_uri) as reader:
            value = reader.execute("SELECT value FROM shared_values").fetchone()[0]

    assert value == "visible"
    assert list(tmp_path.iterdir()) == []


def test_similar_memory_text_remains_a_secure_filesystem_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    db_path = Path("file:ordinary?mode=memory-copy")
    previous_umask = os.umask(0)
    try:
        init_store(db_path)
    finally:
        os.umask(previous_umask)

    assert db_path.is_file()
    assert _mode(db_path) == 0o600


def test_connect_closes_when_live_sqlite_family_validation_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "tendwire.db"
    init_store(db_path)
    before = set(os.listdir("/proc/self/fd"))
    original_snapshot = store_sqlite._snapshot_sqlite_family_at
    inspection_count = 0

    def broaden_live_wal(
        parent_fd: int,
        leaf: str,
        *,
        require_main: bool,
    ) -> Any:
        nonlocal inspection_count
        inspection_count += 1
        if inspection_count == 3:
            os.chmod(f"{leaf}-wal", 0o644, dir_fd=parent_fd)
        return original_snapshot(
            parent_fd,
            leaf,
            require_main=require_main,
        )

    created: list[sqlite3.Connection] = []
    original_connect = sqlite3.connect

    def capture_connection(*args: Any, **kwargs: Any) -> sqlite3.Connection:
        conn = original_connect(*args, **kwargs)
        created.append(conn)
        return conn

    monkeypatch.setattr(
        store_sqlite,
        "_snapshot_sqlite_family_at",
        broaden_live_wal,
    )
    monkeypatch.setattr(store_sqlite.sqlite3, "connect", capture_connection)

    with pytest.raises(LocalStateError) as caught:
        store_sqlite._connect(db_path)

    assert caught.value.code is LocalStateErrorCode.INSECURE_MODE
    assert len(created) == 1
    with pytest.raises(sqlite3.ProgrammingError):
        created[0].execute("SELECT 1")
    assert set(os.listdir("/proc/self/fd")) == before


@pytest.mark.parametrize("change", ["disappear", "substitute"])
def test_connect_expected_main_identity_fails_before_open_and_restores_resources(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    change: str,
) -> None:
    db_path = tmp_path / "expected-main.db"
    displaced = tmp_path / "selected-main.db"
    init_store(db_path)
    selected = db_path.stat()
    expected = EntryIdentity(int(selected.st_dev), int(selected.st_ino))
    db_path.rename(displaced)
    replacement_bytes = b"untrusted replacement must not be opened"
    if change == "substitute":
        db_path.write_bytes(replacement_bytes)
        db_path.chmod(0o600)

    before_fds = set(os.listdir("/proc/self/fd"))
    before_threads = {id(thread) for thread in threading.enumerate()}
    before_children = {process.pid for process in multiprocessing.active_children()}
    opened: list[sqlite3.Connection] = []
    original_connect = sqlite3.connect

    def capture_connection(*args: Any, **kwargs: Any) -> sqlite3.Connection:
        conn = original_connect(*args, **kwargs)
        opened.append(conn)
        return conn

    monkeypatch.setattr(store_sqlite.sqlite3, "connect", capture_connection)

    with pytest.raises(LocalStateError) as caught:
        store_sqlite._connect(
            db_path,
            read_only=True,
            _expected_db_identity=expected,
        )

    assert caught.value.code is LocalStateErrorCode.ENTRY_CHANGED
    assert opened == []
    assert (displaced.stat().st_dev, displaced.stat().st_ino) == (
        selected.st_dev,
        selected.st_ino,
    )
    if change == "substitute":
        assert db_path.read_bytes() == replacement_bytes
    else:
        assert not db_path.exists()
    assert set(os.listdir("/proc/self/fd")) == before_fds
    assert {id(thread) for thread in threading.enumerate()} == before_threads
    assert {process.pid for process in multiprocessing.active_children()} == before_children


def test_store_initializes_v8_schema_with_companion_attention_lifecycle(tmp_path: Path) -> None:
    db_path = tmp_path / "tendwire.db"

    init_store(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        assert _PR6_TABLES <= _table_names(conn)
        columns = {row[1] for row in conn.execute("PRAGMA table_info(snapshots)")}
        assert _user_version(conn) == store_sqlite.STORE_SCHEMA_VERSION
        assert {"host_id", "created_at", "payload", "content_fingerprint"} <= columns
        snapshot_indexes = {
            str(row[1])
            for row in conn.execute("PRAGMA index_list(snapshots)").fetchall()
        }
        assert {
            "idx_snapshots_host_newest",
            "idx_snapshots_created_host_id",
        } <= snapshot_indexes
        assert {
            "idx_snapshots_host_id",
            "idx_snapshots_created_at",
            "idx_snapshots_content_fingerprint",
            "idx_snapshots_host_created_id",
        }.isdisjoint(snapshot_indexes)
        binding_columns = {row[1] for row in conn.execute("PRAGMA table_info(worker_bindings)")}
        assert {
            "host_id",
            "worker_id",
            "worker_fingerprint",
            "backend",
            "target_kind",
            "target_value",
            "turn_target_kind",
            "turn_target_value",
            "sendable",
            "reason",
            "observed_at",
            "expires_at",
            "private_fingerprint",
        } <= binding_columns
        binding_indexed = _indexed_columns(conn, "worker_bindings")
        assert {
            "worker_id",
            "worker_fingerprint",
            "private_fingerprint",
            "target_kind",
            "target_value",
            "expires_at",
        } <= binding_indexed
        command_columns = {
            row[1] for row in conn.execute("PRAGMA table_info(commands)")
        }
        assert {
            "host_id",
            "request_id",
            "action",
            "canonical_version",
            "canonical_fingerprint",
            "public_worker_id",
            "state",
            "status",
            "request_json",
            "result_json",
            "reserved_at",
            "send_started_at",
            "terminal_at",
            "updated_at",
            "legacy_collision",
            "legacy_collision_count",
        } == command_columns - {"id", "created_at"}
        receipt_columns = {
            row[1] for row in conn.execute("PRAGMA table_info(command_receipts)")
        }
        assert {
            "canonical_version",
            "canonical_fingerprint",
            "canonical_request_json",
            "public_worker_id",
            "state",
            "owner_token_hash",
            "owner_expires_at",
            "binding_fingerprint",
            "reserved_at",
            "send_started_at",
            "terminal_at",
            "updated_at",
        } <= receipt_columns
        assert conn.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        attention_columns = {row[1] for row in conn.execute("PRAGMA table_info(attention_items)")}
        assert {
            "attention_id",
            "fingerprint",
            "first_seen_at",
            "last_seen_at",
            "last_changed_at",
            "resolved_at",
            "lifecycle_status",
            "resolved_reason",
            "signal_count",
        } <= attention_columns
        attention_indexed = _indexed_columns(conn, "attention_items")
        assert {"lifecycle_status", "last_seen_at", "fingerprint"} <= attention_indexed
        lifecycle_columns = {
            row[1]
            for row in conn.execute("PRAGMA table_info(attention_lifecycles)")
        }
        assert lifecycle_columns == {
            "host_id",
            "family_key",
            "generation",
            "lifecycle_status",
            "current_attention_id",
            "first_seen_at",
            "last_positive_at",
            "first_missing_at",
            "missing_observation_count",
            "last_accepted_at",
            "last_observation_key",
            "max_notified_severity_rank",
        }
        assert {"lifecycle_status", "current_attention_id"} <= _indexed_columns(
            conn, "attention_lifecycles"
        )
        assert {
            row[1]
            for row in conn.execute("PRAGMA table_info(turn_content_revisions)")
        } == {
            "host_id",
            "turn_id",
            "content_revision",
            "user_text",
            "assistant_final_text",
            "user_state",
            "final_state",
            "user_char_length",
            "user_byte_length",
            "final_char_length",
            "final_byte_length",
            "user_page_count",
            "final_page_count",
            "is_current",
            "created_at",
            "superseded_at",
        }
        revision_indexes = {
            row[1]
            for row in conn.execute("PRAGMA index_list(turn_content_revisions)")
        }
        assert {"ux_turn_content_current", "idx_turn_content_cleanup"} <= revision_indexes
        assert {
            row[1]
            for row in conn.execute(
                "PRAGMA table_info(turn_content_page_boundaries)"
            )
        } == {
            "host_id",
            "turn_id",
            "content_revision",
            "field",
            "page_index",
            "start_char",
            "start_byte",
        }
        assert {
            row[1]
            for row in conn.execute("PRAGMA table_info(turn_presentation_plans)")
        } == {
            "id",
            "host_id",
            "name",
            "plan_token",
            "turn_id",
            "content_revision",
            "presentation_version",
            "generation",
            "part_count",
            "state",
            "replaces_plan_token",
            "recovers_plan_token",
            "created_at",
            "activated_at",
            "completed_at",
            "source_outbox_id",
        }
        assert {
            row[1]
            for row in conn.execute("PRAGMA table_info(turn_presentation_jobs)")
        } == {
            "id",
            "plan_id",
            "sequence_index",
            "operation",
            "part_ordinal",
            "spans_json",
            "outbox_id",
            "created_at",
        }
        job_indexes = {
            row[1]
            for row in conn.execute("PRAGMA index_list(turn_presentation_jobs)")
        }
        assert {
            "idx_turn_presentation_jobs_plan_sequence",
            "idx_turn_presentation_jobs_outbox",
        } <= job_indexes
        assert {
            row[1]
            for row in conn.execute(
                "PRAGMA table_info(turn_presentation_recoveries)"
            )
        } == {
            "id",
            "host_id",
            "name",
            "request_id",
            "failed_plan_id",
            "recovered_plan_id",
            "failed_plan_token",
            "recovered_plan_token",
            "generation",
            "source_job_count",
            "delivered_prefix_count",
            "fresh_job_count",
            "retained_failed_job_count",
            "prior_attempt_count",
            "outcome",
            "created_at",
        }


def test_store_connections_apply_wal_busy_timeout_and_foreign_keys(tmp_path: Path) -> None:
    db_path = tmp_path / "pragmas.db"

    init_store(db_path)

    with store_sqlite._connect(db_path) as conn:
        assert conn.execute("PRAGMA journal_mode").fetchone()[0].lower() == "wal"
        assert conn.execute("PRAGMA busy_timeout").fetchone()[0] == 30000
        assert conn.execute("PRAGMA foreign_keys").fetchone()[0] == 1
        assert conn.execute("PRAGMA synchronous").fetchone()[0] == 1


def test_store_command_receipts_have_unique_logical_key_index(tmp_path: Path) -> None:
    db_path = tmp_path / "receipts.db"

    init_store(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        receipt_indexes = _unique_index_columns(conn, "command_receipts")
        command_indexes = _unique_index_columns(conn, "commands")
        receipt_index_names = {
            row[1] for row in conn.execute("PRAGMA index_list(command_receipts)")
        }
        command_index_names = {
            row[1] for row in conn.execute("PRAGMA index_list(commands)")
        }

    assert receipt_indexes["ux_command_receipts_host_request"] == (
        "host_id",
        "request_id",
    )
    assert command_indexes["ux_commands_host_request"] == (
        "host_id",
        "request_id",
    )
    assert "idx_command_receipts_host_state_terminal" in receipt_index_names
    assert "idx_commands_host_state_updated" in command_index_names
    assert "ux_command_receipts_host_request_action" not in receipt_index_names
    assert "ux_commands_host_request_action" not in command_index_names


def test_store_status_tail_and_retention_cleanup_are_host_scoped_and_bounded(tmp_path: Path) -> None:
    db_path = tmp_path / "maintenance.db"
    config = Config(host_id="storehost", db_path=db_path)
    snapshot = project_from_raw(config, workers=[{"id": "worker-1", "name": "Worker One"}])
    save_snapshot(db_path, snapshot)
    append_event(
        db_path,
        "storehost",
        "private.event",
        {"pane_id": "sentinel-private-pane", "raw_payload": "sentinel-private-raw"},
        observed_at="2026-01-01T00:00:00+00:00",
    )
    append_event(
        db_path,
        "storehost",
        "public.event",
        {"safe": "kept"},
        observed_at="2026-01-09T00:00:00+00:00",
    )
    append_event(
        db_path,
        "otherhost",
        "other.event",
        {"safe": "kept"},
        observed_at="2026-01-01T00:00:00+00:00",
    )
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            """
            INSERT INTO connector_outbox (
                host_id, connector, delivery_key, status, payload_json,
                private_state_json, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "storehost",
                "attention",
                "job-1",
                "queued",
                '{"safe":"kept"}',
                '{"token":"sentinel-private-token"}',
                "2026-01-01T00:00:00+00:00",
                "2026-01-01T00:00:00+00:00",
            ),
        )

    before = store_status(db_path, "storehost")
    tail = tail_event_metadata(db_path, "storehost", limit=2)
    dry_run = cleanup_event_retention(
        db_path,
        "storehost",
        retention_days=7,
        now="2026-01-10T00:00:00+00:00",
        dry_run=True,
    )
    after_dry_run_count = store_status(db_path, "storehost")["counts"]["events"]
    cleanup = cleanup_event_retention(
        db_path,
        "storehost",
        retention_days=7,
        now="2026-01-10T00:00:00+00:00",
    )

    with sqlite3.connect(str(db_path)) as conn:
        host_events = conn.execute("SELECT COUNT(*) FROM events WHERE host_id = ?", ("storehost",)).fetchone()[0]
        other_events = conn.execute("SELECT COUNT(*) FROM events WHERE host_id = ?", ("otherhost",)).fetchone()[0]
        snapshots = conn.execute("SELECT COUNT(*) FROM snapshots WHERE host_id = ?", ("storehost",)).fetchone()[0]
        outbox_rows = conn.execute("SELECT COUNT(*) FROM connector_outbox WHERE host_id = ?", ("storehost",)).fetchone()[0]

    assert before["ok"] is True
    assert before["outbox"]["pending"] == 1
    assert len(tail["events"]) == 2
    assert "payload_json" not in json.dumps(tail)
    assert "sentinel-private" not in json.dumps(tail)
    assert dry_run["deleted"] == 1
    assert after_dry_run_count == before["counts"]["events"]
    assert cleanup["deleted"] == 1
    assert host_events == before["counts"]["events"] - 1
    assert other_events == 1
    assert snapshots == 1
    assert outbox_rows == 1


def test_store_operational_metadata_buckets_unsafe_labels(tmp_path: Path) -> None:
    db_path = tmp_path / "unsafe-metadata.db"
    init_store(db_path)
    append_event(
        db_path,
        "storehost",
        "telegram.delivery",
        {"safe": "kept"},
        aggregate_type="raw_payload",
        observed_at="2026-01-01T00:00:00+00:00",
    )
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            """
            INSERT INTO connector_outbox (
                host_id, connector, delivery_key, status, payload_json,
                private_state_json, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "storehost",
                "attention",
                "job-unsafe",
                "telegram_delivery",
                '{"safe":"kept"}',
                "{}",
                "2026-01-01T00:00:00+00:00",
                "2026-01-01T00:00:00+00:00",
            ),
        )
        conn.execute(
            """
            INSERT INTO connector_outbox (
                host_id, connector, delivery_key, status, payload_json,
                private_state_json, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "storehost",
                "attention",
                "job-queued",
                "queued",
                '{"safe":"kept"}',
                "{}",
                "2026-01-01T00:00:00+00:00",
                "2026-01-01T00:00:00+00:00",
            ),
        )

    status = store_status(db_path, "storehost")
    tail = tail_event_metadata(db_path, "storehost", limit=10)
    encoded = json.dumps({"status": status, "tail": tail}, sort_keys=True).lower()

    assert status["outbox"]["pending"] == 1
    assert status["outbox"]["by_status"]["queued"] == 1
    assert status["outbox"]["by_status"]["unknown"] == 1
    assert "telegram_delivery" not in status["outbox"]["by_status"]
    assert tail["events"][0]["event_type"] == "unknown"
    assert tail["events"][0]["aggregate_type"] == "unknown"
    assert "telegram" not in encoded
    assert "raw_payload" not in encoded
    assert "delivery" not in encoded


def test_attention_payload_from_store_buckets_unsafe_row_text(tmp_path: Path) -> None:
    db_path = tmp_path / "unsafe-attention.db"
    init_store(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            """
            INSERT INTO attention_items (
                host_id, attention_id, source, kind, severity, status,
                updated_at, fingerprint, snapshot_content_fingerprint, observed_at,
                first_seen_at, last_seen_at, last_changed_at, lifecycle_status,
                resolved_reason, signal_count, payload_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "storehost",
                "attn-unsafe",
                "telegram:chat",
                "herdres_delivery",
                "warning",
                "blocked",
                "2026-01-01T00:00:00+00:00",
                "fp-unsafe",
                "snapshot-fp",
                "2026-01-01T00:00:00+00:00",
                "2026-01-01T00:00:00+00:00",
                "2026-01-01T00:00:00+00:00",
                "2026-01-01T00:00:00+00:00",
                "open",
                "telegram delivery resolved",
                1,
                json.dumps(
                    {
                        "reason": "telegram delivery token",
                        "meta": {"kept": "visible", "unsafe": "herdres route"},
                        "suggested_actions": [
                            {"label": "notify telegram", "tendwire_action": "noop"}
                        ],
                    }
                ),
            ),
        )
        family_key = store_sqlite._attention_family_key(
            "storehost",
            {"source": "telegram:chat", "kind": "herdres_delivery"},
        )
        conn.execute(
            """
            INSERT INTO attention_lifecycles (
                host_id, family_key, generation, lifecycle_status,
                current_attention_id, first_seen_at, last_positive_at,
                first_missing_at, missing_observation_count, last_accepted_at,
                last_observation_key, max_notified_severity_rank
            ) VALUES (?, ?, 1, 'open', ?, ?, ?, NULL, 0, ?, ?, 1)
            """,
            (
                "storehost",
                family_key,
                "attn-unsafe",
                "2026-01-01T00:00:00+00:00",
                "2026-01-01T00:00:00+00:00",
                "2026-01-01T00:00:00+00:00",
                "observation-key",
            ),
        )

    payload = attention_payload_from_store(db_path, "storehost")
    assert payload is not None
    item = payload["attention"][0]
    encoded = json.dumps(payload, sort_keys=True).lower()

    assert item["source"] == "unknown"
    assert item["kind"] == "unknown"
    assert item["reason"] == ""
    assert item["meta"] == {"kept": "visible"}
    assert "resolved_reason" not in item
    assert item["suggested_actions"][0]["label"] == "notify telegram"
    assert "herdres" not in encoded
    assert "delivery" not in encoded
    assert "token" not in encoded
    assert "route" not in encoded


def _provider_shaped_store_secret() -> str:
    return "".join(("s", "k", "-", "sentinel", "-store-secret"))


@pytest.mark.parametrize(
    "unsafe_value",
    [
        _provider_shaped_store_secret(),
        " ".join(("Bearer", _provider_shaped_store_secret())),
        "".join(
            (
                "https://user:",
                "sentinel",
                "-store-password",
                "@example.invalid/private",
            )
        ),
    ],
    ids=("provider-token", "authorization", "credential-url"),
)
def test_store_public_json_boundaries_share_recursive_sanitizer(
    tmp_path: Path,
    unsafe_value: str,
) -> None:
    db_path = tmp_path / "public-boundaries.db"
    safe_markdown = "**Useful update** — [docs](https://example.com/tendwire/guide)"
    config = Config(host_id="public-host", db_path=db_path)
    snapshot = project_from_raw(
        config,
        spaces=[
            {
                "id": "space-public",
                "name": safe_markdown,
                "status": "active",
                "meta": {"note": unsafe_value},
            }
        ],
        workers=[
            {
                "id": "worker-public",
                "name": safe_markdown,
                "status": "blocked",
                "space_id": "space-public",
                "summary": safe_markdown,
                "meta": {"needs_human": True, "note": unsafe_value},
            }
        ],
        backend_health=[
            {
                "name": "generic",
                "status": "healthy",
                "outcome": "healthy_non_empty",
                "observed_at": "2026-01-01T00:00:00+00:00",
                "message": f"{safe_markdown}\ncredential={unsafe_value}",
                "counts": {"workers": 1},
            }
        ],
        timestamp=datetime.fromisoformat("2026-01-01T00:00:00+00:00"),
    )

    _save_observation(db_path, snapshot, "positive", snapshot.updated_at)
    append_event(
        db_path,
        "public-host",
        "private.adapter",
        {
            "note": unsafe_value,
            "pane_id": "private-pane",
            "markdown": safe_markdown,
        },
        aggregate_type="worker",
        aggregate_id="worker-public",
        observed_at="2026-01-01T00:01:00+00:00",
    )
    binding = WorkerBinding(
        host_id="public-host",
        worker_id="worker-public",
        worker_fingerprint=snapshot.workers[0].fingerprint,
        backend="private-backend",
        target_kind="opaque",
        target_value=unsafe_value,
        sendable=True,
        observed_at="2026-01-01T00:01:00+00:00",
        private_fingerprint="private-binding-fingerprint",
    )
    upsert_worker_bindings(db_path, [binding])
    private_result_json = json.dumps(
        {"note": unsafe_value, "pane_id": "private-pane"},
        sort_keys=True,
    )
    reservation = reserve_command_request(
        db_path,
        host_id="public-host",
        request_id="private-receipt",
        action="send_instruction",
        canonical_version=1,
        canonical_fingerprint="private-receipt-fingerprint",
        canonical_request_json='{"action":"send_instruction"}',
        public_worker_id="worker-public",
        pending_result_json='{"status":"pending"}',
        now="2026-01-01T00:01:00+00:00",
    )
    started = mark_command_send_started(
        db_path,
        host_id="public-host",
        request_id="private-receipt",
        canonical_fingerprint="private-receipt-fingerprint",
        owner_token=reservation["owner_token"],
        binding_fingerprint="private-binding-fingerprint",
        now="2026-01-01T00:01:01+00:00",
    )
    finish_command_request(
        db_path,
        host_id="public-host",
        request_id="private-receipt",
        canonical_fingerprint="private-receipt-fingerprint",
        owner_token=started["owner_token"],
        expected_state="send_started",
        terminal_state="accepted",
        status="accepted",
        result_json=private_result_json,
        now="2026-01-01T00:01:02+00:00",
    )

    assert merge_turn_content(
        db_path,
        "public-host",
        "worker-public",
        {
            "assistant_final_text": f"{safe_markdown}\ncredential={unsafe_value}",
            "complete": True,
            "has_open_turn": False,
        },
        observed_at="2026-01-01T00:02:00+00:00",
    ) == 1
    assert store_sqlite.merge_backend_pending(
        db_path,
        "public-host",
        "worker-public",
        {
            "id": f"pending-{'a' * 24}",
            "question": f"{safe_markdown}\ncredential={unsafe_value}",
            "note": unsafe_value,
            "choices": [
                {
                    "id": f"choice-{'b' * 24}",
                    "label": safe_markdown,
                    "note": unsafe_value,
                }
            ],
        },
    )

    reader_columns = (
        ("snapshots", "payload"),
        ("turns", "payload_json"),
        ("attention_items", "payload_json"),
        ("backend_pending", "payload_json"),
        ("connector_outbox", "payload_json"),
    )
    legacy_public_rows: list[tuple[str, str, int, str]] = []
    with sqlite3.connect(str(db_path)) as conn:
        for table, column in reader_columns:
            rows = conn.execute(
                f"SELECT rowid, {column} FROM {table} WHERE host_id = ?",
                ("public-host",),
            ).fetchall()
            assert rows, f"{table}.{column} should be readable"
            for row_id, raw_value in rows:
                legacy_public_rows.append((table, column, int(row_id), str(raw_value)))
                poisoned = json.loads(raw_value)
                poisoned["legacy_note"] = unsafe_value
                conn.execute(
                    f"UPDATE {table} SET {column} = ? WHERE rowid = ?",
                    (json.dumps(poisoned, sort_keys=True), int(row_id)),
                )
        conn.execute(
            """
            UPDATE connector_outbox
            SET private_state_json = ?
            WHERE host_id = ? AND connector = ?
            """,
            (
                json.dumps({"note": unsafe_value}, sort_keys=True),
                "public-host",
                "attention",
            ),
        )

    polled = poll_connector_outbox(
        db_path,
        "public-host",
        "attention",
        now="2026-01-01T00:03:00+00:00",
    )
    assert polled["items"]

    restored = latest_snapshot(db_path, "public-host")
    assert restored is not None
    turns = turns_payload_from_store(db_path, "public-host", snapshot=restored)
    attention = attention_payload_from_store(db_path, "public-host")
    assert attention is not None
    attention_items = list_attention_items(db_path, "public-host")
    backend_pending = store_sqlite.list_backend_pending(db_path, "public-host")
    tail = tail_event_metadata(db_path, "public-host", limit=20)
    public_readers = {
        "snapshot": restored.to_dict(),
        "turns": turns,
        "attention": attention,
        "attention_items": attention_items,
        "backend_pending": backend_pending,
        "tail": tail,
        "outbox": polled,
    }
    for value in public_readers.values():
        assert unsafe_value not in json.dumps(value, ensure_ascii=False, sort_keys=True)

    for boundary in ("snapshot", "turns", "attention", "backend_pending", "outbox"):
        assert safe_markdown in json.dumps(
            public_readers[boundary],
            ensure_ascii=False,
            sort_keys=True,
        )
    assert "payload_json" not in json.dumps(tail, sort_keys=True)
    with sqlite3.connect(str(db_path)) as conn:
        for table, column, row_id, raw_value in legacy_public_rows:
            conn.execute(
                f"UPDATE {table} SET {column} = ? WHERE rowid = ?",
                (raw_value, row_id),
            )

    public_columns = (
        ("snapshots", "payload"),
        ("spaces", "payload_json"),
        ("workers", "payload_json"),
        ("turns", "payload_json"),
        ("pending_interactions", "payload_json"),
        ("attention_items", "payload_json"),
        ("backend_health", "payload_json"),
        ("backend_pending", "payload_json"),
        ("connector_outbox", "payload_json"),
        ("connector_deliveries", "response_json"),
    )
    with sqlite3.connect(str(db_path)) as conn:
        for table, column in public_columns:
            values = [
                str(row[0])
                for row in conn.execute(
                    f"SELECT {column} FROM {table} WHERE host_id = ?",
                    ("public-host",),
                ).fetchall()
            ]
            assert values, f"{table}.{column} should be populated"
            assert all(unsafe_value not in value for value in values)

        event_payload = json.loads(
            conn.execute(
                """
                SELECT payload_json
                FROM events
                WHERE host_id = ? AND event_type = ?
                ORDER BY id DESC
                LIMIT 1
                """,
                ("public-host", "private.adapter"),
            ).fetchone()[0]
        )
        outbox_private = json.loads(
            conn.execute(
                """
                SELECT private_state_json
                FROM connector_outbox
                WHERE host_id = ? AND connector = ?
                ORDER BY id
                LIMIT 1
                """,
                ("public-host", "attention"),
            ).fetchone()[0]
        )

    assert event_payload["note"] == unsafe_value
    assert event_payload["pane_id"] == "private-pane"
    assert outbox_private["note"] == unsafe_value
    assert "lease_token" in outbox_private
    private_bindings = list_worker_bindings(
        db_path,
        "public-host",
        backend="private-backend",
    )
    assert private_bindings[0].target_value == unsafe_value
    receipt = get_command_request(
        db_path,
        "public-host",
        "private-receipt",
    )
    assert receipt is not None
    assert json.loads(receipt["result_json"])["note"] == unsafe_value


def test_store_maintenance_dry_run_and_exhausted_outbox_status(tmp_path: Path) -> None:
    db_path = tmp_path / "outbox-maintenance.db"
    init_store(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            """
            INSERT INTO connector_outbox (
                host_id, connector, delivery_key, status, payload_json,
                private_state_json, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "storehost",
                "attention",
                "job-1",
                "retry",
                '{"safe":"kept"}',
                '{}',
                "2026-01-01T00:00:00+00:00",
                "2026-01-01T00:00:00+00:00",
            ),
        )
        outbox_id = int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])
        conn.execute(
            """
            INSERT INTO connector_deliveries (
                outbox_id, host_id, connector, delivery_key, attempt, status,
                response_json, private_state_json, created_at, delivered_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                outbox_id,
                "storehost",
                "attention",
                "job-1",
                3,
                "failed",
                '{}',
                '{"token":"sentinel-private-token"}',
                "2026-01-01T00:00:00+00:00",
                "2026-01-01T00:01:00+00:00",
            ),
        )

    dry_run = run_store_maintenance(
        db_path,
        "storehost",
        retention_days=7,
        max_outbox_attempts=3,
        dry_run=True,
    )
    with sqlite3.connect(str(db_path)) as conn:
        dry_status = conn.execute("SELECT status FROM connector_outbox").fetchone()[0]
    real = run_store_maintenance(
        db_path,
        "storehost",
        retention_days=7,
        max_outbox_attempts=3,
    )
    with sqlite3.connect(str(db_path)) as conn:
        real_status, private_state = conn.execute(
            "SELECT status, private_state_json FROM connector_outbox"
        ).fetchone()

    assert dry_run["outbox"]["updated"] == 1
    assert dry_status == "retry"
    assert real["outbox"]["updated"] == 1
    assert real_status == "dead_letter"
    assert json.loads(private_state) == {}


def test_exhaust_connector_retries_reclaims_expired_leases_before_dead_letter(tmp_path: Path) -> None:
    db_path = tmp_path / "expired-maintenance.db"
    init_store(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            """
            INSERT INTO connector_outbox (
                host_id, connector, delivery_key, status, payload_json,
                private_state_json, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "storehost",
                "attention",
                "leased-job",
                "queued",
                '{"safe":"kept"}',
                "{}",
                "2026-01-01T00:00:00+00:00",
                "2026-01-01T00:00:00+00:00",
            ),
        )
    first = poll_connector_outbox(
        db_path,
        "storehost",
        "attention",
        lease_seconds=1,
        max_attempts=2,
        now="2026-01-01T00:00:00+00:00",
    )["items"][0]
    fail_connector_delivery(
        db_path,
        host_id="storehost",
        name="attention",
        ref=first["ref"],
        delay_seconds=0,
        max_attempts=2,
        now="2026-01-01T00:00:01+00:00",
    )
    second = poll_connector_outbox(
        db_path,
        "storehost",
        "attention",
        lease_seconds=1,
        max_attempts=2,
        now="2026-01-01T00:00:02+00:00",
    )["items"][0]

    dry_run = exhaust_connector_retries(
        db_path,
        "storehost",
        max_attempts=2,
        now="2026-01-01T00:00:04+00:00",
        dry_run=True,
    )
    with sqlite3.connect(str(db_path)) as conn:
        dry_run_status = conn.execute(
            "SELECT status FROM connector_outbox WHERE delivery_key = ?",
            ("leased-job",),
        ).fetchone()[0]
    result = exhaust_connector_retries(
        db_path,
        "storehost",
        max_attempts=2,
        now="2026-01-01T00:00:04+00:00",
    )

    with sqlite3.connect(str(db_path)) as conn:
        outbox_status = conn.execute(
            "SELECT status FROM connector_outbox WHERE delivery_key = ?",
            ("leased-job",),
        ).fetchone()[0]
        attempt_count, max_attempt = conn.execute(
            """
            SELECT COUNT(*), COALESCE(MAX(attempt), 0)
            FROM connector_deliveries
            WHERE delivery_key = ?
            """,
            ("leased-job",),
        ).fetchone()

    assert first["attempt"] == 1
    assert second["attempt"] == 2
    assert dry_run["updated"] == 1
    assert dry_run_status == "leased"
    assert result["updated"] == 1
    assert outbox_status == "dead_letter"
    assert (attempt_count, max_attempt) == (2, 2)


def test_store_migrates_v1_schema_and_persists_content_fingerprint(tmp_path: Path) -> None:
    db_path = tmp_path / "legacy.db"
    with sqlite3.connect(str(db_path)) as conn:
        conn.executescript(
            """
            CREATE TABLE snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                host_id TEXT NOT NULL,
                created_at TEXT NOT NULL,
                payload TEXT NOT NULL
            );
            PRAGMA user_version = 1;
            """
        )

    init_store(db_path)
    config = Config(host_id="storehost", db_path=db_path)
    snapshot = project_from_raw(
        config,
        workers=[{"id": "worker-1", "name": "Agent One", "status": "blocked"}],
    )
    save_snapshot(db_path, snapshot)

    with sqlite3.connect(str(db_path)) as conn:
        assert _user_version(conn) == store_sqlite.STORE_SCHEMA_VERSION
        row = conn.execute(
            "SELECT host_id, content_fingerprint, payload FROM snapshots ORDER BY id DESC LIMIT 1"
        ).fetchone()

    assert row[0] == "storehost"
    assert row[1] == snapshot.content_fingerprint
    assert json.loads(row[2]) == json.loads(snapshot.to_json())
    restored = latest_snapshot(db_path)
    assert restored is not None
    assert restored.host_id == "storehost"
    assert restored.content_fingerprint == snapshot.content_fingerprint


def test_store_migrates_partial_v3_db_with_legacy_data_idempotently(tmp_path: Path) -> None:
    db_path = tmp_path / "partial-v3.db"
    snapshot = project_empty(Config(host_id="legacy-host", db_path=db_path))
    with sqlite3.connect(str(db_path)) as conn:
        conn.executescript(
            """
            CREATE TABLE snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                host_id TEXT NOT NULL,
                created_at TEXT NOT NULL,
                content_fingerprint TEXT NOT NULL DEFAULT '',
                payload TEXT NOT NULL
            );
            CREATE TABLE command_receipts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                host_id TEXT NOT NULL,
                request_id TEXT NOT NULL,
                action TEXT NOT NULL,
                payload_fingerprint TEXT NOT NULL,
                status TEXT NOT NULL,
                result_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                completed_at TEXT,
                uncertain INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE worker_bindings (
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
            PRAGMA user_version = 3;
            """
        )
        conn.execute(
            """
            INSERT INTO snapshots (host_id, created_at, content_fingerprint, payload)
            VALUES (?, ?, ?, ?)
            """,
            (
                snapshot.host_id,
                snapshot.updated_at,
                snapshot.content_fingerprint,
                snapshot.to_json(),
            ),
        )
        conn.execute(
            """
            INSERT INTO command_receipts (
                host_id, request_id, action, payload_fingerprint, status,
                result_json, created_at, completed_at, uncertain
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "legacy-host",
                "legacy-req",
                "send_instruction",
                "legacy-fp",
                STATUS_ACCEPTED,
                '{"status":"accepted"}',
                "2026-01-01T00:00:00+00:00",
                "2026-01-01T00:00:01+00:00",
                0,
            ),
        )
        conn.execute(
            """
            INSERT INTO worker_bindings (
                host_id, worker_id, worker_fingerprint, backend, target_kind,
                target_value, sendable, reason, observed_at, expires_at,
                private_fingerprint
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "legacy-host",
                "worker-legacy",
                "worker-fp",
                "herdr",
                "agent_id",
                "agent-private",
                1,
                None,
                "2026-01-01T00:00:00+00:00",
                "9999-12-31T23:59:59+00:00",
                "legacy-private",
            ),
        )

    init_store(db_path)
    init_store(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        assert _PR6_TABLES <= _table_names(conn)
        assert _user_version(conn) == store_sqlite.STORE_SCHEMA_VERSION
        assert conn.execute("SELECT COUNT(*) FROM snapshots").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM command_receipts").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM worker_bindings").fetchone()[0] == 1
        command = conn.execute(
            """
            SELECT status, canonical_fingerprint, result_json
            FROM commands
            WHERE host_id = 'legacy-host'
              AND request_id = 'legacy-req'
            """
        ).fetchone()

    assert command == (STATUS_ACCEPTED, "legacy-fp", '{"status":"accepted"}')


def _snapshot_with_worker_status(
    config: Config,
    *,
    status: str | None,
    observed_at: str,
    health_status: str = "healthy",
    outcome: str = "healthy_non_empty",
) -> Any:
    workers = []
    if status is not None:
        workers = [
            {
                "id": "worker-1",
                "name": "Worker One",
                "status": status,
                "meta": {
                    "safe": "kept",
                    "pane_id": "sentinel-private-pane",
                    "terminalId": "sentinel-private-terminal",
                    "backendTarget": "sentinel-private-backend",
                    "authToken": "sentinel-private-token",
                },
            }
        ]
    return project_from_raw(
        config,
        workers=workers,
        backend_health=[
            {
                "name": "herdr",
                "status": health_status,
                "outcome": outcome,
                "observed_at": observed_at,
                "counts": {"workers": len(workers)},
            }
        ],
        timestamp=datetime.fromisoformat(observed_at),
    )


def _connector_outbox_rows(db_path: Path) -> list[tuple[str, str, str]]:
    with sqlite3.connect(str(db_path)) as conn:
        return conn.execute(
            """
            SELECT connector, delivery_key, payload_json
            FROM connector_outbox
            ORDER BY id
            """
        ).fetchall()


def _save_observation(
    db_path: Path,
    snapshot: Any,
    authority: str,
    observed_at: str,
) -> None:
    save_snapshot(
        db_path,
        snapshot,
        observation=SnapshotObservationContext(
            authority=authority,  # type: ignore[arg-type]
            observed_at=observed_at,
        ),
    )


def _lifecycle_rows(db_path: Path) -> list[tuple[Any, ...]]:
    with sqlite3.connect(str(db_path)) as conn:
        return conn.execute(
            """
            SELECT host_id, family_key, generation, lifecycle_status,
                   current_attention_id, first_seen_at, last_positive_at,
                   first_missing_at, missing_observation_count,
                   last_accepted_at, last_observation_key,
                   max_notified_severity_rank
            FROM attention_lifecycles
            ORDER BY host_id, family_key
            """
        ).fetchall()


def test_store_default_context_is_non_authoritative_and_snapshot_fallback_remains(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "attention-snapshot-only.db"
    config = Config(host_id="attention-host", db_path=db_path)
    snapshot = _snapshot_with_worker_status(
        config,
        status="blocked",
        observed_at="2026-01-01T00:00:00+00:00",
    )
    save_snapshot(db_path, snapshot)

    assert _lifecycle_rows(db_path) == []
    assert list_attention_items(db_path, "attention-host") == []
    payload = attention_payload_from_store(db_path, "attention-host")
    assert payload is not None
    assert payload["attention"][0]["id"] == snapshot.attention[0].id


def test_store_late_first_miss_then_positive_stays_generation_one(tmp_path: Path) -> None:
    db_path = tmp_path / "late-first-miss.db"
    config = Config(host_id="attention-host", db_path=db_path)
    _save_observation(
        db_path,
        _snapshot_with_worker_status(
            config, status="blocked", observed_at="2026-01-01T00:00:00+00:00"
        ),
        "complete",
        "2026-01-01T00:00:00+00:00",
    )
    _save_observation(
        db_path,
        _snapshot_with_worker_status(
            config, status=None, observed_at="2026-01-01T01:00:00+00:00"
        ),
        "complete",
        "2026-01-01T01:00:00+00:00",
    )
    assert _lifecycle_rows(db_path)[0][7:9] == (
        "2026-01-01T01:00:00+00:00",
        1,
    )
    _save_observation(
        db_path,
        _snapshot_with_worker_status(
            config, status="blocked", observed_at="2026-01-01T01:01:00+00:00"
        ),
        "positive",
        "2026-01-01T01:01:00+00:00",
    )

    lifecycle = _lifecycle_rows(db_path)[0]
    assert lifecycle[2:4] == (1, "open")
    assert lifecycle[7:9] == (None, 0)
    assert len(_connector_outbox_rows(db_path)) == 1


def test_store_resolution_requires_distinct_misses_and_120_seconds(tmp_path: Path) -> None:
    db_path = tmp_path / "miss-threshold.db"
    config = Config(host_id="attention-host", db_path=db_path)
    present = _snapshot_with_worker_status(
        config, status="blocked", observed_at="2026-01-01T00:00:00+00:00"
    )
    _save_observation(db_path, present, "complete", present.updated_at)
    miss_10 = _snapshot_with_worker_status(
        config, status=None, observed_at="2026-01-01T00:00:10+00:00"
    )
    _save_observation(db_path, miss_10, "complete", miss_10.updated_at)
    _save_observation(db_path, miss_10, "complete", miss_10.updated_at)
    assert _lifecycle_rows(db_path)[0][8] == 1
    miss_129 = _snapshot_with_worker_status(
        config, status=None, observed_at="2026-01-01T00:02:09+00:00"
    )
    _save_observation(db_path, miss_129, "complete", miss_129.updated_at)
    assert _lifecycle_rows(db_path)[0][3:4] == ("open",)
    assert _lifecycle_rows(db_path)[0][8] == 2
    miss_130 = _snapshot_with_worker_status(
        config, status=None, observed_at="2026-01-01T00:02:10+00:00"
    )
    _save_observation(db_path, miss_130, "complete", miss_130.updated_at)

    lifecycle = _lifecycle_rows(db_path)[0]
    assert lifecycle[3:5] == ("resolved", None)
    assert list_attention_items(db_path, "attention-host") == []
    audit = list_attention_items(
        db_path, "attention-host", include_resolved=True
    )
    assert audit[0]["resolved_reason"] == "gone"


def test_store_restart_positive_resets_pending_and_old_clock_cannot_resolve(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "restart-pending.db"
    config = Config(host_id="attention-host", db_path=db_path)
    for status, at, authority in (
        ("blocked", "2026-01-01T00:00:00+00:00", "complete"),
        (None, "2026-01-01T00:00:10+00:00", "complete"),
    ):
        snapshot = _snapshot_with_worker_status(config, status=status, observed_at=at)
        _save_observation(db_path, snapshot, authority, at)
    init_store(db_path)
    positive = _snapshot_with_worker_status(
        config, status="blocked", observed_at="2026-01-01T00:01:00+00:00"
    )
    _save_observation(db_path, positive, "positive", positive.updated_at)
    init_store(db_path)
    later_miss = _snapshot_with_worker_status(
        config, status=None, observed_at="2026-01-01T00:03:00+00:00"
    )
    _save_observation(db_path, later_miss, "complete", later_miss.updated_at)

    lifecycle = _lifecycle_rows(db_path)[0]
    assert lifecycle[2:4] == (1, "open")
    assert lifecycle[7:9] == ("2026-01-01T00:03:00+00:00", 1)


@pytest.mark.parametrize(
    "outcome",
    ["degraded", "unavailable", "malformed", "unknown", "partial"],
)
def test_store_none_authority_is_lifecycle_byte_equivalent(
    tmp_path: Path,
    outcome: str,
) -> None:
    db_path = tmp_path / f"none-{outcome}.db"
    config = Config(host_id="attention-host", db_path=db_path)
    present = _snapshot_with_worker_status(
        config, status="blocked", observed_at="2026-01-01T00:00:00+00:00"
    )
    _save_observation(db_path, present, "complete", present.updated_at)
    before_lifecycle = _lifecycle_rows(db_path)
    before_attention = list_attention_items(
        db_path, "attention-host", include_resolved=True
    )
    before_outbox = _connector_outbox_rows(db_path)
    degraded = _snapshot_with_worker_status(
        config,
        status=None,
        observed_at="2026-01-01T00:05:00+00:00",
        health_status="degraded",
        outcome=outcome,
    )
    _save_observation(db_path, degraded, "none", degraded.updated_at)

    assert _lifecycle_rows(db_path) == before_lifecycle
    assert list_attention_items(
        db_path, "attention-host", include_resolved=True
    ) == before_attention
    assert _connector_outbox_rows(db_path) == before_outbox
    with sqlite3.connect(str(db_path)) as conn:
        assert conn.execute("SELECT COUNT(*) FROM snapshots").fetchone()[0] == 2


def test_store_incremental_positive_can_escalate_and_clear_but_omission_cannot_miss(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "positive-authority.db"
    config = Config(host_id="attention-host", db_path=db_path)
    blocked = _snapshot_with_worker_status(
        config, status="blocked", observed_at="2026-01-01T00:00:00+00:00"
    )
    _save_observation(db_path, blocked, "positive", blocked.updated_at)
    omitted = _snapshot_with_worker_status(
        config, status=None, observed_at="2026-01-01T00:01:00+00:00"
    )
    _save_observation(db_path, omitted, "positive", omitted.updated_at)
    assert _lifecycle_rows(db_path)[0][7:9] == (None, 0)
    first_miss = _snapshot_with_worker_status(
        config, status=None, observed_at="2026-01-01T00:02:00+00:00"
    )
    _save_observation(db_path, first_miss, "complete", first_miss.updated_at)
    failed = _snapshot_with_worker_status(
        config, status="failed", observed_at="2026-01-01T00:03:00+00:00"
    )
    _save_observation(db_path, failed, "positive", failed.updated_at)

    assert _lifecycle_rows(db_path)[0][7:9] == (None, 0)
    assert [json.loads(row[2])["event_type"] for row in _connector_outbox_rows(db_path)] == [
        "attention_created",
        "attention_escalated",
    ]


def test_store_order_guards_reject_stale_duplicate_and_equal_time_conflict(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "ordering.db"
    config = Config(host_id="attention-host", db_path=db_path)
    first = _snapshot_with_worker_status(
        config, status="blocked", observed_at="2026-01-01T00:10:00+00:00"
    )
    _save_observation(db_path, first, "complete", first.updated_at)
    baseline = _lifecycle_rows(db_path)
    stale = _snapshot_with_worker_status(
        config, status="failed", observed_at="2026-01-01T00:09:00+00:00"
    )
    _save_observation(db_path, stale, "positive", stale.updated_at)
    _save_observation(db_path, first, "complete", first.updated_at)
    conflict = _snapshot_with_worker_status(
        config, status="failed", observed_at=first.updated_at
    )
    _save_observation(db_path, conflict, "positive", conflict.updated_at)
    init_store(db_path)

    assert _lifecycle_rows(db_path) == baseline
    assert len(_connector_outbox_rows(db_path)) == 1
    newer = _snapshot_with_worker_status(
        config, status="failed", observed_at="2026-01-01T00:11:00+00:00"
    )
    _save_observation(db_path, newer, "positive", newer.updated_at)
    assert _lifecycle_rows(db_path)[0][9] == newer.updated_at


def test_store_escalation_downgrade_one_generation_one_current_pointer(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "escalation.db"
    config = Config(host_id="attention-host", db_path=db_path)
    for status, at in (
        ("blocked", "2026-01-01T00:00:00+00:00"),
        ("failed", "2026-01-01T00:01:00+00:00"),
        ("failed", "2026-01-01T00:02:00+00:00"),
        ("blocked", "2026-01-01T00:03:00+00:00"),
    ):
        snapshot = _snapshot_with_worker_status(config, status=status, observed_at=at)
        _save_observation(db_path, snapshot, "positive", at)

    assert _lifecycle_rows(db_path)[0][2:4] == (1, "open")
    current = list_attention_items(db_path, "attention-host")
    assert len(current) == 1
    assert current[0]["status"] == "blocked"
    audit = list_attention_items(
        db_path, "attention-host", include_resolved=True
    )
    assert len(audit) == 2
    assert {row.get("resolved_reason") for row in audit if row["lifecycle_status"] == "resolved"} == {
        "superseded"
    }
    events = [json.loads(row[2])["event_type"] for row in _connector_outbox_rows(db_path)]
    assert events == ["attention_created", "attention_escalated"]


def test_store_confirmed_resolution_recurrence_increments_generation_once(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "recurrence.db"
    config = Config(host_id="attention-host", db_path=db_path)
    sequence = (
        ("blocked", "2026-01-01T00:00:00+00:00"),
        (None, "2026-01-01T00:01:00+00:00"),
        (None, "2026-01-01T00:03:00+00:00"),
        ("blocked", "2026-01-01T00:04:00+00:00"),
        ("blocked", "2026-01-01T00:05:00+00:00"),
    )
    for status, at in sequence:
        snapshot = _snapshot_with_worker_status(config, status=status, observed_at=at)
        _save_observation(
            db_path,
            snapshot,
            "complete" if status is None else "positive",
            at,
        )

    assert _lifecycle_rows(db_path)[0][2:4] == (2, "open")
    initial_keys = [
        row[1]
        for row in _connector_outbox_rows(db_path)
        if "attention_created" in row[1]
    ]
    assert len(initial_keys) == len(set(initial_keys)) == 2


def test_store_snapshot_lifecycle_outbox_transaction_rolls_back_and_retry_succeeds(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "atomic.db"
    init_store(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            """
            CREATE TRIGGER abort_attention_outbox
            BEFORE INSERT ON connector_outbox
            WHEN NEW.connector = 'attention'
            BEGIN
                SELECT RAISE(ABORT, 'outbox rejected');
            END
            """
        )
    config = Config(host_id="attention-host", db_path=db_path)
    snapshot = _snapshot_with_worker_status(
        config, status="blocked", observed_at="2026-01-01T00:00:00+00:00"
    )
    with pytest.raises(sqlite3.IntegrityError, match="outbox rejected"):
        _save_observation(db_path, snapshot, "complete", snapshot.updated_at)
    with sqlite3.connect(str(db_path)) as conn:
        assert conn.execute("SELECT COUNT(*) FROM snapshots").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM attention_lifecycles").fetchone()[0] == 0
        conn.execute("DROP TRIGGER abort_attention_outbox")
    _save_observation(db_path, snapshot, "complete", snapshot.updated_at)
    assert len(_lifecycle_rows(db_path)) == 1
    assert len(_connector_outbox_rows(db_path)) == 1


def test_store_concurrent_identical_saves_produce_one_transition(tmp_path: Path) -> None:
    db_path = tmp_path / "concurrent-attention.db"
    init_store(db_path)
    config = Config(host_id="attention-host", db_path=db_path)
    snapshot = _snapshot_with_worker_status(
        config, status="blocked", observed_at="2026-01-01T00:00:00+00:00"
    )
    errors: list[BaseException] = []

    def save() -> None:
        try:
            _save_observation(db_path, snapshot, "complete", snapshot.updated_at)
        except BaseException as exc:  # pragma: no cover - asserted below
            errors.append(exc)

    threads = [threading.Thread(target=save) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert errors == []
    assert len(_lifecycle_rows(db_path)) == 1
    assert len(_connector_outbox_rows(db_path)) == 1


def test_store_family_isolation_across_hosts_workers_and_kinds(tmp_path: Path) -> None:
    db_path = tmp_path / "family-isolation.db"
    config_a = Config(host_id="host-a", db_path=db_path)
    config_b = Config(host_id="host-b", db_path=db_path)
    at = "2026-01-01T00:00:00+00:00"
    for config in (config_a, config_b):
        projected = project_from_raw(
            config,
            workers=[
                {"id": "worker-1", "name": "One", "status": "blocked"},
                {"id": "worker-2", "name": "Two", "status": "blocked"},
            ],
            timestamp=datetime.fromisoformat(at),
        )
        if config.host_id == "host-a":
            data = projected.to_dict()
            other_kind = dict(data["attention"][0])
            other_kind.update(
                {
                    "id": "attention-other-kind",
                    "fingerprint": "fingerprint-other-kind",
                    "kind": "other_condition",
                }
            )
            data["attention"].append(other_kind)
            projected = store_sqlite.Snapshot.from_dict(data)
        _save_observation(db_path, projected, "complete", at)

    rows = _lifecycle_rows(db_path)
    assert len(rows) == 5
    assert len({(row[0], row[1]) for row in rows}) == 5
    assert sum(row[0] == "host-a" for row in rows) == 3


def test_store_same_family_variant_selection_is_order_independent(tmp_path: Path) -> None:
    selected_ids: list[str] = []
    for reverse in (False, True):
        db_path = tmp_path / f"variants-{reverse}.db"
        config = Config(host_id="attention-host", db_path=db_path)
        snapshot = _snapshot_with_worker_status(
            config, status="blocked", observed_at="2026-01-01T00:00:00+00:00"
        )
        data = snapshot.to_dict()
        critical = dict(data["attention"][0])
        critical.update(
            {
                "id": "critical-variant",
                "fingerprint": "critical-fingerprint",
                "severity": "critical",
                "status": "failed",
                "updated_at": "2026-01-01T00:00:01+00:00",
            }
        )
        variants = [data["attention"][0], critical]
        data["attention"] = list(reversed(variants)) if reverse else variants
        snapshot = store_sqlite.Snapshot.from_dict(data)
        _save_observation(db_path, snapshot, "complete", snapshot.updated_at)
        current = list_attention_items(db_path, "attention-host")
        assert len(current) == 1
        selected_ids.append(current[0]["id"])
    assert selected_ids == ["critical-variant", "critical-variant"]


@pytest.mark.parametrize("observed_at", ["not-a-time", "2026-01-01T00:01:00"])
def test_store_invalid_or_naive_lifecycle_time_is_noop(
    tmp_path: Path,
    observed_at: str,
) -> None:
    db_path = tmp_path / f"invalid-time-{observed_at.replace(':', '-')}.db"
    config = Config(host_id="attention-host", db_path=db_path)
    first = _snapshot_with_worker_status(
        config, status="blocked", observed_at="2026-01-01T00:00:00+00:00"
    )
    _save_observation(db_path, first, "complete", first.updated_at)
    before = (
        _lifecycle_rows(db_path),
        list_attention_items(db_path, "attention-host", include_resolved=True),
        _connector_outbox_rows(db_path),
    )
    failed = _snapshot_with_worker_status(
        config, status="failed", observed_at="2026-01-01T00:01:00+00:00"
    )
    _save_observation(db_path, failed, "positive", observed_at)
    after = (
        _lifecycle_rows(db_path),
        list_attention_items(db_path, "attention-host", include_resolved=True),
        _connector_outbox_rows(db_path),
    )
    assert after == before


def test_store_strict_order_preserves_subsecond_observations(tmp_path: Path) -> None:
    db_path = tmp_path / "subsecond-order.db"
    config = Config(host_id="attention-host", db_path=db_path)
    for at in (
        "2026-01-01T00:00:00.000001+00:00",
        "2026-01-01T00:00:00.000002+00:00",
    ):
        snapshot = _snapshot_with_worker_status(
            config, status="blocked", observed_at=at
        )
        _save_observation(db_path, snapshot, "positive", at)
    lifecycle = _lifecycle_rows(db_path)[0]
    current = list_attention_items(db_path, "attention-host")[0]
    assert lifecycle[9] == "2026-01-01T00:00:00.000002+00:00"
    assert current["signal_count"] == 2
    assert current["last_seen_at"] == lifecycle[9]


def test_store_deterministic_30_minute_flap_has_one_episode_and_initial(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "flap.db"
    config = Config(host_id="attention-host", db_path=db_path)
    start = datetime.fromisoformat("2026-01-01T00:00:00+00:00")
    for minute in range(31):
        at = (start + store_sqlite.timedelta(minutes=minute)).isoformat()
        status = "blocked" if minute % 2 == 0 else None
        snapshot = _snapshot_with_worker_status(config, status=status, observed_at=at)
        _save_observation(
            db_path,
            snapshot,
            "positive" if status is not None else "complete",
            at,
        )

    lifecycle = _lifecycle_rows(db_path)[0]
    assert lifecycle[2:4] == (1, "open")
    assert lifecycle[7:9] == (None, 0)
    assert len(list_attention_items(db_path, "attention-host")) == 1
    assert len(_connector_outbox_rows(db_path)) == 1


def _reset_store_to_v4(db_path: Path) -> None:
    init_store(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute("DROP TABLE attention_lifecycles")
        conn.execute("PRAGMA user_version = 4")


def _insert_legacy_attention(
    conn: sqlite3.Connection,
    *,
    attention_id: str,
    source: str,
    severity: str,
    status: str,
    lifecycle_status: str,
    at: str,
    first_seen_at: str | None = None,
    signal_count: int = 1,
    last_seen_at: str | None = None,
    last_changed_at: str | None = None,
) -> None:
    payload = {
        "id": attention_id,
        "source": source,
        "kind": "worker_status",
        "severity": severity,
        "status": status,
        "fingerprint": f"fp-{attention_id}",
    }
    # last_seen_at drives the migration's positive_at; last_changed_at drives its
    # change/resolve progress. Allowing them to differ from `at` is what lets a
    # test build a resolved episode whose resolution is newer than its last
    # positive (the ordering the collapsed default hides).
    seen_at = last_seen_at or at
    changed_at = last_changed_at or at
    resolved_at = (changed_at if lifecycle_status != "open" else None)
    conn.execute(
        """
        INSERT INTO attention_items (
            host_id, attention_id, source, kind, severity, status, updated_at,
            fingerprint, snapshot_content_fingerprint, observed_at,
            first_seen_at, last_seen_at, last_changed_at, resolved_at,
            lifecycle_status, resolved_reason, signal_count, payload_json
        ) VALUES (
            'legacy-host', ?, ?, 'worker_status', ?, ?, ?, ?, 'snapshot-fp', ?,
            ?, ?, ?, ?, ?, ?, ?, ?
        )
        """,
        (
            attention_id,
            source,
            severity,
            status,
            at,
            f"fp-{attention_id}",
            at,
            first_seen_at or at,
            seen_at,
            changed_at,
            resolved_at,
            lifecycle_status,
            "gone" if lifecycle_status != "open" else None,
            signal_count,
            json.dumps(payload, sort_keys=True),
        ),
    )


def test_store_v4_collision_migration_is_deterministic_and_preserves_audit(
    tmp_path: Path,
) -> None:
    winners: list[tuple[Any, ...]] = []
    for reverse in (False, True):
        db_path = tmp_path / f"collision-{reverse}.db"
        _reset_store_to_v4(db_path)
        candidates = [
            {
                "attention_id": "attn-blocked",
                "severity": "warning",
                "status": "blocked",
                "lifecycle_status": "open",
                "at": "2026-01-01T00:01:00+00:00",
                "first_seen_at": "2026-01-01T00:00:00+00:00",
                "signal_count": 3,
            },
            {
                "attention_id": "attn-failed",
                "severity": "critical",
                "status": "failed",
                "lifecycle_status": "open",
                "at": "2026-01-01T00:02:00+00:00",
                "first_seen_at": "2026-01-01T00:01:00+00:00",
                "signal_count": 4,
            },
            {
                "attention_id": "attn-resolved",
                "severity": "critical",
                "status": "failed",
                "lifecycle_status": "resolved",
                "at": "2026-01-01T00:03:00+00:00",
                "signal_count": 2,
            },
        ]
        with sqlite3.connect(str(db_path)) as conn:
            for candidate in (
                reversed(candidates) if reverse else candidates
            ):
                _insert_legacy_attention(
                    conn,
                    source="worker:legacy",
                    **candidate,
                )
        init_store(db_path)
        init_store(db_path)
        with sqlite3.connect(str(db_path)) as conn:
            lifecycle = conn.execute(
                """
                SELECT generation, lifecycle_status, current_attention_id,
                       first_seen_at, last_positive_at,
                       max_notified_severity_rank
                FROM attention_lifecycles
                """
            ).fetchone()
            public_rows = conn.execute(
                """
                SELECT attention_id, lifecycle_status, resolved_reason,
                       signal_count
                FROM attention_items ORDER BY attention_id
                """
            ).fetchall()
            assert _user_version(conn) == store_sqlite.STORE_SCHEMA_VERSION
        winners.append((lifecycle, public_rows))

    assert winners[0] == winners[1]
    lifecycle, public_rows = winners[0]
    assert lifecycle == (
        1,
        "resolved",
        None,
        "2026-01-01T00:00:00+00:00",
        "2026-01-01T00:03:00+00:00",
        2,
    )
    assert public_rows == [
        ("attn-blocked", "resolved", "superseded", 3),
        ("attn-failed", "resolved", "superseded", 4),
        ("attn-resolved", "resolved", "gone", 2),
    ]


_MIG_T0 = "2026-01-01T00:00:00+00:00"
_MIG_T5 = "2026-01-01T00:05:00+00:00"
_MIG_T10 = "2026-01-01T00:10:00+00:00"
_MIG_T11 = "2026-01-01T00:11:00+00:00"


def _migrate_resolved_skewed_episode(db_path: Path) -> None:
    """Legacy resolved episode whose resolution (t10) is newer than its last
    positive (t0), migrated to v5."""
    _reset_store_to_v4(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_legacy_attention(
            conn,
            attention_id="attn-legacy",
            source="worker:legacy",
            severity="warning",
            status="blocked",
            lifecycle_status="resolved",
            at=_MIG_T0,
            first_seen_at=_MIG_T0,
            last_seen_at=_MIG_T0,
            last_changed_at=_MIG_T10,
            signal_count=2,
        )
    init_store(db_path)


def _migrated_lifecycle_row(db_path: Path) -> tuple[Any, ...]:
    with sqlite3.connect(str(db_path)) as conn:
        return conn.execute(
            """
            SELECT generation, lifecycle_status, current_attention_id,
                   last_positive_at, last_accepted_at
            FROM attention_lifecycles
            """
        ).fetchone()


def _attention_outbox_job_count(db_path: Path) -> int:
    with sqlite3.connect(str(db_path)) as conn:
        return int(
            conn.execute(
                "SELECT COUNT(*) FROM connector_outbox WHERE connector = 'attention'"
            ).fetchone()[0]
        )


def _legacy_worker_snapshot(status: str, timestamp: str):
    from datetime import datetime

    config = Config(host_id="legacy-host", db_path=Path("unused"))
    return project_from_raw(
        config,
        workers=[{"id": "legacy", "name": "Legacy Worker", "status": status}],
        backend_health=[
            {
                "name": "herdr",
                "status": "healthy",
                "outcome": "healthy_non_empty",
                "observed_at": timestamp,
                "counts": {"workers": 1},
            }
        ],
        timestamp=datetime.fromisoformat(timestamp),
    )


def test_migration_resolved_episode_seeds_accepted_watermark_from_resolution(
    tmp_path: Path,
) -> None:
    """The migrated watermark is the resolution progress (t10), not the last
    positive (t0), and a delayed positive at t5 cannot reopen the lifecycle."""
    db_path = tmp_path / "skewed-resolved.db"
    _migrate_resolved_skewed_episode(db_path)

    assert _migrated_lifecycle_row(db_path) == (
        1,
        "resolved",
        None,
        _MIG_T0,   # last_positive_at = actual latest positive
        _MIG_T10,  # last_accepted_at = newest lifecycle progress (resolution)
    )
    assert _attention_outbox_job_count(db_path) == 0

    # A delayed positive observation timestamped t5 (< the authoritative t10
    # resolution) must be ignored: no reopen, no generation 2, no job.
    save_snapshot(
        db_path,
        _legacy_worker_snapshot("blocked", _MIG_T5),
        observation=SnapshotObservationContext(authority="positive", observed_at=_MIG_T5),
    )
    assert _migrated_lifecycle_row(db_path) == (1, "resolved", None, _MIG_T0, _MIG_T10)
    assert _attention_outbox_job_count(db_path) == 0


def test_migration_resolved_episode_genuine_later_positive_opens_one_generation(
    tmp_path: Path,
) -> None:
    """A genuine positive after the resolution watermark opens generation 2 and
    enqueues exactly one notification."""
    db_path = tmp_path / "genuine-reopen.db"
    _migrate_resolved_skewed_episode(db_path)
    assert _attention_outbox_job_count(db_path) == 0

    save_snapshot(
        db_path,
        _legacy_worker_snapshot("blocked", _MIG_T11),
        observation=SnapshotObservationContext(authority="positive", observed_at=_MIG_T11),
    )
    generation, status, current, _positive, accepted = _migrated_lifecycle_row(db_path)
    assert (generation, status) == (2, "open")
    assert current is not None
    assert accepted == _MIG_T11
    assert _attention_outbox_job_count(db_path) == 1


def test_migration_resolved_episode_delayed_complete_miss_is_inert(
    tmp_path: Path,
) -> None:
    """A delayed 'complete' (missing) observation before the resolution watermark
    cannot mutate the migrated resolved lifecycle."""
    db_path = tmp_path / "delayed-complete.db"
    _migrate_resolved_skewed_episode(db_path)

    save_snapshot(
        db_path,
        _legacy_worker_snapshot("idle", _MIG_T5),
        observation=SnapshotObservationContext(authority="complete", observed_at=_MIG_T5),
    )
    assert _migrated_lifecycle_row(db_path) == (1, "resolved", None, _MIG_T0, _MIG_T10)
    assert _attention_outbox_job_count(db_path) == 0


def _legacy_attention_job_payload(
    *,
    event_type: str = "attention_created",
    severity: str = "warning",
    attention_id: str = "attn-legacy",
    transition_at: str = "2026-01-01T00:00:00+00:00",
) -> str:
    return json.dumps(
        {
            "schema_version": 1,
            "event_type": event_type,
            "host_id": "legacy-host",
            "attention": {
                "id": attention_id,
                "source": "worker:legacy",
                "kind": "worker_status",
                "severity": severity,
                "status": "blocked",
                "fingerprint": "fp-attn-legacy",
            },
            "transition_at": transition_at,
        },
        sort_keys=True,
    )


def test_store_v4_migration_consolidates_duplicate_jobs_and_preserves_terminal_audit(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "migration-jobs.db"
    _reset_store_to_v4(db_path)
    created_payload = _legacy_attention_job_payload()
    escalation_payload = _legacy_attention_job_payload(
        event_type="attention_escalated", severity="critical"
    )
    with sqlite3.connect(str(db_path)) as conn:
        _insert_legacy_attention(
            conn,
            attention_id="attn-legacy",
            source="worker:legacy",
            severity="warning",
            status="blocked",
            lifecycle_status="open",
            at="2026-01-01T00:00:00+00:00",
        )
        outbox_ids: dict[str, int] = {}
        for index, status in enumerate(
            ("queued", "retry", "deferred", "delivered", "dead_letter")
        ):
            cursor = conn.execute(
                """
                INSERT INTO connector_outbox (
                    host_id, connector, delivery_key, status, payload_json,
                    private_state_json, created_at, updated_at, next_attempt_at
                ) VALUES ('legacy-host', 'attention', ?, ?, ?, '{}', ?, ?, NULL)
                """,
                (
                    f"legacy-created-{index}",
                    status,
                    created_payload,
                    "2026-01-01T00:00:00+00:00",
                    "2026-01-01T00:00:00+00:00",
                ),
            )
            outbox_ids[status] = int(cursor.lastrowid)
        conn.execute(
            """
            INSERT INTO connector_deliveries (
                outbox_id, host_id, connector, delivery_key, attempt, status,
                response_json, private_state_json, created_at, delivered_at
            ) VALUES (?, 'legacy-host', 'attention', 'legacy-created-3', 1,
                      'delivered', '{}', '{}', ?, ?)
            """,
            (
                outbox_ids["delivered"],
                "2026-01-01T00:00:00+00:00",
                "2026-01-01T00:00:01+00:00",
            ),
        )
        leased_cursor = conn.execute(
            """
            INSERT INTO connector_outbox (
                host_id, connector, delivery_key, status, payload_json,
                private_state_json, created_at, updated_at, next_attempt_at
            ) VALUES ('legacy-host', 'attention', 'legacy-created-leased',
                      'leased', ?, '{}', ?, ?, NULL)
            """,
            (
                created_payload,
                "2026-01-01T00:00:00+00:00",
                "2026-01-01T00:00:00+00:00",
            ),
        )
        conn.execute(
            """
            INSERT INTO connector_deliveries (
                outbox_id, host_id, connector, delivery_key, attempt, status,
                response_json, private_state_json, created_at, delivered_at
            ) VALUES (?, 'legacy-host', 'attention', 'legacy-created-leased', 1,
                      'leased', '{}', '{}', ?, NULL)
            """,
            (
                int(leased_cursor.lastrowid),
                "2026-01-01T00:00:00+00:00",
            ),
        )
        for index in range(8):
            conn.execute(
                """
                INSERT INTO connector_outbox (
                    host_id, connector, delivery_key, status, payload_json,
                    private_state_json, created_at, updated_at, next_attempt_at
                ) VALUES ('legacy-host', 'attention', ?, 'queued', ?, '{}', ?, ?, NULL)
                """,
                (
                    f"legacy-escalation-{index}",
                    escalation_payload,
                    "2026-01-01T00:01:00+00:00",
                    "2026-01-01T00:01:00+00:00",
                ),
            )

    init_store(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        created_statuses = conn.execute(
            """
            SELECT status, private_state_json FROM connector_outbox
            WHERE delivery_key LIKE 'legacy-created-%'
            ORDER BY id
            """
        ).fetchall()
        escalation_statuses = conn.execute(
            """
            SELECT status, delivery_key FROM connector_outbox
            WHERE json_extract(payload_json, '$.event_type') = 'attention_escalated'
            ORDER BY id
            """
        ).fetchall()
        delivered_count = conn.execute(
            "SELECT COUNT(*) FROM connector_deliveries WHERE status = 'delivered'"
        ).fetchone()[0]
    assert created_statuses == [
        ("superseded", "{}"),
        ("superseded", "{}"),
        ("superseded", "{}"),
        ("delivered", "{}"),
        ("dead_letter", "{}"),
        (
            "leased",
            next(
                private
                for status, private in created_statuses
                if status == "leased"
            ),
        ),
    ]
    leased_state = json.loads(created_statuses[-1][1])
    assert leased_state["migration_canonical"] is False
    assert leased_state["terminal_after_lease"] is True
    assert sum(status == "queued" for status, _ in escalation_statuses) == 1
    assert sum(status == "superseded" for status, _ in escalation_statuses) == 8
    assert delivered_count == 1


def test_store_v4_delivered_old_does_not_suppress_active_current_recurrence(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "migration-delivered-old.db"
    _reset_store_to_v4(db_path)
    old_payload = _legacy_attention_job_payload(
        attention_id="attn-current",
        transition_at="2026-01-01T00:00:00+00:00",
    )
    current_payload = _legacy_attention_job_payload(
        attention_id="attn-current",
        transition_at="2026-01-01T00:10:00+00:00",
    )
    with sqlite3.connect(str(db_path)) as conn:
        _insert_legacy_attention(
            conn,
            attention_id="attn-current",
            source="worker:legacy",
            severity="warning",
            status="blocked",
            lifecycle_status="open",
            at="2026-01-01T00:10:00+00:00",
            first_seen_at="2026-01-01T00:00:00+00:00",
        )
        delivered_ids: list[int] = []
        for key in ("old-delivered-audit", "old-delivered-outbox-only"):
            cursor = conn.execute(
                """
                INSERT INTO connector_outbox (
                    host_id, connector, delivery_key, status, payload_json,
                    private_state_json, created_at, updated_at, next_attempt_at
                ) VALUES ('legacy-host', 'attention', ?, 'delivered', ?, '{}',
                          ?, ?, NULL)
                """,
                (
                    key,
                    old_payload,
                    "2026-01-01T00:00:00+00:00",
                    "2026-01-01T00:00:00+00:00",
                ),
            )
            delivered_ids.append(int(cursor.lastrowid))
        conn.execute(
            """
            INSERT INTO connector_deliveries (
                outbox_id, host_id, connector, delivery_key, attempt, status,
                response_json, private_state_json, created_at, delivered_at
            ) VALUES (?, 'legacy-host', 'attention', 'old-delivered-audit', 1,
                      'delivered', '{}', '{}', ?, ?)
            """,
            (
                delivered_ids[0],
                "2026-01-01T00:00:00+00:00",
                "2026-01-01T00:00:01+00:00",
            ),
        )
        conn.execute(
            """
            INSERT INTO connector_outbox (
                host_id, connector, delivery_key, status, payload_json,
                private_state_json, created_at, updated_at, next_attempt_at
            ) VALUES ('legacy-host', 'attention', 'current-recurrence',
                      'queued', ?, '{}', ?, ?, NULL)
            """,
            (
                current_payload,
                "2026-01-01T00:10:00+00:00",
                "2026-01-01T00:10:00+00:00",
            ),
        )
    init_store(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        rows = conn.execute(
            """
            SELECT delivery_key, status, payload_json
            FROM connector_outbox ORDER BY id
            """
        ).fetchall()
    assert [row[1] for row in rows] == [
        "delivered",
        "delivered",
        "superseded",
        "queued",
    ]
    assert json.loads(rows[-1][2])["attention"]["id"] == "attn-current"
    assert rows[-1][0].startswith("attention:attention_created:")


@pytest.mark.parametrize("with_delivery_audit", [False, True])
def test_store_v4_delivered_current_episode_suppresses_duplicate(
    tmp_path: Path,
    with_delivery_audit: bool,
) -> None:
    db_path = tmp_path / f"migration-delivered-current-{with_delivery_audit}.db"
    _reset_store_to_v4(db_path)
    payload = _legacy_attention_job_payload(
        attention_id="attn-current",
        transition_at="2026-01-01T00:10:00+00:00",
    )
    with sqlite3.connect(str(db_path)) as conn:
        _insert_legacy_attention(
            conn,
            attention_id="attn-current",
            source="worker:legacy",
            severity="warning",
            status="blocked",
            lifecycle_status="open",
            at="2026-01-01T00:10:00+00:00",
        )
        delivered_cursor = conn.execute(
            """
            INSERT INTO connector_outbox (
                host_id, connector, delivery_key, status, payload_json,
                private_state_json, created_at, updated_at, next_attempt_at
            ) VALUES ('legacy-host', 'attention', 'current-delivered',
                      'delivered', ?, '{}', ?, ?, NULL)
            """,
            (
                payload,
                "2026-01-01T00:10:00+00:00",
                "2026-01-01T00:10:00+00:00",
            ),
        )
        if with_delivery_audit:
            conn.execute(
                """
                INSERT INTO connector_deliveries (
                    outbox_id, host_id, connector, delivery_key, attempt,
                    status, response_json, private_state_json, created_at,
                    delivered_at
                ) VALUES (?, 'legacy-host', 'attention', 'current-delivered',
                          1, 'delivered', '{}', '{}', ?, ?)
                """,
                (
                    int(delivered_cursor.lastrowid),
                    "2026-01-01T00:10:00+00:00",
                    "2026-01-01T00:10:01+00:00",
                ),
            )
        conn.execute(
            """
            INSERT INTO connector_outbox (
                host_id, connector, delivery_key, status, payload_json,
                private_state_json, created_at, updated_at, next_attempt_at
            ) VALUES ('legacy-host', 'attention', 'current-duplicate',
                      'queued', ?, '{}', ?, ?, NULL)
            """,
            (
                payload,
                "2026-01-01T00:10:00+00:00",
                "2026-01-01T00:10:00+00:00",
            ),
        )
    init_store(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        statuses = conn.execute(
            "SELECT status FROM connector_outbox ORDER BY id"
        ).fetchall()
    assert statuses == [("delivered",), ("superseded",)]


@pytest.mark.parametrize(
    ("dead_at", "expected_statuses"),
    [
        (
            "2026-01-01T00:00:00+00:00",
            [("dead_letter",), ("superseded",), ("queued",)],
        ),
        (
            "2026-01-01T00:10:00+00:00",
            [("dead_letter",), ("superseded",)],
        ),
    ],
)
def test_store_v4_dead_letter_suppresses_only_proven_current_episode(
    tmp_path: Path,
    dead_at: str,
    expected_statuses: list[tuple[str]],
) -> None:
    db_path = tmp_path / f"migration-dead-{dead_at[14:19].replace(':', '-')}.db"
    _reset_store_to_v4(db_path)
    dead_payload = _legacy_attention_job_payload(
        attention_id="attn-current",
        transition_at=dead_at,
    )
    current_payload = _legacy_attention_job_payload(
        attention_id="attn-current",
        transition_at="2026-01-01T00:10:00+00:00",
    )
    with sqlite3.connect(str(db_path)) as conn:
        _insert_legacy_attention(
            conn,
            attention_id="attn-current",
            source="worker:legacy",
            severity="warning",
            status="blocked",
            lifecycle_status="open",
            at="2026-01-01T00:10:00+00:00",
            first_seen_at="2026-01-01T00:00:00+00:00",
        )
        for key, status, payload in (
            ("current-dead", "dead_letter", dead_payload),
            ("current-active", "queued", current_payload),
        ):
            conn.execute(
                """
                INSERT INTO connector_outbox (
                    host_id, connector, delivery_key, status, payload_json,
                    private_state_json, created_at, updated_at, next_attempt_at
                ) VALUES ('legacy-host', 'attention', ?, ?, ?, '{}', ?, ?, NULL)
                """,
                (
                    key,
                    status,
                    payload,
                    "2026-01-01T00:10:00+00:00",
                    "2026-01-01T00:10:00+00:00",
                ),
            )
    init_store(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        statuses = conn.execute(
            "SELECT status FROM connector_outbox ORDER BY id"
        ).fetchall()
    assert statuses == expected_statuses


def test_store_v4_resolved_lifecycle_terminalizes_all_active_jobs(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "migration-resolved-active.db"
    _reset_store_to_v4(db_path)
    payload = _legacy_attention_job_payload(
        attention_id="attn-resolved",
        transition_at="2026-01-01T00:10:00+00:00",
    )
    with sqlite3.connect(str(db_path)) as conn:
        _insert_legacy_attention(
            conn,
            attention_id="attn-resolved",
            source="worker:legacy",
            severity="warning",
            status="blocked",
            lifecycle_status="resolved",
            at="2026-01-01T00:10:00+00:00",
        )
        conn.execute(
            """
            INSERT INTO connector_outbox (
                host_id, connector, delivery_key, status, payload_json,
                private_state_json, created_at, updated_at, next_attempt_at
            ) VALUES ('legacy-host', 'attention', 'resolved-active', 'queued',
                      ?, '{}', ?, ?, NULL)
            """,
            (
                payload,
                "2026-01-01T00:10:00+00:00",
                "2026-01-01T00:10:00+00:00",
            ),
        )
    init_store(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        lifecycle_status = conn.execute(
            "SELECT lifecycle_status FROM attention_lifecycles"
        ).fetchone()[0]
        outbox_status = conn.execute(
            "SELECT status FROM connector_outbox"
        ).fetchone()[0]
    assert (lifecycle_status, outbox_status) == ("resolved", "superseded")




@pytest.mark.parametrize("terminal_action", ["fail", "defer", "expiry"])
def test_store_v4_current_pollable_outranks_stale_live_lease(
    tmp_path: Path,
    terminal_action: str,
) -> None:
    db_path = tmp_path / f"migration-stale-lease-{terminal_action}.db"
    init_store(db_path)
    old_payload = _legacy_attention_job_payload(
        attention_id="attn-old",
        transition_at="2026-01-01T00:00:00+00:00",
    )
    current_payload = _legacy_attention_job_payload(
        attention_id="attn-current",
        transition_at="2026-01-01T00:10:00+00:00",
    )
    with sqlite3.connect(str(db_path)) as conn:
        _insert_legacy_attention(
            conn,
            attention_id="attn-current",
            source="worker:legacy",
            severity="warning",
            status="blocked",
            lifecycle_status="open",
            at="2026-01-01T00:10:00+00:00",
        )
        conn.execute(
            """
            INSERT INTO connector_outbox (
                host_id, connector, delivery_key, status, payload_json,
                private_state_json, created_at, updated_at, next_attempt_at
            ) VALUES ('legacy-host', 'attention', 'old-leased', 'queued',
                      ?, '{}', ?, ?, NULL)
            """,
            (
                old_payload,
                "2026-01-01T00:00:00+00:00",
                "2026-01-01T00:00:00+00:00",
            ),
        )
    old_item = poll_connector_outbox(
        db_path,
        "legacy-host",
        "attention",
        lease_seconds=30,
        now="2026-01-01T00:00:00+00:00",
    )["items"][0]
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            """
            INSERT INTO connector_outbox (
                host_id, connector, delivery_key, status, payload_json,
                private_state_json, created_at, updated_at, next_attempt_at
            ) VALUES ('legacy-host', 'attention', 'current-queued', 'queued',
                      ?, '{}', ?, ?, NULL)
            """,
            (
                current_payload,
                "2026-01-01T00:10:00+00:00",
                "2026-01-01T00:10:00+00:00",
            ),
        )
        conn.execute("DROP TABLE attention_lifecycles")
        conn.execute("PRAGMA user_version = 4")
    init_store(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        rows = conn.execute(
            """
            SELECT delivery_key, status, private_state_json
            FROM connector_outbox ORDER BY id
            """
        ).fetchall()
    assert [row[1] for row in rows] == ["leased", "superseded", "queued"]
    stale_state = json.loads(rows[0][2])
    assert stale_state["migration_canonical"] is False
    assert stale_state["terminal_after_lease"] is True
    if terminal_action == "fail":
        result = fail_connector_delivery(
            db_path,
            host_id="legacy-host",
            name="attention",
            ref=old_item["ref"],
            now="2026-01-01T00:00:10+00:00",
        )
        assert result["status"] == "superseded"
    elif terminal_action == "defer":
        result = defer_connector_delivery(
            db_path,
            host_id="legacy-host",
            name="attention",
            ref=old_item["ref"],
            now="2026-01-01T00:00:10+00:00",
        )
        assert result["status"] == "superseded"
    else:
        result = reclaim_expired_connector_leases(
            db_path,
            "legacy-host",
            "attention",
            now="2026-01-01T00:00:31+00:00",
        )
        assert result["reclaimed"] == 1
    with sqlite3.connect(str(db_path)) as conn:
        statuses = conn.execute(
            "SELECT status, COUNT(*) FROM connector_outbox GROUP BY status"
        ).fetchall()
    assert dict(statuses) == {"queued": 1, "superseded": 2}


def test_store_v4_generated_flap_damage_migrates_bounded_and_idempotent(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "migration-generated-flap.db"
    _reset_store_to_v4(db_path)
    current_payload = _legacy_attention_job_payload(
        attention_id="attn-current",
        transition_at="2026-01-01T00:10:00+00:00",
    )
    old_payload = _legacy_attention_job_payload(
        attention_id="attn-old",
        transition_at="2026-01-01T00:00:00+00:00",
    )
    with sqlite3.connect(str(db_path)) as conn:
        for index in range(509):
            _insert_legacy_attention(
                conn,
                attention_id=f"attn-flap-{index:04d}",
                source="worker:legacy",
                severity="warning",
                status="blocked",
                lifecycle_status="open",
                at="2026-01-01T00:00:00+00:00",
                signal_count=2,
            )
        _insert_legacy_attention(
            conn,
            attention_id="attn-current",
            source="worker:legacy",
            severity="warning",
            status="blocked",
            lifecycle_status="open",
            at="2026-01-01T00:10:00+00:00",
            signal_count=172,
        )
        for index in range(600):
            status = ("queued", "retry", "deferred")[index % 3]
            conn.execute(
                """
                INSERT INTO connector_outbox (
                    host_id, connector, delivery_key, status, payload_json,
                    private_state_json, created_at, updated_at, next_attempt_at
                ) VALUES ('legacy-host', 'attention', ?, ?, ?, '{}', ?, ?, NULL)
                """,
                (
                    f"flap-active-{index:04d}",
                    status,
                    current_payload,
                    "2026-01-01T00:10:00+00:00",
                    "2026-01-01T00:10:00+00:00",
                ),
            )
        delivered_cursor = conn.execute(
            """
            INSERT INTO connector_outbox (
                host_id, connector, delivery_key, status, payload_json,
                private_state_json, created_at, updated_at, next_attempt_at
            ) VALUES ('legacy-host', 'attention', 'flap-old-delivered',
                      'delivered', ?, '{}', ?, ?, NULL)
            """,
            (
                old_payload,
                "2026-01-01T00:00:00+00:00",
                "2026-01-01T00:00:00+00:00",
            ),
        )
        conn.execute(
            """
            INSERT INTO connector_deliveries (
                outbox_id, host_id, connector, delivery_key, attempt, status,
                response_json, private_state_json, created_at, delivered_at
            ) VALUES (?, 'legacy-host', 'attention', 'flap-old-delivered', 1,
                      'delivered', '{}', '{}', ?, ?)
            """,
            (
                int(delivered_cursor.lastrowid),
                "2026-01-01T00:00:00+00:00",
                "2026-01-01T00:00:01+00:00",
            ),
        )
        conn.execute(
            """
            INSERT INTO connector_outbox (
                host_id, connector, delivery_key, status, payload_json,
                private_state_json, created_at, updated_at, next_attempt_at
            ) VALUES ('legacy-host', 'attention', 'flap-old-dead',
                      'dead_letter', ?, '{}', ?, ?, NULL)
            """,
            (
                old_payload,
                "2026-01-01T00:00:00+00:00",
                "2026-01-01T00:00:00+00:00",
            ),
        )

    def migration_evidence() -> tuple[Any, ...]:
        with sqlite3.connect(str(db_path)) as conn:
            lifecycle = conn.execute(
                """
                SELECT COUNT(*), lifecycle_status, current_attention_id
                FROM attention_lifecycles
                """
            ).fetchone()
            attention = conn.execute(
                """
                SELECT COUNT(*),
                       MAX(CASE WHEN lifecycle_status = 'open'
                                THEN signal_count ELSE 0 END)
                FROM attention_items
                """
            ).fetchone()
            outbox = dict(
                conn.execute(
                    "SELECT status, COUNT(*) FROM connector_outbox GROUP BY status"
                ).fetchall()
            )
            delivered_audit = conn.execute(
                """
                SELECT COUNT(*) FROM connector_deliveries
                WHERE status = 'delivered'
                """
            ).fetchone()[0]
            canonical_payload = conn.execute(
                """
                SELECT payload_json FROM connector_outbox
                WHERE status = 'queued'
                """
            ).fetchall()
            return (
                lifecycle,
                attention,
                outbox,
                delivered_audit,
                canonical_payload,
                _user_version(conn),
            )

    init_store(db_path)
    first = migration_evidence()
    init_store(db_path)
    second = migration_evidence()
    with sqlite3.connect(str(db_path)) as conn:
        integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]

    assert first == second
    lifecycle, attention, outbox, delivered_audit, canonical_payload, version = first
    assert lifecycle == (1, "open", "attn-current")
    assert attention == (510, 1190)
    assert outbox == {
        "dead_letter": 1,
        "delivered": 1,
        "queued": 1,
        "superseded": 600,
    }
    assert delivered_audit == 1
    assert len(canonical_payload) == 1
    assert json.loads(canonical_payload[0][0])["attention"]["id"] == "attn-current"
    assert version == store_sqlite.STORE_SCHEMA_VERSION
    assert integrity == "ok"


def test_store_v4_migration_retains_single_and_multiple_live_leases_safely(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "migration-leases.db"
    init_store(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_legacy_attention(
            conn,
            attention_id="attn-legacy",
            source="worker:legacy",
            severity="warning",
            status="blocked",
            lifecycle_status="open",
            at="2026-01-01T00:00:00+00:00",
        )
        created_payload = _legacy_attention_job_payload()
        escalation_payload = _legacy_attention_job_payload(
            event_type="attention_escalated", severity="critical"
        )
        for index in range(5):
            conn.execute(
                """
                INSERT INTO connector_outbox (
                    host_id, connector, delivery_key, status, payload_json,
                    private_state_json, created_at, updated_at, next_attempt_at
                ) VALUES ('legacy-host', 'attention', ?, 'queued', ?, '{}', ?, ?, NULL)
                """,
                (
                    f"leased-created-{index}",
                    created_payload,
                    "2026-01-01T00:00:00+00:00",
                    "2026-01-01T00:00:00+00:00",
                ),
            )
        conn.execute(
            """
            INSERT INTO connector_outbox (
                host_id, connector, delivery_key, status, payload_json,
                private_state_json, created_at, updated_at, next_attempt_at
            ) VALUES ('legacy-host', 'attention', 'leased-escalation', 'queued',
                      ?, '{}', ?, ?, NULL)
            """,
            (
                escalation_payload,
                "2026-01-01T00:00:00+00:00",
                "2026-01-01T00:00:00+00:00",
            ),
        )
    leased = poll_connector_outbox(
        db_path,
        "legacy-host",
        "attention",
        limit=6,
        lease_seconds=600,
        now="2026-01-01T00:00:00+00:00",
    )["items"]
    assert len(leased) == 6
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute("DROP TABLE attention_lifecycles")
        conn.execute("PRAGMA user_version = 4")
    init_store(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        leased_rows = conn.execute(
            """
            SELECT id, delivery_key, private_state_json
            FROM connector_outbox WHERE status = 'leased' ORDER BY id
            """
        ).fetchall()
        pollable = conn.execute(
            """
            SELECT COUNT(*) FROM connector_outbox
            WHERE status IN ('queued', 'retry', 'deferred')
            """
        ).fetchone()[0]
    assert len(leased_rows) == 6
    assert pollable == 0
    created_rows = [row for row in leased_rows if "created" in row[1]]
    created_states = [json.loads(row[2]) for row in created_rows]
    assert sum(bool(state["migration_canonical"]) for state in created_states) == 1
    assert sum(bool(state.get("terminal_after_lease")) for state in created_states) == 4
    escalation_state = json.loads(
        next(row[2] for row in leased_rows if row[1] == "leased-escalation")
    )
    assert escalation_state["migration_canonical"] is True
    assert "terminal_after_lease" not in escalation_state

    items_by_key = {item["key"]: item for item in leased}
    canonical_created = next(
        row for row in created_rows if json.loads(row[2])["migration_canonical"]
    )
    duplicate_created = [
        row
        for row in created_rows
        if json.loads(row[2]).get("terminal_after_lease")
    ]
    canonical_ack = ack_connector_delivery(
        db_path,
        host_id="legacy-host",
        name="attention",
        ref=items_by_key[canonical_created[1]]["ref"],
        now="2026-01-01T00:00:05+00:00",
    )
    assert canonical_ack["status"] == "acknowledged"
    with sqlite3.connect(str(db_path)) as conn:
        sibling_states = [
            json.loads(row[0])
            for row in conn.execute(
                """
                SELECT private_state_json FROM connector_outbox
                WHERE status = 'leased'
                  AND delivery_key LIKE 'leased-created-%'
                """
            ).fetchall()
        ]
    assert len(sibling_states) == 4
    assert all(state["terminal_after_lease"] for state in sibling_states)

    failed = fail_connector_delivery(
        db_path,
        host_id="legacy-host",
        name="attention",
        ref=items_by_key[duplicate_created[0][1]]["ref"],
        now="2026-01-01T00:00:10+00:00",
    )
    deferred = defer_connector_delivery(
        db_path,
        host_id="legacy-host",
        name="attention",
        ref=items_by_key[duplicate_created[1][1]]["ref"],
        now="2026-01-01T00:00:20+00:00",
    )
    assert failed["status"] == deferred["status"] == "superseded"
    with sqlite3.connect(str(db_path)) as conn:
        expiring = duplicate_created[2]
        delivery = conn.execute(
            """
            SELECT id, private_state_json FROM connector_deliveries
            WHERE outbox_id = ? AND status = 'leased'
            """,
            (expiring[0],),
        ).fetchone()
        delivery_state = json.loads(delivery[1])
        delivery_state["lease_expires_at"] = "2026-01-01T00:00:30+00:00"
        conn.execute(
            "UPDATE connector_deliveries SET private_state_json = ? WHERE id = ?",
            (json.dumps(delivery_state, sort_keys=True), delivery[0]),
        )
    reclaimed = reclaim_expired_connector_leases(
        db_path,
        "legacy-host",
        "attention",
        now="2026-01-01T00:01:00+00:00",
    )
    assert reclaimed["reclaimed"] == 1
    duplicate_ack = ack_connector_delivery(
        db_path,
        host_id="legacy-host",
        name="attention",
        ref=items_by_key[duplicate_created[3][1]]["ref"],
        now="2026-01-01T00:01:30+00:00",
    )
    assert duplicate_ack["status"] == "acknowledged"
    single_ack = ack_connector_delivery(
        db_path,
        host_id="legacy-host",
        name="attention",
        ref=items_by_key["leased-escalation"]["ref"],
        now="2026-01-01T00:02:00+00:00",
    )
    assert single_ack["status"] == canonical_ack["status"] == "acknowledged"
    with sqlite3.connect(str(db_path)) as conn:
        statuses = conn.execute(
            "SELECT status, COUNT(*) FROM connector_outbox GROUP BY status"
        ).fetchall()
    assert dict(statuses) == {"delivered": 3, "superseded": 3}


def _worker_binding(
    *,
    worker_id: str = "worker-1",
    worker_fingerprint: str = "fp-1",
    target_kind: str = "pane_id",
    target_value: str = "pane-1",
    private_fingerprint: str = "priv-1",
    sendable: bool = True,
    reason: str | None = None,
    observed_at: str = "2026-01-01T00:00:00+00:00",
    expires_at: str = "2026-01-02T00:00:00+00:00",
) -> WorkerBinding:
    return WorkerBinding(
        host_id="host-a",
        worker_id=worker_id,
        worker_fingerprint=worker_fingerprint,
        backend="herdr",
        target_kind=target_kind,
        target_value=target_value,
        turn_target_kind=None,
        turn_target_value=None,
        sendable=sendable,
        reason=reason,
        observed_at=observed_at,
        expires_at=expires_at,
        private_fingerprint=private_fingerprint,
    )


def test_store_worker_binding_upsert_list_resolve_and_expire(tmp_path: Path) -> None:
    db_path = tmp_path / "bindings.db"
    first = _worker_binding()
    moved = _worker_binding(
        target_value="pane-2",
        observed_at="2026-01-01T00:10:00+00:00",
    )

    init_store(db_path)
    assert upsert_worker_bindings(db_path, [first]) == 1
    assert upsert_worker_bindings(db_path, [moved]) == 1

    current = list_worker_bindings(
        db_path,
        "host-a",
        backend="herdr",
        now="2026-01-01T00:30:00+00:00",
    )
    assert len(current) == 1
    assert current[0].target_value == "pane-2"
    assert current[0].worker_id == "worker-1"
    resolved = resolve_worker_binding(
        db_path,
        "host-a",
        "worker-1",
        worker_fingerprint="fp-1",
        backend="herdr",
        now="2026-01-01T00:30:00+00:00",
    )
    assert resolved is not None
    assert resolved.target_value == "pane-2"

    expired_count = expire_worker_bindings(
        db_path,
        "host-a",
        backend="herdr",
        private_fingerprints=["priv-1"],
        now="2026-01-01T00:45:00+00:00",
        reason="stale_target",
    )
    assert expired_count == 1
    assert list_worker_bindings(db_path, "host-a", backend="herdr", now="2026-01-01T00:46:00+00:00") == []
    history = list_worker_bindings(
        db_path,
        "host-a",
        backend="herdr",
        include_expired=True,
        now="2026-01-01T00:46:00+00:00",
    )
    assert len(history) == 1
    assert history[0].sendable is False
    assert history[0].reason == "stale_target"
    assert resolve_worker_binding(
        db_path,
        "host-a",
        "worker-1",
        backend="herdr",
        now="2026-01-01T00:46:00+00:00",
    ) is None


def test_store_worker_bindings_allow_duplicate_targets_and_expire_stale(tmp_path: Path) -> None:
    db_path = tmp_path / "duplicate-bindings.db"
    binding_a = _worker_binding(
        worker_id="worker-a",
        worker_fingerprint="fp-a",
        private_fingerprint="priv-a",
        target_value="same-pane",
        sendable=False,
        reason="duplicate_backend_target",
    )
    binding_b = _worker_binding(
        worker_id="worker-b",
        worker_fingerprint="fp-b",
        private_fingerprint="priv-b",
        target_value="same-pane",
        sendable=False,
        reason="duplicate_backend_target",
    )
    upsert_worker_bindings(db_path, [binding_a, binding_b])

    current = list_worker_bindings(db_path, "host-a", backend="herdr", now="2026-01-01T00:30:00+00:00")
    assert len(current) == 2
    assert {binding.target_value for binding in current} == {"same-pane"}
    assert {binding.reason for binding in current} == {"duplicate_backend_target"}
    assert resolve_worker_binding(
        db_path,
        "host-a",
        "worker-a",
        backend="herdr",
        now="2026-01-01T00:30:00+00:00",
    ) is None

    expired_count = expire_stale_worker_bindings(
        db_path,
        "host-a",
        backend="herdr",
        current_private_fingerprints=["priv-a"],
        now="2026-01-01T00:40:00+00:00",
        reason="stale_observation",
    )
    assert expired_count == 1
    remaining = list_worker_bindings(db_path, "host-a", backend="herdr", now="2026-01-01T00:41:00+00:00")
    assert [binding.private_fingerprint for binding in remaining] == ["priv-a"]


def test_store_upsert_separates_colliding_duplicate_private_fingerprints(tmp_path: Path) -> None:
    db_path = tmp_path / "colliding-bindings.db"
    binding_a = _worker_binding(
        worker_id="worker-a",
        worker_fingerprint="fp-a",
        target_value="same-agent",
        private_fingerprint="colliding-private",
    )
    binding_b = _worker_binding(
        worker_id="worker-b",
        worker_fingerprint="fp-b",
        target_value="same-agent",
        private_fingerprint="colliding-private",
    )

    assert upsert_worker_bindings(db_path, [binding_a, binding_b]) == 2

    current = list_worker_bindings(db_path, "host-a", backend="herdr", now="2026-01-01T00:30:00+00:00")
    assert len(current) == 2
    assert {binding.worker_id for binding in current} == {"worker-a", "worker-b"}
    assert {binding.sendable for binding in current} == {False}
    assert {binding.reason for binding in current} == {"duplicate_backend_target"}
    assert "colliding-private" not in {binding.private_fingerprint for binding in current}
    assert len({binding.private_fingerprint for binding in current}) == 2
    assert resolve_worker_binding(
        db_path,
        "host-a",
        "worker-a",
        backend="herdr",
        now="2026-01-01T00:30:00+00:00",
    ) is None


def test_store_snapshot_payload_does_not_contain_private_worker_bindings(tmp_path: Path) -> None:
    db_path = tmp_path / "payload-clean.db"
    config = Config(host_id="host-a", db_path=db_path)
    snapshot = project_from_raw(
        config,
        workers=[{"id": "worker-1", "name": "Worker", "status": "active"}],
    )
    binding = _worker_binding(target_value="pane-secret", private_fingerprint="priv-secret")

    save_snapshot(db_path, snapshot)
    upsert_worker_bindings(db_path, [binding])

    with sqlite3.connect(str(db_path)) as conn:
        payload = conn.execute("SELECT payload FROM snapshots ORDER BY id DESC LIMIT 1").fetchone()[0]
        target_value = conn.execute("SELECT target_value FROM worker_bindings LIMIT 1").fetchone()[0]

    assert target_value == "pane-secret"
    assert "pane-secret" not in payload
    assert "priv-secret" not in payload
    assert "target_kind" not in payload


def test_store_save_snapshot_updates_pr6_projections_and_prunes_by_host(tmp_path: Path) -> None:
    db_path = tmp_path / "projections.db"
    config_a = Config(host_id="host-a", db_path=db_path)
    config_b = Config(host_id="host-b", db_path=db_path)
    snapshot_a_old = project_from_raw(
        config_a,
        spaces=[{"id": "space-old", "name": "Old", "status": "active"}],
        workers=[
            {
                "id": "worker-old",
                "name": "Old Worker",
                "status": "active",
                "space_id": "space-old",
                "summary": "old",
            }
        ],
        backend_health=[
            {
                "name": "herdr",
                "status": "healthy",
                "outcome": "healthy_non_empty",
                "observed_at": "2026-01-01T00:00:00+00:00",
                "counts": {"workers": 1},
            }
        ],
    )
    snapshot_b = project_from_raw(
        config_b,
        spaces=[{"id": "space-b", "name": "B", "status": "active"}],
        workers=[{"id": "worker-b", "name": "Worker B", "status": "active"}],
    )
    snapshot_a_new = project_from_raw(
        config_a,
        spaces=[{"id": "space-new", "name": "New", "status": "warning"}],
        workers=[
            {
                "id": "worker-new",
                "name": "New Worker",
                "status": "pending",
                "space_id": "space-new",
                "summary": "human approval required before continuing",
                "meta": {
                    "needs_human": True,
                    "safe": "kept",
                    "connectorId": "sentinel-connector-id",
                    "delivery": "sentinel-delivery",
                },
                "backend_target": {"value": "sentinel-private-target"},
            }
        ],
        backend_health=[
            {
                "name": "herdr",
                "status": "degraded",
                "outcome": "malformed_json",
                "observed_at": "2026-01-01T00:01:00+00:00",
                "message": "Herdr command returned malformed JSON",
                "counts": {"workers": 1},
            }
        ],
    )

    _save_observation(db_path, snapshot_a_old, "positive", snapshot_a_old.updated_at)
    _save_observation(db_path, snapshot_b, "positive", snapshot_b.updated_at)
    _save_observation(db_path, snapshot_a_new, "positive", snapshot_a_new.updated_at)

    with sqlite3.connect(str(db_path)) as conn:
        host_a_workers = conn.execute(
            "SELECT worker_id, status, payload_json FROM workers WHERE host_id = ?",
            ("host-a",),
        ).fetchall()
        host_b_workers = conn.execute(
            "SELECT worker_id FROM workers WHERE host_id = ?",
            ("host-b",),
        ).fetchall()
        host_a_spaces = conn.execute(
            "SELECT space_id FROM spaces WHERE host_id = ?",
            ("host-a",),
        ).fetchall()
        host_a_turns = conn.execute(
            "SELECT worker_id FROM turns WHERE host_id = ?",
            ("host-a",),
        ).fetchall()
        host_a_pending_count = conn.execute(
            "SELECT COUNT(*) FROM pending_interactions WHERE host_id = ?",
            ("host-a",),
        ).fetchone()[0]
        host_a_attention_count = conn.execute(
            "SELECT COUNT(*) FROM attention_items WHERE host_id = ?",
            ("host-a",),
        ).fetchone()[0]
        host_a_health = conn.execute(
            "SELECT backend_name, status, outcome FROM backend_health WHERE host_id = ?",
            ("host-a",),
        ).fetchone()
        event_count = conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]

    assert [(row[0], row[1]) for row in host_a_workers] == [("worker-new", "waiting")]
    assert host_b_workers == [("worker-b",)]
    assert host_a_spaces == [("space-new",)]
    assert host_a_turns == [("worker-new",)]
    assert host_a_pending_count == 1
    assert host_a_attention_count == 1
    assert host_a_health == ("herdr", "degraded", "malformed_json")
    assert event_count == 3
    assert "sentinel-" not in host_a_workers[0][2]


def test_store_merges_public_turn_content_without_private_labels(tmp_path: Path) -> None:
    db_path = tmp_path / "turn-content.db"
    config = Config(host_id="turn-host", db_path=db_path)
    snapshot = project_from_raw(
        config,
        workers=[{"id": "worker-1", "name": "codex", "status": "active", "space_id": "space-1"}],
    )
    init_store(db_path)
    save_snapshot(db_path, snapshot)

    updated = merge_turn_content(
        db_path,
        "turn-host",
        "worker-1",
        {
            "user_text": "Please explain Telegram delivery.",
            "assistant_final_text": "Done without leaking pane_id pane-private or terminal_id term-private.",
            "assistant_stream_text": "Working...",
            "complete": True,
            "has_open_turn": False,
        },
        observed_at="2026-01-01T00:00:00+00:00",
    )
    payload = turns_payload_from_store(db_path, "turn-host", snapshot=snapshot)

    assert updated == 1
    turn = payload["turns"][0]
    assert turn["worker_id"] == "worker-1"
    assert turn["user_text"] == "Please explain Telegram delivery."
    assert "Done without leaking" in turn["assistant_final_text"]
    assert "pane-private" not in json.dumps(payload)
    assert "term-private" not in json.dumps(payload)
    assert turn["complete"] is True
    assert turn["has_open_turn"] is False


def test_store_merges_turn_content_into_matching_command_row_only(tmp_path: Path) -> None:
    db_path = tmp_path / "turn-command-content.db"
    config = Config(host_id="turn-host", db_path=db_path)
    snapshot = project_from_raw(
        config,
        workers=[{"id": "worker-1", "name": "codex", "status": "active", "space_id": "space-1"}],
    )
    init_store(db_path)
    save_snapshot(db_path, snapshot)
    worker = snapshot.workers[0]
    command_turn = store_sqlite.upsert_command_pending_turn(
        db_path,
        "turn-host",
        worker,
        request_id="req-1",
        instruction_text="Please answer from Telegram.",
        observed_at="2026-01-01T00:00:00+00:00",
    )
    assert command_turn is not None

    updated = merge_turn_content(
        db_path,
        "turn-host",
        "worker-1",
        {
            "user_text": "Please answer from Telegram.",
            "assistant_final_text": "Telegram answer delivered.",
            "complete": True,
            "has_open_turn": False,
        },
        observed_at="2026-01-01T00:01:00+00:00",
    )
    payload = turns_payload_from_store(db_path, "turn-host", snapshot=snapshot)
    command_rows = [
        turn for turn in payload["turns"] if turn.get("origin_command_id") == "req-1"
    ]
    snapshot_rows = [
        turn for turn in payload["turns"] if turn.get("id") != command_turn["id"]
    ]

    assert updated == 1
    assert len(command_rows) == 1
    assert command_rows[0]["assistant_final_text"] == "Telegram answer delivered."
    assert command_rows[0]["complete"] is True
    assert snapshot_rows
    assert all(not (turn.get("assistant_final_text") or "") for turn in snapshot_rows)


def test_source_turn_without_matching_prompt_does_not_inherit_old_command_origin(tmp_path: Path) -> None:
    db_path = tmp_path / "turn-source-no-stale-command.db"
    config = Config(host_id="turn-host", db_path=db_path)
    snapshot = project_from_raw(
        config,
        workers=[{"id": "worker-1", "name": "claude", "status": "active", "space_id": "space-1"}],
    )
    init_store(db_path)
    save_snapshot(db_path, snapshot)
    worker = snapshot.workers[0]
    assert store_sqlite.upsert_command_pending_turn(
        db_path,
        "turn-host",
        worker,
        request_id="req-old",
        instruction_text="go",
        observed_at="2026-01-01T00:00:00+00:00",
    )

    updated = merge_turn_content(
        db_path,
        "turn-host",
        "worker-1",
        {
            "assistant_final_text": "Monitor changed state.",
            "complete": True,
            "has_open_turn": False,
            "source_turn_id": "source-unmatched",
        },
        observed_at="2026-01-01T00:01:00+00:00",
    )
    payload = turns_payload_from_store(db_path, "turn-host", snapshot=snapshot)
    source_rows = [turn for turn in payload["turns"] if turn.get("assistant_final_text") == "Monitor changed state."]

    assert updated == 1
    assert len(source_rows) == 1
    assert source_rows[0]["source_turn_id"].startswith("turnsrc-")
    assert "source-unmatched" not in json.dumps(payload, sort_keys=True)
    assert source_rows[0]["assistant_final_text"] == "Monitor changed state."
    assert source_rows[0].get("origin_command_id") is None
    assert source_rows[0]["source"] == "snapshot"


def test_source_turn_with_matching_prompt_keeps_command_origin(tmp_path: Path) -> None:
    db_path = tmp_path / "turn-source-matched-command.db"
    config = Config(host_id="turn-host", db_path=db_path)
    snapshot = project_from_raw(
        config,
        workers=[{"id": "worker-1", "name": "claude", "status": "active", "space_id": "space-1"}],
    )
    init_store(db_path)
    save_snapshot(db_path, snapshot)
    worker = snapshot.workers[0]
    assert store_sqlite.upsert_command_pending_turn(
        db_path,
        "turn-host",
        worker,
        request_id="req-1",
        instruction_text="go",
        observed_at="2026-01-01T00:00:00+00:00",
    )

    updated = merge_turn_content(
        db_path,
        "turn-host",
        "worker-1",
        {
            "user_text": "go",
            "assistant_final_text": "Done.",
            "complete": True,
            "has_open_turn": False,
            "source_turn_id": "source-matched",
        },
        observed_at="2026-01-01T00:01:00+00:00",
    )
    payload = turns_payload_from_store(db_path, "turn-host", snapshot=snapshot)
    source_rows = [turn for turn in payload["turns"] if turn.get("assistant_final_text") == "Done."]

    assert updated == 1
    assert len(source_rows) == 1
    assert source_rows[0]["source_turn_id"].startswith("turnsrc-")
    assert "source-matched" not in json.dumps(payload, sort_keys=True)
    assert source_rows[0]["origin_command_id"] == "req-1"
    assert source_rows[0]["source"] == "command"


@pytest.mark.parametrize("framing", ["\x01", "\x7f", "\x80", "\x9f"])
def test_source_turn_edge_framing_controls_match_command_origin(
    tmp_path: Path,
    framing: str,
) -> None:
    db_path = tmp_path / "turn-source-framing-command.db"
    config = Config(host_id="turn-host", db_path=db_path)
    snapshot = project_from_raw(
        config,
        workers=[
            {
                "id": "worker-1",
                "name": "claude",
                "status": "active",
                "space_id": "space-1",
            }
        ],
    )
    init_store(db_path)
    save_snapshot(db_path, snapshot)
    worker = snapshot.workers[0]
    assert store_sqlite.upsert_command_pending_turn(
        db_path,
        "turn-host",
        worker,
        request_id="req-1",
        instruction_text="hello",
        observed_at="2026-01-01T00:00:00+00:00",
    )

    assert merge_turn_content(
        db_path,
        "turn-host",
        "worker-1",
        {
            "user_text": f"{framing}hello{framing}",
            "assistant_final_text": "Done.",
            "complete": True,
            "has_open_turn": False,
            "source_turn_id": f"source-framed-{ord(framing)}",
        },
        observed_at="2026-01-01T00:01:00+00:00",
    ) == 1

    payload = turns_payload_from_store(db_path, "turn-host", snapshot=snapshot)
    source_row = next(
        turn for turn in payload["turns"] if turn.get("assistant_final_text") == "Done."
    )
    assert source_row["origin_command_id"] == "req-1"


def test_source_turn_interior_control_does_not_match_command_origin(tmp_path: Path) -> None:
    db_path = tmp_path / "turn-source-interior-control.db"
    config = Config(host_id="turn-host", db_path=db_path)
    snapshot = project_from_raw(
        config,
        workers=[
            {
                "id": "worker-1",
                "name": "claude",
                "status": "active",
                "space_id": "space-1",
            }
        ],
    )
    init_store(db_path)
    save_snapshot(db_path, snapshot)
    worker = snapshot.workers[0]
    assert store_sqlite.upsert_command_pending_turn(
        db_path,
        "turn-host",
        worker,
        request_id="req-1",
        instruction_text="hello",
        observed_at="2026-01-01T00:00:00+00:00",
    )

    assert merge_turn_content(
        db_path,
        "turn-host",
        "worker-1",
        {
            "user_text": "hel\x01lo",
            "assistant_final_text": "Separate turn.",
            "complete": True,
            "has_open_turn": False,
            "source_turn_id": "source-interior-control",
        },
        observed_at="2026-01-01T00:01:00+00:00",
    ) == 1

    payload = turns_payload_from_store(db_path, "turn-host", snapshot=snapshot)
    source_row = next(
        turn
        for turn in payload["turns"]
        if turn.get("assistant_final_text") == "Separate turn."
    )
    assert source_row.get("origin_command_id") is None


def test_store_save_latest_host_scope_and_list_hosts(tmp_path: Path) -> None:
    db_path = tmp_path / "tendwire.db"
    config_a = Config(host_id="host-a", db_path=db_path)
    config_b = Config(host_id="host-b", db_path=db_path)

    init_store(db_path)
    assert latest_snapshot(db_path) is None

    snapshot_a_old = project_from_raw(
        config_a,
        workers=[{"id": "worker-a-old", "name": "Host A Old", "status": "active"}],
    )
    snapshot_b = project_from_raw(
        config_b,
        workers=[{"id": "worker-b", "name": "Host B", "status": "idle"}],
    )
    snapshot_a_new = project_from_raw(
        config_a,
        workers=[{"id": "worker-a-new", "name": "Host A New", "status": "waiting"}],
    )

    save_snapshot(db_path, snapshot_a_old)
    save_snapshot(db_path, snapshot_b)
    save_snapshot(db_path, snapshot_a_new)

    global_restored = latest_snapshot(db_path)
    assert global_restored is not None
    assert global_restored.host_id == "host-a"
    assert global_restored.content_fingerprint == snapshot_a_new.content_fingerprint

    restored_a = latest_snapshot(db_path, "host-a")
    assert restored_a is not None
    assert restored_a.host_id == "host-a"
    assert restored_a.content_fingerprint == snapshot_a_new.content_fingerprint
    assert restored_a.workers[0].id == "worker-a-new"

    restored_b = latest_snapshot(db_path, "host-b")
    assert restored_b is not None
    assert restored_b.host_id == "host-b"
    assert restored_b.content_fingerprint == snapshot_b.content_fingerprint
    assert restored_b.workers[0].id == "worker-b"

    assert latest_snapshot(db_path, "missing-host") is None
    assert list_hosts(db_path) == ["host-a", "host-b"]


def _reserve_test_request(
    db_path: Path,
    *,
    host_id: str = "host-a",
    request_id: str,
    action: str = "send_instruction",
    fingerprint: str | None = None,
    now: str = "2026-01-01T00:00:00+00:00",
    lease_seconds: float = 30.0,
) -> dict[str, Any]:
    canonical_fingerprint = fingerprint or f"{action}:{request_id}"
    return reserve_command_request(
        db_path,
        host_id=host_id,
        request_id=request_id,
        action=action,
        canonical_version=1,
        canonical_fingerprint=canonical_fingerprint,
        canonical_request_json=json.dumps(
            {"action": action, "request": request_id},
            sort_keys=True,
            separators=(",", ":"),
        ),
        public_worker_id="worker-public",
        pending_result_json='{"ok":false,"status":"pending"}',
        owner_lease_seconds=lease_seconds,
        now=now,
    )


def _accept_test_request(
    db_path: Path,
    *,
    host_id: str = "host-a",
    request_id: str,
    now: str,
) -> dict[str, Any]:
    reservation = _reserve_test_request(
        db_path,
        host_id=host_id,
        request_id=request_id,
        now=now,
    )
    started = mark_command_send_started(
        db_path,
        host_id=host_id,
        request_id=request_id,
        canonical_fingerprint=f"send_instruction:{request_id}",
        owner_token=reservation["owner_token"],
        binding_fingerprint=f"private:{request_id}",
        now=now,
    )
    return finish_command_request(
        db_path,
        host_id=host_id,
        request_id=request_id,
        canonical_fingerprint=f"send_instruction:{request_id}",
        owner_token=started["owner_token"],
        expected_state="send_started",
        terminal_state="accepted",
        status=STATUS_ACCEPTED,
        result_json='{"ok":true,"status":"accepted"}',
        now=now,
    )


def test_store_host_wide_request_identity_conflicts_across_actions_and_tombstones(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "host-wide.db"
    init_store(db_path)
    reservation = _reserve_test_request(db_path, request_id="same-id")

    collision = _reserve_test_request(
        db_path,
        request_id="same-id",
        action="answer_pending",
        fingerprint="answer-fingerprint",
    )
    assert collision["status"] == "request_id_conflict"
    assert collision["owner_token"] is None
    assert collision["receipt"]["action"] == "send_instruction"

    started = mark_command_send_started(
        db_path,
        host_id="host-a",
        request_id="same-id",
        canonical_fingerprint="send_instruction:same-id",
        owner_token=reservation["owner_token"],
        binding_fingerprint="private-binding",
        now="2026-01-01T00:00:01+00:00",
    )
    terminal = finish_command_request(
        db_path,
        host_id="host-a",
        request_id="same-id",
        canonical_fingerprint="send_instruction:same-id",
        owner_token=started["owner_token"],
        expected_state="send_started",
        terminal_state="accepted",
        status="accepted",
        result_json='{"result":"original"}',
        now="2026-01-01T00:00:02+00:00",
    )
    assert terminal["status"] == "accepted"

    replay = _reserve_test_request(db_path, request_id="same-id")
    overwrite = finish_command_request(
        db_path,
        host_id="host-a",
        request_id="same-id",
        canonical_fingerprint="send_instruction:same-id",
        owner_token=reservation["owner_token"],
        expected_state="send_started",
        terminal_state="uncertain",
        status="request_state_uncertain",
        result_json='{"result":"overwritten"}',
        now="2026-01-01T00:00:03+00:00",
    )
    assert replay["status"] == "terminal"
    assert overwrite["status"] == "terminal"
    receipt = get_command_request(db_path, "host-a", "same-id")
    assert receipt is not None
    assert receipt["state"] == "accepted"
    assert receipt["result_json"] == '{"result":"original"}'
    assert "binding_fingerprint" not in receipt
    with sqlite3.connect(str(db_path)) as conn:
        assert conn.execute("SELECT COUNT(*) FROM command_receipts").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM commands").fetchone()[0] == 1
        assert conn.execute(
            "SELECT binding_fingerprint FROM command_receipts"
        ).fetchone()[0] == "private-binding"


def test_store_enforces_command_transition_graph_owner_and_transactional_effect(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "transitions.db"
    init_store(db_path)
    reservation = _reserve_test_request(db_path, request_id="transition")

    with pytest.raises(ValueError, match="illegal command request transition"):
        finish_command_request(
            db_path,
            host_id="host-a",
            request_id="transition",
            canonical_fingerprint="send_instruction:transition",
            owner_token=reservation["owner_token"],
            expected_state="reserved",
            terminal_state="accepted",
            status="accepted",
            result_json="{}",
        )
    not_owner = mark_command_send_started(
        db_path,
        host_id="host-a",
        request_id="transition",
        canonical_fingerprint="send_instruction:transition",
        owner_token="wrong-owner",
        binding_fingerprint="private-binding",
        now="2026-01-01T00:00:01+00:00",
    )
    assert not_owner["status"] == "not_owner"
    started = mark_command_send_started(
        db_path,
        host_id="host-a",
        request_id="transition",
        canonical_fingerprint="send_instruction:transition",
        owner_token=reservation["owner_token"],
        binding_fingerprint="private-binding",
        now="2026-01-01T00:00:01+00:00",
    )
    assert started["status"] == "send_started"
    invalid = finish_command_request(
        db_path,
        host_id="host-a",
        request_id="transition",
        canonical_fingerprint="send_instruction:transition",
        owner_token=reservation["owner_token"],
        expected_state="reserved",
        terminal_state="rejected",
        status="backend_rejected",
        result_json="{}",
        now="2026-01-01T00:00:02+00:00",
    )
    assert invalid["status"] == "invalid_state"

    def fail_effect(conn: sqlite3.Connection) -> None:
        conn.execute(
            "UPDATE commands SET result_json = 'corrupt' "
            "WHERE host_id = 'host-a' AND request_id = 'transition'"
        )
        raise RuntimeError("effect failed")

    with pytest.raises(RuntimeError, match="effect failed"):
        finish_command_request(
            db_path,
            host_id="host-a",
            request_id="transition",
            canonical_fingerprint="send_instruction:transition",
            owner_token=reservation["owner_token"],
            expected_state="send_started",
            terminal_state="accepted",
            status="accepted",
            result_json='{"result":"accepted"}',
            terminal_effect=fail_effect,
            now="2026-01-01T00:00:02+00:00",
        )
    receipt = get_command_request(db_path, "host-a", "transition")
    assert receipt is not None
    assert receipt["state"] == "send_started"
    with sqlite3.connect(str(db_path)) as conn:
        assert conn.execute(
            "SELECT state, result_json FROM commands "
            "WHERE host_id = 'host-a' AND request_id = 'transition'"
        ).fetchone() == ("send_started", '{"ok":false,"status":"pending"}')

    accepted = finish_command_request(
        db_path,
        host_id="host-a",
        request_id="transition",
        canonical_fingerprint="send_instruction:transition",
        owner_token=reservation["owner_token"],
        expected_state="send_started",
        terminal_state="accepted",
        status="accepted",
        result_json='{"result":"accepted"}',
        now="2026-01-01T00:00:02+00:00",
    )
    assert accepted["status"] == "accepted"

    rejected_reservation = _reserve_test_request(
        db_path,
        request_id="rejected",
        now="2026-01-01T00:01:00+00:00",
    )
    rejected = finish_command_request(
        db_path,
        host_id="host-a",
        request_id="rejected",
        canonical_fingerprint="send_instruction:rejected",
        owner_token=rejected_reservation["owner_token"],
        expected_state="reserved",
        terminal_state="rejected",
        status="backend_rejected",
        result_json='{"ok":false}',
        now="2026-01-01T00:01:01+00:00",
    )
    assert rejected["status"] == "rejected"


def test_command_pending_turn_terminal_effect_is_atomic_with_acceptance(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "pending-turn-effect.db"
    init_store(db_path)
    reservation = _reserve_test_request(db_path, request_id="turn-effect")
    started = mark_command_send_started(
        db_path,
        host_id="host-a",
        request_id="turn-effect",
        canonical_fingerprint="send_instruction:turn-effect",
        owner_token=reservation["owner_token"],
        binding_fingerprint="private-binding",
        now="2026-01-01T00:00:01+00:00",
    )
    invalid_effect = command_pending_turn_terminal_effect(
        host_id="host-a",
        worker={"id": "worker-public", "name": "Worker"},
        request_id="turn-effect",
        instruction_text="",
    )
    with pytest.raises(
        store_sqlite.StoreSchemaError,
        match="command_pending_turn_terminal_effect_failed",
    ):
        finish_command_request(
            db_path,
            host_id="host-a",
            request_id="turn-effect",
            canonical_fingerprint="send_instruction:turn-effect",
            owner_token=started["owner_token"],
            expected_state="send_started",
            terminal_state="accepted",
            status="accepted",
            result_json='{"ok":true}',
            terminal_effect=invalid_effect,
            now="2026-01-01T00:00:02+00:00",
        )
    assert get_command_request(
        db_path, "host-a", "turn-effect"
    )["state"] == "send_started"
    assert turns_payload_from_store(db_path, "host-a")["turns"] == []

    valid_effect = command_pending_turn_terminal_effect(
        host_id="host-a",
        worker={"id": "worker-public", "name": "Worker"},
        request_id="turn-effect",
        instruction_text="Continue safely.",
    )
    result = finish_command_request(
        db_path,
        host_id="host-a",
        request_id="turn-effect",
        canonical_fingerprint="send_instruction:turn-effect",
        owner_token=started["owner_token"],
        expected_state="send_started",
        terminal_state="accepted",
        status="accepted",
        result_json='{"ok":true}',
        terminal_effect=valid_effect,
        now="2026-01-01T00:00:02+00:00",
    )
    assert result["status"] == "accepted"
    turns = turns_payload_from_store(db_path, "host-a")["turns"]
    assert len(turns) == 1
    assert turns[0]["origin_command_id"] == "turn-effect"
    assert turns[0]["user_text"] == "Continue safely."


def test_command_pending_turn_effect_is_atomic_with_send_start(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "pending-turn-send-start-effect.db"
    init_store(db_path)
    reservation = _reserve_test_request(db_path, request_id="turn-send-start")
    invalid_effect = command_pending_turn_terminal_effect(
        host_id="host-a",
        worker={"id": "worker-public", "name": "Worker"},
        request_id="turn-send-start",
        instruction_text="",
    )
    with pytest.raises(
        store_sqlite.StoreSchemaError,
        match="command_pending_turn_terminal_effect_failed",
    ):
        mark_command_send_started(
            db_path,
            host_id="host-a",
            request_id="turn-send-start",
            canonical_fingerprint="send_instruction:turn-send-start",
            owner_token=reservation["owner_token"],
            binding_fingerprint="private-binding",
            send_started_effect=invalid_effect,
            now="2026-01-01T00:00:01+00:00",
        )
    assert get_command_request(
        db_path,
        "host-a",
        "turn-send-start",
    )["state"] == "reserved"
    assert turns_payload_from_store(db_path, "host-a")["turns"] == []

    valid_effect = command_pending_turn_terminal_effect(
        host_id="host-a",
        worker={"id": "worker-public", "name": "Worker"},
        request_id="turn-send-start",
        instruction_text="Continue safely.",
    )
    started = mark_command_send_started(
        db_path,
        host_id="host-a",
        request_id="turn-send-start",
        canonical_fingerprint="send_instruction:turn-send-start",
        owner_token=reservation["owner_token"],
        binding_fingerprint="private-binding",
        send_started_effect=valid_effect,
        now="2026-01-01T00:00:01+00:00",
    )
    assert started["status"] == "send_started"
    assert started["effect_result"]["origin_command_id"] == "turn-send-start"
    turns = turns_payload_from_store(
        db_path,
        "host-a",
        claim_hard_ttl_seconds=1_000_000_000,
    )["turns"]
    assert len(turns) == 1
    assert turns[0]["id"] == started["effect_result"]["id"]


def test_backend_pending_choice_terminal_effect_is_atomic_with_acceptance(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "pending-choice-effect.db"
    init_store(db_path)
    reservation = _reserve_test_request(
        db_path,
        request_id="choice-effect",
        action="answer_pending",
        fingerprint="choice-effect-fingerprint",
    )
    started = mark_command_send_started(
        db_path,
        host_id="host-a",
        request_id="choice-effect",
        canonical_fingerprint="choice-effect-fingerprint",
        owner_token=reservation["owner_token"],
        binding_fingerprint="private-binding",
        now="2026-01-01T00:00:01+00:00",
    )
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            """
            INSERT INTO backend_pending (
                host_id, worker_id, payload_json, observed_at, revision_digest,
                choice_routes_json, binding_private_fingerprint,
                observed_turn_target_value, observation_state, freshness,
                last_success_at, last_failure_at, grace_deadline, updated_at
            ) VALUES (
                'host-a', 'worker-public', '{"kind":"approval"}',
                '2026-01-01T00:00:00+00:00', 'revision',
                '{"choice":1}', 'private-binding', 'private-target',
                'open', 'fresh', '2026-01-01T00:00:00+00:00', NULL, NULL,
                '2026-01-01T00:00:00+00:00'
            )
            """
        )
        conn.execute(
            """
            INSERT INTO backend_pending_claims (
                host_id, worker_id, claim_token, revision_digest, choice_id,
                picker_ordinal, worker_fingerprint,
                binding_private_fingerprint, turn_target_value, state,
                claimed_at, send_started_at
            ) VALUES (
                'host-a', 'worker-public', 'claim-token', 'revision', 'choice',
                1, 'worker-fingerprint', 'private-binding', 'private-target',
                'send_started', '2026-01-01T00:00:00+00:00',
                '2026-01-01T00:00:01+00:00'
            )
            """
        )
    effect = backend_pending_choice_terminal_effect(
        host_id="host-a",
        claim_token="claim-token",
        accepted=True,
    )
    result = finish_command_request(
        db_path,
        host_id="host-a",
        request_id="choice-effect",
        canonical_fingerprint="choice-effect-fingerprint",
        owner_token=started["owner_token"],
        expected_state="send_started",
        terminal_state="accepted",
        status="accepted",
        result_json='{"ok":true}',
        terminal_effect=effect,
        now="2026-01-01T00:00:02+00:00",
    )
    assert result["status"] == "accepted"
    with sqlite3.connect(str(db_path)) as conn:
        assert conn.execute(
            "SELECT observation_state, payload_json FROM backend_pending"
        ).fetchone() == ("none", "{}")
        assert conn.execute(
            "SELECT COUNT(*) FROM backend_pending_claims"
        ).fetchone()[0] == 0

    missing_reservation = _reserve_test_request(
        db_path,
        request_id="missing-choice",
        action="answer_pending",
        fingerprint="missing-choice-fingerprint",
        now="2026-01-01T00:01:00+00:00",
    )
    missing_started = mark_command_send_started(
        db_path,
        host_id="host-a",
        request_id="missing-choice",
        canonical_fingerprint="missing-choice-fingerprint",
        owner_token=missing_reservation["owner_token"],
        binding_fingerprint="private-binding",
        now="2026-01-01T00:01:01+00:00",
    )
    with pytest.raises(
        store_sqlite.StoreSchemaError,
        match="backend_pending_choice_terminal_effect_failed",
    ):
        finish_command_request(
            db_path,
            host_id="host-a",
            request_id="missing-choice",
            canonical_fingerprint="missing-choice-fingerprint",
            owner_token=missing_started["owner_token"],
            expected_state="send_started",
            terminal_state="accepted",
            status="accepted",
            result_json='{"ok":true}',
            terminal_effect=backend_pending_choice_terminal_effect(
                host_id="host-a",
                claim_token="missing-claim",
                accepted=True,
            ),
            now="2026-01-01T00:01:02+00:00",
        )
    assert get_command_request(
        db_path, "host-a", "missing-choice"
    )["state"] == "send_started"


@pytest.mark.parametrize(
    ("terminal_state", "status"),
    [
        ("rejected", "backend_rejected"),
        ("uncertain", "request_state_uncertain"),
    ],
)
def test_store_terminal_replay_atomically_inserts_only_when_request_row_is_missing(
    tmp_path: Path,
    terminal_state: str,
    status: str,
) -> None:
    db_path = tmp_path / f"terminal-replay-{terminal_state}.db"
    init_store(db_path)
    request_id = f"terminal-replay-{terminal_state}"
    fingerprint = f"send_instruction:{request_id}"
    canonical_json = json.dumps(
        {"action": "send_instruction", "request": request_id},
        sort_keys=True,
        separators=(",", ":"),
    )
    inserted = reserve_terminal_command_replay(
        db_path,
        host_id="host-a",
        request_id=request_id,
        action="send_instruction",
        canonical_version=1,
        canonical_fingerprint=fingerprint,
        canonical_request_json=canonical_json,
        public_worker_id="worker-public",
        terminal_state=terminal_state,
        status=status,
        result_json=json.dumps({"ok": False, "status": status}),
        now="2026-01-01T00:00:00+00:00",
    )
    assert inserted["status"] == terminal_state
    assert inserted["owner_token"] is None
    assert inserted["receipt"]["state"] == terminal_state
    assert inserted["receipt"]["status"] == status
    assert inserted["receipt"]["terminal_at"] == "2026-01-01T00:00:00+00:00"

    replay = reserve_terminal_command_replay(
        db_path,
        host_id="host-a",
        request_id=request_id,
        action="send_instruction",
        canonical_version=1,
        canonical_fingerprint=fingerprint,
        canonical_request_json=canonical_json,
        public_worker_id="worker-public",
        terminal_state=terminal_state,
        status=status,
        result_json='{"must":"not overwrite"}',
        now="2026-01-02T00:00:00+00:00",
    )
    assert replay["status"] == "terminal"
    assert replay["receipt"] == inserted["receipt"]

    conflict = reserve_terminal_command_replay(
        db_path,
        host_id="host-a",
        request_id=request_id,
        action="send_instruction",
        canonical_version=1,
        canonical_fingerprint=f"{fingerprint}:changed",
        canonical_request_json=json.dumps(
            {"action": "send_instruction", "request": f"{request_id}:changed"},
            sort_keys=True,
            separators=(",", ":"),
        ),
        public_worker_id="worker-public",
        terminal_state=terminal_state,
        status=status,
        result_json='{"must":"not overwrite"}',
        now="2026-01-02T00:00:01+00:00",
    )
    assert conflict["status"] == "request_id_conflict"
    assert conflict["receipt"] == inserted["receipt"]

    reserved_id = f"existing-reserved-{terminal_state}"
    reserved = _reserve_test_request(
        db_path,
        request_id=reserved_id,
        now="2026-01-01T00:01:00+00:00",
    )
    existing = reserve_terminal_command_replay(
        db_path,
        host_id="host-a",
        request_id=reserved_id,
        action="send_instruction",
        canonical_version=1,
        canonical_fingerprint=f"send_instruction:{reserved_id}",
        canonical_request_json=json.dumps(
            {"action": "send_instruction", "request": reserved_id},
            sort_keys=True,
            separators=(",", ":"),
        ),
        public_worker_id="worker-public",
        terminal_state=terminal_state,
        status=status,
        result_json=json.dumps({"ok": False, "status": status}),
        now="2026-01-01T00:01:01+00:00",
    )
    assert existing["status"] == "in_progress"
    assert existing["owner_token"] is None
    assert existing["receipt"]["state"] == "reserved"
    started = mark_command_send_started(
        db_path,
        host_id="host-a",
        request_id=reserved_id,
        canonical_fingerprint=f"send_instruction:{reserved_id}",
        owner_token=reserved["owner_token"],
        binding_fingerprint="private-binding",
        now="2026-01-01T00:01:02+00:00",
    )
    assert started["status"] == "send_started"


def test_store_expired_exact_owner_can_still_start_or_consume_without_takeover(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "expired-owner.db"
    init_store(db_path)
    send_reservation = _reserve_test_request(
        db_path,
        request_id="expired-send-owner",
        now="2026-01-01T00:00:00+00:00",
        lease_seconds=10,
    )
    started = mark_command_send_started(
        db_path,
        host_id="host-a",
        request_id="expired-send-owner",
        canonical_fingerprint="send_instruction:expired-send-owner",
        owner_token=send_reservation["owner_token"],
        binding_fingerprint="private-binding",
        now="2026-01-01T00:00:11+00:00",
    )
    assert started["status"] == "send_started"
    assert started["owner_token"] == send_reservation["owner_token"]

    terminal_reservation = _reserve_test_request(
        db_path,
        request_id="expired-terminal-owner",
        now="2026-01-01T00:00:00+00:00",
        lease_seconds=10,
    )
    consumed = finish_command_request(
        db_path,
        host_id="host-a",
        request_id="expired-terminal-owner",
        canonical_fingerprint="send_instruction:expired-terminal-owner",
        owner_token=terminal_reservation["owner_token"],
        expected_state="reserved",
        terminal_state="uncertain",
        status="request_state_uncertain",
        result_json='{"ok":false,"status":"request_state_uncertain"}',
        now="2026-01-01T00:00:11+00:00",
    )
    assert consumed["status"] == "uncertain"
    assert consumed["receipt"]["state"] == "uncertain"
    assert _reserve_test_request(
        db_path,
        request_id="expired-terminal-owner",
        now="2026-02-01T00:00:00+00:00",
    )["status"] == "terminal"


def test_store_expired_reserved_takeover_rotates_owner_fence(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "lease.db"
    init_store(db_path)
    first = _reserve_test_request(
        db_path,
        request_id="leased",
        now="2026-01-01T00:00:00+00:00",
        lease_seconds=10,
    )
    second = _reserve_test_request(
        db_path,
        request_id="leased",
        now="2026-01-01T00:00:11+00:00",
        lease_seconds=10,
    )
    assert second["status"] == "reserved"
    assert second["owner_token"] != first["owner_token"]
    with sqlite3.connect(str(db_path)) as conn:
        stored_hash = conn.execute(
            "SELECT owner_token_hash FROM command_receipts "
            "WHERE host_id = 'host-a' AND request_id = 'leased'"
        ).fetchone()[0]
    assert stored_hash == store_sqlite._owner_token_hash(second["owner_token"])
    assert stored_hash != store_sqlite._owner_token_hash(first["owner_token"])

    stale_start = mark_command_send_started(
        db_path,
        host_id="host-a",
        request_id="leased",
        canonical_fingerprint="send_instruction:leased",
        owner_token=first["owner_token"],
        binding_fingerprint="private-old",
        now="2026-01-01T00:00:12+00:00",
    )
    assert stale_start["status"] == "not_owner"
    stale_finish = finish_command_request(
        db_path,
        host_id="host-a",
        request_id="leased",
        canonical_fingerprint="send_instruction:leased",
        owner_token=first["owner_token"],
        expected_state="reserved",
        terminal_state="rejected",
        status="backend_rejected",
        result_json='{"ok":false}',
        now="2026-01-01T00:00:12+00:00",
    )
    assert stale_finish["status"] == "not_owner"

    started = mark_command_send_started(
        db_path,
        host_id="host-a",
        request_id="leased",
        canonical_fingerprint="send_instruction:leased",
        owner_token=second["owner_token"],
        binding_fingerprint="private-new",
        now="2026-01-01T00:00:12+00:00",
    )
    assert started["status"] == "send_started"
    accepted = finish_command_request(
        db_path,
        host_id="host-a",
        request_id="leased",
        canonical_fingerprint="send_instruction:leased",
        owner_token=second["owner_token"],
        expected_state="send_started",
        terminal_state="accepted",
        status="accepted",
        result_json='{"ok":true}',
        now="2026-01-01T00:00:13+00:00",
    )
    assert accepted["status"] == "accepted"
    assert _reserve_test_request(
        db_path,
        request_id="leased",
        now="2026-02-01T00:00:00+00:00",
    )["status"] == "terminal"


def test_store_terminal_consumption_linearizes_before_expired_takeover(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "terminal-takeover-race.db"
    init_store(db_path)
    reservation = _reserve_test_request(
        db_path,
        request_id="terminal-takeover-race",
        now="2026-01-01T00:00:00+00:00",
        lease_seconds=10,
    )
    finish_has_lock = threading.Barrier(2)
    release_finish = threading.Barrier(2)
    takeover_connected = threading.Barrier(2)
    original_row = store_sqlite._command_request_row
    original_connect = store_sqlite._connect
    finish_paused = threading.Event()
    results: dict[str, dict[str, Any]] = {}
    errors: list[BaseException] = []

    def interleaved_row(
        conn: sqlite3.Connection,
        host_id: str,
        request_id: str,
    ) -> Any:
        row = original_row(conn, host_id, request_id)
        if (
            threading.current_thread().name == "terminal-replay"
            and not finish_paused.is_set()
        ):
            finish_paused.set()
            finish_has_lock.wait(timeout=5)
            release_finish.wait(timeout=5)
        return row

    def interleaved_connect(*args: Any, **kwargs: Any) -> sqlite3.Connection:
        conn = original_connect(*args, **kwargs)
        if threading.current_thread().name == "expired-takeover":
            takeover_connected.wait(timeout=5)
        return conn

    monkeypatch.setattr(store_sqlite, "_command_request_row", interleaved_row)
    monkeypatch.setattr(store_sqlite, "_connect", interleaved_connect)

    def consume_terminal() -> None:
        try:
            results["finish"] = finish_command_request(
                db_path,
                host_id="host-a",
                request_id="terminal-takeover-race",
                canonical_fingerprint="send_instruction:terminal-takeover-race",
                owner_token=reservation["owner_token"],
                expected_state="reserved",
                terminal_state="uncertain",
                status="request_state_uncertain",
                result_json='{"ok":false,"status":"request_state_uncertain"}',
                now="2026-01-01T00:00:11+00:00",
            )
        except BaseException as exc:  # pragma: no cover - asserted below
            errors.append(exc)

    def attempt_takeover() -> None:
        try:
            results["takeover"] = _reserve_test_request(
                db_path,
                request_id="terminal-takeover-race",
                now="2026-01-01T00:00:11+00:00",
                lease_seconds=10,
            )
        except BaseException as exc:  # pragma: no cover - asserted below
            errors.append(exc)

    finish_thread = threading.Thread(target=consume_terminal, name="terminal-replay")
    finish_thread.start()
    finish_has_lock.wait(timeout=5)
    takeover_thread = threading.Thread(target=attempt_takeover, name="expired-takeover")
    takeover_thread.start()
    takeover_connected.wait(timeout=5)
    release_finish.wait(timeout=5)
    finish_thread.join(timeout=5)
    takeover_thread.join(timeout=5)

    assert not finish_thread.is_alive()
    assert not takeover_thread.is_alive()
    assert errors == []
    assert results["finish"]["status"] == "uncertain"
    assert results["takeover"]["status"] == "terminal"
    assert results["takeover"]["owner_token"] is None
    receipt = get_command_request(db_path, "host-a", "terminal-takeover-race")
    assert receipt is not None
    assert receipt["state"] == "uncertain"
    assert _reserve_test_request(
        db_path,
        request_id="terminal-takeover-race",
        now="2026-02-01T00:00:00+00:00",
    )["status"] == "terminal"


def test_store_command_reservation_allows_one_concurrent_owner(tmp_path: Path) -> None:
    db_path = tmp_path / "race.db"
    init_store(db_path)
    barrier = threading.Barrier(2)
    results: list[dict[str, Any]] = []
    lock = threading.Lock()

    def attempt() -> None:
        barrier.wait(timeout=5)
        result = _reserve_test_request(db_path, request_id="race")
        with lock:
            results.append(result)

    threads = [threading.Thread(target=attempt) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=5)

    assert not any(thread.is_alive() for thread in threads)
    assert sorted(result["status"] for result in results) == [
        "in_progress",
        "reserved",
    ]
    assert sum(result["owner_token"] is not None for result in results) == 1
    with sqlite3.connect(str(db_path)) as conn:
        assert conn.execute("SELECT COUNT(*) FROM command_receipts").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM commands").fetchone()[0] == 1


def test_store_v11_host_request_collision_migrates_to_uncertain_tombstone(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "legacy-collision.db"
    with sqlite3.connect(str(db_path)) as conn:
        conn.executescript(
            store_sqlite.CREATE_LEGACY_COMMAND_RECEIPTS_TABLE
            + store_sqlite.CREATE_LEGACY_COMMANDS_TABLE
        )
        for index, (action, fingerprint) in enumerate(
            (
                ("send_instruction", "send-fingerprint"),
                ("answer_pending", "answer-fingerprint"),
            )
        ):
            created = f"2026-01-01T00:00:0{index}+00:00"
            conn.execute(
                """
                INSERT INTO command_receipts (
                    host_id, request_id, action, payload_fingerprint, status,
                    result_json, created_at, completed_at, uncertain
                ) VALUES (?, 'collision', ?, ?, 'accepted', ?, ?, ?, 0)
                """,
                (
                    "host-a",
                    action,
                    fingerprint,
                    '{"ok":true,"private":"must-not-survive"}',
                    created,
                    created,
                ),
            )
            conn.execute(
                """
                INSERT INTO commands (
                    host_id, request_id, action, payload_fingerprint, status,
                    dry_run, uncertain, request_json, result_json, created_at,
                    reserved_at, completed_at, updated_at
                ) VALUES (?, 'collision', ?, ?, 'accepted', 0, 0, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "host-a",
                    action,
                    fingerprint,
                    '{"target":{"worker_id":"public","worker_fingerprint":"private"}}',
                    '{"ok":true,"private":"must-not-survive"}',
                    created,
                    created,
                    created,
                    created,
                ),
            )
        conn.execute("PRAGMA user_version = 11")

    init_store(db_path)
    receipt = get_command_request(db_path, "host-a", "collision")
    assert receipt is not None
    assert receipt["state"] == "uncertain"
    assert receipt["status"] == "request_state_uncertain"
    assert receipt["legacy_collision"] is True
    assert receipt["legacy_collision_count"] == 4
    assert receipt["canonical_request_json"] == "{}"
    assert "must-not-survive" not in receipt["result_json"]
    assert _reserve_test_request(
        db_path,
        request_id="collision",
        fingerprint="new-fingerprint",
    )["status"] == "terminal"
    with sqlite3.connect(str(db_path)) as conn:
        assert conn.execute(
            "SELECT state, legacy_collision FROM commands"
        ).fetchone() == ("uncertain", 1)
        assert conn.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        conn.execute(store_sqlite.CREATE_EVENTS_TABLE)

    _accept_test_request(
        db_path,
        request_id="newer-than-collision",
        now="2026-02-02T00:00:00+00:00",
    )
    cleanup = cleanup_command_request_retention(
        db_path,
        retry_horizon_seconds=604_800,
        retention_seconds=2_592_000,
        retention_count=1,
        now="2026-03-05T00:00:00+00:00",
    )
    assert cleanup["deleted"] == 1
    assert get_command_request(db_path, "host-a", "collision") is None
    assert get_command_request(
        db_path, "host-a", "newer-than-collision"
    ) is not None



def test_store_v11_noncollision_replays_only_exact_legacy_raw_fingerprint(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "legacy-noncollision.db"
    legacy_fingerprint = "legacy-raw-payload-fingerprint"
    canonical_json = '{"action":"send_instruction","worker_id":"worker-public"}'
    with sqlite3.connect(str(db_path)) as conn:
        conn.executescript(
            store_sqlite.CREATE_LEGACY_COMMAND_RECEIPTS_TABLE
            + store_sqlite.CREATE_LEGACY_COMMANDS_TABLE
        )
        conn.execute(
            """
            INSERT INTO command_receipts (
                host_id, request_id, action, payload_fingerprint, status,
                result_json, created_at, completed_at, uncertain
            ) VALUES (
                'host-a', 'legacy-exact', 'send_instruction', ?, 'accepted',
                '{"ok":true,"status":"accepted"}',
                '2026-01-01T00:00:00+00:00',
                '2026-01-01T00:00:01+00:00', 0
            )
            """,
            (legacy_fingerprint,),
        )
        conn.execute(
            """
            INSERT INTO commands (
                host_id, request_id, action, payload_fingerprint, status,
                dry_run, uncertain, request_json, result_json, created_at,
                reserved_at, completed_at, updated_at
            ) VALUES (
                'host-a', 'legacy-exact', 'send_instruction', ?, 'accepted',
                0, 0, '{"target":{"worker_id":"worker-public"}}',
                '{"ok":true,"status":"accepted"}',
                '2026-01-01T00:00:00+00:00',
                '2026-01-01T00:00:00+00:00',
                '2026-01-01T00:00:01+00:00',
                '2026-01-01T00:00:01+00:00'
            )
            """,
            (legacy_fingerprint,),
        )
        conn.execute("PRAGMA user_version = 11")

    init_store(db_path)
    canonical_only = reserve_command_request(
        db_path,
        host_id="host-a",
        request_id="legacy-exact",
        action="send_instruction",
        canonical_version=1,
        canonical_fingerprint=legacy_fingerprint,
        canonical_request_json=canonical_json,
        public_worker_id="worker-public",
        pending_result_json='{"ok":false,"status":"pending"}',
    )
    assert canonical_only["status"] == "request_id_conflict"

    exact_replay = reserve_command_request(
        db_path,
        host_id="host-a",
        request_id="legacy-exact",
        action="send_instruction",
        canonical_version=1,
        canonical_fingerprint="new-canonical-fingerprint",
        canonical_request_json=canonical_json,
        public_worker_id="worker-public",
        pending_result_json='{"ok":false,"status":"pending"}',
        legacy_raw_payload_fingerprint=legacy_fingerprint,
    )
    assert exact_replay["status"] == "terminal"
    assert exact_replay["receipt"]["canonical_version"] == 0
    assert exact_replay["receipt"]["canonical_fingerprint"] == legacy_fingerprint
    assert exact_replay["receipt"]["result_json"] == (
        '{"ok":true,"status":"accepted"}'
    )

    wrong_raw = reserve_command_request(
        db_path,
        host_id="host-a",
        request_id="legacy-exact",
        action="send_instruction",
        canonical_version=1,
        canonical_fingerprint="new-canonical-fingerprint",
        canonical_request_json=canonical_json,
        public_worker_id="worker-public",
        pending_result_json='{"ok":false,"status":"pending"}',
        legacy_raw_payload_fingerprint="different-legacy-raw-fingerprint",
    )
    wrong_action = reserve_command_request(
        db_path,
        host_id="host-a",
        request_id="legacy-exact",
        action="answer_pending",
        canonical_version=1,
        canonical_fingerprint="new-canonical-fingerprint",
        canonical_request_json=canonical_json,
        public_worker_id="worker-public",
        pending_result_json='{"ok":false,"status":"pending"}',
        legacy_raw_payload_fingerprint=legacy_fingerprint,
    )
    assert wrong_raw["status"] == "request_id_conflict"
    assert wrong_action["status"] == "request_id_conflict"

def test_store_v11_receipt_audit_disagreement_fails_closed(tmp_path: Path) -> None:
    db_path = tmp_path / "legacy-disagreement.db"
    with sqlite3.connect(str(db_path)) as conn:
        conn.executescript(
            store_sqlite.CREATE_LEGACY_COMMAND_RECEIPTS_TABLE
            + store_sqlite.CREATE_LEGACY_COMMANDS_TABLE
        )
        conn.execute(
            """
            INSERT INTO command_receipts (
                host_id, request_id, action, payload_fingerprint, status,
                result_json, created_at, completed_at, uncertain
            ) VALUES (
                'host-a', 'disagree', 'send_instruction', 'same-fingerprint',
                'accepted', '{"ok":true}', '2026-01-01T00:00:00+00:00',
                '2026-01-01T00:00:01+00:00', 0
            )
            """
        )
        conn.execute(
            """
            INSERT INTO commands (
                host_id, request_id, action, payload_fingerprint, status,
                dry_run, uncertain, request_json, result_json, created_at,
                reserved_at, completed_at, updated_at
            ) VALUES (
                'host-a', 'disagree', 'send_instruction', 'same-fingerprint',
                'backend_failed', 0, 0, '{}', '{"ok":false}',
                '2026-01-01T00:00:00+00:00',
                '2026-01-01T00:00:00+00:00',
                '2026-01-01T00:00:01+00:00',
                '2026-01-01T00:00:01+00:00'
            )
            """
        )
        conn.execute("PRAGMA user_version = 11")
    init_store(db_path)
    receipt = get_command_request(db_path, "host-a", "disagree")
    assert receipt is not None
    assert receipt["state"] == "uncertain"
    assert receipt["legacy_collision"] is True


def test_store_command_retention_obeys_age_count_host_and_batch_floors(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "retention.db"
    init_store(db_path)
    for host_id in ("host-a", "host-b"):
        for index in range(4):
            _accept_test_request(
                db_path,
                host_id=host_id,
                request_id=f"{host_id}-old-{index}",
                now=f"2026-01-0{index + 1}T00:00:00+00:00",
            )
    _accept_test_request(
        db_path,
        host_id="host-a",
        request_id="host-a-inside-floor",
        now="2026-01-30T00:00:00+00:00",
    )
    uncertain_reservation = _reserve_test_request(
        db_path,
        request_id="uncertain",
        now="2026-01-01T00:00:00+00:00",
    )
    finish_command_request(
        db_path,
        host_id="host-a",
        request_id="uncertain",
        canonical_fingerprint="send_instruction:uncertain",
        owner_token=uncertain_reservation["owner_token"],
        expected_state="reserved",
        terminal_state="uncertain",
        status="request_state_uncertain",
        result_json='{"evidence":"keep"}',
        now="2026-01-01T00:00:01+00:00",
    )

    dry_run = cleanup_command_request_retention(
        db_path,
        retry_horizon_seconds=604_800,
        retention_seconds=691_200,
        retention_count=2,
        host_id="host-a",
        now="2026-02-01T00:00:00+00:00",
        dry_run=True,
        batch_size=1,
    )
    assert dry_run["deleted"] == 1
    assert dry_run["remaining_candidates"] is True
    assert get_command_request(db_path, "host-a", "host-a-old-0") is not None

    first = cleanup_command_request_retention(
        db_path,
        retry_horizon_seconds=604_800,
        retention_seconds=691_200,
        retention_count=2,
        host_id="host-a",
        now="2026-02-01T00:00:00+00:00",
        batch_size=1,
    )
    second = cleanup_command_request_retention(
        db_path,
        retry_horizon_seconds=604_800,
        retention_seconds=691_200,
        retention_count=2,
        host_id="host-a",
        now="2026-02-01T00:00:00+00:00",
        batch_size=1,
    )
    assert (first["deleted"], second["deleted"]) == (1, 1)
    assert get_command_request(db_path, "host-a", "host-a-inside-floor") is not None
    assert get_command_request(db_path, "host-a", "uncertain") is None
    assert all(
        get_command_request(db_path, "host-b", f"host-b-old-{index}") is not None
        for index in range(4)
    )
    with sqlite3.connect(str(db_path)) as conn:
        receipt_keys = {
            (row[0], row[1])
            for row in conn.execute(
                "SELECT host_id, request_id FROM command_receipts"
            ).fetchall()
        }
        command_keys = {
            (row[0], row[1])
            for row in conn.execute(
                "SELECT host_id, request_id FROM commands"
            ).fetchall()
        }
        assert receipt_keys == command_keys
        assert conn.execute("PRAGMA integrity_check").fetchone()[0] == "ok"


def test_store_command_retention_rejects_equal_retry_and_retention_horizon(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "retention-equal-horizon.db"
    init_store(db_path)
    _reserve_test_request(
        db_path,
        request_id="must-remain",
        now="2026-01-01T00:00:00+00:00",
    )

    result = cleanup_command_request_retention(
        db_path,
        retry_horizon_seconds=604_800,
        retention_seconds=604_800,
        retention_count=1,
        now="2026-02-01T00:00:00+00:00",
    )
    assert result["ok"] is False
    assert result["status"] == "invalid_policy"
    assert result["deleted"] == 0
    assert get_command_request(db_path, "host-a", "must-remain") is not None


def test_store_retention_reacquires_expired_reserved_without_pre_send_suppression(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "retention-reserved-retry.db"
    init_store(db_path)
    first = _reserve_test_request(
        db_path,
        request_id="stale-reserved",
        now="2026-01-01T00:00:00+00:00",
    )

    at_retry_horizon = cleanup_command_request_retention(
        db_path,
        retry_horizon_seconds=604_800,
        retention_seconds=2_592_000,
        retention_count=1,
        now="2026-01-08T00:00:00+00:00",
    )
    after_retry_horizon = cleanup_command_request_retention(
        db_path,
        retry_horizon_seconds=604_800,
        retention_seconds=2_592_000,
        retention_count=1,
        now="2026-01-08T00:00:01+00:00",
    )
    assert at_retry_horizon["stale_active"] == 0
    assert after_retry_horizon["stale_active"] == 0
    assert after_retry_horizon["deleted"] == 0
    assert get_command_request(
        db_path, "host-a", "stale-reserved"
    )["state"] == "reserved"

    second = _reserve_test_request(
        db_path,
        request_id="stale-reserved",
        now="2026-01-08T00:00:02+00:00",
    )
    assert second["status"] == "reserved"
    assert second["owner_token"] != first["owner_token"]
    assert mark_command_send_started(
        db_path,
        host_id="host-a",
        request_id="stale-reserved",
        canonical_fingerprint="send_instruction:stale-reserved",
        owner_token=first["owner_token"],
        binding_fingerprint="stale-private-binding",
        now="2026-01-08T00:00:03+00:00",
    )["status"] == "not_owner"
    started = mark_command_send_started(
        db_path,
        host_id="host-a",
        request_id="stale-reserved",
        canonical_fingerprint="send_instruction:stale-reserved",
        owner_token=second["owner_token"],
        binding_fingerprint="current-private-binding",
        now="2026-01-08T00:00:03+00:00",
    )
    assert started["status"] == "send_started"
    terminal = finish_command_request(
        db_path,
        host_id="host-a",
        request_id="stale-reserved",
        canonical_fingerprint="send_instruction:stale-reserved",
        owner_token=second["owner_token"],
        expected_state="send_started",
        terminal_state="accepted",
        status="accepted",
        result_json='{"ok":true,"status":"accepted"}',
        now="2026-01-08T00:00:04+00:00",
    )
    assert terminal["status"] == "accepted"


def test_store_retention_sanitizes_stale_send_started_then_bounds_uncertainty(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "retention-send-started.db"
    init_store(db_path)
    reservation = reserve_command_request(
        db_path,
        host_id="host-a",
        request_id="stale-send",
        action="send_instruction",
        canonical_version=1,
        canonical_fingerprint="stale-send-fingerprint",
        canonical_request_json='{"action":"send_instruction"}',
        public_worker_id="worker-public",
        pending_result_json='{"private":"must-not-survive"}',
        now="2026-01-01T00:00:00+00:00",
    )
    assert mark_command_send_started(
        db_path,
        host_id="host-a",
        request_id="stale-send",
        canonical_fingerprint="stale-send-fingerprint",
        owner_token=reservation["owner_token"],
        binding_fingerprint="private-binding",
        now="2026-01-01T00:00:01+00:00",
    )["status"] == "send_started"

    exact_retry_boundary = cleanup_command_request_retention(
        db_path,
        retry_horizon_seconds=604_800,
        retention_seconds=2_592_000,
        retention_count=1,
        now="2026-01-08T00:00:01+00:00",
    )
    assert exact_retry_boundary["stale_active"] == 0
    assert get_command_request(db_path, "host-a", "stale-send")["state"] == (
        "send_started"
    )

    converted = cleanup_command_request_retention(
        db_path,
        retry_horizon_seconds=604_800,
        retention_seconds=2_592_000,
        retention_count=1,
        now="2026-01-08T00:00:02+00:00",
    )
    assert converted["stale_active"] == 1
    assert converted["deleted"] == 0
    receipt = get_command_request(db_path, "host-a", "stale-send")
    assert receipt is not None
    assert receipt["state"] == "uncertain"
    assert receipt["result_json"] == (
        '{"ok":false,"status":"request_state_uncertain"}'
    )
    with sqlite3.connect(str(db_path)) as conn:
        assert conn.execute(
            "SELECT result_json FROM commands "
            "WHERE host_id = 'host-a' AND request_id = 'stale-send'"
        ).fetchone()[0] == '{"ok":false,"status":"request_state_uncertain"}'
        public_events = json.dumps(
            [
                row[0]
                for row in conn.execute(
                    "SELECT payload_json FROM events "
                    "WHERE host_id = 'host-a' "
                    "AND aggregate_type = 'command_request'"
                ).fetchall()
            ]
        )
    assert "must-not-survive" not in public_events

    _accept_test_request(
        db_path,
        request_id="newer-terminal",
        now="2026-01-09T00:00:00+00:00",
    )
    exact_retention_boundary = cleanup_command_request_retention(
        db_path,
        retry_horizon_seconds=604_800,
        retention_seconds=2_592_000,
        retention_count=1,
        now="2026-02-07T00:00:02+00:00",
    )
    assert exact_retention_boundary["deleted"] == 0
    assert get_command_request(db_path, "host-a", "stale-send") is not None

    beyond_retention = cleanup_command_request_retention(
        db_path,
        retry_horizon_seconds=604_800,
        retention_seconds=2_592_000,
        retention_count=1,
        now="2026-02-07T00:00:03+00:00",
    )
    assert beyond_retention["deleted"] == 1
    assert get_command_request(db_path, "host-a", "stale-send") is None
    assert get_command_request(db_path, "host-a", "newer-terminal") is not None


def test_store_retention_bounds_expired_pre_send_only_beyond_age_and_count(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "retention-expired-pre-send.db"
    init_store(db_path)
    _reserve_test_request(
        db_path,
        request_id="old-expired",
        now="2026-01-01T00:00:00+00:00",
    )
    _reserve_test_request(
        db_path,
        request_id="newer-expired",
        now="2026-01-02T00:00:00+00:00",
    )
    _reserve_test_request(
        db_path,
        request_id="active-lease",
        now="2026-01-31T00:00:00+00:00",
    )

    exact_boundary = cleanup_command_request_retention(
        db_path,
        retry_horizon_seconds=604_800,
        retention_seconds=2_592_000,
        retention_count=1,
        now="2026-01-31T00:00:00+00:00",
        batch_size=10,
    )
    assert exact_boundary["stale_active"] == 0
    assert exact_boundary["deleted"] == 0
    assert get_command_request(db_path, "host-a", "old-expired") is not None

    beyond_boundary = cleanup_command_request_retention(
        db_path,
        retry_horizon_seconds=604_800,
        retention_seconds=2_592_000,
        retention_count=1,
        now="2026-01-31T00:00:01+00:00",
        batch_size=10,
    )
    assert beyond_boundary["deleted"] == 1
    assert get_command_request(db_path, "host-a", "old-expired") is None
    assert get_command_request(db_path, "host-a", "newer-expired") is not None
    assert get_command_request(db_path, "host-a", "active-lease") is not None


def test_store_retention_reports_terminal_work_when_send_started_batch_is_full(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "retention-exact-batch.db"
    init_store(db_path)
    reservation = _reserve_test_request(
        db_path,
        request_id="stale-active",
        now="2026-01-01T00:00:00+00:00",
    )
    assert mark_command_send_started(
        db_path,
        host_id="host-a",
        request_id="stale-active",
        canonical_fingerprint="send_instruction:stale-active",
        owner_token=reservation["owner_token"],
        binding_fingerprint="private-binding",
        now="2026-01-01T00:00:01+00:00",
    )["status"] == "send_started"
    for index in range(2):
        _accept_test_request(
            db_path,
            request_id=f"terminal-{index}",
            now=f"2026-01-0{index + 1}T00:00:00+00:00",
        )
    result = cleanup_command_request_retention(
        db_path,
        retry_horizon_seconds=604_800,
        retention_seconds=691_200,
        retention_count=1,
        now="2026-02-01T00:00:00+00:00",
        batch_size=1,
    )
    assert result["stale_active"] == 1
    assert result["deleted"] == 0
    assert result["remaining_candidates"] is True
    receipt = get_command_request(db_path, "host-a", "stale-active")
    assert receipt is not None
    assert receipt["state"] == "uncertain"
    assert receipt["result_json"] == (
        '{"ok":false,"status":"request_state_uncertain"}'
    )


def test_manual_and_automatic_maintenance_reach_command_request_retention(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "command-maintenance.db"
    init_store(db_path)
    for host_id in ("host-a", "host-b"):
        for index in range(2):
            _accept_test_request(
                db_path,
                host_id=host_id,
                request_id=f"{host_id}-terminal-{index}",
                now=f"2026-01-0{index + 1}T00:00:00+00:00",
            )

    dry_run = run_store_maintenance(
        db_path,
        "host-a",
        retention_days=7,
        max_outbox_attempts=3,
        command_retry_horizon_seconds=604_800,
        command_receipt_retention_seconds=691_200,
        command_receipt_retention_count=1,
        now="2026-02-01T00:00:00+00:00",
        dry_run=True,
        content_batch_size=1,
    )
    assert dry_run["command_requests"]["deleted"] == 1
    assert get_command_request(
        db_path, "host-a", "host-a-terminal-0"
    ) is not None

    manual = run_store_maintenance(
        db_path,
        "host-a",
        retention_days=7,
        max_outbox_attempts=3,
        command_retry_horizon_seconds=604_800,
        command_receipt_retention_seconds=691_200,
        command_receipt_retention_count=1,
        now="2026-02-01T00:00:00+00:00",
        content_batch_size=1,
    )
    assert manual["ok"] is True
    assert manual["command_requests"]["deleted"] == 1
    assert sum(
        get_command_request(
            db_path, "host-a", f"host-a-terminal-{index}"
        ) is not None
        for index in range(2)
    ) == 1
    assert all(
        get_command_request(
            db_path, "host-b", f"host-b-terminal-{index}"
        ) is not None
        for index in range(2)
    )

    automatic = store_sqlite.maybe_run_automatic_store_maintenance(
        db_path,
        policy=store_sqlite.SnapshotRetentionPolicy(
            retention_days=30,
            retention_count=4096,
            batch_size=1,
        ),
        command_retry_horizon_seconds=604_800,
        command_receipt_retention_seconds=691_200,
        command_receipt_retention_count=1,
        now="2026-02-01T01:00:00+00:00",
    )
    assert automatic["ok"] is True
    assert automatic["command_requests"]["deleted"] == 1
    assert sum(
        get_command_request(
            db_path, "host-b", f"host-b-terminal-{index}"
        ) is not None
        for index in range(2)
    ) == 1


def test_store_status_command_request_metrics_are_aggregate_only(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "command-status.db"
    init_store(db_path)
    current = datetime.now().astimezone()
    current_at = current.isoformat(timespec="seconds")
    old_at = (current - timedelta(days=40)).isoformat(timespec="seconds")
    old_started_at = (
        current - timedelta(days=40) + timedelta(seconds=1)
    ).isoformat(timespec="seconds")
    reservation = _reserve_test_request(
        db_path,
        request_id="private-request-id",
        now=current_at,
    )
    finish_command_request(
        db_path,
        host_id="host-a",
        request_id="private-request-id",
        canonical_fingerprint="send_instruction:private-request-id",
        owner_token=reservation["owner_token"],
        expected_state="reserved",
        terminal_state="uncertain",
        status="request_state_uncertain",
        result_json='{"private":"secret-evidence"}',
        now=current_at,
    )
    _reserve_test_request(
        db_path,
        request_id="active-private-id",
        now=current_at,
    )
    _reserve_test_request(
        db_path,
        request_id="expired-private-id",
        now=old_at,
    )
    stale_send = _reserve_test_request(
        db_path,
        request_id="send-started-private-id",
        now=old_at,
    )
    assert mark_command_send_started(
        db_path,
        host_id="host-a",
        request_id="send-started-private-id",
        canonical_fingerprint="send_instruction:send-started-private-id",
        owner_token=stale_send["owner_token"],
        binding_fingerprint="private-binding",
        now=old_started_at,
    )["status"] == "send_started"

    status = store_status(
        db_path,
        "host-a",
        command_retry_horizon_seconds=604_800,
        command_receipt_retention_seconds=691_200,
        command_receipt_retention_count=1,
    )
    metrics = status["command_requests"]
    assert metrics["total"] == 4
    assert metrics["states"] == {
        "reserved": 2,
        "send_started": 1,
        "accepted": 0,
        "rejected": 0,
        "uncertain": 1,
    }
    assert metrics["stale_active"] == 1
    assert metrics["eligible"] == 1
    assert metrics["storage_pressure"] is True
    assert metrics["retry_horizon_seconds"] == 604_800
    assert metrics["retention_seconds"] == 691_200
    assert metrics["retention_count"] == 1
    assert status["maintenance"]["backlog"] is True
    public_json = json.dumps(metrics, sort_keys=True)
    assert "private-request-id" not in public_json
    assert "active-private-id" not in public_json
    assert "expired-private-id" not in public_json
    assert "send-started-private-id" not in public_json
    assert "secret-evidence" not in public_json


def test_distinct_source_turns_mint_distinct_public_turn_ids(tmp_path: Path) -> None:
    db_path = tmp_path / "source-turns.db"
    config = Config(host_id="turn-host", db_path=db_path)
    snapshot = project_from_raw(
        config,
        workers=[{"id": "worker-1", "name": "claude", "status": "active", "space_id": "space-1"}],
    )
    init_store(db_path)
    save_snapshot(db_path, snapshot)

    for index, (prompt, reply) in enumerate(
        [("first question", "first answer"), ("second question", "second answer")],
        start=1,
    ):
        merge_turn_content(
            db_path,
            "turn-host",
            "worker-1",
            {
                "user_text": prompt,
                "assistant_final_text": reply,
                "complete": True,
                "has_open_turn": False,
                "source_turn_id": f"uuid-{index}",
            },
            observed_at=f"2026-01-01T00:0{index}:00+00:00",
        )

    payload = turns_payload_from_store(db_path, "turn-host", snapshot=snapshot)
    content_turns = [t for t in payload["turns"] if t.get("assistant_final_text")]
    assert len(content_turns) == 2
    assert len({t["id"] for t in content_turns}) == 2
    # Newest first per worker; the worker's base row must not carry stale text.
    assert content_turns[0]["assistant_final_text"] == "second answer"
    base_rows = [t for t in payload["turns"] if not t.get("source_turn_id")]
    assert all(not t.get("assistant_final_text") and not t.get("user_text") for t in base_rows)

    # Same source turn observed again updates its row, keeping the id stable.
    merge_turn_content(
        db_path,
        "turn-host",
        "worker-1",
        {"assistant_final_text": "second answer, revised", "complete": True, "source_turn_id": "uuid-2"},
    )
    payload2 = turns_payload_from_store(db_path, "turn-host", snapshot=snapshot)
    revised = [t for t in payload2["turns"] if t.get("assistant_final_text") == "second answer, revised"]
    assert len(revised) == 1
    assert revised[0]["id"] in {t["id"] for t in content_turns}

    # Snapshot rewrites must not prune per-source-turn rows.
    save_snapshot(db_path, snapshot)
    payload3 = turns_payload_from_store(db_path, "turn-host", snapshot=snapshot)
    assert len([t for t in payload3["turns"] if t.get("source_turn_id")]) == 2


def test_final_identity_is_stable_neutral_and_argument_bound() -> None:
    identity = turn_final_delivery_identity(
        "host-private-value",
        "turn-public-a",
        "twrev1.revisionA",
    )

    assert identity == turn_final_delivery_identity(
        "host-private-value",
        "turn-public-a",
        "twrev1.revisionA",
    )
    assert identity.startswith("twfinal1.")
    assert "host-private-value" not in identity
    assert "turn-public-a" not in identity
    assert identity != turn_final_delivery_identity(
        "host-private-value",
        "turn-public-a",
        "twrev1.revisionB",
    )
    assert identity != turn_final_delivery_identity(
        "other-host",
        "turn-public-a",
        "twrev1.revisionA",
    )
    with pytest.raises(ValueError, match="invalid_host_id"):
        turn_final_delivery_identity("", "turn-public-a", "twrev1.revisionA")


def test_twenty_offline_source_finals_are_retained_as_unique_ready_anchors(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "source-turn-retention.db"
    config = Config(host_id="turn-host", db_path=db_path)
    snapshot = project_from_raw(
        config,
        workers=[
            {
                "id": "worker-1",
                "name": "claude",
                "status": "active",
                "space_id": "space-1",
                "meta": {
                    "stable_key": "wsk1_" + ("2" * 64),
                    "stable_key_version": 1,
                },
            }
        ],
    )
    init_store(db_path)
    save_snapshot(db_path, snapshot)
    for index in range(20):
        assert merge_turn_content(
            db_path,
            "turn-host",
            "worker-1",
            {
                "assistant_final_text": f"answer {index}",
                "complete": True,
                "source_turn_id": f"uuid-{index}",
            },
            observed_at=f"2026-01-01T00:{index:02d}:00+00:00",
        ) == 1

    payload = turns_payload_from_store(db_path, "turn-host", snapshot=snapshot)
    source_rows = [turn for turn in payload["turns"] if turn.get("source_turn_id")]
    with sqlite3.connect(str(db_path)) as conn:
        anchors = conn.execute(
            """
            SELECT delivery_key, payload_json
            FROM connector_outbox
            WHERE host_id = ?
              AND delivery_kind = 'final_ready'
              AND status = 'queued'
            ORDER BY id
            """,
            ("turn-host",),
        ).fetchall()

    assert len(source_rows) == 20
    assert len(anchors) == len({row[0] for row in anchors}) == 20
    assert source_rows[0]["assistant_final_text"] == "answer 19"
    assert all(
        row[0].startswith("turn-final:revision:twfinal1.")
        for row in anchors
    )
    encoded_anchors = "\n".join(row[1] for row in anchors)
    assert "answer 0" not in encoded_anchors
    assert "answer 19" not in encoded_anchors
    assert "source_turn_id" not in encoded_anchors


def test_turn_claim_sweeper_expires_never_observed_claim(tmp_path: Path) -> None:
    db_path = tmp_path / "turn-claim-expiry.db"
    host_id = "turn-claim-expiry-host"
    snapshot = project_from_raw(
        Config(host_id=host_id, db_path=db_path),
        workers=[
            {
                "id": "worker-1",
                "name": "Worker",
                "status": "active",
                "meta": {
                    "stable_key": "wsk1_" + ("c" * 64),
                    "stable_key_version": 1,
                },
            }
        ],
    )
    init_store(db_path)
    save_snapshot(db_path, snapshot)
    claim = store_sqlite.upsert_command_pending_turn(
        db_path,
        host_id,
        snapshot.workers[0],
        request_id="never-observed-request",
        instruction_text="never observed",
        observed_at="2026-07-19T00:00:00+00:00",
    )
    assert claim is not None

    assert store_sqlite.sweep_turn_claims(
        db_path,
        host_id,
        grace_seconds=1,
        hard_ttl_seconds=60,
        now="2026-07-19T00:02:00+00:00",
    ) == 1

    with sqlite3.connect(str(db_path)) as conn:
        raw = json.loads(
            conn.execute(
                "SELECT payload_json FROM turns WHERE host_id = ? AND turn_id = ?",
                (host_id, claim["id"]),
            ).fetchone()[0]
        )
        foreign_keys = conn.execute("PRAGMA foreign_key_check").fetchall()
    assert raw["complete"] is True
    assert raw["has_open_turn"] is False
    assert raw["status"] == "closed"
    assert raw["superseded_at"] == "2026-07-19T00:02:00+00:00"
    assert raw["superseded_by_turn_id"] is None
    assert claim["id"] not in {
        turn["id"]
        for turn in turns_payload_from_store(db_path, host_id)["turns"]
    }
    assert foreign_keys == []


def test_turn_claim_sweeper_resolves_only_one_identical_claim_per_done_turn(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "turn-claim-identical.db"
    host_id = "turn-claim-identical-host"
    snapshot = project_from_raw(
        Config(host_id=host_id, db_path=db_path),
        workers=[{"id": "worker-1", "name": "Worker", "status": "active"}],
    )
    init_store(db_path)
    save_snapshot(db_path, snapshot)
    assert merge_turn_content(
        db_path,
        host_id,
        "worker-1",
        {
            "source_turn_id": "identical-observed-source",
            "user_text": "same prompt",
            "assistant_final_text": "one answer",
            "complete": True,
            "has_open_turn": False,
        },
        observed_at="2099-07-19T00:00:00+00:00",
    ) == 1
    observed = next(
        turn
        for turn in turns_payload_from_store(db_path, host_id)["turns"]
        if turn.get("source_turn_id")
    )
    first = store_sqlite.upsert_command_pending_turn(
        db_path,
        host_id,
        snapshot.workers[0],
        request_id="identical-first",
        instruction_text="same prompt",
        observed_at="2099-07-19T00:00:01+00:00",
    )
    second = store_sqlite.upsert_command_pending_turn(
        db_path,
        host_id,
        snapshot.workers[0],
        request_id="identical-second",
        instruction_text="same prompt",
        observed_at="2099-07-19T00:00:02+00:00",
    )
    assert first is not None and second is not None

    assert store_sqlite.sweep_turn_claims(
        db_path,
        host_id,
        grace_seconds=1,
        now="2099-07-19T00:01:00+00:00",
    ) == 1

    with sqlite3.connect(str(db_path)) as conn:
        stored = {
            turn_id: json.loads(payload_json)
            for turn_id, payload_json in conn.execute(
                """
                SELECT turn_id, payload_json
                FROM turns
                WHERE host_id = ? AND turn_id IN (?, ?)
                """,
                (host_id, first["id"], second["id"]),
            ).fetchall()
        }
    assert stored[first["id"]]["superseded_by_turn_id"] == observed["id"]
    assert stored[second["id"]].get("superseded_at") is None
    assert stored[second["id"]]["has_open_turn"] is True


@pytest.mark.parametrize(
    ("claim_count", "done_count"),
    [(1, 1), (1, 2), (2, 1)],
)
def test_turn_claim_sweeper_never_infers_match_from_cardinality(
    tmp_path: Path,
    claim_count: int,
    done_count: int,
) -> None:
    db_path = tmp_path / f"turn-fifo-{claim_count}-{done_count}.db"
    host_id = "turn-fifo-unambiguous-host"
    snapshot = project_from_raw(
        Config(host_id=host_id, db_path=db_path),
        workers=[
            {
                "id": "worker-1",
                "name": "Worker",
                "status": "active",
                "meta": {
                    "stable_key": "wsk1_" + ("7" * 64),
                    "stable_key_version": 1,
                },
            }
        ],
    )
    init_store(db_path)
    save_snapshot(db_path, snapshot)
    claims = [
        store_sqlite.upsert_command_pending_turn(
            db_path,
            host_id,
            snapshot.workers[0],
            request_id=f"fifo-claim-{index}",
            instruction_text=f"claim prompt {index}",
            observed_at=f"2026-07-19T00:00:0{index}+00:00",
        )
        for index in range(claim_count)
    ]
    assert all(claim is not None for claim in claims)
    for index in range(done_count):
        assert merge_turn_content(
            db_path,
            host_id,
            "worker-1",
            {
                "source_turn_id": f"fifo-done-{index}",
                "user_text": f"unrelated done {index}",
                "assistant_final_text": f"answer {index}",
                "complete": True,
                "has_open_turn": False,
            },
            observed_at=f"2026-07-19T00:01:0{index}+00:00",
        ) == 1

    assert store_sqlite.sweep_turn_claims(
        db_path,
        host_id,
        grace_seconds=1,
        hard_ttl_seconds=1_000_000,
        now="2026-07-19T00:02:00+00:00",
    ) == 0
    with sqlite3.connect(str(db_path)) as conn:
        stored = [
            json.loads(
                conn.execute(
                    "SELECT payload_json FROM turns WHERE host_id = ? AND turn_id = ?",
                    (host_id, claim["id"]),
                ).fetchone()[0]
            )
            for claim in claims
            if claim is not None
        ]
    assert all(payload.get("superseded_at") is None for payload in stored)


def test_turn_list_lazy_sweep_uses_the_callers_now(tmp_path: Path) -> None:
    db_path = tmp_path / "turn-list-caller-now.db"
    host_id = "turn-list-caller-now-host"
    snapshot = project_from_raw(
        Config(host_id=host_id, db_path=db_path),
        workers=[{"id": "worker-1", "name": "Worker", "status": "active"}],
    )
    init_store(db_path)
    save_snapshot(db_path, snapshot)
    claim = store_sqlite.upsert_command_pending_turn(
        db_path,
        host_id,
        snapshot.workers[0],
        request_id="caller-now-claim",
        instruction_text="caller clock",
        observed_at="2000-01-01T00:00:00+00:00",
    )
    assert claim is not None

    before_ttl = turns_payload_from_store(
        db_path,
        host_id,
        now=datetime.fromisoformat("2000-01-01T00:00:30+00:00").timestamp(),
        claim_hard_ttl_seconds=60,
    )
    assert claim["id"] in {turn["id"] for turn in before_ttl["turns"]}
    after_ttl = turns_payload_from_store(
        db_path,
        host_id,
        now=datetime.fromisoformat("2000-01-01T00:02:00+00:00").timestamp(),
        claim_hard_ttl_seconds=60,
    )
    assert claim["id"] not in {turn["id"] for turn in after_ttl["turns"]}


def test_late_real_completion_never_adopts_tombstoned_claim(tmp_path: Path) -> None:
    db_path = tmp_path / "turn-late-real-completion.db"
    host_id = "turn-late-real-completion-host"
    snapshot = project_from_raw(
        Config(host_id=host_id, db_path=db_path),
        workers=[
            {
                "id": "worker-1",
                "name": "Worker",
                "status": "active",
                "meta": {
                    "stable_key": "wsk1_" + ("8" * 64),
                    "stable_key_version": 1,
                },
            }
        ],
    )
    init_store(db_path)
    save_snapshot(db_path, snapshot)
    claim = store_sqlite.upsert_command_pending_turn(
        db_path,
        host_id,
        snapshot.workers[0],
        request_id="late-real-claim",
        instruction_text="late real prompt",
        observed_at="2026-07-19T00:00:00+00:00",
    )
    assert claim is not None
    assert store_sqlite.sweep_turn_claims(
        db_path,
        host_id,
        grace_seconds=1,
        hard_ttl_seconds=60,
        now="2026-07-19T00:02:00+00:00",
    ) == 1

    assert merge_turn_content(
        db_path,
        host_id,
        "worker-1",
        {
            "source_turn_id": "late-real-source",
            "user_text": "late real prompt",
            "assistant_final_text": "late real answer",
            "complete": True,
            "has_open_turn": False,
        },
        observed_at="2026-07-19T00:02:01+00:00",
    ) == 1
    listed = turns_payload_from_store(
        db_path,
        host_id,
        now=datetime.fromisoformat("2026-07-19T00:02:02+00:00").timestamp(),
    )["turns"]
    real = next(turn for turn in listed if turn.get("assistant_final_text") == "late real answer")
    assert real["id"] != claim["id"]
    with sqlite3.connect(str(db_path)) as conn:
        claim_payload = json.loads(
            conn.execute(
                "SELECT payload_json FROM turns WHERE host_id = ? AND turn_id = ?",
                (host_id, claim["id"]),
            ).fetchone()[0]
        )
    assert claim_payload.get("source_turn_id") is None
    assert claim_payload["superseded_at"] == "2026-07-19T00:02:00+00:00"


def test_submission_adoption_rejects_old_completed_matching_text(tmp_path: Path) -> None:
    db_path = tmp_path / "turn-old-observation-adoption.db"
    host_id = "turn-old-observation-adoption-host"
    snapshot = project_from_raw(
        Config(host_id=host_id, db_path=db_path),
        workers=[
            {
                "id": "worker-1",
                "name": "Worker",
                "status": "active",
                "meta": {
                    "stable_key": "wsk1_" + ("9" * 64),
                    "stable_key_version": 1,
                },
            }
        ],
    )
    init_store(db_path)
    save_snapshot(db_path, snapshot)
    assert merge_turn_content(
        db_path,
        host_id,
        "worker-1",
        {
            "source_turn_id": "old-yes-source",
            "user_text": "yes",
            "assistant_final_text": "old answer",
            "complete": True,
            "has_open_turn": False,
        },
        observed_at="2026-07-19T00:00:00+00:00",
    ) == 1
    old = next(
        turn
        for turn in turns_payload_from_store(
            db_path,
            host_id,
            now=datetime.fromisoformat("2026-07-19T00:00:01+00:00").timestamp(),
        )["turns"]
        if turn.get("assistant_final_text") == "old answer"
    )

    claim = store_sqlite.upsert_command_pending_turn(
        db_path,
        host_id,
        snapshot.workers[0],
        request_id="new-yes-request",
        instruction_text="yes",
        observed_at="2026-07-19T00:05:00+00:00",
    )
    assert claim is not None
    assert claim["id"] != old["id"]
    assert claim["complete"] is False
    assert claim.get("assistant_final_text") in {None, ""}
    assert old.get("origin_command_id") is None


def test_text_drift_tombstone_preserves_cursor_and_since_continuity(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "turn-claim-cursor.db"
    host_id = "turn-claim-cursor-host"
    snapshot = project_from_raw(
        Config(host_id=host_id, db_path=db_path),
        workers=[
            {
                "id": "worker-1",
                "name": "Worker",
                "status": "active",
                "meta": {
                    "stable_key": "wsk1_" + ("e" * 64),
                    "stable_key_version": 1,
                },
            }
        ],
    )
    init_store(db_path)
    save_snapshot(db_path, snapshot)
    baseline = turns_payload_from_store(db_path, host_id, schema_version=2)
    claim = store_sqlite.upsert_command_pending_turn(
        db_path,
        host_id,
        snapshot.workers[0],
        request_id="drift-request",
        instruction_text="final normalized prompt",
        observed_at="2099-07-19T00:00:00+00:00",
    )
    assert claim is not None
    assert merge_turn_content(
        db_path,
        host_id,
        "worker-1",
        {
            "source_turn_id": "drift-source",
            "user_text": "draft prompt",
            "complete": False,
            "has_open_turn": True,
        },
        observed_at="2099-07-19T00:00:01+00:00",
    ) == 1
    first_page = turns_payload_from_store(
        db_path,
        host_id,
        schema_version=2,
        limit=1,
    )
    source_id = first_page["turns"][0]["id"]
    assert source_id != claim["id"]
    assert first_page["has_more"] is True
    assert first_page["next_cursor"] is not None

    assert merge_turn_content(
        db_path,
        host_id,
        "worker-1",
        {
            "source_turn_id": "drift-source",
            "user_text": "final normalized prompt",
            "assistant_final_text": "done",
            "complete": True,
            "has_open_turn": False,
        },
        observed_at="2099-07-19T00:00:02+00:00",
    ) == 1

    continuation = turns_payload_from_store(
        db_path,
        host_id,
        schema_version=2,
        limit=1,
        cursor=first_page["next_cursor"],
    )
    assert continuation.get("status") != "cursor_expired"
    assert claim["id"] not in {turn["id"] for turn in continuation["turns"]}
    since_poll = turns_payload_from_store(
        db_path,
        host_id,
        schema_version=2,
        since=baseline["since"],
    )
    assert {turn["id"] for turn in since_poll["turns"]} == {source_id}
    with sqlite3.connect(str(db_path)) as conn:
        physical = conn.execute(
            "SELECT payload_json FROM turns WHERE host_id = ? AND turn_id = ?",
            (host_id, claim["id"]),
        ).fetchone()
    assert physical is not None
    claim_payload = json.loads(physical[0])
    assert claim_payload["superseded_by_turn_id"] == source_id
    assert claim_payload["superseded_at"] == "2099-07-19T00:00:02+00:00"


def test_v14_migration_tombstones_production_shape_duplicate_claim(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "turn-claim-v14.db"
    host_id = "turn-claim-v14-host"
    snapshot = project_from_raw(
        Config(host_id=host_id, db_path=db_path),
        workers=[
            {
                "id": "worker-1",
                "name": "Worker",
                "status": "active",
                "meta": {
                    "stable_key": "wsk1_" + ("d" * 64),
                    "stable_key_version": 1,
                },
            }
        ],
    )
    init_store(db_path)
    save_snapshot(db_path, snapshot)
    assert merge_turn_content(
        db_path,
        host_id,
        "worker-1",
        {
            "source_turn_id": "migration-observed-source",
            "user_text": "same migration prompt",
            "assistant_final_text": "migration answer",
            "complete": True,
            "has_open_turn": False,
        },
        observed_at="2026-07-19T00:00:00+00:00",
    ) == 1
    observed = next(
        turn
        for turn in turns_payload_from_store(db_path, host_id, schema_version=2)["turns"]
        if turn.get("assistant_final_text") == "migration answer"
    )
    claim = store_sqlite.upsert_command_pending_turn(
        db_path,
        host_id,
        snapshot.workers[0],
        request_id="migration-command-request",
        instruction_text="temporary different prompt",
        observed_at="2026-07-19T00:00:01+00:00",
    )
    assert claim is not None
    assert claim["id"] != observed["id"]
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            """
            UPDATE turn_content_revisions
            SET user_text = 'same migration prompt'
            WHERE host_id = ? AND turn_id = ? AND is_current = 1
            """,
            (host_id, claim["id"]),
        )
        generation_before = conn.execute(
            "SELECT traversal_generation FROM turn_list_hosts WHERE host_id = ?",
            (host_id,),
        ).fetchone()[0]
        conn.execute("PRAGMA user_version = 14")

    init_store(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        claim_payload = json.loads(
            conn.execute(
                "SELECT payload_json FROM turns WHERE host_id = ? AND turn_id = ?",
                (host_id, claim["id"]),
            ).fetchone()[0]
        )
        generation_after = conn.execute(
            "SELECT traversal_generation FROM turn_list_hosts WHERE host_id = ?",
            (host_id,),
        ).fetchone()[0]
        physical_rows = conn.execute(
            "SELECT COUNT(*) FROM turns WHERE host_id = ? AND turn_id IN (?, ?)",
            (host_id, observed["id"], claim["id"]),
        ).fetchone()[0]
        foreign_keys = conn.execute("PRAGMA foreign_key_check").fetchall()
    public = turns_payload_from_store(db_path, host_id, schema_version=2)["turns"]
    assert claim_payload["superseded_by_turn_id"] == observed["id"]
    assert claim_payload["complete"] is True
    assert generation_after == generation_before + 1
    assert physical_rows == 2
    assert public[0]["id"] == observed["id"]
    assert claim["id"] not in {turn["id"] for turn in public}
    assert foreign_keys == []


def test_v14_migration_skips_ambiguous_claim_pair(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "turn-v14-ambiguous-pair.db"
    host_id = "turn-v14-ambiguous-pair-host"
    snapshot = project_from_raw(
        Config(host_id=host_id, db_path=db_path),
        workers=[
            {
                "id": "worker-1",
                "name": "Worker",
                "status": "active",
                "meta": {
                    "stable_key": "wsk1_" + ("a" * 64),
                    "stable_key_version": 1,
                },
            }
        ],
    )
    init_store(db_path)
    save_snapshot(db_path, snapshot)
    assert merge_turn_content(
        db_path,
        host_id,
        "worker-1",
        {
            "source_turn_id": "ambiguous-migration-source",
            "user_text": "ambiguous migration prompt",
            "assistant_final_text": "one observed answer",
            "complete": True,
            "has_open_turn": False,
        },
        observed_at="2026-07-19T00:00:00+00:00",
    ) == 1
    claims = [
        store_sqlite.upsert_command_pending_turn(
            db_path,
            host_id,
            snapshot.workers[0],
            request_id=f"ambiguous-migration-{index}",
            instruction_text=f"temporary migration prompt {index}",
            observed_at=f"2026-07-19T00:02:0{index}+00:00",
        )
        for index in range(2)
    ]
    assert all(claim is not None for claim in claims)
    with sqlite3.connect(str(db_path)) as conn:
        for claim in claims:
            assert claim is not None
            conn.execute(
                """
                UPDATE turn_content_revisions
                SET user_text = 'ambiguous migration prompt'
                WHERE host_id = ? AND turn_id = ? AND is_current = 1
                """,
                (host_id, claim["id"]),
            )
        conn.execute("PRAGMA user_version = 14")
    monkeypatch.setenv("TENDWIRE_TURN_CLAIM_HARD_TTL_SECONDS", "999999999")

    init_store(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        payloads = [
            json.loads(
                conn.execute(
                    "SELECT payload_json FROM turns WHERE host_id = ? AND turn_id = ?",
                    (host_id, claim["id"]),
                ).fetchone()[0]
            )
            for claim in claims
            if claim is not None
        ]
    assert len(payloads) == 2
    assert all(payload.get("superseded_at") is None for payload in payloads)


def test_v14_migration_does_not_reuse_already_claimed_done_row(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "turn-v14-used-done.db"
    host_id = "turn-v14-used-done-host"
    snapshot = project_from_raw(
        Config(host_id=host_id, db_path=db_path),
        workers=[
            {
                "id": "worker-1",
                "name": "Worker",
                "status": "active",
                "meta": {
                    "stable_key": "wsk1_" + ("b" * 64),
                    "stable_key_version": 1,
                },
            }
        ],
    )
    init_store(db_path)
    save_snapshot(db_path, snapshot)
    assert merge_turn_content(
        db_path,
        host_id,
        "worker-1",
        {
            "source_turn_id": "used-done-source",
            "user_text": "used done prompt",
            "assistant_final_text": "used done answer",
            "complete": True,
            "has_open_turn": False,
        },
        observed_at="2026-07-19T00:00:00+00:00",
    ) == 1
    done = next(
        turn
        for turn in turns_payload_from_store(
            db_path,
            host_id,
            now=datetime.fromisoformat("2026-07-19T00:00:01+00:00").timestamp(),
        )["turns"]
        if turn.get("assistant_final_text") == "used done answer"
    )
    claims = [
        store_sqlite.upsert_command_pending_turn(
            db_path,
            host_id,
            snapshot.workers[0],
            request_id=f"used-done-claim-{index}",
            instruction_text=f"temporary used prompt {index}",
            observed_at=f"2026-07-19T00:02:0{index}+00:00",
        )
        for index in range(2)
    ]
    assert all(claim is not None for claim in claims)
    with sqlite3.connect(str(db_path)) as conn:
        for claim in claims:
            assert claim is not None
            conn.execute(
                """
                UPDATE turn_content_revisions
                SET user_text = 'used done prompt'
                WHERE host_id = ? AND turn_id = ? AND is_current = 1
                """,
                (host_id, claim["id"]),
            )
        assert store_sqlite._tombstone_turn_conn(
            conn,
            host_id,
            claims[0]["id"],
            superseded_by_turn_id=done["id"],
            superseded_at="2026-07-19T00:03:00+00:00",
        )
        conn.execute("PRAGMA user_version = 14")
    monkeypatch.setenv("TENDWIRE_TURN_CLAIM_HARD_TTL_SECONDS", "999999999")

    init_store(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        second_payload = json.loads(
            conn.execute(
                "SELECT payload_json FROM turns WHERE host_id = ? AND turn_id = ?",
                (host_id, claims[1]["id"]),
            ).fetchone()[0]
        )
    assert second_payload.get("superseded_at") is None
    assert second_payload["has_open_turn"] is True


def test_v14_migration_uses_configured_claim_hard_ttl(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "turn-v14-configured-ttl.db"
    host_id = "turn-v14-configured-ttl-host"
    snapshot = project_from_raw(
        Config(host_id=host_id, db_path=db_path),
        workers=[{"id": "worker-1", "name": "Worker", "status": "active"}],
    )
    init_store(db_path)
    save_snapshot(db_path, snapshot)
    claim = store_sqlite.upsert_command_pending_turn(
        db_path,
        host_id,
        snapshot.workers[0],
        request_id="configured-ttl-claim",
        instruction_text="expire from configured ttl",
        observed_at=(datetime.now(timezone.utc) - timedelta(seconds=120)).isoformat(),
    )
    assert claim is not None
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute("PRAGMA user_version = 14")
    monkeypatch.setenv("TENDWIRE_TURN_CLAIM_HARD_TTL_SECONDS", "60")

    init_store(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        payload = json.loads(
            conn.execute(
                "SELECT payload_json FROM turns WHERE host_id = ? AND turn_id = ?",
                (host_id, claim["id"]),
            ).fetchone()[0]
        )
    assert payload.get("superseded_at") is not None
    assert payload.get("superseded_by_turn_id") is None


def test_turn_cursor_continues_across_interior_tombstone(tmp_path: Path) -> None:
    db_path = tmp_path / "turn-interior-tombstone-cursor.db"
    host_id = "turn-interior-tombstone-cursor-host"
    snapshot = project_from_raw(
        Config(host_id=host_id, db_path=db_path),
        workers=[{"id": "worker-1", "name": "Worker", "status": "active"}],
    )
    init_store(db_path)
    save_snapshot(db_path, snapshot)
    for index in range(3):
        assert merge_turn_content(
            db_path,
            host_id,
            "worker-1",
            {
                "source_turn_id": f"cursor-source-{index}",
                "user_text": f"cursor prompt {index}",
                "assistant_final_text": f"cursor answer {index}",
                "complete": True,
                "has_open_turn": False,
            },
            observed_at=f"2026-07-19T00:0{index}:00+00:00",
        ) == 1
    with sqlite3.connect(str(db_path)) as conn:
        visible = conn.execute(
            """
            SELECT turn_id
            FROM turns
            WHERE host_id = ?
              AND COALESCE(json_extract(payload_json, '$.source_turn_id'), '') != ''
            ORDER BY worker_id ASC, list_sequence DESC, turn_id ASC
            """,
            (host_id,),
        ).fetchall()
    ordered_ids = [str(row[0]) for row in visible]
    assert len(ordered_ids) == 3
    first = turns_payload_from_store(
        db_path,
        host_id,
        schema_version=2,
        limit=1,
        now=datetime.fromisoformat("2026-07-19T00:03:00+00:00").timestamp(),
    )
    assert first["turns"][0]["id"] == ordered_ids[0]
    assert first["next_cursor"] is not None
    with sqlite3.connect(str(db_path)) as conn:
        assert store_sqlite._tombstone_turn_conn(
            conn,
            host_id,
            ordered_ids[1],
            superseded_by_turn_id=ordered_ids[0],
            superseded_at="2026-07-19T00:03:01+00:00",
        )

    continuation = turns_payload_from_store(
        db_path,
        host_id,
        schema_version=2,
        limit=1,
        cursor=first["next_cursor"],
        now=datetime.fromisoformat("2026-07-19T00:03:02+00:00").timestamp(),
    )
    assert continuation.get("status") != "cursor_expired"
    assert [turn["id"] for turn in continuation["turns"]] == [ordered_ids[2]]


def test_turn_list_lazy_sweep_is_rate_limited(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "turn-list-sweep-rate-limit.db"
    host_id = "turn-list-sweep-rate-limit-host"
    init_store(db_path)
    calls: list[tuple[float | None, str | None]] = []

    def capture_sweep(*_args, grace_seconds=None, now=None, **_kwargs):
        calls.append((grace_seconds, now))
        return 0

    monkeypatch.setattr(store_sqlite, "sweep_turn_claims", capture_sweep)
    for current in (1_000.0, 1_001.0, 1_002.0):
        turns_payload_from_store(
            db_path,
            host_id,
            now=current,
            turn_refresh_interval_seconds=2.0,
        )
    assert calls == [
        (
            60.0,
            datetime.fromtimestamp(1_000.0, tz=timezone.utc).isoformat(),
        ),
        (
            60.0,
            datetime.fromtimestamp(1_002.0, tz=timezone.utc).isoformat(),
        ),
    ]


def test_ingestion_ambiguity_fallthrough_emits_structured_diagnostic(
    tmp_path: Path,
    caplog,
) -> None:
    db_path = tmp_path / "turn-ingestion-ambiguity-diagnostic.db"
    host_id = "turn-ingestion-ambiguity-diagnostic-host"
    snapshot = project_from_raw(
        Config(host_id=host_id, db_path=db_path),
        workers=[
            {
                "id": "worker-1",
                "name": "Worker",
                "status": "active",
                "meta": {
                    "stable_key": "wsk1_" + ("c" * 64),
                    "stable_key_version": 1,
                },
            }
        ],
    )
    init_store(db_path)
    save_snapshot(db_path, snapshot)
    for index in range(2):
        assert store_sqlite.upsert_command_pending_turn(
            db_path,
            host_id,
            snapshot.workers[0],
            request_id=f"diagnostic-claim-{index}",
            instruction_text="same ambiguous prompt",
            observed_at=f"2026-07-19T00:00:0{index}+00:00",
        ) is not None

    with caplog.at_level("WARNING", logger="tendwire.store.sqlite"):
        assert merge_turn_content(
            db_path,
            host_id,
            "worker-1",
            {
                "source_turn_id": "diagnostic-source",
                "user_text": "same ambiguous prompt",
                "assistant_final_text": "independent answer",
                "complete": True,
                "has_open_turn": False,
            },
            observed_at="2026-07-19T00:01:00+00:00",
        ) == 1

    diagnostics = [
        getattr(record, "tendwire_diagnostic", None)
        for record in caplog.records
        if record.getMessage() == "turn_ingestion_ambiguity_fallthrough"
    ]
    assert diagnostics == [
        {
            "code": "turn_ingestion_ambiguity_fallthrough",
            "host_id": host_id,
            "worker_id": "worker-1",
            "candidate_count": 2,
        }
    ]


def _reset_store_to_v5_with_legacy_turn(
    db_path: Path,
    *,
    final_text: str,
) -> tuple[Any, str]:
    config = Config(host_id="legacy-turn-host", db_path=db_path)
    snapshot = project_from_raw(
        config,
        workers=[
            {
                "id": "worker-1",
                "name": "claude",
                "status": "active",
                "space_id": "space-1",
            }
        ],
    )
    init_store(db_path)
    save_snapshot(db_path, snapshot)
    with sqlite3.connect(str(db_path)) as conn:
        turn_id, payload_json = conn.execute(
            """
            SELECT turn_id, payload_json
            FROM turns
            WHERE host_id = 'legacy-turn-host'
            """
        ).fetchone()
        payload = json.loads(payload_json)
        payload["user_text"] = "legacy prompt"
        payload["assistant_final_text"] = final_text
        payload["complete"] = True
        payload["has_open_turn"] = False
        conn.execute(
            "UPDATE turns SET payload_json = ? WHERE host_id = ? AND turn_id = ?",
            (
                json.dumps(payload, sort_keys=True),
                "legacy-turn-host",
                str(turn_id),
            ),
        )
        conn.execute("DROP TABLE turn_presentation_recoveries")
        conn.execute("DROP TABLE turn_presentation_jobs")
        conn.execute("DROP TABLE turn_presentation_plans")
        conn.execute("DROP TABLE turn_content_page_boundaries")
        conn.execute("DROP TABLE turn_content_revisions")
        conn.execute("CREATE TABLE preserved_v5 (value TEXT NOT NULL)")
        conn.execute("INSERT INTO preserved_v5 (value) VALUES ('untouched')")
        conn.execute("PRAGMA user_version = 5")
    return snapshot, str(turn_id)


def _reconstruct_turn_content(
    db_path: Path,
    *,
    host_id: str,
    turn_id: str,
    revision: str,
    field: str,
    work_counters: store_sqlite.TurnContentWorkCounters | None = None,
) -> tuple[str, list[dict[str, Any]]]:
    cursor: str | None = None
    pages: list[dict[str, Any]] = []
    while True:
        page = store_sqlite.get_turn_content(
            db_path,
            host_id,
            turn_id=turn_id,
            content_revision=revision,
            field=field,
            cursor=cursor,
            work_counters=work_counters,
        )
        assert page.get("status") is None
        pages.append(page)
        cursor = page["next_cursor"]
        if cursor is None:
            break
    return "".join(str(page["text"]) for page in pages), pages


def test_store_v5_to_v6_migration_is_atomic_idempotent_and_marks_incomplete(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "turn-v5.db"
    fragment = ("x" * 11_988) + "\n[truncated]"
    snapshot, turn_id = _reset_store_to_v5_with_legacy_turn(
        db_path,
        final_text=fragment,
    )

    init_store(db_path)
    first_v2 = turns_payload_from_store(
        db_path,
        "legacy-turn-host",
        snapshot=snapshot,
        schema_version=2,
    )
    init_store(db_path)
    second_v2 = turns_payload_from_store(
        db_path,
        "legacy-turn-host",
        snapshot=snapshot,
        schema_version=2,
    )

    with sqlite3.connect(str(db_path)) as conn:
        version = _user_version(conn)
        tables = _table_names(conn)
        revision_rows = conn.execute(
            """
            SELECT
                content_revision, user_text, assistant_final_text,
                user_state, final_state, user_page_count, final_page_count,
                is_current
            FROM turn_content_revisions
            WHERE host_id = ? AND turn_id = ?
            """,
            ("legacy-turn-host", turn_id),
        ).fetchall()
        stored_payload = conn.execute(
            "SELECT payload_json FROM turns WHERE host_id = ? AND turn_id = ?",
            ("legacy-turn-host", turn_id),
        ).fetchone()[0]
        preserved = conn.execute("SELECT value FROM preserved_v5").fetchone()[0]
        integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]
        foreign_keys = conn.execute("PRAGMA foreign_key_check").fetchall()

    assert version == store_sqlite.STORE_SCHEMA_VERSION
    assert {
        "turn_content_revisions",
        "turn_presentation_plans",
        "turn_presentation_jobs",
    } <= tables
    assert len(revision_rows) == 1
    revision = revision_rows[0]
    assert revision[1:] == (
        "legacy prompt",
        fragment,
        "complete",
        "known_incomplete",
        1,
        0,
        1,
    )
    assert json.loads(stored_payload).get("assistant_final_text") is None
    assert fragment not in stored_payload
    assert preserved == "untouched"
    assert integrity == "ok"
    assert foreign_keys == []
    assert first_v2 == second_v2
    turn = next(item for item in first_v2["turns"] if item["id"] == turn_id)
    assert turn["content"]["known_incomplete"] is True
    assert turn["content"]["fields"]["assistant_final_text"] == {
        "availability": "known_incomplete",
        "inline": False,
        "char_length": len(fragment),
        "byte_length": len(fragment.encode("utf-8")),
        "page_count": 0,
        "first_cursor": None,
    }
    assert "assistant_final_text" not in turn
    assert turn["assistant_final_preview"] == fragment[:1000]
    assert turns_payload_from_store(
        db_path,
        "legacy-turn-host",
        schema_version=1,
    ) == {
        "schema_version": 1,
        "ok": False,
        "status": "upgrade_required",
        "required_turn_schema_version": 2,
    }
    assert store_sqlite.get_turn_content(
        db_path,
        "legacy-turn-host",
        turn_id=turn_id,
        content_revision=revision[0],
        field="assistant_final_text",
    ) == {
        "schema_version": 1,
        "ok": False,
        "status": "content_known_incomplete",
    }

    recovered = fragment + "\nrecovered suffix"
    assert merge_turn_content(
        db_path,
        "legacy-turn-host",
        "worker-1",
        {"assistant_final_text": recovered, "complete": True},
        observed_at="2099-01-02T00:00:00+00:00",
    ) == 1
    with sqlite3.connect(str(db_path)) as conn:
        recovered_rows = conn.execute(
            """
            SELECT final_state, is_current, assistant_final_text
            FROM turn_content_revisions
            WHERE host_id = ? AND turn_id = ?
            ORDER BY is_current
            """,
            ("legacy-turn-host", turn_id),
        ).fetchall()
    assert recovered_rows == [
        ("known_incomplete", 0, fragment),
        ("complete", 1, recovered),
    ]


def test_store_v5_to_v6_migration_rolls_back_all_v6_ddl(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "turn-v5-rollback.db"
    _reset_store_to_v5_with_legacy_turn(db_path, final_text="legacy answer")

    def fail_backfill(conn: sqlite3.Connection) -> None:
        raise RuntimeError("controlled v6 migration failure")

    monkeypatch.setattr(
        store_sqlite,
        "_backfill_legacy_turn_content_conn",
        fail_backfill,
    )
    with pytest.raises(RuntimeError, match="controlled v6 migration failure"):
        init_store(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        assert _user_version(conn) == 5
        assert "turn_content_revisions" not in _table_names(conn)
        assert "turn_content_page_boundaries" not in _table_names(conn)
        assert "turn_presentation_plans" not in _table_names(conn)
        assert "turn_presentation_jobs" not in _table_names(conn)
        assert "turn_presentation_recoveries" not in _table_names(conn)
        assert conn.execute("SELECT value FROM preserved_v5").fetchone()[0] == "untouched"


def test_v6_to_v7_repairs_mixed_turns_with_absent_content_descriptors(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "mixed-v6-turn-content.db"
    host_id = "mixed-v6-host"
    snapshot = project_from_raw(
        Config(host_id=host_id, db_path=db_path),
        workers=[
            {"id": "worker-complete", "name": "Complete", "status": "active"},
            {"id": "worker-working", "name": "Working", "status": "active"},
        ],
    )
    init_store(db_path)
    save_snapshot(db_path, snapshot)
    assert merge_turn_content(
        db_path,
        host_id,
        "worker-complete",
        {
            "assistant_final_text": "complete final",
            "complete": True,
            "has_open_turn": False,
        },
        observed_at="2026-01-01T00:00:00+00:00",
    ) == 1
    command = store_sqlite.upsert_command_pending_turn(
        db_path,
        host_id,
        snapshot.workers[0],
        request_id="mixed-command-request",
        instruction_text="command metadata must not become canonical during repair",
        observed_at="2026-01-01T00:01:00+00:00",
    )
    assert command is not None

    with sqlite3.connect(str(db_path)) as conn:
        complete_turn_id = str(
            conn.execute(
                """
                SELECT turn_id
                FROM turn_content_revisions
                WHERE host_id = ? AND final_state = 'complete' AND is_current = 1
                """,
                (host_id,),
            ).fetchone()[0]
        )
        missing_rows = conn.execute(
            """
            SELECT turn_id, payload_json
            FROM turns
            WHERE host_id = ? AND turn_id != ?
            ORDER BY turn_id
            """,
            (host_id, complete_turn_id),
        ).fetchall()
        assert len(missing_rows) >= 2
        missing_turn_ids = [str(row[0]) for row in missing_rows]
        for turn_id, payload_json in missing_rows:
            payload = json.loads(payload_json)
            payload["assistant_stream_text"] = "working progress"
            conn.execute(
                """
                UPDATE turns
                SET payload_json = ?
                WHERE host_id = ? AND turn_id = ?
                """,
                (json.dumps(payload, sort_keys=True), host_id, str(turn_id)),
            )
        placeholders = ",".join("?" for _ in missing_turn_ids)
        conn.execute(
            f"""
            DELETE FROM turn_content_page_boundaries
            WHERE host_id = ? AND turn_id IN ({placeholders})
            """,
            (host_id, *missing_turn_ids),
        )
        conn.execute(
            f"""
            DELETE FROM turn_content_revisions
            WHERE host_id = ? AND turn_id IN ({placeholders})
            """,
            (host_id, *missing_turn_ids),
        )
        conn.execute("DROP TABLE turn_content_page_boundaries")
        conn.execute("PRAGMA user_version = 6")

    init_store(db_path)
    first_v2 = turns_payload_from_store(
        db_path,
        host_id,
        snapshot=snapshot,
        schema_version=2,
    )
    first_v1 = turns_payload_from_store(
        db_path,
        host_id,
        snapshot=snapshot,
        schema_version=1,
    )
    with sqlite3.connect(str(db_path)) as conn:
        first_rows = conn.execute(
            """
            SELECT turn_id, content_revision, user_state, final_state, is_current
            FROM turn_content_revisions
            WHERE host_id = ? AND turn_id IN (
                SELECT turn_id
                FROM turns
                WHERE host_id = ? AND turn_id != ?
            )
            ORDER BY turn_id, content_revision
            """,
            (host_id, host_id, complete_turn_id),
        ).fetchall()
        version = conn.execute("PRAGMA user_version").fetchone()[0]
    init_store(db_path)
    second_v2 = turns_payload_from_store(
        db_path,
        host_id,
        snapshot=snapshot,
        schema_version=2,
    )
    with sqlite3.connect(str(db_path)) as conn:
        second_rows = conn.execute(
            """
            SELECT turn_id, content_revision, user_state, final_state, is_current
            FROM turn_content_revisions
            WHERE host_id = ? AND turn_id IN (
                SELECT turn_id
                FROM turns
                WHERE host_id = ? AND turn_id != ?
            )
            ORDER BY turn_id, content_revision
            """,
            (host_id, host_id, complete_turn_id),
        ).fetchall()

    absent_field = {
        "availability": "absent",
        "inline": False,
        "char_length": 0,
        "byte_length": 0,
        "page_count": 0,
        "first_cursor": None,
    }
    assert version == store_sqlite.STORE_SCHEMA_VERSION
    assert first_v2 == second_v2
    assert first_rows == second_rows
    assert len(first_rows) == len(missing_turn_ids)
    assert all(
        row
        == (
            row[0],
            store_sqlite.content_revision(
                str(row[0]),
                None,
                None,
                "absent",
                "absent",
            ),
            "absent",
            "absent",
            1,
        )
        for row in first_rows
    )
    assert len(first_v2["turns"]) >= 2
    for turn in first_v2["turns"]:
        assert turn["content"]["schema_version"] == 1
        if turn["id"] not in missing_turn_ids:
            continue
        assert turn["assistant_stream_text"] == "working progress"
        assert turn["content"]["known_incomplete"] is False
        assert turn["content"]["fields"] == {
            "user_text": absent_field,
            "assistant_final_text": absent_field,
        }
        assert "user_text" not in turn
        assert "assistant_final_text" not in turn
    assert first_v1["schema_version"] == 1
    assert all("content" not in turn for turn in first_v1["turns"])
    for turn in first_v1["turns"]:
        if turn["id"] in missing_turn_ids:
            assert turn["user_text"] is None
            assert turn["assistant_final_text"] is None


def _exact_utf8_fixture(byte_length: int) -> str:
    unit = "😀漢字é\n"
    unit_bytes = len(unit.encode("utf-8"))
    repeats, remainder = divmod(byte_length, unit_bytes)
    return (unit * repeats) + ("x" * remainder)


@pytest.mark.parametrize(
    "target_byte_length",
    (1024 * 1024, 8 * 1024 * 1024),
    ids=("1mib", "8mib"),
)
def test_store_list_is_preview_bounded_and_sequential_pages_are_linear(
    tmp_path: Path,
    target_byte_length: int,
) -> None:
    db_path = tmp_path / f"paging-{target_byte_length}.db"
    host_id = "paging-complexity-host"
    worker_id = "worker-complexity"
    snapshot = project_from_raw(
        Config(host_id=host_id, db_path=db_path),
        workers=[{"id": worker_id, "name": "claude", "status": "active"}],
    )
    init_store(db_path)
    save_snapshot(db_path, snapshot)
    final = _exact_utf8_fixture(target_byte_length)
    assert len(final.encode("utf-8")) == target_byte_length
    assert merge_turn_content(
        db_path,
        host_id,
        worker_id,
        {
            "assistant_final_text": final,
            "complete": True,
            "has_open_turn": False,
        },
        observed_at="2026-01-01T00:00:00+00:00",
    ) == 1

    counters = store_sqlite.TurnContentWorkCounters()
    listed = turns_payload_from_store(
        db_path,
        host_id,
        snapshot=snapshot,
        schema_version=2,
        work_counters=counters,
    )
    turn = next(
        item
        for item in listed["turns"]
        if item.get("content", {}).get("fields", {}).get(
            "assistant_final_text", {}
        ).get("availability")
        == "complete"
    )
    descriptor = turn["content"]["fields"]["assistant_final_text"]

    assert counters.list_sql_queries == 1
    assert counters.list_descriptor_rows == 1
    assert counters.list_preview_chars_examined == 1000
    assert counters.list_inline_chars_examined == 0
    assert descriptor["char_length"] == len(final)
    assert descriptor["byte_length"] == target_byte_length
    assert descriptor["inline"] is False
    assert turn["assistant_final_preview"] == final[:1000]
    assert counters.max_response_utf8_bytes < 1024 * 1024

    rebuilt, pages = _reconstruct_turn_content(
        db_path,
        host_id=host_id,
        turn_id=turn["id"],
        revision=turn["content"]["content_revision"],
        field="assistant_final_text",
        work_counters=counters,
    )
    assert hashlib.sha256(rebuilt.encode("utf-8")).digest() == hashlib.sha256(
        final.encode("utf-8")
    ).digest()
    assert rebuilt == final
    assert descriptor["page_count"] == len(pages)
    assert counters.page_sql_queries == len(pages)
    assert counters.page_blob_reads == len(pages)
    assert counters.page_chars_examined == len(final)
    assert counters.page_bytes_examined == (
        (len(pages) - 1) * store_sqlite.TURN_CONTENT_PAGE_MAX_UTF8_BYTES
        + pages[-1]["segment_byte_length"]
    )
    assert counters.page_bytes_examined <= target_byte_length + 3 * (
        len(pages) - 1
    )
    assert counters.max_response_utf8_bytes < 1024 * 1024
    assert all(
        page["segment_byte_length"]
        <= store_sqlite.TURN_CONTENT_PAGE_MAX_UTF8_BYTES
        for page in pages
    )


def test_migrated_v6_boundaries_make_first_long_read_page_bounded(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "migrated-v6-page-boundaries.db"
    host_id = "legacy-boundary-host"
    worker_id = "worker-boundary"
    snapshot = project_from_raw(
        Config(host_id=host_id, db_path=db_path),
        workers=[{"id": worker_id, "name": "Boundary", "status": "active"}],
    )
    init_store(db_path)
    save_snapshot(db_path, snapshot)
    final = _exact_utf8_fixture(1024 * 1024)
    assert merge_turn_content(
        db_path,
        host_id,
        worker_id,
        {
            "assistant_final_text": final,
            "complete": True,
            "has_open_turn": False,
        },
        observed_at="2026-01-01T00:00:00+00:00",
    ) == 1
    listed = turns_payload_from_store(
        db_path,
        host_id,
        snapshot=snapshot,
        schema_version=2,
    )
    turn = listed["turns"][0]
    revision = turn["content"]["content_revision"]
    page_count = turn["content"]["fields"]["assistant_final_text"]["page_count"]
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            """
            DELETE FROM turn_content_page_boundaries
            WHERE host_id = ?
              AND turn_id = ?
              AND content_revision = ?
              AND field = 'assistant_final_text'
            """,
            (host_id, turn["id"], revision),
        )
        conn.execute("PRAGMA user_version = 6")

    init_store(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        migrated_version = conn.execute("PRAGMA user_version").fetchone()[0]
        migrated_boundaries = conn.execute(
            """
            SELECT COUNT(*)
            FROM turn_content_page_boundaries
            WHERE host_id = ?
              AND turn_id = ?
              AND content_revision = ?
              AND field = 'assistant_final_text'
            """,
            (host_id, turn["id"], revision),
        ).fetchone()[0]
    first_counters = store_sqlite.TurnContentWorkCounters()
    first_rebuilt, first_pages = _reconstruct_turn_content(
        db_path,
        host_id=host_id,
        turn_id=turn["id"],
        revision=revision,
        field="assistant_final_text",
        work_counters=first_counters,
    )

    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            """
            DELETE FROM turn_content_page_boundaries
            WHERE host_id = ?
              AND turn_id = ?
              AND content_revision = ?
              AND field = 'assistant_final_text'
            """,
            (host_id, turn["id"], revision),
        )
    init_store(db_path)
    init_store(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        current_boundaries = conn.execute(
            """
            SELECT COUNT(*)
            FROM turn_content_page_boundaries
            WHERE host_id = ?
              AND turn_id = ?
              AND content_revision = ?
              AND field = 'assistant_final_text'
            """,
            (host_id, turn["id"], revision),
        ).fetchone()[0]
        incomplete_boundary_fields = conn.execute(
            """
            SELECT COUNT(*)
            FROM turn_content_revisions AS revisions
            WHERE (
                revisions.user_state = 'complete'
                AND revisions.user_page_count != (
                    SELECT COUNT(*)
                    FROM turn_content_page_boundaries AS boundaries
                    WHERE boundaries.host_id = revisions.host_id
                      AND boundaries.turn_id = revisions.turn_id
                      AND boundaries.content_revision = revisions.content_revision
                      AND boundaries.field = 'user_text'
                )
            ) OR (
                revisions.final_state = 'complete'
                AND revisions.final_page_count != (
                    SELECT COUNT(*)
                    FROM turn_content_page_boundaries AS boundaries
                    WHERE boundaries.host_id = revisions.host_id
                      AND boundaries.turn_id = revisions.turn_id
                      AND boundaries.content_revision = revisions.content_revision
                      AND boundaries.field = 'assistant_final_text'
                )
            )
            """
        ).fetchone()[0]
    failed_page = store_sqlite.get_turn_content(
        db_path,
        host_id,
        turn_id=turn["id"],
        content_revision=revision,
        field="assistant_final_text",
    )

    assert migrated_version == store_sqlite.STORE_SCHEMA_VERSION
    assert migrated_boundaries == page_count
    assert current_boundaries == 0
    assert incomplete_boundary_fields == 1
    assert first_rebuilt == final
    assert len(first_pages) == page_count
    assert first_counters.page_blob_reads == page_count
    assert first_counters.page_chars_examined == len(final)
    assert first_counters.page_bytes_examined <= (
        len(final.encode("utf-8")) + 3 * (page_count - 1)
    )
    assert failed_page == {
        "schema_version": 1,
        "ok": False,
        "status": "content_not_available",
    }


def test_many_long_turn_descriptors_do_one_bounded_list_query(tmp_path: Path) -> None:
    db_path = tmp_path / "many-long-turns.db"
    host_id = "many-turns-host"
    worker_ids = [f"worker-{index:03d}" for index in range(64)]
    snapshot = project_from_raw(
        Config(host_id=host_id, db_path=db_path),
        workers=[
            {"id": worker_id, "name": worker_id, "status": "active"}
            for worker_id in worker_ids
        ],
    )
    init_store(db_path)
    save_snapshot(db_path, snapshot)
    for index, worker_id in enumerate(worker_ids):
        assert merge_turn_content(
            db_path,
            host_id,
            worker_id,
            {
                "assistant_final_text": f"turn-{index:03d}\n" + ("界" * 14_000),
                "complete": True,
                "has_open_turn": False,
            },
            observed_at=f"2026-01-01T00:{index // 60:02d}:{index % 60:02d}+00:00",
        ) == 1

    counters = store_sqlite.TurnContentWorkCounters()
    listed = turns_payload_from_store(
        db_path,
        host_id,
        snapshot=snapshot,
        schema_version=2,
        work_counters=counters,
    )
    descriptors = [
        turn["content"]["fields"]["assistant_final_text"]
        for turn in listed["turns"]
    ]

    assert len(descriptors) == len(worker_ids)
    assert all(
        descriptor["availability"] == "complete"
        and descriptor["inline"] is False
        and descriptor["page_count"] > 0
        for descriptor in descriptors
    )
    assert counters.to_dict() == {
        "list_sql_queries": 1,
        "list_descriptor_rows": len(worker_ids),
        "list_preview_chars_examined": len(worker_ids) * 1000,
        "list_inline_chars_examined": 0,
        "page_sql_queries": 0,
        "page_blob_reads": 0,
        "page_bytes_examined": 0,
        "page_chars_examined": 0,
        "max_response_utf8_bytes": counters.max_response_utf8_bytes,
    }
    assert counters.max_response_utf8_bytes < 1024 * 1024


def test_store_canonical_pages_round_trip_long_content_without_duplicate_copy(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "long-content.db"
    config = Config(host_id="long-host", db_path=db_path)
    snapshot = project_from_raw(
        config,
        workers=[{"id": "worker-1", "name": "claude", "status": "active"}],
    )
    init_store(db_path)
    save_snapshot(db_path, snapshot)
    prompt = "P" * 20_000
    final = "\n# Heading\n\n```\n" + ("🙂" * 270_000) + "\n```\n"

    assert len(final.encode("utf-8")) > 1024 * 1024
    assert merge_turn_content(
        db_path,
        "long-host",
        "worker-1",
        {
            "user_text": prompt,
            "assistant_final_text": final,
            "complete": True,
            "has_open_turn": False,
        },
        observed_at="2026-01-01T00:00:00+00:00",
    ) == 1

    listed = turns_payload_from_store(
        db_path,
        "long-host",
        snapshot=snapshot,
        schema_version=2,
    )
    turn = next(
        item
        for item in listed["turns"]
        if item.get("content", {}).get("fields", {}).get("assistant_final_text", {}).get(
            "availability"
        )
        == "complete"
    )
    revision = turn["content"]["content_revision"]
    rebuilt_prompt, prompt_pages = _reconstruct_turn_content(
        db_path,
        host_id="long-host",
        turn_id=turn["id"],
        revision=revision,
        field="user_text",
    )
    rebuilt_final, final_pages = _reconstruct_turn_content(
        db_path,
        host_id="long-host",
        turn_id=turn["id"],
        revision=revision,
        field="assistant_final_text",
    )

    assert rebuilt_prompt == prompt
    assert rebuilt_final == final
    assert all(
        page["segment_byte_length"] <= 48 * 1024
        for page in [*prompt_pages, *final_pages]
    )
    assert [page["index"] for page in final_pages] == list(range(len(final_pages)))
    assert len({page["segment_id"] for page in final_pages}) == len(final_pages)
    assert turn["content"]["fields"]["user_text"]["page_count"] == len(prompt_pages)
    assert turn["content"]["fields"]["assistant_final_text"]["page_count"] == len(
        final_pages
    )
    assert "user_text" not in turn
    assert "assistant_final_text" not in turn
    assert turn["user_preview"] == prompt[:1000]
    assert turn["assistant_final_preview"] == final[:1000]
    assert turns_payload_from_store(db_path, "long-host", schema_version=1)[
        "status"
    ] == "upgrade_required"
    invalid_cursor_results = (
        store_sqlite.get_turn_content(
            db_path,
            "long-host",
            turn_id=turn["id"],
            content_revision=revision,
            field="assistant_final_text",
            cursor="not-a-cursor",
        ),
        store_sqlite.get_turn_content(
            db_path,
            "long-host",
            turn_id=turn["id"],
            content_revision=revision,
            field="assistant_final_text",
            cursor=content_cursor(
                revision,
                "assistant_final_text",
                len(final_pages),
                start_char=len(final),
                start_byte=len(final.encode("utf-8")),
            ),
        ),
        store_sqlite.get_turn_content(
            db_path,
            "long-host",
            turn_id=turn["id"],
            content_revision=revision,
            field="assistant_final_text",
            cursor=content_cursor(
                revision,
                "assistant_final_text",
                1,
                start_char=final_pages[0]["segment_char_length"],
                start_byte=final_pages[0]["segment_byte_length"] - 1,
            ),
        ),
        store_sqlite.get_turn_content(
            db_path,
            "long-host",
            turn_id=turn["id"],
            content_revision=revision,
            field="assistant_final_text",
            cursor=content_cursor(
                revision,
                "assistant_final_text",
                1,
                start_char=final_pages[0]["segment_char_length"] + 1,
                start_byte=final_pages[0]["segment_byte_length"],
            ),
        ),
    )
    assert [result["status"] for result in invalid_cursor_results] == [
        "invalid_cursor",
        "invalid_cursor",
        "invalid_cursor",
        "invalid_cursor",
    ]
    with sqlite3.connect(str(db_path)) as conn:
        canonical = conn.execute(
            """
            SELECT user_text, assistant_final_text, user_page_count, final_page_count
            FROM turn_content_revisions
            WHERE host_id = ? AND turn_id = ? AND is_current = 1
            """,
            ("long-host", turn["id"]),
        ).fetchone()
        payload_json = conn.execute(
            "SELECT payload_json FROM turns WHERE host_id = ? AND turn_id = ?",
            ("long-host", turn["id"]),
        ).fetchone()[0]
        revision_count = conn.execute(
            """
            SELECT COUNT(*)
            FROM turn_content_revisions
            WHERE host_id = ? AND turn_id = ?
            """,
            ("long-host", turn["id"]),
        ).fetchone()[0]
    assert canonical == (prompt, final, len(prompt_pages), len(final_pages))
    assert prompt not in payload_json
    assert final[:1000] not in payload_json
    assert revision_count == 2

    assert merge_turn_content(
        db_path,
        "long-host",
        "worker-1",
        {"user_text": prompt, "assistant_final_text": final},
        observed_at="2026-01-01T00:01:00+00:00",
    ) == 0
    assert merge_turn_content(
        db_path,
        "long-host",
        "worker-1",
        {
            "user_text": "",
            "assistant_final_text": "",
            "complete": False,
            "has_open_turn": True,
        },
        observed_at="2026-01-01T00:02:00+00:00",
    ) == 0
    with sqlite3.connect(str(db_path)) as conn:
        assert conn.execute(
            "SELECT COUNT(*) FROM turn_content_revisions WHERE host_id = ? AND turn_id = ?",
            ("long-host", turn["id"]),
        ).fetchone()[0] == 2

def test_store_merge_distinguishes_whitespace_content_from_empty_or_absent_fields(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "turn-content-merge-precedence.db"
    host_id = "merge-precedence-host"
    worker_id = "worker-1"
    snapshot = project_from_raw(
        Config(host_id=host_id, db_path=db_path),
        workers=[{"id": worker_id, "name": "worker", "status": "active"}],
    )
    init_store(db_path)
    save_snapshot(db_path, snapshot)

    assert merge_turn_content(
        db_path,
        host_id,
        worker_id,
        {
            "user_text": "known prompt",
            "assistant_final_text": "known final",
            "complete": True,
        },
        observed_at="2026-01-01T00:00:00+00:00",
    ) == 1
    assert merge_turn_content(
        db_path,
        host_id,
        worker_id,
        {"user_text": ""},
        observed_at="2026-01-01T00:01:00+00:00",
    ) == 0
    assert merge_turn_content(
        db_path,
        host_id,
        worker_id,
        {"assistant_final_text": ""},
        observed_at="2026-01-01T00:02:00+00:00",
    ) == 0

    preserved = turns_payload_from_store(
        db_path,
        host_id,
        snapshot=snapshot,
        schema_version=2,
    )["turns"][0]
    assert preserved["user_text"] == "known prompt"
    assert preserved["assistant_final_text"] == "known final"

    whitespace = " \t\r\n "
    assert merge_turn_content(
        db_path,
        host_id,
        worker_id,
        {"assistant_final_text": whitespace},
        observed_at="2026-01-01T00:03:00+00:00",
    ) == 1
    replaced = turns_payload_from_store(
        db_path,
        host_id,
        snapshot=snapshot,
        schema_version=2,
    )["turns"][0]
    assert replaced["user_text"] == "known prompt"
    assert replaced["assistant_final_text"] == whitespace

    assert merge_turn_content(
        db_path,
        host_id,
        worker_id,
        {"user_text": ""},
        observed_at="2026-01-01T00:04:00+00:00",
    ) == 0
    final_view = turns_payload_from_store(
        db_path,
        host_id,
        snapshot=snapshot,
        schema_version=2,
    )["turns"][0]
    assert final_view["user_text"] == "known prompt"
    assert final_view["assistant_final_text"] == whitespace


def test_store_revision_replacement_rolls_back_projection_and_current_flip(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "revision-rollback.db"
    config = Config(host_id="rollback-host", db_path=db_path)
    snapshot = project_from_raw(
        config,
        workers=[{"id": "worker-1", "name": "claude", "status": "active"}],
    )
    init_store(db_path)
    save_snapshot(db_path, snapshot)
    assert merge_turn_content(
        db_path,
        "rollback-host",
        "worker-1",
        {"assistant_final_text": "first final", "complete": True},
        observed_at="2026-01-01T00:00:00+00:00",
    ) == 1
    with sqlite3.connect(str(db_path)) as conn:
        before = conn.execute(
            """
            SELECT content_revision, assistant_final_text, is_current
            FROM turn_content_revisions
            WHERE host_id = 'rollback-host'
            """
        ).fetchall()
        payload_before = conn.execute(
            "SELECT payload_json FROM turns WHERE host_id = 'rollback-host'"
        ).fetchone()[0]

    def fail_insert(*args: Any, **kwargs: Any) -> str:
        raise RuntimeError("controlled revision insert failure")

    monkeypatch.setattr(store_sqlite, "_insert_turn_content_revision_conn", fail_insert)
    with pytest.raises(RuntimeError, match="controlled revision insert failure"):
        merge_turn_content(
            db_path,
            "rollback-host",
            "worker-1",
            {"assistant_final_text": "replacement final", "complete": True},
            observed_at="2026-01-01T00:01:00+00:00",
        )

    with sqlite3.connect(str(db_path)) as conn:
        after = conn.execute(
            """
            SELECT content_revision, assistant_final_text, is_current
            FROM turn_content_revisions
            WHERE host_id = 'rollback-host'
            """
        ).fetchall()
        payload_after = conn.execute(
            "SELECT payload_json FROM turns WHERE host_id = 'rollback-host'"
        ).fetchone()[0]
        integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]
    assert after == before
    assert payload_after == payload_before
    assert integrity == "ok"


def _seed_superseded_content_revision(
    db_path: Path,
    *,
    host_id: str,
    source_turn_id: str,
    retain_source_anchor: bool = False,
) -> tuple[str, str, str]:
    config = Config(host_id=host_id, db_path=db_path)
    snapshot = project_from_raw(
        config,
        workers=[
            {
                "id": "worker-1",
                "name": "claude",
                "status": "active",
                "meta": {
                    "stable_key": "wsk1_" + ("3" * 64),
                    "stable_key_version": 1,
                },
            }
        ],
    )
    init_store(db_path)
    save_snapshot(db_path, snapshot)
    assert merge_turn_content(
        db_path,
        host_id,
        "worker-1",
        {
            "assistant_final_text": f"old-{source_turn_id}",
            "complete": True,
            "source_turn_id": source_turn_id,
        },
        observed_at="2026-01-01T00:00:00+00:00",
    ) == 1
    assert merge_turn_content(
        db_path,
        host_id,
        "worker-1",
        {
            "assistant_final_text": f"new-{source_turn_id}",
            "complete": True,
            "source_turn_id": source_turn_id,
        },
        observed_at="2026-01-02T00:00:00+00:00",
    ) == 1
    with sqlite3.connect(str(db_path)) as conn:
        rows = conn.execute(
            """
            SELECT turn_id, content_revision, is_current
            FROM turn_content_revisions
            WHERE host_id = ?
              AND assistant_final_text IN (?, ?)
            ORDER BY is_current
            """,
            (
                host_id,
                f"old-{source_turn_id}",
                f"new-{source_turn_id}",
            ),
        ).fetchall()
        if not retain_source_anchor:
            source_ids = [
                int(row[0])
                for row in conn.execute(
                    """
                    SELECT id
                    FROM connector_outbox
                    WHERE host_id = ? AND connector = 'turn-final'
                      AND delivery_kind = 'final_ready'
                      AND content_revision = ? AND status = 'superseded'
                    """,
                    (host_id, str(rows[0][1])),
                ).fetchall()
            ]
            if source_ids:
                placeholders = ",".join("?" for _ in source_ids)
                conn.execute(
                    f"DELETE FROM connector_deliveries "
                    f"WHERE outbox_id IN ({placeholders})",
                    source_ids,
                )
                conn.execute(
                    f"DELETE FROM connector_outbox "
                    f"WHERE id IN ({placeholders})",
                    source_ids,
                )
    assert len(rows) == 2
    assert rows[0][2] == 0
    assert rows[1][2] == 1
    return str(rows[0][0]), str(rows[0][1]), str(rows[1][1])


def _insert_retention_plan(
    conn: sqlite3.Connection,
    *,
    host_id: str,
    turn_id: str,
    revision: str,
    token: str,
    state: str,
    created_at: str = "2026-01-01T00:00:00+00:00",
    activated_at: str | None = None,
    completed_at: str | None = None,
    replaces_plan_token: str | None = None,
    outbox_status: str | None = None,
    audit_status: str | None = None,
    audit_at: str | None = None,
    part_count: int = 1,
    source_outbox_id: int | None = None,
) -> tuple[int, int | None]:
    conn.execute(
        """
        INSERT INTO turn_presentation_plans (
            host_id, name, plan_token, turn_id, content_revision,
            source_outbox_id, presentation_version, part_count, state,
            replaces_plan_token, created_at, activated_at, completed_at
        ) VALUES (?, 'turn-final', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            host_id,
            token,
            turn_id,
            revision,
            source_outbox_id,
            f"test-{token}",
            int(part_count),
            state,
            replaces_plan_token,
            created_at,
            activated_at,
            completed_at,
        ),
    )
    plan_id = int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])
    if outbox_status is None:
        if state == "preparing":
            conn.execute(
                """
                INSERT INTO turn_presentation_jobs (
                    plan_id, sequence_index, operation, part_ordinal,
                    spans_json, created_at
                ) VALUES (?, 0, 'upsert', 0, '[]', ?)
                """,
                (plan_id, created_at),
            )
        return plan_id, None
    payload_json = json.dumps(
        {"schema_version": 1, "content_revision": revision},
        sort_keys=True,
        separators=(",", ":"),
    )
    conn.execute(
        """
        INSERT INTO connector_outbox (
            host_id, connector, delivery_key, status, payload_json,
            private_state_json, created_at, updated_at
        ) VALUES (?, 'turn-final', ?, ?, ?, '{}', ?, ?)
        """,
        (
            host_id,
            f"job-{token}",
            outbox_status,
            payload_json,
            created_at,
            created_at,
        ),
    )
    outbox_id = int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])
    conn.execute(
        """
        INSERT INTO turn_presentation_jobs (
            plan_id, sequence_index, operation, part_ordinal,
            spans_json, outbox_id, created_at
        ) VALUES (?, 0, 'upsert', 0, '[]', ?, ?)
        """,
        (plan_id, outbox_id, created_at),
    )
    if audit_status is not None:
        conn.execute(
            """
            INSERT INTO connector_deliveries (
                outbox_id, host_id, connector, delivery_key, attempt,
                status, created_at, delivered_at
            ) VALUES (?, ?, 'turn-final', ?, 1, ?, ?, ?)
            """,
            (
                outbox_id,
                host_id,
                f"job-{token}",
                audit_status,
                created_at,
                audit_at,
            ),
        )
    return plan_id, outbox_id


def _insert_retention_source_anchor(
    conn: sqlite3.Connection,
    *,
    host_id: str,
    turn_id: str,
    revision: str,
    key: str,
    status: str = "superseded",
    updated_at: str = "2026-01-01T00:00:00+00:00",
    attempt_status: str | None = None,
    attempt_at: str | None = None,
) -> int:
    payload_json = json.dumps(
        {"schema_version": 1, "content_revision": revision},
        sort_keys=True,
        separators=(",", ":"),
    )
    conn.execute(
        """
        INSERT INTO connector_outbox (
            host_id, connector, delivery_key, delivery_kind, turn_id,
            content_revision, status, payload_json, private_state_json,
            created_at, updated_at
        ) VALUES (?, 'turn-final', ?, 'final_ready', ?, ?, ?, ?, '{}', ?, ?)
        """,
        (
            host_id,
            key,
            turn_id,
            revision,
            status,
            payload_json,
            updated_at,
            updated_at,
        ),
    )
    source_outbox_id = int(
        conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    )
    if attempt_status is not None:
        conn.execute(
            """
            INSERT INTO connector_deliveries (
                outbox_id, host_id, connector, delivery_key, attempt,
                status, created_at, delivered_at
            ) VALUES (?, ?, 'turn-final', ?, 1, ?, ?, ?)
            """,
            (
                source_outbox_id,
                host_id,
                key,
                attempt_status,
                attempt_at or updated_at,
                attempt_at,
            ),
        )
    return source_outbox_id


def test_turn_content_maintenance_dry_run_batch_age_idempotence_and_integrity(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "content-maintenance-batch.db"
    host_id = "retention-batch"
    revisions = [
        _seed_superseded_content_revision(
            db_path,
            host_id=host_id,
            source_turn_id=f"source-{index}",
            retain_source_anchor=True,
        )[1]
        for index in range(3)
    ]

    dry_run = run_store_maintenance(
        db_path,
        host_id,
        retention_days=7,
        max_outbox_attempts=99,
        now="2026-01-10T00:00:00+00:00",
        dry_run=True,
        content_batch_size=2,
    )
    with sqlite3.connect(str(db_path)) as conn:
        after_dry_run = conn.execute(
            """
            SELECT COUNT(*)
            FROM turn_content_revisions
            WHERE host_id = ? AND is_current = 0
            """,
            (host_id,),
        ).fetchone()[0]
    batches: list[dict[str, Any]] = []
    for _ in range(8):
        result = run_store_maintenance(
            db_path,
            host_id,
            retention_days=7,
            max_outbox_attempts=99,
            now="2026-01-10T00:00:00+00:00",
            content_batch_size=2,
        )
        batches.append(result["turn_content"])
        if result["turn_content"]["examined"] == 0:
            break
    with sqlite3.connect(str(db_path)) as conn:
        remaining = conn.execute(
            """
            SELECT content_revision, is_current
            FROM turn_content_revisions
            WHERE host_id = ?
            """,
            (host_id,),
        ).fetchall()
        stale_sources = conn.execute(
            """
            SELECT COUNT(*)
            FROM connector_outbox
            WHERE host_id = ? AND delivery_kind = 'final_ready'
              AND status = 'superseded'
            """,
            (host_id,),
        ).fetchone()[0]
        foreign_keys = conn.execute("PRAGMA foreign_key_check").fetchall()
        integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]

    assert len(revisions) == 3
    assert dry_run["turn_content"] == {
        "dry_run": True,
        "retention_days": 7,
        "cutoff_at": "2026-01-03T00:00:00+00:00",
        "stale_preparing_before": "2026-01-03T00:00:00+00:00",
        "batch_size": 2,
        "examined": 2,
        "deleted": 2,
        "skipped_reference": 0,
        "deleted_rows": {
            "plans": 0,
            "recoveries": 0,
            "jobs": 0,
            "queue_anchors": 2,
            "attempts": 0,
            "revisions": 0,
        },
    }
    assert after_dry_run == 3
    assert batches[-1]["examined"] == 0
    assert batches[-1]["deleted"] == 0
    assert all(batch["examined"] <= 2 for batch in batches)
    assert sum(
        batch["deleted_rows"]["queue_anchors"] for batch in batches
    ) == 3
    assert sum(batch["deleted_rows"]["revisions"] for batch in batches) == 3
    assert len(remaining) == 4
    assert all(row[1] == 1 for row in remaining)
    assert stale_sources == 0
    assert foreign_keys == []
    assert integrity == "ok"


def test_turn_content_maintenance_removes_only_stale_preparing_plan(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "content-maintenance-preparing.db"
    host_id = "retention-preparing"
    old_turn, old_revision, _ = _seed_superseded_content_revision(
        db_path,
        host_id=host_id,
        source_turn_id="stale",
    )
    recent_turn, recent_revision, _ = _seed_superseded_content_revision(
        db_path,
        host_id=host_id,
        source_turn_id="recent",
    )
    with sqlite3.connect(str(db_path)) as conn:
        _insert_retention_plan(
            conn,
            host_id=host_id,
            turn_id=old_turn,
            revision=old_revision,
            token="twplan1.stale",
            state="preparing",
        )
        _insert_retention_plan(
            conn,
            host_id=host_id,
            turn_id=recent_turn,
            revision=recent_revision,
            token="twplan1.recent",
            state="preparing",
            created_at="2026-01-09T00:00:00+00:00",
        )

    result = run_store_maintenance(
        db_path,
        host_id,
        retention_days=7,
        max_outbox_attempts=99,
        now="2026-01-10T00:00:00+00:00",
        content_batch_size=10,
    )
    revision_cleanup = run_store_maintenance(
        db_path,
        host_id,
        retention_days=7,
        max_outbox_attempts=99,
        now="2026-01-10T00:00:00+00:00",
        content_batch_size=10,
    )
    with sqlite3.connect(str(db_path)) as conn:
        plans = {
            row[0]
            for row in conn.execute(
                "SELECT plan_token FROM turn_presentation_plans WHERE host_id = ?",
                (host_id,),
            ).fetchall()
        }
        old_exists = conn.execute(
            """
            SELECT COUNT(*) FROM turn_content_revisions
            WHERE host_id = ? AND content_revision = ?
            """,
            (host_id, old_revision),
        ).fetchone()[0]
        recent_exists = conn.execute(
            """
            SELECT COUNT(*) FROM turn_content_revisions
            WHERE host_id = ? AND content_revision = ?
            """,
            (host_id, recent_revision),
        ).fetchone()[0]

    assert result["turn_content"]["examined"] == 1
    assert result["turn_content"]["deleted"] == 1
    assert result["turn_content"]["skipped_reference"] == 0
    assert result["turn_content"]["deleted_rows"]["plans"] == 1
    assert result["turn_content"]["deleted_rows"]["jobs"] == 1
    assert revision_cleanup["turn_content"]["examined"] == 0
    assert result["turn_content"]["deleted_rows"]["revisions"] == 1
    assert plans == {"twplan1.recent"}
    assert old_exists == 0
    assert recent_exists == 1


def test_turn_content_maintenance_reaps_old_terminal_anchor_chain(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "content-maintenance-terminal.db"
    host_id = "retention-terminal"
    turn_id, old_revision, current_revision = _seed_superseded_content_revision(
        db_path,
        host_id=host_id,
        source_turn_id="terminal",
        retain_source_anchor=True,
    )
    with sqlite3.connect(str(db_path)) as conn:
        source_outbox_id = int(
            conn.execute(
                """
                SELECT id
                FROM connector_outbox
                WHERE host_id = ? AND delivery_kind = 'final_ready'
                  AND content_revision = ? AND status = 'superseded'
                """,
                (host_id, old_revision),
            ).fetchone()[0]
        )
        conn.execute(
            """
            INSERT INTO connector_deliveries (
                outbox_id, host_id, connector, delivery_key, attempt,
                status, created_at, delivered_at
            ) VALUES (?, ?, 'turn-final', 'source-old', 1, 'superseded', ?, ?)
            """,
            (
                source_outbox_id,
                host_id,
                "2026-01-02T00:00:00+00:00",
                "2026-01-02T00:00:00+00:00",
            ),
        )
        old_plan_id, _ = _insert_retention_plan(
            conn,
            host_id=host_id,
            turn_id=turn_id,
            revision=old_revision,
            token="twplan1.old",
            state="superseded",
            activated_at="2026-01-01T00:00:00+00:00",
            outbox_status="delivered",
            audit_status="delivered",
            audit_at="2026-01-01T00:01:00+00:00",
            source_outbox_id=source_outbox_id,
        )
        current_plan_id, _ = _insert_retention_plan(
            conn,
            host_id=host_id,
            turn_id=turn_id,
            revision=current_revision,
            token="twplan1.current",
            state="completed",
            created_at="2026-01-02T00:00:00+00:00",
            activated_at="2026-01-02T00:00:00+00:00",
            completed_at="2026-01-02T00:01:00+00:00",
        )
        conn.execute(
            """
            INSERT INTO turn_presentation_recoveries (
                host_id, name, request_id, failed_plan_id, recovered_plan_id,
                failed_plan_token, recovered_plan_token, generation,
                source_job_count, delivered_prefix_count, fresh_job_count,
                retained_failed_job_count, prior_attempt_count, outcome,
                created_at
            ) VALUES (
                ?, 'turn-final', 'retention-recovery', ?, ?,
                'twplan1.old', 'twplan1.current', 2, 1, 0, 1, 1, 1,
                'recovered', ?
            )
            """,
            (
                host_id,
                old_plan_id,
                current_plan_id,
                "2026-01-02T00:01:00+00:00",
            ),
        )

    result = run_store_maintenance(
        db_path,
        host_id,
        retention_days=7,
        max_outbox_attempts=99,
        now="2026-01-20T00:00:00+00:00",
        content_batch_size=10,
    )
    revision_cleanup = run_store_maintenance(
        db_path,
        host_id,
        retention_days=7,
        max_outbox_attempts=99,
        now="2026-01-20T00:00:00+00:00",
        content_batch_size=10,
    )
    with sqlite3.connect(str(db_path)) as conn:
        plans = conn.execute(
            """
            SELECT plan_token FROM turn_presentation_plans
            WHERE host_id = ? ORDER BY plan_token
            """,
            (host_id,),
        ).fetchall()
        revisions = conn.execute(
            """
            SELECT content_revision, is_current
            FROM turn_content_revisions
            WHERE host_id = ?
            """,
            (host_id,),
        ).fetchall()
        counts = (
            conn.execute("SELECT COUNT(*) FROM turn_presentation_jobs").fetchone()[0],
            conn.execute("SELECT COUNT(*) FROM connector_outbox").fetchone()[0],
            conn.execute("SELECT COUNT(*) FROM connector_deliveries").fetchone()[0],
        )
        integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]

    assert result["turn_content"]["examined"] == 1
    assert result["turn_content"]["deleted"] == 1
    assert result["turn_content"]["skipped_reference"] == 0
    assert result["turn_content"]["deleted_rows"] == {
        "plans": 1,
        "recoveries": 1,
        "jobs": 1,
        "queue_anchors": 2,
        "attempts": 2,
        "revisions": 1,
    }
    assert revision_cleanup["turn_content"]["examined"] == 0
    assert revision_cleanup["turn_content"]["deleted_rows"]["revisions"] == 0
    assert plans == [("twplan1.current",)]
    assert len(revisions) == 2
    assert (current_revision, 1) in revisions
    assert all(row[1] == 1 for row in revisions)
    assert counts == (0, 1, 0)
    assert integrity == "ok"


def test_turn_content_maintenance_collapses_terminal_unacked_footprints(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "content-maintenance-summary.db"
    host_id = "retention-summary"
    turn_id, old_revision, current_revision = _seed_superseded_content_revision(
        db_path,
        host_id=host_id,
        source_turn_id="summary",
    )
    part_counts = [2, 5, 3, 4]
    with sqlite3.connect(str(db_path)) as conn:
        for index, part_count in enumerate(part_counts):
            source_outbox_id = _insert_retention_source_anchor(
                conn,
                host_id=host_id,
                turn_id=turn_id,
                revision=old_revision,
                key=f"summary-source-{index}",
                attempt_status="superseded",
                attempt_at="2026-01-01T00:30:00+00:00",
            )
            _insert_retention_plan(
                conn,
                host_id=host_id,
                turn_id=turn_id,
                revision=old_revision,
                token=f"twplan1.summary-{index}",
                state="superseded",
                activated_at=f"2026-01-01T0{index}:00:00+00:00",
                outbox_status="superseded",
                audit_status="superseded",
                audit_at="2026-01-01T00:30:00+00:00",
                part_count=part_count,
                source_outbox_id=source_outbox_id,
            )

    cleanup_batches: list[dict[str, Any]] = []
    for _ in range(4):
        cleanup = run_store_maintenance(
            db_path,
            host_id,
            retention_days=7,
            max_outbox_attempts=99,
            now="2026-01-20T00:00:00+00:00",
            content_batch_size=20,
        )
        cleanup_batches.append(cleanup["turn_content"])
        if cleanup["turn_content"]["examined"] == 0:
            break
    with sqlite3.connect(str(db_path)) as conn:
        summaries = conn.execute(
            """
            SELECT plan_token, part_count, source_outbox_id
            FROM turn_presentation_plans
            WHERE host_id = ? AND turn_id = ? AND state = 'superseded'
            ORDER BY id
            """,
            (host_id, turn_id),
        ).fetchall()
        summary_jobs = conn.execute(
            """
            SELECT COUNT(*)
            FROM turn_presentation_jobs AS jobs
            JOIN turn_presentation_plans AS plans ON plans.id = jobs.plan_id
            WHERE plans.host_id = ? AND plans.turn_id = ?
              AND plans.state = 'superseded'
            """,
            (host_id, turn_id),
        ).fetchone()[0]
        stale_sources = conn.execute(
            """
            SELECT COUNT(*)
            FROM connector_outbox
            WHERE host_id = ? AND delivery_kind = 'final_ready'
              AND status = 'superseded'
            """,
            (host_id,),
        ).fetchone()[0]
        revision_state = conn.execute(
            """
            SELECT user_state, final_state, user_char_length, final_char_length
            FROM turn_content_revisions
            WHERE host_id = ? AND turn_id = ? AND content_revision = ?
            """,
            (host_id, turn_id, current_revision),
        ).fetchone()

    assert sum(batch["examined"] for batch in cleanup_batches) == len(part_counts)
    assert sum(batch["deleted"] for batch in cleanup_batches) == len(part_counts)
    assert sum(
        batch["deleted_rows"]["plans"] for batch in cleanup_batches
    ) == len(part_counts) - 1
    assert sum(
        batch["deleted_rows"]["recoveries"] for batch in cleanup_batches
    ) == 0
    assert sum(
        batch["deleted_rows"]["jobs"] for batch in cleanup_batches
    ) == len(part_counts)
    assert sum(
        batch["deleted_rows"]["queue_anchors"] for batch in cleanup_batches
    ) == len(part_counts) * 2
    assert sum(
        batch["deleted_rows"]["attempts"] for batch in cleanup_batches
    ) == len(part_counts) * 2
    assert sum(
        batch["deleted_rows"]["revisions"] for batch in cleanup_batches
    ) == 0
    assert summaries == [("twplan1.summary-3", max(part_counts), None)]
    assert summary_jobs == 0
    assert stale_sources == 0

    source = store_sqlite.poll_connector_outbox(
        db_path,
        host_id,
        "turn-final",
        limit=1,
        lease_seconds=600,
        now="2026-01-21T00:00:00+00:00",
    )["items"][0]
    begin = store_sqlite.prepare_connector_plan_begin(
        db_path,
        host_id,
        name="turn-final",
        turn_id=turn_id,
        content_revision=current_revision,
        presentation_version="retention-smaller",
        part_count=1,
        source_ref=source["ref"],
        now="2026-01-21T00:00:00+00:00",
    )
    assert begin["ok"] is True, (begin, source)
    spans = []
    if str(revision_state[0]) == "complete":
        spans.append(
            {
                "field": "user_text",
                "start_char": 0,
                "end_char": int(revision_state[2]),
            }
        )
    if str(revision_state[1]) == "complete":
        spans.append(
            {
                "field": "assistant_final_text",
                "start_char": 0,
                "end_char": int(revision_state[3]),
            }
        )
    part = store_sqlite.prepare_connector_plan_part(
        db_path,
        host_id,
        name="turn-final",
        plan_token=str(begin["plan_token"]),
        ordinal=0,
        spans=spans,
        now="2026-01-21T00:01:00+00:00",
    )
    assert part["ok"] is True
    commit = store_sqlite.prepare_connector_plan_commit(
        db_path,
        host_id,
        name="turn-final",
        plan_token=str(begin["plan_token"]),
        source_ref=source["ref"],
        now="2026-01-21T00:02:00+00:00",
    )
    assert commit["ok"] is True
    with sqlite3.connect(str(db_path)) as conn:
        operations = conn.execute(
            """
            SELECT operation, COUNT(*)
            FROM turn_presentation_jobs AS jobs
            JOIN turn_presentation_plans AS plans ON plans.id = jobs.plan_id
            WHERE plans.plan_token = ?
            GROUP BY operation
            ORDER BY operation
            """,
            (str(begin["plan_token"]),),
        ).fetchall()
        foreign_keys = conn.execute("PRAGMA foreign_key_check").fetchall()
        integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]

    assert commit["job_count"] == max(part_counts)
    assert operations == [("retire", max(part_counts) - 1), ("upsert", 1)]
    assert foreign_keys == []
    assert integrity == "ok"


def test_acknowledged_revision_count_bounds_one_frequently_revised_turn(
    tmp_path: Path,
) -> None:
    from tendwire.connectors import ConnectorOutboxAPI

    db_path = tmp_path / "acknowledged-revision-count.db"
    host_id = "acknowledged-revision-count"
    config = Config(host_id=host_id, db_path=db_path)
    snapshot = project_from_raw(
        config,
        workers=[
            {
                "id": "worker-1",
                "name": "claude",
                "status": "active",
                "meta": {
                    "stable_key": "wsk1_" + ("4" * 64),
                    "stable_key_version": 1,
                },
            }
        ],
    )
    init_store(db_path)
    save_snapshot(db_path, snapshot)
    api = ConnectorOutboxAPI(db_path, host_id)
    revisions: list[str] = []
    for index in range(4):
        assert merge_turn_content(
            db_path,
            host_id,
            "worker-1",
            {
                "assistant_final_text": f"revision-{index}",
                "complete": True,
                "source_turn_id": "frequently-revised",
            },
            observed_at=f"2026-07-13T00:0{index}:00+00:00",
        ) == 1
        ready = api.poll({"name": "turn-final", "limit": 1})["items"][0]
        revision = str(ready["payload"]["content_revision"])
        revisions.append(revision)
        final_length = int(
            ready["payload"]["content"]["fields"]["assistant_final_text"][
                "char_length"
            ]
        )
        begun = api.prepare(
            {
                "schema_version": 1,
                "action": "begin",
                "name": "turn-final",
                "turn_id": ready["payload"]["turn_id"],
                "content_revision": revision,
                "presentation_version": "retention-count-v1",
                "part_count": 1,
                "source_ref": ready["ref"],
            }
        )
        assert begun["ok"] is True
        assert api.prepare(
            {
                "schema_version": 1,
                "action": "part",
                "name": "turn-final",
                "plan_token": begun["plan_token"],
                "ordinal": 0,
                "spans": [
                    {
                        "field": "assistant_final_text",
                        "start_char": 0,
                        "end_char": final_length,
                    }
                ],
            }
        )["ok"] is True
        assert api.prepare(
            {
                "schema_version": 1,
                "action": "commit",
                "name": "turn-final",
                "plan_token": begun["plan_token"],
                "source_ref": ready["ref"],
            }
        )["ok"] is True
        job = api.poll({"name": "turn-final", "limit": 1})["items"][0]
        assert api.ack({"name": "turn-final", "ref": job["ref"]})["ok"] is True

    turn_id = str(ready["payload"]["turn_id"])
    maintenance_results: list[dict[str, Any]] = []
    for _ in range(3):
        maintenance_results.append(
            run_store_maintenance(
                db_path,
                host_id,
                retention_days=36500,
                acknowledged_final_retention_days=36500,
                acknowledged_final_retention_count=2,
                max_outbox_attempts=99,
                now="2026-07-14T00:00:00+00:00",
                content_batch_size=20,
            )
        )
    status = store_status(
        db_path,
        host_id,
        acknowledged_final_retention_days=36500,
        acknowledged_final_retention_count=2,
    )
    with sqlite3.connect(str(db_path)) as conn:
        retained_roots = conn.execute(
            """
            SELECT content_revision
            FROM connector_outbox
            WHERE host_id = ? AND delivery_kind = 'final_ready'
              AND status = 'delivered'
            ORDER BY id
            """,
            (host_id,),
        ).fetchall()
        retained_plans = conn.execute(
            """
            SELECT content_revision, state
            FROM turn_presentation_plans
            WHERE host_id = ?
            ORDER BY id
            """,
            (host_id,),
        ).fetchall()
        retained_revisions = conn.execute(
            """
            SELECT content_revision, is_current
            FROM turn_content_revisions
            WHERE host_id = ? AND turn_id = ?
            ORDER BY created_at
            """,
            (host_id, turn_id),
        ).fetchall()
        turn_count = conn.execute(
            "SELECT COUNT(*) FROM turns WHERE host_id = ? AND turn_id = ?",
            (host_id, turn_id),
        ).fetchone()[0]
        foreign_keys = conn.execute("PRAGMA foreign_key_check").fetchall()

    assert maintenance_results[0]["turn_content"]["deleted"] == 2
    assert maintenance_results[0]["turn_content"]["deleted_rows"]["revisions"] == 2
    expected = [(revisions[-2],), (revisions[-1],)]
    assert retained_roots == expected, json.dumps(maintenance_results, sort_keys=True)
    assert retained_plans == [
        (revisions[-2], "superseded"),
        (revisions[-1], "completed"),
    ]
    assert retained_revisions == [
        (revisions[-2], 0),
        (revisions[-1], 1),
    ]
    assert turn_count == 1
    assert foreign_keys == []
    assert status["final_retention"]["acknowledged"] == 2
    assert status["final_retention"]["eligible"] == 0
    assert status["final_retention"]["storage_pressure"] is False


@pytest.mark.parametrize(
    ("source_status", "attempt_status", "source_at", "attempt_at"),
    [
        ("queued", None, "2026-01-01T00:00:00+00:00", None),
        ("leased", None, "2026-01-01T00:00:00+00:00", None),
        ("deferred", None, "2026-01-01T00:00:00+00:00", None),
        ("retry", None, "2026-01-01T00:00:00+00:00", None),
        ("dead_letter", None, "2026-01-01T00:00:00+00:00", None),
        ("awaiting_ack", None, "2026-01-01T00:00:00+00:00", None),
        (
            "superseded",
            "awaiting_ack",
            "2026-01-01T00:00:00+00:00",
            "2026-01-01T00:00:00+00:00",
        ),
        (
            "superseded",
            "delivered",
            "2026-01-01T00:00:00+00:00",
            "2026-01-19T00:00:00+00:00",
        ),
        ("superseded", None, "2026-01-19T00:00:00+00:00", None),
    ],
)
def test_turn_content_maintenance_preserves_live_and_young_final_sources(
    tmp_path: Path,
    source_status: str,
    attempt_status: str | None,
    source_at: str,
    attempt_at: str | None,
) -> None:
    case_name = f"{source_status}-{attempt_status or 'none'}-{source_at[8:10]}"
    db_path = tmp_path / f"content-maintenance-source-{case_name}.db"
    host_id = f"retention-source-{case_name}"
    _, old_revision, _ = _seed_superseded_content_revision(
        db_path,
        host_id=host_id,
        source_turn_id=case_name,
        retain_source_anchor=True,
    )
    with sqlite3.connect(str(db_path)) as conn:
        source = conn.execute(
            """
            SELECT id, turn_id
            FROM connector_outbox
            WHERE host_id = ? AND delivery_kind = 'final_ready'
              AND content_revision = ? AND status = 'superseded'
            """,
            (host_id, old_revision),
        ).fetchone()
        source_outbox_id = int(source[0])
        conn.execute(
            "UPDATE connector_outbox SET status = ?, updated_at = ? WHERE id = ?",
            (source_status, source_at, source_outbox_id),
        )
        if attempt_status is not None:
            conn.execute(
                """
                INSERT INTO connector_deliveries (
                    outbox_id, host_id, connector, delivery_key, attempt,
                    status, created_at, delivered_at
                ) VALUES (?, ?, 'turn-final', 'protected-source', 1, ?, ?, ?)
                """,
                (
                    source_outbox_id,
                    host_id,
                    attempt_status,
                    attempt_at,
                    attempt_at if attempt_status == "delivered" else None,
                ),
            )

    result = run_store_maintenance(
        db_path,
        host_id,
        retention_days=7,
        max_outbox_attempts=99,
        now="2026-01-20T00:00:00+00:00",
    )
    with sqlite3.connect(str(db_path)) as conn:
        preserved = conn.execute(
            "SELECT status FROM connector_outbox WHERE id = ?",
            (source_outbox_id,),
        ).fetchone()
        attempt_count = conn.execute(
            "SELECT COUNT(*) FROM connector_deliveries WHERE outbox_id = ?",
            (source_outbox_id,),
        ).fetchone()[0]
        integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]

    assert result["turn_content"]["examined"] == 0
    assert result["turn_content"]["deleted"] == 0
    assert preserved == (source_status,)
    assert attempt_count == int(attempt_status is not None)
    assert integrity == "ok"


def test_turn_content_maintenance_typed_malformed_final_reference_is_authoritative(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "content-maintenance-typed-reference.db"
    host_id = "retention-typed-reference"
    turn_id, old_revision, _ = _seed_superseded_content_revision(
        db_path,
        host_id=host_id,
        source_turn_id="typed-reference",
    )
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            """
            DELETE FROM connector_deliveries
            WHERE outbox_id IN (
                SELECT id
                FROM connector_outbox
                WHERE host_id = ? AND turn_id = ?
            )
            """,
            (host_id, turn_id),
        )
        conn.execute(
            "DELETE FROM connector_outbox WHERE host_id = ? AND turn_id = ?",
            (host_id, turn_id),
        )
        conn.execute(
            """
            DELETE FROM turn_content_page_boundaries
            WHERE host_id = ? AND turn_id = ? AND content_revision != ?
            """,
            (host_id, turn_id, old_revision),
        )
        conn.execute(
            """
            DELETE FROM turn_content_revisions
            WHERE host_id = ? AND turn_id = ? AND content_revision != ?
            """,
            (host_id, turn_id, old_revision),
        )
        conn.execute(
            "UPDATE turns SET payload_json = '{}' WHERE host_id = ? AND turn_id = ?",
            (host_id, turn_id),
        )
        conn.execute(
            """
            INSERT INTO connector_outbox (
                host_id, connector, delivery_key, delivery_kind, turn_id,
                content_revision, status, payload_json, private_state_json,
                created_at, updated_at
            ) VALUES (
                ?, 'turn-final', 'typed-malformed', 'final_migration_hold',
                ?, ?, 'dead_letter', '{malformed', '{}', ?, ?
            )
            """,
            (
                host_id,
                turn_id,
                old_revision,
                "2026-01-01T00:00:00+00:00",
                "2026-01-01T00:00:00+00:00",
            ),
        )
        prune_deleted = store_sqlite._delete_turn_if_unreferenced_conn(
            conn,
            host_id,
            turn_id,
        )

    protected_cleanup = run_store_maintenance(
        db_path,
        host_id,
        retention_days=7,
        max_outbox_attempts=99,
        now="2026-01-20T00:00:00+00:00",
    )
    with sqlite3.connect(str(db_path)) as conn:
        protected_revision = conn.execute(
            """
            SELECT COUNT(*)
            FROM turn_content_revisions
            WHERE host_id = ? AND turn_id = ? AND content_revision = ?
            """,
            (host_id, turn_id, old_revision),
        ).fetchone()[0]
        protected_turn = conn.execute(
            "SELECT COUNT(*) FROM turns WHERE host_id = ? AND turn_id = ?",
            (host_id, turn_id),
        ).fetchone()[0]
        conn.execute(
            """
            DELETE FROM connector_outbox
            WHERE host_id = ? AND turn_id = ?
              AND delivery_kind GLOB 'final_*'
            """,
            (host_id, turn_id),
        )

    released_cleanup = run_store_maintenance(
        db_path,
        host_id,
        retention_days=7,
        max_outbox_attempts=99,
        now="2026-01-20T00:00:00+00:00",
    )
    with sqlite3.connect(str(db_path)) as conn:
        released_revision = conn.execute(
            """
            SELECT COUNT(*)
            FROM turn_content_revisions
            WHERE host_id = ? AND turn_id = ? AND content_revision = ?
            """,
            (host_id, turn_id, old_revision),
        ).fetchone()[0]
        turn_deleted = store_sqlite._delete_turn_if_unreferenced_conn(
            conn,
            host_id,
            turn_id,
        )
        released_turn = conn.execute(
            "SELECT COUNT(*) FROM turns WHERE host_id = ? AND turn_id = ?",
            (host_id, turn_id),
        ).fetchone()[0]
        foreign_keys = conn.execute("PRAGMA foreign_key_check").fetchall()
        integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]

    assert prune_deleted is False
    assert protected_cleanup["turn_content"]["examined"] == 0
    assert protected_revision == 1
    assert protected_turn == 1
    assert released_cleanup["turn_content"]["deleted_rows"]["revisions"] == 1
    assert released_revision == 0
    assert turn_deleted is True
    assert released_turn == 0
    assert foreign_keys == []
    assert integrity == "ok"


@pytest.mark.parametrize("outbox_status", ["queued", "retry", "deferred", "leased"])
def test_turn_content_maintenance_preserves_live_outbox_and_lease_references(
    tmp_path: Path,
    outbox_status: str,
) -> None:
    db_path = tmp_path / f"content-maintenance-{outbox_status}.db"
    host_id = f"retention-{outbox_status}"
    turn_id, old_revision, _ = _seed_superseded_content_revision(
        db_path,
        host_id=host_id,
        source_turn_id=outbox_status,
    )
    with sqlite3.connect(str(db_path)) as conn:
        _insert_retention_plan(
            conn,
            host_id=host_id,
            turn_id=turn_id,
            revision=old_revision,
            token=f"twplan1.{outbox_status}",
            state="superseded",
            outbox_status=outbox_status,
            audit_status="leased" if outbox_status == "leased" else None,
        )

    result = run_store_maintenance(
        db_path,
        host_id,
        retention_days=7,
        max_outbox_attempts=99,
        now="2026-01-20T00:00:00+00:00",
    )
    with sqlite3.connect(str(db_path)) as conn:
        preserved = conn.execute(
            """
            SELECT COUNT(*) FROM turn_content_revisions
            WHERE host_id = ? AND content_revision = ?
            """,
            (host_id, old_revision),
        ).fetchone()[0]

    assert result["turn_content"]["examined"] == 0
    assert result["turn_content"]["deleted"] == 0
    assert result["turn_content"]["skipped_reference"] == 0
    assert preserved == 1


@pytest.mark.parametrize(
    "protection",
    [
        "active_plan",
        "retained_audit",
        "failed_prefix",
        "replacement_plan",
        "direct_outbox",
        "current_revision",
    ],
)
def test_turn_content_maintenance_preserves_all_reference_classes(
    tmp_path: Path,
    protection: str,
) -> None:
    db_path = tmp_path / f"content-maintenance-{protection}.db"
    host_id = f"retention-{protection}"
    if protection == "current_revision":
        config = Config(host_id=host_id, db_path=db_path)
        save_snapshot(
            db_path,
            project_from_raw(
                config,
                workers=[{"id": "worker-1", "name": "claude", "status": "active"}],
            ),
        )
        assert merge_turn_content(
            db_path,
            host_id,
            "worker-1",
            {
                "assistant_final_text": "current",
                "complete": True,
                "source_turn_id": "current",
            },
            observed_at="2026-01-01T00:00:00+00:00",
        ) == 1
        with sqlite3.connect(str(db_path)) as conn:
            old_revision = str(
                conn.execute(
                    """
                    SELECT content_revision FROM turn_content_revisions
                    WHERE host_id = ? AND is_current = 1
                    """,
                    (host_id,),
                ).fetchone()[0]
            )
    else:
        turn_id, old_revision, current_revision = _seed_superseded_content_revision(
            db_path,
            host_id=host_id,
            source_turn_id=protection,
        )
        with sqlite3.connect(str(db_path)) as conn:
            if protection == "active_plan":
                _insert_retention_plan(
                    conn,
                    host_id=host_id,
                    turn_id=turn_id,
                    revision=old_revision,
                    token="twplan1.active",
                    state="active",
                    activated_at="2026-01-01T00:00:00+00:00",
                )
            elif protection == "retained_audit":
                _insert_retention_plan(
                    conn,
                    host_id=host_id,
                    turn_id=turn_id,
                    revision=old_revision,
                    token="twplan1.audit",
                    state="superseded",
                    outbox_status="delivered",
                    audit_status="delivered",
                    audit_at="2026-01-19T00:00:00+00:00",
                )
            elif protection == "failed_prefix":
                _insert_retention_plan(
                    conn,
                    host_id=host_id,
                    turn_id=turn_id,
                    revision=old_revision,
                    token="twplan1.failed",
                    state="failed",
                    activated_at="2026-01-01T00:00:00+00:00",
                )
            elif protection == "replacement_plan":
                _insert_retention_plan(
                    conn,
                    host_id=host_id,
                    turn_id=turn_id,
                    revision=old_revision,
                    token="twplan1.predecessor",
                    state="superseded",
                )
                _insert_retention_plan(
                    conn,
                    host_id=host_id,
                    turn_id=turn_id,
                    revision=current_revision,
                    token="twplan1.replacement",
                    state="preparing",
                    created_at="2026-01-19T00:00:00+00:00",
                    replaces_plan_token="twplan1.predecessor",
                )
            elif protection == "direct_outbox":
                conn.execute(
                    """
                    INSERT INTO connector_outbox (
                        host_id, connector, delivery_key, status,
                        payload_json, private_state_json, created_at, updated_at
                    ) VALUES (?, 'turn-final', 'direct-reference', 'queued', ?, '{}', ?, ?)
                    """,
                    (
                        host_id,
                        json.dumps({"content_revision": old_revision}),
                        "2026-01-01T00:00:00+00:00",
                        "2026-01-01T00:00:00+00:00",
                    ),
                )

    result = run_store_maintenance(
        db_path,
        host_id,
        retention_days=7,
        max_outbox_attempts=99,
        now="2026-01-20T00:00:00+00:00",
    )
    with sqlite3.connect(str(db_path)) as conn:
        preserved = conn.execute(
            """
            SELECT COUNT(*) FROM turn_content_revisions
            WHERE host_id = ? AND content_revision = ?
            """,
            (host_id, old_revision),
        ).fetchone()[0]

    assert result["turn_content"]["examined"] == 0
    assert result["turn_content"]["deleted"] == 0
    assert result["turn_content"]["skipped_reference"] == 0
    assert preserved == 1


def test_turn_content_maintenance_batch_one_does_not_starve_after_reference(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "content-maintenance-starvation.db"
    host_id = "retention-starvation"
    protected_turn, protected_revision, _ = _seed_superseded_content_revision(
        db_path,
        host_id=host_id,
        source_turn_id="oldest-protected",
    )
    _, deletable_revision, _ = _seed_superseded_content_revision(
        db_path,
        host_id=host_id,
        source_turn_id="newer-deletable",
    )
    with sqlite3.connect(str(db_path)) as conn:
        _insert_retention_plan(
            conn,
            host_id=host_id,
            turn_id=protected_turn,
            revision=protected_revision,
            token="twplan1.starvation",
            state="active",
            activated_at="2026-01-01T00:00:00+00:00",
        )

    first = run_store_maintenance(
        db_path,
        host_id,
        retention_days=7,
        max_outbox_attempts=99,
        now="2026-01-20T00:00:00+00:00",
        content_batch_size=1,
    )
    second = run_store_maintenance(
        db_path,
        host_id,
        retention_days=7,
        max_outbox_attempts=99,
        now="2026-01-20T00:00:00+00:00",
        content_batch_size=1,
    )
    with sqlite3.connect(str(db_path)) as conn:
        protected_exists = conn.execute(
            """
            SELECT COUNT(*) FROM turn_content_revisions
            WHERE host_id = ? AND content_revision = ?
            """,
            (host_id, protected_revision),
        ).fetchone()[0]
        deletable_exists = conn.execute(
            """
            SELECT COUNT(*) FROM turn_content_revisions
            WHERE host_id = ? AND content_revision = ?
            """,
            (host_id, deletable_revision),
        ).fetchone()[0]

    assert first["turn_content"]["examined"] == 1
    assert first["turn_content"]["deleted_rows"]["revisions"] == 1
    assert second["turn_content"]["examined"] == 0
    assert protected_exists == 1
    assert deletable_exists == 0


def test_turn_content_maintenance_rechecks_references_under_immediate_lock(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "content-maintenance-race.db"
    host_id = "retention-race"
    turn_id, old_revision, _ = _seed_superseded_content_revision(
        db_path,
        host_id=host_id,
        source_turn_id="race",
    )
    original_candidates = store_sqlite._turn_content_retention_candidates_conn
    inserted = False

    def insert_reference_after_scan(
        conn: sqlite3.Connection,
        *,
        host_id: str,
        cutoff_at: str,
        batch_size: int,
        retention_count: int,
    ) -> list[tuple[str, int]]:
        nonlocal inserted
        candidates = original_candidates(
            conn,
            host_id=host_id,
            cutoff_at=cutoff_at,
            batch_size=batch_size,
            retention_count=retention_count,
        )
        conn.commit()
        if not inserted:
            with sqlite3.connect(str(db_path)) as concurrent:
                _insert_retention_plan(
                    concurrent,
                    host_id=host_id,
                    turn_id=turn_id,
                    revision=old_revision,
                    token="twplan1.race",
                    state="active",
                    activated_at="2026-01-19T00:00:00+00:00",
                )
            inserted = True
        return candidates

    monkeypatch.setattr(
        store_sqlite,
        "_turn_content_retention_candidates_conn",
        insert_reference_after_scan,
    )
    result = run_store_maintenance(
        db_path,
        host_id,
        retention_days=7,
        max_outbox_attempts=99,
        now="2026-01-20T00:00:00+00:00",
    )
    with sqlite3.connect(str(db_path)) as conn:
        preserved = conn.execute(
            """
            SELECT COUNT(*) FROM turn_content_revisions
            WHERE host_id = ? AND content_revision = ?
            """,
            (host_id, old_revision),
        ).fetchone()[0]
        integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]

    assert inserted is True
    assert result["turn_content"]["examined"] == 1
    assert result["turn_content"]["deleted"] == 0
    assert result["turn_content"]["skipped_reference"] == 1
    assert preserved == 1
    assert integrity == "ok"


def test_source_turn_history_pruning_retains_referenced_old_turn(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "protected-history.db"
    config = Config(host_id="protected-host", db_path=db_path)
    snapshot = project_from_raw(
        config,
        workers=[{"id": "worker-1", "name": "claude", "status": "active"}],
    )
    init_store(db_path)
    save_snapshot(db_path, snapshot)
    for index in range(6):
        assert merge_turn_content(
            db_path,
            "protected-host",
            "worker-1",
            {
                "assistant_final_text": f"answer {index}",
                "complete": True,
                "source_turn_id": f"source-{index}",
            },
            observed_at=f"2026-01-01T00:0{index}:00+00:00",
        ) == 1
    with sqlite3.connect(str(db_path)) as conn:
        oldest_turn, oldest_revision = conn.execute(
            """
            SELECT turns.turn_id, revisions.content_revision
            FROM turns
            JOIN turn_content_revisions AS revisions
              ON revisions.host_id = turns.host_id
             AND revisions.turn_id = turns.turn_id
             AND revisions.is_current = 1
            WHERE turns.host_id = ? AND revisions.assistant_final_text = ?
            """,
            ("protected-host", "answer 0"),
        ).fetchone()
        conn.execute(
            """
            INSERT INTO turn_presentation_plans (
                host_id, name, plan_token, turn_id, content_revision,
                presentation_version, part_count, state, created_at
            ) VALUES (?, 'turn-final', 'twplan1.protected', ?, ?, 'test-v1', 1, 'active', ?)
            """,
            (
                "protected-host",
                oldest_turn,
                oldest_revision,
                "2026-01-01T00:10:00+00:00",
            ),
        )
        plan_id = conn.execute(
            "SELECT id FROM turn_presentation_plans WHERE plan_token = 'twplan1.protected'"
        ).fetchone()[0]
        conn.execute(
            """
            INSERT INTO connector_outbox (
                host_id, connector, delivery_key, status, payload_json,
                private_state_json, created_at, updated_at
            ) VALUES (?, 'turn-final', 'protected-job', 'leased', '{}', '{}', ?, ?)
            """,
            (
                "protected-host",
                "2026-01-01T00:10:00+00:00",
                "2026-01-01T00:10:00+00:00",
            ),
        )
        outbox_id = conn.execute(
            "SELECT id FROM connector_outbox WHERE delivery_key = 'protected-job'"
        ).fetchone()[0]
        conn.execute(
            """
            INSERT INTO turn_presentation_jobs (
                plan_id, sequence_index, operation, part_ordinal,
                spans_json, outbox_id, created_at
            ) VALUES (?, 0, 'upsert', 0, '[]', ?, ?)
            """,
            (plan_id, outbox_id, "2026-01-01T00:10:00+00:00"),
        )
        conn.execute(
            """
            INSERT INTO connector_deliveries (
                outbox_id, host_id, connector, delivery_key, attempt,
                status, created_at
            ) VALUES (?, ?, 'turn-final', 'protected-job', 1, 'leased', ?)
            """,
            (
                outbox_id,
                "protected-host",
                "2026-01-01T00:10:00+00:00",
            ),
        )

    for index in range(6, 10):
        assert merge_turn_content(
            db_path,
            "protected-host",
            "worker-1",
            {
                "assistant_final_text": f"answer {index}",
                "complete": True,
                "source_turn_id": f"source-{index}",
            },
            observed_at=f"2026-01-01T00:{index:02d}:00+00:00",
        ) == 1

    with sqlite3.connect(str(db_path)) as conn:
        retained = conn.execute(
            "SELECT COUNT(*) FROM turns WHERE host_id = ? AND turn_id = ?",
            ("protected-host", oldest_turn),
        ).fetchone()[0]
        retained_revision = conn.execute(
            """
            SELECT COUNT(*)
            FROM turn_content_revisions
            WHERE host_id = ? AND turn_id = ? AND content_revision = ?
            """,
            ("protected-host", oldest_turn, oldest_revision),
        ).fetchone()[0]
        source_count = conn.execute(
            """
            SELECT COUNT(*)
            FROM turns
            WHERE host_id = ? AND json_extract(payload_json, '$.source_turn_id') IS NOT NULL
            """,
            ("protected-host",),
        ).fetchone()[0]
        integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]
    assert retained == 1
    assert retained_revision == 1
    assert source_count == 10
    assert integrity == "ok"
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            """
            UPDATE turn_presentation_plans
            SET state = 'superseded'
            WHERE id = ?
            """,
            (plan_id,),
        )
        conn.execute(
            """
            UPDATE connector_outbox
            SET status = 'delivered', updated_at = ?
            WHERE id = ?
            """,
            ("2026-01-01T00:11:00+00:00", outbox_id),
        )
        conn.execute(
            """
            UPDATE connector_deliveries
            SET status = 'delivered', delivered_at = ?
            WHERE outbox_id = ?
            """,
            ("2026-01-01T00:11:00+00:00", outbox_id),
        )
    cleanup = run_store_maintenance(
        db_path,
        "protected-host",
        retention_days=7,
        max_outbox_attempts=99,
        now="2026-01-20T00:00:00+00:00",
    )
    assert merge_turn_content(
        db_path,
        "protected-host",
        "worker-1",
        {
            "assistant_final_text": "answer 10",
            "complete": True,
            "source_turn_id": "source-10",
        },
        observed_at="2026-01-01T00:20:00+00:00",
    ) == 1
    with sqlite3.connect(str(db_path)) as conn:
        eligible_turn = conn.execute(
            "SELECT COUNT(*) FROM turns WHERE host_id = ? AND turn_id = ?",
            ("protected-host", oldest_turn),
        ).fetchone()[0]
        eligible_revision = conn.execute(
            """
            SELECT COUNT(*) FROM turn_content_revisions
            WHERE host_id = ? AND turn_id = ? AND content_revision = ?
            """,
            ("protected-host", oldest_turn, oldest_revision),
        ).fetchone()[0]
        bounded_source_count = conn.execute(
            """
            SELECT COUNT(*)
            FROM turns
            WHERE host_id = ?
              AND json_extract(payload_json, '$.source_turn_id') IS NOT NULL
            """,
            ("protected-host",),
        ).fetchone()[0]
        final_integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]

    assert cleanup["turn_content"]["deleted_rows"] == {
        "plans": 1,
        "recoveries": 0,
        "jobs": 1,
        "queue_anchors": 1,
        "attempts": 1,
        "revisions": 0,
    }
    assert eligible_turn == 1
    assert eligible_revision == 1
    assert bounded_source_count == 11
    assert final_integrity == "ok"


def test_source_turn_creation_does_not_delete_fallback_current_revision(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "fallback-revision.db"
    host_id = "fallback-host"
    config = Config(host_id=host_id, db_path=db_path)
    save_snapshot(
        db_path,
        project_from_raw(
            config,
            workers=[{"id": "worker-1", "name": "claude", "status": "active"}],
        ),
    )
    assert merge_turn_content(
        db_path,
        host_id,
        "worker-1",
        {"assistant_final_text": "fallback answer", "complete": True},
        observed_at="2026-01-01T00:00:00+00:00",
    ) == 1
    with sqlite3.connect(str(db_path)) as conn:
        fallback_turn, fallback_revision = conn.execute(
            """
            SELECT turn_id, content_revision
            FROM turn_content_revisions
            WHERE host_id = ? AND assistant_final_text = ? AND is_current = 1
            """,
            (host_id, "fallback answer"),
        ).fetchone()

    assert merge_turn_content(
        db_path,
        host_id,
        "worker-1",
        {
            "assistant_final_text": "authoritative answer",
            "complete": True,
            "source_turn_id": "source-authoritative",
        },
        observed_at="2026-01-01T00:01:00+00:00",
    ) == 1
    with sqlite3.connect(str(db_path)) as conn:
        retained = conn.execute(
            """
            SELECT is_current
            FROM turn_content_revisions
            WHERE host_id = ? AND turn_id = ? AND content_revision = ?
            """,
            (host_id, fallback_turn, fallback_revision),
        ).fetchone()
        integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]

    assert retained == (1,)
    assert integrity == "ok"


@pytest.mark.parametrize("failure_phase", ["construction", "pragma"])
def test_connect_closes_parent_fd_on_construction_and_pragma_failures(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure_phase: str,
) -> None:
    db_path = tmp_path / "fd-failure.db"
    init_store(db_path)
    before = set(os.listdir("/proc/self/fd"))
    original_connect = sqlite3.connect
    created: list[sqlite3.Connection] = []

    def controlled_connect(*args: Any, **kwargs: Any) -> sqlite3.Connection:
        if failure_phase == "construction":
            raise RuntimeError("controlled construction failure")
        conn = original_connect(*args, **kwargs)
        created.append(conn)
        return conn

    def fail_pragmas(conn: sqlite3.Connection, db_path: Path | str) -> None:
        raise RuntimeError("controlled pragma failure")

    monkeypatch.setattr(store_sqlite.sqlite3, "connect", controlled_connect)
    if failure_phase == "pragma":
        monkeypatch.setattr(store_sqlite, "_apply_connection_pragmas", fail_pragmas)

    with pytest.raises(RuntimeError):
        store_sqlite._connect(db_path)

    assert set(os.listdir("/proc/self/fd")) == before
    for conn in created:
        with pytest.raises(sqlite3.ProgrammingError):
            conn.execute("SELECT 1")


def test_connect_factory_mismatch_closes_connection_and_parent_fd(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "factory-mismatch.db"
    init_store(db_path)
    before = set(os.listdir("/proc/self/fd"))

    class UnexpectedConnection:
        closed = False

        def close(self) -> None:
            self.closed = True

    unexpected = UnexpectedConnection()
    monkeypatch.setattr(store_sqlite.sqlite3, "connect", lambda *args, **kwargs: unexpected)

    with pytest.raises(LocalStateError) as caught:
        store_sqlite._connect(db_path)

    assert unexpected.closed is True
    assert caught.value.code is LocalStateErrorCode.OPERATION_FAILED
    assert str(db_path) not in str(caught.value)
    assert str(tmp_path) not in str(caught.value)
    assert set(os.listdir("/proc/self/fd")) == before


def test_connect_context_manager_closes_connection(tmp_path: Path) -> None:
    """The connection and its retained parent descriptor close on context exit."""
    db_path = tmp_path / "fd-leak.db"
    init_store(db_path)
    before = set(os.listdir("/proc/self/fd"))

    with store_sqlite._connect(db_path) as conn:
        assert conn.execute("SELECT 1").fetchone() == (1,)

    with pytest.raises(sqlite3.ProgrammingError):
        conn.execute("SELECT 1")
    assert set(os.listdir("/proc/self/fd")) == before


def test_store_v8_maintenance_schema_singleton_and_ordered_indexes(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "v8-schema.db"
    init_store(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        columns = {
            str(row[1])
            for row in conn.execute(
                "PRAGMA table_info(store_maintenance_state)"
            ).fetchall()
        }
        state = conn.execute(
            """
            SELECT scope, last_started_at, last_completed_at, last_status,
                   last_examined, last_deleted, last_examined_id
            FROM store_maintenance_state
            """
        ).fetchall()
        newest = tuple(
            str(row[2])
            for row in conn.execute(
                "PRAGMA index_xinfo(idx_snapshots_host_newest)"
            ).fetchall()
            if int(row[5]) == 1
        )
        created = tuple(
            str(row[2])
            for row in conn.execute(
                "PRAGMA index_xinfo(idx_snapshots_created_host_id)"
            ).fetchall()
            if int(row[5]) == 1
        )

    assert columns == {
        "scope",
        "last_started_at",
        "last_completed_at",
        "last_status",
        "last_examined",
        "last_deleted",
        "last_examined_id",
    }
    assert state == [("automatic", None, None, "never", 0, 0, None)]
    assert newest == ("host_id", "id")
    assert created == ("created_at", "host_id", "id")


def test_current_v9_schema_gate_and_second_init_have_no_mutation_or_wal_setting(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "current-fast-path.db"
    init_store(db_path)

    gate_trace: list[str] = []
    with sqlite3.connect(str(db_path)) as conn:
        conn.set_trace_callback(gate_trace.append)
        store_sqlite.ensure_schema(conn)
    assert [statement.strip().upper() for statement in gate_trace] == [
        "PRAGMA USER_VERSION"
    ]

    traces: list[str] = []
    original_pragmas = store_sqlite._apply_connection_pragmas

    def traced_pragmas(
        conn: sqlite3.Connection,
        path: Path | str,
    ) -> None:
        conn.set_trace_callback(traces.append)
        original_pragmas(conn, path)

    monkeypatch.setattr(store_sqlite, "_apply_connection_pragmas", traced_pragmas)
    monkeypatch.setattr(
        store_sqlite,
        "MIGRATIONS",
        tuple(
            store_sqlite.Migration(
                migration.from_version,
                migration.to_version,
                lambda _conn: (_ for _ in ()).throw(
                    AssertionError("current schema dispatched a migration")
                ),
            )
            for migration in store_sqlite.MIGRATIONS
        ),
    )
    original_flock = fcntl.flock

    def reject_parent_ex(fd: int, operation: int) -> None:
        if operation & fcntl.LOCK_EX:
            raise AssertionError("current schema attempted parent exclusivity")
        original_flock(fd, operation)

    monkeypatch.setattr(store_sqlite.fcntl, "flock", reject_parent_ex)
    init_store(db_path)

    normalized = [" ".join(statement.upper().split()) for statement in traces]
    assert not any(
        statement.startswith(("CREATE ", "ALTER ", "DROP ", "INSERT ", "UPDATE ", "DELETE "))
        for statement in normalized
    )
    assert "PRAGMA JOURNAL_MODE=WAL" not in normalized
    assert not any(
        statement.startswith("PRAGMA USER_VERSION =")
        for statement in normalized
    )

@pytest.mark.parametrize("version", (0, 1))
@pytest.mark.parametrize(
    "factory",
    (sqlite3.Connection, store_sqlite._ClosingConnection),
    ids=("raw", "unpinned-closing"),
)
def test_noncurrent_unowned_filesystem_connection_fails_before_persistent_mutation(
    tmp_path: Path,
    version: int,
    factory: type[sqlite3.Connection],
) -> None:
    db_path = tmp_path / f"unowned-v{version}.db"
    if version == 1:
        with sqlite3.connect(str(db_path)) as legacy:
            legacy.execute("CREATE TABLE legacy_sentinel (value TEXT NOT NULL)")
            legacy.execute("INSERT INTO legacy_sentinel VALUES ('preserved')")
            legacy.execute("PRAGMA user_version = 1")
    else:
        sqlite3.connect(str(db_path)).close()

    traces: list[str] = []
    with sqlite3.connect(str(db_path), factory=factory) as conn:
        conn.set_trace_callback(traces.append)
        with pytest.raises(LocalStateError) as caught:
            store_sqlite.ensure_schema(conn)

    assert caught.value.code is LocalStateErrorCode.OPERATION_FAILED
    assert [statement.strip().upper() for statement in traces] == [
        "PRAGMA USER_VERSION"
    ]
    assert not Path(f"{db_path}-wal").exists()
    assert not Path(f"{db_path}-shm").exists()
    assert not Path(f"{db_path}-journal").exists()
    with sqlite3.connect(str(db_path)) as inspection:
        assert _user_version(inspection) == version
        if version == 0:
            assert _table_names(inspection) == set()
        else:
            assert inspection.execute(
                "SELECT value FROM legacy_sentinel"
            ).fetchone() == ("preserved",)


@pytest.mark.parametrize(
    "db_path",
    (":memory:", "file:tendwire-schema-memory?mode=memory&cache=shared"),
)
def test_noncurrent_store_owned_memory_connection_initializes_schema(
    db_path: str,
) -> None:
    with store_sqlite._connect(db_path) as conn:
        store_sqlite.ensure_schema(conn)
        assert _user_version(conn) == store_sqlite.STORE_SCHEMA_VERSION
        assert "snapshots" in _table_names(conn)


@pytest.mark.parametrize(
    "db_uri",
    (
        "file:tendwire-ambiguous?mode=memory&mode=rw",
        "file:tendwire-ambiguous?mode=rw&mode=memory",
    ),
)
def test_duplicate_sqlite_uri_modes_fail_before_connection_resolution(
    db_uri: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def reject_resolution(*args: Any, **kwargs: Any) -> None:
        raise AssertionError("ambiguous URI must fail before path resolution")

    monkeypatch.setattr(store_sqlite, "open_resolved_parent", reject_resolution)

    with pytest.raises(ValueError, match="at most one mode parameter"):
        store_sqlite._connect(db_uri)


def test_cross_thread_close_keeps_schema_authority_until_connection_closes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "cross-thread-close.db"
    with sqlite3.connect(str(db_path)) as legacy:
        legacy.execute("CREATE TABLE legacy_sentinel (value TEXT NOT NULL)")
        legacy.execute("INSERT INTO legacy_sentinel VALUES ('preserved')")
        legacy.execute("PRAGMA user_version = 1")
    os.chmod(tmp_path, 0o700)
    os.chmod(db_path, 0o600)

    conn = store_sqlite._connect(db_path, prepare=True)
    close_errors: list[BaseException] = []
    original_configure = store_sqlite._configure_persistent_database_conn

    def close_from_foreign_thread(connection: sqlite3.Connection) -> None:
        def close() -> None:
            try:
                connection.close()
            except BaseException as exc:
                close_errors.append(exc)

        worker = threading.Thread(target=close)
        worker.start()
        worker.join()
        original_configure(connection)

    monkeypatch.setattr(
        store_sqlite,
        "_configure_persistent_database_conn",
        close_from_foreign_thread,
    )
    try:
        store_sqlite.ensure_schema(conn)
        assert _user_version(conn) == store_sqlite.STORE_SCHEMA_VERSION
        assert len(close_errors) == 1
        assert isinstance(close_errors[0], sqlite3.ProgrammingError)
    finally:
        conn.close()


def test_abandoned_store_connection_releases_parent_authority(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "abandoned-connection.db"
    init_store(db_path)

    connection = store_sqlite._connect(db_path)
    del connection
    gc.collect()

    parent_fd = os.open(
        db_path.parent,
        os.O_RDONLY | os.O_DIRECTORY | getattr(os, "O_CLOEXEC", 0),
    )
    try:
        fcntl.flock(parent_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        fcntl.flock(parent_fd, fcntl.LOCK_UN)
    finally:
        os.close(parent_fd)


def test_noncurrent_schema_promotion_preflight_preserves_callers_shared_authority(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "preflight-sh-legacy.db"
    with sqlite3.connect(str(db_path)) as legacy:
        legacy.execute("CREATE TABLE legacy_sentinel (value TEXT NOT NULL)")
        legacy.execute("INSERT INTO legacy_sentinel VALUES ('preserved')")
        legacy.execute("PRAGMA user_version = 1")
    os.chmod(tmp_path, 0o700)
    os.chmod(db_path, 0o600)

    before_fds = set(os.listdir("/proc/self/fd"))
    before_threads = {id(thread) for thread in threading.enumerate()}
    before_children = {process.pid for process in multiprocessing.active_children()}
    connection: sqlite3.Connection | None = None
    peer: sqlite3.Connection | None = None
    lock_fd = -1
    try:
        connection = store_sqlite._connect(db_path, prepare=True)
        peer = store_sqlite._connect(db_path)

        with pytest.raises(LocalStateError) as caught:
            store_sqlite.ensure_schema(connection)
        assert caught.value.code is LocalStateErrorCode.OPERATION_FAILED
        assert _user_version(connection) == 1
        assert connection.execute(
            "SELECT value FROM legacy_sentinel"
        ).fetchone() == ("preserved",)

        peer.close()
        peer = None
        lock_fd = os.open(
            db_path.parent,
            os.O_RDONLY | os.O_DIRECTORY | getattr(os, "O_CLOEXEC", 0),
        )
        with pytest.raises(BlockingIOError):
            fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)

        connection.close()
        connection = None
        fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        fcntl.flock(lock_fd, fcntl.LOCK_UN)
    finally:
        if peer is not None:
            peer.close()
        if connection is not None:
            connection.close()
        if lock_fd >= 0:
            os.close(lock_fd)

    assert set(os.listdir("/proc/self/fd")) - before_fds == set()
    assert {id(thread) for thread in threading.enumerate()} == before_threads
    assert {process.pid for process in multiprocessing.active_children()} == before_children


def test_noncurrent_schema_external_upgrade_contention_restores_shared_authority(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "external-upgrade-sh-legacy.db"
    with sqlite3.connect(str(db_path)) as legacy:
        legacy.execute("CREATE TABLE legacy_sentinel (value TEXT NOT NULL)")
        legacy.execute("INSERT INTO legacy_sentinel VALUES ('preserved')")
        legacy.execute("PRAGMA user_version = 1")
    os.chmod(tmp_path, 0o700)
    os.chmod(db_path, 0o600)
    before_threads = {id(thread) for thread in threading.enumerate()}
    before_children = {process.pid for process in multiprocessing.active_children()}
    context = multiprocessing.get_context("spawn")
    acquired = context.Queue()
    release = context.Event()
    process = context.Process(
        target=_cross_process_hold_parent_lock,
        args=(str(db_path.parent), fcntl.LOCK_SH, acquired, release),
    )
    connection: sqlite3.Connection | None = None
    lock_fd = -1
    parent_fd = -1
    connection_id = -1
    started = False
    try:
        process.start()
        started = True
        assert acquired.get(timeout=5) is None
        connection = store_sqlite._connect(db_path, prepare=True)
        authority = store_sqlite._schema_connection_authority(connection)
        assert authority.parent_fd is not None
        parent_fd = authority.parent_fd
        connection_id = id(connection)

        with pytest.raises(LocalStateError) as caught:
            store_sqlite.ensure_schema(connection)
        assert caught.value.code is LocalStateErrorCode.OPERATION_FAILED
        assert _user_version(connection) == 1

        release.set()
        process.join(timeout=15)
        if process.is_alive():
            pytest.fail("external parent SH holder did not terminate")
        assert process.exitcode == 0
        process.close()
        started = False

        lock_fd = os.open(
            db_path.parent,
            os.O_RDONLY | os.O_DIRECTORY | getattr(os, "O_CLOEXEC", 0),
        )
        with pytest.raises(BlockingIOError):
            fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)

        connection.close()
        with pytest.raises(OSError):
            os.fstat(parent_fd)
        assert connection_id not in store_sqlite._SCHEMA_CONNECTION_AUTHORITIES
        connection = None
        fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        fcntl.flock(lock_fd, fcntl.LOCK_UN)
    finally:
        release.set()
        if started:
            process.join(timeout=15)
            if process.is_alive():
                process.terminate()
                process.join(timeout=5)
            process.close()
        if connection is not None:
            connection.close()
        if lock_fd >= 0:
            os.close(lock_fd)
        acquired.close()
        acquired.join_thread()

    assert {id(thread) for thread in threading.enumerate()} == before_threads
    assert {process.pid for process in multiprocessing.active_children()} == before_children


def test_noncurrent_schema_upgrade_recovery_retry_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "upgrade-recovery-legacy.db"
    with sqlite3.connect(str(db_path)) as legacy:
        legacy.execute("CREATE TABLE legacy_sentinel (value TEXT NOT NULL)")
        legacy.execute("INSERT INTO legacy_sentinel VALUES ('preserved')")
        legacy.execute("PRAGMA user_version = 1")
    os.chmod(tmp_path, 0o700)
    os.chmod(db_path, 0o600)

    before_threads = {id(thread) for thread in threading.enumerate()}
    before_children = {process.pid for process in multiprocessing.active_children()}
    context = multiprocessing.get_context("spawn")
    acquired = context.Queue()
    release = context.Event()
    process = context.Process(
        target=_cross_process_hold_parent_lock,
        args=(str(db_path.parent), fcntl.LOCK_EX, acquired, release),
    )
    connection: sqlite3.Connection | None = None
    lock_fd = -1
    started = False
    connection_id = -1
    try:
        connection = store_sqlite._connect(db_path, prepare=True)
        authority = store_sqlite._schema_connection_authority(connection)
        assert authority.parent_fd is not None
        parent_fd = authority.parent_fd
        connection_id = id(connection)
        original_flock = fcntl.flock
        restore_attempts = 0

        def lose_upgrade_to_external_ex(fd: int, operation: int) -> None:
            nonlocal restore_attempts, started
            if fd == parent_fd and operation == (fcntl.LOCK_EX | fcntl.LOCK_NB):
                original_flock(fd, fcntl.LOCK_UN)
                assert acquired.get(timeout=5) is None
                raise BlockingIOError()
            if fd == parent_fd and operation == (fcntl.LOCK_SH | fcntl.LOCK_NB):
                restore_attempts += 1
                try:
                    original_flock(fd, operation)
                except BlockingIOError:
                    if restore_attempts == 1:
                        release.set()
                        process.join(timeout=15)
                        if process.is_alive():
                            pytest.fail("external parent EX holder did not terminate")
                        assert process.exitcode == 0
                        process.close()
                        started = False
                    raise
                return
            original_flock(fd, operation)

        process.start()
        started = True
        monkeypatch.setattr(store_sqlite.fcntl, "flock", lose_upgrade_to_external_ex)
        with pytest.raises(LocalStateError) as caught:
            store_sqlite.ensure_schema(connection)
        assert caught.value.code is LocalStateErrorCode.OPERATION_FAILED
        assert restore_attempts == 2
        with pytest.raises(sqlite3.ProgrammingError):
            connection.execute("SELECT 1")
        with pytest.raises(OSError):
            os.fstat(parent_fd)
        assert connection_id not in store_sqlite._SCHEMA_CONNECTION_AUTHORITIES

        lock_fd = os.open(
            db_path.parent,
            os.O_RDONLY | os.O_DIRECTORY | getattr(os, "O_CLOEXEC", 0),
        )
        original_flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        original_flock(lock_fd, fcntl.LOCK_UN)
    finally:
        release.set()
        if started:
            process.join(timeout=15)
            if process.is_alive():
                process.terminate()
                process.join(timeout=5)
            process.close()
        if connection is not None:
            connection.close()
        if lock_fd >= 0:
            os.close(lock_fd)
        acquired.close()
        acquired.join_thread()

    assert {id(thread) for thread in threading.enumerate()} == before_threads
    assert {process.pid for process in multiprocessing.active_children()} == before_children


def test_noncurrent_schema_downgrade_recovery_retry_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "downgrade-recovery-legacy.db"
    with sqlite3.connect(str(db_path)) as legacy:
        legacy.execute("CREATE TABLE legacy_sentinel (value TEXT NOT NULL)")
        legacy.execute("INSERT INTO legacy_sentinel VALUES ('preserved')")
        legacy.execute("PRAGMA user_version = 1")
    os.chmod(tmp_path, 0o700)
    os.chmod(db_path, 0o600)

    before_fds = set(os.listdir("/proc/self/fd"))
    connection: sqlite3.Connection | None = None
    lock_fd = -1
    try:
        connection = store_sqlite._connect(db_path, prepare=True)
        authority = store_sqlite._schema_connection_authority(connection)
        assert authority.parent_fd is not None
        parent_fd = authority.parent_fd
        connection_id = id(connection)
        original_flock = fcntl.flock
        restore_attempts = 0

        def fail_first_shared_restore(fd: int, operation: int) -> None:
            nonlocal restore_attempts
            if fd == parent_fd and operation == (fcntl.LOCK_SH | fcntl.LOCK_NB):
                restore_attempts += 1
                if restore_attempts == 1:
                    original_flock(fd, fcntl.LOCK_UN)
                    raise BlockingIOError()
            original_flock(fd, operation)

        monkeypatch.setattr(store_sqlite.fcntl, "flock", fail_first_shared_restore)
        with pytest.raises(LocalStateError) as caught:
            store_sqlite.ensure_schema(connection)
        assert caught.value.code is LocalStateErrorCode.OPERATION_FAILED
        assert restore_attempts == 2
        with pytest.raises(sqlite3.ProgrammingError):
            connection.execute("SELECT 1")
        with pytest.raises(OSError):
            os.fstat(parent_fd)
        assert connection_id not in store_sqlite._SCHEMA_CONNECTION_AUTHORITIES

        lock_fd = os.open(
            db_path.parent,
            os.O_RDONLY | os.O_DIRECTORY | getattr(os, "O_CLOEXEC", 0),
        )
        original_flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        original_flock(lock_fd, fcntl.LOCK_UN)
    finally:
        if connection is not None:
            connection.close()
        if lock_fd >= 0:
            os.close(lock_fd)

    assert set(os.listdir("/proc/self/fd")) - before_fds == set()


def test_noncurrent_schema_downgrade_external_ex_contention_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "downgrade-ex-legacy.db"
    with sqlite3.connect(str(db_path)) as legacy:
        legacy.execute("CREATE TABLE legacy_sentinel (value TEXT NOT NULL)")
        legacy.execute("INSERT INTO legacy_sentinel VALUES ('preserved')")
        legacy.execute("PRAGMA user_version = 1")
    os.chmod(tmp_path, 0o700)
    os.chmod(db_path, 0o600)
    before_threads = {id(thread) for thread in threading.enumerate()}
    before_children = {process.pid for process in multiprocessing.active_children()}
    context = multiprocessing.get_context("spawn")
    acquired = context.Queue()
    release = context.Event()
    process = context.Process(
        target=_cross_process_hold_parent_lock,
        args=(str(db_path.parent), fcntl.LOCK_EX, acquired, release),
    )
    connection: sqlite3.Connection | None = None
    lock_fd = -1
    started = False
    connection_id = -1
    try:
        connection = store_sqlite._connect(db_path, prepare=True)
        authority = store_sqlite._schema_connection_authority(connection)
        assert authority.parent_fd is not None
        parent_fd = authority.parent_fd
        connection_id = id(connection)
        original_run_migrations = store_sqlite._run_migrations
        original_flock = fcntl.flock
        failed_restores = 0

        def start_external_ex_after_migration(*args: Any, **kwargs: Any) -> None:
            nonlocal started
            original_run_migrations(*args, **kwargs)
            process.start()
            started = True

        def lose_downgrade_to_external_ex(fd: int, operation: int) -> None:
            nonlocal failed_restores
            if fd == parent_fd and operation == (fcntl.LOCK_SH | fcntl.LOCK_NB):
                failed_restores += 1
                original_flock(fd, fcntl.LOCK_UN)
                if failed_restores == 1:
                    assert acquired.get(timeout=5) is None
                raise BlockingIOError()
            original_flock(fd, operation)

        monkeypatch.setattr(
            store_sqlite,
            "_run_migrations",
            start_external_ex_after_migration,
        )
        monkeypatch.setattr(
            store_sqlite.fcntl,
            "flock",
            lose_downgrade_to_external_ex,
        )
        with pytest.raises(LocalStateError) as caught:
            store_sqlite.ensure_schema(connection)
        assert caught.value.code is LocalStateErrorCode.OPERATION_FAILED
        assert failed_restores == store_sqlite._SCHEMA_PARENT_SHARED_LOCK_RECOVERY_ATTEMPTS
        with pytest.raises(sqlite3.ProgrammingError):
            connection.execute("SELECT 1")
        with pytest.raises(OSError):
            os.fstat(parent_fd)
        assert connection_id not in store_sqlite._SCHEMA_CONNECTION_AUTHORITIES

        release.set()
        process.join(timeout=15)
        if process.is_alive():
            pytest.fail("external parent EX holder did not terminate")
        assert process.exitcode == 0
        process.close()
        started = False

        lock_fd = os.open(
            db_path.parent,
            os.O_RDONLY | os.O_DIRECTORY | getattr(os, "O_CLOEXEC", 0),
        )
        fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        fcntl.flock(lock_fd, fcntl.LOCK_UN)
    finally:
        release.set()
        if started:
            process.join(timeout=15)
            if process.is_alive():
                process.terminate()
                process.join(timeout=5)
            process.close()
        if connection is not None:
            connection.close()
        if lock_fd >= 0:
            os.close(lock_fd)
        acquired.close()
        acquired.join_thread()

    assert {id(thread) for thread in threading.enumerate()} == before_threads
    assert {process.pid for process in multiprocessing.active_children()} == before_children


def test_noncurrent_schema_live_shared_parent_fails_before_persistent_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "shared-legacy.db"
    with sqlite3.connect(str(db_path)) as legacy:
        legacy.execute("CREATE TABLE legacy_sentinel (value TEXT NOT NULL)")
        legacy.execute("INSERT INTO legacy_sentinel VALUES ('preserved')")
        legacy.execute("PRAGMA user_version = 1")
    os.chmod(tmp_path, 0o700)
    os.chmod(db_path, 0o600)

    before_threads = {id(thread) for thread in threading.enumerate()}
    before_children = {process.pid for process in multiprocessing.active_children()}
    holder = store_sqlite._connect(db_path)
    retained_fds = set(os.listdir("/proc/self/fd"))
    traces: list[str] = []
    created: list[sqlite3.Connection] = []
    original_connect = sqlite3.connect

    def traced_connect(*args: Any, **kwargs: Any) -> sqlite3.Connection:
        connection = original_connect(*args, **kwargs)
        connection.set_trace_callback(traces.append)
        created.append(connection)
        return connection

    monkeypatch.setattr(store_sqlite.sqlite3, "connect", traced_connect)
    try:
        with pytest.raises(LocalStateError) as caught:
            init_store(db_path)
        assert caught.value.code is LocalStateErrorCode.OPERATION_FAILED
        assert holder.execute("SELECT value FROM legacy_sentinel").fetchone() == (
            "preserved",
        )
        assert not Path(f"{db_path}-wal").exists()
        assert not Path(f"{db_path}-shm").exists()
        normalized = [" ".join(statement.upper().split()) for statement in traces]
        assert "PRAGMA JOURNAL_MODE=WAL" not in normalized
        assert "PRAGMA DATABASE_LIST" not in normalized
        assert not any(
            statement.startswith(("BEGIN", "CREATE ", "ALTER ", "DROP "))
            for statement in normalized
        )
        assert len(created) == 1
        with pytest.raises(sqlite3.ProgrammingError):
            created[0].execute("SELECT 1")
        assert set(os.listdir("/proc/self/fd")) - retained_fds == set()
    finally:
        holder.close()

    with original_connect(str(db_path)) as inspection:
        assert _user_version(inspection) == 1
        assert inspection.execute(
            "SELECT value FROM legacy_sentinel"
        ).fetchone() == ("preserved",)
    assert {id(thread) for thread in threading.enumerate()} == before_threads
    assert {process.pid for process in multiprocessing.active_children()} == before_children


@pytest.mark.parametrize("change", ("unlink", "replace"))
def test_noncurrent_schema_finalization_rejects_changed_pinned_main_without_creation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    change: str,
) -> None:
    db_path = tmp_path / "unlinked-legacy.db"
    replacement_bytes = b"replacement main must remain untouched"
    with sqlite3.connect(str(db_path)) as legacy:
        legacy.execute("CREATE TABLE legacy_sentinel (value TEXT NOT NULL)")
        legacy.execute("INSERT INTO legacy_sentinel VALUES ('preserved')")
        legacy.execute("PRAGMA user_version = 1")
    os.chmod(tmp_path, 0o700)
    os.chmod(db_path, 0o600)

    selected_identity = entry_identity(os.lstat(db_path))
    before_fds = set(os.listdir("/proc/self/fd"))
    before_threads = {id(thread) for thread in threading.enumerate()}
    before_children = {process.pid for process in multiprocessing.active_children()}
    original_prepare = store_sqlite.prepare_sqlite_family_at
    original_migrations = store_sqlite._run_migrations
    prepare_calls = 0
    migrated = False
    fired = False
    connection: sqlite3.Connection | None = None
    lock_fd = -1

    def record_migration(*args: Any, **kwargs: Any) -> None:
        nonlocal migrated
        original_migrations(*args, **kwargs)
        migrated = True

    def change_before_final_family_prepare(
        parent_fd: int,
        leaf: str,
        **kwargs: Any,
    ) -> tuple[PermissionResult, ...]:
        nonlocal prepare_calls, fired
        prepare_calls += 1
        if prepare_calls == 2:
            assert migrated
            assert kwargs["_parent_exclusive_lock_held"] is True
            assert kwargs["_expected_main_identity"] == selected_identity
            os.unlink(leaf, dir_fd=parent_fd)
            if change == "replace":
                replacement_fd = os.open(
                    leaf,
                    os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                    0o600,
                    dir_fd=parent_fd,
                )
                try:
                    os.write(replacement_fd, replacement_bytes)
                    os.fchmod(replacement_fd, 0o600)
                finally:
                    os.close(replacement_fd)
            fired = True
        return original_prepare(parent_fd, leaf, **kwargs)

    monkeypatch.setattr(store_sqlite, "_run_migrations", record_migration)
    monkeypatch.setattr(
        store_sqlite,
        "prepare_sqlite_family_at",
        change_before_final_family_prepare,
    )
    try:
        connection = store_sqlite._connect(db_path, prepare=True)
        with pytest.raises(LocalStateError) as caught:
            store_sqlite.ensure_schema(connection)

        assert fired
        assert prepare_calls == 2
        assert caught.value.code is LocalStateErrorCode.ENTRY_CHANGED
        if change == "replace":
            assert db_path.read_bytes() == replacement_bytes
            assert _mode(db_path) == 0o600
        else:
            assert not db_path.exists()
        lock_fd = os.open(
            db_path.parent,
            os.O_RDONLY | os.O_DIRECTORY | getattr(os, "O_CLOEXEC", 0),
        )
        with pytest.raises(BlockingIOError):
            fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    finally:
        if connection is not None:
            connection.close()
        if lock_fd >= 0:
            fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            fcntl.flock(lock_fd, fcntl.LOCK_UN)
            os.close(lock_fd)

    if change == "replace":
        assert db_path.read_bytes() == replacement_bytes
    assert set(os.listdir("/proc/self/fd")) - before_fds == set()
    assert {id(thread) for thread in threading.enumerate()} == before_threads
    assert {process.pid for process in multiprocessing.active_children()} == before_children


def test_noncurrent_schema_keeps_sidecars_private_and_restores_shared_parent_lock(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "private-legacy.db"
    with sqlite3.connect(str(db_path)) as legacy:
        legacy.execute("CREATE TABLE legacy_sentinel (value TEXT NOT NULL)")
        legacy.execute("INSERT INTO legacy_sentinel VALUES ('preserved')")
        legacy.execute("PRAGMA user_version = 1")
    os.chmod(tmp_path, 0o700)
    os.chmod(db_path, 0o600)

    before_threads = {id(thread) for thread in threading.enumerate()}
    before_children = {process.pid for process in multiprocessing.active_children()}
    connection: sqlite3.Connection | None = None
    lock_fd = -1
    previous_umask = os.umask(0)
    try:
        connection = store_sqlite._connect(db_path, prepare=True)
        store_sqlite.ensure_schema(connection)

        assert _user_version(connection) == store_sqlite.STORE_SCHEMA_VERSION
        assert connection.execute(
            "SELECT value FROM legacy_sentinel"
        ).fetchone() == ("preserved",)
        for suffix in ("", "-wal", "-shm"):
            assert _mode(Path(f"{db_path}{suffix}")) == 0o600

        lock_fd = os.open(
            db_path.parent,
            os.O_RDONLY | os.O_DIRECTORY | getattr(os, "O_CLOEXEC", 0),
        )
        with pytest.raises(BlockingIOError):
            fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)

        connection.close()
        connection = None
        fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        fcntl.flock(lock_fd, fcntl.LOCK_UN)
    finally:
        if connection is not None:
            connection.close()
        if lock_fd >= 0:
            os.close(lock_fd)
        os.umask(previous_umask)

    assert {id(thread) for thread in threading.enumerate()} == before_threads
    assert {process.pid for process in multiprocessing.active_children()} == before_children


def test_direct_empty_creation_does_not_replay_migration_registry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "direct-current.db"
    monkeypatch.setattr(
        store_sqlite,
        "MIGRATIONS",
        tuple(
            store_sqlite.Migration(
                migration.from_version,
                migration.to_version,
                lambda _conn: (_ for _ in ()).throw(
                    AssertionError("direct creation replayed history")
                ),
            )
            for migration in store_sqlite.MIGRATIONS
        ),
    )

    init_store(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        assert _user_version(conn) == store_sqlite.STORE_SCHEMA_VERSION
        assert "turn_list_hosts" in _table_names(conn)
        assert {"store_maintenance_state", "turn_list_state"} <= _table_names(conn)
        assert conn.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        assert conn.execute("PRAGMA foreign_key_check").fetchall() == []


def test_v13_migration_repairs_nonpositive_turn_sequences_and_blocks_recurrence(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "legacy-turn-sequence.db"
    observed_at = "2026-07-15T00:00:00+00:00"
    with sqlite3.connect(str(db_path)) as conn:
        store_sqlite._run_migrations(conn, target_version=13)
        conn.execute("DROP TABLE turns")
        conn.execute(
            store_sqlite.CREATE_TURNS_TABLE.replace(
                " CHECK (list_sequence > 0)",
                "",
            )
        )
        for statement in store_sqlite.CREATE_TURN_LIST_INDEXES:
            conn.execute(statement)
        for turn_id, sequence in (("turn-valid", 5), ("turn-invalid", 0)):
            payload = {
                "id": turn_id,
                "worker_id": "worker-a",
                "status": "complete",
                "kind": "prompt",
                "source": "snapshot",
                "updated_at": observed_at,
            }
            conn.execute(
                """
                INSERT INTO turns (
                    host_id, turn_id, worker_id, worker_fingerprint, space_id,
                    status, kind, updated_at, fingerprint,
                    snapshot_content_fingerprint, observed_at, payload_json,
                    list_sequence
                ) VALUES (?, ?, 'worker-a', NULL, NULL, 'complete', 'prompt',
                          ?, '', '', ?, ?, ?)
                """,
                (
                    "legacy-host",
                    turn_id,
                    observed_at,
                    observed_at,
                    json.dumps(payload),
                    sequence,
                ),
            )
        conn.execute(
            """
            INSERT INTO turn_list_hosts (
                host_id, next_sequence, traversal_generation
            ) VALUES ('legacy-host', 6, 7)
            """
        )
        conn.commit()
    os.chmod(tmp_path, 0o700)
    os.chmod(db_path, 0o600)

    init_store(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        assert _user_version(conn) == store_sqlite.STORE_SCHEMA_VERSION == 18
        assert conn.execute(
            """
            SELECT turn_id, list_sequence
            FROM turns
            WHERE host_id = 'legacy-host'
            ORDER BY list_sequence
            """
        ).fetchall() == [("turn-valid", 5), ("turn-invalid", 6)]
        assert conn.execute(
            """
            SELECT next_sequence, traversal_generation
            FROM turn_list_hosts
            WHERE host_id = 'legacy-host'
            """
        ).fetchone() == (7, 8)
        assert conn.execute(
            """
            SELECT COUNT(*)
            FROM sqlite_master
            WHERE type = 'trigger'
              AND name LIKE 'trg_turns_positive_list_sequence_%'
            """
        ).fetchone() == (2,)
        with pytest.raises(sqlite3.IntegrityError, match="invalid turn list sequence"):
            conn.execute(
                """
                UPDATE turns SET list_sequence = 0
                WHERE host_id = 'legacy-host' AND turn_id = 'turn-valid'
                """
            )
        conn.rollback()
        with pytest.raises(sqlite3.IntegrityError, match="invalid turn list sequence"):
            conn.execute(
                """
                INSERT INTO turns (
                    host_id, turn_id, worker_id, status, kind, fingerprint,
                    snapshot_content_fingerprint, observed_at, payload_json,
                    list_sequence
                ) VALUES (
                    'legacy-host', 'turn-recurrence', 'worker-a', 'complete',
                    'prompt', '', '', ?, '{}', 0
                )
                """,
                (observed_at,),
            )
        conn.rollback()

    first = turns_payload_from_store(
        db_path,
        "legacy-host",
        schema_version=2,
        limit=1,
        now=1_800_000_000,
    )
    assert first["has_more"] is True
    assert isinstance(first["next_cursor"], str)
    second = turns_payload_from_store(
        db_path,
        "legacy-host",
        schema_version=2,
        limit=1,
        cursor=first["next_cursor"],
        now=1_800_000_001,
    )
    assert second["has_more"] is False
    assert [item["id"] for item in first["turns"] + second["turns"]] == [
        "turn-invalid",
        "turn-valid",
    ]


@pytest.mark.parametrize("source_version", range(store_sqlite.STORE_SCHEMA_VERSION))
def test_migration_registry_transition_rolls_back_resumes_and_reruns(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    source_version: int,
) -> None:
    db_path = tmp_path / f"registry-{source_version}.db"
    original_registry = store_sqlite.MIGRATIONS
    assert tuple(
        (migration.from_version, migration.to_version)
        for migration in original_registry
    ) == tuple(
        (version, version + 1)
        for version in range(store_sqlite.STORE_SCHEMA_VERSION)
    )

    with sqlite3.connect(str(db_path)) as conn:
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("CREATE TABLE durable_sentinel (value TEXT NOT NULL)")
        conn.execute("INSERT INTO durable_sentinel VALUES ('preserved')")
        conn.commit()
        store_sqlite._run_migrations(conn, target_version=source_version)
        assert _user_version(conn) == source_version

        transition = original_registry[source_version]

        def apply_then_fail(current: sqlite3.Connection) -> None:
            transition.apply(current)
            raise RuntimeError("controlled migration interruption")

        interrupted = list(original_registry)
        interrupted[source_version] = store_sqlite.Migration(
            transition.from_version,
            transition.to_version,
            apply_then_fail,
        )
        monkeypatch.setattr(store_sqlite, "MIGRATIONS", tuple(interrupted))
        with pytest.raises(RuntimeError, match="controlled migration interruption"):
            store_sqlite._run_migrations(
                conn,
                target_version=source_version + 1,
            )
        assert _user_version(conn) == source_version
        assert conn.execute("SELECT value FROM durable_sentinel").fetchone() == (
            "preserved",
        )

        monkeypatch.setattr(store_sqlite, "MIGRATIONS", original_registry)
        store_sqlite._run_migrations(conn, target_version=source_version + 1)
        assert _user_version(conn) == source_version + 1
        conn.execute("BEGIN IMMEDIATE")
        transition.apply(conn)
        conn.commit()
        assert conn.execute("SELECT value FROM durable_sentinel").fetchone() == (
            "preserved",
        )
        assert conn.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        assert conn.execute("PRAGMA foreign_key_check").fetchall() == []


def test_ten_thousand_adjacent_identical_saves_keep_one_row_and_event(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "identical-10000.db"
    config = Config(host_id="identical-host", db_path=db_path)
    first = datetime.fromisoformat("2026-01-01T00:00:00+00:00")
    final_snapshot = None
    for index in range(10_000):
        final_snapshot = project_from_raw(
            config,
            timestamp=first + store_sqlite.timedelta(seconds=index),
        )
        save_snapshot(db_path, final_snapshot)

    assert final_snapshot is not None
    with sqlite3.connect(str(db_path)) as conn:
        snapshot_rows = conn.execute(
            "SELECT created_at, payload FROM snapshots"
        ).fetchall()
        saved_events = conn.execute(
            "SELECT COUNT(*) FROM events WHERE event_type = 'snapshot.saved'"
        ).fetchone()[0]
        integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]
        foreign_keys = conn.execute("PRAGMA foreign_key_check").fetchall()

    assert len(snapshot_rows) == 1
    assert saved_events == 1
    assert snapshot_rows[0][0] == final_snapshot.updated_at
    assert json.loads(snapshot_rows[0][1])["updated_at"] == final_snapshot.updated_at
    assert integrity == "ok"
    assert foreign_keys == []


def test_adjacent_dedupe_is_host_local_and_a_b_a_appends_changed_history(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "aba-host-local.db"
    config_a = Config(host_id="host-a", db_path=db_path)
    config_b = Config(host_id="host-b", db_path=db_path)
    at = datetime.fromisoformat("2026-01-01T00:00:00+00:00")
    snapshot_a = project_from_raw(
        config_a,
        workers=[{"id": "worker-a", "name": "A", "status": "active"}],
        timestamp=at,
    )
    snapshot_b = project_from_raw(
        config_a,
        workers=[{"id": "worker-b", "name": "B", "status": "active"}],
        timestamp=at + store_sqlite.timedelta(seconds=1),
    )
    snapshot_a_again = project_from_raw(
        config_a,
        workers=[{"id": "worker-a", "name": "A", "status": "active"}],
        timestamp=at + store_sqlite.timedelta(seconds=2),
    )
    other_host = project_from_raw(
        config_b,
        workers=[{"id": "worker-a", "name": "A", "status": "active"}],
        timestamp=at + store_sqlite.timedelta(seconds=3),
    )

    for snapshot in (snapshot_a, snapshot_b, snapshot_a_again, other_host, other_host):
        save_snapshot(db_path, snapshot)

    with sqlite3.connect(str(db_path)) as conn:
        history = conn.execute(
            """
            SELECT host_id, content_fingerprint
            FROM snapshots
            ORDER BY id
            """
        ).fetchall()
        event_count = conn.execute(
            "SELECT COUNT(*) FROM events WHERE event_type = 'snapshot.saved'"
        ).fetchone()[0]

    assert history == [
        ("host-a", snapshot_a.content_fingerprint),
        ("host-a", snapshot_b.content_fingerprint),
        ("host-a", snapshot_a_again.content_fingerprint),
        ("host-b", other_host.content_fingerprint),
    ]
    assert event_count == 4


def test_stale_same_fingerprint_save_does_not_regress_current_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "stale-identical.db"
    config = Config(host_id="stale-host", db_path=db_path)
    fresh_at = "2026-01-01T00:10:00+00:00"
    stale_at = "2026-01-01T00:05:00+00:00"
    fresh = _snapshot_with_worker_status(
        config,
        status="blocked",
        observed_at=fresh_at,
    )
    stale = _snapshot_with_worker_status(
        config,
        status="blocked",
        observed_at=stale_at,
    )
    assert stale.content_fingerprint == fresh.content_fingerprint

    _save_observation(db_path, fresh, "positive", fresh_at)

    def current_state() -> tuple[Any, ...]:
        with sqlite3.connect(str(db_path)) as conn:
            return (
                conn.execute(
                    """
                    SELECT created_at, payload
                    FROM snapshots
                    WHERE host_id = ?
                    ORDER BY id DESC
                    LIMIT 1
                    """,
                    ("stale-host",),
                ).fetchone(),
                conn.execute(
                    """
                    SELECT observed_at, payload_json
                    FROM workers
                    WHERE host_id = ? AND worker_id = 'worker-1'
                    """,
                    ("stale-host",),
                ).fetchone(),
                conn.execute(
                    """
                    SELECT observed_at, payload_json
                    FROM backend_health
                    WHERE host_id = ? AND backend_name = 'herdr'
                    """,
                    ("stale-host",),
                ).fetchone(),
                conn.execute(
                    """
                    SELECT last_seen_at, last_changed_at, signal_count, payload_json
                    FROM attention_items
                    WHERE host_id = ?
                    """,
                    ("stale-host",),
                ).fetchone(),
                conn.execute(
                    """
                    SELECT last_positive_at, last_accepted_at, last_observation_key
                    FROM attention_lifecycles
                    WHERE host_id = ?
                    """,
                    ("stale-host",),
                ).fetchone(),
                conn.execute(
                    "SELECT COUNT(*) FROM snapshots WHERE host_id = ?",
                    ("stale-host",),
                ).fetchone()[0],
                conn.execute(
                    """
                    SELECT COUNT(*)
                    FROM events
                    WHERE host_id = ? AND event_type = 'snapshot.saved'
                    """,
                    ("stale-host",),
                ).fetchone()[0],
            )

    before = current_state()
    attention_calls: list[str] = []
    original_attention = store_sqlite._apply_attention_observation_conn

    def observed_attention(
        conn: sqlite3.Connection,
        **kwargs: Any,
    ) -> None:
        attention_calls.append(str(kwargs["observation"].observed_at))
        original_attention(conn, **kwargs)

    monkeypatch.setattr(
        store_sqlite,
        "_apply_attention_observation_conn",
        observed_attention,
    )
    _save_observation(db_path, stale, "positive", stale_at)
    after = current_state()

    assert attention_calls == [stale_at]
    assert after == before
    assert after[0][0] == fresh_at
    assert after[1][0] == fresh.updated_at
    assert after[2][0] == fresh_at
    assert after[3][0] == fresh_at
    assert after[4][0:2] == (fresh_at, fresh_at)
    assert after[5:] == (1, 1)

def test_snapshot_created_at_is_canonical_utc_and_invalid_input_fails_before_open(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "canonical-created-at.db"
    config = Config(host_id="canonical-host", db_path=db_path)
    first = project_from_raw(
        config,
        timestamp=datetime.fromisoformat("2026-07-01T05:30:00+05:30"),
    )
    later = project_from_raw(
        config,
        timestamp=datetime.fromisoformat("2026-06-30T21:00:00-04:00"),
    )
    assert first.content_fingerprint == later.content_fingerprint

    save_snapshot(db_path, first)
    save_snapshot(db_path, later)
    with sqlite3.connect(str(db_path)) as conn:
        rows = conn.execute(
            "SELECT created_at FROM snapshots"
        ).fetchall()
    assert rows == [("2026-07-01T01:00:00+00:00",)]

    invalid_path = tmp_path / "invalid" / "store.db"

    def forbidden_connect(*_args: Any, **_kwargs: Any) -> None:
        raise AssertionError("invalid timestamp reached sqlite open")

    monkeypatch.setattr(store_sqlite, "_connect", forbidden_connect)
    for invalid_timestamp in (
        "malformed-timestamp",
        "0001-01-01T00:00:00+14:00",
        "9999-12-31T23:59:59-14:00",
    ):
        invalid = project_empty(
            Config(host_id="invalid-host", db_path=invalid_path)
        )
        object.__setattr__(invalid, "updated_at", invalid_timestamp)
        with pytest.raises(ValueError, match="invalid snapshot updated_at"):
            save_snapshot(invalid_path, invalid)
    assert not invalid_path.parent.exists()


def test_v7_timestamp_normalization_is_transactional_idempotent_and_age_safe(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "offset-migration.db"
    init_store(db_path)
    raw_rows = [
        (
            "2026-07-01T01:00:00+02:00",
            "valid-old-positive-offset",
        ),
        (
            "2026-06-30T13:00:00-12:00",
            "adversarial-newer-negative-offset",
        ),
        ("malformed-legacy-time", "malformed-quarantine"),
        (
            "0001-01-01T00:00:00+14:00",
            "underflow-quarantine",
        ),
        (
            "9999-12-31T23:59:59-14:00",
            "overflow-quarantine",
        ),
        (
            store_sqlite._LEGACY_SNAPSHOT_CREATED_AT_QUARANTINE,
            "legacy-year-9999-quarantine",
        ),
        (
            store_sqlite._LEGACY_SNAPSHOT_CREATED_AT_QUARANTINE,
            "legacy-sentinel-malformed-payload",
        ),
        (
            store_sqlite._LEGACY_SNAPSHOT_CREATED_AT_QUARANTINE,
            "legacy-sentinel-different-payload",
        ),
        (
            store_sqlite._LEGACY_SNAPSHOT_CREATED_AT_QUARANTINE,
            "legacy-sentinel-underflow-payload",
        ),
        (
            store_sqlite._LEGACY_SNAPSHOT_CREATED_AT_QUARANTINE,
            "legacy-sentinel-overflow-payload",
        ),
        (
            store_sqlite._LEGACY_SNAPSHOT_CREATED_AT_QUARANTINE,
            "legitimate-year-9999-observation",
        ),
        (
            "2026-07-01T00:30:00+02:00",
            "old-but-latest",
        ),
    ]
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute("DELETE FROM snapshots")
        conn.executemany(
            """
            INSERT INTO snapshots (
                host_id, created_at, content_fingerprint, payload
            ) VALUES ('offset-host', ?, ?, '{}')
            """,
            raw_rows,
        )
        conn.executemany(
            """
            UPDATE snapshots
            SET payload = ?
            WHERE content_fingerprint = ?
            """,
            (
                (
                    json.dumps({"updated_at": updated_at}, sort_keys=True),
                    fingerprint,
                )
                for fingerprint, updated_at in (
                    ("legacy-sentinel-malformed-payload", "not-a-time"),
                    (
                        "legacy-sentinel-different-payload",
                        "2026-01-01T00:00:00+00:00",
                    ),
                    (
                        "legacy-sentinel-underflow-payload",
                        "0001-01-01T00:00:00+14:00",
                    ),
                    (
                        "legacy-sentinel-overflow-payload",
                        "9999-12-31T23:59:59-14:00",
                    ),
                    (
                        "legitimate-year-9999-observation",
                        store_sqlite._LEGACY_SNAPSHOT_CREATED_AT_QUARANTINE,
                    ),
                )
            ),
        )
        conn.execute("DROP TABLE store_maintenance_state")
        conn.execute("DROP INDEX idx_snapshots_host_newest")
        conn.execute("DROP INDEX idx_snapshots_created_host_id")
        conn.execute(
            "CREATE INDEX idx_snapshots_host_id ON snapshots(host_id)"
        )
        conn.execute(
            "CREATE INDEX idx_snapshots_created_at ON snapshots(created_at)"
        )
        conn.execute(
            """
            CREATE INDEX idx_snapshots_content_fingerprint
            ON snapshots(content_fingerprint)
            """
        )
        conn.execute("PRAGMA user_version = 7")

    original_registry = store_sqlite.MIGRATIONS
    transition = original_registry[7]

    def normalize_then_fail(conn: sqlite3.Connection) -> None:
        transition.apply(conn)
        raise RuntimeError("controlled timestamp migration interruption")

    interrupted = list(original_registry)
    interrupted[7] = store_sqlite.Migration(7, 8, normalize_then_fail)
    monkeypatch.setattr(store_sqlite, "MIGRATIONS", tuple(interrupted))
    with pytest.raises(
        RuntimeError,
        match="controlled timestamp migration interruption",
    ):
        init_store(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        assert _user_version(conn) == 7
        assert conn.execute(
            """
            SELECT created_at, content_fingerprint
            FROM snapshots
            ORDER BY id
            """
        ).fetchall() == raw_rows
        assert "store_maintenance_state" not in _table_names(conn)

    monkeypatch.setattr(store_sqlite, "MIGRATIONS", original_registry)
    init_store(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        normalized = conn.execute(
            """
            SELECT created_at, content_fingerprint
            FROM snapshots
            ORDER BY id
            """
        ).fetchall()
        indexes = {
            str(row[1])
            for row in conn.execute("PRAGMA index_list(snapshots)").fetchall()
        }
        conn.execute("BEGIN IMMEDIATE")
        store_sqlite._migrate_v7_to_v8_conn(conn)
        conn.commit()
        rerun = conn.execute(
            """
            SELECT created_at, content_fingerprint
            FROM snapshots
            ORDER BY id
            """
        ).fetchall()

    assert normalized == [
        (
            "2026-06-30T23:00:00+00:00",
            "valid-old-positive-offset",
        ),
        (
            "2026-07-01T01:00:00+00:00",
            "adversarial-newer-negative-offset",
        ),
        (
            store_sqlite._SNAPSHOT_CREATED_AT_QUARANTINE,
            "malformed-quarantine",
        ),
        (
            store_sqlite._SNAPSHOT_CREATED_AT_QUARANTINE,
            "underflow-quarantine",
        ),
        (
            store_sqlite._SNAPSHOT_CREATED_AT_QUARANTINE,
            "overflow-quarantine",
        ),
        (
            store_sqlite._SNAPSHOT_CREATED_AT_QUARANTINE,
            "legacy-year-9999-quarantine",
        ),
        (
            store_sqlite._SNAPSHOT_CREATED_AT_QUARANTINE,
            "legacy-sentinel-malformed-payload",
        ),
        (
            store_sqlite._SNAPSHOT_CREATED_AT_QUARANTINE,
            "legacy-sentinel-different-payload",
        ),
        (
            store_sqlite._SNAPSHOT_CREATED_AT_QUARANTINE,
            "legacy-sentinel-underflow-payload",
        ),
        (
            store_sqlite._SNAPSHOT_CREATED_AT_QUARANTINE,
            "legacy-sentinel-overflow-payload",
        ),
        (
            store_sqlite._LEGACY_SNAPSHOT_CREATED_AT_QUARANTINE,
            "legitimate-year-9999-observation",
        ),
        (
            "2026-06-30T22:30:00+00:00",
            "old-but-latest",
        ),
    ]
    assert (
        store_sqlite._strict_utc_timestamp(
            store_sqlite._SNAPSHOT_CREATED_AT_QUARANTINE
        )
        is None
    )
    assert (
        store_sqlite._SNAPSHOT_CREATED_AT_QUARANTINE
        > store_sqlite._LEGACY_SNAPSHOT_CREATED_AT_QUARANTINE
    )
    assert rerun == normalized
    assert indexes == {
        "idx_snapshots_host_newest",
        "idx_snapshots_created_host_id",
    }

    cleanup = store_sqlite.cleanup_snapshot_retention(
        db_path,
        retention_days=1,
        retention_count=100,
        batch_size=100,
        now="2026-07-02T00:00:00Z",
    )
    with sqlite3.connect(str(db_path)) as conn:
        retained = conn.execute(
            """
            SELECT content_fingerprint
            FROM snapshots
            ORDER BY id
            """
        ).fetchall()
        integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]
        foreign_keys = conn.execute("PRAGMA foreign_key_check").fetchall()

    assert cleanup["deleted"] == 1
    assert retained == [
        ("adversarial-newer-negative-offset",),
        ("malformed-quarantine",),
        ("underflow-quarantine",),
        ("overflow-quarantine",),
        ("legacy-year-9999-quarantine",),
        ("legacy-sentinel-malformed-payload",),
        ("legacy-sentinel-different-payload",),
        ("legacy-sentinel-underflow-payload",),
        ("legacy-sentinel-overflow-payload",),
        ("legitimate-year-9999-observation",),
        ("old-but-latest",),
    ]
    assert integrity == "ok"
    assert foreign_keys == []


def test_legacy_sentinel_mismatched_payload_recovers_then_remains_monotonic(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "quarantine-recovery.db"
    config = Config(host_id="quarantine-host", db_path=db_path)
    base_at = "2026-01-01T00:00:00+00:00"
    fresh_at = "2027-01-01T00:00:00+00:00"
    stale_at = "2026-06-01T00:00:00+00:00"
    base = _snapshot_with_worker_status(
        config,
        status="blocked",
        observed_at=base_at,
    )
    fresh = _snapshot_with_worker_status(
        config,
        status="blocked",
        observed_at=fresh_at,
    )
    stale = _snapshot_with_worker_status(
        config,
        status="blocked",
        observed_at=stale_at,
    )
    assert {
        base.content_fingerprint,
        fresh.content_fingerprint,
        stale.content_fingerprint,
    } == {base.content_fingerprint}
    _save_observation(db_path, base, "positive", base_at)

    with sqlite3.connect(str(db_path)) as conn:
        row_id = conn.execute(
            """
            SELECT id
            FROM snapshots
            WHERE host_id = ?
            """,
            ("quarantine-host",),
        ).fetchone()[0]
        conn.execute(
            """
            UPDATE snapshots
            SET created_at = ?
            WHERE id = ?
            """,
            (
                store_sqlite._LEGACY_SNAPSHOT_CREATED_AT_QUARANTINE,
                row_id,
            ),
        )
        conn.execute(
            """
            UPDATE workers
            SET observed_at = 'malformed-legacy-time'
            WHERE host_id = ?
            """,
            ("quarantine-host",),
        )
        conn.execute(
            """
            UPDATE backend_health
            SET observed_at = 'malformed-legacy-time'
            WHERE host_id = ?
            """,
            ("quarantine-host",),
        )
        conn.execute("DROP TABLE store_maintenance_state")
        conn.execute("DROP INDEX idx_snapshots_host_newest")
        conn.execute("DROP INDEX idx_snapshots_created_host_id")
        conn.execute("PRAGMA user_version = 7")

    init_store(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        assert conn.execute(
            "SELECT created_at FROM snapshots WHERE id = ?",
            (row_id,),
        ).fetchone() == (store_sqlite._SNAPSHOT_CREATED_AT_QUARANTINE,)

    _save_observation(db_path, fresh, "positive", fresh_at)

    def current_state() -> tuple[Any, ...]:
        with sqlite3.connect(str(db_path)) as conn:
            retained = conn.execute(
                """
                SELECT id, created_at, payload
                FROM snapshots
                WHERE host_id = ?
                """,
                ("quarantine-host",),
            ).fetchone()
            return (
                int(retained[0]),
                str(retained[1]),
                json.loads(retained[2])["updated_at"],
                conn.execute(
                    """
                    SELECT observed_at
                    FROM workers
                    WHERE host_id = ? AND worker_id = 'worker-1'
                    """,
                    ("quarantine-host",),
                ).fetchone()[0],
                conn.execute(
                    """
                    SELECT observed_at
                    FROM backend_health
                    WHERE host_id = ? AND backend_name = 'herdr'
                    """,
                    ("quarantine-host",),
                ).fetchone()[0],
                conn.execute(
                    """
                    SELECT last_seen_at, signal_count
                    FROM attention_items
                    WHERE host_id = ?
                    """,
                    ("quarantine-host",),
                ).fetchone(),
                conn.execute(
                    "SELECT COUNT(*) FROM snapshots WHERE host_id = ?",
                    ("quarantine-host",),
                ).fetchone()[0],
                conn.execute(
                    """
                    SELECT COUNT(*)
                    FROM events
                    WHERE host_id = ? AND event_type = 'snapshot.saved'
                    """,
                    ("quarantine-host",),
                ).fetchone()[0],
            )

    recovered = current_state()
    assert recovered == (
        row_id,
        fresh_at,
        fresh_at,
        fresh_at,
        fresh_at,
        (fresh_at, 2),
        1,
        1,
    )

    _save_observation(db_path, stale, "positive", stale_at)
    assert current_state() == recovered



def test_snapshot_retention_age_count_multi_host_latest_and_durable_sentinels(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "snapshot-retention.db"
    config = Config(host_id="projection-host", db_path=db_path)
    projection_snapshot = project_from_raw(
        config,
        workers=[{"id": "worker-1", "name": "Worker", "status": "active"}],
    )
    save_snapshot(db_path, projection_snapshot)
    with sqlite3.connect(str(db_path)) as conn:
        turn_before = conn.execute(
            "SELECT host_id, turn_id, payload_json FROM turns"
        ).fetchall()
        conn.execute(
            """
            INSERT INTO connector_outbox (
                host_id, connector, delivery_key, status, payload_json,
                private_state_json, created_at, updated_at
            ) VALUES (
                'projection-host', 'attention', 'durable-job', 'queued',
                '{}', '{"sentinel":"durable"}',
                '2026-01-01T00:00:00+00:00',
                '2026-01-01T00:00:00+00:00'
            )
            """
        )
        for host_id, created_at, marker in (
            ("age-host", "2025-12-01T00:00:00+00:00", "age-old"),
            ("age-host", "2026-01-30T00:00:00+00:00", "age-recent-1"),
            ("age-host", "2026-01-31T00:00:00+00:00", "age-recent-2"),
            ("old-host", "2025-12-01T00:00:00+00:00", "old-1"),
            ("old-host", "2025-12-02T00:00:00+00:00", "old-2"),
            ("old-host", "2025-12-03T00:00:00+00:00", "old-latest"),
            ("count-host", "2026-01-27T00:00:00+00:00", "count-1"),
            ("count-host", "2026-01-28T00:00:00+00:00", "count-2"),
            ("count-host", "2026-01-29T00:00:00+00:00", "count-3"),
            ("count-host", "2026-01-30T00:00:00+00:00", "count-4"),
        ):
            conn.execute(
                """
                INSERT INTO snapshots (
                    host_id, created_at, content_fingerprint, payload
                ) VALUES (?, ?, ?, ?)
                """,
                (host_id, created_at, marker, json.dumps({"marker": marker})),
            )
        newest_before = dict(
            conn.execute(
                "SELECT host_id, MAX(id) FROM snapshots GROUP BY host_id"
            ).fetchall()
        )

    dry_run = store_sqlite.cleanup_snapshot_retention(
        db_path,
        retention_days=14,
        retention_count=2,
        batch_size=2,
        now="2026-02-01T00:00:00+00:00",
        dry_run=True,
    )
    results = []
    while True:
        result = store_sqlite.cleanup_snapshot_retention(
            db_path,
            retention_days=14,
            retention_count=2,
            batch_size=2,
            now="2026-02-01T00:00:00+00:00",
        )
        results.append(result)
        if not result["remaining_candidates"]:
            break

    with sqlite3.connect(str(db_path)) as conn:
        retained = conn.execute(
            """
            SELECT host_id, content_fingerprint
            FROM snapshots
            WHERE host_id IN ('age-host', 'old-host', 'count-host')
            ORDER BY host_id, id
            """
        ).fetchall()
        newest_after = dict(
            conn.execute(
                "SELECT host_id, MAX(id) FROM snapshots GROUP BY host_id"
            ).fetchall()
        )
        turn_after = conn.execute(
            "SELECT host_id, turn_id, payload_json FROM turns"
        ).fetchall()
        outbox = conn.execute(
            "SELECT status, private_state_json FROM connector_outbox"
        ).fetchall()
        integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]
        foreign_keys = conn.execute("PRAGMA foreign_key_check").fetchall()

    assert dry_run["deleted"] == 0
    assert dry_run["examined"] == 2
    assert all(result["examined"] <= 2 and result["deleted"] <= 2 for result in results)
    assert retained == [
        ("age-host", "age-recent-1"),
        ("age-host", "age-recent-2"),
        ("count-host", "count-3"),
        ("count-host", "count-4"),
        ("old-host", "old-latest"),
    ]
    assert newest_after == newest_before
    assert turn_after == turn_before
    assert outbox == [("queued", '{"sentinel":"durable"}')]
    assert integrity == "ok"
    assert foreign_keys == []


def test_automatic_snapshot_maintenance_cadence_is_persisted_bounded_and_resumable(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "automatic-maintenance.db"
    init_store(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        conn.executemany(
            """
            INSERT INTO snapshots (
                host_id, created_at, content_fingerprint, payload
            ) VALUES ('cadence-host', ?, ?, '{}')
            """,
            [
                (f"2026-01-{day:02d}T00:00:00+00:00", f"fp-{day}")
                for day in range(1, 7)
            ],
        )

    policy = store_sqlite.SnapshotRetentionPolicy(
        retention_days=30,
        retention_count=2,
        batch_size=2,
    )
    first = store_sqlite.maybe_run_automatic_store_maintenance(
        db_path,
        policy=policy,
        cadence_seconds=3600,
        now="2026-02-01T00:00:00+00:00",
    )
    not_due = store_sqlite.maybe_run_automatic_store_maintenance(
        db_path,
        policy=policy,
        cadence_seconds=3600,
        now="2026-02-01T00:10:00+00:00",
    )
    second = store_sqlite.maybe_run_automatic_store_maintenance(
        db_path,
        policy=policy,
        cadence_seconds=3600,
        now="2026-02-01T01:00:00+00:00",
    )

    with sqlite3.connect(str(db_path)) as conn:
        state = conn.execute(
            """
            SELECT last_completed_at, last_status, last_examined, last_deleted
            FROM store_maintenance_state
            """
        ).fetchone()
        remaining = conn.execute("SELECT COUNT(*) FROM snapshots").fetchone()[0]

    assert first["status"] == "ok"
    assert first["snapshot"] == {
        "examined": 2,
        "deleted": 2,
        "remaining_candidates": True,
    }
    assert not_due["status"] == "not_due"
    assert not_due["due"] is False
    assert second["snapshot"] == {
        "examined": 2,
        "deleted": 2,
        "remaining_candidates": False,
    }
    assert state == ("2026-02-01T01:00:00+00:00", "ok", 2, 2)
    assert remaining == 2


def test_snapshot_retention_query_plans_use_v8_composite_indexes(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "retention-plans.db"
    init_store(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        age_plan = [
            str(row[3])
            for row in conn.execute(
                "EXPLAIN QUERY PLAN " + store_sqlite._SNAPSHOT_AGE_CANDIDATE_SQL,
                {
                    "cutoff_at": "2026-01-01T00:00:00+00:00",
                    "candidate_limit": 100,
                },
            ).fetchall()
        ]
        count_plan = [
            str(row[3])
            for row in conn.execute(
                "EXPLAIN QUERY PLAN " + store_sqlite._SNAPSHOT_COUNT_CANDIDATE_SQL,
                {"retention_offset": 4095, "candidate_limit": 100},
            ).fetchall()
        ]

    age_text = " | ".join(age_plan)
    count_text = " | ".join(count_plan)
    assert "idx_snapshots_created_host_id" in age_text
    assert "idx_snapshots_host_newest" in age_text
    assert "idx_snapshots_host_newest" in count_text
    assert "MATERIALIZE boundaries" in count_text
    assert "host_id>?" in count_text
    assert "host_id=? AND id<?" in count_text
    assert "USE TEMP B-TREE" not in age_text
    assert "USE TEMP B-TREE" not in count_text


def test_snapshot_retention_vm_work_scales_linearly_to_fifty_thousand_rows(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def seed(path: Path, *, row_count: int, host_count: int) -> None:
        init_store(path)
        with sqlite3.connect(str(path)) as conn:
            conn.executemany(
                """
                INSERT INTO snapshots (
                    host_id, created_at, content_fingerprint, payload
                ) VALUES (?, '2026-01-31T00:00:00+00:00', ?, '{}')
                """,
                (
                    (
                        f"host-{index % host_count:02d}",
                        f"fingerprint-{index}",
                    )
                    for index in range(row_count)
                ),
            )

    modest_path = tmp_path / "retention-work-5000.db"
    large_path = tmp_path / "retention-work-50000.db"
    seed(modest_path, row_count=5_000, host_count=1)
    seed(large_path, row_count=50_000, host_count=10)
    def measured_zero_old_age_query(path: Path) -> tuple[int, list[int]]:
        vm_steps = 0
        with sqlite3.connect(str(path)) as conn:
            def count_progress() -> int:
                nonlocal vm_steps
                vm_steps += 10
                return 0

            conn.set_progress_handler(count_progress, 10)
            rows = [
                int(row[0])
                for row in conn.execute(
                    store_sqlite._SNAPSHOT_AGE_CANDIDATE_SQL,
                    {
                        "cutoff_at": "2026-01-18T00:00:00+00:00",
                        "candidate_limit": 101,
                    },
                ).fetchall()
            ]
        return vm_steps, rows

    modest_age_steps, modest_age_rows = measured_zero_old_age_query(modest_path)
    large_age_steps, large_age_rows = measured_zero_old_age_query(large_path)

    original_connect = store_sqlite._connect

    def measured_cleanup(path: Path) -> tuple[int, dict[str, Any]]:
        vm_steps = 0

        def counted_connect(*args: Any, **kwargs: Any) -> sqlite3.Connection:
            nonlocal vm_steps
            conn = original_connect(*args, **kwargs)

            def count_progress() -> int:
                nonlocal vm_steps
                vm_steps += 1_000
                return 0

            conn.set_progress_handler(count_progress, 1_000)
            return conn

        monkeypatch.setattr(store_sqlite, "_connect", counted_connect)
        try:
            result = store_sqlite.cleanup_snapshot_retention(
                path,
                retention_days=14,
                retention_count=4096,
                batch_size=100,
                now="2026-02-01T00:00:00+00:00",
                dry_run=True,
            )
        finally:
            monkeypatch.setattr(store_sqlite, "_connect", original_connect)
        return vm_steps, result

    modest_steps, modest = measured_cleanup(modest_path)
    large_steps, large = measured_cleanup(large_path)

    assert modest_age_rows == large_age_rows == []
    assert modest_age_steps < 1_000
    assert large_age_steps < 1_000
    assert large_age_steps <= modest_age_steps + 100
    assert modest["examined"] == large["examined"] == 100
    assert modest["remaining_candidates"] is True
    assert large["remaining_candidates"] is True
    assert modest_steps < 60_000
    assert large_steps < 600_000
    assert large_steps <= modest_steps * 12


def test_request_and_maintenance_paths_never_issue_vacuum(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "no-request-vacuum.db"
    config = Config(host_id="no-vacuum-host", db_path=db_path)
    snapshot = project_empty(config)
    statements: list[str] = []
    original_connect = sqlite3.connect

    def traced_connect(*args: Any, **kwargs: Any) -> sqlite3.Connection:
        conn = original_connect(*args, **kwargs)
        conn.set_trace_callback(statements.append)
        return conn

    monkeypatch.setattr(store_sqlite.sqlite3, "connect", traced_connect)
    save_snapshot(db_path, snapshot)
    store_sqlite.cleanup_snapshot_retention(
        db_path,
        retention_days=14,
        retention_count=4096,
        batch_size=100,
        now="2026-02-01T00:00:00+00:00",
    )
    run_store_maintenance(
        db_path,
        "no-vacuum-host",
        retention_days=14,
        max_outbox_attempts=3,
        now="2026-02-01T00:00:00+00:00",
    )

    assert not any(statement.lstrip().upper().startswith("VACUUM") for statement in statements)


def test_store_status_require_current_schema_is_query_only_and_nonmutating(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "readonly-current-status.db"
    init_store(db_path)
    traces: list[str] = []
    original_pragmas = store_sqlite._apply_connection_pragmas

    def traced_pragmas(
        conn: sqlite3.Connection,
        path: Path | str,
    ) -> None:
        conn.set_trace_callback(traces.append)
        original_pragmas(conn, path)

    monkeypatch.setattr(store_sqlite, "_apply_connection_pragmas", traced_pragmas)
    result = store_status(
        db_path,
        "readonly-host",
        require_current_schema=True,
    )

    normalized = [" ".join(statement.upper().split()) for statement in traces]
    assert result["ok"] is True
    assert normalized.count("PRAGMA USER_VERSION") == 1
    assert "PRAGMA QUERY_ONLY=ON" in normalized
    assert "PRAGMA JOURNAL_MODE=WAL" not in normalized
    assert not any(
        statement.startswith(
            ("BEGIN", "CREATE ", "ALTER ", "DROP ", "INSERT ", "UPDATE ", "DELETE ")
        )
        for statement in normalized
    )


def test_store_status_require_current_schema_refuses_old_missing_and_unsafe_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    old_path = tmp_path / "old" / "store.db"
    init_store(old_path)
    with sqlite3.connect(str(old_path)) as conn:
        conn.execute("CREATE TABLE readonly_sentinel (value TEXT NOT NULL)")
        conn.execute("INSERT INTO readonly_sentinel VALUES ('preserved')")
        conn.execute("PRAGMA user_version = 7")
    before_names = sorted(path.name for path in old_path.parent.iterdir())
    traces: list[str] = []
    original_pragmas = store_sqlite._apply_connection_pragmas

    def traced_pragmas(
        conn: sqlite3.Connection,
        path: Path | str,
    ) -> None:
        conn.set_trace_callback(traces.append)
        original_pragmas(conn, path)

    monkeypatch.setattr(store_sqlite, "_apply_connection_pragmas", traced_pragmas)
    old = store_status(old_path, "host", require_current_schema=True)
    normalized = [" ".join(statement.upper().split()) for statement in traces]
    with sqlite3.connect(str(old_path)) as conn:
        version = _user_version(conn)
        sentinel = conn.execute("SELECT value FROM readonly_sentinel").fetchone()
    assert old["status"] == "schema_not_current"
    assert old["ok"] is False
    assert version == 7
    assert sentinel == ("preserved",)
    assert sorted(path.name for path in old_path.parent.iterdir()) == before_names
    assert normalized.count("PRAGMA USER_VERSION") == 1
    assert "PRAGMA QUERY_ONLY=ON" in normalized
    assert "PRAGMA JOURNAL_MODE=WAL" not in normalized
    assert not any(
        statement.startswith(
            ("BEGIN", "CREATE ", "ALTER ", "DROP ", "INSERT ", "UPDATE ", "DELETE ")
        )
        for statement in normalized
    )

    missing_path = tmp_path / "missing" / "store.db"
    missing = store_status(
        missing_path,
        "host",
        require_current_schema=True,
    )
    assert missing["status"] == "store_unavailable"
    assert not missing_path.parent.exists()

    unsafe_path = tmp_path / "unsafe" / "store.db"
    init_store(unsafe_path)
    unsafe_path.chmod(0o644)
    unsafe = store_status(
        unsafe_path,
        "host",
        require_current_schema=True,
    )
    encoded = json.dumps(unsafe, sort_keys=True)
    assert unsafe["status"] == "store_unavailable"
    assert _mode(unsafe_path) == 0o644
    assert str(unsafe_path) not in encoded
    assert str(tmp_path) not in encoded


def _seed_compaction_fixture(
    tmp_path: Path,
    *,
    snapshot_rows: int = 8,
) -> tuple[Path, str]:
    db_path = tmp_path / "compact-store.db"
    private_payload = "sentinel-private-payload"
    init_store(db_path)
    with store_sqlite._connect(db_path, isolation_level=None) as conn:
        conn.execute("CREATE TABLE compaction_sentinel (value TEXT NOT NULL)")
        conn.execute(
            "INSERT INTO compaction_sentinel (value) VALUES (?)",
            ("logically-preserved",),
        )
        for sequence in range(snapshot_rows):
            host_id = f"private-host-{sequence % 2}"
            payload = json.dumps(
                {
                    "private": private_payload,
                    "padding": "x" * 16_384,
                    "sequence": sequence,
                },
                sort_keys=True,
            )
            conn.execute(
                """
                INSERT INTO snapshots (
                    host_id, created_at, payload, content_fingerprint
                ) VALUES (?, ?, ?, ?)
                """,
                (
                    host_id,
                    f"2025-01-{sequence + 1:02d}T00:00:00+00:00",
                    payload,
                    f"private-fingerprint-{sequence}",
                ),
            )
    return db_path, private_payload


def _assert_compaction_logical_evidence(db_path: Path) -> None:
    with sqlite3.connect(str(db_path)) as conn:
        assert conn.execute("PRAGMA quick_check").fetchone()[0] == "ok"
        assert conn.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        assert conn.execute("PRAGMA foreign_key_check").fetchone() is None
        assert conn.execute(
            "SELECT value FROM compaction_sentinel"
        ).fetchone()[0] == "logically-preserved"
        assert {
            str(row[0])
            for row in conn.execute(
                "SELECT DISTINCT host_id FROM snapshots"
            ).fetchall()
        } == {"private-host-0", "private-host-1"}
        assert conn.execute(
            "SELECT LENGTH(store_epoch) > 0 FROM turn_list_state WHERE scope = 'turn-list'"
        ).fetchone() == (1,)
        assert "turn_list_hosts" in _table_names(conn)


def test_compaction_metrics_consume_one_authoritative_snapshot_without_lstat(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    members = (
        store_sqlite._SQLiteFamilyMemberSnapshot(
            kind=LocalStateKind.DATABASE,
            state=PermissionState.PRIVATE,
            mode=0o600,
            identity=EntryIdentity(11, 101),
            size=4096,
            link_count=1,
        ),
        store_sqlite._SQLiteFamilyMemberSnapshot(
            kind=LocalStateKind.DATABASE_WAL,
            state=PermissionState.PRIVATE,
            mode=0o400,
            identity=EntryIdentity(11, 102),
            size=128,
            link_count=1,
        ),
        store_sqlite._SQLiteFamilyMemberSnapshot(
            kind=LocalStateKind.DATABASE_SHM,
            state=PermissionState.ABSENT,
            mode=None,
            identity=None,
            size=None,
            link_count=None,
        ),
        store_sqlite._SQLiteFamilyMemberSnapshot(
            kind=LocalStateKind.DATABASE_JOURNAL,
            state=PermissionState.PRIVATE,
            mode=0o600,
            identity=EntryIdentity(11, 103),
            size=64,
            link_count=1,
        ),
    )
    observations: list[tuple[int, str, bool]] = []

    def fixed_snapshot(
        parent_fd: int,
        leaf: str,
        *,
        require_main: bool,
    ) -> tuple[Any, ...]:
        observations.append((parent_fd, leaf, require_main))
        return members

    def reject_second_lookup(*_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError("metrics must not perform a second pathname lookup")

    monkeypatch.setattr(
        store_sqlite,
        "_snapshot_sqlite_family_at",
        fixed_snapshot,
    )
    monkeypatch.setattr(
        store_sqlite,
        "lstat_at",
        reject_second_lookup,
        raising=False,
    )
    monkeypatch.setattr(
        store_sqlite,
        "inspect_sqlite_family_at",
        reject_second_lookup,
        raising=False,
    )

    assert store_sqlite._sqlite_family_bytes_and_identity(17, "store.db") == (
        4288,
        4096,
        0o600,
        EntryIdentity(11, 101),
    )
    assert observations == [(17, "store.db", False)]


def test_compact_store_dry_run_accepts_optional_disappearance_in_its_only_observation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path, _private_payload = _seed_compaction_fixture(tmp_path)
    journal_path = Path(f"{db_path}-journal")
    _create_private_empty_file(journal_path)
    original_snapshot = store_sqlite._snapshot_sqlite_family_at
    observations: list[tuple[PermissionState, ...]] = []

    def disappear_then_snapshot(
        parent_fd: int,
        leaf: str,
        *,
        require_main: bool,
    ) -> tuple[Any, ...]:
        journal_path.unlink()
        members = original_snapshot(
            parent_fd,
            leaf,
            require_main=require_main,
        )
        observations.append(tuple(member.state for member in members))
        return members

    monkeypatch.setattr(
        store_sqlite,
        "_snapshot_sqlite_family_at",
        disappear_then_snapshot,
    )
    before_fds = set(os.listdir("/proc/self/fd"))

    result = compact_store(
        db_path,
        options=CompactionOptions(dry_run=True),
    )

    assert result["status"] == "dry_run"
    assert result["ok"] is True
    assert len(observations) == 1
    assert observations[0][3] is PermissionState.ABSENT
    assert not journal_path.exists()
    assert set(os.listdir("/proc/self/fd")) == before_fds


def test_compact_store_accepts_optional_disappearance_in_locked_observation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path, _private_payload = _seed_compaction_fixture(tmp_path)
    backup_path = tmp_path / "optional-disappearance-backup.db"
    journal_path = Path(f"{db_path}-journal")
    _create_private_empty_file(journal_path)
    original_snapshot = store_sqlite._snapshot_sqlite_family_at
    observations: list[tuple[PermissionState, ...]] = []

    def observe_then_disappear(
        parent_fd: int,
        leaf: str,
        *,
        require_main: bool,
    ) -> tuple[Any, ...]:
        if len(observations) == 1:
            journal_path.unlink()
        members = original_snapshot(
            parent_fd,
            leaf,
            require_main=require_main,
        )
        observations.append(tuple(member.state for member in members))
        return members

    monkeypatch.setattr(
        store_sqlite,
        "_snapshot_sqlite_family_at",
        observe_then_disappear,
    )
    before_fds = set(os.listdir("/proc/self/fd"))

    result = compact_store(
        db_path,
        options=CompactionOptions(
            dry_run=False,
            acknowledge_offline=True,
            backup_path=backup_path,
        ),
    )

    assert result["status"] == "completed"
    assert result["ok"] is True
    assert observations[0][3] is PermissionState.PRIVATE
    assert observations[1][3] is PermissionState.ABSENT
    assert backup_path.is_file()
    assert not journal_path.exists()
    assert set(os.listdir("/proc/self/fd")) == before_fds


@pytest.mark.parametrize("change", ["disappear", "substitute"])
def test_compact_store_rejects_locked_main_change_before_backup_or_vacuum(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    change: str,
) -> None:
    db_path, _private_payload = _seed_compaction_fixture(tmp_path)
    backup_path = tmp_path / f"main-{change}-backup.db"
    displaced = tmp_path / f"selected-main-{change}.db"
    selected = db_path.stat()
    replacement_bytes = b"untrusted compact replacement"
    original_snapshot = store_sqlite._snapshot_sqlite_family_at
    observations = 0
    backup_calls: list[None] = []
    vacuum_calls: list[None] = []

    def change_before_locked_observation(
        parent_fd: int,
        leaf: str,
        *,
        require_main: bool,
    ) -> tuple[Any, ...]:
        nonlocal observations
        observations += 1
        if observations == 2:
            db_path.rename(displaced)
            if change == "substitute":
                db_path.write_bytes(replacement_bytes)
                db_path.chmod(0o600)
        return original_snapshot(
            parent_fd,
            leaf,
            require_main=require_main,
        )

    original_backup = store_sqlite._create_verified_compaction_backup
    original_vacuum = store_sqlite._vacuum_into_replacement

    def record_backup(*args: Any, **kwargs: Any) -> Any:
        backup_calls.append(None)
        return original_backup(*args, **kwargs)

    def record_vacuum(*args: Any, **kwargs: Any) -> Any:
        vacuum_calls.append(None)
        return original_vacuum(*args, **kwargs)

    monkeypatch.setattr(
        store_sqlite,
        "_snapshot_sqlite_family_at",
        change_before_locked_observation,
    )
    monkeypatch.setattr(
        store_sqlite,
        "_create_verified_compaction_backup",
        record_backup,
    )
    monkeypatch.setattr(
        store_sqlite,
        "_vacuum_into_replacement",
        record_vacuum,
    )
    before_fds = set(os.listdir("/proc/self/fd"))
    before_threads = {id(thread) for thread in threading.enumerate()}
    before_children = {process.pid for process in multiprocessing.active_children()}

    result = compact_store(
        db_path,
        options=CompactionOptions(
            dry_run=False,
            acknowledge_offline=True,
            backup_path=backup_path,
        ),
    )

    assert result["status"] == "permissions_failed"
    assert result["ok"] is False
    assert observations == 2
    assert backup_calls == []
    assert vacuum_calls == []
    assert not backup_path.exists()
    assert (displaced.stat().st_dev, displaced.stat().st_ino) == (
        selected.st_dev,
        selected.st_ino,
    )
    if change == "substitute":
        assert db_path.read_bytes() == replacement_bytes
    else:
        assert not db_path.exists()
    assert not any(
        path.name.startswith(".tendwire-sqlite-")
        for path in tmp_path.iterdir()
    )
    assert set(os.listdir("/proc/self/fd")) == before_fds
    assert {id(thread) for thread in threading.enumerate()} == before_threads
    assert {process.pid for process in multiprocessing.active_children()} == before_children


@pytest.mark.parametrize("entry_type", ["symlink", "directory"])
def test_compact_store_refuses_invalid_optional_entry_before_backup_or_vacuum(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    entry_type: str,
) -> None:
    db_path, _private_payload = _seed_compaction_fixture(tmp_path)
    backup_path = tmp_path / f"invalid-{entry_type}-backup.db"
    journal_path = Path(f"{db_path}-journal")
    target = tmp_path / "outside-target"
    _create_private_empty_file(target)
    target_bytes = b"outside must stay unchanged"
    target.write_bytes(target_bytes)
    target.chmod(0o600)
    if entry_type == "symlink":
        journal_path.symlink_to(target)
    else:
        journal_path.mkdir(mode=0o700)
    before_target = target.stat()
    backup_calls: list[None] = []
    vacuum_calls: list[None] = []
    original_backup = store_sqlite._create_verified_compaction_backup
    original_vacuum = store_sqlite._vacuum_into_replacement

    def record_backup(*args: Any, **kwargs: Any) -> Any:
        backup_calls.append(None)
        return original_backup(*args, **kwargs)

    def record_vacuum(*args: Any, **kwargs: Any) -> Any:
        vacuum_calls.append(None)
        return original_vacuum(*args, **kwargs)

    monkeypatch.setattr(
        store_sqlite,
        "_create_verified_compaction_backup",
        record_backup,
    )
    monkeypatch.setattr(
        store_sqlite,
        "_vacuum_into_replacement",
        record_vacuum,
    )

    result = compact_store(
        db_path,
        options=CompactionOptions(
            dry_run=False,
            acknowledge_offline=True,
            backup_path=backup_path,
        ),
    )

    assert result["status"] == "permissions_failed"
    assert result["ok"] is False
    assert backup_calls == []
    assert vacuum_calls == []
    assert not backup_path.exists()
    if entry_type == "symlink":
        assert journal_path.is_symlink()
    else:
        assert journal_path.is_dir()
    after_target = target.stat()
    assert target.read_bytes() == target_bytes
    assert (after_target.st_dev, after_target.st_ino, after_target.st_mode) == (
        before_target.st_dev,
        before_target.st_ino,
        before_target.st_mode,
    )


def test_compact_store_dry_run_is_strictly_non_mutating_and_aggregate_only(
    tmp_path: Path,
) -> None:
    db_path, private_payload = _seed_compaction_fixture(tmp_path)
    before = _tree_metadata(tmp_path)

    result = compact_store(
        db_path,
        options=CompactionOptions(
            dry_run=True,
            snapshot_retention_days=14,
            snapshot_retention_count=1,
            batch_size=2,
        ),
        now="2026-02-01T00:00:00+00:00",
    )

    assert set(result) == {
        "schema_version",
        "ok",
        "status",
        "command",
        "scope",
        "dry_run",
        "maintenance_window_acknowledged",
        "permissions",
        "integrity",
        "space",
        "snapshots",
        "storage",
        "backup",
        "checkpoint",
        "replacement",
        "rollback",
    }
    assert result["command"] == "store.compact"
    assert result["ok"] is True
    assert result["status"] == "dry_run"
    assert result["snapshots"] == {
        "before": 8,
        "retained": 2,
        "eligible": 6,
        "examined": 0,
        "deleted": 0,
        "remaining": 6,
        "latest_hosts_retained": 2,
    }
    assert result["backup"] == {
        "required": True,
        "created": False,
        "verified": False,
    }
    assert result["integrity"] == {
        "before": "ok",
        "backup": "not_run",
        "replacement": "not_run",
        "after": "not_run",
    }
    assert _tree_metadata(tmp_path) == before
    encoded = json.dumps(result, sort_keys=True)
    for private_value in (
        str(db_path),
        db_path.name,
        private_payload,
        "private-host",
        "private-fingerprint",
    ):
        assert private_value not in encoded


def test_compact_store_success_preserves_logical_state_and_retains_backup(
    tmp_path: Path,
) -> None:
    db_path, private_payload = _seed_compaction_fixture(
        tmp_path,
        snapshot_rows=20,
    )
    backup_path = tmp_path / "operator-backup.db"

    result = compact_store(
        db_path,
        options=CompactionOptions(
            dry_run=False,
            acknowledge_offline=True,
            backup_path=backup_path,
            snapshot_retention_days=14,
            snapshot_retention_count=1,
            batch_size=3,
        ),
        now="2026-02-01T00:00:00+00:00",
    )

    assert result["ok"] is True
    assert result["status"] == "completed"
    assert result["snapshots"] == {
        "before": 20,
        "retained": 2,
        "eligible": 18,
        "examined": 18,
        "deleted": 18,
        "remaining": 0,
        "latest_hosts_retained": 2,
    }
    assert result["backup"] == {
        "required": True,
        "created": True,
        "verified": True,
    }
    assert result["checkpoint"] == {"status": "completed"}
    assert result["replacement"] == {"status": "published"}
    assert result["rollback"] == {"status": "not_needed"}
    assert result["integrity"] == {
        "before": "ok",
        "backup": "ok",
        "replacement": "ok",
        "after": "ok",
    }
    assert result["storage"]["after_bytes"] <= result["storage"]["before_bytes"]
    assert result["space"]["available_bytes"] >= result["space"]["required_bytes"]
    assert backup_path.is_file()
    assert _mode(backup_path) & ~0o600 == 0
    _assert_compaction_logical_evidence(db_path)
    with sqlite3.connect(str(backup_path)) as backup:
        assert backup.execute("PRAGMA quick_check").fetchone()[0] == "ok"
        assert backup.execute(
            "SELECT COUNT(*) FROM snapshots"
        ).fetchone()[0] == 20
    encoded = json.dumps(result, sort_keys=True)
    assert private_payload not in encoded
    assert str(db_path) not in encoded
    assert str(backup_path) not in encoded


@pytest.mark.parametrize(
    ("phase", "expected_status", "backup_retained", "source_replaced"),
    [
        ("after_precheck", "backup_failed", False, False),
        ("before_backup", "backup_failed", False, False),
        ("after_backup", "maintenance_failed", True, False),
        ("during_replacement", "rollback_completed", True, True),
        ("after_replacement_check", "rollback_completed", True, True),
        ("before_publish", "rollback_completed", True, True),
        ("publication_failed", "rollback_completed", True, True),
        ("after_publish_check", "rollback_completed", True, True),
    ],
)
def test_compact_store_named_interruptions_preserve_or_restore_source(
    tmp_path: Path,
    phase: str,
    expected_status: str,
    backup_retained: bool,
    source_replaced: bool,
) -> None:
    db_path, _private_payload = _seed_compaction_fixture(tmp_path)
    backup_path = tmp_path / f"backup-{phase}.db"
    source_inode = db_path.stat().st_ino
    observed_phases: list[str] = []

    def interrupt(current: str) -> None:
        observed_phases.append(current)
        if current == phase:
            raise RuntimeError("sentinel-private-interruption")

    result = compact_store(
        db_path,
        options=CompactionOptions(
            dry_run=False,
            acknowledge_offline=True,
            backup_path=backup_path,
            snapshot_retention_days=14,
            snapshot_retention_count=8,
            batch_size=2,
        ),
        now="2026-02-01T00:00:00+00:00",
        phase_hook=interrupt,
    )

    assert result["ok"] is False
    assert result["status"] == expected_status
    assert phase in observed_phases
    assert backup_path.exists() is backup_retained
    if backup_retained:
        with sqlite3.connect(str(backup_path)) as backup:
            assert backup.execute("PRAGMA quick_check").fetchone()[0] == "ok"
    if not source_replaced:
        assert db_path.stat().st_ino == source_inode
    else:
        assert result["rollback"] == {"status": "completed"}
    assert "sentinel-private-interruption" not in json.dumps(result)
    assert not any(
        path.name.startswith(".tendwire-sqlite-")
        for path in tmp_path.iterdir()
    )
    _assert_compaction_logical_evidence(db_path)


def test_compact_store_refuses_insufficient_space_before_backup_or_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path, _private_payload = _seed_compaction_fixture(tmp_path)
    backup_path = tmp_path / "must-not-exist.db"
    before = _tree_metadata(tmp_path)
    monkeypatch.setattr(
        store_sqlite,
        "sqlite_parent_available_bytes_at",
        lambda _parent_fd: 0,
    )

    result = compact_store(
        db_path,
        options=CompactionOptions(
            dry_run=False,
            acknowledge_offline=True,
            backup_path=backup_path,
        ),
    )

    assert result["status"] == "insufficient_space"
    assert result["space"]["headroom_ok"] is False
    assert result["backup"]["created"] is False
    assert not backup_path.exists()
    assert _tree_metadata(tmp_path) == before


def test_compact_store_refuses_bad_integrity_without_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path, _private_payload = _seed_compaction_fixture(tmp_path)
    before = _tree_metadata(tmp_path)
    monkeypatch.setattr(store_sqlite, "_quick_check_ok", lambda _conn: False)

    result = compact_store(
        db_path,
        options=CompactionOptions(dry_run=True),
    )

    assert result["status"] == "integrity_failed"
    assert result["integrity"]["before"] == "failed"
    assert _tree_metadata(tmp_path) == before


def test_compact_store_requires_offline_writer_lock_before_backup(
    tmp_path: Path,
) -> None:
    db_path, _private_payload = _seed_compaction_fixture(tmp_path)
    backup_path = tmp_path / "offline-backup.db"
    writer = store_sqlite._connect(db_path, isolation_level=None)
    try:
        writer.execute("BEGIN IMMEDIATE")
        writer.execute(
            "INSERT INTO compaction_sentinel (value) VALUES ('uncommitted')"
        )
        result = compact_store(
            db_path,
            options=CompactionOptions(
                dry_run=False,
                acknowledge_offline=True,
                backup_path=backup_path,
            ),
        )
    finally:
        writer.rollback()
        writer.close()

    assert result["status"] == "offline_required"
    assert result["backup"]["created"] is False
    assert not backup_path.exists()
    _assert_compaction_logical_evidence(db_path)


def test_compact_store_rejects_execute_without_literal_authority_before_open(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    opened: list[Path] = []

    def forbidden_open(path: Path) -> tuple[int, str]:
        opened.append(path)
        raise AssertionError("invalid execute must not inspect the store")

    monkeypatch.setattr(store_sqlite, "open_resolved_parent", forbidden_open)

    missing_ack = compact_store(
        tmp_path / "private-store.db",
        options=CompactionOptions(
            dry_run=False,
            backup_path=tmp_path / "backup.db",
        ),
    )
    missing_backup = compact_store(
        tmp_path / "private-store.db",
        options=CompactionOptions(
            dry_run=False,
            acknowledge_offline=True,
        ),
    )

    assert missing_ack["status"] == "invalid_request"
    assert missing_backup["status"] == "invalid_request"
    assert opened == []


def test_compact_store_dry_run_reports_low_headroom_without_mutating(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path, _private_payload = _seed_compaction_fixture(tmp_path)
    before = _tree_metadata(tmp_path)
    monkeypatch.setattr(
        store_sqlite,
        "sqlite_parent_available_bytes_at",
        lambda _parent_fd: 0,
    )

    result = compact_store(
        db_path,
        options=CompactionOptions(dry_run=True),
    )

    assert result["status"] == "dry_run"
    assert result["ok"] is True
    assert result["space"]["headroom_ok"] is False
    assert _tree_metadata(tmp_path) == before


def test_compact_store_requires_current_v9_without_migrating(
    tmp_path: Path,
) -> None:
    db_path, _private_payload = _seed_compaction_fixture(tmp_path)
    stale_version = store_sqlite.STORE_SCHEMA_VERSION - 1
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(f"PRAGMA user_version={stale_version}")
    before = _tree_metadata(tmp_path)

    result = compact_store(
        db_path,
        options=CompactionOptions(dry_run=True),
    )

    assert result["status"] == "schema_not_current"
    assert result["integrity"]["before"] == "not_run"
    assert _tree_metadata(tmp_path) == before
    with sqlite3.connect(str(db_path)) as conn:
        assert conn.execute("PRAGMA user_version").fetchone()[0] == stale_version


def test_compact_store_refuses_insecure_source_without_repair(
    tmp_path: Path,
) -> None:
    db_path, _private_payload = _seed_compaction_fixture(tmp_path)
    os.chmod(db_path, 0o644)

    result = compact_store(
        db_path,
        options=CompactionOptions(dry_run=True),
    )

    assert result["status"] == "permissions_failed"
    assert result["permissions"] == {
        "ok": False,
        "outcome": "repair_required",
    }
    assert _mode(db_path) == 0o644


def test_compact_store_refuses_source_hardlink_alias_as_unsafe(
    tmp_path: Path,
) -> None:
    db_path, _private_payload = _seed_compaction_fixture(tmp_path)
    alias_path = tmp_path / "source-alias.db"
    os.link(db_path, alias_path)

    result = compact_store(
        db_path,
        options=CompactionOptions(dry_run=True),
    )

    assert result["status"] == "permissions_failed"
    assert result["permissions"] == {
        "ok": False,
        "outcome": "unsafe",
    }
    assert db_path.stat().st_ino == alias_path.stat().st_ino


@pytest.mark.parametrize(
    "attempt_phase",
    ["after_backup", "during_replacement", "after_publish_check"],
)
def test_compact_store_rejects_reconnecting_writer_until_publication_completes(
    tmp_path: Path,
    attempt_phase: str,
) -> None:
    db_path, _private_payload = _seed_compaction_fixture(tmp_path)
    backup_path = tmp_path / "stable-lock-backup.db"
    attempts: list[str] = []

    def attempt_reconnect(phase: str) -> None:
        if phase != attempt_phase:
            return
        try:
            connection = store_sqlite._connect(
                db_path,
                isolation_level=None,
            )
        except LocalStateError as exc:
            attempts.append(exc.code.value)
        else:
            connection.close()
            attempts.append("unexpectedly_opened")

    result = compact_store(
        db_path,
        options=CompactionOptions(
            dry_run=False,
            acknowledge_offline=True,
            backup_path=backup_path,
            snapshot_retention_days=14,
            snapshot_retention_count=8,
            batch_size=2,
        ),
        now="2026-02-01T00:00:00+00:00",
        phase_hook=attempt_reconnect,
    )

    assert result["status"] == "completed"
    assert attempts == [LocalStateErrorCode.OPERATION_FAILED.value]
    with store_sqlite._connect(db_path, isolation_level=None) as writer:
        writer.execute(
            "INSERT INTO compaction_sentinel (value) VALUES ('after-publication')"
        )
    with sqlite3.connect(str(db_path)) as conn:
        assert conn.execute(
            """
            SELECT COUNT(*)
            FROM compaction_sentinel
            WHERE value = 'after-publication'
            """
        ).fetchone()[0] == 1


def test_compact_store_restores_backup_after_committed_retention_before_publish(
    tmp_path: Path,
) -> None:
    db_path, _private_payload = _seed_compaction_fixture(
        tmp_path,
        snapshot_rows=12,
    )
    backup_path = tmp_path / "prepublication-rollback.db"

    def interrupt_after_retention(phase: str) -> None:
        if phase == "during_replacement":
            raise RuntimeError("private-prepublication-failure")

    result = compact_store(
        db_path,
        options=CompactionOptions(
            dry_run=False,
            acknowledge_offline=True,
            backup_path=backup_path,
            snapshot_retention_days=14,
            snapshot_retention_count=1,
            batch_size=2,
        ),
        now="2026-02-01T00:00:00+00:00",
        phase_hook=interrupt_after_retention,
    )

    assert result["status"] == "rollback_completed"
    assert result["rollback"] == {"status": "completed"}
    assert result["snapshots"]["deleted"] == 10
    with sqlite3.connect(str(db_path)) as conn:
        assert conn.execute("SELECT COUNT(*) FROM snapshots").fetchone()[0] == 12
        assert conn.execute("PRAGMA quick_check").fetchone()[0] == "ok"
        assert conn.execute("PRAGMA foreign_key_check").fetchone() is None
    with sqlite3.connect(str(backup_path)) as backup:
        assert backup.execute("SELECT COUNT(*) FROM snapshots").fetchone()[0] == 12


def test_compaction_rollback_rejects_same_owner_main_substitution_without_overwrite(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path, _private_payload = _seed_compaction_fixture(
        tmp_path,
        snapshot_rows=12,
    )
    backup_path = tmp_path / "rollback-authority-backup.db"
    substitute_path = tmp_path / "valid-substitute.db"
    displaced_source = tmp_path / "mutated-selected-source.db"
    init_store(substitute_path)
    with store_sqlite._connect(substitute_path, isolation_level=None) as substitute:
        substitute.execute(
            "CREATE TABLE substitute_sentinel (value TEXT NOT NULL)"
        )
        substitute.execute(
            "INSERT INTO substitute_sentinel VALUES ('must-not-be-overwritten')"
        )
    substitute_stat = substitute_path.stat()
    substitute_digest = hashlib.sha256(substitute_path.read_bytes()).hexdigest()
    original_restore = store_sqlite._restore_verified_compaction_backup
    original_checkpoint = store_sqlite._checkpoint_truncate
    original_publish = store_sqlite.publish_private_sqlite_replacement_at
    restore_calls = 0
    checkpoint_calls: list[None] = []
    publish_calls: list[None] = []

    def substitute_before_restore(*args: Any, **kwargs: Any) -> bool:
        nonlocal restore_calls
        restore_calls += 1
        db_path.rename(displaced_source)
        substitute_path.rename(db_path)
        return original_restore(*args, **kwargs)

    def record_checkpoint(*args: Any, **kwargs: Any) -> bool:
        checkpoint_calls.append(None)
        return original_checkpoint(*args, **kwargs)

    def record_publish(*args: Any, **kwargs: Any) -> Any:
        publish_calls.append(None)
        return original_publish(*args, **kwargs)

    def interrupt_after_retention(phase: str) -> None:
        if phase == "during_replacement":
            raise RuntimeError("private-prepublication-failure")

    monkeypatch.setattr(
        store_sqlite,
        "_restore_verified_compaction_backup",
        substitute_before_restore,
    )
    monkeypatch.setattr(store_sqlite, "_checkpoint_truncate", record_checkpoint)
    monkeypatch.setattr(
        store_sqlite,
        "publish_private_sqlite_replacement_at",
        record_publish,
    )
    before_fds = set(os.listdir("/proc/self/fd"))
    before_threads = {id(thread) for thread in threading.enumerate()}
    before_children = {process.pid for process in multiprocessing.active_children()}

    result = compact_store(
        db_path,
        options=CompactionOptions(
            dry_run=False,
            acknowledge_offline=True,
            backup_path=backup_path,
            snapshot_retention_days=14,
            snapshot_retention_count=1,
            batch_size=2,
        ),
        now="2026-02-01T00:00:00+00:00",
        phase_hook=interrupt_after_retention,
    )

    assert result["status"] == "rollback_failed"
    assert result["rollback"] == {"status": "failed"}
    assert result["snapshots"]["deleted"] == 10
    assert restore_calls == 1
    assert checkpoint_calls == [None]
    assert publish_calls == []
    assert backup_path.is_file()
    current = db_path.stat()
    assert (current.st_dev, current.st_ino) == (
        substitute_stat.st_dev,
        substitute_stat.st_ino,
    )
    assert hashlib.sha256(db_path.read_bytes()).hexdigest() == substitute_digest
    verification = sqlite3.connect(
        f"file:{db_path}?mode=ro&immutable=1",
        uri=True,
    )
    try:
        assert verification.execute(
            "SELECT value FROM substitute_sentinel"
        ).fetchone() == ("must-not-be-overwritten",)
        assert verification.execute(
            "SELECT COUNT(*) FROM snapshots"
        ).fetchone() == (0,)
    finally:
        verification.close()
    assert displaced_source.is_file()
    assert not any(
        path.name.startswith(".tendwire-sqlite-")
        for path in tmp_path.iterdir()
    )
    assert set(os.listdir("/proc/self/fd")) == before_fds
    assert {id(thread) for thread in threading.enumerate()} == before_threads
    assert {process.pid for process in multiprocessing.active_children()} == before_children


def test_compact_store_cleans_vacuum_output_when_sqlite_raises_after_creation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path, _private_payload = _seed_compaction_fixture(tmp_path)
    backup_path = tmp_path / "vacuum-failure-backup.db"
    original_connect = store_sqlite._connect

    class RaiseAfterVacuum:
        def __init__(self, connection: sqlite3.Connection) -> None:
            self.connection = connection

        def __enter__(self) -> "RaiseAfterVacuum":
            self.connection.__enter__()
            return self

        def __exit__(self, *args: Any) -> Any:
            return self.connection.__exit__(*args)

        def __getattr__(self, name: str) -> Any:
            return getattr(self.connection, name)

        def execute(
            self,
            sql: str,
            parameters: Any = (),
        ) -> Any:
            result = self.connection.execute(sql, parameters)
            if sql.lstrip().upper().startswith("VACUUM INTO"):
                raise sqlite3.OperationalError("private-vacuum-failure")
            return result

    def intercept_connect(*args: Any, **kwargs: Any) -> RaiseAfterVacuum:
        return RaiseAfterVacuum(original_connect(*args, **kwargs))

    monkeypatch.setattr(store_sqlite, "_connect", intercept_connect)
    result = compact_store(
        db_path,
        options=CompactionOptions(
            dry_run=False,
            acknowledge_offline=True,
            backup_path=backup_path,
            snapshot_retention_days=14,
            snapshot_retention_count=8,
            batch_size=2,
        ),
        now="2026-02-01T00:00:00+00:00",
    )

    assert result["status"] == "rollback_completed"
    assert result["rollback"] == {"status": "completed"}
    assert backup_path.is_file()
    assert not any(
        path.name.startswith(".tendwire-sqlite-")
        for path in tmp_path.iterdir()
    )
    _assert_compaction_logical_evidence(db_path)


def test_v7_to_current_conservatively_classifies_legacy_final_and_preserves_state(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "complete-preservation.db"
    backup_path = tmp_path / "complete-preservation-backup.db"
    init_store(db_path)
    old_rows = [
        (
            host_id,
            f"2025-12-{sequence + 1:02d}T00:00:00+00:00",
            f"old-{host_id}-{sequence}",
            json.dumps({"old": sequence, "host_id": host_id}, sort_keys=True),
        )
        for host_id in ("host-a", "host-b")
        for sequence in range(3)
    ]
    with sqlite3.connect(str(db_path)) as conn:
        conn.executemany(
            """
            INSERT INTO snapshots (
                host_id, created_at, content_fingerprint, payload
            ) VALUES (?, ?, ?, ?)
            """,
            old_rows,
        )

    host_a_config = Config(host_id="host-a", db_path=db_path)
    host_b_config = Config(host_id="host-b", db_path=db_path)
    host_a_snapshot = project_from_raw(
        host_a_config,
        spaces=[{"id": "space-a", "name": "Space A", "status": "active"}],
        workers=[
            {
                "id": "worker-1",
                "name": "Worker One",
                "status": "pending",
                "space_id": "space-a",
                "summary": "human approval required before continuing",
            }
        ],
        backend_health=[
            {
                "name": "herdr",
                "status": "healthy",
                "outcome": "healthy_non_empty",
                "observed_at": "2026-01-31T00:00:00+00:00",
                "counts": {"workers": 1},
            }
        ],
        timestamp=datetime.fromisoformat("2026-01-31T00:00:00+00:00"),
    )
    host_b_snapshot = project_from_raw(
        host_b_config,
        spaces=[{"id": "space-b", "name": "Space B", "status": "active"}],
        workers=[
            {
                "id": "worker-b",
                "name": "Worker B",
                "status": "active",
                "space_id": "space-b",
            }
        ],
        backend_health=[
            {
                "name": "herdr",
                "status": "healthy",
                "outcome": "healthy_non_empty",
                "observed_at": "2026-01-31T00:01:00+00:00",
                "counts": {"workers": 1},
            }
        ],
        timestamp=datetime.fromisoformat("2026-01-31T00:01:00+00:00"),
    )
    _save_observation(
        db_path,
        host_a_snapshot,
        "positive",
        "2026-01-31T00:00:00+00:00",
    )
    _save_observation(
        db_path,
        host_b_snapshot,
        "positive",
        "2026-01-31T00:01:00+00:00",
    )
    assert merge_turn_content(
        db_path,
        "host-a",
        "worker-1",
        {
            "user_text": "Preserve this prompt.",
            "assistant_final_text": "Preserve this final.",
            "complete": True,
            "has_open_turn": False,
            "source_turn_id": "complete-preservation-turn",
        },
        observed_at="2026-01-31T00:02:00+00:00",
    ) == 1
    assert upsert_worker_bindings(
        db_path,
        [
            _worker_binding(
                observed_at="2026-01-31T00:00:00+00:00",
                expires_at="2027-01-31T00:00:00+00:00",
            )
        ],
    ) == 1
    reserved = reserve_command_request(
        db_path,
        host_id="host-a",
        request_id="preserved-request",
        action="send_instruction",
        canonical_version=1,
        canonical_fingerprint="preserved-command-fingerprint",
        canonical_request_json='{"action":"send_instruction"}',
        public_worker_id="worker-1",
        pending_result_json='{"status":"pending"}',
        now="2026-01-31T00:02:01+00:00",
    )
    assert reserved["status"] == "reserved"
    started = mark_command_send_started(
        db_path,
        host_id="host-a",
        request_id="preserved-request",
        canonical_fingerprint="preserved-command-fingerprint",
        owner_token=reserved["owner_token"],
        binding_fingerprint="preserved-private-binding",
        now="2026-01-31T00:02:02+00:00",
    )
    finish_command_request(
        db_path,
        host_id="host-a",
        request_id="preserved-request",
        canonical_fingerprint="preserved-command-fingerprint",
        owner_token=started["owner_token"],
        expected_state="send_started",
        terminal_state="accepted",
        status=STATUS_ACCEPTED,
        result_json='{"status":"accepted","result":"preserved"}',
        now="2026-01-31T00:02:03+00:00",
    )
    assert store_sqlite.merge_backend_pending(
        db_path,
        "host-a",
        "worker-1",
        {"kind": "approval", "safe": "preserved"},
    )
    leased = poll_connector_outbox(
        db_path,
        "host-a",
        "attention",
        now="2026-01-31T00:03:00+00:00",
    )
    assert len(leased["items"]) == 1

    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO pending_interactions (
                host_id, pending_id, worker_id, worker_fingerprint, space_id,
                kind, status, updated_at, fingerprint,
                snapshot_content_fingerprint, observed_at, payload_json
            ) VALUES (
                'host-a', 'durable-pending', 'worker-1', 'worker-fingerprint',
                'space-a', 'approval', 'pending',
                '2026-01-31T00:00:00+00:00', 'pending-fingerprint',
                ?, '2026-01-31T00:00:00+00:00',
                '{"kind":"approval","safe":"preserved"}'
            )
            """,
            (host_a_snapshot.content_fingerprint,),
        )
        conn.execute(
            "CREATE TABLE unrelated_preservation_sentinel (value TEXT NOT NULL)"
        )
        conn.execute(
            "INSERT INTO unrelated_preservation_sentinel VALUES ('preserved')"
        )
        conn.execute("DROP TABLE store_maintenance_state")
        conn.execute("DROP INDEX idx_snapshots_host_newest")
        conn.execute("DROP INDEX idx_snapshots_created_host_id")
        conn.execute(
            "CREATE INDEX idx_snapshots_host_id ON snapshots(host_id)"
        )
        conn.execute(
            "CREATE INDEX idx_snapshots_created_at ON snapshots(created_at)"
        )
        conn.execute(
            """
            CREATE INDEX idx_snapshots_content_fingerprint
            ON snapshots(content_fingerprint)
            """
        )
        conn.execute("PRAGMA user_version = 7")

    preserved_tables = (
        "commands",
        "command_receipts",
        "worker_bindings",
        "pending_interactions",
        "backend_pending",
        "attention_items",
        "attention_lifecycles",
        "spaces",
        "workers",
        "turns",
        "turn_content_revisions",
        "turn_content_page_boundaries",
        "backend_health",
        "connector_outbox",
        "connector_deliveries",
        "unrelated_preservation_sentinel",
    )

    def logical_evidence() -> dict[str, Any]:
        with sqlite3.connect(str(db_path)) as conn:
            table_rows = {
                table: tuple(
                    sorted(
                        (
                            tuple(row)
                            for row in conn.execute(
                                f"SELECT * FROM {table}"
                            ).fetchall()
                        ),
                        key=repr,
                    )
                )
                for table in preserved_tables
            }
            latest = tuple(
                conn.execute(
                    """
                    SELECT snapshot.host_id, snapshot.content_fingerprint,
                           snapshot.payload
                    FROM snapshots AS snapshot
                    WHERE snapshot.id = (
                        SELECT MAX(newest.id)
                        FROM snapshots AS newest
                        WHERE newest.host_id = snapshot.host_id
                    )
                    ORDER BY snapshot.host_id
                    """
                ).fetchall()
            )
            snapshot_count = int(
                conn.execute("SELECT COUNT(*) FROM snapshots").fetchone()[0]
            )
            return {
                "tables": table_rows,
                "latest": latest,
                "snapshot_count": snapshot_count,
                "integrity": conn.execute(
                    "PRAGMA integrity_check"
                ).fetchone()[0],
                "foreign_keys": conn.execute(
                    "PRAGMA foreign_key_check"
                ).fetchall(),
            }

    before_migration = logical_evidence()
    assert all(before_migration["tables"][table] for table in preserved_tables)
    assert before_migration["snapshot_count"] == 8
    assert before_migration["integrity"] == "ok"
    assert before_migration["foreign_keys"] == []

    init_store(db_path)
    after_migration = logical_evidence()
    for table in preserved_tables:
        if table != "connector_outbox":
            assert after_migration["tables"][table] == before_migration["tables"][table]
    assert after_migration["latest"] == before_migration["latest"]
    assert after_migration["snapshot_count"] == before_migration["snapshot_count"]
    with sqlite3.connect(str(db_path)) as conn:
        legacy_final = conn.execute(
            """
            SELECT delivery_kind, status, payload_json
            FROM connector_outbox
            WHERE host_id = 'host-a' AND connector = 'turn-final'
            """
        ).fetchone()
    assert legacy_final is not None
    assert legacy_final[:2] == ("final_migration_hold", "dead_letter")
    assert json.loads(legacy_final[2])["operation"] == "materialize"
    assert poll_connector_outbox(
        db_path,
        "host-a",
        "turn-final",
        now="2026-01-31T00:04:00+00:00",
    )["items"] == []
    with sqlite3.connect(str(db_path)) as conn:
        assert _user_version(conn) == store_sqlite.STORE_SCHEMA_VERSION
        assert conn.execute(
            "SELECT scope FROM store_maintenance_state"
        ).fetchone() == ("automatic",)

    while True:
        retention = store_sqlite.cleanup_snapshot_retention(
            db_path,
            retention_days=14,
            retention_count=1,
            batch_size=2,
            now="2026-02-01T00:00:00+00:00",
        )
        if not retention["remaining_candidates"]:
            break
    after_retention = logical_evidence()
    assert after_retention["tables"] == after_migration["tables"]
    assert after_retention["latest"] == after_migration["latest"]
    assert after_retention["snapshot_count"] == 2
    assert after_retention["integrity"] == "ok"
    assert after_retention["foreign_keys"] == []

    compacted = compact_store(
        db_path,
        options=CompactionOptions(
            dry_run=False,
            acknowledge_offline=True,
            backup_path=backup_path,
            snapshot_retention_days=14,
            snapshot_retention_count=1,
            batch_size=2,
        ),
        now="2026-02-01T00:00:00+00:00",
    )
    after_compaction = logical_evidence()
    assert compacted["status"] == "completed"
    assert compacted["ok"] is True
    assert after_compaction == after_retention
    assert backup_path.is_file()


def test_store_v8_to_v9_backfills_host_local_sequences_and_paging_indexes(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "turn-v8-to-v9.db"
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            """
            CREATE TABLE turns (
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
                PRIMARY KEY (host_id, turn_id)
            )
            """
        )
        conn.execute("PRAGMA user_version=8")
        rows = (
            ("host-a", "turn-c", "worker-a", "2026-01-02T00:00:00+00:00"),
            ("host-a", "turn-b", "worker-a", "2026-01-01T00:00:00+00:00"),
            ("host-a", "turn-a", "worker-a", "2026-01-01T00:00:00+00:00"),
            ("host-b", "turn-z", "worker-z", "2026-01-03T00:00:00+00:00"),
        )
        for host_id, turn_id, worker_id, observed_at in rows:
            conn.execute(
                """
                INSERT INTO turns (
                    host_id, turn_id, worker_id, worker_fingerprint, space_id,
                    status, kind, updated_at, fingerprint,
                    snapshot_content_fingerprint, observed_at, payload_json
                ) VALUES (?, ?, ?, NULL, NULL, 'active', 'task', ?, '', '', ?, '{}')
                """,
                (host_id, turn_id, worker_id, observed_at, observed_at),
            )
        conn.commit()

    init_store(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        first = conn.execute(
            """
            SELECT host_id, turn_id, list_sequence
            FROM turns
            ORDER BY host_id, list_sequence
            """
        ).fetchall()
        indexes = {
            str(row[1]): tuple(
                str(column[2])
                for column in conn.execute(
                    f"PRAGMA index_info({row[1]})"
                ).fetchall()
            )
            for row in conn.execute("PRAGMA index_list(turns)").fetchall()
        }
        epoch = conn.execute(
            "SELECT store_epoch FROM turn_list_state WHERE scope = 'turn-list'"
        ).fetchone()[0]
        host_states = conn.execute(
            """
            SELECT host_id, next_sequence, traversal_generation
            FROM turn_list_hosts
            ORDER BY host_id
            """
        ).fetchall()
        assert _user_version(conn) == store_sqlite.STORE_SCHEMA_VERSION
    init_store(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        second = conn.execute(
            """
            SELECT host_id, turn_id, list_sequence
            FROM turns
            ORDER BY host_id, list_sequence
            """
        ).fetchall()
        second_epoch = conn.execute(
            "SELECT store_epoch FROM turn_list_state WHERE scope = 'turn-list'"
        ).fetchone()[0]
        second_host_states = conn.execute(
            """
            SELECT host_id, next_sequence, traversal_generation
            FROM turn_list_hosts
            ORDER BY host_id
            """
        ).fetchall()

    assert first == [
        ("host-a", "turn-a", 1),
        ("host-a", "turn-b", 2),
        ("host-a", "turn-c", 3),
        ("host-b", "turn-z", 1),
    ]
    assert second == first
    assert second_epoch == epoch
    assert host_states == second_host_states == [
        ("host-a", 4, 1),
        ("host-b", 2, 1),
    ]
    assert indexes["ux_turns_host_list_sequence"] == ("host_id", "list_sequence")
    assert indexes["idx_turns_host_worker_list_sequence"] == (
        "host_id",
        "worker_id",
        "list_sequence",
        "turn_id",
    )


def test_all_api_turn_insertions_allocate_unique_immutable_sequences_concurrently(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "turn-sequence-concurrency.db"
    host_id = "sequence-host"
    snapshot = project_from_raw(
        Config(host_id=host_id, db_path=db_path),
        workers=[{"id": "worker-1", "name": "Worker", "status": "active"}],
    )
    init_store(db_path)
    save_snapshot(db_path, snapshot)
    worker = snapshot.workers[0]
    barrier = threading.Barrier(8)
    failures: list[BaseException] = []

    def insert_command(index: int) -> None:
        try:
            barrier.wait(timeout=10)
            assert store_sqlite.upsert_command_pending_turn(
                db_path,
                host_id,
                worker,
                request_id=f"request-{index}",
                instruction_text=f"instruction {index}",
                observed_at=f"2099-01-01T00:00:{index:02d}+00:00",
            )
        except BaseException as exc:
            failures.append(exc)

    threads = [
        threading.Thread(target=insert_command, args=(index,))
        for index in range(8)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=20)

    assert not failures
    assert not any(thread.is_alive() for thread in threads)
    with sqlite3.connect(str(db_path)) as conn:
        before = conn.execute(
            """
            SELECT turn_id, list_sequence
            FROM turns
            WHERE host_id = ?
            ORDER BY list_sequence
            """,
            (host_id,),
        ).fetchall()
    assert len(before) == 9
    assert [row[1] for row in before] == list(range(1, 10))
    save_snapshot(db_path, snapshot)
    with sqlite3.connect(str(db_path)) as conn:
        after = conn.execute(
            """
            SELECT turn_id, list_sequence
            FROM turns
            WHERE host_id = ?
            ORDER BY list_sequence
            """,
            (host_id,),
        ).fetchall()
    assert after == before


def test_turn_list_pagination_is_insert_stable_and_since_discovers_only_new_rows(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "turn-list-stable.db"
    host_id = "page-host"
    snapshot = project_from_raw(
        Config(host_id=host_id, db_path=db_path),
        workers=[
            {"id": f"worker-{index}", "name": f"Worker {index}", "status": "active"}
            for index in range(5)
        ],
    )
    init_store(db_path)
    save_snapshot(db_path, snapshot)
    first = turns_payload_from_store(
        db_path,
        host_id,
        limit=2,
        now=1_000,
    )
    with sqlite3.connect(str(db_path)) as conn:
        captured_ids = {
            str(row[0])
            for row in conn.execute(
                "SELECT turn_id FROM turns WHERE host_id = ?",
                (host_id,),
            ).fetchall()
        }
    assert merge_turn_content(
        db_path,
        host_id,
        snapshot.workers[0].id,
        {
            "source_turn_id": "new-source",
            "assistant_stream_text": "new working turn",
            "complete": False,
            "has_open_turn": True,
        },
        observed_at="2099-01-01T00:00:00+00:00",
    ) == 1

    traversed = [str(turn["id"]) for turn in first["turns"]]
    cursor = first["next_cursor"]
    while cursor is not None:
        page = turns_payload_from_store(
            db_path,
            host_id,
            limit=2,
            cursor=cursor,
            now=1_001,
        )
        assert page.get("ok") is not False
        traversed.extend(str(turn["id"]) for turn in page["turns"])
        cursor = page["next_cursor"]

    fresh = turns_payload_from_store(db_path, host_id, limit=250, now=1_001)
    discovered = turns_payload_from_store(
        db_path,
        host_id,
        limit=250,
        since=first["since"],
        now=1_001,
    )
    assert len(traversed) == len(set(traversed))
    assert set(traversed) == captured_ids
    assert len(fresh["turns"]) == len(captured_ids) + 1
    assert len(discovered["turns"]) == 1
    assert discovered["turns"][0]["source_turn_id"]
    assert discovered["as_of"] == discovered["since"]


def test_turn_list_tokens_distinguish_invalid_cursor_cursor_expiry_and_since_expiry(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "turn-list-expiry.db"
    snapshot = project_from_raw(
        Config(host_id="expiry-host", db_path=db_path),
        workers=[
            {"id": "worker-a", "name": "A", "status": "active"},
            {"id": "worker-b", "name": "B", "status": "active"},
        ],
    )
    init_store(db_path)
    save_snapshot(db_path, snapshot)
    first = turns_payload_from_store(
        db_path,
        "expiry-host",
        limit=1,
        now=100,
    )
    cursor = first["next_cursor"]
    assert cursor

    tampered = cursor[:-1] + ("A" if cursor[-1] != "A" else "B")
    assert turns_payload_from_store(
        db_path,
        "expiry-host",
        limit=1,
        cursor=tampered,
        now=101,
    )["status"] == "invalid_cursor"
    assert turns_payload_from_store(
        db_path,
        "other-host",
        limit=1,
        cursor=cursor,
        now=101,
    )["status"] == "invalid_cursor"
    assert turns_payload_from_store(
        db_path,
        "expiry-host",
        limit=1,
        cursor=cursor,
        now=1_000,
    )["status"] == "cursor_expired"

    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            "UPDATE turn_list_state SET store_epoch = 'replacement-epoch'"
        )
    assert turns_payload_from_store(
        db_path,
        "expiry-host",
        since=first["since"],
        now=101,
    )["status"] == "since_expired"


def test_turn_list_pages_remain_below_frame_cap_for_over_one_mib_logical_list(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "turn-list-large.db"
    host_id = "large-list-host"
    init_store(db_path)
    with store_sqlite._connect(db_path, isolation_level=None) as conn:
        conn.execute("BEGIN IMMEDIATE")
        for index in range(300):
            item = store_sqlite.Turn(
                host_id=host_id,
                worker_id=f"worker-{index:04d}",
                status="active",
                kind="task",
                assistant_final_text=f"{index:04d}-" + ("x" * 6_000),
                complete=True,
                has_open_turn=False,
            ).to_dict()
            conn.execute(
                """
                INSERT INTO turns (
                    host_id, turn_id, worker_id, worker_fingerprint, space_id,
                    status, kind, updated_at, fingerprint,
                    snapshot_content_fingerprint, observed_at, payload_json,
                    list_sequence
                ) VALUES (?, ?, ?, NULL, NULL, 'active', 'task', ?, ?, '', ?, ?, ?)
                """,
                (
                    host_id,
                    item["id"],
                    item["worker_id"],
                    f"2099-01-01T00:{index // 60:02d}:{index % 60:02d}+00:00",
                    item["fingerprint"],
                    f"2099-01-01T00:{index // 60:02d}:{index % 60:02d}+00:00",
                    json.dumps(item, sort_keys=True),
                    index + 1,
                ),
            )
        conn.commit()

    ids: list[str] = []
    total_turn_bytes = 0
    cursor: str | None = None
    while True:
        page = turns_payload_from_store(
            db_path,
            host_id,
            limit=250,
            cursor=cursor,
            now=1_000,
        )
        encoded = json.dumps(
            page,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        ).encode("utf-8")
        assert len(encoded) < 1024 * 1024
        ids.extend(str(turn["id"]) for turn in page["turns"])
        total_turn_bytes += sum(
            len(str(turn.get("assistant_final_text") or "").encode("utf-8"))
            for turn in page["turns"]
        )
        cursor = page["next_cursor"]
        if cursor is None:
            break

    assert len(ids) == len(set(ids)) == 300
    assert total_turn_bytes > 1024 * 1024


def test_same_source_completion_is_observation_monotonic_and_never_reopens(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "turn-monotonic.db"
    host_id = "monotonic-host"
    worker_id = "worker-1"
    snapshot = project_from_raw(
        Config(host_id=host_id, db_path=db_path),
        workers=[{"id": worker_id, "name": "Worker", "status": "active"}],
    )
    init_store(db_path)
    save_snapshot(db_path, snapshot)
    source = "same-source"
    assert merge_turn_content(
        db_path,
        host_id,
        worker_id,
        {
            "source_turn_id": source,
            "assistant_final_text": "authoritative final",
            "complete": True,
            "has_open_turn": False,
        },
        observed_at="2030-01-01T00:00:00+00:00",
    ) == 1
    with sqlite3.connect(str(db_path)) as conn:
        first_revision = conn.execute(
            """
            SELECT revisions.content_revision
            FROM turns
            JOIN turn_content_revisions AS revisions
              ON revisions.host_id = turns.host_id
             AND revisions.turn_id = turns.turn_id
             AND revisions.is_current = 1
            WHERE turns.host_id = ?
              AND json_extract(turns.payload_json, '$.source_turn_id') IS NOT NULL
            """,
            (host_id,),
        ).fetchone()[0]
    for observed_at in (
        "2029-12-31T23:59:59+00:00",
        "2030-01-01T00:00:00+00:00",
    ):
        assert merge_turn_content(
            db_path,
            host_id,
            worker_id,
            {
                "source_turn_id": source,
                "assistant_stream_text": "late working",
                "complete": False,
                "has_open_turn": True,
            },
            observed_at=observed_at,
        ) == 0
    assert merge_turn_content(
        db_path,
        host_id,
        worker_id,
        {
            "source_turn_id": source,
            "assistant_final_text": "revised final",
            "assistant_stream_text": "must clear",
            "complete": True,
            "has_open_turn": True,
        },
        observed_at="2030-01-01T00:00:01+00:00",
    ) == 1
    payload = turns_payload_from_store(
        db_path,
        host_id,
        schema_version=2,
        claim_hard_ttl_seconds=1_000_000_000,
    )
    source_turn = next(turn for turn in payload["turns"] if turn.get("source_turn_id"))
    assert source_turn["assistant_final_text"] == "revised final"
    assert source_turn["assistant_stream_text"] is None
    assert source_turn["complete"] is True
    assert source_turn["has_open_turn"] is False
    with sqlite3.connect(str(db_path)) as conn:
        revisions = conn.execute(
            """
            SELECT content_revision, is_current
            FROM turn_content_revisions
            WHERE host_id = ? AND turn_id = ?
            ORDER BY is_current
            """,
            (host_id, source_turn["id"]),
        ).fetchall()
    assert len(revisions) == 2
    assert revisions[0][0] == first_revision
    assert revisions[0][1] == 0
    assert revisions[1][1] == 1

    assert merge_turn_content(
        db_path,
        host_id,
        worker_id,
        {
            "source_turn_id": "different-source",
            "assistant_stream_text": "independent working",
            "complete": False,
            "has_open_turn": True,
        },
        observed_at="2030-01-01T00:00:02+00:00",
    ) == 1
    refreshed = turns_payload_from_store(db_path, host_id)
    assert any(
        turn.get("source_turn_id") != source_turn["source_turn_id"]
        and turn["has_open_turn"] is True
        for turn in refreshed["turns"]
    )


def test_apply_turn_refresh_rolls_back_turn_pending_and_rejects_stale_binding(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "turn-refresh-atomic.db"
    host_id = "atomic-host"
    snapshot = project_from_raw(
        Config(host_id=host_id, db_path=db_path),
        workers=[{"id": "worker-1", "name": "Worker", "status": "active"}],
    )
    init_store(db_path)
    save_snapshot(db_path, snapshot)
    worker = snapshot.workers[0]
    binding = WorkerBinding(
        host_id=host_id,
        worker_id=worker.id,
        worker_fingerprint=worker.fingerprint,
        backend="herdr",
        target_kind="pane_id",
        target_value="private-pane",
        turn_target_kind="pane_id",
        turn_target_value="private-pane",
    )
    upsert_worker_bindings(db_path, [binding])
    original_pending_apply = store_sqlite._merge_backend_pending_conn

    def fail_pending(*args: Any, **kwargs: Any) -> bool:
        raise RuntimeError("controlled pending failure")

    monkeypatch.setattr(store_sqlite, "_merge_backend_pending_conn", fail_pending)
    with pytest.raises(RuntimeError, match="controlled pending failure"):
        store_sqlite.apply_turn_refresh(
            db_path,
            host_id,
            worker.id,
            {"assistant_final_text": "must roll back", "complete": True},
            backend_pending={"question": "must roll back"},
            expected_binding=binding,
            observed_at="2099-01-01T00:00:00+00:00",
        )
    assert not store_sqlite.list_backend_pending(db_path, host_id)
    assert all(
        turn.get("assistant_final_text") in (None, "")
        for turn in turns_payload_from_store(db_path, host_id)["turns"]
    )

    monkeypatch.setattr(
        store_sqlite,
        "_merge_backend_pending_conn",
        original_pending_apply,
    )
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            """
            UPDATE worker_bindings
            SET turn_target_value = 'changed-private-pane'
            WHERE host_id = ? AND private_fingerprint = ?
            """,
            (host_id, binding.private_fingerprint),
        )
    stale = store_sqlite.apply_turn_refresh(
        db_path,
        host_id,
        worker.id,
        {"assistant_final_text": "stale result", "complete": True},
        backend_pending={"question": "stale pending"},
        expected_binding=binding,
        observed_at="2099-01-01T00:00:01+00:00",
    )
    assert stale == store_sqlite.TurnRefreshApplyResult(0, False, True)
    assert not store_sqlite.list_backend_pending(db_path, host_id)
    assert all(
        turn.get("assistant_final_text") in (None, "")
        for turn in turns_payload_from_store(db_path, host_id)["turns"]
    )


def test_turn_list_query_plan_uses_bounded_composite_index(tmp_path: Path) -> None:
    db_path = tmp_path / "turn-list-plan.db"
    snapshot = project_from_raw(
        Config(host_id="plan-host", db_path=db_path),
        workers=[{"id": "worker-1", "name": "Worker", "status": "active"}],
    )
    init_store(db_path)
    save_snapshot(db_path, snapshot)
    with sqlite3.connect(str(db_path)) as conn:
        plan = conn.execute(
            """
            EXPLAIN QUERY PLAN
            SELECT turn_id
            FROM turns
            WHERE host_id = ?
              AND list_sequence > ?
              AND list_sequence <= ?
            ORDER BY worker_id ASC, list_sequence DESC, turn_id ASC
            LIMIT ?
            """,
            ("plan-host", 0, 100, 101),
        ).fetchall()
    assert "idx_turns_host_worker_list_sequence" in " ".join(
        str(row[3]) for row in plan
    )


def test_merge_begin_immediate_barrier_forces_late_writer_to_read_committed_final(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "turn-merge-barrier.db"
    host_id = "barrier-host"
    worker_id = "worker-1"
    snapshot = project_from_raw(
        Config(host_id=host_id, db_path=db_path),
        workers=[{"id": worker_id, "name": "Worker", "status": "active"}],
    )
    init_store(db_path)
    save_snapshot(db_path, snapshot)
    original_read = store_sqlite._current_turn_content_rows_conn
    first_has_lock = threading.Event()
    release_first = threading.Event()
    second_read_base = threading.Event()
    results: dict[str, int] = {}

    def barrier_read(
        conn: sqlite3.Connection,
        selected_host: str,
        selected_worker: str,
    ) -> Any:
        if threading.current_thread().name == "final-writer":
            first_has_lock.set()
            assert release_first.wait(timeout=10)
        elif threading.current_thread().name == "late-working-writer":
            second_read_base.set()
        return original_read(conn, selected_host, selected_worker)

    monkeypatch.setattr(
        store_sqlite,
        "_current_turn_content_rows_conn",
        barrier_read,
    )

    def write_final() -> None:
        results["final"] = merge_turn_content(
            db_path,
            host_id,
            worker_id,
            {
                "source_turn_id": "barrier-source",
                "assistant_final_text": "committed final",
                "complete": True,
                "has_open_turn": False,
            },
            observed_at="2030-01-01T00:00:01+00:00",
        )

    def write_late_working() -> None:
        results["working"] = merge_turn_content(
            db_path,
            host_id,
            worker_id,
            {
                "source_turn_id": "barrier-source",
                "assistant_stream_text": "late working",
                "complete": False,
                "has_open_turn": True,
            },
            observed_at="2030-01-01T00:00:00+00:00",
        )

    final_thread = threading.Thread(target=write_final, name="final-writer")
    working_thread = threading.Thread(
        target=write_late_working,
        name="late-working-writer",
    )
    final_thread.start()
    assert first_has_lock.wait(timeout=10)
    working_thread.start()
    assert not second_read_base.wait(timeout=0.2)
    release_first.set()
    final_thread.join(timeout=10)
    working_thread.join(timeout=10)

    assert not final_thread.is_alive()
    assert not working_thread.is_alive()
    assert second_read_base.is_set()
    assert results == {"final": 1, "working": 0}
    payload = turns_payload_from_store(db_path, host_id, schema_version=2)
    source_turn = next(turn for turn in payload["turns"] if turn.get("source_turn_id"))
    assert source_turn["assistant_final_text"] == "committed final"
    assert source_turn["assistant_stream_text"] is None
    assert source_turn["complete"] is True
    assert source_turn["has_open_turn"] is False
    with sqlite3.connect(str(db_path)) as conn:
        assert conn.execute(
            """
            SELECT COUNT(*)
            FROM turn_content_revisions
            WHERE host_id = ? AND turn_id = ? AND is_current = 1
            """,
            (host_id, source_turn["id"]),
        ).fetchone()[0] == 1


def test_apply_turn_refresh_deadline_while_writer_locked_never_commits_later(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "turn-refresh-deadline.db"
    host_id = "deadline-host"
    worker_id = "worker-1"
    snapshot = project_from_raw(
        Config(host_id=host_id, db_path=db_path),
        workers=[{"id": worker_id, "name": "Worker", "status": "active"}],
    )
    init_store(db_path)
    save_snapshot(db_path, snapshot)
    blocker = store_sqlite._connect(db_path, isolation_level=None)
    try:
        blocker.execute("BEGIN IMMEDIATE")
        started = time.monotonic()
        result = store_sqlite.apply_turn_refresh(
            db_path,
            host_id,
            worker_id,
            {"assistant_final_text": "must never commit", "complete": True},
            backend_pending={"question": "must never persist"},
            deadline_monotonic=started + 0.15,
            observed_at="2099-01-01T00:00:00+00:00",
        )
        elapsed = time.monotonic() - started
        assert result == store_sqlite.TurnRefreshApplyResult(
            0,
            False,
            False,
            True,
        )
        assert elapsed < 1.0
    finally:
        blocker.rollback()
        blocker.close()

    assert not store_sqlite.list_backend_pending(db_path, host_id)
    assert all(
        turn.get("assistant_final_text") in (None, "")
        for turn in turns_payload_from_store(db_path, host_id)["turns"]
    )
    cancelled = store_sqlite.apply_turn_refresh(
        db_path,
        host_id,
        worker_id,
        {"assistant_final_text": "also must not commit", "complete": True},
        cancelled=lambda: True,
        observed_at="2099-01-01T00:00:01+00:00",
    )
    assert cancelled.cancelled is True
    assert all(
        turn.get("assistant_final_text") in (None, "")
        for turn in turns_payload_from_store(db_path, host_id)["turns"]
    )
    apply_checks = 0

    def cancel_apply_before_commit() -> bool:
        nonlocal apply_checks
        apply_checks += 1
        return apply_checks == 3

    precommit_cancelled = store_sqlite.apply_turn_refresh(
        db_path,
        host_id,
        worker_id,
        {"assistant_final_text": "rollback at commit seam", "complete": True},
        backend_pending={"question": "rollback at commit seam"},
        cancelled=cancel_apply_before_commit,
        observed_at="2099-01-01T00:00:02+00:00",
    )
    assert precommit_cancelled.cancelled is True
    assert apply_checks == 3
    assert not store_sqlite.list_backend_pending(db_path, host_id)
    assert all(
        turn.get("assistant_final_text") in (None, "")
        for turn in turns_payload_from_store(db_path, host_id)["turns"]
    )


def test_prune_backend_pending_deadline_while_writer_locked_is_non_mutating(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "pending-prune-deadline.db"
    init_store(db_path)
    assert store_sqlite.merge_backend_pending(
        db_path,
        "prune-host",
        "orphan-worker",
        {"question": "still present"},
    )
    blocker = store_sqlite._connect(db_path, isolation_level=None)
    try:
        blocker.execute("BEGIN IMMEDIATE")
        started = time.monotonic()
        assert store_sqlite.prune_backend_pending(
            db_path,
            "prune-host",
            (),
            deadline_monotonic=started + 0.15,
        ) == 0
        assert time.monotonic() - started < 1.0
    finally:
        blocker.rollback()
        blocker.close()

    assert "orphan-worker" in store_sqlite.list_backend_pending(
        db_path,
        "prune-host",
    )
    prune_checks = 0

    def cancel_prune_before_commit() -> bool:
        nonlocal prune_checks
        prune_checks += 1
        return prune_checks == 3

    assert store_sqlite.prune_backend_pending(
        db_path,
        "prune-host",
        (),
        cancelled=cancel_prune_before_commit,
    ) == 0
    assert prune_checks == 3
    assert "orphan-worker" in store_sqlite.list_backend_pending(
        db_path,
        "prune-host",
    )
    assert store_sqlite.prune_backend_pending(
        db_path,
        "prune-host",
        (),
    ) == 1
    assert not store_sqlite.list_backend_pending(db_path, "prune-host")


def test_turn_list_filtered_rows_advance_bounded_cursor_without_hiding_public_rows(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "turn-list-filtered-cursor.db"
    host_id = "filtered-host"
    init_store(db_path)
    internal_payload = json.dumps(
        {
            "host_id": host_id,
            "worker_id": "worker-1",
            "status": "active",
            "kind": "task",
            "user_text": "Acme job\n\nTemplate: review-lead\nTemplate instructions:",
            "assistant_final_text": '{"acme_result":{"decision":"approved"}}',
        },
        sort_keys=True,
    )
    public_payload = json.dumps(
        {
            "host_id": host_id,
            "worker_id": "worker-1",
            "status": "active",
            "kind": "task",
            "user_text": "Public prompt",
            "assistant_final_text": "Public answer",
        },
        sort_keys=True,
    )
    with sqlite3.connect(str(db_path)) as conn:
        for sequence in (4, 3, 2):
            conn.execute(
                """
                INSERT INTO turns (
                    host_id, turn_id, worker_id, status, kind, updated_at,
                    fingerprint, snapshot_content_fingerprint, observed_at,
                    payload_json, list_sequence
                ) VALUES (?, ?, 'worker-1', 'active', 'task', '', '', '', '', ?, ?)
                """,
                (
                    host_id,
                    f"internal-{sequence}",
                    internal_payload,
                    sequence,
                ),
            )
        conn.execute(
            """
            INSERT INTO turns (
                host_id, turn_id, worker_id, status, kind, updated_at,
                fingerprint, snapshot_content_fingerprint, observed_at,
                payload_json, list_sequence
            ) VALUES (?, 'public-turn', 'worker-1', 'active', 'task', '', '', '', '', ?, 1)
            """,
            (host_id, public_payload),
        )

    first = turns_payload_from_store(
        db_path,
        host_id,
        limit=2,
        now=1_000,
    )
    assert first["turns"] == []
    assert first["has_more"] is True
    assert first["next_cursor"]
    second = turns_payload_from_store(
        db_path,
        host_id,
        limit=2,
        cursor=first["next_cursor"],
        now=1_001,
    )
    assert [turn["assistant_final_text"] for turn in second["turns"]] == [
        "Public answer"
    ]
    assert second["has_more"] is False
    assert second["next_cursor"] is None


def test_turn_sequence_high_water_never_reuses_deleted_max_and_since_finds_insert(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "turn-sequence-no-reuse.db"
    host_id = "no-reuse-host"
    snapshot = project_from_raw(
        Config(host_id=host_id, db_path=db_path),
        workers=[
            {"id": f"worker-{index}", "name": f"Worker {index}", "status": "active"}
            for index in range(3)
        ],
    )
    init_store(db_path)
    save_snapshot(db_path, snapshot)
    captured = turns_payload_from_store(db_path, host_id, now=1_000)
    with store_sqlite._connect(db_path, isolation_level=None) as conn:
        highest_turn = conn.execute(
            """
            SELECT turn_id
            FROM turns
            WHERE host_id = ?
            ORDER BY list_sequence DESC
            LIMIT 1
            """,
            (host_id,),
        ).fetchone()[0]
        conn.execute("BEGIN IMMEDIATE")
        assert store_sqlite._delete_turn_if_unreferenced_conn(
            conn,
            host_id,
            str(highest_turn),
        )
        conn.commit()
    inserted = store_sqlite.upsert_command_pending_turn(
        db_path,
        host_id,
        snapshot.workers[0],
        request_id="after-delete",
        instruction_text="new logical insertion",
        observed_at="2099-01-01T00:00:00+00:00",
    )
    assert inserted is not None
    with sqlite3.connect(str(db_path)) as conn:
        sequence = conn.execute(
            """
            SELECT list_sequence
            FROM turns
            WHERE host_id = ? AND turn_id = ?
            """,
            (host_id, inserted["id"]),
        ).fetchone()[0]
        state = conn.execute(
            """
            SELECT next_sequence, traversal_generation
            FROM turn_list_hosts
            WHERE host_id = ?
            """,
            (host_id,),
        ).fetchone()
    discovered = turns_payload_from_store(
        db_path,
        host_id,
        since=captured["since"],
        now=1_001,
    )
    assert sequence == 4
    assert state == (5, 2)
    assert [turn["id"] for turn in discovered["turns"]] == [inserted["id"]]
    assert discovered.get("ok") is not False


def test_turn_list_interior_deletion_expires_cursor_but_not_since_watermark(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "turn-list-delete-generation.db"
    host_id = "delete-generation-host"
    snapshot = project_from_raw(
        Config(host_id=host_id, db_path=db_path),
        workers=[
            {"id": f"worker-{index}", "name": f"Worker {index}", "status": "active"}
            for index in range(5)
        ],
    )
    init_store(db_path)
    save_snapshot(db_path, snapshot)
    first = turns_payload_from_store(
        db_path,
        host_id,
        limit=2,
        now=1_000,
    )
    assert first["next_cursor"]
    with store_sqlite._connect(db_path, isolation_level=None) as conn:
        ordered = conn.execute(
            """
            SELECT turn_id
            FROM turns
            WHERE host_id = ?
            ORDER BY worker_id ASC, list_sequence DESC, turn_id ASC
            """,
            (host_id,),
        ).fetchall()
        interior_turn = str(ordered[2][0])
        conn.execute("BEGIN IMMEDIATE")
        assert store_sqlite._delete_turn_if_unreferenced_conn(
            conn,
            host_id,
            interior_turn,
        )
        conn.commit()

    continuation = turns_payload_from_store(
        db_path,
        host_id,
        limit=2,
        cursor=first["next_cursor"],
        now=1_001,
    )
    insertion_poll = turns_payload_from_store(
        db_path,
        host_id,
        since=first["since"],
        now=1_001,
    )
    assert continuation["status"] == "cursor_expired"
    assert insertion_poll.get("ok") is not False
    assert insertion_poll["turns"] == []


def test_stable_owner_snapshot_base_preserves_frozen_id_and_sequence_across_worker_churn(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "stable-owner-snapshot.db"
    host_id = "stable-snapshot-host"
    stable_key = "wsk1_" + ("1" * 64)
    worker_a = Worker(
        id="worker-a",
        name="Worker A",
        status="active",
        space_id="space-a",
        fingerprint="fingerprint-a",
        meta={"stable_key": stable_key, "stable_key_version": 1},
    )
    worker_b = Worker(
        id="worker-b",
        name="Worker B",
        status="waiting",
        space_id="space-b",
        fingerprint="fingerprint-b",
        meta={"stable_key": stable_key, "stable_key_version": 1},
    )
    snapshot_a = Snapshot(
        host_id=host_id,
        updated_at="2026-07-13T00:00:00+00:00",
        workers=[worker_a],
    )
    snapshot_b = Snapshot(
        host_id=host_id,
        updated_at="2026-07-13T00:01:00+00:00",
        workers=[worker_b],
    )
    frozen_turn_id = "turn-744512196ff4efde645035f9"
    frozen_fingerprint = "c835738d6fdd5c5ec1d92b1b"

    init_store(db_path)
    save_snapshot(db_path, snapshot_a)
    with sqlite3.connect(str(db_path)) as conn:
        current_id, raw_payload, list_sequence = conn.execute(
            """
            SELECT turn_id, payload_json, list_sequence
            FROM turns
            WHERE host_id = ?
            """,
            (host_id,),
        ).fetchone()
        assert current_id != frozen_turn_id
        payload = json.loads(str(raw_payload))
        payload["id"] = frozen_turn_id
        payload["fingerprint"] = frozen_fingerprint
        conn.execute(
            "DELETE FROM turn_content_revisions WHERE host_id = ? AND turn_id = ?",
            (host_id, current_id),
        )
        conn.execute(
            """
            UPDATE turns
            SET turn_id = ?, fingerprint = ?, payload_json = ?
            WHERE host_id = ? AND turn_id = ?
            """,
            (
                frozen_turn_id,
                frozen_fingerprint,
                json.dumps(payload, sort_keys=True, separators=(",", ":")),
                host_id,
                current_id,
            ),
        )
        store_sqlite._ensure_absent_turn_content_revision_conn(
            conn,
            host_id=host_id,
            turn_id=frozen_turn_id,
            observed_at=snapshot_a.updated_at,
        )
        conn.commit()
        before_state = conn.execute(
            """
            SELECT next_sequence, traversal_generation
            FROM turn_list_hosts
            WHERE host_id = ?
            """,
            (host_id,),
        ).fetchone()
    assert list_sequence == 1
    assert before_state == (2, 1)

    save_snapshot(db_path, snapshot_b)
    with sqlite3.connect(str(db_path)) as conn:
        rows = conn.execute(
            """
            SELECT turn_id, worker_id, worker_fingerprint, space_id,
                   list_sequence, payload_json
            FROM turns
            WHERE host_id = ?
            """,
            (host_id,),
        ).fetchall()
        after_state = conn.execute(
            """
            SELECT next_sequence, traversal_generation
            FROM turn_list_hosts
            WHERE host_id = ?
            """,
            (host_id,),
        ).fetchone()
        current_revisions = conn.execute(
            """
            SELECT COUNT(*)
            FROM turn_content_revisions
            WHERE host_id = ? AND turn_id = ? AND is_current = 1
            """,
            (host_id, frozen_turn_id),
        ).fetchone()[0]
        foreign_keys = conn.execute("PRAGMA foreign_key_check").fetchall()

    assert len(rows) == 1
    turn_id, worker_id, worker_fingerprint, space_id, sequence, raw_payload = rows[0]
    stored_payload = json.loads(str(raw_payload))
    assert (turn_id, sequence) == (frozen_turn_id, 1)
    assert stored_payload["id"] == frozen_turn_id
    assert (worker_id, worker_fingerprint, space_id) == (
        worker_b.id,
        worker_b.fingerprint,
        worker_b.space_id,
    )
    assert stored_payload["worker_id"] == worker_b.id
    assert stored_payload["worker_fingerprint"] == worker_b.fingerprint
    assert stored_payload["space_id"] == worker_b.space_id
    assert stored_payload["title"] == worker_b.name
    assert stored_payload["meta"]["stable_key"] == stable_key
    assert after_state == before_state
    assert current_revisions == 1
    assert foreign_keys == []

    durable_state = (rows, after_state)
    save_snapshot(db_path, snapshot_b)
    with sqlite3.connect(str(db_path)) as conn:
        replay_rows = conn.execute(
            """
            SELECT turn_id, worker_id, worker_fingerprint, space_id,
                   list_sequence, payload_json
            FROM turns
            WHERE host_id = ?
            """,
            (host_id,),
        ).fetchall()
        replay_state = conn.execute(
            """
            SELECT next_sequence, traversal_generation
            FROM turn_list_hosts
            WHERE host_id = ?
            """,
            (host_id,),
        ).fetchone()
    assert (replay_rows, replay_state) == durable_state
    public_payload = turns_payload_from_store(db_path, host_id, schema_version=2)
    assert [turn["id"] for turn in public_payload["turns"]] == [frozen_turn_id]


def test_stable_owner_snapshot_ambiguity_rolls_back_without_allocating_sequence(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "stable-owner-snapshot-ambiguity.db"
    host_id = "snapshot-ambiguity-host"
    stable_key = "wsk1_" + ("2" * 64)
    worker_a = Worker(
        id="worker-a",
        name="Worker A",
        status="active",
        fingerprint="snapshot-fingerprint-a",
        meta={"stable_key": stable_key, "stable_key_version": 1},
    )
    worker_b = Worker(
        id="worker-b",
        name="Worker B",
        status="waiting",
        fingerprint="snapshot-fingerprint-b",
        meta={"stable_key": stable_key, "stable_key_version": 1},
    )
    snapshot_a = Snapshot(
        host_id=host_id,
        updated_at="2026-07-13T01:00:00+00:00",
        workers=[worker_a],
    )
    snapshot_b = Snapshot(
        host_id=host_id,
        updated_at="2026-07-13T01:01:00+00:00",
        workers=[worker_b],
    )
    duplicate_id = "turn-222222222222222222222222"

    init_store(db_path)
    save_snapshot(db_path, snapshot_a)
    with sqlite3.connect(str(db_path)) as conn:
        row = conn.execute(
            """
            SELECT worker_id, worker_fingerprint, space_id, status, kind,
                   updated_at, fingerprint, snapshot_content_fingerprint,
                   observed_at, payload_json
            FROM turns
            WHERE host_id = ?
            """,
            (host_id,),
        ).fetchone()
        duplicate_payload = json.loads(str(row[9]))
        duplicate_payload["id"] = duplicate_id
        duplicate_payload["fingerprint"] = "frozen-duplicate-fingerprint"
        conn.execute(
            """
            INSERT INTO turns (
                host_id, turn_id, worker_id, worker_fingerprint, space_id,
                status, kind, updated_at, fingerprint,
                snapshot_content_fingerprint, observed_at, payload_json,
                list_sequence
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 2)
            """,
            (
                host_id,
                duplicate_id,
                row[0],
                row[1],
                row[2],
                row[3],
                row[4],
                row[5],
                "frozen-duplicate-fingerprint",
                row[7],
                row[8],
                json.dumps(duplicate_payload, sort_keys=True, separators=(",", ":")),
            ),
        )
        store_sqlite._ensure_absent_turn_content_revision_conn(
            conn,
            host_id=host_id,
            turn_id=duplicate_id,
            observed_at=snapshot_a.updated_at,
        )
        conn.execute(
            """
            UPDATE turn_list_hosts
            SET next_sequence = 3
            WHERE host_id = ?
            """,
            (host_id,),
        )
        conn.commit()

        before_turns = conn.execute(
            """
            SELECT turn_id, worker_id, list_sequence, payload_json
            FROM turns
            WHERE host_id = ?
            ORDER BY list_sequence
            """,
            (host_id,),
        ).fetchall()
        before_workers = conn.execute(
            "SELECT worker_id, payload_json FROM workers WHERE host_id = ?",
            (host_id,),
        ).fetchall()
        before_state = conn.execute(
            """
            SELECT next_sequence, traversal_generation
            FROM turn_list_hosts
            WHERE host_id = ?
            """,
            (host_id,),
        ).fetchone()
        before_snapshot_count = conn.execute(
            "SELECT COUNT(*) FROM snapshots WHERE host_id = ?",
            (host_id,),
        ).fetchone()[0]
        before_event_count = conn.execute(
            "SELECT COUNT(*) FROM events WHERE host_id = ?",
            (host_id,),
        ).fetchone()[0]

    with pytest.raises(
        store_sqlite.StoreSchemaError,
        match="^turn_owner_placeholder_ambiguous$",
    ):
        save_snapshot(db_path, snapshot_b)

    with sqlite3.connect(str(db_path)) as conn:
        after_turns = conn.execute(
            """
            SELECT turn_id, worker_id, list_sequence, payload_json
            FROM turns
            WHERE host_id = ?
            ORDER BY list_sequence
            """,
            (host_id,),
        ).fetchall()
        after_workers = conn.execute(
            "SELECT worker_id, payload_json FROM workers WHERE host_id = ?",
            (host_id,),
        ).fetchall()
        after_state = conn.execute(
            """
            SELECT next_sequence, traversal_generation
            FROM turn_list_hosts
            WHERE host_id = ?
            """,
            (host_id,),
        ).fetchone()
        after_snapshot_count = conn.execute(
            "SELECT COUNT(*) FROM snapshots WHERE host_id = ?",
            (host_id,),
        ).fetchone()[0]
        after_event_count = conn.execute(
            "SELECT COUNT(*) FROM events WHERE host_id = ?",
            (host_id,),
        ).fetchone()[0]
        foreign_keys = conn.execute("PRAGMA foreign_key_check").fetchall()
    assert after_turns == before_turns
    assert after_workers == before_workers
    assert after_state == before_state == (3, 1)
    assert after_snapshot_count == before_snapshot_count
    assert after_event_count == before_event_count
    assert foreign_keys == []


def test_stable_owner_exact_content_adoption_does_not_misattribute_unmatched_turn(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "stable-owner-command-provenance.db"
    host_id = "stable-command-provenance-host"
    stable_key = "wsk1_" + ("3" * 64)
    worker = Worker(
        id="owner-worker",
        name="Owner Worker",
        status="active",
        space_id="owner-space",
        fingerprint="owner-fingerprint",
        meta={"stable_key": stable_key, "stable_key_version": 1},
    )
    snapshot = Snapshot(
        host_id=host_id,
        updated_at="2026-07-13T02:00:00+00:00",
        workers=[worker],
    )
    init_store(db_path)
    save_snapshot(db_path, snapshot)

    command = store_sqlite.upsert_command_pending_turn(
        db_path,
        host_id,
        worker,
        request_id="request-exact",
        instruction_text="exact prompt",
        observed_at="2026-07-13T02:00:01+00:00",
    )
    assert command is not None
    raw_matching_source = "019f5590-1111-7111-8111-111111111111"
    assert merge_turn_content(
        db_path,
        host_id,
        worker.id,
        {
            "source_turn_id": raw_matching_source,
            "user_text": "exact prompt",
            "assistant_final_text": "exact final",
            "complete": True,
            "has_open_turn": False,
        },
        observed_at="2026-07-13T02:00:02+00:00",
    ) == 1
    payload = turns_payload_from_store(
        db_path,
        host_id,
        schema_version=2,
        now=datetime.fromisoformat("2026-07-13T02:00:02+00:00").timestamp(),
    )
    exact_source = next(
        turn for turn in payload["turns"]
        if turn.get("assistant_final_text") == "exact final"
    )
    source_with_origin = Turn(
        host_id=host_id,
        worker_id=worker.id,
        worker_fingerprint=worker.fingerprint,
        space_id=worker.space_id,
        status=worker.status,
        kind="task",
        source="command",
        origin_command_id="request-exact",
        source_turn_id=raw_matching_source,
        meta=worker.meta,
    )
    source_without_origin = Turn(
        host_id=host_id,
        worker_id="changed-worker-diagnostic",
        worker_fingerprint="changed-fingerprint",
        space_id="changed-space",
        status="waiting",
        kind="task",
        source="changed-backend-diagnostic",
        source_turn_id=raw_matching_source,
        meta=worker.meta,
    )
    assert exact_source["id"] == command["id"]
    assert exact_source["id"] != source_with_origin.id
    assert source_with_origin.id == source_without_origin.id
    assert exact_source["origin_command_id"] == "request-exact"
    assert exact_source["source_turn_id"] == source_with_origin.source_turn_id
    assert raw_matching_source not in json.dumps(payload, sort_keys=True)

    stale_command = store_sqlite.upsert_command_pending_turn(
        db_path,
        host_id,
        worker,
        request_id="request-stale",
        instruction_text="different command prompt",
        observed_at="2026-07-13T02:00:03+00:00",
    )
    assert stale_command is not None
    raw_unmatched_source = "019f5590-2222-7222-8222-222222222222"
    assert merge_turn_content(
        db_path,
        host_id,
        worker.id,
        {
            "source_turn_id": raw_unmatched_source,
            "user_text": "independent backend prompt",
            "assistant_final_text": "independent final",
            "complete": True,
            "has_open_turn": False,
        },
        observed_at="2026-07-13T02:00:04+00:00",
    ) == 1
    refreshed = turns_payload_from_store(
        db_path,
        host_id,
        schema_version=2,
        claim_hard_ttl_seconds=1_000_000_000,
        now=datetime.fromisoformat("2026-07-13T02:00:05+00:00").timestamp(),
    )
    unmatched_source = next(
        turn for turn in refreshed["turns"]
        if turn.get("assistant_final_text") == "independent final"
    )
    assert unmatched_source.get("origin_command_id") is None
    assert unmatched_source["id"] != stale_command["id"]
    assert stale_command["id"] in {turn["id"] for turn in refreshed["turns"]}
    with sqlite3.connect(str(db_path)) as conn:
        stale_payload = json.loads(
            conn.execute(
                "SELECT payload_json FROM turns WHERE host_id = ? AND turn_id = ?",
                (host_id, stale_command["id"]),
            ).fetchone()[0]
        )
    assert stale_payload.get("superseded_at") is None
    assert stale_payload.get("superseded_by_turn_id") is None
    encoded = json.dumps(refreshed, sort_keys=True)
    assert raw_matching_source not in encoded
    assert raw_unmatched_source not in encoded


def test_source_reconciliation_isolates_stable_owners_and_legacy_no_owner(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "owner-source-isolation.db"
    host_id = "owner-isolation-host"
    raw_source = "raw-owner-isolation-source"
    stable_key_1 = "wsk1_" + ("4" * 64)
    stable_key_2 = "wsk1_" + ("5" * 64)

    def snapshot_for(
        worker_id: str,
        stable_key: str | None,
        observed_at: str,
    ) -> Snapshot:
        meta = (
            {"stable_key": stable_key, "stable_key_version": 1}
            if stable_key is not None
            else {}
        )
        return Snapshot(
            host_id=host_id,
            updated_at=observed_at,
            workers=[
                Worker(
                    id=worker_id,
                    name="Shared",
                    status="active",
                    space_id="shared-space",
                    fingerprint="shared-fingerprint",
                    meta=meta,
                )
            ],
        )

    init_store(db_path)
    cases = (
        (
            snapshot_for(
                "shared-worker",
                stable_key_1,
                "2026-07-13T03:00:00+00:00",
            ),
            "owner one final",
            "2026-07-13T03:00:01+00:00",
        ),
        (
            snapshot_for(
                "shared-worker",
                stable_key_2,
                "2026-07-13T03:01:00+00:00",
            ),
            "owner two final",
            "2026-07-13T03:01:01+00:00",
        ),
        (
            snapshot_for(
                "shared-worker",
                None,
                "2026-07-13T03:02:00+00:00",
            ),
            "legacy same-worker final",
            "2026-07-13T03:02:01+00:00",
        ),
        (
            snapshot_for(
                "legacy-worker-b",
                None,
                "2026-07-13T03:03:00+00:00",
            ),
            "legacy changed-worker final",
            "2026-07-13T03:03:01+00:00",
        ),
    )
    for snapshot, final_text, observed_at in cases:
        save_snapshot(db_path, snapshot)
        assert merge_turn_content(
            db_path,
            host_id,
            snapshot.workers[0].id,
            {
                "source_turn_id": raw_source,
                "assistant_final_text": final_text,
                "complete": True,
                "has_open_turn": False,
            },
            observed_at=observed_at,
        ) == 1

    payload = turns_payload_from_store(db_path, host_id, schema_version=2)
    source_turns = {
        str(turn.get("assistant_final_text")): turn
        for turn in payload["turns"]
        if turn.get("source_turn_id")
    }
    assert set(source_turns) == {
        "owner one final",
        "owner two final",
        "legacy same-worker final",
        "legacy changed-worker final",
    }
    ids = {str(turn["id"]) for turn in source_turns.values()}
    tokens = {str(turn["source_turn_id"]) for turn in source_turns.values()}
    assert len(ids) == len(tokens) == 4
    assert (
        source_turns["owner one final"]["meta"]["stable_key"],
        source_turns["owner two final"]["meta"]["stable_key"],
    ) == (stable_key_1, stable_key_2)
    assert source_turns["legacy same-worker final"]["meta"].get("stable_key") is None
    assert source_turns["legacy changed-worker final"]["meta"].get("stable_key") is None
    assert (
        source_turns["legacy same-worker final"]["source_turn_id"]
        == "turnsrc-fdc56cfa0289296df514b264"
    )
    assert source_turns["legacy same-worker final"]["worker_id"] == "shared-worker"
    assert source_turns["legacy changed-worker final"]["worker_id"] == "legacy-worker-b"
    assert raw_source not in json.dumps(payload, sort_keys=True)
    with sqlite3.connect(str(db_path)) as conn:
        assert conn.execute("PRAGMA foreign_key_check").fetchall() == []
