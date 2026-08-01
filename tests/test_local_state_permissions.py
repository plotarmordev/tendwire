from __future__ import annotations

import errno
import os
import multiprocessing
import socket
import sqlite3
import stat
import sys
import threading
from dataclasses import asdict
from pathlib import Path

import pytest

import tendwire.local_state as local_state
from tendwire.local_state import (
    EntryType,
    LocalStateError,
    LocalStateErrorCode,
    LocalStateKind,
    PermissionState,
    atomic_replace_private_file,
    cleanup_private_sqlite_replacement_at,
    create_private_directory,
    create_private_file,
    enforce_bound_socket_permissions,
    entry_identity,
    inspect_config_state,
    inspect_owned_socket,
    inspect_private_directory,
    inspect_private_file,
    inspect_sqlite_family,
    open_private_directory,
    publish_private_file_at,
    pin_group_socket_for_client,
    pin_owned_socket,
    prepare_private_socket_parent,
    prepare_private_sqlite_replacement_at,
    prepare_sqlite_family,
    read_private_file_at,
    repair_owned_socket,
    repair_private_directory,
    repair_private_file,
    repair_config_state,
    publish_private_sqlite_replacement_at,
    release_private_sqlite_replacement_at,
    repair_sqlite_family,
    socket_bind_umask,
    unlink_verified_socket,
    sqlite_parent_available_bytes_at,
    unlink_verified_entry,
    validate_owned_regular_stat,
    verify_created_private_sqlite_replacement_at,
    validate_private_socket_parent,
    validate_socket_group_parent,
)

from tendwire.store import sqlite as store_sqlite
from tendwire.store.sqlite import init_store

pytestmark = pytest.mark.skipif(
    os.name != "posix" or not sys.platform.startswith("linux"),
    reason="Linux/POSIX local-state permission contract",
)


def _mode(path: Path) -> int:
    return stat.S_IMODE(os.lstat(path).st_mode)


def _bind(path: Path) -> socket.socket:
    listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        listener.bind(str(path))
    except Exception:
        listener.close()
        raise
    return listener


def _assert_path_free(value: object, *paths: Path) -> None:
    rendered = repr(value)
    for path in paths:
        assert str(path) not in rendered


def _open_fd_identities() -> dict[int, tuple[int, int, int, int]]:
    identities: dict[int, tuple[int, int, int, int]] = {}
    for raw_fd in os.listdir("/proc/self/fd"):
        fd = int(raw_fd)
        try:
            current = os.fstat(fd)
        except OSError as exc:
            # Listing /proc/self/fd briefly exposes the directory descriptor
            # used by listdir itself; it is closed before fstat can inspect it.
            if exc.errno == errno.EBADF:
                continue
            raise
        identities[fd] = (
            current.st_dev,
            current.st_ino,
            current.st_mode,
            current.st_rdev,
        )
    return identities


def _sqlite_replacement_source(
    tmp_path: Path, *, mode: int = 0o600
) -> tuple[Path, Path, int]:
    state_dir = tmp_path / "sqlite-state"
    state_dir.mkdir(mode=0o700)
    source = state_dir / "tendwire.db"
    source.write_bytes(b"original-sqlite-content")
    os.chmod(source, mode)
    parent_fd = os.open(
        state_dir,
        os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
    )
    return state_dir, source, parent_fd


def _create_replacement_output(target: str, content: bytes = b"replacement") -> None:
    fd = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o666)
    try:
        os.write(fd, content)
        os.fsync(fd)
    finally:
        os.close(fd)


def _cross_process_try_immediate(db_path: str, results: object) -> None:
    try:
        with sqlite3.connect(db_path, timeout=0) as conn:
            conn.execute("PRAGMA busy_timeout=0")
            conn.execute("BEGIN IMMEDIATE")
        results.put("acquired")
    except sqlite3.OperationalError as exc:
        results.put("locked" if "locked" in str(exc).lower() else type(exc).__name__)


def _assert_cross_process_immediate_is_locked(db_path: Path) -> None:
    context = multiprocessing.get_context("spawn")
    results = context.Queue()
    process = context.Process(
        target=_cross_process_try_immediate,
        args=(str(db_path), results),
    )
    try:
        process.start()
        process.join(timeout=15)
        if process.is_alive():
            process.terminate()
            process.join(timeout=5)
            pytest.fail("SQLite lock probe did not terminate")
        assert results.get(timeout=5) == "locked"
    finally:
        if process.is_alive():
            process.terminate()
            process.join(timeout=5)
        results.close()
        results.join_thread()




def test_permissive_umask_creation_is_private_before_use(tmp_path: Path) -> None:
    state_dir = tmp_path / "private-state"
    private_file = state_dir / "private-value"
    socket_path = state_dir / "daemon-endpoint"
    listener: socket.socket | None = None
    previous_umask = os.umask(0)
    try:
        try:
            directory_result = create_private_directory(state_dir)
            file_result = create_private_file(private_file)
            with socket_bind_umask():
                listener = _bind(socket_path)
            socket_result = enforce_bound_socket_permissions(socket_path)
        finally:
            os.umask(previous_umask)

        assert directory_result.state is PermissionState.CREATED
        assert file_result.state is PermissionState.CREATED
        assert socket_result.state is PermissionState.CREATED
        assert _mode(state_dir) == 0o700
        assert _mode(private_file) == 0o600
        assert _mode(socket_path) == 0o600
    finally:
        if listener is not None:
            listener.close()
        socket_path.unlink(missing_ok=True)


def test_directory_and_file_repairs_intersect_modes_and_never_widen(
    tmp_path: Path,
) -> None:
    state_dir = tmp_path / "state"
    state_dir.mkdir(mode=0o700)
    private_file = state_dir / "value"
    private_file.write_bytes(b"private")

    os.chmod(state_dir, 0o775)
    os.chmod(private_file, 0o6754)
    assert inspect_private_directory(state_dir).state is PermissionState.REPAIR_REQUIRED
    assert inspect_private_file(private_file).state is PermissionState.REPAIR_REQUIRED

    assert repair_private_directory(state_dir).state is PermissionState.REPAIRED
    assert repair_private_file(private_file).state is PermissionState.REPAIRED
    assert _mode(state_dir) == (0o775 & 0o700)
    assert _mode(private_file) == (0o6754 & 0o600)

    os.chmod(state_dir, 0o500)
    os.chmod(private_file, 0o400)
    assert repair_private_directory(state_dir).state is PermissionState.PRIVATE
    assert repair_private_file(private_file).state is PermissionState.PRIVATE
    assert _mode(state_dir) == 0o500
    assert _mode(private_file) == 0o400


def test_symlinks_and_wrong_types_are_refused_without_mutation(tmp_path: Path) -> None:
    state_dir = tmp_path / "state"
    state_dir.mkdir(mode=0o700)
    regular = state_dir / "regular"
    regular.write_bytes(b"unchanged")
    os.chmod(regular, 0o600)

    directory_link = tmp_path / "directory-link"
    directory_link.symlink_to(state_dir, target_is_directory=True)
    file_link = state_dir / "file-link"
    file_link.symlink_to(regular)
    wrong_file_type = state_dir / "not-a-file"
    wrong_file_type.mkdir(mode=0o700)
    wrong_socket_type = state_dir / "not-a-socket"
    wrong_socket_type.write_bytes(b"unchanged")

    for operation in (
        lambda: inspect_private_directory(directory_link),
        lambda: inspect_private_file(file_link),
        lambda: repair_private_file(wrong_file_type),
        lambda: inspect_owned_socket(wrong_socket_type),
    ):
        with pytest.raises(LocalStateError) as caught:
            operation()
        assert caught.value.code is LocalStateErrorCode.WRONG_TYPE
        _assert_path_free(caught.value, state_dir, regular, directory_link, file_link)

    assert regular.read_bytes() == b"unchanged"
    assert wrong_socket_type.read_bytes() == b"unchanged"
    assert file_link.is_symlink()
    assert directory_link.is_symlink()


def test_wrong_owner_stat_is_rejected_with_no_uid_or_path_in_error(
    tmp_path: Path,
) -> None:
    private_file = tmp_path / "private-owner-value"
    private_file.write_bytes(b"private")
    values = list(os.lstat(private_file))
    unexpected_uid = os.geteuid() + 1
    values[stat.ST_UID] = unexpected_uid
    wrong_owner = os.stat_result(values)

    with pytest.raises(LocalStateError) as caught:
        validate_owned_regular_stat(wrong_owner)

    assert caught.value.code is LocalStateErrorCode.WRONG_OWNER
    assert str(unexpected_uid) not in str(caught.value)
    _assert_path_free(caught.value, private_file)


def test_no_replace_publication_uses_verified_dir_fd_and_complete_content(
    tmp_path: Path,
) -> None:
    state_dir = tmp_path / "state"
    create_private_directory(state_dir)
    dir_fd = open_private_directory(state_dir)
    try:
        result = publish_private_file_at(dir_fd, "identity", b"complete-private-value")
        assert result.state is PermissionState.CREATED
        assert read_private_file_at(dir_fd, "identity") == b"complete-private-value"
        with pytest.raises(LocalStateError) as caught:
            publish_private_file_at(dir_fd, "identity", b"replacement")
        assert caught.value.code is LocalStateErrorCode.ENTRY_EXISTS
        assert read_private_file_at(dir_fd, "identity") == b"complete-private-value"
    finally:
        os.close(dir_fd)

    assert _mode(state_dir / "identity") == 0o600
    assert not tuple(state_dir.glob(".tendwire-*.tmp"))


def test_atomic_replacement_retains_stricter_mode_and_repairs_broad_mode(
    tmp_path: Path,
) -> None:
    private_file = tmp_path / "replace-value"
    private_file.write_bytes(b"old")
    os.chmod(private_file, 0o400)

    strict_result = atomic_replace_private_file(private_file, b"strict-new")
    assert strict_result.state is PermissionState.REPLACED
    assert private_file.read_bytes() == b"strict-new"
    assert _mode(private_file) == 0o400

    os.chmod(private_file, 0o6744)
    broad_result = atomic_replace_private_file(private_file, b"private-new")
    assert broad_result.state is PermissionState.REPLACED
    assert private_file.read_bytes() == b"private-new"
    assert _mode(private_file) == (0o6744 & 0o600)
    assert not tuple(tmp_path.glob(".tendwire-*.tmp"))


def test_sqlite_family_preparation_and_post_creation_repair(tmp_path: Path) -> None:
    db_path = tmp_path / "state" / "tendwire.db"
    previous_umask = os.umask(0)
    try:
        prepared = prepare_sqlite_family(db_path)
        for suffix in ("-wal", "-shm", "-journal"):
            Path(f"{db_path}{suffix}").write_bytes(b"sqlite-sidecar")
    finally:
        os.umask(previous_umask)

    assert prepared[0].kind is LocalStateKind.DATABASE
    assert prepared[0].state is PermissionState.CREATED
    assert _mode(db_path.parent) == 0o700
    assert _mode(db_path) == 0o600
    before = inspect_sqlite_family(db_path)
    assert tuple(result.state for result in before[1:]) == (
        PermissionState.REPAIR_REQUIRED,
        PermissionState.REPAIR_REQUIRED,
        PermissionState.REPAIR_REQUIRED,
    )

    repaired = repair_sqlite_family(db_path)
    assert tuple(result.mode for result in repaired) == (0o600, 0o600, 0o600, 0o600)
    for suffix in ("", "-wal", "-shm", "-journal"):
        assert _mode(Path(f"{db_path}{suffix}")) == 0o600


def test_sqlite_family_validates_every_member_before_repairing(tmp_path: Path) -> None:
    state_dir = tmp_path / "state"
    state_dir.mkdir(mode=0o700)
    db_path = state_dir / "tendwire.db"
    db_path.write_bytes(b"database")
    os.chmod(db_path, 0o644)
    sidecar_target = state_dir / "sidecar-target"
    sidecar_target.write_bytes(b"unchanged")
    os.chmod(sidecar_target, 0o600)
    Path(f"{db_path}-wal").symlink_to(sidecar_target)

    with pytest.raises(LocalStateError) as caught:
        repair_sqlite_family(db_path)

    assert caught.value.code is LocalStateErrorCode.WRONG_TYPE
    assert _mode(db_path) == 0o644
    assert sidecar_target.read_bytes() == b"unchanged"
    _assert_path_free(caught.value, db_path, sidecar_target)


@pytest.mark.parametrize(
    "operation",
    (inspect_sqlite_family, repair_sqlite_family, prepare_sqlite_family),
)
def test_sqlite_family_absent_optional_members_are_never_created(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    operation,
) -> None:
    state_dir = tmp_path / "state"
    state_dir.mkdir(mode=0o700)
    db_path = state_dir / "tendwire.db"
    db_path.write_bytes(b"database")
    os.chmod(db_path, 0o600)
    before_entries = set(os.listdir(state_dir))
    before_fds = set(os.listdir("/proc/self/fd"))
    optional_names = {f"{db_path.name}{suffix}" for suffix in ("-wal", "-shm", "-journal")}
    creation_attempts: list[str] = []
    real_open = local_state.os.open

    def record_optional_creation(
        path,
        flags: int,
        mode: int = 0o777,
        *,
        dir_fd: int | None = None,
    ) -> int:
        rendered = os.fspath(path)
        if rendered in optional_names and flags & os.O_CREAT:
            creation_attempts.append(rendered)
        return real_open(path, flags, mode, dir_fd=dir_fd)

    monkeypatch.setattr(local_state.os, "open", record_optional_creation)
    monkeypatch.setattr(
        local_state.os,
        "supports_dir_fd",
        {*local_state.os.supports_dir_fd, record_optional_creation},
    )

    results = operation(db_path)

    assert tuple(result.state for result in results[1:]) == (
        PermissionState.ABSENT,
        PermissionState.ABSENT,
        PermissionState.ABSENT,
    )
    assert creation_attempts == []
    assert set(os.listdir(state_dir)) == before_entries
    assert set(os.listdir("/proc/self/fd")) == before_fds


@pytest.mark.parametrize("suffix", ("-wal", "-shm", "-journal"))
@pytest.mark.parametrize(
    ("initial_mode", "expected_mode", "expected_state"),
    (
        (0o644, 0o600, PermissionState.REPAIRED),
        (0o400, 0o400, PermissionState.PRIVATE),
    ),
)
def test_sqlite_family_repair_narrows_broad_sidecars_without_widening_strict(
    tmp_path: Path,
    suffix: str,
    initial_mode: int,
    expected_mode: int,
    expected_state: PermissionState,
) -> None:
    state_dir = tmp_path / "state"
    state_dir.mkdir(mode=0o700)
    db_path = state_dir / "tendwire.db"
    db_path.write_bytes(b"database")
    os.chmod(db_path, 0o600)
    sidecar = Path(f"{db_path}{suffix}")
    sidecar.write_bytes(b"sidecar-content")
    os.chmod(sidecar, initial_mode)
    original_identity = entry_identity(os.lstat(sidecar))
    before_fds = set(os.listdir("/proc/self/fd"))

    results = prepare_sqlite_family(db_path)

    selected = results[("-wal", "-shm", "-journal").index(suffix) + 1]
    assert selected.state is expected_state
    assert selected.mode == expected_mode
    assert _mode(sidecar) == expected_mode
    assert entry_identity(os.lstat(sidecar)) == original_identity
    assert sidecar.read_bytes() == b"sidecar-content"
    assert all(
        not Path(f"{db_path}{other}").exists()
        for other in ("-wal", "-shm", "-journal")
        if other != suffix
    )
    assert set(os.listdir("/proc/self/fd")) == before_fds


def test_sqlite_family_closes_descriptors_and_redacts_raw_chmod_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_dir = tmp_path / "private-state-location"
    state_dir.mkdir(mode=0o700)
    db_path = state_dir / "secret-database-name"
    db_path.write_bytes(b"database")
    os.chmod(db_path, 0o644)
    raw_error = "raw-fchmod-secret"
    before_fds = set(os.listdir("/proc/self/fd"))

    def fail_fchmod(_fd: int, _mode: int) -> None:
        raise OSError(errno.EIO, raw_error)

    monkeypatch.setattr(local_state.os, "fchmod", fail_fchmod)
    with pytest.raises(LocalStateError) as caught:
        repair_sqlite_family(db_path)

    assert caught.value.code is LocalStateErrorCode.OPERATION_FAILED
    assert raw_error not in repr(caught.value)
    _assert_path_free(caught.value, state_dir, db_path)
    assert _mode(db_path) == 0o644
    assert set(os.listdir("/proc/self/fd")) == before_fds


def test_sqlite_family_closes_descriptors_when_capture_phase_aborts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class InjectedCaptureFailure(Exception):
        pass

    state_dir = tmp_path / "state"
    state_dir.mkdir(mode=0o700)
    db_path = state_dir / "tendwire.db"
    db_path.write_bytes(b"database")
    os.chmod(db_path, 0o644)
    sidecar = Path(f"{db_path}-wal")
    sidecar.write_bytes(b"wal")
    os.chmod(sidecar, 0o644)
    before_fds = set(os.listdir("/proc/self/fd"))

    def abort_after_open(phase: str, kind: LocalStateKind) -> None:
        if phase == "captured" and kind is LocalStateKind.DATABASE_WAL:
            raise InjectedCaptureFailure

    monkeypatch.setattr(local_state, "_sqlite_family_test_phase", abort_after_open)
    with pytest.raises(InjectedCaptureFailure):
        prepare_sqlite_family(db_path)

    assert _mode(db_path) == 0o644
    assert _mode(sidecar) == 0o644
    assert set(os.listdir("/proc/self/fd")) == before_fds


@pytest.mark.parametrize(
    ("suffix", "kind", "result_index"),
    (
        ("-wal", LocalStateKind.DATABASE_WAL, 1),
        ("-shm", LocalStateKind.DATABASE_SHM, 2),
        ("-journal", LocalStateKind.DATABASE_JOURNAL, 3),
    ),
)
def test_sqlite_family_optional_sidecar_disappears_after_capture(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    suffix: str,
    kind: LocalStateKind,
    result_index: int,
) -> None:
    state_dir = tmp_path / "state"
    state_dir.mkdir(mode=0o700)
    db_path = state_dir / "tendwire.db"
    db_path.write_bytes(b"database")
    os.chmod(db_path, 0o644)
    sidecar = Path(f"{db_path}{suffix}")
    sidecar.write_bytes(b"transient-sidecar")
    os.chmod(sidecar, 0o644)
    parent_fd = os.open(state_dir, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    before_fds = set(os.listdir("/proc/self/fd"))
    fired = False

    def remove_at_preflight(phase: str, selected_kind: LocalStateKind) -> None:
        nonlocal fired
        if phase == "preflight" and selected_kind is kind and not fired:
            fired = True
            os.unlink(sidecar.name, dir_fd=parent_fd)

    monkeypatch.setattr(local_state, "_sqlite_family_test_phase", remove_at_preflight)
    try:
        results = prepare_sqlite_family(db_path)

        assert fired
        assert results[result_index].kind is kind
        assert results[result_index].state is PermissionState.ABSENT
        assert results[result_index].mode is None
        assert not sidecar.exists()
        assert _mode(db_path) == 0o600
        assert set(os.listdir("/proc/self/fd")) == before_fds
    finally:
        os.close(parent_fd)


def test_created_sqlite_main_failure_removes_and_syncs_exact_main(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class InjectedPostCreationFailure(Exception):
        pass

    state_dir = tmp_path / "private-state-location"
    state_dir.mkdir(mode=0o700)
    db_path = state_dir / "secret-database-name"
    parent_fd = os.open(state_dir, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    parent_identity = entry_identity(os.fstat(parent_fd))
    before_fds = set(os.listdir("/proc/self/fd"))
    synced_identities = []
    real_fsync = os.fsync

    def record_fsync(fd: int) -> None:
        synced_identities.append(entry_identity(os.fstat(fd)))
        real_fsync(fd)

    def abort_after_creation(phase: str, kind: LocalStateKind) -> None:
        if phase == "created" and kind is LocalStateKind.DATABASE:
            raise InjectedPostCreationFailure

    monkeypatch.setattr(local_state.os, "fsync", record_fsync)
    monkeypatch.setattr(
        local_state,
        "_sqlite_family_test_phase",
        abort_after_creation,
    )
    try:
        with pytest.raises(InjectedPostCreationFailure):
            prepare_sqlite_family(db_path)

        assert not db_path.exists()
        assert parent_identity in synced_identities
        assert set(os.listdir("/proc/self/fd")) == before_fds
    finally:
        os.close(parent_fd)


def test_created_sqlite_main_fsync_failure_removes_and_syncs_exact_main(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_dir = tmp_path / "private-state-location"
    state_dir.mkdir(mode=0o700)
    db_path = state_dir / "secret-database-name"
    parent_fd = os.open(state_dir, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    parent_identity = entry_identity(os.fstat(parent_fd))
    before_fds = set(os.listdir("/proc/self/fd"))
    synced_identities = []
    real_fsync = os.fsync
    failed = False

    def fail_created_main_fsync(fd: int) -> None:
        nonlocal failed
        current = os.fstat(fd)
        synced_identities.append(entry_identity(current))
        if stat.S_ISREG(current.st_mode) and not failed:
            failed = True
            raise OSError(errno.EIO, "injected-created-main-fsync-failure")
        real_fsync(fd)

    monkeypatch.setattr(local_state.os, "fsync", fail_created_main_fsync)
    try:
        with pytest.raises(LocalStateError) as caught:
            prepare_sqlite_family(db_path)

        assert caught.value.code is LocalStateErrorCode.OPERATION_FAILED
        assert failed
        assert not db_path.exists()
        assert parent_identity in synced_identities
        assert set(os.listdir("/proc/self/fd")) == before_fds
    finally:
        os.close(parent_fd)


def test_created_sqlite_main_replacement_is_never_reported_or_removed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_dir = tmp_path / "private-state-location"
    state_dir.mkdir(mode=0o700)
    db_path = state_dir / "secret-database-name"
    moved_main = state_dir / "created-main-moved"
    parent_fd = os.open(state_dir, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    before_fds = set(os.listdir("/proc/self/fd"))
    replacement_identity = None
    fired = False

    def replace_after_creation(phase: str, kind: LocalStateKind) -> None:
        nonlocal fired, replacement_identity
        if phase != "created" or kind is not LocalStateKind.DATABASE or fired:
            return
        fired = True
        os.rename(db_path.name, moved_main.name, src_dir_fd=parent_fd, dst_dir_fd=parent_fd)
        replacement_fd = os.open(
            db_path.name,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL,
            0o600,
            dir_fd=parent_fd,
        )
        try:
            os.write(replacement_fd, b"replacement-main")
            os.fchmod(replacement_fd, 0o644)
        finally:
            os.close(replacement_fd)
        replacement_identity = entry_identity(os.lstat(db_path))

    monkeypatch.setattr(
        local_state,
        "_sqlite_family_test_phase",
        replace_after_creation,
    )
    try:
        with pytest.raises(LocalStateError) as caught:
            prepare_sqlite_family(db_path)

        assert fired
        assert caught.value.code is LocalStateErrorCode.ENTRY_CHANGED
        assert replacement_identity is not None
        assert entry_identity(os.lstat(db_path)) == replacement_identity
        assert _mode(db_path) == 0o644
        assert db_path.read_bytes() == b"replacement-main"
        assert moved_main.is_file()
        assert _mode(moved_main) == 0o600
        assert set(os.listdir("/proc/self/fd")) == before_fds
    finally:
        os.close(parent_fd)


def test_created_sqlite_main_late_replacement_is_never_reported_created(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_dir = tmp_path / "private-state-location"
    state_dir.mkdir(mode=0o700)
    db_path = state_dir / "secret-database-name"
    moved_main = state_dir / "created-main-moved"
    parent_fd = os.open(state_dir, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    before_fds = set(os.listdir("/proc/self/fd"))
    replacement_identity = None
    fired = False

    def replace_before_created_result(phase: str, kind: LocalStateKind) -> None:
        nonlocal fired, replacement_identity
        if (
            phase != "before_created_result"
            or kind is not LocalStateKind.DATABASE
            or fired
        ):
            return
        fired = True
        os.rename(
            db_path.name,
            moved_main.name,
            src_dir_fd=parent_fd,
            dst_dir_fd=parent_fd,
        )
        replacement_fd = os.open(
            db_path.name,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL,
            0o600,
            dir_fd=parent_fd,
        )
        try:
            os.write(replacement_fd, b"late-replacement-main")
        finally:
            os.close(replacement_fd)
        replacement_identity = entry_identity(os.lstat(db_path))

    monkeypatch.setattr(
        local_state,
        "_sqlite_family_test_phase",
        replace_before_created_result,
    )
    try:
        with pytest.raises(LocalStateError) as caught:
            prepare_sqlite_family(db_path)

        assert fired
        assert caught.value.code is LocalStateErrorCode.ENTRY_CHANGED
        assert replacement_identity is not None
        assert entry_identity(os.lstat(db_path)) == replacement_identity
        assert _mode(db_path) == 0o600
        assert db_path.read_bytes() == b"late-replacement-main"
        assert moved_main.is_file()
        assert _mode(moved_main) == 0o600
        assert set(os.listdir("/proc/self/fd")) == before_fds
    finally:
        os.close(parent_fd)


def test_sqlite_family_selected_main_disappearance_fails_closed_before_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_dir = tmp_path / "private-state-location"
    state_dir.mkdir(mode=0o700)
    db_path = state_dir / "secret-database-name"
    db_path.write_bytes(b"database")
    os.chmod(db_path, 0o644)
    sidecar = Path(f"{db_path}-wal")
    sidecar.write_bytes(b"wal")
    os.chmod(sidecar, 0o644)
    parent_fd = os.open(state_dir, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    before_fds = set(os.listdir("/proc/self/fd"))
    fired = False

    def remove_main(phase: str, kind: LocalStateKind) -> None:
        nonlocal fired
        if phase == "preflight" and kind is LocalStateKind.DATABASE and not fired:
            fired = True
            os.unlink(db_path.name, dir_fd=parent_fd)

    monkeypatch.setattr(local_state, "_sqlite_family_test_phase", remove_main)
    try:
        with pytest.raises(LocalStateError) as caught:
            prepare_sqlite_family(db_path)

        assert fired
        assert caught.value.code is LocalStateErrorCode.ENTRY_CHANGED
        _assert_path_free(caught.value, state_dir, db_path, sidecar)
        assert not db_path.exists()
        assert _mode(sidecar) == 0o644
        assert sidecar.read_bytes() == b"wal"
        assert set(os.listdir("/proc/self/fd")) == before_fds
    finally:
        os.close(parent_fd)


@pytest.mark.parametrize(
    ("suffix", "kind"),
    (
        ("-wal", LocalStateKind.DATABASE_WAL),
        ("-shm", LocalStateKind.DATABASE_SHM),
        ("-journal", LocalStateKind.DATABASE_JOURNAL),
    ),
)
@pytest.mark.parametrize(
    ("adversary", "expected_code"),
    (
        ("symlink", LocalStateErrorCode.WRONG_TYPE),
        ("directory", LocalStateErrorCode.WRONG_TYPE),
        ("wrong_owner", LocalStateErrorCode.WRONG_OWNER),
        ("different_inode", LocalStateErrorCode.ENTRY_CHANGED),
    ),
)
def test_sqlite_family_sidecar_replacement_fails_without_target_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    suffix: str,
    kind: LocalStateKind,
    adversary: str,
    expected_code: LocalStateErrorCode,
) -> None:
    state_dir = tmp_path / "private-state-location"
    state_dir.mkdir(mode=0o700)
    db_path = state_dir / "secret-database-name"
    db_path.write_bytes(b"database")
    os.chmod(db_path, 0o644)
    sidecar = Path(f"{db_path}{suffix}")
    sidecar.write_bytes(b"selected-sidecar")
    os.chmod(sidecar, 0o644)
    moved = state_dir / f"selected-original-{adversary}"
    target = state_dir / "protected-target"
    target.write_bytes(b"protected-content")
    os.chmod(target, 0o666)
    parent_fd = os.open(state_dir, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    original_lstat_at = local_state.lstat_at
    unexpected_uid = os.geteuid() + 100_000
    replacement_active = False
    replacement_identity = None
    before_fds = _open_fd_identities()
    preflight_calls = 0

    def owner_aware_lstat_at(dir_fd: int, name: str):
        current = original_lstat_at(dir_fd, name)
        if (
            adversary == "wrong_owner"
            and replacement_active
            and name == sidecar.name
            and current is not None
        ):
            values = list(current)
            values[stat.ST_UID] = unexpected_uid
            return os.stat_result(values)
        return current

    def replace_at_preflight(phase: str, selected_kind: LocalStateKind) -> None:
        nonlocal preflight_calls, replacement_active, replacement_identity
        if phase != "preflight" or selected_kind is not kind:
            return
        preflight_calls += 1
        if replacement_active:
            return
        os.rename(sidecar.name, moved.name, src_dir_fd=parent_fd, dst_dir_fd=parent_fd)
        if adversary == "symlink":
            os.symlink(target.name, sidecar.name, dir_fd=parent_fd)
        elif adversary == "directory":
            os.mkdir(sidecar.name, 0o755, dir_fd=parent_fd)
        else:
            fd = os.open(
                sidecar.name,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                0o644,
                dir_fd=parent_fd,
            )
            try:
                os.write(fd, b"hostile-replacement")
                os.fchmod(fd, 0o644)
            finally:
                os.close(fd)
            replacement_identity = entry_identity(os.stat(sidecar.name, dir_fd=parent_fd))
        replacement_active = True

    monkeypatch.setattr(local_state, "lstat_at", owner_aware_lstat_at)
    monkeypatch.setattr(local_state, "_sqlite_family_test_phase", replace_at_preflight)
    try:
        with pytest.raises(LocalStateError) as caught:
            prepare_sqlite_family(db_path)

        assert preflight_calls == 1
        assert caught.value.code is expected_code
        _assert_path_free(caught.value, state_dir, db_path, sidecar, moved, target)
        rendered = repr(caught.value)
        assert str(unexpected_uid) not in rendered
        assert str(os.lstat(moved).st_ino) not in rendered
        if replacement_identity is not None:
            assert str(replacement_identity.inode) not in rendered
        assert _mode(db_path) == 0o644
        assert moved.read_bytes() == b"selected-sidecar"
        assert _mode(moved) == 0o644
        assert target.read_bytes() == b"protected-content"
        assert _mode(target) == 0o666
        if adversary == "symlink":
            assert sidecar.is_symlink()
        elif adversary == "directory":
            assert sidecar.is_dir()
            assert _mode(sidecar) == 0o755
        else:
            assert sidecar.read_bytes() == b"hostile-replacement"
            assert _mode(sidecar) == 0o644
        # The full suite can finish background subprocess cleanup while this
        # test runs, legitimately closing descriptors owned by pytest.  Require
        # every remaining descriptor to retain its original identity, which
        # still catches both new descriptors and reuse of a closed fd number.
        assert _open_fd_identities().items() <= before_fds.items()
    finally:
        os.close(parent_fd)


@pytest.mark.parametrize(
    ("suffix", "kind", "result_index"),
    (
        ("-wal", LocalStateKind.DATABASE_WAL, 1),
        ("-shm", LocalStateKind.DATABASE_SHM, 2),
        ("-journal", LocalStateKind.DATABASE_JOURNAL, 3),
    ),
)
def test_sqlite_family_post_open_disappearance_closes_captured_descriptor(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    suffix: str,
    kind: LocalStateKind,
    result_index: int,
) -> None:
    state_dir = tmp_path / "state"
    state_dir.mkdir(mode=0o700)
    db_path = state_dir / "tendwire.db"
    db_path.write_bytes(b"database")
    os.chmod(db_path, 0o600)
    sidecar = Path(f"{db_path}{suffix}")
    sidecar.write_bytes(b"transient-sidecar")
    os.chmod(sidecar, 0o644)
    parent_fd = os.open(state_dir, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    before_fds = set(os.listdir("/proc/self/fd"))
    captured_fd = -1
    fired = False

    def remove_after_open(phase: str, selected_kind: LocalStateKind) -> None:
        nonlocal captured_fd, fired
        if phase != "captured" or selected_kind is not kind or fired:
            return
        fired = True
        for candidate in os.listdir("/proc/self/fd"):
            try:
                linked = os.readlink(f"/proc/self/fd/{candidate}")
            except OSError:
                continue
            if Path(linked).name == sidecar.name:
                captured_fd = int(candidate)
                break
        os.unlink(sidecar.name, dir_fd=parent_fd)

    monkeypatch.setattr(local_state, "_sqlite_family_test_phase", remove_after_open)
    try:
        results = prepare_sqlite_family(db_path)

        assert fired
        assert captured_fd >= 0
        assert results[result_index].state is PermissionState.ABSENT
        assert results[result_index].mode is None
        assert not sidecar.exists()
        with pytest.raises(OSError) as closed:
            os.fstat(captured_fd)
        assert closed.value.errno == errno.EBADF
        assert set(os.listdir("/proc/self/fd")) == before_fds
    finally:
        os.close(parent_fd)


@pytest.mark.parametrize(
    ("suffix", "kind", "result_index"),
    (
        ("-wal", LocalStateKind.DATABASE_WAL, 1),
        ("-shm", LocalStateKind.DATABASE_SHM, 2),
        ("-journal", LocalStateKind.DATABASE_JOURNAL, 3),
    ),
)
@pytest.mark.parametrize(
    "phase",
    (
        "narrow_before_open",
        "narrow_before_pre_fchmod_verify",
        "narrow_before_post_fchmod_verify",
    ),
)
def test_sqlite_family_optional_narrow_retirement_is_absent_at_each_pathname_boundary(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    suffix: str,
    kind: LocalStateKind,
    result_index: int,
    phase: str,
) -> None:
    state_dir = tmp_path / "state"
    state_dir.mkdir(mode=0o700)
    db_path = state_dir / "tendwire.db"
    db_path.write_bytes(b"database")
    os.chmod(db_path, 0o600)
    sidecar = Path(f"{db_path}{suffix}")
    sidecar.write_bytes(b"transient-sidecar")
    os.chmod(sidecar, 0o644)
    parent_fd = os.open(state_dir, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    before_fds = set(os.listdir("/proc/self/fd"))
    fired = False

    def retire_at_narrow_boundary(
        observed_phase: str,
        observed_kind: LocalStateKind,
    ) -> None:
        nonlocal fired
        if observed_phase == phase and observed_kind is kind and not fired:
            fired = True
            os.unlink(sidecar.name, dir_fd=parent_fd)

    monkeypatch.setattr(
        local_state,
        "_sqlite_family_test_phase",
        retire_at_narrow_boundary,
    )
    try:
        results = prepare_sqlite_family(db_path)

        assert fired
        assert results[result_index].kind is kind
        assert results[result_index].state is PermissionState.ABSENT
        assert results[result_index].mode is None
        assert _mode(db_path) == 0o600
        assert not sidecar.exists()
        assert set(os.listdir("/proc/self/fd")) == before_fds
    finally:
        os.close(parent_fd)


@pytest.mark.parametrize(
    "phase",
    (
        "narrow_before_open",
        "narrow_before_pre_fchmod_verify",
        "narrow_before_post_fchmod_verify",
    ),
)
def test_sqlite_family_main_narrow_retirement_fails_closed_at_each_pathname_boundary(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    phase: str,
) -> None:
    state_dir = tmp_path / "state"
    state_dir.mkdir(mode=0o700)
    db_path = state_dir / "tendwire.db"
    db_path.write_bytes(b"database")
    os.chmod(db_path, 0o644)
    parent_fd = os.open(state_dir, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    before_fds = set(os.listdir("/proc/self/fd"))
    fired = False

    def retire_main_at_narrow_boundary(
        observed_phase: str,
        observed_kind: LocalStateKind,
    ) -> None:
        nonlocal fired
        if (
            observed_phase == phase
            and observed_kind is LocalStateKind.DATABASE
            and not fired
        ):
            fired = True
            os.unlink(db_path.name, dir_fd=parent_fd)

    monkeypatch.setattr(
        local_state,
        "_sqlite_family_test_phase",
        retire_main_at_narrow_boundary,
    )
    try:
        with pytest.raises(LocalStateError) as caught:
            prepare_sqlite_family(db_path)

        assert fired
        assert caught.value.code is LocalStateErrorCode.ENTRY_CHANGED
        assert not db_path.exists()
        assert set(os.listdir("/proc/self/fd")) == before_fds
    finally:
        os.close(parent_fd)


def test_sqlite_family_narrowing_rejects_fifo_replacement_without_waiting(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_dir = tmp_path / "state"
    state_dir.mkdir(mode=0o700)
    db_path = state_dir / "tendwire.db"
    db_path.write_bytes(b"database")
    os.chmod(db_path, 0o600)
    sidecar = Path(f"{db_path}-wal")
    sidecar.write_bytes(b"selected-sidecar")
    os.chmod(sidecar, 0o644)
    moved = state_dir / "retired-sidecar"
    parent_fd = os.open(state_dir, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    before_fds = set(os.listdir("/proc/self/fd"))
    completed = threading.Event()
    errors: list[BaseException] = []
    fired = False

    def replace_with_fifo(phase: str, kind: LocalStateKind) -> None:
        nonlocal fired
        if (
            phase == "narrow_before_open"
            and kind is LocalStateKind.DATABASE_WAL
            and not fired
        ):
            fired = True
            os.rename(sidecar.name, moved.name, src_dir_fd=parent_fd, dst_dir_fd=parent_fd)
            os.mkfifo(sidecar.name, 0o600, dir_fd=parent_fd)

    def prepare_in_worker() -> None:
        try:
            prepare_sqlite_family(db_path)
        except BaseException as exc:
            errors.append(exc)
        finally:
            completed.set()

    monkeypatch.setattr(local_state, "_sqlite_family_test_phase", replace_with_fifo)
    worker = threading.Thread(target=prepare_in_worker)
    worker.start()
    settled = completed.wait(timeout=2)
    try:
        if not settled:
            writer_fd = os.open(
                sidecar.name,
                os.O_WRONLY | os.O_NONBLOCK,
                dir_fd=parent_fd,
            )
            os.close(writer_fd)
        worker.join(timeout=2)

        assert settled
        assert not worker.is_alive()
        assert fired
        assert len(errors) == 1
        assert isinstance(errors[0], LocalStateError)
        assert errors[0].code is LocalStateErrorCode.WRONG_TYPE
        assert stat.S_ISFIFO(os.lstat(sidecar).st_mode)
        assert moved.read_bytes() == b"selected-sidecar"
        assert set(os.listdir("/proc/self/fd")) == before_fds
    finally:
        if worker.is_alive():
            worker.join(timeout=2)
        os.close(parent_fd)


def test_sqlite_family_single_appearance_check_captures_and_repairs_valid_sidecar(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_dir = tmp_path / "state"
    state_dir.mkdir(mode=0o700)
    db_path = state_dir / "tendwire.db"
    db_path.write_bytes(b"database")
    os.chmod(db_path, 0o644)
    sidecar = Path(f"{db_path}-wal")
    parent_fd = os.open(state_dir, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    before_fds = set(os.listdir("/proc/self/fd"))
    appearance_checks = 0

    def create_at_appearance(phase: str, kind: LocalStateKind) -> None:
        nonlocal appearance_checks
        if phase != "appearance_check" or kind is not LocalStateKind.DATABASE_WAL:
            return
        appearance_checks += 1
        if appearance_checks == 1:
            fd = os.open(
                sidecar.name,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                0o644,
                dir_fd=parent_fd,
            )
            try:
                os.write(fd, b"appeared-sidecar")
                os.fchmod(fd, 0o644)
            finally:
                os.close(fd)

    monkeypatch.setattr(local_state, "_sqlite_family_test_phase", create_at_appearance)
    try:
        results = prepare_sqlite_family(db_path)

        assert appearance_checks == 1
        assert results[1].state is PermissionState.REPAIRED
        assert results[1].mode == 0o600
        assert sidecar.read_bytes() == b"appeared-sidecar"
        assert _mode(sidecar) == 0o600
        assert _mode(db_path) == 0o600
        assert set(os.listdir("/proc/self/fd")) == before_fds
    finally:
        os.close(parent_fd)


def test_sqlite_family_single_appearance_check_rejects_hostile_sidecar_globally(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_dir = tmp_path / "private-state-location"
    state_dir.mkdir(mode=0o700)
    db_path = state_dir / "secret-database-name"
    db_path.write_bytes(b"database")
    os.chmod(db_path, 0o644)
    sidecar = Path(f"{db_path}-wal")
    target = state_dir / "protected-target"
    target.write_bytes(b"protected-content")
    os.chmod(target, 0o666)
    parent_fd = os.open(state_dir, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    before_fds = set(os.listdir("/proc/self/fd"))
    appearance_checks = 0

    def introduce_symlink(phase: str, kind: LocalStateKind) -> None:
        nonlocal appearance_checks
        if phase == "appearance_check" and kind is LocalStateKind.DATABASE_WAL:
            appearance_checks += 1
            if appearance_checks == 1:
                os.symlink(target.name, sidecar.name, dir_fd=parent_fd)

    monkeypatch.setattr(local_state, "_sqlite_family_test_phase", introduce_symlink)
    try:
        with pytest.raises(LocalStateError) as caught:
            prepare_sqlite_family(db_path)

        assert appearance_checks == 1
        assert caught.value.code is LocalStateErrorCode.WRONG_TYPE
        _assert_path_free(caught.value, state_dir, db_path, sidecar, target)
        assert sidecar.is_symlink()
        assert target.read_bytes() == b"protected-content"
        assert _mode(target) == 0o666
        assert _mode(db_path) == 0o644
        assert set(os.listdir("/proc/self/fd")) == before_fds
    finally:
        os.close(parent_fd)


def test_sqlite_family_identity_replacement_is_not_recursively_retried(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_dir = tmp_path / "state"
    state_dir.mkdir(mode=0o700)
    db_path = state_dir / "tendwire.db"
    db_path.write_bytes(b"database")
    os.chmod(db_path, 0o644)
    sidecar = Path(f"{db_path}-wal")
    sidecar.write_bytes(b"selected-sidecar")
    os.chmod(sidecar, 0o644)
    parent_fd = os.open(state_dir, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    before_fds = set(os.listdir("/proc/self/fd"))
    preflight_calls = 0

    def replace_every_preflight(phase: str, kind: LocalStateKind) -> None:
        nonlocal preflight_calls
        if phase != "preflight" or kind is not LocalStateKind.DATABASE_WAL:
            return
        moved_name = f"retired-{preflight_calls}"
        os.rename(sidecar.name, moved_name, src_dir_fd=parent_fd, dst_dir_fd=parent_fd)
        fd = os.open(
            sidecar.name,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL,
            0o644,
            dir_fd=parent_fd,
        )
        try:
            os.write(fd, b"replacement")
            os.fchmod(fd, 0o644)
        finally:
            os.close(fd)
        preflight_calls += 1

    monkeypatch.setattr(local_state, "_sqlite_family_test_phase", replace_every_preflight)
    try:
        with pytest.raises(LocalStateError) as caught:
            prepare_sqlite_family(db_path)

        assert caught.value.code is LocalStateErrorCode.ENTRY_CHANGED
        assert preflight_calls == 1
        assert _mode(db_path) == 0o644
        assert _mode(sidecar) == 0o644
        assert sidecar.read_bytes() == b"replacement"
        assert set(os.listdir("/proc/self/fd")) == before_fds
    finally:
        os.close(parent_fd)


def test_sqlite_repair_refuses_mode_change_after_authority_decision(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_dir = tmp_path / "state"
    state_dir.mkdir(mode=0o700)
    db_path = state_dir / "tendwire.db"
    db_path.write_bytes(b"database")
    os.chmod(db_path, 0o600)
    original_requires_narrowing = local_state._sqlite_terminal_requires_narrowing
    broadened = False

    def broaden_after_decision(terminal) -> bool:
        nonlocal broadened
        requires_narrowing = original_requires_narrowing(terminal)
        if terminal.kind is LocalStateKind.DATABASE and not broadened:
            broadened = True
            os.chmod(db_path, 0o644)
        return requires_narrowing

    monkeypatch.setattr(
        local_state,
        "_sqlite_terminal_requires_narrowing",
        broaden_after_decision,
    )

    with pytest.raises(LocalStateError) as caught:
        repair_sqlite_family(db_path)

    assert caught.value.code is LocalStateErrorCode.ENTRY_CHANGED
    assert broadened is True
    assert _mode(db_path) == 0o644


def test_config_state_repair_is_startup_wide_idempotent_and_never_creates(
    tmp_path: Path,
) -> None:
    state_dir = tmp_path / "configured-state"
    state_dir.mkdir()
    os.chmod(state_dir, 0o755)
    db_path = state_dir / "state.db"
    Path(f"{db_path}").write_bytes(b"database")
    Path(f"{db_path}-wal").write_bytes(b"wal")
    identity_paths = (
        state_dir / "installation.key",
        state_dir / "installation.key.sha256",
        state_dir / "installation.key.initialized",
    )
    for path in (db_path, Path(f"{db_path}-wal"), *identity_paths):
        if path in identity_paths:
            path.write_bytes(b"identity")
        os.chmod(path, 0o644)
    missing_identity = state_dir / "missing-private-file"

    first = repair_config_state(
        state_dir,
        db_path,
        private_files=(*identity_paths, missing_identity),
    )

    assert first.ok is True
    assert first.issues == ()
    assert _mode(state_dir) == 0o700
    assert _mode(db_path) == 0o600
    assert _mode(Path(f"{db_path}-wal")) == 0o600
    assert all(_mode(path) == 0o600 for path in identity_paths)
    assert not missing_identity.exists()
    _assert_path_free(first, state_dir, db_path, *identity_paths, missing_identity)

    second = repair_config_state(
        state_dir,
        db_path,
        private_files=(*identity_paths, missing_identity),
    )

    assert all(
        result.state in {PermissionState.PRIVATE, PermissionState.ABSENT}
        for result in second.entries
    )
    assert not missing_identity.exists()


def test_config_sqlite_repair_acquires_authority_before_other_repairs(
    tmp_path: Path,
) -> None:
    state_dir = tmp_path / "configured-state"
    db_path = state_dir / "state.db"
    init_store(db_path)
    identity_path = state_dir / "installation.key"
    identity_path.write_bytes(b"identity")
    holder = store_sqlite._connect(db_path, prepare=True)
    try:
        holder.execute("BEGIN IMMEDIATE")
        os.chmod(db_path, 0o644)
        os.chmod(identity_path, 0o644)

        with pytest.raises(LocalStateError) as caught:
            repair_config_state(
                state_dir,
                db_path,
                private_files=(identity_path,),
            )

        assert caught.value.code is LocalStateErrorCode.OPERATION_FAILED
        assert _mode(db_path) == 0o644
        assert _mode(identity_path) == 0o644
    finally:
        holder.rollback()
        holder.close()


def test_config_ordinary_repair_requires_sqlite_parent_authority(
    tmp_path: Path,
) -> None:
    from multiprocessing import resource_tracker

    state_dir = tmp_path / "configured-state"
    db_path = state_dir / "state.db"
    init_store(db_path)
    identity_path = state_dir / "installation.key"
    identity_path.write_bytes(b"identity")
    os.chmod(identity_path, 0o644)
    database_identity = entry_identity(os.lstat(db_path))
    private_identity = entry_identity(os.lstat(identity_path))
    resource_tracker.ensure_running()
    before_fds = set(os.listdir("/proc/self/fd"))
    before_threads = {id(thread) for thread in threading.enumerate()}
    before_children = {process.pid for process in multiprocessing.active_children()}
    holder = store_sqlite._connect(db_path, prepare=True)
    try:
        holder.execute("BEGIN IMMEDIATE")

        with pytest.raises(LocalStateError) as caught:
            repair_config_state(
                state_dir,
                db_path,
                private_files=(identity_path,),
            )

        assert caught.value.code is LocalStateErrorCode.OPERATION_FAILED
        assert _mode(state_dir) == 0o700
        assert _mode(db_path) == 0o600
        assert _mode(identity_path) == 0o644
        assert entry_identity(os.lstat(db_path)) == database_identity
        assert entry_identity(os.lstat(identity_path)) == private_identity
        _assert_cross_process_immediate_is_locked(db_path)
    finally:
        holder.rollback()
        holder.close()
    assert set(os.listdir("/proc/self/fd")) == before_fds
    assert {id(thread) for thread in threading.enumerate()} == before_threads
    assert {process.pid for process in multiprocessing.active_children()} == before_children


@pytest.mark.parametrize(
    ("suffix", "kind", "entry_index"),
    (
        ("-wal", LocalStateKind.DATABASE_WAL, 2),
        ("-shm", LocalStateKind.DATABASE_SHM, 3),
        ("-journal", LocalStateKind.DATABASE_JOURNAL, 4),
    ),
)
def test_config_state_repair_accepts_optional_disappearance_and_finishes_commit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    suffix: str,
    kind: LocalStateKind,
    entry_index: int,
) -> None:
    state_dir = tmp_path / "configured-state"
    state_dir.mkdir(mode=0o755)
    os.chmod(state_dir, 0o755)
    db_path = state_dir / "state.db"
    db_path.write_bytes(b"database")
    os.chmod(db_path, 0o644)
    sidecar = Path(f"{db_path}{suffix}")
    sidecar.write_bytes(b"transient-sidecar")
    os.chmod(sidecar, 0o644)
    identity_path = state_dir / "installation.key"
    identity_path.write_bytes(b"identity")
    os.chmod(identity_path, 0o644)
    parent_fd = os.open(state_dir, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    before_fds = set(os.listdir("/proc/self/fd"))
    fired = False

    def remove_at_preflight(phase: str, selected_kind: LocalStateKind) -> None:
        nonlocal fired
        if phase == "preflight" and selected_kind is kind and not fired:
            fired = True
            os.unlink(sidecar.name, dir_fd=parent_fd)

    monkeypatch.setattr(local_state, "_sqlite_family_test_phase", remove_at_preflight)
    try:
        result = repair_config_state(
            state_dir,
            db_path,
            private_files=(identity_path,),
        )

        assert fired
        assert result.ok
        assert result.entries[entry_index].kind is kind
        assert result.entries[entry_index].state is PermissionState.ABSENT
        assert result.entries[entry_index].mode is None
        assert not sidecar.exists()
        assert _mode(state_dir) == 0o700
        assert _mode(db_path) == 0o600
        assert _mode(identity_path) == 0o600
        assert set(os.listdir("/proc/self/fd")) == before_fds
    finally:
        os.close(parent_fd)


def test_config_state_repair_main_disappearance_preserves_global_prevalidation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_dir = tmp_path / "private-configured-state"
    state_dir.mkdir(mode=0o755)
    os.chmod(state_dir, 0o755)
    db_path = state_dir / "secret-state-database"
    db_path.write_bytes(b"database")
    os.chmod(db_path, 0o644)
    sidecar = Path(f"{db_path}-wal")
    sidecar.write_bytes(b"wal")
    os.chmod(sidecar, 0o644)
    identity_path = state_dir / "secret-installation-key"
    identity_path.write_bytes(b"identity")
    os.chmod(identity_path, 0o644)
    parent_fd = os.open(state_dir, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    before_fds = set(os.listdir("/proc/self/fd"))
    fired = False

    def remove_main(phase: str, kind: LocalStateKind) -> None:
        nonlocal fired
        if phase == "preflight" and kind is LocalStateKind.DATABASE and not fired:
            fired = True
            os.unlink(db_path.name, dir_fd=parent_fd)

    monkeypatch.setattr(local_state, "_sqlite_family_test_phase", remove_main)
    try:
        with pytest.raises(LocalStateError) as caught:
            repair_config_state(
                state_dir,
                db_path,
                private_files=(identity_path,),
            )

        assert fired
        assert caught.value.code is LocalStateErrorCode.ENTRY_CHANGED
        _assert_path_free(caught.value, state_dir, db_path, sidecar, identity_path)
        assert _mode(state_dir) == 0o755
        assert not db_path.exists()
        assert _mode(sidecar) == 0o644
        assert _mode(identity_path) == 0o644
        assert set(os.listdir("/proc/self/fd")) == before_fds
    finally:
        os.close(parent_fd)


def test_config_state_repair_hostile_sidecar_appearance_prevalidates_globally(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_dir = tmp_path / "private-configured-state"
    state_dir.mkdir(mode=0o755)
    os.chmod(state_dir, 0o755)
    db_path = state_dir / "secret-state-database"
    db_path.write_bytes(b"database")
    os.chmod(db_path, 0o644)
    sidecar = Path(f"{db_path}-wal")
    identity_path = state_dir / "secret-installation-key"
    identity_path.write_bytes(b"identity")
    os.chmod(identity_path, 0o644)
    target = state_dir / "protected-target"
    target.write_bytes(b"protected-content")
    os.chmod(target, 0o666)
    parent_fd = os.open(state_dir, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    before_fds = set(os.listdir("/proc/self/fd"))
    appearance_checks = 0

    def introduce_symlink(phase: str, kind: LocalStateKind) -> None:
        nonlocal appearance_checks
        if phase == "appearance_check" and kind is LocalStateKind.DATABASE_WAL:
            appearance_checks += 1
            if appearance_checks == 1:
                os.symlink(target.name, sidecar.name, dir_fd=parent_fd)

    monkeypatch.setattr(local_state, "_sqlite_family_test_phase", introduce_symlink)
    try:
        with pytest.raises(LocalStateError) as caught:
            repair_config_state(
                state_dir,
                db_path,
                private_files=(identity_path,),
            )

        assert appearance_checks == 1
        assert caught.value.code is LocalStateErrorCode.WRONG_TYPE
        _assert_path_free(
            caught.value,
            state_dir,
            db_path,
            sidecar,
            identity_path,
            target,
        )
        assert sidecar.is_symlink()
        assert target.read_bytes() == b"protected-content"
        assert _mode(target) == 0o666
        assert _mode(state_dir) == 0o755
        assert _mode(db_path) == 0o644
        assert _mode(identity_path) == 0o644
        assert set(os.listdir("/proc/self/fd")) == before_fds
    finally:
        os.close(parent_fd)


def test_config_state_repair_without_database_still_repairs_other_state(
    tmp_path: Path,
) -> None:
    state_dir = tmp_path / "configured-state"
    state_dir.mkdir()
    os.chmod(state_dir, 0o755)
    identity_path = state_dir / "installation.key"
    identity_path.write_bytes(b"identity")
    os.chmod(identity_path, 0o644)

    result = repair_config_state(
        state_dir,
        None,
        private_files=(identity_path,),
    )

    assert tuple(entry.kind for entry in result.entries) == (
        LocalStateKind.STATE_DIRECTORY,
        LocalStateKind.PRIVATE_FILE,
    )
    assert _mode(state_dir) == 0o700
    assert _mode(identity_path) == 0o600
    _assert_path_free(result, state_dir, identity_path)


def test_config_state_repair_rejects_entry_appearing_after_prevalidation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_dir = tmp_path / "configured-state"
    state_dir.mkdir()
    os.chmod(state_dir, 0o755)
    db_path = state_dir / "state.db"
    db_path.write_bytes(b"database")
    os.chmod(db_path, 0o644)
    target = state_dir / "protected-target"
    target.write_bytes(b"unchanged")
    os.chmod(target, 0o600)
    missing_identity = state_dir / "installation.key"
    original_verify = local_state._verify_config_repair_entry
    introduced = False

    def introduce_symlink(entry) -> None:
        nonlocal introduced
        if not introduced:
            introduced = True
            missing_identity.symlink_to(target)
        original_verify(entry)

    monkeypatch.setattr(
        local_state,
        "_verify_config_repair_entry",
        introduce_symlink,
    )

    with pytest.raises(LocalStateError) as caught:
        repair_config_state(
            state_dir,
            db_path,
            private_files=(missing_identity,),
        )

    assert caught.value.code is LocalStateErrorCode.ENTRY_CHANGED
    assert _mode(state_dir) == 0o755
    assert _mode(db_path) == 0o644
    assert missing_identity.is_symlink()
    assert target.read_bytes() == b"unchanged"
    _assert_path_free(caught.value, state_dir, db_path, target, missing_identity)


@pytest.mark.parametrize("defect_kind", ["symlink", "wrong_type"])
def test_config_state_repair_prevalidates_all_entries_before_mode_changes(
    tmp_path: Path,
    defect_kind: str,
) -> None:
    state_dir = tmp_path / "configured-state"
    state_dir.mkdir()
    os.chmod(state_dir, 0o755)
    db_path = state_dir / "state.db"
    db_path.write_bytes(b"database")
    os.chmod(db_path, 0o644)
    target = state_dir / "protected-target"
    target.write_bytes(b"unchanged")
    os.chmod(target, 0o600)
    defective_identity = state_dir / "installation.key"
    if defect_kind == "symlink":
        defective_identity.symlink_to(target)
    else:
        defective_identity.mkdir()
        os.chmod(defective_identity, 0o700)

    with pytest.raises(LocalStateError) as caught:
        repair_config_state(
            state_dir,
            db_path,
            private_files=(defective_identity,),
        )

    assert caught.value.code is LocalStateErrorCode.WRONG_TYPE
    assert _mode(state_dir) == 0o755
    assert _mode(db_path) == 0o644
    assert target.read_bytes() == b"unchanged"
    if defect_kind == "symlink":
        assert defective_identity.is_symlink()
    else:
        assert defective_identity.is_dir()
    _assert_path_free(caught.value, state_dir, db_path, target, defective_identity)


def test_config_state_repair_rejects_wrong_owner_without_changing_modes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_dir = tmp_path / "configured-state"
    state_dir.mkdir()
    os.chmod(state_dir, 0o755)
    db_path = state_dir / "state.db"
    db_path.write_bytes(b"database")
    os.chmod(db_path, 0o644)
    unexpected_uid = os.geteuid() + 100_000
    monkeypatch.setattr("tendwire.local_state.os.geteuid", lambda: unexpected_uid)

    with pytest.raises(LocalStateError) as caught:
        repair_config_state(state_dir, db_path)

    assert caught.value.code is LocalStateErrorCode.WRONG_OWNER
    assert _mode(state_dir) == 0o755
    assert _mode(db_path) == 0o644
    assert str(unexpected_uid) not in str(caught.value)
    _assert_path_free(caught.value, state_dir, db_path)


def test_socket_repair_intersects_mode_and_stricter_mode_is_retained(
    tmp_path: Path,
) -> None:
    socket_path = tmp_path / "daemon-socket"
    listener = _bind(socket_path)
    try:
        os.chmod(socket_path, 0o775)
        assert inspect_owned_socket(socket_path).state is PermissionState.REPAIR_REQUIRED
        assert repair_owned_socket(socket_path).state is PermissionState.REPAIRED
        assert _mode(socket_path) == (0o775 & 0o600)

        os.chmod(socket_path, 0o400)
        assert repair_owned_socket(socket_path).state is PermissionState.PRIVATE
        assert _mode(socket_path) == 0o400
    finally:
        listener.close()
        socket_path.unlink(missing_ok=True)


def test_invalid_socket_group_is_rejected_before_chgrp_or_chmod(tmp_path: Path) -> None:
    socket_path = tmp_path / "group-socket"
    listener = _bind(socket_path)
    invalid_group = "tendwire-group-that-does-not-exist-73b8e80e"
    try:
        os.chmod(socket_path, 0o777)
        before = os.lstat(socket_path)
        with pytest.raises(LocalStateError) as caught:
            enforce_bound_socket_permissions(socket_path, socket_group=invalid_group)
        after = os.lstat(socket_path)

        assert caught.value.code is LocalStateErrorCode.INVALID_SOCKET_GROUP
        assert invalid_group not in str(caught.value)
        assert (after.st_dev, after.st_ino, after.st_gid, stat.S_IMODE(after.st_mode)) == (
            before.st_dev,
            before.st_ino,
            before.st_gid,
            stat.S_IMODE(before.st_mode),
        )
    finally:
        listener.close()
        socket_path.unlink(missing_ok=True)
def test_explicit_member_socket_group_uses_exact_group_private_mode(
    tmp_path: Path,
) -> None:
    import grp

    socket_path = tmp_path / "group-socket"
    group_name = grp.getgrgid(os.getegid()).gr_name
    listener: socket.socket | None = None
    previous_umask = os.umask(0)
    try:
        try:
            with socket_bind_umask(group_name):
                listener = _bind(socket_path)
            result = enforce_bound_socket_permissions(
                socket_path,
                socket_group=group_name,
            )
        finally:
            os.umask(previous_umask)

        assert result.mode == 0o660
        assert _mode(socket_path) == 0o660
        assert os.lstat(socket_path).st_gid == os.getegid()
    finally:
        if listener is not None:
            listener.close()
        socket_path.unlink(missing_ok=True)


def test_socket_unlink_requires_the_inspected_identity(tmp_path: Path) -> None:
    socket_path = tmp_path / "daemon-socket"
    moved_path = tmp_path / "moved-socket"
    first = _bind(socket_path)
    second: socket.socket | None = None
    try:
        expected = entry_identity(os.lstat(socket_path))
        os.rename(socket_path, moved_path)
        second = _bind(socket_path)
        with pytest.raises(LocalStateError) as caught:
            unlink_verified_socket(socket_path, expected)
        assert caught.value.code is LocalStateErrorCode.ENTRY_CHANGED
        assert stat.S_ISSOCK(os.lstat(socket_path).st_mode)
    finally:
        first.close()
        if second is not None:
            second.close()
        socket_path.unlink(missing_ok=True)
        moved_path.unlink(missing_ok=True)


def test_verified_socket_unlink_removes_only_the_expected_socket(tmp_path: Path) -> None:
    socket_path = tmp_path / "daemon-socket"
    listener = _bind(socket_path)
    try:
        expected = entry_identity(os.lstat(socket_path))
        unlink_verified_socket(socket_path, expected)
        assert not socket_path.exists()
    finally:
        listener.close()
        socket_path.unlink(missing_ok=True)


def test_config_state_report_and_errors_are_path_free(tmp_path: Path) -> None:
    state_dir = tmp_path / "secret-state-location"
    state_dir.mkdir(mode=0o700)
    db_path = state_dir / "secret-database-name"
    target = state_dir / "secret-target-name"
    target.write_bytes(b"secret-content-value")
    os.chmod(target, 0o600)
    db_path.symlink_to(target)
    socket_path = state_dir / "secret-socket-name"
    socket_path.write_bytes(b"not-a-socket")

    report = inspect_config_state(
        state_dir,
        db_path,
        socket_path=socket_path,
        private_files=(target,),
    )
    rendered = repr(asdict(report))

    assert not report.ok
    assert {issue.code for issue in report.issues} == {LocalStateErrorCode.WRONG_TYPE}
    for forbidden in (
        str(tmp_path),
        state_dir.name,
        db_path.name,
        target.name,
        socket_path.name,
        "secret-content-value",
        "uid",
    ):
        assert forbidden not in rendered
    assert all(not hasattr(result, "path") for result in report.entries)


def test_generic_unlink_type_contract_does_not_follow_symlinks(tmp_path: Path) -> None:
    state_dir = tmp_path / "state"
    state_dir.mkdir(mode=0o700)
    target = state_dir / "target"
    target.write_bytes(b"unchanged")
    os.chmod(target, 0o600)
    link = state_dir / "link"
    link.symlink_to(target)
    dir_fd = os.open(state_dir, os.O_RDONLY | os.O_DIRECTORY)
    try:
        with pytest.raises(LocalStateError) as caught:
            unlink_verified_entry(
                dir_fd,
                "link",
                entry_identity(os.lstat(link)),
                expected_type=EntryType.REGULAR_FILE,
            )
        assert caught.value.code is LocalStateErrorCode.WRONG_TYPE
    finally:
        os.close(dir_fd)

    assert link.is_symlink()
    assert target.read_bytes() == b"unchanged"


@pytest.mark.parametrize("unsafe_mode", [0o1777, 0o720, 0o702])
def test_private_socket_parent_rejects_group_or_world_write_without_mutation(
    tmp_path: Path,
    unsafe_mode: int,
) -> None:
    parent = tmp_path / "private-socket-parent"
    parent.mkdir()
    os.chmod(parent, unsafe_mode)
    sentinel = parent / "sentinel"
    sentinel.write_bytes(b"unchanged")
    socket_path = parent / "daemon.sock"

    with pytest.raises(LocalStateError) as caught:
        prepare_private_socket_parent(socket_path)

    assert caught.value.code is LocalStateErrorCode.INSECURE_SOCKET_PARENT
    assert _mode(parent) == unsafe_mode
    assert sentinel.read_bytes() == b"unchanged"
    assert not os.path.lexists(socket_path)
    _assert_path_free(caught.value, parent, sentinel, socket_path)


def test_private_socket_parent_accepts_owned_nonwritable_directory_unchanged(
    tmp_path: Path,
) -> None:
    parent = tmp_path / "private-socket-parent"
    parent.mkdir()
    os.chmod(parent, 0o755)

    result = validate_private_socket_parent(parent / "daemon.sock")

    assert result.state is PermissionState.PRIVATE
    assert result.mode == 0o755
    assert _mode(parent) == 0o755


def test_private_socket_parent_securely_creates_missing_dedicated_directory(
    tmp_path: Path,
) -> None:
    parent = tmp_path / "private-socket-parent"

    result = prepare_private_socket_parent(parent / "daemon.sock")

    assert result.state is PermissionState.CREATED
    assert result.mode == 0o700
    assert _mode(parent) == 0o700


@pytest.mark.parametrize("parent_kind", ["symlink", "regular-file"])
def test_private_socket_parent_rejects_non_directory_without_mutation(
    tmp_path: Path,
    parent_kind: str,
) -> None:
    protected = tmp_path / "protected"
    if parent_kind == "symlink":
        protected.mkdir()
        os.chmod(protected, 0o755)
        parent = tmp_path / "linked-parent"
        parent.symlink_to(protected, target_is_directory=True)
    else:
        protected.write_bytes(b"unchanged")
        parent = protected
    socket_path = parent / "daemon.sock"

    with pytest.raises(LocalStateError) as caught:
        prepare_private_socket_parent(socket_path)

    assert caught.value.code is LocalStateErrorCode.WRONG_TYPE
    if parent_kind == "symlink":
        assert parent.is_symlink()
        assert _mode(protected) == 0o755
    else:
        assert protected.read_bytes() == b"unchanged"
    _assert_path_free(caught.value, parent, protected, socket_path)


def test_private_socket_parent_rejects_wrong_owner_without_chmod(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    parent = tmp_path / "wrong-owner-parent"
    parent.mkdir()
    os.chmod(parent, 0o755)
    socket_path = parent / "daemon.sock"
    unexpected_uid = os.geteuid() + 100_000
    monkeypatch.setattr(local_state.os, "geteuid", lambda: unexpected_uid)

    with pytest.raises(LocalStateError) as caught:
        prepare_private_socket_parent(socket_path)

    assert caught.value.code is LocalStateErrorCode.WRONG_OWNER
    assert str(unexpected_uid) not in str(caught.value)
    assert _mode(parent) == 0o755
    assert not os.path.lexists(socket_path)
    _assert_path_free(caught.value, parent, socket_path)


@pytest.mark.parametrize("unsafe_mode", [0o700, 0o730, 0o711])
def test_socket_group_parent_requires_group_search_without_shared_writes(
    tmp_path: Path,
    unsafe_mode: int,
) -> None:
    import grp

    parent = tmp_path / "shared-socket-parent"
    parent.mkdir()
    os.chmod(parent, unsafe_mode)
    socket_path = parent / "daemon.sock"
    group_name = grp.getgrgid(os.getegid()).gr_name

    with pytest.raises(LocalStateError) as caught:
        validate_socket_group_parent(socket_path, group_name)

    assert caught.value.code is LocalStateErrorCode.INSECURE_SOCKET_PARENT
    assert _mode(parent) == unsafe_mode
    _assert_path_free(caught.value, parent, socket_path)


def test_socket_group_parent_accepts_owned_target_group_directory(
    tmp_path: Path,
) -> None:
    import grp

    parent = tmp_path / "shared-socket-parent"
    parent.mkdir()
    os.chmod(parent, 0o710)
    socket_path = parent / "daemon.sock"
    group_name = grp.getgrgid(os.getegid()).gr_name

    resolved = validate_socket_group_parent(socket_path, group_name)

    assert resolved.group_id == os.getegid()
    assert _mode(parent) == 0o710


def test_socket_group_parent_rejects_symlink_without_touching_target(
    tmp_path: Path,
) -> None:
    import grp

    target = tmp_path / "protected-target"
    target.mkdir()
    os.chmod(target, 0o710)
    linked_parent = tmp_path / "linked-parent"
    linked_parent.symlink_to(target, target_is_directory=True)
    socket_path = linked_parent / "daemon.sock"
    group_name = grp.getgrgid(os.getegid()).gr_name

    with pytest.raises(LocalStateError) as caught:
        validate_socket_group_parent(socket_path, group_name)

    assert caught.value.code is LocalStateErrorCode.OPERATION_FAILED
    assert linked_parent.is_symlink()
    assert _mode(target) == 0o710
    assert not (target / "daemon.sock").exists()
    _assert_path_free(caught.value, linked_parent, target, socket_path)


def test_invalid_socket_group_is_rejected_before_parent_lookup_or_mutation(
    tmp_path: Path,
) -> None:
    missing_parent = tmp_path / "missing-parent"
    socket_path = missing_parent / "daemon.sock"
    invalid_group = "tendwire-group-that-does-not-exist-1ea05e79"

    with pytest.raises(LocalStateError) as caught:
        validate_socket_group_parent(socket_path, invalid_group)

    assert caught.value.code is LocalStateErrorCode.INVALID_SOCKET_GROUP
    assert not missing_parent.exists()
    assert invalid_group not in str(caught.value)
    _assert_path_free(caught.value, missing_parent, socket_path)


@pytest.mark.parametrize("explicit_socket", [False, True])
def test_config_state_reports_unsafe_group_parent_without_mutation(
    tmp_path: Path,
    explicit_socket: bool,
) -> None:
    import grp

    state_dir = tmp_path / "private-state"
    state_dir.mkdir()
    os.chmod(state_dir, 0o700)
    socket_path = state_dir / "daemon.sock" if explicit_socket else None
    group_name = grp.getgrgid(os.getegid()).gr_name

    report = inspect_config_state(
        state_dir,
        state_dir / "state.db",
        socket_path=socket_path,
        socket_group=group_name,
    )

    group_issues = [
        issue for issue in report.issues if issue.kind is LocalStateKind.SOCKET_GROUP
    ]
    assert len(group_issues) == 1
    assert group_issues[0].code is LocalStateErrorCode.INSECURE_SOCKET_PARENT
    assert _mode(state_dir) == 0o700
    _assert_path_free(report, state_dir)


def test_pinned_socket_identity_cannot_be_reused_during_verified_cleanup(
    tmp_path: Path,
) -> None:
    socket_path = tmp_path / "pinned-socket"
    moved_path = tmp_path / "moved-pinned-socket"
    first = _bind(socket_path)
    pinned = pin_owned_socket(socket_path)
    assert pinned is not None
    pin_fd, expected = pinned
    second: socket.socket | None = None
    try:
        os.rename(socket_path, moved_path)
        second = _bind(socket_path)

        with pytest.raises(LocalStateError) as caught:
            unlink_verified_socket(socket_path, expected)

        assert caught.value.code is LocalStateErrorCode.ENTRY_CHANGED
        assert stat.S_ISSOCK(os.fstat(pin_fd).st_mode)
        assert stat.S_ISSOCK(os.lstat(socket_path).st_mode)
    finally:
        os.close(pin_fd)
        first.close()
        if second is not None:
            second.close()
        socket_path.unlink(missing_ok=True)
        moved_path.unlink(missing_ok=True)


def test_pin_owned_socket_rejects_symlink_without_following_target(
    tmp_path: Path,
) -> None:
    target = tmp_path / "target-socket"
    link = tmp_path / "linked-socket"
    listener = _bind(target)
    link.symlink_to(target)
    try:
        with pytest.raises(LocalStateError) as caught:
            pin_owned_socket(link)

        assert caught.value.code is LocalStateErrorCode.WRONG_TYPE
        assert link.is_symlink()
        assert stat.S_ISSOCK(os.lstat(target).st_mode)
    finally:
        listener.close()
        link.unlink(missing_ok=True)
        target.unlink(missing_ok=True)


def test_group_client_pin_trusts_correlated_daemon_owner_not_client_uid(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import grp

    parent = tmp_path / "shared-parent"
    parent.mkdir()
    os.chmod(parent, 0o710)
    socket_path = parent / "daemon.sock"
    group_name = grp.getgrgid(os.getegid()).gr_name
    listener: socket.socket | None = None
    try:
        with socket_bind_umask(group_name):
            listener = _bind(socket_path)
        enforce_bound_socket_permissions(socket_path, socket_group=group_name)
        daemon_owner = os.lstat(socket_path).st_uid

        with monkeypatch.context() as client_process:
            client_process.setattr(
                "tendwire.local_state.os.geteuid",
                lambda: daemon_owner + 100_000,
            )
            pin_fd, identity, expected_peer_uid = pin_group_socket_for_client(
                socket_path,
                group_name,
            )
        try:
            assert expected_peer_uid == daemon_owner
            assert identity == entry_identity(os.fstat(pin_fd))
        finally:
            os.close(pin_fd)
    finally:
        if listener is not None:
            listener.close()
        socket_path.unlink(missing_ok=True)


def test_intermediate_symlink_data_dir_is_refused_without_target_mutation(
    tmp_path: Path,
) -> None:
    target = tmp_path / "protected-target"
    target.mkdir()
    sentinel = target / "sentinel"
    sentinel.write_bytes(b"unchanged")
    linked = tmp_path / "linked"
    linked.symlink_to(target, target_is_directory=True)
    requested = linked / "nested" / "state"

    with pytest.raises(LocalStateError) as caught:
        local_state.prepare_private_directory(requested)

    assert caught.value.code is LocalStateErrorCode.WRONG_TYPE
    assert sentinel.read_bytes() == b"unchanged"
    assert not (target / "nested").exists()
    _assert_path_free(caught.value, target, linked, requested)


def test_intermediate_symlink_sqlite_family_is_refused_without_target_mutation(
    tmp_path: Path,
) -> None:
    target = tmp_path / "protected-target"
    state = target / "state"
    state.mkdir(parents=True)
    db_path = state / "tendwire.db"
    db_path.write_bytes(b"database")
    os.chmod(db_path, 0o666)
    linked = tmp_path / "linked"
    linked.symlink_to(target, target_is_directory=True)
    configured = linked / "state" / "tendwire.db"

    for operation in (inspect_sqlite_family, repair_sqlite_family, prepare_sqlite_family):
        with pytest.raises(LocalStateError) as caught:
            operation(configured)
        assert caught.value.code is LocalStateErrorCode.WRONG_TYPE
        _assert_path_free(caught.value, target, linked, configured)

    assert db_path.read_bytes() == b"database"
    assert _mode(db_path) == 0o666


def test_intermediate_symlink_socket_paths_are_refused_without_target_mutation(
    tmp_path: Path,
) -> None:
    import grp

    target = tmp_path / "protected-target"
    target.mkdir()
    shared_parent = target / "shared"
    shared_parent.mkdir()
    os.chmod(shared_parent, 0o710)
    socket_path = shared_parent / "daemon.sock"
    listener = _bind(socket_path)
    linked = tmp_path / "linked"
    linked.symlink_to(target, target_is_directory=True)
    configured = linked / "shared" / "daemon.sock"
    group_name = grp.getgrgid(os.getegid()).gr_name
    try:
        for operation in (inspect_owned_socket, prepare_private_socket_parent):
            with pytest.raises(LocalStateError) as private_error:
                operation(configured)
            assert private_error.value.code is LocalStateErrorCode.WRONG_TYPE
            _assert_path_free(private_error.value, target, linked, configured)

        with pytest.raises(LocalStateError) as group_error:
            validate_socket_group_parent(configured, group_name)
        assert group_error.value.code is LocalStateErrorCode.OPERATION_FAILED

        assert stat.S_ISSOCK(os.lstat(socket_path).st_mode)
        assert _mode(shared_parent) == 0o710
        _assert_path_free(group_error.value, target, linked, configured)
    finally:
        listener.close()
        socket_path.unlink(missing_ok=True)


def test_config_helpers_refuse_intermediate_symlink_before_target_repair(
    tmp_path: Path,
) -> None:
    target = tmp_path / "protected-target"
    state = target / "nested" / "state"
    state.mkdir(parents=True)
    os.chmod(state, 0o777)
    db_path = state / "tendwire.db"
    db_path.write_bytes(b"database")
    os.chmod(db_path, 0o666)
    linked = tmp_path / "linked"
    linked.symlink_to(target, target_is_directory=True)
    configured_state = linked / "nested" / "state"
    configured_db = configured_state / "tendwire.db"

    report = inspect_config_state(configured_state, configured_db)
    assert not report.ok
    _assert_path_free(report, target, linked, configured_state, configured_db)

    with pytest.raises(LocalStateError) as caught:
        repair_config_state(configured_state, configured_db)
    assert caught.value.code is LocalStateErrorCode.WRONG_TYPE
    assert _mode(state) == 0o777
    assert _mode(db_path) == 0o666
    assert db_path.read_bytes() == b"database"
    _assert_path_free(caught.value, target, linked, configured_state, configured_db)


def test_secure_component_creation_handles_absolute_and_controlled_relative_paths(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    absolute_db = tmp_path / "absolute" / "deep" / "state" / "tendwire.db"
    prepared = prepare_sqlite_family(absolute_db)
    assert prepared[0].state is PermissionState.CREATED
    assert absolute_db.is_file()
    for directory in (
        tmp_path / "absolute",
        tmp_path / "absolute" / "deep",
        tmp_path / "absolute" / "deep" / "state",
    ):
        assert _mode(directory) == 0o700

    monkeypatch.chdir(tmp_path)
    relative_socket = Path("relative") / "deep" / "sockets" / "daemon.sock"
    result = prepare_private_socket_parent(relative_socket)
    assert result.state is PermissionState.CREATED
    assert _mode(tmp_path / "relative") == 0o700
    assert _mode(tmp_path / "relative" / "deep") == 0o700
    assert _mode(tmp_path / "relative" / "deep" / "sockets") == 0o700

    parent_fd, leaf = local_state.open_resolved_parent(relative_socket)
    try:
        assert leaf == "daemon.sock"
        assert os.path.samefile(
            f"/proc/self/fd/{parent_fd}",
            tmp_path / "relative" / "deep" / "sockets",
        )
        assert local_state.proc_fd_path(parent_fd, leaf).endswith(
            "/daemon.sock"
        )
    finally:
        os.close(parent_fd)


@pytest.mark.parametrize(
    "configured",
    (
        "state/../outside/value",
        "../outside/value",
        "state/value/",
        "",
    ),
)
def test_component_resolver_rejects_parent_escape_nul_and_empty_leaf(
    tmp_path: Path,
    configured: str,
) -> None:
    path = f"{tmp_path}/{configured}" if configured else ""
    with pytest.raises(LocalStateError) as caught:
        local_state.open_resolved_parent(path)
    assert caught.value.code is LocalStateErrorCode.INVALID_ENTRY_NAME
    _assert_path_free(caught.value, tmp_path)

    with pytest.raises(LocalStateError) as nul_error:
        local_state.open_resolved_parent(f"{tmp_path}/state/\x00/value")
    assert nul_error.value.code is LocalStateErrorCode.INVALID_ENTRY_NAME
    _assert_path_free(nul_error.value, tmp_path)


def test_concurrent_directory_winner_is_reopened_and_verified(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    base = tmp_path / "race"
    base.mkdir()
    destination = base / "winner" / "leaf"
    real_mkdir = os.mkdir
    raced = False

    def racing_mkdir(
        name: str | bytes,
        mode: int = 0o777,
        *,
        dir_fd: int | None = None,
    ) -> None:
        nonlocal raced
        if name == "winner" and not raced:
            raced = True
            real_mkdir(name, 0o700, dir_fd=dir_fd)
            raise FileExistsError(errno.EEXIST, "concurrent winner")
        real_mkdir(name, mode, dir_fd=dir_fd)

    supported = set(os.supports_dir_fd)
    supported.discard(real_mkdir)
    supported.add(racing_mkdir)
    monkeypatch.setattr(local_state.os, "supports_dir_fd", supported)
    monkeypatch.setattr(local_state.os, "mkdir", racing_mkdir)
    result = local_state.create_private_directory(
        destination,
        create_missing_parents=True,
    )

    assert raced
    assert result.state is PermissionState.CREATED
    assert _mode(base / "winner") == 0o700
    assert _mode(destination) == 0o700


def test_concurrent_non_directory_winner_fails_closed_and_cleans_fds(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    base = tmp_path / "race"
    base.mkdir()
    destination = base / "winner" / "leaf"
    real_mkdir = os.mkdir
    raced = False
    before = set(os.listdir("/proc/self/fd"))

    def racing_mkdir(
        name: str | bytes,
        mode: int = 0o777,
        *,
        dir_fd: int | None = None,
    ) -> None:
        nonlocal raced
        if name == "winner" and not raced:
            raced = True
            fd = os.open(
                name,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                0o600,
                dir_fd=dir_fd,
            )
            os.close(fd)
            raise FileExistsError(errno.EEXIST, "concurrent non-directory")
        real_mkdir(name, mode, dir_fd=dir_fd)

    supported = set(os.supports_dir_fd)
    supported.discard(real_mkdir)
    supported.add(racing_mkdir)
    monkeypatch.setattr(local_state.os, "supports_dir_fd", supported)
    monkeypatch.setattr(local_state.os, "mkdir", racing_mkdir)
    with pytest.raises(LocalStateError) as caught:
        local_state.create_private_directory(
            destination,
            create_missing_parents=True,
        )

    assert raced
    assert caught.value.code is LocalStateErrorCode.WRONG_TYPE
    assert (base / "winner").is_file()
    assert not destination.exists()
    assert set(os.listdir("/proc/self/fd")) == before
    _assert_path_free(caught.value, base, destination)


def test_prepare_and_open_private_directory_returns_exact_created_and_repaired_inode(
    tmp_path: Path,
) -> None:
    created_path = tmp_path / "created" / "deep" / "state"
    created_fd, created = local_state.prepare_and_open_private_directory(created_path)
    try:
        assert created.state is PermissionState.CREATED
        assert created.mode == 0o700
        assert entry_identity(os.fstat(created_fd)) == entry_identity(
            os.lstat(created_path)
        )
        assert stat.S_IMODE(os.fstat(created_fd).st_mode) == 0o700
    finally:
        os.close(created_fd)

    repaired_path = tmp_path / "repaired"
    repaired_path.mkdir()
    os.chmod(repaired_path, 0o777)
    expected = entry_identity(os.lstat(repaired_path))
    repaired_fd, repaired = local_state.prepare_and_open_private_directory(
        repaired_path
    )
    try:
        assert repaired.state is PermissionState.REPAIRED
        assert repaired.mode == 0o700
        assert entry_identity(os.fstat(repaired_fd)) == expected
        assert entry_identity(os.lstat(repaired_path)) == expected
        assert stat.S_IMODE(os.fstat(repaired_fd).st_mode) == 0o700
    finally:
        os.close(repaired_fd)


def test_prepare_resolved_private_parent_returns_exact_parent_and_leaf(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "new" / "deep" / "state" / "tendwire.db"
    parent_fd, leaf, result = local_state.prepare_resolved_private_parent(db_path)
    try:
        assert leaf == "tendwire.db"
        assert result.state is PermissionState.CREATED
        assert result.mode == 0o700
        assert entry_identity(os.fstat(parent_fd)) == entry_identity(
            os.lstat(db_path.parent)
        )
        assert stat.S_IMODE(os.fstat(parent_fd).st_mode) == 0o700
        assert local_state.proc_fd_path(parent_fd, leaf).endswith("/tendwire.db")
    finally:
        os.close(parent_fd)


def test_retained_directory_helpers_refuse_intermediate_symlink_unchanged(
    tmp_path: Path,
) -> None:
    target = tmp_path / "protected-target"
    nested = target / "nested"
    nested.mkdir(parents=True)
    os.chmod(nested, 0o755)
    sentinel = nested / "sentinel"
    sentinel.write_bytes(b"unchanged")
    linked = tmp_path / "linked"
    linked.symlink_to(target, target_is_directory=True)

    with pytest.raises(LocalStateError) as directory_error:
        local_state.prepare_and_open_private_directory(linked / "nested" / "state")
    with pytest.raises(LocalStateError) as parent_error:
        local_state.prepare_resolved_private_parent(
            linked / "nested" / "tendwire.db"
        )

    assert directory_error.value.code is LocalStateErrorCode.WRONG_TYPE
    assert parent_error.value.code is LocalStateErrorCode.WRONG_TYPE
    assert sentinel.read_bytes() == b"unchanged"
    assert _mode(nested) == 0o755
    assert not (nested / "state").exists()
    _assert_path_free(directory_error.value, target, linked)
    _assert_path_free(parent_error.value, target, linked)


def test_proc_fd_path_has_no_non_linux_fallback(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    parent_fd, leaf = local_state.open_resolved_parent(tmp_path / "value")

    try:
        monkeypatch.setattr(local_state.sys, "platform", "not-linux")
        with pytest.raises(LocalStateError) as caught:
            local_state.proc_fd_path(parent_fd, leaf)
        assert caught.value.code is LocalStateErrorCode.UNSUPPORTED_PLATFORM
        _assert_path_free(caught.value, tmp_path)
    finally:
        os.close(parent_fd)


def test_canonical_path_from_fd_tracks_validated_directory_after_rename(
    tmp_path: Path,
) -> None:
    original = tmp_path / "original"
    original.mkdir()
    parent_fd, leaf = local_state.open_resolved_parent(original / "state.db")
    moved = tmp_path / "moved"

    try:
        original.rename(moved)
        resolved = Path(local_state.canonical_path_from_fd(parent_fd, leaf))
        assert resolved == moved / "state.db"
    finally:
        os.close(parent_fd)


def test_path_wrappers_close_every_resolved_component_fd(tmp_path: Path) -> None:
    db_path = tmp_path / "deep" / "state" / "tendwire.db"
    prepare_sqlite_family(db_path)
    socket_path = tmp_path / "deep" / "sockets" / "daemon.sock"
    prepare_private_socket_parent(socket_path)
    before = set(os.listdir("/proc/self/fd"))

    for _ in range(20):
        inspect_sqlite_family(db_path)
        inspect_owned_socket(socket_path)
        inspect_private_directory(db_path.parent)

    assert set(os.listdir("/proc/self/fd")) == before


def test_sqlite_replacement_lifecycle_is_private_and_retains_stricter_mode(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    previous_umask = os.umask(0)
    state_dir, source, parent_fd = _sqlite_replacement_source(tmp_path, mode=0o400)
    sync_calls: list[int] = []
    real_fsync = os.fsync

    def recording_fsync(fd: int) -> None:
        sync_calls.append(fd)
        real_fsync(fd)

    monkeypatch.setattr(local_state.os, "fsync", recording_fsync)
    try:
        source_identity = entry_identity(os.lstat(source))
        handle = prepare_private_sqlite_replacement_at(
            parent_fd,
            basename=source.name,
            retained_mode=0o600,
        )
        replacement = state_dir / handle._replacement_name
        assert replacement.is_file()
        assert _mode(state_dir) == 0o700
        assert _mode(replacement) == 0o600
        assert str(tmp_path) not in repr(handle)
        assert source.name not in repr(handle)

        with release_private_sqlite_replacement_at(handle) as (released, target):
            assert not replacement.exists()
            assert target.startswith("/proc/self/fd/")
            _create_replacement_output(target)
            assert _mode(replacement) == 0o600
        created = verify_created_private_sqlite_replacement_at(released)
        assert _mode(replacement) == 0o400
        published_identity = publish_private_sqlite_replacement_at(
            created,
            expected_source=source_identity,
        )
        assert published_identity == entry_identity(os.lstat(source))
        assert source.read_bytes() == b"replacement"
        assert _mode(source) == 0o400
        assert not replacement.exists()
        cleanup_private_sqlite_replacement_at(created)
    finally:
        os.close(parent_fd)
        os.umask(previous_umask)

    assert len(sync_calls) >= 6
def test_sqlite_replacement_release_is_vacuum_into_compatible(
    tmp_path: Path,
) -> None:
    previous_umask = os.umask(0)
    state_dir = tmp_path / "sqlite-state"
    state_dir.mkdir(mode=0o700)
    source = state_dir / "tendwire.db"
    connection = sqlite3.connect(source)
    connection.execute("CREATE TABLE durable (value TEXT NOT NULL)")
    connection.execute("INSERT INTO durable VALUES ('preserved')")
    connection.commit()
    connection.close()
    os.chmod(source, 0o600)
    parent_fd = os.open(state_dir, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        source_identity = entry_identity(os.lstat(source))
        handle = prepare_private_sqlite_replacement_at(
            parent_fd,
            basename=source.name,
            retained_mode=0o600,
        )
        replacement = state_dir / handle._replacement_name
        with release_private_sqlite_replacement_at(handle) as (released, target):
            assert not replacement.exists()
            connection = sqlite3.connect(source)
            connection.execute("VACUUM INTO ?", (target,))
            connection.close()
            assert replacement.is_file()
            assert _mode(replacement) == 0o600
        created = verify_created_private_sqlite_replacement_at(released)
        replacement_connection = sqlite3.connect(replacement)
        assert replacement_connection.execute("PRAGMA quick_check").fetchone() == ("ok",)
        assert replacement_connection.execute("SELECT value FROM durable").fetchone() == (
            "preserved",
        )
        replacement_connection.close()
        publish_private_sqlite_replacement_at(
            created,
            expected_source=source_identity,
        )
        published_connection = sqlite3.connect(source)
        assert published_connection.execute("PRAGMA quick_check").fetchone() == ("ok",)
        assert published_connection.execute("SELECT value FROM durable").fetchone() == (
            "preserved",
        )
        published_connection.close()
    finally:
        os.close(parent_fd)
        os.umask(previous_umask)




def test_sqlite_replacement_tracks_renamed_pinned_parent(tmp_path: Path) -> None:
    previous_umask = os.umask(0)
    state_dir, source, parent_fd = _sqlite_replacement_source(tmp_path)
    moved_dir = tmp_path / "renamed-state"
    try:
        source_identity = entry_identity(os.lstat(source))
        state_dir.rename(moved_dir)
        moved_source = moved_dir / source.name
        handle = prepare_private_sqlite_replacement_at(
            parent_fd,
            basename=source.name,
            retained_mode=0o600,
        )
        with release_private_sqlite_replacement_at(handle) as (released, target):
            _create_replacement_output(target, b"renamed-parent-output")
        created = verify_created_private_sqlite_replacement_at(released)
        publish_private_sqlite_replacement_at(
            created,
            expected_source=source_identity,
        )
        assert moved_source.read_bytes() == b"renamed-parent-output"
    finally:
        os.close(parent_fd)
        os.umask(previous_umask)


@pytest.mark.parametrize(
    ("source_kind", "expected_code"),
    [
        ("symlink", LocalStateErrorCode.WRONG_TYPE),
        ("directory", LocalStateErrorCode.WRONG_TYPE),
        ("hardlink", LocalStateErrorCode.ENTRY_CHANGED),
        ("broad_mode", LocalStateErrorCode.INSECURE_MODE),
    ],
)
def test_sqlite_replacement_refuses_unsafe_source_before_reservation(
    tmp_path: Path,
    source_kind: str,
    expected_code: LocalStateErrorCode,
) -> None:
    previous_umask = os.umask(0)
    state_dir = tmp_path / "sqlite-state"
    state_dir.mkdir(mode=0o700)
    source = state_dir / "tendwire.db"
    protected = state_dir / "protected"
    protected.write_bytes(b"protected")
    os.chmod(protected, 0o600)
    if source_kind == "symlink":
        source.symlink_to(protected)
    elif source_kind == "directory":
        source.mkdir(mode=0o700)
    elif source_kind == "hardlink":
        os.link(protected, source)
    else:
        source.write_bytes(b"source")
        os.chmod(source, 0o644)
    parent_fd = os.open(state_dir, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        with pytest.raises(LocalStateError) as caught:
            prepare_private_sqlite_replacement_at(
                parent_fd,
                basename=source.name,
                retained_mode=0o600,
            )
        assert caught.value.code is expected_code
        assert protected.read_bytes() == b"protected"
        assert not tuple(state_dir.glob(".tendwire-sqlite-*.vacuum"))
        _assert_path_free(caught.value, state_dir, source, protected)
    finally:
        os.close(parent_fd)
        os.umask(previous_umask)


def test_sqlite_replacement_refuses_wrong_owner_and_insecure_parent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    previous_umask = os.umask(0)
    state_dir, source, parent_fd = _sqlite_replacement_source(tmp_path)
    try:
        os.chmod(state_dir, 0o770)
        with pytest.raises(LocalStateError) as broad:
            prepare_private_sqlite_replacement_at(
                parent_fd,
                basename=source.name,
                retained_mode=0o600,
            )
        assert broad.value.code is LocalStateErrorCode.INSECURE_MODE
        os.chmod(state_dir, 0o700)

        monkeypatch.setattr(local_state.os, "geteuid", lambda: os.getuid() + 1)
        with pytest.raises(LocalStateError) as owner:
            prepare_private_sqlite_replacement_at(
                parent_fd,
                basename=source.name,
                retained_mode=0o600,
            )
        assert owner.value.code is LocalStateErrorCode.WRONG_OWNER
        assert source.read_bytes() == b"original-sqlite-content"
        assert not tuple(state_dir.glob(".tendwire-sqlite-*.vacuum"))
        _assert_path_free(broad.value, state_dir, source)
        _assert_path_free(owner.value, state_dir, source)
    finally:
        os.close(parent_fd)
        os.umask(previous_umask)


def test_sqlite_replacement_refuses_preexisting_reserved_name(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    previous_umask = os.umask(0)
    state_dir, source, parent_fd = _sqlite_replacement_source(tmp_path)
    token = "a" * 32
    preexisting = state_dir / f".tendwire-sqlite-{token}.vacuum"
    preexisting.write_bytes(b"do-not-replace")
    os.chmod(preexisting, 0o600)
    monkeypatch.setattr(local_state.secrets, "token_hex", lambda _size: token)
    try:
        with pytest.raises(LocalStateError) as caught:
            prepare_private_sqlite_replacement_at(
                parent_fd,
                basename=source.name,
                retained_mode=0o600,
            )
        assert caught.value.code is LocalStateErrorCode.ENTRY_EXISTS
        assert preexisting.read_bytes() == b"do-not-replace"
        assert source.read_bytes() == b"original-sqlite-content"
        _assert_path_free(caught.value, state_dir, source, preexisting)
    finally:
        os.close(parent_fd)
        os.umask(previous_umask)


def test_sqlite_replacement_release_requires_exact_reservation_identity(
    tmp_path: Path,
) -> None:
    previous_umask = os.umask(0)
    state_dir, source, parent_fd = _sqlite_replacement_source(tmp_path)
    try:
        handle = prepare_private_sqlite_replacement_at(
            parent_fd,
            basename=source.name,
            retained_mode=0o600,
        )
        replacement = state_dir / handle._replacement_name
        moved = state_dir / "moved-reservation"
        replacement.rename(moved)
        replacement.write_bytes(b"foreign-entry")
        os.chmod(replacement, 0o600)

        with pytest.raises(LocalStateError) as caught:
            with release_private_sqlite_replacement_at(handle):
                pytest.fail("unsafe reservation was released")
        assert caught.value.code is LocalStateErrorCode.ENTRY_CHANGED
        assert replacement.read_bytes() == b"foreign-entry"
        assert moved.exists()
        assert source.read_bytes() == b"original-sqlite-content"
        with pytest.raises(LocalStateError) as cleanup:
            cleanup_private_sqlite_replacement_at(handle)
        assert cleanup.value.code is LocalStateErrorCode.ENTRY_CHANGED
    finally:
        os.close(parent_fd)
        os.umask(previous_umask)


@pytest.mark.parametrize(
    ("created_kind", "expected_code"),
    [
        ("symlink", LocalStateErrorCode.WRONG_TYPE),
        ("directory", LocalStateErrorCode.WRONG_TYPE),
        ("hardlink", LocalStateErrorCode.ENTRY_CHANGED),
        ("broad_mode", LocalStateErrorCode.INSECURE_MODE),
    ],
)
def test_sqlite_replacement_verify_created_refuses_unsafe_output(
    tmp_path: Path,
    created_kind: str,
    expected_code: LocalStateErrorCode,
) -> None:
    previous_umask = os.umask(0)
    state_dir, source, parent_fd = _sqlite_replacement_source(tmp_path)
    extra = state_dir / "extra-link"
    try:
        handle = prepare_private_sqlite_replacement_at(
            parent_fd,
            basename=source.name,
            retained_mode=0o600,
        )
        with release_private_sqlite_replacement_at(handle) as (released, target):
            replacement = state_dir / handle._replacement_name
            if created_kind == "symlink":
                replacement.symlink_to(source)
            elif created_kind == "directory":
                replacement.mkdir(mode=0o700)
            else:
                _create_replacement_output(target)
                if created_kind == "hardlink":
                    os.link(replacement, extra)
        if created_kind == "broad_mode":
            os.chmod(replacement, 0o644)

        with pytest.raises(LocalStateError) as caught:
            verify_created_private_sqlite_replacement_at(released)
        assert caught.value.code is expected_code
        assert source.read_bytes() == b"original-sqlite-content"
        _assert_path_free(caught.value, state_dir, source, replacement)
    finally:
        os.close(parent_fd)
        os.umask(previous_umask)


def test_sqlite_replacement_publish_refuses_changed_source_and_cleans_exact_output(
    tmp_path: Path,
) -> None:
    previous_umask = os.umask(0)
    state_dir, source, parent_fd = _sqlite_replacement_source(tmp_path)
    moved_source = state_dir / "original-moved"
    try:
        source_identity = entry_identity(os.lstat(source))
        handle = prepare_private_sqlite_replacement_at(
            parent_fd,
            basename=source.name,
            retained_mode=0o600,
        )
        with release_private_sqlite_replacement_at(handle) as (released, target):
            _create_replacement_output(target)
        created = verify_created_private_sqlite_replacement_at(released)
        replacement = state_dir / handle._replacement_name
        source.rename(moved_source)
        source.write_bytes(b"new-source")
        os.chmod(source, 0o600)

        with pytest.raises(LocalStateError) as caught:
            publish_private_sqlite_replacement_at(
                created,
                expected_source=source_identity,
            )
        assert caught.value.code is LocalStateErrorCode.ENTRY_CHANGED
        assert source.read_bytes() == b"new-source"
        assert moved_source.read_bytes() == b"original-sqlite-content"
        assert replacement.read_bytes() == b"replacement"
        cleanup_private_sqlite_replacement_at(created)
        assert not replacement.exists()
        _assert_path_free(caught.value, state_dir, source, replacement)
    finally:
        os.close(parent_fd)
        os.umask(previous_umask)


def test_sqlite_replacement_cleanup_refuses_changed_artifact_identity(
    tmp_path: Path,
) -> None:
    previous_umask = os.umask(0)
    state_dir, source, parent_fd = _sqlite_replacement_source(tmp_path)
    moved = state_dir / "verified-output-moved"
    try:
        handle = prepare_private_sqlite_replacement_at(
            parent_fd,
            basename=source.name,
            retained_mode=0o600,
        )
        with release_private_sqlite_replacement_at(handle) as (released, target):
            _create_replacement_output(target)
        created = verify_created_private_sqlite_replacement_at(released)
        replacement = state_dir / handle._replacement_name
        replacement.rename(moved)
        replacement.write_bytes(b"foreign-output")
        os.chmod(replacement, 0o600)

        with pytest.raises(LocalStateError) as caught:
            cleanup_private_sqlite_replacement_at(created)
        assert caught.value.code is LocalStateErrorCode.ENTRY_CHANGED
        assert replacement.read_bytes() == b"foreign-output"
        assert moved.read_bytes() == b"replacement"
        assert source.read_bytes() == b"original-sqlite-content"
    finally:
        os.close(parent_fd)
        os.umask(previous_umask)


def test_sqlite_parent_available_bytes_uses_pinned_directory(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_dir, _source, parent_fd = _sqlite_replacement_source(tmp_path)
    moved_dir = tmp_path / "available-renamed"
    calls: list[int] = []
    real_fstatvfs = os.fstatvfs

    def recording_fstatvfs(fd: int) -> os.statvfs_result:
        calls.append(fd)
        return real_fstatvfs(fd)

    monkeypatch.setattr(local_state.os, "fstatvfs", recording_fstatvfs)
    try:
        state_dir.rename(moved_dir)
        values = real_fstatvfs(parent_fd)
        expected = values.f_bavail * (values.f_frsize or values.f_bsize)
        assert sqlite_parent_available_bytes_at(parent_fd) == expected
        assert calls == [parent_fd]
    finally:
        os.close(parent_fd)


def test_sqlite_parent_available_bytes_refuses_unverified_parent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_dir, source, parent_fd = _sqlite_replacement_source(tmp_path)
    file_fd = os.open(source, os.O_RDONLY | os.O_NOFOLLOW)
    calls: list[int] = []
    real_fstatvfs = os.fstatvfs

    def recording_fstatvfs(fd: int) -> os.statvfs_result:
        calls.append(fd)
        return real_fstatvfs(fd)

    monkeypatch.setattr(local_state.os, "fstatvfs", recording_fstatvfs)
    try:
        os.chmod(state_dir, 0o770)
        with pytest.raises(LocalStateError) as broad:
            sqlite_parent_available_bytes_at(parent_fd)
        assert broad.value.code is LocalStateErrorCode.INSECURE_MODE
        with pytest.raises(LocalStateError) as wrong_type:
            sqlite_parent_available_bytes_at(file_fd)
        assert wrong_type.value.code is LocalStateErrorCode.WRONG_TYPE
        assert calls == []
        _assert_path_free(broad.value, state_dir, source)
        _assert_path_free(wrong_type.value, state_dir, source)
    finally:
        os.close(file_fd)
        os.close(parent_fd)
