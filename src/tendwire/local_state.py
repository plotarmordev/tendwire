"""Small POSIX primitives for owner-private local state."""

from __future__ import annotations

import grp
import ctypes
import errno
import os
import secrets
import socket
import stat
import threading
from contextlib import contextmanager
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Iterable, Iterator, NoReturn

PRIVATE_DIRECTORY_MODE = 0o700
PRIVATE_FILE_MODE = 0o600
PRIVATE_SOCKET_MODE = 0o600
GROUP_SOCKET_MODE = 0o660


class LocalStateErrorCode(str, Enum):
    UNSUPPORTED_PLATFORM = "unsupported_platform"
    INVALID_ENTRY_NAME = "invalid_entry_name"
    MISSING_ENTRY = "missing_entry"
    ENTRY_EXISTS = "entry_exists"
    WRONG_TYPE = "wrong_type"
    WRONG_OWNER = "wrong_owner"
    WRONG_GROUP = "wrong_group"
    ENTRY_CHANGED = "entry_changed"
    INSECURE_MODE = "insecure_mode"
    INVALID_SOCKET_GROUP = "invalid_socket_group"
    INSECURE_SOCKET_PARENT = "insecure_socket_parent"
    PEER_VALIDATION_FAILED = "peer_validation_failed"
    OPERATION_FAILED = "operation_failed"


class LocalStateError(RuntimeError):
    def __init__(self, code: LocalStateErrorCode) -> None:
        super().__init__(code.value.replace("_", " "))
        self.code = code


class LocalStateKind(str, Enum):
    STATE_DIRECTORY = "state_directory"
    PRIVATE_FILE = "private_file"
    DATABASE = "database"
    DATABASE_WAL = "database_wal"
    DATABASE_SHM = "database_shm"
    DATABASE_JOURNAL = "database_journal"
    SOCKET = "socket"
    SOCKET_GROUP = "socket_group"


class EntryType(str, Enum):
    DIRECTORY = "directory"
    REGULAR_FILE = "regular_file"
    SOCKET = "socket"


class PermissionState(str, Enum):
    ABSENT = "absent"
    PRIVATE = "private"
    REPAIR_REQUIRED = "repair_required"
    CREATED = "created"
    REPAIRED = "repaired"


@dataclass(frozen=True)
class EntryIdentity:
    device: int
    inode: int


@dataclass(frozen=True)
class PermissionResult:
    kind: LocalStateKind
    state: PermissionState
    mode: int | None


@dataclass(frozen=True)
class ConfigStateReport:
    ok: bool
    entries: tuple[PermissionResult, ...]
    issues: tuple[object, ...] = ()


@dataclass(frozen=True)
class SocketGroup:
    group_id: int


_UMASK_LOCK = threading.RLock()


def _raise(code: LocalStateErrorCode) -> NoReturn:
    raise LocalStateError(code)


def _flags(*, directory: bool = False, path_only: bool = False) -> int:
    flags = getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    flags |= getattr(os, "O_PATH", os.O_RDONLY) if path_only else os.O_RDONLY
    if directory:
        flags |= getattr(os, "O_DIRECTORY", 0)
    return flags


def _leaf(name: str) -> str:
    if not name or name in {".", ".."} or "/" in name or "\x00" in name:
        _raise(LocalStateErrorCode.INVALID_ENTRY_NAME)
    return name


def entry_identity(value: os.stat_result) -> EntryIdentity:
    return EntryIdentity(int(value.st_dev), int(value.st_ino))


def identity_matches(identity: EntryIdentity, value: os.stat_result) -> bool:
    return identity == entry_identity(value)


def lstat_at(dir_fd: int, name: str) -> os.stat_result | None:
    try:
        return os.stat(_leaf(name), dir_fd=dir_fd, follow_symlinks=False)
    except FileNotFoundError:
        return None
    except OSError:
        _raise(LocalStateErrorCode.OPERATION_FAILED)


def _validate(value: os.stat_result, expected: EntryType) -> None:
    modes = {
        EntryType.DIRECTORY: stat.S_ISDIR,
        EntryType.REGULAR_FILE: stat.S_ISREG,
        EntryType.SOCKET: stat.S_ISSOCK,
    }
    if not modes[expected](value.st_mode):
        _raise(LocalStateErrorCode.WRONG_TYPE)
    if value.st_uid != os.geteuid():
        _raise(LocalStateErrorCode.WRONG_OWNER)


def validate_owned_regular_stat(value: os.stat_result) -> None:
    _validate(value, EntryType.REGULAR_FILE)


def verify_entry_identity(
    dir_fd: int, name: str, expected: EntryIdentity, *, expected_type: EntryType
) -> os.stat_result:
    current = lstat_at(dir_fd, name)
    if current is None:
        _raise(LocalStateErrorCode.MISSING_ENTRY)
    _validate(current, expected_type)
    if not identity_matches(expected, current):
        _raise(LocalStateErrorCode.ENTRY_CHANGED)
    return current


def open_resolved_parent(
    path: str | os.PathLike[str], *, create_missing: bool = False,
    path_only: bool = False,
) -> tuple[int, str]:
    raw = os.fspath(path)
    if not raw or "\x00" in raw:
        _raise(LocalStateErrorCode.INVALID_ENTRY_NAME)
    value = Path(raw)
    if any(part == ".." for part in value.parts):
        _raise(LocalStateErrorCode.INVALID_ENTRY_NAME)
    leaf = _leaf(value.name)
    parts = list(value.parent.parts)
    absolute = value.is_absolute()
    if absolute and parts and parts[0] == os.sep:
        parts = parts[1:]
    fd = os.open(os.sep if absolute else ".", _flags(directory=True))
    try:
        for part in parts:
            if part in {"", "."}:
                continue
            try:
                child = os.open(part, _flags(directory=True), dir_fd=fd)
            except FileNotFoundError:
                if not create_missing:
                    _raise(LocalStateErrorCode.MISSING_ENTRY)
                try:
                    os.mkdir(part, PRIVATE_DIRECTORY_MODE, dir_fd=fd)
                    child = os.open(part, _flags(directory=True), dir_fd=fd)
                except OSError:
                    _raise(LocalStateErrorCode.OPERATION_FAILED)
            except OSError:
                _raise(LocalStateErrorCode.WRONG_TYPE)
            os.close(fd)
            fd = child
        if path_only and hasattr(os, "O_PATH"):
            replacement = os.open(".", _flags(directory=True, path_only=True), dir_fd=fd)
            os.close(fd)
            fd = replacement
        return fd, leaf
    except BaseException:
        os.close(fd)
        raise


def proc_fd_path(dir_fd: int, name: str) -> str:
    _leaf(name)
    if not os.path.isdir("/proc/self/fd"):
        _raise(LocalStateErrorCode.UNSUPPORTED_PLATFORM)
    return f"/proc/self/fd/{dir_fd}/{name}"


def _directory_result(fd: int, kind: LocalStateKind) -> PermissionResult:
    value = os.fstat(fd)
    _validate(value, EntryType.DIRECTORY)
    mode = stat.S_IMODE(value.st_mode)
    if mode & ~PRIVATE_DIRECTORY_MODE:
        os.fchmod(fd, mode & PRIVATE_DIRECTORY_MODE)
        return PermissionResult(kind, PermissionState.REPAIRED, mode & PRIVATE_DIRECTORY_MODE)
    return PermissionResult(kind, PermissionState.PRIVATE, mode)


def prepare_and_open_private_directory(
    path: str | os.PathLike[str],
) -> tuple[int, PermissionResult]:
    parent_fd, name = open_resolved_parent(path, create_missing=True)
    created = False
    try:
        try:
            os.mkdir(name, PRIVATE_DIRECTORY_MODE, dir_fd=parent_fd)
            created = True
        except FileExistsError:
            pass
        fd = os.open(name, _flags(directory=True), dir_fd=parent_fd)
    except OSError:
        _raise(LocalStateErrorCode.WRONG_TYPE)
    finally:
        os.close(parent_fd)
    result = _directory_result(fd, LocalStateKind.STATE_DIRECTORY)
    if created:
        result = PermissionResult(result.kind, PermissionState.CREATED, result.mode)
    return fd, result


def inspect_private_file_at(dir_fd: int, name: str) -> PermissionResult:
    value = lstat_at(dir_fd, name)
    if value is None:
        return PermissionResult(LocalStateKind.PRIVATE_FILE, PermissionState.ABSENT, None)
    _validate(value, EntryType.REGULAR_FILE)
    mode = stat.S_IMODE(value.st_mode)
    state = PermissionState.REPAIR_REQUIRED if mode & ~PRIVATE_FILE_MODE else PermissionState.PRIVATE
    return PermissionResult(LocalStateKind.PRIVATE_FILE, state, mode)


def repair_private_file_at(dir_fd: int, name: str) -> PermissionResult:
    value = lstat_at(dir_fd, name)
    if value is None:
        _raise(LocalStateErrorCode.MISSING_ENTRY)
    _validate(value, EntryType.REGULAR_FILE)
    mode = stat.S_IMODE(value.st_mode) & PRIVATE_FILE_MODE
    os.chmod(_leaf(name), mode, dir_fd=dir_fd, follow_symlinks=False)
    return PermissionResult(LocalStateKind.PRIVATE_FILE, PermissionState.REPAIRED, mode)


def open_private_file_at(dir_fd: int, name: str, *, flags: int = os.O_RDONLY) -> int:
    try:
        fd = os.open(_leaf(name), flags | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0), dir_fd=dir_fd)
    except FileNotFoundError:
        _raise(LocalStateErrorCode.MISSING_ENTRY)
    except OSError:
        _raise(LocalStateErrorCode.OPERATION_FAILED)
    try:
        value = os.fstat(fd)
        _validate(value, EntryType.REGULAR_FILE)
        if value.st_nlink != 1 or stat.S_IMODE(value.st_mode) & ~PRIVATE_FILE_MODE:
            _raise(LocalStateErrorCode.INSECURE_MODE)
        return fd
    except BaseException:
        os.close(fd)
        raise


def _write_all(fd: int, content: bytes) -> None:
    view = memoryview(content)
    while view:
        try:
            written = os.write(fd, view)
        except InterruptedError:
            continue
        if written <= 0:
            _raise(LocalStateErrorCode.OPERATION_FAILED)
        view = view[written:]


def publish_private_file_at(dir_fd: int, name: str, content: bytes) -> PermissionResult:
    target = _leaf(name)
    temporary = f".{target}.tmp-{secrets.token_hex(16)}"
    try:
        fd = os.open(
            temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL
            | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0),
            PRIVATE_FILE_MODE, dir_fd=dir_fd,
        )
    except OSError:
        _raise(LocalStateErrorCode.OPERATION_FAILED)
    try:
        os.fchmod(fd, PRIVATE_FILE_MODE)
        _write_all(fd, bytes(content))
        os.fsync(fd)
    except BaseException:
        try:
            os.unlink(temporary, dir_fd=dir_fd)
        except OSError:
            pass
        raise
    finally:
        os.close(fd)
    libc = ctypes.CDLL(None, use_errno=True)
    rename_noreplace = getattr(libc, "renameat2", None)
    if rename_noreplace is None:
        os.unlink(temporary, dir_fd=dir_fd)
        _raise(LocalStateErrorCode.UNSUPPORTED_PLATFORM)
    rename_noreplace.argtypes = (
        ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p,
        ctypes.c_uint,
    )
    rename_noreplace.restype = ctypes.c_int
    if rename_noreplace(
        dir_fd, os.fsencode(temporary), dir_fd, os.fsencode(target), 1,
    ) != 0:
        error = ctypes.get_errno()
        os.unlink(temporary, dir_fd=dir_fd)
        _raise(
            LocalStateErrorCode.ENTRY_EXISTS
            if error == errno.EEXIST else LocalStateErrorCode.OPERATION_FAILED
        )
    os.fsync(dir_fd)
    return PermissionResult(LocalStateKind.PRIVATE_FILE, PermissionState.CREATED, PRIVATE_FILE_MODE)


def unlink_verified_entry(
    dir_fd: int, name: str, expected: EntryIdentity, *, expected_type: EntryType
) -> None:
    verify_entry_identity(dir_fd, name, expected, expected_type=expected_type)
    try:
        os.unlink(_leaf(name), dir_fd=dir_fd)
        os.fsync(dir_fd)
    except FileNotFoundError:
        _raise(LocalStateErrorCode.ENTRY_CHANGED)
    except OSError:
        _raise(LocalStateErrorCode.OPERATION_FAILED)


def prepare_resolved_private_parent(
    path: str | os.PathLike[str],
) -> tuple[int, str, PermissionResult]:
    parent_fd, leaf = open_resolved_parent(path, create_missing=True)
    result = _directory_result(parent_fd, LocalStateKind.STATE_DIRECTORY)
    return parent_fd, leaf, result


def prepare_sqlite_family_at(
    parent_fd: int, name: str, *, create_main: bool = False,
    _expected_main_identity: EntryIdentity | None = None,
) -> tuple[PermissionResult, ...]:
    results: list[PermissionResult] = []
    for index, (suffix, kind) in enumerate((
        ("", LocalStateKind.DATABASE), ("-wal", LocalStateKind.DATABASE_WAL),
        ("-shm", LocalStateKind.DATABASE_SHM), ("-journal", LocalStateKind.DATABASE_JOURNAL),
    )):
        leaf = _leaf(name + suffix)
        current = lstat_at(parent_fd, leaf)
        if current is None and index == 0 and create_main:
            try:
                fd = os.open(leaf, os.O_RDWR | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0), PRIVATE_FILE_MODE, dir_fd=parent_fd)
                os.fchmod(fd, PRIVATE_FILE_MODE)
                current = os.fstat(fd)
                os.close(fd)
            except OSError:
                _raise(LocalStateErrorCode.OPERATION_FAILED)
            state = PermissionState.CREATED
        elif current is None and index == 0:
            _raise(LocalStateErrorCode.MISSING_ENTRY)
        elif current is None:
            results.append(PermissionResult(kind, PermissionState.ABSENT, None))
            continue
        else:
            state = PermissionState.PRIVATE
        _validate(current, EntryType.REGULAR_FILE)
        if current.st_nlink != 1:
            _raise(LocalStateErrorCode.INSECURE_MODE)
        if index == 0 and _expected_main_identity is not None and not identity_matches(_expected_main_identity, current):
            _raise(LocalStateErrorCode.ENTRY_CHANGED)
        mode = stat.S_IMODE(current.st_mode)
        if mode & ~PRIVATE_FILE_MODE:
            mode &= PRIVATE_FILE_MODE
            os.chmod(leaf, mode, dir_fd=parent_fd, follow_symlinks=False)
            state = PermissionState.REPAIRED
        results.append(PermissionResult(kind, state, mode))
    return tuple(results)


def validate_private_socket_parent_at(parent_fd: int) -> PermissionResult:
    value = os.fstat(parent_fd)
    _validate(value, EntryType.DIRECTORY)
    mode = stat.S_IMODE(value.st_mode)
    if mode & 0o077:
        _raise(LocalStateErrorCode.INSECURE_SOCKET_PARENT)
    return PermissionResult(LocalStateKind.STATE_DIRECTORY, PermissionState.PRIVATE, mode)


def resolve_socket_group(name: str | None) -> SocketGroup | None:
    if name is None:
        return None
    try:
        group_id = grp.getgrnam(str(name)).gr_gid
    except (KeyError, ValueError):
        _raise(LocalStateErrorCode.INVALID_SOCKET_GROUP)
    if group_id not in {os.getegid(), *os.getgroups()}:
        _raise(LocalStateErrorCode.INVALID_SOCKET_GROUP)
    return SocketGroup(group_id)


def validate_socket_group_parent_at(parent_fd: int, name: str | None) -> PermissionResult:
    group = resolve_socket_group(name)
    if group is None:
        _raise(LocalStateErrorCode.INVALID_SOCKET_GROUP)
    value = os.fstat(parent_fd)
    _validate(value, EntryType.DIRECTORY)
    mode = stat.S_IMODE(value.st_mode)
    if (
        value.st_uid != os.geteuid()
        or value.st_gid != group.group_id
        or mode != 0o710
    ):
        _raise(LocalStateErrorCode.INSECURE_SOCKET_PARENT)
    return PermissionResult(LocalStateKind.SOCKET_GROUP, PermissionState.PRIVATE, mode)


@contextmanager
def socket_bind_umask(socket_group: str | None = None) -> Iterator[SocketGroup | None]:
    group = resolve_socket_group(socket_group)
    with _UMASK_LOCK:
        previous = os.umask(0o007 if group else 0o077)
        try:
            yield group
        finally:
            os.umask(previous)


def owned_socket_identity_at(parent_fd: int, name: str) -> EntryIdentity | None:
    value = lstat_at(parent_fd, name)
    if value is None:
        return None
    _validate(value, EntryType.SOCKET)
    return entry_identity(value)


def pin_owned_socket_at(parent_fd: int, name: str) -> tuple[int, EntryIdentity] | None:
    if lstat_at(parent_fd, name) is None:
        return None
    try:
        fd = os.open(_leaf(name), _flags(path_only=True), dir_fd=parent_fd)
    except OSError:
        _raise(LocalStateErrorCode.OPERATION_FAILED)
    try:
        value = os.fstat(fd)
        _validate(value, EntryType.SOCKET)
        return fd, entry_identity(value)
    except BaseException:
        os.close(fd)
        raise


def pin_socket_for_client_at(
    parent_fd: int, name: str, socket_group: str | None,
) -> tuple[int, EntryIdentity, int]:
    group = resolve_socket_group(socket_group)
    if group is None:
        validate_private_socket_parent_at(parent_fd)
    else:
        parent = os.fstat(parent_fd)
        if not stat.S_ISDIR(parent.st_mode):
            _raise(LocalStateErrorCode.WRONG_TYPE)
        if parent.st_gid != group.group_id or stat.S_IMODE(parent.st_mode) != 0o710:
            _raise(LocalStateErrorCode.INSECURE_SOCKET_PARENT)
    try:
        fd = os.open(_leaf(name), _flags(path_only=True), dir_fd=parent_fd)
    except FileNotFoundError:
        _raise(LocalStateErrorCode.MISSING_ENTRY)
    except OSError:
        _raise(LocalStateErrorCode.OPERATION_FAILED)
    try:
        value = os.fstat(fd)
        if group is None:
            _validate(value, EntryType.SOCKET)
        elif not stat.S_ISSOCK(value.st_mode):
            _raise(LocalStateErrorCode.WRONG_TYPE)
        elif value.st_uid != parent.st_uid:
            _raise(LocalStateErrorCode.WRONG_OWNER)
        elif value.st_gid != group.group_id:
            _raise(LocalStateErrorCode.WRONG_GROUP)
        expected_mode = GROUP_SOCKET_MODE if group else PRIVATE_SOCKET_MODE
        if stat.S_IMODE(value.st_mode) != expected_mode:
            _raise(LocalStateErrorCode.INSECURE_MODE)
        return fd, entry_identity(value), int(value.st_uid)
    except BaseException:
        os.close(fd)
        raise


def enforce_bound_socket_permissions_at(
    parent_fd: int, name: str, *, socket_group: str | None,
    expected: EntryIdentity,
) -> PermissionResult:
    current = verify_entry_identity(parent_fd, name, expected, expected_type=EntryType.SOCKET)
    group = resolve_socket_group(socket_group)
    mode = GROUP_SOCKET_MODE if group else PRIVATE_SOCKET_MODE
    if group is not None and current.st_gid != group.group_id:
        os.chown(_leaf(name), -1, group.group_id, dir_fd=parent_fd, follow_symlinks=False)
    os.chmod(_leaf(name), mode, dir_fd=parent_fd, follow_symlinks=False)
    verify_entry_identity(parent_fd, name, expected, expected_type=EntryType.SOCKET)
    return PermissionResult(LocalStateKind.SOCKET, PermissionState.REPAIRED, mode)


def unlink_verified_socket_at(parent_fd: int, name: str, expected: EntryIdentity) -> None:
    unlink_verified_entry(parent_fd, name, expected, expected_type=EntryType.SOCKET)


def repair_config_state(
    data_dir: str | os.PathLike[str], db_path: str | os.PathLike[str] | None,
    *, private_files: Iterable[str | os.PathLike[str]] = (),
) -> ConfigStateReport:
    staged: list[tuple[int, str | None, EntryIdentity, EntryType, LocalStateKind, int]] = []
    descriptors: set[int] = set()

    def open_parent(path: str | os.PathLike[str]) -> tuple[int, str] | None:
        try:
            opened = open_resolved_parent(path)
        except LocalStateError as exc:
            if exc.code is LocalStateErrorCode.MISSING_ENTRY:
                return None
            raise
        descriptors.add(opened[0])
        return opened

    def stage_leaf(
        parent_fd: int, name: str, expected: EntryType,
        kind: LocalStateKind, maximum: int,
    ) -> bool:
        value = lstat_at(parent_fd, name)
        if value is None:
            return False
        _validate(value, expected)
        if expected is EntryType.REGULAR_FILE and value.st_nlink != 1:
            _raise(LocalStateErrorCode.INSECURE_MODE)
        staged.append((parent_fd, name, entry_identity(value), expected, kind, maximum))
        return True

    try:
        opened = open_parent(data_dir)
        if opened is not None:
            stage_leaf(
                opened[0], opened[1], EntryType.DIRECTORY,
                LocalStateKind.STATE_DIRECTORY, PRIVATE_DIRECTORY_MODE,
            )
        if db_path is not None:
            opened = open_parent(db_path)
            if opened is not None and lstat_at(opened[0], opened[1]) is not None:
                parent = os.fstat(opened[0])
                _validate(parent, EntryType.DIRECTORY)
                staged.append((
                    opened[0], None, entry_identity(parent), EntryType.DIRECTORY,
                    LocalStateKind.STATE_DIRECTORY, PRIVATE_DIRECTORY_MODE,
                ))
                for suffix, kind in (
                    ("", LocalStateKind.DATABASE),
                    ("-wal", LocalStateKind.DATABASE_WAL),
                    ("-shm", LocalStateKind.DATABASE_SHM),
                    ("-journal", LocalStateKind.DATABASE_JOURNAL),
                ):
                    stage_leaf(
                        opened[0], opened[1] + suffix, EntryType.REGULAR_FILE,
                        kind, PRIVATE_FILE_MODE,
                    )
        for path in private_files:
            opened = open_parent(path)
            if opened is not None:
                stage_leaf(
                    opened[0], opened[1], EntryType.REGULAR_FILE,
                    LocalStateKind.PRIVATE_FILE, PRIVATE_FILE_MODE,
                )

        results: list[PermissionResult] = []
        for parent_fd, name, identity, expected, kind, maximum in staged:
            current = (
                os.fstat(parent_fd)
                if name is None
                else verify_entry_identity(
                    parent_fd, name, identity, expected_type=expected
                )
            )
            if name is None and not identity_matches(identity, current):
                _raise(LocalStateErrorCode.ENTRY_CHANGED)
            mode = stat.S_IMODE(current.st_mode)
            state = PermissionState.PRIVATE
            if mode & ~maximum:
                narrowed = mode & maximum
                if name is None:
                    os.fchmod(parent_fd, narrowed)
                else:
                    os.chmod(name, narrowed, dir_fd=parent_fd, follow_symlinks=False)
                    verify_entry_identity(
                        parent_fd, name, identity, expected_type=expected
                    )
                mode = narrowed
                state = PermissionState.REPAIRED
            results.append(PermissionResult(kind, state, mode))
        return ConfigStateReport(True, tuple(results))
    finally:
        for fd in descriptors:
            try:
                os.close(fd)
            except OSError:
                pass


__all__ = tuple(name for name in globals() if not name.startswith("_"))
