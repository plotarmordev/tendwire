"""Focused contracts for the supported local-state security primitives."""

from __future__ import annotations

import grp
import os
import socket
import stat
import sys
from pathlib import Path

import pytest

from tendwire.daemon_api import DaemonUnavailable, _validate_connected_peer
from tendwire.local_state import (
    EntryType,
    LocalStateError,
    LocalStateErrorCode,
    PermissionState,
    enforce_bound_socket_permissions_at,
    entry_identity,
    inspect_private_file_at,
    open_private_file_at,
    pin_group_socket_for_client_at,
    pin_owned_socket_at,
    prepare_and_open_private_directory,
    prepare_resolved_private_parent,
    prepare_sqlite_family_at,
    publish_private_file_at,
    repair_config_state,
    socket_bind_umask,
    unlink_verified_entry,
    unlink_verified_socket_at,
    validate_private_socket_parent_at,
    validate_socket_group_parent_at,
)
from tendwire.worker_identity import (
    InstallationKeyError,
    load_or_create_installation_key,
    reset_installation_key,
)


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
    except BaseException:
        listener.close()
        raise
    return listener


def test_private_directory_and_file_are_nofollow_owner_only(tmp_path: Path) -> None:
    state = tmp_path / "state"
    fd, created = prepare_and_open_private_directory(state)
    try:
        assert created.state is PermissionState.CREATED
        assert _mode(state) == 0o700
        published = publish_private_file_at(fd, "secret", b"content")
        assert published.state is PermissionState.CREATED
        assert inspect_private_file_at(fd, "secret").mode == 0o600
        secret_fd = open_private_file_at(fd, "secret")
        try:
            assert os.read(secret_fd, 32) == b"content"
        finally:
            os.close(secret_fd)
        os.symlink("secret", state / "alias")
        with pytest.raises(LocalStateError):
            open_private_file_at(fd, "alias")
    finally:
        os.close(fd)


def test_private_sqlite_leaf_and_sidecars_are_repaired_without_following(
    tmp_path: Path,
) -> None:
    parent_fd, leaf, _ = prepare_resolved_private_parent(tmp_path / "state" / "db.sqlite")
    try:
        created = prepare_sqlite_family_at(parent_fd, leaf, create_main=True)
        assert created[0].state is PermissionState.CREATED
        wal_fd = os.open(leaf + "-wal", os.O_WRONLY | os.O_CREAT, 0o666, dir_fd=parent_fd)
        os.close(wal_fd)
        repaired = prepare_sqlite_family_at(parent_fd, leaf)
        assert repaired[1].state is PermissionState.REPAIRED
        assert repaired[1].mode == 0o600
        os.symlink(leaf, leaf + "-shm", dir_fd=parent_fd)
        with pytest.raises(LocalStateError) as raised:
            prepare_sqlite_family_at(parent_fd, leaf)
        assert raised.value.code is LocalStateErrorCode.WRONG_TYPE
    finally:
        os.close(parent_fd)


def test_installation_key_create_load_and_acknowledged_reset(tmp_path: Path) -> None:
    state = tmp_path / "identity"
    first = load_or_create_installation_key(state, random_bytes=lambda size: b"a" * size)
    assert load_or_create_installation_key(state) == first
    for name in ("installation.key", "installation.key.sha256", "installation.key.initialized"):
        assert _mode(state / name) == 0o600
    with pytest.raises(InstallationKeyError, match="acknowledged"):
        reset_installation_key(state, acknowledge_continuity_break=False)
    reset_installation_key(state, acknowledge_continuity_break=True)
    second = load_or_create_installation_key(state, random_bytes=lambda size: b"b" * size)
    assert second != first


def test_identity_verified_unlink_refuses_replacement(tmp_path: Path) -> None:
    parent_fd = os.open(tmp_path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        target = tmp_path / "target"
        target.write_bytes(b"first")
        expected = entry_identity(os.lstat(target))
        target.unlink()
        target.write_bytes(b"replacement")
        with pytest.raises(LocalStateError) as raised:
            unlink_verified_entry(
                parent_fd, "target", expected, expected_type=EntryType.REGULAR_FILE
            )
        assert raised.value.code is LocalStateErrorCode.ENTRY_CHANGED
        assert target.read_bytes() == b"replacement"
    finally:
        os.close(parent_fd)


def test_private_socket_parent_mode_pin_and_verified_unlink(tmp_path: Path) -> None:
    parent = tmp_path / "socket-parent"
    parent.mkdir(mode=0o700)
    parent_fd = os.open(parent, os.O_RDONLY | os.O_DIRECTORY)
    listener = _bind(parent / "daemon.sock")
    pinned_fd = -1
    try:
        assert validate_private_socket_parent_at(parent_fd).mode == 0o700
        identity = entry_identity(os.lstat(parent / "daemon.sock"))
        repaired = enforce_bound_socket_permissions_at(
            parent_fd, "daemon.sock", socket_group=None, expected=identity
        )
        assert repaired.mode == 0o600
        pinned = pin_owned_socket_at(parent_fd, "daemon.sock")
        assert pinned is not None
        pinned_fd, pinned_identity = pinned
        assert pinned_identity == identity
        unlink_verified_socket_at(parent_fd, "daemon.sock", identity)
        assert not (parent / "daemon.sock").exists()
    finally:
        if pinned_fd >= 0:
            os.close(pinned_fd)
        listener.close()
        os.close(parent_fd)


def test_group_socket_parent_mode_and_pin_validate_owner_group(tmp_path: Path) -> None:
    group_name = grp.getgrgid(os.getegid()).gr_name
    parent = tmp_path / "group-parent"
    parent.mkdir(mode=0o710)
    parent_fd = os.open(parent, os.O_RDONLY | os.O_DIRECTORY)
    with socket_bind_umask(group_name):
        listener = _bind(parent / "group.sock")
    pinned_fd = -1
    try:
        assert validate_socket_group_parent_at(parent_fd, group_name).mode == 0o710
        identity = entry_identity(os.lstat(parent / "group.sock"))
        result = enforce_bound_socket_permissions_at(
            parent_fd, "group.sock", socket_group=group_name, expected=identity
        )
        assert result.mode == 0o660
        pinned_fd, pinned_identity, owner_uid = pin_group_socket_for_client_at(
            parent_fd, "group.sock", group_name
        )
        assert pinned_identity == identity
        assert owner_uid == os.geteuid()
    finally:
        if pinned_fd >= 0:
            os.close(pinned_fd)
        listener.close()
        os.close(parent_fd)


def test_connected_peer_validation_accepts_owner_and_rejects_wrong_uid() -> None:
    left, right = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        _validate_connected_peer(left, os.geteuid())
        with pytest.raises(DaemonUnavailable):
            _validate_connected_peer(left, os.geteuid() + 1)
    finally:
        left.close()
        right.close()


def test_repair_config_state_only_narrows_existing_supported_entries(
    tmp_path: Path,
) -> None:
    state = tmp_path / "state"
    state.mkdir(mode=0o777)
    db = state / "db.sqlite"
    db.write_bytes(b"db")
    wal = state / "db.sqlite-wal"
    wal.write_bytes(b"wal")
    private = state / "installation.key"
    private.write_bytes(b"key")
    for path in (db, wal, private):
        os.chmod(path, 0o666)

    report = repair_config_state(state, db, private_files=(private, state / "missing"))
    assert report.ok
    assert _mode(state) == 0o700
    assert all(_mode(path) == 0o600 for path in (db, wal, private))
    assert not (state / "missing").exists()
    second = repair_config_state(state, db, private_files=(private,))
    assert second.ok
    assert all(item.state is PermissionState.PRIVATE for item in second.entries)
