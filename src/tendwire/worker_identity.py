"""Private installation identity for stable Herdr worker continuity."""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import secrets
from collections.abc import Callable
from pathlib import Path

from . import local_state

INSTALLATION_KEY_FILENAME = "installation.key"
INSTALLATION_KEY_MARKER_FILENAME = "installation.key.sha256"
INSTALLATION_KEY_SENTINEL_FILENAME = "installation.key.initialized"
INSTALLATION_KEY_BYTES = 32
_INSTALLATION_KEY_SENTINEL_CONTENT = b"1"
STABLE_KEY_VERSION = 1
STABLE_KEY_PREFIX = "wsk1_"
_HERDR_PUBLIC_ID_ALPHABET = frozenset("123456789ABCDEFGHJKMNPQRSTVWXYZ0")
_HERDR_HEX_WORKSPACE_ID_LENGTH = 14
_STABLE_KEY_DOMAIN = "tendwire.worker-stable-key"


class InstallationKeyError(RuntimeError):
    """The installation key cannot be used safely."""


def canonical_herdr_pane_identity(workspace_id: str | None, pane_id: str | None) -> tuple[str, str] | None:
    """Validate an authoritative Herdr workspace/public-pane identity."""
    if not isinstance(workspace_id, str) or not workspace_id.startswith("w"):
        return None
    workspace_number = workspace_id[1:]
    public_id_workspace = bool(workspace_number) and all(
        character in _HERDR_PUBLIC_ID_ALPHABET for character in workspace_number
    )
    current_hex_workspace = (
        len(workspace_number) == _HERDR_HEX_WORKSPACE_ID_LENGTH
        and all(character in "0123456789abcdef" for character in workspace_number)
    )
    if not public_id_workspace and not current_hex_workspace:
        return None
    if not isinstance(pane_id, str):
        return None
    prefix = f"{workspace_id}:p"
    if not pane_id.startswith(prefix):
        return None
    public_number = pane_id[len(prefix) :]
    if not public_number or any(character not in _HERDR_PUBLIC_ID_ALPHABET for character in public_number):
        return None
    return workspace_id, pane_id


def stable_worker_key(
    installation_key: bytes,
    *,
    backend: str,
    host_id: str,
    workspace_id: str,
    pane_id: str,
) -> str:
    """Derive the public opaque key without public sanitizers or binding hashes."""
    if len(installation_key) != INSTALLATION_KEY_BYTES:
        raise InstallationKeyError("installation identity is unavailable")
    message = json.dumps(
        {
            "backend": str(backend),
            "domain": _STABLE_KEY_DOMAIN,
            "host_id": str(host_id),
            "pane_id": pane_id,
            "version": STABLE_KEY_VERSION,
            "workspace_id": workspace_id,
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    digest = hmac.new(installation_key, message, hashlib.sha256).hexdigest()
    return f"{STABLE_KEY_PREFIX}{digest}"

def is_stable_worker_key(value: object) -> bool:
    """Return whether a value has the exact current public key shape."""
    if not isinstance(value, str) or not value.startswith(STABLE_KEY_PREFIX):
        return False
    digest = value[len(STABLE_KEY_PREFIX) :]
    return len(digest) == 64 and all(character in "0123456789abcdef" for character in digest)


def _identity_unavailable() -> InstallationKeyError:
    return InstallationKeyError("installation identity is unavailable")


def _verify_identity_entry(
    dir_fd: int,
    name: str,
    expected: local_state.EntryIdentity,
) -> None:
    local_state.verify_entry_identity(
        dir_fd,
        name,
        expected,
        expected_type=local_state.EntryType.REGULAR_FILE,
    )
    inspected = local_state.inspect_private_file_at(dir_fd, name)
    if inspected.state is local_state.PermissionState.REPAIR_REQUIRED:
        local_state.repair_private_file_at(dir_fd, name)
    local_state.verify_entry_identity(
        dir_fd,
        name,
        expected,
        expected_type=local_state.EntryType.REGULAR_FILE,
    )


def _identity_entry(
    dir_fd: int,
    name: str,
) -> local_state.EntryIdentity | None:
    inspected = local_state.inspect_private_file_at(dir_fd, name)
    if inspected.state is local_state.PermissionState.ABSENT:
        return None
    if inspected.state is local_state.PermissionState.REPAIR_REQUIRED:
        local_state.repair_private_file_at(dir_fd, name)
    current = local_state.lstat_at(dir_fd, name)
    if current is None:
        raise _identity_unavailable()
    expected = local_state.entry_identity(current)
    _verify_identity_entry(dir_fd, name, expected)
    return expected


def _open_data_dir(data_dir: Path) -> int:
    dir_fd, _result = local_state.prepare_and_open_private_directory(data_dir)
    return dir_fd


def _read_exact_file(
    dir_fd: int,
    name: str,
    expected_size: int,
) -> tuple[bytes, local_state.EntryIdentity]:
    expected = _identity_entry(dir_fd, name)
    if expected is None:
        raise _identity_unavailable()
    fd = local_state.open_private_file_at(dir_fd, name)
    try:
        if not local_state.identity_matches(expected, os.fstat(fd)):
            raise _identity_unavailable()
        chunks: list[bytes] = []
        remaining = expected_size + 1
        while remaining:
            try:
                chunk = os.read(fd, remaining)
            except InterruptedError:
                continue
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        content = b"".join(chunks)
    finally:
        os.close(fd)
    _verify_identity_entry(dir_fd, name, expected)
    if len(content) != expected_size:
        raise _identity_unavailable()
    return content, expected


def _create_or_read_file(
    dir_fd: int,
    name: str,
    content: bytes,
) -> tuple[bytes, local_state.EntryIdentity]:
    try:
        local_state.publish_private_file_at(dir_fd, name, content)
    except local_state.LocalStateError as exc:
        if exc.code not in {
            local_state.LocalStateErrorCode.ENTRY_EXISTS,
            local_state.LocalStateErrorCode.ENTRY_CHANGED,
        }:
            raise
    return _read_exact_file(dir_fd, name, len(content))


def _unlink_verified_entry(
    dir_fd: int,
    name: str,
    expected: local_state.EntryIdentity,
) -> None:
    local_state.unlink_verified_entry(
        dir_fd,
        name,
        expected,
        expected_type=local_state.EntryType.REGULAR_FILE,
    )

def load_or_create_installation_key(
    data_dir: Path,
    *,
    random_bytes: Callable[[int], bytes] = secrets.token_bytes,
) -> bytes:
    """Load a verified installation key or publish the first installation identity."""
    dir_fd = -1
    try:
        dir_fd = _open_data_dir(Path(data_dir))
        key_identity = _identity_entry(dir_fd, INSTALLATION_KEY_FILENAME)
        marker_identity = _identity_entry(dir_fd, INSTALLATION_KEY_MARKER_FILENAME)
        sentinel_identity = _identity_entry(dir_fd, INSTALLATION_KEY_SENTINEL_FILENAME)

        sentinel: bytes | None = None
        if sentinel_identity is not None:
            sentinel, sentinel_identity = _read_exact_file(
                dir_fd,
                INSTALLATION_KEY_SENTINEL_FILENAME,
                len(_INSTALLATION_KEY_SENTINEL_CONTENT),
            )
            if not hmac.compare_digest(sentinel, _INSTALLATION_KEY_SENTINEL_CONTENT):
                raise _identity_unavailable()
            if key_identity is None:
                key_identity = _identity_entry(dir_fd, INSTALLATION_KEY_FILENAME)
            if marker_identity is None:
                marker_identity = _identity_entry(
                    dir_fd,
                    INSTALLATION_KEY_MARKER_FILENAME,
                )
            if key_identity is None or marker_identity is None:
                raise _identity_unavailable()

        if key_identity is None and marker_identity is not None:
            # A concurrent creator publishes the key before its marker. Refresh
            # once so a coherent completed publication is not mistaken for loss.
            key_identity = _identity_entry(dir_fd, INSTALLATION_KEY_FILENAME)
        if key_identity is None and (
            marker_identity is not None or sentinel_identity is not None
        ):
            raise _identity_unavailable()

        if key_identity is None:
            try:
                candidate = bytes(random_bytes(INSTALLATION_KEY_BYTES))
            except Exception:
                raise _identity_unavailable() from None
            if len(candidate) != INSTALLATION_KEY_BYTES:
                raise _identity_unavailable()
            key, key_identity = _create_or_read_file(
                dir_fd,
                INSTALLATION_KEY_FILENAME,
                candidate,
            )
        else:
            key, key_identity = _read_exact_file(
                dir_fd,
                INSTALLATION_KEY_FILENAME,
                INSTALLATION_KEY_BYTES,
            )

        marker_expected = hashlib.sha256(key).hexdigest().encode("ascii")
        if marker_identity is None:
            marker, marker_identity = _create_or_read_file(
                dir_fd,
                INSTALLATION_KEY_MARKER_FILENAME,
                marker_expected,
            )
        else:
            marker, marker_identity = _read_exact_file(
                dir_fd,
                INSTALLATION_KEY_MARKER_FILENAME,
                len(marker_expected),
            )
        if not hmac.compare_digest(marker, marker_expected):
            raise _identity_unavailable()

        _verify_identity_entry(dir_fd, INSTALLATION_KEY_FILENAME, key_identity)
        _verify_identity_entry(
            dir_fd,
            INSTALLATION_KEY_MARKER_FILENAME,
            marker_identity,
        )

        if sentinel_identity is None:
            sentinel, sentinel_identity = _create_or_read_file(
                dir_fd,
                INSTALLATION_KEY_SENTINEL_FILENAME,
                _INSTALLATION_KEY_SENTINEL_CONTENT,
            )
        if sentinel is None or not hmac.compare_digest(
            sentinel,
            _INSTALLATION_KEY_SENTINEL_CONTENT,
        ):
            raise _identity_unavailable()

        _verify_identity_entry(dir_fd, INSTALLATION_KEY_FILENAME, key_identity)
        _verify_identity_entry(
            dir_fd,
            INSTALLATION_KEY_MARKER_FILENAME,
            marker_identity,
        )
        _verify_identity_entry(
            dir_fd,
            INSTALLATION_KEY_SENTINEL_FILENAME,
            sentinel_identity,
        )
        return key
    except InstallationKeyError:
        raise
    except (local_state.LocalStateError, OSError):
        raise _identity_unavailable() from None
    finally:
        if dir_fd >= 0:
            os.close(dir_fd)


def reset_installation_key(
    data_dir: Path,
    *,
    acknowledge_continuity_break: bool,
) -> None:
    """Explicitly reset a verified installation identity while all users are offline."""
    if acknowledge_continuity_break is not True:
        raise InstallationKeyError("installation identity reset was not acknowledged")

    dir_fd = -1
    try:
        dir_fd = _open_data_dir(Path(data_dir))
        key, key_identity = _read_exact_file(
            dir_fd,
            INSTALLATION_KEY_FILENAME,
            INSTALLATION_KEY_BYTES,
        )
        marker_expected = hashlib.sha256(key).hexdigest().encode("ascii")
        marker, marker_identity = _read_exact_file(
            dir_fd,
            INSTALLATION_KEY_MARKER_FILENAME,
            len(marker_expected),
        )
        sentinel, sentinel_identity = _read_exact_file(
            dir_fd,
            INSTALLATION_KEY_SENTINEL_FILENAME,
            len(_INSTALLATION_KEY_SENTINEL_CONTENT),
        )
        if not hmac.compare_digest(marker, marker_expected) or not hmac.compare_digest(
            sentinel,
            _INSTALLATION_KEY_SENTINEL_CONTENT,
        ):
            raise _identity_unavailable()

        _verify_identity_entry(dir_fd, INSTALLATION_KEY_FILENAME, key_identity)
        _verify_identity_entry(
            dir_fd,
            INSTALLATION_KEY_MARKER_FILENAME,
            marker_identity,
        )
        _verify_identity_entry(
            dir_fd,
            INSTALLATION_KEY_SENTINEL_FILENAME,
            sentinel_identity,
        )

        # Keep the sentinel until last: an interrupted reset either recovers
        # the intact key or remains fail-closed instead of rotating on load.
        _unlink_verified_entry(
            dir_fd,
            INSTALLATION_KEY_MARKER_FILENAME,
            marker_identity,
        )
        _unlink_verified_entry(dir_fd, INSTALLATION_KEY_FILENAME, key_identity)
        _unlink_verified_entry(
            dir_fd,
            INSTALLATION_KEY_SENTINEL_FILENAME,
            sentinel_identity,
        )
    except InstallationKeyError:
        raise
    except (local_state.LocalStateError, OSError):
        raise _identity_unavailable() from None
    finally:
        if dir_fd >= 0:
            os.close(dir_fd)
