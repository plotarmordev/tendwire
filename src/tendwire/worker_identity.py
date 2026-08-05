"""Private installation identity for stable Herdr worker continuity."""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import secrets
from collections.abc import Callable, Iterator
from contextlib import contextmanager
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


def canonical_herdr_pane_identity(
    workspace_id: str | None,
    pane_id: str | None,
) -> tuple[str, str] | None:
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
    if not public_number or any(
        character not in _HERDR_PUBLIC_ID_ALPHABET for character in public_number
    ):
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
    material = {
        "backend": str(backend),
        "domain": _STABLE_KEY_DOMAIN,
        "host_id": str(host_id),
        "pane_id": pane_id,
        "version": STABLE_KEY_VERSION,
        "workspace_id": workspace_id,
    }
    message = json.dumps(
        material,
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
    return len(digest) == 64 and all(
        character in "0123456789abcdef" for character in digest
    )


def _identity_unavailable() -> InstallationKeyError:
    return InstallationKeyError("installation identity is unavailable")


def _verify_entry(dir_fd: int, name: str, expected: local_state.EntryIdentity) -> None:
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


def _read_file(
    dir_fd: int,
    name: str,
    expected_size: int,
) -> tuple[bytes, local_state.EntryIdentity] | None:
    current = local_state.lstat_at(dir_fd, name)
    if current is None:
        return None
    expected = local_state.entry_identity(current)
    _verify_entry(dir_fd, name, expected)
    fd = local_state.open_private_file_at(dir_fd, name)
    try:
        if not local_state.identity_matches(expected, os.fstat(fd)):
            raise _identity_unavailable()
        with os.fdopen(fd, "rb", closefd=False) as stream:
            content = stream.read(expected_size + 1)
    finally:
        os.close(fd)
    _verify_entry(dir_fd, name, expected)
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
    result = _read_file(dir_fd, name, len(content))
    if result is None:
        raise _identity_unavailable()
    return result


_IDENTITY_FILES = (
    (INSTALLATION_KEY_FILENAME, INSTALLATION_KEY_BYTES),
    (INSTALLATION_KEY_MARKER_FILENAME, 64),
    (INSTALLATION_KEY_SENTINEL_FILENAME, len(_INSTALLATION_KEY_SENTINEL_CONTENT)),
)
_IdentityPart = tuple[bytes, local_state.EntryIdentity]
_IdentityParts = tuple[
    _IdentityPart | None,
    _IdentityPart | None,
    _IdentityPart | None,
]
_VerifiedIdentities = tuple[
    local_state.EntryIdentity,
    local_state.EntryIdentity,
    local_state.EntryIdentity,
]


def _parts(dir_fd: int) -> _IdentityParts:
    return tuple(_read_file(dir_fd, name, size) for name, size in _IDENTITY_FILES)


def _complete(
    dir_fd: int,
    parts: _IdentityParts,
) -> tuple[bytes, _VerifiedIdentities]:
    if any(item is None for item in parts):
        raise _identity_unavailable()
    key, marker, sentinel = parts
    assert key is not None and marker is not None and sentinel is not None
    expected_marker = hashlib.sha256(key[0]).hexdigest().encode("ascii")
    if not hmac.compare_digest(marker[0], expected_marker) or not hmac.compare_digest(
        sentinel[0], _INSTALLATION_KEY_SENTINEL_CONTENT,
    ):
        raise _identity_unavailable()
    identities = (key[1], marker[1], sentinel[1])
    for (name, _size), identity in zip(_IDENTITY_FILES, identities, strict=True):
        _verify_entry(dir_fd, name, identity)
    return key[0], identities


@contextmanager
def _identity_dir(data_dir: Path) -> Iterator[int]:
    dir_fd = -1
    try:
        dir_fd = local_state.prepare_and_open_private_directory(data_dir)[0]
        yield dir_fd
    except (local_state.LocalStateError, OSError):
        raise _identity_unavailable() from None
    finally:
        if dir_fd >= 0:
            os.close(dir_fd)


def load_or_create_installation_key(
    data_dir: Path,
    *,
    random_bytes: Callable[[int], bytes] = secrets.token_bytes,
) -> bytes:
    """Load a verified installation key or publish the first installation identity."""
    with _identity_dir(Path(data_dir)) as dir_fd:
        key, marker, sentinel = _parts(dir_fd)
        if key is None and (marker is not None or sentinel is not None):
            key = _read_file(
                dir_fd,
                INSTALLATION_KEY_FILENAME,
                INSTALLATION_KEY_BYTES,
            )
        if marker is None and sentinel is not None:
            marker = _read_file(dir_fd, INSTALLATION_KEY_MARKER_FILENAME, 64)
        if sentinel is None:
            if key is None:
                if marker is not None:
                    raise _identity_unavailable()
                try:
                    candidate = bytes(random_bytes(INSTALLATION_KEY_BYTES))
                except Exception:
                    raise _identity_unavailable() from None
                if len(candidate) != INSTALLATION_KEY_BYTES:
                    raise _identity_unavailable()
                key = _create_or_read_file(
                    dir_fd,
                    INSTALLATION_KEY_FILENAME,
                    candidate,
                )
            expected_marker = hashlib.sha256(key[0]).hexdigest().encode("ascii")
            if marker is None:
                marker = _create_or_read_file(
                    dir_fd,
                    INSTALLATION_KEY_MARKER_FILENAME,
                    expected_marker,
                )
            if not hmac.compare_digest(marker[0], expected_marker):
                raise _identity_unavailable()
            sentinel = _create_or_read_file(
                dir_fd,
                INSTALLATION_KEY_SENTINEL_FILENAME,
                _INSTALLATION_KEY_SENTINEL_CONTENT,
            )
        value, _identities = _complete(dir_fd, (key, marker, sentinel))
        return value


def reset_installation_key(
    data_dir: Path,
    *,
    acknowledge_continuity_break: bool,
) -> None:
    """Explicitly reset a verified installation identity while all users are offline."""
    if acknowledge_continuity_break is not True:
        raise InstallationKeyError("installation identity reset was not acknowledged")

    with _identity_dir(Path(data_dir)) as dir_fd:
        _key, identities = _complete(dir_fd, _parts(dir_fd))
        # Keep the sentinel until last: an interrupted reset either recovers
        # the intact key or remains fail-closed instead of rotating on load.
        for index in (1, 0, 2):
            name, _size = _IDENTITY_FILES[index]
            local_state.unlink_verified_entry(
                dir_fd,
                name,
                identities[index],
                expected_type=local_state.EntryType.REGULAR_FILE,
            )
