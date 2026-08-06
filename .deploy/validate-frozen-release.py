#!/usr/bin/python3 -I
"""Create or verify transaction-bound immutable release integrity evidence."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import stat
from pathlib import Path
from typing import Any


RELEASE_ID = "9026d9bc-7446533-3659994-r3"
TENDWIRE_REVISION = "7446533bb6fb2560a9a9dd871f638c4a6ccbb086"
HERDRES_REVISION = "36599949daa64f68494d04f96a3bfee31904a804"
HERDR_REVISION = "9026d9bc5a12d9adc2d9f68ebdc564133e4098b4"
HERDR_SHA256 = "2e58e1b11ed289d6a99ba36b80867e5e5d5920d03406bb40a1113e2d391f386f"
ACP_SHA256 = "f5e621738a5651da9d14559806ab1d3491e8a9da6a72e686baf087e67a87e5f6"
RUNNER_ROOT = Path(
    "/home/smith/.local/share/acp-runtime/runners/9026d9bc-7446533-3659994-r3"
)
PREPARE_COMPLETE = Path(
    "/home/smith/.local/share/acp-runtime/prepared/9026d9bc-7446533-3659994-r3.complete"
)
TRANSACTION_ROOT = Path("/home/smith/.local/state/acp-cutover/frozen-7446533-6c0d0f5")
EVIDENCE_PATH = TRANSACTION_ROOT / "release-validation.json"


def tree(path: Path) -> tuple[str, int]:
    digest = hashlib.sha256()
    count = 0
    for item in sorted(path.rglob("*"), key=lambda value: value.relative_to(path).as_posix()):
        relative = item.relative_to(path).as_posix().encode()
        info = item.lstat()
        kind = b"l" if stat.S_ISLNK(info.st_mode) else b"f" if stat.S_ISREG(info.st_mode) else b"d"
        content = os.readlink(item).encode() if kind == b"l" else item.read_bytes() if kind == b"f" else b""
        digest.update(len(relative).to_bytes(4, "big") + relative)
        digest.update(kind + stat.S_IMODE(info.st_mode).to_bytes(2, "big"))
        digest.update(len(content).to_bytes(8, "big") + content)
        count += 1
    return digest.hexdigest(), count


def canonical(value: dict[str, Any]) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def expected(runtime: Path, release: Path, manifest_path: Path) -> dict[str, Any]:
    manifest_raw = manifest_path.read_bytes()
    manifest = json.loads(manifest_raw)
    runtime_value = tree(runtime)
    release_value = tree(release)
    tooling_path = release / "operations-tooling-revision"
    tooling_revision = tooling_path.read_text(encoding="utf-8").strip()
    if len(tooling_revision) != 40 or any(value not in "0123456789abcdef" for value in tooling_revision):
        raise RuntimeError("operations tooling revision is invalid")
    runner = RUNNER_ROOT / "deploy"
    if (
        RUNNER_ROOT.is_symlink()
        or runner.is_symlink()
        or not RUNNER_ROOT.is_dir()
        or not runner.is_file()
        or RUNNER_ROOT.lstat().st_uid != os.geteuid()
        or runner.lstat().st_uid != os.geteuid()
        or stat.S_IMODE(RUNNER_ROOT.lstat().st_mode) != 0o555
        or stat.S_IMODE(runner.lstat().st_mode) != 0o555
    ):
        raise RuntimeError("immutable deploy runner ownership or mode changed")
    runner_digest = hashlib.sha256(runner.read_bytes()).hexdigest()
    completion_info = PREPARE_COMPLETE.lstat()
    completion_expected = (
        f"FROZEN_ACP_RELEASE_PREPARED {tooling_revision} "
        f"{hashlib.sha256(manifest_raw).hexdigest()}\n"
    ).encode()
    if (
        not stat.S_ISREG(completion_info.st_mode)
        or completion_info.st_uid != os.geteuid()
        or completion_info.st_nlink != 1
        or stat.S_IMODE(completion_info.st_mode) != 0o600
        or PREPARE_COMPLETE.read_bytes() != completion_expected
    ):
        raise RuntimeError("prepare completion marker is invalid")
    required = {
        "schema_version": 1,
        "tendwire_revision": TENDWIRE_REVISION,
        "herdres_revision": HERDRES_REVISION,
        "herdr_revision": HERDR_REVISION,
        "tooling_revision": tooling_revision,
        "deploy_runner_sha256": runner_digest,
        "herdr_sha256": HERDR_SHA256,
        "acp_source_sha256": ACP_SHA256,
        "acp_source_files": 29,
        "acp_distribution_version": "0.11.0",
        "tendwire_runtime_sha256": runtime_value[0],
        "tendwire_runtime_entries": runtime_value[1],
        "combined_release_sha256": release_value[0],
        "combined_release_entries": release_value[1],
        "owner_uid": os.geteuid(),
        "tendwire_runtime_root_mode": 0o555,
        "combined_release_root_mode": 0o555,
    }
    if manifest != required or manifest_raw != canonical(required):
        raise RuntimeError("release manifest does not match immutable trees")
    if (
        runtime.lstat().st_uid != os.geteuid()
        or release.lstat().st_uid != os.geteuid()
        or stat.S_IMODE(runtime.lstat().st_mode) != 0o555
        or stat.S_IMODE(release.lstat().st_mode) != 0o555
    ):
        raise RuntimeError("immutable release ownership or mode changed")
    return {
        "schema_version": 1,
        "release_id": RELEASE_ID,
        "manifest_sha256": hashlib.sha256(manifest_raw).hexdigest(),
        "tooling_revision": tooling_revision,
        "deploy_runner_sha256": runner_digest,
        "tendwire_runtime_sha256": runtime_value[0],
        "tendwire_runtime_entries": runtime_value[1],
        "combined_release_sha256": release_value[0],
        "combined_release_entries": release_value[1],
    }


def write_once(value: dict[str, Any]) -> None:
    if EVIDENCE_PATH.exists() or EVIDENCE_PATH.is_symlink():
        raise RuntimeError("release validation evidence already exists")
    temporary = EVIDENCE_PATH.with_name(f".{EVIDENCE_PATH.name}.tmp.{os.getpid()}")
    descriptor = os.open(
        temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC, 0o600
    )
    try:
        body = canonical(value)
        view = memoryview(body)
        while view:
            written = os.write(descriptor, view)
            if written < 1:
                raise RuntimeError("release validation evidence write failed")
            view = view[written:]
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    os.replace(temporary, EVIDENCE_PATH)
    directory = os.open(EVIDENCE_PATH.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--runtime", type=Path, required=True)
    parser.add_argument("--release", type=Path, required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    modes = parser.add_mutually_exclusive_group(required=True)
    modes.add_argument("--create", action="store_true")
    modes.add_argument("--verify", action="store_true")
    args = parser.parse_args()
    value = expected(args.runtime, args.release, args.manifest)
    if args.create:
        write_once(value)
    else:
        raw = EVIDENCE_PATH.read_bytes()
        if raw != canonical(value) or json.loads(raw) != value:
            raise RuntimeError("release validation evidence changed")
    print(json.dumps({"schema_version": 1, "release_id": RELEASE_ID, "valid": True}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
