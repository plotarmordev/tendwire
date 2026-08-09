#!/usr/bin/python3 -I
"""Recover only r8 artifacts proven stale by durable ownership markers."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import stat
import subprocess
import time
from pathlib import Path


BUILD_STATE = Path("/home/smith/.local/state/acp-cutover/r8-build-67568f32-0b94403-f50af73")
PREPARE_STATE = Path("/home/smith/.local/state/acp-cutover/frozen-0b94403-r8")
RECOVERED_ROOT = Path("/home/smith/.local/state/acp-cutover/recovered-r8")
RELEASE = Path("/home/smith/.local/share/acp-runtime/releases/67568f32-0b94403-f50af73-r8")
ALLOWED_BUILD_ARTIFACTS = {
    Path("/home/smith/.local/share/tendwire-runtime/acp-0b94403-f50af73-r8"),
    Path("/home/smith/.local/share/acp-runtime/releases/67568f32-0b94403-f50af73-r8"),
    Path("/home/smith/.local/share/acp-runtime/manifests/67568f32-0b94403-f50af73-r8.json"),
    Path("/home/smith/.config/herdres/frozen-0b94403-f50af73-r8.env"),
    Path("/home/smith/.local/share/tendwire-runtime/.acp-0b94403-f50af73-r8.building"),
    Path("/home/smith/.local/share/acp-runtime/releases/.67568f32-0b94403-f50af73-r8.building"),
    Path("/home/smith/.local/share/acp-adapters/codex-acp-7cb0524624f2e730f48c3dac9b547ca130964ae9"),
    Path("/home/smith/.local/share/acp-adapters/.codex-acp-7cb0524624f2e730f48c3dac9b547ca130964ae9.building"),
    Path("/home/smith/.local/share/herdr-runtime/acp-67568f327cea67bdd1d197feff621c45476da84c"),
    Path("/home/smith/.local/share/herdr-runtime/.acp-67568f327cea67bdd1d197feff621c45476da84c.building"),
    Path("/home/smith/.local/share/herdr-runtime/.target-67568f327cea67bdd1d197feff621c45476da84c.building"),
}
SOURCE_IDENTITIES = (
    (
        Path("/home/smith/tendwire"),
        "0b944031c94e99b9f5fac439a850e8b823e8b1f1",
        "c3745fb8a8f5de3049f1e08f6468e20cb16f5fe0",
        "1f277cd562f3852cfbec4e52b2ffc8c406550fc4",
    ),
    (
        Path("/home/smith/tendwire/.worktrees/reduction-herdres-release-fixes"),
        "f50af733033089020ace3b15e686299cb8a67f1e",
        "566a75fca9a8b7b54b111bfc5e87d5233c2002a8",
        "36599949daa64f68494d04f96a3bfee31904a804",
    ),
    (
        Path("/home/smith/tendwire/.worktrees/herdr-acp-console"),
        "67568f327cea67bdd1d197feff621c45476da84c",
        "def32e75c4751d2acd504fdfc6de9303ab394b6b",
        "9026d9bc5a12d9adc2d9f68ebdc564133e4098b4",
    ),
)


def _source_preflight() -> None:
    for repository, commit, tree, ancestor in SOURCE_IDENTITIES:
        if "REQUIRED" in commit or "REQUIRED" in tree:
            raise RuntimeError("authorized Herdr source identity is required")
        actual_tree = subprocess.run(
            ["git", "-C", str(repository), "rev-parse", f"{commit}^{{tree}}"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
        if actual_tree != tree:
            raise RuntimeError(f"source tree identity mismatch: {repository}")
        ancestry = subprocess.run(
            ["git", "-C", str(repository), "merge-base", "--is-ancestor", ancestor, commit],
            check=False,
        )
        if ancestry.returncode != 0:
            raise RuntimeError(f"reviewed source ancestry mismatch: {repository}")


def _directory_fsync(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _load_private_json(path: Path) -> dict[str, object]:
    metadata = path.lstat()
    if not stat.S_ISREG(metadata.st_mode) or metadata.st_uid != os.geteuid():
        raise RuntimeError(f"untrusted ownership marker: {path}")
    if stat.S_IMODE(metadata.st_mode) & 0o077:
        raise RuntimeError(f"ownership marker is not private: {path}")
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise RuntimeError("ownership marker is not an object")
    return value


def _process_start(pid: int) -> str:
    return Path(f"/proc/{pid}/stat").read_text(encoding="ascii").rsplit(") ", 1)[1].split()[19]


def _owner_live(path: Path, allowed_tags: set[str]) -> bool:
    try:
        owner = _load_private_json(path)
        tag = str(owner["tag"])
        pid = int(owner["pid"])
        start = str(owner["start_time"])
        boot = str(owner["boot_id"])
        uid = int(owner["uid"])
        command = Path(f"/proc/{pid}/cmdline").read_bytes().replace(b"\0", b" ")
        actual_start = _process_start(pid)
        current_boot = Path("/proc/sys/kernel/random/boot_id").read_text(encoding="ascii").strip()
    except (KeyError, OSError, TypeError, ValueError):
        return False
    return (
        tag in allowed_tags
        and pid > 1
        and uid == os.geteuid()
        and boot == current_boot
        and start == actual_start
        and any(token.encode() in command for token in ("build-acp-release-r8", "prepare-acp-release-r8", "rollout-acp-release-r8", "monitor-one-hour-strict-r8"))
    )


def _legacy_text_owner_live(path: Path, expected_tag: str, command_token: bytes) -> bool:
    """Validate the rollout/monitor owner format already consumed by guards."""
    try:
        metadata = path.lstat()
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_uid != os.geteuid():
            return False
        tag, raw_pid, expected_start = path.read_text(encoding="ascii").split()
        pid = int(raw_pid)
        command = Path(f"/proc/{pid}/cmdline").read_bytes().replace(b"\0", b" ")
        actual_start = _process_start(pid)
    except (OSError, ValueError):
        return False
    return (
        tag == expected_tag
        and pid > 1
        and actual_start == expected_start
        and command_token in command
    )


def _archive(root: Path, label: str) -> None:
    RECOVERED_ROOT.mkdir(mode=0o700, parents=True, exist_ok=True)
    destination = RECOVERED_ROOT / f"{label}-{time.time_ns()}"
    os.replace(root, destination)
    _directory_fsync(root.parent)
    _directory_fsync(RECOVERED_ROOT)


def _remove_owned(path: Path) -> None:
    if path.is_symlink() or path.is_file():
        path.unlink()
        return
    if path.is_dir():
        for current, directories, _files in os.walk(path, topdown=False, followlinks=False):
            for name in directories:
                child = Path(current, name)
                if not child.is_symlink():
                    child.chmod(child.stat().st_mode | stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR)
            Path(current).chmod(Path(current).stat().st_mode | stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR)
        shutil.rmtree(path)
        return
    if path.exists():
        raise RuntimeError(f"unsupported owned artifact: {path}")


def recover_build() -> None:
    root = BUILD_STATE
    if not root.exists():
        return
    metadata = root.lstat()
    if not stat.S_ISDIR(metadata.st_mode) or metadata.st_uid != os.geteuid() or stat.S_IMODE(metadata.st_mode) != 0o700:
        raise RuntimeError("untrusted r8 build state")
    phase = (root / "phase").read_text(encoding="ascii").strip()
    if phase == "published":
        raise RuntimeError("the r8 release is already durably published")
    if phase not in {"building", "publishing", "failed"}:
        raise RuntimeError("unrecognized stale r8 build phase")
    if _owner_live(root / "owner.json", {"R8_BUILD"}):
        raise RuntimeError("the r8 build owner is still live")
    inventory = _load_private_json(root / "owned-artifacts.json")
    entries = inventory.get("paths")
    if not isinstance(entries, list) or set(map(Path, entries)) != ALLOWED_BUILD_ARTIFACTS:
        raise RuntimeError("r8 build ownership inventory is not exact")
    if inventory.get("all_initially_absent") is not True:
        raise RuntimeError("r8 build did not prove exclusive artifact ownership")
    for path in sorted(ALLOWED_BUILD_ARTIFACTS, key=lambda item: len(item.parts), reverse=True):
        _remove_owned(path)
        if path.parent.exists():
            _directory_fsync(path.parent)
    _archive(root, "build-recovered")


def recover_prepare() -> None:
    root = PREPARE_STATE
    if not root.exists():
        return
    metadata = root.lstat()
    if not stat.S_ISDIR(metadata.st_mode) or metadata.st_uid != os.geteuid() or stat.S_IMODE(metadata.st_mode) != 0o700:
        raise RuntimeError("untrusted r8 prepare state")
    phase = (root / "phase").read_text(encoding="ascii").strip()
    if phase == "validation_passed":
        raise RuntimeError("the accepted r8 transaction must not be recovered as stale")
    if phase not in {
        "snapshotting", "preparing", "prepared", "committing", "provisional",
        "validating", "validation_failed", "failed", "rolling_back", "rolled_back",
    }:
        raise RuntimeError("unrecognized stale r8 prepare phase")
    if (root / "prepare-owner.json").exists() and _owner_live(
        root / "prepare-owner.json", {"R8_PREPARE"}
    ):
        raise RuntimeError("an r8 transaction owner is still live")
    if _legacy_text_owner_live(root / "rollout-owner", "R8_ROLLOUT", b"rollout-acp-release-r8"):
        raise RuntimeError("an r8 transaction owner is still live")
    if _legacy_text_owner_live(
        root / "validation-monitor-owner",
        "FROZEN_ACP_VALIDATION_MONITOR",
        b"monitor-one-hour-strict",
    ):
        raise RuntimeError("an r8 transaction owner is still live")
    backup_complete = (root / "backup-complete").read_text(encoding="ascii").strip() == "BACKUP_COMPLETE" if (root / "backup-complete").is_file() else False
    live_started = (root / "live-mutation-started").is_file()
    if live_started and not backup_complete:
        raise RuntimeError("live mutation lacks a complete durable backup")
    if backup_complete:
        subprocess.run([str(RELEASE / "rollback-r8")], check=True, timeout=300)
        if (root / "phase").read_text(encoding="ascii").strip() != "rolled_back":
            raise RuntimeError("stale r8 rollback did not reach rolled_back")
        _archive(root, "prepare-rolled-back")
        return
    if live_started:
        raise RuntimeError("cannot archive a transaction that may have mutated live state")
    _archive(root, "prepare-recovered")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=("build", "prepare"))
    args = parser.parse_args()
    _source_preflight()
    if args.mode == "build":
        recover_build()
    else:
        recover_prepare()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
