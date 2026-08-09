#!/usr/bin/python3 -I
"""Offline failure injection using actual r8 recovery code and restore function."""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
import re
import stat
import subprocess
import tempfile
from pathlib import Path


DEPLOY = Path(__file__).resolve().parents[1]


def load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load production module {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def private_json(path: Path, value: dict[str, object]) -> None:
    path.write_text(json.dumps(value) + "\n", encoding="utf-8")
    path.chmod(0o600)


def dead_owner(tag: str) -> dict[str, object]:
    return {
        "schema_version": 1,
        "tag": tag,
        "pid": 2_000_000_000,
        "start_time": "0",
        "boot_id": "stale",
        "uid": os.geteuid(),
    }


def production_build_recovery(root: Path) -> None:
    recovery = load_module("r8_recovery_injection", DEPLOY / "recover-stale-r8-artifacts.py")
    state = root / "build-state"
    recovered = root / "recovered"
    state.mkdir(mode=0o700)
    artifacts = {root / "runtime", root / "release", root / "manifest.json"}
    for path in artifacts:
        if path.suffix:
            path.write_text("candidate\n", encoding="utf-8")
        else:
            path.mkdir()
            (path / "owned").write_text("candidate\n", encoding="utf-8")
            path.chmod(0o555)
    (state / "phase").write_text("publishing\n", encoding="ascii")
    (state / "phase").chmod(0o600)
    private_json(state / "owner.json", dead_owner("R8_BUILD"))
    private_json(
        state / "owned-artifacts.json",
        {"schema_version": 1, "all_initially_absent": True, "paths": [str(path) for path in artifacts]},
    )
    recovery.BUILD_STATE = state
    recovery.RECOVERED_ROOT = recovered
    recovery.ALLOWED_BUILD_ARTIFACTS = artifacts
    recovery.recover_build()
    assert not any(path.exists() for path in artifacts)
    assert len(list(recovered.iterdir())) == 1


def restore_function() -> str:
    source = (DEPLOY / "rollback-r8.sh").read_text(encoding="utf-8")
    match = re.search(r"(?ms)^restore_entries\(\) \{.*?^\}$", source)
    if match is None:
        raise RuntimeError("production restore_entries function is absent")
    return match.group(0)


def run_real_restore(root: Path, first: int, last: int) -> None:
    prestate = root / "prestate"
    paths = [root / f"live-{index}" for index in range(3)]
    quoted_paths = " ".join(repr(str(path)) for path in paths)
    program = "\n".join(
        (
            "set -Eeuo pipefail",
            f"PRESTATE={str(prestate)!r}",
            f"SNAPSHOT_PATHS=({quoted_paths})",
            restore_function(),
            f"restore_entries {first} {last}",
        )
    )
    subprocess.run(["bash", "-c", program], check=True)


def production_partial_restore(root: Path) -> None:
    files = root / "prestate/files"
    files.mkdir(parents=True)
    for index in range(3):
        (files / f"{index}.state").write_text("present\n", encoding="ascii")
        backup = files / f"{index}.entry"
        backup.write_text(f"old-{index}\n", encoding="utf-8")
        backup.chmod(0o640)
        (root / f"live-{index}").write_text(f"candidate-{index}\n", encoding="utf-8")
    # Stop after the first real restore boundary, then rerun the same production
    # function over the full inventory to prove idempotent crash recovery.
    run_real_restore(root, 0, 0)
    assert (root / "live-0").read_text(encoding="utf-8") == "old-0\n"
    assert (root / "live-1").read_text(encoding="utf-8") == "candidate-1\n"
    run_real_restore(root, 0, 2)
    for index in range(3):
        live = root / f"live-{index}"
        assert live.read_text(encoding="utf-8") == f"old-{index}\n"
        assert stat.S_IMODE(live.stat().st_mode) == 0o640


def production_guard_reboot(root: Path) -> None:
    guard = load_module("r8_guard_injection", DEPLOY / "release-guard-r8.py")
    baseline = root / "baseline.json"
    boot = root / "boot-id"
    baseline.write_text(
        json.dumps({"schema_version": 1, "pid": os.getpid(), "start_time": "fixture", "boot_id": "old"}),
        encoding="utf-8",
    )
    boot.write_text("new\n", encoding="ascii")
    guard.HERDR_BASELINE = baseline
    guard.BOOT_ID_PATH = boot
    guard.process_start_time = lambda _pid: "fixture"

    class Result:
        stdout = f"{os.getpid()}\n"

    guard.subprocess.run = lambda *_args, **_kwargs: Result()
    assert guard.herdr_unchanged() is False


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "scenario",
        choices=("build-recovery", "partial-restore", "guard-reboot", "all"),
        default="all",
        nargs="?",
    )
    args = parser.parse_args()
    selected = {
        "build-recovery": (production_build_recovery,),
        "partial-restore": (production_partial_restore,),
        "guard-reboot": (production_guard_reboot,),
        "all": (production_build_recovery, production_partial_restore, production_guard_reboot),
    }[args.scenario]
    with tempfile.TemporaryDirectory(prefix="tendwire-r8-production-fi-") as temporary:
        base = Path(temporary)
        for function in selected:
            scenario = base / function.__name__
            scenario.mkdir()
            function(scenario)
    print(f"R8_FAILURE_INJECTION_OK {args.scenario}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
