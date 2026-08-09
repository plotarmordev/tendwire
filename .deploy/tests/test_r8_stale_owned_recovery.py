from __future__ import annotations

import importlib.util
import json
import os
from pathlib import Path

import pytest


SCRIPT = Path(__file__).resolve().parents[1] / "recover-stale-r8-artifacts.py"


def load_recovery():
    spec = importlib.util.spec_from_file_location("r8_stale_recovery_test", SCRIPT)
    assert spec is not None and spec.loader is not None
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


def test_build_recovery_deletes_only_exact_owned_inventory(tmp_path: Path) -> None:
    recovery = load_recovery()
    state = tmp_path / "state"
    recovered = tmp_path / "recovered"
    state.mkdir(mode=0o700)
    artifacts = {tmp_path / "release", tmp_path / "manifest"}
    (tmp_path / "release").mkdir()
    (tmp_path / "release/file").write_text("candidate\n", encoding="utf-8")
    (tmp_path / "release/file").chmod(0o444)
    (tmp_path / "release").chmod(0o555)
    (tmp_path / "manifest").write_text("candidate\n", encoding="utf-8")
    (state / "phase").write_text("publishing\n", encoding="ascii")
    (state / "phase").chmod(0o600)
    private_json(state / "owner.json", dead_owner("R8_BUILD"))
    private_json(
        state / "owned-artifacts.json",
        {
            "schema_version": 1,
            "all_initially_absent": True,
            "paths": [str(path) for path in artifacts],
        },
    )
    unrelated = tmp_path / "unrelated"
    unrelated.write_text("keep\n", encoding="utf-8")
    recovery.BUILD_STATE = state
    recovery.RECOVERED_ROOT = recovered
    recovery.ALLOWED_BUILD_ARTIFACTS = artifacts

    recovery.recover_build()

    assert not any(path.exists() for path in artifacts)
    assert unrelated.read_text(encoding="utf-8") == "keep\n"
    assert not state.exists()
    assert len(list(recovered.iterdir())) == 1


def test_build_recovery_rejects_unowned_extra_path(tmp_path: Path) -> None:
    recovery = load_recovery()
    state = tmp_path / "state"
    state.mkdir(mode=0o700)
    artifact = tmp_path / "artifact"
    artifact.write_text("candidate\n", encoding="utf-8")
    (state / "phase").write_text("failed\n", encoding="ascii")
    (state / "phase").chmod(0o600)
    private_json(state / "owner.json", dead_owner("R8_BUILD"))
    private_json(
        state / "owned-artifacts.json",
        {"all_initially_absent": True, "paths": [str(artifact), str(tmp_path / "extra")]},
    )
    recovery.BUILD_STATE = state
    recovery.RECOVERED_ROOT = tmp_path / "recovered"
    recovery.ALLOWED_BUILD_ARTIFACTS = {artifact}

    with pytest.raises(RuntimeError, match="inventory is not exact"):
        recovery.recover_build()
    assert artifact.exists()


def test_prepare_recovery_archives_only_pre_mutation_snapshot(tmp_path: Path) -> None:
    recovery = load_recovery()
    state = tmp_path / "prepare"
    state.mkdir(mode=0o700)
    (state / "phase").write_text("snapshotting\n", encoding="ascii")
    (state / "phase").chmod(0o600)
    private_json(state / "prepare-owner.json", dead_owner("R8_PREPARE"))
    recovery.PREPARE_STATE = state
    recovery.RECOVERED_ROOT = tmp_path / "recovered"

    recovery.recover_prepare()

    assert not state.exists()
    assert len(list(recovery.RECOVERED_ROOT.iterdir())) == 1


def test_prepare_recovery_rejects_mutation_without_backup(tmp_path: Path) -> None:
    recovery = load_recovery()
    state = tmp_path / "prepare"
    state.mkdir(mode=0o700)
    (state / "phase").write_text("preparing\n", encoding="ascii")
    (state / "phase").chmod(0o600)
    (state / "live-mutation-started").write_text("yes\n", encoding="ascii")
    private_json(state / "prepare-owner.json", dead_owner("R8_PREPARE"))
    recovery.PREPARE_STATE = state
    recovery.RECOVERED_ROOT = tmp_path / "recovered"

    with pytest.raises(RuntimeError, match="lacks a complete durable backup"):
        recovery.recover_prepare()
    assert state.exists()
