from __future__ import annotations

import importlib.util
import hashlib
import json
import os
import stat
import sys
from pathlib import Path

import pytest


SCRIPT = Path(__file__).parents[1] / "validate-frozen-release.py"
DEPLOY_TEMPLATE = Path(__file__).parents[1] / "deploy-frozen-7446533-6c0d0f5.sh"
PYTHON_ENTRYPOINTS = tuple(
    Path(__file__).parents[1] / name
    for name in (
        "capture-frozen-discard-inventory.py",
        "monitor-frozen-7446533-6c0d0f5-hour.py",
        "validate-frozen-release.py",
        "finalize-frozen-release.py",
    )
)
TOOLING_REVISION = "a" * 40


def _load_validator():
    spec = importlib.util.spec_from_file_location("validate_frozen_release_test", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _write_manifest(
    module,
    runtime: Path,
    release: Path,
    runner: Path,
    manifest: Path,
) -> dict[str, object]:
    runtime_digest, runtime_entries = module.tree(runtime)
    release_digest, release_entries = module.tree(release)
    value = {
        "schema_version": 1,
        "tendwire_revision": module.TENDWIRE_REVISION,
        "herdres_revision": module.HERDRES_REVISION,
        "herdr_revision": module.HERDR_REVISION,
        "tooling_revision": TOOLING_REVISION,
        "deploy_runner_sha256": hashlib.sha256(runner.read_bytes()).hexdigest(),
        "herdr_sha256": module.HERDR_SHA256,
        "acp_source_sha256": module.ACP_SHA256,
        "acp_source_files": 29,
        "acp_distribution_version": "0.11.0",
        "tendwire_runtime_sha256": runtime_digest,
        "tendwire_runtime_entries": runtime_entries,
        "combined_release_sha256": release_digest,
        "combined_release_entries": release_entries,
        "owner_uid": os.geteuid(),
        "tendwire_runtime_root_mode": 0o555,
        "combined_release_root_mode": 0o555,
    }
    manifest.write_bytes(module.canonical(value))
    manifest.chmod(0o600)
    return value


@pytest.fixture
def release_fixture(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    module = _load_validator()
    transaction = tmp_path / "transaction"
    transaction.mkdir(mode=0o700)
    evidence = transaction / "release-validation.json"
    monkeypatch.setattr(module, "TRANSACTION_ROOT", transaction)
    monkeypatch.setattr(module, "EVIDENCE_PATH", evidence)

    runtime = tmp_path / "runtime"
    release = tmp_path / "release"
    runner_root = tmp_path / "runner"
    runtime.mkdir()
    release.mkdir()
    runner_root.mkdir()
    (runtime / "bin").mkdir()
    (runtime / "bin/tendwire").write_text("frozen tendwire runtime\n")
    (runtime / "bin/tendwire").chmod(0o555)
    (release / "herdres").mkdir()
    (release / "herdres/herdres").write_text("frozen herdres runtime\n")
    (release / "herdres/herdres").chmod(0o555)
    (release / "operations-tooling-revision").write_text(TOOLING_REVISION + "\n")
    (release / "operations-tooling-revision").chmod(0o444)
    runner = runner_root / "deploy"
    runner.write_text("#!/bin/sh\nexec true\n")
    runner.chmod(0o555)
    runtime.chmod(0o555)
    release.chmod(0o555)
    runner_root.chmod(0o555)
    monkeypatch.setattr(module, "RUNNER_ROOT", runner_root)
    manifest = tmp_path / "release-manifest.json"
    manifest_value = _write_manifest(module, runtime, release, runner, manifest)
    completion = tmp_path / "prepared.complete"
    completion.write_text(
        f"FROZEN_ACP_RELEASE_PREPARED {TOOLING_REVISION} "
        f"{hashlib.sha256(manifest.read_bytes()).hexdigest()}\n",
        encoding="utf-8",
    )
    completion.chmod(0o600)
    monkeypatch.setattr(module, "PREPARE_COMPLETE", completion)
    return (
        module,
        runtime,
        release,
        runner_root,
        runner,
        manifest,
        manifest_value,
        evidence,
    )


def _main(
    module,
    monkeypatch: pytest.MonkeyPatch,
    *,
    mode: str,
    runtime: Path,
    release: Path,
    manifest: Path,
) -> int:
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "validate-frozen-release",
            f"--{mode}",
            "--runtime",
            str(runtime),
            "--release",
            str(release),
            "--manifest",
            str(manifest),
        ],
    )
    return module.main()


def test_canonical_create_then_verify_succeeds(
    release_fixture,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    (
        module,
        runtime,
        release,
        _runner_root,
        runner,
        manifest,
        _manifest_value,
        evidence,
    ) = release_fixture

    assert _main(
        module,
        monkeypatch,
        mode="create",
        runtime=runtime,
        release=release,
        manifest=manifest,
    ) == 0
    created = json.loads(evidence.read_text())
    assert created == module.expected(runtime, release, manifest)
    assert created["tooling_revision"] == TOOLING_REVISION
    assert created["deploy_runner_sha256"] == hashlib.sha256(runner.read_bytes()).hexdigest()
    assert stat.S_IMODE(evidence.stat().st_mode) == 0o600
    assert json.loads(capsys.readouterr().out) == {
        "schema_version": 1,
        "release_id": module.RELEASE_ID,
        "valid": True,
    }

    assert _main(
        module,
        monkeypatch,
        mode="verify",
        runtime=runtime,
        release=release,
        manifest=manifest,
    ) == 0
    assert json.loads(capsys.readouterr().out)["valid"] is True


def test_verify_rejects_tampered_release_tree(
    release_fixture, monkeypatch: pytest.MonkeyPatch,
) -> None:
    module, runtime, release, _, _, manifest, _, _ = release_fixture
    assert _main(
        module,
        monkeypatch,
        mode="create",
        runtime=runtime,
        release=release,
        manifest=manifest,
    ) == 0
    binary = release / "herdres/herdres"
    binary.chmod(0o755)
    binary.write_text("tampered herdres runtime\n")

    with pytest.raises(RuntimeError, match="manifest does not match immutable trees"):
        _main(
            module,
            monkeypatch,
            mode="verify",
            runtime=runtime,
            release=release,
            manifest=manifest,
        )


def test_verify_rejects_tampered_manifest(
    release_fixture, monkeypatch: pytest.MonkeyPatch,
) -> None:
    module, runtime, release, _, _, manifest, manifest_value, _ = release_fixture
    assert _main(
        module,
        monkeypatch,
        mode="create",
        runtime=runtime,
        release=release,
        manifest=manifest,
    ) == 0
    tampered = dict(manifest_value)
    tampered["herdres_revision"] = "0" * 40
    manifest.write_bytes(module.canonical(tampered))

    with pytest.raises(RuntimeError, match="manifest|completion marker"):
        _main(
            module,
            monkeypatch,
            mode="verify",
            runtime=runtime,
            release=release,
            manifest=manifest,
        )


def test_verify_rejects_tampered_runner(
    release_fixture, monkeypatch: pytest.MonkeyPatch,
) -> None:
    module, runtime, release, _, runner, manifest, _, _ = release_fixture
    assert _main(
        module,
        monkeypatch,
        mode="create",
        runtime=runtime,
        release=release,
        manifest=manifest,
    ) == 0
    runner.chmod(0o755)
    runner.write_text("#!/bin/sh\nexit 1\n")
    runner.chmod(0o555)

    with pytest.raises(RuntimeError, match="manifest does not match immutable trees"):
        _main(
            module,
            monkeypatch,
            mode="verify",
            runtime=runtime,
            release=release,
            manifest=manifest,
        )


def test_create_rejects_missing_or_tampered_prepare_completion(
    release_fixture, monkeypatch: pytest.MonkeyPatch,
) -> None:
    module, runtime, release, _, _, manifest, _, _ = release_fixture
    module.PREPARE_COMPLETE.write_text("not complete\n", encoding="utf-8")

    with pytest.raises(RuntimeError, match="prepare completion marker is invalid"):
        _main(
            module,
            monkeypatch,
            mode="create",
            runtime=runtime,
            release=release,
            manifest=manifest,
        )


@pytest.mark.parametrize("root_name", ["runtime", "release"])
def test_create_rejects_wrong_root_mode(
    release_fixture,
    monkeypatch: pytest.MonkeyPatch,
    root_name: str,
) -> None:
    module, runtime, release, _, _, manifest, _, _ = release_fixture
    {"runtime": runtime, "release": release}[root_name].chmod(0o755)

    with pytest.raises(RuntimeError, match="ownership or mode changed"):
        _main(
            module,
            monkeypatch,
            mode="create",
            runtime=runtime,
            release=release,
            manifest=manifest,
        )


@pytest.mark.parametrize("root_name", ["runtime", "release"])
def test_create_rejects_symlink_root(
    release_fixture,
    monkeypatch: pytest.MonkeyPatch,
    root_name: str,
) -> None:
    module, runtime, release, _, _, manifest, _, _ = release_fixture
    root = {"runtime": runtime, "release": release}[root_name]
    real = root.with_name(root.name + "-real")
    root.rename(real)
    root.symlink_to(real, target_is_directory=True)

    with pytest.raises(RuntimeError):
        _main(
            module,
            monkeypatch,
            mode="create",
            runtime=runtime,
            release=release,
            manifest=manifest,
        )


@pytest.mark.parametrize("runner_part", ["root", "deploy"])
def test_create_rejects_bad_runner_mode(
    release_fixture,
    monkeypatch: pytest.MonkeyPatch,
    runner_part: str,
) -> None:
    module, runtime, release, runner_root, runner, manifest, _, _ = release_fixture
    {"root": runner_root, "deploy": runner}[runner_part].chmod(0o755)

    with pytest.raises(RuntimeError, match="deploy runner ownership or mode changed"):
        _main(
            module,
            monkeypatch,
            mode="create",
            runtime=runtime,
            release=release,
            manifest=manifest,
        )


@pytest.mark.parametrize("runner_part", ["root", "deploy"])
def test_create_rejects_runner_symlink(
    release_fixture,
    monkeypatch: pytest.MonkeyPatch,
    runner_part: str,
) -> None:
    module, runtime, release, runner_root, runner, manifest, _, _ = release_fixture
    if runner_part == "root":
        real = runner_root.with_name("runner-real")
        runner_root.rename(real)
        runner_root.symlink_to(real, target_is_directory=True)
    else:
        runner_root.chmod(0o755)
        real = runner.with_name("deploy-real")
        runner.rename(real)
        runner.symlink_to(real.name)
        runner_root.chmod(0o555)

    with pytest.raises(RuntimeError, match="deploy runner ownership or mode changed"):
        _main(
            module,
            monkeypatch,
            mode="create",
            runtime=runtime,
            release=release,
            manifest=manifest,
        )


def test_deploy_template_binds_executing_source_and_rejects_sourcing() -> None:
    source = DEPLOY_TEMPLATE.read_text(encoding="utf-8")
    execution_guard = '[[ "${BASH_SOURCE[0]}" = "$0" ]]'
    origin_guard = (
        'test "$(readlink -f -- "${BASH_SOURCE[0]}")" = "${DEPLOY_RUNNER}"'
    )
    authorization = (
        '[[ "${ACP_CUTOVER_DISCARD_AUTHORIZATION:-}" = "${DISCARD_AUTHORIZATION}" ]]'
    )
    assert source.count(execution_guard) == 1
    assert source.count(origin_guard) == 1
    assert source.index(execution_guard) < source.index(origin_guard) < source.index(authorization)
    assert 'readlink -f -- "$0"' not in source


def test_python_entrypoints_use_absolute_isolated_interpreter() -> None:
    for path in PYTHON_ENTRYPOINTS:
        assert path.read_text(encoding="utf-8").startswith("#!/usr/bin/python3 -I\n")


def test_deploy_inline_python_and_topic_tool_use_isolated_mode() -> None:
    source = DEPLOY_TEMPLATE.read_text(encoding="utf-8")
    assert '"${TENDWIRE_RUNTIME}/bin/python" -P' not in source
    assert '"${TENDWIRE_RUNTIME}/bin/python" -B -I -' in source
    assert '"${TOPIC_RESET_PYTHON}" -I "${TOPIC_RESET_TOOL}"' in source
