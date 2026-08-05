from __future__ import annotations

import importlib.util
import json
import os
import subprocess
import sys
import threading
from pathlib import Path

import pytest

_HERDRES_ROOT = os.environ.get("TENDWIRE_BENCHMARK_HERDRES_ROOT")
_requires_herdres_root = pytest.mark.skipif(
    not (_HERDRES_ROOT and Path(_HERDRES_ROOT).is_dir()),
    reason="sidecar benchmark needs TENDWIRE_BENCHMARK_HERDRES_ROOT pointing at a herdres checkout",
)
from types import ModuleType
from typing import Any

import pytest


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "sqlite_sidecar_race_benchmark.py"


def _load_driver() -> ModuleType:
    spec = importlib.util.spec_from_file_location("sqlite_sidecar_race_benchmark", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _direct_children() -> set[int]:
    children: set[int] = set()
    for task in Path("/proc/self/task").iterdir():
        try:
            children.update(
                int(value)
                for value in (task / "children").read_text(encoding="ascii").split()
            )
        except FileNotFoundError:
            continue
    return children


def _fd_snapshot() -> dict[int, tuple[int, int, int]]:
    result: dict[int, tuple[int, int, int]] = {}
    for raw in os.listdir("/proc/self/fd"):
        try:
            descriptor = int(raw)
            observed = os.fstat(descriptor)
        except (FileNotFoundError, OSError, ValueError):
            continue
        result[descriptor] = (
            int(observed.st_dev),
            int(observed.st_ino),
            int(observed.st_mode),
        )
    return result


def _temporary_roots() -> set[Path]:
    return {
        path
        for path in Path("/dev/shm").iterdir()
        if path.name.startswith("tendwire-sidecar-evidence-")
    }


def _invoke(*arguments: str, testing: bool = False, timeout: float = 240.0) -> subprocess.CompletedProcess[str]:
    environment = os.environ.copy()
    environment.pop("PYTHONPATH", None)
    if testing:
        environment["TENDWIRE_BENCHMARK_TESTING"] = "1"
    return subprocess.run(
        [sys.executable, str(SCRIPT), *arguments],
        cwd=ROOT,
        stdin=subprocess.DEVNULL,
        capture_output=True,
        text=True,
        env=environment,
        check=False,
        timeout=timeout,
    )


def _single_object(completed: subprocess.CompletedProcess[str]) -> dict[str, Any]:
    assert completed.stderr == ""
    lines = completed.stdout.splitlines()
    assert len(lines) == 1
    value = json.loads(lines[0])
    assert isinstance(value, dict)
    assert lines[0] == json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return value


def test_invalid_arguments_emit_one_fixed_compact_object() -> None:
    completed = _invoke(
        "--requests-per-method",
        "0",
        "--json",
        timeout=15,
    )

    assert completed.returncode == 2
    assert _single_object(completed) == {
        "schema_version": 1,
        "ok": False,
        "status": "invalid_arguments",
    }


def test_recursive_privacy_gate_rejects_values_and_success_error_keys() -> None:
    driver = _load_driver()
    safe = {
        "schema_version": 1,
        "ok": True,
        "status": "completed",
        "counts": {"attempts": 2, "typed_codes": {"missing_entry": 0}},
    }

    assert driver._privacy_scan(safe, ["private-sentinel"])
    assert not driver._privacy_scan({**safe, "nested": ["private-sentinel"]}, ["private-sentinel"])
    assert not driver._privacy_scan({**safe, "error": None}, [])
    assert not driver._privacy_scan({**safe, "nested": {"error_type": "none"}}, [])


def test_paired_source_binding_requires_a_clean_stable_checkout(tmp_path: Path) -> None:
    driver = _load_driver()
    subprocess.run(["git", "init", "-q"], cwd=tmp_path, check=True)
    subprocess.run(
        ["git", "config", "user.email", "benchmark@example.invalid"],
        cwd=tmp_path,
        check=True,
    )
    subprocess.run(
        ["git", "config", "user.name", "Benchmark Test"], cwd=tmp_path, check=True
    )
    source = tmp_path / "paired.py"
    source.write_text("VALUE = 1\n", encoding="utf-8")
    subprocess.run(["git", "add", "paired.py"], cwd=tmp_path, check=True)
    subprocess.run(["git", "commit", "-qm", "fixture"], cwd=tmp_path, check=True)

    revision, tree, digest, count = driver._clean_source_binding(tmp_path)
    assert len(revision) == len(tree) == 40
    assert len(digest) == 64
    assert count == 1

    (tmp_path / "untracked.py").write_text("VALUE = 2\n", encoding="utf-8")
    with pytest.raises(RuntimeError, match="checkout_dirty"):
        driver._clean_source_binding(tmp_path)
    (tmp_path / "untracked.py").unlink()
    source.write_text("VALUE = 3\n", encoding="utf-8")
    with pytest.raises(RuntimeError, match="checkout_dirty"):
        driver._clean_source_binding(tmp_path)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("requests_per_method", 0),
        ("herdres_presenter_passes", 3),
        ("phase_timeout_seconds", 0),
    ],
)
def test_argument_contract_requires_positive_exact_work(
    field: str, value: int,
) -> None:
    driver = _load_driver()
    namespace = driver._parser().parse_args(["--json"])
    setattr(namespace, field, value)

    with pytest.raises(driver._ArgumentError):
        driver._argument_values(namespace)


@_requires_herdres_root
def test_tiny_installed_candidate_run_emits_complete_aggregate() -> None:
    completed = _invoke(
        "--requests-per-method",
        "1",
        "--herdres-presenter-passes",
        "2",
        "--phase-timeout-seconds",
        "30",
        "--json",
    )

    assert completed.returncode == 0
    report = _single_object(completed)
    assert report["ok"] is True
    assert report["status"] == "completed"
    assert report["candidate"]["installation"] == "private_versioned_wheel"
    assert report["candidate"]["origin_verified"] is True
    assert len(report["candidate"]["wheel_sha256"]) == 64
    assert len(report["candidate"]["source_tree_sha256"]) == 64
    assert (
        report["candidate"]["source_revision_binding"]
        == "base_revision_plus_source_tree_sha256"
    )
    assert len(report["paired_herdres"]["revision"]) == 40
    assert len(report["paired_herdres"]["tree"]) == 40
    assert len(report["paired_herdres"]["tracked_source_sha256"]) == 64
    assert report["paired_herdres"]["tracked_files"] > 0
    assert report["paired_herdres"]["clean_checkout_verified"] is True
    assert str(Path(_HERDRES_ROOT).resolve()) not in completed.stdout
    assert report["daemon"]["api_successes"] == {
        "health_get": 1,
        "snapshot_get": 1,
        "turn_list": 1,
    }
    assert report["daemon"]["api_failures"] == {}
    assert report["herdres"]["mode"] == "daemon_socket_presenter"
    assert report["herdres"]["presenter_passes"] == 2
    assert report["herdres"]["noop_passes"] == 2
    assert report["herdres"]["noop_passes_valid"] == 2
    assert report["herdres"]["connector_poll_requests"] == 2
    assert report["herdres"]["state_file_identity_pinned"] is True
    assert report["herdres"]["daemon_socket_identity_pinned"] is True
    assert report["herdres"]["direct_herdr_calls"] == 0
    assert report["herdres"]["external_network_attempts"] == 0
    assert report["accounting"]["fd_count_after"] == report["accounting"]["fd_count_before"]
    assert report["accounting"]["thread_count_after"] == report["accounting"]["thread_count_before"]
    assert report["accounting"]["direct_children_after"] == report["accounting"]["direct_children_before"]
    assert (
        report["accounting"]["fd_count_peak_observed"]
        > report["accounting"]["fd_count_before"]
    )
    assert (
        report["accounting"]["thread_count_peak_observed"]
        > report["accounting"]["thread_count_before"]
    )
    assert report["accounting"]["socket_present_after"] is False
    assert report["checks"]
    assert all(value is True for value in report["checks"].values())


@_requires_herdres_root
def test_hostile_failure_is_redacted_and_restores_parent_resources() -> None:
    roots_before = _temporary_roots()
    fds_before = _fd_snapshot()
    threads_before = {id(thread) for thread in threading.enumerate()}
    children_before = _direct_children()

    completed = _invoke(
        "--requests-per-method",
        "1",
        "--herdres-presenter-passes",
        "2",
        "--phase-timeout-seconds",
        "30",
        "--inject-failure",
        "herdres",
        "--json",
        testing=True,
    )

    assert completed.returncode == 1
    assert _single_object(completed) == {
        "schema_version": 1,
        "ok": False,
        "status": "benchmark_failed",
        "error_type": "RuntimeError",
    }
    assert _temporary_roots() == roots_before
    assert _fd_snapshot() == fds_before
    assert {id(thread) for thread in threading.enumerate()} == threads_before
    assert _direct_children() == children_before
