from __future__ import annotations

import copy
import fcntl
import importlib.util
import json
import os
import shlex
import stat
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import pytest


ROOT = Path(__file__).resolve().parents[2]
MONITOR_PATH = ROOT / ".deploy/monitor-frozen-7446533-6c0d0f5-hour.py"
GUARD_PATH = ROOT / ".deploy/frozen-cutover-recovery.sh"
RESUME_PATH = ROOT / ".deploy/resume-frozen-cutover.sh"
SPEC = importlib.util.spec_from_file_location("frozen_monitor_failure_safety", MONITOR_PATH)
assert SPEC is not None and SPEC.loader is not None
monitor = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(monitor)


def _configure_monitor_paths(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> tuple[Path, Path, Path, Path]:
    transaction = tmp_path / monitor.TRANSACTION_ID
    transaction.mkdir(mode=0o700)
    phase = transaction / "phase"
    owner = transaction / "validation-monitor-owner"
    heartbeat = transaction / "validation-monitor-heartbeat"
    output = transaction / "one-hour-monitor.json"
    phase.write_text("provisional\n", encoding="utf-8")
    phase.chmod(0o600)
    monkeypatch.setattr(monitor, "TRANSACTION_ROOT", transaction)
    monkeypatch.setattr(monitor, "PHASE_PATH", phase)
    monkeypatch.setattr(monitor, "MONITOR_OWNER_PATH", owner)
    monkeypatch.setattr(monitor, "MONITOR_HEARTBEAT_PATH", heartbeat)
    monkeypatch.setattr(monitor, "DEFAULT_OUTPUT", output)
    return phase, owner, heartbeat, output


def _owner_fields(owner: Path) -> tuple[str, str, str]:
    fields = owner.read_text(encoding="utf-8").strip().split()
    assert len(fields) == 3
    return fields[0], fields[1], fields[2]


def _run_main(monkeypatch: pytest.MonkeyPatch) -> int:
    monkeypatch.setattr(
        sys,
        "argv",
        [str(MONITOR_PATH), "--duration-seconds", "3600", "--interval-seconds", "15"],
    )
    monkeypatch.setattr(monitor.signal, "signal", lambda *_args: None)
    return monitor.main()


def test_monitor_claims_provisional_before_switching_to_validating(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    phase, owner, heartbeat, _output = _configure_monitor_paths(monkeypatch, tmp_path)

    monitor._begin_validation()

    tag, owner_pid, owner_start = _owner_fields(owner)
    current_start = Path("/proc/self/stat").read_text(encoding="utf-8").split()[21]
    assert tag == "FROZEN_ACP_VALIDATION_MONITOR"
    assert owner_pid == str(os.getpid())
    assert owner_start == current_start
    assert stat.S_IMODE(owner.stat().st_mode) == 0o600
    assert heartbeat.read_text(encoding="utf-8") == (
        f"FROZEN_ACP_VALIDATION_HEARTBEAT {os.getpid()} -1\n"
    )
    assert stat.S_IMODE(heartbeat.stat().st_mode) == 0o600
    assert phase.read_text(encoding="utf-8") == "validating\n"


def test_monitor_failure_marks_validation_failed_and_removes_owner(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    phase, owner, heartbeat, output = _configure_monitor_paths(monkeypatch, tmp_path)
    monkeypatch.setattr(
        monitor,
        "_release_integrity_preflight",
        lambda: (_ for _ in ()).throw(monitor.MonitorFailure("fixture_preflight")),
    )

    assert _run_main(monkeypatch) == 1

    report = json.loads(output.read_text(encoding="utf-8"))
    assert phase.read_text(encoding="utf-8") == "validation_failed\n"
    assert not owner.exists()
    assert not heartbeat.exists()
    assert report["status"] == "failed"
    assert report["failure"] == "MonitorFailure"
    assert report["started_at"] is None
    assert report["elapsed_seconds"] == 0
    assert stat.S_IMODE(output.stat().st_mode) == 0o600


def test_finish_validation_removes_owner_after_durable_phase_write(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    phase, owner, heartbeat, _output = _configure_monitor_paths(monkeypatch, tmp_path)
    monitor._begin_validation()
    assert owner.exists()
    assert heartbeat.exists()

    monitor._finish_validation("validation_passed")

    assert phase.read_text(encoding="utf-8") == "validation_passed\n"
    assert not owner.exists()
    assert not heartbeat.exists()


def test_failure_fallback_stops_targets_with_systemctl_fully_stubbed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    phase, owner, heartbeat, output = _configure_monitor_paths(monkeypatch, tmp_path)
    calls: list[tuple[list[str], dict[str, Any]]] = []

    def systemctl_stub(command: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        calls.append((command, kwargs))
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    monkeypatch.setattr(monitor.subprocess, "run", systemctl_stub)
    monkeypatch.setattr(
        monitor,
        "_release_integrity_preflight",
        lambda: (_ for _ in ()).throw(monitor.MonitorFailure("fixture_preflight")),
    )
    monkeypatch.setattr(
        monitor,
        "_finish_validation",
        lambda _phase: (_ for _ in ()).throw(OSError("fixture_phase_write")),
    )
    monkeypatch.setattr(
        monitor,
        "_atomic_json",
        lambda _path, _value: (_ for _ in ()).throw(OSError("fixture_evidence_write")),
    )

    assert _run_main(monkeypatch) == 1

    expected = [
        "systemctl",
        "--user",
        "stop",
        "--no-block",
        "herdres-gateway.service",
        "herdres.service",
        "tendwired.service",
    ]
    assert [command for command, _kwargs in calls] == [expected, expected]
    assert all(kwargs["check"] is False for _command, kwargs in calls)
    assert all(kwargs["env"]["PATH"] == "/usr/bin:/bin" for _command, kwargs in calls)
    assert phase.read_text(encoding="utf-8") == "validating\n"
    assert owner.exists()
    assert heartbeat.read_text(encoding="utf-8") == (
        f"FROZEN_ACP_VALIDATION_HEARTBEAT {os.getpid()} -1\n"
    )
    assert not output.exists()


def test_success_window_clock_starts_after_slow_preflight(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    phase, owner, heartbeat, output = _configure_monitor_paths(monkeypatch, tmp_path)
    clock = [100.0]

    def advance_preflight() -> None:
        clock[0] += 600.0

    def baseline() -> dict[str, str]:
        clock[0] += 600.0
        return {unit: str(index + 100) for index, unit in enumerate(monitor.UNITS)}

    def sample(
        _baseline: dict[str, str], index: int, elapsed: int
    ) -> dict[str, Any]:
        return {"index": index, "elapsed_seconds": elapsed}

    monkeypatch.setattr(monitor, "_release_integrity_preflight", advance_preflight)
    monkeypatch.setattr(monitor, "_release_preflight", advance_preflight)
    monkeypatch.setattr(monitor, "_service_baseline", baseline)
    monkeypatch.setattr(monitor, "_sample", sample)
    monkeypatch.setattr(monitor.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(
        monitor.time,
        "sleep",
        lambda seconds: clock.__setitem__(0, clock[0] + seconds),
    )
    monkeypatch.setattr(monitor, "_utc_now", lambda: f"fixture-clock-{int(clock[0])}")

    assert _run_main(monkeypatch) == 0

    report = json.loads(output.read_text(encoding="utf-8"))
    assert report["status"] == "success"
    assert report["started_at"] == "fixture-clock-1900"
    assert report["completed_at"] == "fixture-clock-5500"
    assert report["samples"][0]["elapsed_seconds"] == 0
    assert report["elapsed_seconds"] == 3600
    assert report["sample_count"] == 241
    assert phase.read_text(encoding="utf-8") == "validation_passed\n"
    assert not owner.exists()
    assert not heartbeat.exists()


def _ordered(source: str, *needles: str) -> None:
    position = -1
    for needle in needles:
        position = source.find(needle, position + 1)
        assert position >= 0, f"missing handoff step: {needle}"


def test_resume_and_guard_preserve_owned_provisional_handoff_contract() -> None:
    resume = RESUME_PATH.read_text(encoding="utf-8")
    resume_main = resume[resume.index('exec 9>"${CUTOVER_LOCK}"') :]
    _ordered(
        resume_main,
        'exec 9>"${CUTOVER_LOCK}"',
        'printf \'%s %s %s\\n\' RESUME_FROZEN_ACP_CUTOVER',
        "phase_write provisional",
        "systemctl --user start acp-frozen-live-monitor.service",
        '[[ "$(<"${PHASE_FILE}")" = validating ]]',
        'rm -- "${RESUME_AUTH}"',
    )

    guard = GUARD_PATH.read_text(encoding="utf-8")
    authorized = guard[guard.index("phase_is_authorized()") : guard.index("stop_targets()")]
    assert "committing|provisional)" in authorized
    assert "committing_owner_is_live" in authorized
    assert "validating)" in authorized
    assert "validation_monitor_is_live" in authorized
    assert "/usr/bin/systemd-notify --ready" in guard
    recovery_unit = (ROOT / ".deploy/acp-frozen-release-recovery.service").read_text(
        encoding="utf-8"
    )
    assert "Type=notify\nNotifyAccess=all\n" in recovery_unit
    validation_owner = guard[
        guard.index("validation_monitor_is_live()") : guard.index("phase_is_authorized()")
    ]
    assert "FROZEN_ACP_VALIDATION_HEARTBEAT" in validation_owner
    assert '"${heartbeat_pid}" = "${owner_pid}"' in validation_owner


def test_guard_accepts_only_live_owned_provisional_then_stops_on_loss(
    tmp_path: Path,
) -> None:
    transaction = tmp_path / "transaction"
    transaction.mkdir(mode=0o700)
    phase = transaction / "phase"
    authorization = transaction / "forward-resume-authorized"
    owner = transaction / "validation-monitor-owner"
    lock_path = tmp_path / "cutover.lock"
    phase.write_text("provisional\n", encoding="utf-8")
    start_time = Path("/proc/self/stat").read_text(encoding="utf-8").split()[21]
    authorization.write_text(
        f"RESUME_FROZEN_ACP_CUTOVER {os.getpid()} {start_time}\n",
        encoding="utf-8",
    )
    authorization.chmod(0o600)

    source = GUARD_PATH.read_text(encoding="utf-8")
    replacements = {
        "readonly TRANSACTION_ROOT=/home/smith/.local/state/acp-cutover/frozen-7446533-6c0d0f5": (
            f"readonly TRANSACTION_ROOT={shlex.quote(str(transaction))}"
        ),
        "readonly CUTOVER_LOCK=/home/smith/.local/state/acp-cutover/frozen-7446533-6c0d0f5.lock": (
            f"readonly CUTOVER_LOCK={shlex.quote(str(lock_path))}"
        ),
    }
    for original, replacement in replacements.items():
        assert original in source
        source = source.replace(original, replacement, 1)
    rendered = tmp_path / "guard.sh"
    rendered.write_text(source, encoding="utf-8")
    rendered.chmod(0o700)

    stub_directory = tmp_path / "stub-bin"
    stub_directory.mkdir()
    systemctl_log = tmp_path / "systemctl.log"
    systemctl = stub_directory / "systemctl"
    systemctl.write_text(
        "#!/bin/sh\n"
        f"printf '%s\\n' \"$*\" >> {shlex.quote(str(systemctl_log))}\n"
        "exit 0\n",
        encoding="utf-8",
    )
    systemctl.chmod(0o700)
    source = rendered.read_text(encoding="utf-8").replace(
        "export PATH=/usr/bin:/bin",
        f"export PATH={shlex.quote(str(stub_directory))}:/usr/bin:/bin",
        1,
    )
    rendered.write_text(source, encoding="utf-8")
    environment = os.environ.copy()
    environment.pop("NOTIFY_SOCKET", None)
    environment["PATH"] = f"{stub_directory}:/usr/bin:/bin"

    with lock_path.open("w", encoding="utf-8") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        process = subprocess.Popen(
            [rendered],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=environment,
        )
        time.sleep(0.2)
        assert process.poll() is None
        assert not systemctl_log.exists()
        phase.write_text("authorization_lost\n", encoding="utf-8")
        stdout, stderr = process.communicate(timeout=3)

    assert process.returncode == 1
    assert stdout == ""
    assert "authorization was lost" in stderr
    assert systemctl_log.read_text(encoding="utf-8").splitlines() == [
        "--user stop --no-block herdres-gateway.service herdres.service tendwired.service"
    ]
    assert not owner.exists()
