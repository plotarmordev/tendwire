#!/usr/bin/python3 -I
"""Block r8 writers after an incomplete or failed cutover."""

from __future__ import annotations

import os
import json
import socket
import subprocess
import time
from pathlib import Path


# R8_IDENTITY_PLACEHOLDER: replace this release's Herdr hash and every matching
# path atomically after the reviewed Herdr commit exists; 9026d9bc is not live.
ROOT = Path("/home/smith/.local/state/acp-cutover/frozen-0b94403-r8")
PHASE = ROOT / "phase"
OWNER = ROOT / "rollout-owner"
MONITOR_OWNER = ROOT / "validation-monitor-owner"
MONITOR_HEARTBEAT = ROOT / "validation-monitor-heartbeat"
EVIDENCE = ROOT / "strict-live-proof.json"
STATUS = ROOT / "rollout-status.json"
HERDR_BASELINE = ROOT / "herdr-baseline.json"
ACTIVE = Path("/home/smith/.local/share/acp-runtime/active")
RELEASE = Path(
    "/home/smith/.local/share/acp-runtime/releases/67568f32-0b94403-f50af73-r8"
)
MANIFEST = Path(
    "/home/smith/.local/share/acp-runtime/manifests/"
    "67568f32-0b94403-f50af73-r8.json"
)
EXPECTED_HERDR = RELEASE / "herdr"
EXPECTED_PATH = (
    "/home/smith/.local/share/acp-adapters/codex-acp-7cb0524624f2e730f48c3dac9b547ca130964ae9/"
    "bin:/home/smith/.local/bin:/usr/local/bin:/usr/bin:/bin"
)
BOOT_ID_PATH = Path("/proc/sys/kernel/random/boot_id")


def live_owner() -> bool:
    try:
        prefix, raw_pid, expected_start = OWNER.read_text(encoding="ascii").split()
        pid = int(raw_pid)
        actual_start = process_start_time(pid)
        command = Path(f"/proc/{pid}/cmdline").read_bytes().replace(b"\0", b" ")
    except (OSError, ValueError, IndexError):
        return False
    return (
        prefix == "R8_ROLLOUT"
        and pid > 1
        and expected_start == actual_start
        and b"rollout-acp-release-r8" in command
        and os.geteuid() == 1000
    )


def live_monitor() -> bool:
    try:
        prefix, raw_pid, expected_start = MONITOR_OWNER.read_text(
            encoding="ascii"
        ).split()
        pid = int(raw_pid)
        actual_start = process_start_time(pid)
        command = Path(f"/proc/{pid}/cmdline").read_bytes().replace(b"\0", b" ")
        heartbeat_age = time.time() - MONITOR_HEARTBEAT.stat().st_mtime
        heartbeat_prefix, heartbeat_pid = MONITOR_HEARTBEAT.read_text(
            encoding="ascii"
        ).split()
    except (OSError, ValueError, IndexError):
        return False
    return (
        prefix == "FROZEN_ACP_VALIDATION_MONITOR"
        and pid > 1
        and expected_start == actual_start
        and b"monitor-one-hour-strict" in command
        and heartbeat_prefix == "FROZEN_ACP_STRICT_HEARTBEAT"
        and int(heartbeat_pid) == pid
        and 0 <= heartbeat_age <= 60
    )


def strict_success() -> bool:
    try:
        evidence = json.loads(EVIDENCE.read_text(encoding="utf-8"))
        status = json.loads(STATUS.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return False
    return (
        evidence.get("status") == "success"
        and evidence.get("correlated_live_chains", 0) > 0
        and evidence.get("release_integrity_valid") is True
        and evidence.get("installed_config_attestation_valid") is True
        and evidence.get("telegram_render_verified") is True
        and evidence.get("verified_telegram_final_parts", 0) > 0
        and evidence.get("herdr_restarted") is False
        and evidence.get("historical_recovery") is False
        and status.get("state") == "success"
        and status.get("phase") == "validation_passed"
        and status.get("herdr_restarted") is False
        and status.get("historical_recovery") is False
    )


def process_start_time(pid: int) -> str:
    raw = Path(f"/proc/{pid}/stat").read_text(encoding="ascii")
    suffix = raw.rsplit(") ", 1)[1].split()
    return suffix[19]


def herdr_unchanged() -> bool:
    try:
        expected = json.loads(HERDR_BASELINE.read_text(encoding="utf-8"))
        pid = int(
            subprocess.run(
                [
                    "systemctl",
                    "--user",
                    "show",
                    "--value",
                    "-p",
                    "MainPID",
                    "herdr-server.service",
                ],
                check=True,
                capture_output=True,
                text=True,
                timeout=10,
            ).stdout.strip()
        )
        actual = {
            "schema_version": 1,
            "pid": pid,
            "start_time": process_start_time(pid),
            "boot_id": BOOT_ID_PATH.read_text(encoding="ascii").strip(),
        }
    except (OSError, ValueError, IndexError, subprocess.SubprocessError):
        return False
    return actual == expected


def accepted_release_is_healthy() -> bool:
    """Validate durable identities without pinning a PID or boot instance."""
    try:
        if ACTIVE.resolve(strict=True) != RELEASE:
            return False
        subprocess.run(
            [str(RELEASE / "release-integrity"), "verify", "--manifest", str(MANIFEST)],
            check=True,
            capture_output=True,
            text=True,
            timeout=30,
        )
        pid = int(
            subprocess.run(
                [
                    "systemctl",
                    "--user",
                    "show",
                    "--value",
                    "-p",
                    "MainPID",
                    "herdr-server.service",
                ],
                check=True,
                capture_output=True,
                text=True,
                timeout=10,
            ).stdout.strip()
        )
        if pid <= 1:
            return False
        actual_exe = Path(f"/proc/{pid}/exe").resolve(strict=True)
        expected_exe = EXPECTED_HERDR.resolve(strict=True)
        environment = Path(f"/proc/{pid}/environ").read_bytes().split(b"\0")
        path_values = [
            item.removeprefix(b"PATH=").decode("utf-8")
            for item in environment
            if item.startswith(b"PATH=")
        ]
    except (OSError, UnicodeError, ValueError, subprocess.SubprocessError):
        return False
    return actual_exe == expected_exe and path_values == [EXPECTED_PATH]


def evaluate_once() -> None:
    phase = PHASE.read_text(encoding="ascii").strip()
    if phase == "validation_passed":
        allowed = strict_success() and accepted_release_is_healthy()
        herdr_valid = True
    else:
        allowed = (
            (phase == "committing" and live_owner())
            or (phase == "provisional" and live_owner())
            or (phase == "validating" and live_monitor())
        )
        herdr_valid = herdr_unchanged()
    if not allowed or not herdr_valid:
        raise RuntimeError("r8 release phase does not authorize service operation")


def notify_ready() -> None:
    """Tell Type=notify systemd only after the synchronous first precheck."""
    address = os.environ.get("NOTIFY_SOCKET")
    if not address:
        raise RuntimeError("release guard is missing NOTIFY_SOCKET")
    if address.startswith("@"):
        address = "\0" + address[1:]
    with socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM | socket.SOCK_CLOEXEC) as channel:
        channel.connect(address)
        channel.sendall(b"READY=1\nSTATUS=r8 release guard precheck passed")


def main() -> int:
    evaluate_once()
    notify_ready()
    while True:
        evaluate_once()
        time.sleep(1)


if __name__ == "__main__":
    raise SystemExit(main())
