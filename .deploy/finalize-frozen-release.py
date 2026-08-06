#!/usr/bin/python3 -I
"""Promote a validated frozen candidate only after every public gate passes."""

from __future__ import annotations

import json
import os
import re
import stat
import subprocess
from pathlib import Path
from typing import Any, Iterator


RELEASE_ID = "9026d9bc-7446533-3659994-r5"
TENDWIRE_REVISION = "7446533bb6fb2560a9a9dd871f638c4a6ccbb086"
HERDRES_REVISION = "36599949daa64f68494d04f96a3bfee31904a804"
HERDR_REVISION = "9026d9bc5a12d9adc2d9f68ebdc564133e4098b4"
TRANSACTION_ROOT = Path("/home/smith/.local/state/acp-cutover/frozen-7446533-6c0d0f5")
PHASE_PATH = TRANSACTION_ROOT / "phase"
MONITOR_PATH = TRANSACTION_ROOT / "one-hour-monitor.json"
RELEASE_ROOT = Path(f"/home/smith/.local/share/acp-runtime/releases/{RELEASE_ID}")
RUNTIME_ROOT = Path("/home/smith/.local/share/tendwire-runtime/acp-7446533-3659994-r5")
MANIFEST = Path(f"/home/smith/.local/share/acp-runtime/manifests/{RELEASE_ID}.json")
EVIDENCE_ROOT = Path("/home/smith/tendwire/docs/evidence")
JSON_ARTIFACTS = (
    EVIDENCE_ROOT / "wave16-cross-repo-e2e.json",
    EVIDENCE_ROOT / "wave16-live-telegram-e2e.json",
    EVIDENCE_ROOT / "wave16-acp-adapter-matrix.json",
)
SUMMARY = EVIDENCE_ROOT / "wave16-release-summary.md"
FORBIDDEN_KEYS = {
    "argv", "command", "cwd", "endpoint", "environment", "fingerprint",
    "message_id", "pane_id", "pid", "prompt", "session", "stderr",
    "stdout", "target", "ticket", "token", "topic_id",
}
TOKEN_PATTERN = re.compile(r"[0-9]{6,20}:[A-Za-z0-9_-]{20,200}")


def canonical(value: dict[str, Any]) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def private_json(path: Path) -> dict[str, Any]:
    info = path.lstat()
    if (
        not stat.S_ISREG(info.st_mode)
        or info.st_uid != os.geteuid()
        or info.st_nlink != 1
        or stat.S_IMODE(info.st_mode) & 0o077
    ):
        raise RuntimeError("private finalization input is unsafe")
    raw = path.read_bytes()
    value = json.loads(raw)
    if not isinstance(value, dict) or raw != canonical(value):
        raise RuntimeError("private finalization input is not canonical")
    return value


def public_file(path: Path) -> bytes:
    info = path.lstat()
    if (
        not stat.S_ISREG(info.st_mode)
        or info.st_uid != os.geteuid()
        or info.st_nlink != 1
        or stat.S_IMODE(info.st_mode) & 0o022
        or info.st_size > 1_048_576
    ):
        raise RuntimeError("public release artifact is unsafe")
    return path.read_bytes()


def walk(value: Any) -> Iterator[tuple[str | None, Any]]:
    if isinstance(value, dict):
        for key, item in value.items():
            yield str(key), item
            yield from walk(item)
    elif isinstance(value, list):
        for item in value:
            yield None, item
            yield from walk(item)


def common_artifact(value: dict[str, Any]) -> None:
    required = {
        "schema_version": 1,
        "release_id": RELEASE_ID,
        "tendwire_revision": TENDWIRE_REVISION,
        "herdres_revision": HERDRES_REVISION,
        "herdr_revision": HERDR_REVISION,
        "status": "pass",
        "privacy_findings": 0,
    }
    if any(value.get(key) != expected for key, expected in required.items()):
        raise RuntimeError("public release artifact did not pass")
    for key, item in walk(value):
        if key is not None and key.lower() in FORBIDDEN_KEYS:
            raise RuntimeError("public release artifact contains a forbidden field")
        if isinstance(item, str) and (
            "/home/" in item or "/run/user/" in item or TOKEN_PATTERN.search(item)
        ):
            raise RuntimeError("public release artifact contains private material")


def phase_write(phase: str) -> None:
    temporary = PHASE_PATH.with_name(f".{PHASE_PATH.name}.tmp.{os.getpid()}")
    descriptor = os.open(
        temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC, 0o600
    )
    try:
        view = memoryview((phase + "\n").encode())
        while view:
            written = os.write(descriptor, view)
            if written < 1:
                raise RuntimeError("finalization phase write failed")
            view = view[written:]
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    os.replace(temporary, PHASE_PATH)
    directory = os.open(TRANSACTION_ROOT, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def main() -> int:
    if PHASE_PATH.read_text(encoding="utf-8").strip() != "validation_passed":
        raise RuntimeError("live validation has not passed")
    subprocess.run(
        [
            str(RELEASE_ROOT / "validate-frozen-release"),
            "--runtime", str(RUNTIME_ROOT), "--release", str(RELEASE_ROOT),
            "--manifest", str(MANIFEST), "--verify",
        ],
        check=True,
        capture_output=True,
        text=True,
        timeout=120,
        env={"PATH": "/usr/bin:/bin", "PYTHONSAFEPATH": "1"},
    )
    monitor = private_json(MONITOR_PATH)
    if (
        monitor.get("release_id") != RELEASE_ID
        or monitor.get("tendwire_revision") != TENDWIRE_REVISION
        or monitor.get("herdres_revision") != HERDRES_REVISION
        or monitor.get("herdr_revision") != HERDR_REVISION
        or monitor.get("status") != "success"
        or type(monitor.get("elapsed_seconds")) is not int
        or monitor["elapsed_seconds"] < 3600
        or type(monitor.get("sample_count")) is not int
        or monitor["sample_count"] < 60
    ):
        raise RuntimeError("one-hour monitor evidence did not pass")
    artifacts: dict[str, dict[str, Any]] = {}
    for path in JSON_ARTIFACTS:
        raw = public_file(path)
        value = json.loads(raw)
        if not isinstance(value, dict) or raw != canonical(value):
            raise RuntimeError("public JSON release artifact is not canonical")
        common_artifact(value)
        artifacts[path.name] = value
    matrix = artifacts["wave16-acp-adapter-matrix.json"]
    rows = matrix.get("adapters")
    if not isinstance(rows, list):
        raise RuntimeError("adapter matrix is incomplete")
    statuses = {
        row.get("adapter"): row.get("status")
        for row in rows
        if isinstance(row, dict)
    }
    if statuses != {
        "codex": "pass", "claude": "pass", "gemini": "pass",
        "hermes": "pass", "omp": "pass", "kimi": "owner_exempt",
    } or matrix.get("kimi_model_process_invocations") != 0:
        raise RuntimeError("adapter matrix did not pass")
    summary = public_file(SUMMARY).decode("utf-8")
    required_summary = (
        f"release_id: {RELEASE_ID}", f"tendwire_revision: {TENDWIRE_REVISION}",
        f"herdres_revision: {HERDRES_REVISION}", f"herdr_revision: {HERDR_REVISION}",
        "status: pass", "privacy_findings: 0", "kimi_model_process_invocations: 0",
    )
    if any(line not in summary for line in required_summary):
        raise RuntimeError("release summary did not pass")
    if "/home/" in summary or "/run/user/" in summary or TOKEN_PATTERN.search(summary):
        raise RuntimeError("release summary contains private material")
    phase_write("deployed")
    print(json.dumps({"schema_version": 1, "release_id": RELEASE_ID, "status": "deployed"}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
