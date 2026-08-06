#!/usr/bin/python3 -I
"""Collect aggregate-only one-hour evidence for the frozen ACP release."""

from __future__ import annotations

import argparse
import json
import os
import signal
import sqlite3
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


RELEASE_ID = "9026d9bc-7446533-3659994-r5"
TRANSACTION_ID = "frozen-7446533-6c0d0f5"
TENDWIRE_REVISION = "7446533bb6fb2560a9a9dd871f638c4a6ccbb086"
HERDRES_REVISION = "36599949daa64f68494d04f96a3bfee31904a804"
HERDR_REVISION = "9026d9bc5a12d9adc2d9f68ebdc564133e4098b4"
ACTIVE_LINK = Path("/home/smith/.local/share/acp-runtime/active")
RELEASE_ROOT = Path(f"/home/smith/.local/share/acp-runtime/releases/{RELEASE_ID}")
MANIFEST = Path(f"/home/smith/.local/share/acp-runtime/manifests/{RELEASE_ID}.json")
TRANSACTION_ROOT = Path(f"/home/smith/.local/state/acp-cutover/{TRANSACTION_ID}")
TENDWIRE_RUNTIME = Path("/home/smith/.local/share/tendwire-runtime/acp-7446533-3659994-r5")
TENDWIRE_PYTHON = RELEASE_ROOT / "tendwire/bin/python"
TENDWIRE_SOCKET = Path("/home/smith/.local/share/tendwire/tendwire.sock")
TENDWIRE_DB = Path("/home/smith/.local/share/tendwire/candidates/7446533/tendwire.db")
HERDRES_STATE = Path("/home/smith/.local/share/herdres/candidates/3659994/state.json")
HERDRES_INGRESS = Path("/home/smith/.local/share/herdres/candidates/3659994/ingress.db")
TOPIC_TOOL = RELEASE_ROOT / "reset-telegram-topics"
TOPIC_PYTHON = Path("/home/smith/.local/share/uv/tools/contexto/bin/python")
DEFAULT_OUTPUT = TRANSACTION_ROOT / "one-hour-monitor.json"
PHASE_PATH = TRANSACTION_ROOT / "phase"
MONITOR_OWNER_PATH = TRANSACTION_ROOT / "validation-monitor-owner"
MONITOR_HEARTBEAT_PATH = TRANSACTION_ROOT / "validation-monitor-heartbeat"
MAX_STATE_BYTES = 16_777_216
UNITS = (
    "herdr-server.service",
    "tendwired.service",
    "herdres.service",
    "herdres-gateway.service",
)
HEALTH_PROGRAM = r"""
import json
import sys
from tendwire.daemon_api import DaemonAPIClient

client = DaemonAPIClient(sys.argv[1], timeout_seconds=5)
response = client.request("health.get", {})
snapshot_response = client.request("snapshot.get", {})
result = response.get("result") or {}
snapshot = snapshot_response.get("result") or {}
daemon = result.get("daemon") or {}
backend = result.get("backend") or {}
acp = result.get("acp") or {}
workers = snapshot.get("workers")
healthy = (
    response.get("ok") is True
    and snapshot_response.get("ok") is True
    and daemon.get("status") == "healthy"
    and backend.get("status") == "healthy"
    and backend.get("ready") is True
    and backend.get("running") is True
    and acp.get("healthy") is True
    and acp.get("state") == "running"
    and acp.get("failure_type") is None
    and acp.get("worker_count") == 1
    and isinstance(workers, list)
    and len(workers) == 1
    and isinstance(workers[0], dict)
    and workers[0].get("name") == "codex"
    and workers[0].get("status") in {"active", "idle", "waiting", "blocked"}
)
print(json.dumps({"healthy": healthy, "worker_count": len(workers) if isinstance(workers, list) else -1}, separators=(",", ":")))
raise SystemExit(0 if healthy else 1)
"""


class MonitorFailure(RuntimeError):
    pass


def _utc_now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _atomic_json(path: Path, value: dict[str, Any]) -> None:
    if path.parent != TRANSACTION_ROOT or path.name != DEFAULT_OUTPUT.name:
        raise MonitorFailure("evidence output is not transaction-bound")
    temporary = path.with_name(f".{path.name}.tmp.{os.getpid()}")
    descriptor = os.open(
        temporary,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC,
        0o600,
    )
    try:
        body = (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()
        view = memoryview(body)
        while view:
            written = os.write(descriptor, view)
            if written < 1:
                raise MonitorFailure("evidence write failed")
            view = view[written:]
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    os.replace(temporary, path)
    directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def _atomic_private(path: Path, body: bytes) -> None:
    if path.parent != TRANSACTION_ROOT:
        raise MonitorFailure("private monitor path is not transaction-bound")
    temporary = path.with_name(f".{path.name}.tmp.{os.getpid()}")
    descriptor = os.open(
        temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC, 0o600
    )
    try:
        view = memoryview(body)
        while view:
            written = os.write(descriptor, view)
            if written < 1:
                raise MonitorFailure("private monitor write failed")
            view = view[written:]
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    os.replace(temporary, path)
    directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def _phase_write(phase: str) -> None:
    _atomic_private(PHASE_PATH, (phase + "\n").encode())


def _begin_validation() -> None:
    if PHASE_PATH.read_text(encoding="utf-8").strip() != "provisional":
        raise MonitorFailure("transaction_not_provisional")
    if MONITOR_OWNER_PATH.exists() or MONITOR_OWNER_PATH.is_symlink():
        raise MonitorFailure("validation_monitor_owner_exists")
    start_time = Path("/proc/self/stat").read_text(encoding="utf-8").split()[21]
    if not start_time.isdigit() or int(start_time) < 1:
        raise MonitorFailure("validation_monitor_start_time_invalid")
    _atomic_private(
        MONITOR_OWNER_PATH,
        f"FROZEN_ACP_VALIDATION_MONITOR {os.getpid()} {start_time}\n".encode(),
    )
    _heartbeat(-1)
    _phase_write("validating")


def _heartbeat(index: int) -> None:
    _atomic_private(
        MONITOR_HEARTBEAT_PATH,
        f"FROZEN_ACP_VALIDATION_HEARTBEAT {os.getpid()} {index}\n".encode(),
    )


def _stop_targets() -> None:
    subprocess.run(
        [
            "systemctl", "--user", "stop", "--no-block",
            "herdres-gateway.service", "herdres.service", "tendwired.service",
        ],
        check=False,
        env={
            "PATH": "/usr/bin:/bin",
            "XDG_RUNTIME_DIR": os.environ.get("XDG_RUNTIME_DIR", f"/run/user/{os.geteuid()}"),
            **(
                {"DBUS_SESSION_BUS_ADDRESS": os.environ["DBUS_SESSION_BUS_ADDRESS"]}
                if "DBUS_SESSION_BUS_ADDRESS" in os.environ
                else {}
            ),
        },
    )


def _finish_validation(phase: str) -> None:
    _phase_write(phase)
    try:
        MONITOR_OWNER_PATH.unlink(missing_ok=True)
        MONITOR_HEARTBEAT_PATH.unlink(missing_ok=True)
    finally:
        directory = os.open(TRANSACTION_ROOT, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)


def _run(command: list[str], *, timeout: int = 10) -> str:
    environment = {
        "PATH": "/usr/bin:/bin",
        "PYTHONPATH": "",
        "PYTHONSAFEPATH": "1",
        "XDG_RUNTIME_DIR": os.environ.get("XDG_RUNTIME_DIR", f"/run/user/{os.geteuid()}"),
    }
    if "DBUS_SESSION_BUS_ADDRESS" in os.environ:
        environment["DBUS_SESSION_BUS_ADDRESS"] = os.environ["DBUS_SESSION_BUS_ADDRESS"]
    result = subprocess.run(
        command,
        check=True,
        capture_output=True,
        text=True,
        timeout=timeout,
        env=environment,
    )
    return result.stdout.strip()


def _unit_value(unit: str, property_name: str) -> str:
    return _run(
        ["systemctl", "--user", "show", "--value", "-p", property_name, unit]
    )


def _release_preflight() -> None:
    if ACTIVE_LINK.resolve(strict=True) != RELEASE_ROOT:
        raise MonitorFailure("active_release_mismatch")
    if PHASE_PATH.read_text(encoding="utf-8").strip() != "validating":
        raise MonitorFailure("transaction_not_validating")
    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
    expected = {
        "tendwire_revision": TENDWIRE_REVISION,
        "herdres_revision": HERDRES_REVISION,
        "herdr_revision": HERDR_REVISION,
    }
    if any(manifest.get(key) != value for key, value in expected.items()):
        raise MonitorFailure("release_manifest_mismatch")


def _release_integrity_preflight() -> None:
    value = json.loads(
        _run(
            [
                str(RELEASE_ROOT / "validate-frozen-release"),
                "--runtime", str(TENDWIRE_RUNTIME),
                "--release", str(RELEASE_ROOT),
                "--manifest", str(MANIFEST),
                "--verify",
            ],
            timeout=120,
        )
    )
    if value != {"schema_version": 1, "release_id": RELEASE_ID, "valid": True}:
        raise MonitorFailure("release_integrity_failed")


def _service_baseline() -> dict[str, str]:
    result: dict[str, str] = {}
    for unit in UNITS:
        if _unit_value(unit, "ActiveState") != "active":
            raise MonitorFailure("service_not_active")
        if _unit_value(unit, "NRestarts") != "0":
            raise MonitorFailure("service_restart_baseline_nonzero")
        pid = _unit_value(unit, "MainPID")
        if not pid.isdigit() or int(pid) < 1:
            raise MonitorFailure("service_pid_invalid")
        result[unit] = pid
    return result


def _service_sample(baseline: dict[str, str]) -> dict[str, Any]:
    for unit, pid in baseline.items():
        if _unit_value(unit, "ActiveState") != "active":
            raise MonitorFailure("service_became_inactive")
        if _unit_value(unit, "NRestarts") != "0":
            raise MonitorFailure("service_restarted")
        if _unit_value(unit, "MainPID") != pid:
            raise MonitorFailure("service_pid_changed")
    return {"active": len(UNITS), "stable": len(UNITS), "restart_count": 0}


def _health_sample() -> dict[str, Any]:
    output = _run(
        [str(TENDWIRE_PYTHON), "-B", "-I", "-c", HEALTH_PROGRAM, str(TENDWIRE_SOCKET)],
        timeout=10,
    )
    value = json.loads(output)
    if value != {"healthy": True, "worker_count": 1}:
        raise MonitorFailure("acp_health_failed")
    return value


def _fixed_counts(
    connection: sqlite3.Connection,
    table: str,
    column: str,
    values: tuple[str, ...],
) -> dict[str, int]:
    result: dict[str, int] = {}
    for value in values:
        row = connection.execute(
            f"SELECT COUNT(*) FROM {table} WHERE {column}=?",  # fixed identifiers
            (value,),
        ).fetchone()
        result[value] = int(row[0])
    total = int(connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
    result["other"] = total - sum(result.values())
    return result


def _database_sample() -> dict[str, Any]:
    tendwire = sqlite3.connect(f"file:{TENDWIRE_DB}?mode=ro", uri=True, timeout=5)
    ingress = sqlite3.connect(f"file:{HERDRES_INGRESS}?mode=ro", uri=True, timeout=5)
    try:
        if int(tendwire.execute("PRAGMA user_version").fetchone()[0]) != 30:
            raise MonitorFailure("tendwire_schema_changed")
        if int(ingress.execute("PRAGMA user_version").fetchone()[0]) != 1:
            raise MonitorFailure("ingress_schema_changed")
        outbox = _fixed_counts(
            tendwire,
            "connector_outbox",
            "status",
            (
                "staged", "blocked", "queued", "leased", "retry", "deferred",
                "awaiting_ack", "delivered", "superseded", "dead_letter",
            ),
        )
        requests = _fixed_counts(
            ingress,
            "requests",
            "state",
            ("pending", "processing", "retry", "terminal", "quarantine"),
        )
        cursor_set = {
            str(row[0])
            for row in ingress.execute("SELECT receiver_id FROM receiver_cursors")
        }
    finally:
        tendwire.close()
        ingress.close()
    if outbox["other"] != 0:
        raise MonitorFailure("outbox_status_unknown")
    if requests["other"] != 0:
        raise MonitorFailure("ingress_state_unknown")
    expected_cursors = {"manager", "managed-codex", "managed-omp", "managed-kimi"}
    if cursor_set != expected_cursors:
        raise MonitorFailure("receiver_cursor_set_changed")
    return {
        "outbox_status": outbox,
        "ingress_state": requests,
        "receiver_cursor_count": len(cursor_set),
    }


def _state_sample() -> dict[str, Any]:
    if HERDRES_STATE.stat().st_size > MAX_STATE_BYTES:
        raise MonitorFailure("herdres_state_exceeds_bound")
    raw = HERDRES_STATE.read_bytes()
    if len(raw) > MAX_STATE_BYTES:
        raise MonitorFailure("herdres_state_exceeds_bound")
    value = json.loads(raw)
    if not isinstance(value, dict) or value.get("schema_version") != 1:
        raise MonitorFailure("herdres_state_schema_changed")
    result: dict[str, dict[str, int]] = {}
    specifications = {
        "workers": ("lifecycle_status", ("live", "quarantined", "retired")),
        "topics": ("status", ("active", "closed", "retiring", "quarantined", "retired", "gone")),
        "decision_controls": ("status", ("active", "resolved", "retired", "quarantined")),
    }
    for collection, (field, statuses) in specifications.items():
        rows = value.get(collection)
        if not isinstance(rows, dict):
            raise MonitorFailure("herdres_state_incomplete")
        counts = {status: 0 for status in statuses}
        counts["other"] = 0
        for row in rows.values():
            status = row.get(field) if isinstance(row, dict) else None
            counts[status if status in counts else "other"] += 1
        result[collection] = counts
        if counts["other"] != 0:
            raise MonitorFailure("herdres_lifecycle_status_unknown")
    workers = result["workers"]
    topics = result["topics"]
    if workers["live"] != 1 or topics["active"] != 1:
        raise MonitorFailure("herdres_live_topic_cardinality_changed")
    return result


def _forum_sample() -> dict[str, Any]:
    command = [str(TOPIC_PYTHON), "-I", str(TOPIC_TOOL), "--monitor-sample"]
    for attempt in range(2):
        try:
            output = _run(command, timeout=30)
            break
        except subprocess.TimeoutExpired:
            if attempt == 1:
                raise MonitorFailure("telegram_forum_probe_timeout") from None
    value = json.loads(output)
    expected_fields = {
        "schema_version", "mode", "live_workers", "active_bindings",
        "forum_topics", "general_preserved", "unsettled_topic_claims",
        "plan_sha256",
    }
    if (
        not isinstance(value, dict)
        or set(value) != expected_fields
        or value.get("schema_version") != 1
        or value.get("mode") != "monitor_sample"
        or type(value.get("live_workers")) is not int
        or value["live_workers"] != 1
        or value.get("active_bindings") != value["live_workers"]
        or value.get("forum_topics") != value["live_workers"] + 1
        or value.get("general_preserved") is not True
        or value.get("unsettled_topic_claims") != 0
        or not isinstance(value.get("plan_sha256"), str)
        or len(value["plan_sha256"]) != 64
    ):
        raise MonitorFailure("telegram_forum_invariant_failed")
    return {
        "live_workers": value["live_workers"],
        "active_bindings": value["active_bindings"],
        "forum_topics": value["forum_topics"],
        "general_preserved": True,
        "unsettled_topic_claims": 0,
    }


def _sample(baseline: dict[str, str], index: int, elapsed: int) -> dict[str, Any]:
    _release_preflight()
    return {
        "index": index,
        "observed_at": _utc_now(),
        "elapsed_seconds": elapsed,
        "services": _service_sample(baseline),
        "health": _health_sample(),
        "stores": _database_sample(),
        "lifecycle": _state_sample(),
        "telegram_forum": _forum_sample(),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--duration-seconds", type=int, default=3600)
    parser.add_argument("--interval-seconds", type=int, default=15)
    args = parser.parse_args()
    if args.duration_seconds < 3600:
        raise SystemExit("release monitor duration must be at least 3600 seconds")
    if args.interval_seconds != 15:
        raise SystemExit("invalid monitor timing")
    TRANSACTION_ROOT.mkdir(mode=0o700, parents=True, exist_ok=True)

    interrupted = False

    def stop(_signum: int, _frame: Any) -> None:
        nonlocal interrupted
        interrupted = True

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)
    started: float | None = None
    evidence: dict[str, Any] = {
        "schema_version": 1,
        "release_id": RELEASE_ID,
        "transaction_id": TRANSACTION_ID,
        "tendwire_revision": TENDWIRE_REVISION,
        "herdres_revision": HERDRES_REVISION,
        "herdr_revision": HERDR_REVISION,
        "started_at": None,
        "required_duration_seconds": args.duration_seconds,
        "interval_seconds": args.interval_seconds,
        "status": "initializing",
        "aggregate_counts_only": True,
        "contains_process_ids": False,
        "telegram_live_proof": "concurrent_forum_invariants",
        "samples": [],
    }
    validation_started = False
    try:
        _begin_validation()
        validation_started = True
        _release_integrity_preflight()
        _release_preflight()
        baseline = _service_baseline()
        if interrupted:
            raise MonitorFailure("monitor_interrupted")
        evidence["started_at"] = _utc_now()
        evidence["status"] = "running"
        started = time.monotonic()
        index = 0
        while True:
            assert started is not None
            elapsed = int(time.monotonic() - started)
            if evidence["samples"] and elapsed - evidence["samples"][-1]["elapsed_seconds"] > 60:
                raise MonitorFailure("monitor_sample_gap_exceeded")
            evidence["samples"].append(_sample(baseline, index, elapsed))
            evidence["sample_count"] = len(evidence["samples"])
            _atomic_json(DEFAULT_OUTPUT, evidence)
            _heartbeat(index)
            if elapsed >= args.duration_seconds:
                break
            if interrupted:
                raise MonitorFailure("monitor_interrupted")
            remaining = args.duration_seconds - (time.monotonic() - started)
            time.sleep(min(float(args.interval_seconds), max(0.0, remaining)))
            index += 1
        evidence["status"] = "success"
        evidence["completed_at"] = _utc_now()
        evidence["elapsed_seconds"] = int(time.monotonic() - started)
        if evidence["elapsed_seconds"] < args.duration_seconds:
            raise MonitorFailure("monitor_window_too_short")
        if evidence["sample_count"] < 60:
            raise MonitorFailure("monitor_sample_count_too_low")
        _atomic_json(DEFAULT_OUTPUT, evidence)
        _finish_validation("validation_passed")
        print(
            f"MONITOR_SUCCESS release_id={RELEASE_ID} "
            f"elapsed_seconds={evidence['elapsed_seconds']} samples={evidence['sample_count']}"
        )
        return 0
    except Exception as exc:
        evidence["status"] = "failed"
        evidence["completed_at"] = _utc_now()
        evidence["elapsed_seconds"] = (
            0 if started is None else int(time.monotonic() - started)
        )
        evidence["failure"] = type(exc).__name__
        if validation_started or MONITOR_OWNER_PATH.exists():
            try:
                _finish_validation("validation_failed")
            except Exception:
                _stop_targets()
        try:
            _atomic_json(DEFAULT_OUTPUT, evidence)
        except Exception:
            _stop_targets()
        print(f"MONITOR_FAILURE release_id={RELEASE_ID} reason={type(exc).__name__}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
