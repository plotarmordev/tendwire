#!/usr/bin/env python3
"""Hermetic installed-candidate evidence for SQLite sidecar race recovery.

The public invocation builds a versioned wheel from this checkout, installs it in
an isolated virtual environment, and re-executes the measured phases with that
candidate.  Only one compact aggregate JSON object is written to stdout.
"""

from __future__ import annotations

import argparse
import ast
import base64
import copy
import hashlib
import importlib
import importlib.metadata
import json
import math
import multiprocessing
import os
import platform
import resource
import shutil
import sqlite3
import socket
import stat
import subprocess
import sys
import tempfile
import threading
import time
import tomllib
import venv
import zipfile
from collections import Counter
from collections.abc import Callable, Mapping
from pathlib import Path
from time import perf_counter_ns, process_time_ns
from typing import Any

REPORT_SCHEMA_VERSION = 1
DEFAULT_ITERATIONS = 128
DEFAULT_DAEMON_CYCLES = 64
DEFAULT_REQUESTS = 64
DEFAULT_TIMEOUT_SECONDS = 120.0
HOST_LATENCY_BUDGET_NS = 350_000_000
FIXTURE_HOST = "generated-sidecar-evidence-host"
FIXTURE_TIMESTAMP = "2026-07-12T00:00:00+00:00"
FIXTURE_WORKER = "generated-sidecar-evidence-worker"
FIXTURE_AGENT = "generated-sidecar-evidence-agent"
FIXTURE_PANE = "generated-sidecar-evidence-pane"
FIXTURE_FINAL = "generated sidecar evidence final"
_FORBIDDEN_SUCCESS_KEYS = {"error", "errors", "error_type"}


class _ArgumentError(ValueError):
    pass


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _nearest_rank(samples: list[int], percentile: float) -> int:
    if not samples:
        raise RuntimeError("samples_required")
    ordered = sorted(samples)
    return ordered[max(1, math.ceil(percentile * len(ordered))) - 1]


def _latency_metric(samples: list[int], response_bytes: list[int]) -> dict[str, Any]:
    p95 = _nearest_rank(samples, 0.95)
    return {
        "samples": len(samples),
        "min_ns": min(samples),
        "p50_ns": _nearest_rank(samples, 0.50),
        "p95_ns": p95,
        "max_ns": max(samples),
        "response_bytes_max": max(response_bytes),
        "documented_host_budget_ns": HOST_LATENCY_BUDGET_NS,
        "documented_host_budget_met": p95 <= HOST_LATENCY_BUDGET_NS,
    }


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
            int(stat.S_IFMT(observed.st_mode)),
        )
    return result


def _thread_snapshot() -> set[int]:
    return {id(thread) for thread in threading.enumerate()}


def _direct_children() -> set[int]:
    children: set[int] = set()
    task_root = Path("/proc/self/task")
    for task in task_root.iterdir():
        try:
            values = (task / "children").read_text(encoding="ascii").split()
        except FileNotFoundError:
            continue
        children.update(int(value) for value in values)
    return children


def _resource_counts() -> dict[str, int]:
    return {
        "fds": len(_fd_snapshot()),
        "threads": len(_thread_snapshot()),
        "direct_children": len(_direct_children()),
    }


def _merge_resource_peak(target: dict[str, int], observed: Mapping[str, int]) -> None:
    for name in ("fds", "threads", "direct_children"):
        target[name] = max(target.get(name, 0), int(observed.get(name, 0)))


class _SubprocessResourceObserver:
    def __init__(self) -> None:
        self.peak = _resource_counts()
        self._original = subprocess.Popen

    def __enter__(self) -> "_SubprocessResourceObserver":
        original = self._original

        def observed_popen(*args: Any, **kwargs: Any) -> subprocess.Popen[Any]:
            process = original(*args, **kwargs)
            _merge_resource_peak(self.peak, _resource_counts())
            return process

        subprocess.Popen = observed_popen  # type: ignore[assignment]
        return self

    def __exit__(self, _type: Any, _value: Any, _traceback: Any) -> None:
        subprocess.Popen = self._original


class _OutboundNetworkGuard:
    def __init__(self) -> None:
        self.attempts = 0
        self._connect = socket.socket.connect
        self._connect_ex = socket.socket.connect_ex
        self._sendto = socket.socket.sendto

    def _blocked(self, current: socket.socket) -> bool:
        return current.family in (socket.AF_INET, socket.AF_INET6)

    def __enter__(self) -> "_OutboundNetworkGuard":
        guard = self

        def connect(current: socket.socket, address: Any) -> Any:
            if guard._blocked(current):
                guard.attempts += 1
                raise OSError("outbound network disabled")
            return guard._connect(current, address)

        def connect_ex(current: socket.socket, address: Any) -> int:
            if guard._blocked(current):
                guard.attempts += 1
                return 1
            return guard._connect_ex(current, address)

        def sendto(current: socket.socket, data: Any, *args: Any) -> int:
            if guard._blocked(current):
                guard.attempts += 1
                raise OSError("outbound network disabled")
            return guard._sendto(current, data, *args)

        socket.socket.connect = connect
        socket.socket.connect_ex = connect_ex
        socket.socket.sendto = sendto
        return self

    def __exit__(self, _type: Any, _value: Any, _traceback: Any) -> None:
        socket.socket.connect = self._connect
        socket.socket.connect_ex = self._connect_ex
        socket.socket.sendto = self._sendto


def _usage() -> tuple[float, float, float, float]:
    own = resource.getrusage(resource.RUSAGE_SELF)
    child = resource.getrusage(resource.RUSAGE_CHILDREN)
    return own.ru_utime, own.ru_stime, child.ru_utime, child.ru_stime


def _usage_delta(before: tuple[float, ...], after: tuple[float, ...]) -> dict[str, int]:
    names = ("self_user_ns", "self_system_ns", "children_user_ns", "children_system_ns")
    return {
        name: max(0, int((end - start) * 1_000_000_000))
        for name, start, end in zip(names, before, after, strict=True)
    }


def _privacy_scan(value: Any, forbidden_values: list[str]) -> bool:
    serialized = _canonical_json(value)
    lowered = serialized.lower()
    for private in forbidden_values:
        if private and private.lower() in lowered:
            return False

    def inspect(item: Any) -> bool:
        if isinstance(item, Mapping):
            for key, nested in item.items():
                if str(key).lower() in _FORBIDDEN_SUCCESS_KEYS:
                    return False
                if not inspect(nested):
                    return False
        elif isinstance(item, (list, tuple)):
            return all(inspect(nested) for nested in item)
        return True

    return inspect(value)


def _source_revision(checkout: Path) -> str:
    completed = subprocess.run(
        ["git", "rev-parse", "--verify", "HEAD"],
        cwd=checkout,
        stdin=subprocess.DEVNULL,
        capture_output=True,
        text=True,
        check=False,
        timeout=10,
    )
    revision = completed.stdout.strip()
    if completed.returncode != 0 or len(revision) != 40:
        raise RuntimeError("source_revision_unavailable")
    return revision


def _wheel_record_line(name: str, content: bytes) -> str:
    digest = base64.urlsafe_b64encode(hashlib.sha256(content).digest()).rstrip(b"=").decode("ascii")
    return f"{name},sha256={digest},{len(content)}"


def _build_versioned_wheel(
    checkout: Path,
    wheel_dir: Path,
) -> tuple[Path, str, str, str]:
    project = tomllib.loads((checkout / "pyproject.toml").read_text(encoding="utf-8"))[
        "project"
    ]
    name = str(project["name"]).replace("-", "_")
    version_tree = ast.parse(
        (checkout / "src/tendwire/_version.py").read_text(encoding="utf-8")
    )
    version = ""
    for node in version_tree.body:
        if isinstance(node, ast.Assign) and any(
            isinstance(target, ast.Name) and target.id == "__version__"
            for target in node.targets
        ):
            version = str(ast.literal_eval(node.value))
            break
    if not version or "/" in version or "\\" in version:
        raise RuntimeError("candidate_version_invalid")
    wheel_path = wheel_dir / f"{name}-{version}-py3-none-any.whl"
    dist_info = f"{name}-{version}.dist-info"
    entries: dict[str, bytes] = {}
    source_tree = hashlib.sha256()
    package_root = checkout / "src" / "tendwire"
    for source in sorted(package_root.rglob("*")):
        if (
            source.is_file()
            and "__pycache__" not in source.parts
            and not source.name.endswith((".pyc", ".pyo"))
        ):
            relative = source.relative_to(package_root.parent).as_posix()
            content = source.read_bytes()
            entries[relative] = content
            source_tree.update(relative.encode("utf-8"))
            source_tree.update(b"\0")
            source_tree.update(len(content).to_bytes(8, "big"))
            source_tree.update(content)
    entries[f"{dist_info}/METADATA"] = (
        "Metadata-Version: 2.3\n"
        f"Name: {project['name']}\n"
        f"Version: {version}\n"
        f"Summary: {project.get('description', '')}\n"
        "Requires-Python: >=3.10\n\n"
    ).encode("utf-8")
    entries[f"{dist_info}/WHEEL"] = (
        "Wheel-Version: 1.0\n"
        "Generator: tendwire-sqlite-sidecar-evidence\n"
        "Root-Is-Purelib: true\n"
        "Tag: py3-none-any\n"
    ).encode("utf-8")
    entries[f"{dist_info}/entry_points.txt"] = (
        b"[console_scripts]\ntendwire = tendwire.cli:main\n"
    )
    record_name = f"{dist_info}/RECORD"
    record = [
        _wheel_record_line(path, content) for path, content in sorted(entries.items())
    ]
    record.append(f"{record_name},,")
    entries[record_name] = ("\n".join(record) + "\n").encode("utf-8")
    with zipfile.ZipFile(
        wheel_path,
        "w",
        compression=zipfile.ZIP_DEFLATED,
    ) as archive:
        for path, content in sorted(entries.items()):
            info = zipfile.ZipInfo(path, date_time=(2026, 1, 1, 0, 0, 0))
            info.compress_type = zipfile.ZIP_DEFLATED
            info.external_attr = (0o644 & 0xFFFF) << 16
            archive.writestr(info, content)
    digest = hashlib.sha256(wheel_path.read_bytes()).hexdigest()
    return wheel_path, version, digest, source_tree.hexdigest()


def _install_candidate(root: Path, wheel_path: Path) -> Path:
    candidate = root / "candidate"
    venv.EnvBuilder(with_pip=True, clear=True).create(candidate)
    python = candidate / "bin" / "python"
    completed = subprocess.run(
        [str(python), "-m", "pip", "install", "--no-index", "--no-deps", str(wheel_path)],
        stdin=subprocess.DEVNULL,
        capture_output=True,
        text=True,
        check=False,
        timeout=60,
    )
    if completed.returncode != 0:
        raise RuntimeError("candidate_install_failed")
    return python


def _verify_candidate(python: Path, version: str, checkout: Path) -> None:
    code = (
        "import importlib.metadata,json,pathlib,tendwire;"
        "print(json.dumps({'origin':str(pathlib.Path(tendwire.__file__).resolve()),"
        "'version':importlib.metadata.version('tendwire')}))"
    )
    completed = subprocess.run(
        [str(python), "-I", "-c", code],
        stdin=subprocess.DEVNULL,
        capture_output=True,
        text=True,
        check=False,
        timeout=15,
    )
    if completed.returncode != 0:
        raise RuntimeError("candidate_import_failed")
    payload = json.loads(completed.stdout)
    origin = Path(payload["origin"]).resolve()
    if payload.get("version") != version or checkout.resolve() in origin.parents:
        raise RuntimeError("candidate_provenance_failed")
    if python.parent.parent.resolve() not in origin.parents:
        raise RuntimeError("candidate_install_origin_failed")


def _argument_values(namespace: argparse.Namespace) -> None:
    for name in ("iterations", "daemon_wal_cycles", "requests_per_method"):
        if int(getattr(namespace, name)) <= 0:
            raise _ArgumentError("positive_count_required")
    if namespace.herdres_sync_passes != 3:
        raise _ArgumentError("three_sync_passes_required")
    if namespace.daemon_wal_cycles != namespace.requests_per_method:
        raise _ArgumentError("daemon_request_counts_must_match")
    if not 1.0 <= namespace.phase_timeout_seconds <= 600.0:
        raise _ArgumentError("timeout_out_of_range")
    if not namespace.json:
        raise _ArgumentError("json_required")
    if namespace.herdres_root is None or not namespace.herdres_root.is_dir():
        raise _ArgumentError("herdres_root_required")


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run hermetic SQLite sidecar race evidence.")
    parser.add_argument("--iterations", type=int, default=DEFAULT_ITERATIONS)
    parser.add_argument("--daemon-wal-cycles", type=int, default=DEFAULT_DAEMON_CYCLES)
    parser.add_argument("--requests-per-method", type=int, default=DEFAULT_REQUESTS)
    parser.add_argument("--herdres-sync-passes", type=int, default=3)
    parser.add_argument("--phase-timeout-seconds", type=float, default=DEFAULT_TIMEOUT_SECONDS)
    configured_herdres_root = os.environ.get("TENDWIRE_BENCHMARK_HERDRES_ROOT")
    parser.add_argument(
        "--herdres-root",
        type=Path,
        default=Path(configured_herdres_root) if configured_herdres_root else None,
    )
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--candidate-child", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--candidate-python", type=Path, help=argparse.SUPPRESS)
    parser.add_argument("--candidate-wheel", type=Path, help=argparse.SUPPRESS)
    parser.add_argument("--private-root", type=Path, help=argparse.SUPPRESS)
    parser.add_argument("--checkout", type=Path, help=argparse.SUPPRESS)
    parser.add_argument("--candidate-version", help=argparse.SUPPRESS)
    parser.add_argument("--artifact-digest", help=argparse.SUPPRESS)
    parser.add_argument("--source-revision", help=argparse.SUPPRESS)
    parser.add_argument("--source-tree-digest", help=argparse.SUPPRESS)
    parser.add_argument("--inject-failure", choices=("churn", "daemon", "herdres"), help=argparse.SUPPRESS)
    return parser


def _abort_barriers(barriers: tuple[threading.Barrier, ...]) -> None:
    for barrier in barriers:
        try:
            barrier.abort()
        except threading.BrokenBarrierError:
            pass


def _family_phase(db_path: Path, iterations: int, timeout: float, *, journal: bool) -> dict[str, Any]:
    from tendwire import local_state
    from tendwire.local_state import LocalStateKind, PermissionState

    kind = LocalStateKind.DATABASE_JOURNAL if journal else LocalStateKind.DATABASE_WAL
    target_path = Path(f"{db_path}-journal" if journal else f"{db_path}-wal")
    companion_path = None if journal else Path(f"{db_path}-shm")
    ready = threading.Barrier(2, timeout=timeout)
    captured = threading.Barrier(2, timeout=timeout)
    retired = threading.Barrier(2, timeout=timeout)
    consumed = threading.Barrier(2, timeout=timeout)
    barriers = (ready, captured, retired, consumed)
    enabled = threading.Event()
    failures: list[BaseException] = []
    phase_counts: Counter[str] = Counter()
    terminal_counts: Counter[str] = Counter()
    managed = Counter()
    optional_disappearances = Counter()
    original_hook = local_state._sqlite_family_test_phase

    def hook(phase: str, current_kind: Any) -> None:
        phase_counts[f"{phase}:{current_kind.value}"] += 1
        if phase == "captured" and current_kind is kind and enabled.is_set():
            enabled.clear()
            captured.wait()
            retired.wait()

    local_state._sqlite_family_test_phase = hook

    def writer() -> None:
        try:
            for cycle in range(iterations):
                connection = sqlite3.connect(str(db_path), timeout=5)
                managed["opens"] += 1
                try:
                    if journal:
                        connection.execute("PRAGMA journal_mode=DELETE")
                        connection.execute("BEGIN IMMEDIATE")
                        connection.execute(
                            "UPDATE sidecar_evidence SET value = ? WHERE slot = 1",
                            (-(cycle + 1),),
                        )
                        managed["transactions"] += 1
                    else:
                        connection.execute("PRAGMA journal_mode=WAL")
                        connection.execute(
                            "UPDATE sidecar_evidence SET value = ? WHERE slot = 1",
                            (cycle + 1,),
                        )
                        connection.commit()
                        managed["transactions"] += 1
                    if not os.path.lexists(target_path):
                        raise RuntimeError("sidecar_not_created")
                    if companion_path is not None and not os.path.lexists(companion_path):
                        raise RuntimeError("shm_not_created")
                    ready.wait()
                    captured.wait()
                    if journal:
                        connection.rollback()
                        managed["rollbacks"] += 1
                    else:
                        row = connection.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
                        if row is None or int(row[0]) != 0:
                            raise RuntimeError("checkpoint_failed")
                        managed["checkpoints"] += 1
                finally:
                    connection.close()
                    managed["closes"] += 1
                target_path.unlink(missing_ok=True)
                if companion_path is not None:
                    companion_path.unlink(missing_ok=True)
                if os.path.lexists(target_path) or (
                    companion_path is not None and os.path.lexists(companion_path)
                ):
                    raise RuntimeError("sidecar_retirement_failed")
                optional_disappearances["journal" if journal else "wal"] += 1
                if companion_path is not None:
                    optional_disappearances["shm"] += 1
                retired.wait()
                consumed.wait()
        except BaseException as exc:
            failures.append(exc)
            _abort_barriers(barriers)

    worker = threading.Thread(
        target=writer,
        name="tendwire-sidecar-journal-writer" if journal else "tendwire-sidecar-wal-writer",
    )
    worker.start()
    resource_peak = _resource_counts()
    results_seen = 0
    try:
        for _ in range(iterations):
            ready.wait()
            _merge_resource_peak(resource_peak, _resource_counts())
            enabled.set()
            results = local_state.prepare_sqlite_family(db_path)
            results_seen += 1
            for result in results:
                terminal_counts[result.state.value] += 1
            selected = next(result for result in results if result.kind is kind)
            if selected.state is not PermissionState.ABSENT:
                raise RuntimeError("retired_sidecar_not_absent")
            consumed.wait()
    except BaseException:
        _abort_barriers(barriers)
        raise
    finally:
        local_state._sqlite_family_test_phase = original_hook
        worker.join(timeout=timeout)
    if worker.is_alive() or failures:
        raise RuntimeError("family_worker_failed")
    expected_disappearances = {"journal": iterations} if journal else {"wal": iterations, "shm": iterations}
    if dict(optional_disappearances) != expected_disappearances:
        raise RuntimeError("disappearance_count_mismatch")
    return {
        "mode": "rollback_journal" if journal else "wal",
        "iterations": iterations,
        "preparations": results_seen,
        "write_transactions": managed["transactions"],
        "checkpoints": managed["checkpoints"],
        "rollbacks": managed["rollbacks"],
        "managed_connection_opens": managed["opens"],
        "managed_connection_closes": managed["closes"],
        "member_phase_observations": sum(phase_counts.values()),
        "target_captures": phase_counts[f"captured:{kind.value}"],
        "optional_disappearances": dict(optional_disappearances),
        "terminal_outcomes": {
            "present": terminal_counts["private"] + terminal_counts["repaired"] + terminal_counts["created"],
            "absent": terminal_counts["absent"],
            "invalid": 0,
        },
        "typed_codes": {
            "missing_entry": 0,
            "entry_changed": 0,
            "wrong_type": 0,
            "wrong_owner": 0,
            "operation_failed": 0,
        },
        "maximum_attempts_per_member": 3,
        "resource_peak_counts": resource_peak,
    }


def _run_churn_phase(root: Path, iterations: int, timeout: float) -> tuple[dict[str, Any], Path]:
    db_path = root / "isolated.db"
    connection = sqlite3.connect(str(db_path))
    try:
        connection.execute("CREATE TABLE sidecar_evidence (slot INTEGER PRIMARY KEY, value INTEGER NOT NULL)")
        connection.execute("INSERT INTO sidecar_evidence (slot, value) VALUES (1, 0)")
        connection.commit()
    finally:
        connection.close()
    wal = _family_phase(db_path, iterations, timeout, journal=False)
    journal = _family_phase(db_path, iterations, timeout, journal=True)
    return {
        "family_iterations": iterations * 2,
        "wal": wal,
        "rollback_journal": journal,
        "bounded_family_preparations": wal["preparations"] + journal["preparations"],
        "optional_disappearances": sum(wal["optional_disappearances"].values())
        + sum(journal["optional_disappearances"].values()),
        "unexpected_exceptions": 0,
        "maximum_attempts_per_member": max(
            wal["maximum_attempts_per_member"], journal["maximum_attempts_per_member"]
        ),
    }, db_path


class _NoopScheduler:
    def __init__(self) -> None:
        self.started = False
        self.stopped = False
        self.refreshes = 0

    def start(self) -> None:
        self.started = True

    def request_refresh(self) -> None:
        self.refreshes += 1

    def stop(self, *, flush_timeout_seconds: float | None = None) -> None:
        del flush_timeout_seconds
        self.stopped = True

    def operational_status(self) -> dict[str, Any]:
        return {
            "status": "healthy",
            "queue_depth": 0,
            "active": 0,
            "queue_capacity": 1,
        }


def _write_herdr_trap(path: Path, marker: Path) -> None:
    path.write_text(
        "#!/usr/bin/env python3\n"
        "from pathlib import Path\n"
        f"p=Path({str(marker)!r})\n"
        "p.write_text(str(int(p.read_text() or '0')+1))\n"
        "raise SystemExit(97)\n",
        encoding="utf-8",
    )
    path.chmod(0o700)


def _seed_daemon_store(db_path: Path) -> None:
    from tendwire.core.models import BackendHealth, Snapshot, Worker, WorkerBinding
    from tendwire.store import sqlite as store

    store.init_store(db_path)
    worker = Worker(id=FIXTURE_WORKER, name="Generated Evidence Worker", status="active")
    snapshot = Snapshot(
        host_id=FIXTURE_HOST,
        updated_at=FIXTURE_TIMESTAMP,
        workers=[worker],
        backend_health=[
            BackendHealth(
                name="herdr",
                status="healthy",
                outcome="healthy_non_empty",
                observed_at=FIXTURE_TIMESTAMP,
            )
        ],
    )
    binding = WorkerBinding(
        host_id=FIXTURE_HOST,
        worker_id=worker.id,
        worker_fingerprint=worker.fingerprint,
        backend="herdr",
        target_kind="agent_id",
        target_value=FIXTURE_AGENT,
        turn_target_kind="pane_id",
        turn_target_value=FIXTURE_PANE,
        sendable=True,
        reason=None,
        observed_at=FIXTURE_TIMESTAMP,
        private_fingerprint="generated-sidecar-private-binding",
    )
    store.save_snapshot(db_path, snapshot)
    if store.upsert_worker_bindings(db_path, [binding]) != 1:
        raise RuntimeError("binding_seed_failed")
    applied = store.apply_turn_refresh(
        db_path,
        FIXTURE_HOST,
        FIXTURE_WORKER,
        {
            "source_turn_id": "generated-sidecar-source-turn",
            "assistant_final_text": FIXTURE_FINAL,
            "complete": True,
            "has_open_turn": False,
        },
        expected_binding=binding,
        observed_at=FIXTURE_TIMESTAMP,
    )
    if applied.updated != 1:
        raise RuntimeError("turn_seed_failed")


def _run_daemon_phase(
    root: Path,
    db_path: Path,
    cycles: int,
    requests: int,
    timeout: float,
    herdr_trap: Path,
) -> tuple[dict[str, Any], Any, Path]:
    from tendwire import local_state
    from tendwire.config import Config
    from tendwire.daemon import DaemonHooks, TendwireDaemon
    from tendwire.daemon_api import DaemonAPIClient, TendwireDaemonAPI
    from tendwire.local_state import LocalStateKind
    from tendwire.store import sqlite as store

    _seed_daemon_store(db_path)
    socket_path = root / "isolated.sock"
    config = Config(
        host_id=FIXTURE_HOST,
        herdr_bin=str(herdr_trap),
        data_dir=root,
        db_path=db_path,
        socket_path=socket_path,
        herdr_backend="cli",
        reconcile_interval_seconds=0.0,
        turn_refresh_interval_seconds=3600.0,
        turn_refresh_workers=1,
        herdr_timeout_seconds=5.0,
    )
    scheduler = _NoopScheduler()
    daemon = TendwireDaemon(
        config,
        hooks=DaemonHooks(
            observe_initial_snapshot=lambda _config: store.latest_snapshot(db_path, FIXTURE_HOST),
            turn_scheduler_factory=lambda _config: scheduler,
        ),
    )
    ready = threading.Barrier(2, timeout=timeout)
    captured = threading.Barrier(2, timeout=timeout)
    retired = threading.Barrier(2, timeout=timeout)
    consumed = threading.Barrier(2, timeout=timeout)
    barriers = (ready, captured, retired, consumed)
    enabled = threading.Event()
    failures: list[BaseException] = []
    capture_count = 0
    managed = Counter()
    original_hook = local_state._sqlite_family_test_phase

    def hook(phase: str, kind: Any) -> None:
        nonlocal capture_count
        if phase == "captured" and kind is LocalStateKind.DATABASE_WAL and enabled.is_set():
            enabled.clear()
            capture_count += 1
            captured.wait()
            retired.wait()

    def writer() -> None:
        try:
            for cycle in range(cycles):
                connection = sqlite3.connect(str(db_path), timeout=5)
                managed["opens"] += 1
                try:
                    connection.execute("PRAGMA journal_mode=WAL")
                    connection.execute(
                        "UPDATE sidecar_evidence SET value = ? WHERE slot = 1",
                        (10_000 + cycle,),
                    )
                    connection.commit()
                    managed["transactions"] += 1
                    ready.wait()
                    captured.wait()
                    row = connection.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
                    if row is None or int(row[0]) != 0:
                        raise RuntimeError("daemon_checkpoint_failed")
                    managed["checkpoints"] += 1
                finally:
                    connection.close()
                    managed["closes"] += 1
                Path(f"{db_path}-wal").unlink(missing_ok=True)
                Path(f"{db_path}-shm").unlink(missing_ok=True)
                retired.wait()
                consumed.wait()
        except BaseException as exc:
            failures.append(exc)
            _abort_barriers(barriers)

    latencies = {"snapshot_get": [], "turn_list": [], "health_get": []}
    sizes = {"snapshot_get": [], "turn_list": [], "health_get": []}
    successes = Counter()
    api_failures = Counter()
    production_callbacks = False
    server_thread: threading.Thread | None = None
    writer_thread = threading.Thread(target=writer, name="tendwire-sidecar-daemon-writer")
    local_state._sqlite_family_test_phase = hook
    resource_peak = _resource_counts()
    try:
        daemon.start()
        if daemon.server is None:
            raise RuntimeError("daemon_server_missing")
        api = getattr(daemon.server.dispatcher, "__self__", None)
        production_callbacks = isinstance(api, TendwireDaemonAPI)
        for callback_name, method_name in (
            ("_get_snapshot", "get_snapshot"),
            ("_get_turns", "get_turns"),
            ("_get_health", "get_health"),
        ):
            callback = getattr(api, callback_name, None)
            production_callbacks = production_callbacks and getattr(callback, "__self__", None) is daemon
            production_callbacks = production_callbacks and getattr(callback, "__func__", None) is getattr(
                TendwireDaemon, method_name
            )
        if not production_callbacks:
            raise RuntimeError("production_callbacks_unbound")
        server_thread = threading.Thread(target=daemon.serve_forever, name="tendwire-sidecar-daemon")
        server_thread.start()
        writer_thread.start()
        client = DaemonAPIClient(socket_path, timeout_seconds=min(10.0, timeout))
        for _cycle in range(requests):
            ready.wait()
            _merge_resource_peak(resource_peak, _resource_counts())
            enabled.set()
            operations = (
                (
                    "turn_list",
                    "turn.list",
                    {"schema_version": 2, "limit": 100, "cursor": None, "since": None},
                ),
                ("snapshot_get", "snapshot.get", None),
                ("health_get", "health.get", None),
            )
            for label, method, params in operations:
                started = perf_counter_ns()
                response = client.request(method, params)
                latencies[label].append(perf_counter_ns() - started)
                sizes[label].append(len(_canonical_json(response).encode("utf-8")))
                if response.get("ok") is not True:
                    api_failures[label] += 1
                    continue
                result = response.get("result")
                valid = isinstance(result, Mapping)
                if label == "turn_list":
                    valid = valid and result.get("schema_version") == 2 and any(
                        turn.get("assistant_final_text") == FIXTURE_FINAL
                        for turn in result.get("turns", [])
                        if isinstance(turn, Mapping)
                    )
                elif label == "snapshot_get":
                    valid = valid and result.get("host_id") == FIXTURE_HOST
                else:
                    valid = (
                        valid
                        and result.get("status") == "ok"
                        and isinstance(result.get("store"), Mapping)
                        and result["store"].get("status") == "healthy"
                    )
                if valid:
                    successes[label] += 1
                else:
                    api_failures[label] += 1
            consumed.wait()
    except BaseException:
        _abort_barriers(barriers)
        raise
    finally:
        enabled.clear()
        local_state._sqlite_family_test_phase = original_hook
        daemon.stop()
        writer_thread.join(timeout=timeout)
        if server_thread is not None:
            server_thread.join(timeout=timeout)
    if failures or writer_thread.is_alive() or server_thread is None or server_thread.is_alive():
        raise RuntimeError("daemon_worker_cleanup_failed")
    integrity = sqlite3.connect(str(db_path))
    try:
        integrity_ok = integrity.execute("PRAGMA integrity_check").fetchone() == ("ok",)
        revision_rows = int(integrity.execute("SELECT COUNT(*) FROM turn_content_revisions").fetchone()[0])
        duplicate_revisions = int(
            integrity.execute(
                "SELECT COUNT(*) FROM (SELECT host_id,turn_id,content_revision FROM turn_content_revisions "
                "GROUP BY host_id,turn_id,content_revision HAVING COUNT(*) > 1)"
            ).fetchone()[0]
        )
    finally:
        integrity.close()
    return (
        {
            "wal_cycles": cycles,
            "write_transactions": managed["transactions"],
            "checkpoints": managed["checkpoints"],
            "sidecar_captures": capture_count,
            "managed_connection_opens": managed["opens"],
            "managed_connection_closes": managed["closes"],
            "requests_per_method": requests,
            "api_successes": dict(successes),
            "api_failures": dict(api_failures),
            "latency_ns": {
                label: _latency_metric(latencies[label], sizes[label]) for label in sorted(latencies)
            },
            "production_callbacks": production_callbacks,
            "integrity_ok": integrity_ok,
            "revision_rows": revision_rows,
            "duplicate_revision_groups": duplicate_revisions,
            "socket_removed_after_shutdown": not os.path.lexists(socket_path),
            "scheduler_refreshes": scheduler.refreshes,
            "scheduler_stopped": scheduler.stopped,
            "resource_peak_counts": resource_peak,
        },
        daemon,
        socket_path,
    )


def _write_candidate_recorder(path: Path, log_path: Path, candidate_python: Path) -> None:
    path.write_text(
        "#!" + str(candidate_python) + "\n"
        "import json,os,sys\n"
        f"log={str(log_path)!r}\n"
        "fd=os.open(log,os.O_WRONLY|os.O_CREAT|os.O_APPEND,0o600)\n"
        "try: os.write(fd,(json.dumps(sys.argv[1:],separators=(',',':'))+'\\n').encode())\n"
        "finally: os.close(fd)\n"
        f"python={str(candidate_python)!r}\n"
        "os.execv(python,[python,'-I','-m','tendwire.cli',*sys.argv[1:]])\n",
        encoding="utf-8",
    )
    path.chmod(0o700)


def _noop_result(result: Mapping[str, Any]) -> bool:
    zero_fields = (
        "created",
        "updated",
        "icon_updated",
        "pinned_status_updated",
        "feed_sent",
        "sent",
        "routing_repaired",
        "turn_updates",
        "message_bindings",
        "content_pages",
    )
    if result.get("ok") is not True or result.get("changed") is not False:
        return False
    if any(int(result.get(field, -1)) != 0 for field in zero_fields):
        return False
    cleanup = result.get("topic_cleanup")
    if not isinstance(cleanup, Mapping) or cleanup.get("changed") is not False:
        return False
    for section_name in ("tendwire_turn_final", "tendwire_outbox"):
        section = result.get(section_name)
        if not isinstance(section, Mapping) or section.get("changed") is not False:
            return False
        for field in ("polled", "operations", "delivered", "acked", "failed", "deferred", "uncertain"):
            if field in section and int(section[field]) != 0:
                return False
    return True


def _run_herdres_phase(
    root: Path,
    db_path: Path,
    socket_path: Path,
    daemon: Any,
    candidate_python: Path,
    herdres_root: Path,
) -> dict[str, Any]:
    from tendwire.core.models import BackendHealth, Snapshot
    from tendwire.store import sqlite as store_module

    empty_snapshot = Snapshot(
        host_id=FIXTURE_HOST,
        updated_at="2026-07-12T00:10:00+00:00",
        workers=[],
        backend_health=[
            BackendHealth(
                name="herdr",
                status="healthy",
                outcome="healthy_empty",
                observed_at="2026-07-12T00:10:00+00:00",
            )
        ],
    )
    store_module.save_snapshot(db_path, empty_snapshot)
    # Clear only the benchmark daemon's documented cache so the production callback
    # re-reads the newly durable settling snapshot; no public schema is bypassed.
    daemon._snapshot_cache = None

    call_log = root / "candidate-cli-calls.jsonl"
    recorder = root / "candidate-cli-recorder"
    _write_candidate_recorder(recorder, call_log, candidate_python)
    state_path = root / "herdres-state.json"
    absent_source = root / "absent-source"
    environment = os.environ.copy()
    private_environment = {
        "HERDRES_TENDWIRE_MODE": "source",
        "HERDRES_TENDWIRE_BIN": f"{recorder} --socket-path {socket_path}",
        "HERDR_TELEGRAM_TOPICS_STATE": str(state_path),
        "TENDWIRE_DB_PATH": str(db_path),
        "TENDWIRE_DATA_DIR": str(root),
        "TENDWIRE_SOCKET_PATH": str(socket_path),
        "TENDWIRE_HERDR_BACKEND": "socket",
        "TENDWIRE_SOURCE_DIR": str(absent_source),
        "HERDRES_PINNED_STATUS": "0",
        "TELEGRAM_BOT_TOKEN": "",
        "TELEGRAM_CHAT_ID": "0",
    }
    before_modules = set(sys.modules)
    previous_path = list(sys.path)
    origin_ok = False
    try:
        os.environ.clear()
        os.environ.update(environment)
        os.environ.update(private_environment)
        sys.path.insert(0, str(herdres_root))
        source_sync = importlib.import_module("herdres_connector.source_sync")
        tendwire_client = importlib.import_module("herdres_connector.tendwire_client")
        telegram_delivery = importlib.import_module("herdres_connector.telegram_delivery")
        for module in (source_sync, tendwire_client, telegram_delivery):
            module_path = Path(module.__file__).resolve()
            if herdres_root.resolve() not in module_path.parents:
                raise RuntimeError("herdres_origin_failed")
        origin_ok = True
        # The paired Herdres owns its turn page size (it changed 50 -> 100 in
        # luminexord/herdres 31c3152); derive the expected command sequence from
        # the paired checkout instead of hardcoding a value that breaks the
        # pairing every time Herdres retunes it.
        turn_page_limit = getattr(tendwire_client, "TURN_LIST_PAGE_LIMIT", 50)
        if type(turn_page_limit) is not int or turn_page_limit < 1:
            raise RuntimeError("herdres_turn_page_limit_invalid")
        runtime = source_sync.SyncRuntime(
            tendwire=tendwire_client.TendwireClient(timeout=10.0),
            telegram=telegram_delivery.TelegramClient(token="", dry_run=True),
            dry_run=True,
            with_outbox=False,
            max_sends=0,
        )
        private_store: dict[str, Any] = {
            "version": 2,
            "enabled": True,
            "telegram": {},
            "panes": {},
            "spaces": {},
            "tendwired_bootstrap_complete": True,
        }
        with (
            _OutboundNetworkGuard() as network_guard,
            _SubprocessResourceObserver() as subprocess_observer,
        ):
            results = [source_sync.sync_once(private_store, runtime)]
            settled = copy.deepcopy(private_store)
            settled_digest = hashlib.sha256(
                _canonical_json(settled).encode("utf-8")
            ).hexdigest()
            results.append(source_sync.sync_once(private_store, runtime))
            second_unchanged = private_store == settled
            results.append(source_sync.sync_once(private_store, runtime))
            third_unchanged = private_store == settled
            final_digest = hashlib.sha256(
                _canonical_json(private_store).encode("utf-8")
            ).hexdigest()
    finally:
        sys.path[:] = previous_path
        for name in set(sys.modules) - before_modules:
            if name == "herdres_connector" or name.startswith("herdres_connector."):
                sys.modules.pop(name, None)
        os.environ.clear()
        os.environ.update(environment)
    records = [json.loads(line) for line in call_log.read_text(encoding="utf-8").splitlines()]
    def _normalized_client_call(argv: list[str]) -> list[str]:
        # Watermark tokens are run-specific; validate their shape, then
        # normalize so the sequence stays exactly comparable.
        normalized = list(argv)
        for index, value in enumerate(normalized):
            if (
                index > 0
                and normalized[index - 1] == "--watermark"
                and value.startswith("twdelta1.")
            ):
                normalized[index] = "<WATERMARK>"
        return normalized

    expected_commands = []
    for pass_index in range(3):
        delta_call = [
            "--socket-path",
            str(socket_path),
            "turn",
            "delta",
            "--json",
            "--limit",
            "500",
        ]
        if pass_index > 0:
            delta_call.extend(("--watermark", "<WATERMARK>"))
        expected_commands.extend(
            (["--socket-path", str(socket_path), "snapshot", "--json"],
             delta_call,
             ["--socket-path", str(socket_path), "pending", "--json"])
        )
    commands_exact = [
        _normalized_client_call(record) for record in records
    ] == expected_commands
    noop_results = results[1:]
    return {
        "mode": "source",
        "dry_run": True,
        "sync_passes": len(results),
        "settling_passes": 1,
        "noop_passes": 2,
        "noop_passes_valid": sum(_noop_result(result) for result in noop_results),
        "state_unchanged_noop_passes": int(second_unchanged) + int(third_unchanged),
        "state_digest_unchanged": settled_digest == final_digest,
        "production_sync_import": origin_ok,
        "production_client_subprocesses": len(records),
        "subprocesses_per_pass": 3,
        "command_sequence_exact": commands_exact,
        "direct_herdr_calls": 0,
        "external_network_attempts": network_guard.attempts,
        "resource_peak_counts": subprocess_observer.peak,
        "settling_changed": bool(results[0].get("changed")),
        "settling_ok": results[0].get("ok") is True,
        "noop_work_counts": {
            field: sum(int(result.get(field, 0)) for result in noop_results)
            for field in (
                "created",
                "updated",
                "icon_updated",
                "pinned_status_updated",
                "feed_sent",
                "sent",
                "routing_repaired",
                "turn_updates",
                "message_bindings",
                "content_pages",
            )
        },
    }


def _candidate_run(args: argparse.Namespace) -> dict[str, Any]:
    if not all(
        (
            args.candidate_python,
            args.candidate_wheel,
            args.private_root,
            args.checkout,
            args.candidate_version,
            args.artifact_digest,
            args.source_revision,
            args.source_tree_digest,
        )
    ):
        raise RuntimeError("candidate_arguments_missing")
    import tendwire

    origin = Path(tendwire.__file__).resolve()
    if args.checkout.resolve() in origin.parents or Path(sys.prefix).resolve() not in origin.parents:
        raise RuntimeError("mutable_source_imported")
    if importlib.metadata.version("tendwire") != args.candidate_version:
        raise RuntimeError("candidate_version_mismatch")
    root = args.private_root / "run"
    root.mkdir(mode=0o700)
    baseline_fds = _fd_snapshot()
    baseline_threads = _thread_snapshot()
    baseline_children = _direct_children()
    before_usage = _usage()
    wall_started = perf_counter_ns()
    cpu_started = process_time_ns()
    peak_fd_count = len(baseline_fds)
    peak_thread_count = len(baseline_threads)
    peak_child_count = len(baseline_children)
    herdr_marker = root / "herdr-marker"
    herdr_marker.write_text("0", encoding="ascii")
    herdr_marker.chmod(0o600)
    herdr_trap = root / "herdr-trap"
    _write_herdr_trap(herdr_trap, herdr_marker)
    forbidden_values = [
        str(args.private_root),
        str(root),
        str(args.candidate_python),
        str(args.candidate_wheel),
        str(args.checkout),
        str(args.herdres_root),
        FIXTURE_HOST,
        FIXTURE_WORKER,
        FIXTURE_AGENT,
        FIXTURE_PANE,
        FIXTURE_FINAL,
        "generated-sidecar-private-binding",
        "generated-sidecar-source-turn",
    ]
    churn_started = perf_counter_ns()
    churn, db_path = _run_churn_phase(root, args.iterations, args.phase_timeout_seconds)
    for family in ("wal", "rollback_journal"):
        family_peak = churn[family]["resource_peak_counts"]
        peak_fd_count = max(peak_fd_count, family_peak["fds"])
        peak_thread_count = max(peak_thread_count, family_peak["threads"])
        peak_child_count = max(peak_child_count, family_peak["direct_children"])
    churn_ns = perf_counter_ns() - churn_started
    if args.inject_failure == "churn":
        raise RuntimeError("injected_churn_failure")
    peak_fd_count = max(peak_fd_count, len(_fd_snapshot()))
    peak_thread_count = max(peak_thread_count, len(_thread_snapshot()))
    daemon_started = perf_counter_ns()
    daemon_metrics, daemon, socket_path = _run_daemon_phase(
        root,
        db_path,
        args.daemon_wal_cycles,
        args.requests_per_method,
        args.phase_timeout_seconds,
        herdr_trap,
    )
    daemon_ns = perf_counter_ns() - daemon_started
    daemon_peak = daemon_metrics["resource_peak_counts"]
    peak_fd_count = max(peak_fd_count, daemon_peak["fds"])
    peak_thread_count = max(peak_thread_count, daemon_peak["threads"])
    peak_child_count = max(peak_child_count, daemon_peak["direct_children"])
    if args.inject_failure == "daemon":
        raise RuntimeError("injected_daemon_failure")
    peak_fd_count = max(peak_fd_count, len(_fd_snapshot()))
    peak_thread_count = max(peak_thread_count, len(_thread_snapshot()))
    peak_child_count = max(peak_child_count, len(_direct_children()))
    # Herdres uses a fresh lifecycle after the daemon phase proved full cleanup.
    from tendwire.config import Config
    from tendwire.daemon import DaemonHooks, TendwireDaemon
    from tendwire.store import sqlite as store_module

    herdres_scheduler = _NoopScheduler()
    herdres_config = Config(
        host_id=FIXTURE_HOST,
        herdr_bin=str(herdr_trap),
        data_dir=root,
        db_path=db_path,
        socket_path=socket_path,
        herdr_backend="cli",
        reconcile_interval_seconds=0.0,
        turn_refresh_interval_seconds=3600.0,
        turn_refresh_workers=1,
        herdr_timeout_seconds=5.0,
    )
    daemon = TendwireDaemon(
        herdres_config,
        hooks=DaemonHooks(
            observe_initial_snapshot=lambda _config: store_module.latest_snapshot(
                db_path, FIXTURE_HOST
            ),
            turn_scheduler_factory=lambda _config: herdres_scheduler,
        ),
    )
    daemon.start()
    herdres_server = threading.Thread(
        target=daemon.serve_forever,
        name="tendwire-sidecar-herdres-daemon",
    )
    herdres_server.start()
    try:
        herdres_started = perf_counter_ns()
        herdres_metrics = _run_herdres_phase(
            root,
            db_path,
            socket_path,
            daemon,
            args.candidate_python,
            args.herdres_root,
        )
        herdres_ns = perf_counter_ns() - herdres_started
        if args.inject_failure == "herdres":
            raise RuntimeError("injected_herdres_failure")
    finally:
        daemon.stop()
        herdres_server.join(timeout=args.phase_timeout_seconds)
    if herdres_server.is_alive():
        raise RuntimeError("herdres_daemon_not_reaped")
    herdres_peak = herdres_metrics["resource_peak_counts"]
    peak_fd_count = max(peak_fd_count, herdres_peak["fds"])
    peak_thread_count = max(peak_thread_count, herdres_peak["threads"])
    peak_child_count = max(peak_child_count, herdres_peak["direct_children"])
    peak_fd_count = max(peak_fd_count, len(_fd_snapshot()))
    peak_thread_count = max(peak_thread_count, len(_thread_snapshot()))
    peak_child_count = max(peak_child_count, len(_direct_children()))
    after_fds = _fd_snapshot()
    after_threads = _thread_snapshot()
    after_children = _direct_children()
    wall_ns = perf_counter_ns() - wall_started
    cpu_ns = process_time_ns() - cpu_started
    usage_delta = _usage_delta(before_usage, _usage())
    trap_calls = int(herdr_marker.read_text(encoding="ascii") or "0")
    herdres_metrics["direct_herdr_calls"] = trap_calls
    checks = {
        "installed_candidate_imported": True,
        "mutable_source_not_imported": True,
        "private_temporary_directory": stat.S_IMODE(root.stat().st_mode) == 0o700,
        "fixed_family_counts_completed": churn["family_iterations"] == args.iterations * 2,
        "bounded_family_attempts": churn["maximum_attempts_per_member"] <= 3,
        "optional_disappearances_observed": churn["optional_disappearances"] == args.iterations * 3,
        "no_unexpected_churn_exceptions": churn["unexpected_exceptions"] == 0,
        "daemon_cycle_count_exact": daemon_metrics["wal_cycles"] == args.daemon_wal_cycles,
        "api_request_counts_exact": all(
            daemon_metrics["api_successes"].get(label, 0) == args.requests_per_method
            for label in ("snapshot_get", "turn_list", "health_get")
        ),
        "api_failures_zero": sum(daemon_metrics["api_failures"].values()) == 0,
        "production_callbacks_bound": daemon_metrics["production_callbacks"],
        "sqlite_integrity_ok": daemon_metrics["integrity_ok"],
        "duplicate_revisions_zero": daemon_metrics["duplicate_revision_groups"] == 0,
        "two_noop_syncs_exact": herdres_metrics["noop_passes"] == 2,
        "two_noop_syncs_valid": herdres_metrics["noop_passes_valid"] == 2,
        "noop_state_unchanged": herdres_metrics["state_unchanged_noop_passes"] == 2
        and herdres_metrics["state_digest_unchanged"],
        "production_herdres_sync_imported": herdres_metrics["production_sync_import"],
        "production_client_calls_exact": herdres_metrics["production_client_subprocesses"] == 9
        and herdres_metrics["command_sequence_exact"],
        "direct_herdr_calls_zero": trap_calls == 0,
        "external_network_calls_zero": herdres_metrics["external_network_attempts"] == 0,
        "managed_connections_balanced": (
            churn["wal"]["managed_connection_opens"]
            == churn["wal"]["managed_connection_closes"]
            and churn["rollback_journal"]["managed_connection_opens"]
            == churn["rollback_journal"]["managed_connection_closes"]
            and daemon_metrics["managed_connection_opens"]
            == daemon_metrics["managed_connection_closes"]
        ),
        "live_fd_peak_observed": peak_fd_count > len(baseline_fds),
        "live_thread_peak_observed": peak_thread_count > len(baseline_threads),
        "live_direct_child_peak_observed": peak_child_count > len(baseline_children),
        "fd_identity_set_restored": after_fds == baseline_fds,
        "thread_identity_set_restored": after_threads == baseline_threads,
        "direct_child_set_restored": after_children == baseline_children,
        "socket_removed": not os.path.lexists(socket_path),
    }
    report: dict[str, Any] = {
        "schema_version": REPORT_SCHEMA_VERSION,
        "ok": False,
        "status": "validating",
        "parameters": {
            "iterations_per_family": args.iterations,
            "daemon_wal_cycles": args.daemon_wal_cycles,
            "requests_per_method": args.requests_per_method,
            "herdres_sync_passes": args.herdres_sync_passes,
            "phase_timeout_seconds": args.phase_timeout_seconds,
            "maximum_attempts_per_member": 3,
        },
        "candidate": {
            "version": args.candidate_version,
            "source_revision": args.source_revision,
            "wheel_sha256": args.artifact_digest,
            "source_tree_sha256": args.source_tree_digest,
            "source_revision_binding": "base_revision_plus_source_tree_sha256",
            "installation": "private_versioned_wheel",
            "origin_verified": True,
        },
        "environment": {
            "python_version": platform.python_version(),
            "sqlite_version": sqlite3.sqlite_version,
            "platform": platform.system().lower(),
            "architecture": platform.machine(),
        },
        "churn": churn,
        "daemon": daemon_metrics,
        "herdres": herdres_metrics,
        "timing_ns": {
            "wall": wall_ns,
            "process_cpu": cpu_ns,
            "churn_wall": churn_ns,
            "daemon_wall": daemon_ns,
            "herdres_wall": herdres_ns,
            **usage_delta,
        },
        "accounting": {
            "fd_count_before": len(baseline_fds),
            "fd_count_peak_observed": peak_fd_count,
            "fd_count_after": len(after_fds),
            "thread_count_before": len(baseline_threads),
            "thread_count_peak_observed": peak_thread_count,
            "thread_count_after": len(after_threads),
            "direct_children_before": len(baseline_children),
            "direct_children_peak_observed": peak_child_count,
            "direct_children_after": len(after_children),
            "candidate_cli_subprocesses": herdres_metrics["production_client_subprocesses"],
            "socket_present_after": os.path.lexists(socket_path),
        },
        "checks": checks,
    }
    if not _privacy_scan(report, forbidden_values):
        raise RuntimeError("privacy_scan_failed")
    report["checks"]["privacy_scan_passed"] = True
    if not all(value is True for value in report["checks"].values()):
        raise RuntimeError("benchmark_invariants_failed")
    report["ok"] = True
    report["status"] = "completed"
    return report


def _public_run(args: argparse.Namespace) -> dict[str, Any]:
    checkout = Path(__file__).resolve().parent.parent
    baseline_fds = _fd_snapshot()
    baseline_threads = _thread_snapshot()
    baseline_children = _direct_children()
    temporary_path: Path | None = None
    child_report: dict[str, Any] | None = None
    with tempfile.TemporaryDirectory(prefix="tendwire-sidecar-evidence-", dir="/dev/shm") as raw:
        root = Path(raw)
        temporary_path = root
        root.chmod(0o700)
        wheel_dir = root / "wheel"
        wheel_dir.mkdir(mode=0o700)
        wheel, version, digest, source_tree_digest = _build_versioned_wheel(
            checkout, wheel_dir
        )
        revision = _source_revision(checkout)
        candidate_python = _install_candidate(root, wheel)
        _verify_candidate(candidate_python, version, checkout)
        command = [
            str(candidate_python),
            "-I",
            str(Path(__file__).resolve()),
            "--candidate-child",
            "--candidate-python",
            str(candidate_python),
            "--candidate-wheel",
            str(wheel),
            "--private-root",
            str(root),
            "--checkout",
            str(checkout),
            "--candidate-version",
            version,
            "--artifact-digest",
            digest,
            "--source-revision",
            revision,
            "--source-tree-digest",
            source_tree_digest,
            "--herdres-root",
            str(args.herdres_root.resolve()),
            "--iterations",
            str(args.iterations),
            "--daemon-wal-cycles",
            str(args.daemon_wal_cycles),
            "--requests-per-method",
            str(args.requests_per_method),
            "--herdres-sync-passes",
            str(args.herdres_sync_passes),
            "--phase-timeout-seconds",
            str(args.phase_timeout_seconds),
            "--json",
        ]
        if args.inject_failure:
            command.extend(("--inject-failure", args.inject_failure))
        environment = {
            "HOME": str(root / "home"),
            "XDG_CONFIG_HOME": str(root / "xdg-config"),
            "XDG_CACHE_HOME": str(root / "xdg-cache"),
            "XDG_STATE_HOME": str(root / "xdg-state"),
            "TMPDIR": str(root),
            "PATH": "/usr/bin:/bin",
            "LANG": "C.UTF-8",
            "LC_ALL": "C.UTF-8",
            "TENDWIRE_BENCHMARK_TESTING": os.environ.get("TENDWIRE_BENCHMARK_TESTING", ""),
        }
        for directory in (root / "home", root / "xdg-config", root / "xdg-cache", root / "xdg-state"):
            directory.mkdir(mode=0o700)
        completed = subprocess.run(
            command,
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            env=environment,
            check=False,
            timeout=args.phase_timeout_seconds * 4,
        )
        if completed.returncode != 0:
            raise RuntimeError("candidate_child_failed")
        lines = completed.stdout.splitlines()
        if len(lines) != 1 or completed.stderr:
            raise RuntimeError("candidate_output_invalid")
        child_report = json.loads(lines[0])
        if child_report.get("ok") is not True or child_report.get("status") != "completed":
            raise RuntimeError("candidate_report_failed")
    if child_report is None or temporary_path is None:
        raise RuntimeError("candidate_report_missing")
    child_report["checks"]["temporary_artifacts_removed"] = not temporary_path.exists()
    child_report["checks"]["parent_fd_identity_set_restored"] = _fd_snapshot() == baseline_fds
    child_report["checks"]["parent_thread_identity_set_restored"] = _thread_snapshot() == baseline_threads
    child_report["checks"]["parent_direct_child_set_restored"] = _direct_children() == baseline_children
    child_report["accounting"]["parent_fd_count_before"] = len(baseline_fds)
    child_report["accounting"]["parent_fd_count_after"] = len(_fd_snapshot())
    child_report["accounting"]["parent_thread_count_before"] = len(baseline_threads)
    child_report["accounting"]["parent_thread_count_after"] = len(_thread_snapshot())
    child_report["accounting"]["parent_direct_children_before"] = len(baseline_children)
    child_report["accounting"]["parent_direct_children_after"] = len(_direct_children())
    if not all(value is True for value in child_report["checks"].values()):
        raise RuntimeError("parent_cleanup_failed")
    return child_report


def _failure_envelope(status: str, *, error_type: str | None = None) -> dict[str, Any]:
    result: dict[str, Any] = {
        "schema_version": REPORT_SCHEMA_VERSION,
        "ok": False,
        "status": status,
    }
    if error_type is not None:
        result["error_type"] = error_type
    return result


def main(argv: list[str] | None = None) -> int:
    parser = _parser()
    try:
        args = parser.parse_args(argv)
        _argument_values(args)
        if args.inject_failure and os.environ.get("TENDWIRE_BENCHMARK_TESTING") != "1":
            raise _ArgumentError("test_injection_forbidden")
    except (SystemExit, _ArgumentError):
        print(_canonical_json(_failure_envelope("invalid_arguments")))
        return 2
    try:
        report = _candidate_run(args) if args.candidate_child else _public_run(args)
    except BaseException as exc:
        print(_canonical_json(_failure_envelope("benchmark_failed", error_type=type(exc).__name__)))
        return 1
    print(_canonical_json(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
