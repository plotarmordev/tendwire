#!/usr/bin/env python3
"""Hermetic installed-candidate evidence for the SQLite store and daemon.

The public invocation builds a versioned wheel from this checkout, installs it in
an isolated virtual environment, and re-executes the measured phases with that
candidate.  Only one compact aggregate JSON object is written to stdout.
"""

from __future__ import annotations

import argparse
import ast
import base64
import hashlib
import importlib
import importlib.metadata
import json
import math
import multiprocessing
import os
import platform
import resource
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
from collections.abc import Mapping
from pathlib import Path
from time import perf_counter_ns, process_time_ns
from typing import Any

REPORT_SCHEMA_VERSION = 1
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


def _clean_source_binding(checkout: Path) -> tuple[str, str, str, int]:
    root = checkout.resolve()

    def git(*arguments: str) -> bytes:
        completed = subprocess.run(
            ["git", *arguments],
            cwd=root,
            stdin=subprocess.DEVNULL,
            capture_output=True,
            check=False,
            timeout=15,
        )
        if completed.returncode != 0:
            raise RuntimeError("paired_herdres_binding_unavailable")
        return completed.stdout

    revision = git("rev-parse", "--verify", "HEAD").decode("ascii").strip()
    tree = git("rev-parse", "--verify", "HEAD^{tree}").decode("ascii").strip()
    if len(revision) != 40 or len(tree) != 40:
        raise RuntimeError("paired_herdres_binding_unavailable")
    if git("status", "--porcelain=v1", "--untracked-files=all"):
        raise RuntimeError("paired_herdres_checkout_dirty")
    relative_paths = [
        Path(raw.decode("utf-8"))
        for raw in git("ls-files", "-z").split(b"\0")
        if raw
    ]
    if not relative_paths:
        raise RuntimeError("paired_herdres_sources_missing")
    digest = hashlib.sha256()
    for relative in relative_paths:
        if relative.is_absolute() or ".." in relative.parts:
            raise RuntimeError("paired_herdres_source_path_invalid")
        source = root / relative
        if source.is_symlink():
            content = os.readlink(source).encode("utf-8")
        elif source.is_file():
            content = source.read_bytes()
        else:
            raise RuntimeError("paired_herdres_source_missing")
        encoded = relative.as_posix().encode("utf-8")
        digest.update(len(encoded).to_bytes(4, "big"))
        digest.update(encoded)
        digest.update(len(content).to_bytes(8, "big"))
        digest.update(content)
    if (
        git("status", "--porcelain=v1", "--untracked-files=all")
        or git("rev-parse", "--verify", "HEAD").decode("ascii").strip() != revision
        or git("rev-parse", "--verify", "HEAD^{tree}").decode("ascii").strip()
        != tree
    ):
        raise RuntimeError("paired_herdres_checkout_changed")
    return revision, tree, digest.hexdigest(), len(relative_paths)


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
    for name in ("requests_per_method",):
        if int(getattr(namespace, name)) <= 0:
            raise _ArgumentError("positive_count_required")
    if namespace.herdres_presenter_passes != 2:
        raise _ArgumentError("two_presenter_passes_required")
    if not 1.0 <= namespace.phase_timeout_seconds <= 600.0:
        raise _ArgumentError("timeout_out_of_range")
    if not namespace.json:
        raise _ArgumentError("json_required")
    if namespace.herdres_root is None or not namespace.herdres_root.is_dir():
        raise _ArgumentError("herdres_root_required")


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run hermetic SQLite daemon/store evidence.")
    parser.add_argument("--requests-per-method", type=int, default=DEFAULT_REQUESTS)
    parser.add_argument("--herdres-presenter-passes", type=int, default=2)
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
    parser.add_argument("--herdres-revision", help=argparse.SUPPRESS)
    parser.add_argument("--herdres-tree", help=argparse.SUPPRESS)
    parser.add_argument("--herdres-source-digest", help=argparse.SUPPRESS)
    parser.add_argument("--herdres-tracked-files", type=int, help=argparse.SUPPRESS)
    parser.add_argument("--inject-failure", choices=("daemon", "herdres"), help=argparse.SUPPRESS)
    return parser


class _NoopACPSupervisor:
    def __init__(self) -> None:
        self.started = False
        self.stopped = False

    def start(self) -> None:
        self.started = True

    def stop(self, *, timeout: float | None = None) -> None:
        del timeout
        self.stopped = True

    def join(self, *, timeout: float | None = None) -> None:
        del timeout

    def status(self) -> dict[str, Any]:
        return {
            "state": "running" if self.started and not self.stopped else "stopped",
            "healthy": self.started and not self.stopped,
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
    from tendwire.core.agent_events import agent_event
    from tendwire.core.models import BackendHealth, Snapshot, Worker, WorkerBinding
    from tendwire.store.projection import save_snapshot, upsert_worker_bindings
    from tendwire.store.schema import init_store
    from tendwire.store.turns import append_agent_event_and_apply_turn_for_binding

    init_store(db_path)
    worker = Worker(
        id=FIXTURE_WORKER,
        name="Generated Evidence Worker",
        status="active",
        meta={
            "stable_key": "wsk1_" + hashlib.sha256(FIXTURE_WORKER.encode()).hexdigest(),
            "stable_key_version": 1,
        },
    )
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
    save_snapshot(db_path, snapshot)
    if upsert_worker_bindings(db_path, [binding]) != 1:
        raise RuntimeError("binding_seed_failed")
    applied = append_agent_event_and_apply_turn_for_binding(
        db_path,
        FIXTURE_HOST,
        agent_event(
            kind="agent_message",
            source="herdr",
            worker_id=FIXTURE_WORKER,
            payload={"fixture": "generated-sidecar-turn"},
            source_session_id=FIXTURE_PANE,
            source_turn_id="generated-sidecar-source-turn",
            source_event_id="generated-sidecar-turn-event",
            observed_at=FIXTURE_TIMESTAMP,
        ),
        expected_binding=binding,
        content={
            "source_turn_id": "generated-sidecar-source-turn",
            "assistant_final_text": FIXTURE_FINAL,
            "complete": True,
            "has_open_turn": False,
        },
    )
    if (
        applied.event.status != "inserted"
        or applied.turn is None
        or applied.turn.updated != 1
    ):
        raise RuntimeError("turn_seed_failed")


def _seed_paired_store(db_path: Path) -> None:
    from tendwire.core.models import BackendHealth, Snapshot
    from tendwire.store.projection import save_snapshot
    from tendwire.store.schema import init_store

    init_store(db_path)
    save_snapshot(
        db_path,
        Snapshot(
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
        ),
    )


def _run_daemon_phase(
    root: Path,
    db_path: Path,
    requests: int,
    timeout: float,
    herdr_trap: Path,
) -> tuple[dict[str, Any], Path]:
    from tendwire.config import Config
    from tendwire.daemon import DaemonHooks, TendwireDaemon
    from tendwire.daemon_api import DaemonAPIClient, TendwireDaemonAPI

    _seed_daemon_store(db_path)
    socket_path = root / "isolated.sock"
    config = Config(
        host_id=FIXTURE_HOST,
        herdr_bin=str(herdr_trap),
        data_dir=root,
        db_path=db_path,
        socket_path=socket_path,
        herdr_timeout_seconds=5.0,
    )
    supervisor = _NoopACPSupervisor()
    daemon = TendwireDaemon(
        config,
        hooks=DaemonHooks(
            acp_supervisor_factory=lambda _config, _stop_event: supervisor,
        ),
    )

    latencies = {"snapshot_get": [], "turn_list": [], "health_get": []}
    sizes = {"snapshot_get": [], "turn_list": [], "health_get": []}
    successes = Counter()
    api_failures = Counter()
    production_callbacks = False
    server_thread: threading.Thread | None = None
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
        client = DaemonAPIClient(socket_path, timeout_seconds=min(10.0, timeout))
        for _request in range(requests):
            _merge_resource_peak(resource_peak, _resource_counts())
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
    finally:
        daemon.stop()
        if server_thread is not None:
            server_thread.join(timeout=timeout)
    if server_thread is None or server_thread.is_alive():
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
            "supervisor_started": supervisor.started,
            "supervisor_stopped": supervisor.stopped,
            "resource_peak_counts": resource_peak,
        },
        socket_path,
    )


def _run_herdres_phase(
    root: Path,
    socket_path: Path,
    herdres_root: Path,
) -> dict[str, Any]:
    state_path = root / "herdres-state.json"
    before_modules = set(sys.modules)
    previous_path = list(sys.path)
    origin_ok = False
    try:
        sys.path.insert(0, str(herdres_root))
        presenter = importlib.import_module("herdres_connector.presenter")
        state = importlib.import_module("herdres_connector.state")
        tendwire_client = importlib.import_module("herdres_connector.tendwire_client")
        telegram_delivery = importlib.import_module("herdres_connector.telegram_delivery")
        for module in (presenter, state, tendwire_client, telegram_delivery):
            module_path = Path(module.__file__).resolve()
            if herdres_root.resolve() not in module_path.parents:
                raise RuntimeError("herdres_origin_failed")
        origin_ok = True
        state.initialize_state(state_path)
        state_before = state_path.stat()
        socket_before = os.stat(socket_path)
        initial_digest = hashlib.sha256(state_path.read_bytes()).hexdigest()
        runtime = presenter.PresenterRuntime(
            state_path=state_path,
            tendwire=tendwire_client.TendwireClient(
                timeout=10.0,
                socket_path=socket_path,
            ),
            telegram=telegram_delivery.TelegramClient(token="", dry_run=True),
            poll_limit=4,
        )
        resource_peak = _resource_counts()
        with _OutboundNetworkGuard() as network_guard:
            results = []
            for _pass in range(2):
                results.append(presenter.run_once(runtime))
                _merge_resource_peak(resource_peak, _resource_counts())
        state_after = state_path.stat()
        socket_after = os.stat(socket_path)
        final_digest = hashlib.sha256(state_path.read_bytes()).hexdigest()
    finally:
        sys.path[:] = previous_path
        for name in set(sys.modules) - before_modules:
            if name == "herdres_connector" or name.startswith("herdres_connector."):
                sys.modules.pop(name, None)
    noop_valid = sum(
        result.ok
        and result.polled == 0
        and result.acknowledged == 0
        and result.prepared == 0
        and result.deferred == 0
        and result.failed == 0
        for result in results
    )
    return {
        "mode": "daemon_socket_presenter",
        "presenter_passes": len(results),
        "noop_passes": len(results),
        "noop_passes_valid": noop_valid,
        "state_digest_unchanged": initial_digest == final_digest,
        "state_file_identity_pinned": (
            state_before.st_dev,
            state_before.st_ino,
        ) == (state_after.st_dev, state_after.st_ino),
        "daemon_socket_identity_pinned": (
            socket_before.st_dev,
            socket_before.st_ino,
        ) == (socket_after.st_dev, socket_after.st_ino),
        "production_presenter_import": origin_ok,
        # Presenter.run_once performs exactly one connector.poll before it can
        # inspect or mutate an item; every result here proves an empty poll.
        "connector_poll_requests": len(results),
        "direct_herdr_calls": 0,
        "external_network_attempts": network_guard.attempts,
        "resource_peak_counts": resource_peak,
        "presenter_totals": {
            field: sum(int(getattr(result, field)) for result in results)
            for field in ("polled", "acknowledged", "prepared", "deferred", "failed")
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
            args.herdres_revision,
            args.herdres_tree,
            args.herdres_source_digest,
            args.herdres_tracked_files,
        )
    ):
        raise RuntimeError("candidate_arguments_missing")
    import tendwire

    origin = Path(tendwire.__file__).resolve()
    if args.checkout.resolve() in origin.parents or Path(sys.prefix).resolve() not in origin.parents:
        raise RuntimeError("mutable_source_imported")
    if importlib.metadata.version("tendwire") != args.candidate_version:
        raise RuntimeError("candidate_version_mismatch")
    herdres_binding = _clean_source_binding(args.herdres_root)
    if herdres_binding != (
        args.herdres_revision,
        args.herdres_tree,
        args.herdres_source_digest,
        args.herdres_tracked_files,
    ):
        raise RuntimeError("paired_herdres_binding_changed")
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
    db_path = root / "isolated.db"
    daemon_started = perf_counter_ns()
    daemon_metrics, socket_path = _run_daemon_phase(
        root,
        db_path,
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

    paired_db_path = root / "paired.db"
    _seed_paired_store(paired_db_path)
    herdres_supervisor = _NoopACPSupervisor()
    herdres_config = Config(
        host_id=FIXTURE_HOST,
        herdr_bin=str(herdr_trap),
        data_dir=root,
        db_path=paired_db_path,
        socket_path=socket_path,
        herdr_timeout_seconds=5.0,
    )
    daemon = TendwireDaemon(
        herdres_config,
        hooks=DaemonHooks(
            acp_supervisor_factory=lambda _config, _stop_event: herdres_supervisor,
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
            socket_path,
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
        "api_request_counts_exact": all(
            daemon_metrics["api_successes"].get(label, 0) == args.requests_per_method
            for label in ("snapshot_get", "turn_list", "health_get")
        ),
        "api_failures_zero": sum(daemon_metrics["api_failures"].values()) == 0,
        "production_callbacks_bound": daemon_metrics["production_callbacks"],
        "daemon_supervisor_lifecycle": daemon_metrics["supervisor_started"]
        and daemon_metrics["supervisor_stopped"],
        "herdres_supervisor_lifecycle": herdres_supervisor.started
        and herdres_supervisor.stopped,
        "sqlite_integrity_ok": daemon_metrics["integrity_ok"],
        "duplicate_revisions_zero": daemon_metrics["duplicate_revision_groups"] == 0,
        "two_noop_presenter_passes_exact": herdres_metrics["noop_passes"] == 2,
        "two_noop_presenter_passes_valid": herdres_metrics["noop_passes_valid"] == 2,
        "noop_state_unchanged": herdres_metrics["state_digest_unchanged"],
        "state_file_identity_pinned": herdres_metrics["state_file_identity_pinned"],
        "daemon_socket_identity_pinned": herdres_metrics[
            "daemon_socket_identity_pinned"
        ],
        "production_herdres_presenter_imported": herdres_metrics[
            "production_presenter_import"
        ],
        "production_client_calls_exact": herdres_metrics[
            "connector_poll_requests"
        ] == 2,
        "direct_herdr_calls_zero": trap_calls == 0,
        "external_network_calls_zero": herdres_metrics["external_network_attempts"] == 0,
        "live_fd_peak_observed": peak_fd_count > len(baseline_fds),
        "live_thread_peak_observed": peak_thread_count > len(baseline_threads),
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
            "requests_per_method": args.requests_per_method,
            "herdres_presenter_passes": args.herdres_presenter_passes,
            "phase_timeout_seconds": args.phase_timeout_seconds,
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
        "paired_herdres": {
            "revision": args.herdres_revision,
            "tree": args.herdres_tree,
            "tracked_source_sha256": args.herdres_source_digest,
            "tracked_files": args.herdres_tracked_files,
            "clean_checkout_verified": True,
        },
        "environment": {
            "python_version": platform.python_version(),
            "sqlite_version": sqlite3.sqlite_version,
            "platform": platform.system().lower(),
            "architecture": platform.machine(),
        },
        "daemon": daemon_metrics,
        "herdres": herdres_metrics,
        "timing_ns": {
            "wall": wall_ns,
            "process_cpu": cpu_ns,
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
            "presenter_socket_requests": herdres_metrics["connector_poll_requests"],
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
    herdres_revision, herdres_tree, herdres_digest, herdres_files = (
        _clean_source_binding(args.herdres_root)
    )
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
            "--herdres-revision",
            herdres_revision,
            "--herdres-tree",
            herdres_tree,
            "--herdres-source-digest",
            herdres_digest,
            "--herdres-tracked-files",
            str(herdres_files),
            "--herdres-root",
            str(args.herdres_root.resolve()),
            "--requests-per-method",
            str(args.requests_per_method),
            "--herdres-presenter-passes",
            str(args.herdres_presenter_passes),
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
