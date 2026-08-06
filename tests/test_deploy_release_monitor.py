from __future__ import annotations

import importlib.util
import io
import json
import sqlite3
import sys
import types
from pathlib import Path

import pytest


_MONITORS = tuple((Path(__file__).parents[1] / ".deploy").glob("monitor-frozen-*-hour.py"))
assert len(_MONITORS) == 1
SCRIPT = _MONITORS[0]
EXPECTED_RECEIVERS = {"manager", "managed-codex", "managed-omp", "managed-kimi"}


def _load_monitor():
    spec = importlib.util.spec_from_file_location("frozen_release_monitor_test", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _monitor_databases(tmp_path: Path, receivers: set[str]) -> tuple[Path, Path]:
    tendwire_path = tmp_path / "tendwire.db"
    tendwire = sqlite3.connect(tendwire_path)
    tendwire.execute("CREATE TABLE connector_outbox(status TEXT NOT NULL)")
    tendwire.execute("PRAGMA user_version=30")
    tendwire.commit()
    tendwire.close()

    ingress_path = tmp_path / "ingress.db"
    ingress = sqlite3.connect(ingress_path)
    ingress.execute("CREATE TABLE receiver_cursors(receiver_id TEXT PRIMARY KEY)")
    ingress.execute("CREATE TABLE requests(state TEXT NOT NULL)")
    ingress.executemany(
        "INSERT INTO receiver_cursors VALUES(?)", ((receiver,) for receiver in receivers)
    )
    ingress.execute("PRAGMA user_version=1")
    ingress.commit()
    ingress.close()
    return tendwire_path, ingress_path


@pytest.mark.parametrize(
    ("receivers", "valid"),
    [
        (EXPECTED_RECEIVERS, True),
        (EXPECTED_RECEIVERS - {"managed-omp"}, False),
        (EXPECTED_RECEIVERS | {"unexpected"}, False),
    ],
)
def test_monitor_requires_exact_receiver_set(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    receivers: set[str],
    valid: bool,
) -> None:
    module = _load_monitor()
    tendwire, ingress = _monitor_databases(tmp_path, receivers)
    monkeypatch.setattr(module, "TENDWIRE_DB", tendwire)
    monkeypatch.setattr(module, "HERDRES_INGRESS", ingress)
    if valid:
        sample = module._database_sample()
        assert sample["receiver_cursor_count"] == 4
    else:
        with pytest.raises(module.MonitorFailure, match="receiver_cursor_set_changed"):
            module._database_sample()


@pytest.mark.parametrize(
    ("workers", "expected_exit"),
    [
        ([{"name": "codex", "status": "idle"}], 0),
        ([], 1),
        ([{"name": "codex", "status": "idle"}, {"name": "codex", "status": "idle"}], 1),
        ([{"name": "claude", "status": "idle"}], 1),
    ],
)
def test_health_program_requires_exactly_one_codex_worker(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    workers: list[dict[str, str]],
    expected_exit: int,
) -> None:
    module = _load_monitor()

    class Client:
        def __init__(self, *_args, **_kwargs) -> None:
            pass

        def request(self, method: str, _params: dict[str, object]):
            if method == "health.get":
                return {
                    "ok": True,
                    "result": {
                        "daemon": {"status": "healthy"},
                        "backend": {"status": "healthy", "ready": True, "running": True},
                        "acp": {
                            "healthy": True,
                            "state": "running",
                            "failure_type": None,
                            "worker_count": len(workers),
                        },
                    },
                }
            return {"ok": True, "result": {"workers": workers}}

    package = types.ModuleType("tendwire")
    daemon_api = types.ModuleType("tendwire.daemon_api")
    daemon_api.DaemonAPIClient = Client
    monkeypatch.setitem(sys.modules, "tendwire", package)
    monkeypatch.setitem(sys.modules, "tendwire.daemon_api", daemon_api)
    monkeypatch.setattr(sys, "argv", ["health-program", "/tmp/not-opened.sock"])
    with pytest.raises(SystemExit) as raised:
        exec(compile(module.HEALTH_PROGRAM, "<health-program>", "exec"), {})
    assert raised.value.code == expected_exit
    assert json.loads(capsys.readouterr().out)["healthy"] is (expected_exit == 0)


def test_monitor_requires_exactly_one_live_herdres_worker_and_topic(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _load_monitor()
    state_path = tmp_path / "state.json"
    monkeypatch.setattr(module, "HERDRES_STATE", state_path)

    def write_state(worker_count: int, topic_count: int) -> None:
        state_path.write_text(json.dumps({
            "schema_version": 1,
            "workers": {
                str(index): {"lifecycle_status": "live"}
                for index in range(worker_count)
            },
            "topics": {
                str(index): {"status": "active"}
                for index in range(topic_count)
            },
            "decision_controls": {},
        }))

    write_state(1, 1)
    sample = module._state_sample()
    assert sample["workers"]["live"] == sample["topics"]["active"] == 1
    write_state(2, 1)
    with pytest.raises(module.MonitorFailure, match="cardinality"):
        module._state_sample()
    write_state(1, 2)
    with pytest.raises(module.MonitorFailure, match="cardinality"):
        module._state_sample()


@pytest.mark.parametrize(
    ("live", "bindings", "forum", "valid"),
    [(1, 1, 2, True), (2, 2, 3, False), (1, 2, 2, False), (1, 1, 3, False)],
)
def test_monitor_requires_exactly_one_presenter_binding_and_one_non_general_forum_topic(
    monkeypatch: pytest.MonkeyPatch,
    live: int,
    bindings: int,
    forum: int,
    valid: bool,
) -> None:
    module = _load_monitor()
    result = {
        "schema_version": 1,
        "mode": "monitor_sample",
        "live_workers": live,
        "active_bindings": bindings,
        "forum_topics": forum,
        "general_preserved": True,
        "unsettled_topic_claims": 0,
        "plan_sha256": "a" * 64,
    }
    monkeypatch.setattr(module, "_run", lambda *_args, **_kwargs: json.dumps(result))
    if valid:
        assert module._forum_sample()["forum_topics"] == 2
    else:
        with pytest.raises(module.MonitorFailure, match="forum_invariant"):
            module._forum_sample()


def test_monitor_has_no_under_hour_or_interval_bypass(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _load_monitor()
    monkeypatch.setattr(module, "TRANSACTION_ROOT", tmp_path / "transaction")
    monkeypatch.setattr(sys, "argv", ["monitor", "--duration-seconds", "3599"])
    with pytest.raises(SystemExit, match="at least 3600"):
        module.main()
    assert not module.TRANSACTION_ROOT.exists()

    monkeypatch.setattr(
        sys,
        "argv",
        ["monitor", "--duration-seconds", "3600", "--interval-seconds", "14"],
    )
    with pytest.raises(SystemExit, match="invalid monitor timing"):
        module.main()
    assert not module.TRANSACTION_ROOT.exists()


def test_success_evidence_is_one_hour_and_aggregate_only(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    module = _load_monitor()
    transaction = tmp_path / module.TRANSACTION_ID
    monkeypatch.setattr(module, "TRANSACTION_ROOT", transaction)
    monkeypatch.setattr(module, "DEFAULT_OUTPUT", transaction / "one-hour-monitor.json")
    monkeypatch.setattr(module, "_begin_validation", lambda: None)
    monkeypatch.setattr(module, "_finish_validation", lambda _phase: None)
    monkeypatch.setattr(module, "_release_integrity_preflight", lambda: None)
    monkeypatch.setattr(module, "_release_preflight", lambda: None)
    monkeypatch.setattr(module, "_service_baseline", lambda: {"service": "private-pid"})
    monkeypatch.setattr(module, "_service_sample", lambda _baseline: {
        "active": 4, "stable": 4, "restart_count": 0,
    })
    monkeypatch.setattr(module, "_health_sample", lambda: {"healthy": True, "worker_count": 1})
    monkeypatch.setattr(module, "_database_sample", lambda: {
        "outbox_status": {"queued": 0, "other": 0},
        "ingress_state": {"pending": 0, "other": 0},
        "receiver_cursor_count": 4,
    })
    monkeypatch.setattr(module, "_state_sample", lambda: {
        "workers": {"live": 1, "other": 0},
        "topics": {"active": 1, "other": 0},
        "decision_controls": {"active": 0, "other": 0},
    })
    monkeypatch.setattr(module, "_forum_sample", lambda: {
        "live_workers": 1,
        "active_bindings": 1,
        "forum_topics": 2,
        "general_preserved": True,
        "unsettled_topic_claims": 0,
    })
    clock = [0.0]
    monkeypatch.setattr(module.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(module.time, "sleep", lambda seconds: clock.__setitem__(0, clock[0] + seconds))
    monkeypatch.setattr(sys, "argv", ["monitor"])

    assert module.main() == 0
    evidence = json.loads(module.DEFAULT_OUTPUT.read_text())
    assert evidence["status"] == "success"
    assert evidence["elapsed_seconds"] >= 3600
    assert evidence["required_duration_seconds"] == 3600
    assert evidence["sample_count"] >= 60
    assert evidence["aggregate_counts_only"] is True
    assert evidence["contains_process_ids"] is False
    assert all(
        set(sample) == {
            "index", "observed_at", "elapsed_seconds", "services", "health",
            "stores", "lifecycle", "telegram_forum",
        }
        for sample in evidence["samples"]
    )
    encoded = json.dumps(evidence, sort_keys=True)
    assert "private-pid" not in encoded
    assert not any(
        forbidden in encoded
        for forbidden in ('"worker_id"', '"topic_id"', '"message_id"', '"receiver_id"')
    )
    assert "MONITOR_SUCCESS" in capsys.readouterr().out
