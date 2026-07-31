"""ACP lifecycle policy tests for the Tendwire daemon."""

from __future__ import annotations

import json
import os
import stat
import threading
from pathlib import Path
from typing import Any

import pytest

from tendwire.config import Config
from tendwire.core.models import BackendHealth, Snapshot
from tendwire.daemon import DaemonHooks, TendwireDaemon
from tendwire.store.sqlite import init_store, save_snapshot


def _snapshot() -> Snapshot:
    return Snapshot(
        host_id="daemon-host",
        updated_at="2026-01-01T00:00:00+00:00",
        backend_health=[
            BackendHealth(
                name="herdr",
                status="healthy",
                outcome="empty_healthy",
                observed_at="2026-01-01T00:00:00+00:00",
            )
        ],
    )


class _Scheduler:
    def __init__(self, calls: list[str]) -> None:
        self.calls = calls

    def start(self) -> None:
        self.calls.append("scheduler_start")

    def request_refresh(self) -> None:
        self.calls.append("scheduler_request")

    def stop(self, *, flush_timeout_seconds: float | None = None) -> None:
        self.calls.append(f"scheduler_stop:{flush_timeout_seconds}")

    def operational_status(self) -> dict[str, Any]:
        return {"status": "healthy"}


class _Runtime:
    def __init__(
        self,
        calls: list[str],
        *,
        healthy: bool = True,
        start_failure: BaseException | None = None,
    ) -> None:
        self.calls = calls
        self.healthy = healthy
        self.start_failure = start_failure

    def start(self) -> None:
        self.calls.append("acp_start")
        if self.start_failure is not None:
            raise self.start_failure

    def status(self) -> dict[str, Any]:
        return {
            "state": "running" if self.healthy else "failed",
            "healthy": self.healthy,
            "updates_ingested": 7,
            "permissions_ingested": 2,
            "permissions_selected": 1,
            "permissions_cancelled": 1,
            "invalid_permission_selections": 0,
            "prompts_started": 3,
            "prompts_completed": 2,
            "prompts_failed": 1,
            "cancellation_requests": 1,
            "failure_type": None if self.healthy else "AcpTransportError",
            # Deliberately private transport material must never be projected.
            "argv": ["sentinel-private-command"],
            "session_id": "sentinel-private-session",
            "binding_id": "sentinel-private-binding",
        }

    def stop(self, *, timeout: float) -> None:
        self.calls.append(f"acp_stop:{timeout}")

    def join(self, *, timeout: float) -> bool:
        self.calls.append(f"acp_join:{timeout}")
        return True


def _hooks(
    tmp_path: Path,
    calls: list[str],
    *,
    acp_runtime_factory: Any = None,
    scheduler_factory: Any = None,
) -> DaemonHooks:
    db_path = tmp_path / "daemon.db"

    def initialize(path: Path) -> None:
        calls.append("init_store")
        init_store(path)

    def observe(_config: Config) -> Snapshot:
        calls.append("observe")
        snapshot = _snapshot()
        save_snapshot(db_path, snapshot)
        return snapshot

    def make_scheduler(_config: Config) -> _Scheduler:
        calls.append("scheduler_factory")
        return _Scheduler(calls)

    return DaemonHooks(
        init_store=initialize,
        observe_initial_snapshot=observe,
        turn_scheduler_factory=scheduler_factory or make_scheduler,
        acp_runtime_factory=acp_runtime_factory,
    )


def _config(tmp_path: Path, policy: str) -> Config:
    return Config(
        host_id="daemon-host",
        data_dir=tmp_path,
        db_path=tmp_path / "daemon.db",
        socket_path=tmp_path / "daemon.sock",
        agent_event_source=policy,
        acp_shutdown_timeout_seconds=1.25,
    )


def test_legacy_policy_never_calls_acp_factory(tmp_path: Path) -> None:
    calls: list[str] = []

    def forbidden_factory(_config: Config, _stop_event: threading.Event) -> Any:
        raise AssertionError("legacy mode must never discover or start ACP")

    daemon = TendwireDaemon(
        _config(tmp_path, "legacy"),
        hooks=_hooks(tmp_path, calls, acp_runtime_factory=forbidden_factory),
    )
    daemon.start()
    try:
        assert daemon.get_health()["acp"] == {
            "policy": "legacy",
            "status": "disabled",
            "healthy": False,
            "state": "disabled",
            "failure_type": None,
            "counters": {
                "updates_ingested": 0,
                "permissions_ingested": 0,
                "permissions_selected": 0,
                "permissions_cancelled": 0,
                "invalid_permission_selections": 0,
                "prompts_started": 0,
                "prompts_completed": 0,
                "prompts_failed": 0,
                "cancellation_requests": 0,
            },
        }
    finally:
        daemon.stop()

    assert calls[-1] == "scheduler_stop:6.0"


@pytest.mark.parametrize("policy", ["acp_shadow", "acp_preferred"])
def test_optional_acp_policy_tolerates_unavailable_runtime(
    tmp_path: Path,
    policy: str,
) -> None:
    calls: list[str] = []

    def unavailable(_config: Config, _stop_event: threading.Event) -> None:
        calls.append("acp_factory")
        return None

    daemon = TendwireDaemon(
        _config(tmp_path, policy),
        hooks=_hooks(tmp_path, calls, acp_runtime_factory=unavailable),
    )
    daemon.start()
    try:
        assert calls[-2:] == ["scheduler_start", "scheduler_request"]
        assert daemon.get_health()["acp"]["status"] == "unavailable"
    finally:
        daemon.stop()


def test_required_acp_without_factory_fails_before_socket_or_scheduler(
    tmp_path: Path,
) -> None:
    calls: list[str] = []
    socket_path = tmp_path / "daemon.sock"
    daemon = TendwireDaemon(
        _config(tmp_path, "acp_required"),
        hooks=_hooks(tmp_path, calls),
    )

    with pytest.raises(RuntimeError, match="ACP runtime is required"):
        daemon.start()

    assert calls == ["init_store", "observe"]
    assert not os.path.lexists(socket_path)
    assert daemon.server is None


def test_required_acp_starts_before_socket_and_exposes_only_redacted_health(
    tmp_path: Path,
) -> None:
    calls: list[str] = []
    socket_path = tmp_path / "daemon.sock"
    runtime = _Runtime(calls)

    def runtime_factory(config: Config, stop_event: threading.Event) -> _Runtime:
        assert config.agent_event_source == "acp_required"
        assert stop_event.is_set() is False
        assert not os.path.lexists(socket_path)
        calls.append("acp_factory")
        return runtime

    daemon = TendwireDaemon(
        _config(tmp_path, "acp_required"),
        hooks=_hooks(tmp_path, calls, acp_runtime_factory=runtime_factory),
    )
    daemon.start()
    try:
        assert calls == [
            "init_store",
            "observe",
            "acp_factory",
            "acp_start",
        ]
        assert stat.S_ISSOCK(os.lstat(socket_path).st_mode)
        health = daemon.get_health()
        assert health["status"] == "ok"
        acp = health["acp"]
        assert acp == {
            "policy": "acp_required",
            "status": "healthy",
            "healthy": True,
            "state": "running",
            "failure_type": None,
            "counters": {
                "updates_ingested": 7,
                "permissions_ingested": 2,
                "permissions_selected": 1,
                "permissions_cancelled": 1,
                "invalid_permission_selections": 0,
                "prompts_started": 3,
                "prompts_completed": 2,
                "prompts_failed": 1,
                "cancellation_requests": 1,
            },
        }
        encoded = json.dumps(acp)
        assert "sentinel-private" not in encoded
        assert "argv" not in encoded
        assert "session" not in encoded
        assert "binding" not in encoded
        runtime.healthy = False
        degraded = daemon.get_health()
        assert degraded["status"] == "degraded"
        assert degraded["acp"]["status"] == "degraded"
    finally:
        daemon.stop()

    assert calls[-2:] == [
        "acp_stop:1.25",
        "acp_join:1.25",
    ]


@pytest.mark.parametrize("policy", ["acp_shadow", "acp_preferred"])
def test_optional_unhealthy_acp_is_stopped_and_legacy_scheduler_continues(
    tmp_path: Path,
    policy: str,
) -> None:
    calls: list[str] = []
    runtime = _Runtime(calls, healthy=False)
    daemon = TendwireDaemon(
        _config(tmp_path, policy),
        hooks=_hooks(
            tmp_path,
            calls,
            acp_runtime_factory=lambda _config, _stop_event: runtime,
        ),
    )

    daemon.start()
    try:
        assert calls == [
            "init_store",
            "observe",
            "acp_start",
            "acp_stop:1.25",
            "acp_join:1.25",
            "scheduler_factory",
            "scheduler_start",
            "scheduler_request",
        ]
        acp = daemon.get_health()["acp"]
        assert acp["status"] == "unavailable"
        assert acp["failure_type"] == "AcpTransportError"
    finally:
        daemon.stop()


def test_required_unhealthy_acp_stops_and_fails_closed(tmp_path: Path) -> None:
    calls: list[str] = []
    runtime = _Runtime(calls, healthy=False)
    daemon = TendwireDaemon(
        _config(tmp_path, "acp_required"),
        hooks=_hooks(
            tmp_path,
            calls,
            acp_runtime_factory=lambda _config, _stop_event: runtime,
        ),
    )

    with pytest.raises(RuntimeError, match=r"failed to start \(AcpTransportError\)"):
        daemon.start()

    assert calls == [
        "init_store",
        "observe",
        "acp_start",
        "acp_stop:1.25",
        "acp_join:1.25",
    ]
    assert not os.path.lexists(tmp_path / "daemon.sock")


def test_optional_acp_start_failure_is_cleaned_up_before_legacy_fallback(
    tmp_path: Path,
) -> None:
    calls: list[str] = []
    runtime = _Runtime(
        calls,
        start_failure=RuntimeError("sentinel-private-command --session secret"),
    )
    daemon = TendwireDaemon(
        _config(tmp_path, "acp_preferred"),
        hooks=_hooks(
            tmp_path,
            calls,
            acp_runtime_factory=lambda _config, _stop_event: runtime,
        ),
    )

    daemon.start()
    try:
        assert calls[2:5] == ["acp_start", "acp_stop:1.25", "acp_join:1.25"]
        acp = daemon.get_health()["acp"]
        assert acp["status"] == "unavailable"
        assert acp["failure_type"] == "RuntimeError"
        assert "sentinel-private" not in json.dumps(acp)
        assert calls[-2:] == ["scheduler_start", "scheduler_request"]
    finally:
        daemon.stop()


def test_required_acp_never_constructs_legacy_scheduler(tmp_path: Path) -> None:
    calls: list[str] = []
    runtime = _Runtime(calls)

    def forbidden_scheduler(_config: Config) -> _Scheduler:
        raise AssertionError("acp_required must never construct legacy ingestion")

    daemon = TendwireDaemon(
        _config(tmp_path, "acp_required"),
        hooks=_hooks(
            tmp_path,
            calls,
            acp_runtime_factory=lambda _config, _stop_event: runtime,
            scheduler_factory=forbidden_scheduler,
        ),
    )

    daemon.start()
    try:
        assert calls == ["init_store", "observe", "acp_start"]
    finally:
        daemon.stop()

    assert calls[-2:] == ["acp_stop:1.25", "acp_join:1.25"]
