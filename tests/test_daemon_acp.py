"""Required ACP lifecycle contract tests for the Tendwire daemon."""

from __future__ import annotations

import json
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
        updated_at="2026-08-04T00:00:00+00:00",
        backend_health=[
            BackendHealth(
                name="herdr",
                status="healthy",
                outcome="empty_healthy",
            )
        ],
    )


class _Supervisor:
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
            "prompts_completed": 3,
            "failure_type": None if self.healthy else "AcpTransportError",
            "argv": ["sentinel-private-command"],
            "session_id": "sentinel-private-session",
        }

    def stop(self, *, timeout: float) -> None:
        self.calls.append(f"acp_stop:{timeout}")

    def join(self, *, timeout: float) -> bool:
        self.calls.append(f"acp_join:{timeout}")
        return True


def _config(tmp_path: Path) -> Config:
    tmp_path.chmod(0o700)
    return Config(
        host_id="daemon-host",
        data_dir=tmp_path,
        db_path=tmp_path / "daemon.db",
        socket_path=tmp_path / "daemon.sock",
        herdr_backend="cli",
        acp_shutdown_timeout_seconds=1.25,
    )


def _hooks(
    config: Config,
    supervisor_factory: Any,
    calls: list[str],
) -> DaemonHooks:
    assert config.db_path is not None

    def initialize(path: Path) -> None:
        calls.append("init_store")
        init_store(path)

    def observe(_config: Config) -> Snapshot:
        calls.append("observe")
        snapshot = _snapshot()
        save_snapshot(config.db_path, snapshot)
        return snapshot

    return DaemonHooks(
        init_store=initialize,
        observe_initial_snapshot=observe,
        acp_supervisor_factory=supervisor_factory,
    )


def test_daemon_requires_an_acp_supervisor_before_binding_socket(tmp_path: Path) -> None:
    config = _config(tmp_path)
    calls: list[str] = []
    daemon = TendwireDaemon(config, hooks=_hooks(config, None, calls))

    with pytest.raises(RuntimeError, match="ACP supervisor is required"):
        daemon.start()

    assert not config.socket_path.exists()
    assert calls == ["init_store", "observe"]


def test_daemon_starts_required_acp_and_exposes_only_public_health(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("tendwire.daemon.UnixSocketJSONServer.start", lambda _self: None)
    config = _config(tmp_path)
    calls: list[str] = []
    supervisor = _Supervisor(calls)
    daemon = TendwireDaemon(
        config,
        hooks=_hooks(config, lambda _config, _stop: supervisor, calls),
    )

    daemon.start()
    try:
        health = daemon.get_health()
        assert health["status"] == "ok"
        assert health["acp"]["required"] is True
        assert health["acp"]["healthy"] is True
        assert health["acp"]["state"] == "running"
        assert health["acp"]["counters"]["updates_ingested"] == 7
        assert "sentinel-private" not in json.dumps(health)
        assert calls[:3] == ["init_store", "observe", "acp_start"]
    finally:
        daemon.stop()

    assert calls[-2:] == ["acp_stop:1.25", "acp_join:1.25"]


@pytest.mark.parametrize(
    "supervisor",
    [
        pytest.param(_Supervisor([], healthy=False), id="unhealthy"),
        pytest.param(
            _Supervisor([], start_failure=OSError("private transport detail")),
            id="start-failure",
        ),
    ],
)
def test_daemon_fails_closed_when_required_acp_cannot_start(
    tmp_path: Path,
    supervisor: _Supervisor,
) -> None:
    config = _config(tmp_path)
    calls = supervisor.calls
    daemon = TendwireDaemon(
        config,
        hooks=_hooks(config, lambda _config, _stop: supervisor, calls),
    )

    with pytest.raises(RuntimeError, match="ACP supervisor is required"):
        daemon.start()

    assert not config.socket_path.exists()
    assert any(call.startswith("acp_stop:") for call in calls)
    assert "private transport detail" not in repr(daemon._acp_startup_failure_type)


def test_supervisor_receives_daemon_stop_event(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("tendwire.daemon.UnixSocketJSONServer.start", lambda _self: None)
    config = _config(tmp_path)
    calls: list[str] = []
    seen: list[threading.Event] = []

    def factory(_config: Config, stop_event: threading.Event) -> _Supervisor:
        seen.append(stop_event)
        return _Supervisor(calls)

    daemon = TendwireDaemon(config, hooks=_hooks(config, factory, calls))
    daemon.start()
    try:
        assert seen == [daemon.stop_event]
    finally:
        daemon.stop()
