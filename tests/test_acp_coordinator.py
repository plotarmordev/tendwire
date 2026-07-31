"""Production ACP coordinator and command-path contract tests."""

from __future__ import annotations

import json
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from tendwire.backends.acp_coordinator import (
    AcpCoordinatorError,
    AcpRuntimeCoordinator,
    _parse_endpoint,
)
from tendwire.backends.herdr_turns import TurnIngestionScheduler, TurnRefreshResult
from tendwire.command_submission import submit_command
from tendwire.config import Config
from tendwire.core.models import BackendHealth, Snapshot, Worker, WorkerBinding
from tendwire.store.sqlite import (
    get_command_request,
    init_store,
    save_snapshot,
    upsert_worker_bindings,
)


def _config(tmp_path: Path, *, policy: str = "acp_preferred") -> Config:
    return Config(
        host_id="acp-host",
        data_dir=tmp_path,
        db_path=tmp_path / "tendwire.db",
        herdr_backend="socket",
        herdr_bin="herdr",
        agent_event_source=policy,
    )


def _binding() -> WorkerBinding:
    return WorkerBinding(
        host_id="acp-host",
        worker_id="worker-1",
        worker_fingerprint="worker-fingerprint",
        backend="herdr",
        target_kind="pane_id",
        target_value="pane-private",
        turn_target_kind="pane_id",
        turn_target_value="pane-private",
        sendable=True,
        private_fingerprint="herdr-private-binding",
    )


def _endpoint(*, generation: int = 42, lifecycle: str = "acp_owned_ready") -> dict[str, Any]:
    return {
        "type": "agent_acp_endpoint",
        "endpoint": {
            "transport": "stdio",
            "command": "herdr",
            "args": [
                "agent",
                "acp-attach",
                "pane-private",
                "--generation",
                str(generation),
                "--ticket",
                "one-shot-private-ticket",
            ],
            "protocol_version": 1,
        },
        "worker": {
            "terminal_id": "pane-private",
            "workspace_id": "workspace-private",
            "tab_id": "tab-private",
            "pane_id": "pane-private",
            "name": "agent-name",
            "agent": "codex",
            "generation": generation,
        },
        "adapter": {"name": "codex-acp", "version": "1.2.3"},
        "session": {"mode": "new"},
        "cwd": "/tmp/project",
        "lifecycle": lifecycle,
    }


def _status(*, generation: int = 42, lifecycle: str = "acp_owned_attached") -> dict[str, Any]:
    endpoint = _endpoint(generation=generation)
    return {
        "type": "agent_acp_status",
        "worker": endpoint["worker"],
        "adapter": endpoint["adapter"],
        "session": endpoint["session"],
        "cwd": endpoint["cwd"],
        "lifecycle": lifecycle,
    }


def test_endpoint_requires_explicit_acp_ownership_and_strict_attach_shape(tmp_path: Path) -> None:
    config = _config(tmp_path)
    parsed = _parse_endpoint(config, _binding(), _endpoint())
    assert parsed.generation == "42"
    assert parsed.command[0:3] == ("herdr", "agent", "acp-attach")

    with pytest.raises(AcpCoordinatorError, match="not ready"):
        _parse_endpoint(config, _binding(), _endpoint(lifecycle="ready"))
    injected = _endpoint()
    injected["endpoint"]["command"] = "/tmp/evil"
    with pytest.raises(AcpCoordinatorError, match="configured Herdr"):
        _parse_endpoint(config, _binding(), injected)
    replayed = _endpoint(generation=43)
    replayed["endpoint"]["args"][4] = "42"
    with pytest.raises(AcpCoordinatorError, match="inconsistent"):
        _parse_endpoint(config, _binding(), replayed)


def test_canonical_herdr_acp_contract_fixture_executes_configured_binary(
    tmp_path: Path,
) -> None:
    fixture = json.loads(
        (Path(__file__).parent / "fixtures" / "herdr_acp_contract_v1.json").read_text(
            encoding="utf-8"
        )
    )
    assert fixture["request"] == {
        "id": "tw:acp:endpoint",
        "method": "agent.acp_endpoint",
        "params": {"target": "term_abc"},
    }
    config = replace(_config(tmp_path), herdr_bin="/opt/herdr/bin/herdr")
    continuity = replace(
        _binding(),
        target_kind="terminal_id",
        target_value="term_abc",
        turn_target_kind="terminal_id",
        turn_target_value="term_abc",
        private_fingerprint="fixture-binding",
    )
    parsed = _parse_endpoint(config, continuity, fixture["result"])
    assert parsed.command[0] == "/opt/herdr/bin/herdr"
    assert parsed.command[1:] == tuple(fixture["result"]["endpoint"]["args"])
    assert parsed.generation == "42"


class _Route:
    binding_fingerprint = "acp-private-binding"

    def __init__(self, failure: BaseException | None = None) -> None:
        self.failure = failure
        self.calls: list[tuple[str, str, float]] = []

    def prompt(
        self,
        text: str,
        *,
        producer_turn_id: str,
        timeout: float,
    ) -> None:
        self.calls.append((text, producer_turn_id, timeout))
        if self.failure is not None:
            raise self.failure


def _seed(config: Config) -> Worker:
    assert config.db_path is not None
    init_store(config.db_path)
    worker = Worker(
        id="worker-1",
        name="worker",
        status="idle",
        fingerprint="worker-fingerprint",
    )
    save_snapshot(
        config.db_path,
        Snapshot(
            host_id=config.host_id,
            updated_at="2026-07-31T00:00:00+00:00",
            workers=[worker],
            backend_health=[
                BackendHealth(
                    name="herdr",
                    status="healthy",
                    outcome="healthy_non_empty",
                )
            ],
        ),
    )
    upsert_worker_bindings(config.db_path, [_binding()])
    return worker


def _request(request_id: str = "request-1") -> dict[str, Any]:
    return {
        "schema_version": 1,
        "action": "send_instruction",
        "request_id": request_id,
        "dry_run": False,
        "target": {"worker_id": "worker-1"},
        "instruction": {"text": "do the work"},
    }


def test_acp_command_uses_durable_receipt_and_duplicate_does_not_resend(tmp_path: Path) -> None:
    config = _config(tmp_path)
    worker = _seed(config)
    route = _Route()
    first = submit_command(
        config,
        _request(),
        acp_prompt_router=lambda routed: route if routed == worker else None,
    )
    second = submit_command(
        config,
        _request(),
        acp_prompt_router=lambda _worker: route,
    )
    assert first.status == "accepted"
    assert second.status == "accepted"
    assert len(route.calls) == 1
    receipt = get_command_request(config.db_path, config.host_id, "request-1")
    assert receipt is not None and receipt["state"] == "accepted"
    assert "acp-private-binding" not in json.dumps(first.to_dict())


def test_acp_failure_after_send_started_is_uncertain_and_never_falls_back(tmp_path: Path) -> None:
    config = _config(tmp_path)
    _seed(config)
    route = _Route(RuntimeError("private ticket and argv"))
    legacy_calls: list[str] = []
    envelope = submit_command(
        config,
        _request("request-uncertain"),
        acp_prompt_router=lambda _worker: route,
    )
    assert envelope.status == "request_state_uncertain"
    assert envelope.disposition == "terminal_uncertain"
    assert len(route.calls) == 1
    assert "private ticket" not in json.dumps(envelope.to_dict())
    receipt = get_command_request(
        config.db_path,
        config.host_id,
        "request-uncertain",
    )
    assert receipt is not None and receipt["state"] == "uncertain"


def test_concurrent_duplicate_acp_command_has_one_external_send(tmp_path: Path) -> None:
    config = _config(tmp_path)
    _seed(config)
    entered = threading.Event()
    release = threading.Event()

    class BlockingRoute(_Route):
        def prompt(self, text: str, *, producer_turn_id: str, timeout: float) -> None:
            self.calls.append((text, producer_turn_id, timeout))
            entered.set()
            assert release.wait(1.0)

    route = BlockingRoute()
    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(
            submit_command,
            config,
            _request("request-concurrent"),
            acp_prompt_router=lambda _worker: route,
        )
        assert entered.wait(1.0)
        second = pool.submit(
            submit_command,
            config,
            _request("request-concurrent"),
            acp_prompt_router=lambda _worker: route,
        )
        second_result = second.result(timeout=1.0)
        release.set()
        first_result = first.result(timeout=1.0)
    assert first_result.status == "accepted"
    assert second_result.status == "pending"
    assert len(route.calls) == 1


def test_required_has_no_legacy_fallback_when_route_is_absent(tmp_path: Path) -> None:
    config = _config(tmp_path, policy="acp_required")
    _seed(config)
    envelope = submit_command(
        config,
        _request("request-required"),
        acp_prompt_router=lambda _worker: None,
        acp_required=True,
    )
    assert envelope.status == "backend_unavailable"
    assert get_command_request(
        config.db_path,
        config.host_id,
        "request-required",
    ) is None


def test_new_and_resumed_endpoint_session_invariants(tmp_path: Path) -> None:
    config = _config(tmp_path)
    invalid_new = _endpoint()
    invalid_new["session"] = {"mode": "new", "id": "already-bound"}
    with pytest.raises(AcpCoordinatorError, match="already has"):
        _parse_endpoint(config, _binding(), invalid_new)
    resumed = _endpoint()
    resumed["session"] = {"mode": "resume", "id": "session-private"}
    assert _parse_endpoint(config, _binding(), resumed).session_id == "session-private"
    missing = _endpoint()
    missing["session"] = {"mode": "resume"}
    with pytest.raises(AcpCoordinatorError, match="session id"):
        _parse_endpoint(config, _binding(), missing)


def test_reconnect_remints_endpoint_instead_of_replaying_attach_ticket(tmp_path: Path) -> None:
    config = _config(tmp_path)
    assert config.db_path is not None
    init_store(config.db_path)
    upsert_worker_bindings(config.db_path, [_binding()])
    minted: list[str] = []
    generation = [42]
    runtimes: list[Any] = []

    class EndpointClient:
        def agent_acp_endpoint(self, _target: Any, *, timeout: float) -> Any:
            value = _endpoint(generation=generation[0])
            ticket = f"one-shot-private-ticket-{len(minted) + 1}"
            value["endpoint"]["args"][6] = ticket
            minted.append(ticket)
            return value

        def agent_acp_status(self, _target: Any, *, timeout: float) -> Any:
            return _status(generation=generation[0])

        def close(self) -> None:
            return None

    class Runtime:
        def __init__(self, _client: Any, **kwargs: Any) -> None:
            self._binding = kwargs["binding"]
            self.stopped = False
            self.prompt_calls = 0
            runtimes.append(self)

        def start(self) -> None:
            return None

        def stop(self, *, timeout: float) -> None:
            self.stopped = True

        def status(self) -> Any:
            return SimpleNamespace(
                healthy=not self.stopped,
                failure_type=None,
                updates_ingested=0,
                permissions_ingested=0,
                permissions_selected=0,
                permissions_cancelled=0,
                invalid_permission_selections=0,
                prompts_started=0,
                prompts_completed=0,
                prompts_failed=0,
                cancellation_requests=0,
            )

        def submit_prompt(self, *_args: Any, **_kwargs: Any) -> None:
            self.prompt_calls += 1
            return None

    coordinator = AcpRuntimeCoordinator(
        config,
        threading.Event(),
        endpoint_client_factory=lambda _config: EndpointClient(),
        client_factory=lambda *_args, **_kwargs: object(),
        runtime_factory=Runtime,
        reconcile_interval=60.0,
    ).start()
    try:
        assert minted == ["one-shot-private-ticket-1"]
        coordinator._reconcile_worker("worker-1", strict=True)
        worker = Worker(
            id="worker-1",
            name="worker",
            status="idle",
            fingerprint="worker-fingerprint",
        )
        route = coordinator.prompt_route(worker)
        assert route is not None
        route.prompt("hello", producer_turn_id="producer", timeout=1.0)
        assert minted == ["one-shot-private-ticket-1"]
        assert runtimes[0].prompt_calls == 1
        generation[0] = 43
        with pytest.raises(AcpCoordinatorError, match="generation lease"):
            route.prompt("stale", producer_turn_id="producer-2", timeout=1.0)
        assert runtimes[0].prompt_calls == 1
        coordinator._reconcile_worker("worker-1", strict=True)
        assert minted == [
            "one-shot-private-ticket-1",
            "one-shot-private-ticket-2",
        ]
    finally:
        coordinator.stop()


def test_preferred_legacy_scheduler_excludes_acp_owned_worker(tmp_path: Path) -> None:
    config = _config(tmp_path)
    assert config.db_path is not None
    init_store(config.db_path)
    upsert_worker_bindings(config.db_path, [_binding()])
    read = threading.Event()

    def reader(*_args: Any, **_kwargs: Any) -> TurnRefreshResult:
        read.set()
        return TurnRefreshResult("updated", 1)

    scheduler = TurnIngestionScheduler(
        config,
        refresh_interval_seconds=0.05,
        max_workers=1,
        reader=reader,
    )
    scheduler.set_worker_exclusion(
        lambda worker_id, fingerprint: (
            worker_id == "worker-1" and fingerprint == "worker-fingerprint"
        )
    )
    scheduler.start()
    try:
        assert not read.wait(0.2)
    finally:
        scheduler.stop(flush_timeout_seconds=1.0)


def test_required_zero_workers_is_idle_healthy_then_new_unowned_worker_degrades(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path, policy="acp_required")
    assert config.db_path is not None
    init_store(config.db_path)

    class RejectingClient:
        def agent_acp_endpoint(self, _target: Any, *, timeout: float) -> Any:
            raise AcpCoordinatorError("ACP ownership required")

        def close(self) -> None:
            return None

    coordinator = AcpRuntimeCoordinator(
        config,
        threading.Event(),
        endpoint_client_factory=lambda _config: RejectingClient(),
        reconcile_interval=60.0,
    ).start()
    try:
        assert coordinator.status()["healthy"] is True
        upsert_worker_bindings(config.db_path, [_binding()])
        coordinator._reconcile(strict=False)
        health = coordinator.status()
        assert health["healthy"] is False
        assert health["failure_type"] == "AcpCoordinatorError"
    finally:
        coordinator.stop()
