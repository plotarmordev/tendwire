"""End-to-end durable ACP permission decision bridge tests."""

from __future__ import annotations

import json
import sqlite3
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest
import tendwire.store.sqlite as store_sqlite
import tendwire.command_submission as command_submission
from tendwire.backends.acp_permissions import AcpPermissionBroker
from tendwire.backends.acp_protocol import (
    PermissionOption,
    PermissionOptionKind,
    PermissionRequest,
    SessionResult,
)
from tendwire.backends.acp_runtime import AcpRuntime, SessionOpenMode
from tendwire.command_submission import submit_command
from tendwire.core.models import Worker
from tendwire.daemon import TendwireDaemon
from tendwire.store.sqlite import (
    expire_worker_bindings,
    get_command_request,
    list_worker_bindings,
    pending_payload_from_store,
    upsert_worker_bindings,
)

from tests.test_answer_decision import _answer_request
from tests.test_command_submission import _binding, _config, _seed
from tests.test_acp_runtime import FakeClient, FakeIngestor


class _Router:
    def __init__(self, broker: AcpPermissionBroker) -> None:
        self.broker = broker

    def owns_permission_decision(self, decision: Any) -> bool:
        return self.broker.owns(decision)

    def answer_permission_decision(self, decision: Any, *, timeout: float) -> None:
        self.broker.answer(decision, timeout=timeout)


def _permission(session_id: str) -> PermissionRequest:
    options = (
        PermissionOption(
            "private-allow-id",
            "Allow once",
            PermissionOptionKind.ALLOW_ONCE,
            {},
        ),
        PermissionOption(
            "private-reject-id",
            "Reject",
            PermissionOptionKind.REJECT_ONCE,
            {},
        ),
    )
    return PermissionRequest(
        71,
        session_id,
        {
            "toolCallId": "private-tool-id",
            "title": "Run tests",
            "rawInput": "/secret",
        },
        options,
        {"private": "metadata"},
        {},
    )


def _wait_pending(config: Any, worker_id: str) -> dict[str, Any]:
    assert config.db_path is not None
    deadline = time.monotonic() + 2
    while time.monotonic() < deadline:
        payload = pending_payload_from_store(config.db_path, config.host_id)
        rows = [
            row
            for row in payload["pending_interactions"]
            if row["worker_id"] == worker_id
        ]
        if rows:
            return rows[0]
        time.sleep(0.01)
    raise AssertionError("permission overlay was not published")


def _setup(tmp_path: Path) -> tuple[Any, Worker, str, AcpPermissionBroker]:
    config = _config(tmp_path)
    worker = Worker(id="w-1", name="Alpha", status="active")
    continuity = _binding(worker)
    _seed(config, [worker], [continuity])
    session_id = "private-session-id"
    acp_binding = replace(
        continuity,
        backend="acp",
        turn_target_kind="acp_session_id",
        turn_target_value=session_id,
        private_fingerprint="",
    )
    assert config.db_path is not None
    upsert_worker_bindings(config.db_path, [acp_binding])
    return (
        config,
        worker,
        session_id,
        AcpPermissionBroker(
            config,
            worker_id=worker.id,
            worker_fingerprint=worker.fingerprint,
            generation="42",
            timeout=2,
        ),
    )


def test_permission_bridge_is_private_durable_and_frame_acknowledged(
    tmp_path: Path,
) -> None:
    config, worker, session_id, broker = _setup(tmp_path)
    assert config.db_path is not None
    acp_binding = list_worker_bindings(
        config.db_path, config.host_id, backend="acp"
    )[0]
    client = FakeClient()
    client.restored_session_result = SessionResult(session_id, None, (), {})
    runtime = AcpRuntime(
        client,
        config=config,
        binding=acp_binding,
        cwd=tmp_path,
        session_mode=SessionOpenMode.LOAD,
        session_id=session_id,
        stream_generation="42",
        permission_callback=broker,
        ingestor=FakeIngestor(session_id),
        poll_timeout=0.01,
        stop_timeout=1,
    )
    runtime.start()
    try:
        client.permissions.put(_permission(session_id))
        pending = _wait_pending(config, worker.id)
        serialized = json.dumps(pending, sort_keys=True)
        assert pending["question"] == "Run tests"
        assert pending["meta"]["decision"]["options"] == [
            {"ref": "1", "label": "Allow once (allow_once)"},
            {"ref": "2", "label": "Reject (reject_once)"},
        ]
        for secret in (
            "private-allow-id",
            "private-reject-id",
            "private-tool-id",
            session_id,
            "/secret",
        ):
            assert secret not in serialized

        result = submit_command(
            config,
            _answer_request(
                pending["meta"]["decision"]["decision_ref"],
                selection={"option_refs": ["1"]},
            ),
            acp_permission_router=_Router(broker),
        )
        assert result.ok is True
        assert client.permission_responses == [(71, "private-allow-id", False)]
        receipt = get_command_request(
            config.db_path, config.host_id, "decision-request-1"
        )
        assert receipt is not None and receipt["state"] == "accepted"
    finally:
        broker.close()
        runtime.stop()


def test_acp_permission_never_falls_back_to_legacy_socket(tmp_path: Path) -> None:
    config, worker, session_id, broker = _setup(tmp_path)
    thread = threading.Thread(target=lambda: broker(_permission(session_id)))
    thread.start()
    pending = _wait_pending(config, worker.id)
    assert config.db_path is not None
    assert expire_worker_bindings(
        config.db_path,
        config.host_id,
        backend="acp",
        private_fingerprints=[
            row.private_fingerprint
            for row in list_worker_bindings(
                config.db_path, config.host_id, backend="acp"
            )
        ],
        reason="test_retired_before_answer",
    ) == 1
    socket_calls: list[bool] = []
    result = submit_command(
        config,
        _answer_request(pending["meta"]["decision"]["decision_ref"]),
        socket_client_factory=lambda _config: socket_calls.append(True),
        acp_permission_router=None,
    )
    assert result.ok is False
    assert result.disposition == "no_receipt"
    assert socket_calls == []
    broker.close()
    thread.join(timeout=2)


def test_concurrent_answers_write_exactly_one_permission_response(
    tmp_path: Path,
) -> None:
    config, worker, session_id, broker = _setup(tmp_path)
    selections: list[Any] = []

    def adapter_side() -> None:
        selected = broker(_permission(session_id))
        selections.append(selected)
        assert selected is not None
        selected.response_written()

    adapter = threading.Thread(target=adapter_side)
    adapter.start()
    pending = _wait_pending(config, worker.id)
    decision_ref = pending["meta"]["decision"]["decision_ref"]
    router = _Router(broker)
    requests = [
        _answer_request(decision_ref, request_id=f"concurrent-{index}")
        for index in range(2)
    ]
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(
            pool.map(
                lambda request: submit_command(
                    config,
                    request,
                    acp_permission_router=router,
                ),
                requests,
            )
        )
    adapter.join(timeout=2)
    assert not adapter.is_alive()
    assert sum(result.ok is True for result in results) == 1
    assert len(selections) == 1
    assert selections[0].option_id in {
        "private-allow-id",
        "private-reject-id",
    }


def test_broker_stop_cancels_waiter_and_closes_public_overlay(tmp_path: Path) -> None:
    config, worker, session_id, broker = _setup(tmp_path)
    outcomes: list[Any] = []
    thread = threading.Thread(
        target=lambda: outcomes.append(broker(_permission(session_id)))
    )
    thread.start()
    _wait_pending(config, worker.id)
    broker.close()
    thread.join(timeout=2)
    assert not thread.is_alive()
    assert outcomes == [None]
    assert config.db_path is not None
    payload = pending_payload_from_store(config.db_path, config.host_id)
    assert all(
        row["worker_id"] != worker.id
        for row in payload["pending_interactions"]
    )


def test_v27_provenance_migration_preserves_stale_pending_state(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "v26.db"
    with sqlite3.connect(db_path) as conn:
        store_sqlite._run_migrations(conn, target_version=26)
        conn.execute(
            """
            INSERT INTO backend_pending (
                host_id, worker_id, payload_json, observed_at,
                revision_digest, choice_routes_json,
                binding_private_fingerprint, observed_turn_target_value,
                observation_state, freshness, updated_at
            ) VALUES ('host', 'worker', '{}', '2026-01-01T00:00:00+00:00',
                      '', '{}', '', '', 'failed', 'stale',
                      '2026-01-01T00:00:00+00:00')
            """
        )
        conn.commit()
    store_sqlite.init_store(db_path)
    with sqlite3.connect(db_path) as conn:
        assert conn.execute("PRAGMA user_version").fetchone() == (27,)
        assert conn.execute(
            "SELECT freshness, route_kind FROM backend_pending"
        ).fetchone() == ("stale", "legacy")


def test_shadow_daemon_routes_permission_answers_without_enabling_acp_prompts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = replace(_config(tmp_path), agent_event_source="acp_shadow")

    class Runtime:
        def answer_permission_decision(self, _decision: Any, *, timeout: float) -> None:
            del timeout

    runtime = Runtime()
    daemon = TendwireDaemon(config)
    daemon._acp_runtime = runtime
    captured: dict[str, Any] = {}

    def submit(_config: Any, _payload: Any, **kwargs: Any) -> str:
        captured.update(kwargs)
        return "routed"

    monkeypatch.setattr(command_submission, "submit_command", submit)
    assert daemon.submit_command({"schema_version": 1, "action": "noop"}) == "routed"
    assert captured["acp_permission_router"] is runtime
    assert captured["acp_prompt_router"] is None
