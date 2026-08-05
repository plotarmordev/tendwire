"""End-to-end durable ACP permission decision bridge tests."""

from __future__ import annotations

import json
import sqlite3
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
import tendwire.command_submission as command_submission
from tendwire.backends.acp_permissions import (
    AcpPermissionBroker,
    AcpPermissionBrokerError,
)
from tendwire.backends.acp_protocol import (
    PermissionOption,
    PermissionRequest,
)
from tendwire.backends.acp_runtime import AcpWorkerSession, SessionOpenMode
from tendwire.command_submission import submit_command
from tendwire.config import Config
from tendwire.core.models import BackendHealth, Snapshot, Worker, WorkerBinding
from tendwire.daemon import TendwireDaemon
from tendwire.store.pending import (
    backend_pending_decision_terminal_effect,
    claim_backend_pending_decision,
    pending_payload_from_store,
    start_backend_pending_decision_send,
)
from tendwire.store.db import write_transaction
from tendwire.store.projection import (
    expire_worker_bindings,
    list_worker_bindings,
    save_snapshot,
)
from tendwire.store.receipts import get_command_request
from tendwire.store.retention import RetentionPolicy, run_retention_cycle
from tendwire.store.schema import init_store

from tests.test_acp_runtime import FakeClient, FakeIngestor
from .store_helpers import (
    apply_test_backend_pending,
    upsert_test_worker_bindings as upsert_worker_bindings,
)


def _config(tmp_path: Path) -> Config:
    return Config(
        host_id="cmd-host",
        data_dir=tmp_path,
        db_path=tmp_path / "commands.db",
    )


def _binding(worker: Worker) -> WorkerBinding:
    return WorkerBinding(
        host_id="cmd-host",
        worker_id=worker.id,
        worker_fingerprint=worker.fingerprint,
        backend="herdr",
        target_kind="agent_id",
        target_value="agent-secret",
        turn_target_kind=None,
        turn_target_value=None,
        sendable=True,
        reason=None,
        observed_at="2026-01-01T00:00:00+00:00",
        private_fingerprint="private-secret",
    )


def _seed(
    config: Config,
    workers: list[Worker],
    bindings: list[WorkerBinding],
) -> None:
    assert config.db_path is not None
    init_store(config.db_path)
    save_snapshot(
        config.db_path,
        Snapshot(
            host_id=config.host_id,
            updated_at="2026-01-01T00:00:00+00:00",
            workers=workers,
            backend_health=[
                BackendHealth(
                    name="herdr",
                    status="healthy",
                    outcome="healthy_non_empty",
                    observed_at="2026-01-01T00:00:00+00:00",
                    counts={"workers": len(workers)},
                )
            ],
        ),
    )
    upsert_worker_bindings(config.db_path, bindings)


def _answer_request(
    decision_ref: str,
    *,
    request_id: str = "decision-request-1",
    worker_id: str = "w-1",
    selection: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "action": "answer_decision",
        "request_id": request_id,
        "target": {"worker_id": worker_id},
        "params": {
            "decision_ref": decision_ref,
            "selection": selection or {"option_refs": ["2"]},
        },
    }


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
            "allow_once",
        ),
        PermissionOption(
            "private-reject-id",
            "Reject",
            "reject_once",
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


def _publish_test_choice(config: Config, worker_id: str, question: str) -> str:
    assert config.db_path is not None
    binding = list_worker_bindings(
        config.db_path, config.host_id, backend="acp"
    )[0]
    assert apply_test_backend_pending(
        config.db_path,
        config.host_id,
        worker_id,
        {
            "question": question,
            "kind": "choice",
            "choices": [{"label": "Allow"}, {"label": "Reject"}],
        },
        expected_binding=binding,
    )
    _wait_pending(config, worker_id)
    with sqlite3.connect(config.db_path) as conn:
        return str(
            conn.execute(
                """SELECT decision_ref FROM backend_pending
                WHERE host_id=? AND worker_id=? AND state='open'""",
                (config.host_id, worker_id),
            ).fetchone()[0]
        )


def _setup(tmp_path: Path) -> tuple[Any, Worker, str, AcpPermissionBroker]:
    config = _config(tmp_path)
    worker = Worker(
        id="w-1",
        name="Alpha",
        status="active",
        meta={"stable_key": "wsk1_" + "a" * 64, "stable_key_version": 1},
    )
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


def test_expired_pending_claim_takeover_fences_old_token(
    tmp_path: Path,
) -> None:
    config, worker, _session_id, _broker = _setup(tmp_path)
    assert config.db_path is not None
    decision_ref = _publish_test_choice(config, worker.id, "Choose once")
    first = claim_backend_pending_decision(
        config.db_path,
        config.host_id,
        worker.id,
        decision_ref,
        {"text": "first"},
        observed_at="2026-08-05T00:00:00Z",
        claim_lease_seconds=10,
    )
    second = claim_backend_pending_decision(
        config.db_path,
        config.host_id,
        worker.id,
        decision_ref,
        {"text": "second"},
        observed_at="2026-08-05T00:00:11Z",
        claim_lease_seconds=10,
    )
    assert first.status == second.status == "claimed"
    assert first.claim_token and second.claim_token
    assert first.claim_token != second.claim_token
    assert start_backend_pending_decision_send(
        config.db_path,
        config.host_id,
        first.claim_token,
        observed_at="2026-08-05T00:00:12Z",
    ).status == "not_found"
    assert start_backend_pending_decision_send(
        config.db_path,
        config.host_id,
        second.claim_token,
        observed_at="2026-08-05T00:00:12Z",
    ).status == "started"


def test_send_started_pending_claim_is_never_reclaimed_and_late_claim_is_fenced(
    tmp_path: Path,
) -> None:
    config, worker, _session_id, _broker = _setup(tmp_path)
    assert config.db_path is not None
    decision_ref = _publish_test_choice(config, worker.id, "Choose once")
    claim = claim_backend_pending_decision(
        config.db_path,
        config.host_id,
        worker.id,
        decision_ref,
        {"text": "first"},
        observed_at="2026-08-05T00:00:00Z",
        claim_lease_seconds=10,
    )
    assert claim.claim_token
    assert start_backend_pending_decision_send(
        config.db_path,
        config.host_id,
        claim.claim_token,
        observed_at="2026-08-05T00:00:01Z",
    ).status == "started"
    assert claim_backend_pending_decision(
        config.db_path,
        config.host_id,
        worker.id,
        decision_ref,
        {"text": "second"},
        observed_at="2026-08-05T00:00:20Z",
    ).status == "already_claimed"

    other_ref = _publish_test_choice(config, worker.id, "Another choice")
    late = claim_backend_pending_decision(
        config.db_path,
        config.host_id,
        worker.id,
        other_ref,
        {"text": "late"},
        observed_at="2026-08-05T00:01:00Z",
        claim_lease_seconds=5,
    )
    assert late.claim_token
    assert start_backend_pending_decision_send(
        config.db_path,
        config.host_id,
        late.claim_token,
        observed_at="2026-08-05T00:01:06Z",
    ).status == "changed"


def test_new_pending_revision_after_expired_claim_can_be_claimed(
    tmp_path: Path,
) -> None:
    config, worker, _session_id, _broker = _setup(tmp_path)
    assert config.db_path is not None
    old_ref = _publish_test_choice(config, worker.id, "First revision")
    old = claim_backend_pending_decision(
        config.db_path,
        config.host_id,
        worker.id,
        old_ref,
        {"text": "old"},
        observed_at="2026-08-05T00:00:00Z",
        claim_lease_seconds=5,
    )
    assert old.status == "claimed"
    new_ref = _publish_test_choice(config, worker.id, "Second revision")
    new = claim_backend_pending_decision(
        config.db_path,
        config.host_id,
        worker.id,
        new_ref,
        {"text": "new"},
        observed_at="2026-08-05T00:00:06Z",
        claim_lease_seconds=5,
    )
    assert new.status == "claimed"
    assert new.claim_token and new.claim_token != old.claim_token


def test_older_pending_open_cannot_supersede_newer_observation(
    tmp_path: Path,
) -> None:
    config, worker, _session_id, _broker = _setup(tmp_path)
    assert config.db_path is not None
    binding = list_worker_bindings(
        config.db_path, config.host_id, backend="acp"
    )[0]
    assert apply_test_backend_pending(
        config.db_path,
        config.host_id,
        worker.id,
        {"question": "Newest question", "kind": "choice", "choices": []},
        expected_binding=binding,
        observed_at="2026-08-05T00:00:10Z",
    )
    before = pending_payload_from_store(config.db_path, config.host_id)
    assert not apply_test_backend_pending(
        config.db_path,
        config.host_id,
        worker.id,
        {"question": "Stale question", "kind": "choice", "choices": []},
        expected_binding=binding,
        observed_at="2026-08-05T00:00:09Z",
    )
    assert pending_payload_from_store(config.db_path, config.host_id) == before


def test_older_pending_close_cannot_close_newer_observation(tmp_path: Path) -> None:
    config, worker, _session_id, _broker = _setup(tmp_path)
    assert config.db_path is not None
    binding = list_worker_bindings(
        config.db_path, config.host_id, backend="acp"
    )[0]
    assert apply_test_backend_pending(
        config.db_path,
        config.host_id,
        worker.id,
        {"question": "Still current", "kind": "question", "choices": []},
        expected_binding=binding,
        observed_at="2026-08-05T00:00:10Z",
    )
    assert not apply_test_backend_pending(
        config.db_path,
        config.host_id,
        worker.id,
        None,
        expected_binding=binding,
        observed_at="2026-08-05T00:00:09Z",
    )
    assert pending_payload_from_store(config.db_path, config.host_id)[
        "pending_interactions"
    ][0]["question"] == "Still current"


def test_retention_deletes_settled_pending_claim_before_prompt(tmp_path: Path) -> None:
    config, worker, _session_id, _broker = _setup(tmp_path)
    assert config.db_path is not None
    binding = list_worker_bindings(
        config.db_path, config.host_id, backend="acp"
    )[0]
    assert apply_test_backend_pending(
        config.db_path,
        config.host_id,
        worker.id,
        {"question": "Historical choice", "kind": "choice", "choices": []},
        expected_binding=binding,
        observed_at="2026-08-05T00:00:00Z",
    )
    with sqlite3.connect(config.db_path) as conn:
        decision_ref = str(
            conn.execute(
                "SELECT decision_ref FROM backend_pending WHERE state='open'"
            ).fetchone()[0]
        )
    claim = claim_backend_pending_decision(
        config.db_path,
        config.host_id,
        worker.id,
        decision_ref,
        {"text": "Allow"},
        observed_at="2026-08-05T00:00:01Z",
    )
    assert claim.claim_token
    assert start_backend_pending_decision_send(
        config.db_path,
        config.host_id,
        claim.claim_token,
        observed_at="2026-08-05T00:00:02Z",
    ).status == "started"
    effect = backend_pending_decision_terminal_effect(
        host_id=config.host_id, claim_token=claim.claim_token, accepted=True,
    )
    with write_transaction(config.db_path) as conn:
        effect(conn)
    result = run_retention_cycle(
        config.db_path,
        policy=RetentionPolicy(command_retention_days=1),
        now="2026-08-07T00:00:04Z",
    )
    assert result["pending_claims"] == 1
    assert result["pending_prompts"] == 1
    with sqlite3.connect(config.db_path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM backend_pending_claims").fetchone() == (0,)
        assert conn.execute("SELECT COUNT(*) FROM backend_pending").fetchone() == (0,)
        assert conn.execute("SELECT COUNT(*) FROM pending_interactions").fetchone() == (0,)


def test_retention_preserves_send_started_claim_while_prompt_is_open(
    tmp_path: Path,
) -> None:
    config, worker, _session_id, _broker = _setup(tmp_path)
    assert config.db_path is not None
    binding = list_worker_bindings(
        config.db_path, config.host_id, backend="acp"
    )[0]
    assert apply_test_backend_pending(
        config.db_path,
        config.host_id,
        worker.id,
        {"question": "Long-running send", "kind": "choice", "choices": []},
        expected_binding=binding,
        observed_at="2026-08-01T00:00:00Z",
    )
    with sqlite3.connect(config.db_path) as conn:
        old_ref = str(
            conn.execute(
                "SELECT decision_ref FROM backend_pending WHERE state='open'"
            ).fetchone()[0]
        )
    claim = claim_backend_pending_decision(
        config.db_path,
        config.host_id,
        worker.id,
        old_ref,
        {"text": "Allow"},
        observed_at="2026-08-01T00:00:01Z",
    )
    assert claim.claim_token
    assert start_backend_pending_decision_send(
        config.db_path,
        config.host_id,
        claim.claim_token,
        observed_at="2026-08-01T00:00:02Z",
    ).status == "started"

    retained = run_retention_cycle(
        config.db_path,
        policy=RetentionPolicy(command_retention_days=1),
        now="2026-08-05T00:00:00Z",
    )
    assert retained["pending_claims"] == retained["pending_prompts"] == 0
    assert claim_backend_pending_decision(
        config.db_path,
        config.host_id,
        worker.id,
        old_ref,
        {"text": "Again"},
        observed_at="2026-08-05T00:00:01Z",
    ).status == "already_claimed"

    assert apply_test_backend_pending(
        config.db_path,
        config.host_id,
        worker.id,
        {"question": "New revision", "kind": "choice", "choices": []},
        expected_binding=binding,
        observed_at="2026-08-05T00:00:02Z",
    )
    cleaned = run_retention_cycle(
        config.db_path,
        policy=RetentionPolicy(command_retention_days=1),
        now="2026-08-07T00:00:03Z",
    )
    assert cleaned["pending_claims"] == 1
    assert cleaned["pending_prompts"] == 1
    with sqlite3.connect(config.db_path) as conn:
        assert conn.execute(
            "SELECT COUNT(*) FROM backend_pending_claims WHERE decision_ref=?",
            (old_ref,),
        ).fetchone() == (0,)
        assert conn.execute(
            "SELECT COUNT(*) FROM backend_pending WHERE decision_ref=?", (old_ref,)
        ).fetchone() == (0,)


def test_permission_bridge_is_private_durable_and_frame_acknowledged(
    tmp_path: Path,
) -> None:
    config, worker, session_id, broker = _setup(tmp_path)
    assert config.db_path is not None
    acp_binding = list_worker_bindings(
        config.db_path, config.host_id, backend="acp"
    )[0]
    client = FakeClient()
    client.restored_session_result = session_id
    runtime = AcpWorkerSession(
        client,
        config=config,
        binding=acp_binding,
        cwd=tmp_path,
        session_mode=SessionOpenMode.LOAD,
        session_id=session_id,
        stream_generation="42",
        permission_callback=broker,
        ingestor_factory=lambda *_args, **_kwargs: FakeIngestor(session_id),
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
    result = submit_command(
        config,
        _answer_request(pending["meta"]["decision"]["decision_ref"]),
        acp_permission_router=None,
    )
    assert result.ok is False
    assert result.disposition == "no_receipt"
    broker.close()
    thread.join(timeout=2)


def test_empty_permission_options_fail_closed_without_public_overlay(
    tmp_path: Path,
) -> None:
    config, worker, session_id, broker = _setup(tmp_path)
    assert broker(replace(_permission(session_id), options=())) is None
    assert config.db_path is not None
    payload = pending_payload_from_store(config.db_path, config.host_id)
    assert all(
        row["worker_id"] != worker.id for row in payload["pending_interactions"]
    )


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


def test_late_permission_frame_completion_retires_uncertain_overlay(
    tmp_path: Path,
) -> None:
    config, worker, session_id, broker = _setup(tmp_path)
    selections: list[Any] = []

    def adapter_side() -> None:
        selections.append(broker(_permission(session_id)))

    adapter = threading.Thread(target=adapter_side)
    adapter.start()
    pending = _wait_pending(config, worker.id)
    router = _Router(broker)
    result = submit_command(
        replace(config, acp_request_timeout_seconds=0.01),
        _answer_request(pending["meta"]["decision"]["decision_ref"]),
        acp_permission_router=router,
    )
    adapter.join(timeout=2)
    assert not adapter.is_alive()
    assert len(selections) == 1 and selections[0] is not None
    assert result.status == "request_state_uncertain"

    # The JSON-RPC writer completes after the durable command deadline.  The
    # original receipt remains uncertain and cannot be replayed, while the
    # now-resolved permission prompt is removed instead of becoming a forever
    # visible, forever-unanswerable overlay.
    selections[0].response_written()
    assert config.db_path is not None
    payload = pending_payload_from_store(config.db_path, config.host_id)
    assert all(
        row["worker_id"] != worker.id
        for row in payload["pending_interactions"]
    )
    receipt = get_command_request(
        config.db_path,
        config.host_id,
        "decision-request-1",
    )
    assert receipt is not None and receipt["state"] == "uncertain"
    broker.close()


def test_answer_timeout_before_adapter_consumes_selection_keeps_ordinal(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config, worker, session_id, broker = _setup(tmp_path)
    broker.timeout = 0.2
    selections: list[Any] = []
    adapter = threading.Thread(
        target=lambda: selections.append(broker(_permission(session_id)))
    )
    adapter.start()
    pending = _wait_pending(config, worker.id)
    assert config.db_path is not None
    binding = list_worker_bindings(
        config.db_path, config.host_id, backend="acp"
    )[0]

    # Suppress the answer notification so its 0.1s deadline expires before
    # the adapter's 0.2s wait. The selected ordinal must remain immutable while
    # the late adapter consumes it and completes the response frame.
    monkeypatch.setattr(broker._condition, "notify_all", lambda: None)
    decision = SimpleNamespace(
        worker_id=worker.id,
        worker_fingerprint=worker.fingerprint,
        binding_private_fingerprint=binding.private_fingerprint,
        turn_target_value=session_id,
        decision_ref=pending["meta"]["decision"]["decision_ref"],
        text=None,
        option_refs=("1",),
    )
    with pytest.raises(AcpPermissionBrokerError, match="response state is uncertain"):
        broker.answer(decision, timeout=0.01)
    adapter.join(timeout=1)
    assert len(selections) == 1 and selections[0] is not None
    assert selections[0].option_id == "private-allow-id"
    selections[0].response_written()
    assert not pending_payload_from_store(config.db_path, config.host_id)[
        "pending_interactions"
    ]


def test_failed_permission_frame_retires_overlay_without_retry(
    tmp_path: Path,
) -> None:
    config, worker, session_id, broker = _setup(tmp_path)

    def adapter_side() -> None:
        selected = broker(_permission(session_id))
        assert selected is not None
        selected.response_failed(OSError("partial private frame"))

    adapter = threading.Thread(target=adapter_side)
    adapter.start()
    pending = _wait_pending(config, worker.id)
    result = submit_command(
        config,
        _answer_request(pending["meta"]["decision"]["decision_ref"]),
        acp_permission_router=_Router(broker),
    )
    adapter.join(timeout=2)
    assert not adapter.is_alive()
    assert result.status == "request_state_uncertain"
    assert "private frame" not in json.dumps(result.to_dict())
    assert config.db_path is not None
    payload = pending_payload_from_store(config.db_path, config.host_id)
    assert all(
        row["worker_id"] != worker.id
        for row in payload["pending_interactions"]
    )
    broker.close()




def test_daemon_routes_acp_permission_answers_without_a_prompt_route(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config(tmp_path)

    class Runtime:
        def answer_permission_decision(self, _decision: Any, *, timeout: float) -> None:
            del timeout

    runtime = Runtime()
    daemon = TendwireDaemon(config)
    daemon._acp_supervisor = runtime
    captured: dict[str, Any] = {}

    def submit(_config: Any, _payload: Any, **kwargs: Any) -> str:
        captured.update(kwargs)
        return "routed"

    monkeypatch.setattr(command_submission, "submit_command", submit)
    assert daemon.submit_command({"schema_version": 1, "action": "noop"}) == "routed"
    assert captured["acp_permission_router"] is runtime
    assert captured["acp_prompt_router"] is None
