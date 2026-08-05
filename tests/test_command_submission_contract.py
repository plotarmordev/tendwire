"""Black-box command submission and durable receipt characterization."""

from __future__ import annotations

import json
import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import replace
from pathlib import Path
from typing import Any, Callable, Mapping

import pytest

import tendwire.command_submission as command_submission
from tendwire.command_submission import submit_command
from tendwire.config import Config
from tendwire.core.commands import (
    DISPOSITION_IN_PROGRESS,
    STATUS_PENDING,
    CommandEnvelope,
    CommandRequest,
    build_canonical_mutation,
    error_value,
    parse_command_request,
)
from tendwire.core.models import BackendHealth, Snapshot, Worker, WorkerBinding
from tendwire.core.turns import PendingObservation
from tendwire.store.pending import pending_payload_from_store
from tendwire.store.projection import (
    latest_snapshot,
    list_worker_bindings,
    save_snapshot,
)
from tendwire.store.receipts import (
    command_reservation_is_live,
    envelope_to_receipt_json,
    get_command_request,
    mark_command_send_started,
    reserve_command_request,
)
from tendwire.store.schema import init_store
from tendwire.store.turns import apply_turn_refresh

from .store_helpers import upsert_test_worker_bindings


_APPROVED_HEALTH_AUTHORITY = pytest.mark.xfail(
    strict=True,
    reason="approved: a prepared ACP generation, not persisted Herdr health, authorizes send",
)


def _config(tmp_path: Path) -> Config:
    return Config(
        host_id="command-contract-host",
        data_dir=tmp_path,
        db_path=tmp_path / "commands.db",
    )


def _continuity_binding(worker: Worker) -> WorkerBinding:
    return WorkerBinding(
        host_id="command-contract-host",
        worker_id=worker.id,
        worker_fingerprint=worker.fingerprint,
        backend="herdr",
        target_kind="pane_id",
        target_value="private-pane",
        turn_target_kind="pane_id",
        turn_target_value="private-pane",
        sendable=True,
        observed_at="2026-08-05T00:00:00.000000Z",
        private_fingerprint="private-continuity-binding",
    )


def _seed(
    config: Config,
    *,
    health: str = "healthy",
) -> tuple[Worker, WorkerBinding]:
    assert config.db_path is not None
    init_store(config.db_path)
    worker = Worker(
        id="worker-1",
        name="Coda",
        status="idle",
        fingerprint="worker-public-fingerprint",
        meta={
            "stable_key": "wsk1_" + "a" * 64,
            "stable_key_version": 1,
        },
    )
    continuity = _continuity_binding(worker)
    saved = save_snapshot(
        config.db_path,
        Snapshot(
            host_id=config.host_id,
            updated_at="2026-08-05T00:00:00.000000Z",
            workers=[worker],
            backend_health=[
                BackendHealth(
                    name="herdr",
                    status=health,
                    outcome="healthy_non_empty" if health == "healthy" else "failed",
                )
            ],
        ),
        worker_bindings=[continuity],
        binding_backend="herdr",
    )
    public_worker = saved.workers[0]
    acp_binding = replace(
        continuity,
        worker_fingerprint=public_worker.fingerprint,
        backend="acp",
        target_kind="acp_session_id",
        target_value="private-session",
        turn_target_kind="acp_session_id",
        turn_target_value="private-session",
        private_fingerprint="private-acp-binding",
    )
    upsert_test_worker_bindings(config.db_path, [acp_binding])
    stored = latest_snapshot(config.db_path, config.host_id)
    assert stored is not None
    public_worker = save_snapshot(
        config.db_path,
        stored,
        worker_bindings=[acp_binding],
        binding_backend="acp",
    ).workers[0]
    binding = list_worker_bindings(
        config.db_path,
        config.host_id,
        backend="acp",
    )[0]
    return public_worker, binding


def _instruction(
    request_id: str,
    *,
    target: Mapping[str, Any] | None = None,
    response_schema_version: int | None = None,
) -> dict[str, Any]:
    request: dict[str, Any] = {
        "schema_version": 1,
        "action": "send_instruction",
        "request_id": request_id,
        "dry_run": False,
        "target": dict(target or {"worker_id": "worker-1"}),
        "instruction": {"text": "do the work"},
    }
    if response_schema_version is not None:
        request["response_schema_version"] = response_schema_version
    return request


def _answer(
    request_id: str,
    decision_ref: str,
) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "action": "answer_decision",
        "request_id": request_id,
        "dry_run": False,
        "target": {"worker_id": "worker-1"},
        "params": {
            "decision_ref": decision_ref,
            "selection": {"option_refs": ["1"]},
        },
    }


class _Route:
    def __init__(
        self,
        *,
        failure: str | None = None,
        steering: bool = False,
        steering_outcome: str = "injected",
    ) -> None:
        self.failure = failure
        self.steering = steering
        self.steering_outcome = steering_outcome
        self.prepare_count = 0
        self.send_count = 0

    @property
    def binding_fingerprint(self) -> str:
        if self.failure == "binding":
            raise RuntimeError("private generation vanished")
        return "private-acp-binding"

    @property
    def supports_steering(self) -> bool:
        return self.steering

    @contextmanager
    def prepare(self):
        self.prepare_count += 1
        if self.failure == "prepare":
            raise RuntimeError("private generation is stale")
        yield self

    def _send(self, on_send_start: Callable[[], None] | None) -> None:
        self.send_count += 1
        if self.failure == "before_send_start":
            raise RuntimeError("private generation lost before frame")
        if on_send_start is not None:
            on_send_start()
        if self.failure == "after_send_start":
            raise RuntimeError("private generation lost after frame start")

    def prompt(
        self,
        _text: str,
        *,
        producer_turn_id: str,
        timeout: float,
        on_send_start: Callable[[], None] | None = None,
    ) -> None:
        assert producer_turn_id.startswith("twsub1.")
        assert timeout > 0
        self._send(on_send_start)

    def steer(
        self,
        _text: str,
        *,
        producer_turn_id: str,
        timeout: float,
        on_send_start: Callable[[], None] | None = None,
    ) -> str:
        assert producer_turn_id.startswith("twsub1.")
        assert timeout > 0
        self._send(on_send_start)
        return self.steering_outcome


def _receipt_counts(config: Config) -> tuple[int, int, int]:
    assert config.db_path is not None
    with sqlite3.connect(config.db_path) as conn:
        return tuple(
            int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
            for table in ("command_receipts", "turn_submissions", "agent_events")
        )


@_APPROVED_HEALTH_AUTHORITY
def test_prepared_acp_route_outranks_stale_unhealthy_snapshot_health(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    worker, _binding = _seed(config, health="failed")
    route = _Route()

    result = submit_command(
        config,
        _instruction("health-is-observation"),
        acp_prompt_router=lambda candidate: route if candidate == worker else None,
    )

    assert result.status == "accepted"
    assert result.disposition == "terminal_accepted"
    assert route.prepare_count == route.send_count == 1
    receipt = get_command_request(
        config.db_path,
        config.host_id,
        "health-is-observation",
    )
    assert receipt is not None and receipt["state"] == "accepted"


@pytest.mark.parametrize("failure", ["prepare", "binding"])
def test_failed_or_stale_generation_before_reservation_leaves_no_receipt(
    tmp_path: Path,
    failure: str,
) -> None:
    config = _config(tmp_path)
    worker, _binding = _seed(config)
    route = _Route(failure=failure)

    result = submit_command(
        config,
        _instruction(f"generation-{failure}"),
        acp_prompt_router=lambda candidate: route if candidate == worker else None,
    )

    assert result.status == "backend_unavailable"
    assert result.disposition == "no_receipt"
    assert route.send_count == 0
    assert _receipt_counts(config) == (0, 0, 0)


def test_generation_loss_before_transport_boundary_is_retryable(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    worker, _binding = _seed(config)
    failed = _Route(failure="before_send_start")
    request = _instruction("generation-before-boundary")

    first = submit_command(
        config,
        request,
        acp_prompt_router=lambda candidate: failed if candidate == worker else None,
    )

    assert first.status == "backend_unavailable"
    assert first.disposition == "no_receipt"
    receipt = get_command_request(
        config.db_path,
        config.host_id,
        "generation-before-boundary",
    )
    assert receipt is not None and receipt["state"] == "reserved"
    assert command_reservation_is_live(receipt) is False

    healthy = _Route()
    second = submit_command(
        config,
        request,
        acp_prompt_router=lambda candidate: healthy if candidate == worker else None,
    )
    assert second.status == "accepted"
    assert failed.send_count == healthy.send_count == 1


def test_generation_loss_after_transport_boundary_is_uncertain_and_never_retried(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    worker, _binding = _seed(config)
    route = _Route(failure="after_send_start")
    request = _instruction("generation-after-boundary")

    first = submit_command(
        config,
        request,
        acp_prompt_router=lambda candidate: route if candidate == worker else None,
    )
    second = submit_command(
        config,
        request,
        acp_prompt_router=lambda _candidate: route,
    )

    assert first.to_dict() == second.to_dict()
    assert first.status == "request_state_uncertain"
    assert first.disposition == "terminal_uncertain"
    assert route.send_count == 1
    receipt = get_command_request(
        config.db_path,
        config.host_id,
        "generation-after-boundary",
    )
    assert receipt is not None and receipt["state"] == "uncertain"


@pytest.mark.parametrize(
    ("route", "terminal_state"),
    [
        pytest.param(_Route(), "accepted", id="accepted"),
        pytest.param(
            _Route(failure="after_send_start"),
            "uncertain",
            id="uncertain",
        ),
        pytest.param(
            _Route(steering=True, steering_outcome="failed"),
            "rejected",
            id="rejected",
        ),
    ],
)
def test_terminal_receipt_replay_is_exact_and_store_stable(
    tmp_path: Path,
    route: _Route,
    terminal_state: str,
) -> None:
    config = _config(tmp_path)
    worker, _binding = _seed(config)
    request_id = f"terminal-{terminal_state}"
    request = _instruction(request_id)

    first = submit_command(
        config,
        request,
        acp_prompt_router=lambda candidate: route if candidate == worker else None,
    )
    stored_before = get_command_request(config.db_path, config.host_id, request_id)
    second = submit_command(
        config,
        request,
        acp_prompt_router=lambda _candidate: route,
    )
    stored_after = get_command_request(config.db_path, config.host_id, request_id)

    assert stored_before == stored_after
    assert stored_before is not None and stored_before["state"] == terminal_state
    assert envelope_to_receipt_json(first) == stored_before["result_json"]
    assert second.to_dict() == first.to_dict()
    assert route.send_count == 1


def _request_object(value: Mapping[str, Any]) -> CommandRequest:
    parsed, error = parse_command_request(json.dumps(dict(value)))
    assert error is None and parsed is not None
    return parsed


def _pending_envelope(request: CommandRequest) -> CommandEnvelope:
    return CommandEnvelope.from_result(
        request,
        ok=False,
        status=STATUS_PENDING,
        disposition=DISPOSITION_IN_PROGRESS,
        error=error_value(STATUS_PENDING, "request is already in progress"),
    )


@pytest.mark.parametrize("receipt_state", ["reserved", "send_started"])
def test_live_receipt_replay_returns_exact_stored_pending_result(
    tmp_path: Path,
    receipt_state: str,
) -> None:
    config = _config(tmp_path)
    worker, binding = _seed(config)
    request_data = _instruction(f"live-{receipt_state}")
    request = _request_object(request_data)
    canonical = build_canonical_mutation(request, public_worker_id=worker.id)
    pending = _pending_envelope(request)
    reserved = reserve_command_request(
        config.db_path,
        host_id=config.host_id,
        request_id=request.request_id or "",
        action=canonical.action,
        canonical_version=canonical.canonical_version,
        canonical_fingerprint=canonical.fingerprint,
        canonical_request_json=canonical.canonical_json,
        public_worker_id=canonical.public_worker_id,
        pending_result_json=envelope_to_receipt_json(pending),
    )
    if receipt_state == "send_started":
        started = mark_command_send_started(
            config.db_path,
            host_id=config.host_id,
            request_id=request.request_id or "",
            canonical_fingerprint=canonical.fingerprint,
            owner_token=str(reserved["owner_token"]),
            binding_fingerprint=binding.private_fingerprint,
            submission_worker=worker,
            instruction_text="do the work",
        )
        assert started["status"] == "send_started"
    stored = get_command_request(
        config.db_path,
        config.host_id,
        request.request_id or "",
    )
    route = _Route()

    result = submit_command(
        config,
        request_data,
        acp_prompt_router=lambda _candidate: route,
    )

    assert stored is not None and stored["state"] == receipt_state
    assert envelope_to_receipt_json(result) == stored["result_json"]
    assert route.prepare_count == route.send_count == 0


def test_expired_send_started_terminalizes_uncertain_once_without_acp(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    worker, binding = _seed(config)
    request_data = _instruction("expired-send-started")
    request = _request_object(request_data)
    canonical = build_canonical_mutation(request, public_worker_id=worker.id)
    pending = _pending_envelope(request)
    reserved = reserve_command_request(
        config.db_path,
        host_id=config.host_id,
        request_id=request.request_id or "",
        action=canonical.action,
        canonical_version=canonical.canonical_version,
        canonical_fingerprint=canonical.fingerprint,
        canonical_request_json=canonical.canonical_json,
        public_worker_id=canonical.public_worker_id,
        pending_result_json=envelope_to_receipt_json(pending),
        owner_lease_seconds=1,
        now="2026-01-01T00:00:00.000000Z",
    )
    started = mark_command_send_started(
        config.db_path,
        host_id=config.host_id,
        request_id=request.request_id or "",
        canonical_fingerprint=canonical.fingerprint,
        owner_token=str(reserved["owner_token"]),
        binding_fingerprint=binding.private_fingerprint,
        submission_worker=worker,
        instruction_text="do the work",
        now="2026-01-01T00:00:00.000000Z",
    )
    assert started["status"] == "send_started"
    route = _Route()

    first = submit_command(
        config,
        request_data,
        acp_prompt_router=lambda _candidate: route,
    )
    stored_after_first = get_command_request(
        config.db_path,
        config.host_id,
        request.request_id or "",
    )
    second = submit_command(
        config,
        request_data,
        acp_prompt_router=lambda _candidate: route,
    )
    stored_after_second = get_command_request(
        config.db_path,
        config.host_id,
        request.request_id or "",
    )

    assert first.status == "request_state_uncertain"
    assert first.disposition == "terminal_uncertain"
    assert second.to_dict() == first.to_dict()
    assert stored_after_first == stored_after_second
    assert stored_after_first is not None and stored_after_first["state"] == "uncertain"
    assert envelope_to_receipt_json(first) == stored_after_first["result_json"]
    assert route.prepare_count == route.send_count == 0


@pytest.mark.parametrize(
    "corruption",
    ["invalid_json", "wrong_schema", "status_mismatch", "pending_terminal"],
)
def test_corrupt_terminal_receipt_fails_closed_without_transport(
    tmp_path: Path,
    corruption: str,
) -> None:
    config = _config(tmp_path)
    worker, _binding = _seed(config)
    request_id = f"corrupt-{corruption}"
    request = _instruction(request_id)
    route = _Route()
    accepted = submit_command(
        config,
        request,
        acp_prompt_router=lambda candidate: route if candidate == worker else None,
    )
    assert accepted.status == "accepted"
    assert config.db_path is not None
    with sqlite3.connect(config.db_path) as conn:
        if corruption == "invalid_json":
            conn.execute(
                "UPDATE command_receipts SET result_json='not-json' WHERE request_id=?",
                (request_id,),
            )
        elif corruption == "wrong_schema":
            payload = accepted.to_dict()
            payload["schema_version"] = 1
            conn.execute(
                "UPDATE command_receipts SET result_json=? WHERE request_id=?",
                (json.dumps(payload), request_id),
            )
        elif corruption == "status_mismatch":
            conn.execute(
                "UPDATE command_receipts SET status='rejected' WHERE request_id=?",
                (request_id,),
            )
        else:
            pending = _pending_envelope(_request_object(request))
            conn.execute(
                "UPDATE command_receipts SET result_json=? WHERE request_id=?",
                (envelope_to_receipt_json(pending), request_id),
            )
        conn.commit()

    replay = submit_command(
        config,
        request,
        acp_prompt_router=lambda _candidate: route,
    )

    assert replay.status == "request_state_uncertain"
    assert replay.disposition == "terminal_uncertain"
    assert route.send_count == 1
    assert "private" not in replay.to_json()


def test_stored_selector_evidence_precedes_snapshot_churn_and_changed_selector(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    worker, _binding = _seed(config)
    request_id = "selector-proof"
    request = _instruction(request_id, target={"name": "Coda"})
    route = _Route()
    accepted = submit_command(
        config,
        request,
        acp_prompt_router=lambda candidate: route if candidate == worker else None,
    )
    assert accepted.status == "accepted"
    assert config.db_path is not None
    save_snapshot(
        config.db_path,
        Snapshot(
            host_id=config.host_id,
            updated_at="2026-08-05T00:01:00.000000Z",
            workers=[],
            backend_health=[],
        ),
    )

    exact = submit_command(config, request, acp_prompt_router=lambda _worker: route)
    changed_name = submit_command(
        config,
        _instruction(request_id, target={"name": "Other"}),
        acp_prompt_router=lambda _worker: route,
    )
    changed_id = submit_command(
        config,
        _instruction(request_id, target={"worker_id": "worker-2"}),
        acp_prompt_router=lambda _worker: route,
    )

    assert exact.to_dict() == accepted.to_dict()
    assert changed_name.status == "request_state_uncertain"
    assert changed_id.status == "duplicate_request"
    assert route.send_count == 1
    receipt = get_command_request(config.db_path, config.host_id, request_id)
    assert receipt is not None and receipt["selector_proof"].startswith("v1:")
    assert receipt["selector_proof"] not in accepted.to_json()


def test_reserve_commit_then_exception_recovers_without_transport(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config(tmp_path)
    worker, _binding = _seed(config)
    route = _Route()
    real_reserve = command_submission.reserve_command_request
    calls = 0

    def committed_then_failed(*args: Any, **kwargs: Any) -> Any:
        nonlocal calls
        calls += 1
        real_reserve(*args, **kwargs)
        raise OSError("lost reply after commit")

    monkeypatch.setattr(
        command_submission,
        "reserve_command_request",
        committed_then_failed,
    )
    result = submit_command(
        config,
        _instruction("reserve-commit-loss"),
        acp_prompt_router=lambda candidate: route if candidate == worker else None,
    )

    assert result.status == "pending"
    assert result.disposition == "in_progress"
    assert calls == 1
    assert route.send_count == 0
    receipt = get_command_request(
        config.db_path,
        config.host_id,
        "reserve-commit-loss",
    )
    assert receipt is not None and receipt["state"] == "reserved"


def test_send_start_commit_then_exception_is_never_replayed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config(tmp_path)
    worker, _binding = _seed(config)
    route = _Route()
    real_start = command_submission.mark_command_send_started

    def committed_then_failed(*args: Any, **kwargs: Any) -> Any:
        real_start(*args, **kwargs)
        raise OSError("lost send-start acknowledgement")

    monkeypatch.setattr(
        command_submission,
        "mark_command_send_started",
        committed_then_failed,
    )
    request = _instruction("send-start-commit-loss")
    first = submit_command(
        config,
        request,
        acp_prompt_router=lambda candidate: route if candidate == worker else None,
    )
    second = submit_command(
        config,
        request,
        acp_prompt_router=lambda _candidate: route,
    )

    assert first.status == second.status == "pending"
    assert route.send_count == 1
    receipt = get_command_request(
        config.db_path,
        config.host_id,
        "send-start-commit-loss",
    )
    assert receipt is not None and receipt["state"] == "send_started"


def test_finish_commit_then_exception_recovers_exact_terminal_receipt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config(tmp_path)
    worker, _binding = _seed(config)
    route = _Route()
    real_finish = command_submission.finish_command_request

    def committed_then_failed(*args: Any, **kwargs: Any) -> Any:
        real_finish(*args, **kwargs)
        raise OSError("lost terminal acknowledgement")

    monkeypatch.setattr(
        command_submission,
        "finish_command_request",
        committed_then_failed,
    )
    request = _instruction("finish-commit-loss")
    first = submit_command(
        config,
        request,
        acp_prompt_router=lambda candidate: route if candidate == worker else None,
    )
    second = submit_command(
        config,
        request,
        acp_prompt_router=lambda _candidate: route,
    )
    receipt = get_command_request(
        config.db_path,
        config.host_id,
        "finish-commit-loss",
    )

    assert receipt is not None and receipt["state"] == "accepted"
    assert envelope_to_receipt_json(first) == receipt["result_json"]
    assert second.to_dict() == first.to_dict()
    assert route.send_count == 1


def _pending_decision(config: Config) -> str:
    assert config.db_path is not None
    binding = list_worker_bindings(
        config.db_path,
        config.host_id,
        backend="acp",
    )[0]
    applied = apply_turn_refresh(
        config.db_path,
        config.host_id,
        "worker-1",
        {},
        backend_pending_observation=PendingObservation(
            "open_prompt",
            question="Allow the exact operation?",
            pending_kind="choice",
            revision_digest="permission-contract-revision",
            decision_kind="single",
            decision_options=("Allow", "Reject"),
            decision_question_count=1,
        ),
        expected_binding=binding,
    )
    assert applied.pending_changed
    payload = pending_payload_from_store(config.db_path, config.host_id)
    assert len(payload["pending_interactions"]) == 1
    return str(payload["pending_interactions"][0]["id"])


def _decision_route(value: Any) -> tuple[Any, ...]:
    return tuple(
        getattr(value, field, None)
        for field in (
            "worker_id",
            "worker_fingerprint",
            "binding_private_fingerprint",
            "turn_target_value",
            "decision_ref",
            "decision_kind",
            "option_count",
            "option_refs",
            "text",
        )
    )


class _DecisionRouter:
    def __init__(
        self,
        *,
        entered: threading.Event | None = None,
        release: threading.Event | None = None,
    ) -> None:
        self.entered = entered
        self.release = release
        self.owned_routes: list[tuple[Any, ...]] = []
        self.answered_routes: list[tuple[Any, ...]] = []
        self._lock = threading.Lock()

    def owns_permission_decision(self, decision: Any) -> bool:
        with self._lock:
            self.owned_routes.append(_decision_route(decision))
        return True

    def answer_permission_decision(self, decision: Any, *, timeout: float) -> None:
        assert timeout > 0
        with self._lock:
            self.answered_routes.append(_decision_route(decision))
        if self.entered is not None:
            self.entered.set()
        if self.release is not None:
            assert self.release.wait(2)


def _decision_rows(config: Config, decision_ref: str) -> tuple[str, str, str]:
    assert config.db_path is not None
    with sqlite3.connect(config.db_path) as conn:
        receipt = conn.execute(
            "SELECT state FROM command_receipts ORDER BY created_at DESC LIMIT 1"
        ).fetchone()
        pending = conn.execute(
            "SELECT state FROM backend_pending WHERE decision_ref=?",
            (decision_ref,),
        ).fetchone()
        claim = conn.execute(
            "SELECT state FROM backend_pending_claims WHERE decision_ref=?",
            (decision_ref,),
        ).fetchone()
    assert receipt is not None and pending is not None and claim is not None
    return str(receipt[0]), str(pending[0]), str(claim[0])


def test_permission_route_tuple_is_stable_and_private(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    _worker, _binding = _seed(config)
    decision_ref = _pending_decision(config)
    router = _DecisionRouter()

    result = submit_command(
        config,
        _answer("permission-route", decision_ref),
        acp_permission_router=router,
    )

    assert result.status == "accepted"
    assert router.owned_routes == router.answered_routes
    assert len(router.answered_routes) == 1
    serialized = result.to_json()
    for private in ("private-acp-binding", "private-session"):
        assert private not in serialized
    assert _decision_rows(config, decision_ref) == (
        "accepted",
        "resolved",
        "settled",
    )


def test_permission_terminal_effect_failure_rolls_back_receipt_and_pending(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config(tmp_path)
    _worker, _binding = _seed(config)
    decision_ref = _pending_decision(config)
    router = _DecisionRouter()

    def failing_effect(**_kwargs: Any) -> Callable[[Any], None]:
        def fail(_conn: Any) -> None:
            raise RuntimeError("terminal effect failed")

        return fail

    monkeypatch.setattr(
        command_submission,
        "backend_pending_decision_terminal_effect",
        failing_effect,
    )
    result = submit_command(
        config,
        _answer("permission-effect-rollback", decision_ref),
        acp_permission_router=router,
    )

    assert result.status == "pending"
    assert result.disposition == "in_progress"
    assert _decision_rows(config, decision_ref) == (
        "send_started",
        "open",
        "send_started",
    )
    assert len(router.answered_routes) == 1


def test_permission_terminal_commit_loss_recovers_both_atomic_states(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config(tmp_path)
    _worker, _binding = _seed(config)
    decision_ref = _pending_decision(config)
    router = _DecisionRouter()
    real_finish = command_submission.finish_command_request

    def committed_then_failed(*args: Any, **kwargs: Any) -> Any:
        real_finish(*args, **kwargs)
        raise OSError("lost atomic terminal acknowledgement")

    monkeypatch.setattr(
        command_submission,
        "finish_command_request",
        committed_then_failed,
    )
    request = _answer("permission-effect-commit", decision_ref)
    first = submit_command(
        config,
        request,
        acp_permission_router=router,
    )
    second = submit_command(
        config,
        request,
        acp_permission_router=router,
    )

    assert first.to_dict() == second.to_dict()
    assert first.status == "accepted"
    assert _decision_rows(config, decision_ref) == (
        "accepted",
        "resolved",
        "settled",
    )
    assert len(router.answered_routes) == 1


def test_concurrent_permission_answers_emit_one_acp_response(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    _worker, _binding = _seed(config)
    decision_ref = _pending_decision(config)
    entered = threading.Event()
    release = threading.Event()
    router = _DecisionRouter(entered=entered, release=release)
    requests = [
        _answer(f"permission-concurrent-{index}", decision_ref)
        for index in range(2)
    ]

    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(
            submit_command,
            config,
            requests[0],
            acp_permission_router=router,
        )
        assert entered.wait(2)
        second = pool.submit(
            submit_command,
            config,
            requests[1],
            acp_permission_router=router,
        )
        second_result = second.result(timeout=2)
        release.set()
        first_result = first.result(timeout=2)

    assert first_result.status == "accepted"
    assert first_result.disposition == "terminal_accepted"
    assert second_result.status == "answer_in_progress"
    assert second_result.disposition == "in_progress"
    assert len(router.answered_routes) == 1


@pytest.mark.parametrize(
    "command_payload",
    [
        {"schema_version": 1, "action": "noop"},
        {
            "schema_version": 1,
            "action": "send_instruction",
            "dry_run": True,
            "target": {"worker_id": "worker-1"},
            "instruction": {"text": "preview"},
        },
        {
            "schema_version": 1,
            "action": "answer_decision",
            "dry_run": True,
            "target": {"worker_id": "worker-1"},
            "params": {
                "decision_ref": "pending-public",
                "selection": {"option_refs": ["1"]},
            },
        },
    ],
)
def test_noop_and_mutation_dry_runs_are_store_and_acp_free(
    tmp_path: Path,
    command_payload: dict[str, Any],
) -> None:
    config = _config(tmp_path)

    def forbidden_route(_worker: Worker) -> Any:
        raise AssertionError("nonmutating and dry-run commands must not resolve ACP")

    result = submit_command(
        config,
        command_payload,
        acp_prompt_router=forbidden_route,
    )

    assert result.ok is True
    assert result.status in {"noop", "dry_run"}
    assert config.db_path is not None and not config.db_path.exists()


def test_nonmutating_snapshot_and_resolution_read_projection_without_receipts_or_acp(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    worker, _binding = _seed(config)
    counts_before = _receipt_counts(config)

    def forbidden_route(_worker: Worker) -> Any:
        raise AssertionError("nonmutating commands must not resolve ACP")

    snapshot = submit_command(
        config,
        {"schema_version": 1, "action": "read_snapshot"},
        acp_prompt_router=forbidden_route,
    )
    resolved = submit_command(
        config,
        {
            "schema_version": 1,
            "action": "resolve_target",
            "target": {"worker_id": worker.id},
        },
        acp_prompt_router=forbidden_route,
    )

    assert snapshot.status == "snapshot" and snapshot.ok is True
    assert resolved.status == "resolved" and resolved.ok is True
    assert resolved.result == {
        "target": {
            "worker_id": worker.id,
            "name": worker.name,
            "space_id": worker.space_id,
            "status": worker.status,
            "worker_fingerprint": worker.fingerprint,
        }
    }
    assert _receipt_counts(config) == counts_before


def test_v2_receipt_is_immutable_and_v3_projection_runs_once_per_response(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config(tmp_path)
    worker, _binding = _seed(config)
    route = _Route()
    real_settle = command_submission.settle_submission_link_for_request
    settle_count = 0

    def counted_settle(*args: Any, **kwargs: Any) -> Any:
        nonlocal settle_count
        settle_count += 1
        return real_settle(*args, **kwargs)

    monkeypatch.setattr(
        command_submission,
        "settle_submission_link_for_request",
        counted_settle,
    )
    request = _instruction("v3-once", response_schema_version=3)
    first = submit_command(
        config,
        request,
        acp_prompt_router=lambda candidate: route if candidate == worker else None,
    )
    receipt = get_command_request(config.db_path, config.host_id, "v3-once")
    second = submit_command(
        config,
        request,
        acp_prompt_router=lambda _candidate: route,
    )

    assert first.schema_version == second.schema_version == 3
    assert first.result is not None and first.result["submission_id"].startswith("twsub1.")
    assert receipt is not None
    stored = json.loads(receipt["result_json"])
    assert stored["schema_version"] == 2
    assert "submission_id" not in (stored.get("result") or {})
    assert settle_count == 2
    assert route.send_count == 1
