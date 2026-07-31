"""Semantic connector answers for current backend-owned Claude decisions."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import tendwire.command_submission as command_submission

from tendwire.backends.herdr_decision import calibrate_decision_steps
from tendwire.backends.herdr_turns import (
    PENDING_DECISION_MAX_OPTIONS,
    _pending_observation_from_turn,
)
from tendwire.command_submission import submit_command
from tendwire.core.commands import (
    DISPOSITION_IN_PROGRESS,
    DISPOSITION_NO_RECEIPT,
    STATUS_ACCEPTED,
    STATUS_ANSWER_IN_PROGRESS,
    STATUS_DECISION_NOT_PENDING,
    STATUS_INVALID_SELECTION,
    STATUS_PENDING,
    STATUS_REQUEST_STATE_UNCERTAIN,
    STATUS_UNKNOWN_WORKER,
    STATUS_UNSUPPORTED_DECISION,
    CommandRequest,
    build_canonical_mutation,
)
from tendwire.core.models import Worker
from tendwire.store.sqlite import (
    abandon_backend_pending_choice_claim,
    apply_backend_pending_observation,
    claim_backend_pending_decision,
    envelope_to_receipt_json,
    get_command_request,
    pending_payload_from_store,
    reserve_command_request,
)

from tests.test_command_submission import (
    _FakeSocketClient,
    _binding,
    _config,
    _factory,
    _seed,
)


def _decision_turn(
    *,
    prompt: str = "Choose a database",
    kind: str = "AskUserQuestion",
    multi_select: bool = False,
    question_count: int = 1,
) -> dict[str, Any]:
    return {
        "pending_decision": {
            "decision_id": "private-tool-use",
            "kind": kind,
            "question": prompt,
            "options": [
                {"id": "postgres", "label": "Postgres"},
                {"id": "sqlite", "label": "SQLite"},
                {"id": "duckdb", "label": "DuckDB"},
                {"id": "mysql", "label": "MySQL"},
            ],
            "multi_select": multi_select,
            "question_count": question_count,
        }
    }


def _seed_pending_decision(
    tmp_path: Path,
    *,
    turn: dict[str, Any] | None = None,
    turn_model: str = "observed",
) -> tuple[Any, Worker, str]:
    config = _config(tmp_path, turn_model=turn_model)
    worker = Worker(id="w-1", name="Alpha", status="active")
    binding = _binding(
        worker,
        private_fingerprint="decision-binding-private",
        turn_target_value="decision-pane-private",
    )
    _seed(config, [worker], [binding])
    assert config.db_path is not None
    observation = _pending_observation_from_turn(turn or _decision_turn())
    assert apply_backend_pending_observation(
        config.db_path,
        config.host_id,
        worker.id,
        observation,
        binding_private_fingerprint=binding.private_fingerprint,
        observed_turn_target_value=binding.turn_target_value,
    )
    payload = pending_payload_from_store(config.db_path, config.host_id)
    row = next(item for item in payload["pending_interactions"] if item["worker_id"] == worker.id)
    return config, worker, row["meta"]["decision"]["decision_ref"]


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
        "dry_run": False,
        "target": {"worker_id": worker_id},
        "params": {
            "decision_ref": decision_ref,
            "selection": selection or {"option_refs": ["2"]},
        },
    }


def test_pending_payload_carries_stable_structured_decision_and_rotates_ref(
    tmp_path: Path,
) -> None:
    config, worker, first_ref = _seed_pending_decision(tmp_path)
    assert config.db_path is not None
    first = pending_payload_from_store(config.db_path, config.host_id)
    first_row = next(item for item in first["pending_interactions"] if item["worker_id"] == worker.id)
    assert first_row["meta"]["decision"] == {
        "decision_ref": first_ref,
        "kind": "single",
        "prompt": "Choose a database",
        "options": [
            {"ref": "1", "label": "Postgres"},
            {"ref": "2", "label": "SQLite"},
            {"ref": "3", "label": "DuckDB"},
            {"ref": "4", "label": "MySQL"},
        ],
        "multi_select": False,
        "question_count": 1,
    }

    binding = _binding(
        worker,
        private_fingerprint="decision-binding-private",
        turn_target_value="decision-pane-private",
    )
    changed = _pending_observation_from_turn(
        _decision_turn(prompt="Choose a durable database")
    )
    assert apply_backend_pending_observation(
        config.db_path,
        config.host_id,
        worker.id,
        changed,
        binding_private_fingerprint=binding.private_fingerprint,
        observed_turn_target_value=binding.turn_target_value,
    )
    second = pending_payload_from_store(config.db_path, config.host_id)
    second_row = next(item for item in second["pending_interactions"] if item["worker_id"] == worker.id)
    assert second_row["meta"]["decision"]["decision_ref"] != first_ref


def test_answer_decision_stale_ref_fails_before_pane_io(tmp_path: Path) -> None:
    config, _worker, _decision_ref = _seed_pending_decision(tmp_path)
    calls: list[dict[str, Any]] = []
    result = submit_command(
        config,
        _answer_request("decision-stale"),
        socket_client_factory=_factory(calls),
    )
    assert result.ok is False
    assert result.status == STATUS_DECISION_NOT_PENDING
    assert calls == []


def test_answer_decision_omitted_dry_run_is_preview_only(tmp_path: Path) -> None:
    config, _worker, decision_ref = _seed_pending_decision(tmp_path)
    calls: list[dict[str, Any]] = []
    request = _answer_request(decision_ref)
    request.pop("dry_run")

    result = submit_command(
        config,
        request,
        socket_client_factory=_factory(calls),
    )

    assert result.ok is True
    assert result.status == "dry_run"
    assert result.dry_run is True
    assert calls == []


def test_answer_decision_unknown_worker_fails_before_pane_io(tmp_path: Path) -> None:
    config = _config(tmp_path)
    worker = Worker(id="w-1", name="Alpha", status="active")
    _seed(config, [worker], [_binding(worker)])
    calls: list[dict[str, Any]] = []
    result = submit_command(
        config,
        _answer_request("decision-any", worker_id="worker-missing"),
        socket_client_factory=_factory(calls),
    )
    assert result.ok is False
    assert result.status == STATUS_UNKNOWN_WORKER
    assert calls == []


@pytest.mark.parametrize(
    "selection",
    [
        {"option_refs": ["9"]},
        {"option_refs": ["1", "2"]},
        {"option_refs": ["2", "2"]},
        {"text": ""},
        {"option_refs": ["1"], "text": "both"},
    ],
)
def test_answer_decision_invalid_selection_fails_before_pane_io(
    tmp_path: Path,
    selection: dict[str, Any],
) -> None:
    config, _worker, decision_ref = _seed_pending_decision(tmp_path)
    calls: list[dict[str, Any]] = []
    result = submit_command(
        config,
        _answer_request(decision_ref, selection=selection),
        socket_client_factory=_factory(calls),
    )
    assert result.ok is False
    assert result.status == STATUS_INVALID_SELECTION
    assert calls == []


def test_answer_decision_refuses_multi_question_before_pane_io(tmp_path: Path) -> None:
    config, _worker, decision_ref = _seed_pending_decision(
        tmp_path,
        turn=_decision_turn(question_count=2),
    )
    calls: list[dict[str, Any]] = []
    result = submit_command(
        config,
        _answer_request(decision_ref),
        socket_client_factory=_factory(calls),
    )
    assert result.ok is False
    assert result.status == STATUS_UNSUPPORTED_DECISION
    assert calls == []


def test_answer_decision_rejects_plan_write_in_before_pane_io(tmp_path: Path) -> None:
    config, _worker, decision_ref = _seed_pending_decision(
        tmp_path,
        turn=_decision_turn(kind="ExitPlanMode"),
    )
    calls: list[dict[str, Any]] = []
    result = submit_command(
        config,
        _answer_request(decision_ref, selection={"text": "Revise this plan"}),
        socket_client_factory=_factory(calls),
    )
    assert result.ok is False
    assert result.status == STATUS_INVALID_SELECTION
    assert calls == []


def test_decision_calibration_steps_cover_single_plan_write_in_and_multi() -> None:
    single = calibrate_decision_steps(
        kind="single", option_count=4, option_refs=("2",)
    )
    assert [(item.operation, item.keys, item.text) for item in single] == [
        ("keys", ("2",), None)  # digit alone selects AND submits (live-verified)
    ]

    plan = calibrate_decision_steps(
        kind="plan", option_count=2, option_refs=("1",)
    )
    assert [(item.operation, item.keys, item.text) for item in plan] == [
        ("keys", ("1",), None)
    ]

    write_in = calibrate_decision_steps(
        kind="single", option_count=4, text="Use another database"
    )
    assert [(item.operation, item.keys, item.text) for item in write_in] == [
        ("keys", ("Down", "Down", "Down", "Down"), None),  # write-in row ignores digits
        ("input", ("Enter",), "Use another database"),
    ]

    multi = calibrate_decision_steps(
        kind="multi", option_count=4, option_refs=("3", "1")
    )
    assert [(item.operation, item.keys, item.text) for item in multi] == [
        ("keys", ("1",), None),          # digits toggle rows absolutely
        ("keys", ("3",), None),
        ("keys", ("Right", "Enter"), None),  # Submit tab, then submit
    ]


def test_production_shaped_multi_decision_end_to_end(tmp_path: Path) -> None:
    tool_input = {
        "questions": [
            {
                "question": "Which databases should we support?",
                "header": "Database support",
                "options": [
                    {"label": "Postgres", "description": "Primary database"},
                    {"label": "SQLite", "description": "Local database"},
                    {"label": "DuckDB", "description": "Analytics database"},
                    {"label": "MySQL", "description": "Compatibility database"},
                ],
                "multiSelect": True,
            }
        ]
    }
    question = tool_input["questions"][0]
    adapter_pending_decision = {
        "decision_id": "toolu_multi_123",
        "prompt": f'{question["header"]}: {question["question"]}',
        "mode": "multi",
        "multi_select": question["multiSelect"],
        "options": [
            {"id": str(ordinal), "label": option["label"]}
            for ordinal, option in enumerate(question["options"], 1)
        ],
    }
    assert adapter_pending_decision == {
        "decision_id": "toolu_multi_123",
        "prompt": "Database support: Which databases should we support?",
        "mode": "multi",
        "multi_select": True,
        "options": [
            {"id": "1", "label": "Postgres"},
            {"id": "2", "label": "SQLite"},
            {"id": "3", "label": "DuckDB"},
            {"id": "4", "label": "MySQL"},
        ],
    }

    config, worker, decision_ref = _seed_pending_decision(
        tmp_path,
        turn={"pending_decision": adapter_pending_decision},
    )
    assert config.db_path is not None
    payload = pending_payload_from_store(config.db_path, config.host_id)
    pending = next(
        row for row in payload["pending_interactions"]
        if row["worker_id"] == worker.id
    )
    assert pending["meta"]["decision"] == {
        "decision_ref": decision_ref,
        "kind": "multi",
        "prompt": "Database support: Which databases should we support?",
        "options": [
            {"ref": "1", "label": "Postgres"},
            {"ref": "2", "label": "SQLite"},
            {"ref": "3", "label": "DuckDB"},
            {"ref": "4", "label": "MySQL"},
        ],
        "multi_select": True,
        "question_count": 1,
    }

    calls: list[dict[str, Any]] = []
    result = submit_command(
        config,
        _answer_request(
            decision_ref,
            request_id="multi-production-answer",
            selection={"option_refs": ["3", "1"]},
        ),
        socket_client_factory=_factory(calls),
    )

    assert result.ok is True
    assert result.status == STATUS_ACCEPTED
    assert calls == [
        {
            "method": "pane.send_keys",
            "params": {"pane_id": "decision-pane-private", "keys": ["1"]},
        },
        {
            "method": "pane.send_keys",
            "params": {"pane_id": "decision-pane-private", "keys": ["3"]},
        },
        {
            "method": "pane.send_keys",
            "params": {"pane_id": "decision-pane-private", "keys": ["Right", "Enter"]},
        },
    ]


def _bounded_decision_turn(option_count: int, *, custom_last: bool = False) -> dict[str, Any]:
    options = [
        {"id": str(ordinal), "label": f"Option {ordinal}"}
        for ordinal in range(1, option_count + 1)
    ]
    if custom_last:
        options.append({"id": "custom", "label": "Type something"})
    return {
        "pending_decision": {
            "decision_id": "toolu_bound",
            "kind": "AskUserQuestion",
            "prompt": "Choose one",
            "multi_select": False,
            "options": options,
        }
    }


def _unknown_decision_turn() -> dict[str, Any]:
    turn = _bounded_decision_turn(2)
    turn["pending_decision"]["kind"] = "FutureDecisionKind"
    return turn


def test_decision_option_bound_accepts_exactly_nine(tmp_path: Path) -> None:
    config, worker, _decision_ref = _seed_pending_decision(
        tmp_path,
        turn=_bounded_decision_turn(PENDING_DECISION_MAX_OPTIONS),
    )
    assert config.db_path is not None
    payload = pending_payload_from_store(config.db_path, config.host_id)
    row = next(
        item for item in payload["pending_interactions"]
        if item["worker_id"] == worker.id
    )
    assert len(row["meta"]["decision"]["options"]) == PENDING_DECISION_MAX_OPTIONS


def test_decision_option_bound_accepts_nine_plus_trailing_write_in(
    tmp_path: Path,
) -> None:
    config, worker, _decision_ref = _seed_pending_decision(
        tmp_path,
        turn=_bounded_decision_turn(
            PENDING_DECISION_MAX_OPTIONS,
            custom_last=True,
        ),
    )
    assert config.db_path is not None
    payload = pending_payload_from_store(config.db_path, config.host_id)
    row = next(
        item for item in payload["pending_interactions"]
        if item["worker_id"] == worker.id
    )
    assert len(row["meta"]["decision"]["options"]) == PENDING_DECISION_MAX_OPTIONS


@pytest.mark.parametrize(
    "turn",
    [
        _bounded_decision_turn(PENDING_DECISION_MAX_OPTIONS + 1),
        _bounded_decision_turn(
            PENDING_DECISION_MAX_OPTIONS + 1,
            custom_last=True,
        ),
        _unknown_decision_turn(),
    ],
    ids=[
        "ten-real-options",
        "custom-row-after-ten-real-options",
        "unknown-kind",
    ],
)
def test_over_bound_decision_fails_closed_without_pane_io(
    tmp_path: Path,
    turn: dict[str, Any],
) -> None:
    config = _config(tmp_path)
    worker = Worker(id="w-1", name="Alpha", status="active")
    binding = _binding(
        worker,
        private_fingerprint="decision-binding-private",
        turn_target_value="decision-pane-private",
    )
    _seed(config, [worker], [binding])
    observation = _pending_observation_from_turn(turn)
    assert observation.kind == "read_succeeded_unsupported_decision"
    assert config.db_path is not None
    assert apply_backend_pending_observation(
        config.db_path,
        config.host_id,
        worker.id,
        observation,
        binding_private_fingerprint=binding.private_fingerprint,
        observed_turn_target_value=binding.turn_target_value,
    )
    payload = pending_payload_from_store(config.db_path, config.host_id)
    assert not any(
        item["worker_id"] == worker.id
        for item in payload["pending_interactions"]
    )

    calls: list[dict[str, Any]] = []
    result = submit_command(
        config,
        _answer_request("decision-unsupported-bound"),
        socket_client_factory=_factory(calls),
    )
    assert result.ok is False
    assert result.status == STATUS_UNSUPPORTED_DECISION
    assert calls == []


def test_answer_decision_request_id_replay_does_not_resend_keys(tmp_path: Path) -> None:
    config, _worker, decision_ref = _seed_pending_decision(tmp_path)
    calls: list[dict[str, Any]] = []
    request = _answer_request(decision_ref)

    first = submit_command(config, request, socket_client_factory=_factory(calls))
    replay = submit_command(config, request, socket_client_factory=_factory(calls))

    assert first.ok is True
    assert first.status == STATUS_ACCEPTED
    assert replay.to_dict() == first.to_dict()
    assert calls == [
        {
            "method": "pane.send_keys",
            "params": {
                "pane_id": "decision-pane-private",
                "keys": ["2"],
            },
        }
    ]


def test_answer_decision_observed_mode_completes_without_instruction_turn(
    tmp_path: Path,
) -> None:
    config, _worker, decision_ref = _seed_pending_decision(
        tmp_path,
        turn_model="observed",
    )
    calls: list[dict[str, Any]] = []

    result = submit_command(
        config,
        _answer_request(decision_ref, request_id="observed-answer-decision"),
        socket_client_factory=_factory(calls),
    )

    assert result.ok is True
    assert result.status == STATUS_ACCEPTED
    assert calls == [
        {
            "method": "pane.send_keys",
            "params": {
                "pane_id": "decision-pane-private",
                "keys": ["2"],
            },
        }
    ]


def test_two_requests_race_one_decision_and_only_first_claimant_sends(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config, worker, decision_ref = _seed_pending_decision(tmp_path)
    assert config.db_path is not None
    winner = _answer_request(decision_ref, request_id="decision-winner")
    loser = _answer_request(decision_ref, request_id="decision-loser")
    calls: list[dict[str, Any]] = []
    raced: dict[str, Any] = {}
    real_mark = command_submission._mark_request_send_started

    def race_after_claim(*args: Any, **kwargs: Any) -> Any:
        pending = pending_payload_from_store(config.db_path, config.host_id)
        assert any(
            item["worker_id"] == worker.id
            and item["meta"]["decision"]["decision_ref"] == decision_ref
            for item in pending["pending_interactions"]
        )
        raced["loser"] = submit_command(
            config,
            loser,
            socket_client_factory=_factory(calls),
        )
        return real_mark(*args, **kwargs)

    monkeypatch.setattr(
        command_submission,
        "_mark_request_send_started",
        race_after_claim,
    )
    first = submit_command(
        config,
        winner,
        socket_client_factory=_factory(calls),
    )

    assert first.ok is True
    assert first.status == STATUS_ACCEPTED
    assert raced["loser"].ok is False
    assert raced["loser"].status == STATUS_ANSWER_IN_PROGRESS
    assert raced["loser"].disposition == DISPOSITION_NO_RECEIPT
    assert calls == [
        {
            "method": "pane.send_keys",
            "params": {
                "pane_id": "decision-pane-private",
                "keys": ["2"],
            },
        }
    ]


def test_winner_safe_presend_failure_releases_claim_for_loser_retry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config, _worker, decision_ref = _seed_pending_decision(tmp_path)
    winner = _answer_request(decision_ref, request_id="decision-safe-failure")
    loser = _answer_request(decision_ref, request_id="decision-safe-retry")
    calls: list[dict[str, Any]] = []
    raced: dict[str, Any] = {}
    real_mark = command_submission._mark_request_send_started
    first_mark = True

    def fail_winner_before_send(*args: Any, **kwargs: Any) -> Any:
        nonlocal first_mark
        if first_mark:
            first_mark = False
            raced["loser"] = submit_command(
                config,
                loser,
                socket_client_factory=_factory(calls),
            )
            return command_submission._request_in_progress(args[1])
        return real_mark(*args, **kwargs)

    monkeypatch.setattr(
        command_submission,
        "_mark_request_send_started",
        fail_winner_before_send,
    )
    failed_winner = submit_command(
        config,
        winner,
        socket_client_factory=_factory(calls),
    )
    retried_loser = submit_command(
        config,
        loser,
        socket_client_factory=_factory(calls),
    )

    assert failed_winner.status == STATUS_PENDING
    assert raced["loser"].status == STATUS_ANSWER_IN_PROGRESS
    assert raced["loser"].disposition == DISPOSITION_NO_RECEIPT
    assert retried_loser.ok is True
    assert retried_loser.status == STATUS_ACCEPTED
    assert calls == [
        {
            "method": "pane.send_keys",
            "params": {
                "pane_id": "decision-pane-private",
                "keys": ["2"],
            },
        }
    ]


def test_claim_time_loser_releases_unsent_reservation_for_takeover(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config, worker, decision_ref = _seed_pending_decision(tmp_path)
    assert config.db_path is not None
    request = _answer_request(decision_ref, request_id="claim-time-loser")
    calls: list[dict[str, Any]] = []
    real_claim = command_submission._claim_pending_decision
    injected_claim: dict[str, Any] = {}

    def lose_during_claim(
        claim_config: Any,
        claim_request: CommandRequest,
        validated: Any,
    ) -> Any:
        if "value" not in injected_claim:
            injected_claim["value"] = claim_backend_pending_decision(
                config.db_path,
                config.host_id,
                worker.id,
                decision_ref,
                {"option_refs": ["2"]},
                claim=True,
            )
            assert injected_claim["value"].status == "claimed"
        return real_claim(claim_config, claim_request, validated)

    monkeypatch.setattr(
        command_submission,
        "_claim_pending_decision",
        lose_during_claim,
    )
    first = submit_command(
        config,
        request,
        socket_client_factory=_factory(calls),
    )
    receipt = get_command_request(
        config.db_path,
        config.host_id,
        "claim-time-loser",
    )

    assert first.status == STATUS_ANSWER_IN_PROGRESS
    assert first.disposition == DISPOSITION_IN_PROGRESS
    assert receipt is not None
    assert receipt["state"] == "reserved"
    assert receipt["status"] == STATUS_PENDING
    assert calls == []

    assert abandon_backend_pending_choice_claim(
        config.db_path,
        config.host_id,
        injected_claim["value"].claim_token,
    )
    retry = submit_command(
        config,
        request,
        socket_client_factory=_factory(calls),
    )
    assert retry.ok is True
    assert retry.status == STATUS_ACCEPTED
    assert len(calls) == 1


def test_abandoned_reservation_is_not_terminalized_by_live_claim(
    tmp_path: Path,
) -> None:
    config, worker, decision_ref = _seed_pending_decision(tmp_path)
    assert config.db_path is not None
    payload = _answer_request(decision_ref, request_id="abandoned-loser")
    request = CommandRequest.from_dict(payload)
    canonical = build_canonical_mutation(request, public_worker_id=worker.id)
    initial = reserve_command_request(
        config.db_path,
        host_id=config.host_id,
        request_id=request.request_id or "",
        action=canonical.action,
        canonical_version=canonical.canonical_version,
        canonical_fingerprint=canonical.fingerprint,
        canonical_request_json=canonical.canonical_json,
        public_worker_id=canonical.public_worker_id,
        pending_result_json=envelope_to_receipt_json(
            command_submission._request_in_progress(request)
        ),
        legacy_raw_payload_fingerprint=request.payload_fingerprint(),
        owner_lease_seconds=1,
        now="2020-01-01T00:00:00+00:00",
    )
    assert initial["status"] == "reserved"
    competing = claim_backend_pending_decision(
        config.db_path,
        config.host_id,
        worker.id,
        decision_ref,
        {"option_refs": ["1"]},
        claim=True,
    )
    assert competing.status == "claimed"

    calls: list[dict[str, Any]] = []
    result = submit_command(
        config,
        payload,
        socket_client_factory=_factory(calls),
    )
    receipt = get_command_request(
        config.db_path,
        config.host_id,
        request.request_id or "",
    )

    assert result.status == STATUS_ANSWER_IN_PROGRESS
    assert result.disposition == DISPOSITION_IN_PROGRESS
    assert receipt is not None
    assert receipt["state"] == "reserved"
    assert receipt["status"] == STATUS_PENDING
    assert calls == []


def test_process_loss_before_send_started_is_recovered_after_lease_expiry(
    tmp_path: Path,
) -> None:
    config, worker, decision_ref = _seed_pending_decision(tmp_path)
    assert config.db_path is not None
    payload = _answer_request(decision_ref, request_id="crashed-before-send")
    request = CommandRequest.from_dict(payload)
    canonical = build_canonical_mutation(request, public_worker_id=worker.id)
    reservation = reserve_command_request(
        config.db_path,
        host_id=config.host_id,
        request_id=request.request_id or "",
        action=canonical.action,
        canonical_version=canonical.canonical_version,
        canonical_fingerprint=canonical.fingerprint,
        canonical_request_json=canonical.canonical_json,
        public_worker_id=canonical.public_worker_id,
        pending_result_json=envelope_to_receipt_json(
            command_submission._request_in_progress(request)
        ),
        legacy_raw_payload_fingerprint=request.payload_fingerprint(),
        owner_lease_seconds=1,
        now="2020-01-01T00:00:00+00:00",
    )
    assert reservation["status"] == "reserved"
    abandoned_claim = claim_backend_pending_decision(
        config.db_path,
        config.host_id,
        worker.id,
        decision_ref,
        {"option_refs": ["2"]},
        claim=True,
        observed_at="2020-01-01T00:00:00+00:00",
        claim_lease_seconds=1,
    )
    assert abandoned_claim.status == "claimed"

    calls: list[dict[str, Any]] = []
    recovered = submit_command(
        config,
        payload,
        socket_client_factory=_factory(calls),
    )

    assert recovered.ok is True
    assert recovered.status == STATUS_ACCEPTED
    assert calls == [
        {
            "method": "pane.send_keys",
            "params": {
                "pane_id": "decision-pane-private",
                "keys": ["2"],
            },
        }
    ]


def test_send_uncertainty_is_durable_and_never_resends(
    tmp_path: Path,
) -> None:
    config, _worker, decision_ref = _seed_pending_decision(tmp_path)
    request = _answer_request(decision_ref, request_id="uncertain-decision")
    calls: list[dict[str, Any]] = []

    class FailingKeyClient(_FakeSocketClient):
        def request(
            self,
            method: str,
            params: dict[str, Any],
            *,
            timeout: float | None = None,
        ) -> dict[str, Any]:
            self.calls.append({"method": method, "params": dict(params)})
            if method == "pane.send_keys":
                raise RuntimeError("response lost after key send")
            return {"accepted": True}

    first = submit_command(
        config,
        request,
        socket_client_factory=lambda _config: FailingKeyClient(calls),
    )
    replay = submit_command(
        config,
        request,
        socket_client_factory=lambda _config: FailingKeyClient(calls),
    )

    assert first.ok is False
    assert first.status == STATUS_REQUEST_STATE_UNCERTAIN
    assert replay.status == STATUS_REQUEST_STATE_UNCERTAIN
    assert calls == [
        {
            "method": "pane.send_keys",
            "params": {
                "pane_id": "decision-pane-private",
                "keys": ["2"],
            },
        }
    ]
