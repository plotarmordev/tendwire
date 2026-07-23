"""Tests for the authoritative daemon command submission path."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from threading import Event

import json
import sqlite3
from pathlib import Path
from typing import Any

import pytest
import tendwire.command_submission as command_submission
import tendwire.store.sqlite as store_sqlite

from tendwire.backends.herdr_protocol import HerdrProtocolError
from tendwire.backends.herdr_socket import (
    HerdrSocketDisconnectedError,
    HerdrSocketTimeoutError,
)
from tendwire.command_submission import replay_command_receipt, submit_command
from tendwire.config import Config
from tendwire.core.commands import (
    DISPOSITION_IN_PROGRESS,
    DISPOSITION_NO_RECEIPT,
    DISPOSITION_TERMINAL_ACCEPTED,
    DISPOSITION_TERMINAL_REJECTED,
    DISPOSITION_TERMINAL_UNCERTAIN,
    STATUS_ACCEPTED,
    STATUS_AMBIGUOUS_BACKEND_TARGET,
    STATUS_BACKEND_UNAVAILABLE,
    STATUS_BACKEND_UNSUPPORTED,
    STATUS_DUPLICATE_REQUEST,
    STATUS_INVALID_REQUEST,
    STATUS_NOT_FOUND,
    STATUS_PENDING,
    STATUS_REJECTED,
    STATUS_REQUEST_STATE_UNCERTAIN,
    STATUS_STALE_TARGET,
    CommandEnvelope,
    CommandRequest,
    build_canonical_mutation,
)
from tendwire.core.models import BackendHealth, Snapshot, Worker, WorkerBinding
from tendwire.core.turns import PendingObservation, PendingObservedChoice
from tendwire.store.sqlite import (
    apply_backend_pending_observation,
    cleanup_command_request_retention,
    get_command_request,
    init_store,
    merge_turn_content,
    pending_payload_from_store,
    save_snapshot,
    turns_payload_from_store,
    upsert_command_pending_turn,
    upsert_worker_bindings,
    reserve_command_request,
)


def _receipt_for_action(
    db_path: Path,
    host_id: str,
    request_id: str,
    action: str,
) -> dict[str, Any] | None:
    receipt = get_command_request(db_path, host_id, request_id)
    if receipt is None or receipt["action"] != action:
        return None
    return {**receipt, "uncertain": receipt["state"] in {"send_started", "uncertain"}}


_FORBIDDEN_PUBLIC_KEYS = {
    "pane_id",
    "terminal_id",
    "backend_target",
    "agent_session",
    "argv",
    "shell",
    "target_kind",
    "target_value",
    "private_fingerprint",
}


def _assert_no_private_json(value: Any, path: str = "$") -> None:
    if isinstance(value, dict):
        for key, item in value.items():
            assert key not in _FORBIDDEN_PUBLIC_KEYS, f"forbidden field {path}.{key}"
            _assert_no_private_json(item, f"{path}.{key}")
    elif isinstance(value, list):
        for index, item in enumerate(value):
            _assert_no_private_json(item, f"{path}[{index}]")


def _config(
    tmp_path: Path,
    *,
    backend: str = "socket",
    timeout: float = 5.0,
) -> Config:
    return Config(
        host_id="cmd-host",
        data_dir=tmp_path,
        db_path=tmp_path / "commands.db",
        herdr_backend=backend,
        herdr_timeout_seconds=timeout,
    )


def _request(
    *,
    request_id: str = "req-1",
    worker_id: str = "w-1",
    text: str = "hello",
    worker_fingerprint: str | None = None,
) -> dict[str, Any]:
    target: dict[str, Any] = {"worker_id": worker_id}
    if worker_fingerprint is not None:
        target["worker_fingerprint"] = worker_fingerprint
    return {
        "schema_version": 1,
        "action": "send_instruction",
        "request_id": request_id,
        "dry_run": False,
        "target": target,
        "instruction": {"text": text},
    }


def _healthy_backend() -> BackendHealth:
    return BackendHealth(
        name="herdr",
        status="healthy",
        outcome="healthy_non_empty",
        observed_at="2026-01-01T00:00:00+00:00",
        counts={"workers": 1},
    )


def _degraded_backend() -> BackendHealth:
    return BackendHealth(
        name="herdr",
        status="degraded",
        outcome="timeout",
        observed_at="2026-01-01T00:01:00+00:00",
    )


def _seed(
    config: Config,
    workers: list[Worker],
    bindings: list[WorkerBinding] | None = None,
    *,
    health: BackendHealth | None = None,
) -> None:
    assert config.db_path is not None
    init_store(config.db_path)
    save_snapshot(
        config.db_path,
        Snapshot(
            host_id=config.host_id,
            updated_at="2026-01-01T00:00:00+00:00",
            workers=workers,
            backend_health=[health or _healthy_backend()],
        ),
    )
    if bindings:
        upsert_worker_bindings(config.db_path, bindings)


def _binding(
    worker: Worker,
    *,
    target_kind: str = "agent_id",
    value: str = "agent-secret",
    sendable: bool = True,
    reason: str | None = None,
    fingerprint: str | None = None,
    private_fingerprint: str = "private-secret",
    turn_target_kind: str | None = "pane_id",
    turn_target_value: str | None = "pane-secret",
) -> WorkerBinding:
    return WorkerBinding(
        host_id="cmd-host",
        worker_id=worker.id,
        worker_fingerprint=fingerprint or worker.fingerprint,
        backend="herdr",
        target_kind=target_kind,
        target_value=value,
        turn_target_kind=turn_target_kind,
        turn_target_value=turn_target_value,
        sendable=sendable,
        reason=reason,
        observed_at="2026-01-01T00:00:00+00:00",
        private_fingerprint=private_fingerprint,
    )


class _FakeSocketClient:
    def __init__(
        self,
        calls: list[dict[str, Any]],
        *,
        raises: BaseException | None = None,
        pane_id: str = "pane-secret",
    ) -> None:
        self.calls = calls
        self.raises = raises
        self.pane_id = pane_id
        self.close_count = 0

    def connect(self) -> "_FakeSocketClient":
        return self

    def request(self, method: str, params: dict[str, Any], *, timeout: float | None = None) -> dict[str, Any]:
        self.calls.append({"method": method, "params": dict(params)})
        if self.raises is not None and method == "pane.send_input":
            raise self.raises
        if method == "agent.get":
            return {"result": {"agent": {"pane_id": self.pane_id}}}
        return {"accepted": True}

    def close(self) -> None:
        self.close_count += 1


def _factory(calls: list[dict[str, Any]], *, raises: BaseException | None = None, pane_id: str = "pane-secret"):
    def make_client(config: Config) -> _FakeSocketClient:
        return _FakeSocketClient(calls, raises=raises, pane_id=pane_id)

    return make_client


def _expected_submit_calls(target: str = "agent-secret", *, pane_id: str = "pane-secret") -> list[dict[str, Any]]:
    return [
        {"method": "agent.get", "params": {"target": target}},
        *_expected_private_clear_calls(pane_id),
        {
            "method": "pane.send_input",
            "params": {"pane_id": pane_id, "text": "hello", "keys": ["Enter"]},
        },
    ]


def _expected_private_clear_calls(pane_id: str = "pane-secret") -> list[dict[str, Any]]:
    return [
        {"method": "pane.send_keys", "params": {"pane_id": pane_id, "keys": ["ctrl+u"]}},
        {"method": "pane.send_keys", "params": {"pane_id": pane_id, "keys": ["ctrl+a", "ctrl+k"]}},
        {"method": "pane.send_keys", "params": {"pane_id": pane_id, "keys": ["ctrl+a", "backspace"]}},
    ]

@pytest.mark.parametrize(
    "action", ["send_instruction", "answer_pending", "answer_decision"]
)
@pytest.mark.parametrize(
    ("request_id", "include_request_id"),
    [
        pytest.param(None, False, id="missing"),
        pytest.param(None, True, id="null"),
        pytest.param(123, True, id="non-string"),
        pytest.param("", True, id="empty"),
        pytest.param("x" * 129, True, id="max-plus-one"),
        pytest.param("x" * ((1024 * 1024) - 256), True, id="near-frame-size"),
        pytest.param(" leading", True, id="leading-space"),
        pytest.param("trailing ", True, id="trailing-space"),
        pytest.param("interior space", True, id="interior-space"),
        pytest.param("\t", True, id="tab"),
        pytest.param("\n", True, id="newline"),
        pytest.param("\0", True, id="nul"),
        pytest.param("é", True, id="unicode-nfc"),
        pytest.param("e\u0301", True, id="unicode-nfd"),
        pytest.param("Ａ", True, id="unicode-fullwidth"),
    ],
)
def test_submit_command_rejects_invalid_request_id_before_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    action: str,
    request_id: Any,
    include_request_id: bool,
) -> None:
    config = _config(tmp_path)
    assert config.db_path is not None
    init_store(config.db_path)
    calls: list[str] = []

    def guarded_store(*args: Any, **kwargs: Any) -> Any:
        calls.append("store")
        raise AssertionError("invalid request_id must not access the store")

    def guarded_observation(config: Config) -> Snapshot:
        calls.append("observe")
        raise AssertionError("invalid request_id must not observe")

    def guarded_socket_factory(config: Config) -> _FakeSocketClient:
        calls.append("socket")
        raise AssertionError("invalid request_id must not construct a socket client")

    monkeypatch.setattr("tendwire.command_submission.project_from_observations", guarded_observation)
    monkeypatch.setattr(command_submission, "get_command_request", guarded_store)
    monkeypatch.setattr(command_submission, "latest_snapshot", guarded_store)
    payload = _request()
    if action == "answer_pending":
        payload = {
            "schema_version": 1,
            "action": "answer_pending",
            "request_id": request_id,
            "dry_run": False,
            "params": {
                "pending_id": "pending-public",
                "pending_fingerprint": "pending-revision",
                "choice_id": "choice-public",
            },
        }
    elif action == "answer_decision":
        payload = {
            "schema_version": 1,
            "action": "answer_decision",
            "request_id": request_id,
            "dry_run": False,
            "target": {"worker_id": "w-1"},
            "params": {
                "decision_ref": "decision-public",
                "selection": {"option_refs": ["1"]},
            },
        }
    if include_request_id:
        payload["request_id"] = request_id
    else:
        del payload["request_id"]

    envelope = submit_command(config, payload, socket_client_factory=guarded_socket_factory)

    assert envelope.status == STATUS_INVALID_REQUEST
    assert envelope.error is not None
    assert envelope.error["code"] == STATUS_INVALID_REQUEST
    assert calls == []
    with sqlite3.connect(str(config.db_path)) as conn:
        assert conn.execute("SELECT COUNT(*) FROM commands").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 0


@pytest.mark.parametrize(
    ("label", "factory_exception", "connect_exception"),
    [
        ("factory", RuntimeError("raw setup failure with private detail"), None),
        ("path", ValueError("bad socket path /private/herdr.sock"), None),
        ("missing", None, FileNotFoundError("missing /private/herdr.sock")),
        ("refused", None, ConnectionRefusedError("refused /private/herdr.sock")),
        ("permission", None, PermissionError("denied /private/herdr.sock")),
    ],
)
def test_submit_command_socket_setup_failures_are_backend_unavailable(
    tmp_path: Path,
    label: str,
    factory_exception: BaseException | None,
    connect_exception: BaseException | None,
) -> None:
    config = _config(tmp_path / label)
    worker = Worker(id="w-1", name="Alpha", status="active")
    _seed(config, [worker], [_binding(worker)])
    calls: list[dict[str, Any]] = []

    class SetupFailsClient:
        def connect(self) -> "SetupFailsClient":
            assert connect_exception is not None
            raise connect_exception

        def request(self, method: str, params: dict[str, Any], *, timeout: float | None = None) -> dict[str, Any]:
            calls.append({"method": method, "params": dict(params)})
            raise AssertionError("private send request must not run before setup succeeds")

        def close(self) -> None:
            return None

    def make_client(config: Config) -> SetupFailsClient:
        if factory_exception is not None:
            raise factory_exception
        return SetupFailsClient()

    envelope = submit_command(
        config,
        _request(request_id=f"setup-{label}"),
        socket_client_factory=make_client,
    )

    # Socket setup failed before any transmission. That is a safe pre-send
    # transient: no send began, so the request ID stays retryable and no durable
    # rejection receipt is written.
    assert envelope.status == STATUS_BACKEND_UNAVAILABLE
    assert envelope.disposition == DISPOSITION_NO_RECEIPT
    assert envelope.error is not None
    assert envelope.error["code"] == STATUS_BACKEND_UNAVAILABLE
    assert "private" not in json.dumps(envelope.to_dict())
    assert calls == []
    assert envelope.status != STATUS_NOT_FOUND
    assert envelope.status != STATUS_REQUEST_STATE_UNCERTAIN
    assert config.db_path is not None
    assert get_command_request(config.db_path, "cmd-host", f"setup-{label}") is None
    with sqlite3.connect(str(config.db_path)) as conn:
        assert conn.execute("SELECT COUNT(*) FROM command_receipts").fetchone()[0] == 0
        command_events = conn.execute(
            "SELECT COUNT(*) FROM events WHERE aggregate_type = 'command_request'"
        ).fetchone()[0]
    assert command_events == 0

    # Once the transient clears, the same request ID succeeds exactly once.
    recovery_calls: list[dict[str, Any]] = []
    recovered = submit_command(
        config,
        _request(request_id=f"setup-{label}"),
        socket_client_factory=_factory(recovery_calls),
    )
    assert recovered.status == STATUS_ACCEPTED
    assert recovered.disposition == DISPOSITION_TERMINAL_ACCEPTED
    assert [call["method"] for call in recovery_calls].count("pane.send_input") == 1
    receipt = get_command_request(config.db_path, "cmd-host", f"setup-{label}")
    assert receipt is not None
    assert receipt["state"] == "accepted"


@pytest.mark.parametrize(
    "exc",
    [
        HerdrSocketDisconnectedError("disconnected"),
        HerdrProtocolError("malformed response"),
        OSError("transport failed after write"),
    ],
)
def test_submit_command_post_send_transport_failures_are_uncertain(
    tmp_path: Path,
    exc: BaseException,
) -> None:
    config = _config(tmp_path)
    worker = Worker(id="w-1", name="Alpha", status="active")
    _seed(config, [worker], [_binding(worker)])
    calls: list[dict[str, Any]] = []

    envelope = submit_command(
        config,
        _request(request_id=f"uncertain-{type(exc).__name__}"),
        socket_client_factory=_factory(calls, raises=exc),
    )

    assert envelope.status == STATUS_REQUEST_STATE_UNCERTAIN
    assert envelope.disposition == DISPOSITION_TERMINAL_UNCERTAIN
    assert envelope.status != STATUS_BACKEND_UNAVAILABLE
    assert calls == [
        {"method": "agent.get", "params": {"target": "agent-secret"}},
        *_expected_private_clear_calls(),
        {"method": "pane.send_input", "params": {"pane_id": "pane-secret", "text": "hello", "keys": ["Enter"]}},
    ]
    assert config.db_path is not None
    receipt = _receipt_for_action(config.db_path, "cmd-host", f"uncertain-{type(exc).__name__}", "send_instruction")
    assert receipt is not None
    assert receipt["uncertain"] is True
    with sqlite3.connect(str(config.db_path)) as conn:
        events = [row[0] for row in conn.execute("SELECT event_type FROM events ORDER BY id").fetchall()]
    assert "command.request.send_started" in events
    assert "command.request.uncertain" in events


def test_submit_command_uses_socket_pane_input_once_and_caches_result(tmp_path: Path) -> None:
    config = _config(tmp_path)
    worker = Worker(id="w-1", name="Alpha", status="active")
    _seed(config, [worker], [_binding(worker)])
    calls: list[dict[str, Any]] = []

    first = submit_command(config, _request(), socket_client_factory=_factory(calls))
    second = submit_command(config, _request(), socket_client_factory=_factory(calls))
    duplicate = submit_command(
        config,
        _request(text="changed"),
        socket_client_factory=_factory(calls),
    )

    assert first.status == STATUS_ACCEPTED
    assert first.disposition == DISPOSITION_TERMINAL_ACCEPTED
    assert str(first.result["turn_id"]).startswith("turn-")
    assert first.result == {
        "target": {"worker_id": "w-1"},
        "delivery_state": "submitted",
        "transport_state": "submitted",
        "target_state_at_send": "active",
        "observed_turn_state": "pending_observation",
        "turn_id": first.result["turn_id"],
    }
    assert second.to_dict() == first.to_dict()
    assert duplicate.status == STATUS_DUPLICATE_REQUEST
    assert duplicate.disposition == DISPOSITION_TERMINAL_REJECTED
    assert calls == _expected_submit_calls()

    assert config.db_path is not None
    receipt = _receipt_for_action(config.db_path, "cmd-host", "req-1", "send_instruction")
    assert receipt is not None
    assert receipt["status"] == STATUS_ACCEPTED
    assert receipt["uncertain"] is False
    stored_result = json.loads(receipt["result_json"])
    assert stored_result["schema_version"] == 2
    assert stored_result["disposition"] == DISPOSITION_TERMINAL_ACCEPTED
    assert set(stored_result) == {
        "schema_version",
        "action",
        "request_id",
        "ok",
        "dry_run",
        "status",
        "disposition",
        "result",
        "error",
        "warnings",
    }
    turns_payload = turns_payload_from_store(config.db_path, "cmd-host")
    command_turns = [
        turn
        for turn in turns_payload["turns"]
        if turn.get("origin_command_id") == "req-1"
    ]
    assert len(command_turns) == 1
    command_turn = command_turns[0]
    assert command_turn["worker_id"] == "w-1"
    assert command_turn["worker_fingerprint"] == worker.fingerprint
    assert command_turn["status"] == "active"
    assert command_turn["user_text"] == "hello"
    assert command_turn["assistant_final_text"] is None
    assert command_turn["complete"] is False
    assert command_turn["has_open_turn"] is True
    save_snapshot(
        config.db_path,
        Snapshot(
            host_id=config.host_id,
            updated_at="2026-01-01T00:01:00+00:00",
            workers=[worker],
            backend_health=[_healthy_backend()],
        ),
    )
    turns_after_snapshot = turns_payload_from_store(config.db_path, "cmd-host")
    assert any(turn.get("origin_command_id") == "req-1" for turn in turns_after_snapshot["turns"])

    with sqlite3.connect(str(config.db_path)) as conn:
        event_rows = conn.execute("SELECT event_type, payload_json FROM events ORDER BY id").fetchall()
        command_row = conn.execute(
            "SELECT request_json, result_json FROM commands WHERE request_id = 'req-1'"
        ).fetchone()
    assert [row[0] for row in event_rows] == [
        "snapshot.saved",
        "command.request.reserved",
        "command.request.send_started",
        "command.request.accepted",
    ]
    command_events = [json.loads(row[1]) for row in event_rows[1:]]
    assert [
        (event["state"], event["status"])
        for event in command_events
    ] == [
        ("reserved", STATUS_PENDING),
        ("send_started", STATUS_PENDING),
        ("accepted", STATUS_ACCEPTED),
    ]
    assert "detail" not in command_events[0]
    assert command_events[1]["detail"]["target"] == {"worker_id": "w-1"}
    assert command_events[2]["detail"]["envelope"] == first.to_dict()
    assert command_row is not None
    assert json.loads(command_row[0])["target"] == {"worker_id": "w-1"}
    assert "request_id" not in json.loads(command_row[0])

    public_surfaces = [
        first.to_dict(),
        duplicate.to_dict(),
        turns_payload,
        turns_after_snapshot,
        json.loads(receipt["result_json"]),
        json.loads(command_row[1]),
        *[json.loads(row[1]) for row in event_rows],
    ]
    encoded = json.dumps(public_surfaces)
    assert "agent-secret" not in encoded
    assert "private-secret" not in encoded
    for surface in public_surfaces:
        _assert_no_private_json(surface)


def test_submit_command_writes_turn_claim_before_pane_send(tmp_path: Path) -> None:
    config = _config(tmp_path)
    assert config.db_path is not None
    worker = Worker(
        id="w-1",
        name="Alpha",
        status="active",
        meta={
            "stable_key": "wsk1_" + ("a" * 64),
            "stable_key_version": 1,
        },
    )
    _seed(config, [worker], [_binding(worker)])
    calls: list[dict[str, Any]] = []
    claim_ids: list[str] = []

    class InspectingClient(_FakeSocketClient):
        def request(self, method, params, *, timeout=None):
            if method == "pane.send_input":
                with sqlite3.connect(str(config.db_path)) as conn:
                    claim_ids.extend(
                        str(row[0])
                        for row in conn.execute(
                            """
                            SELECT turn_id FROM turns
                            WHERE host_id = ?
                              AND json_extract(payload_json, '$.origin_command_id') = ?
                            """,
                            (config.host_id, "write-early-request"),
                        ).fetchall()
                    )
            return super().request(method, params, timeout=timeout)

    accepted = submit_command(
        config,
        _request(request_id="write-early-request"),
        socket_client_factory=lambda _config: InspectingClient(calls),
    )

    assert accepted.status == STATUS_ACCEPTED
    assert claim_ids == [accepted.result["turn_id"]]


def test_submit_command_removes_claim_when_pane_input_never_starts(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    assert config.db_path is not None
    worker = Worker(id="w-1", name="Alpha", status="active")
    _seed(config, [worker], [_binding(worker)])
    calls: list[dict[str, Any]] = []

    class ClearFailsClient(_FakeSocketClient):
        def request(self, method, params, *, timeout=None):
            if method == "pane.send_keys":
                calls.append({"method": method, "params": dict(params)})
                raise HerdrSocketDisconnectedError("clear failed before input")
            return super().request(method, params, timeout=timeout)

    envelope = submit_command(
        config,
        _request(request_id="clear-failure-request"),
        socket_client_factory=lambda _config: ClearFailsClient(calls),
    )

    assert envelope.status == STATUS_REQUEST_STATE_UNCERTAIN
    assert not any(call["method"] == "pane.send_input" for call in calls)
    receipt = get_command_request(
        config.db_path,
        config.host_id,
        "clear-failure-request",
    )
    assert receipt is not None
    assert receipt["state"] == "uncertain"
    with sqlite3.connect(str(config.db_path)) as conn:
        claims = conn.execute(
            """
            SELECT turn_id
            FROM turns
            WHERE host_id = ?
              AND json_extract(payload_json, '$.origin_command_id') = ?
            """,
            (config.host_id, "clear-failure-request"),
        ).fetchall()
    assert claims == []


def test_observation_first_submission_adopts_source_row_in_place(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    assert config.db_path is not None
    worker = Worker(
        id="w-1",
        name="Alpha",
        status="active",
        meta={
            "stable_key": "wsk1_" + ("b" * 64),
            "stable_key_version": 1,
        },
    )
    _seed(config, [worker], [_binding(worker)])
    observed_at = store_sqlite.utc_timestamp()
    assert merge_turn_content(
        config.db_path,
        config.host_id,
        worker.id,
        {
            "source_turn_id": "observation-first-source",
            "user_text": "hello",
            "assistant_final_text": "already observed",
            "complete": True,
            "has_open_turn": False,
        },
        observed_at=observed_at,
    ) == 1
    observed = next(
        turn
        for turn in turns_payload_from_store(
            config.db_path,
            config.host_id,
            schema_version=2,
        )["turns"]
        if turn.get("assistant_final_text") == "already observed"
    )
    with sqlite3.connect(str(config.db_path)) as conn:
        observed_list_sequence = conn.execute(
            "SELECT list_sequence FROM turns WHERE host_id = ? AND turn_id = ?",
            (config.host_id, observed["id"]),
        ).fetchone()[0]

    calls: list[dict[str, Any]] = []
    accepted = submit_command(
        config,
        _request(request_id="observation-first-request"),
        socket_client_factory=_factory(calls),
    )

    assert accepted.status == STATUS_ACCEPTED
    assert accepted.result["turn_id"] == observed["id"]
    assert accepted.result["observed_turn_state"] == "complete"
    assert calls == _expected_submit_calls()
    assert merge_turn_content(
        config.db_path,
        config.host_id,
        worker.id,
        {
            "source_turn_id": "observation-first-source",
            "user_text": "hello",
            "assistant_final_text": "tier one refresh",
            "complete": True,
            "has_open_turn": False,
        },
        observed_at=store_sqlite.utc_timestamp(),
    ) == 1
    with sqlite3.connect(str(config.db_path)) as conn:
        matching = conn.execute(
            """
            SELECT turns.turn_id, turns.list_sequence
            FROM turns
            JOIN turn_content_revisions AS revisions
              ON revisions.host_id = turns.host_id
             AND revisions.turn_id = turns.turn_id
             AND revisions.is_current = 1
            WHERE turns.host_id = ? AND revisions.user_text = 'hello'
            """,
            (config.host_id,),
        ).fetchall()
    assert len(matching) == 1
    assert matching[0][0] == observed["id"]
    assert matching[0][1] == observed_list_sequence
    refreshed = turns_payload_from_store(
        config.db_path,
        config.host_id,
        schema_version=2,
    )["turns"]
    assert len([turn for turn in refreshed if turn.get("source_turn_id")]) == 1
    assert next(
        turn for turn in refreshed if turn.get("source_turn_id")
    )["assistant_final_text"] == "tier one refresh"


def test_submission_first_envelope_reflects_observation_during_send(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    assert config.db_path is not None
    worker = Worker(
        id="w-1",
        name="Alpha",
        status="active",
        meta={
            "stable_key": "wsk1_" + ("c" * 64),
            "stable_key_version": 1,
        },
    )
    _seed(config, [worker], [_binding(worker)])
    calls: list[dict[str, Any]] = []

    class ObservingClient(_FakeSocketClient):
        def request(self, method, params, *, timeout=None):
            result = super().request(method, params, timeout=timeout)
            if method == "pane.send_input":
                assert merge_turn_content(
                    config.db_path,
                    config.host_id,
                    worker.id,
                    {
                        "source_turn_id": "submission-first-source",
                        "user_text": "hello",
                        "assistant_final_text": "observed during send",
                        "complete": True,
                        "has_open_turn": False,
                    },
                    observed_at="2099-07-19T10:00:00+00:00",
                ) == 1
            return result

    accepted = submit_command(
        config,
        _request(request_id="submission-first-request"),
        socket_client_factory=lambda _config: ObservingClient(calls),
    )

    assert accepted.status == STATUS_ACCEPTED
    assert accepted.result["observed_turn_state"] == "complete"
    turns = turns_payload_from_store(
        config.db_path,
        config.host_id,
        schema_version=2,
    )["turns"]
    matching = [turn for turn in turns if turn.get("user_text") == "hello"]
    assert len(matching) == 1
    assert matching[0]["id"] == accepted.result["turn_id"]
    assert matching[0]["assistant_final_text"] == "observed during send"


def test_legacy_submission_envelope_follows_tombstoned_claim_successor(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    assert config.db_path is not None
    worker = Worker(id="w-1", name="Alpha", status="active")
    _seed(config, [worker], [_binding(worker)])
    calls: list[dict[str, Any]] = []

    class LegacyObservingClient(_FakeSocketClient):
        def request(self, method, params, *, timeout=None):
            result = super().request(method, params, timeout=timeout)
            if method == "pane.send_input":
                assert merge_turn_content(
                    config.db_path,
                    config.host_id,
                    worker.id,
                    {
                        "source_turn_id": "legacy-envelope-source",
                        "user_text": "hello",
                        "assistant_final_text": "legacy observed during send",
                        "complete": True,
                        "has_open_turn": False,
                    },
                    observed_at=store_sqlite.utc_timestamp(),
                ) == 1
            return result

    accepted = submit_command(
        config,
        _request(request_id="legacy-envelope-request"),
        socket_client_factory=lambda _config: LegacyObservingClient(calls),
    )

    assert accepted.status == STATUS_ACCEPTED
    assert accepted.result["observed_turn_state"] == "complete"
    turns = turns_payload_from_store(
        config.db_path,
        config.host_id,
        schema_version=2,
    )["turns"]
    successor = next(
        turn
        for turn in turns
        if turn.get("assistant_final_text") == "legacy observed during send"
    )
    assert accepted.result["turn_id"] == successor["id"]
    with sqlite3.connect(str(config.db_path)) as conn:
        tombstones = [
            json.loads(row[0])
            for row in conn.execute(
                """
                SELECT payload_json
                FROM turns
                WHERE host_id = ?
                  AND json_extract(payload_json, '$.origin_command_id') = ?
                  AND COALESCE(json_extract(payload_json, '$.superseded_at'), '') != ''
                """,
                (config.host_id, "legacy-envelope-request"),
            ).fetchall()
        ]
    assert len(tombstones) == 1
    assert tombstones[0]["superseded_by_turn_id"] == successor["id"]


def test_submit_command_sends_identical_100_character_instructions_with_distinct_ids(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    worker = Worker(
        id="w-1",
        name="Alpha",
        status="active",
        meta={
            "stable_key": "wsk1_" + ("d" * 64),
            "stable_key_version": 1,
        },
    )
    _seed(config, [worker], [_binding(worker)])
    calls: list[dict[str, Any]] = []
    text = "x" * 100

    first = submit_command(
        config,
        _request(request_id="long-1", text=text),
        socket_client_factory=_factory(calls),
    )
    second = submit_command(
        config,
        _request(request_id="long-2", text=text),
        socket_client_factory=_factory(calls),
    )

    assert first.status == STATUS_ACCEPTED
    assert second.status == STATUS_ACCEPTED
    assert first.result["turn_id"] != second.result["turn_id"]
    expected_send = [
        {"method": "agent.get", "params": {"target": "agent-secret"}},
        *_expected_private_clear_calls(),
        {"method": "pane.send_input", "params": {"pane_id": "pane-secret", "text": text, "keys": ["Enter"]}},
    ]
    assert calls == [*expected_send, *expected_send]

    assert config.db_path is not None
    first_receipt = get_command_request(config.db_path, "cmd-host", "long-1")
    second_receipt = get_command_request(config.db_path, "cmd-host", "long-2")
    assert first_receipt is not None
    assert first_receipt["state"] == "accepted"
    assert second_receipt is not None
    assert second_receipt["state"] == "accepted"
    with sqlite3.connect(str(config.db_path)) as conn:
        turn_count = conn.execute(
            """
            SELECT COUNT(*)
            FROM turns
            WHERE host_id = ?
              AND json_extract(payload_json, '$.source') = 'command'
              AND json_extract(payload_json, '$.has_open_turn') = 1
            """,
            (config.host_id,),
        ).fetchone()[0]
        events = [
            row[0]
            for row in conn.execute(
                "SELECT event_type FROM events WHERE aggregate_type = 'command_request' ORDER BY id"
            ).fetchall()
        ]
    assert turn_count == 2
    assert merge_turn_content(
        config.db_path,
        config.host_id,
        worker.id,
        {
            "source_turn_id": "ambiguous-identical-source",
            "user_text": text,
            "assistant_final_text": "independent observation",
            "complete": True,
            "has_open_turn": False,
        },
        observed_at="2099-07-19T10:00:00+00:00",
    ) == 1
    with sqlite3.connect(str(config.db_path)) as conn:
        stored = [
            (str(turn_id), json.loads(payload_json))
            for turn_id, payload_json in conn.execute(
                "SELECT turn_id, payload_json FROM turns WHERE host_id = ?",
                (config.host_id,),
            ).fetchall()
        ]
    relevant = [
        (turn_id, payload)
        for turn_id, payload in stored
        if payload.get("origin_command_id")
        or payload.get("source_turn_id")
    ]
    assert len(relevant) == 3
    assert {first.result["turn_id"], second.result["turn_id"]}.issubset(
        {turn_id for turn_id, _payload in relevant}
    )
    observed = [
        (turn_id, payload)
        for turn_id, payload in relevant
        if payload.get("source_turn_id")
    ]
    assert len(observed) == 1
    assert observed[0][0] not in {
        first.result["turn_id"],
        second.result["turn_id"],
    }
    assert events == [
        "command.request.reserved",
        "command.request.send_started",
        "command.request.accepted",
        "command.request.reserved",
        "command.request.send_started",
        "command.request.accepted",
    ]
    public_json = json.dumps(
        [
            first.to_dict(),
            second.to_dict(),
            json.loads(first_receipt["result_json"]),
            json.loads(second_receipt["result_json"]),
        ]
    )
    assert text not in public_json
    assert "agent-secret" not in public_json
    assert "private-secret" not in public_json


def test_submit_command_allows_same_instruction_after_worker_fingerprint_changes(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    old_worker = Worker(id="w-1", name="Alpha", status="active", fingerprint="old-fp")
    new_worker = Worker(id="w-1", name="Alpha", status="active", fingerprint="new-fp")
    _seed(
        config,
        [old_worker],
        [_binding(old_worker, value="old-agent-secret", private_fingerprint="old-private-secret")],
    )
    calls: list[dict[str, Any]] = []
    text = "When this exact long Telegram instruction appears for a new binding, it should send."

    first = submit_command(
        config,
        _request(request_id="fingerprint-1", text=text, worker_fingerprint="old-fp"),
        socket_client_factory=_factory(calls),
    )
    assert first.status == STATUS_ACCEPTED

    assert config.db_path is not None
    save_snapshot(
        config.db_path,
        Snapshot(
            host_id=config.host_id,
            updated_at="2026-01-01T00:01:00+00:00",
            workers=[new_worker],
            backend_health=[_healthy_backend()],
        ),
    )
    upsert_worker_bindings(
        config.db_path,
        [_binding(new_worker, value="new-agent-secret", private_fingerprint="new-private-secret")],
    )

    second = submit_command(
        config,
        _request(request_id="fingerprint-2", text=text, worker_fingerprint="new-fp"),
        socket_client_factory=_factory(calls),
    )

    assert second.status == STATUS_ACCEPTED
    assert calls == [
        {"method": "agent.get", "params": {"target": "old-agent-secret"}},
        *_expected_private_clear_calls(),
        {"method": "pane.send_input", "params": {"pane_id": "pane-secret", "text": text, "keys": ["Enter"]}},
        {"method": "agent.get", "params": {"target": "new-agent-secret"}},
        *_expected_private_clear_calls(),
        {"method": "pane.send_input", "params": {"pane_id": "pane-secret", "text": text, "keys": ["Enter"]}},
    ]


def test_submit_command_terminal_worker_id_and_fingerprint_replays_after_healthy_worker_churn(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    worker = Worker(
        id="w-1",
        name="Alpha",
        status="active",
        fingerprint="worker-fingerprint-1",
    )
    _seed(config, [worker], [_binding(worker)])
    calls: list[dict[str, Any]] = []
    request = _request(
        request_id="terminal-worker-churn",
        worker_fingerprint=worker.fingerprint,
    )

    accepted = submit_command(
        config,
        request,
        socket_client_factory=_factory(calls),
    )
    assert accepted.status == STATUS_ACCEPTED
    assert accepted.disposition == DISPOSITION_TERMINAL_ACCEPTED

    assert config.db_path is not None
    save_snapshot(
        config.db_path,
        Snapshot(
            host_id=config.host_id,
            updated_at="2026-01-01T00:01:00+00:00",
            workers=[],
            backend_health=[_healthy_backend()],
        ),
    )
    no_backend = lambda _config: pytest.fail(
        "terminal replay after worker churn must not create a socket client"
    )

    exact_replay = submit_command(
        config,
        request,
        socket_client_factory=no_backend,
    )
    refreshed_fingerprint = submit_command(
        config,
        _request(
            request_id="terminal-worker-churn",
            worker_fingerprint="worker-fingerprint-2",
        ),
        socket_client_factory=no_backend,
    )
    changed_worker = submit_command(
        config,
        _request(
            request_id="terminal-worker-churn",
            worker_id="w-2",
            worker_fingerprint=worker.fingerprint,
        ),
        socket_client_factory=no_backend,
    )
    changed_instruction = submit_command(
        config,
        _request(
            request_id="terminal-worker-churn",
            text="changed",
            worker_fingerprint=worker.fingerprint,
        ),
        socket_client_factory=no_backend,
    )

    assert exact_replay.to_dict() == accepted.to_dict()
    assert refreshed_fingerprint.to_dict() == accepted.to_dict()
    assert changed_worker.status == STATUS_DUPLICATE_REQUEST
    assert changed_worker.disposition == DISPOSITION_TERMINAL_REJECTED
    assert changed_instruction.status == STATUS_DUPLICATE_REQUEST
    assert changed_instruction.disposition == DISPOSITION_TERMINAL_REJECTED
    assert calls == _expected_submit_calls()


def test_submit_command_sends_text_and_enter_atomically(tmp_path: Path) -> None:
    config = _config(tmp_path)
    worker = Worker(id="w-1", name="Alpha", status="active")
    _seed(config, [worker], [_binding(worker)])
    calls: list[dict[str, Any]] = []

    envelope = submit_command(config, _request(), socket_client_factory=_factory(calls))

    assert envelope.status == STATUS_ACCEPTED
    assert calls == [
        {"method": "agent.get", "params": {"target": "agent-secret"}},
        *_expected_private_clear_calls(),
        {"method": "pane.send_input", "params": {"pane_id": "pane-secret", "text": "hello", "keys": ["Enter"]}},
    ]
    assert not any(call["method"] == "pane.send_text" for call in calls)
    assert not any(
        call["method"] == "pane.send_keys" and call["params"].get("keys") == ["Enter"]
        for call in calls
    )


def test_submit_command_reports_submitted_transport_and_worker_state(tmp_path: Path) -> None:
    config = _config(tmp_path)
    worker = Worker(id="w-1", name="Alpha", status="working")
    _seed(config, [worker], [_binding(worker)])
    calls: list[dict[str, Any]] = []

    envelope = submit_command(config, _request(), socket_client_factory=_factory(calls))

    assert envelope.status == STATUS_ACCEPTED
    assert str(envelope.result["turn_id"]).startswith("turn-")
    assert envelope.result == {
        "target": {"worker_id": "w-1"},
        "delivery_state": "submitted",
        "transport_state": "submitted",
        "target_state_at_send": "active",
        "observed_turn_state": "pending_observation",
        "turn_id": envelope.result["turn_id"],
    }


def test_submit_command_marks_idle_worker_delivery_as_submitted(tmp_path: Path) -> None:
    config = _config(tmp_path)
    worker = Worker(id="w-1", name="Alpha", status="idle")
    _seed(config, [worker], [_binding(worker)])
    calls: list[dict[str, Any]] = []

    envelope = submit_command(config, _request(), socket_client_factory=_factory(calls))

    assert envelope.status == STATUS_ACCEPTED
    assert str(envelope.result["turn_id"]).startswith("turn-")
    assert envelope.result == {
        "target": {"worker_id": "w-1"},
        "delivery_state": "submitted",
        "transport_state": "submitted",
        "target_state_at_send": "idle",
        "observed_turn_state": "pending_observation",
        "turn_id": envelope.result["turn_id"],
    }


def test_submit_command_terminal_binding_resolves_pane_and_submits_input(tmp_path: Path) -> None:
    config = _config(tmp_path)
    worker = Worker(id="w-1", name="Alpha", status="active")
    _seed(config, [worker], [_binding(worker, target_kind="terminal_id", value="term-secret")])
    calls: list[dict[str, Any]] = []

    envelope = submit_command(config, _request(), socket_client_factory=_factory(calls, pane_id="pane-private"))

    assert envelope.status == STATUS_ACCEPTED
    assert calls == _expected_submit_calls("term-secret", pane_id="pane-private")
    public_json = json.dumps(envelope.to_dict())
    assert "term-secret" not in public_json
    assert "pane-private" not in public_json
    _assert_no_private_json(envelope.to_dict())


def test_submit_command_pane_binding_submits_without_public_pane_leak(tmp_path: Path) -> None:
    config = _config(tmp_path)
    worker = Worker(id="w-1", name="Alpha", status="active")
    _seed(config, [worker], [_binding(worker, target_kind="pane_id", value="pane-private")])
    calls: list[dict[str, Any]] = []

    envelope = submit_command(config, _request(), socket_client_factory=_factory(calls))

    assert envelope.status == STATUS_ACCEPTED
    assert calls == [
        *_expected_private_clear_calls("pane-private"),
        {"method": "pane.send_input", "params": {"pane_id": "pane-private", "text": "hello", "keys": ["Enter"]}},
    ]
    public_json = json.dumps(envelope.to_dict())
    assert "pane-private" not in public_json
    _assert_no_private_json(envelope.to_dict())


def test_submit_command_backend_unavailable_prevents_not_found_and_send(tmp_path: Path) -> None:
    config = _config(tmp_path)
    health = BackendHealth(
        name="herdr",
        status="degraded",
        outcome="protocol_error",
        observed_at="2026-01-01T00:00:00+00:00",
    )
    _seed(config, [], [], health=health)
    calls: list[dict[str, Any]] = []

    envelope = submit_command(
        config,
        _request(worker_id="missing", request_id="degraded-1"),
        socket_client_factory=_factory(calls),
    )

    assert envelope.status == STATUS_BACKEND_UNAVAILABLE
    assert envelope.disposition == DISPOSITION_NO_RECEIPT
    assert calls == []
    assert envelope.status != STATUS_NOT_FOUND
    assert config.db_path is not None
    assert get_command_request(config.db_path, config.host_id, "degraded-1") is None


def test_submit_command_missing_worker_can_return_not_found_when_backend_healthy(tmp_path: Path) -> None:
    config = _config(tmp_path)
    _seed(config, [], [])
    calls: list[dict[str, Any]] = []

    envelope = submit_command(
        config,
        _request(worker_id="missing", request_id="missing-1"),
        socket_client_factory=_factory(calls),
    )

    assert envelope.status == STATUS_NOT_FOUND
    assert calls == []
def test_submit_command_resolved_health_failure_is_terminal_and_replayed(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    worker = Worker(id="w-1", name="Alpha", status="active")
    _seed(config, [worker], [_binding(worker)], health=_degraded_backend())
    calls: list[dict[str, Any]] = []

    first = submit_command(
        config,
        _request(request_id="resolved-degraded"),
        socket_client_factory=_factory(calls),
    )
    assert config.db_path is not None
    save_snapshot(
        config.db_path,
        Snapshot(
            host_id=config.host_id,
            updated_at="2026-01-01T00:02:00+00:00",
            workers=[worker],
            backend_health=[_healthy_backend()],
        ),
    )
    replay = submit_command(
        config,
        _request(request_id="resolved-degraded"),
        socket_client_factory=_factory(calls),
    )

    assert first.status == STATUS_BACKEND_UNAVAILABLE
    assert first.disposition == DISPOSITION_TERMINAL_REJECTED
    assert replay.to_dict() == first.to_dict()
    assert calls == []
    receipt = get_command_request(
        config.db_path,
        config.host_id,
        "resolved-degraded",
    )
    assert receipt is not None
    assert receipt["state"] == "rejected"
    assert receipt["status"] == STATUS_BACKEND_UNAVAILABLE
    stored_result = json.loads(receipt["result_json"])
    assert stored_result["schema_version"] == 2
    assert stored_result["disposition"] == DISPOSITION_TERMINAL_REJECTED


def test_backend_unavailable_disposition_depends_on_receipt_authority(
    tmp_path: Path,
) -> None:
    no_authority_config = _config(tmp_path / "no-authority")
    _seed(no_authority_config, [], [], health=_degraded_backend())
    no_authority = submit_command(
        no_authority_config,
        _request(request_id="unavailable-no-authority", worker_id="missing"),
        socket_client_factory=lambda _config: pytest.fail(
            "unresolved authority must not create a socket client"
        ),
    )

    rejected_config = _config(tmp_path / "terminal-rejection")
    worker = Worker(id="w-1", name="Alpha", status="active")
    _seed(
        rejected_config,
        [worker],
        [_binding(worker)],
        health=_degraded_backend(),
    )
    terminal_rejection = submit_command(
        rejected_config,
        _request(request_id="unavailable-terminal-rejection"),
        socket_client_factory=lambda _config: pytest.fail(
            "failed health must not create a socket client"
        ),
    )

    assert no_authority.status == terminal_rejection.status == STATUS_BACKEND_UNAVAILABLE
    assert no_authority.disposition == DISPOSITION_NO_RECEIPT
    assert terminal_rejection.disposition == DISPOSITION_TERMINAL_REJECTED
    assert no_authority_config.db_path is not None
    assert rejected_config.db_path is not None
    assert get_command_request(
        no_authority_config.db_path,
        no_authority_config.host_id,
        "unavailable-no-authority",
    ) is None
    receipt = get_command_request(
        rejected_config.db_path,
        rejected_config.host_id,
        "unavailable-terminal-rejection",
    )
    assert receipt is not None
    assert receipt["state"] == "rejected"


def test_submit_command_disallowed_worker_rejection_is_terminal_and_replayed(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    closed = Worker(id="w-1", name="Alpha", status="closed")
    _seed(config, [closed], [_binding(closed)])
    calls: list[dict[str, Any]] = []

    first = submit_command(
        config,
        _request(request_id="closed-worker"),
        socket_client_factory=_factory(calls),
    )
    active = Worker(id="w-1", name="Alpha", status="active")
    assert config.db_path is not None
    save_snapshot(
        config.db_path,
        Snapshot(
            host_id=config.host_id,
            updated_at="2026-01-01T00:02:00+00:00",
            workers=[active],
            backend_health=[_healthy_backend()],
        ),
    )
    replay = submit_command(
        config,
        _request(request_id="closed-worker"),
        socket_client_factory=_factory(calls),
    )

    assert first.status == STATUS_REJECTED
    assert replay.to_dict() == first.to_dict()
    assert calls == []
    receipt = get_command_request(config.db_path, config.host_id, "closed-worker")
    assert receipt is not None
    assert receipt["state"] == "rejected"
    assert receipt["status"] == STATUS_REJECTED


def test_submit_command_rejects_stale_worker_fingerprint_and_binding(tmp_path: Path) -> None:
    config = _config(tmp_path)
    worker = Worker(id="w-1", name="Alpha", status="active")
    _seed(config, [worker], [_binding(worker, fingerprint="old-fingerprint")])
    calls: list[dict[str, Any]] = []

    stale_request = submit_command(
        config,
        _request(request_id="stale-request", worker_fingerprint="old-fingerprint"),
        socket_client_factory=_factory(calls),
    )
    stale_binding = submit_command(
        config,
        _request(request_id="stale-binding"),
        socket_client_factory=_factory(calls),
    )

    assert stale_request.status == STATUS_STALE_TARGET
    assert stale_binding.status == STATUS_STALE_TARGET
    assert calls == []


def test_submit_command_rejects_duplicate_missing_and_unsendable_bindings(tmp_path: Path) -> None:
    config = _config(tmp_path)
    worker = Worker(id="w-1", name="Alpha", status="active")

    _seed(config, [worker], [])
    calls: list[dict[str, Any]] = []
    missing = submit_command(
        config,
        _request(request_id="missing-binding"),
        socket_client_factory=_factory(calls),
    )
    assert missing.status == STATUS_BACKEND_UNSUPPORTED

    config = _config(tmp_path / "unsendable")
    worker = Worker(id="w-1", name="Alpha", status="active")
    _seed(config, [worker], [_binding(worker, sendable=False, reason="disabled")])
    unsendable = submit_command(
        config,
        _request(request_id="unsendable-binding"),
        socket_client_factory=_factory(calls),
    )
    assert unsendable.status == STATUS_BACKEND_UNSUPPORTED

    config = _config(tmp_path / "duplicate")
    worker = Worker(id="w-1", name="Alpha", status="active")
    _seed(
        config,
        [worker],
        [
            _binding(worker, value="agent-a", private_fingerprint="private-a"),
            _binding(worker, value="agent-b", private_fingerprint="private-b"),
        ],
    )
    duplicate = submit_command(
        config,
        _request(request_id="duplicate-binding"),
        socket_client_factory=_factory(calls),
    )
    assert duplicate.status == STATUS_AMBIGUOUS_BACKEND_TARGET
    assert calls == []


def test_submit_command_timeout_after_send_start_is_uncertain_and_not_retried(tmp_path: Path) -> None:
    config = _config(tmp_path)
    worker = Worker(id="w-1", name="Alpha", status="active")
    _seed(config, [worker], [_binding(worker)])
    calls: list[dict[str, Any]] = []

    first = submit_command(
        config,
        _request(request_id="timeout-1"),
        socket_client_factory=_factory(calls, raises=HerdrSocketTimeoutError("timeout")),
    )
    second = submit_command(
        config,
        _request(request_id="timeout-1"),
        socket_client_factory=_factory(calls),
    )

    assert first.status == STATUS_REQUEST_STATE_UNCERTAIN
    assert first.disposition == DISPOSITION_TERMINAL_UNCERTAIN
    assert second.status == STATUS_REQUEST_STATE_UNCERTAIN
    assert second.disposition == DISPOSITION_TERMINAL_UNCERTAIN
    assert calls == [
        {"method": "agent.get", "params": {"target": "agent-secret"}},
        *_expected_private_clear_calls(),
        {"method": "pane.send_input", "params": {"pane_id": "pane-secret", "text": "hello", "keys": ["Enter"]}},
    ]

    assert config.db_path is not None
    receipt = _receipt_for_action(config.db_path, "cmd-host", "timeout-1", "send_instruction")
    assert receipt is not None
    assert receipt["uncertain"] is True
    with sqlite3.connect(str(config.db_path)) as conn:
        event_rows = conn.execute(
            "SELECT event_type, payload_json FROM events "
            "WHERE aggregate_id = ? ORDER BY id",
            ("timeout-1",),
        ).fetchall()
    assert [row[0] for row in event_rows] == [
        "command.request.reserved",
        "command.request.send_started",
        "command.request.uncertain",
    ]
    payloads = [json.loads(row[1]) for row in event_rows]
    assert [(item["state"], item["status"]) for item in payloads] == [
        ("reserved", STATUS_PENDING),
        ("send_started", STATUS_PENDING),
        ("uncertain", STATUS_REQUEST_STATE_UNCERTAIN),
    ]


def test_submit_command_unprovable_selector_spelling_fails_closed(
    tmp_path: Path,
) -> None:
    """A spelling the receipt cannot vouch for is never resolved by a degraded snapshot."""
    config = _config(tmp_path)
    worker = Worker(
        id="w-1",
        name="Alpha",
        status="active",
        space_id="space-1",
    )
    _seed(config, [worker], [_binding(worker)])
    calls: list[dict[str, Any]] = []

    first = submit_command(
        config,
        _request(request_id="selector-unavailable"),
        socket_client_factory=_factory(calls),
    )
    assert config.db_path is not None
    save_snapshot(
        config.db_path,
        Snapshot(
            host_id=config.host_id,
            updated_at="2026-01-01T00:01:00+00:00",
            workers=[],
            backend_health=[_degraded_backend()],
        ),
    )
    # The receipt was issued for an explicit worker ID, so it holds no proof of
    # either alias. Only a healthy observation could show they mean the same
    # worker, and a degraded one may not stand in for it.
    by_name = _request(request_id="selector-unavailable")
    by_name["target"] = {"name": "Alpha"}
    by_space = _request(request_id="selector-unavailable")
    by_space["target"] = {"space_id": "space-1"}

    name_replay = submit_command(config, by_name, socket_client_factory=_factory(calls))
    space_replay = submit_command(config, by_space, socket_client_factory=_factory(calls))

    assert first.status == STATUS_ACCEPTED
    assert name_replay.status == STATUS_REQUEST_STATE_UNCERTAIN
    assert name_replay.disposition == DISPOSITION_TERMINAL_UNCERTAIN
    assert space_replay.status == STATUS_REQUEST_STATE_UNCERTAIN
    assert space_replay.disposition == DISPOSITION_TERMINAL_UNCERTAIN
    assert calls == _expected_submit_calls()
    receipt = get_command_request(config.db_path, config.host_id, "selector-unavailable")
    assert receipt is not None
    assert (receipt["state"], receipt["status"]) == ("accepted", STATUS_ACCEPTED)


def test_submit_command_equivalent_resolved_selectors_replay_once(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    worker = Worker(
        id="w-1",
        name="Alpha",
        status="active",
        space_id="space-1",
    )
    _seed(config, [worker], [_binding(worker)])
    calls: list[dict[str, Any]] = []
    by_name = _request(request_id="selector-equivalent")
    by_name["target"] = {"name": "Alpha"}
    by_space_with_origin = _request(request_id="selector-equivalent")
    by_space_with_origin["target"] = {"space_id": "space-1"}
    by_space_with_origin["params"] = {"origin": "connector-observation"}

    first = submit_command(
        config,
        _request(request_id="selector-equivalent"),
        socket_client_factory=_factory(calls),
    )
    second = submit_command(config, by_name, socket_client_factory=_factory(calls))
    third = submit_command(
        config,
        by_space_with_origin,
        socket_client_factory=_factory(calls),
    )

    assert first.status == STATUS_ACCEPTED
    assert second.to_dict() == first.to_dict()
    assert third.to_dict() == first.to_dict()
    assert calls == _expected_submit_calls()
    assert config.db_path is not None
    receipt = get_command_request(config.db_path, config.host_id, "selector-equivalent")
    assert receipt is not None
    assert receipt["public_worker_id"] == worker.id
    canonical = json.loads(receipt["canonical_request_json"])
    assert canonical["target"] == {"worker_id": worker.id}
    assert canonical["options"] == {}
    assert "origin" not in receipt["canonical_request_json"]


@pytest.mark.parametrize("selector_kind", ["name", "space_id"])
def test_submit_command_exact_selector_retry_survives_selector_reuse(
    tmp_path: Path,
    selector_kind: str,
) -> None:
    """A reused name or space is worker churn, not a changed request."""
    config = _config(tmp_path / selector_kind)
    first_worker = Worker(
        id="w-1",
        name="Alpha",
        status="active",
        space_id="space-1",
    )
    second_worker = Worker(
        id="w-2",
        name="Beta",
        status="active",
        space_id="space-2",
    )
    _seed(config, [first_worker, second_worker], [_binding(first_worker)])
    selector = {"name": "Alpha"} if selector_kind == "name" else {"space_id": "space-1"}
    request = _request(request_id=f"selector-reused-{selector_kind}")
    request["target"] = selector
    calls: list[dict[str, Any]] = []

    accepted = submit_command(config, request, socket_client_factory=_factory(calls))
    # The selector the caller spelled now names a different public worker.
    if selector_kind == "name":
        changed_workers = [
            Worker(id="w-1", name="Former", status="active", space_id="space-1"),
            Worker(id="w-2", name="Alpha", status="active", space_id="space-2"),
        ]
    else:
        changed_workers = [
            Worker(id="w-1", name="Alpha", status="active", space_id="former-space"),
            Worker(id="w-2", name="Beta", status="active", space_id="space-1"),
        ]
    assert config.db_path is not None
    save_snapshot(
        config.db_path,
        Snapshot(
            host_id=config.host_id,
            updated_at="2026-01-01T00:02:00+00:00",
            workers=changed_workers,
            backend_health=[_healthy_backend()],
        ),
    )
    replay = submit_command(
        config,
        request,
        socket_client_factory=lambda _config: pytest.fail(
            "an exact retry must not create a socket client"
        ),
    )

    assert accepted.status == STATUS_ACCEPTED
    # Re-resolving the selector would deliver the instruction a second time, to
    # a worker the caller never addressed. The receipt outranks the snapshot.
    assert replay.to_dict() == accepted.to_dict()
    assert replay.result is not None
    assert replay.result["target"] == {"worker_id": "w-1"}
    assert calls == _expected_submit_calls()


@pytest.mark.parametrize(
    ("selector_kind", "changed_selector"),
    [
        ("name", {"name": "Beta"}),
        ("space_id", {"space_id": "space-2"}),
        ("name_and_space", {"name": "Beta", "space_id": "space-2"}),
    ],
)
def test_submit_command_changed_selector_conflicts_without_backend(
    tmp_path: Path,
    selector_kind: str,
    changed_selector: dict[str, Any],
) -> None:
    """Reusing a request ID with a different selector cannot claim its result."""
    config = _config(tmp_path / selector_kind)
    first_worker = Worker(
        id="w-1",
        name="Alpha",
        status="active",
        space_id="space-1",
    )
    second_worker = Worker(
        id="w-2",
        name="Beta",
        status="active",
        space_id="space-2",
    )
    _seed(config, [first_worker, second_worker], [_binding(first_worker)])
    request = _request(request_id=f"selector-changed-{selector_kind}")
    request["target"] = {"name": "Alpha"}
    calls: list[dict[str, Any]] = []

    accepted = submit_command(config, request, socket_client_factory=_factory(calls))
    changed = _request(request_id=f"selector-changed-{selector_kind}")
    changed["target"] = changed_selector
    conflict = submit_command(
        config,
        changed,
        socket_client_factory=lambda _config: pytest.fail(
            "a changed selector must not create a socket client"
        ),
    )

    assert accepted.status == STATUS_ACCEPTED
    assert conflict.status == STATUS_DUPLICATE_REQUEST
    assert conflict.disposition == DISPOSITION_TERMINAL_REJECTED
    assert calls == _expected_submit_calls()
    assert config.db_path is not None
    receipt = get_command_request(
        config.db_path,
        config.host_id,
        f"selector-changed-{selector_kind}",
    )
    assert receipt is not None
    assert (receipt["state"], receipt["status"]) == ("accepted", STATUS_ACCEPTED)
    assert json.loads(receipt["result_json"]) == accepted.to_dict()


def test_submit_command_migrated_v11_exact_raw_request_replays(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    assert config.db_path is not None
    request_payload = _request(request_id="legacy-exact")
    request = CommandRequest.from_dict(request_payload)
    accepted = CommandEnvelope.from_result(
        request,
        ok=True,
        status=STATUS_ACCEPTED,
        disposition=DISPOSITION_TERMINAL_ACCEPTED,
        result={
            "target": {"worker_id": "w-1"},
            "delivery_state": "submitted",
            "transport_state": "submitted",
            "target_state_at_send": "active",
            "observed_turn_state": "pending_observation",
        },
    )
    legacy_result = accepted.to_dict()
    legacy_result.pop("disposition")
    legacy_result["schema_version"] = 1
    legacy_fingerprint = request.payload_fingerprint()
    result_json = json.dumps(
        legacy_result,
        sort_keys=True,
        separators=(",", ":"),
    )
    request_json = json.dumps(
        request.to_dict(),
        sort_keys=True,
        separators=(",", ":"),
    )
    with sqlite3.connect(str(config.db_path)) as conn:
        conn.executescript(
            store_sqlite.CREATE_LEGACY_COMMAND_RECEIPTS_TABLE
            + store_sqlite.CREATE_LEGACY_COMMANDS_TABLE
        )
        conn.execute(
            """
            INSERT INTO command_receipts (
                host_id, request_id, action, payload_fingerprint, status,
                result_json, created_at, completed_at, uncertain
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0)
            """,
            (
                config.host_id,
                request.request_id,
                request.action,
                legacy_fingerprint,
                STATUS_ACCEPTED,
                result_json,
                "2026-01-01T00:00:00+00:00",
                "2026-01-01T00:00:01+00:00",
            ),
        )
        conn.execute(
            """
            INSERT INTO commands (
                host_id, request_id, action, payload_fingerprint, status,
                dry_run, uncertain, request_json, result_json, created_at,
                reserved_at, completed_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, 0, 0, ?, ?, ?, ?, ?, ?)
            """,
            (
                config.host_id,
                request.request_id,
                request.action,
                legacy_fingerprint,
                STATUS_ACCEPTED,
                request_json,
                result_json,
                "2026-01-01T00:00:00+00:00",
                "2026-01-01T00:00:00+00:00",
                "2026-01-01T00:00:01+00:00",
                "2026-01-01T00:00:01+00:00",
            ),
        )
        conn.execute("PRAGMA user_version = 11")

    init_store(config.db_path)
    replay = submit_command(
        config,
        request_payload,
        socket_client_factory=lambda _config: pytest.fail(
            "legacy terminal replay must not create a socket client"
        ),
    )
    changed = submit_command(
        config,
        _request(request_id="legacy-exact", text="changed"),
        socket_client_factory=lambda _config: pytest.fail(
            "legacy request collision must not create a socket client"
        ),
    )

    assert replay.to_dict() == accepted.to_dict()
    assert replay.disposition == DISPOSITION_TERMINAL_ACCEPTED
    assert changed.status == STATUS_DUPLICATE_REQUEST
    assert changed.disposition == DISPOSITION_TERMINAL_REJECTED


def test_submit_command_same_id_text_target_and_action_collide_without_backend(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    first_worker = Worker(id="w-1", name="Alpha", status="active")
    second_worker = Worker(id="w-2", name="Beta", status="active")
    _seed(
        config,
        [first_worker, second_worker],
        [
            _binding(first_worker),
            _binding(
                second_worker,
                value="other-agent-secret",
                private_fingerprint="other-private-secret",
            ),
        ],
    )
    calls: list[dict[str, Any]] = []
    request_id = "collision-1"

    accepted = submit_command(
        config,
        _request(request_id=request_id),
        socket_client_factory=_factory(calls),
    )
    changed_text = submit_command(
        config,
        _request(request_id=request_id, text="changed"),
        socket_client_factory=lambda _config: pytest.fail(
            "text collision must not create a socket client"
        ),
    )
    changed_target = submit_command(
        config,
        _request(request_id=request_id, worker_id=second_worker.id),
        socket_client_factory=lambda _config: pytest.fail(
            "target collision must not create a socket client"
        ),
    )
    changed_action = submit_command(
        config,
        _answer_request(request_id=request_id),
        socket_client_factory=lambda _config: pytest.fail(
            "action collision must not create a socket client"
        ),
    )
    unknown_options = _request(request_id=request_id)
    unknown_options["options"] = {"mode": "invented"}
    invalid_options = submit_command(
        config,
        unknown_options,
        socket_client_factory=lambda _config: pytest.fail(
            "invalid options must not create a socket client"
        ),
    )
    fresh_unknown_options = _request(request_id="options-invalid-fresh")
    fresh_unknown_options["options"] = {"mode": "invented"}
    fresh_invalid_options = submit_command(
        config,
        fresh_unknown_options,
        socket_client_factory=lambda _config: pytest.fail(
            "invalid options must not create a socket client"
        ),
    )

    assert accepted.status == STATUS_ACCEPTED
    assert accepted.disposition == DISPOSITION_TERMINAL_ACCEPTED
    assert changed_text.status == STATUS_DUPLICATE_REQUEST
    assert changed_text.disposition == DISPOSITION_TERMINAL_REJECTED
    assert changed_target.status == STATUS_DUPLICATE_REQUEST
    assert changed_target.disposition == DISPOSITION_TERMINAL_REJECTED
    assert changed_action.status == STATUS_DUPLICATE_REQUEST
    assert changed_action.disposition == DISPOSITION_TERMINAL_REJECTED
    assert invalid_options.status == STATUS_INVALID_REQUEST
    assert fresh_invalid_options.status == STATUS_INVALID_REQUEST
    assert calls == _expected_submit_calls()
    assert config.db_path is not None
    receipt = get_command_request(config.db_path, config.host_id, request_id)
    assert receipt is not None
    assert receipt["state"] == "accepted"
    assert receipt["public_worker_id"] == first_worker.id
    assert (
        get_command_request(config.db_path, config.host_id, "options-invalid-fresh")
        is None
    )


def test_submit_command_private_preparation_over_30_second_budget_precedes_reservation_and_sends_once(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config(tmp_path, timeout=31)
    worker = Worker(id="w-1", name="Alpha", status="active")
    _seed(config, [worker], [_binding(worker)])
    pane_lookup_started = Event()
    release_pane_lookup = Event()
    first_calls: list[dict[str, Any]] = []
    second_calls: list[dict[str, Any]] = []
    clients: list[_FakeSocketClient] = []

    class BlockingClient(_FakeSocketClient):
        def request(
            self,
            method: str,
            params: dict[str, Any],
            *,
            timeout: float | None = None,
        ) -> dict[str, Any]:
            if method == "agent.get":
                assert timeout == 31
                pane_lookup_started.set()
                assert release_pane_lookup.wait(timeout=5)
            return super().request(method, params, timeout=timeout)

    def first_factory(config: Config) -> BlockingClient:
        client = BlockingClient(first_calls)
        clients.append(client)
        return client

    def second_factory(config: Config) -> _FakeSocketClient:
        client = _FakeSocketClient(second_calls)
        clients.append(client)
        return client

    with ThreadPoolExecutor(max_workers=2) as executor:
        first_future = executor.submit(
            submit_command,
            config,
            _request(request_id="concurrent-1"),
            socket_client_factory=first_factory,
        )
        assert pane_lookup_started.wait(timeout=5)
        assert config.db_path is not None
        assert get_command_request(config.db_path, config.host_id, "concurrent-1") is None
        second = submit_command(
            config,
            _request(request_id="concurrent-1"),
            socket_client_factory=second_factory,
        )
        release_pane_lookup.set()
        first = first_future.result(timeout=5)

    assert first.status == STATUS_ACCEPTED
    assert second.status == STATUS_ACCEPTED
    assert first_calls == [{"method": "agent.get", "params": {"target": "agent-secret"}}]
    assert second_calls == _expected_submit_calls()
    assert [client.close_count for client in clients] == [1, 1]
    receipt = get_command_request(config.db_path, config.host_id, "concurrent-1")
    assert receipt is not None
    assert receipt["state"] == "accepted"
    with sqlite3.connect(str(config.db_path)) as conn:
        assert conn.execute(
            "SELECT COUNT(*) FROM command_receipts "
            "WHERE host_id = ? AND request_id = ? AND state = 'accepted'",
            (config.host_id, "concurrent-1"),
        ).fetchone()[0] == 1
    assert sum(call["method"] == "pane.send_input" for call in first_calls + second_calls) == 1


@pytest.mark.parametrize(
    ("after_commit", "expected_status", "expected_state"),
    [
        (False, STATUS_PENDING, "reserved"),
        (True, STATUS_PENDING, "send_started"),
    ],
)
def test_submit_command_send_start_exception_recovers_durable_state_and_closes_prepared_clients(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    after_commit: bool,
    expected_status: str,
    expected_state: str,
) -> None:
    config = _config(tmp_path / expected_state)
    worker = Worker(id="w-1", name="Alpha", status="active")
    _seed(config, [worker], [_binding(worker)])
    real_mark = command_submission.mark_command_send_started
    calls: list[dict[str, Any]] = []
    clients: list[_FakeSocketClient] = []

    def lose_send_start_response(*args: Any, **kwargs: Any) -> Any:
        if after_commit:
            result = real_mark(*args, **kwargs)
            assert result["status"] == "send_started"
        raise HerdrSocketTimeoutError("send-start response lost")

    def factory(config: Config) -> _FakeSocketClient:
        client = _FakeSocketClient(calls)
        clients.append(client)
        return client

    monkeypatch.setattr(
        command_submission,
        "mark_command_send_started",
        lose_send_start_response,
    )
    first = submit_command(
        config,
        _request(request_id="send-start-loss"),
        socket_client_factory=factory,
    )

    assert first.status == expected_status
    assert first.disposition == DISPOSITION_IN_PROGRESS
    assert calls == [{"method": "agent.get", "params": {"target": "agent-secret"}}]
    assert clients[0].close_count == 1
    assert config.db_path is not None
    receipt = get_command_request(config.db_path, config.host_id, "send-start-loss")
    assert receipt is not None
    assert receipt["state"] == expected_state

    if after_commit:
        replay = submit_command(
            config,
            _request(request_id="send-start-loss"),
            socket_client_factory=lambda _config: pytest.fail(
                "send-started replay must not prepare another client"
            ),
        )
        assert replay.status == STATUS_PENDING
        assert replay.disposition == DISPOSITION_IN_PROGRESS
        assert len(clients) == 1
    else:
        # The reservation is still owned, so both retries are decided by the
        # receipt alone: no second client, no second private observation.
        replay = submit_command(
            config,
            _request(request_id="send-start-loss"),
            socket_client_factory=factory,
        )
        assert replay.status == STATUS_PENDING
        assert replay.disposition == DISPOSITION_IN_PROGRESS
        conflict = submit_command(
            config,
            _request(request_id="send-start-loss", text="different"),
            socket_client_factory=factory,
        )
        assert conflict.status == STATUS_DUPLICATE_REQUEST
        assert [client.close_count for client in clients] == [1]
        assert calls == [
            {"method": "agent.get", "params": {"target": "agent-secret"}},
        ]
    assert not any(call["method"] == "pane.send_input" for call in calls)


@pytest.mark.parametrize(
    ("initial_kind", "expected_replay_status", "expected_state"),
    [
        ("accepted", STATUS_REQUEST_STATE_UNCERTAIN, "uncertain"),
        ("rejected", STATUS_REJECTED, "rejected"),
    ],
)
def test_submit_command_terminal_replay_retention_delete_atomically_fences_prepared_takeover(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    initial_kind: str,
    expected_replay_status: str,
    expected_state: str,
) -> None:
    config = _config(tmp_path / initial_kind)
    worker_status = "active" if initial_kind == "accepted" else "closed"
    worker = Worker(id="w-1", name="Alpha", status=worker_status)
    _seed(config, [worker], [_binding(worker)])
    initial_calls: list[dict[str, Any]] = []
    first = submit_command(
        config,
        _request(request_id="terminal-delete-race"),
        socket_client_factory=_factory(initial_calls),
    )
    assert first.status == (
        STATUS_ACCEPTED if initial_kind == "accepted" else STATUS_REJECTED
    )
    assert config.db_path is not None

    active_worker = Worker(id="w-1", name="Alpha", status="active")
    save_snapshot(
        config.db_path,
        Snapshot(
            host_id=config.host_id,
            updated_at="2026-01-01T00:02:00+00:00",
            workers=[active_worker],
            backend_health=[_healthy_backend()],
        ),
    )
    upsert_worker_bindings(config.db_path, [_binding(active_worker)])

    real_atomic_replay = command_submission.reserve_terminal_command_replay
    deleted = Event()
    contender_prepared = Event()
    release_contender = Event()
    atomic_calls = 0

    def delete_then_insert_terminal(*args: Any, **kwargs: Any) -> Any:
        nonlocal atomic_calls
        atomic_calls += 1
        with sqlite3.connect(str(config.db_path)) as conn:
            conn.execute(
                "DELETE FROM command_receipts WHERE host_id = ? AND request_id = ?",
                (config.host_id, "terminal-delete-race"),
            )
            conn.commit()
        deleted.set()
        assert contender_prepared.wait(timeout=5)
        try:
            return real_atomic_replay(*args, **kwargs)
        finally:
            release_contender.set()

    monkeypatch.setattr(
        command_submission,
        "reserve_terminal_command_replay",
        delete_then_insert_terminal,
    )
    contender_calls: list[dict[str, Any]] = []

    class PreparedContenderClient(_FakeSocketClient):
        def request(
            self,
            method: str,
            params: dict[str, Any],
            *,
            timeout: float | None = None,
        ) -> dict[str, Any]:
            if method == "agent.get":
                contender_prepared.set()
                assert release_contender.wait(timeout=5)
            return super().request(method, params, timeout=timeout)

    contender_client = PreparedContenderClient(contender_calls)
    with ThreadPoolExecutor(max_workers=2) as executor:
        replay_future = executor.submit(
            submit_command,
            config,
            _request(request_id="terminal-delete-race"),
            socket_client_factory=lambda _config: pytest.fail(
                "terminal replay must not prepare another client"
            ),
        )
        assert deleted.wait(timeout=5)
        contender_future = executor.submit(
            submit_command,
            config,
            _request(request_id="terminal-delete-race"),
            socket_client_factory=lambda _config: contender_client,
        )
        replay = replay_future.result(timeout=5)
        contender = contender_future.result(timeout=5)

    assert replay.status == expected_replay_status
    assert contender.to_dict() == replay.to_dict()
    assert atomic_calls == 1
    assert contender_client.close_count == 1
    assert contender_calls == [
        {"method": "agent.get", "params": {"target": "agent-secret"}}
    ]
    receipt = get_command_request(config.db_path, config.host_id, "terminal-delete-race")
    assert receipt is not None
    assert receipt["state"] == expected_state
    assert receipt["state"] != "reserved"
    assert sum(call["method"] == "pane.send_input" for call in initial_calls) == (
        1 if initial_kind == "accepted" else 0
    )


def test_submit_command_timeout_before_send_start_stays_retryable(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    worker = Worker(id="w-1", name="Alpha", status="active")
    _seed(config, [worker], [_binding(worker)])

    class ConnectTimeoutClient:
        def connect(self) -> None:
            raise HerdrSocketTimeoutError("connect timeout")

        def request(
            self,
            method: str,
            params: dict[str, Any],
            *,
            timeout: float | None = None,
        ) -> dict[str, Any]:
            raise AssertionError("pane operations must not run")

        def close(self) -> None:
            return None

    first = submit_command(
        config,
        _request(request_id="before-timeout"),
        socket_client_factory=lambda _config: ConnectTimeoutClient(),
    )

    # A connect timeout occurs before any request transmission, so no send began.
    # It must stay retryable rather than durably reject the unsent command.
    assert first.status == STATUS_BACKEND_UNAVAILABLE
    assert first.disposition == DISPOSITION_NO_RECEIPT
    assert config.db_path is not None
    assert get_command_request(config.db_path, config.host_id, "before-timeout") is None

    # A retry under the same request ID re-attempts and, once the socket
    # responds, sends exactly once.
    recovery_calls: list[dict[str, Any]] = []
    recovered = submit_command(
        config,
        _request(request_id="before-timeout"),
        socket_client_factory=_factory(recovery_calls),
    )
    assert recovered.status == STATUS_ACCEPTED
    assert [call["method"] for call in recovery_calls].count("pane.send_input") == 1
    receipt = get_command_request(config.db_path, config.host_id, "before-timeout")
    assert receipt is not None
    assert receipt["state"] == "accepted"
    assert receipt["send_started_at"] is not None


def test_submit_command_finalization_timeout_after_send_is_uncertain_and_not_retried(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config(tmp_path)
    worker = Worker(id="w-1", name="Alpha", status="active")
    _seed(config, [worker], [_binding(worker)])
    calls: list[dict[str, Any]] = []

    def timeout_finish(*args: Any, **kwargs: Any) -> Any:
        raise HerdrSocketTimeoutError("receipt finalization timeout")

    monkeypatch.setattr(command_submission, "finish_command_request", timeout_finish)
    first = submit_command(
        config,
        _request(request_id="after-timeout"),
        socket_client_factory=_factory(calls),
    )
    replay = submit_command(
        config,
        _request(request_id="after-timeout"),
        socket_client_factory=lambda _config: pytest.fail(
            "send-started replay must not create a socket client"
        ),
    )

    assert first.status == STATUS_PENDING
    assert replay.status == STATUS_PENDING
    assert first.disposition == DISPOSITION_IN_PROGRESS
    assert replay.disposition == DISPOSITION_IN_PROGRESS
    assert calls == _expected_submit_calls()
    assert config.db_path is not None
    receipt = get_command_request(config.db_path, config.host_id, "after-timeout")
    assert receipt is not None
    assert receipt["state"] == "send_started"
    maintenance = cleanup_command_request_retention(
        config.db_path,
        retry_horizon_seconds=60,
        retention_seconds=691_200,
        retention_count=4096,
        host_id=config.host_id,
        now="2099-01-01T00:00:00+00:00",
    )
    terminal = replay_command_receipt(
        config,
        _request(request_id="after-timeout"),
    )
    assert maintenance["stale_active"] == 1
    assert terminal is not None
    assert terminal.status == STATUS_REQUEST_STATE_UNCERTAIN
    assert terminal.disposition == DISPOSITION_TERMINAL_UNCERTAIN
    assert calls == _expected_submit_calls()


def test_submit_command_accepted_finalization_response_loss_replays_accepted(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config(tmp_path)
    worker = Worker(id="w-1", name="Alpha", status="active")
    _seed(config, [worker], [_binding(worker)])
    calls: list[dict[str, Any]] = []
    real_finish = command_submission.finish_command_request

    def finish_then_lose_response(*args: Any, **kwargs: Any) -> Any:
        result = real_finish(*args, **kwargs)
        assert result["status"] == "accepted"
        raise HerdrSocketTimeoutError("accepted response lost")

    monkeypatch.setattr(
        command_submission,
        "finish_command_request",
        finish_then_lose_response,
    )
    first = submit_command(
        config,
        _request(request_id="accepted-response-loss"),
        socket_client_factory=_factory(calls),
    )
    replay = submit_command(
        config,
        _request(request_id="accepted-response-loss"),
        socket_client_factory=lambda _config: pytest.fail(
            "accepted replay must not create a socket client"
        ),
    )

    assert first.status == STATUS_ACCEPTED
    assert first.disposition == DISPOSITION_TERMINAL_ACCEPTED
    assert replay.to_dict() == first.to_dict()
    assert calls == _expected_submit_calls()
    assert config.db_path is not None
    receipt = get_command_request(
        config.db_path,
        config.host_id,
        "accepted-response-loss",
    )
    assert receipt is not None
    assert receipt["state"] == "accepted"
    turns = turns_payload_from_store(config.db_path, config.host_id)["turns"]
    assert (
        sum(
            turn.get("origin_command_id") == "accepted-response-loss"
            for turn in turns
        )
        == 1
    )


@pytest.mark.parametrize(
    ("schema_version", "legacy_v1"),
    [
        pytest.param(True, True, id="v1-bool-alias"),
        pytest.param(1.0, True, id="v1-float-alias"),
        pytest.param(2.0, False, id="v2-float-alias"),
    ],
)
def test_replay_command_receipt_rejects_non_exact_stored_schema_versions(
    tmp_path: Path,
    schema_version: Any,
    legacy_v1: bool,
) -> None:
    config = _config(tmp_path)
    worker = Worker(id="w-1", name="Alpha", status="active")
    _seed(config, [worker], [_binding(worker)])
    calls: list[dict[str, Any]] = []
    request_payload = _request(request_id=f"schema-alias-{type(schema_version).__name__}")
    accepted = submit_command(
        config,
        request_payload,
        socket_client_factory=_factory(calls),
    )
    assert accepted.status == STATUS_ACCEPTED
    assert config.db_path is not None
    receipt = get_command_request(
        config.db_path,
        config.host_id,
        request_payload["request_id"],
    )
    assert receipt is not None
    stored = json.loads(receipt["result_json"])
    if legacy_v1:
        stored.pop("disposition")
    stored["schema_version"] = schema_version
    malformed_json = json.dumps(stored, sort_keys=True, separators=(",", ":"))
    with sqlite3.connect(str(config.db_path)) as conn:
        conn.execute(
            "UPDATE command_receipts SET result_json = ? "
            "WHERE host_id = ? AND request_id = ?",
            (malformed_json, config.host_id, request_payload["request_id"]),
        )
        conn.commit()
        rows_before = conn.execute(
            "SELECT * FROM command_receipts ORDER BY id"
        ).fetchall()

    replay = replay_command_receipt(config, request_payload)

    assert replay is not None
    assert replay.status == STATUS_REQUEST_STATE_UNCERTAIN
    assert replay.disposition == DISPOSITION_TERMINAL_UNCERTAIN
    assert calls == _expected_submit_calls()
    with sqlite3.connect(str(config.db_path)) as conn:
        assert (
            conn.execute("SELECT * FROM command_receipts ORDER BY id").fetchall()
            == rows_before
        )


def test_response_loss_replay_uses_only_current_exact_worker_selector(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config(tmp_path)
    worker = Worker(
        id="w-1",
        name="Alpha",
        status="active",
        space_id="space-1",
    )
    _seed(config, [worker], [_binding(worker)])
    calls: list[dict[str, Any]] = []
    request_payload = _request(request_id="response-loss-target")
    accepted = submit_command(
        config,
        request_payload,
        socket_client_factory=_factory(calls),
    )
    assert accepted.status == STATUS_ACCEPTED
    assert config.db_path is not None

    def stored_rows() -> tuple[list[tuple[Any, ...]], list[tuple[Any, ...]]]:
        with sqlite3.connect(str(config.db_path)) as conn:
            return (
                conn.execute("SELECT * FROM command_receipts ORDER BY id").fetchall(),
                conn.execute("SELECT * FROM events ORDER BY id").fetchall(),
            )

    rows_before = stored_rows()
    exact = replay_command_receipt(config, request_payload)
    changed_request = _request(request_id="response-loss-target", worker_id="w-2")
    changed = replay_command_receipt(config, changed_request)

    monkeypatch.setattr(
        command_submission,
        "_current_snapshot",
        lambda _config: pytest.fail(
            "read-only response-loss reconciliation must not consult current authority"
        ),
    )
    mutable_request = _request(request_id="response-loss-target")
    mutable_request["target"] = {"name": "Alpha"}
    mutable = replay_command_receipt(config, mutable_request)

    assert exact is not None
    assert exact.to_dict() == accepted.to_dict()
    assert changed is not None
    assert changed.status == STATUS_DUPLICATE_REQUEST
    assert changed.disposition == DISPOSITION_TERMINAL_REJECTED
    assert mutable is None
    assert calls == _expected_submit_calls()
    assert stored_rows() == rows_before


@pytest.mark.parametrize("damage", ["malformed_result", "illegal_state"])
def test_submit_command_illegal_or_malformed_terminal_receipt_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    damage: str,
) -> None:
    config = _config(tmp_path / damage)
    worker = Worker(id="w-1", name="Alpha", status="active")
    _seed(config, [worker], [_binding(worker)])
    calls: list[dict[str, Any]] = []
    request_id = f"damaged-{damage}"
    accepted = submit_command(
        config,
        _request(request_id=request_id),
        socket_client_factory=_factory(calls),
    )
    assert accepted.status == STATUS_ACCEPTED
    assert config.db_path is not None

    if damage == "malformed_result":
        with sqlite3.connect(str(config.db_path)) as conn:
            conn.execute(
                "UPDATE command_receipts SET result_json = '{' "
                "WHERE host_id = ? AND request_id = ?",
                (config.host_id, request_id),
            )
            conn.commit()
    else:
        real_reserve = command_submission.reserve_terminal_command_replay

        def illegal_reserve(*args: Any, **kwargs: Any) -> dict[str, Any]:
            result = real_reserve(*args, **kwargs)
            receipt = dict(result["receipt"])
            receipt["state"] = "illegal"
            return {**result, "receipt": receipt}

        monkeypatch.setattr(
            command_submission,
            "reserve_terminal_command_replay",
            illegal_reserve,
        )

    replay = submit_command(
        config,
        _request(request_id=request_id),
        socket_client_factory=lambda _config: pytest.fail(
            "damaged receipt replay must not create a socket client"
        ),
    )

    assert replay.status == STATUS_REQUEST_STATE_UNCERTAIN
    assert replay.disposition == DISPOSITION_TERMINAL_UNCERTAIN
    assert calls == _expected_submit_calls()


def _answer_request(
    *,
    request_id: str = "answer-1",
    dry_run: bool = False,
    pending_id: str = "pending-public",
    pending_fingerprint: str = "revision-public",
    choice_id: str = "choice-public",
) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "action": "answer_pending",
        "request_id": request_id,
        "dry_run": dry_run,
        "params": {
            "pending_id": pending_id,
            "pending_fingerprint": pending_fingerprint,
            "choice_id": choice_id,
        },
    }


@pytest.mark.parametrize("terminal_status", [STATUS_ACCEPTED, STATUS_REJECTED])
def test_answer_pending_migrated_v11_terminal_replays_without_current_authority(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    terminal_status: str,
) -> None:
    config = _config(tmp_path)
    assert config.db_path is not None
    request_payload = _answer_request(request_id=f"legacy-answer-{terminal_status}")
    request = CommandRequest.from_dict(request_payload)
    if terminal_status == STATUS_ACCEPTED:
        terminal = CommandEnvelope.from_result(
            request,
            ok=True,
            disposition=DISPOSITION_TERMINAL_ACCEPTED,
            status=terminal_status,
            result={
                "target": {"worker_id": "legacy-worker"},
                "pending": {
                    "id": "pending-public",
                    "fingerprint": "revision-public",
                },
                "choice": {"choice_id": "choice-public"},
                "delivery_state": "submitted",
                "transport_state": "submitted",
                "observed_pending_state": "pending_observation",
            },
        )
    else:
        terminal = CommandEnvelope.from_result(
            request,
            ok=False,
            disposition=DISPOSITION_TERMINAL_REJECTED,
            status=terminal_status,
            error={
                "code": terminal_status,
                "message": "legacy pending answer was rejected before send",
            },
        )
    raw_fingerprint = request.payload_fingerprint()
    expected_terminal = terminal.to_dict()
    legacy_terminal = dict(expected_terminal)
    legacy_terminal.pop("disposition")
    legacy_terminal["schema_version"] = 1
    result_json = json.dumps(
        legacy_terminal,
        sort_keys=True,
        separators=(",", ":"),
    )
    request_json = json.dumps(
        request.to_dict(),
        sort_keys=True,
        separators=(",", ":"),
    )
    with sqlite3.connect(str(config.db_path)) as conn:
        conn.executescript(
            store_sqlite.CREATE_LEGACY_COMMAND_RECEIPTS_TABLE
            + store_sqlite.CREATE_LEGACY_COMMANDS_TABLE
        )
        conn.execute(
            """
            INSERT INTO command_receipts (
                host_id, request_id, action, payload_fingerprint, status,
                result_json, created_at, completed_at, uncertain
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0)
            """,
            (
                config.host_id,
                request.request_id,
                request.action,
                raw_fingerprint,
                terminal_status,
                result_json,
                "2026-01-01T00:00:00+00:00",
                "2026-01-01T00:00:01+00:00",
            ),
        )
        conn.execute(
            """
            INSERT INTO commands (
                host_id, request_id, action, payload_fingerprint, status,
                dry_run, uncertain, request_json, result_json, created_at,
                reserved_at, completed_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, 0, 0, ?, ?, ?, ?, ?, ?)
            """,
            (
                config.host_id,
                request.request_id,
                request.action,
                raw_fingerprint,
                terminal_status,
                request_json,
                result_json,
                "2026-01-01T00:00:00+00:00",
                "2026-01-01T00:00:00+00:00",
                "2026-01-01T00:00:01+00:00",
                "2026-01-01T00:00:01+00:00",
            ),
        )
        conn.execute("PRAGMA user_version = 11")

    init_store(config.db_path)
    migrated = get_command_request(
        config.db_path,
        config.host_id,
        request.request_id or "",
    )
    assert migrated is not None
    assert migrated["canonical_version"] == 0
    assert migrated["canonical_fingerprint"] == raw_fingerprint
    assert migrated["public_worker_id"] == ""
    assert migrated["state"] == (
        "accepted" if terminal_status == STATUS_ACCEPTED else "rejected"
    )
    assert migrated["legacy_collision"] is False

    def command_rows() -> tuple[list[tuple[Any, ...]], list[tuple[Any, ...]]]:
        with sqlite3.connect(str(config.db_path)) as conn:
            return (
                conn.execute(
                    "SELECT * FROM command_receipts ORDER BY id"
                ).fetchall(),
                conn.execute("SELECT * FROM commands ORDER BY id").fetchall(),
            )

    rows_before = command_rows()

    def forbidden(*args: Any, **kwargs: Any) -> Any:
        pytest.fail("legacy terminal replay must not consult or mutate current authority")

    monkeypatch.setattr(command_submission, "_validate_pending_choice", forbidden)
    monkeypatch.setattr(command_submission, "_answer_pending", forbidden)
    monkeypatch.setattr(command_submission, "reserve_command_request", forbidden)

    read_only_replay = replay_command_receipt(config, request_payload)
    replay = submit_command(
        config,
        request_payload,
        socket_client_factory=forbidden,
    )
    changed_choice = submit_command(
        config,
        _answer_request(
            request_id=request.request_id or "",
            choice_id="changed-choice",
        ),
        socket_client_factory=forbidden,
    )

    assert read_only_replay is not None
    assert read_only_replay.to_dict() == expected_terminal
    assert replay.to_dict() == expected_terminal
    assert replay.disposition == (
        DISPOSITION_TERMINAL_ACCEPTED
        if terminal_status == STATUS_ACCEPTED
        else DISPOSITION_TERMINAL_REJECTED
    )
    assert changed_choice.status == STATUS_DUPLICATE_REQUEST
    assert changed_choice.disposition == DISPOSITION_TERMINAL_REJECTED
    assert command_rows() == rows_before


class _PendingClaim:
    def __init__(
        self,
        status: str,
        worker: Worker,
        *,
        claim_token: str | None,
        private_fingerprint: str = "binding-private",
        turn_target_value: str = "pane-secret",
        picker_ordinal: int = 2,
    ) -> None:
        self.status = status
        self.claim_token = claim_token
        self.worker_id = worker.id if status in {"claimed", "validated"} else None
        self.worker_fingerprint = worker.fingerprint if status in {"claimed", "validated"} else None
        self.binding_private_fingerprint = (
            private_fingerprint if status in {"claimed", "validated"} else None
        )
        self.turn_target_value = (
            turn_target_value if status in {"claimed", "validated"} else None
        )
        self.picker_ordinal = picker_ordinal if status in {"claimed", "validated"} else None


class _PendingSend:
    def __init__(
        self,
        status: str,
        worker: Worker,
        *,
        private_fingerprint: str = "binding-private",
        turn_target_value: str = "pane-secret",
        picker_ordinal: int = 2,
    ) -> None:
        self.status = status
        self.worker_id = worker.id if status == "started" else None
        self.worker_fingerprint = worker.fingerprint if status == "started" else None
        self.binding_private_fingerprint = private_fingerprint if status == "started" else None
        self.turn_target_value = turn_target_value if status == "started" else None
        self.picker_ordinal = picker_ordinal if status == "started" else None


def _patch_pending_store_flow(
    monkeypatch: pytest.MonkeyPatch,
    worker: Worker,
    *,
    claim_status: str = "claimed",
    private_fingerprint: str = "binding-private",
    picker_ordinal: int = 2,
    turn_target_value: str = "pane-secret",
    finish_result: bool = True,
) -> list[tuple[Any, ...]]:
    transitions: list[tuple[Any, ...]] = []

    def claim(
        db_path: Path,
        host_id: str,
        pending_id: str,
        pending_fingerprint: str,
        choice_id: str,
        *,
        claim: bool = True,
        observed_at: str | None = None,
    ) -> _PendingClaim:
        transitions.append(
            ("claim", claim, pending_id, pending_fingerprint, choice_id, observed_at)
        )
        status = claim_status
        token: str | None = "claim-private" if status == "claimed" else None
        if not claim and status == "claimed":
            status = "validated"
        return _PendingClaim(
            status,
            worker,
            claim_token=token,
            private_fingerprint=private_fingerprint,
            turn_target_value=turn_target_value,
            picker_ordinal=picker_ordinal,
        )

    def start(
        db_path: Path,
        host_id: str,
        claim_token: str,
        *,
        observed_at: str | None = None,
    ) -> _PendingSend:
        transitions.append(("start", claim_token, observed_at))
        return _PendingSend(
            "started",
            worker,
            private_fingerprint=private_fingerprint,
            turn_target_value=turn_target_value,
            picker_ordinal=picker_ordinal,
        )

    def finish_effect(
        *,
        host_id: str,
        claim_token: str,
        accepted: bool,
    ) -> Any:
        def apply(conn: sqlite3.Connection) -> None:
            transitions.append(("finish", claim_token, accepted))
            if not finish_result:
                raise RuntimeError("pending finish failed")

        return apply

    def abandon(db_path: Path, host_id: str, claim_token: str) -> bool:
        transitions.append(("abandon", claim_token))
        return True

    monkeypatch.setattr(command_submission, "claim_backend_pending_choice", claim)
    monkeypatch.setattr(command_submission, "start_backend_pending_choice_send", start)
    monkeypatch.setattr(
        command_submission,
        "backend_pending_choice_terminal_effect",
        finish_effect,
    )
    monkeypatch.setattr(command_submission, "abandon_backend_pending_choice_claim", abandon)
    return transitions


def _expected_answer_calls(
    *,
    pane_id: str = "pane-secret",
    ordinal: int = 2,
) -> list[dict[str, Any]]:
    return [
        *_expected_private_clear_calls(pane_id),
        {"method": "pane.send_input", "params": {"pane_id": pane_id, "text": str(ordinal), "keys": ["Enter"]}},
    ]


@pytest.mark.parametrize(
    ("payload", "expected_result"),
    [
        (
            {
                "schema_version": 1,
                "action": "send_instruction",
                "dry_run": True,
                "target": {"name": "Alpha", "space_id": "space-1"},
                "instruction": {"text": "hello"},
            },
            {
                "target": {"name": "Alpha", "space_id": "space-1"},
                "instruction": {"text": "hello"},
            },
        ),
        (
            _answer_request(request_id="", dry_run=True),
            {
                "pending": {
                    "id": "pending-public",
                    "fingerprint": "revision-public",
                },
                "choice": {"choice_id": "choice-public"},
                "delivery_state": "not_submitted",
            },
        ),
    ],
)
def test_mutation_dry_run_is_pure_without_store_snapshot_or_socket(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    payload: dict[str, Any],
    expected_result: dict[str, Any],
) -> None:
    config = _config(tmp_path, backend="cli")
    calls: list[str] = []

    def forbidden(*args: Any, **kwargs: Any) -> Any:
        calls.append("io")
        raise AssertionError("dry-run must not consult mutable command authority")

    monkeypatch.setattr(command_submission, "get_command_request", forbidden)
    monkeypatch.setattr(command_submission, "_current_snapshot", forbidden)
    monkeypatch.setattr(command_submission, "_validate_pending_choice", forbidden)
    monkeypatch.setattr(command_submission, "reserve_command_request", forbidden)

    envelope = submit_command(
        config,
        payload,
        socket_client_factory=forbidden,
    )

    assert envelope.ok is True
    assert envelope.status == "dry_run"
    assert envelope.result == expected_result
    assert calls == []
    assert config.db_path is not None
    assert not config.db_path.exists()
def test_answer_pending_reacquired_reservation_worker_drift_finishes_rejected(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config(tmp_path)
    current_worker = Worker(id="w-2", name="Beta", status="active")
    _seed(config, [current_worker], [_binding(current_worker)])
    payload = _answer_request(request_id="answer-reacquired")
    request = CommandRequest.from_dict(payload)
    canonical = build_canonical_mutation(request, public_worker_id="w-1")
    pending = command_submission._request_in_progress(request)
    assert config.db_path is not None
    initial = reserve_command_request(
        config.db_path,
        host_id=config.host_id,
        request_id=request.request_id or "",
        action=canonical.action,
        canonical_version=canonical.canonical_version,
        canonical_fingerprint=canonical.fingerprint,
        canonical_request_json=canonical.canonical_json,
        public_worker_id=canonical.public_worker_id,
        pending_result_json=store_sqlite.envelope_to_receipt_json(pending),
        legacy_raw_payload_fingerprint=request.payload_fingerprint(),
        owner_lease_seconds=1,
        now="2020-01-01T00:00:00+00:00",
    )
    assert initial["status"] == "reserved"
    transitions = _patch_pending_store_flow(monkeypatch, current_worker)
    calls: list[dict[str, Any]] = []

    drift = submit_command(
        config,
        payload,
        socket_client_factory=_factory(calls),
    )
    replay = submit_command(
        config,
        payload,
        socket_client_factory=_factory(calls),
    )

    assert drift.status == STATUS_DUPLICATE_REQUEST
    assert replay.to_dict() == drift.to_dict()
    assert calls == []
    assert transitions == [
        (
            "claim",
            False,
            "pending-public",
            "revision-public",
            "choice-public",
            None,
        )
    ]
    receipt = get_command_request(
        config.db_path,
        config.host_id,
        request.request_id or "",
    )
    assert receipt is not None
    assert receipt["state"] == "rejected"
    assert receipt["status"] == STATUS_DUPLICATE_REQUEST


def test_answer_pending_claims_sends_only_ordinal_and_replays_receipt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config(tmp_path)
    worker = Worker(id="w-1", name="Alpha", status="active")
    _seed(
        config,
        [worker],
        [_binding(worker, private_fingerprint="binding-private")],
    )
    transitions = _patch_pending_store_flow(monkeypatch, worker)
    monkeypatch.setattr(
        command_submission,
        "list_worker_bindings",
        lambda *args, **kwargs: pytest.fail(
            "answer_pending must use the binding authenticated by the claim API"
        ),
    )
    calls: list[dict[str, Any]] = []

    first = submit_command(
        config,
        _answer_request(),
        socket_client_factory=_factory(calls),
    )
    assert config.db_path is not None
    save_snapshot(
        config.db_path,
        Snapshot(
            host_id=config.host_id,
            updated_at="2026-01-01T00:01:00+00:00",
            workers=[worker],
            backend_health=[_degraded_backend()],
        ),
    )
    replay = submit_command(
        config,
        _answer_request(),
        socket_client_factory=_factory(calls),
    )
    changed_choice = submit_command(
        config,
        _answer_request(choice_id="different-choice"),
        socket_client_factory=lambda _config: pytest.fail(
            "choice collision must not create a socket client"
        ),
    )
    changed_pending_revision = submit_command(
        config,
        _answer_request(pending_fingerprint="different-revision"),
        socket_client_factory=lambda _config: pytest.fail(
            "pending collision must not create a socket client"
        ),
    )

    assert first.ok is True
    assert first.status == STATUS_ACCEPTED
    assert first.result == {
        "target": {"worker_id": "w-1"},
        "pending": {"id": "pending-public", "fingerprint": "revision-public"},
        "choice": {"choice_id": "choice-public"},
        "delivery_state": "submitted",
        "transport_state": "submitted",
        "observed_pending_state": "pending_observation",
    }
    assert replay.to_dict() == first.to_dict()
    assert changed_choice.status == STATUS_DUPLICATE_REQUEST
    assert changed_pending_revision.status == STATUS_DUPLICATE_REQUEST
    assert calls == _expected_answer_calls()
    assert transitions == [
        (
            "claim",
            False,
            "pending-public",
            "revision-public",
            "choice-public",
            None,
        ),
        (
            "claim",
            True,
            "pending-public",
            "revision-public",
            "choice-public",
            None,
        ),
        ("start", "claim-private", None),
        ("finish", "claim-private", True),
    ]

    assert config.db_path is not None
    receipt = _receipt_for_action(config.db_path, "cmd-host", "answer-1", "answer_pending")
    assert receipt is not None
    assert receipt["status"] == STATUS_ACCEPTED
    turns = turns_payload_from_store(config.db_path, config.host_id)["turns"]
    assert not any(turn.get("origin_command_id") == "answer-1" for turn in turns)


@pytest.mark.parametrize(
    "claim_status",
    ["not_found", "stale", "changed", "unknown_choice", "already_claimed"],
)
def test_answer_pending_changed_disappeared_stale_or_unknown_fails_before_socket(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    claim_status: str,
) -> None:
    config = _config(tmp_path / claim_status)
    worker = Worker(id="w-1", name="Alpha", status="active")
    _seed(
        config,
        [worker],
        [_binding(worker, private_fingerprint="binding-private")],
    )
    transitions = _patch_pending_store_flow(
        monkeypatch,
        worker,
        claim_status=claim_status,
    )
    calls: list[dict[str, Any]] = []

    envelope = submit_command(
        config,
        _answer_request(request_id=f"answer-{claim_status}"),
        socket_client_factory=_factory(calls),
    )

    assert envelope.ok is False
    assert envelope.status == STATUS_STALE_TARGET
    assert envelope.error == {
        "code": STATUS_STALE_TARGET,
        "message": "pending interaction changed or is no longer answerable",
        "details": {},
    }
    assert calls == []
    assert len(transitions) == 1
    assert transitions[0][0] == "claim"




def test_answer_pending_claim_race_closes_prepared_loser_without_second_send(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config(tmp_path)
    worker = Worker(id="w-1", name="Alpha", status="active")
    _seed(
        config,
        [worker],
        [_binding(worker, private_fingerprint="binding-private")],
    )
    transitions = _patch_pending_store_flow(monkeypatch, worker)
    original_claim = command_submission.claim_backend_pending_choice
    mutating_claim_count = 0

    def racing_claim(*args: Any, **kwargs: Any) -> _PendingClaim:
        nonlocal mutating_claim_count
        if kwargs.get("claim", True):
            mutating_claim_count += 1
            if mutating_claim_count == 2:
                transitions.append(("claim_race_lost",))
                return _PendingClaim("already_claimed", worker, claim_token=None)
        return original_claim(*args, **kwargs)

    monkeypatch.setattr(command_submission, "claim_backend_pending_choice", racing_claim)
    calls: list[dict[str, Any]] = []
    nested_result: list[Any] = []
    clients: list[_FakeSocketClient] = []

    def nested_factory(config: Config) -> _FakeSocketClient:
        client = _FakeSocketClient(calls)
        clients.append(client)
        return client

    class _RacingClient(_FakeSocketClient):
        def connect(self) -> "_FakeSocketClient":
            nested_result.append(
                submit_command(
                    config,
                    _answer_request(request_id="answer-race-nested"),
                    socket_client_factory=nested_factory,
                )
            )
            return self

    outer_client = _RacingClient(calls)
    clients.append(outer_client)
    outer = submit_command(
        config,
        _answer_request(request_id="answer-race-outer"),
        socket_client_factory=lambda _config: outer_client,
    )

    assert outer.status == STATUS_STALE_TARGET
    assert len(nested_result) == 1
    assert nested_result[0].status == STATUS_ACCEPTED
    assert calls == _expected_answer_calls()
    assert [client.close_count for client in clients] == [1, 1]


def test_answer_pending_post_send_failure_is_uncertain_and_not_retried(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config(tmp_path)
    worker = Worker(id="w-1", name="Alpha", status="active")
    _seed(
        config,
        [worker],
        [_binding(worker, private_fingerprint="binding-private")],
    )
    transitions = _patch_pending_store_flow(monkeypatch, worker)
    calls: list[dict[str, Any]] = []

    first = submit_command(
        config,
        _answer_request(request_id="answer-uncertain"),
        socket_client_factory=_factory(
            calls,
            raises=HerdrSocketDisconnectedError("disconnected"),
        ),
    )
    replay = submit_command(
        config,
        _answer_request(request_id="answer-uncertain"),
        socket_client_factory=_factory(calls),
    )

    assert first.status == STATUS_REQUEST_STATE_UNCERTAIN
    assert replay.status == STATUS_REQUEST_STATE_UNCERTAIN
    assert transitions[-1] == ("finish", "claim-private", False)
    assert calls == [
        *_expected_private_clear_calls(),
        {"method": "pane.send_input", "params": {"pane_id": "pane-secret", "text": "2", "keys": ["Enter"]}},
    ]
    assert config.db_path is not None
    receipt = _receipt_for_action(config.db_path,
    "cmd-host",
    "answer-uncertain",
    "answer_pending",)
    assert receipt is not None
    assert receipt["uncertain"] is True


def test_answer_pending_public_surfaces_recursively_exclude_private_route_values(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config(tmp_path)
    worker = Worker(id="w-1", name="Alpha", status="active")
    private_binding = "raw-binding-sentinel"
    private_target = "raw-target-sentinel"
    private_pane = "raw-pane-sentinel"
    _seed(
        config,
        [worker],
        [
            _binding(
                worker,
                value=private_target,
                private_fingerprint=private_binding,
                turn_target_kind="pane_id",
                turn_target_value=private_pane,
            )
        ],
    )
    _patch_pending_store_flow(
        monkeypatch,
        worker,
        private_fingerprint=private_binding,
        turn_target_value=private_pane,
        picker_ordinal=3,
    )
    calls: list[dict[str, Any]] = []

    envelope = submit_command(
        config,
        _answer_request(request_id="answer-private"),
        socket_client_factory=_factory(calls, pane_id=private_pane),
    )

    assert envelope.status == STATUS_ACCEPTED
    assert calls == _expected_answer_calls(
        pane_id=private_pane,
        ordinal=3,
    )
    assert config.db_path is not None
    receipt = _receipt_for_action(config.db_path,
    "cmd-host",
    "answer-private",
    "answer_pending",)
    assert receipt is not None
    with sqlite3.connect(str(config.db_path)) as conn:
        event_payloads = [
            json.loads(row[0])
            for row in conn.execute(
                "SELECT payload_json FROM events WHERE aggregate_id = ? ORDER BY id",
                ("answer-private",),
            ).fetchall()
        ]
    public_surfaces = [
        envelope.to_dict(),
        json.loads(receipt["result_json"]),
        *event_payloads,
    ]
    encoded = json.dumps(public_surfaces)
    assert private_binding not in encoded
    assert private_target not in encoded
    assert private_pane not in encoded
    for surface in public_surfaces:
        _assert_no_private_json(surface)


def test_answer_pending_integrates_with_durable_two_phase_claim(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config(tmp_path)
    worker = Worker(id="w-1", name="Alpha", status="active")
    binding = _binding(
        worker,
        private_fingerprint="binding-private",
        turn_target_kind="pane_id",
        turn_target_value="pane-secret",
    )
    _seed(config, [worker], [binding])
    assert config.db_path is not None
    assert apply_backend_pending_observation(
        config.db_path,
        config.host_id,
        worker.id,
        PendingObservation(
            kind="open_prompt",
            question="Choose a safe option",
            pending_kind="choice",
            choices=(
                PendingObservedChoice(
                    choice_id="choice-aaaaaaaaaaaaaaaaaaaaaaaa",
                    label="First",
                    picker_ordinal=1,
                ),
                PendingObservedChoice(
                    choice_id="choice-bbbbbbbbbbbbbbbbbbbbbbbb",
                    label="Second",
                    picker_ordinal=2,
                ),
            ),
            revision_digest="private-revision-digest",
        ),
        binding_private_fingerprint=binding.private_fingerprint,
        observed_turn_target_value=binding.turn_target_value,
    )
    pending_before = pending_payload_from_store(config.db_path, config.host_id)
    assert len(pending_before["pending_interactions"]) == 1
    interaction = pending_before["pending_interactions"][0]
    calls: list[dict[str, Any]] = []

    envelope = submit_command(
        config,
        _answer_request(
            request_id="answer-real-claim",
            pending_id=interaction["id"],
            pending_fingerprint=interaction["fingerprint"],
            choice_id=interaction["choices"][1]["choice_id"],
        ),
        socket_client_factory=_factory(calls),
    )

    assert envelope.status == STATUS_ACCEPTED
    assert envelope.result == {
        "target": {"worker_id": worker.id},
        "pending": {
            "id": interaction["id"],
            "fingerprint": interaction["fingerprint"],
        },
        "choice": {"choice_id": interaction["choices"][1]["choice_id"]},
        "delivery_state": "submitted",
        "transport_state": "submitted",
        "observed_pending_state": "pending_observation",
    }
    assert calls == _expected_answer_calls(ordinal=2)
    pending_after = pending_payload_from_store(config.db_path, config.host_id)
    assert pending_after["pending_interactions"] == []


@pytest.mark.parametrize("start_status", ["changed", "stale", "binding_changed"])
def test_answer_pending_post_receipt_send_start_cas_change_is_uncertain_without_pane_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    start_status: str,
) -> None:
    config = _config(tmp_path / start_status)
    worker = Worker(id="w-1", name="Alpha", status="active")
    _seed(
        config,
        [worker],
        [_binding(worker, private_fingerprint="binding-private")],
    )
    transitions = _patch_pending_store_flow(monkeypatch, worker)

    def changed_start(
        db_path: Path,
        host_id: str,
        claim_token: str,
        *,
        observed_at: str | None = None,
    ) -> _PendingSend:
        transitions.append(("start", claim_token, start_status))
        return _PendingSend(start_status, worker)

    monkeypatch.setattr(
        command_submission,
        "start_backend_pending_choice_send",
        changed_start,
    )
    calls: list[dict[str, Any]] = []

    envelope = submit_command(
        config,
        _answer_request(request_id=f"answer-start-{start_status}"),
        socket_client_factory=_factory(calls),
    )

    assert envelope.status == STATUS_REQUEST_STATE_UNCERTAIN
    assert envelope.error == {
        "code": STATUS_REQUEST_STATE_UNCERTAIN,
        "message": "previous request state is uncertain; not retrying mutation",
        "details": {},
    }
    assert calls == []
    assert transitions[-2:] == [
        ("abandon", "claim-private"),
        ("finish", "claim-private", False),
    ]
    assert config.db_path is not None
    receipt = get_command_request(
        config.db_path,
        config.host_id,
        f"answer-start-{start_status}",
    )
    assert receipt is not None
    assert receipt["state"] == "uncertain"



@pytest.mark.parametrize(
    ("claim_released", "expected_status", "expected_state"),
    [
        (True, STATUS_PENDING, "reserved"),
        (False, STATUS_REQUEST_STATE_UNCERTAIN, "uncertain"),
    ],
)
def test_answer_pending_send_start_exception_is_retryable_only_after_claim_release(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    claim_released: bool,
    expected_status: str,
    expected_state: str,
) -> None:
    config = _config(tmp_path / expected_state)
    worker = Worker(id="w-1", name="Alpha", status="active")
    _seed(
        config,
        [worker],
        [_binding(worker, private_fingerprint="binding-private")],
    )
    transitions = _patch_pending_store_flow(monkeypatch, worker)

    if not claim_released:
        def fail_abandon(db_path: Path, host_id: str, claim_token: str) -> bool:
            transitions.append(("abandon_failed", claim_token))
            return False

        monkeypatch.setattr(
            command_submission,
            "abandon_backend_pending_choice_claim",
            fail_abandon,
        )

    def fail_before_send_start(*args: Any, **kwargs: Any) -> Any:
        raise HerdrSocketTimeoutError("send-start unavailable before commit")

    monkeypatch.setattr(
        command_submission,
        "mark_command_send_started",
        fail_before_send_start,
    )
    client = _FakeSocketClient([])
    envelope = submit_command(
        config,
        _answer_request(request_id="answer-mark-failure"),
        socket_client_factory=lambda _config: client,
    )

    assert envelope.status == expected_status
    assert client.close_count == 1
    assert client.calls == []
    assert transitions[-1] == (
        ("abandon", "claim-private")
        if claim_released
        else ("abandon_failed", "claim-private")
    )
    assert config.db_path is not None
    receipt = get_command_request(
        config.db_path,
        config.host_id,
        "answer-mark-failure",
    )
    assert receipt is not None
    assert receipt["state"] == expected_state

def test_answer_pending_socket_setup_failure_precedes_claim_and_stays_retryable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config(tmp_path)
    worker = Worker(id="w-1", name="Alpha", status="active")
    _seed(
        config,
        [worker],
        [_binding(worker, private_fingerprint="replacement-private")],
    )
    transitions = _patch_pending_store_flow(
        monkeypatch,
        worker,
        private_fingerprint="claimed-private",
    )

    connect_ok = {"value": False}

    def flaky_factory(config: Config) -> Any:
        if not connect_ok["value"]:
            raise OSError("socket unavailable")
        return _FakeSocketClient([])

    envelope = submit_command(
        config,
        _answer_request(request_id="answer-setup-failed"),
        socket_client_factory=flaky_factory,
    )

    # The socket could not be reached, before any pending choice was claimed and
    # before any transmission. That is a safe pre-send transient.
    assert envelope.status == STATUS_BACKEND_UNAVAILABLE
    assert envelope.disposition == DISPOSITION_NO_RECEIPT
    assert envelope.error == {
        "code": STATUS_BACKEND_UNAVAILABLE,
        "message": "Herdr socket could not be reached",
        "details": {},
    }
    # Only the read-only validation ran; nothing was claimed or sent.
    assert transitions == [
        (
            "claim",
            False,
            "pending-public",
            "revision-public",
            "choice-public",
            None,
        )
    ]
    assert config.db_path is not None
    assert _receipt_for_action(
        config.db_path,
        config.host_id,
        "answer-setup-failed",
        "answer_pending",
    ) is None

    # The same request ID answers exactly once after the socket recovers.
    connect_ok["value"] = True
    recovered = submit_command(
        config,
        _answer_request(request_id="answer-setup-failed"),
        socket_client_factory=flaky_factory,
    )
    assert recovered.status == STATUS_ACCEPTED
    assert recovered.disposition == DISPOSITION_TERMINAL_ACCEPTED
    receipt = _receipt_for_action(
        config.db_path,
        config.host_id,
        "answer-setup-failed",
        "answer_pending",
    )
    assert receipt is not None
    assert receipt["state"] == "accepted"
    assert ("finish", "claim-private", True) in transitions


def test_stable_owner_pending_command_survives_worker_churn_and_source_wins(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    assert config.db_path is not None
    stable_key = "wsk1_" + ("6" * 64)
    request_id = "stable-owner-request"
    worker_a = Worker(
        id="owner-worker-a",
        name="Owner Worker A",
        status="active",
        space_id="owner-space-a",
        fingerprint="owner-fingerprint-a",
        meta={"stable_key": stable_key, "stable_key_version": 1},
    )
    worker_b = Worker(
        id="owner-worker-b",
        name="Owner Worker B",
        status="waiting",
        space_id="owner-space-b",
        fingerprint="owner-fingerprint-b",
        meta={"stable_key": stable_key, "stable_key_version": 1},
    )
    binding_a = _binding(
        worker_a,
        value="owner-agent-a-private",
        private_fingerprint="owner-binding-a-private",
        turn_target_value="owner-pane-a-private",
    )
    _seed(config, [worker_a], [binding_a])
    calls: list[dict[str, Any]] = []

    accepted = submit_command(
        config,
        _request(request_id=request_id, worker_id=worker_a.id),
        socket_client_factory=_factory(calls, pane_id="owner-pane-a-private"),
    )
    assert accepted.status == STATUS_ACCEPTED
    command_before = next(
        turn
        for turn in turns_payload_from_store(config.db_path, config.host_id)["turns"]
        if turn.get("origin_command_id") == request_id
    )
    with sqlite3.connect(str(config.db_path)) as conn:
        command_sequence = conn.execute(
            """
            SELECT list_sequence
            FROM turns
            WHERE host_id = ? AND turn_id = ?
            """,
            (config.host_id, command_before["id"]),
        ).fetchone()[0]

    save_snapshot(
        config.db_path,
        Snapshot(
            host_id=config.host_id,
            updated_at="2026-07-13T04:01:00+00:00",
            workers=[worker_b],
            backend_health=[_healthy_backend()],
        ),
    )
    upsert_worker_bindings(
        config.db_path,
        [
            _binding(
                worker_b,
                value="owner-agent-b-private",
                private_fingerprint="owner-binding-b-private",
                turn_target_value="owner-pane-b-private",
            )
        ],
    )
    command_after = upsert_command_pending_turn(
        config.db_path,
        config.host_id,
        worker_b,
        request_id=request_id,
        instruction_text="hello",
        observed_at="2026-07-13T04:01:01+00:00",
    )
    assert command_after is not None
    assert command_after["id"] == command_before["id"]
    assert command_after["worker_id"] == worker_b.id
    assert command_after["worker_fingerprint"] == worker_b.fingerprint
    assert command_after["space_id"] == worker_b.space_id
    assert command_after["complete"] is False
    assert command_after["has_open_turn"] is True
    with sqlite3.connect(str(config.db_path)) as conn:
        rows = conn.execute(
            """
            SELECT turn_id, list_sequence
            FROM turns
            WHERE host_id = ?
              AND json_extract(payload_json, '$.origin_command_id') = ?
            """,
            (config.host_id, request_id),
        ).fetchall()
    assert rows == [(command_before["id"], command_sequence)]

    raw_source = "019f5590-3333-7333-8333-333333333333"
    assert merge_turn_content(
        config.db_path,
        config.host_id,
        worker_b.id,
        {
            "source_turn_id": raw_source,
            "user_text": "hello",
            "assistant_final_text": "durable owner answer",
            "complete": True,
            "has_open_turn": False,
        },
        observed_at="2026-07-13T04:01:02+00:00",
    ) == 1
    completed_payload = turns_payload_from_store(
        config.db_path,
        config.host_id,
        schema_version=2,
    )
    completed_source = next(
        turn
        for turn in completed_payload["turns"]
        if turn.get("assistant_final_text") == "durable owner answer"
    )
    assert completed_source["id"] == command_before["id"]
    assert completed_source["origin_command_id"] == request_id
    assert completed_source["source_turn_id"].startswith("turnsrc-")
    assert completed_source["source_turn_id"] != raw_source
    assert completed_source["complete"] is True
    assert completed_source["has_open_turn"] is False
    with sqlite3.connect(str(config.db_path)) as conn:
        final_root = json.loads(
            conn.execute(
                """
                SELECT payload_json
                FROM connector_outbox
                WHERE host_id = ?
                  AND connector = 'turn-final'
                  AND delivery_kind = 'final_ready'
                  AND turn_id = ?
                """,
                (config.host_id, completed_source["id"]),
            ).fetchone()[0]
        )
        source_before_retry = conn.execute(
            """
            SELECT turns.turn_id, turns.list_sequence, turns.payload_json,
                   revisions.content_revision
            FROM turns
            JOIN turn_content_revisions AS revisions
              ON revisions.host_id = turns.host_id
             AND revisions.turn_id = turns.turn_id
             AND revisions.is_current = 1
            WHERE turns.host_id = ? AND turns.turn_id = ?
            """,
            (config.host_id, completed_source["id"]),
        ).fetchone()
        list_state_before_retry = conn.execute(
            """
            SELECT next_sequence, traversal_generation
            FROM turn_list_hosts
            WHERE host_id = ?
            """,
            (config.host_id,),
        ).fetchone()
    # Adopt-in-place preserves the command row's turn id, so the final's own
    # turn IS the working predecessor; the payload pointer is correctly
    # omitted (it is only stamped when the ids differ).
    assert "working_predecessor_turn_id" not in final_root

    source_wins = upsert_command_pending_turn(
        config.db_path,
        config.host_id,
        worker_b,
        request_id=request_id,
        instruction_text="hello",
        observed_at="2026-07-13T04:01:03+00:00",
    )
    assert source_wins is not None
    assert source_wins["id"] == completed_source["id"]
    assert source_wins["source_turn_id"] == completed_source["source_turn_id"]
    assert source_wins["assistant_final_text"] == "durable owner answer"
    assert source_wins["complete"] is True
    assert source_wins["has_open_turn"] is False
    with sqlite3.connect(str(config.db_path)) as conn:
        source_after_retry = conn.execute(
            """
            SELECT turns.turn_id, turns.list_sequence, turns.payload_json,
                   revisions.content_revision
            FROM turns
            JOIN turn_content_revisions AS revisions
              ON revisions.host_id = turns.host_id
             AND revisions.turn_id = turns.turn_id
             AND revisions.is_current = 1
            WHERE turns.host_id = ? AND turns.turn_id = ?
            """,
            (config.host_id, completed_source["id"]),
        ).fetchone()
        list_state_after_retry = conn.execute(
            """
            SELECT next_sequence, traversal_generation
            FROM turn_list_hosts
            WHERE host_id = ?
            """,
            (config.host_id,),
        ).fetchone()
        origin_rows = conn.execute(
            """
            SELECT turn_id, json_extract(payload_json, '$.source_turn_id')
            FROM turns
            WHERE host_id = ?
              AND json_extract(payload_json, '$.origin_command_id') = ?
            """,
            (config.host_id, request_id),
        ).fetchall()
        foreign_keys = conn.execute("PRAGMA foreign_key_check").fetchall()
    assert source_after_retry == source_before_retry
    assert list_state_after_retry == list_state_before_retry
    assert origin_rows == [
        (completed_source["id"], completed_source["source_turn_id"])
    ]
    assert foreign_keys == []
    assert calls == _expected_submit_calls(
        "owner-agent-a-private",
        pane_id="owner-pane-a-private",
    )

    receipt = _receipt_for_action(config.db_path,
    config.host_id,
    request_id,
    "send_instruction",)
    assert receipt is not None
    with sqlite3.connect(str(config.db_path)) as conn:
        event_payloads = [
            json.loads(str(row[0]))
            for row in conn.execute(
                "SELECT payload_json FROM events WHERE aggregate_id = ? ORDER BY id",
                (request_id,),
            ).fetchall()
        ]
    public_surfaces = [
        accepted.to_dict(),
        command_before,
        command_after,
        completed_payload,
        source_wins,
        json.loads(receipt["result_json"]),
        *event_payloads,
    ]
    encoded = json.dumps(public_surfaces, sort_keys=True)
    for private_value in (
        raw_source,
        "owner-agent-a-private",
        "owner-binding-a-private",
        "owner-pane-a-private",
        "owner-agent-b-private",
        "owner-binding-b-private",
        "owner-pane-b-private",
    ):
        assert private_value not in encoded
    for surface in public_surfaces:
        _assert_no_private_json(surface)


def test_completed_source_command_replay_after_owner_churn_adopts_current_projection_without_reopen(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    assert config.db_path is not None
    stable_key = "wsk1_" + ("8" * 64)
    request_id = "completed-owner-request"
    raw_source = "019f5590-5555-7555-8555-555555555555"
    worker_a = Worker(
        id="completed-worker-a",
        name="Completed Worker A",
        status="active",
        space_id="completed-space-a",
        fingerprint="completed-fingerprint-a",
        meta={"stable_key": stable_key, "stable_key_version": 1},
    )
    worker_b = Worker(
        id="completed-worker-b",
        name="Completed Worker B",
        status="waiting",
        space_id="completed-space-b",
        fingerprint="completed-fingerprint-b",
        meta={"stable_key": stable_key, "stable_key_version": 1},
    )
    _seed(config, [worker_a])
    command = upsert_command_pending_turn(
        config.db_path,
        config.host_id,
        worker_a,
        request_id=request_id,
        instruction_text="complete this command",
        observed_at="2026-07-13T06:00:00+00:00",
    )
    assert command is not None
    assert merge_turn_content(
        config.db_path,
        config.host_id,
        worker_a.id,
        {
            "source_turn_id": raw_source,
            "user_text": "complete this command",
            "assistant_final_text": "terminal answer from A",
            "complete": True,
            "has_open_turn": False,
        },
        observed_at="2026-07-13T06:00:01+00:00",
    ) == 1
    before_public = turns_payload_from_store(
        config.db_path,
        config.host_id,
        schema_version=2,
    )
    source_before = next(
        turn
        for turn in before_public["turns"]
        if turn.get("origin_command_id") == request_id
        and turn.get("source_turn_id")
    )
    assert source_before["worker_id"] == worker_a.id
    assert source_before["assistant_final_text"] == "terminal answer from A"
    assert source_before["complete"] is True
    assert source_before["has_open_turn"] is False

    def durable_identity() -> tuple[Any, ...]:
        with sqlite3.connect(str(config.db_path)) as conn:
            row = conn.execute(
                """
                SELECT turns.turn_id,
                       turns.list_sequence,
                       json_extract(turns.payload_json, '$.source_turn_id'),
                       revisions.content_revision,
                       outbox.id,
                       outbox.delivery_key,
                       json_extract(outbox.payload_json, '$.final_identity'),
                       outbox.status
                FROM turns
                JOIN turn_content_revisions AS revisions
                  ON revisions.host_id = turns.host_id
                 AND revisions.turn_id = turns.turn_id
                 AND revisions.is_current = 1
                JOIN connector_outbox AS outbox
                  ON outbox.host_id = turns.host_id
                 AND outbox.turn_id = turns.turn_id
                 AND outbox.content_revision = revisions.content_revision
                 AND outbox.delivery_kind = 'final_ready'
                WHERE turns.host_id = ?
                  AND turns.turn_id = ?
                """,
                (config.host_id, source_before["id"]),
            ).fetchone()
            assert row is not None
            return tuple(row)

    durable_before = durable_identity()
    with sqlite3.connect(str(config.db_path)) as conn:
        list_state_before = conn.execute(
            """
            SELECT next_sequence, traversal_generation
            FROM turn_list_hosts
            WHERE host_id = ?
            """,
            (config.host_id,),
        ).fetchone()

    save_snapshot(
        config.db_path,
        Snapshot(
            host_id=config.host_id,
            updated_at="2026-07-13T06:01:00+00:00",
            workers=[worker_b],
            backend_health=[_healthy_backend()],
        ),
    )
    replayed = upsert_command_pending_turn(
        config.db_path,
        config.host_id,
        worker_b,
        request_id=request_id,
        instruction_text="complete this command",
        observed_at="2026-07-13T06:01:01+00:00",
    )
    assert replayed is not None
    assert replayed["id"] == source_before["id"]
    assert replayed["source_turn_id"] == source_before["source_turn_id"]
    assert replayed["worker_id"] == worker_b.id
    assert replayed["worker_fingerprint"] == worker_b.fingerprint
    assert replayed["space_id"] == worker_b.space_id
    assert replayed["assistant_final_text"] == "terminal answer from A"
    assert replayed["complete"] is True
    assert replayed["has_open_turn"] is False

    after_public = turns_payload_from_store(
        config.db_path,
        config.host_id,
        schema_version=2,
    )
    source_after = next(
        turn
        for turn in after_public["turns"]
        if turn.get("id") == source_before["id"]
    )
    assert source_after["worker_id"] == worker_b.id
    assert source_after["worker_fingerprint"] == worker_b.fingerprint
    assert source_after["space_id"] == worker_b.space_id
    assert source_after["assistant_final_text"] == "terminal answer from A"
    assert source_after["complete"] is True
    assert source_after["has_open_turn"] is False
    assert durable_identity() == durable_before
    with sqlite3.connect(str(config.db_path)) as conn:
        list_state_after = conn.execute(
            """
            SELECT next_sequence, traversal_generation
            FROM turn_list_hosts
            WHERE host_id = ?
            """,
            (config.host_id,),
        ).fetchone()
        origin_rows = conn.execute(
            """
            SELECT turn_id,
                   json_extract(payload_json, '$.source_turn_id'),
                   json_extract(payload_json, '$.complete')
            FROM turns
            WHERE host_id = ?
              AND json_extract(payload_json, '$.origin_command_id') = ?
            """,
            (config.host_id, request_id),
        ).fetchall()
        root_count = conn.execute(
            """
            SELECT COUNT(*)
            FROM connector_outbox
            WHERE host_id = ?
              AND delivery_kind = 'final_ready'
              AND turn_id = ?
            """,
            (config.host_id, source_before["id"]),
        ).fetchone()[0]
        current_revision_count = conn.execute(
            """
            SELECT COUNT(*)
            FROM turn_content_revisions
            WHERE host_id = ? AND turn_id = ? AND is_current = 1
            """,
            (config.host_id, source_before["id"]),
        ).fetchone()[0]
        foreign_keys = conn.execute("PRAGMA foreign_key_check").fetchall()
    assert list_state_after == list_state_before
    assert origin_rows == [
        (source_before["id"], source_before["source_turn_id"], 1)
    ]
    assert root_count == 1
    assert current_revision_count == 1
    assert foreign_keys == []
    assert raw_source not in json.dumps(after_public, sort_keys=True)


def test_terminal_id_binding_falls_back_to_agent_list_when_agent_get_refuses(monkeypatch) -> None:
    # Herdr 0.7.5 regression: agent.get no longer resolves terminal-id targets;
    # agent.list still publishes terminal_id -> pane_id. A definite error from
    # agent.get must fall back to the listing instead of terminalizing.
    from tendwire import command_submission as cs
    from tendwire.backends.herdr_protocol import HerdrErrorResponse

    calls = []

    def fake_socket_request(client, method, params, *, timeout):
        calls.append(method)
        if method == "agent.get":
            raise HerdrErrorResponse({"code": "agent_not_found", "message": "agent target term_new not found"}, "req-1")
        if method == "agent.list":
            return {
                "agents": [
                    {"terminal_id": "term_other", "pane_id": "w1:p1"},
                    {"terminal_id": "term_new", "pane_id": "w1:p9"},
                ]
            }
        raise AssertionError(f"unexpected method {method}")

    monkeypatch.setattr(cs, "_socket_request", fake_socket_request)

    class _Binding:
        target_kind = "terminal_id"
        target_value = "term_new"

    pane_id = cs._private_pane_id_for_binding(object(), _Binding(), timeout=5.0)
    assert pane_id == "w1:p9"
    assert calls == ["agent.get", "agent.list"]

    # A non-terminal binding kind must NOT consult the listing: the original
    # error propagates.
    class _SessionBinding:
        target_kind = "agent_session"
        target_value = "sess-1"

    calls.clear()
    try:
        cs._private_pane_id_for_binding(object(), _SessionBinding(), timeout=5.0)
        raise AssertionError("expected HerdrErrorResponse to propagate")
    except HerdrErrorResponse:
        pass
    assert calls == ["agent.get"]


def test_terminal_id_agent_list_fallback_rejects_ambiguous_matches(monkeypatch) -> None:
    from tendwire import command_submission as cs
    from tendwire.backends.herdr_protocol import HerdrErrorResponse

    def fake_socket_request(client, method, params, *, timeout):
        if method == "agent.get":
            raise HerdrErrorResponse(
                {"code": "agent_not_found", "message": "target not found"},
                "req-1",
            )
        if method == "agent.list":
            return {
                "agents": [
                    {"terminal_id": "term-shared", "pane_id": "w1:p1"},
                    {"terminal_id": "term-shared", "pane_id": "w1:p2"},
                ]
            }
        raise AssertionError(f"unexpected method {method}")

    monkeypatch.setattr(cs, "_socket_request", fake_socket_request)

    class _Binding:
        target_kind = "terminal_id"
        target_value = "term-shared"

    with pytest.raises(ValueError, match="ambiguous agent.list terminal_id match"):
        cs._private_pane_id_for_binding(object(), _Binding(), timeout=5.0)


@pytest.mark.parametrize(
    ("agents", "expected"),
    [
        (
            [
                {"terminal_id": "term-shared", "pane_id": "w1:p1"},
                {"terminal_id": "term-shared", "pane_id": "w1:p1"},
            ],
            "w1:p1",
        ),
        ([], ""),
    ],
)
def test_terminal_id_agent_list_fallback_uses_one_unique_mapping(
    agents,
    expected,
) -> None:
    from tendwire import command_submission as cs

    assert (
        cs._pane_id_from_terminal_listing(
            {"agents": agents},
            "term-shared",
        )
        == expected
    )


@pytest.mark.parametrize(
    "listing",
    [
        None,
        {},
        {"agents": {}},
        {"agents": ["not-an-agent"]},
        {"agents": [{"terminal_id": "term-shared"}]},
        {"agents": [{"terminal_id": "", "pane_id": "w1:p1"}]},
        {"agents": [{"terminal_id": "term-shared", "pane_id": ""}]},
        {"agents": [{"terminal_id": "term-shared", "pane_id": 7}]},
    ],
)
def test_terminal_id_agent_list_fallback_rejects_malformed_listing(listing) -> None:
    from tendwire import command_submission as cs

    with pytest.raises(ValueError, match="invalid agent.list"):
        cs._pane_id_from_terminal_listing(listing, "term-shared")
