"""Receipt authority over mutable worker resolution for retried commands.

Goal 11 made ``request_id`` the sole idempotency key. This module proves the
follow-up property: an existing receipt decides its own retry from stored
evidence, before any mutable worker snapshot is consulted. A worker that
vanishes, is renamed, or is recycled must never turn a live receipt into a
no-receipt failure, and must never let one request mutate the backend twice.
"""

from __future__ import annotations

import json
import sqlite3
import threading
from pathlib import Path
from typing import Any

import pytest

import tendwire.command_submission as command_submission
import tendwire.store.sqlite as store_sqlite

from tendwire.backends.herdr_socket import HerdrSocketTimeoutError
from tendwire.command_submission import replay_command_receipt, submit_command
from tendwire.config import Config
from tendwire.core.commands import (
    DISPOSITION_IN_PROGRESS,
    DISPOSITION_NO_RECEIPT,
    DISPOSITION_TERMINAL_ACCEPTED,
    DISPOSITION_TERMINAL_REJECTED,
    DISPOSITION_TERMINAL_UNCERTAIN,
    STATUS_ACCEPTED,
    STATUS_BACKEND_UNSUPPORTED,
    STATUS_DUPLICATE_REQUEST,
    STATUS_INVALID_REQUEST,
    STATUS_PENDING,
    STATUS_REQUEST_STATE_UNCERTAIN,
    CommandRequest,
    build_selector_proof,
    is_selector_proof,
)
from tendwire.core.models import BackendHealth, Snapshot, Worker, WorkerBinding
from tendwire.daemon_api import TendwireDaemonAPI
from tendwire.store.sqlite import (
    get_command_request,
    init_store,
    run_store_maintenance,
    save_snapshot,
    store_status,
    upsert_worker_bindings,
)


HOST_ID = "cmd-host"

RECEIPT_STATES = ("reserved", "send_started", "accepted", "rejected", "uncertain")
SELECTOR_KINDS = (
    "worker_id",
    "worker_id_fingerprint",
    "name",
    "space_id",
    "name_and_space",
)

_EXPECTED_REPLAY = {
    "reserved": (STATUS_PENDING, DISPOSITION_IN_PROGRESS),
    "send_started": (STATUS_PENDING, DISPOSITION_IN_PROGRESS),
    "accepted": (STATUS_ACCEPTED, DISPOSITION_TERMINAL_ACCEPTED),
    "rejected": (STATUS_BACKEND_UNSUPPORTED, DISPOSITION_TERMINAL_REJECTED),
    "uncertain": (STATUS_REQUEST_STATE_UNCERTAIN, DISPOSITION_TERMINAL_UNCERTAIN),
}

# What reaching each state costs at the backend. Only a send that was actually
# attempted puts text on a pane; the rest fail earlier.
_SENDS_TO_REACH = {
    "reserved": [],
    "send_started": [],
    "accepted": ["hello"],
    "rejected": [],
    "uncertain": ["hello"],
}


def _config(tmp_path: Path) -> Config:
    return Config(
        host_id=HOST_ID,
        data_dir=tmp_path,
        db_path=tmp_path / "commands.db",
        herdr_backend="socket",
        herdr_timeout_seconds=5.0,
    )


def _worker(
    *,
    worker_id: str = "w-1",
    name: str = "Alpha",
    space_id: str | None = "space-1",
    status: str = "active",
) -> Worker:
    return Worker(id=worker_id, name=name, status=status, space_id=space_id)


def _binding(worker: Worker, *, sendable: bool = True) -> WorkerBinding:
    # Each worker owns a distinct private route. Sharing one would make every
    # worker an ambiguous backend target and mask what these tests measure.
    return WorkerBinding(
        host_id=HOST_ID,
        worker_id=worker.id,
        worker_fingerprint=worker.fingerprint,
        backend="herdr",
        target_kind="agent_id",
        target_value=f"agent-{worker.id}",
        turn_target_kind="pane_id",
        turn_target_value=f"pane-{worker.id}",
        sendable=sendable,
        reason=None if sendable else "not_sendable",
        observed_at="2026-01-01T00:00:00+00:00",
        private_fingerprint=f"private-{worker.id}",
    )


def _health(status: str = "healthy") -> BackendHealth:
    return BackendHealth(
        name="herdr",
        status=status,
        outcome="healthy_non_empty" if status == "healthy" else "timeout",
        observed_at="2026-01-01T00:00:00+00:00",
        counts={"workers": 1},
    )


def _snapshot(
    workers: list[Worker],
    *,
    health: str = "healthy",
    updated_at: str = "2026-01-01T00:00:00+00:00",
) -> Snapshot:
    return Snapshot(
        host_id=HOST_ID,
        updated_at=updated_at,
        workers=workers,
        backend_health=[_health(health)],
    )


def _seed(
    config: Config,
    workers: list[Worker],
    bindings: list[WorkerBinding],
) -> None:
    assert config.db_path is not None
    init_store(config.db_path)
    save_snapshot(config.db_path, _snapshot(workers))
    if bindings:
        upsert_worker_bindings(config.db_path, bindings)


def _remove_workers(config: Config, *, health: str = "healthy") -> None:
    """Publish a newer authoritative snapshot in which the worker is gone."""
    assert config.db_path is not None
    save_snapshot(
        config.db_path,
        _snapshot([], health=health, updated_at="2026-01-01T00:05:00+00:00"),
    )


def _selector(kind: str, worker: Worker) -> dict[str, Any]:
    return {
        "worker_id": {"worker_id": worker.id},
        "worker_id_fingerprint": {
            "worker_id": worker.id,
            "worker_fingerprint": worker.fingerprint,
        },
        "name": {"name": worker.name},
        "space_id": {"space_id": worker.space_id},
        "name_and_space": {"name": worker.name, "space_id": worker.space_id},
    }[kind]


def _request(
    *,
    request_id: str,
    target: dict[str, Any],
    text: str = "hello",
) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "action": "send_instruction",
        "request_id": request_id,
        "dry_run": False,
        "target": dict(target),
        "instruction": {"text": text},
    }


class _FakeSocketClient:
    def __init__(
        self,
        calls: list[dict[str, Any]],
        *,
        raises: BaseException | None = None,
    ) -> None:
        self.calls = calls
        self.raises = raises

    def connect(self) -> "_FakeSocketClient":
        return self

    def request(
        self,
        method: str,
        params: dict[str, Any],
        *,
        timeout: float | None = None,
    ) -> dict[str, Any]:
        self.calls.append({"method": method, "params": dict(params)})
        if self.raises is not None and method == "agent.prompt":
            raise self.raises
        if method == "agent.get":
            return {"result": {"agent": {"pane_id": "pane-secret"}}}
        if method == "pane.read":
            return {
                "type": "pane_read",
                "read": {"text": "Completed previous turn.\n── status: idle ──"},
            }
        if method == "agent.prompt":
            return {
                "type": "agent_prompted",
                "agent": {"pane_id": "pane-secret"},
                "delivery": "submitted",
            }
        return {"accepted": True}

    def close(self) -> None:
        return None


def _factory(calls: list[dict[str, Any]], *, raises: BaseException | None = None):
    def make_client(config: Config) -> _FakeSocketClient:
        return _FakeSocketClient(calls, raises=raises)

    return make_client


def _forbidden_factory(config: Config) -> Any:
    pytest.fail("a receipt replay must not create a socket client")


def _receipt(config: Config, request_id: str) -> dict[str, Any]:
    assert config.db_path is not None
    receipt = get_command_request(config.db_path, config.host_id, request_id)
    assert receipt is not None
    return receipt


def _receipt_rows(config: Config) -> list[tuple[Any, ...]]:
    assert config.db_path is not None
    with sqlite3.connect(str(config.db_path)) as conn:
        return conn.execute("SELECT * FROM command_receipts ORDER BY id").fetchall()


def _sent_texts(calls: list[dict[str, Any]]) -> list[str]:
    return [
        str(call["params"].get("text"))
        for call in calls
        if call["method"] == "agent.prompt"
    ]


def _drive_to_state(
    config: Config,
    payload: dict[str, Any],
    state: str,
    calls: list[dict[str, Any]],
    monkeypatch: pytest.MonkeyPatch,
) -> Any:
    """Submit one request and leave its receipt in the requested state.

    ``rejected`` is reached through an unsendable private binding, which the
    submission path terminalizes before any send; the caller seeds that binding.
    """
    if state == "accepted":
        return submit_command(config, payload, socket_client_factory=_factory(calls))
    if state == "rejected":
        return submit_command(config, payload, socket_client_factory=_factory(calls))
    if state == "uncertain":
        return submit_command(
            config,
            payload,
            socket_client_factory=_factory(
                calls,
                raises=HerdrSocketTimeoutError("send response lost"),
            ),
        )

    real_mark = command_submission.mark_command_send_started

    def lose_send_start(*args: Any, **kwargs: Any) -> Any:
        if state == "send_started":
            result = real_mark(*args, **kwargs)
            assert result["status"] == "send_started"
        raise HerdrSocketTimeoutError("send-start response lost")

    monkeypatch.setattr(command_submission, "mark_command_send_started", lose_send_start)
    envelope = submit_command(config, payload, socket_client_factory=_factory(calls))
    monkeypatch.setattr(command_submission, "mark_command_send_started", real_mark)
    return envelope


@pytest.mark.parametrize("selector_kind", SELECTOR_KINDS)
@pytest.mark.parametrize("receipt_state", RECEIPT_STATES)
def test_exact_retry_replays_receipt_after_healthy_worker_removal(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    receipt_state: str,
    selector_kind: str,
) -> None:
    """Every receipt state replays for every selector shape once the worker is gone."""
    config = _config(tmp_path)
    worker = _worker()
    _seed(config, [worker], [_binding(worker, sendable=receipt_state != "rejected")])
    request_id = f"replay-{receipt_state}-{selector_kind}"
    payload = _request(request_id=request_id, target=_selector(selector_kind, worker))
    calls: list[dict[str, Any]] = []

    first = _drive_to_state(config, payload, receipt_state, calls, monkeypatch)
    stored = _receipt(config, request_id)
    assert stored["state"] == receipt_state

    _remove_workers(config)
    rows_before = _receipt_rows(config)
    retry = submit_command(config, payload, socket_client_factory=_forbidden_factory)

    assert (retry.status, retry.disposition) == _EXPECTED_REPLAY[receipt_state]
    if receipt_state in {"accepted", "rejected"}:
        assert retry.to_dict() == first.to_dict()
    # The retry itself sent nothing and left the authoritative receipt intact.
    assert _sent_texts(calls) == _SENDS_TO_REACH[receipt_state]
    assert _receipt_rows(config) == rows_before


@pytest.mark.parametrize("selector_kind", SELECTOR_KINDS)
def test_exact_retry_replays_accepted_receipt_while_backend_is_degraded(
    tmp_path: Path,
    selector_kind: str,
) -> None:
    """A degraded observation cannot override truth the receipt already holds."""
    config = _config(tmp_path)
    worker = _worker()
    _seed(config, [worker], [_binding(worker)])
    request_id = f"degraded-{selector_kind}"
    payload = _request(request_id=request_id, target=_selector(selector_kind, worker))
    calls: list[dict[str, Any]] = []

    accepted = submit_command(config, payload, socket_client_factory=_factory(calls))
    assert accepted.status == STATUS_ACCEPTED
    _remove_workers(config, health="degraded")

    retry = submit_command(config, payload, socket_client_factory=_forbidden_factory)

    assert retry.to_dict() == accepted.to_dict()
    assert _sent_texts(calls) == ["hello"]


def test_fingerprint_only_targets_cannot_claim_another_workers_receipt(
    tmp_path: Path,
) -> None:
    """The collision that blocked 2edc6cc: two fingerprints, one request ID.

    A fingerprint is a mutable precondition, not identity, so the selector proof
    excludes it. If a fingerprint could stand alone as the whole target, every
    such target would share one proof -- and reusing the request ID with a
    fingerprint naming a different worker would replay the first worker's
    accepted result. Rejecting the shape outright is what closes that hole.
    """
    config = _config(tmp_path)
    first_worker = _worker()
    second_worker = _worker(worker_id="w-2", name="Beta", space_id="space-2")
    _seed(
        config,
        [first_worker, second_worker],
        [_binding(first_worker), _binding(second_worker)],
    )
    assert first_worker.fingerprint != second_worker.fingerprint
    calls: list[dict[str, Any]] = []

    first = submit_command(
        config,
        _request(
            request_id="fingerprint-collision",
            target={"worker_fingerprint": first_worker.fingerprint},
        ),
        socket_client_factory=_factory(calls),
    )
    changed = submit_command(
        config,
        _request(
            request_id="fingerprint-collision",
            target={"worker_fingerprint": second_worker.fingerprint},
        ),
        socket_client_factory=_factory(calls),
    )

    # Under 2edc6cc the first was accepted and the second replayed its stored
    # terminal_accepted body: one worker's result claimed for another.
    for envelope in (first, changed):
        assert envelope.ok is False
        assert envelope.status == STATUS_INVALID_REQUEST
        assert envelope.disposition == DISPOSITION_NO_RECEIPT
    assert _sent_texts(calls) == []
    assert _receipt_rows(config) == []


def test_fingerprint_only_target_does_no_store_source_or_backend_work(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Rejection happens in the parser, before anything durable or observable."""
    config = _config(tmp_path)
    touched: list[str] = []

    def forbidden(*args: Any, **kwargs: Any) -> Any:
        touched.append("io")
        raise AssertionError("an invalid target must not reach store or source work")

    monkeypatch.setattr(command_submission, "get_command_request", forbidden)
    monkeypatch.setattr(command_submission, "_current_snapshot", forbidden)
    monkeypatch.setattr(command_submission, "latest_snapshot", forbidden)
    monkeypatch.setattr(command_submission, "reserve_command_request", forbidden)
    monkeypatch.setattr(
        "tendwire.command_submission.project_from_observations",
        forbidden,
    )

    envelope = submit_command(
        config,
        _request(
            request_id="fingerprint-only",
            target={"worker_fingerprint": "fingerprint-A"},
        ),
        socket_client_factory=forbidden,
    )

    assert envelope.ok is False
    assert envelope.status == STATUS_INVALID_REQUEST
    assert envelope.disposition == DISPOSITION_NO_RECEIPT
    assert touched == []
    assert config.db_path is not None
    assert not config.db_path.exists()


def test_fingerprint_only_target_is_rejected_by_daemon_and_read_only_replay(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    worker = _worker()
    _seed(config, [worker], [_binding(worker)])
    payload = _request(
        request_id="fingerprint-only",
        target={"worker_fingerprint": worker.fingerprint},
    )
    api = TendwireDaemonAPI(
        get_snapshot=lambda: _snapshot([worker]),
        get_health=lambda: {"ok": True},
        submit_command=lambda params: submit_command(
            config,
            params,
            socket_client_factory=_forbidden_factory,
        ),
    )

    response = api.dispatch({"method": "command.submit", "params": payload, "id": "1"})

    assert response["result"]["ok"] is False
    assert response["result"]["status"] == STATUS_INVALID_REQUEST
    assert response["result"]["disposition"] == DISPOSITION_NO_RECEIPT
    # The rejection names the rule it broke without echoing the observation the
    # caller sent, and it leaves no durable trace of the request.
    rendered = json.dumps(response)
    assert worker.fingerprint not in rendered
    assert "worker_fingerprint" in response["result"]["error"]["message"]
    assert response["result"]["error"]["details"]["allowed"] == [
        "name",
        "space_id",
        "stable_key",
        "worker_id",
    ]
    # The read-only response-loss path cannot resolve it either, and must never
    # invent a receipt for a request that could not have created one.
    assert replay_command_receipt(config, payload) is None
    assert _receipt_rows(config) == []
    assert config.db_path is not None
    with sqlite3.connect(str(config.db_path)) as conn:
        command_events = conn.execute(
            "SELECT COUNT(*) FROM events WHERE aggregate_type = 'command_request'"
        ).fetchone()[0]
    assert command_events == 0


@pytest.mark.parametrize("selector_kind", ["name", "space_id", "name_and_space"])
def test_refreshed_fingerprint_beside_a_stable_selector_replays_stored_result(
    tmp_path: Path,
    selector_kind: str,
) -> None:
    """A refreshed fingerprint is a precondition, not a different command.

    This is the alias path, so equivalence is proven by the stored selector
    proof -- which is exactly the evidence that must ignore the fingerprint.
    """
    config = _config(tmp_path)
    worker = _worker()
    _seed(config, [worker], [_binding(worker)])
    stable = _selector(selector_kind, worker)
    payload = _request(
        request_id=f"fingerprint-beside-{selector_kind}",
        target={**stable, "worker_fingerprint": worker.fingerprint},
    )
    calls: list[dict[str, Any]] = []

    accepted = submit_command(config, payload, socket_client_factory=_factory(calls))
    assert accepted.status == STATUS_ACCEPTED

    # The worker is re-observed with fresh data, so its fingerprint moves.
    refreshed_worker = _worker(status="waiting")
    assert refreshed_worker.fingerprint != worker.fingerprint
    assert config.db_path is not None
    save_snapshot(
        config.db_path,
        _snapshot([refreshed_worker], updated_at="2026-01-01T00:06:00+00:00"),
    )
    refreshed = _request(
        request_id=f"fingerprint-beside-{selector_kind}",
        target={**stable, "worker_fingerprint": refreshed_worker.fingerprint},
    )

    retry = submit_command(config, refreshed, socket_client_factory=_forbidden_factory)

    assert retry.to_dict() == accepted.to_dict()
    assert _sent_texts(calls) == ["hello"]


def test_refreshed_worker_fingerprint_stays_noncanonical_on_retry(
    tmp_path: Path,
) -> None:
    """A worker ID that survives with new observation data is the same target."""
    config = _config(tmp_path)
    worker = _worker()
    _seed(config, [worker], [_binding(worker)])
    payload = _request(
        request_id="fingerprint-churn",
        target={"worker_id": worker.id, "worker_fingerprint": worker.fingerprint},
    )
    calls: list[dict[str, Any]] = []

    accepted = submit_command(config, payload, socket_client_factory=_factory(calls))
    assert accepted.status == STATUS_ACCEPTED

    # The worker keeps its public ID but every observed attribute changes.
    recycled = _worker(name="Renamed", space_id="space-9", status="waiting")
    assert config.db_path is not None
    save_snapshot(
        config.db_path,
        _snapshot([recycled], updated_at="2026-01-01T00:06:00+00:00"),
    )
    refreshed = _request(
        request_id="fingerprint-churn",
        target={"worker_id": worker.id, "worker_fingerprint": recycled.fingerprint},
    )

    retry = submit_command(config, refreshed, socket_client_factory=_forbidden_factory)

    assert retry.to_dict() == accepted.to_dict()
    assert _sent_texts(calls) == ["hello"]


@pytest.mark.parametrize("receipt_state", RECEIPT_STATES)
@pytest.mark.parametrize(
    ("collision", "changed"),
    [
        ("instruction", {"instruction": {"text": "different"}}),
        ("worker_id", {"target": {"worker_id": "w-2"}}),
        ("name", {"target": {"name": "Beta"}}),
        ("space_id", {"target": {"space_id": "space-2"}}),
        ("action", {"action": "answer_pending"}),
    ],
)
def test_changed_request_cannot_claim_a_stored_receipt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    receipt_state: str,
    collision: str,
    changed: dict[str, Any],
) -> None:
    """Reusing a request ID with any changed canonical field mutates nothing."""
    config = _config(tmp_path)
    worker = _worker()
    other = _worker(worker_id="w-2", name="Beta", space_id="space-2")
    _seed(
        config,
        [worker, other],
        [
            _binding(worker, sendable=receipt_state != "rejected"),
            _binding(other, sendable=receipt_state != "rejected"),
        ],
    )
    request_id = f"collision-{receipt_state}-{collision}"
    payload = _request(request_id=request_id, target=_selector("name", worker))
    calls: list[dict[str, Any]] = []

    _drive_to_state(config, payload, receipt_state, calls, monkeypatch)
    assert _receipt(config, request_id)["state"] == receipt_state
    sent_before = list(_sent_texts(calls))
    rows_before = _receipt_rows(config)

    reused = _request(request_id=request_id, target=_selector("name", worker))
    reused.update(changed)
    if collision == "action":
        reused.pop("target", None)
        reused.pop("instruction", None)
        reused["params"] = {
            "pending_id": "pending-public",
            "pending_fingerprint": "revision-public",
            "choice_id": "choice-public",
        }

    conflict = submit_command(
        config,
        reused,
        socket_client_factory=lambda _config: pytest.fail(
            "a changed request must not create a socket client"
        ),
    )

    assert conflict.status == STATUS_DUPLICATE_REQUEST
    assert conflict.disposition == DISPOSITION_TERMINAL_REJECTED
    assert _sent_texts(calls) == sent_before
    assert _receipt_rows(config) == rows_before


@pytest.mark.parametrize("receipt_state", RECEIPT_STATES)
def test_unsupported_stored_selector_proof_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    receipt_state: str,
) -> None:
    """Evidence this version cannot read decides nothing rather than deciding wrong."""
    config = _config(tmp_path)
    worker = _worker()
    _seed(config, [worker], [_binding(worker, sendable=receipt_state != "rejected")])
    request_id = f"proof-{receipt_state}"
    payload = _request(request_id=request_id, target=_selector("name", worker))
    calls: list[dict[str, Any]] = []

    _drive_to_state(config, payload, receipt_state, calls, monkeypatch)
    assert config.db_path is not None
    with sqlite3.connect(str(config.db_path)) as conn:
        conn.execute(
            "UPDATE command_receipts SET selector_proof = ? WHERE request_id = ?",
            ("v9:not-a-supported-proof", request_id),
        )
    _remove_workers(config)
    sent_before = list(_sent_texts(calls))
    rows_before = _receipt_rows(config)

    retry = submit_command(config, payload, socket_client_factory=_forbidden_factory)

    assert retry.status == STATUS_REQUEST_STATE_UNCERTAIN
    assert retry.disposition == DISPOSITION_TERMINAL_UNCERTAIN
    assert _sent_texts(calls) == sent_before
    assert _receipt_rows(config) == rows_before


def test_legacy_receipt_without_selector_proof_fails_closed_on_alias_retry(
    tmp_path: Path,
) -> None:
    """A migrated v12 alias receipt is never guessed into a replay."""
    config = _config(tmp_path)
    worker = _worker()
    _seed(config, [worker], [_binding(worker)])
    payload = _request(request_id="legacy-alias", target=_selector("name", worker))
    calls: list[dict[str, Any]] = []

    accepted = submit_command(config, payload, socket_client_factory=_factory(calls))
    assert accepted.status == STATUS_ACCEPTED
    # Reproduce what the v12 migration leaves behind: a canonical receipt whose
    # original selector spelling was never recorded.
    assert config.db_path is not None
    with sqlite3.connect(str(config.db_path)) as conn:
        conn.execute("UPDATE command_receipts SET selector_proof = ''")
    _remove_workers(config)
    rows_before = _receipt_rows(config)

    retry = submit_command(config, payload, socket_client_factory=_forbidden_factory)

    assert retry.status == STATUS_REQUEST_STATE_UNCERTAIN
    assert retry.disposition == DISPOSITION_TERMINAL_UNCERTAIN
    assert _sent_texts(calls) == ["hello"]
    assert _receipt_rows(config) == rows_before


def test_legacy_alias_receipt_fails_closed_when_current_authority_store_raises(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A transient authority read cannot erase a receipt or permit a resend."""
    config = _config(tmp_path)
    worker = _worker()
    _seed(config, [worker], [_binding(worker)])
    payload = _request(request_id="legacy-store-race", target=_selector("name", worker))
    calls: list[dict[str, Any]] = []

    accepted = submit_command(config, payload, socket_client_factory=_factory(calls))
    assert accepted.status == STATUS_ACCEPTED
    assert config.db_path is not None
    with sqlite3.connect(str(config.db_path)) as conn:
        conn.execute("UPDATE command_receipts SET selector_proof = ''")

    def contended_snapshot(*args: Any, **kwargs: Any) -> Any:
        raise sqlite3.OperationalError("database is locked")

    monkeypatch.setattr(command_submission, "latest_snapshot", contended_snapshot)
    rows_before = _receipt_rows(config)

    retry = submit_command(config, payload, socket_client_factory=_forbidden_factory)

    assert retry.status == STATUS_REQUEST_STATE_UNCERTAIN
    assert retry.disposition == DISPOSITION_TERMINAL_UNCERTAIN
    assert _sent_texts(calls) == ["hello"]
    assert _receipt_rows(config) == rows_before


def test_legacy_receipt_without_selector_proof_replays_explicit_worker_id(
    tmp_path: Path,
) -> None:
    """Explicit worker IDs still replay from a legacy receipt: the ID is the proof."""
    config = _config(tmp_path)
    worker = _worker()
    _seed(config, [worker], [_binding(worker)])
    payload = _request(request_id="legacy-explicit", target={"worker_id": worker.id})
    calls: list[dict[str, Any]] = []

    accepted = submit_command(config, payload, socket_client_factory=_factory(calls))
    assert accepted.status == STATUS_ACCEPTED
    assert config.db_path is not None
    with sqlite3.connect(str(config.db_path)) as conn:
        conn.execute("UPDATE command_receipts SET selector_proof = ''")
    _remove_workers(config)

    retry = submit_command(config, payload, socket_client_factory=_forbidden_factory)

    assert retry.to_dict() == accepted.to_dict()
    assert _sent_texts(calls) == ["hello"]


def test_abandoned_reservation_is_redriven_not_replayed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An expired reservation lease is the existing crash-recovery path, not a replay."""
    config = _config(tmp_path)
    worker = _worker()
    _seed(config, [worker], [_binding(worker)])
    payload = _request(request_id="abandoned", target=_selector("name", worker))
    calls: list[dict[str, Any]] = []

    _drive_to_state(config, payload, "reserved", calls, monkeypatch)
    assert _receipt(config, "abandoned")["state"] == "reserved"
    assert _sent_texts(calls) == []

    # Expire the owner lease the crashed sender left behind.
    assert config.db_path is not None
    with sqlite3.connect(str(config.db_path)) as conn:
        conn.execute(
            "UPDATE command_receipts SET owner_expires_at = ? WHERE request_id = ?",
            ("2020-01-01T00:00:00+00:00", "abandoned"),
        )

    recovered = submit_command(config, payload, socket_client_factory=_factory(calls))

    assert recovered.status == STATUS_ACCEPTED
    assert recovered.disposition == DISPOSITION_TERMINAL_ACCEPTED
    assert _sent_texts(calls) == ["hello"]
    assert _receipt(config, "abandoned")["state"] == "accepted"


def test_abandoned_reservation_reports_in_progress_when_worker_is_gone(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A vanished worker never restates a stored reservation as a no-receipt failure."""
    config = _config(tmp_path)
    worker = _worker()
    _seed(config, [worker], [_binding(worker)])
    payload = _request(request_id="abandoned-gone", target=_selector("name", worker))
    calls: list[dict[str, Any]] = []

    _drive_to_state(config, payload, "reserved", calls, monkeypatch)
    assert config.db_path is not None
    with sqlite3.connect(str(config.db_path)) as conn:
        conn.execute(
            "UPDATE command_receipts SET owner_expires_at = ? WHERE request_id = ?",
            ("2020-01-01T00:00:00+00:00", "abandoned-gone"),
        )
    _remove_workers(config)
    rows_before = _receipt_rows(config)

    retry = submit_command(config, payload, socket_client_factory=_forbidden_factory)

    assert retry.status == STATUS_PENDING
    assert retry.disposition == DISPOSITION_IN_PROGRESS
    assert _sent_texts(calls) == []
    assert _receipt_rows(config) == rows_before


def test_abandoned_reservation_conflicts_when_selector_now_names_another_worker(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Re-driving an abandoned send never redirects it to a different worker."""
    config = _config(tmp_path)
    worker = _worker()
    other = _worker(worker_id="w-2", name="Beta", space_id="space-2")
    _seed(config, [worker, other], [_binding(worker), _binding(other)])
    payload = _request(request_id="abandoned-drift", target=_selector("name", worker))
    calls: list[dict[str, Any]] = []

    _drive_to_state(config, payload, "reserved", calls, monkeypatch)
    assert config.db_path is not None
    with sqlite3.connect(str(config.db_path)) as conn:
        conn.execute(
            "UPDATE command_receipts SET owner_expires_at = ?, selector_proof = ''",
            ("2020-01-01T00:00:00+00:00",),
        )
    # "Alpha" now names the other worker, so the abandoned reservation's target
    # can no longer be reached by the spelling that created it.
    save_snapshot(
        config.db_path,
        _snapshot(
            [_worker(worker_id="w-2", name="Alpha", space_id="space-2")],
            updated_at="2026-01-01T00:07:00+00:00",
        ),
    )
    rows_before = _receipt_rows(config)

    conflict = submit_command(
        config,
        payload,
        socket_client_factory=lambda _config: pytest.fail(
            "a redirected takeover must not create a socket client"
        ),
    )

    assert conflict.status == STATUS_DUPLICATE_REQUEST
    assert _sent_texts(calls) == []
    assert _receipt_rows(config) == rows_before


@pytest.mark.parametrize("selector_kind", SELECTOR_KINDS)
@pytest.mark.parametrize("stage", ["reservation", "send_start", "completion"])
def test_exact_retry_racing_a_live_mutation_never_sends_twice(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    stage: str,
    selector_kind: str,
) -> None:
    """A retry landing mid-mutation replays the receipt, even as the worker vanishes.

    The sender is held at one stage of the state machine while an exact retry
    runs against a snapshot that no longer contains the worker. The retry must
    read in-progress from the receipt, open no socket, and leave the in-flight
    mutation to finish exactly once.
    """
    config = _config(tmp_path)
    worker = _worker()
    _seed(config, [worker], [_binding(worker)])
    request_id = f"race-{stage}-{selector_kind}"
    payload = _request(request_id=request_id, target=_selector(selector_kind, worker))
    calls: list[dict[str, Any]] = []

    reached = threading.Event()
    release = threading.Event()

    def pause() -> None:
        reached.set()
        assert release.wait(timeout=30), "the racing retry never released the sender"

    real_reserve = command_submission.reserve_command_request
    real_mark = command_submission.mark_command_send_started
    real_finish = command_submission.finish_command_request

    def reserve(*args: Any, **kwargs: Any) -> Any:
        reserved = real_reserve(*args, **kwargs)
        if stage == "reservation":
            pause()
        return reserved

    def mark(*args: Any, **kwargs: Any) -> Any:
        started = real_mark(*args, **kwargs)
        if stage == "send_start":
            pause()
        return started

    def finish(*args: Any, **kwargs: Any) -> Any:
        if stage == "completion":
            pause()
        return real_finish(*args, **kwargs)

    monkeypatch.setattr(command_submission, "reserve_command_request", reserve)
    monkeypatch.setattr(command_submission, "mark_command_send_started", mark)
    monkeypatch.setattr(command_submission, "finish_command_request", finish)

    sent: list[Any] = []

    def send() -> None:
        sent.append(
            submit_command(config, payload, socket_client_factory=_factory(calls))
        )

    sender = threading.Thread(target=send, name=f"sender-{stage}")
    sender.start()
    assert reached.wait(timeout=30), "the sender never reached the raced stage"

    # Healthy authority loses the worker while the mutation is still in flight.
    _remove_workers(config)
    retry = submit_command(config, payload, socket_client_factory=_forbidden_factory)
    release.set()
    sender.join(timeout=30)

    assert not sender.is_alive()
    assert len(sent) == 1
    assert retry.status == STATUS_PENDING
    assert retry.disposition == DISPOSITION_IN_PROGRESS
    assert sent[0].status == STATUS_ACCEPTED
    assert _sent_texts(calls) == ["hello"]
    assert len(_receipt_rows(config)) == 1
    assert _receipt(config, request_id)["state"] == "accepted"

    # Once the race settles, the retry converges on the one stored result.
    settled = submit_command(config, payload, socket_client_factory=_forbidden_factory)
    assert settled.to_dict() == sent[0].to_dict()
    assert _sent_texts(calls) == ["hello"]


def test_retry_racing_receipt_retention_never_sends_twice(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Retention deleting a receipt mid-replay must not reopen the mutation."""
    config = _config(tmp_path)
    worker = _worker()
    _seed(config, [worker], [_binding(worker)])
    payload = _request(request_id="retention-race", target=_selector("name", worker))
    calls: list[dict[str, Any]] = []

    accepted = submit_command(config, payload, socket_client_factory=_factory(calls))
    assert accepted.status == STATUS_ACCEPTED

    real_reserve = command_submission.reserve_terminal_command_replay

    def delete_then_reserve(*args: Any, **kwargs: Any) -> Any:
        # Retention wins the race between reading the receipt and replaying it.
        assert config.db_path is not None
        with sqlite3.connect(str(config.db_path)) as conn:
            conn.execute("DELETE FROM command_receipts")
        return real_reserve(*args, **kwargs)

    monkeypatch.setattr(
        command_submission,
        "reserve_terminal_command_replay",
        delete_then_reserve,
    )
    _remove_workers(config)

    retry = submit_command(config, payload, socket_client_factory=_forbidden_factory)

    # The accepted body is gone, so the only honest answer is terminal
    # uncertainty. It must never become a fresh send.
    assert retry.status == STATUS_REQUEST_STATE_UNCERTAIN
    assert retry.disposition == DISPOSITION_TERMINAL_UNCERTAIN
    assert _sent_texts(calls) == ["hello"]
    restored = _receipt(config, "retention-race")
    assert restored["state"] == "uncertain"
    # The rebuilt row keeps the original spelling's evidence.
    assert is_selector_proof(restored["selector_proof"])
    assert restored["selector_proof"] == build_selector_proof(
        CommandRequest.from_dict(payload)
    )


def test_read_only_replay_honors_selector_proof_without_observing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The CLI's response-loss path replays an exact alias without any observation."""
    config = _config(tmp_path)
    worker = _worker()
    _seed(config, [worker], [_binding(worker)])
    payload = _request(request_id="read-only", target=_selector("name", worker))
    calls: list[dict[str, Any]] = []

    accepted = submit_command(config, payload, socket_client_factory=_factory(calls))
    assert accepted.status == STATUS_ACCEPTED
    monkeypatch.setattr(
        command_submission,
        "_current_snapshot",
        lambda _config: pytest.fail("read-only replay must not consult authority"),
    )
    rows_before = _receipt_rows(config)

    exact = replay_command_receipt(config, payload)
    changed = _request(request_id="read-only", target={"name": "Beta"})
    unprovable = replay_command_receipt(config, changed)

    assert exact is not None
    assert exact.to_dict() == accepted.to_dict()
    # Proving a different spelling would need an observation this path may not
    # make, so it stays unresolved rather than guessing.
    assert unprovable is None
    assert _receipt_rows(config) == rows_before


# ---------------------------------------------------------------------------
# Migration
# ---------------------------------------------------------------------------

# A literal, frozen copy of the schema-v12 receipt table. It must not be rebuilt
# from the live DDL: the point is to migrate what v12 actually wrote.
_V12_COMMAND_RECEIPTS_TABLE = """
CREATE TABLE command_receipts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    host_id TEXT NOT NULL,
    request_id TEXT NOT NULL,
    action TEXT NOT NULL,
    canonical_version INTEGER NOT NULL CHECK (canonical_version >= 0),
    canonical_fingerprint TEXT NOT NULL,
    canonical_request_json TEXT NOT NULL,
    public_worker_id TEXT NOT NULL,
    state TEXT NOT NULL CHECK (
        state IN ('reserved', 'send_started', 'accepted', 'rejected', 'uncertain')
    ),
    status TEXT NOT NULL,
    result_json TEXT NOT NULL,
    owner_token_hash TEXT NOT NULL DEFAULT '',
    owner_expires_at TEXT,
    binding_fingerprint TEXT,
    created_at TEXT NOT NULL,
    reserved_at TEXT NOT NULL,
    send_started_at TEXT,
    terminal_at TEXT,
    updated_at TEXT NOT NULL,
    legacy_collision INTEGER NOT NULL DEFAULT 0 CHECK (legacy_collision IN (0, 1)),
    legacy_collision_count INTEGER NOT NULL DEFAULT 0 CHECK (
        legacy_collision_count >= 0
    ),
    CHECK (
        (
            state IN ('reserved', 'send_started')
            AND terminal_at IS NULL
            AND owner_token_hash <> ''
        )
        OR (
            state IN ('accepted', 'rejected', 'uncertain')
            AND terminal_at IS NOT NULL
            AND owner_token_hash = ''
            AND owner_expires_at IS NULL
        )
    ),
    CHECK (state NOT IN ('reserved', 'send_started') OR status = 'pending'),
    CHECK (
        state != 'accepted'
        OR (status = 'accepted' AND send_started_at IS NOT NULL)
    ),
    CHECK (state != 'uncertain' OR status = 'request_state_uncertain'),
    CHECK (
        state != 'rejected'
        OR status NOT IN ('pending', 'accepted', 'request_state_uncertain')
    ),
    CHECK (
        legacy_collision = 0
        OR (state = 'uncertain' AND legacy_collision_count >= 2)
    )
);
"""

_V12_ACTIVE = (
    "host-a",
    "v12-active",
    "send_instruction",
    1,
    "fingerprint-active",
    '{"action":"send_instruction"}',
    "w-1",
    "reserved",
    "pending",
    '{"ok":false,"status":"pending"}',
    "owner-hash",
    "2026-01-01T00:00:30+00:00",
    None,
    "2026-01-01T00:00:00+00:00",
    "2026-01-01T00:00:00+00:00",
    None,
    None,
    "2026-01-01T00:00:00+00:00",
)
_V12_TERMINAL = (
    "host-a",
    "v12-terminal",
    "send_instruction",
    1,
    "fingerprint-terminal",
    '{"action":"send_instruction"}',
    "w-2",
    "accepted",
    "accepted",
    '{"ok":true,"status":"accepted"}',
    "",
    None,
    "2026-01-01T00:00:00+00:00",
    "2026-01-01T00:00:00+00:00",
    "2026-01-01T00:00:01+00:00",
    "2026-01-01T00:00:02+00:00",
    "2026-01-01T00:00:02+00:00",
)


def _write_v12_store(db_path: Path) -> None:
    with sqlite3.connect(str(db_path)) as conn:
        conn.executescript(
            _V12_COMMAND_RECEIPTS_TABLE + store_sqlite.CREATE_COMMANDS_TABLE
        )
        conn.execute(
            """
            INSERT INTO command_receipts (
                host_id, request_id, action, canonical_version,
                canonical_fingerprint, canonical_request_json, public_worker_id,
                state, status, result_json, owner_token_hash, owner_expires_at,
                binding_fingerprint, created_at, reserved_at, send_started_at,
                terminal_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            _V12_ACTIVE,
        )
        conn.execute(
            """
            INSERT INTO command_receipts (
                host_id, request_id, action, canonical_version,
                canonical_fingerprint, canonical_request_json, public_worker_id,
                state, status, result_json, owner_token_hash, owner_expires_at,
                created_at, reserved_at, send_started_at, terminal_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            _V12_TERMINAL,
        )
        conn.execute("PRAGMA user_version = 12")


def test_v12_receipts_migrate_to_empty_selector_proof(tmp_path: Path) -> None:
    """Active and terminal v12 receipts survive with no invented selector evidence."""
    db_path = tmp_path / "v12.db"
    _write_v12_store(db_path)

    init_store(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        assert (
            int(conn.execute("PRAGMA user_version").fetchone()[0])
            == store_sqlite.STORE_SCHEMA_VERSION
        )
        proofs = conn.execute(
            "SELECT request_id, selector_proof FROM command_receipts ORDER BY request_id"
        ).fetchall()
    # A v12 row records the worker a request resolved to, never how it was
    # spelled. Deriving a proof from that would let a changed target replay it.
    assert proofs == [("v12-active", ""), ("v12-terminal", "")]

    active = get_command_request(db_path, "host-a", "v12-active")
    terminal = get_command_request(db_path, "host-a", "v12-terminal")
    assert active is not None and terminal is not None
    assert (active["state"], active["status"]) == ("reserved", "pending")
    assert (terminal["state"], terminal["status"]) == ("accepted", "accepted")
    assert terminal["result_json"] == '{"ok":true,"status":"accepted"}'


def test_v12_migration_is_idempotent_across_reruns(tmp_path: Path) -> None:
    db_path = tmp_path / "v12-idempotent.db"
    _write_v12_store(db_path)

    init_store(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        first = conn.execute("SELECT * FROM command_receipts ORDER BY id").fetchall()
    init_store(db_path)
    init_store(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        second = conn.execute("SELECT * FROM command_receipts ORDER BY id").fetchall()
        version = int(conn.execute("PRAGMA user_version").fetchone()[0])

    assert first == second
    assert version == store_sqlite.STORE_SCHEMA_VERSION


def test_v12_migration_rolls_back_without_partial_schema(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failing v13 transition leaves the v12 store exactly as it was."""
    db_path = tmp_path / "v12-rollback.db"
    _write_v12_store(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        before = conn.execute("SELECT * FROM command_receipts ORDER BY id").fetchall()

    real_migrate = store_sqlite._migrate_v12_to_v13_conn

    def failing_migrate(conn: sqlite3.Connection) -> None:
        real_migrate(conn)
        raise RuntimeError("v13 migration failed after adding the column")

    monkeypatch.setattr(
        store_sqlite,
        "MIGRATIONS",
        tuple(
            store_sqlite.Migration(item.from_version, item.to_version, failing_migrate)
            if item.to_version == 13
            else item
            for item in store_sqlite.MIGRATIONS
        ),
    )

    with pytest.raises(RuntimeError):
        init_store(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        assert int(conn.execute("PRAGMA user_version").fetchone()[0]) == 12
        columns = {
            str(row[1])
            for row in conn.execute("PRAGMA table_info(command_receipts)").fetchall()
        }
        assert "selector_proof" not in columns
        assert (
            conn.execute("SELECT * FROM command_receipts ORDER BY id").fetchall()
            == before
        )


def _downgrade_store_to_v12(db_path: Path) -> None:
    """Rebuild a live store as schema v12, dropping every selector proof."""
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute("ALTER TABLE command_receipts RENAME TO command_receipts_v13")
        conn.executescript(_V12_COMMAND_RECEIPTS_TABLE)
        conn.execute(
            """
            INSERT INTO command_receipts (
                id, host_id, request_id, action, canonical_version,
                canonical_fingerprint, canonical_request_json, public_worker_id,
                state, status, result_json, owner_token_hash, owner_expires_at,
                binding_fingerprint, created_at, reserved_at, send_started_at,
                terminal_at, updated_at, legacy_collision, legacy_collision_count
            )
            SELECT
                id, host_id, request_id, action, canonical_version,
                canonical_fingerprint, canonical_request_json, public_worker_id,
                state, status, result_json, owner_token_hash, owner_expires_at,
                binding_fingerprint, created_at, reserved_at, send_started_at,
                terminal_at, updated_at, legacy_collision, legacy_collision_count
            FROM command_receipts_v13
            """
        )
        conn.execute("DROP TABLE command_receipts_v13")
        for statement in store_sqlite.CREATE_COMMAND_RECEIPT_INDEXES:
            conn.execute(statement)
        conn.execute("PRAGMA user_version = 12")


def test_migrated_v12_store_proves_new_requests_but_never_old_ones(
    tmp_path: Path,
) -> None:
    """Migration is conservative: it earns proofs forward, it does not backfill them."""
    config = _config(tmp_path)
    assert config.db_path is not None
    worker = _worker()
    _seed(config, [worker], [_binding(worker)])
    calls: list[dict[str, Any]] = []
    legacy_payload = _request(
        request_id="pre-migration",
        target=_selector("name", worker),
    )
    legacy_accepted = submit_command(
        config,
        legacy_payload,
        socket_client_factory=_factory(calls),
    )
    assert legacy_accepted.status == STATUS_ACCEPTED

    _downgrade_store_to_v12(config.db_path)
    init_store(config.db_path)

    with sqlite3.connect(str(config.db_path)) as conn:
        assert (
            int(conn.execute("PRAGMA user_version").fetchone()[0])
            == store_sqlite.STORE_SCHEMA_VERSION
        )
    assert _receipt(config, "pre-migration")["selector_proof"] == ""

    fresh_payload = _request(
        request_id="post-migration",
        target=_selector("name", worker),
    )
    fresh_accepted = submit_command(
        config,
        fresh_payload,
        socket_client_factory=_factory(calls),
    )
    assert fresh_accepted.status == STATUS_ACCEPTED
    assert is_selector_proof(_receipt(config, "post-migration")["selector_proof"])

    _remove_workers(config)
    legacy_retry = submit_command(
        config,
        legacy_payload,
        socket_client_factory=_forbidden_factory,
    )
    fresh_retry = submit_command(
        config,
        fresh_payload,
        socket_client_factory=_forbidden_factory,
    )

    # The pre-migration alias has no evidence of how it was spelled, so it fails
    # closed. The post-migration one carries its own proof and replays.
    assert legacy_retry.status == STATUS_REQUEST_STATE_UNCERTAIN
    assert legacy_retry.disposition == DISPOSITION_TERMINAL_UNCERTAIN
    assert fresh_retry.to_dict() == fresh_accepted.to_dict()
    assert _sent_texts(calls) == ["hello", "hello"]


# ---------------------------------------------------------------------------
# Public boundary
# ---------------------------------------------------------------------------


def _assert_no_selector_proof(value: Any, proof: str, path: str = "$") -> None:
    if isinstance(value, dict):
        for key, item in value.items():
            assert "selector" not in str(key).lower().replace("-", "_"), (
                f"selector evidence leaked at {path}.{key}"
            )
            _assert_no_selector_proof(item, proof, f"{path}.{key}")
    elif isinstance(value, list):
        for index, item in enumerate(value):
            _assert_no_selector_proof(item, proof, f"{path}[{index}]")
    elif isinstance(value, str):
        assert proof not in value, f"selector proof leaked at {path}"


def test_selector_proof_never_reaches_a_public_surface(tmp_path: Path) -> None:
    """The proof stays private evidence: not in envelopes, events, or the audit row."""
    config = _config(tmp_path)
    worker = _worker()
    _seed(config, [worker], [_binding(worker)])
    payload = _request(request_id="boundary", target=_selector("name", worker))
    calls: list[dict[str, Any]] = []

    accepted = submit_command(config, payload, socket_client_factory=_factory(calls))
    _remove_workers(config)
    replay = submit_command(config, payload, socket_client_factory=_forbidden_factory)
    proof = _receipt(config, "boundary")["selector_proof"]
    assert is_selector_proof(proof)

    for envelope in (accepted, replay):
        _assert_no_selector_proof(envelope.to_dict(), proof)
        _assert_no_selector_proof(json.loads(envelope.to_json()), proof)

    assert config.db_path is not None
    with sqlite3.connect(str(config.db_path)) as conn:
        events = conn.execute("SELECT payload_json FROM events ORDER BY id").fetchall()
        audit = conn.execute("SELECT * FROM commands ORDER BY id").fetchall()
        audit_columns = {
            str(row[1]) for row in conn.execute("PRAGMA table_info(commands)").fetchall()
        }
    assert events
    for (payload_json,) in events:
        _assert_no_selector_proof(json.loads(payload_json), proof)
        assert proof not in payload_json
    # The non-authoritative audit projection must not carry the proof at all.
    assert "selector_proof" not in audit_columns
    for row in audit:
        assert proof not in json.dumps(row, default=str)

    # Daemon JSON, and the health and maintenance surfaces that summarize
    # command receipts, are built from the same receipts and must stay clean.
    health = store_status(config.db_path, config.host_id)
    api = TendwireDaemonAPI(
        get_snapshot=lambda: _snapshot([]),
        get_health=lambda: health,
        submit_command=lambda params: submit_command(
            config,
            params,
            socket_client_factory=_forbidden_factory,
        ),
    )
    daemon_response = api.dispatch(
        {"method": "command.submit", "params": payload, "id": "1"}
    )
    assert daemon_response["result"]["status"] == STATUS_ACCEPTED
    _assert_no_selector_proof(daemon_response, proof)
    _assert_no_selector_proof(api.dispatch({"method": "health.get", "id": "2"}), proof)
    _assert_no_selector_proof(health, proof)
    _assert_no_selector_proof(
        run_store_maintenance(
            config.db_path,
            config.host_id,
            retention_days=14,
            max_outbox_attempts=5,
        ),
        proof,
    )


def test_selector_proof_field_is_rejected_from_a_request(tmp_path: Path) -> None:
    """No caller can inject or forge selector evidence through the public request."""
    config = _config(tmp_path)
    worker = _worker()
    _seed(config, [worker], [_binding(worker)])

    for injected in (
        {"target": {"worker_id": "w-1", "selector_proof": "v1:" + "a" * 64}},
        {"instruction": {"text": "hello", "selector_proof": "v1:" + "a" * 64}},
        {"selector_proof": "v1:" + "a" * 64},
    ):
        payload = _request(request_id="injected", target={"worker_id": worker.id})
        payload.update(injected)
        envelope = submit_command(
            config,
            payload,
            socket_client_factory=lambda _config: pytest.fail(
                "an invalid request must not create a socket client"
            ),
        )
        assert envelope.ok is False
        assert envelope.status == "invalid_request"

    assert _receipt_rows(config) == []
