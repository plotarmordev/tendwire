"""Transient pre-send failures stay retryable; permanent ones stay terminal.

Goal 11B made an existing receipt authoritative for its retry. Goal 11C closes
the gap that stress testing exposed: a transient *local* failure before any
backend send -- the binding store or receipt store raising, a socket that will
not connect, a pane read that times out -- was reserved and written as a durable
``terminal_rejected`` receipt, permanently dropping a command that was never
sent.

The corrected rule classifies a pre-send failure by the last irreversible stage
it reached. A failed local or backend *operation* proves nothing durable and
stays ``no_receipt`` / retryable under the same request ID. Only an authoritative
observation of proven target unsuitability -- a disallowed worker status, an
unavailable backend, or a missing/stale/ambiguous private binding -- may
terminalize. Anything after a send may have started stays terminal uncertainty.
"""

from __future__ import annotations

import sqlite3
import threading
from pathlib import Path
from typing import Any

import pytest

import tendwire.command_submission as command_submission

from tendwire.backends.herdr_socket import HerdrSocketTimeoutError
from tendwire.command_submission import submit_command
from tendwire.config import Config
from tendwire.core.commands import (
    DISPOSITION_IN_PROGRESS,
    DISPOSITION_NO_RECEIPT,
    DISPOSITION_TERMINAL_ACCEPTED,
    DISPOSITION_TERMINAL_REJECTED,
    STATUS_ACCEPTED,
    STATUS_BACKEND_UNAVAILABLE,
    STATUS_BACKEND_UNSUPPORTED,
    STATUS_PENDING,
    STATUS_REJECTED,
    STATUS_STALE_TARGET,
)
from tendwire.core.models import BackendHealth, Snapshot, Worker, WorkerBinding
from tendwire.daemon_api import TendwireDaemonAPI
from tendwire.local_state import LocalStateError, LocalStateErrorCode
from tendwire.store.sqlite import (
    get_command_request,
    init_store,
    save_snapshot,
    upsert_worker_bindings,
)


HOST_ID = "cmd-host"


def _config(tmp_path: Path) -> Config:
    return Config(
        host_id=HOST_ID,
        data_dir=tmp_path,
        db_path=tmp_path / "commands.db",
        herdr_backend="socket",
        herdr_timeout_seconds=5.0,
    )


def _worker(*, worker_id: str = "w-1", status: str = "active") -> Worker:
    return Worker(id=worker_id, name="Alpha", status=status, space_id="space-1")


def _binding(
    worker: Worker,
    *,
    sendable: bool = True,
    fingerprint: str | None = None,
) -> WorkerBinding:
    return WorkerBinding(
        host_id=HOST_ID,
        worker_id=worker.id,
        worker_fingerprint=fingerprint or worker.fingerprint,
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


def _seed(
    config: Config,
    workers: list[Worker],
    bindings: list[WorkerBinding],
    *,
    health: str = "healthy",
) -> None:
    assert config.db_path is not None
    init_store(config.db_path)
    save_snapshot(
        config.db_path,
        Snapshot(
            host_id=HOST_ID,
            updated_at="2026-01-01T00:00:00+00:00",
            workers=workers,
            backend_health=[_health(health)],
        ),
    )
    if bindings:
        upsert_worker_bindings(config.db_path, bindings)


def _request(
    *,
    request_id: str,
    worker_id: str = "w-1",
    text: str = "hello",
) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "action": "send_instruction",
        "request_id": request_id,
        "dry_run": False,
        "target": {"worker_id": worker_id},
        "instruction": {"text": text},
    }


class _FakeSocketClient:
    def __init__(
        self,
        calls: list[dict[str, Any]],
        *,
        agent_get_raises: BaseException | None = None,
        agent_get_response: dict[str, Any] | None = None,
    ) -> None:
        self.calls = calls
        self.agent_get_raises = agent_get_raises
        self.agent_get_response = agent_get_response

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
        if method == "agent.get":
            if self.agent_get_raises is not None:
                raise self.agent_get_raises
            if self.agent_get_response is not None:
                return self.agent_get_response
            return {"result": {"agent": {"pane_id": "pane-w-1"}}}
        if method == "pane.read":
            return {
                "type": "pane_read",
                "read": {"text": "Completed previous turn.\n── status: idle ──"},
            }
        if method == "agent.prompt":
            return {
                "type": "agent_prompted",
                "agent": {"pane_id": "pane-w-1"},
                "delivery": "submitted",
            }
        return {"accepted": True}

    def close(self) -> None:
        return None


def _factory(calls: list[dict[str, Any]], **kwargs: Any):
    def make_client(config: Config) -> _FakeSocketClient:
        return _FakeSocketClient(calls, **kwargs)

    return make_client


def _forbidden_factory(config: Config) -> Any:
    pytest.fail("a receipt replay must not create a socket client")


def _sent_texts(calls: list[dict[str, Any]]) -> list[str]:
    return [
        str(call["params"].get("text"))
        for call in calls
        if call["method"] == "agent.prompt"
    ]


def _receipt(config: Config, request_id: str) -> dict[str, Any] | None:
    assert config.db_path is not None
    return get_command_request(config.db_path, config.host_id, request_id)


def _receipt_count(config: Config) -> int:
    assert config.db_path is not None
    with sqlite3.connect(str(config.db_path)) as conn:
        return conn.execute("SELECT COUNT(*) FROM command_receipts").fetchone()[0]


# ---------------------------------------------------------------------------
# Deterministic injected transients: each must stay retryable
# ---------------------------------------------------------------------------


class _TransientInjection:
    """One armed pre-send transient that clears after the first attempt."""

    def __init__(self, kind: str) -> None:
        self.kind = kind
        self.armed = True

    def install(
        self,
        monkeypatch: pytest.MonkeyPatch,
        calls: list[dict[str, Any]],
    ) -> Any:
        real_bindings = command_submission.list_worker_bindings
        real_latest_snapshot = command_submission.latest_snapshot
        real_reserve = command_submission.reserve_command_request

        if self.kind == "snapshot_store_local_state":
            def latest_snapshot(*a: Any, **k: Any) -> Any:
                if self.armed:
                    raise LocalStateError(
                        LocalStateErrorCode.ENTRY_CHANGED,
                        LocalStateErrorCode.ENTRY_CHANGED,
                        "local-state entry changed during validation",
                    )
                return real_latest_snapshot(*a, **k)

            monkeypatch.setattr(command_submission, "latest_snapshot", latest_snapshot)
            return _factory(calls)

        if self.kind == "binding_store_sqlite":
            def bindings(*a: Any, **k: Any) -> Any:
                if self.armed:
                    raise sqlite3.OperationalError("database is locked")
                return real_bindings(*a, **k)

            monkeypatch.setattr(command_submission, "list_worker_bindings", bindings)
            return _factory(calls)

        if self.kind == "binding_store_local_state":
            def bindings(*a: Any, **k: Any) -> Any:
                if self.armed:
                    raise LocalStateError(
                        LocalStateErrorCode.ENTRY_CHANGED,
                        "local-state entry changed during validation",
                    )
                return real_bindings(*a, **k)

            monkeypatch.setattr(command_submission, "list_worker_bindings", bindings)
            return _factory(calls)

        if self.kind == "receipt_store_open":
            def reserve(*a: Any, **k: Any) -> Any:
                if self.armed:
                    raise sqlite3.OperationalError("database is locked")
                return real_reserve(*a, **k)

            monkeypatch.setattr(command_submission, "reserve_command_request", reserve)
            return _factory(calls)

        if self.kind == "socket_connect":
            def make_client(config: Config) -> Any:
                if self.armed:
                    raise ConnectionRefusedError("socket refused")
                return _FakeSocketClient(calls)

            return make_client

        if self.kind == "pane_resolution":
            def make_client(config: Config) -> _FakeSocketClient:
                if self.armed:
                    return _FakeSocketClient(
                        calls,
                        agent_get_raises=HerdrSocketTimeoutError("pane read timeout"),
                    )
                return _FakeSocketClient(calls)

            return make_client

        raise AssertionError(f"unknown injection {self.kind!r}")


TRANSIENT_KINDS = [
    "snapshot_store_local_state",
    "binding_store_sqlite",
    "binding_store_local_state",
    "receipt_store_open",
    "socket_connect",
    "pane_resolution",
]


@pytest.mark.parametrize("kind", TRANSIENT_KINDS)
def test_pre_send_transient_stays_retryable_then_succeeds_once(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    kind: str,
) -> None:
    config = _config(tmp_path)
    worker = _worker()
    _seed(config, [worker], [_binding(worker)])
    calls: list[dict[str, Any]] = []
    injection = _TransientInjection(kind)
    factory = injection.install(monkeypatch, calls)
    request_id = f"transient-{kind}"

    first = submit_command(config, _request(request_id=request_id), socket_client_factory=factory)

    # No external mutation began and no durable authority was written.
    assert first.ok is False
    assert first.status == STATUS_BACKEND_UNAVAILABLE
    assert first.disposition == DISPOSITION_NO_RECEIPT
    assert _sent_texts(calls) == []
    assert _receipt(config, request_id) is None
    assert _receipt_count(config) == 0

    # The transient clears; the same request ID succeeds exactly once.
    injection.armed = False
    recovered = submit_command(
        config, _request(request_id=request_id), socket_client_factory=factory
    )
    assert recovered.status == STATUS_ACCEPTED
    assert recovered.disposition == DISPOSITION_TERMINAL_ACCEPTED
    assert _sent_texts(calls) == ["hello"]
    receipt = _receipt(config, request_id)
    assert receipt is not None
    assert receipt["state"] == "accepted"

    # A later replay returns the stored accepted result without another send.
    replay = submit_command(
        config, _request(request_id=request_id), socket_client_factory=_forbidden_factory
    )
    assert replay.to_dict() == recovered.to_dict()
    assert _sent_texts(calls) == ["hello"]


@pytest.mark.parametrize("kind", TRANSIENT_KINDS)
def test_pre_send_transient_never_requires_a_new_request_id(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    kind: str,
) -> None:
    """Retrying is legal under the SAME id; a different id would double-send."""
    config = _config(tmp_path)
    worker = _worker()
    _seed(config, [worker], [_binding(worker)])
    calls: list[dict[str, Any]] = []
    injection = _TransientInjection(kind)
    factory = injection.install(monkeypatch, calls)

    first = submit_command(
        config, _request(request_id="same-id"), socket_client_factory=factory
    )
    assert first.disposition == DISPOSITION_NO_RECEIPT

    # Two more attempts under the same id while still transient: still no receipt,
    # still no send. Nothing accumulates.
    injection.armed = True
    again = submit_command(
        config, _request(request_id="same-id"), socket_client_factory=factory
    )
    assert again.disposition == DISPOSITION_NO_RECEIPT
    assert _receipt_count(config) == 0
    assert _sent_texts(calls) == []

    injection.armed = False
    done = submit_command(
        config, _request(request_id="same-id"), socket_client_factory=factory
    )
    assert done.status == STATUS_ACCEPTED
    assert _sent_texts(calls) == ["hello"]
    # Exactly one receipt for the one request id.
    assert _receipt_count(config) == 1


def test_receipt_store_transient_closes_the_prepared_client(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A receipt-store transient after a successful prepare must not leak the socket."""
    config = _config(tmp_path)
    worker = _worker()
    _seed(config, [worker], [_binding(worker)])
    closed: list[bool] = []

    class _TrackingClient(_FakeSocketClient):
        def close(self) -> None:
            closed.append(True)

    def factory(_config: Config) -> _TrackingClient:
        return _TrackingClient([])

    def reserve_raises(*a: Any, **k: Any) -> Any:
        raise sqlite3.OperationalError("database is locked")

    monkeypatch.setattr(command_submission, "reserve_command_request", reserve_raises)

    envelope = submit_command(
        config, _request(request_id="store-transient"), socket_client_factory=factory
    )

    assert envelope.status == STATUS_BACKEND_UNAVAILABLE
    assert envelope.disposition == DISPOSITION_NO_RECEIPT
    assert _receipt(config, "store-transient") is None
    # The prepared socket was opened (pane resolved) but never sent, and closed.
    assert closed == [True]


# ---------------------------------------------------------------------------
# Permanent pre-send failures: terminal and non-sending
# ---------------------------------------------------------------------------


def test_disallowed_worker_status_is_terminal(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    worker = _worker(status="closed")
    _seed(config, [worker], [_binding(worker)])
    calls: list[dict[str, Any]] = []

    first = submit_command(
        config, _request(request_id="disallowed"), socket_client_factory=_factory(calls)
    )
    replay = submit_command(
        config, _request(request_id="disallowed"), socket_client_factory=_forbidden_factory
    )

    assert first.status == STATUS_REJECTED
    assert first.disposition == DISPOSITION_TERMINAL_REJECTED
    assert replay.to_dict() == first.to_dict()
    assert _sent_texts(calls) == []
    receipt = _receipt(config, "disallowed")
    assert receipt is not None
    assert receipt["state"] == "rejected"


def test_missing_private_binding_is_terminal(
    tmp_path: Path,
) -> None:
    """A worker with no sendable backend binding is an unsupported target."""
    config = _config(tmp_path)
    worker = _worker()
    _seed(config, [worker], [])  # worker resolves, but no herdr binding exists
    calls: list[dict[str, Any]] = []

    first = submit_command(
        config, _request(request_id="no-binding"), socket_client_factory=_factory(calls)
    )
    replay = submit_command(
        config, _request(request_id="no-binding"), socket_client_factory=_forbidden_factory
    )

    assert first.status == STATUS_BACKEND_UNSUPPORTED
    assert first.disposition == DISPOSITION_TERMINAL_REJECTED
    assert replay.to_dict() == first.to_dict()
    assert _sent_texts(calls) == []
    receipt = _receipt(config, "no-binding")
    assert receipt is not None
    assert receipt["state"] == "rejected"


def test_stale_private_binding_is_terminal(
    tmp_path: Path,
) -> None:
    """A binding whose fingerprint no longer matches the worker is a proven stale target."""
    config = _config(tmp_path)
    worker = _worker()
    _seed(config, [worker], [_binding(worker, fingerprint="stale-observation")])
    calls: list[dict[str, Any]] = []

    first = submit_command(
        config, _request(request_id="stale-binding"), socket_client_factory=_factory(calls)
    )
    replay = submit_command(
        config, _request(request_id="stale-binding"), socket_client_factory=_forbidden_factory
    )

    assert first.status == STATUS_STALE_TARGET
    assert first.disposition == DISPOSITION_TERMINAL_REJECTED
    assert replay.to_dict() == first.to_dict()
    assert _sent_texts(calls) == []
    receipt = _receipt(config, "stale-binding")
    assert receipt is not None
    assert receipt["state"] == "rejected"


def test_authoritative_degraded_backend_is_terminal(
    tmp_path: Path,
) -> None:
    """A resolved worker plus a degraded backend is an authoritative rejection."""
    config = _config(tmp_path)
    worker = _worker()
    _seed(config, [worker], [_binding(worker)], health="degraded")
    calls: list[dict[str, Any]] = []

    first = submit_command(
        config, _request(request_id="degraded"), socket_client_factory=_factory(calls)
    )
    replay = submit_command(
        config, _request(request_id="degraded"), socket_client_factory=_forbidden_factory
    )

    assert first.status == STATUS_BACKEND_UNAVAILABLE
    assert first.disposition == DISPOSITION_TERMINAL_REJECTED
    assert replay.to_dict() == first.to_dict()
    assert _sent_texts(calls) == []
    receipt = _receipt(config, "degraded")
    assert receipt is not None
    assert receipt["state"] == "rejected"


def _pane_resolution_factory(kind: str, calls: list[dict[str, Any]]):
    """A socket factory whose pane resolution fails in a specific way."""
    from tendwire.backends.herdr_protocol import HerdrErrorResponse

    def make_client(config: Config) -> _FakeSocketClient:
        if kind == "no_pane":
            # Herdr answered, but the agent has no resolvable pane.
            return _FakeSocketClient(calls, agent_get_response={"result": {"agent": {}}})
        if kind == "herdr_error_response":
            # Herdr returned an authoritative error response.
            return _FakeSocketClient(
                calls,
                agent_get_raises=HerdrErrorResponse({"message": "no such agent"}, "rid"),
            )
        if kind == "unsupported":
            # The resolution response was malformed / unsupported.
            return _FakeSocketClient(calls, agent_get_raises=ValueError("bad agent info"))
        raise AssertionError(f"unknown pane failure {kind!r}")

    return make_client


@pytest.mark.parametrize("kind", ["no_pane", "herdr_error_response", "unsupported"])
def test_authoritative_pane_resolution_failure_is_terminal(
    tmp_path: Path,
    kind: str,
) -> None:
    """A definite backend answer during pane resolution is a proven target failure.

    These are not transport read failures: Herdr answered (or the response was
    unusable), and a same-ID retry would get the same answer. They must stay
    terminal so the connector advances instead of retrying to its horizon.
    """
    config = _config(tmp_path)
    worker = _worker()
    _seed(config, [worker], [_binding(worker)])
    calls: list[dict[str, Any]] = []
    factory = _pane_resolution_factory(kind, calls)
    request_id = f"pane-{kind}"

    first = submit_command(config, _request(request_id=request_id), socket_client_factory=factory)
    replay = submit_command(
        config, _request(request_id=request_id), socket_client_factory=_forbidden_factory
    )

    # A durable terminal rejection, replayed on retry, with no send.
    assert first.ok is False
    assert first.disposition == DISPOSITION_TERMINAL_REJECTED
    assert first.status in {"backend_failed", "backend_unavailable"}
    assert replay.to_dict() == first.to_dict()
    assert _sent_texts(calls) == []
    receipt = _receipt(config, request_id)
    assert receipt is not None
    assert receipt["state"] == "rejected"
    # The pane read was attempted exactly once and never repeated by the replay.
    assert [call["method"] for call in calls].count("agent.get") == 1


def test_transient_pane_read_failure_stays_retryable(
    tmp_path: Path,
) -> None:
    """A pane-read timeout is a transport failure, not an authoritative answer."""
    config = _config(tmp_path)
    worker = _worker()
    _seed(config, [worker], [_binding(worker)])
    calls: list[dict[str, Any]] = []
    connect_ok = {"value": False}

    def flaky_factory(config: Config) -> _FakeSocketClient:
        if not connect_ok["value"]:
            return _FakeSocketClient(
                calls,
                agent_get_raises=HerdrSocketTimeoutError("pane read timeout"),
            )
        return _FakeSocketClient(calls)

    first = submit_command(
        config, _request(request_id="pane-timeout"), socket_client_factory=flaky_factory
    )
    assert first.status == STATUS_BACKEND_UNAVAILABLE
    assert first.disposition == DISPOSITION_NO_RECEIPT
    assert _receipt(config, "pane-timeout") is None
    assert _sent_texts(calls) == []

    connect_ok["value"] = True
    recovered = submit_command(
        config, _request(request_id="pane-timeout"), socket_client_factory=flaky_factory
    )
    assert recovered.status == STATUS_ACCEPTED
    assert _sent_texts(calls) == ["hello"]


def test_transient_binding_store_never_masks_a_permanent_rejection(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Once the store recovers and the target is unsuitable, the truth is terminal."""
    config = _config(tmp_path)
    worker = _worker()
    # No binding: the target is permanently unsupported, but the binding store
    # is transiently unavailable on the first attempt.
    _seed(config, [worker], [])
    calls: list[dict[str, Any]] = []
    injection = _TransientInjection("binding_store_sqlite")
    factory = injection.install(monkeypatch, calls)

    transient = submit_command(
        config, _request(request_id="mixed"), socket_client_factory=factory
    )
    assert transient.disposition == DISPOSITION_NO_RECEIPT
    assert _receipt(config, "mixed") is None

    injection.armed = False
    terminal = submit_command(
        config, _request(request_id="mixed"), socket_client_factory=factory
    )
    assert terminal.status == STATUS_BACKEND_UNSUPPORTED
    assert terminal.disposition == DISPOSITION_TERMINAL_REJECTED
    assert _sent_texts(calls) == []


# ---------------------------------------------------------------------------
# Concurrency (barriers, not sleeps)
# ---------------------------------------------------------------------------


def test_concurrent_callers_with_one_transient_send_at_most_once(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Three same-id callers, a subset transient: one send, one terminal, no rejection."""
    config = _config(tmp_path)
    worker = _worker()
    _seed(config, [worker], [_binding(worker)])
    calls: list[dict[str, Any]] = []
    calls_lock = threading.Lock()
    real_bindings = command_submission.list_worker_bindings
    fail_budget = {"n": 2}  # two of three callers hit a transient binding-store error
    budget_lock = threading.Lock()

    def flaky_bindings(*a: Any, **k: Any) -> Any:
        with budget_lock:
            if fail_budget["n"] > 0:
                fail_budget["n"] -= 1
                raise sqlite3.OperationalError("database is locked")
        return real_bindings(*a, **k)

    monkeypatch.setattr(command_submission, "list_worker_bindings", flaky_bindings)

    class _SerializedClient(_FakeSocketClient):
        def request(self, method: str, params: dict[str, Any], *, timeout: float | None = None):
            with calls_lock:
                return super().request(method, params, timeout=timeout)

    def factory(_config: Config) -> _SerializedClient:
        return _SerializedClient(calls)

    barrier = threading.Barrier(3)
    results: list[Any] = []
    results_lock = threading.Lock()

    def attempt() -> None:
        barrier.wait(timeout=30)
        envelope = submit_command(
            config, _request(request_id="race"), socket_client_factory=factory
        )
        with results_lock:
            results.append(envelope)

    threads = [threading.Thread(target=attempt) for _ in range(3)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=30)

    assert not any(thread.is_alive() for thread in threads)
    assert len(results) == 3
    # At most one send, at most one terminal receipt, and no caller was durably
    # rejected because of the transient binding-store failure.
    assert _sent_texts(calls).count("hello") <= 1
    assert _receipt_count(config) <= 1
    for envelope in results:
        assert envelope.disposition != DISPOSITION_TERMINAL_REJECTED
        assert envelope.status in {
            STATUS_ACCEPTED,
            STATUS_PENDING,
            STATUS_BACKEND_UNAVAILABLE,
        }
    # The transient callers can retry the same id and converge on the result.
    settled = submit_command(
        config, _request(request_id="race"), socket_client_factory=factory
    )
    assert settled.status == STATUS_ACCEPTED
    assert _sent_texts(calls).count("hello") == 1


def test_transient_racing_reservation_creation_sends_once(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A transient caller cannot corrupt a concurrent caller's reservation+send."""
    config = _config(tmp_path)
    worker = _worker()
    _seed(config, [worker], [_binding(worker)])
    calls: list[dict[str, Any]] = []
    calls_lock = threading.Lock()
    real_reserve = command_submission.reserve_command_request
    at_reservation = threading.Event()
    let_reservation_proceed = threading.Event()

    def gated_reserve(*a: Any, **k: Any) -> Any:
        at_reservation.set()
        assert let_reservation_proceed.wait(timeout=30)
        return real_reserve(*a, **k)

    class _SerializedClient(_FakeSocketClient):
        def request(self, method: str, params: dict[str, Any], *, timeout: float | None = None):
            with calls_lock:
                return super().request(method, params, timeout=timeout)

    def factory(_config: Config) -> _SerializedClient:
        return _SerializedClient(calls)

    monkeypatch.setattr(command_submission, "reserve_command_request", gated_reserve)

    sender_result: list[Any] = []

    def sender() -> None:
        sender_result.append(
            submit_command(config, _request(request_id="rr"), socket_client_factory=factory)
        )

    sender_thread = threading.Thread(target=sender, name="sender")
    sender_thread.start()
    assert at_reservation.wait(timeout=30), "sender never reached reservation"

    # While the sender is parked at reservation, a second caller hits a transient
    # binding-store failure. It must not reserve, send, or reject durably.
    real_bindings = command_submission.list_worker_bindings

    def bindings_raise(*a: Any, **k: Any) -> Any:
        raise sqlite3.OperationalError("database is locked")

    monkeypatch.setattr(command_submission, "list_worker_bindings", bindings_raise)
    transient = submit_command(
        config, _request(request_id="rr"), socket_client_factory=factory
    )
    monkeypatch.setattr(command_submission, "list_worker_bindings", real_bindings)

    let_reservation_proceed.set()
    sender_thread.join(timeout=30)

    assert not sender_thread.is_alive()
    assert transient.disposition == DISPOSITION_NO_RECEIPT
    assert transient.status == STATUS_BACKEND_UNAVAILABLE
    assert sender_result[0].status == STATUS_ACCEPTED
    assert _sent_texts(calls) == ["hello"]
    assert _receipt_count(config) == 1


def test_transient_racing_send_start_cas_sends_once(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A transient retry arriving during the sender's send-start replays in-progress."""
    config = _config(tmp_path)
    worker = _worker()
    _seed(config, [worker], [_binding(worker)])
    calls: list[dict[str, Any]] = []
    calls_lock = threading.Lock()
    real_mark = command_submission.mark_command_send_started
    at_send_start = threading.Event()
    let_send_start_proceed = threading.Event()

    def gated_mark(*a: Any, **k: Any) -> Any:
        at_send_start.set()
        assert let_send_start_proceed.wait(timeout=30)
        return real_mark(*a, **k)

    monkeypatch.setattr(command_submission, "mark_command_send_started", gated_mark)

    class _SerializedClient(_FakeSocketClient):
        def request(self, method: str, params: dict[str, Any], *, timeout: float | None = None):
            with calls_lock:
                return super().request(method, params, timeout=timeout)

    def factory(_config: Config) -> _SerializedClient:
        return _SerializedClient(calls)

    sender_result: list[Any] = []

    def sender() -> None:
        sender_result.append(
            submit_command(config, _request(request_id="ss"), socket_client_factory=factory)
        )

    sender_thread = threading.Thread(target=sender, name="sender")
    sender_thread.start()
    assert at_send_start.wait(timeout=30), "sender never reached send-start"

    # The reservation now exists (state reserved). A retry that then hits a
    # transient binding-store failure must read in-progress from that receipt,
    # never a no-receipt failure and never a second send.
    real_bindings = command_submission.list_worker_bindings

    def bindings_raise(*a: Any, **k: Any) -> Any:
        raise sqlite3.OperationalError("database is locked")

    monkeypatch.setattr(command_submission, "list_worker_bindings", bindings_raise)
    retry = submit_command(config, _request(request_id="ss"), socket_client_factory=factory)
    monkeypatch.setattr(command_submission, "list_worker_bindings", real_bindings)

    let_send_start_proceed.set()
    sender_thread.join(timeout=30)

    assert not sender_thread.is_alive()
    assert retry.status == STATUS_PENDING
    assert retry.disposition == DISPOSITION_IN_PROGRESS
    assert sender_result[0].status == STATUS_ACCEPTED
    assert _sent_texts(calls) == ["hello"]
    assert _receipt_count(config) == 1


def test_retry_after_abandoned_reservation_with_transient_reports_in_progress(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An abandoned reservation plus a transient retry stays in-progress, not rejected."""
    config = _config(tmp_path)
    worker = _worker()
    _seed(config, [worker], [_binding(worker)])
    calls: list[dict[str, Any]] = []

    # Reserve then crash before send by losing the send-start response.
    real_mark = command_submission.mark_command_send_started

    def lose_send_start(*a: Any, **k: Any) -> Any:
        raise HerdrSocketTimeoutError("send-start response lost")

    monkeypatch.setattr(command_submission, "mark_command_send_started", lose_send_start)
    reserved = submit_command(
        config, _request(request_id="abandoned"), socket_client_factory=_factory(calls)
    )
    assert reserved.status == STATUS_PENDING
    monkeypatch.setattr(command_submission, "mark_command_send_started", real_mark)

    # Expire the crashed owner's lease, then retry while a transient binding-store
    # failure is active. The abandoned reservation must not be terminalized.
    assert config.db_path is not None
    with sqlite3.connect(str(config.db_path)) as conn:
        conn.execute(
            "UPDATE command_receipts SET owner_expires_at = ? WHERE request_id = ?",
            ("2020-01-01T00:00:00+00:00", "abandoned"),
        )

    def bindings_raise(*a: Any, **k: Any) -> Any:
        raise sqlite3.OperationalError("database is locked")

    monkeypatch.setattr(command_submission, "list_worker_bindings", bindings_raise)
    retry = submit_command(
        config, _request(request_id="abandoned"), socket_client_factory=_factory(calls)
    )

    assert retry.status == STATUS_PENDING
    assert retry.disposition == DISPOSITION_IN_PROGRESS
    assert _sent_texts(calls) == []
    receipt = _receipt(config, "abandoned")
    assert receipt is not None
    assert receipt["state"] == "reserved"


def test_process_response_loss_after_send_replays_without_second_send(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Once a send committed, a lost finish response is recovered, never re-sent."""
    config = _config(tmp_path)
    worker = _worker()
    _seed(config, [worker], [_binding(worker)])
    calls: list[dict[str, Any]] = []
    real_finish = command_submission.finish_command_request
    finished_state: dict[str, Any] = {}

    def finish_then_lose(*a: Any, **k: Any) -> Any:
        result = real_finish(*a, **k)
        finished_state["result"] = result
        raise HerdrSocketTimeoutError("finish response lost")

    monkeypatch.setattr(command_submission, "finish_command_request", finish_then_lose)
    first = submit_command(
        config, _request(request_id="resp-loss"), socket_client_factory=_factory(calls)
    )
    monkeypatch.setattr(command_submission, "finish_command_request", real_finish)

    # The send happened and the receipt committed accepted; the caller only lost
    # the response. A same-id retry replays it without a second send.
    assert _sent_texts(calls) == ["hello"]
    replay = submit_command(
        config, _request(request_id="resp-loss"), socket_client_factory=_forbidden_factory
    )
    assert replay.status == STATUS_ACCEPTED
    assert replay.disposition == DISPOSITION_TERMINAL_ACCEPTED
    assert _sent_texts(calls) == ["hello"]
    assert _receipt_count(config) == 1


# ---------------------------------------------------------------------------
# Paired connector boundary: the exact tuple Herdres retries
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("kind", TRANSIENT_KINDS)
def test_daemon_emits_the_retryable_tuple_herdres_expects(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    kind: str,
) -> None:
    """The daemon JSON for a transient is the ``backend_unavailable/no_receipt``
    tuple that the Herdres client validates and the reduce loop retries under the
    same request ID (see the Herdres client's ``_RETRY_DISPOSITIONS``). A durable
    ``terminal_rejected`` would instead advance the offset and drop the command.
    """
    config = _config(tmp_path)
    worker = _worker()
    _seed(config, [worker], [_binding(worker)])
    calls: list[dict[str, Any]] = []
    injection = _TransientInjection(kind)
    factory = injection.install(monkeypatch, calls)
    payload = _request(request_id=f"paired-{kind}")

    api = TendwireDaemonAPI(
        get_snapshot=lambda: Snapshot(
            host_id=HOST_ID, updated_at="2026-01-01T00:00:00+00:00", workers=[worker]
        ),
        get_health=lambda: {"ok": True},
        submit_command=lambda params: submit_command(
            config, params, socket_client_factory=factory
        ),
    )
    response = api.dispatch({"method": "command.submit", "params": payload, "id": "1"})
    result = response["result"]

    # The exact tuple asserted by the paired Herdres test
    # ``test_backend_unavailable_authority_comes_only_from_disposition``.
    assert result["ok"] is False
    assert result["status"] == STATUS_BACKEND_UNAVAILABLE
    assert result["disposition"] == DISPOSITION_NO_RECEIPT
    assert result["request_id"] == payload["request_id"]
    assert result["dry_run"] is False
    assert _receipt(config, payload["request_id"]) is None
    assert _sent_texts(calls) == []

    # The same request ID, once the transient clears, produces the accepted tuple
    # Herdres marks terminal -- exactly once.
    injection.armed = False
    accepted = api.dispatch({"method": "command.submit", "params": payload, "id": "2"})
    assert accepted["result"]["status"] == STATUS_ACCEPTED
    assert accepted["result"]["disposition"] == DISPOSITION_TERMINAL_ACCEPTED
    assert _sent_texts(calls) == ["hello"]


# ---------------------------------------------------------------------------
# Bounded real SQLite/local-state stress fixture
# ---------------------------------------------------------------------------


def test_real_store_contention_never_durably_rejects_an_unsent_command(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Reproduce the original contention shape against a real store, bounded.

    Many concurrent same-id submits run against a real SQLite store while a real
    SQLite ``OperationalError`` is injected into the binding-store read on a
    bounded fraction of attempts -- the exact failure the stress run surfaced.
    The invariant: at most one send, at most one terminal receipt, and no receipt
    is ever a rejection produced solely by a pre-send transient.
    """
    config = _config(tmp_path)
    worker = _worker()
    _seed(config, [worker], [_binding(worker)])
    calls: list[dict[str, Any]] = []
    calls_lock = threading.Lock()
    real_bindings = command_submission.list_worker_bindings
    attempt_counter = {"n": 0}
    counter_lock = threading.Lock()

    def contended_bindings(*a: Any, **k: Any) -> Any:
        with counter_lock:
            attempt_counter["n"] += 1
            # Fail the binding-store read on odd attempts with a real SQLite error.
            fail = attempt_counter["n"] % 2 == 1
        if fail:
            raise sqlite3.OperationalError("database is locked")
        return real_bindings(*a, **k)

    monkeypatch.setattr(command_submission, "list_worker_bindings", contended_bindings)

    class _SerializedClient(_FakeSocketClient):
        def request(self, method: str, params: dict[str, Any], *, timeout: float | None = None):
            with calls_lock:
                return super().request(method, params, timeout=timeout)

    def factory(_config: Config) -> _SerializedClient:
        return _SerializedClient(calls)

    barrier = threading.Barrier(8)
    results: list[Any] = []
    results_lock = threading.Lock()

    def attempt() -> None:
        barrier.wait(timeout=30)
        envelope = submit_command(
            config, _request(request_id="contended"), socket_client_factory=factory
        )
        with results_lock:
            results.append(envelope)

    threads = [threading.Thread(target=attempt) for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=30)

    assert not any(thread.is_alive() for thread in threads)
    assert len(results) == 8
    assert _sent_texts(calls).count("hello") <= 1
    assert _receipt_count(config) <= 1
    # No durable rejection was ever manufactured from the transient store errors.
    receipt = _receipt(config, "contended")
    if receipt is not None:
        assert receipt["state"] in {"reserved", "send_started", "accepted"}
    for envelope in results:
        if envelope.disposition == DISPOSITION_TERMINAL_REJECTED:
            pytest.fail("a pre-send transient produced a durable rejection")

    # After contention clears, exactly one accepted result stands.
    monkeypatch.setattr(command_submission, "list_worker_bindings", real_bindings)
    settled = submit_command(
        config, _request(request_id="contended"), socket_client_factory=factory
    )
    assert settled.status == STATUS_ACCEPTED
    assert _sent_texts(calls).count("hello") == 1
