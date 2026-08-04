"""Tests for the `tendwire command --json` CLI orchestration."""

from __future__ import annotations

import io
import json
import socket
import sqlite3
from pathlib import Path
from typing import Any

import pytest

from tendwire.backends.herdr_cli import HerdrCommandObservation
from tendwire.cli import main
from tendwire.core.commands import (
    DISPOSITION_IN_PROGRESS,
    DISPOSITION_NO_RECEIPT,
    DISPOSITION_TERMINAL_ACCEPTED,
    DISPOSITION_TERMINAL_REJECTED,
    DISPOSITION_TERMINAL_UNCERTAIN,
    STATUS_ACCEPTED,
    STATUS_AMBIGUOUS_BACKEND_TARGET,
    STATUS_BACKEND_FAILED,
    STATUS_BACKEND_UNAVAILABLE,
    STATUS_BACKEND_UNSUPPORTED,
    STATUS_DRY_RUN,
    STATUS_DUPLICATE_REQUEST,
    STATUS_INVALID_REQUEST,
    STATUS_NOT_FOUND,
    STATUS_REQUEST_STATE_UNCERTAIN,
    CommandEnvelope,
    CommandRequest,
    build_canonical_mutation,
)
from tendwire.core.models import Space, Worker, WorkerBinding
from tendwire.store.sqlite import (
    finish_command_request,
    get_command_request,
    init_store,
    list_worker_bindings,
    mark_command_send_started,
    reserve_command_request,
    upsert_worker_bindings,
)


@pytest.fixture(autouse=True)
def _isolate_cli_paths(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    private_home = tmp_path / "home"
    private_home.mkdir(mode=0o700)
    monkeypatch.setenv("HOME", str(private_home))
    monkeypatch.setenv("TENDWIRE_DATA_DIR", str(tmp_path / "tendwire-data"))
    monkeypatch.delenv("TENDWIRE_DB_PATH", raising=False)


def _fake_herdr_state(config: Any) -> tuple[list[Space], list[Worker]]:
    workers = [
        Worker(
            id="w-1",
            name="Alpha",
            status="active",
            space_id="s-1",
            backend_target={"kind": "agent_id", "value": "agent-1", "sendable": True, "reason": None},
        ),
        Worker(
            id="w-2",
            name="Beta",
            status="idle",
            space_id="s-1",
            backend_target={"kind": "agent_id", "value": "agent-2", "sendable": True, "reason": None},
        ),
    ]
    return [], workers


def _fake_herdr_command_observation(config: Any) -> HerdrCommandObservation:
    spaces, workers = _fake_herdr_state(config)
    return HerdrCommandObservation(
        spaces=spaces,
        workers=workers,
        status="healthy",
        outcome="healthy_non_empty",
    )




def _seed_uncertain_request(db_path: Path, request: CommandRequest) -> None:
    canonical = build_canonical_mutation(request, public_worker_id="w-1")
    pending = CommandEnvelope.from_result(
        request,
        ok=False,
        status=STATUS_REQUEST_STATE_UNCERTAIN,
        disposition=DISPOSITION_TERMINAL_UNCERTAIN,
        error={
            "code": STATUS_REQUEST_STATE_UNCERTAIN,
            "message": "pending",
            "details": {},
        },
    )
    reservation = reserve_command_request(
        db_path,
        host_id="cmd-host",
        request_id=request.request_id or "",
        action=request.action,
        canonical_version=canonical.canonical_version,
        canonical_fingerprint=canonical.fingerprint,
        canonical_request_json=canonical.canonical_json,
        public_worker_id=canonical.public_worker_id,
        pending_result_json=pending.to_json(),
    )
    owner_token = reservation["owner_token"]
    assert isinstance(owner_token, str)
    started = mark_command_send_started(
        db_path,
        host_id="cmd-host",
        request_id=request.request_id or "",
        canonical_fingerprint=canonical.fingerprint,
        owner_token=owner_token,
        binding_fingerprint="seed-binding",
    )
    assert started["status"] == "send_started"
    finished = finish_command_request(
        db_path,
        host_id="cmd-host",
        request_id=request.request_id or "",
        canonical_fingerprint=canonical.fingerprint,
        owner_token=owner_token,
        expected_state="send_started",
        terminal_state="uncertain",
        status=STATUS_REQUEST_STATE_UNCERTAIN,
        result_json=pending.to_json(),
    )
    assert finished["status"] == "uncertain"


def _seed_accepted_request(
    db_path: Path,
    request: CommandRequest,
    *,
    worker_id: str = "w-1",
) -> None:
    init_store(db_path)
    canonical = build_canonical_mutation(request, public_worker_id=worker_id)
    pending = CommandEnvelope.from_result(
        request,
        ok=False,
        status="pending",
        disposition=DISPOSITION_IN_PROGRESS,
        error={"code": "pending", "message": "pending"},
    )
    reservation = reserve_command_request(
        db_path,
        host_id="cmd-host",
        request_id=request.request_id or "",
        action=request.action,
        canonical_version=canonical.canonical_version,
        canonical_fingerprint=canonical.fingerprint,
        canonical_request_json=canonical.canonical_json,
        public_worker_id=canonical.public_worker_id,
        pending_result_json=pending.to_json(),
    )
    owner_token = reservation["owner_token"]
    assert isinstance(owner_token, str)
    started = mark_command_send_started(
        db_path,
        host_id="cmd-host",
        request_id=request.request_id or "",
        canonical_fingerprint=canonical.fingerprint,
        owner_token=owner_token,
        binding_fingerprint="seed-binding",
    )
    assert started["status"] == "send_started"
    accepted = CommandEnvelope.from_result(
        request,
        ok=True,
        status=STATUS_ACCEPTED,
        disposition=DISPOSITION_TERMINAL_ACCEPTED,
        result={"target": {"worker_id": worker_id}},
    )
    finished = finish_command_request(
        db_path,
        host_id="cmd-host",
        request_id=request.request_id or "",
        canonical_fingerprint=canonical.fingerprint,
        owner_token=owner_token,
        expected_state="send_started",
        terminal_state="accepted",
        status=STATUS_ACCEPTED,
        result_json=accepted.to_json(),
    )
    assert finished["status"] == "accepted"


def test_cli_command_invalid_json(capsys, monkeypatch) -> None:
    monkeypatch.setattr("sys.stdin", io.StringIO("not json"))
    code = main(
        [
            "--host-id",
            "cmd-host",
            "--herdr-bin",
            "definitely-not-a-real-herdr-binary",
            "command",
            "--json",
        ]
    )
    captured = capsys.readouterr()
    assert code == 1
    payload = json.loads(captured.out)
    assert payload["ok"] is False
    assert payload["status"] == STATUS_INVALID_REQUEST


def test_cli_command_noop_success(capsys, monkeypatch) -> None:
    calls: list[str] = []

    def guarded_fetch(config: Any) -> tuple[list[Space], list[Worker]]:
        calls.append("fetch")
        raise AssertionError("noop must not fetch Herdr state")

    monkeypatch.setattr("tendwire.cli.fetch_herdr_state", guarded_fetch)
    monkeypatch.setattr(
        "sys.stdin",
        io.StringIO(json.dumps({"schema_version": 1, "action": "noop"})),
    )
    code = main(
        [
            "--host-id",
            "cmd-host",
            "--herdr-bin",
            "definitely-not-a-real-herdr-binary",
            "command",
            "--json",
        ]
    )
    captured = capsys.readouterr()
    assert code == 0
    payload = json.loads(captured.out)
    assert payload["ok"] is True
    assert payload["status"] == "noop"
    assert payload["schema_version"] == 2
    assert payload["disposition"] == DISPOSITION_NO_RECEIPT
    assert captured.err == ""
    assert calls == []


def test_cli_command_unknown_action_rejected(capsys, monkeypatch) -> None:
    monkeypatch.setattr(
        "sys.stdin",
        io.StringIO(json.dumps({"schema_version": 1, "action": "explode"})),
    )
    code = main(
        [
            "--host-id",
            "cmd-host",
            "--herdr-bin",
            "definitely-not-a-real-herdr-binary",
            "command",
            "--json",
        ]
    )
    captured = capsys.readouterr()
    assert code == 1
    payload = json.loads(captured.out)
    assert payload["ok"] is False
    assert captured.err == ""


def test_cli_command_fingerprint_only_target_rejected(capsys, monkeypatch, tmp_path) -> None:
    """A fingerprint-only send is invalid at the CLI, before any store or backend work."""
    db_path = tmp_path / "fingerprint-only.db"
    monkeypatch.setattr(
        "sys.stdin",
        io.StringIO(
            json.dumps(
                {
                    "schema_version": 1,
                    "action": "send_instruction",
                    "request_id": "fingerprint-only",
                    "dry_run": False,
                    "target": {"worker_fingerprint": "fingerprint-A"},
                    "instruction": {"text": "hello"},
                }
            )
        ),
    )

    code = main(
        [
            "--host-id",
            "cmd-host",
            "--socket-path",
            str(tmp_path / "unavailable.sock"),
            "command",
            "--json",
            "--db-path",
            str(db_path),
        ]
    )

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert code == 1
    assert payload["ok"] is False
    assert payload["status"] == "invalid_request"
    assert payload["disposition"] == DISPOSITION_NO_RECEIPT
    assert captured.err == ""
    # A request this malformed never reaches durable state.
    assert not db_path.exists()


def test_cli_command_read_snapshot_neutral_result(capsys, monkeypatch) -> None:
    monkeypatch.setattr(
        "sys.stdin",
        io.StringIO(json.dumps({"schema_version": 1, "action": "read_snapshot"})),
    )
    code = main(
        [
            "--host-id",
            "cmd-host",
            "--herdr-bin",
            "definitely-not-a-real-herdr-binary",
            "command",
            "--json",
        ]
    )
    captured = capsys.readouterr()
    assert code == 0
    payload = json.loads(captured.out)
    assert payload["ok"] is True
    assert payload["status"] == "snapshot"
    assert payload["result"]["snapshot"]["schema_version"] == 2
    assert payload["result"]["snapshot"]["backend_health"][0]["status"] == "unavailable"
    assert payload["result"]["snapshot"]["backend_health"][0]["outcome"] == "missing_binary"
    assert captured.err == ""


@pytest.mark.parametrize(
    ("request_payload", "expected_result"),
    [
        (
            {
                "schema_version": 1,
                "action": "send_instruction",
                "target": {"name": "Alpha"},
                "instruction": {"text": "hello"},
            },
            {
                "target": {"name": "Alpha"},
                "instruction": {"text": "hello"},
            },
        ),
        (
            {
                "schema_version": 1,
                "action": "answer_pending",
                "params": {
                    "pending_id": "pending-public",
                    "pending_fingerprint": "revision-public",
                    "choice_id": "choice-public",
                },
            },
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
def test_cli_command_mutation_dry_run_is_pure_and_creates_no_receipt(
    request_payload: dict[str, Any],
    expected_result: dict[str, Any],
    capsys,
    monkeypatch,
    tmp_path: Path,
) -> None:
    calls: list[str] = []

    def forbidden(*args: Any, **kwargs: Any) -> Any:
        calls.append("io")
        raise AssertionError("dry-run must not call daemon, backend, or command store")

    monkeypatch.setattr("tendwire.cli._try_daemon_attempt", forbidden)
    monkeypatch.setattr("tendwire.command_submission.get_command_request", forbidden)
    monkeypatch.setattr("tendwire.command_submission._current_snapshot", forbidden)
    monkeypatch.setattr("tendwire.command_submission.reserve_command_request", forbidden)

    db_path = tmp_path / f"{request_payload['action']}.db"
    init_store(db_path)
    monkeypatch.setattr("sys.stdin", io.StringIO(json.dumps(request_payload)))
    code = main(
        [
            "--host-id",
            "cmd-host",
            "--socket-path",
            str(tmp_path / "unavailable.sock"),
            "command",
            "--json",
            "--db-path",
            str(db_path),
        ]
    )
    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert code == 0
    assert payload["ok"] is True
    assert payload["status"] == "dry_run"
    assert payload["dry_run"] is True
    assert payload["disposition"] == DISPOSITION_NO_RECEIPT
    assert payload["result"] == expected_result
    assert calls == []
    with sqlite3.connect(str(db_path)) as conn:
        assert conn.execute("SELECT COUNT(*) FROM command_receipts").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM commands").fetchone()[0] == 0


@pytest.mark.parametrize("mode", ["cli_socket_path", "env_socket_path", "env_backend_socket"])
def test_cli_command_socket_mode_mutation_unavailable_does_not_fallback(
    mode: str,
    capsys,
    monkeypatch,
    tmp_path: Path,
) -> None:
    calls: list[str] = []

    def guarded_fetch(*args: Any, **kwargs: Any) -> HerdrCommandObservation:
        calls.append("fetch")
        raise AssertionError("explicit daemon/socket mode must not fall back to Herdr observation")

    def guarded_send(*args: Any, **kwargs: Any) -> CommandEnvelope:
        calls.append("send")
        raise AssertionError("explicit daemon/socket mode must not send through Herdr CLI")

    monkeypatch.delenv("TENDWIRE_SOCKET_PATH", raising=False)
    monkeypatch.delenv("TENDWIRE_HERDR_BACKEND", raising=False)
    monkeypatch.delenv("TENDWIRE_DATA_DIR", raising=False)



    socket_path = tmp_path / f"{mode}.sock"
    data_dir = tmp_path / "data"
    args = [
        "--host-id",
        "cmd-host",
        "--herdr-bin",
        "definitely-not-a-real-herdr-binary",
    ]
    forbidden_fragments = [str(socket_path)]
    if mode == "cli_socket_path":
        args.extend(["--socket-path", str(socket_path)])
    elif mode == "env_socket_path":
        monkeypatch.setenv("TENDWIRE_SOCKET_PATH", str(socket_path))
    elif mode == "env_backend_socket":
        monkeypatch.setenv("TENDWIRE_HERDR_BACKEND", "socket")
        monkeypatch.setenv("TENDWIRE_DATA_DIR", str(data_dir))
        forbidden_fragments.extend([str(data_dir), "tendwire.sock"])
    else:
        raise AssertionError(f"unexpected mode {mode}")

    db_path = tmp_path / f"{mode}.db"
    request_id = f"daemon-unavailable-{mode}"
    monkeypatch.setattr(
        "sys.stdin",
        io.StringIO(
            json.dumps(
                {
                    "schema_version": 1,
                    "action": "send_instruction",
                    "request_id": request_id,
                    "dry_run": False,
                    "target": {"worker_id": "w-1"},
                    "instruction": {"text": "hello"},
                }
            )
        ),
    )

    code = main(
        [
            *args,
            "command",
            "--json",
            "--db-path",
            str(db_path),
        ]
    )
    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    serialized = json.dumps(payload)

    assert code == 1
    assert captured.err == ""
    assert payload["ok"] is False
    assert payload["status"] == STATUS_BACKEND_UNAVAILABLE
    assert payload["disposition"] == DISPOSITION_NO_RECEIPT
    assert payload["request_id"] == request_id
    assert calls == []
    for fragment in forbidden_fragments:
        assert fragment not in serialized
    _assert_no_command_public_forbidden_fields(payload)

    # A definite pre-start daemon failure never creates a local receipt.
    assert get_command_request(db_path, "cmd-host", request_id) is None


def test_cli_daemon_client_uses_method_specific_timeouts(
    capsys,
    monkeypatch,
    tmp_path: Path,
) -> None:
    calls: list[tuple[str, float]] = []

    class FakeDaemonAPIClient:
        def __init__(self, socket_path: Any, *, timeout_seconds: float, max_response_bytes: int = 1024 * 1024):
            self.timeout_seconds = timeout_seconds

        def request(self, method: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
            calls.append((method, self.timeout_seconds))
            if method == "snapshot.get":
                return {"ok": True, "result": {"schema_version": 2, "spaces": [], "workers": []}}
            return {
                "ok": True,
                "result": {
                    "schema_version": 2,
                    "action": "send_instruction",
                    "request_id": "daemon-timeout-method",
                    "ok": True,
                    "dry_run": False,
                    "status": STATUS_ACCEPTED,
                    "disposition": DISPOSITION_TERMINAL_ACCEPTED,
                    "result": {},
                    "error": None,
                    "warnings": [],
                },
            }

    monkeypatch.setattr("tendwire.daemon_api.DaemonAPIClient", FakeDaemonAPIClient)
    socket_path = tmp_path / "daemon.sock"

    assert (
        main(
            [
                "--host-id",
                "cmd-host",
                "--socket-path",
                str(socket_path),
                "snapshot",
                "--json",
            ]
        )
        == 0
    )
    capsys.readouterr()

    monkeypatch.setattr(
        "sys.stdin",
        io.StringIO(
            json.dumps(
                {
                    "schema_version": 1,
                    "action": "send_instruction",
                    "request_id": "daemon-timeout-method",
                    "dry_run": False,
                    "target": {"worker_id": "w-1"},
                    "instruction": {"text": "hello"},
                }
            )
        ),
    )
    assert (
        main(
            [
                "--host-id",
                "cmd-host",
                "--socket-path",
                str(socket_path),
                "command",
                "--json",
                "--db-path",
                str(tmp_path / "cmd.db"),
            ]
        )
        == 0
    )

    assert calls[0] == ("snapshot.get", 2.0)
    assert calls[1][0] == "command.submit"
    assert calls[1][1] > calls[0][1]
    assert calls[1][1] >= 5.0


def test_cli_command_socket_mode_daemon_timeout_is_uncertain(
    capsys,
    monkeypatch,
    tmp_path: Path,
) -> None:
    calls: list[str] = []

    def guarded_fetch(*args: Any, **kwargs: Any) -> HerdrCommandObservation:
        calls.append("fetch")
        raise AssertionError("explicit daemon/socket mode must not fall back to Herdr observation")

    def guarded_send(*args: Any, **kwargs: Any) -> CommandEnvelope:
        calls.append("send")
        raise AssertionError("explicit daemon/socket mode must not send through Herdr CLI")

    class TimeoutDaemonAPIClient:
        def __init__(self, socket_path: Any, *, timeout_seconds: float, max_response_bytes: int = 1024 * 1024):
            self.timeout_seconds = timeout_seconds

        def request(self, method: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
            from tendwire.daemon_api import DaemonUnavailable

            try:
                raise socket.timeout("timed out")
            except socket.timeout as exc:
                raise DaemonUnavailable(
                    "timed out",
                    timed_out=True,
                    request_started=True,
                ) from exc



    monkeypatch.setattr("tendwire.daemon_api.DaemonAPIClient", TimeoutDaemonAPIClient)

    db_path = tmp_path / "timeout.db"
    request_id = "daemon-timeout-uncertain"
    monkeypatch.setattr(
        "sys.stdin",
        io.StringIO(
            json.dumps(
                {
                    "schema_version": 1,
                    "action": "send_instruction",
                    "request_id": request_id,
                    "dry_run": False,
                    "target": {"worker_id": "w-1"},
                    "instruction": {"text": "hello"},
                }
            )
        ),
    )

    code = main(
        [
            "--host-id",
            "cmd-host",
            "--socket-path",
            str(tmp_path / "daemon.sock"),
            "command",
            "--json",
            "--db-path",
            str(db_path),
        ]
    )
    captured = capsys.readouterr()

    assert code == 2
    assert captured.out == ""
    assert "unresolved" in captured.err
    assert calls == []

    # A lost daemon response with no authoritative receipt is not a command
    # envelope and must never be labeled terminal uncertainty.
    assert get_command_request(db_path, "cmd-host", request_id) is None


def test_cli_rejects_malformed_daemon_inner_envelope_without_fabricating_json(
    capsys,
    monkeypatch,
    tmp_path: Path,
) -> None:
    request = {
        "schema_version": 1,
        "action": "send_instruction",
        "request_id": "malformed-daemon-result",
        "dry_run": False,
        "target": {"worker_id": "w-1"},
        "instruction": {"text": "hello"},
    }

    class MalformedResultClient:
        def __init__(
            self,
            socket_path: Any,
            *,
            timeout_seconds: float,
            max_response_bytes: int = 1024 * 1024,
        ) -> None:
            del socket_path, timeout_seconds, max_response_bytes

        def request(
            self,
            method: str,
            params: dict[str, Any] | None = None,
        ) -> dict[str, Any]:
            assert method == "command.submit"
            assert params == request
            return {
                "schema_version": 1,
                "ok": True,
                "status": "ok",
                "result": {
                    "schema_version": 2,
                    "action": request["action"],
                    "request_id": request["request_id"],
                    "ok": True,
                    "dry_run": False,
                    "status": STATUS_ACCEPTED,
                    "result": {},
                    "error": None,
                    "warnings": [],
                },
                "error": None,
            }

    monkeypatch.setattr(
        "tendwire.daemon_api.DaemonAPIClient",
        MalformedResultClient,
    )
    monkeypatch.setattr("sys.stdin", io.StringIO(json.dumps(request)))
    db_path = tmp_path / "malformed-result.db"

    code = main(
        [
            "--host-id",
            "cmd-host",
            "--socket-path",
            str(tmp_path / "daemon.sock"),
            "command",
            "--json",
            "--db-path",
            str(db_path),
        ]
    )
    captured = capsys.readouterr()

    assert code == 2
    assert captured.out == ""
    assert "unresolved" in captured.err
    assert get_command_request(
        db_path,
        "cmd-host",
        "malformed-daemon-result",
    ) is None


@pytest.mark.parametrize(
    ("case", "response_request_id", "ok", "status", "disposition"),
    [
        (
            "receipt-null-id",
            None,
            True,
            STATUS_ACCEPTED,
            DISPOSITION_TERMINAL_ACCEPTED,
        ),
        (
            "receipt-invalid-id",
            "invalid id",
            True,
            STATUS_ACCEPTED,
            DISPOSITION_TERMINAL_ACCEPTED,
        ),
        (
            "no-receipt-success",
            "illegal-daemon-tuple",
            True,
            STATUS_BACKEND_UNAVAILABLE,
            DISPOSITION_NO_RECEIPT,
        ),
        (
            "no-receipt-accepted",
            "illegal-daemon-tuple",
            False,
            STATUS_ACCEPTED,
            DISPOSITION_NO_RECEIPT,
        ),
        (
            "no-receipt-pending",
            "illegal-daemon-tuple",
            False,
            "pending",
            DISPOSITION_NO_RECEIPT,
        ),
        (
            "no-receipt-uncertain",
            "illegal-daemon-tuple",
            False,
            STATUS_REQUEST_STATE_UNCERTAIN,
            DISPOSITION_NO_RECEIPT,
        ),
    ],
)
def test_cli_strictly_rejects_illegal_daemon_disposition_tuples(
    case: str,
    response_request_id: Any,
    ok: bool,
    status: str,
    disposition: str,
    capsys,
    monkeypatch,
    tmp_path: Path,
) -> None:
    request = {
        "schema_version": 1,
        "action": "send_instruction",
        "request_id": "illegal-daemon-tuple",
        "dry_run": False,
        "target": {"worker_id": "w-1"},
        "instruction": {"text": "hello"},
    }
    inner = {
        "schema_version": 2,
        "action": request["action"],
        "request_id": response_request_id,
        "ok": ok,
        "dry_run": False,
        "status": status,
        "disposition": disposition,
        "result": {},
        "error": None if ok else {"code": status, "message": "invalid tuple"},
        "warnings": [],
    }

    class IllegalTupleClient:
        def __init__(
            self,
            socket_path: Any,
            *,
            timeout_seconds: float,
            max_response_bytes: int = 1024 * 1024,
        ) -> None:
            del socket_path, timeout_seconds, max_response_bytes

        def request(
            self,
            method: str,
            params: dict[str, Any] | None = None,
        ) -> dict[str, Any]:
            assert method == "command.submit"
            assert params == request
            return {
                "schema_version": 1,
                "ok": True,
                "status": "ok",
                "result": inner,
                "error": None,
            }

    monkeypatch.setattr("tendwire.daemon_api.DaemonAPIClient", IllegalTupleClient)
    monkeypatch.setattr("sys.stdin", io.StringIO(json.dumps(request)))
    db_path = tmp_path / f"{case}.db"

    code = main(
        [
            "--host-id",
            "cmd-host",
            "--socket-path",
            str(tmp_path / "daemon.sock"),
            "command",
            "--json",
            "--db-path",
            str(db_path),
        ]
    )
    captured = capsys.readouterr()

    assert code == 2
    assert captured.out == ""
    assert "unresolved" in captured.err
    assert get_command_request(
        db_path,
        "cmd-host",
        request["request_id"],
    ) is None


def test_cli_daemon_response_loss_recovers_accepted_receipt_exactly_once(
    capsys,
    monkeypatch,
    tmp_path: Path,
) -> None:
    from tendwire.daemon_api import DaemonProtocolError

    db_path = tmp_path / "accepted-loss.db"
    init_store(db_path)
    request_payload = {
        "schema_version": 1,
        "action": "send_instruction",
        "request_id": "accepted-loss",
        "dry_run": False,
        "target": {"worker_id": "w-1"},
        "instruction": {"text": "hello"},
    }
    request = CommandRequest.from_dict(request_payload)
    canonical = build_canonical_mutation(request, public_worker_id="w-1")
    effects: list[str] = []

    class LostAcceptedResponseClient:
        def __init__(
            self,
            socket_path: Any,
            *,
            timeout_seconds: float,
            max_response_bytes: int = 1024 * 1024,
        ) -> None:
            del socket_path, timeout_seconds, max_response_bytes

        def request(
            self,
            method: str,
            params: dict[str, Any] | None = None,
        ) -> dict[str, Any]:
            assert method == "command.submit"
            assert params == request_payload
            pending = CommandEnvelope.from_result(
                request,
                ok=False,
                status="pending",
                disposition=DISPOSITION_IN_PROGRESS,
                error={"code": "pending", "message": "pending"},
            )
            reservation = reserve_command_request(
                db_path,
                host_id="cmd-host",
                request_id=request.request_id or "",
                action=request.action,
                canonical_version=canonical.canonical_version,
                canonical_fingerprint=canonical.fingerprint,
                canonical_request_json=canonical.canonical_json,
                public_worker_id=canonical.public_worker_id,
                pending_result_json=pending.to_json(),
            )
            owner_token = reservation["owner_token"]
            mark_command_send_started(
                db_path,
                host_id="cmd-host",
                request_id=request.request_id or "",
                canonical_fingerprint=canonical.fingerprint,
                owner_token=owner_token,
                binding_fingerprint="private-binding",
            )
            effects.append("sent")
            accepted = CommandEnvelope.from_result(
                request,
                ok=True,
                status=STATUS_ACCEPTED,
                disposition=DISPOSITION_TERMINAL_ACCEPTED,
                result={"target": {"worker_id": "w-1"}},
            )
            finish_command_request(
                db_path,
                host_id="cmd-host",
                request_id=request.request_id or "",
                canonical_fingerprint=canonical.fingerprint,
                owner_token=owner_token,
                expected_state="send_started",
                terminal_state="accepted",
                status=STATUS_ACCEPTED,
                result_json=accepted.to_json(),
            )
            raise DaemonProtocolError(
                "accepted response lost",
                request_started=True,
            )

    monkeypatch.setattr(
        "tendwire.daemon_api.DaemonAPIClient",
        LostAcceptedResponseClient,
    )
    monkeypatch.setattr("sys.stdin", io.StringIO(json.dumps(request_payload)))

    code = main(
        [
            "--host-id",
            "cmd-host",
            "--socket-path",
            str(tmp_path / "daemon.sock"),
            "command",
            "--json",
            "--db-path",
            str(db_path),
        ]
    )
    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert code == 0
    assert captured.err == ""
    assert payload["status"] == STATUS_ACCEPTED
    assert payload["disposition"] == DISPOSITION_TERMINAL_ACCEPTED
    assert effects == ["sent"]
    receipt = get_command_request(db_path, "cmd-host", "accepted-loss")
    assert receipt is not None
    assert receipt["state"] == "accepted"
    _assert_no_command_public_forbidden_fields(payload)


@pytest.mark.parametrize(
    ("case", "current_target", "expected_status"),
    [
        (
            "changed-worker-id",
            {"worker_id": "w-2"},
            STATUS_DUPLICATE_REQUEST,
        ),
        ("mutable-name", {"name": "Alpha"}, None),
        ("mutable-space", {"space_id": "space-1"}, None),
        (
            "worker-precondition",
            {"worker_id": "w-1", "worker_fingerprint": "current-fingerprint"},
            STATUS_ACCEPTED,
        ),
        (
            "worker-plus-null-alias",
            {"worker_id": "w-1", "name": None},
            None,
        ),
    ],
)
def test_cli_response_loss_reconciliation_only_uses_stored_explicit_worker_identity(
    case: str,
    current_target: dict[str, Any],
    expected_status: str | None,
    capsys,
    monkeypatch,
    tmp_path: Path,
) -> None:
    request_id = f"response-loss-{case}"
    db_path = tmp_path / f"{case}.db"
    stored_request = CommandRequest(
        action="send_instruction",
        request_id=request_id,
        dry_run=False,
        target={"worker_id": "w-1"},
        instruction={"text": "hello"},
    )
    _seed_accepted_request(db_path, stored_request)
    current_request = {
        **stored_request.to_dict(),
        "target": current_target,
    }
    daemon_calls: list[tuple[str, dict[str, Any]]] = []

    def stored_rows() -> tuple[list[tuple[Any, ...]], list[tuple[Any, ...]]]:
        with sqlite3.connect(str(db_path)) as conn:
            return (
                conn.execute("SELECT * FROM command_receipts ORDER BY id").fetchall(),
                conn.execute("SELECT * FROM events ORDER BY id").fetchall(),
            )

    rows_before = stored_rows()

    class LostResponseClient:
        def __init__(
            self,
            socket_path: Any,
            *,
            timeout_seconds: float,
            max_response_bytes: int = 1024 * 1024,
        ) -> None:
            del socket_path, timeout_seconds, max_response_bytes

        def request(
            self,
            method: str,
            params: dict[str, Any] | None = None,
        ) -> dict[str, Any]:
            from tendwire.daemon_api import DaemonProtocolError

            daemon_calls.append((method, dict(params or {})))
            raise DaemonProtocolError("response lost", request_started=True)

    def forbidden(*args: Any, **kwargs: Any) -> Any:
        del args, kwargs
        raise AssertionError(
            "response-loss reconciliation must not submit or resolve mutable authority"
        )

    monkeypatch.setattr("tendwire.daemon_api.DaemonAPIClient", LostResponseClient)
    monkeypatch.setattr("tendwire.command_submission._current_snapshot", forbidden)
    monkeypatch.setattr("tendwire.cli.command_envelope_from_payload", forbidden)
    monkeypatch.setattr("sys.stdin", io.StringIO(json.dumps(current_request)))

    code = main(
        [
            "--host-id",
            "cmd-host",
            "--socket-path",
            str(tmp_path / "daemon.sock"),
            "command",
            "--json",
            "--db-path",
            str(db_path),
        ]
    )
    captured = capsys.readouterr()

    assert daemon_calls == [("command.submit", current_request)]
    assert stored_rows() == rows_before
    if expected_status is None:
        assert code == 2
        assert captured.out == ""
        assert "unresolved" in captured.err
    elif expected_status == STATUS_ACCEPTED:
        assert code == 0
        assert captured.err == ""
        payload = json.loads(captured.out)
        assert payload["status"] == STATUS_ACCEPTED
        assert payload["disposition"] == DISPOSITION_TERMINAL_ACCEPTED
        _assert_no_command_public_forbidden_fields(payload)
    else:
        assert code == 1
        assert captured.err == ""
        payload = json.loads(captured.out)
        assert payload["status"] == expected_status
        assert payload["status"] != STATUS_ACCEPTED
        assert payload["disposition"] == DISPOSITION_TERMINAL_REJECTED
        _assert_no_command_public_forbidden_fields(payload)


@pytest.mark.parametrize(
    ("action", "action_fields"),
    [
        (
            "send_instruction",
            {
                "target": {"worker_id": "w-1"},
                "instruction": {"text": "hello"},
            },
        ),
        (
            "answer_pending",
            {
                "params": {
                    "pending_id": "pending-" + ("a" * 24),
                    "pending_fingerprint": "b" * 24,
                    "choice_id": "choice-" + ("c" * 24),
                },
            },
        ),
    ],
    ids=["send", "answer"],
)
@pytest.mark.parametrize(
    "request_started",
    [False, True],
    ids=["pre-start", "may-have-started"],
)
def test_cli_mutations_never_fallback_or_write_receipt_on_daemon_edge_failure(
    action: str,
    action_fields: dict[str, Any],
    request_started: bool,
    capsys,
    monkeypatch,
    tmp_path: Path,
) -> None:
    calls: list[tuple[str, dict[str, Any]]] = []

    class FailingDaemonAPIClient:
        def __init__(
            self,
            socket_path: Any,
            *,
            timeout_seconds: float,
            max_response_bytes: int = 1024 * 1024,
        ) -> None:
            del socket_path, timeout_seconds, max_response_bytes

        def request(
            self,
            method: str,
            params: dict[str, Any] | None = None,
        ) -> dict[str, Any]:
            from tendwire.daemon_api import DaemonProtocolError

            calls.append((method, dict(params or {})))
            raise DaemonProtocolError(
                "deterministic protocol failure",
                request_started=request_started,
            )

    request_id = f"{action}-{request_started}"
    request = {
        "schema_version": 1,
        "action": action,
        "request_id": request_id,
        "dry_run": False,
        **action_fields,
    }
    db_path = tmp_path / f"{action}-{request_started}.db"
    monkeypatch.setattr(
        "tendwire.daemon_api.DaemonAPIClient",
        FailingDaemonAPIClient,
    )
    monkeypatch.setattr("sys.stdin", io.StringIO(json.dumps(request)))

    code = main(
        [
            "--host-id",
            "cmd-host",
            "--socket-path",
            str(tmp_path / "daemon.sock"),
            "command",
            "--json",
            "--db-path",
            str(db_path),
        ]
    )
    captured = capsys.readouterr()

    assert calls == [("command.submit", request)]
    assert get_command_request(db_path, "cmd-host", request_id) is None
    if request_started:
        assert code == 2
        assert captured.out == ""
        assert "unresolved" in captured.err
    else:
        payload = json.loads(captured.out)
        assert code == 1
        assert captured.err == ""
        assert payload["status"] == STATUS_BACKEND_UNAVAILABLE
        assert payload["disposition"] == DISPOSITION_NO_RECEIPT
        assert payload["request_id"] == request_id
        _assert_no_command_public_forbidden_fields(payload)


@pytest.mark.parametrize(
    ("request_id", "include_request_id"),
    [
        (None, False),
        (None, True),
        ("", True),
        ("   \t", True),
        (" leading", True),
        ("trailing ", True),
        ("\twrapped\t", True),
    ],
)
def test_cli_command_send_instruction_non_dry_run_requires_request_id(
    capsys,
    monkeypatch,
    tmp_path: Path,
    request_id: Any,
    include_request_id: bool,
) -> None:
    calls: list[str] = []

    def guarded(*args: Any, **kwargs: Any) -> Any:
        calls.append("called")
        raise AssertionError("invalid request_id must stop before backend or store mutation")

    monkeypatch.setattr("tendwire.command_submission.reserve_command_request", guarded)
    payload: dict[str, Any] = {
        "schema_version": 1,
        "action": "send_instruction",
        "dry_run": False,
        "target": {"worker_id": "w-1"},
        "instruction": {"text": "hello"},
    }
    if include_request_id:
        payload["request_id"] = request_id
    monkeypatch.setattr("sys.stdin", io.StringIO(json.dumps(payload)))
    db_path = tmp_path / "invalid-request-id.db"

    code = main(
        [
            "--host-id",
            "cmd-host",
            "--herdr-bin",
            "definitely-not-a-real-herdr-binary",
            "command",
            "--json",
            "--db-path",
            str(db_path),
        ]
    )

    captured = capsys.readouterr()
    assert code == 1
    payload_out = json.loads(captured.out)
    assert payload_out["ok"] is False
    assert payload_out["status"] == STATUS_INVALID_REQUEST
    assert calls == []
    assert not db_path.exists()

def test_cli_command_send_instruction_requires_socket_backend_for_mutation(
    capsys, monkeypatch, tmp_path: Path
) -> None:
    db_path = tmp_path / "literal-false.db"
    calls: list[tuple[Any, Any]] = []


    monkeypatch.setattr(
        "sys.stdin",
        io.StringIO(
            json.dumps(
                {
                    "schema_version": 1,
                    "action": "send_instruction",
                    "request_id": "literal-false",
                    "dry_run": False,
                    "target": {"worker_id": "w-1"},
                    "instruction": {"text": "hello"},
                }
            )
        ),
    )

    code = main(
        [
            "--host-id",
            "cmd-host",
            "--herdr-bin",
            "herdr",
            "command",
            "--json",
            "--db-path",
            str(db_path),
        ]
    )
    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert code == 1
    _assert_socket_backend_required_payload(payload, request_id="literal-false")
    assert payload["dry_run"] is False
    assert calls == []

    assert get_command_request(db_path, "cmd-host", "literal-false") is None


def test_cli_command_default_backend_does_not_rehydrate_private_stored_binding(
    capsys,
    monkeypatch,
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "stored-binding.db"
    upsert_worker_bindings(
        db_path,
        [
            WorkerBinding(
                host_id="cmd-host",
                worker_id="w-1",
                worker_fingerprint="old-fp",
                backend="herdr",
                target_kind="agent_id",
                target_value="agent-stored",
                sendable=True,
                reason=None,
                observed_at="2026-01-01T00:00:00+00:00",
                expires_at="9999-12-31T23:59:59+00:00",
                private_fingerprint="stored-private",
            )
        ],
    )
    calls: list[tuple[Any, Any]] = []

    def targetless_observation(config: Any) -> HerdrCommandObservation:
        return HerdrCommandObservation(
            spaces=[],
            workers=[Worker(id="w-1", name="Alpha", status="active", space_id="s-1")],
            status="healthy",
            outcome="healthy_non_empty",
        )



    monkeypatch.setattr(
        "sys.stdin",
        io.StringIO(
            json.dumps(
                {
                    "schema_version": 1,
                    "action": "send_instruction",
                    "request_id": "stored-binding",
                    "dry_run": False,
                    "target": {"worker_id": "w-1"},
                    "instruction": {"text": "hello"},
                }
            )
        ),
    )

    code = main(
        [
            "--host-id",
            "cmd-host",
            "--herdr-bin",
            "herdr",
            "command",
            "--json",
            "--db-path",
            str(db_path),
        ]
    )
    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert code == 1
    _assert_socket_backend_required_payload(payload, request_id="stored-binding")
    assert calls == []
    serialized = json.dumps(payload)
    assert "agent-stored" not in serialized
    assert "stored-private" not in serialized
    _assert_no_command_public_forbidden_fields(payload)


def test_cli_command_does_not_send_through_expired_stored_binding(
    capsys,
    monkeypatch,
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "expired-binding.db"
    upsert_worker_bindings(
        db_path,
        [
            WorkerBinding(
                host_id="cmd-host",
                worker_id="w-1",
                worker_fingerprint="old-fp",
                backend="herdr",
                target_kind="agent_id",
                target_value="agent-expired",
                sendable=True,
                reason=None,
                observed_at="2026-01-01T00:00:00+00:00",
                expires_at="2026-01-02T00:00:00+00:00",
                private_fingerprint="expired-private",
            )
        ],
    )
    calls: list[tuple[Any, Any]] = []

    def targetless_observation(config: Any) -> HerdrCommandObservation:
        return HerdrCommandObservation(
            spaces=[],
            workers=[Worker(id="w-1", name="Alpha", status="active", space_id="s-1")],
            status="healthy",
            outcome="healthy_non_empty",
        )



    monkeypatch.setattr(
        "sys.stdin",
        io.StringIO(
            json.dumps(
                {
                    "schema_version": 1,
                    "action": "send_instruction",
                    "request_id": "expired-binding",
                    "dry_run": False,
                    "target": {"worker_id": "w-1"},
                    "instruction": {"text": "hello"},
                }
            )
        ),
    )

    code = main(
        [
            "--host-id",
            "cmd-host",
            "--herdr-bin",
            "herdr",
            "command",
            "--json",
            "--db-path",
            str(db_path),
        ]
    )
    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert code == 1
    _assert_socket_backend_required_payload(payload, request_id="expired-binding")
    assert calls == []
    assert "agent-expired" not in json.dumps(payload)
    _assert_no_command_public_forbidden_fields(payload)


def test_cli_command_duplicate_current_binding_is_ambiguous_and_not_expired(
    capsys,
    monkeypatch,
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "current-duplicates.db"
    calls: list[tuple[Any, Any]] = []

    def duplicate_observation(config: Any) -> HerdrCommandObservation:
        worker_a = Worker(
            id="dup-a",
            name="Duplicate A",
            status="active",
            backend_target={"kind": "agent_id", "value": "same-agent", "sendable": True, "reason": None},
        )
        worker_b = Worker(
            id="dup-b",
            name="Duplicate B",
            status="active",
            backend_target={"kind": "agent_id", "value": "same-agent", "sendable": True, "reason": None},
        )
        return HerdrCommandObservation(
            spaces=[],
            workers=[worker_a, worker_b],
            status="healthy",
            outcome="healthy_non_empty",
            bindings=[
                WorkerBinding(
                    host_id="cmd-host",
                    worker_id=worker_a.id,
                    worker_fingerprint=worker_a.fingerprint,
                    backend="herdr",
                    target_kind="agent_id",
                    target_value="same-agent",
                    sendable=False,
                    reason="duplicate_backend_target",
                    observed_at="2026-01-01T00:00:00+00:00",
                    private_fingerprint="colliding-private",
                ),
                WorkerBinding(
                    host_id="cmd-host",
                    worker_id=worker_b.id,
                    worker_fingerprint=worker_b.fingerprint,
                    backend="herdr",
                    target_kind="agent_id",
                    target_value="same-agent",
                    sendable=False,
                    reason="duplicate_backend_target",
                    observed_at="2026-01-01T00:00:00+00:00",
                    private_fingerprint="colliding-private",
                ),
            ],
        )



    monkeypatch.setattr(
        "sys.stdin",
        io.StringIO(
            json.dumps(
                {
                    "schema_version": 1,
                    "action": "send_instruction",
                    "request_id": "current-duplicate",
                    "dry_run": False,
                    "target": {"worker_id": "dup-a"},
                    "instruction": {"text": "hello"},
                }
            )
        ),
    )

    code = main(
        [
            "--host-id",
            "cmd-host",
            "--herdr-bin",
            "herdr",
            "command",
            "--json",
            "--db-path",
            str(db_path),
        ]
    )
    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    current = list_worker_bindings(db_path, "cmd-host", backend="herdr")

    assert code == 1
    _assert_socket_backend_required_payload(payload, request_id="current-duplicate")
    assert calls == []
    assert current == []
    assert "colliding-private" not in json.dumps(payload)
    _assert_no_command_public_forbidden_fields(payload)


def test_cli_command_stored_duplicate_binding_is_ambiguous_and_skips_backend(
    capsys,
    monkeypatch,
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "stored-duplicate.db"
    upsert_worker_bindings(
        db_path,
        [
            WorkerBinding(
                host_id="cmd-host",
                worker_id="dup-stored",
                worker_fingerprint="old-fp",
                backend="herdr",
                target_kind="agent_id",
                target_value="same-agent",
                sendable=False,
                reason="duplicate_backend_target",
                observed_at="2026-01-01T00:00:00+00:00",
                expires_at="9999-12-31T23:59:59+00:00",
                private_fingerprint="private-duplicate-fingerprint",
            )
        ],
    )
    calls: list[tuple[Any, Any]] = []

    def targetless_observation(config: Any) -> HerdrCommandObservation:
        return HerdrCommandObservation(
            spaces=[],
            workers=[Worker(id="dup-stored", name="Duplicate", status="active", space_id="s-1")],
            status="healthy",
            outcome="healthy_non_empty",
        )



    monkeypatch.setattr(
        "sys.stdin",
        io.StringIO(
            json.dumps(
                {
                    "schema_version": 1,
                    "action": "send_instruction",
                    "request_id": "stored-duplicate",
                    "dry_run": False,
                    "target": {"worker_id": "dup-stored"},
                    "instruction": {"text": "hello"},
                }
            )
        ),
    )

    code = main(
        [
            "--host-id",
            "cmd-host",
            "--herdr-bin",
            "herdr",
            "command",
            "--json",
            "--db-path",
            str(db_path),
        ]
    )
    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert code == 1
    _assert_socket_backend_required_payload(payload, request_id="stored-duplicate")
    assert calls == []
    serialized = json.dumps(payload)
    assert "same-agent" not in serialized
    assert "private-duplicate-fingerprint" not in serialized
    _assert_no_command_public_forbidden_fields(payload)


@pytest.mark.parametrize("value", ["false", "true", 0, 1, None, [], {}])
def test_cli_command_rejects_non_boolean_dry_run_before_backend(
    value: Any, capsys, monkeypatch
) -> None:
    calls: list[str] = []

    def guarded_observation(config: Any) -> HerdrCommandObservation:
        calls.append("fetch")
        raise AssertionError("invalid dry_run must not fetch")


    monkeypatch.setattr(
        "sys.stdin",
        io.StringIO(
            json.dumps(
                {
                    "schema_version": 1,
                    "action": "send_instruction",
                    "request_id": "bad-dry-run",
                    "dry_run": value,
                    "target": {"worker_id": "w-1"},
                    "instruction": {"text": "hello"},
                }
            )
        ),
    )

    code = main(
        [
            "--host-id",
            "cmd-host",
            "--herdr-bin",
            "herdr",
            "command",
            "--json",
        ]
    )
    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert code == 1
    assert payload["status"] == STATUS_INVALID_REQUEST
    assert calls == []
    _assert_no_command_public_forbidden_fields(payload)


@pytest.mark.parametrize("value", ["1", 1.0, True, False, None, [], {}, 2])
def test_cli_command_rejects_malformed_schema_version_before_pipeline(
    value: Any, capsys, monkeypatch
) -> None:
    calls: list[str] = []

    def guarded(*args: Any, **kwargs: Any) -> Any:
        calls.append("called")
        raise AssertionError("invalid schema_version must stop before pipeline work")

    monkeypatch.setattr("tendwire.command_submission.reserve_command_request", guarded)
    monkeypatch.setattr(
        "sys.stdin",
        io.StringIO(
            json.dumps(
                {
                    "schema_version": value,
                    "action": "send_instruction",
                    "request_id": "bad-schema",
                    "dry_run": False,
                    "target": {"worker_id": "w-1"},
                    "instruction": {"text": "hello"},
                }
            )
        ),
    )

    code = main(
        [
            "--host-id",
            "cmd-host",
            "--herdr-bin",
            "herdr",
            "command",
            "--json",
        ]
    )
    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert code == 1
    assert payload["status"] == STATUS_INVALID_REQUEST
    assert calls == []
    _assert_no_command_public_forbidden_fields(payload)


def test_cli_command_send_instruction_empty_target_rejects_before_fetch(capsys, monkeypatch) -> None:
    calls: list[str] = []

    def guarded_fetch(config: Any) -> tuple[list[Space], list[Worker]]:
        calls.append("fetch")
        raise AssertionError("empty target must reject before Herdr fetch")

    monkeypatch.setattr("tendwire.cli.fetch_herdr_state", guarded_fetch)
    monkeypatch.setattr(
        "sys.stdin",
        io.StringIO(
            json.dumps(
                {
                    "schema_version": 1,
                    "action": "send_instruction",
                    "request_id": "empty-target",
                    "dry_run": False,
                    "target": {},
                    "instruction": {"text": "hello"},
                }
            )
        ),
    )
    code = main(
        [
            "--host-id",
            "cmd-host",
            "--herdr-bin",
            "definitely-not-a-real-herdr-binary",
            "command",
            "--json",
        ]
    )
    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert code == 1
    assert payload["status"] == STATUS_INVALID_REQUEST
    assert calls == []


def test_cli_command_duplicate_request_id_same_payload_returns_cached(capsys, monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr("tendwire.cli.fetch_herdr_state", _fake_herdr_state)

    calls: list[tuple[Any, Any]] = []

    db_path = tmp_path / "cmd.db"
    payload = json.dumps(
        {
            "schema_version": 1,
            "action": "send_instruction",
            "request_id": "dup-1",
            "dry_run": False,
            "target": {"worker_id": "w-1"},
            "instruction": {"text": "hello"},
        }
    )

    monkeypatch.setattr("sys.stdin", io.StringIO(payload))
    code1 = main(
        [
            "--host-id",
            "cmd-host",
            "--herdr-bin",
            "definitely-not-a-real-herdr-binary",
            "command",
            "--json",
            "--db-path",
            str(db_path),
        ]
    )
    captured1 = capsys.readouterr()
    assert code1 == 1
    result1 = json.loads(captured1.out)
    _assert_socket_backend_required_payload(result1, request_id="dup-1")

    assert get_command_request(db_path, "cmd-host", "dup-1") is None

    monkeypatch.setattr("sys.stdin", io.StringIO(payload))
    code2 = main(
        [
            "--host-id",
            "cmd-host",
            "--herdr-bin",
            "definitely-not-a-real-herdr-binary",
            "command",
            "--json",
            "--db-path",
            str(db_path),
        ]
    )
    captured2 = capsys.readouterr()
    assert code2 == 1
    result2 = json.loads(captured2.out)
    assert result2["status"] == STATUS_BACKEND_UNAVAILABLE
    assert result2 == result1
    assert calls == []


def test_cli_command_duplicate_request_id_different_payload_rejects(capsys, monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr("tendwire.cli.fetch_herdr_state", _fake_herdr_state)

    calls: list[tuple[Any, Any]] = []

    db_path = tmp_path / "cmd.db"
    payload1 = json.dumps(
        {
            "schema_version": 1,
            "action": "send_instruction",
            "request_id": "dup-2",
            "dry_run": False,
            "target": {"worker_id": "w-1"},
            "instruction": {"text": "hello"},
        }
    )
    payload2 = json.dumps(
        {
            "schema_version": 1,
            "action": "send_instruction",
            "request_id": "dup-2",
            "dry_run": False,
            "target": {"worker_id": "w-1"},
            "instruction": {"text": "world"},
        }
    )

    monkeypatch.setattr("sys.stdin", io.StringIO(payload1))
    code1 = main(
        [
            "--host-id",
            "cmd-host",
            "--herdr-bin",
            "definitely-not-a-real-herdr-binary",
            "command",
            "--json",
            "--db-path",
            str(db_path),
        ]
    )
    capsys.readouterr()
    assert code1 == 1

    monkeypatch.setattr("sys.stdin", io.StringIO(payload2))
    code = main(
        [
            "--host-id",
            "cmd-host",
            "--herdr-bin",
            "definitely-not-a-real-herdr-binary",
            "command",
            "--json",
            "--db-path",
            str(db_path),
        ]
    )
    captured = capsys.readouterr()
    assert code == 1
    result = json.loads(captured.out)
    assert result["status"] == STATUS_BACKEND_UNAVAILABLE
    assert calls == []


def test_cli_command_pending_receipt_rejects_without_retry(capsys, monkeypatch, tmp_path: Path) -> None:
    db_path = tmp_path / "cmd.db"
    pending_request = CommandRequest(
        action="send_instruction",
        request_id="uncertain-1",
        dry_run=False,
        target={"worker_id": "w-1"},
        instruction={"text": "hello"},
    )
    # Seed terminal uncertainty through the v12 CAS lifecycle.
    init_store(db_path)
    _seed_uncertain_request(db_path, pending_request)

    monkeypatch.setattr(
        "sys.stdin",
        io.StringIO(
            json.dumps(
                {
                    "schema_version": 1,
                    "action": "send_instruction",
                    "request_id": "uncertain-1",
                    "dry_run": False,
                    "target": {"worker_id": "w-1"},
                    "instruction": {"text": "hello"},
                }
            )
        ),
    )
    code = main(
        [
            "--host-id",
            "cmd-host",
            "--herdr-bin",
            "definitely-not-a-real-herdr-binary",
            "command",
            "--json",
            "--db-path",
            str(db_path),
        ]
    )
    captured = capsys.readouterr()
    assert code == 1
    result = json.loads(captured.out)
    assert result["status"] == STATUS_REQUEST_STATE_UNCERTAIN
    assert result["disposition"] == DISPOSITION_TERMINAL_UNCERTAIN


def test_cli_command_uncertain_receipt_changed_payload_is_duplicate_without_retry(
    capsys, monkeypatch, tmp_path: Path
) -> None:
    db_path = tmp_path / "cmd.db"

    original_request = CommandRequest(
        action="send_instruction",
        request_id="uncertain-changed",
        dry_run=False,
        target={"worker_id": "w-1"},
        instruction={"text": "hello"},
    )
    init_store(db_path)
    _seed_uncertain_request(db_path, original_request)
    calls: list[str] = []

    def guarded_backend(*args: Any, **kwargs: Any) -> Any:
        calls.append("backend")
        raise AssertionError("changed duplicate receipt must not reach the backend")

    monkeypatch.setattr(
        "sys.stdin",
        io.StringIO(
            json.dumps(
                {
                    "schema_version": 1,
                    "action": "send_instruction",
                    "request_id": "uncertain-changed",
                    "dry_run": False,
                    "target": {"worker_id": "w-1"},
                    "instruction": {"text": "world"},
                }
            )
        ),
    )

    code = main(
        [
            "--host-id",
            "cmd-host",
            "--herdr-bin",
            "definitely-not-a-real-herdr-binary",
            "command",
            "--json",
            "--db-path",
            str(db_path),
        ]
    )
    captured = capsys.readouterr()
    result = json.loads(captured.out)

    assert code == 1
    assert result["status"] == STATUS_DUPLICATE_REQUEST
    assert calls == []


def test_cli_command_forbidden_field_rejected(capsys, monkeypatch) -> None:
    monkeypatch.setattr(
        "sys.stdin",
        io.StringIO(
            json.dumps(
                {
                    "schema_version": 1,
                    "action": "noop",
                    "params": {"pane_id": "leaked"},
                }
            )
        ),
    )
    code = main(
        [
            "--host-id",
            "cmd-host",
            "--herdr-bin",
            "definitely-not-a-real-herdr-binary",
            "command",
            "--json",
        ]
    )
    captured = capsys.readouterr()
    assert code == 1
    payload = json.loads(captured.out)
    assert payload["status"] == STATUS_INVALID_REQUEST


def test_cli_command_rejects_control_sequence_instruction_as_json_only(capsys, monkeypatch) -> None:
    monkeypatch.setattr(
        "sys.stdin",
        io.StringIO(
            json.dumps(
                {
                    "schema_version": 1,
                    "action": "send_instruction",
                    "target": {"worker_id": "w-1"},
                    "instruction": {"text": "hello\x1b[31mworld"},
                }
            )
        ),
    )

    code = main(
        [
            "--host-id",
            "cmd-host",
            "--herdr-bin",
            "definitely-not-a-real-herdr-binary",
            "command",
            "--json",
        ]
    )
    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert code == 1
    assert captured.err == ""
    assert payload["ok"] is False
    assert payload["status"] == STATUS_INVALID_REQUEST
    assert payload["error"]["details"] == {"field": "instruction.text"}
    _assert_no_command_public_forbidden_fields(payload)


def test_cli_command_legacy_backend_guard_and_receipt_are_sanitized(
    capsys, monkeypatch, tmp_path: Path
) -> None:
    db_path = tmp_path / "cmd.db"
    monkeypatch.setattr("tendwire.cli.fetch_herdr_state", _fake_herdr_state)

    calls: list[tuple[Any, Any]] = []

    def leaky_backend(config: Any, target: Any, instruction: Any) -> CommandEnvelope:
        calls.append((target, instruction))
        return CommandEnvelope(
            ok=False,
            status=STATUS_BACKEND_FAILED,
            action="send_instruction",
            result={
                "target": target,
                "pane_id": "p-1",
                "nested": {"argv": ["herdr"], "safe": "kept"},
            },
            error={
                "code": STATUS_BACKEND_FAILED,
                "message": "failed",
                "details": {"terminal_id": "t-1", "safe": "kept"},
            },
        )


    monkeypatch.setattr(
        "sys.stdin",
        io.StringIO(
            json.dumps(
                {
                    "schema_version": 1,
                    "action": "send_instruction",
                    "request_id": "sanitize-1",
                    "dry_run": False,
                    "target": {"worker_id": "w-1"},
                    "instruction": {"text": "hello"},
                }
            )
        ),
    )
    code = main(
        [
            "--host-id",
            "cmd-host",
            "--herdr-bin",
            "definitely-not-a-real-herdr-binary",
            "command",
            "--json",
            "--db-path",
            str(db_path),
        ]
    )
    captured = capsys.readouterr()
    assert code == 1
    payload = json.loads(captured.out)
    _assert_socket_backend_required_payload(payload, request_id="sanitize-1")
    assert calls == []
    _assert_no_command_public_forbidden_fields(payload)

    assert get_command_request(db_path, "cmd-host", "sanitize-1") is None
    assert captured.err == ""


_COMMAND_PUBLIC_FORBIDDEN_KEYS = {
    "pane_id",
    "terminal_id",
    "pid",
    "tty",
    "pty",
    "tmux",
    "screen_session",
    "window_id",
    "tab_id",
    "argv",
    "shell",
    "command",
    "route",
    "routes",
    "delivery",
    "deliveries",
    "token",
    "tokens",
    "connector",
    "connectors",
    "backend_target",
    "agent_session",
    "session_id",
    "herdr_state",
    "herdres_state",
    "target_kind",
    "target_value",
    "turn_target_kind",
    "turn_target_value",
    "private_fingerprint",
}


def _assert_no_command_public_forbidden_fields(value: Any, path: str = "$") -> None:
    if isinstance(value, dict):
        for key, item in value.items():
            assert key not in _COMMAND_PUBLIC_FORBIDDEN_KEYS, f"forbidden field {path}.{key}"
            _assert_no_command_public_forbidden_fields(item, f"{path}.{key}")
    elif isinstance(value, list):
        for index, item in enumerate(value):
            _assert_no_command_public_forbidden_fields(item, f"{path}[{index}]")


def _assert_socket_backend_required_payload(value: dict[str, Any], *, request_id: str) -> None:
    assert value["ok"] is False
    assert value["status"] == STATUS_BACKEND_UNAVAILABLE
    assert value["request_id"] == request_id
    assert value["error"]["code"] == STATUS_BACKEND_UNAVAILABLE
    assert value["error"]["message"] == "Herdr socket backend is not enabled"
    _assert_no_command_public_forbidden_fields(value)


def _fake_herdr_state_with_terminal(config: Any) -> tuple[list[Space], list[Worker]]:
    return [], [
        Worker(
            id="w-terminal",
            name="Terminal",
            status="active",
            space_id="s-1",
            meta={
                "pane_id": "p-1",
                "terminal_id": "t-1",
                "pid": 123,
                "tty": "/dev/pts/0",
                "pty": "pts",
                "tmux": "sess",
                "screen_session": "scr",
                "window_id": "win-1",
                "tab_id": "tab-1",
                "argv": ["bash"],
                "shell": "bash",
                "command": "python app.py",
                "route": "telegram",
                "routes": ["r1"],
                "delivery": {"id": 1},
                "deliveries": [{"id": 2}],
                "token": "secret",
                "tokens": ["t1"],
                "connector": {"x": 1},
                "connectors": [{"y": 2}],
                "backend_target": {"kind": "agent_id", "value": "agent-1", "sendable": True, "reason": None},
                "agent_session": {"value": "sess-1"},
                "session_id": "session-1",
                "safe": "kept",
            },
            backend_target={"kind": "agent_id", "value": "agent-1", "sendable": True, "reason": None},
        )
    ]


def test_cli_command_read_snapshot_strips_command_public_terminal_fields(
    capsys, monkeypatch
) -> None:
    """Command-public read_snapshot strips terminal/connector identifiers while
    leaving the ordinary snapshot --json output unchanged.
    """
    monkeypatch.setattr("tendwire.cli.fetch_herdr_state", _fake_herdr_state_with_terminal)
    monkeypatch.setattr(
        "sys.stdin",
        io.StringIO(json.dumps({"schema_version": 1, "action": "read_snapshot"})),
    )
    code = main(
        [
            "--host-id",
            "cmd-host",
            "--herdr-bin",
            "definitely-not-a-real-herdr-binary",
            "command",
            "--json",
        ]
    )
    captured = capsys.readouterr()
    assert code == 0
    payload = json.loads(captured.out)
    assert payload["ok"] is True
    assert payload["status"] == "snapshot"
    assert payload["action"] == "read_snapshot"
    assert "request_id" in payload
    meta = payload["result"]["snapshot"]["workers"][0]["meta"]
    for key in _COMMAND_PUBLIC_FORBIDDEN_KEYS:
        assert key not in meta, key
    assert meta["safe"] == "kept"

    # The standalone snapshot path is public too, so backend identifiers are absent there as well.
    code2 = main(
        [
            "--host-id",
            "cmd-host",
            "--herdr-bin",
            "definitely-not-a-real-herdr-binary",
            "snapshot",
            "--json",
        ]
    )
    captured2 = capsys.readouterr()
    assert code2 == 0
    snapshot = json.loads(captured2.out)
    assert snapshot["schema_version"] == 2
    snap_meta = snapshot["workers"][0]["meta"]
    for key in ("pane_id", "terminal_id", "backend_target", "agent_session", "session_id"):
        assert key not in snap_meta
    assert snap_meta["safe"] == "kept"


def test_cli_command_does_not_auto_discover_default_daemon_socket(
    capsys,
    monkeypatch,
    tmp_path: Path,
) -> None:
    """A default socket file must not opt ordinary CLI commands into daemon mode."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    (data_dir / "tendwire.sock").touch()
    calls: list[str] = []

    class GuardedDaemonAPIClient:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            calls.append("daemon")
            raise AssertionError("implicit default socket should not be contacted")

    monkeypatch.delenv("TENDWIRE_SOCKET_PATH", raising=False)
    monkeypatch.delenv("TENDWIRE_HERDR_BACKEND", raising=False)
    monkeypatch.setenv("TENDWIRE_DATA_DIR", str(data_dir))
    monkeypatch.setattr("tendwire.daemon_api.DaemonAPIClient", GuardedDaemonAPIClient)
    monkeypatch.setattr("tendwire.cli.fetch_herdr_state", _fake_herdr_state)
    monkeypatch.setattr(
        "sys.stdin",
        io.StringIO(json.dumps({"schema_version": 1, "action": "read_snapshot"})),
    )

    code = main(["--host-id", "cmd-host", "command", "--json"])
    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert code == 0
    assert calls == []
    assert payload["ok"] is True
    assert payload["status"] == "snapshot"
    assert payload["result"]["snapshot"]["workers"][0]["id"] == "w-1"


def test_cli_command_forbidden_field_rejects_before_backend_and_store(
    capsys, monkeypatch
) -> None:
    """A contract-invalid request must be rejected before any backend or store call."""
    calls: list[str] = []

    def guarded_fetch(config: Any) -> tuple[list[Space], list[Worker]]:
        calls.append("fetch")
        raise AssertionError("fetch_herdr_state called before validation")

    def guarded_reserve_request(*args: Any, **kwargs: Any) -> Any:
        calls.append("reserve_request")
        raise AssertionError("reserve_command_request called before validation")

    monkeypatch.setattr("tendwire.cli.fetch_herdr_state", guarded_fetch)
    monkeypatch.setattr(
        "tendwire.command_submission.reserve_command_request",
        guarded_reserve_request,
    )
    monkeypatch.setattr(
        "sys.stdin",
        io.StringIO(
            json.dumps(
                {
                    "schema_version": 1,
                    "action": "send_instruction",
                    "request_id": "rej-1",
                    "dry_run": False,
                    "target": {"worker_id": "w-1"},
                    "instruction": {"text": "hello"},
                    "params": {"pane_id": "leaked"},
                }
            )
        ),
    )
    code = main(
        [
            "--host-id",
            "cmd-host",
            "--herdr-bin",
            "definitely-not-a-real-herdr-binary",
            "command",
            "--json",
        ]
    )
    captured = capsys.readouterr()
    assert code == 1
    payload = json.loads(captured.out)
    assert payload["status"] == STATUS_INVALID_REQUEST
    assert payload["request_id"] == "rej-1"
    assert calls == []


def test_cli_command_raw_top_level_forbidden_rejects_before_pipeline(
    capsys, monkeypatch
) -> None:
    """Raw top-level forbidden fields reject before store, projection, or backend work."""
    calls: list[str] = []

    def guarded_reserve_request(*args: Any, **kwargs: Any) -> Any:
        calls.append("reserve_request")
        raise AssertionError("reserve_command_request called before raw validation")

    def guarded_fetch(config: Any) -> tuple[list[Space], list[Worker]]:
        calls.append("fetch")
        raise AssertionError("fetch_herdr_state called before raw validation")

    def guarded_project(*args: Any, **kwargs: Any) -> Any:
        calls.append("project")
        raise AssertionError("project_from_observations called before raw validation")

    def guarded_execute(*args: Any, **kwargs: Any) -> Any:
        calls.append("execute")
        raise AssertionError("execute_command called before raw validation")

    def guarded_send(*args: Any, **kwargs: Any) -> Any:
        calls.append("backend")
        raise AssertionError("backend sender called before raw validation")

    monkeypatch.setattr(
        "tendwire.command_submission.reserve_command_request",
        guarded_reserve_request,
    )
    monkeypatch.setattr("tendwire.cli.fetch_herdr_state", guarded_fetch)
    monkeypatch.setattr("tendwire.cli.project_from_observations", guarded_project)
    monkeypatch.setattr("tendwire.cli.execute_command", guarded_execute)
    monkeypatch.setattr(
        "sys.stdin",
        io.StringIO(
            json.dumps(
                {
                    "schema_version": 1,
                    "action": "send_instruction",
                    "request_id": "raw-rej",
                    "dry_run": False,
                    "target": {"worker_id": "w-1"},
                    "instruction": {"text": "hello"},
                    "pane_id": "leaked",
                }
            )
        ),
    )
    code = main(
        [
            "--host-id",
            "cmd-host",
            "--herdr-bin",
            "definitely-not-a-real-herdr-binary",
            "command",
            "--json",
        ]
    )
    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert code == 1
    assert payload["status"] == STATUS_INVALID_REQUEST
    assert payload["request_id"] is None
    assert "pane_id" in str(payload["error"]["details"])
    assert calls == []


def test_cli_command_backend_unavailable_preserves_request_id(
    capsys, monkeypatch, tmp_path: Path
) -> None:
    """A pre-start backend failure preserves request_id without store authority."""
    db_path = tmp_path / "req.db"
    upsert_worker_bindings(
        db_path,
        [
            WorkerBinding(
                host_id="cmd-host",
                worker_id="still-live",
                worker_fingerprint="old-fp",
                backend="herdr",
                target_kind="agent_id",
                target_value="agent-still-live",
                sendable=True,
                reason=None,
                observed_at="2026-01-01T00:00:00+00:00",
                expires_at="9999-12-31T23:59:59+00:00",
                private_fingerprint="still-live-private",
            )
        ],
    )
    monkeypatch.setattr("tendwire.cli.fetch_herdr_state", _fake_herdr_state)
    monkeypatch.setattr(
        "sys.stdin",
        io.StringIO(
            json.dumps(
                {
                    "schema_version": 1,
                    "action": "send_instruction",
                    "request_id": "req-visible",
                    "dry_run": False,
                    "target": {"worker_id": "w-1"},
                    "instruction": {"text": "hello"},
                }
            )
        ),
    )
    code = main(
        [
            "--host-id",
            "cmd-host",
            "--herdr-bin",
            "definitely-not-a-real-herdr-binary",
            "command",
            "--json",
            "--db-path",
            str(db_path),
        ]
    )
    captured = capsys.readouterr()
    assert code == 1
    payload = json.loads(captured.out)
    assert payload["status"] == STATUS_BACKEND_UNAVAILABLE
    assert payload["disposition"] == DISPOSITION_NO_RECEIPT
    assert payload["request_id"] == "req-visible"

    assert get_command_request(db_path, "cmd-host", "req-visible") is None
    with sqlite3.connect(str(db_path)) as conn:
        assert conn.execute(
            """
            SELECT COUNT(*)
            FROM commands
            WHERE host_id = ?
              AND request_id = ?
            """,
            ("cmd-host", "req-visible"),
        ).fetchone()[0] == 0
    current = list_worker_bindings(db_path, "cmd-host", backend="herdr")
    assert [binding.private_fingerprint for binding in current] == ["still-live-private"]


def test_cli_command_default_backend_blocks_degraded_observation_send(
    capsys, monkeypatch, tmp_path: Path
) -> None:
    db_path = tmp_path / "degraded.db"
    upsert_worker_bindings(
        db_path,
        [
            WorkerBinding(
                host_id="cmd-host",
                worker_id="still-live",
                worker_fingerprint="old-fp",
                backend="herdr",
                target_kind="agent_id",
                target_value="agent-still-live",
                sendable=True,
                reason=None,
                observed_at="2026-01-01T00:00:00+00:00",
                expires_at="9999-12-31T23:59:59+00:00",
                private_fingerprint="still-live-private",
            )
        ],
    )

    def degraded_observation(config: Any) -> HerdrCommandObservation:
        return HerdrCommandObservation(
            spaces=[],
            workers=[],
            status="degraded",
            outcome="malformed_json",
            message="Herdr agent observation is not healthy",
        )



    monkeypatch.setattr(
        "sys.stdin",
        io.StringIO(
            json.dumps(
                {
                    "schema_version": 1,
                    "action": "send_instruction",
                    "request_id": "degraded-1",
                    "dry_run": False,
                    "target": {"worker_id": "missing"},
                    "instruction": {"text": "hello"},
                }
            )
        ),
    )

    code = main(
        [
            "--host-id",
            "cmd-host",
            "--herdr-bin",
            "herdr",
            "command",
            "--json",
            "--db-path",
            str(db_path),
        ]
    )
    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert code == 1
    _assert_socket_backend_required_payload(payload, request_id="degraded-1")
    assert get_command_request(db_path, "cmd-host", "degraded-1") is None
    current = list_worker_bindings(db_path, "cmd-host", backend="herdr")
    assert [binding.private_fingerprint for binding in current] == ["still-live-private"]


def test_cli_command_default_backend_blocks_healthy_empty_observation_send(
    capsys, monkeypatch, tmp_path: Path
) -> None:
    db_path = tmp_path / "empty.db"

    def empty_observation(config: Any) -> HerdrCommandObservation:
        return HerdrCommandObservation(
            spaces=[],
            workers=[],
            status="healthy",
            outcome="empty_healthy",
        )



    monkeypatch.setattr(
        "sys.stdin",
        io.StringIO(
            json.dumps(
                {
                    "schema_version": 1,
                    "action": "send_instruction",
                    "request_id": "empty-1",
                    "dry_run": False,
                    "target": {"worker_id": "missing"},
                    "instruction": {"text": "hello"},
                }
            )
        ),
    )

    code = main(
        [
            "--host-id",
            "cmd-host",
            "--herdr-bin",
            "herdr",
            "command",
            "--json",
            "--db-path",
            str(db_path),
        ]
    )
    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert code == 1
    _assert_socket_backend_required_payload(payload, request_id="empty-1")
    assert get_command_request(db_path, "cmd-host", "empty-1") is None
