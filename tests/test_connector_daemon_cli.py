"""Daemon/CLI connector JSON boundary without direct-store fallback."""

from __future__ import annotations

import copy
import io
import json
import random
import threading
import time
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from tendwire.cli import main
from tendwire.core.turns import TURN_CONTENT_PAGE_MAX_UTF8_BYTES, content_cursor
from tendwire.connectors.protocol import valid_generic_payload, valid_turn_final_delivery
from tendwire.daemon_api import DaemonAPIClient, TendwireDaemonAPI, UnixSocketJSONServer


def _result(
    name: str = "turn-final", method: str = "connector.poll"
) -> dict[str, Any]:
    key = "turn-final:working:twwork1." + "c" * 43
    common: dict[str, Any] = {
        "schema_version": 1,
        "ok": True,
        "host_id": "host-a",
        "name": name,
    }
    results: dict[str, dict[str, Any]] = {
        "connector.prepare": {
            "status": "ok", "plan_token": "twplan1." + "a" * 43,
            "state": "active", "generation": 1, "part_count": 1,
            "accepted_ordinals": [],
        },
        "connector.poll": {"status": "ok", "items": []},
        "connector.ack": {
            "status": "acknowledged", "ref": "twref1." + "b" * 43,
            "key": key, "attempt": 1,
        },
        "connector.fail": {
            "status": "retry_scheduled", "ref": "twref1." + "b" * 43,
            "key": key, "attempt": 1, "available_at": "2026-08-05T00:01:00.000000Z",
        },
        "connector.defer": {
            "status": "deferred", "ref": "twref1." + "b" * 43,
            "key": key, "attempt": 1, "available_at": "2026-08-05T00:01:00.000000Z",
        },
        "connector.renew": {
            "status": "renewed", "ref": "twref1." + "b" * 43,
            "key": key, "attempt": 1,
            "leased_until": "2026-08-05T00:01:00.000000Z",
        },
        "connector.release": {
            "status": "released", "ref": "twref1." + "b" * 43,
            "key": key, "attempt": 1,
        },
        "connector.reclaim": {"status": "ok", "reclaimed": 0},
        "connector.retry": {
            "status": "requeued", "key": key, "retry_generation": 1,
            "prior_attempt_count": 1,
        },
        "connector.inspect": {"status": "ok", "total": 0, "items": []},
    }
    return common | results[method]


def _api(calls: list[tuple[str, dict[str, Any]]], result: dict[str, Any] | None = None) -> TendwireDaemonAPI:
    def connector_call(method: str, params: dict[str, Any]) -> dict[str, Any]:
        calls.append((method, params))
        return result or _result(str(params.get("name") or "turn-final"), method)

    return TendwireDaemonAPI(
        get_snapshot=lambda: None,  # type: ignore[arg-type]
        get_health=lambda: {},
        submit_command=lambda _params: {},
        get_attention=lambda: {},
        get_turns=lambda **_kwargs: {},
        get_turn_delta=lambda **_kwargs: {},
        get_turn_content=lambda _params: {},
        get_pending=lambda: {},
        connector_call=connector_call,
    )


@pytest.mark.parametrize(
    "method",
    [
        "connector.prepare",
        "connector.poll",
        "connector.ack",
        "connector.fail",
        "connector.defer",
        "connector.renew",
        "connector.release",
        "connector.reclaim",
        "connector.retry",
        "connector.inspect",
    ],
)
def test_daemon_routes_only_the_frozen_connector_methods(method: str) -> None:
    calls: list[tuple[str, dict[str, Any]]] = []
    response = _api(calls).dispatch({"method": method, "params": {"name": "turn-final"}})
    assert response["ok"] is True
    assert calls == [(method, {"name": "turn-final"})]


def test_transport_and_connector_errors_remain_separate_envelopes() -> None:
    connector_error = {
        "schema_version": 1,
        "ok": False,
        "status": "invalid_ref",
        "host_id": "host-a",
        "name": "turn-final",
        "message": "invalid_ref",
    }
    response = _api([], connector_error).dispatch(
        {"method": "connector.ack", "params": {"name": "turn-final", "ref": "bad"}}
    )
    assert response["ok"] is True
    assert response["result"] == connector_error
    malformed = _api([]).dispatch({"method": "connector.poll", "params": []})
    assert malformed["ok"] is False
    assert malformed["result"] is None
    assert malformed["error"]["code"] == "invalid_params"


def test_exact_opaque_tokens_survive_dispatch_byte_for_byte() -> None:
    calls: list[tuple[str, dict[str, Any]]] = []
    token = "twplan1.AbCd_-0123456789"
    response = _api(calls).dispatch(
        {
            "method": "connector.prepare",
            "params": {
                "name": "turn-final",
                "action": "part",
                "plan_token": token,
                "ordinal": 0,
                "spans": [],
            },
        }
    )
    assert response["ok"] is True
    assert calls[0][1]["plan_token"] == token


def test_private_or_nonfinite_connector_result_fails_closed() -> None:
    result = _result()
    result["private_binding"] = {"chat_id": 42}
    response = _api([], result).dispatch(
        {"method": "connector.poll", "params": {"name": "turn-final"}}
    )
    assert response["ok"] is False
    assert response["error"]["code"] == "internal_error"


def _paged_final_ready_result() -> dict[str, Any]:
    revision = "twrev1." + "c" * 43
    identity = "twfinal1." + "d" * 43
    absent = {
        "availability": "absent", "inline": None, "char_length": 0,
        "byte_length": 0, "page_count": 0, "first_cursor": None,
    }
    paged = {
        "availability": "complete", "inline": None,
        "char_length": TURN_CONTENT_PAGE_MAX_UTF8_BYTES + 1,
        "byte_length": TURN_CONTENT_PAGE_MAX_UTF8_BYTES + 1,
        "page_count": 2,
        "first_cursor": content_cursor(revision, "assistant_final_text", 0),
    }
    payload = {
        "schema_version": 3, "kind": "final_ready",
        "created_at": "2026-08-05T01:02:03.000000Z",
        "worker": {
            "worker_id": "worker-a", "stable_key": "wsk1_" + "a" * 64,
            "stable_key_version": 1, "route_generation": "twroute1." + "a" * 43,
        },
        "route": {"partition_key": "twpart1_" + "b" * 64, "partition_sequence": 1},
        "turn": {
            "turn_id": "turn-a", "final_identity": identity,
            "content_revision": revision, "replaces_key": None,
            "content": {
                "schema_version": 1, "content_revision": revision,
                "known_incomplete": False,
                "fields": {"user_text": absent, "assistant_final_text": paged},
            },
        },
    }
    return {
        "schema_version": 1, "ok": True, "status": "ok", "host_id": "host-a",
        "name": "turn-final", "items": [{
            "key": f"turn-final:revision:{identity}", "ref": "twref1." + "e" * 43,
            "attempt": 1, "leased_until": "2026-08-05T01:03:03.000000Z",
            "available_at": "2026-08-05T01:02:03.000000Z",
            "created_at": "2026-08-05T01:02:03.000000Z", "payload": payload,
        }],
    }


def test_paged_final_ready_cursor_survives_exact_connector_framing() -> None:
    expected = _paged_final_ready_result()
    response = _api([], expected).dispatch(
        {"method": "connector.poll", "params": {"name": "turn-final"}}
    )
    assert response["ok"] is True
    framed = json.loads(json.dumps(response, ensure_ascii=False, allow_nan=False))
    assert framed["result"] == expected


@pytest.mark.parametrize(
    "location",
    ["turn_id", "worker_id", "inline", "working_text", "revision_digest", "title", "label"],
)
def test_shared_connector_validator_rejects_non_utf8_protocol_strings(location: str) -> None:
    result = _paged_final_ready_result()
    payload = result["items"][0]["payload"]
    if location == "turn_id":
        payload["turn"]["turn_id"] = "\ud800"
    elif location == "worker_id":
        payload["worker"]["worker_id"] = "\ud800"
    elif location == "inline":
        payload["turn"]["content"]["fields"]["user_text"].update(
            availability="complete", inline="\ud800", char_length=1,
        )
    else:
        payload[location] = "\ud800"
    assert not valid_turn_final_delivery(payload, result["items"][0]["key"], "host-a")
    assert not valid_generic_payload({location: "\ud800"})


def test_shared_connector_validators_are_total_for_random_json_values() -> None:
    rng = random.Random(0)
    atoms: list[Any] = [None, True, False, 0, 1.5, float("nan"), "ok", "\ud800"]
    values: list[Any] = atoms.copy()
    for _ in range(500):
        children = [rng.choice(values) for _ in range(rng.randrange(4))]
        values.append(children if rng.randrange(2) else {f"k{index}": child for index, child in enumerate(children)})
    for value in values:
        assert valid_generic_payload(value) in {True, False}
        assert valid_turn_final_delivery(value, value, value) in {True, False}


def test_connector_boundaries_reject_deep_decoded_json_without_recursing() -> None:
    nested: dict[str, Any] = {}
    for _ in range(1_200):
        nested = {"x": nested}
    assert valid_generic_payload(nested) is False
    assert valid_turn_final_delivery(nested, "key", "host-a") is False

    result = _result("notice")
    result["items"] = [{
        "key": "notice-key", "ref": "twref1." + "a" * 43, "attempt": 1,
        "leased_until": "2026-08-05T00:01:00.000000Z",
        "available_at": "2026-08-05T00:00:00.000000Z", "payload": nested,
    }]
    response = _api([], result).dispatch(
        {"method": "connector.poll", "params": {"name": "notice"}}
    )
    assert response["ok"] is False
    assert response["error"]["code"] == "internal_error"


@pytest.mark.parametrize(
    "mutation",
    [
        lambda result: result["items"][0]["payload"]["turn"]["content"]["fields"]["user_text"].update(char_length=1),
        lambda result: result["items"][0]["payload"]["turn"]["content"]["fields"]["assistant_final_text"].update(page_count=0),
        lambda result: result["items"][0]["payload"]["turn"]["content"]["fields"]["assistant_final_text"].update(page_count=99),
        lambda result: result["items"][0]["payload"]["turn"]["content"]["fields"]["assistant_final_text"].update(first_cursor=content_cursor("twrev1." + "f" * 43, "assistant_final_text", 0)),
        lambda result: result["items"][0].update(key="turn-final:revision:twfinal1." + "f" * 43),
    ],
)
def test_final_ready_descriptor_and_delivery_key_fail_closed(mutation: Any) -> None:
    result = copy.deepcopy(_paged_final_ready_result())
    mutation(result)
    response = _api([], result).dispatch(
        {"method": "connector.poll", "params": {"name": "turn-final"}}
    )
    assert response["ok"] is False
    assert response["error"]["code"] == "internal_error"


@pytest.mark.parametrize(
    "mutation",
    [
        lambda result: result.update(name="notice"),
        lambda result: result.update(schema_version=True),
    ],
)
def test_connector_result_cannot_substitute_identity_or_boolean_schema(mutation: Any) -> None:
    result = _result()
    mutation(result)
    response = _api([], result).dispatch(
        {"method": "connector.poll", "params": {"name": "turn-final"}}
    )
    assert response["ok"] is False
    assert response["error"]["code"] == "internal_error"


def test_connector_name_contract_accepts_uppercase_and_preserves_invalid_name_error() -> None:
    uppercase = _result("Notice")
    response = _api([], uppercase).dispatch(
        {"method": "connector.poll", "params": {"name": "Notice"}}
    )
    assert response["ok"] is True
    assert response["result"] == uppercase

    invalid = {
        "schema_version": 1, "ok": False, "status": "invalid_params",
        "host_id": "host-a", "name": "", "message": "invalid_params",
    }
    error_response = _api([], invalid).dispatch(
        {"method": "connector.poll", "params": {"name": "bad name"}}
    )
    assert error_response["ok"] is True
    assert error_response["result"] == invalid


def test_connector_poll_rejects_shape_only_noncanonical_timestamp() -> None:
    result = _paged_final_ready_result()
    result["items"][0]["leased_until"] = "2026-02-30T01:03:03.000000Z"
    response = _api([], result).dispatch(
        {"method": "connector.poll", "params": {"name": "turn-final"}}
    )
    assert response["ok"] is False
    assert response["error"]["code"] == "internal_error"


@pytest.mark.parametrize(
    ("method", "mutation"),
    [
        ("connector.fail", lambda result: result.pop("available_at")),
        ("connector.fail", lambda result: result.update(status="attempts_exhausted")),
        ("connector.defer", lambda result: result.pop("available_at")),
        ("connector.defer", lambda result: result.update(status="superseded")),
    ],
)
def test_connector_settlement_status_requires_exact_due_date_shape(
    method: str, mutation: Any,
) -> None:
    result = _result(method=method)
    mutation(result)
    response = _api([], result).dispatch(
        {"method": method, "params": {"name": "turn-final"}}
    )
    assert response["ok"] is False
    assert response["error"]["code"] == "internal_error"


@pytest.mark.parametrize(
    "mutation",
    [
        lambda result: result.update(generation=0),
        lambda result: result.update(part_count=0),
        lambda result: result.update(plan_token=None),
        lambda result: result.update(status="recovered"),
        lambda result: result.update(accepted_ordinals=[0, 0]),
        lambda result: result.update(accepted_ordinals=[1]),
    ],
)
def test_connector_prepare_rejects_invalid_counts_and_ordinals(mutation: Any) -> None:
    result = _result(method="connector.prepare")
    mutation(result)
    response = _api([], result).dispatch(
        {"method": "connector.prepare", "params": {"name": "turn-final"}}
    )
    assert response["ok"] is False
    assert response["error"]["code"] == "internal_error"


def test_connector_prepare_recovery_requires_both_non_null_plan_tokens() -> None:
    recovered = {
        "schema_version": 1, "ok": True, "status": "recovered",
        "host_id": "host-a", "name": "turn-final",
        "failed_plan_token": "twplan1." + "a" * 43,
        "plan_token": "twplan1." + "b" * 43, "generation": 2,
        "content_revision": "twrev1." + "c" * 43, "state": "active",
        "acknowledged_prefix_count": 0, "executable_job_count": 1,
        "retained_failed_job_count": 0, "prior_attempt_count": 1,
        "idempotent_replay": False,
    }
    request = {"method": "connector.prepare", "params": {"name": "turn-final"}}
    assert _api([], recovered).dispatch(request)["result"] == recovered
    recovered["failed_plan_token"] = None
    rejected = _api([], recovered).dispatch(request)
    assert rejected["ok"] is False
    assert rejected["error"]["code"] == "internal_error"


def test_real_unix_socket_round_trip_preserves_connector_result(tmp_path: Path) -> None:
    tmp_path.chmod(0o700)
    socket_path = tmp_path / "daemon.sock"
    stop = threading.Event()
    expected = _result()
    server = UnixSocketJSONServer(
        socket_path,
        _api([], expected).dispatch,
        stop_event=stop,
        accept_timeout_seconds=0.02,
        request_workers=1,
        max_in_flight_requests=1,
    )
    thread = threading.Thread(target=server.serve_forever)
    thread.start()
    try:
        deadline = time.monotonic() + 2
        while not socket_path.exists() and time.monotonic() < deadline:
            time.sleep(0.01)
        response = DaemonAPIClient(socket_path).request(
            "connector.poll", {"name": "turn-final"}
        )
        assert response["result"] == expected
    finally:
        stop.set()
        thread.join(timeout=2)
        assert not thread.is_alive()


def test_cli_connector_uses_daemon_and_never_accepts_database_fallback(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    calls: list[tuple[str, dict[str, Any]]] = []

    def request(_self: object, method: str, params: dict[str, Any]) -> dict[str, Any]:
        calls.append((method, params))
        return {"schema_version": 1, "ok": True, "result": _result()}

    monkeypatch.setattr(DaemonAPIClient, "request", request)
    assert main(["connector", "poll", "--name", "turn-final"]) == 0
    assert json.loads(capsys.readouterr().out)["name"] == "turn-final"
    assert calls == [("connector.poll", {"name": "turn-final", "limit": 1})]


def test_cli_connector_argv_maps_to_exact_request_json(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    cases = (
        (
            ["prepare", "--name", "notice"],
            '{"action":"begin","plan_token":"token"}',
            {"name": "notice", "action": "begin", "plan_token": "token"},
        ),
        (
            ["poll", "--name", "turn-final", "--limit", "2", "--lease-seconds", "9"],
            "",
            {"name": "turn-final", "limit": 2, "lease_seconds": 9},
        ),
        (
            ["inspect", "--name", "turn-final", "--status", "dead_letter", "--limit", "3"],
            "",
            {
                "name": "turn-final",
                "schema_version": 1,
                "status": "dead_letter",
                "limit": 3,
            },
        ),
        (
            ["retry", "--name", "turn-final", "--final-identity", "final-a"],
            "",
            {"name": "turn-final", "schema_version": 1, "final_identity": "final-a"},
        ),
        (
            ["ack", "--name", "turn-final", "--ref", "ref-a", "--response-json", '{"ok":true}'],
            "",
            {"name": "turn-final", "ref": "ref-a", "response": {"ok": True}},
        ),
        (
            [
                "fail", "--name", "turn-final", "--ref", "ref-b",
                "--reason", "retry", "--delay-seconds", "4",
            ],
            "",
            {"name": "turn-final", "ref": "ref-b", "reason": "retry", "delay_seconds": 4},
        ),
        (
            [
                "defer", "--name", "turn-final", "--ref", "ref-c",
                "--available-at", "2026-08-06T00:00:00Z",
            ],
            "",
            {
                "name": "turn-final",
                "ref": "ref-c",
                "reason": "",
                "available_at": "2026-08-06T00:00:00Z",
            },
        ),
        (
            ["renew", "--name", "turn-final", "--ref", "ref-d", "--lease-seconds", "8"],
            "",
            {"name": "turn-final", "ref": "ref-d", "lease_seconds": 8},
        ),
        (
            ["release", "--name", "turn-final", "--ref", "ref-e"],
            "",
            {"name": "turn-final", "ref": "ref-e"},
        ),
        (["reclaim", "--name", "turn-final"], "", {"name": "turn-final"}),
    )
    calls: list[tuple[str, dict[str, Any]]] = []

    def attempt(_config, method: str, params: dict[str, Any]) -> SimpleNamespace:
        calls.append((method, params))
        return SimpleNamespace(
            result={"schema_version": 1, "ok": True, "status": "ok"},
            response_error=None,
            error_kind=None,
            request_started=True,
        )

    monkeypatch.setattr("tendwire.cli._try_daemon_attempt", attempt)
    for argv, stdin, expected in cases:
        monkeypatch.setattr("sys.stdin", io.StringIO(stdin))
        assert main(["connector", *argv]) == 0
        captured = capsys.readouterr()
        assert json.loads(captured.out)["ok"] is True
        assert captured.err == ""
        assert calls.pop() == (f"connector.{argv[0]}", expected)
