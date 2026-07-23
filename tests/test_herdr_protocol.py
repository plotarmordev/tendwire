"""Tests for the inactive Herdr socket protocol helpers."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from tendwire.backends.herdr_protocol import (
    HerdrEnvelopeError,
    HerdrMalformedLineError,
    HerdrRequestIdMismatchError,
    HerdrSocketPathError,
    HERDR_EVENTS_SUBSCRIBE_METHOD,
    HERDR_OFFICIAL_EVENT_NAME_SET,
    HERDR_OFFICIAL_EVENT_NAMES,
    build_events_subscribe_params,
    build_events_subscribe_request,
    build_request,
    ensure_response_id,
    error_payload,
    frame_request,
    is_error_response,
    is_event,
    is_result_response,
    parse_json_line,
    resolve_socket_path,
    result_payload,
    validate_event,
    validate_response,
    validate_server_envelope,
)


def test_resolve_socket_path_prefers_explicit_absolute_path(tmp_path: Path) -> None:
    socket_path = tmp_path / "herdr.sock"
    env = {"TENDWIRE_HERDR_SOCKET": str(tmp_path / "ignored.sock")}

    assert resolve_socket_path(socket_path, env=env) == str(socket_path)


def test_resolve_socket_path_expands_home_for_explicit_path(tmp_path: Path) -> None:
    home = tmp_path / "home"

    assert resolve_socket_path("~/custom.sock", home=home) == str(home / "custom.sock")


def test_resolve_socket_path_rejects_relative_explicit_path() -> None:
    with pytest.raises(HerdrSocketPathError):
        resolve_socket_path("relative.sock")


def test_resolve_socket_path_env_order_and_home_expansion(tmp_path: Path) -> None:
    home = tmp_path / "home"
    env = {
        "TENDWIRE_HERDR_SOCKET": "~/primary.sock",
        "HERDR_SOCKET_PATH": str(tmp_path / "secondary.sock"),
        "TENDWIRE_HERDR_SESSION": "session-a",
        "HERDR_SESSION": "session-b",
    }

    assert resolve_socket_path(env=env, home=home) == str(home / "primary.sock")


def test_resolve_socket_path_uses_herdr_socket_path_when_primary_empty(tmp_path: Path) -> None:
    env = {
        "TENDWIRE_HERDR_SOCKET": "",
        "HERDR_SOCKET_PATH": str(tmp_path / "secondary.sock"),
        "TENDWIRE_HERDR_SESSION": "ignored",
    }

    assert resolve_socket_path(env=env, home=tmp_path) == str(tmp_path / "secondary.sock")


def test_resolve_socket_path_uses_tendwire_session_before_herdr_session(tmp_path: Path) -> None:
    env = {
        "TENDWIRE_HERDR_SESSION": "alpha",
        "HERDR_SESSION": "beta",
    }

    assert resolve_socket_path(env=env, home=tmp_path) == str(
        tmp_path / ".config" / "herdr" / "sessions" / "alpha" / "herdr.sock"
    )


def test_resolve_socket_path_uses_herdr_session_when_tendwire_session_empty(tmp_path: Path) -> None:
    env = {
        "TENDWIRE_HERDR_SOCKET": "",
        "HERDR_SOCKET_PATH": "   ",
        "TENDWIRE_HERDR_SESSION": "",
        "HERDR_SESSION": "beta",
    }

    assert resolve_socket_path(env=env, home=tmp_path) == str(
        tmp_path / ".config" / "herdr" / "sessions" / "beta" / "herdr.sock"
    )


def test_resolve_socket_path_defaults_to_config_socket(tmp_path: Path) -> None:
    assert resolve_socket_path(env={}, home=tmp_path) == str(
        tmp_path / ".config" / "herdr" / "herdr.sock"
    )


def test_resolve_socket_path_rejects_relative_env_socket_path() -> None:
    with pytest.raises(HerdrSocketPathError):
        resolve_socket_path(env={"TENDWIRE_HERDR_SOCKET": "relative.sock"})


def test_build_request_uses_unique_string_ids_and_newline_framing() -> None:
    first = build_request("pane.read", {"pane_id": "p-1"})
    second = build_request("pane.read", {"pane_id": "p-1"})

    assert isinstance(first["id"], str)
    assert isinstance(second["id"], str)
    assert first["id"] != second["id"]
    assert first["method"] == "pane.read"
    assert first["params"] == {"pane_id": "p-1"}

    line = frame_request(first)
    assert line.endswith(b"\n")
    assert json.loads(line.decode("utf-8")) == first



def test_build_events_subscribe_request_uses_official_method_params_and_default_order() -> None:
    params = build_events_subscribe_params()

    assert params == {"subscriptions": [{"type": name} for name in HERDR_OFFICIAL_EVENT_NAMES]}
    assert build_events_subscribe_request(request_id="sub-1") == {
        "id": "sub-1",
        "method": HERDR_EVENTS_SUBSCRIBE_METHOD,
        "params": params,
    }
    assert HERDR_OFFICIAL_EVENT_NAME_SET == frozenset(HERDR_OFFICIAL_EVENT_NAMES)


@pytest.mark.parametrize("event_name", HERDR_OFFICIAL_EVENT_NAMES)
def test_build_events_subscribe_params_accepts_each_official_event_name(event_name: str) -> None:
    assert build_events_subscribe_params([event_name]) == {"subscriptions": [{"type": event_name}]}


def test_official_event_subscription_names_exclude_legacy_aliases() -> None:
    legacy = {
        "pane.observed",
        "workspace.observed",
        "agent.status_changed",
        "agent.detected",
        "worktree.updated",
    }

    assert legacy.isdisjoint(HERDR_OFFICIAL_EVENT_NAME_SET)


@pytest.mark.parametrize(
    "event_names",
    [
        ["pane.observed"],
        ["workspace.observed"],
        ["agent.status_changed"],
        ["worktree.updated"],
        [""],
        ["   "],
        [" pane.created "],
        [123],
        [None],
        object(),
    ],
)
def test_build_events_subscribe_params_rejects_unknown_non_string_and_empty_names(
    event_names: object,
) -> None:
    with pytest.raises(HerdrEnvelopeError):
        build_events_subscribe_params(event_names)  # type: ignore[arg-type]

def test_parse_valid_result_error_and_event_envelopes() -> None:
    result = validate_response(
        parse_json_line(
            b'{"id":"req-1","result":{"items":[{"id":"a"}]},"future":"ignored"}\n'
        )
    )
    error = validate_response(parse_json_line(b'{"id":"req-2","error":{"message":"no"}}\n'))
    event = validate_event(
        parse_json_line(
            b'{"id":"sub-1","event":"pane.output_matched","data":{"text":"hello"}}\n'
        )
    )
    idless_event = validate_event(
        parse_json_line(
            b'{"event":"pane.agent_status_changed","data":{"status":"blocked"}}\n'
        )
    )

    assert is_result_response(result) is True
    assert result_payload(result) == {"items": [{"id": "a"}]}
    assert is_error_response(error) is True
    assert error_payload(error) == {"message": "no"}
    assert is_event(event) is True
    assert event == {
        "id": "sub-1",
        "event": "pane.output_matched",
        "data": {"text": "hello"},
    }
    assert is_event(idless_event) is True
    assert idless_event == {
        "event": "pane.agent_status_changed",
        "data": {"status": "blocked"},
    }


def test_parse_json_line_rejects_malformed_json() -> None:
    with pytest.raises(HerdrMalformedLineError):
        parse_json_line(b"{not json}\n")


def test_parse_json_line_rejects_malformed_utf8() -> None:
    with pytest.raises(HerdrMalformedLineError):
        parse_json_line(b"\xff\n")


def test_validate_server_envelope_rejects_missing_id() -> None:
    with pytest.raises(HerdrEnvelopeError):
        validate_server_envelope({"result": {"ok": True}})


def test_validate_server_envelope_scopes_herdr_075_uncorrelated_error() -> None:
    envelope = {
        "id": "",
        "error": {
            "code": "invalid_request",
            "message": "invalid request: missing field pane_id",
        },
    }

    with pytest.raises(HerdrEnvelopeError):
        validate_server_envelope(envelope)

    assert validate_server_envelope(
        envelope,
        allow_uncorrelated_error=True,
    ) == envelope

    for error in (
        {"code": "permission_denied", "message": "invalid request: denied"},
        {"code": "invalid_request", "message": "subscription unavailable"},
    ):
        with pytest.raises(HerdrEnvelopeError):
            validate_server_envelope(
                {"id": "", "error": error},
                allow_uncorrelated_error=True,
            )


def test_validate_server_envelope_rejects_missing_id_error() -> None:
    with pytest.raises(HerdrEnvelopeError):
        validate_server_envelope({"error": {"message": "uncorrelated"}})


@pytest.mark.parametrize(
    "envelope",
    [
        {"id": "req-1"},
        {"id": "req-1", "result": {}, "error": {}},
        {"id": "req-1", "event": "pane.output", "result": {}},
    ],
)
def test_validate_server_envelope_rejects_wrong_envelope_shape(envelope: dict[str, object]) -> None:
    with pytest.raises(HerdrEnvelopeError):
        validate_server_envelope(envelope)


def test_parse_json_line_rejects_non_object_envelope() -> None:
    with pytest.raises(HerdrEnvelopeError):
        parse_json_line(b'["not","an","object"]\n')


def test_unknown_fields_are_tolerated_without_changing_result_payload() -> None:
    response = validate_response(
        {"id": "req-1", "result": {"raw": {"unknown": [1, 2]}}, "extra": {"ignored": True}}
    )

    assert result_payload(response) == {"raw": {"unknown": [1, 2]}}


def test_ensure_response_id_rejects_mismatch() -> None:
    with pytest.raises(HerdrRequestIdMismatchError):
        ensure_response_id({"id": "actual", "result": {}}, "expected")
