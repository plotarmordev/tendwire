from __future__ import annotations

import pytest

from tendwire.backends.herdr_protocol import (
    HerdrEnvelopeError,
    HerdrMalformedLineError,
    HerdrRequestIdMismatchError,
    build_request,
    ensure_response_id,
    frame_request,
    parse_json_line,
    resolve_socket_path,
    validate_server_envelope,
)


def test_request_round_trip_is_one_correlated_json_line() -> None:
    request = build_request("agent.acp_status", {"target": "worker"}, request_id="r1")
    assert parse_json_line(frame_request(request)) == request
    ensure_response_id({"id": "r1", "result": {}}, "r1")
    with pytest.raises(HerdrRequestIdMismatchError):
        ensure_response_id({"id": "other", "result": {}}, "r1")


def test_protocol_rejects_events_and_malformed_lines() -> None:
    with pytest.raises(HerdrEnvelopeError):
        validate_server_envelope({"event": "pane.created", "data": {}})
    with pytest.raises(HerdrMalformedLineError):
        parse_json_line(b"not-json\n")


def test_socket_resolution_keeps_frozen_precedence(tmp_path) -> None:
    explicit = tmp_path / "explicit.sock"
    assert resolve_socket_path(explicit, env={"HERDR_SESSION": "ignored"}) == str(explicit)
    assert resolve_socket_path(env={"TENDWIRE_HERDR_SESSION": "work"}, home=tmp_path) == str(
        tmp_path / ".config" / "herdr" / "sessions" / "work" / "herdr.sock"
    )
