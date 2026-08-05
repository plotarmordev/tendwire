from __future__ import annotations

import pytest

from tendwire.backends.acp_protocol import (
    AcpEnvelopeError,
    AcpFramingError,
    AcpRemoteError,
    JsonRpcNotification,
    JsonRpcRequest,
    JsonRpcResponse,
    decode_json_line,
    encode_message,
    parse_permission_request,
    parse_session_update,
    validate_envelope,
)


def test_strict_json_line_round_trip() -> None:
    envelope = {
        "jsonrpc": "2.0",
        "id": 7,
        "method": "session/new",
        "params": {"cwd": "/tmp", "mcpServers": []},
    }
    encoded = encode_message(envelope)
    assert encoded.endswith(b"\n")
    assert decode_json_line(encoded) == JsonRpcRequest(
        7, "session/new", {"cwd": "/tmp", "mcpServers": []}
    )


@pytest.mark.parametrize(
    "line",
    [
        b'{"jsonrpc":"2.0","method":"x"}',
        b'{"jsonrpc":"2.0","method":"x"}\n{}\n',
        b'{"jsonrpc":"2.0","method":"x","method":"y"}\n',
        b'{"jsonrpc":"2.0","method":"x","params":[],"id":1}\n',
        b'{"jsonrpc":"2.0","method":"x","params":{"n":NaN}}\n',
        b"\xff\n",
    ],
)
def test_rejects_invalid_or_ambiguous_frames(line: bytes) -> None:
    with pytest.raises((AcpFramingError, AcpEnvelopeError)):
        decode_json_line(line)


def test_rejects_oversized_inbound_and_outbound_frames() -> None:
    with pytest.raises(AcpFramingError):
        decode_json_line(b'{"jsonrpc":"2.0","method":"long"}\n', max_frame_bytes=10)
    with pytest.raises(AcpFramingError):
        encode_message(
            {"jsonrpc": "2.0", "method": "long", "params": {}},
            max_frame_bytes=10,
        )


def test_response_error_is_typed() -> None:
    response = decode_json_line(
        b'{"jsonrpc":"2.0","id":"r1","error":{"code":-32000,"message":"nope","data":{"retry":false}}}\n'
    )
    assert isinstance(response, JsonRpcResponse)
    with pytest.raises(AcpRemoteError) as raised:
        response.result_or_raise()
    assert raised.value.request_id == "r1"
    assert raised.value.code == -32000
    assert raised.value.data == {"retry": False}


def test_notification_and_session_update_are_typed_but_extension_safe() -> None:
    message = decode_json_line(
        b'{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"s1","update":{"sessionUpdate":"agent_thought_chunk","content":{"type":"text","text":"summary"}}}}\n'
    )
    assert isinstance(message, JsonRpcNotification)
    update = parse_session_update(message.params)
    assert update.session_id == "s1"
    assert update.raw["update"]["sessionUpdate"] == "agent_thought_chunk"
    assert update.raw["update"]["content"]["text"] == "summary"

    extension = parse_session_update(
        {"sessionId": "s1", "update": {"sessionUpdate": "vendor_progress"}}
    )
    assert extension.raw["update"]["sessionUpdate"] == "vendor_progress"


def test_permission_request_validation_and_typed_options() -> None:
    request = JsonRpcRequest(
        42,
        "session/request_permission",
        {
            "sessionId": "s1",
            "toolCall": {"toolCallId": "tool-1", "status": "pending"},
            "options": [
                {"optionId": "yes", "name": "Allow", "kind": "allow_once"}
            ],
        },
    )
    parsed = parse_permission_request(request)
    assert parsed.request_id == 42
    assert parsed.options[0].kind == "allow_once"


def test_request_ids_follow_acp_json_rpc_domain() -> None:
    assert isinstance(
        validate_envelope({"jsonrpc": "2.0", "id": "", "method": "_vendor.example/x"}),
        JsonRpcRequest,
    )
    null_request = validate_envelope(
        {"jsonrpc": "2.0", "id": None, "method": "_vendor.example/x"}
    )
    assert isinstance(null_request, JsonRpcRequest)
    assert null_request.request_id is None
    assert isinstance(
        validate_envelope(
            {"jsonrpc": "2.0", "id": 2**63 - 1, "result": {}}
        ),
        JsonRpcResponse,
    )
    for invalid_id in (True, 2**63, -(2**63) - 1, 1.5):
        with pytest.raises(AcpEnvelopeError):
            validate_envelope(
                {"jsonrpc": "2.0", "id": invalid_id, "method": "_vendor.example/x"}
            )


def test_permission_option_ids_must_be_unambiguous() -> None:
    request = JsonRpcRequest(
        42,
        "session/request_permission",
        {
            "sessionId": "s1",
            "toolCall": {"toolCallId": "tool-1"},
            "options": [
                {"optionId": "same", "name": "Allow", "kind": "allow_once"},
                {"optionId": "same", "name": "Reject", "kind": "reject_once"},
            ],
        },
    )
    with pytest.raises(AcpEnvelopeError, match="unique"):
        parse_permission_request(request)


def test_permission_request_matches_stable_v1_required_fields_and_enum() -> None:
    empty_options = JsonRpcRequest(
        42,
        "session/request_permission",
        {"sessionId": "s1", "toolCall": {"toolCallId": "tool-1"}, "options": []},
    )
    assert parse_permission_request(empty_options).options == ()

    missing_tool_call_id = JsonRpcRequest(
        43,
        "session/request_permission",
        {
            "sessionId": "s1",
            "toolCall": {},
            "options": [
                {"optionId": "allow", "name": "Allow", "kind": "allow_once"}
            ],
        },
    )
    with pytest.raises(AcpEnvelopeError, match="toolCallId"):
        parse_permission_request(missing_tool_call_id)

    unknown_kind = JsonRpcRequest(
        44,
        "session/request_permission",
        {
            "sessionId": "s1",
            "toolCall": {"toolCallId": "tool-1"},
            "options": [
                {"optionId": "future", "name": "Future", "kind": "future_kind"}
            ],
        },
    )
    with pytest.raises(AcpEnvelopeError, match="ACP v1"):
        parse_permission_request(unknown_kind)


def test_unknown_update_and_nested_extension_payload_are_preserved() -> None:
    extension = parse_session_update(
        {
            "sessionId": "s1",
            "update": {
                "sessionUpdate": "vendor/future_progress",
                "opaque": {"revision": 7, "items": [1, 2]},
            },
            "_meta": {"vendor.example/trace": "abc"},
        }
    )
    assert extension.raw["update"]["sessionUpdate"] == "vendor/future_progress"
    assert extension.raw["update"]["opaque"] == {"revision": 7, "items": [1, 2]}
    assert extension.raw["_meta"] == {"vendor.example/trace": "abc"}
