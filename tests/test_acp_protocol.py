from __future__ import annotations

import pytest

from tendwire.backends.acp_protocol import (
    AgentCapabilities,
    AcpEnvelopeError,
    AcpFramingError,
    AcpRemoteError,
    JsonRpcNotification,
    JsonRpcRequest,
    JsonRpcResponse,
    PermissionOptionKind,
    SessionUpdateKind,
    decode_json_line,
    encode_message,
    parse_permission_request,
    parse_session_update,
    request_envelope,
)


def test_strict_json_line_round_trip() -> None:
    envelope = request_envelope(7, "session/new", {"cwd": "/tmp", "mcpServers": []})
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
    assert update.update_kind is SessionUpdateKind.AGENT_THOUGHT_CHUNK
    assert update.update["content"]["text"] == "summary"

    extension = parse_session_update(
        {"sessionId": "s1", "update": {"sessionUpdate": "vendor_progress"}}
    )
    assert extension.update_kind == "vendor_progress"


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
    assert parsed.options[0].kind is PermissionOptionKind.ALLOW_ONCE


def test_capability_presence_uses_acp_object_semantics() -> None:
    capabilities = AgentCapabilities.from_mapping(
        {
            "loadSession": True,
            "sessionCapabilities": {
                "list": {},
                "resume": {},
                "additionalDirectories": {},
            },
        }
    )
    assert capabilities.load_session
    assert capabilities.session_list
    assert capabilities.session_resume
    assert capabilities.additional_directories
