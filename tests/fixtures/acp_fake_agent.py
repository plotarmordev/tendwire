"""Deterministic newline-delimited JSON-RPC peer used by ACP client tests."""

from __future__ import annotations

import json
import signal
import sys
import time


MODE = sys.argv[1] if len(sys.argv) > 1 else "normal"


def send(value: object) -> None:
    sys.stdout.write(json.dumps(value, separators=(",", ":")) + "\n")
    sys.stdout.flush()


def response(request_id: object, result: object) -> None:
    send({"jsonrpc": "2.0", "id": request_id, "result": result})


def update(session_id: str, kind: str, **values: object) -> None:
    send(
        {
            "jsonrpc": "2.0",
            "method": "session/update",
            "params": {
                "sessionId": session_id,
                "update": {"sessionUpdate": kind, **values},
            },
        }
    )


pending_prompt_id: object | None = None
pending_prompt_session = ""
pending_permission_ids: set[object] = set()

if MODE == "no_read":
    time.sleep(60)

if MODE == "stubborn":
    signal.signal(signal.SIGTERM, signal.SIG_IGN)

for line in sys.stdin:
    message = json.loads(line)
    method = message.get("method")
    request_id = message.get("id")
    params = message.get("params", {})

    if MODE == "initialize_only" and method != "initialize":
        send(
            {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {"code": -32601, "message": "Method not found"},
            }
        )
        continue

    if method == "initialize":
        if MODE == "malformed":
            sys.stdout.write("not-json\n")
            sys.stdout.flush()
            continue
        if MODE == "partial_eof":
            sys.stdout.write('{"jsonrpc":"2.0","id":')
            sys.stdout.flush()
            raise SystemExit(0)
        if MODE == "oversize":
            response(request_id, {"protocolVersion": 1, "padding": "x" * 10000})
            continue
        response(
            request_id,
            {
                "protocolVersion": True if MODE == "bool_version" else 1,
                "agentCapabilities": (
                    {}
                    if MODE in {"baseline", "initialize_only"}
                    else {
                        "loadSession": True,
                        "sessionCapabilities": {
                            "list": {},
                            "delete": {},
                            "resume": {},
                            "close": {},
                            "additionalDirectories": {},
                        },
                        "promptCapabilities": {
                            "image": True,
                            "audio": True,
                            "embeddedContext": True,
                        },
                        "mcpCapabilities": {"http": True, "sse": True},
                        "_meta": {"vendor.example": {"level": 2}},
                    }
                ),
                "agentInfo": {"name": "fake", "version": "1.0"},
                **(
                    {
                        "_meta": {
                            "steering": {
                                "supported": True,
                                "injectOnly": {"version": 1},
                            }
                        }
                    }
                    if MODE in {"steering", "steering_idle"}
                    else {}
                ),
                **(
                    {
                        "authMethods": [
                            {},
                            {"id": "missing-name"},
                            {"id": "valid", "name": "Valid agent login"},
                            {"type": "future", "id": "x", "name": "Future"},
                        ]
                    }
                    if MODE == "auth_shapes"
                    else {}
                ),
            },
        )
        if MODE == "extensions":
            send(
                {
                    "jsonrpc": "2.0",
                    "method": "_vendor.example/future_notification",
                    "params": {"opaque": {"revision": 9}},
                }
            )
        if MODE == "extension_flood":
            for index in range(32):
                send(
                    {
                        "jsonrpc": "2.0",
                        "method": "_vendor.example/progress",
                        "params": {"sequence": index},
                    }
                )
        if MODE in {"unknown_request", "supported_request"}:
            send(
                {
                    "jsonrpc": "2.0",
                    "id": 777 if MODE == "unknown_request" else 778,
                    "method": "_vendor.example/request",
                    "params": {"opaque": True},
                }
            )
        if MODE == "null_response":
            send(
                {
                    "jsonrpc": "2.0",
                    "id": None,
                    "error": {"code": -32600, "message": "unrelated invalid request"},
                }
            )
        if MODE == "stderr_tail":
            sys.stderr.write("prefix-" + "x" * 500 + "-TAIL")
            sys.stderr.flush()
        if MODE == "exit_after_init":
            time.sleep(0.05)
            raise SystemExit(0)
        if MODE == "flood":
            time.sleep(0.05)
            for index in range(4):
                update(
                    "s-flood",
                    "agent_message_chunk",
                    content={"type": "text", "text": str(index)},
                )
    elif method == "session/new":
        update("s-new", "agent_message_chunk", content={"type": "text", "text": "hi"})
        response(
            request_id,
            {
                "sessionId": "s-new",
                "modes": {
                    "currentModeId": "default",
                    "availableModes": [{"id": "default", "name": "Default"}],
                },
            },
        )
    elif method == "session/load" or method == "session/resume":
        if MODE == "load_replay" and method == "session/load":
            for index in range(64):
                update(
                    params["sessionId"],
                    "user_message_chunk" if index % 2 == 0 else "agent_message_chunk",
                    messageId=f"replay-{index}",
                    content={"type": "text", "text": str(index)},
                )
        response(
            request_id,
            {
                "configOptions": [
                    {
                        "id": "model",
                        "name": "Model",
                        "type": "select",
                        "currentValue": "x",
                        "options": [{"value": "x", "name": "Model X"}],
                    }
                ]
            },
        )
    elif method == "session/close" or method == "session/delete":
        response(request_id, {"_meta": {"vendor.example": {"receipt": method}}})
    elif method == "session/list":
        if MODE == "slow":
            time.sleep(2)
            continue
        cursor = params.get("cursor")
        response(
            request_id,
            {
                "sessions": [
                    {
                        "sessionId": "s2" if cursor else "s1",
                        "cwd": "/tmp/project",
                        "title": "second" if cursor else "first",
                    }
                ],
                **({} if cursor else {"nextCursor": "page-2"}),
            },
        )
    elif method == "session/prompt":
        if MODE == "echo_prompt":
            response(request_id, {"stopReason": "end_turn"})
            continue
        pending_prompt_id = request_id
        pending_prompt_session = params["sessionId"]
        update(
            pending_prompt_session,
            "agent_thought_chunk",
            content={"type": "text", "text": "reasoning summary"},
        )
        send(
            {
                "jsonrpc": "2.0",
                "id": 900,
                "method": "session/request_permission",
                "params": {
                    "sessionId": pending_prompt_session,
                    "toolCall": {"toolCallId": "tool-1", "status": "pending"},
                    "options": [
                        {
                            "optionId": "allow",
                            "name": "Allow once",
                            "kind": "allow_once",
                        },
                        {
                            "optionId": "reject",
                            "name": "Reject once",
                            "kind": "reject_once",
                        },
                    ],
                },
            }
        )
        pending_permission_ids.add(900)
    elif method == "_session/steering":
        correlation_id = params.get("correlationId")
        if MODE == "steering_idle" and params.get("startNewTurnWhenIdle") is False:
            response(
                request_id,
                {
                    "outcome": "notActive",
                    **(
                        {"correlationId": correlation_id}
                        if correlation_id is not None
                        else {}
                    ),
                },
            )
            continue
        update(
            params["sessionId"],
            "user_message_chunk",
            content=params["prompt"][0],
        )
        response(
            request_id,
            {
                "outcome": "injected",
                **(
                    {"correlationId": correlation_id}
                    if correlation_id is not None
                    else {}
                ),
            },
        )
    elif method == "session/cancel":
        if MODE == "cancel_race" and pending_prompt_id is not None:
            send(
                {
                    "jsonrpc": "2.0",
                    "id": 901,
                    "method": "session/request_permission",
                    "params": {
                        "sessionId": pending_prompt_session,
                        "toolCall": {"toolCallId": "tool-race", "status": "pending"},
                        "options": [
                            {
                                "optionId": "allow-race",
                                "name": "Allow once",
                                "kind": "allow_once",
                            }
                        ],
                    },
                }
            )
            pending_permission_ids.add(901)
    elif request_id in {777, 778}:
        error = message.get("error", {})
        update(
            "s-extension",
            "agent_message_chunk",
            content={
                "type": "text",
                "text": "method-not-found" if error.get("code") == -32601 else "unexpected",
            },
        )
    elif request_id in pending_permission_ids and pending_prompt_id is not None:
        outcome = message["result"]["outcome"]["outcome"]
        pending_permission_ids.remove(request_id)
        if not pending_permission_ids:
            update(
                pending_prompt_session,
                "plan",
                entries=[
                    {"content": "done", "priority": "medium", "status": "completed"}
                ],
            )
            response(
                pending_prompt_id,
                {"stopReason": "cancelled" if outcome == "cancelled" else "end_turn"},
            )
            pending_prompt_id = None

if MODE == "stubborn":
    while True:
        time.sleep(60)
