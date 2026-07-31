"""Deterministic newline-delimited JSON-RPC peer used by ACP client tests."""

from __future__ import annotations

import json
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

for line in sys.stdin:
    message = json.loads(line)
    method = message.get("method")
    request_id = message.get("id")
    params = message.get("params", {})

    if method == "initialize":
        if MODE == "malformed":
            sys.stdout.write("not-json\n")
            sys.stdout.flush()
            continue
        if MODE == "oversize":
            response(request_id, {"protocolVersion": 1, "padding": "x" * 10000})
            continue
        response(
            request_id,
            {
                "protocolVersion": 1,
                "agentCapabilities": (
                    {}
                    if MODE == "baseline"
                    else {
                        "loadSession": True,
                        "sessionCapabilities": {
                            "list": {},
                            "resume": {},
                            "additionalDirectories": {},
                        },
                    }
                ),
                "agentInfo": {"name": "fake", "version": "1.0"},
            },
        )
    elif method == "initialized":
        send(
            {
                "jsonrpc": "2.0",
                "method": "fake/initialized_seen",
                "params": {},
            }
        )
    elif method == "session/new":
        update("s-new", "agent_message_chunk", content={"type": "text", "text": "hi"})
        response(
            request_id,
            {"sessionId": "s-new", "modes": {"currentModeId": "default"}},
        )
    elif method == "session/load" or method == "session/resume":
        response(request_id, {"configOptions": [{"id": "model", "currentValue": "x"}]})
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
    elif method == "session/cancel":
        # The client must additionally resolve permission request 900 as cancelled.
        pass
    elif request_id == 900 and pending_prompt_id is not None:
        outcome = message["result"]["outcome"]["outcome"]
        update(
            pending_prompt_session,
            "plan",
            entries=[{"content": "done", "status": "completed"}],
        )
        response(
            pending_prompt_id,
            {"stopReason": "cancelled" if outcome == "cancelled" else "end_turn"},
        )
        pending_prompt_id = None
