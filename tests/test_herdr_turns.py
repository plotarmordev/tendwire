"""Tests for private Herdr turn ingestion into public Tendwire turns."""

from __future__ import annotations

import json
import multiprocessing
import sqlite3
import subprocess
import threading
import pytest
from pathlib import Path
from typing import Any

from tendwire.backends import herdr_turns
from tendwire.backends.herdr_turns import refresh_structured_turn_content
from tendwire.config import Config
from tendwire.core.models import WorkerBinding, sanitize_canonical_turn_text
from tendwire.core.projector import project_from_raw
from tendwire.core.turns import (
    TURN_CONTENT_PAGE_MAX_UTF8_BYTES,
    TURN_STREAM_TEXT_MAX_CHARS,
    is_internal_automation_turn_payload,
)
from tendwire.store import sqlite as store_sqlite
from tendwire.store.sqlite import (
    init_store,
    save_snapshot,
    turns_payload_from_store,
    upsert_worker_bindings,
)


def _read_test_ipc_request(channel):
    return json.loads(
        herdr_turns._blocking_recv_frame(
            channel,
            herdr_turns._CODEX_STATE_IPC_MAX_BYTES,
        ).decode("utf-8")
    )


def _send_test_ipc_response(channel, response) -> None:
    herdr_turns._blocking_send_frame(
        channel,
        json.dumps(response, separators=(",", ":")).encode("utf-8"),
    )


def _failed_isolated_turn_child(channel) -> None:
    try:
        request = _read_test_ipc_request(channel)
        _send_test_ipc_response(
            channel,
            {
                "protocol": 1,
                "nonce": request["nonce"],
                "disposition": "failed",
                "content": None,
                "parser_state": None,
                "bytes_read": 0,
            },
        )
    finally:
        channel.close()


def _invalid_isolated_turn_child(channel) -> None:
    try:
        _read_test_ipc_request(channel)
        _send_test_ipc_response(
            channel,
            {
                "protocol": 1,
                "nonce": "not-the-request-nonce",
                "disposition": "ok",
                "content": None,
                "parser_state": None,
                "bytes_read": 0,
            },
        )
    finally:
        channel.close()


def _blocked_isolated_turn_child(channel) -> None:
    _read_test_ipc_request(channel)
    threading.Event().wait(30)


def _growing_isolated_turn_child(channel) -> None:
    try:
        request = _read_test_ipc_request(channel)
        large_final = "grew-during-read-" + ("g" * (2 * 1024 * 1024))
        with open(request["target_value"], "a", encoding="utf-8") as handle:
            handle.write(
                "\n"
                + json.dumps(
                    {
                        "type": "message",
                        "id": "grown-final",
                        "message": {
                            "role": "assistant",
                            "stopReason": "stop",
                            "content": [{"type": "text", "text": large_final}],
                        },
                    },
                    separators=(",", ":"),
                )
            )
        parser_state = request["parser_state"]
        assert parser_state["source"] == "omp"
        parsed = herdr_turns._read_omp_session_turn_with_state(
            request["target_value"],
            herdr_turns._deserialize_omp_state(parser_state["state"]),
        )
        content, checkpoint, bytes_read = parsed
        response = {
            "protocol": 1,
            "nonce": request["nonce"],
            "disposition": "ok",
            "content": content,
            "parser_state": {
                "source": "omp",
                "state": herdr_turns._serialize_omp_state(checkpoint),
            },
            "bytes_read": bytes_read,
        }
        herdr_turns._blocking_send_streamed_omp_response(
            channel,
            json.dumps(response, separators=(",", ":")).encode("utf-8"),
            request["nonce"],
        )
    finally:
        channel.close()


def test_refresh_structured_turn_content_uses_private_binding_without_public_leak(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "turns.db"
    config = Config(
        host_id="turn-host",
        db_path=db_path,
        herdr_bin="herdr_turn_adapter.py",
        herdr_timeout_seconds=2,
    )
    snapshot = project_from_raw(
        config,
        workers=[{"id": "worker-1", "name": "codex", "status": "active", "space_id": "space-1"}],
    )
    init_store(db_path)
    save_snapshot(db_path, snapshot)
    worker = snapshot.workers[0]
    upsert_worker_bindings(
        db_path,
        [
            WorkerBinding(
                host_id=config.host_id,
                worker_id=worker.id,
                worker_fingerprint=worker.fingerprint,
                backend="herdr",
                target_kind="agent_id",
                target_value="agent-private",
                turn_target_kind="pane_id",
                turn_target_value="pane-private",
                sendable=True,
                observed_at="2026-01-01T00:00:00+00:00",
                expires_at="9999-12-31T23:59:59+00:00",
                private_fingerprint="private-binding",
            )
        ],
    )
    calls: list[tuple[list[str], dict[str, Any]]] = []

    def fake_run(args: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        calls.append((args, kwargs))
        return subprocess.CompletedProcess(
            args=args,
            returncode=0,
            stdout=json.dumps(
                {
                    "result": {
                            "turn": {
                                "available": True,
                                "source_turn_id": "private-binding-source",
                                "user_text": "Why is Telegram showing lifecycle status?",
                            "assistant_final_text": "Use Tendwire turn text, not pane_id pane-private.",
                            "assistant_stream_text": "Checking source mode...",
                            "complete": True,
                            "has_open_turn": False,
                        }
                    }
                }
            ),
            stderr="",
        )

    monkeypatch.setattr(herdr_turns.subprocess, "run", fake_run)

    result = refresh_structured_turn_content(config)
    payload = turns_payload_from_store(db_path, config.host_id, snapshot=snapshot)

    assert result["attempted"] == 1
    assert result["updated"] == 1
    assert calls[0][0] == [
        "herdr_turn_adapter.py",
        "pane",
        "turn",
        "pane-private",
        "--last",
        "--format",
        "json",
    ]
    assert calls[0][1]["timeout"] == 2
    turn = payload["turns"][0]
    assert turn["user_text"] == "Why is Telegram showing lifecycle status?"
    assert "Use Tendwire turn text" in turn["assistant_final_text"]
    public_json = json.dumps(payload)
    assert "pane-private" not in public_json
    assert "agent-private" not in public_json
    assert turn["complete"] is True
    assert turn["has_open_turn"] is False


def test_refresh_structured_turn_content_reads_codex_session_jsonl(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "turns.db"
    codex_home = tmp_path / "codex-home"
    session_id = "019f2307-092b-7810-8323-418d7c55bd26"
    session_file = (
        codex_home
        / "sessions"
        / "2026"
        / "07"
        / "03"
        / f"rollout-2026-07-03T00-00-00-{session_id}.jsonl"
    )
    session_file.parent.mkdir(parents=True)
    turn_id = "turn-live"
    lines = [
        {
            "timestamp": "2026-07-03T08:00:00Z",
            "type": "event_msg",
            "payload": {"type": "task_started", "turn_id": turn_id},
        },
        {
            "timestamp": "2026-07-03T08:00:01Z",
            "type": "response_item",
            "payload": {
                "type": "message",
                "role": "user",
                "content": [{"type": "input_text", "text": "Please fix the source feed"}],
                "internal_chat_message_metadata_passthrough": {"turn_id": turn_id},
            },
        },
        {
            "timestamp": "2026-07-03T08:00:02Z",
            "type": "response_item",
            "payload": {
                "type": "message",
                "role": "assistant",
                "phase": "commentary",
                "content": [{"type": "output_text", "text": "Checking source state."}],
                "internal_chat_message_metadata_passthrough": {"turn_id": turn_id},
            },
        },
        {
            "timestamp": "2026-07-03T08:00:03Z",
            "type": "event_msg",
            "payload": {
                "type": "task_complete",
                "turn_id": turn_id,
                "last_agent_message": "Fixed the source feed.",
            },
        },
    ]
    session_file.write_text(
        "\n".join(json.dumps(item) for item in lines) + "\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("CODEX_HOME", str(codex_home))
    monkeypatch.setattr(
        herdr_turns.subprocess,
        "run",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("pane turn fallback should not run")),
    )

    config = Config(host_id="turn-host", db_path=db_path)
    snapshot = project_from_raw(
        config,
        workers=[{"id": "worker-1", "name": "codex", "status": "active", "space_id": "space-1"}],
    )
    init_store(db_path)
    save_snapshot(db_path, snapshot)
    worker = snapshot.workers[0]
    upsert_worker_bindings(
        db_path,
        [
            WorkerBinding(
                host_id=config.host_id,
                worker_id=worker.id,
                worker_fingerprint=worker.fingerprint,
                backend="herdr",
                target_kind="terminal_id",
                target_value="term-private",
                turn_target_kind="codex_session_id",
                turn_target_value=session_id,
                sendable=True,
                observed_at="2026-07-03T08:00:00+00:00",
                expires_at="9999-12-31T23:59:59+00:00",
                private_fingerprint="private-binding",
            )
        ],
    )

    result = refresh_structured_turn_content(config)
    payload = turns_payload_from_store(db_path, config.host_id, snapshot=snapshot)

    assert result == {"ok": True, "status": "ok", "updated": 1, "attempted": 1}
    turn = payload["turns"][0]
    assert turn["user_text"] == "Please fix the source feed"
    assert turn["assistant_final_text"] == "Fixed the source feed."
    assert turn["assistant_stream_text"] is None
    assert turn["complete"] is True
    assert turn["has_open_turn"] is False
    public_json = json.dumps(payload)
    assert session_id not in public_json
    assert "term-private" not in public_json


def test_refresh_structured_turn_content_skips_codex_automation_protocol_turn(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "turns.db"
    codex_home = tmp_path / "codex-home"
    session_id = "019f31a8-57cf-7353-b4f0-c25e523267af"
    session_file = (
        codex_home
        / "sessions"
        / "2026"
        / "07"
        / "05"
        / f"rollout-2026-07-05T00-00-00-{session_id}.jsonl"
    )
    session_file.parent.mkdir(parents=True)
    turn_id = "automation-turn"
    lines = [
        {
            "timestamp": "2026-07-05T08:00:00Z",
            "type": "event_msg",
            "payload": {"type": "task_started", "turn_id": turn_id},
        },
        {
            "timestamp": "2026-07-05T08:00:01Z",
            "type": "response_item",
            "payload": {
                "type": "message",
                "role": "user",
                "content": [
                    {
                        "type": "input_text",
                        "text": "Acme job\n\nTemplate: review-lead\nTemplate instructions:",
                    }
                ],
                "internal_chat_message_metadata_passthrough": {"turn_id": turn_id},
            },
        },
        {
            "timestamp": "2026-07-05T08:00:02Z",
            "type": "event_msg",
            "payload": {
                "type": "task_complete",
                "turn_id": turn_id,
                "last_agent_message": '{"acme_result":{"decision":"approved","summary":"internal job result"}}',
            },
        },
    ]
    session_file.write_text(
        "\n".join(json.dumps(item) for item in lines) + "\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("CODEX_HOME", str(codex_home))

    config = Config(host_id="turn-host", db_path=db_path)
    snapshot = project_from_raw(
        config,
        workers=[{"id": "worker-1", "name": "codex", "status": "active", "space_id": "space-1"}],
    )
    init_store(db_path)
    save_snapshot(db_path, snapshot)
    worker = snapshot.workers[0]
    upsert_worker_bindings(
        db_path,
        [
            WorkerBinding(
                host_id=config.host_id,
                worker_id=worker.id,
                worker_fingerprint=worker.fingerprint,
                backend="herdr",
                target_kind="terminal_id",
                target_value="term-private",
                turn_target_kind="codex_session_id",
                turn_target_value=session_id,
                sendable=True,
                observed_at="2026-07-05T08:00:00+00:00",
                expires_at="9999-12-31T23:59:59+00:00",
                private_fingerprint="private-binding",
            )
        ],
    )

    result = refresh_structured_turn_content(config)
    payload = turns_payload_from_store(db_path, config.host_id, snapshot=snapshot)
    public_json = json.dumps(payload)

    assert result == {"ok": True, "status": "ok", "updated": 0, "attempted": 1}
    assert "Acme job" not in public_json
    assert "acme_result" not in public_json


def test_turns_payload_from_store_quarantines_existing_automation_protocol_rows(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "turns.db"
    host_id = "turn-host"
    init_store(db_path)
    with sqlite3.connect(db_path) as conn:
        rows = [
            (
                host_id,
                "bad-turn",
                "worker-1",
                "active",
                "task",
                "2026-07-05T08:00:00+00:00",
                "bad-fp",
                "snap-fp",
                "2026-07-05T08:00:00+00:00",
                json.dumps(
                    {
                        "host_id": host_id,
                        "worker_id": "worker-1",
                        "status": "active",
                        "kind": "task",
                        "user_text": "Acme job\n\nTemplate: review-lead\nTemplate instructions:",
                        "assistant_final_text": '{"acme_result":{"decision":"approved","summary":"internal"}}',
                    }
                ),
                1,
            ),
            (
                host_id,
                "good-turn",
                "worker-1",
                "active",
                "task",
                "2026-07-05T08:01:00+00:00",
                "good-fp",
                "snap-fp",
                "2026-07-05T08:01:00+00:00",
                json.dumps(
                    {
                        "host_id": host_id,
                        "worker_id": "worker-1",
                        "status": "active",
                        "kind": "task",
                        "user_text": "Please review the issue",
                        "assistant_final_text": "Normal answer.",
                    }
                ),
                2,
            ),
        ]
        conn.executemany(
            """
            INSERT INTO turns (
                host_id, turn_id, worker_id, status, kind, updated_at, fingerprint,
                snapshot_content_fingerprint, observed_at, payload_json, list_sequence
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )

    payload = turns_payload_from_store(db_path, host_id)
    public_json = json.dumps(payload)

    assert len(payload["turns"]) == 1
    assert payload["turns"][0]["assistant_final_text"] == "Normal answer."
    assert "Acme job" not in public_json
    assert "acme_result" not in public_json


def test_internal_user_text_detects_local_command_artifacts() -> None:
    assert herdr_turns._is_internal_user_text("<local-command-caveat>Caveat: ...")
    assert herdr_turns._is_internal_user_text("  <command-name>/model</command-name>")
    assert herdr_turns._is_internal_user_text("<local-command-stdout>Set model</local-command-stdout>")
    assert herdr_turns._is_internal_user_text("<system-reminder>context</system-reminder>")
    assert herdr_turns._is_internal_user_text("<subagent_notification>done</subagent_notification>")
    assert not herdr_turns._is_internal_user_text("another test")


def test_internal_turn_filter_detects_automation_protocol_without_blocking_discussion() -> None:
    assert herdr_turns._is_internal_user_text(
        "Acme job\n\nTemplate: review-lead\nTemplate instructions:"
    )
    assert herdr_turns._is_internal_user_text(
        "Your previous response did not contain a valid acme_result JSON object.\n"
        "Validation errors (fix every line):"
    )
    assert is_internal_automation_turn_payload(
        {"assistant_final_text": '{"acme_result":{"decision":"approved","summary":"internal job result"}}'}
    )
    assert is_internal_automation_turn_payload(
        {"assistant_final_text": '```json\n{"acme_result":{"decision":"blocked"}}\n```'}
    )
    assert not herdr_turns._is_internal_user_text("Can you investigate why automation job responses leaked?")
    assert not is_internal_automation_turn_payload(
        {"assistant_final_text": "I found a leaked automation_result row in the Tendwire DB."}
    )
    assert not is_internal_automation_turn_payload(
        {
            "user_text": "Please return a JSON status object.",
            "assistant_final_text": '{"acme_result":{"status":"ok"}}',
        }
    )


def test_read_private_turn_skips_local_command_turns(monkeypatch) -> None:
    config = Config(host_id="turn-host", herdr_bin="herdr", herdr_timeout_seconds=2)
    payload = {
        "result": {
            "turn": {
                "available": True,
                "user_text": "<local-command-caveat>Caveat: The messages below were generated by the user while running local commands.</local-command-caveat>",
                "has_open_turn": True,
                "complete": False,
            }
        }
    }

    def fake_run(args, **kwargs):
        return subprocess.CompletedProcess(args=args, returncode=0, stdout=json.dumps(payload), stderr="")

    monkeypatch.setattr(herdr_turns.subprocess, "run", fake_run)
    assert herdr_turns._read_private_turn(config, "pane-1") is None


def test_read_private_turn_skips_automation_protocol_turns(monkeypatch) -> None:
    config = Config(host_id="turn-host", herdr_bin="herdr", herdr_timeout_seconds=2)
    payload = {
        "result": {
            "turn": {
                "available": True,
                "user_text": "Acme job\n\nTemplate: review-lead\nTemplate instructions:",
                "assistant_final_text": '{"acme_result":{"decision":"approved"}}',
                "has_open_turn": False,
                "complete": True,
                "source_turn_id": "automation-turn",
            }
        }
    }

    monkeypatch.setattr(herdr_turns.subprocess, "run", _run_returning(payload))
    assert herdr_turns._read_private_turn(config, "pane-1") is None


def test_read_private_turn_skips_promptless_status_finals(monkeypatch) -> None:
    config = Config(host_id="turn-host", herdr_bin="herdr", herdr_timeout_seconds=2)
    payload = {
        "result": {
            "turn": {
                "available": True,
                "assistant_final_text": "Initial state (review in progress; gate phase not yet reached). Waiting quietly for the gate verdict or merge. Standing by.",
                "complete": True,
                "has_open_turn": False,
                "source_turn_id": "status-only-turn",
                "model": "claude-opus-4-8",
            }
        }
    }

    monkeypatch.setattr(herdr_turns.subprocess, "run", _run_returning(payload))
    assert herdr_turns._read_private_turn(config, "pane-1") is None


def test_read_private_turn_keeps_prompted_status_like_final(monkeypatch) -> None:
    config = Config(host_id="turn-host", herdr_bin="herdr", herdr_timeout_seconds=2)
    payload = {
        "result": {
            "turn": {
                "available": True,
                "user_text": "What is the current state?",
                "assistant_final_text": "Current state: the review is complete.",
                "complete": True,
                "has_open_turn": False,
                "source_turn_id": "prompted-turn",
            }
        }
    }

    monkeypatch.setattr(herdr_turns.subprocess, "run", _run_returning(payload))
    content = herdr_turns._read_private_turn(config, "pane-1")
    assert content is not None
    assert content["user_text"] == "What is the current state?"
    assert content["assistant_final_text"] == "Current state: the review is complete."


def _run_returning(payload):
    def fake_run(args, **kwargs):
        return subprocess.CompletedProcess(args=args, returncode=0, stdout=json.dumps(payload), stderr="")
    return fake_run


def test_read_private_turn_emits_open_turn_from_open_fields(monkeypatch) -> None:
    config = Config(host_id="turn-host", herdr_bin="herdr", herdr_timeout_seconds=2)
    payload = {
        "result": {
            "turn": {
                "available": True,
                # top level is the PREVIOUS completed turn
                "complete": True,
                "has_open_turn": True,
                "user_text": "previous prompt",
                "assistant_final_text": "previous answer",
                "source_turn_id": "prompt-prev",
                # the in-progress turn is carried in open_* fields
                "open_turn_id": "prompt-open",
                "open_user_text": "current prompt",
                "assistant_stream_text": "thinking live...",
            }
        }
    }
    monkeypatch.setattr(herdr_turns.subprocess, "run", _run_returning(payload))
    content = herdr_turns._read_private_turn(config, "pane-1")
    assert content is not None
    assert content["user_text"] == "current prompt"
    assert content["assistant_stream_text"] == "thinking live..."
    assert content["assistant_final_text"] is None
    assert content["complete"] is False
    assert content["has_open_turn"] is True
    # keyed by the OPEN turn's stable prompt id, not the completed one
    assert content["source_turn_id"] == "prompt-open"


def test_open_turn_and_its_completion_share_source_turn_id(monkeypatch) -> None:
    """The open turn (prompt-open) and its later completion must share the id so
    a working card edits into the final instead of duplicating."""
    config = Config(host_id="turn-host", herdr_bin="herdr", herdr_timeout_seconds=2)
    open_payload = {
        "result": {
            "turn": {
                "available": True,
                "complete": True,
                "has_open_turn": True,
                "user_text": "older",
                "assistant_final_text": "older answer",
                "source_turn_id": "prompt-older",
                "open_turn_id": "prompt-X",
                "open_user_text": "the question",
                "assistant_stream_text": "working...",
            }
        }
    }
    monkeypatch.setattr(herdr_turns.subprocess, "run", _run_returning(open_payload))
    open_content = herdr_turns._read_private_turn(config, "pane-1")
    assert open_content["source_turn_id"] == "prompt-X"
    assert open_content["complete"] is False

    # Now the same turn completes (no open fields; it is the last completed one).
    done_payload = {
        "result": {
            "turn": {
                "available": True,
                "complete": True,
                "has_open_turn": False,
                "user_text": "the question",
                "assistant_final_text": "the answer",
                "source_turn_id": "prompt-X",
                "turn_id": "assistant-uuid-differs",
            }
        }
    }
    monkeypatch.setattr(herdr_turns.subprocess, "run", _run_returning(done_payload))
    done_content = herdr_turns._read_private_turn(config, "pane-1")
    assert done_content["source_turn_id"] == "prompt-X"  # same id, not the assistant uuid
    assert done_content["complete"] is True
    assert done_content["assistant_final_text"] == "the answer"


def test_read_private_turn_prefers_source_turn_id_over_turn_id(monkeypatch) -> None:
    config = Config(host_id="turn-host", herdr_bin="herdr", herdr_timeout_seconds=2)
    payload = {
        "result": {
            "turn": {
                "available": True,
                "complete": True,
                "has_open_turn": False,
                "user_text": "q",
                "assistant_final_text": "a",
                "source_turn_id": "stable-prompt",
                "turn_id": "assistant-uuid",
            }
        }
    }
    monkeypatch.setattr(herdr_turns.subprocess, "run", _run_returning(payload))
    content = herdr_turns._read_private_turn(config, "pane-1")
    assert content["source_turn_id"] == "stable-prompt"


def test_omp_agent_session_id_also_maps_to_omp_turn_target() -> None:
    from tendwire.backends.herdr_cli import _turn_target_from_item

    item = {
        "agent": "omp",
        "pane_id": "wX:p1",
        "agent_session": {"agent": "omp", "kind": "id", "value": "019f-omp-session"},
    }
    assert _turn_target_from_item(item) == ("omp_session_path", "019f-omp-session")


def _write_omp_session(tmp_path, lines):
    with herdr_turns._OMP_SESSION_CACHE_LOCK:
        herdr_turns._OMP_SESSION_CACHE.clear()
        herdr_turns._OMP_SESSION_CACHE_LIVE_KEYS = None
        herdr_turns._OMP_SESSION_CACHE_BINDING_GENERATIONS.clear()
    root = tmp_path / "omp-sessions"
    session_dir = root / "-demoapp"
    session_dir.mkdir(parents=True)
    path = session_dir / "2026-07-05T00-00-00-000Z_session.jsonl"
    path.write_text("\n".join(json.dumps(line) for line in lines), encoding="utf-8")
    return root, path


def _write_valid_git_head(git_dir: Path) -> None:
    git_dir.mkdir(parents=True)
    (git_dir / "HEAD").write_text("ref: refs/heads/main\n", encoding="ascii")


def _mark_git_repository(path: Path) -> None:
    _write_valid_git_head(path / ".git")


def _omp_msg(entry_id, role, text, stop=None, attribution=None):
    message = {"role": role, "content": [{"type": "text", "text": text}]}
    if stop:
        message["stopReason"] = stop
    if attribution:
        message["attribution"] = attribution
    return {"type": "message", "id": entry_id, "message": message}


def _omp_read_msg(entry_id, path_value):
    return {
        "type": "message",
        "id": entry_id,
        "message": {
            "role": "assistant",
            "stopReason": "toolUse",
            "content": [
                {
                    "type": "toolCall",
                    "id": f"{entry_id}-tool",
                    "name": "read",
                    "arguments": {"path": path_value},
                }
            ],
        },
    }


def test_read_omp_session_open_then_complete_turn(tmp_path, monkeypatch) -> None:
    root, path = _write_omp_session(
        tmp_path,
        [
            {"type": "session", "id": "s1"},
            _omp_msg("u1", "user", "please fix the bug", attribution="user"),
            _omp_msg("a1", "assistant", "looking at the code", stop="toolUse"),
        ],
    )
    monkeypatch.setenv("OMP_SESSIONS_DIR", str(root))
    content = herdr_turns._read_omp_session_turn(str(path))
    assert content["user_text"] == "please fix the bug"
    assert content["assistant_stream_text"] == "looking at the code"
    assert content["complete"] is False
    assert content["has_open_turn"] is True
    assert content["source_turn_id"] == "u1"

    # Same turn completes: same source id, final text, stream cleared.
    path.write_text(
        path.read_text(encoding="utf-8")
        + "\n"
        + json.dumps(_omp_msg("a2", "assistant", "fixed and pushed", stop="stop")),
        encoding="utf-8",
    )
    done = herdr_turns._read_omp_session_turn(str(path))
    assert done["source_turn_id"] == "u1"
    assert done["assistant_final_text"] == "fixed and pushed"
    assert done["complete"] is True
    assert done["assistant_stream_text"] is None


def test_read_omp_session_rejects_paths_outside_root(tmp_path, monkeypatch) -> None:
    root, path = _write_omp_session(tmp_path, [_omp_msg("u1", "user", "hi", attribution="user")])
    monkeypatch.setenv("OMP_SESSIONS_DIR", str(tmp_path / "elsewhere"))
    assert herdr_turns._read_omp_session_turn(str(path)) is None


def test_omp_agent_session_path_maps_to_omp_turn_target() -> None:
    from tendwire.backends.herdr_cli import _turn_target_from_item

    item = {
        "agent": "omp",
        "pane_id": "wX:p1",
        "agent_session": {"agent": "omp", "kind": "path", "value": "/home/user/.omp/agent/sessions/-x/a.jsonl"},
    }
    assert _turn_target_from_item(item) == ("omp_session_path", "/home/user/.omp/agent/sessions/-x/a.jsonl")


def test_omp_open_turn_streams_thinking_headlines(tmp_path, monkeypatch) -> None:
    def thinking(entry_id, text):
        return {"type": "message", "id": entry_id, "message": {"role": "assistant", "stopReason": "toolUse", "content": [{"type": "thinking", "thinking": text}, {"type": "toolCall", "id": "c1", "name": "bash"}]}}

    root, path = _write_omp_session(
        tmp_path,
        [
            _omp_msg("u1", "user", "add the feature", attribution="user"),
            thinking("a1", "**Reading the goal doc**\n\nlong reasoning body..."),
            thinking("a2", "checking the branch state first\nmore detail"),
        ],
    )
    monkeypatch.setenv("OMP_SESSIONS_DIR", str(root))
    content = herdr_turns._read_omp_session_turn(str(path))
    assert content["has_open_turn"] is True
    assert content["assistant_stream_text"] == (
        "Reading the goal doc\n\n"
        "step 1 · run command\n\n"
        "checking the branch state first\n\n"
        "step 2 · run command"
    )


def test_omp_cold_start_scans_back_until_current_user_prompt(tmp_path, monkeypatch) -> None:
    root, path = _write_omp_session(
        tmp_path,
        [
            _omp_msg("u1", "user", "large turn please", attribution="user"),
            _omp_msg("a1", "assistant", "x" * 512, stop="toolUse"),
            _omp_msg("a2", "assistant", "finished large turn", stop="stop"),
        ],
    )
    monkeypatch.setenv("OMP_SESSIONS_DIR", str(root))
    monkeypatch.setattr(herdr_turns, "_OMP_TAIL_BYTES", 80)

    content = herdr_turns._read_omp_session_turn(str(path))

    assert content["source_turn_id"] == "u1"
    assert content["user_text"] == "large turn please"
    assert content["assistant_final_text"] == "finished large turn"
    assert content["complete"] is True


def test_omp_cold_start_ignores_internal_user_lines_when_finding_prompt(tmp_path, monkeypatch) -> None:
    root, path = _write_omp_session(
        tmp_path,
        [
            _omp_msg("u1", "user", "real prompt", attribution="user"),
            _omp_msg("a1", "assistant", "x" * 512, stop="toolUse"),
            _omp_msg("internal", "user", "<environment_context>\nignore me", attribution="user"),
            _omp_msg("a2", "assistant", "still answering real prompt", stop="toolUse"),
        ],
    )
    monkeypatch.setenv("OMP_SESSIONS_DIR", str(root))
    monkeypatch.setattr(herdr_turns, "_OMP_TAIL_BYTES", 80)

    content = herdr_turns._read_omp_session_turn(str(path))

    assert content["source_turn_id"] == "u1"
    assert content["user_text"] == "real prompt"
    assert "still answering real prompt" in content["assistant_stream_text"]
    assert "<environment_context>" not in content["assistant_stream_text"]


def test_omp_incremental_cache_keeps_turn_state_when_prompt_leaves_tail(tmp_path, monkeypatch) -> None:
    root, path = _write_omp_session(
        tmp_path,
        [
            _omp_msg("u1", "user", "keep streaming this", attribution="user"),
            _omp_msg("a1", "assistant", "started", stop="toolUse"),
        ],
    )
    monkeypatch.setenv("OMP_SESSIONS_DIR", str(root))
    monkeypatch.setattr(herdr_turns, "_OMP_TAIL_BYTES", 80)

    first = herdr_turns._read_omp_session_turn(str(path))
    assert first["source_turn_id"] == "u1"
    assert first["assistant_stream_text"] == "started"

    def fail_cold_start(*_args):
        raise AssertionError("incremental read should not cold-scan a cached growing file")

    monkeypatch.setattr(herdr_turns, "_read_omp_state_from_recent", fail_cold_start)
    path.write_text(
        path.read_text(encoding="utf-8")
        + "\n"
        + json.dumps(
            {
                "type": "message",
                "id": "a2",
                "message": {
                    "role": "assistant",
                    "stopReason": "toolUse",
                    "content": [
                        {"type": "thinking", "thinking": "**Still working**\n\n" + ("x" * 512)},
                        {"type": "toolCall", "id": "c1", "name": "bash", "arguments": {"command": "git status"}},
                    ],
                },
            }
        ),
        encoding="utf-8",
    )

    second = herdr_turns._read_omp_session_turn(str(path))

    assert second["source_turn_id"] == "u1"
    assert second["user_text"] == "keep streaming this"
    assert second["complete"] is False
    assert "Still working" in second["assistant_stream_text"]
    assert "step 1 · git status" in second["assistant_stream_text"]


def test_isolated_omp_response_bound_tracks_valid_growth_during_child_read(
    tmp_path: Path,
    monkeypatch,
) -> None:
    root, path = _write_omp_session(
        tmp_path,
        [_omp_msg("growing-user", "user", "wait for large final", attribution="user")],
    )
    monkeypatch.setenv("OMP_SESSIONS_DIR", str(root))
    monkeypatch.setattr(herdr_turns, "_file_turn_child", _growing_isolated_turn_child)

    content = herdr_turns._read_file_turn_isolated(
        "omp_session_path",
        str(path),
        timeout_seconds=5,
    )

    assert content["source_turn_id"] == "growing-user"
    assert content["assistant_final_text"].startswith("grew-during-read-")
    assert len(content["assistant_final_text"]) > 2 * 1024 * 1024
    assert content["complete"] is True


def test_isolated_omp_authoritative_final_over_64mib_streams_losslessly(
    tmp_path: Path,
    monkeypatch,
) -> None:
    large_final = "lossless-omp-" + (
        "z" * (herdr_turns._CODEX_POLL_MAX_BYTES + 257)
    )
    root, path = _write_omp_session(
        tmp_path,
        [
            _omp_msg("over-limit-user", "user", "retain exact OMP final", attribution="user"),
            _omp_msg("over-limit-final", "assistant", large_final, stop="stop"),
        ],
    )
    monkeypatch.setenv("OMP_SESSIONS_DIR", str(root))
    before_children = {child.pid for child in multiprocessing.active_children()}
    before_threads = {
        thread.ident
        for thread in threading.enumerate()
        if thread.name.startswith("tendwire-turn")
    }

    content = herdr_turns._read_file_turn_isolated(
        "omp_session_path",
        str(path),
        timeout_seconds=60,
    )

    assert content["assistant_final_text"] == large_final
    assert content["complete"] is True
    assert {child.pid for child in multiprocessing.active_children()} == before_children
    assert {
        thread.ident
        for thread in threading.enumerate()
        if thread.name.startswith("tendwire-turn")
    } == before_threads


def test_isolated_omp_large_final_unchanged_fast_path_and_appended_turn(
    tmp_path: Path,
    monkeypatch,
) -> None:
    large_final = "canonical-" + ("x" * (6 * 1024 * 1024))
    root, path = _write_omp_session(
        tmp_path,
        [
            _omp_msg("six-mib-user", "user", "first private prompt", attribution="user"),
            _omp_msg("six-mib-final", "assistant", large_final, stop="stop"),
        ],
    )
    monkeypatch.setenv("OMP_SESSIONS_DIR", str(root))
    byte_reads: list[int] = []
    monkeypatch.setattr(herdr_turns, "_OMP_ISOLATED_READ_OBSERVER", byte_reads.append)

    first = herdr_turns._read_file_turn_isolated(
        "omp_session_path",
        str(path),
        timeout_seconds=10,
    )
    assert first["assistant_final_text"] == large_final
    cache_key = herdr_turns._omp_cache_key(str(path))
    assert cache_key is not None
    with herdr_turns._OMP_SESSION_CACHE_LOCK:
        checkpoint = herdr_turns._serialize_omp_state(
            herdr_turns._OMP_SESSION_CACHE[cache_key]
        )
    checkpoint_json = json.dumps(checkpoint, separators=(",", ":"))
    assert len(checkpoint_json.encode("utf-8")) < 1024
    assert "canonical-" not in checkpoint_json
    assert set(checkpoint) == {
        "offset",
        "observed_size",
        "file_id",
        "mtime_ns",
        "ctime_ns",
        "replay_offset",
        "turn_open",
        "project_root",
    }
    assert checkpoint["turn_open"] is False

    original_get_context = herdr_turns.multiprocessing.get_context

    def fail_if_spawned(*_args, **_kwargs):
        raise AssertionError("unchanged OMP poll spawned a child")

    monkeypatch.setattr(herdr_turns.multiprocessing, "get_context", fail_if_spawned)
    second = herdr_turns._read_file_turn_isolated(
        "omp_session_path",
        str(path),
        timeout_seconds=1,
    )
    monkeypatch.setattr(herdr_turns.multiprocessing, "get_context", original_get_context)
    assert second is herdr_turns._UNCHANGED_TURN
    assert byte_reads[-1] == 0
    with herdr_turns._OMP_SESSION_CACHE_LOCK:
        assert (
            herdr_turns._serialize_omp_state(herdr_turns._OMP_SESSION_CACHE[cache_key])
            == checkpoint
        )

    appended = "\n".join(
        [
            "",
            json.dumps(
                _omp_msg("next-user", "user", "new appended prompt", attribution="user"),
                separators=(",", ":"),
            ),
            json.dumps(
                _omp_msg("next-final", "assistant", "new answer", stop="stop"),
                separators=(",", ":"),
            ),
        ]
    )
    with open(path, "a", encoding="utf-8") as handle:
        handle.write(appended)
    third = herdr_turns._read_file_turn_isolated(
        "omp_session_path",
        str(path),
        timeout_seconds=5,
    )
    assert third["source_turn_id"] == "next-user"
    assert third["user_text"] == "new appended prompt"
    assert third["assistant_final_text"] == "new answer"
    assert large_final not in json.dumps(third)
    assert byte_reads[-1] == len(appended.encode("utf-8"))


def test_isolated_omp_cache_validates_identity_and_retains_good_state_on_failures(
    tmp_path: Path,
    monkeypatch,
) -> None:
    root, path = _write_omp_session(
        tmp_path,
        [_omp_msg("original-user", "user", "original prompt", attribution="user")],
    )
    monkeypatch.setenv("OMP_SESSIONS_DIR", str(root))
    original_child = herdr_turns._file_turn_child

    first = herdr_turns._read_file_turn_isolated(
        "omp_session_path",
        str(path),
        timeout_seconds=2,
    )
    cache_key = herdr_turns._omp_cache_key(str(path))
    assert cache_key is not None

    def cached_state():
        with herdr_turns._OMP_SESSION_CACHE_LOCK:
            return herdr_turns._serialize_omp_state(herdr_turns._OMP_SESSION_CACHE[cache_key])

    first_state = cached_state()
    assert first["source_turn_id"] == "original-user"

    for child, expected_error, timeout_seconds in (
        (_blocked_isolated_turn_child, herdr_turns._TurnReadTimeout, 0.1),
        (_failed_isolated_turn_child, herdr_turns._TurnReadFailed, 5),
        (_invalid_isolated_turn_child, herdr_turns._TurnReadFailed, 5),
    ):
        with open(path, "a", encoding="utf-8") as handle:
            handle.write(
                "\n"
                + json.dumps(
                    {"type": "ignored", "failure_attempt": child.__name__},
                    separators=(",", ":"),
                )
            )
        monkeypatch.setattr(herdr_turns, "_file_turn_child", child)
        with pytest.raises(expected_error):
            herdr_turns._read_file_turn_isolated(
                "omp_session_path",
                str(path),
                timeout_seconds=timeout_seconds,
            )
        assert cached_state() == first_state
    monkeypatch.setattr(herdr_turns, "_file_turn_child", original_child)

    replacement = path.with_name("replacement.jsonl")
    replacement.write_text(
        json.dumps(_omp_msg("replacement-user", "user", "replacement prompt", attribution="user")),
        encoding="utf-8",
    )
    replacement.replace(path)
    replaced = herdr_turns._read_file_turn_isolated(
        "omp_session_path",
        str(path),
        timeout_seconds=2,
    )
    replaced_state = cached_state()
    assert replaced["source_turn_id"] == "replacement-user"
    assert replaced_state["file_id"] != first_state["file_id"]

    replaced_inode = path.stat().st_ino
    path.write_text(
        json.dumps(_omp_msg("truncated-user", "user", "short", attribution="user")),
        encoding="utf-8",
    )
    assert path.stat().st_ino == replaced_inode
    truncated = herdr_turns._read_file_turn_isolated(
        "omp_session_path",
        str(path),
        timeout_seconds=2,
    )
    assert truncated["source_turn_id"] == "truncated-user"

    final_line = json.dumps(_omp_msg("final-answer", "assistant", "published", stop="stop"))
    with open(path, "a", encoding="utf-8") as handle:
        handle.write("\n" + final_line)
    final = herdr_turns._read_file_turn_isolated(
        "omp_session_path",
        str(path),
        timeout_seconds=2,
    )
    final_state = cached_state()
    assert final["source_turn_id"] == "truncated-user"
    assert final["assistant_final_text"] == "published"
    assert final_state["offset"] == path.stat().st_size


def test_omp_same_inode_same_size_rewrite_forces_cold_rescan(
    tmp_path: Path,
    monkeypatch,
) -> None:
    root, path = _write_omp_session(
        tmp_path,
        [
            _omp_msg("user-old", "user", "prompt-old", attribution="user"),
            _omp_msg("final-old", "assistant", "answer-old", stop="stop"),
        ],
    )
    monkeypatch.setenv("OMP_SESSIONS_DIR", str(root))
    first = herdr_turns._read_file_turn_isolated(
        "omp_session_path",
        str(path),
        timeout_seconds=2,
    )
    original_stat = path.stat()
    original_size = original_stat.st_size
    assert first["assistant_final_text"] == "answer-old"

    rewritten = "\n".join(
        json.dumps(line)
        for line in (
            _omp_msg("user-new", "user", "prompt-new", attribution="user"),
            _omp_msg("final-new", "assistant", "answer-new", stop="stop"),
        )
    )
    assert len(rewritten.encode("utf-8")) == original_size
    path.write_text(rewritten, encoding="utf-8")
    rewritten_stat = path.stat()
    assert rewritten_stat.st_ino == original_stat.st_ino
    assert rewritten_stat.st_size == original_size
    assert (
        rewritten_stat.st_mtime_ns != original_stat.st_mtime_ns
        or rewritten_stat.st_ctime_ns != original_stat.st_ctime_ns
    )

    second = herdr_turns._read_file_turn_isolated(
        "omp_session_path",
        str(path),
        timeout_seconds=2,
    )

    assert second["source_turn_id"] == "user-new"
    assert second["user_text"] == "prompt-new"
    assert second["assistant_final_text"] == "answer-new"


def test_omp_every_assistant_after_final_is_ignored_until_next_user(
    tmp_path: Path,
    monkeypatch,
) -> None:
    irrelevant = {
        "type": "message",
        "id": "metadata-only",
        "message": {
            "role": "assistant",
            "stopReason": "toolUse",
            "content": [{"type": "metadata", "value": "private bookkeeping"}],
        },
    }
    root, path = _write_omp_session(
        tmp_path,
        [
            _omp_msg("stable-user", "user", "stable prompt", attribution="user"),
            _omp_msg("stable-final", "assistant", "stable answer", stop="stop"),
            irrelevant,
            _omp_msg(
                "renderable-progress",
                "assistant",
                "must not reopen the completed turn",
                stop="toolUse",
            ),
        ],
    )
    monkeypatch.setenv("OMP_SESSIONS_DIR", str(root))

    content = herdr_turns._read_file_turn_isolated(
        "omp_session_path",
        str(path),
        timeout_seconds=2,
    )

    assert content["assistant_final_text"] == "stable answer"
    assert content["complete"] is True
    assert content["assistant_stream_text"] is None
    cache_key = herdr_turns._omp_cache_key(str(path))
    assert cache_key is not None
    with herdr_turns._OMP_SESSION_CACHE_LOCK:
        checkpoint = herdr_turns._OMP_SESSION_CACHE[cache_key]
        assert checkpoint.turn_open is False
        assert checkpoint.replay_offset == checkpoint.offset


def test_omp_appended_assistant_after_committed_final_advances_idle_checkpoint(
    tmp_path: Path,
    monkeypatch,
) -> None:
    root, path = _write_omp_session(
        tmp_path,
        [
            _omp_msg("done-user", "user", "finish once", attribution="user"),
            _omp_msg("done-final", "assistant", "finished once", stop="stop"),
        ],
    )
    monkeypatch.setenv("OMP_SESSIONS_DIR", str(root))
    first = herdr_turns._read_file_turn_isolated(
        "omp_session_path",
        str(path),
        timeout_seconds=2,
    )
    assert first["assistant_final_text"] == "finished once"

    appended = "\n" + json.dumps(
        _omp_msg(
            "late-progress",
            "assistant",
            "late renderable progress",
            stop="toolUse",
        ),
        separators=(",", ":"),
    )
    with open(path, "a", encoding="utf-8") as handle:
        handle.write(appended)
    second = herdr_turns._read_file_turn_isolated(
        "omp_session_path",
        str(path),
        timeout_seconds=2,
    )

    assert second is None
    cache_key = herdr_turns._omp_cache_key(str(path))
    assert cache_key is not None
    with herdr_turns._OMP_SESSION_CACHE_LOCK:
        checkpoint = herdr_turns._OMP_SESSION_CACHE[cache_key]
        assert checkpoint.turn_open is False
        assert checkpoint.offset == path.stat().st_size
        assert checkpoint.replay_offset == checkpoint.offset
    assert (
        herdr_turns._read_file_turn_isolated(
            "omp_session_path",
            str(path),
            timeout_seconds=1,
        )
        is herdr_turns._UNCHANGED_TURN
    )


def test_omp_retries_atomic_replacement_that_occurs_during_read(
    tmp_path: Path,
    monkeypatch,
) -> None:
    root, path = _write_omp_session(
        tmp_path,
        [
            _omp_msg("old-user", "user", "old prompt", attribution="user"),
            _omp_msg("old-final", "assistant", "stale answer", stop="stop"),
        ],
    )
    replacement = path.with_name("during-read-replacement.jsonl")
    replacement.write_text(
        "\n".join(
            json.dumps(line, separators=(",", ":"))
            for line in (
                _omp_msg("new-user", "user", "new prompt", attribution="user"),
                _omp_msg("new-final", "assistant", "current answer", stop="stop"),
            )
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("OMP_SESSIONS_DIR", str(root))
    original_open = open
    replaced = False

    class ReplacingReader:
        def __init__(self, handle):
            self.handle = handle

        def __enter__(self):
            self.handle.__enter__()
            return self

        def __exit__(self, *args):
            return self.handle.__exit__(*args)

        def fileno(self):
            return self.handle.fileno()

        def seek(self, *args):
            return self.handle.seek(*args)

        def read(self, *args):
            nonlocal replaced
            if not replaced:
                replaced = True
                replacement.replace(path)
            return self.handle.read(*args)

    def racing_open(file, mode="r", *args, **kwargs):
        handle = original_open(file, mode, *args, **kwargs)
        if Path(file) == path and mode == "rb" and not replaced:
            return ReplacingReader(handle)
        return handle

    monkeypatch.setattr("builtins.open", racing_open)
    content = herdr_turns._read_omp_session_turn(str(path))

    assert replaced is True
    assert content["source_turn_id"] == "new-user"
    assert content["assistant_final_text"] == "current answer"
    assert "stale answer" not in json.dumps(content)


def test_omp_concurrent_cache_loser_cannot_overwrite_accepted_checkpoint() -> None:
    cache_key = "concurrent-publication"
    prior = herdr_turns._OmpSessionState(
        offset=100,
        observed_size=100,
        file_id=(7, 11),
    )
    accepted = herdr_turns._OmpSessionState(
        offset=300,
        observed_size=300,
        file_id=(7, 11),
    )
    losing = herdr_turns._OmpSessionState(
        offset=300,
        observed_size=300,
        file_id=(7, 11),
        turn_open=True,
    )
    with herdr_turns._OMP_SESSION_CACHE_LOCK:
        herdr_turns._OMP_SESSION_CACHE_LIVE_KEYS = None
        herdr_turns._OMP_SESSION_CACHE[cache_key] = accepted

    returned = herdr_turns._publish_omp_cache_state(
        cache_key,
        herdr_turns._serialize_omp_state(prior),
        losing,
        {"assistant_final_text": "losing duplicate"},
    )

    assert returned is None
    with herdr_turns._OMP_SESSION_CACHE_LOCK:
        assert herdr_turns._OMP_SESSION_CACHE[cache_key] is accepted


def test_omp_cache_lru_capacity_moves_hits_and_evicts_oldest(monkeypatch) -> None:
    monkeypatch.setattr(herdr_turns, "_OMP_SESSION_CACHE_CAPACITY", 3)
    with herdr_turns._OMP_SESSION_CACHE_LOCK:
        herdr_turns._OMP_SESSION_CACHE.clear()
        herdr_turns._OMP_SESSION_CACHE_LIVE_KEYS = None
    for index in range(3):
        state = herdr_turns._OmpSessionState(
            offset=index + 1,
            observed_size=index + 1,
            file_id=(1, index + 1),
        )
        herdr_turns._publish_omp_cache_state(
            f"key-{index}",
            None,
            state,
            None,
        )
    with herdr_turns._OMP_SESSION_CACHE_LOCK:
        assert herdr_turns._omp_cache_get_locked("key-0") is not None

    newest = herdr_turns._OmpSessionState(
        offset=4,
        observed_size=4,
        file_id=(1, 4),
    )
    herdr_turns._publish_omp_cache_state(
        "key-3",
        None,
        newest,
        None,
    )

    with herdr_turns._OMP_SESSION_CACHE_LOCK:
        assert list(herdr_turns._OMP_SESSION_CACHE) == ["key-2", "key-0", "key-3"]
        assert len(herdr_turns._OMP_SESSION_CACHE) == 3


def test_omp_sixty_four_large_completed_sessions_keep_only_bounded_coordinates(
    tmp_path: Path,
    monkeypatch,
) -> None:
    root = tmp_path / "omp-sessions"
    session_dir = root / "-many"
    session_dir.mkdir(parents=True)
    monkeypatch.setenv("OMP_SESSIONS_DIR", str(root))
    with herdr_turns._OMP_SESSION_CACHE_LOCK:
        herdr_turns._OMP_SESSION_CACHE.clear()
        herdr_turns._OMP_SESSION_CACHE_LIVE_KEYS = None

    for index in range(64):
        large_final = f"large-final-{index}-" + ("z" * (128 * 1024))
        path = session_dir / f"{index:02d}.jsonl"
        path.write_text(
            "\n".join(
                json.dumps(line, separators=(",", ":"))
                for line in (
                    _omp_msg(f"user-{index}", "user", f"prompt-{index}", attribution="user"),
                    _omp_msg(f"final-{index}", "assistant", large_final, stop="stop"),
                )
            ),
            encoding="utf-8",
        )
        content = herdr_turns._read_omp_session_turn(str(path))
        assert content["assistant_final_text"] == large_final

    with herdr_turns._OMP_SESSION_CACHE_LOCK:
        serialized = json.dumps(
            {
                key: herdr_turns._serialize_omp_state(state)
                for key, state in herdr_turns._OMP_SESSION_CACHE.items()
            },
            separators=(",", ":"),
        )
        assert len(herdr_turns._OMP_SESSION_CACHE) == 64
        assert herdr_turns._omp_cache_weight_locked() <= herdr_turns._OMP_SESSION_CACHE_MAX_BYTES
        assert all(not state.turn_open for state in herdr_turns._OMP_SESSION_CACHE.values())
    assert "large-final-" not in serialized
    assert "z" * 1024 not in serialized


def test_omp_cache_enforces_serialized_byte_bound(monkeypatch) -> None:
    monkeypatch.setattr(herdr_turns, "_OMP_SESSION_CACHE_CAPACITY", 64)
    monkeypatch.setattr(herdr_turns, "_OMP_SESSION_CACHE_MAX_BYTES", 600)
    with herdr_turns._OMP_SESSION_CACHE_LOCK:
        herdr_turns._OMP_SESSION_CACHE.clear()
        herdr_turns._OMP_SESSION_CACHE_LIVE_KEYS = None
        for index in range(20):
            herdr_turns._omp_cache_store_locked(
                f"long-cache-key-{index}-" + ("k" * 40),
                herdr_turns._OmpSessionState(
                    offset=index,
                    observed_size=index,
                    file_id=(1, index),
                ),
            )
        assert len(herdr_turns._OMP_SESSION_CACHE) < 20
        assert herdr_turns._omp_cache_weight_locked() <= 600


def test_omp_cache_prunes_disappeared_bindings_and_keeps_live_binding(
    tmp_path: Path,
    monkeypatch,
) -> None:
    root, live_path = _write_omp_session(
        tmp_path,
        [_omp_msg("live", "user", "live", attribution="user")],
    )
    stale_path = live_path.with_name("stale.jsonl")
    stale_path.write_text(
        json.dumps(_omp_msg("stale", "user", "stale", attribution="user")),
        encoding="utf-8",
    )
    monkeypatch.setenv("OMP_SESSIONS_DIR", str(root))
    live_key = herdr_turns._omp_cache_key(str(live_path))
    stale_key = herdr_turns._omp_cache_key(str(stale_path))
    assert live_key is not None
    assert stale_key is not None
    live_state = herdr_turns._OmpSessionState(
        offset=1,
        observed_size=1,
        file_id=(1, 1),
    )
    with herdr_turns._OMP_SESSION_CACHE_LOCK:
        herdr_turns._omp_cache_store_locked(live_key, live_state)
        herdr_turns._omp_cache_store_locked(
            stale_key,
            herdr_turns._OmpSessionState(offset=1, file_id=(1, 2)),
        )
    live_binding = WorkerBinding(
        host_id="host",
        worker_id="worker",
        worker_fingerprint="worker-fingerprint",
        backend="herdr",
        target_kind="agent_id",
        target_value="agent",
        turn_target_kind="omp_session_path",
        turn_target_value=str(live_path),
        sendable=True,
        observed_at="2026-07-12T00:00:00+00:00",
        expires_at="9999-12-31T23:59:59+00:00",
        private_fingerprint="private",
    )

    herdr_turns._prune_omp_cache_for_bindings([live_binding])
    with herdr_turns._OMP_SESSION_CACHE_LOCK:
        assert list(herdr_turns._OMP_SESSION_CACHE) == [live_key]
        retained_prior = herdr_turns._serialize_omp_state(
            herdr_turns._OMP_SESSION_CACHE[live_key]
        )
        retained_generation = herdr_turns._omp_cache_binding_generation_locked(live_key)

    live_path.unlink()
    herdr_turns._prune_omp_cache_for_bindings([live_binding])
    with herdr_turns._OMP_SESSION_CACHE_LOCK:
        assert list(herdr_turns._OMP_SESSION_CACHE) == [live_key]
        assert herdr_turns._omp_cache_binding_generation_locked(live_key) == retained_generation

    herdr_turns._prune_omp_cache_for_bindings([])
    with herdr_turns._OMP_SESSION_CACHE_LOCK:
        assert not herdr_turns._OMP_SESSION_CACHE

    herdr_turns._prune_omp_cache_for_bindings([live_binding])
    with herdr_turns._OMP_SESSION_CACHE_LOCK:
        assert (
            herdr_turns._omp_cache_binding_generation_locked(live_key)
            != retained_generation
        )

    resurrected = herdr_turns._OmpSessionState(
        offset=2,
        observed_size=2,
        file_id=(1, 1),
    )
    returned = herdr_turns._publish_omp_cache_state(
        live_key,
        retained_prior,
        resurrected,
        None,
        retained_generation,
    )
    assert returned is None
    with herdr_turns._OMP_SESSION_CACHE_LOCK:
        assert live_key not in herdr_turns._OMP_SESSION_CACHE


def test_omp_tool_progress_uses_only_allowlisted_structured_summaries(tmp_path, monkeypatch) -> None:
    project = tmp_path / "project"
    project.mkdir()
    _mark_git_repository(project)
    (project / "README.md").write_text("public", encoding="utf-8")
    private_key_path = "/home/alice/.ssh/id_ed25519"
    herdr_socket_path = "/run/user/1000/herdr/private.sock"
    credential_url = "https://alice:password@internal.example/private"
    provider_key = "sk-" + "proj-" + "PUBLICSAFETY1234567890"
    raw_tool_id = "toolu_PUBLICSAFETYTOOL01"
    tool_message = {
        "type": "message",
        "id": "a1",
        "message": {
            "role": "assistant",
            "stopReason": "toolUse",
            "content": [
                {
                    "type": "toolCall",
                    "id": raw_tool_id,
                    "name": "bash",
                    "arguments": {
                        "command": f"cat {private_key_path}; connect {herdr_socket_path} {credential_url}",
                        "env": {"TOKEN": provider_key},
                    },
                },
                {
                    "type": "toolCall",
                    "id": "c2",
                    "toolName": "bash",
                    "input": {"command": "python -m pytest -q tests/test_turns.py"},
                },
                {
                    "type": "toolCall",
                    "id": "c3",
                    "tool": "read",
                    "args": {"path": "README.md", "stdout": private_key_path},
                },
                {
                    "type": "toolCall",
                    "id": "c4",
                    "name": raw_tool_id,
                    "arguments": {
                        "nested": [private_key_path, herdr_socket_path, provider_key],
                        "url": credential_url,
                    },
                },
            ],
        },
    }
    root, path = _write_omp_session(
        tmp_path,
        [
            {"type": "session", "id": "private-session", "cwd": str(project)},
            _omp_msg("u1", "user", "show safe tool progress", attribution="user"),
            tool_message,
        ],
    )
    monkeypatch.setenv("OMP_SESSIONS_DIR", str(root))

    content = herdr_turns._read_omp_session_turn(str(path))
    stream = content["assistant_stream_text"]

    assert stream.split("\n\n") == [
        "step 1 · run command",
        "step 2 · test: pytest",
        "step 3 · read: README.md",
        "step 4 · tool",
    ]
    for private_value in (
        private_key_path,
        herdr_socket_path,
        credential_url,
        provider_key,
        raw_tool_id,
    ):
        assert private_value not in stream


def test_omp_shell_progress_uses_a_small_constant_allowlist() -> None:
    cases = [
        ("git status --short", "step 1 · git status"),
        ("pytest -q tests/test_turns.py", "step 1 · test: pytest"),
        ("uv run pytest tests/test_turns.py", "step 1 · test: pytest"),
        ("cargo test --workspace", "step 1 · test: cargo"),
        ("npm run build", "step 1 · build: npm"),
        ("make all", "step 1 · build: make"),
        ("git checkout -b private-branch", "step 1 · run command"),
        ("echo arbitrary private text", "step 1 · run command"),
    ]

    for command, expected in cases:
        item = {"name": "bash", "arguments": {"command": command}}
        assert herdr_turns._omp_tool_snippet(item, 1) == expected


def test_omp_file_progress_requires_repository_root_proof(tmp_path: Path) -> None:
    project = tmp_path / "repo"
    docs = project / "docs"
    docs.mkdir(parents=True)
    _mark_git_repository(project)
    readme = project / "README.md"
    guide = docs / "guide.md"
    outside = tmp_path / "outside.txt"
    readme.write_text("readme", encoding="utf-8")
    guide.write_text("guide", encoding="utf-8")
    outside.write_text("private", encoding="utf-8")
    escape = project / "escape"
    escape.symlink_to(outside)

    def snippet(path_value: str, root: Path | None = project) -> str:
        return herdr_turns._omp_tool_snippet(
            {"name": "read", "arguments": {"path": path_value}},
            1,
            root,
        )

    assert snippet("README.md") == "step 1 · read: README.md"
    assert snippet(str(guide)) == "step 1 · read: docs/guide.md"
    assert snippet("README.md", None) == "step 1 · read file"
    assert snippet(str(outside)) == "step 1 · read file"
    assert snippet("../outside.txt") == "step 1 · read file"
    assert snippet(str(escape)) == "step 1 · read file"
    assert snippet("~/.ssh/id_ed25519") == "step 1 · read file"
    assert snippet("docs/../README.md") == "step 1 · read file"
    assert snippet(".env") == "step 1 · read file"
    assert snippet(".git/config") == "step 1 · read file"
    assert snippet("secrets/key.txt") == "step 1 · read file"
    assert snippet("credentials.json") == "step 1 · read file"


def test_omp_file_progress_rejects_unproven_session_cwds(tmp_path: Path, monkeypatch) -> None:
    home = tmp_path / "home" / "alice"
    home.mkdir(parents=True)
    (home / ".bashrc").write_text("operator shell settings", encoding="utf-8")
    notes = tmp_path / "operator-work"
    notes.mkdir()
    (notes / "operator-notes.txt").write_text("private operator notes", encoding="utf-8")

    cases = (
        ("filesystem-root", Path("/"), "/etc/passwd"),
        ("home", home, ".bashrc"),
        ("notes", notes, "operator-notes.txt"),
    )
    for label, cwd, path_value in cases:
        root, session_path = _write_omp_session(
            tmp_path / label,
            [
                {"type": "session", "id": label, "cwd": str(cwd)},
                _omp_msg(f"{label}-user", "user", "inspect a file", attribution="user"),
                _omp_read_msg(f"{label}-assistant", path_value),
            ],
        )
        monkeypatch.setenv("OMP_SESSIONS_DIR", str(root))

        content = herdr_turns._read_omp_session_turn(str(session_path))

        assert content is not None
        assert content["assistant_stream_text"] == "step 1 · read file"


def test_omp_file_progress_finds_repository_above_session_cwd(
    tmp_path: Path,
    monkeypatch,
) -> None:
    project = tmp_path / "repo-with-subdir"
    docs = project / "docs"
    docs.mkdir(parents=True)
    _mark_git_repository(project)
    guide = docs / "guide.md"
    guide.write_text("public", encoding="utf-8")
    root, session_path = _write_omp_session(
        tmp_path / "subdir-session",
        [
            {"type": "session", "id": "subdir", "cwd": str(docs)},
            _omp_msg("subdir-user", "user", "inspect the guide", attribution="user"),
            _omp_read_msg("subdir-assistant", str(guide)),
        ],
    )
    monkeypatch.setenv("OMP_SESSIONS_DIR", str(root))

    content = herdr_turns._read_omp_session_turn(str(session_path))

    assert content is not None
    assert content["assistant_stream_text"] == "step 1 · read: docs/guide.md"


def test_omp_file_progress_accepts_worktree_gitdir_file(tmp_path: Path, monkeypatch) -> None:
    checkout = tmp_path / "checkout"
    checkout.mkdir()
    (checkout / "README.md").write_text("public", encoding="utf-8")
    worktree_git_dir = tmp_path / "main" / ".git" / "worktrees" / "checkout"
    _write_valid_git_head(worktree_git_dir)
    (checkout / ".git").write_text(
        f"gitdir: {worktree_git_dir}\n",
        encoding="utf-8",
    )
    root, session_path = _write_omp_session(
        tmp_path / "worktree-session",
        [
            {"type": "session", "id": "worktree", "cwd": str(checkout)},
            _omp_msg("worktree-user", "user", "inspect the readme", attribution="user"),
            _omp_read_msg("worktree-assistant", "README.md"),
        ],
    )
    monkeypatch.setenv("OMP_SESSIONS_DIR", str(root))

    content = herdr_turns._read_omp_session_turn(str(session_path))

    assert content is not None
    assert content["assistant_stream_text"] == "step 1 · read: README.md"


def test_omp_file_progress_rejects_invalid_worktree_gitdir_file(
    tmp_path: Path,
    monkeypatch,
) -> None:
    checkout = tmp_path / "invalid-checkout"
    checkout.mkdir()
    (checkout / "README.md").write_text("public", encoding="utf-8")
    (checkout / ".git").write_text("gitdir: ../missing-git-dir\n", encoding="utf-8")
    root, session_path = _write_omp_session(
        tmp_path / "invalid-worktree-session",
        [
            {"type": "session", "id": "invalid-worktree", "cwd": str(checkout)},
            _omp_msg("invalid-user", "user", "inspect the readme", attribution="user"),
            _omp_read_msg("invalid-assistant", "README.md"),
        ],
    )
    monkeypatch.setenv("OMP_SESSIONS_DIR", str(root))

    content = herdr_turns._read_omp_session_turn(str(session_path))

    assert content is not None
    assert content["assistant_stream_text"] == "step 1 · read file"


def _prepare_pane_turn_store(
    tmp_path: Path,
    *,
    herdr_bin: str = "herdr",
) -> tuple[Config, Any]:
    db_path = tmp_path / "turns.db"
    config = Config(
        host_id="turn-host",
        db_path=db_path,
        herdr_bin=herdr_bin,
        herdr_timeout_seconds=10,
    )
    snapshot = project_from_raw(
        config,
        workers=[{"id": "worker-1", "name": "codex", "status": "active", "space_id": "space-1"}],
    )
    init_store(db_path)
    save_snapshot(db_path, snapshot)
    worker = snapshot.workers[0]
    upsert_worker_bindings(
        db_path,
        [
            WorkerBinding(
                host_id=config.host_id,
                worker_id=worker.id,
                worker_fingerprint=worker.fingerprint,
                backend="herdr",
                target_kind="agent_id",
                target_value="private-agent",
                turn_target_kind="pane_id",
                turn_target_value="private-pane",
                sendable=True,
                observed_at="2026-07-11T00:00:00+00:00",
                expires_at="9999-12-31T23:59:59+00:00",
                private_fingerprint="private-binding",
            )
        ],
    )
    return config, snapshot


def _pane_turn_payload(
    *,
    user_text: str | None,
    final_text: str | None,
    stream_text: str | None = None,
    complete: bool = True,
    source_turn_id: str = "source-turn-long",
) -> dict[str, Any]:
    return {
        "result": {
            "turn": {
                "available": True,
                "user_text": user_text,
                "assistant_final_text": final_text,
                "assistant_stream_text": stream_text,
                "complete": complete,
                "has_open_turn": not complete,
                "source_turn_id": source_turn_id,
            }
        }
    }


def _current_content_turn(
    config: Config,
    snapshot: Any,
) -> tuple[dict[str, Any], str]:
    payload = turns_payload_from_store(
        config.db_path,
        config.host_id,
        snapshot=snapshot,
        schema_version=2,
    )
    turn = next(
        item
        for item in payload["turns"]
        if (item.get("content") or {}).get("content_revision")
    )
    return turn, str(turn["content"]["content_revision"])


def _reconstruct_turn_field(
    config: Config,
    *,
    turn_id: str,
    revision: str,
    field: str,
) -> str:
    cursor: str | None = None
    pages: list[dict[str, Any]] = []
    while True:
        page = store_sqlite.get_turn_content(
            config.db_path,
            config.host_id,
            turn_id=turn_id,
            content_revision=revision,
            field=field,
            cursor=cursor,
            schema_version=1,
        )
        assert page["availability"] == "complete"
        assert page["index"] == len(pages)
        pages.append(page)
        cursor = page["next_cursor"]
        if cursor is None:
            break
    assert all(page["count"] == len(pages) for page in pages)
    assert all(
        len(str(page["text"]).encode("utf-8")) <= TURN_CONTENT_PAGE_MAX_UTF8_BYTES
        for page in pages
    )
    return "".join(str(page["text"]) for page in pages)


@pytest.mark.parametrize("content_size", [20_000, 1024 * 1024 + 257])
def test_wrapper_adapter_round_trips_unbounded_authoritative_content(
    tmp_path: Path,
    monkeypatch,
    content_size: int,
) -> None:
    adapter = tmp_path / "long_turn_adapter.py"
    adapter.write_text(
        r"""#!/usr/bin/env python3
import json
import os

size = int(os.environ["TENDWIRE_TEST_TURN_SIZE"])
prompt = "  Prompt ﬁ\n" + ("p" * size) + "\u200b\x00\n"
final = "\n# Final\n" + ("f" * size) + "\u200b\x00  "
print(json.dumps({"result": {"turn": {
    "available": True,
    "user_text": prompt,
    "assistant_final_text": final,
    "assistant_stream_text": None,
    "complete": True,
    "has_open_turn": False,
    "source_turn_id": "source-turn-long",
}}}))
""",
        encoding="utf-8",
    )
    adapter.chmod(0o700)
    monkeypatch.setenv("TENDWIRE_TEST_TURN_SIZE", str(content_size))
    config, snapshot = _prepare_pane_turn_store(tmp_path, herdr_bin=str(adapter))

    result = refresh_structured_turn_content(config)
    turn, revision = _current_content_turn(config, snapshot)
    expected_prompt = sanitize_canonical_turn_text(
        "  Prompt ﬁ\n" + ("p" * content_size) + "\u200b\x00\n"
    )
    expected_final = sanitize_canonical_turn_text(
        "\n# Final\n" + ("f" * content_size) + "\u200b\x00  "
    )
    serialized_source = json.dumps(
        _pane_turn_payload(
            user_text="  Prompt ﬁ\n" + ("p" * content_size) + "\u200b\x00\n",
            final_text="\n# Final\n" + ("f" * content_size) + "\u200b\x00  ",
        )
    ).encode("utf-8")
    assert len(serialized_source) > content_size * 2

    assert result == {"ok": True, "status": "ok", "updated": 1, "attempted": 1}
    assert expected_prompt is not None
    assert expected_final is not None
    assert _reconstruct_turn_field(
        config,
        turn_id=turn["id"],
        revision=revision,
        field="user_text",
    ) == expected_prompt
    assert _reconstruct_turn_field(
        config,
        turn_id=turn["id"],
        revision=revision,
        field="assistant_final_text",
    ) == expected_final


def test_refresh_keeps_only_a_rolling_bounded_stream(
    tmp_path: Path,
    monkeypatch,
) -> None:
    config, snapshot = _prepare_pane_turn_store(tmp_path)
    stream = "".join(str(index % 10) for index in range(TURN_STREAM_TEXT_MAX_CHARS + 137))
    monkeypatch.setattr(
        herdr_turns.subprocess,
        "run",
        _run_returning(
            _pane_turn_payload(
                user_text="stream this turn",
                final_text=None,
                stream_text=stream,
                complete=False,
            )
        ),
    )

    assert refresh_structured_turn_content(config)["updated"] == 1
    turn, _revision = _current_content_turn(config, snapshot)

    assert turn["assistant_stream_text"] == stream[-TURN_STREAM_TEXT_MAX_CHARS:]
    assert len(turn["assistant_stream_text"]) == TURN_STREAM_TEXT_MAX_CHARS


def test_empty_later_observation_does_not_erase_authoritative_final(
    tmp_path: Path,
    monkeypatch,
) -> None:
    config, snapshot = _prepare_pane_turn_store(tmp_path)
    final_text = "authoritative final"
    observations = iter(
        [
            _pane_turn_payload(user_text="prompt", final_text=final_text),
            _pane_turn_payload(user_text="", final_text=""),
        ]
    )
    monkeypatch.setattr(
        herdr_turns.subprocess,
        "run",
        lambda args, **kwargs: subprocess.CompletedProcess(
            args=args,
            returncode=0,
            stdout=json.dumps(next(observations)),
            stderr="",
        ),
    )

    assert refresh_structured_turn_content(config)["updated"] == 1
    first_turn, first_revision = _current_content_turn(config, snapshot)
    second_result = refresh_structured_turn_content(config)
    second_turn, second_revision = _current_content_turn(config, snapshot)

    assert second_result["updated"] == 0
    assert second_turn["id"] == first_turn["id"]
    assert second_revision == first_revision
    assert _reconstruct_turn_field(
        config,
        turn_id=second_turn["id"],
        revision=second_revision,
        field="assistant_final_text",
    ) == final_text


def test_identical_source_turn_observation_is_a_revision_noop(
    tmp_path: Path,
    monkeypatch,
) -> None:
    config, snapshot = _prepare_pane_turn_store(tmp_path)
    payload = _pane_turn_payload(user_text="same prompt", final_text="same final")
    monkeypatch.setattr(herdr_turns.subprocess, "run", _run_returning(payload))

    assert refresh_structured_turn_content(config)["updated"] == 1
    first_turn, first_revision = _current_content_turn(config, snapshot)
    with sqlite3.connect(config.db_path) as conn:
        first_count = conn.execute(
            "SELECT COUNT(*) FROM turn_content_revisions WHERE host_id = ? AND turn_id = ?",
            (config.host_id, first_turn["id"]),
        ).fetchone()[0]

    assert refresh_structured_turn_content(config)["updated"] == 0
    second_turn, second_revision = _current_content_turn(config, snapshot)
    with sqlite3.connect(config.db_path) as conn:
        second_count = conn.execute(
            "SELECT COUNT(*) FROM turn_content_revisions WHERE host_id = ? AND turn_id = ?",
            (config.host_id, second_turn["id"]),
        ).fetchone()[0]

    assert second_turn["id"] == first_turn["id"]
    assert second_revision == first_revision
    assert second_count == first_count


def test_complete_reobservation_recovers_known_incomplete_source_turn(
    tmp_path: Path,
    monkeypatch,
) -> None:
    config, snapshot = _prepare_pane_turn_store(tmp_path)
    fragment = ("legacy fragment " * 900)[:11_988] + "\n[truncated]"
    complete_final = fragment.removesuffix("\n[truncated]") + " recovered authoritative suffix"
    first_payload = _pane_turn_payload(user_text="recover this", final_text=fragment)
    complete_payload = _pane_turn_payload(user_text="recover this", final_text=complete_final)
    monkeypatch.setattr(herdr_turns.subprocess, "run", _run_returning(first_payload))

    assert refresh_structured_turn_content(config)["updated"] == 1
    turn, incomplete_revision = _current_content_turn(config, snapshot)
    with sqlite3.connect(config.db_path) as conn:
        conn.execute(
            """
            UPDATE turn_content_revisions
            SET final_state = 'known_incomplete'
            WHERE host_id = ? AND turn_id = ? AND content_revision = ?
            """,
            (config.host_id, turn["id"], incomplete_revision),
        )
        conn.commit()

    monkeypatch.setattr(herdr_turns.subprocess, "run", _run_returning(complete_payload))
    assert refresh_structured_turn_content(config)["updated"] == 1
    recovered_turn, recovered_revision = _current_content_turn(config, snapshot)
    with sqlite3.connect(config.db_path) as conn:
        revisions = conn.execute(
            """
            SELECT content_revision, final_state, is_current
            FROM turn_content_revisions
            WHERE host_id = ? AND turn_id = ?
            ORDER BY created_at, content_revision
            """,
            (config.host_id, turn["id"]),
        ).fetchall()

    assert recovered_turn["id"] == turn["id"]
    assert recovered_revision != incomplete_revision
    assert (incomplete_revision, "known_incomplete", 0) in revisions
    assert (recovered_revision, "complete", 1) in revisions
    assert _reconstruct_turn_field(
        config,
        turn_id=recovered_turn["id"],
        revision=recovered_revision,
        field="assistant_final_text",
    ) == complete_final


def test_completed_pane_refresh_uses_authoritative_binding_hint(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    config = Config(host_id="completion-route", db_path=tmp_path / "route.db")
    binding = WorkerBinding(
        host_id=config.host_id,
        worker_id="worker-1",
        worker_fingerprint="worker-fingerprint",
        backend="herdr",
        target_kind="agent_id",
        target_value="agent-private",
        turn_target_kind="codex_session_id",
        turn_target_value="session-private",
        sendable=True,
        observed_at="2026-07-23T00:00:00+00:00",
        expires_at="9999-12-31T23:59:59+00:00",
        private_fingerprint="binding-private",
    )
    monkeypatch.setattr(
        herdr_turns,
        "list_worker_bindings",
        lambda *_args, **_kwargs: [binding],
    )
    refreshed: list[WorkerBinding] = []
    monkeypatch.setattr(
        herdr_turns,
        "_refresh_turn_binding",
        lambda _config, current, **_kwargs: (
            refreshed.append((current, _kwargs.get("pane_target_override")))
            or herdr_turns.TurnRefreshResult("updated", 1)
        ),
    )
    monkeypatch.setattr(
        herdr_turns,
        "latest_turn_id_for_worker",
        lambda *_args, **_kwargs: "public-turn-1",
    )

    result = herdr_turns.refresh_completed_pane_turn(
        config,
        "w123456789abcde:pA",
        terminal_id="different-terminal",
        binding_private_fingerprint="binding-private",
    )

    assert refreshed == [(binding, "w123456789abcde:pA")]
    assert result == herdr_turns.CompletedPaneTurnRefreshResult(
        "updated",
        worker_id="worker-1",
        refreshed_turn_id="public-turn-1",
    )


def test_completed_pane_refresh_ignores_stale_session_generation_for_content_read(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    db_path = tmp_path / "generation-mismatch.db"
    config = Config(
        host_id="generation-mismatch",
        db_path=db_path,
        herdr_bin="turn-adapter",
        herdr_timeout_seconds=2,
    )
    snapshot = project_from_raw(
        config,
        workers=[
            {
                "id": "worker-generation-a",
                "name": "claude",
                "status": "idle",
                "space_id": "space-1",
            }
        ],
    )
    init_store(db_path)
    save_snapshot(db_path, snapshot)
    worker = snapshot.workers[0]
    stale_binding = WorkerBinding(
        host_id=config.host_id,
        worker_id=worker.id,
        worker_fingerprint=worker.fingerprint,
        backend="herdr",
        target_kind="terminal_id",
        target_value="terminal-stable",
        turn_target_kind="codex_session_id",
        turn_target_value="session-generation-a",
        sendable=True,
        observed_at="2026-07-23T00:00:00+00:00",
        expires_at="9999-12-31T23:59:59+00:00",
        private_fingerprint="binding-generation-a",
    )
    upsert_worker_bindings(db_path, [stale_binding])
    adapter_calls: list[list[str]] = []
    payload = _pane_turn_payload(
        user_text="reply with purple elephant 42",
        final_text="purple elephant 42",
        source_turn_id="generation-b-turn",
    )

    def fake_run(args: list[str], **_kwargs: Any) -> subprocess.CompletedProcess[str]:
        adapter_calls.append(args)
        return subprocess.CompletedProcess(
            args=args,
            returncode=0,
            stdout=json.dumps(payload),
            stderr="",
        )

    monkeypatch.setattr(herdr_turns.subprocess, "run", fake_run)
    monkeypatch.setattr(
        herdr_turns,
        "_read_file_turn_isolated",
        lambda *_args, **_kwargs: pytest.fail(
            "completion must not read the stale session generation"
        ),
    )

    result = herdr_turns.refresh_completed_pane_turn(
        config,
        "pane-generation-b",
        terminal_id="terminal-stable",
        binding_private_fingerprint="binding-generation-a",
    )
    payload_from_store = turns_payload_from_store(
        db_path,
        config.host_id,
        snapshot=snapshot,
    )

    assert result.status == "updated"
    assert result.worker_id == worker.id
    assert adapter_calls == [
        [
            "turn-adapter",
            "pane",
            "turn",
            "pane-generation-b",
            "--last",
            "--format",
            "json",
        ]
    ]
    captured = next(
        turn
        for turn in payload_from_store["turns"]
        if turn.get("assistant_final_text")
    )
    assert captured["status"] == "idle"
    assert captured["assistant_final_text"] == "purple elephant 42"
