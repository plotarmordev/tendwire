"""Tests for tendwire CLI snapshot JSON output and optional storage."""

from __future__ import annotations

import io
import json
import os
import sqlite3
import subprocess
import sys
import threading
import time
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from tendwire.backends import herdr_cli
from tendwire.cli import _build_parser, main, observe_public_snapshot
from tendwire.config import Config
from tendwire.core.models import AttentionSignal, Snapshot, SuggestedAction, Worker, WorkerBinding
from tendwire.core.projector import project_from_raw
from tendwire.daemon_api import TendwireDaemonAPI, UnixSocketJSONServer
from tendwire.store.sqlite import (
    SnapshotObservationContext,
    append_event,
    get_turn_content,
    init_store,
    latest_snapshot,
    list_worker_bindings,
    merge_backend_pending,
    pending_payload_from_store,
    merge_turn_content,
    save_snapshot,
    turns_payload_from_store,
)


@pytest.fixture(autouse=True)
def _isolate_cli_paths(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    private_home = tmp_path / "home"
    private_home.mkdir(mode=0o700)
    monkeypatch.setenv("HOME", str(private_home))
    monkeypatch.setenv("TENDWIRE_DATA_DIR", str(tmp_path / "tendwire-data"))
    monkeypatch.delenv("TENDWIRE_DB_PATH", raising=False)


_PUBLIC_JSON_FORBIDDEN_KEYS = {
    "tty",
    "pty",
    "pid",
    "pids",
    "process_id",
    "process_ids",
    "tmux",
    "tmux_session",
    "tmux_sessions",
    "screen_session",
    "screen_sessions",
    "window_id",
    "window_ids",
    "tab_id",
    "tab_ids",
    "pane_id",
    "pane_ids",
    "terminal_id",
    "terminal_ids",
    "backend_target",
    "backend_targets",
    "session_id",
    "private",
    "private_binding",
    "private_bindings",
    "private_fingerprint",
    "private_fingerprints",
    "route",
    "routes",
    "delivery",
    "deliveries",
    "connector",
    "connectors",
    "command",
    "command_args",
    "command_argv",
    "command_line",
    "command_payload",
    "command_text",
    "raw_args",
    "raw_argv",
    "raw_command",
    "raw_command_line",
    "shell_command",
    "chat_id",
    "chat_ids",
    "topic_id",
    "topic_ids",
    "message_id",
    "message_ids",
    "token",
    "tokens",
    "secret",
    "secrets",
    "password",
    "passwords",
    "credentials",
    "cookie",
    "auth_token",
    "auth_tokens",
}
_PUBLIC_JSON_FORBIDDEN_COMPACT = {
    key.replace("_", "") for key in _PUBLIC_JSON_FORBIDDEN_KEYS
}


def _assert_no_public_json_forbidden(value: Any, path: str = "$") -> None:
    if isinstance(value, dict):
        for key, item in value.items():
            normalized = str(key).lower().replace("-", "_")
            assert (
                normalized not in _PUBLIC_JSON_FORBIDDEN_KEYS
                and normalized.replace("_", "") not in _PUBLIC_JSON_FORBIDDEN_COMPACT
            ), f"forbidden field {path}.{key}"
            _assert_no_public_json_forbidden(item, f"{path}.{key}")
    elif isinstance(value, list):
        for index, item in enumerate(value):
            _assert_no_public_json_forbidden(item, f"{path}[{index}]")


def test_cli_snapshot_json_prints_contract_json_only(capsys) -> None:
    code = main(
        [
            "--host-id",
            "cli-host",
            "--herdr-bin",
            "definitely-not-a-real-herdr-binary",
            "snapshot",
            "--json",
        ]
    )
    captured = capsys.readouterr()

    assert code == 0
    assert captured.err == ""
    payload = json.loads(captured.out)
    assert payload["schema_version"] == 2
    assert payload["host_id"] == "cli-host"
    assert len(payload["content_fingerprint"]) == 24
    assert {"updated_at", "spaces", "workers", "attention", "backend_health"} <= set(payload)
    assert payload["backend_health"][0]["name"] == "herdr"
    assert payload["backend_health"][0]["status"] == "unavailable"
    assert payload["backend_health"][0]["outcome"] == "missing_binary"


def test_cli_snapshot_no_herdr_works() -> None:
    """Empty snapshot works even when herdr is not installed."""
    code = main(["--herdr-bin", "definitely-not-a-real-herdr-binary", "snapshot", "--json"])
    assert code == 0


def test_cli_snapshot_post_send_timeout_never_observes_source(
    tmp_path: Path,
    capsys,
    monkeypatch,
) -> None:
    from tendwire.daemon_api import DaemonUnavailable

    class TimeoutClient:
        def __init__(self, _socket_path: Any, **_kwargs: Any) -> None:
            pass

        def request(self, _method: str, _params: dict[str, Any] | None = None) -> dict[str, Any]:
            raise DaemonUnavailable(
                "timed out",
                timed_out=True,
                request_started=True,
            )

    def forbidden(*_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError("post-send timeout must not observe Herdr or mutate the store")

    monkeypatch.setattr("tendwire.daemon_api.DaemonAPIClient", TimeoutClient)
    monkeypatch.setattr("tendwire.cli.observe_public_snapshot", forbidden)

    code = main(
        [
            "--socket-path",
            str(tmp_path / "daemon.sock"),
            "snapshot",
            "--json",
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert code == 1
    assert payload == {
        "schema_version": 2,
        "ok": False,
        "status": "daemon_timeout",
        "error": {
            "code": "daemon_timeout",
            "message": "Tendwire daemon request timed out",
        },
    }


def test_cli_socket_group_option_is_daemon_only_and_normalized(monkeypatch) -> None:
    captured: list[Config] = []

    def capture_daemon_config(config: Config) -> int:
        captured.append(config)
        return 0

    monkeypatch.delenv("TENDWIRE_SOCKET_GROUP", raising=False)
    monkeypatch.setattr("tendwire.cli.cmd_daemon", capture_daemon_config)

    snapshot_args = _build_parser().parse_args(["snapshot"])
    assert not hasattr(snapshot_args, "socket_group")
    assert main(["daemon", "--socket-group", "  daemon-clients  "]) == 0
    assert captured[0].socket_group == "daemon-clients"


def test_cli_daemon_startup_conflict_is_clear_and_nonzero(
    tmp_path: Path,
    capsys,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from tendwire.daemon_api import DaemonUnavailable

    def active_socket(_config: Config) -> int:
        raise DaemonUnavailable(
            "daemon socket is already active: holder is tendwire 0.1.0rc4 "
            "(PID 4242); refusing to start tendwire 0.1.0rc5"
        )

    monkeypatch.setattr("tendwire.daemon.run_daemon", active_socket)

    code = main(["daemon", "--db-path", str(tmp_path / "daemon.db")])
    captured = capsys.readouterr()

    assert code == 1
    assert captured.out == ""
    assert captured.err == (
        "tendwire daemon 0.1.0rc5: startup failed: "
        "daemon socket is already active: holder is tendwire 0.1.0rc4 "
        "(PID 4242); refusing to start tendwire 0.1.0rc5\n"
    )


def test_cli_turns_json_without_cached_store_is_publicly_unavailable(capsys) -> None:
    code = main(
        [
            "--host-id",
            "turns-host",
            "--herdr-bin",
            "definitely-not-a-real-herdr-binary",
            "turns",
            "--json",
        ]
    )
    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert code == 1
    assert captured.err == ""
    assert payload == {
        "schema_version": 1,
        "host_id": "turns-host",
        "ok": False,
        "status": "store_unavailable",
    }


def test_cli_turns_schema_v2_daemon_request_requires_no_content_fetch(
    tmp_path: Path,
    capsys,
    monkeypatch,
) -> None:
    calls: list[tuple[str, dict[str, Any]]] = []

    class FakeDaemonAPIClient:
        def __init__(self, _socket_path: Any, **_kwargs: Any) -> None:
            pass

        def request(self, method: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
            calls.append((method, dict(params or {})))
            return {
                "ok": True,
                "result": {
                    "schema_version": 2,
                    "host_id": "turns-host",
                    "turns": [
                        {
                            "id": "turn-public",
                            "assistant_final_text": "short final",
                            "content": {
                                "schema_version": 1,
                                "content_revision": "twrev1.public",
                                "known_incomplete": False,
                                "fields": {
                                    "assistant_final_text": {
                                        "availability": "complete",
                                        "inline": True,
                                        "char_length": 11,
                                        "byte_length": 11,
                                        "page_count": 1,
                                        "first_cursor": None,
                                    }
                                },
                            },
                        }
                    ],
                },
            }

    monkeypatch.setattr("tendwire.daemon_api.DaemonAPIClient", FakeDaemonAPIClient)
    code = main(
        [
            "--host-id",
            "turns-host",
            "--socket-path",
            str(tmp_path / "daemon.sock"),
            "turns",
            "--schema-version",
            "2",
            "--json",
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert code == 0
    assert payload["schema_version"] == 2
    assert payload["turns"][0]["assistant_final_text"] == "short final"
    assert payload["turns"][0]["content"]["fields"]["assistant_final_text"]["inline"] is True
    assert calls == [
        (
            "turn.list",
            {
                "schema_version": 2,
                "limit": 100,
                "cursor": None,
                "since": None,
            },
        )
    ]


def test_cli_turns_v1_upgrade_required_is_json_and_nonzero(
    tmp_path: Path,
    capsys,
    monkeypatch,
) -> None:
    class FakeDaemonAPIClient:
        def __init__(self, _socket_path: Any, **_kwargs: Any) -> None:
            pass

        def request(self, method: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
            assert method == "turn.list"
            assert params == {
                "schema_version": 1,
                "limit": 100,
                "cursor": None,
                "since": None,
            }
            return {
                "ok": True,
                "result": {
                    "schema_version": 1,
                    "ok": False,
                    "status": "upgrade_required",
                    "required_turn_schema_version": 2,
                    "error": {
                        "code": "upgrade_required",
                        "message": "turn content requires schema version 2",
                    },
                },
            }

    monkeypatch.setattr("tendwire.daemon_api.DaemonAPIClient", FakeDaemonAPIClient)
    code = main(
        [
            "--host-id",
            "turns-host",
            "--socket-path",
            str(tmp_path / "daemon.sock"),
            "turns",
            "--json",
        ]
    )
    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert code == 1
    assert captured.err == ""
    assert payload["status"] == "upgrade_required"
    assert payload["required_turn_schema_version"] == 2
    assert payload["error"]["code"] == "upgrade_required"


def test_cli_turns_parser_defaults_bounds_and_exclusive_positions() -> None:
    parser = _build_parser()

    defaults = parser.parse_args(["turns"])
    assert defaults.limit == 100
    assert defaults.cursor is None
    assert defaults.since is None

    bounded = parser.parse_args(["turns", "--limit", "250", "--cursor", "twlist1.page"])
    assert bounded.limit == 250
    assert bounded.cursor == "twlist1.page"
    assert bounded.since is None

    for invalid_limit in ("0", "251", "1.5"):
        with pytest.raises(SystemExit):
            parser.parse_args(["turns", "--limit", invalid_limit])
    with pytest.raises(SystemExit):
        parser.parse_args(
            ["turns", "--cursor", "twlist1.page", "--since", "twsince1.new"]
        )


def test_cli_turns_definite_unavailable_refreshes_once_then_reads_exact_page(
    tmp_path: Path,
    capsys,
    monkeypatch,
) -> None:
    from tendwire.daemon_api import DaemonUnavailable

    daemon_calls: list[tuple[str, dict[str, Any]]] = []
    refresh_calls: list[dict[str, Any]] = []
    store_calls: list[tuple[Any, ...]] = []

    class UnavailableClient:
        def __init__(self, _socket_path: Any, **_kwargs: Any) -> None:
            pass

        def request(self, method: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
            daemon_calls.append((method, dict(params or {})))
            raise DaemonUnavailable("not listening", request_started=False)

    def refresh(_config: Config, **kwargs: Any) -> dict[str, Any]:
        refresh_calls.append(kwargs)
        return {"ok": True, "status": "ok", "updated": 1, "attempted": 1}

    def read_page(db_path: Path, host_id: str, **kwargs: Any) -> dict[str, Any]:
        store_calls.append((db_path, host_id, kwargs))
        return {
            "schema_version": 2,
            "host_id": host_id,
            "ok": True,
            "status": "ok",
            "turns": [{"id": "cached-turn"}],
            "next_cursor": "twlist1.next",
            "since": "twsince1.done",
        }

    def forbidden_snapshot(*_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError("turn fallback must not observe a snapshot")

    db_path = tmp_path / "fallback.db"
    monkeypatch.setattr("tendwire.daemon_api.DaemonAPIClient", UnavailableClient)
    monkeypatch.setattr("tendwire.cli.refresh_structured_turn_content", refresh)
    monkeypatch.setattr("tendwire.cli.turns_payload_from_store", read_page)
    monkeypatch.setattr("tendwire.cli._current_public_snapshot", forbidden_snapshot)

    code = main(
        [
            "--host-id",
            "fallback-host",
            "--herdr-timeout",
            "0.75",
            "--socket-path",
            str(tmp_path / "daemon.sock"),
            "turns",
            "--schema-version",
            "2",
            "--limit",
            "7",
            "--db-path",
            str(db_path),
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert code == 0
    assert payload["turns"] == [{"id": "cached-turn"}]
    assert daemon_calls == [
        (
            "turn.list",
            {"schema_version": 2, "limit": 7, "cursor": None, "since": None},
        )
    ]
    assert refresh_calls == [
        {
            "adapter_timeout_seconds": 0.75,
            "max_workers": 4,
            "total_timeout_seconds": 1.75,
        }
    ]
    assert store_calls == [
        (
            db_path,
            "fallback-host",
            {
                "schema_version": 2,
                "limit": 7,
                "cursor": None,
                "since": None,
                "turn_refresh_interval_seconds": 2.0,
                "turn_model": os.environ.get("TENDWIRE_TURN_MODEL", "observed"),
            },
        )
    ]


@pytest.mark.parametrize(
    ("position_flag", "position_value"),
    [
        ("--cursor", "twlist1.page"),
        ("--since", "twsince1.done"),
    ],
)
def test_cli_turns_continuation_unavailable_reads_cache_without_refresh(
    tmp_path: Path,
    capsys,
    monkeypatch,
    position_flag: str,
    position_value: str,
) -> None:
    from tendwire.daemon_api import DaemonUnavailable

    store_calls: list[dict[str, Any]] = []

    class UnavailableClient:
        def __init__(self, _socket_path: Any, **_kwargs: Any) -> None:
            pass

        def request(self, _method: str, _params: dict[str, Any] | None = None) -> dict[str, Any]:
            raise DaemonUnavailable("not listening", request_started=False)

    def forbidden_refresh(*_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError("continuation must never refresh")

    def read_page(_db_path: Path, host_id: str, **kwargs: Any) -> dict[str, Any]:
        store_calls.append(kwargs)
        return {
            "schema_version": 2,
            "host_id": host_id,
            "ok": True,
            "status": "ok",
            "turns": [],
            "next_cursor": None,
            "since": "twsince1.next",
        }

    monkeypatch.setattr("tendwire.daemon_api.DaemonAPIClient", UnavailableClient)
    monkeypatch.setattr("tendwire.cli.refresh_structured_turn_content", forbidden_refresh)
    monkeypatch.setattr("tendwire.cli.turns_payload_from_store", read_page)

    code = main(
        [
            "--host-id",
            "continuation-host",
            "--socket-path",
            str(tmp_path / "daemon.sock"),
            "turns",
            "--schema-version",
            "2",
            "--limit",
            "9",
            position_flag,
            position_value,
            "--db-path",
            str(tmp_path / "cache.db"),
        ]
    )
    json.loads(capsys.readouterr().out)

    assert code == 0
    assert store_calls == [
        {
            "schema_version": 2,
            "limit": 9,
            "cursor": position_value if position_flag == "--cursor" else None,
            "since": position_value if position_flag == "--since" else None,
            "turn_refresh_interval_seconds": 2.0,
            "turn_model": os.environ.get("TENDWIRE_TURN_MODEL", "observed"),
        }
    ]


@pytest.mark.parametrize(
    ("mode", "expected"),
    [
        (
            "timeout",
            {
                "schema_version": 1,
                "ok": False,
                "status": "daemon_timeout",
                "error": {
                    "code": "daemon_timeout",
                    "message": "Tendwire daemon request timed out",
                },
            },
        ),
        (
            "protocol",
            {
                "schema_version": 1,
                "ok": False,
                "status": "daemon_protocol_error",
                "error": {
                    "code": "daemon_protocol_error",
                    "message": "Tendwire daemon returned an invalid response",
                },
            },
        ),
        (
            "malformed",
            {
                "schema_version": 1,
                "ok": False,
                "status": "daemon_protocol_error",
                "error": {
                    "code": "daemon_protocol_error",
                    "message": "Tendwire daemon returned an invalid response",
                },
            },
        ),
        (
            "daemon_error",
            {
                "schema_version": 1,
                "ok": False,
                "status": "error",
                "result": None,
                "error": {
                    "code": "invalid_params",
                    "message": "invalid parameters",
                },
            },
        ),
    ],
)
def test_cli_turns_reachable_or_ambiguous_failure_never_reads_sources(
    tmp_path: Path,
    capsys,
    monkeypatch,
    mode: str,
    expected: dict[str, Any],
) -> None:
    from tendwire.daemon_api import DaemonProtocolError, DaemonUnavailable

    calls = 0

    class FailingClient:
        def __init__(self, _socket_path: Any, **_kwargs: Any) -> None:
            pass

        def request(self, method: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
            nonlocal calls
            calls += 1
            assert method == "turn.list"
            assert params == {
                "schema_version": 1,
                "limit": 100,
                "cursor": None,
                "since": None,
            }
            if mode == "timeout":
                raise DaemonUnavailable(
                    "timed out",
                    timed_out=True,
                    request_started=True,
                )
            if mode == "protocol":
                raise DaemonProtocolError("invalid frame", request_started=True)
            if mode == "malformed":
                return {"ok": True, "result": ["not", "a", "mapping"]}
            return expected

    def forbidden_read(*_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError("ambiguous/reachable failures must not read any source")

    monkeypatch.setattr("tendwire.daemon_api.DaemonAPIClient", FailingClient)
    monkeypatch.setattr("tendwire.cli.refresh_structured_turn_content", forbidden_read)
    monkeypatch.setattr("tendwire.cli.turns_payload_from_store", forbidden_read)
    monkeypatch.setattr("tendwire.cli._current_public_snapshot", forbidden_read)
    monkeypatch.setattr("tendwire.cli.fetch_herdr_state", forbidden_read)

    code = main(
        [
            "--host-id",
            "failure-host",
            "--socket-path",
            str(tmp_path / "daemon.sock"),
            "turns",
            "--db-path",
            str(tmp_path / "cache.db"),
        ]
    )
    captured = capsys.readouterr()

    assert code == 1
    assert captured.err == ""
    assert json.loads(captured.out) == expected
    assert calls == 1


@pytest.mark.parametrize("status", ["invalid_cursor", "cursor_expired", "since_expired"])
def test_cli_turns_reachable_invalid_or_expired_page_is_authoritative(
    tmp_path: Path,
    capsys,
    monkeypatch,
    status: str,
) -> None:
    result = {
        "schema_version": 2,
        "ok": False,
        "status": status,
    }

    class AuthoritativeClient:
        def __init__(self, _socket_path: Any, **_kwargs: Any) -> None:
            pass

        def request(self, method: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
            assert method == "turn.list"
            assert params == {
                "schema_version": 2,
                "limit": 11,
                "cursor": "twlist1.requested",
                "since": None,
            }
            return {"ok": True, "result": result}

    def forbidden_read(*_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError("reachable page result must be authoritative")

    monkeypatch.setattr("tendwire.daemon_api.DaemonAPIClient", AuthoritativeClient)
    monkeypatch.setattr("tendwire.cli.refresh_structured_turn_content", forbidden_read)
    monkeypatch.setattr("tendwire.cli.turns_payload_from_store", forbidden_read)
    monkeypatch.setattr("tendwire.cli._current_public_snapshot", forbidden_read)

    code = main(
        [
            "--host-id",
            "authoritative-host",
            "--socket-path",
            str(tmp_path / "daemon.sock"),
            "turns",
            "--schema-version",
            "2",
            "--limit",
            "11",
            "--cursor",
            "twlist1.requested",
            "--db-path",
            str(tmp_path / "cache.db"),
        ]
    )

    assert code == 1
    assert json.loads(capsys.readouterr().out) == result


def test_cli_turns_traverses_over_one_mib_across_bounded_daemon_pages(
    tmp_path: Path,
    capsys,
) -> None:
    socket_path = tmp_path / "paged-turns.sock"
    snapshot = Snapshot(host_id="paged-host", updated_at="2026-01-01T00:00:00+00:00")
    canonical_text = {
        f"turn-{index:03d}": (
            f"\n# Turn {index:03d}\n"
            + (f"exact-{index:03d}-αβγ\n" * 650)
            + "終\n"
        )
        for index in range(110)
    }
    ordered_ids = list(canonical_text)
    seen_requests: list[dict[str, Any]] = []

    def get_turns(
        *,
        schema_version: int,
        limit: int,
        cursor: str | None,
        since: str | None,
    ) -> dict[str, Any]:
        seen_requests.append(
            {
                "schema_version": schema_version,
                "limit": limit,
                "cursor": cursor,
                "since": since,
            }
        )
        start = 0 if cursor is None else 55
        page_ids = ordered_ids[start : start + 55]
        return {
            "schema_version": 2,
            "host_id": "paged-host",
            "ok": True,
            "status": "ok",
            "turns": [
                {
                    "id": turn_id,
                    "assistant_final_text": canonical_text[turn_id],
                    "content": {
                        "schema_version": 1,
                        "content_revision": f"twrev1.{turn_id}",
                        "known_incomplete": False,
                        "fields": {
                            "assistant_final_text": {
                                "availability": "complete",
                                "inline": True,
                                "char_length": len(canonical_text[turn_id]),
                                "byte_length": len(canonical_text[turn_id].encode("utf-8")),
                                "page_count": 1,
                                "first_cursor": None,
                            }
                        },
                    },
                }
                for turn_id in page_ids
            ],
            "next_cursor": "twlist1.second" if start == 0 else None,
            "since": "twsince1.complete" if start else None,
        }

    api = TendwireDaemonAPI(
        get_snapshot=lambda: snapshot,
        get_health=lambda: {"schema_version": 1, "status": "ok"},
        submit_command=lambda _params: {},
        get_turns=get_turns,
    )
    server = UnixSocketJSONServer(
        socket_path,
        api.dispatch,
        accept_timeout_seconds=0.05,
    )
    thread = threading.Thread(target=server.serve_forever)
    thread.start()
    pages: list[dict[str, Any]] = []
    encoded_sizes: list[int] = []
    cursor: str | None = None
    try:
        deadline = time.monotonic() + 2
        while not server.listening and time.monotonic() < deadline:
            time.sleep(0.01)
        while True:
            argv = [
                "--host-id",
                "paged-host",
                "--socket-path",
                str(socket_path),
                "turns",
                "--schema-version",
                "2",
                "--limit",
                "55",
            ]
            if cursor is not None:
                argv += ["--cursor", cursor]
            assert main(argv) == 0
            captured = capsys.readouterr()
            assert captured.err == ""
            encoded = captured.out.encode("utf-8")
            assert len(encoded) < 1024 * 1024
            encoded_sizes.append(len(encoded))
            page = json.loads(captured.out)
            pages.append(page)
            cursor = page["next_cursor"]
            if cursor is None:
                break
    finally:
        server.close()
        thread.join(timeout=2)

    listed = [turn for page in pages for turn in page["turns"]]
    assert sum(encoded_sizes) > 1024 * 1024
    assert [turn["id"] for turn in listed] == ordered_ids
    assert {
        turn["id"]: turn["assistant_final_text"] for turn in listed
    } == canonical_text
    assert seen_requests == [
        {
            "schema_version": 2,
            "limit": 55,
            "cursor": None,
            "since": None,
        },
        {
            "schema_version": 2,
            "limit": 55,
            "cursor": "twlist1.second",
            "since": None,
        },
    ]
    assert not thread.is_alive()


def test_cli_turn_content_get_preserves_exact_page_and_params(
    tmp_path: Path,
    capsys,
    monkeypatch,
) -> None:
    page_text = "\n  " + ("界" * 20_000) + "\r\n  "
    calls: list[tuple[str, dict[str, Any]]] = []

    class FakeDaemonAPIClient:
        def __init__(self, _socket_path: Any, **_kwargs: Any) -> None:
            pass

        def request(self, method: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
            calls.append((method, dict(params or {})))
            return {
                "ok": True,
                "result": {
                    "schema_version": 1,
                    "ok": True,
                    "status": "ok",
                    "turn_id": "turn-public",
                    "content_revision": "twrev1.public",
                    "field": "assistant_final_text",
                    "availability": "complete",
                    "segment_id": "twseg1.public",
                    "index": 1,
                    "count": 2,
                    "text": page_text,
                    "segment_char_length": len(page_text),
                    "segment_byte_length": len(page_text.encode("utf-8")),
                    "total_char_length": 40_000,
                    "total_byte_length": 120_000,
                    "next_cursor": None,
                },
            }

    monkeypatch.setattr("tendwire.daemon_api.DaemonAPIClient", FakeDaemonAPIClient)
    code = main(
        [
            "--host-id",
            "turns-host",
            "--socket-path",
            str(tmp_path / "daemon.sock"),
            "turn",
            "content",
            "get",
            "--json",
            "--turn-id",
            "turn-public",
            "--revision",
            "twrev1.public",
            "--field",
            "assistant_final_text",
            "--cursor",
            "twcur1.public",
        ]
    )
    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert code == 0
    assert captured.err == ""
    assert payload["turn_id"] == "turn-public"
    assert payload["segment_id"] == "twseg1.public"
    assert payload["text"] == page_text
    assert calls == [
        (
            "turn.content.get",
            {
                "schema_version": 1,
                "turn_id": "turn-public",
                "content_revision": "twrev1.public",
                "field": "assistant_final_text",
                "cursor": "twcur1.public",
            },
        )
    ]


@pytest.mark.parametrize("with_db_path", [False, True])
@pytest.mark.parametrize(
    ("error_code", "details"),
    [
        ("internal_error", {"type": "RuntimeError"}),
        ("response_too_large", {"max_response_bytes": 1024 * 1024}),
    ],
)
def test_cli_turn_content_preserves_reachable_daemon_errors_without_store_fallback(
    tmp_path: Path,
    capsys,
    monkeypatch,
    with_db_path: bool,
    error_code: str,
    details: dict[str, Any],
) -> None:
    direct_calls: list[str] = []
    original_error = {
        "code": error_code,
        "message": f"daemon {error_code}",
        "details": details,
    }

    class FakeDaemonAPIClient:
        def __init__(self, _socket_path: Any, **_kwargs: Any) -> None:
            pass

        def request(self, method: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
            assert method == "turn.content.get"
            return {
                "schema_version": 1,
                "ok": False,
                "status": "error",
                "result": None,
                "error": original_error,
            }

    def forbidden_store_call(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
        direct_calls.append("store")
        raise AssertionError("reachable daemon errors must not fall back to the store")

    monkeypatch.setattr("tendwire.daemon_api.DaemonAPIClient", FakeDaemonAPIClient)
    monkeypatch.setattr("tendwire.store.sqlite.init_store", forbidden_store_call)
    monkeypatch.setattr("tendwire.store.sqlite.get_turn_content", forbidden_store_call)
    argv = [
        "--host-id",
        "turns-host",
        "--socket-path",
        str(tmp_path / "daemon.sock"),
        "turn",
        "content",
        "get",
        "--json",
        "--turn-id",
        "turn-public",
        "--revision",
        "twrev1.public",
        "--field",
        "assistant_final_text",
    ]
    if with_db_path:
        argv += ["--db-path", str(tmp_path / "direct.db")]

    code = main(argv)
    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert code == 1
    assert captured.err == ""
    assert payload["status"] == "error"
    assert payload["error"] == original_error
    assert direct_calls == []


def test_cli_long_content_pages_match_direct_store_and_daemon(
    tmp_path: Path,
    capsys,
    monkeypatch,
) -> None:
    db_path = tmp_path / "long-content.db"
    socket_path = tmp_path / "long-content.sock"
    config = Config(host_id="long-host", db_path=db_path)
    snapshot = project_from_raw(
        config,
        workers=[{"id": "worker-1", "name": "Worker", "status": "active"}],
    )
    canonical = (
        "# Exact heading\n\n"
        + ("safe-value αβγ\n- nested-looking item\n```text\ncode\n```\n" * 30_000)
    )[:1_100_000] + "終"
    init_store(db_path)
    save_snapshot(db_path, snapshot)
    assert merge_turn_content(
        db_path,
        "long-host",
        "worker-1",
        {
            "source_turn_id": "cli-long-content-source",
            "user_text": "short prompt",
            "assistant_final_text": canonical,
            "complete": True,
            "has_open_turn": False,
        },
        observed_at="2026-01-01T00:00:00+00:00",
    ) == 1
    listed = turns_payload_from_store(
        db_path,
        "long-host",
        snapshot=snapshot,
        schema_version=2,
    )
    turn = listed["turns"][0]
    revision = turn["content"]["content_revision"]
    descriptor = turn["content"]["fields"]["assistant_final_text"]
    monkeypatch.setattr(
        "tendwire.cli.refresh_structured_turn_content",
        lambda _config, **_kwargs: {"ok": True},
    )

    v1_code = main(
        [
            "--host-id",
            "long-host",
            "--herdr-bin",
            "definitely-not-a-real-herdr-binary",
            "turns",
            "--db-path",
            str(db_path),
            "--json",
        ]
    )
    v1_payload = json.loads(capsys.readouterr().out)
    v2_code = main(
        [
            "--host-id",
            "long-host",
            "--herdr-bin",
            "definitely-not-a-real-herdr-binary",
            "turns",
            "--db-path",
            str(db_path),
            "--schema-version",
            "2",
            "--json",
        ]
    )
    v2_payload = json.loads(capsys.readouterr().out)

    assert v1_code == 1
    assert v1_payload["status"] == "upgrade_required"
    assert v1_payload["required_turn_schema_version"] == 2
    assert v2_code == 0
    assert v2_payload["schema_version"] == 2
    assert descriptor["inline"] is False
    assert descriptor["char_length"] == len(canonical)
    assert descriptor["byte_length"] == len(canonical.encode("utf-8"))
    assert descriptor["page_count"] > 1

    def fetch_pages(*, socket: Path | None, direct_db: Path | None) -> list[dict[str, Any]]:
        pages: list[dict[str, Any]] = []
        cursor: str | None = None
        while True:
            argv = ["--host-id", "long-host"]
            if socket is not None:
                argv += ["--socket-path", str(socket)]
            argv += [
                "turn",
                "content",
                "get",
                "--json",
                "--turn-id",
                turn["id"],
                "--revision",
                revision,
                "--field",
                "assistant_final_text",
            ]
            if direct_db is not None:
                argv += ["--db-path", str(direct_db)]
            if cursor is not None:
                argv += ["--cursor", cursor]
            assert main(argv) == 0
            captured = capsys.readouterr()
            assert captured.err == ""
            page = json.loads(captured.out)
            assert len(json.dumps(page, ensure_ascii=False).encode("utf-8")) < 1024 * 1024
            pages.append(page)
            next_cursor = page["next_cursor"]
            if next_cursor is None:
                return pages
            assert next_cursor not in {item.get("next_cursor") for item in pages[:-1]}
            cursor = next_cursor

    direct_pages = fetch_pages(socket=None, direct_db=db_path)
    bad_cursor_code = main(
        [
            "--host-id",
            "long-host",
            "turn",
            "content",
            "get",
            "--json",
            "--turn-id",
            turn["id"],
            "--revision",
            revision,
            "--field",
            "assistant_final_text",
            "--cursor",
            "twcur1.tampered",
            "--db-path",
            str(db_path),
        ]
    )
    bad_cursor_payload = json.loads(capsys.readouterr().out)
    assert bad_cursor_code == 1
    assert bad_cursor_payload["status"] == "invalid_cursor"

    api = TendwireDaemonAPI(
        get_snapshot=lambda: snapshot,
        get_health=lambda: {"schema_version": 1, "status": "ok"},
        submit_command=lambda _params: {},
        get_turn_content=lambda params: get_turn_content(
            db_path,
            "long-host",
            turn_id=params["turn_id"],
            content_revision=params["content_revision"],
            field=params["field"],
            cursor=params.get("cursor"),
            schema_version=params.get("schema_version", 1),
        ),
    )
    server = UnixSocketJSONServer(
        socket_path,
        api.dispatch,
        accept_timeout_seconds=0.05,
    )
    thread = threading.Thread(target=server.serve_forever)
    thread.start()
    try:
        deadline = time.monotonic() + 2
        while not server.listening and time.monotonic() < deadline:
            time.sleep(0.01)
        daemon_pages = fetch_pages(socket=socket_path, direct_db=None)
        daemon_bad_code = main(
            [
                "--host-id",
                "long-host",
                "--socket-path",
                str(socket_path),
                "turn",
                "content",
                "get",
                "--json",
                "--turn-id",
                turn["id"],
                "--revision",
                revision,
                "--field",
                "assistant_final_text",
                "--cursor",
                "twcur1.tampered",
            ]
        )
        daemon_bad_payload = json.loads(capsys.readouterr().out)
        assert daemon_bad_code == 1
        assert daemon_bad_payload == bad_cursor_payload
    finally:
        server.close()
        thread.join(timeout=2)

    assert daemon_pages == direct_pages
    assert "".join(page["text"] for page in direct_pages) == canonical
    assert [page["index"] for page in direct_pages] == list(range(len(direct_pages)))
    assert all(page["count"] == len(direct_pages) for page in direct_pages)
    assert not thread.is_alive()


def test_cli_short_v1_compatibility_then_known_incomplete_refusal(
    tmp_path: Path,
    capsys,
    monkeypatch,
) -> None:
    db_path = tmp_path / "content-compatibility.db"
    config = Config(host_id="compat-host", db_path=db_path)
    snapshot = project_from_raw(
        config,
        workers=[{"id": "worker-1", "name": "Worker", "status": "active"}],
    )
    init_store(db_path)
    save_snapshot(db_path, snapshot)
    assert merge_turn_content(
        db_path,
        "compat-host",
        "worker-1",
        {
            "source_turn_id": "cli-short-content-source",
            "user_text": "  short prompt\n",
            "assistant_final_text": "\n short final  ",
            "complete": True,
        },
    ) == 1
    monkeypatch.setattr(
        "tendwire.cli.refresh_structured_turn_content",
        lambda _config, **_kwargs: {"ok": True},
    )
    common = [
        "--host-id",
        "compat-host",
        "--herdr-bin",
        "definitely-not-a-real-herdr-binary",
        "turns",
        "--db-path",
        str(db_path),
        "--json",
    ]

    short_v1_code = main(common)
    short_v1 = json.loads(capsys.readouterr().out)
    short_v2_code = main([*common, "--schema-version", "2"])
    short_v2 = json.loads(capsys.readouterr().out)
    short_turn = short_v2["turns"][0]

    assert short_v1_code == 0
    assert short_v1["schema_version"] == 1
    assert short_v1["turns"][0]["assistant_final_text"] == "\n short final  "
    assert short_v1["turns"][0]["user_text"] == "  short prompt\n"
    assert "content" not in short_v1["turns"][0]
    assert short_v2_code == 0
    assert short_turn["assistant_final_text"] == "\n short final  "
    assert short_turn["content"]["fields"]["assistant_final_text"]["inline"] is True

    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            """
            UPDATE turn_content_revisions
            SET final_state = 'known_incomplete'
            WHERE host_id = ? AND turn_id = ? AND is_current = 1
            """,
            ("compat-host", short_turn["id"]),
        )

    incomplete_v1_code = main(common)
    incomplete_v1 = json.loads(capsys.readouterr().out)
    incomplete_v2_code = main([*common, "--schema-version", "2"])
    incomplete_v2 = json.loads(capsys.readouterr().out)
    revision = incomplete_v2["turns"][0]["content"]["content_revision"]
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            """
            UPDATE turn_content_revisions
            SET content_revision = ?
            WHERE host_id = ? AND turn_id = ? AND is_current = 1
            """,
            (revision, "compat-host", short_turn["id"]),
        )
    content_code = main(
        [
            "--host-id",
            "compat-host",
            "turn",
            "content",
            "get",
            "--json",
            "--turn-id",
            short_turn["id"],
            "--revision",
            revision,
            "--field",
            "assistant_final_text",
            "--db-path",
            str(db_path),
        ]
    )
    content_error = json.loads(capsys.readouterr().out)

    assert incomplete_v1_code == 1
    assert incomplete_v1["status"] == "upgrade_required"
    assert incomplete_v1["required_turn_schema_version"] == 2
    assert incomplete_v2_code == 0
    incomplete_field = incomplete_v2["turns"][0]["content"]["fields"]["assistant_final_text"]
    assert incomplete_field["availability"] == "known_incomplete"
    assert incomplete_field["inline"] is False
    assert "assistant_final_text" not in incomplete_v2["turns"][0]
    assert content_code == 1
    assert content_error["status"] == "content_known_incomplete"


def test_cli_pending_missing_store_is_fixed_and_never_observes_sources(
    tmp_path: Path,
    capsys,
    monkeypatch,
) -> None:
    def forbidden(*_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError("pending fallback must not observe Herdr or source state")

    monkeypatch.setattr("tendwire.cli._current_public_snapshot", forbidden)
    monkeypatch.setattr("tendwire.cli.fetch_herdr_state", forbidden)
    monkeypatch.setattr("tendwire.cli.refresh_structured_turn_content", forbidden)

    code = main(
        [
            "--host-id",
            "pending-host",
            "pending",
            "--json",
            "--db-path",
            str(tmp_path / "missing.db"),
        ]
    )
    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert code == 1
    assert captured.err == ""
    assert payload == {
        "schema_version": 1,
        "host_id": "pending-host",
        "ok": False,
        "status": "store_unavailable",
        "pending_interactions": [],
        "backend_health": [],
        "pending_health": {
            "status": "store_unavailable",
            "counts": {"fresh": 0, "stale": 0, "total": 0},
        },
    }
    _assert_no_public_json_forbidden(payload)


@pytest.mark.parametrize("mode", ["success", "error"])
def test_cli_pending_structured_daemon_result_or_error_is_authoritative(
    tmp_path: Path,
    capsys,
    monkeypatch,
    mode: str,
) -> None:
    success = {
        "schema_version": 1,
        "host_id": "authoritative-pending",
        "pending_interactions": [],
        "backend_health": [],
        "pending_health": {
            "status": "healthy",
            "counts": {"fresh": 0, "stale": 0, "total": 0},
        },
        "content_fingerprint": "a" * 24,
    }
    error = {
        "schema_version": 1,
        "ok": False,
        "status": "error",
        "result": None,
        "error": {
            "code": "invalid_params",
            "message": "invalid pending request",
        },
    }
    calls = 0

    class AuthoritativeClient:
        def __init__(self, _socket_path: Any, **_kwargs: Any) -> None:
            pass

        def request(self, method: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
            nonlocal calls
            calls += 1
            assert method == "pending.list"
            assert params == {}
            if mode == "success":
                return {"ok": True, "result": success}
            return error

    def forbidden(*_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError("authoritative daemon response must not read fallback state")

    monkeypatch.setattr("tendwire.daemon_api.DaemonAPIClient", AuthoritativeClient)
    monkeypatch.setattr("tendwire.cli.pending_payload_from_store", forbidden)
    monkeypatch.setattr("tendwire.cli._current_public_snapshot", forbidden)
    monkeypatch.setattr("tendwire.cli.fetch_herdr_state", forbidden)
    monkeypatch.setattr("tendwire.cli.refresh_structured_turn_content", forbidden)

    code = main(
        [
            "--host-id",
            "authoritative-pending",
            "--socket-path",
            str(tmp_path / "daemon.sock"),
            "pending",
            "--json",
            "--db-path",
            str(tmp_path / "cache.db"),
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert calls == 1
    assert payload == (success if mode == "success" else error)
    assert code == (0 if mode == "success" else 1)


@pytest.mark.parametrize(
    ("mode", "expected_status"),
    [
        ("timeout", "daemon_timeout"),
        ("protocol", "daemon_protocol_error"),
        ("malformed", "daemon_protocol_error"),
        ("post_send_unavailable", "daemon_protocol_error"),
    ],
)
def test_cli_pending_post_send_failure_never_reads_or_retries(
    tmp_path: Path,
    capsys,
    monkeypatch,
    mode: str,
    expected_status: str,
) -> None:
    from tendwire.daemon_api import DaemonProtocolError, DaemonUnavailable

    calls = 0

    class FailingClient:
        def __init__(self, _socket_path: Any, **_kwargs: Any) -> None:
            pass

        def request(self, method: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
            nonlocal calls
            calls += 1
            assert method == "pending.list"
            assert params == {}
            if mode == "timeout":
                raise DaemonUnavailable(
                    "timed out",
                    timed_out=True,
                    request_started=True,
                )
            if mode == "protocol":
                raise DaemonProtocolError("invalid frame", request_started=True)
            if mode == "post_send_unavailable":
                raise DaemonUnavailable("connection lost", request_started=True)
            return {"ok": True, "result": ["not", "a", "mapping"]}

    def forbidden(*_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError("post-send failure must not read fallback state")

    monkeypatch.setattr("tendwire.daemon_api.DaemonAPIClient", FailingClient)
    monkeypatch.setattr("tendwire.cli.pending_payload_from_store", forbidden)
    monkeypatch.setattr("tendwire.cli._current_public_snapshot", forbidden)
    monkeypatch.setattr("tendwire.cli.fetch_herdr_state", forbidden)
    monkeypatch.setattr("tendwire.cli.refresh_structured_turn_content", forbidden)

    code = main(
        [
            "--host-id",
            "failed-pending",
            "--socket-path",
            str(tmp_path / "daemon.sock"),
            "pending",
            "--json",
            "--db-path",
            str(tmp_path / "cache.db"),
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert code == 1
    assert calls == 1
    assert payload == {
        "schema_version": 1,
        "ok": False,
        "status": expected_status,
        "error": {
            "code": expected_status,
            "message": (
                "Tendwire daemon request timed out"
                if expected_status == "daemon_timeout"
                else "Tendwire daemon returned an invalid response"
            ),
        },
    }


@pytest.mark.parametrize(
    ("mode", "expected_status"),
    [
        ("timeout", "daemon_timeout"),
        ("protocol", "daemon_protocol_error"),
        ("post_send_unavailable", "daemon_protocol_error"),
    ],
)
def test_cli_connector_post_send_failure_never_mutates_store_fallback(
    tmp_path: Path,
    capsys,
    monkeypatch,
    mode: str,
    expected_status: str,
) -> None:
    from tendwire.daemon_api import DaemonProtocolError, DaemonUnavailable

    calls = 0

    class FailingClient:
        def __init__(
            self,
            _socket_path: Any,
            *,
            timeout_seconds: float,
            **_kwargs: Any,
        ) -> None:
            assert timeout_seconds == 30.0

        def request(
            self,
            method: str,
            params: dict[str, Any] | None = None,
        ) -> dict[str, Any]:
            nonlocal calls
            calls += 1
            assert method == "connector.poll"
            assert params == {
                "name": "turn-final",
                "limit": 1,
                "lease_seconds": 60,
            }
            if mode == "timeout":
                raise DaemonUnavailable(
                    "timed out",
                    timed_out=True,
                    request_started=True,
                )
            if mode == "protocol":
                raise DaemonProtocolError(
                    "invalid frame",
                    request_started=True,
                )
            raise DaemonUnavailable(
                "connection lost",
                request_started=True,
            )

    def forbidden(*_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError(
            "post-send connector failure must not execute store fallback"
        )

    monkeypatch.setattr(
        "tendwire.daemon_api.DaemonAPIClient",
        FailingClient,
    )
    monkeypatch.setattr("tendwire.store.sqlite.init_store", forbidden)

    code = main(
        [
            "--host-id",
            "connector-timeout-host",
            "--socket-path",
            str(tmp_path / "daemon.sock"),
            "connector",
            "poll",
            "--db-path",
            str(tmp_path / "cache.db"),
            "--name",
            "turn-final",
            "--limit",
            "1",
            "--lease-seconds",
            "60",
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert code == 1
    assert calls == 1
    assert payload == {
        "schema_version": 1,
        "ok": False,
        "status": expected_status,
        "host_id": "connector-timeout-host",
        "name": "turn-final",
        "error": {
            "code": expected_status,
            "message": (
                "Tendwire daemon request timed out"
                if expected_status == "daemon_timeout"
                else "Tendwire daemon returned an invalid response"
            ),
        },
    }


def test_cli_connector_pre_send_unavailable_uses_store_fallback(
    tmp_path: Path,
    capsys,
    monkeypatch,
) -> None:
    from tendwire.daemon_api import DaemonUnavailable

    db_path = tmp_path / "connector-fallback.db"

    class UnavailableClient:
        def __init__(self, _socket_path: Any, **_kwargs: Any) -> None:
            pass

        def request(
            self,
            method: str,
            params: dict[str, Any] | None = None,
        ) -> dict[str, Any]:
            assert method == "connector.poll"
            raise DaemonUnavailable(
                "not listening",
                request_started=False,
            )

    monkeypatch.setattr(
        "tendwire.daemon_api.DaemonAPIClient",
        UnavailableClient,
    )

    code = main(
        [
            "--host-id",
            "connector-fallback-host",
            "--socket-path",
            str(tmp_path / "missing.sock"),
            "connector",
            "poll",
            "--db-path",
            str(db_path),
            "--name",
            "turn-final",
            "--limit",
            "1",
            "--lease-seconds",
            "60",
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert code == 0
    assert payload["schema_version"] == 1
    assert payload["ok"] is True
    assert payload["host_id"] == "connector-fallback-host"
    assert payload["name"] == "turn-final"
    assert payload["items"] == []


def test_cli_store_hooks_print_json_only_and_support_dry_run(tmp_path: Path, capsys) -> None:
    db_path = tmp_path / "store-cli.db"
    init_store(db_path)
    append_event(
        db_path,
        "store-cli",
        "private.event",
        {"pane_id": "sentinel-private-pane", "raw_payload": "sentinel-private-raw"},
        observed_at="2026-01-01T00:00:00+00:00",
    )
    append_event(
        db_path,
        "store-cli",
        "public.event",
        {"safe": "kept"},
        observed_at="9999-01-09T00:00:00+00:00",
    )

    status_code = main(["--host-id", "store-cli", "store", "status", "--db-path", str(db_path)])
    status_captured = capsys.readouterr()
    status_payload = json.loads(status_captured.out)

    tail_code = main(
        [
            "--host-id",
            "store-cli",
            "store",
            "events-tail",
            "--db-path",
            str(db_path),
            "--limit",
            "5",
        ]
    )
    tail_captured = capsys.readouterr()
    tail_payload = json.loads(tail_captured.out)

    cleanup_code = main(
        [
            "--host-id",
            "store-cli",
            "store",
            "cleanup",
            "--db-path",
            str(db_path),
            "--retention-days",
            "7",
            "--dry-run",
        ]
    )
    cleanup_captured = capsys.readouterr()
    cleanup_payload = json.loads(cleanup_captured.out)

    missing_code = main(
        [
            "--host-id",
            "store-cli",
            "store",
            "status",
            "--db-path",
            str(tmp_path / "missing.db"),
        ]
    )
    missing_captured = capsys.readouterr()
    missing_payload = json.loads(missing_captured.out)

    with sqlite3.connect(str(db_path)) as conn:
        event_count = conn.execute("SELECT COUNT(*) FROM events WHERE host_id = ?", ("store-cli",)).fetchone()[0]

    assert status_code == 0
    assert tail_code == 0
    assert cleanup_code == 0
    assert missing_code == 1
    assert status_captured.err == tail_captured.err == cleanup_captured.err == missing_captured.err == ""
    assert status_payload["counts"]["events"] == 2
    assert tail_payload["events"]
    assert "sentinel-private" not in json.dumps(tail_payload)
    assert "payload_json" not in json.dumps(tail_payload)
    assert cleanup_payload["dry_run"] is True
    assert cleanup_payload["retention"]["deleted"] == 1
    assert "last_examined_id" not in json.dumps(cleanup_payload)
    assert event_count == 2
    assert missing_payload["status"] == "store_unavailable"


@pytest.mark.parametrize("timed_out", [False, True])
def test_cli_pending_pre_send_unavailable_uses_durable_overlay_only(
    tmp_path: Path,
    capsys,
    monkeypatch,
    timed_out: bool,
) -> None:
    from tendwire.daemon_api import DaemonUnavailable

    db_path = tmp_path / "pending-fallback.db"
    snapshot = Snapshot(
        host_id="projection-cli",
        updated_at="2026-01-01T00:00:00+00:00",
        workers=[
            Worker(
                id="worker-1",
                name="Worker One",
                status="pending",
                space_id="space-1",
                summary="human approval required before continuing",
                meta={
                    "needs_human": True,
                    "pane_id": "sentinel-cli-pane",
                },
                backend_target={
                    "kind": "agent_id",
                    "value": "sentinel-cli-target",
                    "sendable": True,
                },
            )
        ],
        backend_health=[
            {
                "name": "herdr",
                "status": "healthy",
                "outcome": "healthy_non_empty",
                "observed_at": "2026-01-01T00:00:00+00:00",
            }
        ],
    )
    init_store(db_path)
    save_snapshot(db_path, snapshot)
    merge_backend_pending(
        db_path,
        snapshot.host_id,
        "worker-1",
        {
            "question": "Which durable option?",
            "kind": "choice",
            "choices": [
                {"choice_id": "safe", "label": "Safe"},
                {
                    "choice_id": "private",
                    "label": "sentinel-cli-private",
                    "value": "sentinel-cli-command",
                },
            ],
            "meta": {"source": "backend", "pane_id": "sentinel-cli-pane"},
        },
    )
    expected = pending_payload_from_store(db_path, snapshot.host_id)
    calls = 0

    class UnavailableClient:
        def __init__(self, _socket_path: Any, **_kwargs: Any) -> None:
            pass

        def request(self, method: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
            nonlocal calls
            calls += 1
            assert method == "pending.list"
            assert params == {}
            raise DaemonUnavailable(
                "not listening",
                timed_out=timed_out,
                request_started=False,
            )

    def forbidden(*_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError("durable pending fallback must not observe Herdr")

    monkeypatch.setattr("tendwire.daemon_api.DaemonAPIClient", UnavailableClient)
    monkeypatch.setattr("tendwire.cli._current_public_snapshot", forbidden)
    monkeypatch.setattr("tendwire.cli.fetch_herdr_state", forbidden)
    monkeypatch.setattr("tendwire.cli.refresh_structured_turn_content", forbidden)

    code = main(
        [
            "--host-id",
            snapshot.host_id,
            "--socket-path",
            str(tmp_path / "missing.sock"),
            "pending",
            "--json",
            "--db-path",
            str(db_path),
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert code == 0
    assert calls == 1
    assert payload == expected
    assert payload["pending_interactions"][0]["question"] == "Which durable option?"
    assert "sentinel-cli" not in json.dumps(payload, sort_keys=True)
    _assert_no_public_json_forbidden(payload)


def test_cli_pending_durable_snapshot_strips_raw_command_action_material(
    tmp_path: Path,
    capsys,
    monkeypatch,
) -> None:
    db_path = tmp_path / "raw-command-pending.db"
    snapshot = Snapshot(
        host_id="raw-command-cli",
        updated_at="2026-01-01T00:00:00+00:00",
        workers=[
            Worker(
                id="worker-1",
                name="Worker One",
                status="waiting",
                space_id="space-1",
                summary="waiting for action",
            )
        ],
        attention=[
            AttentionSignal(
                kind="worker_status",
                severity="warning",
                status="waiting",
                reason="Choose next action",
                source="worker:worker-1",
                updated_at="2026-01-01T00:00:00+00:00",
                suggested_actions=[
                    SuggestedAction(
                        command="sentinel-cli-safe-looking-command-alias",
                        params={
                            "safe_choice": "kept",
                            "commandLine": "sentinel-cli-command-line",
                            "terminal_id": "sentinel-cli-terminal",
                            "backendTarget": "sentinel-cli-backend",
                            "session-id": "sentinel-cli-session",
                            "token": "sentinel-cli-token",
                            "secret": "sentinel-cli-secret",
                        },
                    )
                ],
                meta={
                    "worker_id": "worker-1",
                    "space_id": "space-1",
                    "needs_human": True,
                },
                host_id="raw-command-cli",
            )
        ],
    )
    init_store(db_path)
    save_snapshot(db_path, snapshot)

    def forbidden(*_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError("pending projection must not observe current state")

    monkeypatch.setattr("tendwire.cli._current_public_snapshot", forbidden)
    monkeypatch.setattr("tendwire.cli.fetch_herdr_state", forbidden)

    code = main(
        [
            "--host-id",
            snapshot.host_id,
            "pending",
            "--json",
            "--db-path",
            str(db_path),
        ]
    )
    payload = json.loads(capsys.readouterr().out)
    encoded = json.dumps(payload, sort_keys=True)

    assert code == 0
    assert payload["pending_interactions"][0]["choices"] == [
        {
            "choice_id": payload["pending_interactions"][0]["choices"][0]["choice_id"],
            "label": "Action",
        }
    ]
    assert "sentinel-cli-" not in encoded
    _assert_no_public_json_forbidden(payload)


def test_cli_snapshot_json_reports_healthy_empty_herdr(capsys, monkeypatch) -> None:
    responses = {
        ("workspace", "list"): {"result": {"workspaces": []}},
        ("agent", "list"): {"result": {"agents": []}},
        ("pane", "list"): {"result": {"panes": []}},
    }

    def _fake_run_herdr(args, cfg):
        if tuple(args) in responses:
            return subprocess.CompletedProcess(
                args=list(args),
                returncode=0,
                stdout=json.dumps(responses[tuple(args)]),
                stderr="",
            )
        return subprocess.CompletedProcess(args=list(args), returncode=1, stdout="", stderr="")

    monkeypatch.setattr(herdr_cli.shutil, "which", lambda _: "/usr/bin/herdr")
    monkeypatch.setattr(herdr_cli, "_run_herdr", _fake_run_herdr)

    code = main(["--host-id", "cli-empty", "--herdr-bin", "herdr", "snapshot", "--json"])
    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert code == 0
    assert payload["spaces"] == []
    assert payload["workers"] == []
    assert payload["backend_health"][0]["status"] == "healthy"
    assert payload["backend_health"][0]["outcome"] == "empty_healthy"
    assert payload["backend_health"][0]["counts"] == {"spaces": 0, "workers": 0}


def test_cli_snapshot_store_persists_printed_snapshot(tmp_path: Path, capsys) -> None:
    db_path = tmp_path / "cli.db"
    code = main(
        [
            "--host-id",
            "cli-store",
            "--herdr-bin",
            "definitely-not-a-real-herdr-binary",
            "snapshot",
            "--db-path",
            str(db_path),
            "--json",
            "--store",
        ]
    )
    captured = capsys.readouterr()

    assert code == 0
    payload = json.loads(captured.out)
    assert captured.err == ""
    restored = latest_snapshot(db_path)
    assert restored is not None
    assert restored.host_id == "cli-store"
    assert restored.content_fingerprint == payload["content_fingerprint"]


@pytest.mark.parametrize(
    ("outcome", "has_workers", "expected_authority"),
    [
        ("healthy_non_empty", True, "complete"),
        ("empty_healthy", False, "complete"),
        ("missing_binary", False, "none"),
        ("timeout", False, "none"),
        ("malformed_json", False, "none"),
        ("continuity_unavailable", True, "none"),
        ("unknown", False, "none"),
    ],
)
def test_cli_snapshot_persistence_passes_explicit_observation_authority(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    outcome: str,
    has_workers: bool,
    expected_authority: str,
) -> None:
    db_path = tmp_path / f"{outcome}.db"
    init_store(db_path)
    config = Config(host_id=f"cli-{outcome}", db_path=db_path)
    observed_at = "2026-01-01T00:00:00+00:00"
    workers = [Worker(id="worker-1", name="Worker One", status="active")] if has_workers else []
    health = herdr_cli.herdr_backend_health(
        outcome,
        observed_at=observed_at,
        workers=workers,
    )
    observation = SimpleNamespace(
        spaces=[],
        workers=workers,
        bindings=[],
        backend_health=[health],
    )
    captured: list[SnapshotObservationContext] = []
    captured_atomic: list[tuple[list[WorkerBinding], str | None, bool, bool]] = []
    captured_turn_models: list[str] = []

    monkeypatch.setattr(
        "tendwire.cli.fetch_herdr_snapshot_observation",
        lambda _config, *, stored_bindings: observation,
    )

    def _capture_save(
        _db_path: Path,
        _snapshot: Snapshot,
        *,
        turn_model: str,
        observation: SnapshotObservationContext,
        worker_bindings: list[WorkerBinding],
        binding_backend: str | None,
        binding_observation_authoritative: bool,
        binding_workers_present: bool,
    ) -> bool:
        captured_turn_models.append(turn_model)
        captured.append(observation)
        captured_atomic.append(
            (
                worker_bindings,
                binding_backend,
                binding_observation_authoritative,
                binding_workers_present,
            )
        )
        return True

    monkeypatch.setattr("tendwire.store.sqlite.save_snapshot", _capture_save)

    observe_public_snapshot(config, store_snapshot=True)

    assert len(captured) == 1
    assert captured[0].authority == expected_authority
    assert captured[0].observed_at == observed_at
    assert captured_atomic == [
        ([], "herdr", health.status == "healthy", bool(workers))
    ]
    assert captured_turn_models == [config.turn_model]


def test_rejected_stale_snapshot_does_not_persist_stale_worker_bindings(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "rejected-stale-binding.db"
    init_store(db_path)
    config = Config(host_id="cli-stale-binding", db_path=db_path)
    worker = Worker(id="worker-stale", name="Worker Stale", status="active")
    health = herdr_cli.herdr_backend_health(
        "healthy_non_empty",
        observed_at="2026-01-01T00:00:00+00:00",
        workers=[worker],
    )
    observation = SimpleNamespace(
        spaces=[],
        workers=[worker],
        bindings=[{"worker_id": worker.id, "turn_target_value": "stale-target"}],
        backend_health=[health],
    )
    monkeypatch.setattr(
        "tendwire.cli.fetch_herdr_snapshot_observation",
        lambda _config, *, stored_bindings: observation,
    )
    monkeypatch.setattr(
        "tendwire.store.sqlite.save_snapshot",
        lambda *_args, **_kwargs: False,
    )

    def _forbidden_binding_persistence(*_args: Any, **_kwargs: Any) -> None:
        raise AssertionError("rejected snapshot must not update private bindings")

    monkeypatch.setattr(
        "tendwire.cli._persist_binding_observation",
        _forbidden_binding_persistence,
    )

    observed = observe_public_snapshot(config, store_snapshot=True)
    assert observed.host_id == config.host_id


def test_cli_atomic_snapshot_binding_write_serializes_delayed_older_observer(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from tendwire.store import sqlite as store_sqlite

    db_path = tmp_path / "cli-atomic-binding-race.db"
    config = Config(host_id="cli-atomic-binding", db_path=db_path)
    init_store(db_path)
    worker = Worker(id="worker-atomic", name="Worker Atomic", status="active")

    def binding(target: str, observed_at: str) -> WorkerBinding:
        return WorkerBinding(
            host_id=config.host_id,
            worker_id=worker.id,
            worker_fingerprint=worker.fingerprint,
            backend="herdr",
            target_kind="terminal_id",
            target_value=target,
            sendable=True,
            observed_at=observed_at,
            private_fingerprint="same-private-owner",
        )

    observations = {
        "older-observer": SimpleNamespace(
            spaces=[],
            workers=[worker],
            bindings=[binding("older-private-target", "2026-01-01T00:00:00+00:00")],
            backend_health=[
                herdr_cli.herdr_backend_health(
                    "healthy_non_empty",
                    observed_at="2026-01-01T00:00:00+00:00",
                    workers=[worker],
                )
            ],
        ),
        "newer-observer": SimpleNamespace(
            spaces=[],
            workers=[worker],
            bindings=[binding("newer-private-target", "2026-01-01T00:00:01+00:00")],
            backend_health=[
                herdr_cli.herdr_backend_health(
                    "healthy_non_empty",
                    observed_at="2026-01-01T00:00:01+00:00",
                    workers=[worker],
                )
            ],
        ),
    }
    monkeypatch.setattr(
        "tendwire.cli.fetch_herdr_snapshot_observation",
        lambda _config, *, stored_bindings: observations[threading.current_thread().name],
    )
    original_upsert = store_sqlite._upsert_worker_bindings_conn
    older_inside_transaction = threading.Event()
    release_older = threading.Event()

    def delayed_upsert(conn: sqlite3.Connection, bindings: Any) -> int:
        binding_list = list(bindings)
        if binding_list and binding_list[0].target_value == "older-private-target":
            older_inside_transaction.set()
            assert release_older.wait(timeout=10)
        return original_upsert(conn, binding_list)

    monkeypatch.setattr(store_sqlite, "_upsert_worker_bindings_conn", delayed_upsert)
    errors: list[BaseException] = []

    def observe() -> None:
        try:
            observe_public_snapshot(config, store_snapshot=True)
        except BaseException as exc:
            errors.append(exc)

    older = threading.Thread(target=observe, name="older-observer")
    newer = threading.Thread(target=observe, name="newer-observer")
    older.start()
    assert older_inside_transaction.wait(timeout=10)
    newer.start()
    time.sleep(0.05)
    release_older.set()
    older.join(timeout=10)
    newer.join(timeout=10)

    assert not errors
    assert not older.is_alive() and not newer.is_alive()
    stored = list_worker_bindings(
        db_path,
        config.host_id,
        backend="herdr",
        include_expired=True,
    )
    assert len(stored) == 1
    assert stored[0].target_value == "newer-private-target"


def test_cli_legacy_observation_cannot_claim_complete_authority(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "legacy-observation.db"
    init_store(db_path)
    config = Config(host_id="cli-legacy", db_path=db_path)
    worker = Worker(id="worker-1", name="Worker One", status="blocked")
    captured: list[SnapshotObservationContext] = []
    captured_atomic: list[tuple[list[WorkerBinding], str | None]] = []
    captured_turn_models: list[str] = []

    monkeypatch.setattr(
        "tendwire.cli.fetch_herdr_state",
        lambda _config, **_kwargs: ([], [worker]),
    )

    def _capture_save(
        _db_path: Path,
        _snapshot: Snapshot,
        *,
        turn_model: str,
        observation: SnapshotObservationContext,
        worker_bindings: list[WorkerBinding],
        binding_backend: str | None,
        binding_observation_authoritative: bool,
        binding_workers_present: bool,
    ) -> bool:
        captured_turn_models.append(turn_model)
        captured.append(observation)
        captured_atomic.append((worker_bindings, binding_backend))
        return True

    monkeypatch.setattr("tendwire.store.sqlite.save_snapshot", _capture_save)

    observe_public_snapshot(config, store_snapshot=True)

    assert len(captured) == 1
    assert captured[0].authority == "none"
    assert captured_atomic == [([], "herdr")]
    assert captured_turn_models == [config.turn_model]


def test_cli_attention_json_reads_store_backed_lifecycle(
    tmp_path: Path,
    capsys,
) -> None:
    db_path = tmp_path / "attention.db"
    socket_path = tmp_path / "absent.sock"
    config = Config(host_id="cli-attention", db_path=db_path)
    snapshot = project_from_raw(
        config,
        workers=[
            {
                "id": "worker-1",
                "name": "Worker One",
                "status": "blocked",
                "meta": {
                    "safe": "kept",
                    "pane_id": "sentinel-private-pane",
                    "terminalId": "sentinel-private-terminal",
                    "backendTarget": "sentinel-private-backend",
                    "authToken": "sentinel-private-token",
                },
            }
        ],
        backend_health=[
            {
                "name": "herdr",
                "status": "healthy",
                "outcome": "healthy_non_empty",
                "observed_at": "2026-01-01T00:00:00+00:00",
                "counts": {"workers": 1},
            }
        ],
        timestamp=datetime.fromisoformat("2026-01-01T00:00:00+00:00"),
    )
    save_snapshot(
        db_path,
        snapshot,
        observation=SnapshotObservationContext(
            authority="complete",
            observed_at="2026-01-01T00:00:00+00:00",
        ),
    )

    code = main(
        [
            "--host-id",
            "cli-attention",
            "--socket-path",
            str(socket_path),
            "attention",
            "--json",
            "--db-path",
            str(db_path),
        ]
    )
    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert code == 0
    assert captured.err == ""
    assert payload["host_id"] == "cli-attention"
    assert payload["attention"][0]["lifecycle_status"] == "open"
    assert payload["attention"][0]["first_seen_at"] == "2026-01-01T00:00:00+00:00"
    assert payload["attention"][0]["signal_count"] == 1
    assert len(payload["attention"]) == 1
    assert not {
        "family_key",
        "generation",
        "first_missing_at",
        "missing_observation_count",
        "last_accepted_at",
        "last_observation_key",
        "max_notified_severity_rank",
    }.intersection(payload["attention"][0])
    assert "sentinel-private" not in json.dumps(payload, sort_keys=True)
    _assert_no_public_json_forbidden(payload)


def test_cli_attention_json_falls_back_to_snapshot_when_store_is_unavailable(
    tmp_path: Path,
    capsys,
    monkeypatch,
) -> None:
    db_path = tmp_path / "missing.db"
    socket_path = tmp_path / "absent.sock"

    def _fake_herdr_state(config):
        return [], [
            Worker(
                id="worker-1",
                name="Worker One",
                status="blocked",
                meta={"pane_id": "sentinel-private-pane"},
            )
        ]

    monkeypatch.setattr("tendwire.cli.fetch_herdr_state", _fake_herdr_state)

    code = main(
        [
            "--host-id",
            "cli-attention-fallback",
            "--socket-path",
            str(socket_path),
            "attention",
            "--json",
            "--db-path",
            str(db_path),
        ]
    )
    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert code == 0
    assert captured.err == ""
    assert len(payload["attention"]) == 1
    assert payload["attention"][0]["status"] == "blocked"
    assert "first_seen_at" not in payload["attention"][0]
    assert "sentinel-private" not in json.dumps(payload, sort_keys=True)
    _assert_no_public_json_forbidden(payload)


def test_cli_public_json_does_not_emit_connector_private_store_rows(
    tmp_path: Path,
    capsys,
    monkeypatch,
) -> None:
    db_path = tmp_path / "connector-private.db"
    init_store(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            """
            INSERT INTO connector_outbox (
                host_id, connector, delivery_key, status, payload_json,
                private_state_json, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "public-host",
                "sentinel-connector-private",
                "sentinel-delivery-key",
                "queued",
                '{"safe":"kept"}',
                '{"chat_id":"sentinel-chat","route":"sentinel-route"}',
                "2026-01-01T00:00:00+00:00",
                "2026-01-01T00:00:00+00:00",
            ),
        )
        outbox_id = int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])
        conn.execute(
            """
            INSERT INTO connector_deliveries (
                outbox_id, host_id, connector, delivery_key, attempt, status,
                response_json, private_state_json, created_at, delivered_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                outbox_id,
                "public-host",
                "sentinel-connector-private",
                "sentinel-delivery-key",
                1,
                "delivered",
                '{"ok":true}',
                '{"message_id":"sentinel-message","token":"sentinel-token"}',
                "2026-01-01T00:00:00+00:00",
                "2026-01-01T00:00:01+00:00",
            ),
        )

    payloads: list[dict[str, Any]] = []

    snapshot_code = main(
        [
            "--host-id",
            "public-host",
            "--herdr-bin",
            "definitely-not-a-real-herdr-binary",
            "snapshot",
            "--json",
            "--store",
            "--db-path",
            str(db_path),
        ]
    )
    snapshot_captured = capsys.readouterr()
    payloads.append(json.loads(snapshot_captured.out))

    turns_code = main(
        [
            "--host-id",
            "public-host",
            "--herdr-bin",
            "definitely-not-a-real-herdr-binary",
            "turns",
            "--json",
        ]
    )
    turns_captured = capsys.readouterr()
    payloads.append(json.loads(turns_captured.out))

    pending_code = main(
        [
            "--host-id",
            "public-host",
            "--herdr-bin",
            "definitely-not-a-real-herdr-binary",
            "pending",
            "--json",
            "--db-path",
            str(db_path),
        ]
    )
    pending_captured = capsys.readouterr()
    payloads.append(json.loads(pending_captured.out))

    doctor_code = main(
        [
            "--host-id",
            "public-host",
            "--herdr-bin",
            "definitely-not-a-real-herdr-binary",
            "doctor",
            "--json",
        ]
    )
    doctor_captured = capsys.readouterr()
    payloads.append(json.loads(doctor_captured.out))

    monkeypatch.setattr(
        "sys.stdin",
        io.StringIO(json.dumps({"schema_version": 1, "action": "read_snapshot"})),
    )
    command_code = main(
        [
            "--host-id",
            "public-host",
            "--herdr-bin",
            "definitely-not-a-real-herdr-binary",
            "command",
            "--json",
            "--db-path",
            str(db_path),
        ]
    )
    command_captured = capsys.readouterr()
    payloads.append(json.loads(command_captured.out))

    with sqlite3.connect(str(db_path)) as conn:
        private_counts = (
            conn.execute("SELECT COUNT(*) FROM connector_outbox").fetchone()[0],
            conn.execute("SELECT COUNT(*) FROM connector_deliveries").fetchone()[0],
        )

    encoded = json.dumps(payloads, sort_keys=True)
    assert snapshot_code == 0
    assert turns_code == 1
    assert pending_code == 0
    assert doctor_code == 1
    assert command_code == 0
    assert private_counts == (1, 1)
    assert "sentinel-" not in encoded


def test_cli_snapshot_store_persists_private_bindings_outside_snapshot_payload(
    tmp_path: Path,
    capsys,
    monkeypatch,
) -> None:
    db_path = tmp_path / "bindings.db"
    responses = {
        ("workspace", "list"): {
            "result": {
                "workspaces": [
                    {"workspace_id": "wA", "label": "Bindings"}
                ]
            }
        },
        ("agent", "list"): {
            "result": {
                "agents": [
                    {
                        "worker_id": "public-worker",
                        "agent_id": "agent-secret",
                        "agent": "Worker",
                        "workspace_id": "wA",
                        "pane_id": "wA:p1",
                    }
                ]
            }
        },
        ("pane", "list"): {
            "result": {
                "panes": [
                    {
                        "workspace_id": "wA",
                        "pane_id": "wA:p1",
                        "terminal_id": "terminal-secret",
                        "agent": "Worker",
                    }
                ]
            }
        },
    }

    def _fake_run_herdr(args, cfg):
        if tuple(args) in responses:
            return subprocess.CompletedProcess(
                args=list(args),
                returncode=0,
                stdout=json.dumps(responses[tuple(args)]),
                stderr="",
            )
        return subprocess.CompletedProcess(args=list(args), returncode=1, stdout="", stderr="")

    monkeypatch.setattr(herdr_cli.shutil, "which", lambda _: "/usr/bin/herdr")
    monkeypatch.setattr(herdr_cli, "_run_herdr", _fake_run_herdr)

    code = main(
        [
            "--host-id",
            "cli-bindings",
            "--herdr-bin",
            "herdr",
            "snapshot",
            "--db-path",
            str(db_path),
            "--json",
            "--store",
        ]
    )
    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    bindings = list_worker_bindings(db_path, "cli-bindings", backend="herdr")

    assert code == 0
    assert len(bindings) == 1
    assert bindings[0].worker_id == "public-worker"
    assert bindings[0].target_kind == "agent_id"
    assert bindings[0].target_value == "agent-secret"
    encoded = json.dumps(payload)
    assert "agent-secret" not in encoded
    assert "wA:p1" not in encoded
    assert "target_kind" not in encoded


def test_cli_module_invocation() -> None:
    """python -m tendwire.cli snapshot --json works."""
    env = os.environ.copy()
    env["PYTHONPATH"] = os.path.join(os.path.dirname(__file__), "..", "src")
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "tendwire.cli",
            "--host-id",
            "module-host",
            "--herdr-bin",
            "definitely-not-a-real-herdr-binary",
            "snapshot",
            "--json",
        ],
        capture_output=True,
        text=True,
        check=False,
        env=env,
    )
    assert result.returncode == 0, result.stderr
    turn_model = env.get("TENDWIRE_TURN_MODEL", "observed").strip().lower()
    expected_stderr = (
        ""
        if turn_model == "observed"
        else f"turn_model={turn_model} is a compatibility alias and behaves as observed\n"
    )
    assert result.stderr == expected_stderr
    payload = json.loads(result.stdout)
    assert payload["schema_version"] == 2
    assert payload["host_id"] == "module-host"
    assert len(payload["content_fingerprint"]) == 24


def test_cli_snapshot_with_live_shaped_herdr_fixtures(capsys, monkeypatch) -> None:
    """CLI emits schema v2 JSON with non-empty spaces and workers from Herdr fixtures."""

    def _fake_run_herdr(args, cfg):
        if tuple(args) == ("workspace", "list", "--json"):
            return subprocess.CompletedProcess(
                args=list(args),
                returncode=0,
                stdout=json.dumps({
                    "result": {
                        "workspaces": [
                            {
                                "workspace_id": "wA",
                                "label": "CLI Space",
                                "agent_status": "working",
                                "focused": True,
                            }
                        ]
                    }
                }),
                stderr="",
            )
        if tuple(args) == ("agent", "list", "--json"):
            return subprocess.CompletedProcess(
                args=list(args),
                returncode=0,
                stdout=json.dumps({
                    "result": {
                        "agents": [
                            {
                                "agent_session": {"value": "sess-cli"},
                                "agent": "CLI Agent",
                                "workspace_id": "wA",
                                "pane_id": "wA:p1",
                                "agent_status": "executing",
                                "cwd": "/tmp",
                            }
                        ]
                    }
                }),
                stderr="",
            )
        if tuple(args) == ("pane", "list"):
            return subprocess.CompletedProcess(
                args=list(args),
                returncode=0,
                stdout=json.dumps({
                    "result": {
                        "panes": [
                            {
                                "workspace_id": "wA",
                                "pane_id": "wA:p1",
                                "terminal_id": "terminal-cli",
                                "agent": "CLI Agent",
                                "agent_session": {"value": "sess-cli"},
                                "agent_status": "executing",
                            }
                        ]
                    }
                }),
                stderr="",
            )
        return subprocess.CompletedProcess(args=list(args), returncode=1, stdout="", stderr="")

    monkeypatch.setattr(herdr_cli.shutil, "which", lambda _: "/usr/bin/herdr")
    monkeypatch.setattr(herdr_cli, "_run_herdr", _fake_run_herdr)

    code = main(["--host-id", "cli-live", "--herdr-bin", "herdr", "snapshot", "--json"])
    captured = capsys.readouterr()

    assert code == 0
    assert captured.err == ""
    payload = json.loads(captured.out)
    assert payload["schema_version"] == 2
    assert payload["host_id"] == "cli-live"
    assert len(payload["spaces"]) == 1
    assert payload["spaces"][0]["id"] == "wA"
    assert payload["spaces"][0]["status"] == "active"
    assert len(payload["workers"]) == 1
    assert payload["workers"][0]["id"] == "CLI Agent"
    assert payload["workers"][0]["status"] == "active"
    assert payload["backend_health"][0]["name"] == "herdr"
    assert payload["backend_health"][0]["status"] == "healthy"
    assert payload["backend_health"][0]["outcome"] == "healthy_non_empty"
    assert payload["backend_health"][0]["counts"] == {"spaces": 1, "workers": 1}
    assert "agent_session" not in json.dumps(payload)
    assert "sess-cli" not in json.dumps(payload)


def test_cli_store_compact_parser_requires_exactly_one_mode() -> None:
    parser = _build_parser()
    parsed = parser.parse_args(
        [
            "store",
            "compact",
            "--db-path",
            "private.db",
            "--dry-run",
            "--snapshot-retention-days",
            "9",
            "--snapshot-retention-count",
            "17",
            "--batch-size",
            "3",
        ]
    )

    assert parsed.store_action == "compact"
    assert parsed.compact_dry_run is True
    assert parsed.compact_execute is False
    assert parsed.snapshot_retention_days == 9
    assert parsed.snapshot_retention_count == 17
    assert parsed.snapshot_batch_size == 3
    with pytest.raises(SystemExit):
        parser.parse_args(["store", "compact"])
    with pytest.raises(SystemExit):
        parser.parse_args(
            ["store", "compact", "--dry-run", "--execute"]
        )


@pytest.mark.parametrize(
    "extra",
    [
        ["--execute"],
        ["--execute", "--acknowledge-offline"],
        ["--execute", "--backup-path", "private-backup.db"],
        ["--dry-run", "--acknowledge-offline"],
        ["--dry-run", "--backup-path", "private-backup.db"],
        ["--dry-run", "--batch-size", "0"],
    ],
)
def test_cli_store_compact_rejects_invalid_authority_as_one_json_object(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    extra: list[str],
) -> None:
    db_path = tmp_path / "must-not-be-opened.db"

    code = main(
        [
            "store",
            "compact",
            "--db-path",
            str(db_path),
            *extra,
        ]
    )
    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert code == 1
    assert captured.err == ""
    assert payload["status"] == "invalid_request"
    assert payload["command"] == "store.compact"
    assert not db_path.exists()


def test_cli_store_compact_dry_run_is_read_only_json_and_skips_generic_repair(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "compact-dry-run.db"
    init_store(db_path)
    before = tuple(
        sorted(
            (
                path.name,
                path.stat().st_size,
                path.stat().st_mtime_ns,
            )
            for path in tmp_path.iterdir()
        )
    )

    def forbidden_repair(*_args: Any, **_kwargs: Any) -> None:
        raise AssertionError("compact must not run generic permission repair")

    monkeypatch.setattr("tendwire.cli.repair_config_state", forbidden_repair)
    code = main(
        [
            "store",
            "compact",
            "--db-path",
            str(db_path),
            "--dry-run",
        ]
    )
    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    after = tuple(
        sorted(
            (
                path.name,
                path.stat().st_size,
                path.stat().st_mtime_ns,
            )
            for path in tmp_path.iterdir()
        )
    )

    assert code == 0
    assert captured.err == ""
    assert payload["status"] == "dry_run"
    assert payload["ok"] is True
    assert payload["backup"]["created"] is False
    assert after == before


def test_cli_store_compact_execute_success_is_public_safe_and_retains_backup(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    db_path = tmp_path / "sentinel-private-store-name.db"
    backup_path = tmp_path / "sentinel-private-backup-name.db"
    init_store(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute("CREATE TABLE compact_private (value TEXT NOT NULL)")
        conn.execute(
            "INSERT INTO compact_private (value) VALUES (?)",
            ("sentinel-private-payload",),
        )

    code = main(
        [
            "store",
            "compact",
            "--db-path",
            str(db_path),
            "--execute",
            "--acknowledge-offline",
            "--backup-path",
            str(backup_path),
            "--snapshot-retention-days",
            "7",
            "--snapshot-retention-count",
            "2",
            "--batch-size",
            "1",
        ]
    )
    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    encoded = json.dumps(payload, sort_keys=True)

    assert code == 0
    assert captured.err == ""
    assert payload["status"] == "completed"
    assert payload["command"] == "store.compact"
    assert payload["backup"] == {
        "required": True,
        "created": True,
        "verified": True,
    }
    assert backup_path.is_file()
    with sqlite3.connect(str(db_path)) as conn:
        assert conn.execute(
            "SELECT value FROM compact_private"
        ).fetchone()[0] == "sentinel-private-payload"
        assert conn.execute("PRAGMA quick_check").fetchone()[0] == "ok"
    for private_value in (
        str(db_path),
        db_path.name,
        str(backup_path),
        backup_path.name,
        "sentinel-private-payload",
    ):
        assert private_value not in encoded


def test_cli_store_cleanup_passes_snapshot_policy_overrides(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "cleanup-policy.db"
    init_store(db_path)
    captured_options: dict[str, Any] = {}

    def capture_maintenance(
        _db_path: Path,
        _host_id: str,
        **kwargs: Any,
    ) -> dict[str, Any]:
        captured_options.update(kwargs)
        return {"schema_version": 1, "ok": True, "status": "ok"}

    monkeypatch.setattr(
        "tendwire.cli.run_store_maintenance",
        capture_maintenance,
    )
    code = main(
        [
            "store",
            "cleanup",
            "--db-path",
            str(db_path),
            "--dry-run",
            "--acknowledged-final-retention-days",
            "17",
            "--acknowledged-final-retention-count",
            "29",
            "--snapshot-retention-days",
            "11",
            "--snapshot-retention-count",
            "23",
            "--snapshot-batch-size",
            "5",
        ]
    )
    captured = capsys.readouterr()

    assert code == 0
    assert captured.err == ""
    assert json.loads(captured.out)["ok"] is True
    assert captured_options["dry_run"] is True
    assert captured_options["acknowledged_final_retention_days"] == 17
    assert captured_options["acknowledged_final_retention_count"] == 29
    assert captured_options["snapshot_retention_days"] == 11
    assert captured_options["snapshot_retention_count"] == 23
    assert captured_options["snapshot_batch_size"] == 5


def test_cli_store_status_passes_configured_maintenance_policy(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "status-policy.db"
    init_store(db_path)
    monkeypatch.setenv("TENDWIRE_SNAPSHOT_RETENTION_DAYS", "19")
    monkeypatch.setenv("TENDWIRE_SNAPSHOT_RETENTION_COUNT", "211")
    monkeypatch.setenv("TENDWIRE_SNAPSHOT_MAINTENANCE_BATCH_SIZE", "37")
    monkeypatch.setenv("TENDWIRE_STORE_MAINTENANCE_CADENCE_SECONDS", "7200")
    monkeypatch.setenv("TENDWIRE_ACKNOWLEDGED_FINAL_RETENTION_DAYS", "31")
    monkeypatch.setenv("TENDWIRE_ACKNOWLEDGED_FINAL_RETENTION_COUNT", "422")
    received: dict[str, Any] = {}

    def capture_status(
        _db_path: Path,
        _host_id: str,
        **kwargs: Any,
    ) -> dict[str, Any]:
        received.update(kwargs)
        return {"schema_version": 1, "ok": True, "status": "ok"}

    monkeypatch.setattr("tendwire.cli.store_status", capture_status)
    code = main(
        [
            "store",
            "status",
            "--db-path",
            str(db_path),
        ]
    )
    captured = capsys.readouterr()

    assert code == 0
    assert captured.err == ""
    assert json.loads(captured.out)["ok"] is True
    assert received == {
        "acknowledged_final_retention_days": 31,
        "acknowledged_final_retention_count": 422,
        "snapshot_retention_days": 19,
        "snapshot_retention_count": 211,
        "maintenance_batch_size": 37,
        "maintenance_cadence_seconds": 7200,
        "command_retry_horizon_seconds": 604800,
        "command_receipt_retention_seconds": 2592000,
        "command_receipt_retention_count": 4096,
    }


def test_cli_doctor_with_absent_sqlite_sidecars_is_noncreating(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_dir = tmp_path / "private-cli-doctor-state"
    state_dir.mkdir(mode=0o700)
    db_path = state_dir / "private-cli-doctor-database"
    init_store(db_path)
    sidecars = tuple(
        Path(f"{db_path}{suffix}") for suffix in ("-wal", "-shm", "-journal")
    )
    assert all(not os.path.lexists(path) for path in sidecars)
    before = (
        tuple(sorted(path.name for path in state_dir.iterdir())),
        db_path.stat().st_ino,
        db_path.stat().st_size,
        db_path.stat().st_mtime_ns,
    )
    monkeypatch.setenv("TENDWIRE_DATA_DIR", str(state_dir))
    monkeypatch.setenv("TENDWIRE_DB_PATH", str(db_path))

    def forbidden_repair(*_args: Any, **_kwargs: Any) -> None:
        raise AssertionError("doctor must remain validation-only")

    monkeypatch.setattr("tendwire.cli.repair_config_state", forbidden_repair)
    code = main(
        [
            "--host-id",
            "doctor-host",
            "--herdr-bin",
            "definitely-not-a-real-herdr-binary",
            "doctor",
            "--json",
        ]
    )
    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    database_check = next(
        check for check in payload["checks"] if check["name"] == "database_permissions"
    )
    after = (
        tuple(sorted(path.name for path in state_dir.iterdir())),
        db_path.stat().st_ino,
        db_path.stat().st_size,
        db_path.stat().st_mtime_ns,
    )

    assert code == 1
    assert captured.err == ""
    assert database_check == {
        "name": "database_permissions",
        "ok": True,
        "outcome": "compliant",
        "remediation": "No action required.",
    }
    assert after == before
    assert all(not os.path.lexists(path) for path in sidecars)


@pytest.mark.parametrize("hostile_member", ["main", "wal"])
def test_cli_doctor_sqlite_failures_are_one_fixed_path_free_record(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
    hostile_member: str,
) -> None:
    state_dir = tmp_path / "private-cli-hostile-state"
    state_dir.mkdir(mode=0o700)
    db_path = state_dir / "private-cli-hostile-database"
    target = state_dir / "private-cli-hostile-target"
    private_contents = b"raw-OSError-private-cli-target"
    target.write_bytes(private_contents)
    os.chmod(target, 0o600)
    if hostile_member == "main":
        hostile_path = db_path
    else:
        init_store(db_path)
        hostile_path = Path(f"{db_path}-wal")
    hostile_path.symlink_to(target)
    hostile_inode = str(os.lstat(hostile_path).st_ino)
    target_before = target.read_bytes()
    monkeypatch.setenv("TENDWIRE_DATA_DIR", str(state_dir))
    monkeypatch.setenv("TENDWIRE_DB_PATH", str(db_path))

    def forbidden_repair(*_args: Any, **_kwargs: Any) -> None:
        raise AssertionError("doctor must not repair hostile SQLite entries")

    monkeypatch.setattr("tendwire.cli.repair_config_state", forbidden_repair)
    code = main(
        [
            "--host-id",
            "doctor-host",
            "--herdr-bin",
            "definitely-not-a-real-herdr-binary",
            "doctor",
            "--json",
        ]
    )
    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    database_check = [
        check for check in payload["checks"] if check["name"] == "database_permissions"
    ]

    assert code == 1
    assert captured.err == ""
    assert database_check == [
        {
            "name": "database_permissions",
            "ok": False,
            "outcome": "unsafe",
            "remediation": "Move unsafe local state aside and restore from a trusted backup.",
        }
    ]
    assert hostile_path.is_symlink()
    assert target.read_bytes() == target_before
    serialized = json.dumps(payload, sort_keys=True)
    for forbidden in (
        str(state_dir),
        str(db_path),
        db_path.name,
        str(hostile_path),
        hostile_path.name,
        str(target),
        target.name,
        private_contents.decode(),
        hostile_inode,
        "-wal",
        "-shm",
        "-journal",
        "OSError",
        "[Errno",
        '"uid"',
        '"gid"',
        '"inode"',
    ):
        assert forbidden not in serialized
