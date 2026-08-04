"""Daemon and CLI coverage for connector JSON boundary."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import closing, contextmanager
from datetime import datetime, timedelta, timezone
import io
import json
import sqlite3
import threading
from pathlib import Path
from typing import Any

import pytest

from tendwire.cli import main
from tendwire.connectors import ConnectorOutboxAPI
from tendwire.config import Config
from tendwire.core.models import Snapshot
from tendwire.daemon import TendwireDaemon
from tendwire.daemon_api import (
    DaemonAPIClient,
    TendwireDaemonAPI,
    UnixSocketJSONServer,
)
from tendwire.store import sqlite as store_sqlite
from tendwire.store.sqlite import init_store



@pytest.fixture(autouse=True)
def _isolate_cli_state(tmp_path: Path, monkeypatch) -> None:
    private_home = tmp_path / "isolated-home"
    private_home.mkdir(mode=0o700)
    data_dir = tmp_path / "isolated-data"
    monkeypatch.setenv("HOME", str(private_home))
    monkeypatch.setenv("TENDWIRE_DATA_DIR", str(data_dir))
    monkeypatch.setenv("TENDWIRE_DB_PATH", str(data_dir / "tendwire.db"))


def _enqueue(db_path: Path, *, host_id: str = "host-a", key: str = "job-1") -> None:
    init_store(db_path)
    with closing(sqlite3.connect(str(db_path))) as conn, conn:
        conn.execute(
            """
            INSERT INTO connector_outbox (
                host_id, connector, delivery_key, status, payload_json,
                private_state_json, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                host_id,
                "attention",
                key,
                "queued",
                json.dumps(
                    {
                        "schema_version": 1,
                        "event_type": "attention_escalated",
                        "safe": "kept",
                        "transport": "telegram",
                        "chat_id": "must-strip",
                        "nested": {"backend_value": "herdres", "safe": "nested"},
                    }
                ),
                json.dumps({"message_id": "private"}),
                "2026-01-01T00:00:00+00:00",
                "2026-01-01T00:00:00+00:00",
            ),
        )


def _canonical_final_turn(db_path: Path, *, host_id: str) -> tuple[str, str]:
    init_store(db_path)
    turn_id = "turn-worker-contract-source-contract"
    final_text = "abcdefgh"
    revision = store_sqlite.content_revision(
        turn_id,
        None,
        final_text,
        "absent",
        "complete",
    )
    created_at = "2026-01-01T00:00:00+00:00"
    with closing(sqlite3.connect(str(db_path))) as conn, conn:
        conn.execute(
            """
            INSERT INTO turns (
                host_id, turn_id, worker_id, status, kind, updated_at,
                fingerprint, snapshot_content_fingerprint, observed_at,
                payload_json, list_sequence
            ) VALUES (?, ?, ?, 'complete', 'turn', ?, ?, ?, ?, ?, 1)
            """,
            (
                host_id,
                turn_id,
                "worker-contract",
                created_at,
                "fingerprint-contract",
                "snapshot-contract",
                created_at,
                json.dumps(
                    {
                        "source_turn_id": "source-contract",
                        "complete": True,
                        "meta": {
                            "stable_key": "wsk1_" + ("c" * 64),
                            "stable_key_version": 1,
                        },
                    }
                ),
            ),
        )
        conn.execute(
            """
            INSERT INTO turn_content_revisions (
                host_id, turn_id, content_revision, user_text,
                assistant_final_text, user_state, final_state,
                user_char_length, user_byte_length,
                final_char_length, final_byte_length,
                user_page_count, final_page_count,
                is_current, created_at, superseded_at
            ) VALUES (?, ?, ?, NULL, ?, 'absent', 'complete', 0, 0, 8, 8,
                      0, 1, 1, ?, NULL)
            """,
            (host_id, turn_id, revision, final_text, created_at),
        )
        source_id = store_sqlite._ensure_final_ready_anchor_conn(
            conn,
            host_id=host_id,
            turn_id=turn_id,
            content_revision_value=revision,
            now=created_at,
        )
        assert source_id is not None
    return turn_id, revision


def _assert_json_only_and_safe(payload: dict[str, Any]) -> None:
    encoded = json.dumps(payload, sort_keys=True).lower()
    for forbidden in (
        "private_state_json",
        "backend_target",
        "chat_id",
        "topic_id",
        "message_id",
        "bot_token",
        "telegram",
        "herdres",
        "pane_id",
        "session_id",
        "terminal_id",
        "shell",
        "argv",
        "connector",
        "delivery",
    ):
        assert forbidden not in encoded


@contextmanager
def _socket_client(
    tmp_path: Path,
    api: TendwireDaemonAPI,
) -> Iterator[DaemonAPIClient]:
    socket_path = tmp_path / "s"
    server = UnixSocketJSONServer(socket_path, api.dispatch)
    server.start()
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield DaemonAPIClient(socket_path, timeout_seconds=2)
    finally:
        server.close()
        thread.join(timeout=2)
        assert not thread.is_alive()


@pytest.mark.skipif(
    not hasattr(__import__("socket"), "AF_UNIX"),
    reason="Unix sockets required",
)
def test_socket_ack_lost_repoll_preserves_durable_identity_and_payload(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "ack-lost.db"
    binding_db = tmp_path / "provider-bindings.db"
    turn_id, revision = _canonical_final_turn(db_path, host_id="daemon-host")
    current = datetime(2026, 1, 1, tzinfo=timezone.utc)
    monkeypatch.setattr(
        store_sqlite,
        "utc_timestamp",
        lambda: current.isoformat(timespec="seconds"),
    )
    first_outbox = ConnectorOutboxAPI(db_path, "daemon-host")
    first_api = TendwireDaemonAPI(
        get_snapshot=lambda: Snapshot(host_id="daemon-host"),
        get_health=lambda: {},
        submit_command=lambda _params: {},
        connector_call=first_outbox.dispatch,
    )

    with closing(sqlite3.connect(str(binding_db))) as conn, conn:
        conn.execute(
            """
            CREATE TABLE provider_message_bindings (
                delivery_key TEXT PRIMARY KEY,
                provider_message_id TEXT NOT NULL,
                payload_json TEXT NOT NULL
            )
            """
        )

    with _socket_client(tmp_path, first_api) as client:
        source_outer = client.request(
            "connector.poll",
            {"name": "turn-final", "lease_seconds": 5},
        )
        source = source_outer["result"]["items"][0]
        assert source["payload"]["operation"] == "materialize"
        assert source["payload"]["content_revision"] == revision
        begun = client.request(
            "connector.prepare",
            {
                "schema_version": 1,
                "action": "begin",
                "name": "turn-final",
                "turn_id": turn_id,
                "content_revision": revision,
                "presentation_version": "contract-v2",
                "part_count": 2,
                "source_ref": source["ref"],
            },
        )
        plan_token = begun["result"]["plan_token"]
        for ordinal, start in enumerate((0, 4)):
            staged = client.request(
                "connector.prepare",
                {
                    "schema_version": 1,
                    "action": "part",
                    "name": "turn-final",
                    "plan_token": plan_token,
                    "ordinal": ordinal,
                    "spans": [
                        {
                            "field": "assistant_final_text",
                            "start_char": start,
                            "end_char": start + 4,
                        }
                    ],
                },
            )
            assert staged["ok"] is True
            assert staged["result"]["ok"] is True
        committed = client.request(
            "connector.prepare",
            {
                "schema_version": 1,
                "action": "commit",
                "name": "turn-final",
                "plan_token": plan_token,
                "source_ref": source["ref"],
            },
        )
        assert begun["ok"] is True
        assert begun["result"]["ok"] is True
        assert committed["ok"] is True
        assert committed["result"]["ok"] is True
        assert committed["result"]["job_count"] == 2
        with closing(sqlite3.connect(str(db_path))) as conn:
            source_state = conn.execute(
                """
                SELECT outbox.status, attempts.status
                FROM connector_outbox AS outbox
                JOIN connector_deliveries AS attempts
                  ON attempts.outbox_id = outbox.id
                WHERE outbox.delivery_key = ?
                """,
                (source["key"],),
            ).fetchone()
        assert source_state == ("awaiting_ack", "awaiting_ack")

        first_outer = client.request(
            "connector.poll",
            {"name": "turn-final", "lease_seconds": 5},
        )
        first = first_outer["result"]["items"][0]

        # The provider accepted the message and Herdres persisted the binding,
        # but the connector ACK was lost before Tendwire received it.
        with closing(sqlite3.connect(str(binding_db))) as conn, conn:
            conn.execute(
                """
                INSERT INTO provider_message_bindings (
                    delivery_key, provider_message_id, payload_json
                ) VALUES (?, ?, ?)
                """,
                (
                    first["key"],
                    "provider-message-17",
                    json.dumps(
                        first["payload"],
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                ),
            )

    # The connector and its socket server have stopped. Advance beyond the lease,
    # then construct fresh connector/server objects from the durable databases.
    current += timedelta(seconds=6)
    restarted_outbox = ConnectorOutboxAPI(db_path, "daemon-host")
    restarted_api = TendwireDaemonAPI(
        get_snapshot=lambda: Snapshot(host_id="daemon-host"),
        get_health=lambda: {},
        submit_command=lambda _params: {},
        connector_call=restarted_outbox.dispatch,
    )

    with _socket_client(tmp_path, restarted_api) as client:
        reclaimed = client.request("connector.reclaim", {"name": "turn-final"})
        second_outer = client.request(
            "connector.poll",
            {"name": "turn-final", "lease_seconds": 5},
        )
        second = second_outer["result"]["items"][0]

        assert first_outer["ok"] is True
        assert first_outer["result"]["ok"] is True
        assert reclaimed["ok"] is True
        assert reclaimed["result"]["reclaimed"] == 1
        assert second["key"] == first["key"]
        assert second["payload"] == first["payload"]
        assert json.dumps(
            second["payload"], sort_keys=True, separators=(",", ":")
        ) == json.dumps(first["payload"], sort_keys=True, separators=(",", ":"))
        assert second["ref"] != first["ref"]
        assert second["attempt"] == first["attempt"] + 1
        stale_ack = client.request(
            "connector.ack",
            {"name": "turn-final", "ref": first["ref"]},
        )
        assert stale_ack["ok"] is True
        assert stale_ack["result"]["ok"] is False
        assert stale_ack["result"]["status"] in {
            "expired_ref",
            "invalid_ref",
            "stale_ref",
        }

        # A thin connector recognizes the durable key, does not resend, and
        # acknowledges the current attempt rather than the stale first ref.
        with closing(sqlite3.connect(str(binding_db))) as conn, conn:
            durable_binding = conn.execute(
                """
                SELECT provider_message_id, payload_json
                FROM provider_message_bindings
                WHERE delivery_key = ?
                """,
                (second["key"],),
            ).fetchone()
            mutation_count_before = conn.total_changes
            if durable_binding is None:
                conn.execute(
                    """
                    INSERT INTO provider_message_bindings (
                        delivery_key, provider_message_id, payload_json
                    ) VALUES (?, ?, ?)
                    """,
                    (second["key"], "unexpected-second-send", "{}"),
                )
            mutation_count_after = conn.total_changes
            provider_row_count = conn.execute(
                "SELECT COUNT(*) FROM provider_message_bindings"
            ).fetchone()[0]
        assert durable_binding == (
            "provider-message-17",
            json.dumps(first["payload"], sort_keys=True, separators=(",", ":")),
        )
        assert mutation_count_after == mutation_count_before
        assert provider_row_count == 1
        acknowledged = client.request(
            "connector.ack",
            {
                "name": "turn-final",
                "ref": second["ref"],
                "response": {"status": "deduplicated"},
            },
        )
        assert acknowledged["ok"] is True
        assert acknowledged["result"]["ok"] is True
        assert acknowledged["result"]["status"] == "acknowledged"
        next_item = client.request("connector.poll", {"name": "turn-final"})[
            "result"
        ]["items"][0]
        assert next_item["key"] != second["key"]
        assert next_item["payload"]["plan_token"] == plan_token
        assert next_item["payload"]["sequence_index"] == 1
        assert client.request(
            "connector.ack",
            {"name": "turn-final", "ref": next_item["ref"]},
        )["result"]["ok"] is True
        assert client.request("connector.poll", {"name": "turn-final"})["result"][
            "items"
        ] == []
    with closing(sqlite3.connect(str(db_path))) as conn:
        completed_source_state = conn.execute(
            """
            SELECT outbox.status, attempts.status
            FROM connector_outbox AS outbox
            JOIN connector_deliveries AS attempts
              ON attempts.outbox_id = outbox.id
            WHERE outbox.delivery_key = ?
            """,
            (source["key"],),
        ).fetchone()
    assert completed_source_state == ("delivered", "delivered")


@pytest.mark.skipif(
    not hasattr(__import__("socket"), "AF_UNIX"),
    reason="Unix sockets required",
)
def test_socket_keeps_transport_and_connector_errors_in_separate_envelopes(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "error-envelopes.db"
    init_store(db_path)
    outbox = ConnectorOutboxAPI(db_path, "daemon-host")
    api = TendwireDaemonAPI(
        get_snapshot=lambda: Snapshot(host_id="daemon-host"),
        get_health=lambda: {},
        submit_command=lambda _params: {},
        connector_call=outbox.dispatch,
    )

    with _socket_client(tmp_path, api) as client:
        connector_error = client.request(
            "connector.ack",
            {"name": "attention", "ref": "not-a-live-ref"},
        )
        protocol_error = client.request("turn_final_ack", {})

    assert connector_error["ok"] is True
    assert connector_error["status"] == "ok"
    assert connector_error["error"] is None
    assert connector_error["result"]["ok"] is False
    assert connector_error["result"]["status"] == "invalid_ref"
    assert connector_error["result"]["error"]["code"] == "invalid_ref"
    assert protocol_error["ok"] is False
    assert protocol_error["status"] == "error"
    assert protocol_error["result"] is None
    assert protocol_error["error"]["code"] == "unknown_method"


@pytest.mark.skipif(
    not hasattr(__import__("socket"), "AF_UNIX"),
    reason="Unix sockets required",
)
def test_socket_explicit_retry_resets_generation_attempt_and_bounds_history(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "retry-generation.db"
    _canonical_final_turn(db_path, host_id="daemon-host")
    outbox = ConnectorOutboxAPI(db_path, "daemon-host", max_attempts=2)
    api = TendwireDaemonAPI(
        get_snapshot=lambda: Snapshot(host_id="daemon-host"),
        get_health=lambda: {},
        submit_command=lambda _params: {},
        connector_call=outbox.dispatch,
    )

    with _socket_client(tmp_path, api) as client:
        first = client.request("connector.poll", {"name": "turn-final"})[
            "result"
        ]["items"][0]
        assert first["attempt"] == 1
        assert client.request(
            "connector.fail",
            {"name": "turn-final", "ref": first["ref"], "delay_seconds": 0},
        )["result"]["status"] == "retry_scheduled"
        second = client.request("connector.poll", {"name": "turn-final"})[
            "result"
        ]["items"][0]
        assert second["key"] == first["key"]
        assert second["attempt"] == 2
        assert client.request(
            "connector.fail",
            {"name": "turn-final", "ref": second["ref"], "delay_seconds": 0},
        )["result"]["status"] == "attempts_exhausted"
        inspected = client.request(
            "connector.inspect",
            {
                "schema_version": 1,
                "name": "turn-final",
                "status": "dead_letter",
                "limit": 10,
            },
        )["result"]
        assert inspected["items"][0]["attempt_count"] == 2
        retried = client.request(
            "connector.retry",
            {
                "schema_version": 1,
                "name": "turn-final",
                "key": first["key"],
            },
        )["result"]
        assert retried["status"] == "requeued"
        assert retried["prior_attempt_count"] == 2

        with closing(sqlite3.connect(str(db_path))) as conn:
            compacted = conn.execute(
                """
                SELECT COUNT(attempts.id), outbox.private_state_json
                FROM connector_outbox AS outbox
                LEFT JOIN connector_deliveries AS attempts
                  ON attempts.outbox_id = outbox.id
                WHERE outbox.delivery_key = ?
                GROUP BY outbox.id
                """,
                (first["key"],),
            ).fetchone()
        assert compacted is not None
        assert compacted[0] == 0
        assert json.loads(compacted[1])["prior_attempt_count"] == 2
        fresh = client.request("connector.poll", {"name": "turn-final"})[
            "result"
        ]["items"][0]
        assert fresh["key"] == first["key"]
        assert fresh["attempt"] == 1


@pytest.mark.skipif(
    not hasattr(__import__("socket"), "AF_UNIX"),
    reason="Unix sockets required",
)
def test_socket_preserves_opaque_connector_tokens_byte_for_byte(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "token-preservation.db"
    turn_id, revision = _canonical_final_turn(db_path, host_id="daemon-host")
    with closing(sqlite3.connect(str(db_path))) as conn:
        key, raw_source_payload = conn.execute(
            """
            SELECT delivery_key, payload_json
            FROM connector_outbox
            WHERE delivery_kind = 'final_ready'
            """
        ).fetchone()
    expected_source = json.loads(raw_source_payload)
    outbox = ConnectorOutboxAPI(db_path, "daemon-host")
    api = TendwireDaemonAPI(
        get_snapshot=lambda: Snapshot(host_id="daemon-host"),
        get_health=lambda: {},
        submit_command=lambda _params: {},
        connector_call=outbox.dispatch,
    )

    with _socket_client(tmp_path, api) as client:
        source = client.request("connector.poll", {"name": "turn-final"})[
            "result"
        ]["items"][0]
        assert source["key"].encode() == key.encode()
        assert source["payload"]["final_identity"].encode() == expected_source[
            "final_identity"
        ].encode()
        assert source["payload"]["content_revision"].encode() == revision.encode()
        assert source["payload"]["content"]["content_revision"].encode() == (
            expected_source["content"]["content_revision"].encode()
        )

        begun = client.request(
            "connector.prepare",
            {
                "schema_version": 1,
                "action": "begin",
                "name": "turn-final",
                "turn_id": turn_id,
                "content_revision": revision,
                "presentation_version": "presentation-v2",
                "part_count": 1,
                "source_ref": source["ref"],
            },
        )["result"]
        assert begun["ok"] is True, begun
        plan_token = begun["plan_token"]
        assert client.request(
            "connector.prepare",
            {
                "schema_version": 1,
                "action": "part",
                "name": "turn-final",
                "plan_token": plan_token,
                "ordinal": 0,
                "spans": [
                    {
                        "field": "assistant_final_text",
                        "start_char": 0,
                        "end_char": 8,
                    }
                ],
            },
        )["result"]["ok"] is True
        assert client.request(
            "connector.prepare",
            {
                "schema_version": 1,
                "action": "commit",
                "name": "turn-final",
                "plan_token": plan_token,
                "source_ref": source["ref"],
            },
        )["result"]["ok"] is True
        part = client.request("connector.poll", {"name": "turn-final"})[
            "result"
        ]["items"][0]

    with closing(sqlite3.connect(str(db_path))) as conn:
        stored_part = json.loads(
            conn.execute(
                "SELECT payload_json FROM connector_outbox WHERE delivery_key = ?",
                (part["key"],),
            ).fetchone()[0]
        )
    for path in (
        ("plan_token",),
        ("content_revision",),
        ("turn", "final_identity"),
        ("turn", "content_revision"),
        ("turn", "content", "content_revision"),
    ):
        actual: Any = part["payload"]
        expected: Any = stored_part
        for field in path:
            actual = actual[field]
            expected = expected[field]
        assert actual.encode() == expected.encode()
    assert part["payload"]["plan_token"].encode() == plan_token.encode()


def test_daemon_api_routes_connector_methods_safely(tmp_path: Path) -> None:
    db_path = tmp_path / "daemon-connector.db"
    _enqueue(db_path, host_id="daemon-host")
    config = Config(host_id="daemon-host", db_path=db_path)
    daemon = TendwireDaemon(config)
    api = TendwireDaemonAPI(
        get_snapshot=lambda: Snapshot(host_id="daemon-host"),
        get_health=lambda: {"schema_version": 1, "status": "ok", "host_id": "daemon-host"},
        submit_command=daemon.submit_command,
        connector_call=daemon.connector_call,
    )

    poll = api.dispatch({"method": "connector.poll", "params": {"name": "attention"}})
    ref = poll["result"]["items"][0]["ref"]
    ack = api.dispatch(
        {
            "method": "connector.ack",
            "params": {
                "name": "attention",
                "ref": ref,
                "response": {"safe": "kept", "message_id": "must-strip"},
            },
        }
    )
    after = api.dispatch({"method": "connector.poll", "params": {"name": "attention"}})

    assert poll["ok"] is True
    assert poll["result"]["ok"] is True
    assert ack["result"]["status"] == "acknowledged"
    assert after["result"]["items"] == []
    _assert_json_only_and_safe(poll)
    _assert_json_only_and_safe(ack)


def test_daemon_api_routes_renew_and_release(tmp_path: Path) -> None:
    db_path = tmp_path / "daemon-renew-release.db"
    _enqueue(db_path, host_id="daemon-host", key="renew-release")
    daemon = TendwireDaemon(Config(host_id="daemon-host", db_path=db_path))
    api = TendwireDaemonAPI(
        get_snapshot=lambda: Snapshot(host_id="daemon-host"),
        get_health=lambda: {"schema_version": 1, "status": "ok", "host_id": "daemon-host"},
        submit_command=daemon.submit_command,
        connector_call=daemon.connector_call,
    )
    first = api.dispatch(
        {"method": "connector.poll", "params": {"name": "attention"}}
    )["result"]["items"][0]
    renewed = api.dispatch(
        {
            "method": "connector.renew",
            "params": {
                "name": "attention",
                "ref": first["ref"],
                "lease_seconds": 120,
            },
        }
    )
    released = api.dispatch(
        {
            "method": "connector.release",
            "params": {"name": "attention", "ref": first["ref"]},
        }
    )
    second = api.dispatch(
        {"method": "connector.poll", "params": {"name": "attention"}}
    )["result"]["items"][0]

    assert renewed["result"]["status"] == "renewed"
    assert released["result"]["status"] == "released"
    assert second["attempt"] == 2
    _assert_json_only_and_safe(renewed)
    _assert_json_only_and_safe(released)


def test_daemon_periodic_tick_reclaims_without_a_followup_poll(tmp_path: Path) -> None:
    db_path = tmp_path / "daemon-periodic-reclaim.db"
    _enqueue(db_path, host_id="daemon-host", key="expired")
    daemon = TendwireDaemon(Config(host_id="daemon-host", db_path=db_path))
    item = daemon.connector_call(
        "connector.poll",
        {"name": "attention", "lease_seconds": 60},
    )["items"][0]
    assert item["attempt"] == 1
    with sqlite3.connect(str(db_path)) as conn:
        for table in ("connector_outbox", "connector_deliveries"):
            rows = conn.execute(
                f"SELECT id, private_state_json FROM {table}"
            ).fetchall()
            for row_id, private_state_json in rows:
                private = json.loads(private_state_json)
                private["lease_expires_at"] = "2000-01-01T00:00:00+00:00"
                conn.execute(
                    f"UPDATE {table} SET private_state_json = ? WHERE id = ?",
                    (json.dumps(private), row_id),
                )

    daemon._connector_periodic_tick()

    with sqlite3.connect(str(db_path)) as conn:
        outbox_status = conn.execute(
            "SELECT status FROM connector_outbox"
        ).fetchone()[0]
        delivery_status = conn.execute(
            "SELECT status FROM connector_deliveries"
        ).fetchone()[0]
    assert outbox_status == "queued"
    assert delivery_status == "expired"


def test_daemon_periodic_tick_skips_write_reclaim_when_nothing_is_due(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "daemon-no-reclaim-due.db"
    _enqueue(db_path, host_id="daemon-host", key="not-leased")
    daemon = TendwireDaemon(Config(host_id="daemon-host", db_path=db_path))

    def reject_write(*_args: Any, **_kwargs: Any) -> None:
        raise AssertionError("write reclaim should not run without due work")

    monkeypatch.setattr(
        store_sqlite,
        "reclaim_expired_connector_leases",
        reject_write,
    )

    daemon._connector_periodic_tick()


def test_cli_connector_prepare_reads_bounded_action_from_stdin(
    tmp_path: Path,
    capsys,
    monkeypatch,
) -> None:
    private_home = tmp_path / "home"
    private_home.mkdir(mode=0o700)
    data_dir = tmp_path / "tendwire-data"
    monkeypatch.setenv("HOME", str(private_home))
    monkeypatch.setenv("TENDWIRE_DATA_DIR", str(data_dir))
    monkeypatch.setenv("TENDWIRE_DB_PATH", str(data_dir / "tendwire.db"))
    calls: list[tuple[str, dict[str, Any]]] = []
    action = {
        "schema_version": 1,
        "action": "part",
        "plan_token": "twplan1.public",
        "ordinal": 0,
        "spans": [
            {
                "field": "assistant_final_text",
                "start_char": 0,
                "end_char": 42,
            }
        ],
    }

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
                    "name": "turn-final",
                    "plan_token": "twplan1.public",
                    "ordinal": 0,
                    "accepted_parts": 1,
                },
            }

    monkeypatch.setenv("TENDWIRE_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("TENDWIRE_DB_PATH", str(tmp_path / "data" / "tendwire.db"))
    monkeypatch.setattr("tendwire.daemon_api.DaemonAPIClient", FakeDaemonAPIClient)
    monkeypatch.setattr("sys.stdin", io.StringIO(json.dumps(action)))
    code = main(
        [
            "--host-id",
            "host-a",
            "--socket-path",
            str(tmp_path / "daemon.sock"),
            "connector",
            "prepare",
            "--name",
            "turn-final",
            "--json",
        ]
    )
    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert code == 0
    assert captured.err == ""
    assert payload["plan_token"] == "twplan1.public"
    assert calls == [
        (
            "connector.prepare",
            {
                **action,
                "name": "turn-final",
            },
        )
    ]


def test_cli_daemon_connector_result_is_sanitized_before_printing(
    tmp_path: Path,
    capsys,
    monkeypatch,
) -> None:
    revision = "twrev1.ueLJtVatFOQxa1UePvWId8C01qdrb05FpW_ipSSPHMM"

    class FakeDaemonAPIClient:
        def __init__(self, socket_path: Any, *, timeout_seconds: float, max_response_bytes: int = 1024 * 1024):
            pass

        def request(self, method: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
            assert method == "connector.poll"
            return {
                "ok": True,
                "result": {
                    "schema_version": 1,
                    "ok": True,
                    "status": "ok",
                    "host_id": "host-a",
                    "name": "attention",
                    "backend_target": "sentinel-private-target",
                    "items": [
                        {
                            "ref": "twref1.publicSafeRef",
                            "payload": {
                                "safe": "kept",
                                "turn_id": "turn-public-final",
                                "plan_token": "twplan1.publicPlan",
                                "content_revision": revision,
                                "content": {
                                    "schema_version": 1,
                                    "content_revision": revision,
                                },
                                "chat_id": "sentinel-private-chat",
                                "raw_payload": "sentinel-private-raw",
                            },
                            "pane_id": "sentinel-private-pane",
                        }
                    ],
                },
            }

    monkeypatch.setattr("tendwire.daemon_api.DaemonAPIClient", FakeDaemonAPIClient)

    code = main(
        [
            "--host-id",
            "host-a",
            "--socket-path",
            str(tmp_path / "daemon.sock"),
            "connector",
            "poll",
            "--name",
            "attention",
        ]
    )
    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    encoded = json.dumps(payload, sort_keys=True).lower()

    assert code == 0
    assert captured.err == ""
    assert payload["items"][0]["payload"] == {
        "safe": "kept",
        "turn_id": "turn-public-final",
        "plan_token": "twplan1.publicPlan",
        "content_revision": revision,
        "content": {
            "schema_version": 1,
            "content_revision": revision,
        },
    }
    assert "sentinel-private" not in encoded
    assert "raw_payload" not in encoded
    _assert_json_only_and_safe(payload)


def test_daemon_connector_preserves_public_turn_id_for_final_ready() -> None:
    revision = "twrev1.ueLJtVatFOQxa1UePvWId8C01qdrb05FpW_ipSSPHMM"
    api = TendwireDaemonAPI(
        get_snapshot=lambda: Snapshot(host_id="host-a"),
        get_health=lambda: {},
        submit_command=lambda _params: {},
        connector_call=lambda _method, _params: {
            "schema_version": 1,
            "ok": True,
            "status": "ok",
            "items": [
                {
                    "key": "turn-final:revision:twfinal1.public",
                    "ref": "twref1.publicSafeRef",
                    "payload": {
                        "schema_version": 2,
                        "operation": "final_ready",
                        "turn_id": "turn-public-final",
                        "content_revision": revision,
                        "content": {
                            "schema_version": 1,
                            "content_revision": revision,
                            "known_incomplete": False,
                            "fields": {},
                        },
                        "pane_id": "sentinel-private-pane",
                        "session_id": "sentinel-private-session",
                        "terminal_id": "sentinel-private-terminal",
                        "topic_id": "sentinel-private-topic",
                        "message_id": "sentinel-private-message",
                    },
                }
            ],
        },
    )

    response = api.dispatch(
        {"method": "connector.poll", "params": {"name": "turn-final"}}
    )

    assert response["result"]["items"][0]["payload"] == {
        "schema_version": 2,
        "operation": "final_ready",
        "turn_id": "turn-public-final",
        "content_revision": revision,
        "content": {
            "schema_version": 1,
            "content_revision": revision,
            "known_incomplete": False,
            "fields": {},
        },
    }
    _assert_json_only_and_safe(response)


def test_daemon_connector_preserves_nested_plan_token_for_final_part() -> None:
    api = TendwireDaemonAPI(
        get_snapshot=lambda: Snapshot(host_id="host-a"),
        get_health=lambda: {},
        submit_command=lambda _params: {},
        connector_call=lambda _method, _params: {
            "schema_version": 1,
            "ok": True,
            "status": "ok",
            "items": [
                {
                    "key": "turn-final:twplan1.publicPlan:000000",
                    "ref": "twref1.publicSafeRef",
                    "payload": {
                        "schema_version": 1,
                        "plan_token": "twplan1.publicPlan",
                        "operation": "upsert",
                        "turn": {
                            "turn_id": "turn-public-final",
                            "pane_id": "sentinel-private-pane",
                        },
                    },
                }
            ],
        },
    )

    response = api.dispatch(
        {"method": "connector.poll", "params": {"name": "turn-final"}}
    )

    assert response["result"]["items"][0]["payload"] == {
        "schema_version": 1,
        "plan_token": "twplan1.publicPlan",
        "operation": "upsert",
        "turn": {"turn_id": "turn-public-final"},
    }
    _assert_json_only_and_safe(response)


def test_connector_api_store_unavailable_returns_safe_error() -> None:
    payload = ConnectorOutboxAPI(None, "host-a").poll({"name": "attention"})

    assert payload["ok"] is False
    assert payload["status"] == "store_unavailable"
    _assert_json_only_and_safe(payload)


def test_recover_rpc_is_forwarded_and_printed_with_exact_frozen_contract(
    tmp_path: Path,
    capsys,
    monkeypatch,
) -> None:
    failed_token = "twplan1.failedPublic"
    recovered_token = "twplan1.recoveredPublic"
    params = {
        "schema_version": 1,
        "action": "recover",
        "name": "turn-final",
        "failed_plan_token": failed_token,
        "request_id": "recover-request-42",
    }
    result = {
        "schema_version": 1,
        "ok": True,
        "status": "recovered",
        "failed_plan_token": failed_token,
        "plan_token": recovered_token,
        "generation": 2,
        "content_revision": "twrev1.publicRevision",
        "state": "active",
        "acknowledged_prefix_count": 1,
        "executable_job_count": 2,
        "retained_failed_job_count": 1,
        "prior_attempt_count": 3,
        "idempotent_replay": False,
    }
    daemon_calls: list[tuple[str, dict[str, Any]]] = []
    api = TendwireDaemonAPI(
        get_snapshot=lambda: Snapshot(host_id="host-a"),
        get_health=lambda: {},
        submit_command=lambda _params: {},
        connector_call=lambda method, call_params: (
            daemon_calls.append((method, dict(call_params))) or result
        ),
    )
    envelope = api.dispatch({"method": "connector.prepare", "params": params})
    assert envelope["result"] == result
    assert daemon_calls == [("connector.prepare", params)]

    cli_calls: list[tuple[str, dict[str, Any]]] = []

    class FakeDaemonAPIClient:
        def __init__(self, _socket_path: Any, **_kwargs: Any) -> None:
            pass

        def request(
            self,
            method: str,
            params: dict[str, Any] | None = None,
        ) -> dict[str, Any]:
            cli_calls.append((method, dict(params or {})))
            return {"ok": True, "result": result}

    monkeypatch.setattr("tendwire.daemon_api.DaemonAPIClient", FakeDaemonAPIClient)
    monkeypatch.setattr("sys.stdin", io.StringIO(json.dumps(params)))
    code = main(
        [
            "--host-id",
            "host-a",
            "--socket-path",
            str(tmp_path / "daemon.sock"),
            "connector",
            "prepare",
            "--name",
            "turn-final",
            "--json",
        ]
    )
    captured = capsys.readouterr()
    assert code == 0
    assert captured.err == ""
    assert json.loads(captured.out) == result
    assert cli_calls == [("connector.prepare", params)]


def test_daemon_routes_strict_neutral_inspect_and_retry_methods() -> None:
    final_identity = "twfinal1.publicSafeIdentity"
    calls: list[tuple[str, dict[str, Any]]] = []

    def connector_call(method: str, params: dict[str, Any]) -> dict[str, Any]:
        calls.append((method, dict(params)))
        if method == "connector.inspect":
            return {
                "schema_version": 1,
                "ok": True,
                "status": "ok",
                "name": "turn-final",
                "items": [
                    {
                        "final_identity": final_identity,
                        "status": "dead_letter",
                    }
                ],
            }
        return {
            "schema_version": 1,
            "ok": True,
            "status": "requeued",
            "name": "turn-final",
            "final_identity": final_identity,
        }

    api = TendwireDaemonAPI(
        get_snapshot=lambda: Snapshot(host_id="host-a"),
        get_health=lambda: {},
        submit_command=lambda _params: {},
        connector_call=connector_call,
    )
    inspect_params = {
        "schema_version": 1,
        "name": "turn-final",
        "status": "dead_letter",
        "limit": 25,
    }
    retry_params = {
        "schema_version": 1,
        "name": "turn-final",
        "final_identity": final_identity,
    }

    inspected = api.dispatch(
        {"method": "connector.inspect", "params": inspect_params}
    )
    retried = api.dispatch({"method": "connector.retry", "params": retry_params})

    assert calls == [
        ("connector.inspect", inspect_params),
        ("connector.retry", retry_params),
    ]
    assert inspected["result"]["items"] == [
        {"final_identity": final_identity, "status": "dead_letter"}
    ]
    assert retried["result"]["status"] == "requeued"
    _assert_json_only_and_safe(inspected)
    _assert_json_only_and_safe(retried)


def test_cli_connector_inspect_then_retry_forwards_exact_neutral_contract(
    tmp_path: Path,
    capsys,
    monkeypatch,
) -> None:
    final_identity = "twfinal1.publicSafeIdentity"
    calls: list[tuple[str, dict[str, Any]]] = []

    class FakeDaemonAPIClient:
        def __init__(self, _socket_path: Any, **_kwargs: Any) -> None:
            pass

        def request(
            self,
            method: str,
            params: dict[str, Any] | None = None,
        ) -> dict[str, Any]:
            call_params = dict(params or {})
            calls.append((method, call_params))
            if method == "connector.inspect":
                result = {
                    "schema_version": 1,
                    "ok": True,
                    "status": "ok",
                    "name": "turn-final",
                    "items": [
                        {
                            "final_identity": final_identity,
                            "status": "dead_letter",
                        }
                    ],
                }
            else:
                result = {
                    "schema_version": 1,
                    "ok": True,
                    "status": "requeued",
                    "name": "turn-final",
                    "final_identity": final_identity,
                }
            return {"ok": True, "result": result}

    monkeypatch.setattr("tendwire.daemon_api.DaemonAPIClient", FakeDaemonAPIClient)
    socket_path = str(tmp_path / "daemon.sock")
    inspect_code = main(
        [
            "--socket-path",
            socket_path,
            "connector",
            "inspect",
            "--name",
            "turn-final",
            "--status",
            "dead_letter",
            "--limit",
            "25",
        ]
    )
    inspect_capture = capsys.readouterr()
    retry_code = main(
        [
            "--socket-path",
            socket_path,
            "connector",
            "retry",
            "--name",
            "turn-final",
            "--final-identity",
            final_identity,
        ]
    )
    retry_capture = capsys.readouterr()

    assert inspect_code == retry_code == 0
    assert inspect_capture.err == retry_capture.err == ""
    assert calls == [
        (
            "connector.inspect",
            {
                "schema_version": 1,
                "name": "turn-final",
                "status": "dead_letter",
                "limit": 25,
            },
        ),
        (
            "connector.retry",
            {
                "schema_version": 1,
                "name": "turn-final",
                "final_identity": final_identity,
            },
        ),
    ]
    inspect_payload = json.loads(inspect_capture.out)
    retry_payload = json.loads(retry_capture.out)
    assert inspect_payload["items"][0]["final_identity"] == final_identity
    assert retry_payload["status"] == "requeued"
    _assert_json_only_and_safe(inspect_payload)
    _assert_json_only_and_safe(retry_payload)


@pytest.mark.parametrize("limit", ["0", "101"])
def test_cli_connector_inspect_rejects_unbounded_limits(
    limit: str,
    capsys,
) -> None:
    with pytest.raises(SystemExit) as caught:
        main(
            [
                "connector",
                "inspect",
                "--name",
                "turn-final",
                "--status",
                "dead_letter",
                "--limit",
                limit,
            ]
        )

    captured = capsys.readouterr()
    assert caught.value.code == 2
    assert captured.out == ""
    assert "limit must be between 1 and 100" in captured.err


@pytest.mark.parametrize(
    "argv",
    [
        [
            "connector",
            "inspect",
            "--name",
            "attention",
            "--status",
            "dead_letter",
        ],
        [
            "connector",
            "retry",
            "--name",
            "attention",
            "--final-identity",
            "twfinal1.publicSafeIdentity",
        ],
        [
            "connector",
            "retry",
            "--name",
            "turn-final",
            "--final-identity",
            " ",
        ],
    ],
)
def test_cli_connector_inspect_and_retry_reject_nonfinal_selectors(
    argv: list[str],
    capsys,
    monkeypatch,
) -> None:
    class ForbiddenDaemonAPIClient:
        def __init__(self, *_args: Any, **_kwargs: Any) -> None:
            raise AssertionError("invalid connector selector reached daemon")

    monkeypatch.setattr(
        "tendwire.daemon_api.DaemonAPIClient",
        ForbiddenDaemonAPIClient,
    )

    code = main(argv)
    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert code == 2
    assert captured.err == ""
    assert payload["status"] == "invalid_request"
    assert payload["error"]["code"] == "invalid_request"
    _assert_json_only_and_safe(payload)
