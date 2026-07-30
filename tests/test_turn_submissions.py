"""Stage 1 coverage for the observation-authoritative turn model."""

from __future__ import annotations

import json
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from tendwire.config import Config
from tendwire.core import turns as core_turns
from tendwire.core.commands import (
    instruction_fingerprint,
    normalize_instruction_text,
    turn_submission_id,
)
from tendwire.core.projector import project_from_raw
from tendwire.core.turns import Turn
from tendwire.store import sqlite as store_sqlite
from tendwire.store.sqlite import (
    TURN_SUBMISSION_STATE_TRANSITIONS,
    cancel_turn_submission,
    init_store,
    is_valid_turn_submission_state_transition,
    sweep_expired_turn_submissions,
    turn_delta_payload_from_store,
)


_LEDGER_TABLES = ("turn_submissions", "turn_supersessions")


def _ledger_schema(conn: sqlite3.Connection) -> tuple[tuple[object, ...], ...]:
    placeholders = ", ".join("?" for _ in _LEDGER_TABLES)
    return tuple(
        conn.execute(
            f"""
            SELECT type, name, tbl_name, sql
            FROM sqlite_master
            WHERE tbl_name IN ({placeholders})
            ORDER BY type, name
            """,
            _LEDGER_TABLES,
        ).fetchall()
    )


def _assert_empty_v20_ledgers(
    conn: sqlite3.Connection,
    *,
    expected_version: int = store_sqlite.STORE_SCHEMA_VERSION,
) -> None:
    assert conn.execute("PRAGMA user_version").fetchone() == (expected_version,)
    assert conn.execute("SELECT COUNT(*) FROM turn_submissions").fetchone() == (0,)
    assert conn.execute("SELECT COUNT(*) FROM turn_supersessions").fetchone() == (0,)

    submission_columns = tuple(
        row[1] for row in conn.execute("PRAGMA table_info(turn_submissions)")
    )
    assert submission_columns == (
        "host_id",
        "submission_id",
        "request_id",
        "owner_key",
        "owner_key_version",
        "instruction_fingerprint",
        "state",
        "linked_turn_id",
        "link_not_before",
        "link_expires_at",
        "hard_expires_at",
        "linked_at",
        "terminal_at",
        "submitted_at",
        "send_started_at",
        "updated_at",
    )
    supersession_columns = tuple(
        row[1] for row in conn.execute("PRAGMA table_info(turn_supersessions)")
    )
    assert supersession_columns == (
        "host_id",
        "superseded_turn_id",
        "canonical_turn_id",
        "reason",
        "created_at",
    )

    submission_indexes = {
        str(row[1]): tuple(
            str(column[2])
            for column in conn.execute(f"PRAGMA index_info({row[1]})").fetchall()
        )
        for row in conn.execute("PRAGMA index_list(turn_submissions)").fetchall()
    }
    assert submission_indexes["idx_turn_submissions_link_candidates"] == (
        "host_id",
        "owner_key",
        "instruction_fingerprint",
        "state",
    )
    assert submission_indexes["ux_turn_submissions_linked_turn"] == (
        "host_id",
        "linked_turn_id",
    )
    linked_turn_index = conn.execute(
        "SELECT sql FROM sqlite_master WHERE name = 'ux_turn_submissions_linked_turn'"
    ).fetchone()[0]
    assert "WHERE linked_turn_id IS NOT NULL" in linked_turn_index
    assert conn.execute(
        """
        SELECT partial FROM pragma_index_list('turn_submissions')
        WHERE name = 'ux_turn_submissions_linked_turn'
        """
    ).fetchone() == (1,)
    assert submission_indexes["sqlite_autoindex_turn_submissions_1"] == (
        "host_id",
        "submission_id",
    )
    assert submission_indexes["sqlite_autoindex_turn_submissions_2"] == (
        "host_id",
        "request_id",
    )

    supersession_indexes = {
        str(row[1]): tuple(
            str(column[2])
            for column in conn.execute(f"PRAGMA index_info({row[1]})").fetchall()
        )
        for row in conn.execute("PRAGMA index_list(turn_supersessions)").fetchall()
    }
    assert supersession_indexes == {
        "idx_turn_supersessions_canonical": ("host_id", "canonical_turn_id"),
        "sqlite_autoindex_turn_supersessions_1": (
            "host_id",
            "superseded_turn_id",
        ),
    }


def test_fresh_v20_store_creates_empty_turn_ledgers_and_all_indexes(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "fresh-v20.db"
    init_store(db_path)

    with sqlite3.connect(str(db_path)) as conn:
        _assert_empty_v20_ledgers(conn)


def test_v18_to_v20_migration_matches_fresh_schema(tmp_path: Path) -> None:
    fresh_path = tmp_path / "fresh.db"
    upgrade_path = tmp_path / "upgrade.db"
    init_store(fresh_path)
    with sqlite3.connect(str(fresh_path)) as fresh:
        fresh_schema = _ledger_schema(fresh)

    with sqlite3.connect(str(upgrade_path)) as upgrade:
        store_sqlite._run_migrations(upgrade, target_version=18)
        assert not set(_LEDGER_TABLES) & {
            str(row[0])
            for row in upgrade.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }
        store_sqlite._run_migrations(upgrade, target_version=20)
        _assert_empty_v20_ledgers(upgrade, expected_version=20)
        assert _ledger_schema(upgrade) == fresh_schema


@pytest.mark.parametrize("source_version", range(store_sqlite.STORE_SCHEMA_VERSION))
def test_every_prior_schema_upgrades_to_identical_empty_v20_ledgers(
    tmp_path: Path,
    source_version: int,
) -> None:
    fresh_path = tmp_path / f"fresh-{source_version}.db"
    upgrade_path = tmp_path / f"upgrade-{source_version}.db"
    init_store(fresh_path)
    with sqlite3.connect(str(fresh_path)) as fresh:
        fresh_schema = _ledger_schema(fresh)

    with sqlite3.connect(str(upgrade_path)) as upgrade:
        store_sqlite._run_migrations(upgrade, target_version=source_version)
        store_sqlite._run_migrations(upgrade)
        _assert_empty_v20_ledgers(upgrade)
        assert _ledger_schema(upgrade) == fresh_schema


def _insert_historical_send_receipt(
    conn: sqlite3.Connection,
    *,
    request_id: str,
    state: str,
    status: str,
    instruction_text: str,
) -> None:
    active = state in {"reserved", "send_started"}
    sent = state != "reserved"
    terminal = state in {"accepted", "rejected", "uncertain"}
    canonical = json.dumps(
        {
            "canonical_version": 1,
            "action": "send_instruction",
            "target": {"worker_id": "worker-a"},
            "instruction": {"text": instruction_text},
            "options": {},
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    conn.execute(
        """
        INSERT INTO command_receipts (
            host_id, request_id, action, canonical_version,
            canonical_fingerprint, canonical_request_json, public_worker_id,
            state, status, result_json, owner_token_hash, owner_expires_at,
            binding_fingerprint, created_at, reserved_at, send_started_at,
            terminal_at, updated_at, legacy_collision,
            legacy_collision_count
        ) VALUES (
            'host-a', ?, 'send_instruction', 1, ?, ?, 'worker-a', ?, ?, '{}',
            ?, ?, NULL, '2026-01-01T00:00:00+00:00',
            '2026-01-01T00:00:01+00:00', ?, ?, ?, 0, 0
        )
        """,
        (
            request_id,
            f"canonical:{request_id}",
            canonical,
            state,
            status,
            "owner-token-hash" if active else "",
            "2026-01-01T00:01:00+00:00" if active else None,
            "2026-01-01T00:00:02+00:00" if sent else None,
            "2026-01-01T00:00:03+00:00" if terminal else None,
            (
                "2026-01-01T00:00:03+00:00"
                if terminal
                else "2026-01-01T00:00:02+00:00"
            ),
        ),
    )


def test_v18_to_v20_backfills_historical_submission_receipt(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "submission-backfill.db"
    with sqlite3.connect(str(db_path)) as conn:
        store_sqlite._run_migrations(conn, target_version=18)
        _insert_historical_send_receipt(
            conn,
            request_id="historical-submit",
            state="accepted",
            status="accepted",
            instruction_text="  historical   prompt  ",
        )
        conn.commit()
        store_sqlite._run_migrations(conn)

        assert conn.execute(
            """
            SELECT owner_key, owner_key_version, instruction_fingerprint,
                   state, linked_turn_id
            FROM turn_submissions
            WHERE host_id = 'host-a' AND request_id = 'historical-submit'
            """
        ).fetchone() == (
            "legacy-worker:worker-a",
            0,
            instruction_fingerprint("historical prompt"),
            "submitted",
            None,
        )


def test_v19_to_v20_backfills_legacy_tombstone_alias(tmp_path: Path) -> None:
    db_path = tmp_path / "supersession-backfill.db"
    _seed_link_worker(db_path)
    canonical_turn_id = _observe_link_turn(
        db_path,
        source_turn_id="canonical-source",
    )
    legacy_turn_id = "turn-" + ("2" * 24)
    observed_at = "2026-02-01T12:00:01+00:00"
    legacy_payload = {
        "id": legacy_turn_id,
        "host_id": "host-a",
        "worker_id": "worker-a",
        "status": "done",
        "kind": "task",
        "source": "command",
        "origin_command_id": "historical-submit",
        "complete": True,
        "has_open_turn": False,
        "updated_at": observed_at,
        "superseded_at": observed_at,
        "superseded_by_turn_id": canonical_turn_id,
    }
    with sqlite3.connect(str(db_path)) as conn:
        next_sequence = int(
            conn.execute(
                "SELECT COALESCE(MAX(list_sequence), 0) + 1 FROM turns"
            ).fetchone()[0]
        )
        conn.execute(
            """
            INSERT INTO turns (
                host_id, turn_id, worker_id, worker_fingerprint, space_id,
                status, kind, updated_at, fingerprint,
                snapshot_content_fingerprint, observed_at, payload_json,
                list_sequence
            ) VALUES (
                'host-a', ?, 'worker-a', NULL, NULL, 'done', 'task', ?, '',
                '', ?, ?, ?
            )
            """,
            (
                legacy_turn_id,
                observed_at,
                observed_at,
                json.dumps(legacy_payload, sort_keys=True, separators=(",", ":")),
                next_sequence,
            ),
        )
        conn.execute("DELETE FROM turn_supersessions")
        conn.execute("PRAGMA user_version = 19")
        conn.commit()
        store_sqlite._run_migrations(conn)

        assert conn.execute(
            """
            SELECT canonical_turn_id, reason
            FROM turn_supersessions
            WHERE host_id = 'host-a' AND superseded_turn_id = ?
            """,
            (legacy_turn_id,),
        ).fetchone() == (canonical_turn_id, "phase1_migration")


@pytest.mark.parametrize(
    ("receipt_state", "receipt_status", "expected"),
    (
        (None, None, None),
        ({"state": "accepted"}, [], None),
        ("unknown", "accepted", None),
        ("reserved", "pending", None),
        ("send_started", "pending", "send_started"),
        ("accepted", "accepted", "submitted"),
        ("uncertain", "request_state_uncertain", "uncertain"),
        ("rejected", "cancelled", "cancelled"),
        ("accepted", "purged", None),
    ),
)
def test_backfill_submission_state_fails_closed_for_malformed_receipt_values(
    receipt_state: object,
    receipt_status: object,
    expected: str | None,
) -> None:
    assert (
        store_sqlite._backfill_submission_state(receipt_state, receipt_status)
        == expected
    )


@pytest.mark.parametrize(
    "canonical_request_json",
    (
        None,
        "not-json",
        json.dumps([]),
        json.dumps({"action": "observe", "instruction": {"text": "hello"}}),
        json.dumps({"action": "send_instruction"}),
        json.dumps({"action": "send_instruction", "instruction": []}),
        json.dumps({"action": "send_instruction", "instruction": {}}),
        json.dumps(
            {"action": "send_instruction", "instruction": {"text": 42}}
        ),
    ),
)
def test_receipt_instruction_text_rejects_malformed_receipt_shapes(
    canonical_request_json: object,
) -> None:
    assert store_sqlite._receipt_instruction_text(canonical_request_json) is None


def test_receipt_instruction_text_accepts_valid_send_instruction() -> None:
    canonical_request_json = json.dumps(
        {
            "action": "send_instruction",
            "instruction": {"text": "ship the release"},
        }
    )

    assert (
        store_sqlite._receipt_instruction_text(canonical_request_json)
        == "ship the release"
    )


def test_turn_submission_state_transition_table() -> None:
    assert TURN_SUBMISSION_STATE_TRANSITIONS == {
        "send_started": frozenset(
            {
                "submitted",
                "uncertain",
                "linked",
                "ambiguous",
                "expired",
                "cancelled",
            }
        ),
        "submitted": frozenset({"linked", "ambiguous", "expired", "cancelled"}),
        "uncertain": frozenset({"linked", "ambiguous", "expired", "cancelled"}),
        "linked": frozenset(),
        "ambiguous": frozenset(),
        "expired": frozenset(),
        "cancelled": frozenset(),
    }

    for current_state, allowed_states in TURN_SUBMISSION_STATE_TRANSITIONS.items():
        for next_state in TURN_SUBMISSION_STATE_TRANSITIONS:
            assert is_valid_turn_submission_state_transition(
                current_state,
                next_state,
            ) is (next_state in allowed_states)

    assert not is_valid_turn_submission_state_transition("unknown", "submitted")
    assert not is_valid_turn_submission_state_transition("submitted", "unknown")
    assert not is_valid_turn_submission_state_transition(None, "submitted")


def test_instruction_fingerprint_is_normalized_deterministic_and_opaque() -> None:
    variants = (
        "  deploy   the build\nthen verify  ",
        "deploy the build\nthen   verify",
        "deploy\tthe build\nthen verify",
    )

    assert {normalize_instruction_text(value) for value in variants} == {
        "deploy the build\nthen verify"
    }
    fingerprints = {instruction_fingerprint(value) for value in variants}
    assert len(fingerprints) == 1
    fingerprint = fingerprints.pop()
    assert fingerprint.startswith("twins1.")
    assert "deploy" not in fingerprint
    assert instruction_fingerprint("deploy the other build") != fingerprint
    assert normalize_instruction_text("\x01deploy the build\x7f") == (
        "deploy the build"
    )
    assert instruction_fingerprint("deploy the build\x01") == (
        instruction_fingerprint("deploy the build")
    )
    assert instruction_fingerprint("deploy\x01 the build") != (
        instruction_fingerprint("deploy the build")
    )
    assert normalize_instruction_text(" \n\t ") == ""
    assert instruction_fingerprint(" \n\t ") != instruction_fingerprint("\n")

    first = turn_submission_id("host-a", "request-1")
    assert first == turn_submission_id("host-a", "request-1")
    assert first.startswith("twsub1.")
    assert turn_submission_id("host-b", "request-1") != first
    assert turn_submission_id("host-a", "request-2") != first


def _insert_submission(
    conn: sqlite3.Connection,
    *,
    request_id: str,
    state: str,
    hard_expires_at: str,
) -> None:
    conn.execute(
        """
        INSERT INTO turn_submissions (
            host_id, submission_id, request_id, owner_key,
            owner_key_version, instruction_fingerprint, state,
            linked_turn_id, link_not_before, link_expires_at,
            hard_expires_at, linked_at, terminal_at, submitted_at,
            send_started_at, updated_at
        ) VALUES (
            'host-a', ?, ?, 'owner-a', 1, ?, ?, NULL,
            '2026-01-01T00:00:00+00:00',
            '2026-01-01T00:02:00+00:00', ?, NULL, NULL,
            '2026-01-01T00:01:00+00:00',
            '2026-01-01T00:00:00+00:00',
            '2026-01-01T00:01:00+00:00'
        )
        """,
        (
            turn_submission_id("host-a", request_id),
            request_id,
            instruction_fingerprint("hello"),
            state,
            hard_expires_at,
        ),
    )


def test_submission_expiry_sweeper_expires_only_old_unlinked_rows(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "expiry.db"
    init_store(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_submission(
            conn,
            request_id="old",
            state="submitted",
            hard_expires_at="2026-01-02T00:00:00+00:00",
        )
        _insert_submission(
            conn,
            request_id="future",
            state="uncertain",
            hard_expires_at="2026-03-01T00:00:00+00:00",
        )

    assert sweep_expired_turn_submissions(
        db_path,
        host_id="host-a",
        now="2026-02-01T00:00:00+00:00",
    ) == 1

    with sqlite3.connect(str(db_path)) as conn:
        rows = conn.execute(
            """
            SELECT request_id, state, terminal_at
            FROM turn_submissions ORDER BY request_id
            """
        ).fetchall()
    assert rows == [
        ("future", "uncertain", None),
        ("old", "expired", "2026-02-01T00:00:00+00:00"),
    ]


def test_submission_cancellation_is_terminal_and_idempotent(tmp_path: Path) -> None:
    db_path = tmp_path / "cancel.db"
    init_store(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        _insert_submission(
            conn,
            request_id="cancel-me",
            state="send_started",
            hard_expires_at="2026-03-01T00:00:00+00:00",
        )

    assert cancel_turn_submission(
        db_path,
        host_id="host-a",
        request_id="cancel-me",
        now="2026-02-01T00:00:00+00:00",
    )
    assert not cancel_turn_submission(
        db_path,
        host_id="host-a",
        request_id="cancel-me",
        now="2026-02-01T00:00:01+00:00",
    )

    with sqlite3.connect(str(db_path)) as conn:
        assert conn.execute(
            """
            SELECT state, terminal_at FROM turn_submissions
            WHERE host_id = 'host-a' AND request_id = 'cancel-me'
            """
        ).fetchone() == ("cancelled", "2026-02-01T00:00:00+00:00")


def _seed_link_worker(
    db_path: Path,
    *,
    host_id: str = "host-a",
    worker_id: str = "worker-a",
    owner_char: str = "a",
) -> str:
    owner_key = "wsk1_" + (owner_char * 64)
    snapshot = project_from_raw(
        Config(host_id=host_id, db_path=db_path),
        workers=[
            {
                "id": worker_id,
                "name": worker_id,
                "status": "active",
                "meta": {
                    "stable_key": owner_key,
                    "stable_key_version": 1,
                },
            }
        ],
    )
    init_store(db_path)
    store_sqlite.save_snapshot(db_path, snapshot)
    return owner_key


def _insert_link_submission(
    db_path: Path,
    *,
    request_id: str,
    owner_key: str,
    instruction_text: str = "hello",
    host_id: str = "host-a",
    state: str = "submitted",
    link_not_before: str = "2026-02-01T11:59:00+00:00",
    link_expires_at: str = "2026-02-01T12:00:00+00:00",
    hard_expires_at: str = "2026-02-02T12:00:00+00:00",
) -> None:
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            """
            INSERT INTO turn_submissions (
                host_id, submission_id, request_id, owner_key,
                owner_key_version, instruction_fingerprint, state,
                linked_turn_id, link_not_before, link_expires_at,
                hard_expires_at, linked_at, terminal_at, submitted_at,
                send_started_at, updated_at
            ) VALUES (
                ?, ?, ?, ?, 1, ?, ?, NULL, ?, ?,
                ?, NULL, NULL,
                '2026-02-01T12:00:00+00:00',
                '2026-02-01T12:00:00+00:00',
                '2026-02-01T12:00:00+00:00'
            )
            """,
            (
                host_id,
                turn_submission_id(host_id, request_id),
                request_id,
                owner_key,
                instruction_fingerprint(instruction_text),
                state,
                link_not_before,
                link_expires_at,
                hard_expires_at,
            ),
        )


def _set_link_worker_prod_shape(
    db_path: Path,
    *,
    worker_id: str = "worker-a",
    host_id: str = "host-a",
    explicit_null: bool = False,
) -> None:
    version_update = (
        "json_set(payload_json, '$.meta.stable_key_version', NULL)"
        if explicit_null
        else "json_remove(payload_json, '$.meta.stable_key_version')"
    )
    with sqlite3.connect(str(db_path)) as conn:
        updated = conn.execute(
            f"""
            UPDATE workers
            SET payload_json = {version_update}
            WHERE host_id = ? AND worker_id = ?
            """,
            (host_id, worker_id),
        )
        assert updated.rowcount == 1


def _observe_link_turn(
    db_path: Path,
    *,
    source_turn_id: str,
    worker_id: str = "worker-a",
    host_id: str = "host-a",
    instruction_text: str = "hello",
    observed_at: str = "2026-02-01T12:00:00+00:00",
    turn_model: str = "dual",
) -> str:
    result = store_sqlite.apply_turn_refresh(
        db_path,
        host_id,
        worker_id,
        {
            "source_turn_id": source_turn_id,
            "user_text": instruction_text,
            "assistant_final_text": f"answer for {source_turn_id}",
            "complete": True,
            "has_open_turn": False,
        },
        observed_at=observed_at,
        turn_model=turn_model,
    )
    assert result.updated in {0, 1}
    with sqlite3.connect(str(db_path)) as conn:
        rows = conn.execute(
            """
            SELECT turn_id, payload_json
            FROM turns
            WHERE host_id = ?
              AND COALESCE(json_extract(payload_json, '$.source_turn_id'), '') != ''
            ORDER BY turn_id
            """,
            (host_id,),
        ).fetchall()
    matching = [
        (str(turn_id), store_sqlite._json_object(payload_json))
        for turn_id, payload_json in rows
        if store_sqlite._json_object(payload_json).get("source_turn_id")
        in store_sqlite.turn_source_id_candidates(
            source_turn_id,
            meta=store_sqlite._json_object(payload_json).get("meta") or {},
            source=store_sqlite._json_object(payload_json).get("source"),
            kind=store_sqlite._json_object(payload_json).get("kind"),
        )
    ]
    assert len(matching) == 1
    turn_id, payload = matching[0]
    assert Turn.from_dict(payload).id == turn_id
    return turn_id


def _submission_rows(db_path: Path) -> list[tuple[str, str, str | None]]:
    with sqlite3.connect(str(db_path)) as conn:
        return [
            (str(row[0]), str(row[1]), None if row[2] is None else str(row[2]))
            for row in conn.execute(
                """
                SELECT request_id, state, linked_turn_id
                FROM turn_submissions ORDER BY request_id
                """
            ).fetchall()
        ]


def test_shadow_linker_handles_both_race_directions_without_changing_turn_id(
    tmp_path: Path,
) -> None:
    submission_first = tmp_path / "submission-first.db"
    owner_key = _seed_link_worker(submission_first)
    _insert_link_submission(
        submission_first,
        request_id="submission-first",
        owner_key=owner_key,
    )
    first_turn_id = _observe_link_turn(
        submission_first,
        source_turn_id="submission-first-source",
    )
    assert _submission_rows(submission_first) == [
        ("submission-first", "linked", first_turn_id)
    ]

    observation_first = tmp_path / "observation-first.db"
    owner_key = _seed_link_worker(observation_first)
    observed_turn_id = _observe_link_turn(
        observation_first,
        source_turn_id="observation-first-source",
    )
    _insert_link_submission(
        observation_first,
        request_id="observation-first",
        owner_key=owner_key,
    )
    # Through Stage 5, observation-first settlement is opportunistic: the
    # submission stays open until this worker produces another refresh. Stage 6
    # must add a lazy or periodic sweep before linked_turn_id gains authority.
    assert _submission_rows(observation_first) == [
        ("observation-first", "submitted", None)
    ]
    refreshed_turn_id = _observe_link_turn(
        observation_first,
        source_turn_id="observation-first-source",
        observed_at="2026-02-01T12:00:01+00:00",
    )
    assert refreshed_turn_id == observed_turn_id
    assert _submission_rows(observation_first) == [
        ("observation-first", "linked", observed_turn_id)
    ]


@pytest.mark.parametrize(
    ("order", "explicit_null"),
    (("submission-first", True), ("observation-first", False)),
)
def test_observed_linker_accepts_prod_shape_turn_owner_version(
    tmp_path: Path,
    order: str,
    explicit_null: bool,
) -> None:
    db_path = tmp_path / f"prod-shape-{order}.db"
    owner_key = _seed_link_worker(db_path)
    assert store_sqlite._turn_submission_owner_identity(
        {
            "id": "worker-a",
            "meta": {"stable_key": owner_key, "stable_key_version": 1},
        }
    ) == (owner_key, 1)
    _set_link_worker_prod_shape(db_path, explicit_null=explicit_null)

    if order == "submission-first":
        _insert_link_submission(
            db_path,
            request_id=order,
            owner_key=owner_key,
        )
    observed_turn_id = _observe_link_turn(
        db_path,
        source_turn_id=f"prod-shape-{order}-source",
        turn_model="observed",
    )
    if order == "observation-first":
        _insert_link_submission(
            db_path,
            request_id=order,
            owner_key=owner_key,
        )

    store_sqlite.sweep_submission_links(
        db_path,
        host_id="host-a",
        now="2026-02-01T12:00:01+00:00",
    )

    assert _submission_rows(db_path) == [(order, "linked", observed_turn_id)]
    with sqlite3.connect(str(db_path)) as conn:
        turn_shape = conn.execute(
            """
            SELECT json_extract(turns.payload_json, '$.meta.stable_key'),
                   json_extract(turns.payload_json, '$.meta.stable_key_version'),
                   json_extract(turns.payload_json, '$.stable_key_version'),
                   json_extract(turns.payload_json, '$.user_text'),
                   revisions.user_text
            FROM turns
            JOIN turn_content_revisions AS revisions
              ON revisions.host_id = turns.host_id
             AND revisions.turn_id = turns.turn_id
             AND revisions.is_current = 1
            WHERE turns.host_id = 'host-a' AND turns.turn_id = ?
            """,
            (observed_turn_id,),
        ).fetchone()
        turn_payload = store_sqlite._json_object(
            conn.execute(
                """
                SELECT payload_json FROM turns
                WHERE host_id = 'host-a' AND turn_id = ?
                """,
                (observed_turn_id,),
            ).fetchone()[0]
        )
    assert turn_shape == (owner_key, None, None, None, "hello")
    turn_worker = {"id": "worker-a", "meta": turn_payload.get("meta")}
    assert store_sqlite._turn_submission_owner_identity(turn_worker) == (
        "legacy-worker:worker-a",
        0,
    )
    assert store_sqlite._turn_link_candidate_owner_identity(turn_worker) == (
        owner_key,
        1,
    )


@pytest.mark.parametrize(
    ("submission_count", "observation_count"),
    ((2, 1), (1, 2), (2, 2)),
)
def test_observed_linker_prod_shape_turns_still_fail_closed_on_ambiguity(
    tmp_path: Path,
    submission_count: int,
    observation_count: int,
) -> None:
    db_path = tmp_path / (
        f"prod-shape-ambiguous-{submission_count}-{observation_count}.db"
    )
    owner_key = _seed_link_worker(db_path)
    _set_link_worker_prod_shape(db_path)
    for index in range(submission_count):
        _insert_link_submission(
            db_path,
            request_id=f"prod-shape-{index}",
            owner_key=owner_key,
        )

    for index in range(observation_count):
        _observe_link_turn(
            db_path,
            source_turn_id=f"prod-shape-ambiguous-source-{index}",
            turn_model="observed",
        )
    store_sqlite.sweep_submission_links(
        db_path,
        host_id="host-a",
        now="2026-02-01T12:00:01+00:00",
    )

    rows = _submission_rows(db_path)
    assert [state for _request, state, _turn in rows] == [
        "ambiguous"
    ] * submission_count
    assert all(linked_turn_id is None for _request, _state, linked_turn_id in rows)


def test_observed_linker_prod_shape_turns_keep_owner_hash_isolated(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "prod-shape-owner-isolation.db"
    first_owner = _seed_link_worker(db_path)
    second_owner = "wsk1_" + ("b" * 64)
    snapshot = project_from_raw(
        Config(host_id="host-a", db_path=db_path),
        workers=[
            {
                "id": "worker-a",
                "name": "worker-a",
                "status": "active",
                "meta": {
                    "stable_key": first_owner,
                    "stable_key_version": 1,
                },
            },
            {
                "id": "worker-b",
                "name": "worker-b",
                "status": "active",
                "meta": {
                    "stable_key": second_owner,
                    "stable_key_version": 1,
                },
            },
        ],
    )
    store_sqlite.save_snapshot(db_path, snapshot)
    _set_link_worker_prod_shape(db_path, worker_id="worker-a")
    _set_link_worker_prod_shape(db_path, worker_id="worker-b", explicit_null=True)
    _insert_link_submission(
        db_path,
        request_id="first-owner",
        owner_key=first_owner,
        link_expires_at="2026-02-01T12:01:00+00:00",
    )

    _observe_link_turn(
        db_path,
        worker_id="worker-b",
        source_turn_id="wrong-owner-source",
        turn_model="observed",
    )
    store_sqlite.sweep_submission_links(
        db_path,
        host_id="host-a",
        now="2026-02-01T12:00:01+00:00",
    )
    assert _submission_rows(db_path) == [("first-owner", "submitted", None)]

    matching_turn_id = _observe_link_turn(
        db_path,
        worker_id="worker-a",
        source_turn_id="first-owner-source",
        observed_at="2026-02-01T12:00:02+00:00",
        turn_model="observed",
    )
    store_sqlite.sweep_submission_links(
        db_path,
        host_id="host-a",
        now="2026-02-01T12:00:03+00:00",
    )
    assert _submission_rows(db_path) == [
        ("first-owner", "linked", matching_turn_id)
    ]


def test_shadow_linker_failure_keeps_observation_and_rolls_back_link_attempt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "settle-failure.db"
    owner_key = _seed_link_worker(db_path)
    _insert_link_submission(
        db_path,
        request_id="settle-failure",
        owner_key=owner_key,
    )

    def fail_after_partial_settlement(
        conn: sqlite3.Connection,
        *_args: object,
        **_kwargs: object,
    ) -> int:
        conn.execute(
            """
            UPDATE turn_submissions
            SET state = 'ambiguous'
            WHERE host_id = 'host-a' AND request_id = 'settle-failure'
            """
        )
        raise RuntimeError("injected settlement failure")

    monkeypatch.setattr(
        store_sqlite,
        "settle_submission_links_conn",
        fail_after_partial_settlement,
    )
    turn_id = _observe_link_turn(
        db_path,
        source_turn_id="settle-failure-source",
    )

    assert _submission_rows(db_path) == [
        ("settle-failure", "submitted", None)
    ]
    with sqlite3.connect(str(db_path)) as conn:
        observed = conn.execute(
            """
            SELECT json_extract(turns.payload_json, '$.source_turn_id'),
                   revisions.user_text, revisions.assistant_final_text
            FROM turns
            JOIN turn_content_revisions AS revisions
              ON revisions.host_id = turns.host_id
             AND revisions.turn_id = turns.turn_id
             AND revisions.is_current = 1
            WHERE turns.host_id = 'host-a' AND turns.turn_id = ?
            """,
            (turn_id,),
        ).fetchone()
        assert observed is not None
        assert str(observed[0])
        assert tuple(observed[1:]) == (
            "hello",
            "answer for settle-failure-source",
        )


def test_shadow_linker_never_uses_unverified_send_started_as_turn_evidence(
    tmp_path: Path,
) -> None:
    linked_path = tmp_path / "send-started-linked.db"
    owner_key = _seed_link_worker(linked_path)
    _insert_link_submission(
        linked_path,
        request_id="send-started-linked",
        owner_key=owner_key,
        state="send_started",
    )
    _observe_link_turn(
        linked_path,
        source_turn_id="send-started-linked-source",
    )
    assert _submission_rows(linked_path) == [
        ("send-started-linked", "expired", None)
    ]

    ambiguous_path = tmp_path / "send-started-ambiguous.db"
    owner_key = _seed_link_worker(ambiguous_path)
    for index in range(2):
        _insert_link_submission(
            ambiguous_path,
            request_id=f"send-started-ambiguous-{index}",
            owner_key=owner_key,
            state="send_started",
        )
    _observe_link_turn(
        ambiguous_path,
        source_turn_id="send-started-ambiguous-source",
    )
    assert _submission_rows(ambiguous_path) == [
        ("send-started-ambiguous-0", "expired", None),
        ("send-started-ambiguous-1", "expired", None),
    ]


def test_shadow_linker_waits_for_window_close_before_failing_closed(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "delayed.db"
    owner_key = _seed_link_worker(db_path)
    _insert_link_submission(
        db_path,
        request_id="delayed",
        owner_key=owner_key,
        link_expires_at="2026-02-01T12:01:00+00:00",
    )
    _observe_link_turn(db_path, source_turn_id="delayed-source-a")
    assert _submission_rows(db_path) == [("delayed", "submitted", None)]
    _observe_link_turn(
        db_path,
        source_turn_id="delayed-source-b",
        observed_at="2026-02-01T12:00:30+00:00",
    )
    assert _submission_rows(db_path) == [("delayed", "submitted", None)]
    _observe_link_turn(
        db_path,
        source_turn_id="delayed-source-a",
        observed_at="2026-02-01T12:01:00+00:00",
    )
    assert _submission_rows(db_path) == [("delayed", "ambiguous", None)]


@pytest.mark.parametrize("turn_model", ("dual", "observed"))
def test_single_open_submission_links_on_first_sweep_after_observation(
    tmp_path: Path,
    turn_model: str,
) -> None:
    db_path = tmp_path / f"instant-single-{turn_model}.db"
    owner_key = _seed_link_worker(db_path)
    _set_link_worker_prod_shape(db_path)
    _insert_link_submission(
        db_path,
        request_id=f"instant-single-{turn_model}",
        owner_key=owner_key,
        link_expires_at="2026-02-01T12:01:00+00:00",
    )
    observed_at = "2026-02-01T12:00:03+00:00"
    turn_id = _observe_link_turn(
        db_path,
        source_turn_id=f"instant-single-{turn_model}-source",
        observed_at=observed_at,
        turn_model=turn_model,
    )
    assert _submission_rows(db_path) == [
        (f"instant-single-{turn_model}", "submitted", None)
    ]

    swept_at = "2026-02-01T12:00:05+00:00"
    store_sqlite.sweep_submission_links(
        db_path,
        host_id="host-a",
        now=swept_at,
    )

    assert _submission_rows(db_path) == [
        (f"instant-single-{turn_model}", "linked", turn_id)
    ]
    with sqlite3.connect(str(db_path)) as conn:
        linked_at = conn.execute(
            """
            SELECT linked_at FROM turn_submissions
            WHERE host_id = 'host-a' AND request_id = ?
            """,
            (f"instant-single-{turn_model}",),
        ).fetchone()[0]
    assert linked_at == swept_at
    assert (
        datetime.fromisoformat(linked_at)
        - datetime.fromisoformat(observed_at)
    ).total_seconds() == 2
    assert linked_at < "2026-02-01T12:01:00+00:00"


def test_two_open_same_fingerprint_submissions_do_not_instant_link(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "two-open-no-instant.db"
    owner_key = _seed_link_worker(db_path)
    for index in range(2):
        _insert_link_submission(
            db_path,
            request_id=f"two-open-{index}",
            owner_key=owner_key,
            link_expires_at="2026-02-01T12:01:00+00:00",
        )

    _observe_link_turn(
        db_path,
        source_turn_id="two-open-source",
        observed_at="2026-02-01T12:00:03+00:00",
    )
    store_sqlite.sweep_submission_links(
        db_path,
        host_id="host-a",
        now="2026-02-01T12:00:05+00:00",
    )

    assert _submission_rows(db_path) == [
        ("two-open-0", "submitted", None),
        ("two-open-1", "submitted", None),
    ]


def test_disconnected_singleton_component_still_links_immediately(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "disconnected-singleton-instant.db"
    owner_key = _seed_link_worker(db_path)
    _insert_link_submission(
        db_path,
        request_id="old-disconnected",
        owner_key=owner_key,
        link_not_before="2026-02-01T11:00:00+00:00",
        link_expires_at="2026-02-01T11:01:00+00:00",
    )
    _insert_link_submission(
        db_path,
        request_id="live-singleton",
        owner_key=owner_key,
        link_expires_at="2026-02-01T12:01:00+00:00",
    )
    turn_id = _observe_link_turn(
        db_path,
        source_turn_id="live-singleton-source",
        observed_at="2026-02-01T12:00:03+00:00",
    )

    store_sqlite.sweep_submission_links(
        db_path,
        host_id="host-a",
        now="2026-02-01T12:00:05+00:00",
    )

    assert _submission_rows(db_path) == [
        ("live-singleton", "linked", turn_id),
        ("old-disconnected", "expired", None),
    ]


def test_manual_same_text_turn_links_single_open_submission(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "manual-same-text.db"
    owner_key = _seed_link_worker(db_path)
    _insert_link_submission(
        db_path,
        request_id="manual-same-text",
        owner_key=owner_key,
        link_expires_at="2026-02-01T12:01:00+00:00",
    )

    turn_id = _observe_link_turn(
        db_path,
        source_turn_id="manually-entered-turn",
        observed_at="2026-02-01T12:00:03+00:00",
    )

    # The linker sees content and worker identity, not the origin of typed text.
    # With one open same-fingerprint submission this attribution is harmless.
    assert _submission_rows(db_path) == [
        ("manual-same-text", "linked", turn_id)
    ]


def test_stale_send_started_submission_uses_windowed_settlement(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "stale-send-started.db"
    owner_key = _seed_link_worker(db_path)
    _insert_link_submission(
        db_path,
        request_id="stale-send-started",
        owner_key=owner_key,
        state="send_started",
        link_expires_at="2026-02-01T12:01:00+00:00",
    )
    observed_at = (
        datetime.fromisoformat("2026-02-01T12:00:00+00:00")
        + timedelta(
            seconds=store_sqlite.SUBMISSION_SEND_ACK_TIMEOUT_SECONDS + 1
        )
    ).isoformat()
    _observe_link_turn(
        db_path,
        source_turn_id="stale-send-started-source",
        observed_at=observed_at,
    )

    assert _submission_rows(db_path) == [
        ("stale-send-started", "send_started", None)
    ]
    store_sqlite.sweep_submission_links(
        db_path,
        host_id="host-a",
        now="2026-02-01T12:00:30+00:00",
    )
    assert _submission_rows(db_path) == [
        ("stale-send-started", "send_started", None)
    ]

    store_sqlite.sweep_submission_links(
        db_path,
        host_id="host-a",
        now="2026-02-01T12:01:00+00:00",
    )
    assert _submission_rows(db_path) == [
        ("stale-send-started", "expired", None)
    ]


def test_ambiguous_component_is_stamped_at_window_close_not_hard_ttl(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "ambiguous-at-window-close.db"
    owner_key = _seed_link_worker(db_path)
    for index in range(2):
        _insert_link_submission(
            db_path,
            request_id=f"ambiguous-at-close-{index}",
            owner_key=owner_key,
            link_expires_at="2026-02-01T12:01:00+00:00",
            hard_expires_at="2026-02-02T12:00:00+00:00",
        )
    _observe_link_turn(
        db_path,
        source_turn_id="ambiguous-at-close-source",
        observed_at="2026-02-01T12:00:03+00:00",
    )

    store_sqlite.sweep_submission_links(
        db_path,
        host_id="host-a",
        now="2026-02-01T12:01:00+00:00",
    )

    assert _submission_rows(db_path) == [
        ("ambiguous-at-close-0", "ambiguous", None),
        ("ambiguous-at-close-1", "ambiguous", None),
    ]
    with sqlite3.connect(str(db_path)) as conn:
        stamps = conn.execute(
            """
            SELECT terminal_at, hard_expires_at
            FROM turn_submissions
            WHERE host_id = 'host-a'
            ORDER BY request_id
            """
        ).fetchall()
    assert stamps == [
        ("2026-02-01T12:01:00+00:00", "2026-02-02T12:00:00+00:00"),
        ("2026-02-01T12:01:00+00:00", "2026-02-02T12:00:00+00:00"),
    ]


def test_lone_submission_without_candidate_expires_at_window_close(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "no-candidate-at-window-close.db"
    owner_key = _seed_link_worker(db_path)
    _insert_link_submission(
        db_path,
        request_id="no-candidate-at-window-close",
        owner_key=owner_key,
        link_expires_at="2026-02-01T12:01:00+00:00",
        hard_expires_at="2026-02-02T12:00:00+00:00",
    )

    store_sqlite.sweep_submission_links(
        db_path,
        host_id="host-a",
        now="2026-02-01T12:00:30+00:00",
    )
    assert _submission_rows(db_path) == [
        ("no-candidate-at-window-close", "submitted", None)
    ]
    store_sqlite.sweep_submission_links(
        db_path,
        host_id="host-a",
        now="2026-02-01T12:01:00+00:00",
    )

    assert _submission_rows(db_path) == [
        ("no-candidate-at-window-close", "expired", None)
    ]
    with sqlite3.connect(str(db_path)) as conn:
        assert conn.execute(
            """
            SELECT terminal_at, hard_expires_at
            FROM turn_submissions
            WHERE host_id = 'host-a'
              AND request_id = 'no-candidate-at-window-close'
            """
        ).fetchone() == (
            "2026-02-01T12:01:00+00:00",
            "2026-02-02T12:00:00+00:00",
        )


def test_each_no_candidate_component_expires_at_its_window_close(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "multiple-no-candidate-components.db"
    owner_key = _seed_link_worker(db_path)
    _insert_link_submission(
        db_path,
        request_id="closed-no-candidate",
        owner_key=owner_key,
        link_expires_at="2026-02-01T12:00:10+00:00",
    )
    _insert_link_submission(
        db_path,
        request_id="open-no-candidate",
        owner_key=owner_key,
        link_not_before="2026-02-01T12:01:00+00:00",
        link_expires_at="2026-02-01T12:02:00+00:00",
    )

    store_sqlite.sweep_submission_links(
        db_path,
        host_id="host-a",
        now="2026-02-01T12:01:30+00:00",
    )

    assert _submission_rows(db_path) == [
        ("closed-no-candidate", "expired", None),
        ("open-no-candidate", "submitted", None),
    ]


def test_turn_observed_outside_link_window_never_links(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "outside-link-window.db"
    owner_key = _seed_link_worker(db_path)
    _insert_link_submission(
        db_path,
        request_id="outside-link-window",
        owner_key=owner_key,
        link_expires_at="2026-02-01T12:00:10+00:00",
        hard_expires_at="2026-02-02T12:00:00+00:00",
    )
    turn_id = _observe_link_turn(
        db_path,
        source_turn_id="outside-link-window-source",
        observed_at="2026-02-01T12:00:11+00:00",
    )

    assert _submission_rows(db_path) == [
        ("outside-link-window", "expired", None)
    ]
    with sqlite3.connect(str(db_path)) as conn:
        assert conn.execute(
            """
            SELECT COUNT(*) FROM turn_submissions
            WHERE host_id = 'host-a' AND linked_turn_id = ?
            """,
            (turn_id,),
        ).fetchone() == (0,)


@pytest.mark.parametrize(
    ("submission_count", "observation_count"),
    ((2, 1), (1, 2), (2, 2)),
)
def test_shadow_linker_marks_larger_identical_components_ambiguous(
    tmp_path: Path,
    submission_count: int,
    observation_count: int,
) -> None:
    db_path = tmp_path / f"ambiguous-{submission_count}-{observation_count}.db"
    owner_key = _seed_link_worker(db_path)
    for index in range(observation_count):
        _observe_link_turn(
            db_path,
            source_turn_id=f"ambiguous-source-{index}",
        )
    for index in range(submission_count):
        _insert_link_submission(
            db_path,
            request_id=f"ambiguous-request-{index}",
            owner_key=owner_key,
        )
    _observe_link_turn(
        db_path,
        source_turn_id="ambiguous-source-0",
        observed_at="2026-02-01T12:00:01+00:00",
    )
    rows = _submission_rows(db_path)
    assert [state for _request, state, _turn in rows] == [
        "ambiguous"
    ] * submission_count
    assert all(linked_turn_id is None for _request, _state, linked_turn_id in rows)


def test_submission_linker_isolates_owners_and_legacy_alias_links(
    tmp_path: Path,
) -> None:
    first_path = tmp_path / "owners.db"
    first_owner = _seed_link_worker(first_path)
    second_owner = "wsk1_" + ("b" * 64)
    snapshot = project_from_raw(
        Config(host_id="host-a", db_path=first_path),
        workers=[
            {
                "id": "worker-a",
                "name": "worker-a",
                "status": "active",
                "meta": {"stable_key": first_owner, "stable_key_version": 1},
            },
            {
                "id": "worker-b",
                "name": "worker-b",
                "status": "active",
                "meta": {"stable_key": second_owner, "stable_key_version": 1},
            },
        ],
    )
    store_sqlite.save_snapshot(first_path, snapshot)
    _insert_link_submission(first_path, request_id="owner-a", owner_key=first_owner)
    _insert_link_submission(first_path, request_id="owner-b", owner_key=second_owner)
    first_turn_id = _observe_link_turn(
        first_path,
        source_turn_id="owner-a-source",
    )
    second_turn_id = _observe_link_turn(
        first_path,
        worker_id="worker-b",
        source_turn_id="owner-b-source",
    )
    assert _submission_rows(first_path) == [
        ("owner-a", "linked", first_turn_id),
        ("owner-b", "linked", second_turn_id),
    ]

    legacy_path = tmp_path / "legacy.db"
    owner_key = _seed_link_worker(legacy_path)
    _insert_link_submission(legacy_path, request_id="legacy", owner_key=owner_key)
    legacy_turn_id = _observe_link_turn(
        legacy_path,
        source_turn_id="legacy-source",
        turn_model="legacy",
    )
    assert _submission_rows(legacy_path) == [("legacy", "linked", legacy_turn_id)]


def test_goal13_delta_is_unperturbed_when_observed_turn_links_later(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "delta.db"
    owner_key = _seed_link_worker(db_path)
    bootstrap = turn_delta_payload_from_store(db_path, "host-a")
    assert bootstrap["has_more"] is False
    checkpoint = str(bootstrap["checkpoint"])

    observed_turn_id = _observe_link_turn(
        db_path,
        source_turn_id="delta-source",
    )
    observed_delta = turn_delta_payload_from_store(
        db_path,
        "host-a",
        watermark=checkpoint,
    )
    observed_changes = [
        change
        for change in observed_delta["changes"]
        if change["turn_id"] == observed_turn_id
    ]
    assert [(change["op"], change["turn_id"]) for change in observed_changes] == [
        ("upsert", observed_turn_id)
    ]

    _insert_link_submission(db_path, request_id="delta-request", owner_key=owner_key)
    assert _observe_link_turn(
        db_path,
        source_turn_id="delta-source",
        observed_at="2026-02-01T12:00:01+00:00",
    ) == observed_turn_id
    linked_delta = turn_delta_payload_from_store(
        db_path,
        "host-a",
        watermark=str(observed_delta["checkpoint"]),
    )
    assert not any(
        change["turn_id"] == observed_turn_id
        for change in linked_delta["changes"]
    )


def test_two_racing_refreshes_cannot_double_link_one_observation(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "race.db"
    owner_key = _seed_link_worker(db_path)
    _insert_link_submission(db_path, request_id="race", owner_key=owner_key)

    def refresh() -> str:
        return _observe_link_turn(
            db_path,
            source_turn_id="race-source",
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        turn_ids = list(pool.map(lambda _index: refresh(), range(2)))
    assert len(set(turn_ids)) == 1
    assert _submission_rows(db_path) == [("race", "linked", turn_ids[0])]
    with sqlite3.connect(str(db_path)) as conn:
        assert conn.execute(
            """
            SELECT COUNT(*) FROM turn_submissions
            WHERE linked_turn_id = ?
            """,
            (turn_ids[0],),
        ).fetchone() == (1,)


def test_idle_observation_first_submission_links_from_lazy_turn_read(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "idle-observation-first.db"
    owner_key = _seed_link_worker(db_path)
    observed_turn_id = _observe_link_turn(
        db_path,
        source_turn_id="idle-observation-source",
    )
    _insert_link_submission(
        db_path,
        request_id="idle-observation-first",
        owner_key=owner_key,
    )

    payload = store_sqlite.turns_payload_from_store(
        db_path,
        "host-a",
        now=datetime.fromisoformat(
            "2026-02-01T12:00:01+00:00"
        ).timestamp(),
        turn_model="observed",
    )

    assert payload["turns"]
    assert _submission_rows(db_path) == [
        ("idle-observation-first", "linked", observed_turn_id)
    ]


def test_observed_prod_shape_sweep_never_runs_public_sanitizers(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "prod-shape-no-public-sanitize.db"
    owner_key = _seed_link_worker(db_path)
    _set_link_worker_prod_shape(db_path)
    observed_turn_id = _observe_link_turn(
        db_path,
        source_turn_id="prod-shape-no-public-sanitize-source",
        turn_model="observed",
    )
    _insert_link_submission(
        db_path,
        request_id="prod-shape-no-public-sanitize",
        owner_key=owner_key,
    )
    contains_calls = 0
    sanitize_calls = 0
    original_contains = core_turns._contains_forbidden_public_text
    original_sanitize = core_turns.sanitize_public_text

    def record_contains(value: str) -> bool:
        nonlocal contains_calls
        contains_calls += 1
        return original_contains(value)

    def record_sanitize(value: object, **kwargs: object) -> str:
        nonlocal sanitize_calls
        sanitize_calls += 1
        return original_sanitize(value, **kwargs)

    monkeypatch.setattr(
        core_turns,
        "_contains_forbidden_public_text",
        record_contains,
    )
    monkeypatch.setattr(core_turns, "sanitize_public_text", record_sanitize)

    store_sqlite.sweep_submission_links(
        db_path,
        host_id="host-a",
        now="2026-02-01T12:00:01+00:00",
    )

    assert contains_calls == 0
    assert sanitize_calls == 0
    assert _submission_rows(db_path) == [
        ("prod-shape-no-public-sanitize", "linked", observed_turn_id)
    ]


def test_submission_link_sweep_backs_off_until_matching_observation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "submission-link-backoff.db"
    owner_key = _seed_link_worker(db_path)
    _insert_link_submission(
        db_path,
        request_id="submission-link-backoff",
        owner_key=owner_key,
    )
    candidate_calls = 0
    original_candidates = store_sqlite._submission_link_candidate_turns_conn

    def record_candidates(*args: object, **kwargs: object):
        nonlocal candidate_calls
        candidate_calls += 1
        return original_candidates(*args, **kwargs)

    monkeypatch.setattr(
        store_sqlite,
        "_submission_link_candidate_turns_conn",
        record_candidates,
    )

    for _ in range(2):
        store_sqlite.sweep_submission_links(
            db_path,
            host_id="host-a",
            now="2026-02-01T11:59:30+00:00",
        )
    assert candidate_calls == 1

    # Production observations can persist the authenticated stable owner key
    # without its version marker. That shape cannot use the observation-time
    # direct settlement path, so this specifically proves that the observation
    # re-arms the component for the next sweep.
    _set_link_worker_prod_shape(db_path)
    observed_turn_id = _observe_link_turn(
        db_path,
        source_turn_id="submission-link-backoff-source",
        observed_at="2026-02-01T12:00:00+00:00",
        turn_model="observed",
    )
    assert candidate_calls == 1

    store_sqlite.sweep_submission_links(
        db_path,
        host_id="host-a",
        now="2026-02-01T12:00:01+00:00",
    )

    assert candidate_calls == 2
    assert _submission_rows(db_path) == [
        ("submission-link-backoff", "linked", observed_turn_id)
    ]


@pytest.mark.parametrize("turn_model", sorted(store_sqlite.TURN_MODELS))
def test_turn_alias_resolves_public_content_and_final_root_under_every_model(
    tmp_path: Path,
    turn_model: str,
) -> None:
    db_path = tmp_path / "alias-lookup.db"
    _seed_link_worker(db_path)
    canonical_turn_id = _observe_link_turn(
        db_path,
        source_turn_id="alias-source",
    )
    legacy_turn_id = "turn-" + ("1" * 24)
    with sqlite3.connect(str(db_path)) as conn:
        revision = str(
            conn.execute(
                """
                SELECT content_revision FROM turn_content_revisions
                WHERE host_id = 'host-a' AND turn_id = ? AND is_current = 1
                """,
                (canonical_turn_id,),
            ).fetchone()[0]
        )
        conn.execute(
            """
            INSERT INTO turn_supersessions (
                host_id, superseded_turn_id, canonical_turn_id,
                reason, created_at
            ) VALUES ('host-a', ?, ?, 'phase1_migration', ?)
            """,
            (
                legacy_turn_id,
                canonical_turn_id,
                "2026-02-01T12:00:01+00:00",
            ),
        )

    page = store_sqlite.get_turn_content(
        db_path,
        "host-a",
        turn_id=legacy_turn_id,
        content_revision=revision,
        field="assistant_final_text",
        turn_model=turn_model,
    )
    assert page["turn_id"] == canonical_turn_id
    assert page["text"] == "answer for alias-source"

    leased = store_sqlite.poll_connector_outbox(
        db_path,
        "host-a",
        "turn-final",
        now="2026-02-01T12:00:02+00:00",
    )["items"][0]
    begun = store_sqlite.prepare_connector_plan_begin(
        db_path,
        "host-a",
        name="turn-final",
        turn_id=legacy_turn_id,
        content_revision=revision,
        presentation_version="alias-aware-v1",
        part_count=1,
        source_ref=leased["ref"],
        turn_model=turn_model,
        now="2026-02-01T12:00:03+00:00",
    )
    assert begun["ok"] is True
    with sqlite3.connect(str(db_path)) as conn:
        assert conn.execute(
            """
            SELECT turn_id FROM turn_presentation_plans
            WHERE plan_token = ?
            """,
            (begun["plan_token"],),
        ).fetchone() == (canonical_turn_id,)


def test_link_candidate_owner_identity_normalizes_only_missing_version() -> None:
    # Guard-regression for the prod-shape normalization: ONLY a syntactically
    # valid stable key whose version is exactly None is treated as v1; every
    # other shape falls through to the strict submission-side identity.
    valid_key = "wsk1_" + ("a" * 64)
    normalized = store_sqlite._turn_link_candidate_owner_identity(
        {"id": "worker-x", "meta": {"stable_key": valid_key, "stable_key_version": None}}
    )
    assert normalized == (valid_key, 1)

    fallthrough_cases = [
        {"stable_key": valid_key, "stable_key_version": 2},
        {"stable_key": valid_key, "stable_key_version": 0},
        {"stable_key": valid_key, "stable_key_version": "1"},
        {"stable_key": valid_key, "stable_key_version": True},
        {"stable_key": "wsk1_short", "stable_key_version": None},
        {"stable_key": "not-a-key", "stable_key_version": None},
        {"stable_key": "", "stable_key_version": None},
        {"stable_key": None, "stable_key_version": None},
    ]
    for meta in fallthrough_cases:
        result = store_sqlite._turn_link_candidate_owner_identity(
            {"id": "worker-x", "meta": meta}
        )
        assert result == ("legacy-worker:worker-x", 0), meta


def test_observed_lazy_delta_sweep_links_first_poll_after_observation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Mirror the idle-pane production canary timeline through turn.delta."""
    db_path = tmp_path / "observed-live-timeline.db"
    owner_key = _seed_link_worker(db_path)
    _insert_link_submission(
        db_path,
        request_id="observed-live-timeline",
        owner_key=owner_key,
        link_not_before="2026-07-22T12:01:23+00:00",
        link_expires_at="2026-07-22T12:03:23+00:00",
        hard_expires_at="2026-07-23T12:02:23+00:00",
    )
    _set_link_worker_prod_shape(db_path)

    # Herdres polls turn.delta every ~5s.  The early empty sweeps exercise the
    # component backoff before the idle pane produces its first observation.
    for second in range(23, 36, 5):
        turn_delta_payload_from_store(
            db_path,
            "host-a",
            now=datetime.fromisoformat(
                f"2026-07-22T12:01:{second:02d}+00:00"
            ).timestamp(),
            turn_model="observed",
        )
    for second in range(38, 60, 5):
        turn_delta_payload_from_store(
            db_path,
            "host-a",
            now=datetime.fromisoformat(
                f"2026-07-22T12:01:{second:02d}+00:00"
            ).timestamp(),
            turn_model="observed",
        )
    for second in range(3, 34, 5):
        turn_delta_payload_from_store(
            db_path,
            "host-a",
            now=datetime.fromisoformat(
                f"2026-07-22T12:02:{second:02d}+00:00"
            ).timestamp(),
            turn_model="observed",
        )

    candidate_calls = 0
    original_candidates = store_sqlite._submission_link_candidate_turns_conn

    def record_candidates(*args: object, **kwargs: object):
        nonlocal candidate_calls
        candidate_calls += 1
        return original_candidates(*args, **kwargs)

    monkeypatch.setattr(
        store_sqlite,
        "_submission_link_candidate_turns_conn",
        record_candidates,
    )
    # The live observed prompt retained Herdr's trailing U+0001 framing byte.
    # Submission input rejects that byte, so matching must ignore it only at
    # the observation edge and re-arm the original component key.
    observed_turn_id = _observe_link_turn(
        db_path,
        source_turn_id="turn-803b8be4224ccec08a20c794",
        instruction_text="hello\x01",
        observed_at="2026-07-22T12:02:36+00:00",
        turn_model="observed",
    )
    assert _submission_rows(db_path) == [
        ("observed-live-timeline", "submitted", None)
    ]
    candidate_calls_after_observation = candidate_calls

    turn_delta_payload_from_store(
        db_path,
        "host-a",
        now=datetime.fromisoformat(
            "2026-07-22T12:02:38+00:00"
        ).timestamp(),
        turn_model="observed",
    )
    assert candidate_calls == candidate_calls_after_observation + 1
    assert _submission_rows(db_path) == [
        ("observed-live-timeline", "linked", observed_turn_id)
    ]
    with sqlite3.connect(str(db_path)) as conn:
        linked_at = conn.execute(
            """
            SELECT linked_at FROM turn_submissions
            WHERE host_id = 'host-a'
              AND request_id = 'observed-live-timeline'
            """
        ).fetchone()[0]
    assert linked_at == "2026-07-22T12:02:38+00:00"
    assert linked_at < "2026-07-22T12:03:23+00:00"


def test_observed_link_rearm_uses_stable_owner_across_worker_renumber(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "observed-renumber.db"
    owner_key = _seed_link_worker(db_path, worker_id="claude-1")
    _insert_link_submission(
        db_path,
        request_id="observed-renumber",
        owner_key=owner_key,
        link_not_before="2026-07-22T12:01:23+00:00",
        link_expires_at="2026-07-22T12:03:23+00:00",
        hard_expires_at="2026-07-23T12:02:23+00:00",
    )
    turn_delta_payload_from_store(
        db_path,
        "host-a",
        now=datetime.fromisoformat(
            "2026-07-22T12:02:33+00:00"
        ).timestamp(),
        turn_model="observed",
    )

    renumbered = project_from_raw(
        Config(host_id="host-a", db_path=db_path),
        workers=[
            {
                "id": "claude-9",
                "name": "claude-9",
                "status": "active",
                "meta": {
                    "stable_key": owner_key,
                    "stable_key_version": 1,
                },
            }
        ],
        timestamp=datetime.fromisoformat("2099-01-01T00:00:00+00:00"),
    )
    assert store_sqlite.save_snapshot(
        db_path,
        renumbered,
        turn_model="observed",
    )

    rearmed_keys: list[tuple[str, str]] = []
    original_rearm = store_sqlite._rearm_submission_link_component

    def record_rearm(
        db: Path | str,
        host: str,
        owner: str,
        fingerprint: str,
    ) -> None:
        rearmed_keys.append((owner, fingerprint))
        original_rearm(db, host, owner, fingerprint)

    monkeypatch.setattr(
        store_sqlite,
        "_rearm_submission_link_component",
        record_rearm,
    )
    observed_turn_id = _observe_link_turn(
        db_path,
        worker_id="claude-9",
        source_turn_id="renumbered-source",
        instruction_text="hello\x01",
        observed_at="2026-07-22T12:02:36+00:00",
        turn_model="observed",
    )
    assert (owner_key, instruction_fingerprint("hello")) in rearmed_keys
    assert all(not owner.startswith("legacy-worker:") for owner, _ in rearmed_keys)

    turn_delta_payload_from_store(
        db_path,
        "host-a",
        now=datetime.fromisoformat(
            "2026-07-22T12:03:23+00:00"
        ).timestamp(),
        turn_model="observed",
    )
    assert _submission_rows(db_path) == [
        ("observed-renumber", "linked", observed_turn_id)
    ]


def test_observed_busy_pane_completion_links_immediately(
    tmp_path: Path,
) -> None:
    """Preserve the observation-first ordering from the working busy-pane case."""
    db_path = tmp_path / "observed-busy-pane.db"
    owner_key = _seed_link_worker(db_path)
    started = store_sqlite.apply_turn_refresh(
        db_path,
        "host-a",
        "worker-a",
        {
            "source_turn_id": "busy-pane-source",
            "user_text": "hello",
            "assistant_stream_text": "working",
            "complete": False,
            "has_open_turn": True,
        },
        observed_at="2026-07-22T12:01:30+00:00",
        turn_model="observed",
    )
    assert started.updated == 1
    _insert_link_submission(
        db_path,
        request_id="observed-busy-pane",
        owner_key=owner_key,
        link_not_before="2026-07-22T12:01:23+00:00",
        link_expires_at="2026-07-22T12:03:23+00:00",
        hard_expires_at="2026-07-23T12:02:23+00:00",
    )
    observed_turn_id = _observe_link_turn(
        db_path,
        source_turn_id="busy-pane-source",
        observed_at="2026-07-22T12:02:36+00:00",
        turn_model="observed",
    )
    assert _submission_rows(db_path) == [
        ("observed-busy-pane", "linked", observed_turn_id)
    ]
