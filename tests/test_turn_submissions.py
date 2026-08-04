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
    init_store,
    is_valid_turn_submission_state_transition,
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
            terminal_at, updated_at
        ) VALUES (
            'host-a', ?, 'send_instruction', 1, ?, ?, 'worker-a', ?, ?, '{}',
            ?, ?, NULL, '2026-01-01T00:00:00+00:00',
            '2026-01-01T00:00:01+00:00', ?, ?, ?
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


def _observe_link_turn(
    db_path: Path,
    *,
    source_turn_id: str,
    worker_id: str = "worker-a",
    host_id: str = "host-a",
    instruction_text: str = "hello",
    observed_at: str = "2026-02-01T12:00:00+00:00",
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


def test_observed_submission_linker_handles_both_race_directions_without_changing_turn_id(
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








def test_observed_submission_linker_failure_keeps_observation_and_rolls_back_link_attempt(
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
        "_settle_submission_links_conn",
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


def test_observed_submission_linker_never_uses_unverified_send_started_as_turn_evidence(
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


def test_observed_submission_linker_waits_for_window_close_before_failing_closed(
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








@pytest.mark.parametrize(
    (
        "scenario",
        "initial_state",
        "submission_count",
        "observe_candidate",
        "terminal_state",
    ),
    (
        ("no-candidate", "submitted", 1, False, "expired"),
        ("stale-send-started", "send_started", 1, True, "expired"),
        ("two-by-one", "submitted", 2, True, "ambiguous"),
    ),
)
def test_link_window_close_settles_current_submission_components(
    tmp_path: Path,
    scenario: str,
    initial_state: str,
    submission_count: int,
    observe_candidate: bool,
    terminal_state: str,
) -> None:
    db_path = tmp_path / f"window-close-{scenario}.db"
    owner_key = _seed_link_worker(db_path)
    for index in range(submission_count):
        _insert_link_submission(
            db_path,
            request_id=f"{scenario}-{index}",
            owner_key=owner_key,
            state=initial_state,
            link_expires_at="2026-02-01T12:01:00+00:00",
            hard_expires_at="2026-02-02T12:00:00+00:00",
        )
    if observe_candidate:
        observed_at = "2026-02-01T12:00:03+00:00"
        if initial_state == "send_started":
            observed_at = (
                datetime.fromisoformat("2026-02-01T12:00:00+00:00")
                + timedelta(
                    seconds=store_sqlite.SUBMISSION_SEND_ACK_TIMEOUT_SECONDS + 1
                )
            ).isoformat()
        _observe_link_turn(
            db_path,
            source_turn_id=f"{scenario}-source",
            observed_at=observed_at,
        )

    before_close = turn_delta_payload_from_store(
        db_path,
        "host-a",
        now=datetime.fromisoformat(
            "2026-02-01T12:00:30+00:00"
        ).timestamp(),
    )
    assert before_close["host_id"] == "host-a"
    assert _submission_rows(db_path) == [
        (f"{scenario}-{index}", initial_state, None)
        for index in range(submission_count)
    ]

    at_close = turn_delta_payload_from_store(
        db_path,
        "host-a",
        now=datetime.fromisoformat(
            "2026-02-01T12:01:00+00:00"
        ).timestamp(),
    )
    assert at_close["host_id"] == "host-a"
    assert _submission_rows(db_path) == [
        (f"{scenario}-{index}", terminal_state, None)
        for index in range(submission_count)
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
        ("2026-02-01T12:01:00+00:00", "2026-02-02T12:00:00+00:00")
    ] * submission_count


def test_disconnected_submission_components_settle_independently(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "disconnected-components.db"
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

    payload = turn_delta_payload_from_store(
        db_path,
        "host-a",
        now=datetime.fromisoformat(
            "2026-02-01T12:00:05+00:00"
        ).timestamp(),
    )

    assert payload["host_id"] == "host-a"
    assert _submission_rows(db_path) == [
        ("live-singleton", "linked", turn_id),
        ("old-disconnected", "expired", None),
    ]
    with sqlite3.connect(str(db_path)) as conn:
        assert conn.execute(
            """
            SELECT request_id, terminal_at, linked_at
            FROM turn_submissions
            WHERE host_id = 'host-a'
            ORDER BY request_id
            """
        ).fetchall() == [
            ("live-singleton", None, "2026-02-01T12:00:03+00:00"),
            ("old-disconnected", "2026-02-01T12:00:03+00:00", None),
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
def test_observed_submission_linker_marks_larger_identical_components_ambiguous(
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


def test_submission_linker_isolates_stable_owners(
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
    )

    assert payload["turns"]
    assert _submission_rows(db_path) == [
        ("idle-observation-first", "linked", observed_turn_id)
    ]






def test_turn_alias_resolves_public_content_and_final_root(
    tmp_path: Path,
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




def test_stable_key_delta_lazy_sweep_rearms_component_backoff(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "stable-key-delta-rearm.db"
    owner_key = _seed_link_worker(db_path)
    _insert_link_submission(
        db_path,
        request_id="stable-key-delta-rearm",
        owner_key=owner_key,
        link_not_before="2026-07-22T12:01:23+00:00",
        link_expires_at="2026-07-22T12:03:23+00:00",
        hard_expires_at="2026-07-23T12:02:23+00:00",
    )
    candidate_calls = 0
    rearmed_keys: list[tuple[str, str]] = []
    original_candidates = store_sqlite._submission_link_candidate_turns_conn
    original_rearm = store_sqlite._rearm_submission_link_component
    original_settle = store_sqlite._settle_submission_links_conn
    fail_direct_settlement = False

    def record_candidates(*args: object, **kwargs: object):
        nonlocal candidate_calls
        candidate_calls += 1
        return original_candidates(*args, **kwargs)

    def record_rearm(
        db: Path | str,
        host: str,
        owner: str,
        fingerprint: str,
    ) -> None:
        rearmed_keys.append((owner, fingerprint))
        original_rearm(db, host, owner, fingerprint)

    def fail_one_direct_settlement(*args: object, **kwargs: object) -> int:
        if fail_direct_settlement:
            raise RuntimeError("controlled direct settlement failure")
        return original_settle(*args, **kwargs)

    monkeypatch.setattr(
        store_sqlite,
        "_submission_link_candidate_turns_conn",
        record_candidates,
    )
    monkeypatch.setattr(
        store_sqlite,
        "_rearm_submission_link_component",
        record_rearm,
    )
    monkeypatch.setattr(
        store_sqlite,
        "_settle_submission_links_conn",
        fail_one_direct_settlement,
    )

    for observed_at in (
        "2026-07-22T12:02:33+00:00",
        "2026-07-22T12:02:38+00:00",
    ):
        payload = turn_delta_payload_from_store(
            db_path,
            "host-a",
            now=datetime.fromisoformat(observed_at).timestamp(),
        )
        assert payload["host_id"] == "host-a"
    assert candidate_calls == 1
    assert _submission_rows(db_path) == [
        ("stable-key-delta-rearm", "submitted", None)
    ]

    fail_direct_settlement = True
    observed_turn_id = _observe_link_turn(
        db_path,
        source_turn_id="stable-key-delta-rearm-source",
        instruction_text="hello\x01",
        observed_at="2026-07-22T12:02:40+00:00",
    )
    fail_direct_settlement = False
    assert (owner_key, instruction_fingerprint("hello")) in rearmed_keys
    assert all(owner == owner_key for owner, _fingerprint in rearmed_keys)
    assert candidate_calls == 1
    assert _submission_rows(db_path) == [
        ("stable-key-delta-rearm", "submitted", None)
    ]

    linked_page = turn_delta_payload_from_store(
        db_path,
        "host-a",
        now=datetime.fromisoformat(
            "2026-07-22T12:02:42+00:00"
        ).timestamp(),
    )
    assert linked_page["host_id"] == "host-a"
    assert candidate_calls == 2
    assert _submission_rows(db_path) == [
        ("stable-key-delta-rearm", "linked", observed_turn_id)
    ]
    linked_turn = next(
        change["turn"]
        for change in linked_page["changes"]
        if change.get("op") == "upsert"
        and change.get("turn_id") == observed_turn_id
    )
    assert linked_turn["submission_id"] == turn_submission_id(
        "host-a", "stable-key-delta-rearm"
    )
    assert linked_turn["submission_state"] == "linked"
    with sqlite3.connect(str(db_path)) as conn:
        assert conn.execute(
            """
            SELECT linked_at FROM turn_submissions
            WHERE host_id = 'host-a'
              AND request_id = 'stable-key-delta-rearm'
            """
        ).fetchone() == ("2026-07-22T12:02:42+00:00",)


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
    )
    assert (owner_key, instruction_fingerprint("hello")) in rearmed_keys
    assert all(owner == owner_key for owner, _ in rearmed_keys)

    turn_delta_payload_from_store(
        db_path,
        "host-a",
        now=datetime.fromisoformat(
            "2026-07-22T12:03:23+00:00"
        ).timestamp(),
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
    )
    assert _submission_rows(db_path) == [
        ("observed-busy-pane", "linked", observed_turn_id)
    ]
