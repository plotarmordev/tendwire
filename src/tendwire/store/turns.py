"""Atomic turn projection, immutable content, list, delta, and paging."""

from __future__ import annotations

import base64
import hashlib
import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Mapping

from ..core.agent_events import AgentEvent, AppendBoundAgentEventResult
from ..core.models import (
    Snapshot,
    WorkerBinding,
    sanitize_canonical_turn_text,
    stable_fingerprint,
)
from ..core.turns import (
    TURN_CONTENT_PAGE_MAX_UTF8_BYTES,
    TURN_DELTA_BOOTSTRAP_MAX_PAGES,
    TURN_DELTA_BOOTSTRAP_MAX_ROWS,
    TURN_DELTA_CURSOR_TTL_SECONDS,
    TURN_DELTA_DEFAULT_LIMIT,
    TURN_DELTA_MAX_LIMIT,
    TURN_DELTA_MAX_BATCH_SEQUENCES,
    TURN_DELTA_PROJECTION_SCHEMA_VERSION,
    TURN_DELTA_SCHEMA_VERSION,
    TURN_LIST_CURSOR_TTL_SECONDS,
    TURN_LIST_DEFAULT_LIMIT,
    TURN_LIST_MAX_LIMIT,
    TURN_LIST_SCHEMA_VERSION,
    TURN_STREAM_TEXT_MAX_CHARS,
    TURN_TEXT_MAX_CHARS,
    build_turn_content_page,
    content_cursor,
    content_revision as make_revision,
    decode_turn_delta_cursor,
    decode_turn_delta_watermark,
    decode_turn_list_cursor,
    decode_turn_since_token,
    project_turn_content,
    turn_delta_cursor,
    turn_delta_watermark,
    turn_final_delivery_identity,
    turn_list_cursor,
    turn_since_token,
)
from .db import canonical_utc, read_transaction, utc_now, write_transaction
from .events import _append
from .outbox import OutboxInvariantError, _validate_polled_payload
from .projection import presentation_binding_row


@dataclass(frozen=True)
class TurnRefreshApplyResult:
    updated: int
    pending_changed: bool
    stale_binding: bool = False
    cancelled: bool = False


@dataclass(frozen=True)
class AppendProjectedAgentEventResult:
    event: AppendBoundAgentEventResult
    turn: TurnRefreshApplyResult | None = None


def _json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def _token(domain: str, value: Any, prefix: str) -> str:
    digest = hashlib.sha256(_json([domain, value]).encode()).digest()
    return prefix + base64.urlsafe_b64encode(digest).rstrip(b"=").decode()


def _page_spans(text: str) -> list[tuple[int, int, int, int]]:
    spans: list[tuple[int, int, int, int]] = []
    start_char = start_byte = 0
    byte_count = 0
    for index, character in enumerate(text):
        encoded = len(character.encode("utf-8"))
        if byte_count and byte_count + encoded > TURN_CONTENT_PAGE_MAX_UTF8_BYTES:
            spans.append((start_char, index, start_byte, start_byte + byte_count))
            start_char = index
            start_byte += byte_count
            byte_count = 0
        byte_count += encoded
    if byte_count or not spans:
        spans.append((start_char, len(text), start_byte, start_byte + byte_count))
    return spans


def _field_descriptor(revision: str, field: str, text: str | None) -> dict[str, Any]:
    if text is None:
        return {
            "availability": "absent",
            "inline": None,
            "char_length": 0,
            "byte_length": 0,
            "page_count": 0,
            "first_cursor": None,
        }
    byte_length = len(text.encode("utf-8"))
    if byte_length <= TURN_CONTENT_PAGE_MAX_UTF8_BYTES:
        return {
            "availability": "complete",
            "inline": text,
            "char_length": len(text),
            "byte_length": byte_length,
            "page_count": 0,
            "first_cursor": None,
        }
    pages = _page_spans(text)
    return {
        "availability": "complete",
        "inline": None,
        "char_length": len(text),
        "byte_length": byte_length,
        "page_count": len(pages),
        "first_cursor": content_cursor(revision, field, 0),
    }


def _content_descriptor(
    revision: str,
    user: str | None,
    final: str | None,
) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "content_revision": revision,
        "known_incomplete": False,
        "fields": {
            "user_text": _field_descriptor(revision, "user_text", user),
            "assistant_final_text": _field_descriptor(
                revision,
                "assistant_final_text",
                final,
            ),
        },
    }


def _binding_row(
    conn: Any,
    host_id: str,
    worker_id: str,
    expected: WorkerBinding | None = None,
) -> Any:
    rows = conn.execute(
        """SELECT * FROM worker_bindings
        WHERE host_id=? AND worker_id=? AND (expires_at IS NULL OR expires_at>?)
        ORDER BY observed_at DESC LIMIT 2""",
        (host_id, worker_id, utc_now()),
    ).fetchall()
    if expected is not None:
        matching = []
        for row in rows:
            private = json.loads(row["private_binding_json"])
            expected_private = {
                "host_id": expected.host_id,
                "worker_id": expected.worker_id,
                "worker_fingerprint": expected.worker_fingerprint,
                "backend": expected.backend,
                "target_kind": expected.target_kind,
                "target_value": expected.target_value,
                "turn_target_kind": expected.turn_target_kind,
                "turn_target_value": expected.turn_target_value,
                "sendable": expected.sendable,
                "private_fingerprint": expected.private_fingerprint,
            }
            if all(private.get(key) == value for key, value in expected_private.items()):
                matching.append(row)
        if len(matching) != 1:
            return None
        return matching[0]
    return rows[0] if len(rows) == 1 else None


def _sequence(
    conn: Any,
    host_id: str,
    worker_id: str,
    route_generation: str,
) -> tuple[str, int]:
    row = conn.execute(
        """UPDATE worker_bindings SET next_partition_sequence=next_partition_sequence+1
        WHERE host_id=? AND worker_id=? AND route_generation=?
          AND (expires_at IS NULL OR expires_at>?)
        RETURNING partition_key,next_partition_sequence-1""",
        (host_id, worker_id, route_generation, utc_now()),
    ).fetchone()
    if row is None:
        raise RuntimeError("route unavailable")
    return str(row[0]), int(row[1])


def _worker_public(
    conn: Any,
    host_id: str,
    worker_id: str,
    binding: Any,
) -> dict[str, Any]:
    snapshot = conn.execute(
        """SELECT payload_json FROM snapshots
        WHERE host_id=? ORDER BY id DESC LIMIT 1""",
        (host_id,),
    ).fetchone()
    workers = json.loads(snapshot[0]).get("workers", []) if snapshot else []
    worker = next((item for item in workers if item.get("id") == worker_id), None)
    if worker is None:
        raise RuntimeError("worker projection unavailable")
    return {
        "worker_id": worker_id,
        "stable_key": binding["stable_key"],
        "stable_key_version": 1,
        "route_generation": binding["route_generation"],
    }


def _projection_blueprint(
    host_id: str,
    turn_id: str,
    revision: str,
    content: Mapping[str, Any],
    complete: bool,
    route_generation: str,
) -> dict[str, Any]:
    if complete:
        final_identity = turn_final_delivery_identity(host_id, turn_id, revision)
        descriptor = _content_descriptor(
            revision,
            content.get("user_text")
            if isinstance(content.get("user_text"), str)
            else None,
            content.get("assistant_final_text")
            if isinstance(content.get("assistant_final_text"), str)
            else None,
        )
        return {
            "key": f"turn-final:revision:{final_identity}",
            "kind": "final_ready",
            "version": 3,
            "final_identity": final_identity,
            "turn": {
                "turn_id": turn_id,
                "final_identity": final_identity,
                "content_revision": revision,
                "content": descriptor,
            },
        }
    text = str(
        content.get("assistant_stream_text")
        or content.get("assistant_final_text")
        or ""
    )
    return {
        "key": "turn-final:working:" + _token(
            "tendwire.working.v1",
            [host_id, turn_id, revision, route_generation],
            "twwork1.",
        ),
        "kind": "working",
        "version": 1,
        "final_identity": None,
        "turn": {
            "turn_id": turn_id,
            "content_revision": revision,
            "text": {
                "assistant_stream_text": text,
                "char_length": len(text),
                "byte_length": len(text.encode("utf-8")),
            },
        },
    }


def _projection_replayed(
    conn: Any,
    host_id: str,
    turn_id: str,
    revision: str,
    worker: Mapping[str, Any],
    partition: str,
    blueprint: Mapping[str, Any],
) -> bool:
    existing = conn.execute(
        """SELECT * FROM connector_outbox
        WHERE host_id=? AND connector='turn-final' AND key=?""",
        (host_id, blueprint["key"]),
    ).fetchone()
    if existing is None:
        return False
    try:
        persisted = _validate_polled_payload(conn, existing)
    except OutboxInvariantError as exc:
        raise OutboxInvariantError("deterministic producer identity conflict") from exc
    persisted_turn = persisted.get("turn")
    persisted_route = persisted.get("route")
    semantic_turn = (
        {name: value for name, value in persisted_turn.items() if name != "replaces_key"}
        if isinstance(persisted_turn, Mapping)
        else None
    )
    if not (
        existing["kind"] == blueprint["kind"]
        and existing["payload_version"] == blueprint["version"]
        and existing["turn_id"] == turn_id
        and existing["final_identity"] == blueprint["final_identity"]
        and existing["content_revision"] == revision
        and persisted.get("worker") == worker
        and isinstance(persisted_route, Mapping)
        and persisted_route.get("partition_key") == partition
        and semantic_turn == blueprint["turn"]
    ):
        raise OutboxInvariantError("deterministic producer identity conflict")
    return True


def _enqueue_projection(
    conn: Any,
    host_id: str,
    worker_id: str,
    turn_id: str,
    revision: str,
    content: Mapping[str, Any],
    complete: bool,
    now: str,
    *,
    binding: Any = None,
) -> None:
    binding = binding if binding is not None else _binding_row(conn, host_id, worker_id)
    if binding is None:
        raise RuntimeError("route unavailable")
    worker = _worker_public(conn, host_id, worker_id, binding)
    partition = str(binding["partition_key"])
    blueprint = _projection_blueprint(
        host_id,
        turn_id,
        revision,
        content,
        complete,
        str(binding["route_generation"]),
    )
    if _projection_replayed(
        conn, host_id, turn_id, revision, worker, partition, blueprint
    ):
        return

    partition, sequence = _sequence(
        conn, host_id, worker_id, str(binding["route_generation"])
    )
    route = {"partition_key": partition, "partition_sequence": sequence}
    previous = conn.execute(
        """SELECT * FROM connector_outbox
        WHERE host_id=? AND connector='turn-final' AND turn_id=?
          AND kind IN('working','final_ready')
          AND status NOT IN('superseded','dead_letter')
        ORDER BY id DESC LIMIT 1""",
        (host_id, turn_id),
    ).fetchone()
    replaces: str | None = None
    if previous is not None and previous["kind"] == "working":
        replaces = str(previous["key"])
    elif previous is not None and previous["kind"] == "final_ready":
        prior_head = conn.execute(
            """SELECT key FROM connector_outbox
            WHERE source_outbox_id=? AND kind='final_part' AND logical_ordinal=0
              AND status='delivered'
            ORDER BY active_lineage_generation DESC,id DESC LIMIT 1""",
            (previous["id"],),
        ).fetchone()
        if prior_head is not None:
            replaces = str(prior_head["key"])
    if complete:
        payload = {
            "schema_version": 3,
            "kind": "final_ready",
            "created_at": now,
            "worker": worker,
            "route": route,
            "turn": {**blueprint["turn"], "replaces_key": replaces},
        }
    else:
        payload = {
            "schema_version": 1,
            "kind": "working",
            "created_at": now,
            "worker": worker,
            "route": route,
            "turn": {**blueprint["turn"], "replaces_key": replaces},
        }
    if previous:
        if previous["kind"] == "final_ready" and previous["status"] == "awaiting_ack":
            conn.execute(
                """UPDATE connector_outbox SET status='superseded',updated_at=?
                WHERE source_outbox_id=? AND kind IN('final_part','retire')
                  AND status IN('staged','blocked','queued','retry','deferred')""",
                (now, previous["id"]),
            )
            conn.execute(
                """UPDATE connector_outbox SET terminal_after_lease=1,updated_at=?
                WHERE source_outbox_id=? AND kind IN('final_part','retire')
                  AND status='leased'""",
                (now, previous["id"]),
            )
        conn.execute(
            """UPDATE connector_outbox SET status='superseded',updated_at=?
            WHERE id=? AND status IN('queued','retry','deferred')""",
            (now, previous["id"]),
        )
        conn.execute(
            """UPDATE connector_outbox SET terminal_after_lease=1,updated_at=?
            WHERE id=? AND status='leased'""",
            (now, previous["id"]),
        )
    conn.execute(
        """INSERT INTO connector_outbox(
        host_id,connector,key,kind,payload_version,status,partition_key,partition_sequence,
        turn_id,final_identity,content_revision,replaces_outbox_id,retry_generation,
        prior_attempt_count,payload_json,created_at,updated_at,available_at)
        VALUES(?,'turn-final',?,?,?,'queued',?,?,?,?,?,?,1,0,?,?,?,?)
        ON CONFLICT(host_id,connector,key) DO NOTHING""",
        (
            host_id,
            blueprint["key"],
            blueprint["kind"],
            blueprint["version"],
            partition,
            sequence,
            turn_id,
            blueprint["final_identity"],
            revision,
            previous["id"] if previous else None,
            _json(payload),
            now,
            now,
            now,
        ),
    )


def _apply(
    conn: Any,
    host_id: str,
    worker_id: str,
    content: Mapping[str, Any],
    now: str,
    *,
    complete: bool,
    expected_binding: WorkerBinding | None = None,
    binding_row: Any = None,
) -> int:
    now = canonical_utc(now)
    connector_now = now
    turn_id = str(content.get("source_turn_id") or content.get("turn_id") or "")
    if not turn_id:
        return 0
    if content.get("removed") is True:
        prior = conn.execute(
            """SELECT worker_id,stable_key,route_generation,observed_at,removed_at FROM turns
            WHERE host_id=? AND turn_id=?""",
            (host_id, turn_id),
        ).fetchone()
        binding = (
            binding_row
            if binding_row is not None
            else _binding_row(conn, host_id, worker_id, expected_binding)
        )
        if (
            prior is None
            or binding is None
            or prior["worker_id"] != worker_id
            or prior["stable_key"] != binding["stable_key"]
            or prior["removed_at"] is not None
            or now < prior["observed_at"]
        ):
            return 0
        change = int(
            conn.execute(
                """SELECT COALESCE(MAX(change_sequence),0)+1
                FROM turns WHERE host_id=?""",
                (host_id,),
            ).fetchone()[0]
        )
        conn.execute(
            """UPDATE turns SET change_sequence=?,state='removed',observed_at=?,removed_at=?
            WHERE host_id=? AND turn_id=?""",
            (change, now, now, host_id, turn_id),
        )
        return 1
    user = sanitize_canonical_turn_text(content.get("user_text"))
    final = sanitize_canonical_turn_text(content.get("assistant_final_text"))
    stream = sanitize_canonical_turn_text(content.get("assistant_stream_text"))
    if stream is None:
        stream = final
    revision = make_revision(
        turn_id,
        user,
        final if complete else stream,
        "complete" if user is not None else "absent",
        "complete" if (final if complete else stream) is not None else "absent",
    )
    prior = conn.execute(
        """SELECT insertion_sequence,change_sequence,state,content_revision,
        worker_id,stable_key,route_generation,observed_at
        FROM turns WHERE host_id=? AND turn_id=?""",
        (host_id, turn_id),
    ).fetchone()
    if prior is not None and (
        now < prior["observed_at"]
        or (prior["state"] in {"removed", "removed_floor"} and now <= prior["observed_at"])
    ):
        return 0
    if prior is not None and prior["state"] == "complete" and not complete:
        return 0
    high = int(
        conn.execute(
            """SELECT COALESCE(MAX(change_sequence),0)+1
            FROM turns WHERE host_id=?""",
            (host_id,),
        ).fetchone()[0]
    )
    insertion = (
        int(prior[0])
        if prior
        else int(
            conn.execute(
                """SELECT COALESCE(MAX(insertion_sequence),0)+1
                FROM turns WHERE host_id=?""",
                (host_id,),
            ).fetchone()[0]
        )
    )
    binding = (
        binding_row
        if binding_row is not None
        else _binding_row(conn, host_id, worker_id, expected_binding)
    )
    if binding is None:
        return 0
    if (
        prior is not None
        and prior["state"] == ("complete" if complete else "working")
        and prior["content_revision"] == revision
        and prior["worker_id"] == worker_id
        and prior["stable_key"] == binding["stable_key"]
        and prior["route_generation"] == binding["route_generation"]
    ):
        return 0
    payload = {
        "id": turn_id,
        "turn_id": turn_id,
        "worker_id": worker_id,
        "user_text": user,
        "assistant_final_text": final,
        "assistant_stream_text": stream,
        "content_revision": revision,
        "complete": complete,
        "observed_at": now,
        "stable_key": binding["stable_key"],
        "stable_key_version": 1,
        "route_generation": binding["route_generation"],
    }
    conn.execute(
        """INSERT INTO turns(
        host_id,turn_id,worker_id,stable_key,stable_key_version,
        route_generation,partition_key,
        insertion_sequence,change_sequence,state,payload_json,content_revision,observed_at,removed_at)
        VALUES(?,?,?,?,1,?,?,?,?,?,?,?,?,NULL) ON CONFLICT(host_id,turn_id) DO UPDATE SET
        worker_id=excluded.worker_id,stable_key=excluded.stable_key,
        route_generation=excluded.route_generation,partition_key=excluded.partition_key,
        change_sequence=excluded.change_sequence,state=excluded.state,
        payload_json=excluded.payload_json,content_revision=excluded.content_revision,
        observed_at=excluded.observed_at,removed_at=NULL""",
        (
            host_id,
            turn_id,
            worker_id,
            binding["stable_key"],
            binding["route_generation"],
            binding["partition_key"],
            insertion,
            high,
            "complete" if complete else "working",
            _json(payload),
            revision,
            now,
        ),
    )
    conn.execute(
        """UPDATE turn_content_revisions SET is_current=0
        WHERE host_id=? AND turn_id=?""",
        (host_id, turn_id),
    )
    conn.execute(
        """INSERT INTO turn_content_revisions(
        host_id,turn_id,content_revision,user_text,assistant_final_text,
        known_incomplete,is_current,created_at)
        VALUES(?,?,?,?,?,0,1,?)
        ON CONFLICT(host_id,turn_id,content_revision) DO UPDATE SET is_current=1""",
        (host_id, turn_id, revision, user, final if complete else stream, now),
    )
    revision_id = int(
        conn.execute(
            """SELECT id FROM turn_content_revisions
            WHERE host_id=? AND turn_id=? AND content_revision=?""",
            (host_id, turn_id, revision),
        ).fetchone()[0]
    )
    conn.execute(
        "DELETE FROM turn_content_page_boundaries WHERE revision_id=?",
        (revision_id,),
    )
    for field, value in (
        ("user_text", user),
        ("assistant_final_text", final if complete else stream),
    ):
        if value is None:
            continue
        for page, span in enumerate(_page_spans(value)):
            conn.execute(
                """INSERT INTO turn_content_page_boundaries(
                revision_id,field,page,start_char,end_char,start_byte,end_byte)
                VALUES(?,?,?,?,?,?,?)""",
                (revision_id, field, page, *span),
            )
    _enqueue_projection(
        conn,
        host_id,
        worker_id,
        turn_id,
        revision,
        {
            **content,
            "user_text": user,
            "assistant_stream_text": stream,
            "assistant_final_text": final if complete else stream,
        },
        complete,
        connector_now,
        binding=binding,
    )
    return 1


def append_agent_event_and_apply_turn_for_binding(
    db_path: Path | str,
    host_id: str,
    event: AgentEvent,
    *,
    expected_binding: WorkerBinding,
    content: Mapping[str, Any] | None = None,
    _fault_inject: Callable[[str], None] | None = None,
) -> AppendProjectedAgentEventResult:
    with write_transaction(db_path) as conn:
        authenticated = _binding_row(
            conn, host_id, event.worker_id, expected_binding
        )
        if authenticated is None:
            return AppendProjectedAgentEventResult(
                AppendBoundAgentEventResult("binding_changed", event.event_id)
            )
        if _fault_inject:
            _fault_inject("after_binding_check")
        appended = _append(conn, host_id, event)
        persisted_observed_at = str(
            conn.execute(
                """SELECT observed_at FROM agent_events
                WHERE host_id=? AND event_id=?""",
                (host_id, event.event_id),
            ).fetchone()[0]
        )
        if _fault_inject:
            _fault_inject("after_event_append")
        turn = None
        if content is not None and (
            appended.inserted
            or conn.execute(
                "SELECT 1 FROM turns WHERE host_id=? AND turn_id=?",
                (host_id, event.source_turn_id),
            ).fetchone()
            is None
        ):
            presentation = (
                authenticated
                if content.get("removed") is True
                else presentation_binding_row(
                    conn,
                    host_id,
                    event.worker_id,
                    authenticated,
                )
            )
            if presentation is None:
                raise RuntimeError("presentation route unavailable")
            complete = content.get("complete") is True
            turn = TurnRefreshApplyResult(
                _apply(
                    conn,
                    host_id,
                    event.worker_id,
                    content,
                    persisted_observed_at,
                    complete=complete,
                    expected_binding=expected_binding,
                    binding_row=presentation,
                ),
                False,
            )
        if _fault_inject:
            _fault_inject("after_turn_projection")
        if _fault_inject:
            _fault_inject("before_commit")
    return AppendProjectedAgentEventResult(
        AppendBoundAgentEventResult(
            "inserted" if appended.inserted else "replayed",
            appended.event_id,
            appended.sequence,
        ),
        turn,
    )


def _store_epoch(db_path: Path | str) -> str:
    stat = Path(db_path).stat()
    return hashlib.sha256(
        _json(["tendwire.store-epoch.v1", stat.st_dev, stat.st_ino]).encode("utf-8")
    ).hexdigest()


def _project_turn_row(row: Any, schema_version: int) -> tuple[dict[str, Any], bool]:
    payload = json.loads(str(row["payload_json"]))
    if not isinstance(payload, Mapping):
        raise ValueError("turn projection is corrupt")
    item = dict(payload)
    stream = item.pop("assistant_stream_text", None)
    item.pop("user_text", None)
    item.pop("assistant_final_text", None)
    user = row["user_text"]
    final = row["assistant_final_text"]
    if isinstance(stream, str) and not bool(item.get("complete")):
        item["assistant_stream_text"] = stream[-TURN_STREAM_TEXT_MAX_CHARS:]
    if schema_version == 1:
        incompatible = any(
            isinstance(text, str) and len(text) > TURN_TEXT_MAX_CHARS
            for text in (user, final)
        )
        item.update(user_text=user, assistant_final_text=final, schema_version=1)
        return item, incompatible
    projection = project_turn_content(str(row["turn_id"]), user, final)
    if projection["content"]["content_revision"] != row["content_revision"]:
        raise ValueError("turn content revision is corrupt")
    item.update(projection)
    item["schema_version"] = TURN_LIST_SCHEMA_VERSION
    return item, False


def _list_state(conn: Any, host_id: str) -> tuple[int, int]:
    row = conn.execute(
        """SELECT COALESCE(MIN(insertion_sequence),0),
        COALESCE(MAX(insertion_sequence),0) FROM turns WHERE host_id=?""",
        (host_id,),
    ).fetchone()
    return int(row[0]), int(row[1])


def _retention_floors(conn: Any, host_id: str) -> tuple[int, int]:
    row = conn.execute(
        """SELECT
        COALESCE(MAX(CAST(json_extract(payload_json,'$._retention_floor.insertion_sequence') AS INTEGER)),0),
        COALESCE(MAX(CAST(json_extract(payload_json,'$._retention_floor.change_sequence') AS INTEGER)),0)
        FROM turns WHERE host_id=? AND state='removed_floor'""",
        (host_id,),
    ).fetchone()
    return int(row[0]), int(row[1])


def turns_payload_from_store(
    db_path: Path | str,
    host_id: str,
    *,
    snapshot: Snapshot | None = None,
    schema_version: int = 1,
    limit: int = TURN_LIST_DEFAULT_LIMIT,
    cursor: str | None = None,
    since: str | None = None,
    now: float | int | None = None,
    **_: Any,
) -> dict[str, Any]:
    error = {"schema_version": schema_version, "host_id": str(host_id), "ok": False}
    if schema_version not in {1, TURN_LIST_SCHEMA_VERSION}:
        return {
            **error,
            "status": "unsupported_turn_schema_version",
            "required_turn_schema_version": TURN_LIST_SCHEMA_VERSION,
        }
    if (
        type(limit) is not int
        or not 1 <= limit <= TURN_LIST_MAX_LIMIT
        or (cursor is not None and since is not None)
    ):
        return {**error, "status": "invalid_cursor"}
    clock = time.time() if now is None else float(now)
    try:
        epoch = _store_epoch(db_path)
        decoded_cursor = (
            decode_turn_list_cursor(
                cursor,
                host_id=str(host_id),
                schema_version=schema_version,
                limit=limit,
                now=clock,
            )
            if cursor is not None
            else None
        )
        decoded_since = (
            decode_turn_since_token(
                since,
                host_id=str(host_id),
                schema_version=schema_version,
            )
            if since is not None
            else None
        )
        with read_transaction(db_path) as conn:
            floor, current_high = _list_state(conn, str(host_id))
            insertion_floor, _change_floor = _retention_floors(conn, str(host_id))
            if decoded_cursor is not None:
                if (
                    decoded_cursor.store_epoch != epoch
                    or decoded_cursor.watermark > current_high
                    or (
                        decoded_cursor.floor_sequence
                        and floor > decoded_cursor.floor_sequence
                    )
                ):
                    return {**error, "status": "cursor_expired"}
                anchor = conn.execute(
                    """SELECT 1 FROM turns
                    WHERE host_id=? AND worker_id=?
                      AND insertion_sequence=? AND turn_id=?""",
                    (
                        host_id,
                        decoded_cursor.worker_id,
                        decoded_cursor.list_sequence,
                        decoded_cursor.turn_id,
                    ),
                ).fetchone()
                if anchor is None:
                    return {**error, "status": "cursor_expired"}
                accepted, high, expires = (
                    decoded_cursor.since_sequence,
                    decoded_cursor.watermark,
                    decoded_cursor.expires_at,
                )
                position = (
                    decoded_cursor.worker_id,
                    decoded_cursor.list_sequence,
                    decoded_cursor.turn_id,
                )
            else:
                if decoded_since is not None and (
                    decoded_since.store_epoch != epoch
                    or decoded_since.watermark > current_high
                    or decoded_since.watermark < insertion_floor
                ):
                    return {**error, "status": "since_expired"}
                accepted, high = (
                    decoded_since.watermark if decoded_since else 0
                ), current_high
                expires, position = int(clock) + TURN_LIST_CURSOR_TTL_SECONDS, None
            params: list[Any] = [str(host_id), accepted, high]
            continuation = ""
            if position is not None:
                continuation = (
                    "AND (t.worker_id>? OR (t.worker_id=? AND "
                    "(t.insertion_sequence<? OR "
                    "(t.insertion_sequence=? AND t.turn_id>?))))"
                )
                params.extend(
                    [
                        position[0],
                        position[0],
                        position[1],
                        position[1],
                        position[2],
                    ]
                )
            params.append(limit + 1)
            rows = conn.execute(
                f"""SELECT t.*,r.user_text,r.assistant_final_text
                FROM turns t LEFT JOIN turn_content_revisions r
                  ON r.host_id=t.host_id AND r.turn_id=t.turn_id AND r.is_current=1
                WHERE t.host_id=? AND t.insertion_sequence>?
                  AND t.insertion_sequence<=? AND t.removed_at IS NULL {continuation}
                ORDER BY t.worker_id,t.insertion_sequence DESC,t.turn_id LIMIT ?""",
                params,
            ).fetchall()
    except ValueError as exc:
        return {
            **error,
            "status": (
                "cursor_expired" if str(exc) == "cursor_expired" else "invalid_cursor"
            ),
        }
    except Exception:
        return {**error, "status": "store_unavailable"}
    selected: list[Any] = []
    projected: list[tuple[dict[str, Any], bool]] = []
    accumulated = 0
    for row in rows[:limit]:
        item = _project_turn_row(row, schema_version)
        item_bytes = 1 if item[1] else len(_json(item[0]).encode("utf-8")) + 1
        if not item[1] and item_bytes > 850_000:
            return {**error, "status": "store_unavailable"}
        if selected and accumulated + item_bytes > 850_000:
            break
        selected.append(row)
        projected.append(item)
        accumulated += item_bytes
    if schema_version == 1 and any(incompatible for _item, incompatible in projected):
        return {
            **error,
            "status": "upgrade_required",
            "required_turn_schema_version": TURN_LIST_SCHEMA_VERSION,
        }
    turns = [item for item, _incompatible in projected]
    has_more = len(rows) > len(selected)
    next_cursor = None
    if has_more and selected:
        tail = selected[-1]
        next_cursor = turn_list_cursor(
            str(host_id),
            schema_version=schema_version,
            limit=limit,
            since_sequence=accepted,
            watermark=high,
            floor_sequence=floor,
            traversal_generation=1,
            worker_id=tail["worker_id"],
            list_sequence=tail["insertion_sequence"],
            turn_id=tail["turn_id"],
            store_epoch=epoch,
            expires_at=expires,
        )
    token = turn_since_token(
        str(host_id),
        schema_version=schema_version,
        watermark=high,
        store_epoch=epoch,
    )
    health = [item.to_dict() for item in snapshot.backend_health] if snapshot else []
    result = {
        "schema_version": schema_version,
        "host_id": str(host_id),
        "updated_at": max(
            (str(row["observed_at"]) for row in selected),
            default=(snapshot.updated_at if snapshot else None),
        ),
        "turns": turns,
        "backend_health": health,
        "next_cursor": next_cursor,
        "has_more": has_more,
        "as_of": token,
        "since": token,
    }
    result["content_fingerprint"] = stable_fingerprint(
        {
            name: result[name]
            for name in (
                "schema_version",
                "host_id",
                "turns",
                "backend_health",
                "has_more",
                "as_of",
            )
        }
    )
    if len(_json(result).encode("utf-8")) > 850_000:
        return {**error, "status": "store_unavailable"}
    return result


def _delta_error(host_id: str, status: str) -> dict[str, Any]:
    return {
        "schema_version": TURN_DELTA_SCHEMA_VERSION,
        "projection_schema_version": TURN_DELTA_PROJECTION_SCHEMA_VERSION,
        "host_id": str(host_id),
        "ok": False,
        "status": status,
    }


def turn_delta_payload_from_store(
    db_path: Path | str,
    host_id: str,
    *,
    watermark: str | None = None,
    cursor: str | None = None,
    limit: int = TURN_DELTA_DEFAULT_LIMIT,
    now: float | int | None = None,
    **_: Any,
) -> dict[str, Any]:
    if (
        type(limit) is not int
        or not 1 <= limit <= TURN_DELTA_MAX_LIMIT
        or (watermark is not None and cursor is not None)
    ):
        return _delta_error(
            host_id,
            "invalid_cursor" if cursor is not None else "invalid_watermark",
        )
    clock = time.time() if now is None else float(now)
    try:
        epoch = _store_epoch(db_path)
        decoded_cursor = (
            decode_turn_delta_cursor(
                cursor,
                host_id=str(host_id),
                limit=limit,
                now=clock,
            )
            if cursor
            else None
        )
        decoded_watermark = (
            decode_turn_delta_watermark(watermark, host_id=str(host_id))
            if watermark
            else None
        )
        with read_transaction(db_path) as conn:
            insertion_high = int(
                conn.execute(
                    """SELECT COALESCE(MAX(insertion_sequence),0)
                    FROM turns WHERE host_id=?""",
                    (host_id,),
                ).fetchone()[0]
            )
            journal_high = int(
                conn.execute(
                    """SELECT COALESCE(MAX(change_sequence),0)
                    FROM turns WHERE host_id=?""",
                    (host_id,),
                ).fetchone()[0]
            )
            _insertion_floor, change_floor = _retention_floors(conn, str(host_id))
            if decoded_cursor:
                if decoded_cursor.store_epoch != epoch:
                    return _delta_error(host_id, "invalid_cursor")
                mode, accepted, batch_high = (
                    decoded_cursor.mode,
                    decoded_cursor.accepted_sequence,
                    decoded_cursor.batch_high,
                )
                if mode == "changes" and accepted < change_floor:
                    return _delta_error(host_id, "expired_cursor")
                insertion_high, expires, page = (
                    decoded_cursor.insertion_high,
                    decoded_cursor.expires_at,
                    decoded_cursor.page_number + 1,
                )
                position = (
                    decoded_cursor.position_worker_id,
                    decoded_cursor.position_sequence,
                    decoded_cursor.position_turn_id,
                )
                anchor_column = (
                    "insertion_sequence" if mode == "bootstrap" else "change_sequence"
                )
                anchor = conn.execute(
                    f"SELECT 1 FROM turns WHERE host_id=? AND {anchor_column}=? AND turn_id=?",
                    (host_id, position[1], position[2]),
                ).fetchone()
                if anchor is None:
                    return _delta_error(host_id, "expired_cursor")
            elif decoded_watermark:
                if (
                    decoded_watermark.store_epoch != epoch
                    or decoded_watermark.sequence > journal_high
                ):
                    return _delta_error(host_id, "invalid_watermark")
                if decoded_watermark.sequence < change_floor:
                    return _delta_error(host_id, "expired_watermark")
                mode, accepted = "changes", decoded_watermark.sequence
                batch_high = min(
                    journal_high,
                    accepted + TURN_DELTA_MAX_BATCH_SEQUENCES,
                )
                expires, page, position = (
                    int(clock) + TURN_DELTA_CURSOR_TTL_SECONDS,
                    1,
                    None,
                )
            else:
                mode, accepted, batch_high = "bootstrap", 0, journal_high
                expires, page, position = (
                    int(clock) + TURN_DELTA_CURSOR_TTL_SECONDS,
                    1,
                    None,
                )
                bootstrap_count = int(
                    conn.execute(
                        """SELECT COUNT(*) FROM turns
                        WHERE host_id=? AND removed_at IS NULL
                          AND insertion_sequence<=?""",
                        (host_id, insertion_high),
                    ).fetchone()[0]
                )
                if (
                    bootstrap_count > TURN_DELTA_BOOTSTRAP_MAX_ROWS
                    or (bootstrap_count + TURN_DELTA_MAX_LIMIT - 1) // TURN_DELTA_MAX_LIMIT
                    > TURN_DELTA_BOOTSTRAP_MAX_PAGES
                ):
                    return _delta_error(host_id, "bootstrap_too_large")
            params: list[Any] = [str(host_id)]
            if mode == "bootstrap":
                where = "removed_at IS NULL AND insertion_sequence<=?"
                params.append(insertion_high)
                if position:
                    where += (
                        " AND (t.worker_id>? OR (t.worker_id=? AND "
                        "(t.insertion_sequence<? OR "
                        "(t.insertion_sequence=? AND t.turn_id>?))))"
                    )
                    params.extend(
                        [
                            position[0],
                            position[0],
                            position[1],
                            position[1],
                            position[2],
                        ]
                    )
                order = "t.worker_id,t.insertion_sequence DESC,t.turn_id"
            else:
                where = "change_sequence>? AND change_sequence<=?"
                params.extend([accepted, batch_high])
                if position:
                    where += (
                        " AND (t.change_sequence>? OR "
                        "(t.change_sequence=? AND t.turn_id>?))"
                    )
                    params.extend([position[1], position[1], position[2]])
                order = "t.change_sequence,t.turn_id"
            params.append(limit + 1)
            rows = conn.execute(
                f"""SELECT t.*,r.user_text,r.assistant_final_text FROM turns t
                LEFT JOIN turn_content_revisions r
                  ON r.host_id=t.host_id AND r.turn_id=t.turn_id AND r.is_current=1
                WHERE t.host_id=? AND {where} ORDER BY {order} LIMIT ?""",
                params,
            ).fetchall()
    except ValueError as exc:
        allowed = {
            "invalid_watermark",
            "expired_watermark",
            "cross_host_watermark",
            "incompatible_schema",
            "invalid_cursor",
            "expired_cursor",
        }
        return _delta_error(
            host_id,
            str(exc)
            if str(exc) in allowed
            else ("invalid_cursor" if cursor else "invalid_watermark"),
        )
    except Exception:
        return _delta_error(host_id, "store_unavailable")
    selected: list[Any] = []
    changes = []
    accumulated = 0
    for row in rows[:limit]:
        removed = row["removed_at"] is not None
        change = {
            "op": "remove" if removed else "upsert",
            "turn_id": row["turn_id"],
            "changed_at": row["removed_at"] or row["observed_at"],
            "turn": (
                None
                if removed
                else _project_turn_row(row, TURN_LIST_SCHEMA_VERSION)[0]
            ),
        }
        item_bytes = len(_json(change).encode("utf-8")) + 1
        if item_bytes > 850_000:
            return _delta_error(host_id, "store_unavailable")
        if selected and accumulated + item_bytes > 850_000:
            break
        selected.append(row)
        changes.append(change)
        accumulated += item_bytes
    has_more = len(rows) > len(selected)
    next_cursor = None
    if has_more and selected:
        tail = selected[-1]
        next_cursor = turn_delta_cursor(
            str(host_id),
            mode=mode,
            limit=limit,
            accepted_sequence=accepted,
            batch_high=batch_high,
            insertion_high=insertion_high,
            page_number=page,
            position_worker_id=(tail["worker_id"] if mode == "bootstrap" else ""),
            position_sequence=(
                tail["insertion_sequence"]
                if mode == "bootstrap"
                else tail["change_sequence"]
            ),
            position_turn_id=tail["turn_id"],
            store_epoch=epoch,
            expires_at=expires,
        )
    checkpoint = (
        None
        if has_more
        else turn_delta_watermark(
            str(host_id),
            sequence=batch_high,
            store_epoch=epoch,
        )
    )
    result = {
        "schema_version": TURN_DELTA_SCHEMA_VERSION,
        "projection_schema_version": TURN_DELTA_PROJECTION_SCHEMA_VERSION,
        "host_id": str(host_id),
        "mode": mode,
        "changes": changes,
        "has_more": has_more,
        "next_cursor": next_cursor,
        "checkpoint": checkpoint,
        "aggregate": {
            "journal_rows_scanned": len(rows) if mode == "changes" else 0,
            "projection_rows_read": len(rows),
            "changes_returned": len(changes),
            "duration_ms": 0,
        },
    }
    return (
        result
        if len(_json(result).encode("utf-8")) <= 850_000
        else _delta_error(host_id, "store_unavailable")
    )


def get_turn_content(
    db_path: Path,
    host_id: str,
    *,
    turn_id: str,
    content_revision: str,
    field: str,
    cursor: str | None = None,
    schema_version: int = 1,
    **_: Any,
) -> dict[str, Any]:
    if schema_version != 1:
        return {
            "schema_version": schema_version,
            "ok": False,
            "status": "unsupported_content_schema_version",
            "required_content_schema_version": 1,
        }
    if field not in {"user_text", "assistant_final_text"}:
        return {"schema_version": 1, "ok": False, "status": "invalid_content_field"}
    with read_transaction(db_path) as conn:
        row = conn.execute(
            f"""SELECT id,{field} FROM turn_content_revisions
            WHERE host_id=? AND turn_id=? AND content_revision=?""",
            (host_id, turn_id, content_revision),
        ).fetchone()
    if row is None:
        return {
            "schema_version": 1,
            "ok": False,
            "status": "content_revision_not_found",
        }
    if row[1] is None:
        return {"schema_version": 1, "ok": False, "status": "content_not_available"}
    text = str(row[1])
    try:
        page = build_turn_content_page(
            turn_id,
            content_revision,
            field,
            text,
            cursor=cursor,
            max_utf8_bytes=TURN_CONTENT_PAGE_MAX_UTF8_BYTES,
        )
    except ValueError as exc:
        return {
            "schema_version": 1,
            "ok": False,
            "status": (
                "content_unavailable"
                if str(exc) == "content_has_no_segments"
                else "invalid_cursor"
            ),
        }
    with read_transaction(db_path) as conn:
        boundary = conn.execute(
            """SELECT start_char,end_char,start_byte,end_byte
            FROM turn_content_page_boundaries
            WHERE revision_id=? AND field=? AND page=?""",
            (row[0], field, page["index"]),
        ).fetchone()
    if boundary is None:
        return {"schema_version": 1, "ok": False, "status": "content_unavailable"}
    if (
        int(boundary[1]) - int(boundary[0]) != page["segment_char_length"]
        or int(boundary[3]) - int(boundary[2]) != page["segment_byte_length"]
    ):
        return {"schema_version": 1, "ok": False, "status": "content_unavailable"}
    return {"ok": True, **page}


__all__ = (
    "AppendProjectedAgentEventResult",
    "TurnRefreshApplyResult",
    "append_agent_event_and_apply_turn_for_binding",
    "turns_payload_from_store",
    "turn_delta_payload_from_store",
    "get_turn_content",
)
