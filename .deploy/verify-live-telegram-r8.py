#!/usr/bin/python3 -I
"""Verify exact public final rendering against the live Telegram messages."""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import stat
import sys
from pathlib import Path

sys.path.insert(
    0,
    "/home/smith/.local/share/acp-runtime/releases/"
    "67568f32-0b94403-f50af73-r8/herdres",
)

from telethon.crypto import AuthKey
from telethon.extensions.html import parse as parse_html
from telethon.sessions import MemorySession
from telethon.sync import TelegramClient

from herdres_connector.presentation import (
    FinalPartPayload,
    materialize_content,
    render_spans,
    validate_payload,
)
from herdres_connector.tendwire_client import TendwireClient


TENDWIRE_DB = Path("/home/smith/.local/share/tendwire/candidates/0b94403-r8/tendwire.db")
STATE_PATH = Path("/home/smith/.local/share/herdres/candidates/f50af73-r8/state.json")
ENV_PATH = Path("/home/smith/.config/herdres/frozen-0b94403-f50af73-r8.env")
SESSION_PATH = Path("/home/smith/.local/share/contexto/contexto_capture.session")
SOCKET_PATH = Path("/home/smith/.local/share/tendwire/tendwire.sock")
API_ID = 2040
API_HASH = "b18441a1ff607e10a989891a5462e627"


def private_file(path: Path, maximum: int) -> bytes:
    flags = os.O_RDONLY | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags)
    try:
        before = os.fstat(descriptor)
        named = path.lstat()
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_uid != os.geteuid()
            or before.st_nlink != 1
            or stat.S_IMODE(before.st_mode) & 0o077
            or (before.st_dev, before.st_ino) != (named.st_dev, named.st_ino)
            or before.st_size > maximum
        ):
            raise RuntimeError("unsafe private input")
        pieces: list[bytes] = []
        length = 0
        while True:
            block = os.read(descriptor, min(1_048_576, maximum + 1 - length))
            if not block:
                break
            pieces.append(block)
            length += len(block)
            if length > maximum:
                raise RuntimeError("private input exceeds bound")
        after = os.fstat(descriptor)
        named_after = path.lstat()
        stable = ("st_dev", "st_ino", "st_size", "st_mtime_ns", "st_ctime_ns")
        if any(getattr(before, key) != getattr(after, key) for key in stable) or any(
            getattr(after, key) != getattr(named_after, key) for key in stable
        ):
            raise RuntimeError("private input changed")
        return b"".join(pieces)
    finally:
        os.close(descriptor)


def environment() -> dict[str, str]:
    result: dict[str, str] = {}
    for raw in private_file(ENV_PATH, 65_536).decode().splitlines():
        if not raw or raw.startswith("#"):
            continue
        key, separator, value = raw.partition("=")
        if not separator or key in result or "\0" in value:
            raise RuntimeError("invalid environment")
        result[key] = value
    return result


def authorized_client() -> TelegramClient:
    session_raw = private_file(SESSION_PATH, 16_777_216)
    database = sqlite3.connect(":memory:")
    try:
        database.deserialize(session_raw)
        row = database.execute(
            "SELECT dc_id,server_address,port,auth_key FROM sessions LIMIT 1"
        ).fetchone()
    finally:
        database.close()
    if row is None or not isinstance(row[3], bytes) or len(row[3]) != 256:
        raise RuntimeError("Telegram authorization unavailable")
    memory = MemorySession()
    memory.set_dc(int(row[0]), str(row[1]), int(row[2]))
    memory.auth_key = AuthKey(row[3])
    return TelegramClient(memory, API_ID, API_HASH)


def entity_rows(value: object) -> list[dict[str, object]]:
    rows = []
    for entity in list(value or []):
        row = entity.to_dict()
        if not isinstance(row, dict):
            raise RuntimeError("invalid Telegram entity")
        rows.append(row)
    return rows


def render_fingerprint(text: str) -> str:
    body = json.dumps(
        {"text": text, "markup": None},
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )
    return hashlib.sha256(body.encode()).hexdigest()


def verify() -> int:
    env = environment()
    state = json.loads(private_file(STATE_PATH, 16_777_216))
    jobs = state.get("provider_jobs") or {}
    messages = state.get("provider_messages") or {}
    aliases = state.get("provider_aliases") or {}
    workers = state.get("workers") or {}
    live_workers = [
        row for row in workers.values() if row.get("lifecycle_status") == "live"
    ]
    if len(live_workers) != 1:
        raise RuntimeError("invalid live worker count")
    bot_kind = str(live_workers[0].get("bot_kind") or "").upper()
    bot_username = env[f"TELEGRAM_{bot_kind}_BOT_USERNAME"].lstrip("@").lower()
    chat_id = int(env["TELEGRAM_CHAT_ID"])
    with sqlite3.connect(f"file:{TENDWIRE_DB}?mode=ro", uri=True) as connection:
        rows = connection.execute(
            "SELECT key,payload_json FROM connector_outbox "
            "WHERE kind='final_part' AND status='delivered' "
            "ORDER BY turn_id,logical_ordinal"
        ).fetchall()
    if not rows:
        raise RuntimeError("no delivered final parts")
    tendwire = TendwireClient(socket_path=SOCKET_PATH, timeout=20)
    checked = 0
    edited_working = 0
    with authorized_client() as client:
        peer = next(
            (
                dialog.input_entity
                for dialog in client.iter_dialogs()
                if int(dialog.id) == chat_id
            ),
            None,
        )
        if peer is None:
            raise RuntimeError("Telegram forum unavailable")
        for key, raw_payload in rows:
            payload = validate_payload(str(key), json.loads(raw_payload))
            if not isinstance(payload, FinalPartPayload):
                raise RuntimeError("invalid final part")
            fields = tuple(dict.fromkeys(span.field for span in payload.spans))
            content = materialize_content(
                turn_id=payload.turn_id,
                content_revision=payload.content_revision,
                getter=tendwire.turn_content_get,
                fields=fields,
            )
            expected_html = render_spans(
                content,
                payload.spans,
                ordinal=payload.ordinal,
                part_count=payload.part_count,
            )
            job = jobs.get(key) or {}
            binding = job.get("provider_message_binding_id")
            message_row = messages.get(binding) or {}
            owner = job.get("physical_owner") or {}
            if (
                job.get("kind") != "final_part"
                or job.get("render_fingerprint") != render_fingerprint(expected_html)
                or job.get("captured_route_generation")
                != payload.worker.route_generation
                or job.get("outcome") not in {"sent", "edited", "not_modified"}
                or message_row.get("current_owner_key") != key
                or message_row.get("status") != "active"
                or message_row.get("physical_owner") != owner
                or message_row.get("captured_route_generation")
                != payload.worker.route_generation
                or int(owner.get("chat_id")) != chat_id
            ):
                raise RuntimeError("provider proof mismatch")
            if payload.ordinal == 0:
                prior_key = payload.replaces_key
                prior_job = jobs.get(prior_key) or {}
                alias = aliases.get(prior_key) or {}
                if (
                    not prior_key
                    or job.get("outcome") != "edited"
                    or message_row.get("created_kind") != "working"
                    or prior_job.get("kind") != "working"
                    or prior_job.get("captured_route_generation")
                    != payload.worker.route_generation
                    or prior_job.get("provider_message_binding_id") != binding
                    or alias.get("provider_message_binding_id") != binding
                    or alias.get("current_owner_key") != key
                ):
                    raise RuntimeError("working replacement proof mismatch")
            telegram_message = client.get_messages(
                peer, ids=int(message_row["message_id"])
            )
            if telegram_message is None:
                raise RuntimeError("Telegram message missing")
            sender = client.get_entity(telegram_message.sender_id)
            reply = getattr(telegram_message, "reply_to", None)
            thread_id = (
                getattr(reply, "reply_to_top_id", None)
                or getattr(reply, "reply_to_msg_id", None)
            )
            expected_text, expected_entities = parse_html(expected_html)
            if (
                str(getattr(sender, "username", "") or "").lower() != bot_username
                or int(thread_id or 0) != int(owner["topic_id"])
                or str(getattr(telegram_message, "message", "") or "")
                != expected_text
                or entity_rows(getattr(telegram_message, "entities", None))
                != entity_rows(expected_entities)
                or getattr(telegram_message, "reply_markup", None) is not None
            ):
                raise RuntimeError("live Telegram rendering mismatch")
            if payload.ordinal == 0:
                if getattr(telegram_message, "edit_date", None) is None:
                    raise RuntimeError("working message was not edited")
                edited_working += 1
            checked += 1
    if checked != len(rows) or edited_working < 1:
        raise RuntimeError("incomplete Telegram verification")
    print(
        json.dumps(
            {
                "schema_version": 1,
                "verified_final_parts": checked,
                "all_messages_exact": True,
                "working_card_edited": True,
                "text_emitted": False,
            },
            sort_keys=True,
            separators=(",", ":"),
        )
    )
    return 0


def main() -> int:
    try:
        return verify()
    except Exception:
        print("live_telegram_render_mismatch", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
