from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sqlite3
import stat
import subprocess
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator


PRIVATE_ENV = Path("/home/smith/.config/herdres/frozen-7446533-6c0d0f5.env")
SESSION_PATH = Path("/home/smith/.local/share/contexto/contexto_capture.session")
STATE_PATH = Path("/home/smith/.local/share/herdres/candidates/6c0d0f5/state.json")
INGRESS_PATH = Path("/home/smith/.local/share/herdres/candidates/6c0d0f5/ingress.db")
PHASE_PATH = Path("/home/smith/.local/state/acp-cutover/frozen-7446533-6c0d0f5/phase")
EVIDENCE_ROOT = PHASE_PATH.parent
PLAN_PATH = EVIDENCE_ROOT / "telegram-topic-reset-plan.json"
RESET_EVIDENCE_PATH = EVIDENCE_ROOT / "telegram-topic-reset-evidence.json"
RESET_STARTED_PATH = EVIDENCE_ROOT / "telegram-topic-reset-started.json"
PRESENTER_EVIDENCE_PATH = EVIDENCE_ROOT / "telegram-topic-presenter-evidence.json"
GATEWAY_EVIDENCE_PATH = EVIDENCE_ROOT / "telegram-fresh-cursor-evidence.json"
API_ID = 2040
API_HASH = "b18441a1ff607e10a989891a5462e627"
EXPECTED_RECEIVERS = {"manager", "managed-codex", "managed-omp", "managed-kimi"}


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
            raise RuntimeError("private topic input is unsafe")
        chunks: list[bytes] = []
        length = 0
        while True:
            chunk = os.read(descriptor, min(1_048_576, maximum + 1 - length))
            if not chunk:
                break
            chunks.append(chunk)
            length += len(chunk)
            if length > maximum:
                raise RuntimeError("private topic input exceeds bound")
        after = os.fstat(descriptor)
        named_after = path.lstat()
        stable = ("st_dev", "st_ino", "st_size", "st_mtime_ns", "st_ctime_ns")
        if any(getattr(before, key) != getattr(after, key) for key in stable) or any(
            getattr(after, key) != getattr(named_after, key) for key in stable
        ):
            raise RuntimeError("private topic input changed during read")
        return b"".join(chunks)
    finally:
        os.close(descriptor)


def canonical(value: dict[str, object]) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def atomic_private_json(path: Path, value: dict[str, object]) -> str:
    parent = path.parent
    parent_info = parent.lstat()
    if (
        not stat.S_ISDIR(parent_info.st_mode)
        or parent_info.st_uid != os.geteuid()
        or stat.S_IMODE(parent_info.st_mode) & 0o077
    ):
        raise RuntimeError("topic evidence directory is unsafe")
    temporary = path.with_name(path.name + f".tmp.{os.getpid()}")
    if path.exists() or path.is_symlink() or temporary.exists() or temporary.is_symlink():
        raise RuntimeError("topic evidence already exists")
    body = canonical(value)
    descriptor = os.open(
        temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC, 0o600
    )
    try:
        view = memoryview(body)
        while view:
            written = os.write(descriptor, view)
            if written < 1:
                raise RuntimeError("topic evidence write failed")
            view = view[written:]
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    os.replace(temporary, path)
    directory = os.open(parent, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)
    return hashlib.sha256(body).hexdigest()


def read_env() -> dict[str, str]:
    values: dict[str, str] = {}
    for number, raw in enumerate(private_file(PRIVATE_ENV, 65_536).decode().splitlines(), 1):
        if not raw or raw.startswith("#"):
            continue
        key, separator, value = raw.partition("=")
        if (
            not separator
            or re.fullmatch(r"[A-Z][A-Z0-9_]{0,127}", key) is None
            or key in values
            or len(value.encode()) > 4096
            or "\x00" in value
        ):
            raise RuntimeError(f"invalid private environment line {number}")
        values[key] = value
    required = {"TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID"}
    if not required <= set(values):
        raise RuntimeError("private Telegram environment is incomplete")
    return values


@contextmanager
def authorized_client() -> Iterator[object]:
    from telethon.crypto import AuthKey
    from telethon.sessions import MemorySession
    from telethon.sync import TelegramClient

    # Contexto owns this dedicated capture authorization. Copying it into a
    # MemorySession keeps the on-disk SQLite database strictly read-only.
    session_raw = private_file(SESSION_PATH, 16_777_216)
    database = sqlite3.connect(":memory:")
    try:
        database.deserialize(session_raw)
        row = database.execute(
            "SELECT dc_id, server_address, port, auth_key FROM sessions LIMIT 1"
        ).fetchone()
    finally:
        database.close()
    if row is None or not isinstance(row[3], bytes) or len(row[3]) != 256:
        raise RuntimeError("dedicated Telegram session is unavailable")
    memory = MemorySession()
    memory.set_dc(int(row[0]), str(row[1]), int(row[2]))
    memory.auth_key = AuthKey(row[3])
    client = TelegramClient(memory, API_ID, API_HASH)
    client.connect()
    try:
        identity = client.get_me()
        if identity is None or bool(getattr(identity, "bot", False)):
            raise RuntimeError("dedicated Telegram user authorization is unavailable")
        yield client
    finally:
        client.disconnect()


def forum_topics(client: object, chat_id: int) -> set[int]:
    from telethon.tl.functions.messages import GetForumTopicsRequest

    peer = next(
        (dialog.input_entity for dialog in client.iter_dialogs() if int(dialog.id) == chat_id),
        None,
    )
    if peer is None:
        raise RuntimeError("forum entity is absent from authorized dialogs")
    topics: set[int] = set()
    offset_date = None
    offset_id = 0
    offset_topic = 0
    for _page in range(20):
        response = client(
            GetForumTopicsRequest(
                peer=peer,
                q="",
                offset_date=offset_date,
                offset_id=offset_id,
                offset_topic=offset_topic,
                limit=100,
            )
        )
        page = list(response.topics)
        before = len(topics)
        topics.update(int(topic.id) for topic in page)
        if len(page) < 100 or len(topics) == before:
            return topics
        last = page[-1]
        offset_date = getattr(last, "date", None)
        offset_id = int(getattr(last, "top_message", 0) or 0)
        offset_topic = int(last.id)
    raise RuntimeError("Telegram topic inventory exceeds bound")


def require_topic_admin(client: object, chat_id: int) -> None:
    from telethon.tl.functions.channels import GetParticipantRequest

    identity = client.get_me()
    peer = next(
        (dialog.input_entity for dialog in client.iter_dialogs() if int(dialog.id) == chat_id),
        None,
    )
    if identity is None or peer is None:
        raise RuntimeError("forum administrator identity is unavailable")
    participant = client(GetParticipantRequest(peer, identity)).participant
    rights = getattr(participant, "admin_rights", None)
    if not (
        rights is not None
        and bool(getattr(rights, "manage_topics", False))
        and bool(getattr(rights, "delete_messages", False))
    ):
        raise RuntimeError("forum topic reset authorization is insufficient")


def phase_is(*allowed: str) -> None:
    if private_file(PHASE_PATH, 64).decode().strip() not in set(allowed):
        raise RuntimeError("frozen cutover phase does not authorize this operation")


def services_stopped() -> None:
    for unit in ("herdres.service", "herdres-gateway.service"):
        result = subprocess.run(
            ["systemctl", "--user", "is-active", "--quiet", unit], check=False
        )
        if result.returncode == 0:
            raise RuntimeError("Telegram writer is still active")


def plan() -> tuple[dict[str, object], bytes, str]:
    raw = private_file(PLAN_PATH, 1_048_576)
    value = json.loads(raw)
    if not isinstance(value, dict) or raw != canonical(value):
        raise RuntimeError("topic reset plan is not canonical")
    return value, raw, hashlib.sha256(raw).hexdigest()


def exact_private_json(path: Path, expected: dict[str, object]) -> str:
    raw = private_file(path, 1_048_576)
    value = json.loads(raw)
    if value != expected or raw != canonical(expected):
        raise RuntimeError("topic evidence does not match the bound transaction")
    return hashlib.sha256(raw).hexdigest()


def create_or_verify_private_json(path: Path, value: dict[str, object]) -> str:
    if path.exists() and not path.is_symlink():
        return exact_private_json(path, value)
    return atomic_private_json(path, value)


def public_result(mode: str, **values: object) -> int:
    result = {"schema_version": 1, "mode": mode, **values}
    print(json.dumps(result, sort_keys=True))
    return 0


def preview() -> int:
    phase_is("prepared")
    env = read_env()
    chat_id = int(env["TELEGRAM_CHAT_ID"])
    with authorized_client() as client:
        require_topic_admin(client, chat_id)
        topics = forum_topics(client, chat_id)
    if 1 not in topics:
        raise RuntimeError("General topic is absent")
    selected = sorted(topics - {1})
    value: dict[str, object] = {
        "schema_version": 1,
        "operation": "reset_all_non_general_topics",
        "chat_id": chat_id,
        "inventory_topic_ids": sorted(topics),
        "reset_topic_ids": selected,
    }
    digest = atomic_private_json(PLAN_PATH, value)
    return public_result(
        "preview", topics_before=len(topics), reset_selected=len(selected),
        general_preserved=True, plan_sha256=digest,
    )


def apply_reset() -> int:
    from telethon.tl.functions.messages import DeleteTopicHistoryRequest

    phase_is("committing")
    saved, raw, digest = plan()
    chat_id = int(saved["chat_id"])
    selected = {int(value) for value in saved["reset_topic_ids"]}
    expected = {int(value) for value in saved["inventory_topic_ids"]}
    if 1 in selected or expected != selected | {1}:
        raise RuntimeError("topic reset plan is invalid")
    final_evidence: dict[str, object] = {
        "schema_version": 1,
        "operation": "reset_all_non_general_topics",
        "plan_sha256": digest,
        "plan_bytes_sha256": hashlib.sha256(raw).hexdigest(),
        "topics_before": len(expected),
        "topics_reset": len(selected),
        "topics_after": 1,
        "general_preserved": True,
    }
    if RESET_EVIDENCE_PATH.exists() and not RESET_EVIDENCE_PATH.is_symlink():
        services_stopped()
        evidence_digest = exact_private_json(RESET_EVIDENCE_PATH, final_evidence)
        if STATE_PATH.is_file() and not STATE_PATH.is_symlink():
            presenter_snapshot()
        else:
            with authorized_client() as client:
                if forum_topics(client, chat_id) != {1}:
                    raise RuntimeError("Telegram forum drifted before presenter start")
        return public_result(
            "apply", topics_before=len(expected), topics_reset=len(selected),
            topics_after=1, general_preserved=True, plan_sha256=digest,
            evidence_sha256=evidence_digest, resumed=True,
        )
    services_stopped()
    if STATE_PATH.exists() or STATE_PATH.is_symlink() or INGRESS_PATH.exists() or INGRESS_PATH.is_symlink():
        raise RuntimeError("fresh candidate state must be absent before topic reset")
    started_evidence: dict[str, object] = {
        "schema_version": 1,
        "operation": "reset_all_non_general_topics",
        "plan_sha256": digest,
        "reset_count": len(selected),
    }
    with authorized_client() as client:
        require_topic_admin(client, chat_id)
        before = forum_topics(client, chat_id)
        if 1 not in before or not before <= expected:
            raise RuntimeError("Telegram inventory diverged from the reset plan")
        peer = next(
            dialog.input_entity for dialog in client.iter_dialogs() if int(dialog.id) == chat_id
        )
        create_or_verify_private_json(RESET_STARTED_PATH, started_evidence)
        for topic_id in sorted(before - {1}):
            client(DeleteTopicHistoryRequest(peer=peer, top_msg_id=topic_id))
        for _attempt in range(60):
            time.sleep(1)
            after = forum_topics(client, chat_id)
            if after == {1}:
                break
        else:
            raise RuntimeError("Telegram topic reset did not converge")
    evidence_digest = create_or_verify_private_json(RESET_EVIDENCE_PATH, final_evidence)
    return public_result(
        "apply", topics_before=len(expected), topics_reset=len(selected), topics_after=len(after),
        general_preserved=after == {1}, plan_sha256=digest,
        evidence_sha256=evidence_digest,
    )


def gate_presenter() -> int:
    phase_is("committing")
    services_stopped()
    if STATE_PATH.exists() or STATE_PATH.is_symlink() or INGRESS_PATH.exists() or INGRESS_PATH.is_symlink():
        raise RuntimeError("candidate state appeared before presenter gate")
    saved, _raw, digest = plan()
    with authorized_client() as client:
        topics = forum_topics(client, int(saved["chat_id"]))
    if topics != {1}:
        raise RuntimeError("non-General topics remain before presenter start")
    if not RESET_EVIDENCE_PATH.is_file():
        raise RuntimeError("topic reset evidence is absent")
    return public_result(
        "presenter_gate", forum_topic_count=1, non_general_topics=0,
        candidate_state_absent=True, general_preserved=True, plan_sha256=digest,
    )


def presenter_snapshot() -> tuple[dict[str, object], str]:
    saved, _raw, digest = plan()
    state_raw = private_file(STATE_PATH, 16_777_216)
    state = json.loads(state_raw)
    if state.get("schema_version") != 1:
        raise RuntimeError("candidate state schema is invalid")
    workers = state.get("workers")
    topic_rows = state.get("topics")
    claims = state.get("topic_create_claims")
    if not all(isinstance(value, dict) for value in (workers, topic_rows, claims)):
        raise RuntimeError("candidate topic state is invalid")
    live = [row for row in workers.values() if row.get("lifecycle_status") == "live"]
    active = [row for row in topic_rows.values() if row.get("status") == "active"]
    topic_ids = [int(row["physical_owner"]["topic_id"]) for row in active]
    owners = {row.get("lifecycle_owner_key") for row in active}
    live_keys = {row.get("stable_key") for row in live}
    unsettled = [
        row for row in claims.values() if row.get("status") in {"reserved", "in_flight", "ambiguous"}
    ]
    valid = (
        len(live) == 1
        and len(active) == len(live)
        and len(topic_ids) == len(set(topic_ids))
        and owners == live_keys
        and not unsettled
        and all(topic_id != 1 for topic_id in topic_ids)
    )
    if not valid:
        raise RuntimeError("presenter topic bindings are incomplete")
    with authorized_client() as client:
        forum = forum_topics(client, int(saved["chat_id"]))
    if forum != set(topic_ids) | {1}:
        raise RuntimeError("presenter topics do not match the live forum")
    return {
        "schema_version": 1,
        "plan_sha256": digest,
        "live_workers": len(live),
        "active_bindings": len(active),
        "forum_topics": len(forum),
        "general_preserved": 1 in forum,
        "unsettled_topic_claims": 0,
    }, digest


def verify_presenter() -> int:
    phase_is("committing")
    evidence, digest = presenter_snapshot()
    evidence_digest = create_or_verify_private_json(PRESENTER_EVIDENCE_PATH, evidence)
    return public_result(
        "verify_presenter", live_workers=evidence["live_workers"],
        active_bindings=evidence["active_bindings"],
        forum_topics=evidence["forum_topics"], general_preserved=True,
        plan_sha256=digest,
        evidence_sha256=evidence_digest,
    )


def gate_existing_presenter() -> int:
    phase_is("committing")
    services_stopped()
    if not STATE_PATH.is_file():
        raise RuntimeError("candidate presenter state is absent")
    evidence, digest = presenter_snapshot()
    return public_result(
        "existing_presenter_gate", live_workers=evidence["live_workers"],
        active_bindings=evidence["active_bindings"],
        forum_topics=evidence["forum_topics"], general_preserved=True,
        unsettled_topic_claims=0, plan_sha256=digest,
    )


def monitor_sample() -> int:
    phase_is("validating")
    evidence, digest = presenter_snapshot()
    return public_result(
        "monitor_sample", live_workers=evidence["live_workers"],
        active_bindings=evidence["active_bindings"],
        forum_topics=evidence["forum_topics"], general_preserved=True,
        unsettled_topic_claims=0, plan_sha256=digest,
    )


def verify_gateway() -> int:
    phase_is("committing")
    descriptor = os.open(
        INGRESS_PATH,
        os.O_RDONLY | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0),
    )
    os.close(descriptor)
    connection = sqlite3.connect(f"file:{INGRESS_PATH}?mode=ro", uri=True, timeout=10)
    try:
        cursors = {row[0] for row in connection.execute("SELECT receiver_id FROM receiver_cursors")}
        requests = int(connection.execute("SELECT COUNT(*) FROM requests").fetchone()[0])
    finally:
        connection.close()
    if cursors != EXPECTED_RECEIVERS or requests != 0:
        raise RuntimeError("fresh Telegram cursor evidence is incomplete")
    saved, _raw, digest = plan()
    evidence = {
        "schema_version": 1,
        "plan_sha256": digest,
        "receiver_count": len(cursors),
        "request_count": requests,
        "historical_requests_discarded": requests == 0,
    }
    evidence_digest = create_or_verify_private_json(GATEWAY_EVIDENCE_PATH, evidence)
    return public_result(
        "verify_gateway", receiver_count=len(cursors), request_count=requests,
        historical_requests_discarded=True, plan_sha256=digest,
        evidence_sha256=evidence_digest,
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument("--apply", action="store_true")
    modes.add_argument("--gate-presenter", action="store_true")
    modes.add_argument("--verify-presenter", action="store_true")
    modes.add_argument("--verify-gateway", action="store_true")
    modes.add_argument("--monitor-sample", action="store_true")
    modes.add_argument("--gate-existing-presenter", action="store_true")
    args = parser.parse_args()
    if args.apply:
        return apply_reset()
    if args.gate_presenter:
        return gate_presenter()
    if args.verify_presenter:
        return verify_presenter()
    if args.verify_gateway:
        return verify_gateway()
    if args.monitor_sample:
        return monitor_sample()
    if args.gate_existing_presenter:
        return gate_existing_presenter()
    return preview()


if __name__ == "__main__":
    raise SystemExit(main())
