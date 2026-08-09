#!/usr/bin/python3 -I
"""Fail the release monitor on poisoned delivery state or missing live proof."""

from __future__ import annotations

import json
import os
import signal
import sqlite3
import stat
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path


RELEASE_ID = "67568f32-0b94403-f50af73-r8"
RELEASE_ROOT = Path("/home/smith/.local/share/acp-runtime/releases") / RELEASE_ID
TENDWIRE_DB = Path(
    "/home/smith/.local/share/tendwire/candidates/0b94403-r8/tendwire.db"
)
INGRESS_DB = Path(
    "/home/smith/.local/share/herdres/candidates/f50af73-r8/ingress.db"
)
HERDRES_STATE = Path(
    "/home/smith/.local/share/herdres/candidates/f50af73-r8/state.json"
)
EVIDENCE = Path(
    "/home/smith/.local/state/acp-cutover/frozen-0b94403-r8/"
    "strict-live-proof.json"
)
PHASE = EVIDENCE.parent / "phase"
ROLLOUT_STATUS = EVIDENCE.parent / "rollout-status.json"
HERDR_BASELINE = EVIDENCE.parent / "herdr-baseline.json"
MONITOR_OWNER = EVIDENCE.parent / "validation-monitor-owner"
MONITOR_HEARTBEAT = EVIDENCE.parent / "validation-monitor-heartbeat"
MANIFEST = Path("/home/smith/.local/share/acp-runtime/manifests") / f"{RELEASE_ID}.json"
TOPIC_PYTHON = Path("/home/smith/.local/share/uv/tools/contexto/bin/python")
MIGRATION_ANCHOR = Path(
    "/home/smith/.local/state/acp-pane-migration/current/anchor.json"
)


def pinned_terminal_identity() -> str:
    descriptor = os.open(
        MIGRATION_ANCHOR,
        os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW,
    )
    try:
        metadata = os.fstat(descriptor)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or stat.S_IMODE(metadata.st_mode) != 0o600
            or metadata.st_uid != os.geteuid()
            or metadata.st_gid != os.getegid()
            or metadata.st_nlink != 1
            or not 1 <= metadata.st_size <= 1024
        ):
            raise RuntimeError("ACP migration anchor metadata is unsafe")
        chunks = []
        remaining = metadata.st_size
        while remaining:
            chunk = os.read(descriptor, remaining)
            if not chunk:
                raise RuntimeError("ACP migration anchor is truncated")
            chunks.append(chunk)
            remaining -= len(chunk)
        if os.read(descriptor, 1):
            raise RuntimeError("ACP migration anchor changed while reading")
        body = b"".join(chunks)
    finally:
        os.close(descriptor)
    value = json.loads(body.decode("utf-8"))
    expected = {
        "schema_version": 1,
        "agent": "codex",
        "pane_id": "w53:p8",
        "workspace_id": "w53",
        "session_id": "019f96b6-3f4e-74a0-9ad9-6fbf68203f74",
    }
    if (
        set(value) != {*expected, "terminal_id"}
        or type(value.get("schema_version")) is not int
        or any(
            value.get(key) != expected_value
            for key, expected_value in expected.items()
        )
    ):
        raise RuntimeError("ACP migration anchor identity is not exact")
    terminal = value.get("terminal_id")
    if not isinstance(terminal, str) or not terminal:
        raise RuntimeError("ACP migration terminal identity is invalid")
    return terminal


def counts() -> dict[str, int]:
    current = datetime.now(UTC).isoformat(timespec="microseconds").replace(
        "+00:00", "Z"
    )
    with sqlite3.connect(f"file:{TENDWIRE_DB}?mode=ro", uri=True) as tendwire:
        result = {
            "dead_letter": tendwire.execute(
                "SELECT COUNT(*) FROM connector_outbox WHERE status='dead_letter'"
            ).fetchone()[0],
            "retry": tendwire.execute(
                "SELECT COUNT(*) FROM connector_outbox WHERE status='retry'"
            ).fetchone()[0],
            "deferred": tendwire.execute(
                "SELECT COUNT(*) FROM connector_outbox WHERE status='deferred'"
            ).fetchone()[0],
            "delivered_final_parts": tendwire.execute(
                "SELECT COUNT(*) FROM connector_outbox "
                "WHERE kind='final_part' AND status='delivered'"
            ).fetchone()[0],
            "complete_acp_turns": tendwire.execute(
                "SELECT COUNT(*) FROM turns "
                "WHERE turn_id LIKE 'acpt\\_%' ESCAPE '\\' AND state='complete'"
            ).fetchone()[0],
            "unsettled_outbox": tendwire.execute(
                "SELECT COUNT(*) FROM connector_outbox WHERE status IN "
                "('staged','blocked','queued','leased','awaiting_ack')"
            ).fetchone()[0],
            "unsettled_commands": tendwire.execute(
                "SELECT (SELECT COUNT(*) FROM command_receipts WHERE state<>'accepted') + "
                "(SELECT COUNT(*) FROM turn_submissions WHERE state<>'linked') + "
                "(SELECT COUNT(*) FROM backend_pending) + "
                "(SELECT COUNT(*) FROM backend_pending_claims)"
            ).fetchone()[0],
            "unexpected_bindings": tendwire.execute(
                "SELECT CASE WHEN COUNT(*)=2 AND COUNT(DISTINCT worker_id)=1 "
                "AND COUNT(DISTINCT backend)=2 "
                "AND SUM(CASE WHEN backend IN ('acp','herdr') THEN 1 ELSE 0 END)=2 "
                "AND SUM(CASE WHEN expires_at IS NULL OR expires_at>? "
                "THEN 1 ELSE 0 END)=2 THEN 0 ELSE 1 END FROM worker_bindings",
                (current,),
            ).fetchone()[0],
        }
    with sqlite3.connect(f"file:{INGRESS_DB}?mode=ro", uri=True) as ingress:
        result.update(
            {
                "terminal_ingress": ingress.execute(
                    "SELECT COUNT(*) FROM requests WHERE state='terminal'"
                ).fetchone()[0],
                "retry_ingress": ingress.execute(
                    "SELECT COUNT(*) FROM requests WHERE state='retry'"
                ).fetchone()[0],
                "quarantine_ingress": ingress.execute(
                    "SELECT COUNT(*) FROM requests WHERE state='quarantine'"
                ).fetchone()[0],
                "nonterminal_ingress": ingress.execute(
                    "SELECT COUNT(*) FROM requests WHERE state IN "
                    "('pending','processing')"
                ).fetchone()[0],
            }
        )
    state = json.loads(HERDRES_STATE.read_text(encoding="utf-8"))
    collections = {
        name: state.get(name) if isinstance(state.get(name), dict) else {}
        for name in (
            "workers",
            "spaces",
            "topics",
            "topic_create_claims",
            "topic_tombstones",
            "provider_messages",
            "provider_jobs",
            "current_slots",
            "decision_controls",
        )
    }
    herdres_poison = 0
    herdres_poison += int(
        len(collections["workers"]) != 1
        or any(
            row.get("lifecycle_status") != "live"
            for row in collections["workers"].values()
        )
    )
    herdres_poison += int(
        not collections["spaces"]
        or any(
            row.get("lifecycle_status") != "live"
            for row in collections["spaces"].values()
        )
    )
    herdres_poison += int(
        len(collections["topics"]) != 1
        or any(row.get("status") != "active" for row in collections["topics"].values())
    )
    herdres_poison += int(
        len(collections["topic_create_claims"]) != 1
        or any(
            row.get("status") != "active"
            for row in collections["topic_create_claims"].values()
        )
    )
    herdres_poison += int(bool(collections["topic_tombstones"]))
    herdres_poison += sum(
        row.get("status") != "active"
        for row in collections["provider_messages"].values()
    )
    herdres_poison += sum(
        not (
            row.get("outcome") in {"sent", "edited", "not_modified"}
            or (
                row.get("kind") == "retire"
                and row.get("outcome") == "protected_reuse_noop"
            )
        )
        for row in collections["provider_jobs"].values()
    )
    herdres_poison += sum(
        row.get("status") not in {"current", "retired"}
        for row in collections["current_slots"].values()
    )
    herdres_poison += sum(
        row.get("status") not in {"active", "resolved"}
        for row in collections["decision_controls"].values()
    )
    result["herdres_poison"] = int(herdres_poison)
    return {key: int(value) for key, value in result.items()}


def poisoned(value: dict[str, int]) -> bool:
    return any(
        value[key] != 0
        for key in (
            "dead_letter",
            "retry",
            "deferred",
            "retry_ingress",
            "quarantine_ingress",
            "unexpected_bindings",
            "herdres_poison",
        )
    )


def atomic_private(path: Path, body: bytes) -> None:
    temporary = path.with_name(f".{path.name}.tmp.{os.getpid()}")
    descriptor = os.open(
        temporary,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC,
        0o600,
    )
    try:
        view = memoryview(body)
        while view:
            written = os.write(descriptor, view)
            if written < 1:
                raise OSError("private monitor write failed")
            view = view[written:]
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    os.replace(temporary, path)
    directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def heartbeat() -> None:
    atomic_private(
        MONITOR_HEARTBEAT,
        f"FROZEN_ACP_STRICT_HEARTBEAT {os.getpid()}\n".encode("ascii"),
    )


def begin_validation() -> None:
    if PHASE.read_text(encoding="ascii").strip() != "provisional":
        raise RuntimeError("transaction is not provisional")
    if MONITOR_OWNER.exists() or MONITOR_OWNER.is_symlink():
        raise RuntimeError("validation owner already exists")
    start_time = (
        Path("/proc/self/stat")
        .read_text(encoding="ascii")
        .rsplit(") ", 1)[1]
        .split()[19]
    )
    command = Path("/proc/self/cmdline").read_bytes().replace(b"\0", b" ")
    if not start_time.isdigit() or b"monitor-one-hour-strict" not in command:
        raise RuntimeError("strict monitor identity is invalid")
    atomic_private(
        MONITOR_OWNER,
        f"FROZEN_ACP_VALIDATION_MONITOR {os.getpid()} {start_time}\n".encode(
            "ascii"
        ),
    )
    heartbeat()
    atomic_private(PHASE, b"validating\n")


def verify_integrity() -> None:
    subprocess.run(
        [
            str(RELEASE_ROOT / "release-integrity"),
            "verify",
            "--manifest",
            str(MANIFEST),
        ],
        check=True,
        stdout=subprocess.DEVNULL,
        timeout=120,
    )


def verify_attestation() -> None:
    subprocess.run(
        [str(RELEASE_ROOT / "attest-r8"), "live"],
        check=True,
        stdout=subprocess.DEVNULL,
        timeout=45,
    )


def verify_live_telegram() -> dict[str, object]:
    result = subprocess.run(
        [str(TOPIC_PYTHON), "-I", str(RELEASE_ROOT / "verify-live-telegram")],
        check=True,
        capture_output=True,
        text=True,
        timeout=45,
        cwd=RELEASE_ROOT / "herdres",
    )
    value = json.loads(result.stdout)
    if (
        not isinstance(value, dict)
        or set(value)
        != {
            "schema_version",
            "verified_final_parts",
            "all_messages_exact",
            "working_card_edited",
            "text_emitted",
        }
        or value.get("schema_version") != 1
        or type(value.get("verified_final_parts")) is not int
        or value["verified_final_parts"] < 1
        or value.get("all_messages_exact") is not True
        or value.get("working_card_edited") is not True
        or value.get("text_emitted") is not False
    ):
        raise RuntimeError("live Telegram verification failed")
    return value


def herdr_unchanged() -> bool:
    expected = json.loads(HERDR_BASELINE.read_text(encoding="utf-8"))
    pid = int(
        subprocess.run(
            [
                "systemctl",
                "--user",
                "show",
                "--value",
                "-p",
                "MainPID",
                "herdr-server.service",
            ],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
    )
    actual = {
        "schema_version": 1,
        "pid": pid,
        "start_time": Path(f"/proc/{pid}/stat")
        .read_text(encoding="ascii")
        .rsplit(") ", 1)[1]
        .split()[19],
        "boot_id": Path("/proc/sys/kernel/random/boot_id")
        .read_text(encoding="ascii")
        .strip(),
    }
    return actual == expected


def pinned_acp_owner(terminal_id: str) -> bool:
    try:
        raw = subprocess.run(
            [str(RELEASE_ROOT / "herdr"), "agent", "acp-status", "tendwire-live"],
            check=True,
            capture_output=True,
            text=True,
            timeout=10,
        ).stdout
        raw_workspaces = subprocess.run(
            [str(RELEASE_ROOT / "herdr"), "workspace", "list"],
            check=True,
            capture_output=True,
            text=True,
            timeout=10,
        ).stdout
        status = json.loads(raw).get("result") or {}
        workspaces = (json.loads(raw_workspaces).get("result") or {}).get(
            "workspaces"
        ) or []
        worker = status.get("worker") or {}
        session = status.get("session") or {}
        adapter = status.get("adapter") or {}
    except (OSError, subprocess.SubprocessError, ValueError):
        return False
    return (
        status.get("lifecycle") == "acp_owned_attached"
        and status.get("console_lifecycle") == "attached"
        and status.get("cwd") == "/home/smith/tendwire"
        and adapter.get("name") == "codex-acp"
        and adapter.get("version") == "@agentclientprotocol/codex-acp 1.1.14"
        and session
        == {
            "mode": "resume",
            "id": "019f96b6-3f4e-74a0-9ad9-6fbf68203f74",
        }
        and worker.get("pane_id") == "w53:p8"
        and worker.get("workspace_id") == "w53"
        and worker.get("terminal_id") == terminal_id
        and worker.get("name") == "tendwire-live"
        and worker.get("agent") == "codex"
        and len(
            [
                item
                for item in workspaces
                if item.get("workspace_id") == "w53"
                and item.get("label") == "Tendwire"
            ]
        )
        == 1
    )


def correlated_live_proof(terminal_id: str) -> int:
    state = json.loads(HERDRES_STATE.read_text(encoding="utf-8"))
    provider_jobs = state.get("provider_jobs") or {}
    provider_messages = state.get("provider_messages") or {}
    with sqlite3.connect(f"file:{INGRESS_DB}?mode=ro", uri=True) as ingress:
        request_ids = [
            str(row[0])
            for row in ingress.execute(
                "SELECT request_id FROM requests "
                "WHERE state='terminal' AND command_json IS NOT NULL"
            )
        ]
    proved = 0
    with sqlite3.connect(f"file:{TENDWIRE_DB}?mode=ro", uri=True) as tendwire:
        tendwire.row_factory = sqlite3.Row
        for request_id in request_ids:
            chain = tendwire.execute(
                """SELECT s.turn_id,s.route_generation,t.route_generation AS turn_generation,
                t.worker_id
                FROM command_receipts c
                JOIN turn_submissions s ON s.host_id=c.host_id AND s.request_id=c.request_id
                JOIN turns t ON t.host_id=s.host_id AND t.turn_id=s.turn_id
                WHERE c.request_id=? AND c.state='accepted' AND s.state='linked'
                  AND t.state='complete' AND t.turn_id LIKE 'acpt\\_%' ESCAPE '\\'""",
                (request_id,),
            ).fetchone()
            if chain is None or chain["route_generation"] != chain["turn_generation"]:
                continue
            final_ready = tendwire.execute(
                "SELECT COUNT(*) FROM connector_outbox "
                "WHERE turn_id=? AND kind='final_ready' AND status='delivered'",
                (chain["turn_id"],),
            ).fetchone()[0]
            parts = tendwire.execute(
                "SELECT key,status,logical_ordinal,payload_json FROM connector_outbox "
                "WHERE turn_id=? AND kind='final_part'",
                (chain["turn_id"],),
            ).fetchall()
            herdr = tendwire.execute(
                "SELECT stable_key,private_binding_json FROM worker_bindings "
                "WHERE worker_id=? AND backend='herdr' AND route_generation=?",
                (chain["worker_id"], chain["turn_generation"]),
            ).fetchone()
            if not final_ready or not parts or herdr is None:
                continue
            herdr_private = json.loads(herdr["private_binding_json"])
            if (
                herdr_private.get("sendable") is not True
                or herdr_private.get("target_kind") != "terminal_id"
                or herdr_private.get("target_value") != terminal_id
            ):
                continue
            valid_parts = all(
                row["status"] == "delivered"
                and json.loads(row["payload_json"]).get("worker", {}).get(
                    "route_generation"
                )
                == chain["turn_generation"]
                for row in parts
            )
            provider_valid = all(
                isinstance(provider_jobs.get(row["key"]), dict)
                and provider_jobs[row["key"]].get("kind") == "final_part"
                and provider_jobs[row["key"]].get("captured_route_generation")
                == chain["turn_generation"]
                and provider_jobs[row["key"]].get("outcome")
                in {"sent", "edited", "not_modified"}
                for row in parts
            )
            first_parts = [row for row in parts if row["logical_ordinal"] == 0]
            replaced_working = False
            if len(first_parts) == 1:
                first_job = provider_jobs.get(first_parts[0]["key"]) or {}
                binding_id = first_job.get("provider_message_binding_id")
                message = provider_messages.get(binding_id) or {}
                replaced_working = (
                    first_job.get("outcome") in {"edited", "not_modified"}
                    and message.get("created_kind") == "working"
                    and message.get("current_owner_key") == first_parts[0]["key"]
                    and message.get("status") == "active"
                )
            acp = tendwire.execute(
                "SELECT private_binding_json FROM worker_bindings "
                "WHERE worker_id=? AND backend='acp' AND stable_key=? "
                "AND route_generation<>?",
                (chain["worker_id"], herdr["stable_key"], chain["turn_generation"]),
            ).fetchall()
            acp_matches = [
                json.loads(row[0])
                for row in acp
                if json.loads(row[0]).get("target_kind") == "terminal_id"
                and json.loads(row[0]).get("target_value") == terminal_id
            ]
            if (
                valid_parts
                and provider_valid
                and replaced_working
                and len(acp_matches) == 1
            ):
                proved += 1
    return proved


def fail_release() -> None:
    atomic_private(PHASE, b"validation_failed\n")
    # Do not synchronously stop this monitor's own required dependencies here.
    # A blocking stop can terminate us before rollback runs.  ExecStopPost owns
    # the blocking shutdown and pane restoration after this process exits.
    subprocess.run(
        [
            "systemctl",
            "--user",
            "stop",
            "--no-block",
            "herdres-gateway.service",
            "herdres.service",
            "tendwired.service",
        ],
        check=False,
    )


def atomic_evidence(value: dict[str, object]) -> None:
    EVIDENCE.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    temporary = EVIDENCE.with_name(f".{EVIDENCE.name}.tmp.{os.getpid()}")
    descriptor = os.open(
        temporary,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC,
        0o600,
    )
    try:
        body = (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()
        view = memoryview(body)
        while view:
            written = os.write(descriptor, view)
            if written < 1:
                raise OSError("strict evidence write failed")
            view = view[written:]
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    os.replace(temporary, EVIDENCE)


def main() -> int:
    if len(sys.argv) != 1:
        raise SystemExit("strict monitor takes no arguments")
    verify_integrity()
    terminal_id = pinned_terminal_identity()
    if not herdr_unchanged() or not pinned_acp_owner(terminal_id):
        raise SystemExit("Herdr identity changed before monitoring")
    verify_attestation()
    baseline = counts()
    if poisoned(baseline) or any(
        baseline[key] != 0
        for key in (
            "terminal_ingress",
            "complete_acp_turns",
            "delivered_final_parts",
            "unsettled_outbox",
            "nonterminal_ingress",
            "unsettled_commands",
        )
    ):
        raise SystemExit(
            "candidate is poisoned before monitoring: "
            + json.dumps(baseline, sort_keys=True, separators=(",", ":"))
        )
    # Publish validation ownership only after the clean, empty baseline is
    # frozen.  Rollout cannot advertise readiness or accept the proof prompt
    # before this point.
    begin_validation()
    verify_attestation()
    heartbeat()
    command = [str(RELEASE_ROOT / "monitor-one-hour-core")]
    process = subprocess.Popen(command)
    failed_reason: str | None = None
    try:
        while process.poll() is None:
            heartbeat()
            time.sleep(15)
            heartbeat()
            current = counts()
            try:
                verify_attestation()
                attestation_valid = True
            except Exception:
                attestation_valid = False
            heartbeat()
            if (
                poisoned(current)
                or not herdr_unchanged()
                or pinned_terminal_identity() != terminal_id
                or not pinned_acp_owner(terminal_id)
                or not attestation_valid
            ):
                failed_reason = "poisoned_delivery_state"
                process.send_signal(signal.SIGTERM)
                break
        return_code = process.wait(timeout=60)
    except BaseException:
        process.send_signal(signal.SIGTERM)
        try:
            process.wait(timeout=30)
        except subprocess.TimeoutExpired:
            process.kill()
        raise
    heartbeat()
    final = counts()
    proof_count = correlated_live_proof(terminal_id)
    telegram_proof: dict[str, object] | None = None
    try:
        telegram_proof = verify_live_telegram()
    except Exception:
        telegram_proof = None
    heartbeat()
    try:
        verify_integrity()
        integrity_valid = True
    except Exception:
        integrity_valid = False
    try:
        verify_attestation()
        attestation_valid = True
    except Exception:
        attestation_valid = False
    heartbeat()
    passed = (
        failed_reason is None
        and return_code == 0
        and not poisoned(final)
        and final["terminal_ingress"] > baseline["terminal_ingress"]
        and final["complete_acp_turns"] > baseline["complete_acp_turns"]
        and final["delivered_final_parts"] > baseline["delivered_final_parts"]
        and final["unsettled_outbox"] == 0
        and final["nonterminal_ingress"] == 0
        and final["unsettled_commands"] == 0
        and proof_count > 0
        and telegram_proof is not None
        and integrity_valid
        and attestation_valid
        and herdr_unchanged()
        and pinned_terminal_identity() == terminal_id
        and pinned_acp_owner(terminal_id)
    )
    evidence: dict[str, object] = {
        "schema_version": 1,
        "release_id": RELEASE_ID,
        "completed_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "status": "success" if passed else "failed",
        "historical_recovery": False,
        "baseline": baseline,
        "final": final,
        "core_return_code": return_code,
        "correlated_live_chains": proof_count,
        "telegram_render_verified": telegram_proof is not None,
        "verified_telegram_final_parts": (
            0 if telegram_proof is None else telegram_proof["verified_final_parts"]
        ),
        "release_integrity_valid": integrity_valid,
        "installed_config_attestation_valid": attestation_valid,
        "herdr_restarted": not herdr_unchanged(),
    }
    if failed_reason is not None:
        evidence["failure"] = failed_reason
    atomic_evidence(evidence)
    if not passed:
        fail_release()
        return 1
    status = {
        "schema_version": 1,
        "release_id": RELEASE_ID,
        "state": "success",
        "phase": "validation_passed",
        "updated_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "herdr_restarted": False,
        "historical_recovery": False,
    }
    atomic_private(
        ROLLOUT_STATUS,
        (
            json.dumps(status, sort_keys=True, separators=(",", ":")) + "\n"
        ).encode("utf-8"),
    )
    atomic_private(PHASE, b"validation_passed\n")
    MONITOR_OWNER.unlink(missing_ok=True)
    MONITOR_HEARTBEAT.unlink(missing_ok=True)
    print(f"STRICT_MONITOR_SUCCESS release_id={RELEASE_ID}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
