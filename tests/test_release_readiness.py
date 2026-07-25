"""Release-readiness guards for the public Tendwire contract.

Consolidates the RC invariants: public snapshot/turns/pending JSON carry no
forbidden connector/terminal keys and no pseudo pane-id string values, even
when the raw backend observation embeds pane ids, terminal ids, and send-key
payloads.
"""

from __future__ import annotations

import ast
import json
import re
import sqlite3
import subprocess
import tomllib
from pathlib import Path
from typing import Any

from tendwire.backends import herdr_cli
from tendwire.backends.herdr_cli import diagnose_herdr
from tendwire.daemon import TendwireDaemon

from tendwire.config import Config
from tendwire.core.projector import project_from_raw
from tendwire.store.sqlite import (
    CompactionOptions,
    SnapshotRetentionPolicy,
    compact_store,
    init_store,
    maybe_run_automatic_store_maintenance,
    run_store_maintenance,
    save_snapshot,
    store_status,
)
from tendwire.core.turns import (
    pending_payload_from_snapshot,
    turns_payload_from_snapshot,
)


_FORBIDDEN_KEYS = {
    "pane_id",
    "pane_ids",
    "terminal_id",
    "terminal_ids",
    "backend_target",
    "backend_targets",
    "send_keys",
    "session_id",
    "private_fingerprint",
    "chat_id",
    "topic_id",
    "message_id",
    "token",
    "secret",
    "last_examined_id",
}
_FORBIDDEN_COMPACT = {key.replace("_", "") for key in _FORBIDDEN_KEYS}

# Herdr pane/terminal identifiers look like ``w<hex>:p<n>`` / ``w<hex>:t<n>``.
_PSEUDO_PANE_ID_RE = re.compile(r"\bw[0-9a-f]+:(?:p|t)[0-9a-f]+\b", re.IGNORECASE)

# Private values seeded into the raw observation below; none may surface publicly.
_PRIVATE_VALUES = (
    "wX8:p1",
    "term_655private",
    "send-secret",
    "019f-private-session",
    "release-private-payload",
    "release-private-state",
)


def _assert_public_clean(value: Any, path: str = "$") -> None:
    if isinstance(value, dict):
        for key, item in value.items():
            normalized = str(key).lower().replace("-", "_")
            assert normalized not in _FORBIDDEN_KEYS, f"forbidden key {path}.{key}"
            assert normalized.replace("_", "") not in _FORBIDDEN_COMPACT, f"forbidden key {path}.{key}"
            _assert_public_clean(item, f"{path}.{key}")
    elif isinstance(value, list):
        for index, item in enumerate(value):
            _assert_public_clean(item, f"{path}[{index}]")
    elif isinstance(value, str):
        assert not _PSEUDO_PANE_ID_RE.search(value), f"pseudo pane id in {path}: {value!r}"
        for private in _PRIVATE_VALUES:
            assert private not in value, f"private value in {path}: {private!r}"


def _public_payloads() -> list[dict[str, Any]]:
    config = Config(host_id="rc-host")
    snapshot = project_from_raw(
        config,
        spaces=[{"id": "wX8", "name": "projectx", "status": "active"}],
        workers=[
            {
                "id": "codex",
                "name": "codex",
                "status": "working",
                "space_id": "wX8",
                "pane_id": "wX8:p1",
                "terminal_id": "term_655private",
                "agent_session": {"agent": "codex", "kind": "id", "value": "019f-private-session"},
                "meta": {"send_keys": "send-secret", "backend_target": {"kind": "pane_id", "value": "wX8:p1"}},
            }
        ],
    )
    return [
        json.loads(snapshot.to_json()),
        turns_payload_from_snapshot(snapshot),
        pending_payload_from_snapshot(snapshot),
    ]


def test_public_payloads_have_zero_forbidden_keys_and_no_pseudo_pane_ids():
    for payload in _public_payloads():
        _assert_public_clean(payload)


def test_private_values_never_appear_in_public_payloads():
    blob = json.dumps(_public_payloads())
    for private in _PRIVATE_VALUES:
        assert private not in blob, f"private value leaked into public JSON: {private!r}"


def test_public_worker_identity_is_neutral():
    snapshot_payload, _turns, _pending = _public_payloads()
    workers = snapshot_payload["workers"]
    assert workers, "expected a projected worker"
    # Public id is the neutral agent id, not a pane/terminal identifier.
    assert workers[0]["id"] == "codex"
    assert "pane_id" not in workers[0]
    assert "backend_target" not in workers[0]


def test_maintenance_release_surfaces_are_fixed_aggregate_and_private_clean(
    monkeypatch,
    tmp_path: Path,
) -> None:
    state_dir = tmp_path / "release-private-state-directory"
    state_dir.mkdir(mode=0o700)
    db_path = state_dir / "release-private-store.db"
    config = Config(
        host_id="release-host",
        herdr_bin="herdr",
        data_dir=state_dir,
        db_path=db_path,
        snapshot_retention_days=36500,
        snapshot_retention_count=100,
        snapshot_maintenance_batch_size=5,
        store_maintenance_cadence_seconds=3600,
        acknowledged_final_retention_days=36500,
        acknowledged_final_retention_count=100,
        command_retry_horizon_seconds=120,
        command_receipt_retention_seconds=691_200,
        command_receipt_retention_count=11,
    )
    init_store(db_path)
    snapshot = project_from_raw(
        config,
        workers=[
            {
                "id": "release-worker",
                "name": "Release Worker",
                "status": "working",
                "pane_id": "wX8:p1",
                "terminal_id": "term_655private",
                "meta": {
                    "send_keys": "send-secret",
                    "private_payload": "release-private-payload",
                },
            }
        ],
    )
    save_snapshot(db_path, snapshot)
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            """
            INSERT INTO connector_outbox (
                host_id, connector, delivery_key, status, payload_json,
                private_state_json, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                config.host_id,
                "attention",
                "release-private-delivery-key",
                "queued",
                '{"content":"release-private-payload"}',
                '{"state":"release-private-state"}',
                "2026-01-10T00:00:00+00:00",
                "2026-01-10T00:00:00+00:00",
            ),
        )

    automatic = maybe_run_automatic_store_maintenance(
        db_path,
        policy=SnapshotRetentionPolicy(
            retention_days=config.snapshot_retention_days,
            retention_count=config.snapshot_retention_count,
            batch_size=config.snapshot_maintenance_batch_size,
        ),
        acknowledged_final_retention_days=(
            config.acknowledged_final_retention_days
        ),
        acknowledged_final_retention_count=(
            config.acknowledged_final_retention_count
        ),
        command_retry_horizon_seconds=config.command_retry_horizon_seconds,
        command_receipt_retention_seconds=config.command_receipt_retention_seconds,
        command_receipt_retention_count=config.command_receipt_retention_count,
        cadence_seconds=config.store_maintenance_cadence_seconds,
        now="2026-01-10T00:00:00+00:00",
    )
    status = store_status(
        db_path,
        config.host_id,
        acknowledged_final_retention_days=config.acknowledged_final_retention_days,
        acknowledged_final_retention_count=config.acknowledged_final_retention_count,
        snapshot_retention_days=config.snapshot_retention_days,
        snapshot_retention_count=config.snapshot_retention_count,
        maintenance_batch_size=config.snapshot_maintenance_batch_size,
        maintenance_cadence_seconds=config.store_maintenance_cadence_seconds,
        command_retry_horizon_seconds=config.command_retry_horizon_seconds,
        command_receipt_retention_seconds=config.command_receipt_retention_seconds,
        command_receipt_retention_count=config.command_receipt_retention_count,
    )
    cleanup = run_store_maintenance(
        db_path,
        config.host_id,
        retention_days=36500,
        max_outbox_attempts=10,
        acknowledged_final_retention_days=config.acknowledged_final_retention_days,
        acknowledged_final_retention_count=config.acknowledged_final_retention_count,
        command_retry_horizon_seconds=config.command_retry_horizon_seconds,
        command_receipt_retention_seconds=config.command_receipt_retention_seconds,
        command_receipt_retention_count=config.command_receipt_retention_count,
        now="2026-01-10T00:00:00+00:00",
        dry_run=True,
        snapshot_retention_days=config.snapshot_retention_days,
        snapshot_retention_count=config.snapshot_retention_count,
        snapshot_batch_size=config.snapshot_maintenance_batch_size,
    )
    health = TendwireDaemon(config).get_health()

    monkeypatch.setattr(herdr_cli.shutil, "which", lambda _value: "/usr/bin/herdr")
    monkeypatch.setattr(
        herdr_cli.subprocess,
        "run",
        lambda args, **_kwargs: subprocess.CompletedProcess(
            args=args,
            returncode=0,
            stdout='{"items":[]}',
            stderr="",
        ),
    )
    monkeypatch.setattr(
        herdr_cli,
        "utc_timestamp",
        lambda *_args, **_kwargs: "2026-01-10T00:30:00+00:00",
    )
    doctor = diagnose_herdr(config)

    before_compaction = db_path.read_bytes()
    compaction = compact_store(
        db_path,
        options=CompactionOptions(
            dry_run=True,
            snapshot_retention_days=config.snapshot_retention_days,
            snapshot_retention_count=config.snapshot_retention_count,
            batch_size=config.snapshot_maintenance_batch_size,
        ),
        now="2026-01-10T00:30:00+00:00",
    )

    assert automatic == {
        "schema_version": 1,
        "ok": True,
        "status": "ok",
        "due": True,
        "last_completed_at": "2026-01-10T00:00:00+00:00",
        "next_due_at": "2026-01-10T01:00:00+00:00",
        "snapshot": {
            "examined": 0,
            "deleted": 0,
            "remaining_candidates": False,
        },
        "final_retention": {
            "examined": 0,
            "deleted": 0,
            "remaining_candidates": False,
            "acknowledged_final_retention_days": 36500,
            "acknowledged_final_retention_count": 100,
        },
        "command_requests": {
            "ok": True,
            "status": "ok",
            "retry_horizon_seconds": 120,
            "retention_seconds": 691_200,
            "retention_count": 11,
            "batch_size": 5,
            "retry_cutoff_at": "2026-01-09T23:58:00+00:00",
            "cutoff_at": "2026-01-02T00:00:00+00:00",
            "examined": 0,
            "stale_active": 0,
            "deleted": 0,
            "remaining_candidates": False,
        },
        "herdr_turns": {
            "schema_version": 1,
            "ok": True,
            "status": "ok",
            "scope": "database",
            "host_id": None,
            "dry_run": False,
            "retention_days": 36500,
            "retention_count": 100,
            "cutoff_at": "1926-02-04T00:00:00+00:00",
            "batch_size": 5,
            "examined": 0,
            "deleted": 0,
            "deleted_completions": 0,
            "deleted_watermarks": 0,
            "remaining_candidates": False,
        },
        "batch_size": 5,
    }
    assert status["counts"]["snapshots"] == 1
    assert status["outbox"] == {
        "pending": 1,
        "leased": 0,
        "completed": 0,
        "by_status": {"queued": 1},
        "due": 1,
        "oldest_due_at": "2026-01-10T00:00:00+00:00",
        "overdue_awaiting_ack": 0,
        "drain_target_seconds": 30,
        "starved": True,
    }
    assert status["final_retention"] == {
        "acknowledged": 0,
        "unresolved": 0,
        "queued": 0,
        "leased": 0,
        "deferred": 0,
        "retry": 0,
        "dead_letter": 0,
        "awaiting_ack": 0,
        "eligible": 0,
        "acknowledged_final_retention_days": 36500,
        "acknowledged_final_retention_count": 100,
        "storage_pressure": False,
    }
    assert status["command_requests"] == {
        "total": 0,
        "states": {
            "reserved": 0,
            "send_started": 0,
            "accepted": 0,
            "rejected": 0,
            "uncertain": 0,
        },
        "stale_active": 0,
        "eligible": 0,
        "retry_horizon_seconds": 120,
        "retention_seconds": 691_200,
        "retention_count": 11,
        "storage_pressure": False,
    }
    assert (
        status["command_requests"]["retention_seconds"]
        > status["command_requests"]["retry_horizon_seconds"]
    )
    assert status["maintenance"] == {
        "last_completed_at": "2026-01-10T00:00:00+00:00",
        "status": "ok",
        "snapshot_count": 1,
        "snapshot_retention_days": 36500,
        "snapshot_retention_count": 100,
        "maintenance_batch_size": 5,
        "maintenance_cadence_seconds": 3600,
        "backlog": False,
    }
    assert cleanup["dry_run"] is True
    assert cleanup["retention"]["examined"] == 0
    assert cleanup["snapshots"]["examined"] == 0
    assert cleanup["outbox"]["updated"] == 0
    assert cleanup["turn_content"]["examined"] == 0
    assert cleanup["herdr_turns"] == {
        "dry_run": True,
        "retention_days": 30,
        "retention_count": 4096,
        "cutoff_at": "2025-12-11T00:00:00+00:00",
        "batch_size": 100,
        "examined": 0,
        "deleted": 0,
        "deleted_completions": 0,
        "deleted_watermarks": 0,
        "remaining_candidates": False,
    }
    assert cleanup["command_requests"] == {
        "ok": True,
        "status": "ok",
        "retry_horizon_seconds": 120,
        "retention_seconds": 691_200,
        "retention_count": 11,
        "batch_size": 100,
        "retry_cutoff_at": "2026-01-09T23:58:00+00:00",
        "cutoff_at": "2026-01-02T00:00:00+00:00",
        "examined": 0,
        "stale_active": 0,
        "deleted": 0,
        "remaining_candidates": False,
    }
    assert cleanup["final_retention"] == {
        "dry_run": True,
        "acknowledged_final_retention_days": 36500,
        "acknowledged_final_retention_count": 100,
        "cutoff_at": "1926-02-04T00:00:00+00:00",
        "batch_size": 100,
        "examined": 0,
        "deleted": 0,
        "remaining_candidates": False,
        "deleted_rows": {
            "recoveries": 0,
            "attempts": 0,
            "jobs": 0,
            "plans": 0,
            "anchors": 0,
            "revisions": 0,
            "turns": 0,
        },
    }
    assert health["status"] == "degraded"
    assert health["store"]["outbox"]["starved"] is True
    assert health["store"]["counts"]["snapshots"] == 1
    assert health["limits"]["acknowledged_final_retention_days"] == 36500
    assert health["limits"]["acknowledged_final_retention_count"] == 100
    assert health["store"]["final_retention"] == status["final_retention"]
    maintenance_checks = [
        check for check in doctor["checks"] if check["name"] == "store_maintenance"
    ]
    assert maintenance_checks == [
        {
            "name": "store_maintenance",
            "ok": True,
            "outcome": "ok",
            "remediation": "No action required.",
            "snapshot_retention_days": 36500,
            "snapshot_retention_count": 100,
            "maintenance_batch_size": 5,
            "maintenance_cadence_seconds": 3600,
            "snapshot_count": 1,
            "last_completed_at": "2026-01-10T00:00:00+00:00",
        }
    ]
    assert compaction["command"] == "store.compact"
    assert compaction["scope"] == "database"
    assert compaction["dry_run"] is True
    assert compaction["status"] == "dry_run"
    assert compaction["snapshots"]["before"] == 1
    assert compaction["snapshots"]["deleted"] == 0
    assert db_path.read_bytes() == before_compaction

    public_surfaces = [status, automatic, cleanup, health, doctor, compaction]
    for surface in public_surfaces:
        _assert_public_clean(surface)
    serialized = json.dumps(public_surfaces, sort_keys=True)
    for private in (*_PRIVATE_VALUES, str(state_dir), str(db_path)):
        assert private not in serialized


_PROJECT_ROOT = Path(__file__).resolve().parents[1]
_GOAL08B_ARTIFACTS = (
    "scripts/sqlite_sidecar_race_benchmark.py",
    "tests/test_sqlite_sidecar_race_benchmark.py",
    "docs/evidence/goal08b-sqlite-sidecar-race-recovery.md",
)

_GOAL08B_SOURCE_TREE_SHA256 = "15b1ca262f6051b191d1587d353c465cc74fd6c6a9d0676eb9348eafef35ff87"
_GOAL08B_WHEEL_SHA256 = "7be0f975b0241aaf092a9bba38ace2e3e2efd2f91996f02b2cbcb24b93fac02d"


def _release_goal08b_section() -> str:
    release = (_PROJECT_ROOT / "RELEASE.md").read_text(encoding="utf-8")
    match = re.search(
        r"(?ms)^## \d+\. Goal 08B SQLite sidecar race/recovery verification$"
        r".*?(?=^## |\Z)",
        release,
    )
    assert match is not None, "RELEASE.md must carry a discrete Goal 08B gate"
    return match.group(0)


def _logical_shell_text(value: str) -> str:
    return re.sub(r"[ \t]*\\\n[ \t]*", " ", value)


def test_goal08b_release_contract_and_artifact_references_are_audited() -> None:
    section = _release_goal08b_section()
    lowered = section.lower()
    normalized = re.sub(r"\s+", " ", lowered)
    requirements = {
        "optional absence": r"absent optional sidecars.*transiently disappear",
        "selected main": r"once selected, the main\s+database is mandatory",
        "hostile family": r"wrong type.*wrong owner.*identity replacement.*fail closed",
        "narrow prepare": r"prepare may\s+create only a missing main database.*intersect modes",
        "narrow repair": r"repair only intersects modes of validated\s+existing members",
        "transaction preservation": r"cannot disturb active tendwire sqlite\s+transactions",
        "exclusive parent authority": r"(?:main creation|permission narrowing).*"
        r"bounded,\s+nonblocking exclusive authority.*store parent directory",
        "shared authority rejection": r"live tendwire\s+connection.*"
        r"shared parent-directory authority.*repair\s+fails.*"
        r"typed,\s+path-free error before mutation",
        "connection authority restoration": r"shared\s+authority before preparation.*"
        r"promotes the same authority.*restores shared authority.*"
        r"remainder of its lifetime",
        "validation only": r"ordinary reads, diagnostics, health,\s+and `store status` "
        r"are validation-only and non-creating",
        "bounded churn": r"bounded and does not recursively retry churn",
        "compaction authority": r"execute compaction alone\s+has explicit offline "
        r"replacement authority.*--acknowledge-offline",
        "typed private errors": r"typed, path-free `localstateerror`",
        "fixed public records": r"fixed aggregate records.*database_permissions: unsafe"
        r".*store_unavailable",
        "current-schema nonmutation": r"current-schema filesystem reads stay cheap and "
        r"nonmutating after their schema-version read.*no exclusive parent authority.*"
        r"no persistent wal negotiation or schema ddl",
        "schema transition authority": r"uninitialized or migrating filesystem store.*"
        r"exclusive authority before persistent wal negotiation or schema ddl.*"
        r"private creation mode.*revalidates and narrows the resulting main database, "
        r"`-wal`, and `-shm` members before restoring retained shared authority",
        "shared schema rejection": r"live tendwire connection.*"
        r"shared parent-directory authority.*rejects the schema branch before wal, "
        r"ddl, or sidecar mutation",
        "no-op prepare preservation": r"no-op private prepare preserves",
    }
    for name, pattern in requirements.items():
        assert re.search(pattern, normalized, re.DOTALL), name

    for artifact in _GOAL08B_ARTIFACTS:
        assert artifact in section
    assert str(Path.home() / "tendwire") not in section
    assert "not a mutable source checkout" in section
    assert "exactly two no-op syncs" in section
    for accounting in (
        "file-descriptor",
        "thread",
        "direct-child",
        "socket",
    ):
        assert accounting in lowered


def test_goal08b_release_commands_and_sdist_driver_are_exact() -> None:
    raw_section = _release_goal08b_section()
    commands = {
        _logical_shell_text(block).strip()
        for block in re.findall(r"(?ms)^```sh\n(.*?)^```$", raw_section)
    }
    focused = next(
        (
            command
            for command in commands
            if command.startswith("PYTHONPATH=src python3 -m pytest -q ")
            and "tests/test_sqlite_sidecar_race_benchmark.py" in command
        ),
        None,
    )
    assert focused is not None
    for test_path in (
        "tests/test_local_state_permissions.py",
        "tests/test_store.py",
        "tests/test_diagnostics.py",
        "tests/test_cli.py",
        "tests/test_daemon.py",
        "tests/test_release_readiness.py",
        "tests/test_sqlite_sidecar_race_benchmark.py",
    ):
        assert test_path in focused
    for command in (
        "PYTHONPATH=src python3 -m pytest -q",
        "PYTHONPATH=src python3 -m py_compile $(git ls-files '*.py')",
        "git diff --check",
        (
            "python3 scripts/sqlite_sidecar_race_benchmark.py "
            "--iterations 128 --daemon-wal-cycles 64 "
            "--requests-per-method 64 --herdres-sync-passes 3 "
            "--phase-timeout-seconds 120 --json"
        ),
    ):
        assert command in commands

    pyproject = (_PROJECT_ROOT / "pyproject.toml").read_text(encoding="utf-8")
    sdist = re.search(
        r"(?ms)^\[tool\.hatch\.build\.targets\.sdist\]\s*"
        r"^include\s*=\s*\[(.*?)^\]",
        pyproject,
    )
    assert sdist is not None
    includes = ast.literal_eval(f"[{sdist.group(1)}]")
    assert "/scripts" in includes


def test_goal08b_frozen_evidence_records_installed_candidate_and_cleanup() -> None:
    evidence = (
        _PROJECT_ROOT / "docs/evidence/goal08b-sqlite-sidecar-race-recovery.md"
    ).read_text(encoding="utf-8")
    logical = _logical_shell_text(evidence)
    assert (
        "python3 scripts/sqlite_sidecar_race_benchmark.py "
        "--iterations 128 --daemon-wal-cycles 64 --requests-per-method 64 "
        "--herdres-sync-passes 3 --phase-timeout-seconds 120 --json"
    ) in logical
    aggregate_match = re.search(r"(?ms)^```json\n(\{.*\})\n```$", evidence)
    assert aggregate_match is not None
    aggregate = json.loads(aggregate_match.group(1))
    assert aggregate["ok"] is True
    assert aggregate["status"] == "completed"
    candidate = aggregate["candidate"]
    assert candidate["installation"] == "private_versioned_wheel"
    assert candidate["origin_verified"] is True
    assert candidate["source_revision_binding"] == (
        "base_revision_plus_source_tree_sha256"
    )
    assert candidate["source_tree_sha256"] == _GOAL08B_SOURCE_TREE_SHA256
    assert candidate["wheel_sha256"] == _GOAL08B_WHEEL_SHA256
    release_section = _release_goal08b_section()
    assert _GOAL08B_SOURCE_TREE_SHA256 in release_section
    assert _GOAL08B_WHEEL_SHA256 in release_section
    assert aggregate["herdres"]["sync_passes"] == 3
    assert aggregate["herdres"]["noop_passes"] == 2
    assert aggregate["herdres"]["production_client_subprocesses"] == 9
    assert aggregate["herdres"]["direct_herdr_calls"] == 0
    assert aggregate["herdres"]["external_network_attempts"] == 0
    accounting = aggregate["accounting"]
    for resource in ("fd_count", "thread_count", "direct_children"):
        assert accounting[f"{resource}_after"] == accounting[f"{resource}_before"]
    assert accounting["socket_present_after"] is False


def test_goal08b_operator_docs_keep_family_authority_explicit() -> None:
    readme = (_PROJECT_ROOT / "README.md").read_text(encoding="utf-8")
    install = (_PROJECT_ROOT / "INSTALL.md").read_text(encoding="utf-8")
    env_example = (_PROJECT_ROOT / ".env.example").read_text(encoding="utf-8")

    for value in (readme, install):
        normalized = re.sub(r"\s+", " ", value)
        assert all(suffix in value for suffix in ("`-wal`", "`-shm`", "`-journal`"))
        assert "validation-only" in value
        assert "LocalStateError" in value
        assert "store_unavailable" in value
        assert all(
            invariant in normalized
            for invariant in (
                "cannot disturb active Tendwire SQLite transactions",
                "bounded, nonblocking exclusive authority over the store parent directory",
                "shared parent-directory authority",
                "typed, path-free error before mutation",
                "restores shared authority for the remainder of its lifetime",
            )
        )
        schema_authority_requirements = (
            r"current-schema filesystem reads stay cheap and nonmutating after their "
            r"schema-version read:.*no exclusive parent authority.*no persistent wal "
            r"negotiation or schema ddl",
            r"uninitialized or migrating filesystem store takes that exclusive authority "
            r"before persistent wal negotiation or schema ddl.*private creation mode.*"
            r"revalidates and narrows the resulting main database, `-wal`, and `-shm` "
            r"members before restoring retained shared authority",
            r"live tendwire connection retains shared parent-directory authority.*"
            r"rejects the schema branch before wal, ddl, or sidecar mutation",
            r"no-op private prepare preserves",
        )
        assert all(
            re.search(pattern, normalized.lower(), re.DOTALL)
            for pattern in schema_authority_requirements
        )
    for artifact in _GOAL08B_ARTIFACTS:
        assert artifact in readme or artifact in install
    assert "diagnostics/status do not create or repair family members" in env_example


def test_herdr_plugin_manifest_exposes_only_read_only_actions() -> None:
    manifest_path = _PROJECT_ROOT / "herdr-plugin.toml"
    manifest = tomllib.loads(manifest_path.read_text(encoding="utf-8"))

    assert manifest["id"] == "plotarmordev.tendwire"
    assert manifest["version"] == "0.1.0rc5"
    assert manifest["min_herdr_version"] == "0.7.0"
    assert manifest["platforms"] == ["linux"]
    actions = {item["id"]: item for item in manifest["actions"]}
    assert set(actions) == {"doctor", "snapshot", "turns", "pending"}
    assert all(
        item["command"][:2] == ["python3", "scripts/herdr_plugin.py"]
        for item in actions.values()
    )
    serialized = json.dumps(manifest, sort_keys=True)
    assert "/home/" not in serialized
    assert "telegram" not in serialized.lower()
