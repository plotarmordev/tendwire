from __future__ import annotations

import json
import importlib.util
import os
import re
import shutil
import stat
import sys
from pathlib import Path

import pytest


DEPLOY = Path(__file__).resolve().parents[1]


def source(name: str) -> str:
    return (DEPLOY / name).read_text(encoding="utf-8")


def load_integrity():
    path = DEPLOY / "release-integrity-r8.py"
    spec = importlib.util.spec_from_file_location("release_integrity_r8_test", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def ordered(text: str, *needles: str) -> None:
    cursor = -1
    for needle in needles:
        position = text.find(needle, cursor + 1)
        assert position >= 0, needle
        assert position > cursor, needle
        cursor = position


def test_candidate_herdr_failure_restores_control_plane_before_cli_use() -> None:
    rollback = source("rollback-r8.sh")
    unit = source("acp-r8-rollback.service")
    assert "Requires=herdr-server.service" not in unit
    ordered(
        rollback,
        "restore_entries 0 4",
        'restore_entries "${CONFIG_LAST_INDEX}" "${CONFIG_LAST_INDEX}"',
        "systemctl --user daemon-reload",
        "if ! verify_herdr_process; then",
        '"${RESTORE}"',
        'capture_snapshot "${TRANSACTION}/rollback-herdr-topology.json"',
    )
    capture = re.search(r"capture_snapshot\(\) \{(?P<body>.*?)\n\}", rollback, re.S)
    assert capture is not None
    assert '"${ACTIVE}/herdr" api snapshot' in capture.group("body")


def test_backup_completion_is_durable_before_any_live_dropin() -> None:
    prepare = source("prepare-acp-release-r8.sh")
    snapshot = re.search(r"snapshot_prestate\(\) \{(?P<body>.*?)\n\}", prepare, re.S)
    assert snapshot is not None
    ordered(
        snapshot.group("body"),
        "sync_backup_tree",
        'atomic_text "${TRANSACTION}/backup-complete" BACKUP_COMPLETE',
    )
    ordered(
        prepare,
        'install -m 644 "${RELEASE}/systemd/acp-r8-rollback.service"',
        "systemctl --user enable acp-r8-release-guard.service",
        'sync "${INSTALLED_FILES[5]}"',
        'install -m 644 "${RELEASE}/systemd/herdr-99-acp-adapter.conf"',
        'install -m 644 "${RELEASE}/systemd/tendwired-99.conf"',
    )
    sync_body = re.search(r"sync_backup_tree\(\) \{(?P<body>.*?)\n\}", prepare, re.S)
    assert sync_body is not None
    assert "os.fsync(descriptor)" in sync_body.group("body")
    assert "reversed(directories)" in sync_body.group("body")
    ordered(
        prepare,
        '"${RELEASE}/release-integrity" verify',
        "fsync_published_release",
        'python3 -I "${RELEASE}/recover-stale-r8-artifacts" prepare',
        'atomic_text "${TRANSACTION}/live-mutation-started"',
        'install -d -m 700 "${TENDWIRE_CANDIDATE}"',
    )


def test_exact_entry_copy_model_preserves_symlink_and_metadata(tmp_path: Path) -> None:
    original = tmp_path / "original"
    backup = tmp_path / "backup"
    restored = tmp_path / "restored"
    target = tmp_path / "target"
    target.write_text("bound\n", encoding="utf-8")
    original.symlink_to(target.name)
    os.utime(original, ns=(1_700_000_000_000_000_000,) * 2, follow_symlinks=False)

    shutil.copy2(original, backup, follow_symlinks=False)
    shutil.copy2(backup, restored, follow_symlinks=False)

    before = original.lstat()
    after = restored.lstat()
    assert stat.S_ISLNK(after.st_mode)
    assert os.readlink(restored) == os.readlink(original)
    assert (stat.S_IMODE(after.st_mode), after.st_uid, after.st_gid, after.st_mtime_ns) == (
        stat.S_IMODE(before.st_mode),
        before.st_uid,
        before.st_gid,
        before.st_mtime_ns,
    )


def test_rollback_restores_all_mutated_state_and_exact_topology_fields() -> None:
    prepare = source("prepare-acp-release-r8.sh")
    rollback = source("rollback-r8.sh")
    for token in (
        '"${GUARD_ENABLE_LINK}"',
        '"${ACTIVE}"',
        '"${MIGRATION_STATE}"',
        '"${MIGRATION_LOCK}"',
        '"${TENDWIRE_CANDIDATE}"',
        '"${HERDRES_CANDIDATE}"',
        '"${TENDWIRE_SOCKET}"',
    ):
        assert token in prepare
        assert token in rollback
    assert 'restore_entries "${STATE_FIRST_INDEX}"' in rollback
    assert "entry_fingerprint" in rollback
    assert '"${RELEASE}/topology-normalizer"' in rollback
    topology = source("topology-normalizer-r8.py")
    for field in (
        '"focused_workspace_id"', '"focused_tab_id"', '"focused_pane_id"',
        '"active_tab_id"', '"terminal_id"', '"agent_session"', '"worktree"',
        '"foreground_cwd"', '"terminal_title"', '"scroll"', '"area"', '"rect"',
    ):
        assert field in topology
    ordered(
        rollback,
        'restore_entries "${STATE_FIRST_INDEX}"',
        "sync",
        "restore_entries 8 8",
        "restore_entries 5 7",
        "systemctl --user daemon-reload",
        "systemctl --user reset-failed acp-r8-rollout.service",
        "verify_restored_prestate",
    )
    assert "reset-failed acp-r8-rollback.service" not in rollback


def test_terminal_generation_is_dynamic_and_monitoring_uses_the_migration_anchor() -> None:
    guarded = source("guarded-herdr-restart-r8.sh")
    rollback = source("rollback-r8.sh")
    rollout = source("rollout-acp-release-r8.sh")
    monitor = source("monitor-one-hour-strict-r8.py")
    restore = source("restore-current-pane-r8.sh")

    for script in (guarded, rollback):
        assert "--terminal" not in script
        assert "term_6589e193be915a" not in script
    assert "term_6589e193be915a" not in rollout
    assert "term_6589e193be915a" not in monitor
    assert "or not isinstance(terminal, str)" in rollout
    assert "or not terminal" in rollout
    target_idle = rollout.split("target_idle() {", 1)[1].split("\nwrite_phase() {", 1)[0]
    for token in (
        "herdr agent list",
        "herdr pane get w53:p8",
        'row.get("cwd") == "/home/smith/tendwire"',
        'row.get("agent_session") == expected_session',
        'pane.get("pane_id") == "w53:p8"',
        'pane.get("workspace_id") == "w53"',
        'pane.get("cwd") == "/home/smith/tendwire"',
        'pane.get("agent_session") == expected_session',
        'pane.get("terminal_id") == terminal',
    ):
        assert token in target_idle
    assert "foreground_cwd" not in target_idle
    ordered(
        rollout,
        'first_terminal="$(target_idle || true)"',
        "sleep 1",
        'second_terminal="$(target_idle || true)"',
        '[[ "${first_terminal}" = "${second_terminal}" ]]',
        '"${RELEASE}/migrate-current-pane"',
    )
    for token in (
        "MIGRATION_ANCHOR",
        "def pinned_terminal_identity()",
        "os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW",
        'stat.S_IMODE(metadata.st_mode) != 0o600',
        'metadata.st_uid != os.geteuid()',
        'metadata.st_gid != os.getegid()',
        'metadata.st_nlink != 1',
        'set(value) != {*expected, "terminal_id"}',
        'type(value.get("schema_version")) is not int',
        "pinned_acp_owner(terminal_id)",
        "correlated_live_proof(terminal_id)",
        'herdr_private.get("target_value") != terminal_id',
    ):
        assert token in monitor
    assert "or pinned_terminal_identity() != terminal_id" in monitor
    assert "and pinned_terminal_identity() == terminal_id" in monitor
    for token in (
        '"schema_version", "agent", "pane_id", "workspace_id", "terminal_id",',
        'type(value.get("schema_version")) is not int',
        'value.get("schema_version") != 1',
        'value.get("agent") != "codex"',
        'and token.startswith("term_")',
        'all(character in "0123456789abcdef" for character in token[5:])',
        'expected_terminals = {anchor_terminal, current_terminal}',
        'worker.get("terminal_id") in expected_terminals',
        "resolve_shell_terminal()",
        'pane.get("agent") in {None, "unknown"}',
        'not (pane.get("agent_session") or {})',
        'info.get("foreground_process_group_id") == shell_pid',
    ):
        assert token in restore
    assert "term_6589e193be915a" not in restore
    assert restore.count("expected_terminals =") == 1
    ordered(
        restore,
        'if status="$(${HERDR} agent acp-status tendwire-live',
        'anchor_terminal="$(python3 - "${ANCHOR}"',
        'current_terminal="$(resolve_active_terminal)"',
        'current_terminal="$(resolve_shell_terminal)"',
        'expected_terminals = {anchor_terminal, current_terminal}',
        'worker.get("terminal_id") in expected_terminals',
        'agent acp-unregister tendwire-live',
    )
    assert 'current_terminal="$(resolve_target_terminal)"' not in restore
    ordered(
        restore,
        'if shell_terminal="$(resolve_shell_terminal)"; then',
        '"${HERDR}" pane run "${PANE}" "${command}"',
        "if conventional_ready || session_ready_on_target; then",
    )


def test_preflight_does_not_mutate_shared_migration_state() -> None:
    migration = source("acp-migrate-current-pane-r8.sh")
    guarded_init = re.search(
        r'if \[\[ "\$\{PREFLIGHT_ONLY\}" != "--preflight-only" \]\]; then'
        r'(?P<body>.*?)\nfi',
        migration,
        re.S,
    )
    assert guarded_init is not None
    assert ': >"${STATE_DIR}/history.log"' in guarded_init.group("body")
    assert "write_status validating" in guarded_init.group("body")
    preflight_exit = re.search(
        r'if \[\[ "\$\{PREFLIGHT_ONLY\}" = "--preflight-only" \]\]; then'
        r'(?P<body>.*?)\nfi',
        migration,
        re.S,
    )
    assert preflight_exit is not None
    assert "write_status" not in preflight_exit.group("body")
    assert "write_anchor" not in preflight_exit.group("body")
    assert 'exec 9<>"${LOCK}"' in migration
    assert 'exec 9>"${LOCK}"' not in migration


def test_stale_release_state_is_preconditioned_not_mutated() -> None:
    prepare = source("prepare-acp-release-r8.sh")
    assert "systemctl --user reset-failed" not in prepare
    assert "systemctl --user disable" not in prepare
    assert 'is-enabled "${unit}"' in prepare
    assert "restore_enabled_states" not in source("rollback-r8.sh")


def test_guard_readiness_and_accepted_identity_are_continuous() -> None:
    unit = source("acp-r8-release-guard.service")
    guard = source("release-guard-r8.py")
    attest = source("attest-r8.py")
    assert "Type=notify" in unit
    assert "NotifyAccess=main" in unit
    ordered(guard, "evaluate_once()", "notify_ready()", "while True:", "evaluate_once()")
    accepted = re.search(
        r"def accepted_release_is_healthy\(\).*?(?=\n\ndef )", guard, re.S
    )
    assert accepted is not None
    for token in (
        'RELEASE / "release-integrity"',
        '"MainPID"',
        'Path(f"/proc/{pid}/exe")',
        'Path(f"/proc/{pid}/environ")',
        "EXPECTED_PATH",
    ):
        assert token in accepted.group(0)
    assert "boot_id" not in accepted.group(0)
    assert '!= "notify"' in attest


def test_frozen_monitor_state_is_part_of_exact_prestate() -> None:
    prepare = source("prepare-acp-release-r8.sh")
    rollback = source("rollback-r8.sh")
    for text in (prepare, rollback):
        assert "acp-frozen-live-monitor.service" in text
        assert "frozen-monitor-state" in text
    assert 'cmp -s - "${PRESTATE}/frozen-monitor-state"' in rollback


def test_preexisting_live_endpoint_is_rejected_without_unregister() -> None:
    migration = source("acp-migrate-current-pane-r8.sh")
    ordered(
        migration,
        'if ${HERDR} agent acp-status "${NAME}"',
        "exit 1",
        'agent_json="$(${HERDR} agent list)"',
    )
    assert "previous_unregistered" not in migration
    assert migration.count('agent acp-unregister "${NAME}"') == 1
    assert "unregister_owned_endpoint" in migration
    assert 'worker.get("generation") == registered_generation' in migration
    assert 'set -o noclobber' in migration
    ordered(
        migration,
        "registered=1",
        '${HERDR} agent acp-register "${NAME}"',
        'sync "${registration_path}"',
        "write_status registered",
        "wait_for_console",
        "systemctl --user start tendwired.service",
        "wait_for_acp",
        "write_status complete",
        "registered=0",
        'rm -f -- "${registration_path}"',
    )


def test_stale_recovery_requires_owned_phase_markers() -> None:
    build = source("build-acp-release-r8.sh")
    prepare = source("prepare-acp-release-r8.sh")
    recovery = source("recover-stale-r8-artifacts.py")
    assert 'recover-stale-r8-artifacts.py" build' in build
    assert 'recover-stale-r8-artifacts" prepare' in prepare
    for token in (
        '"owned-artifacts.json"',
        '"owner.json"',
        '"phase"',
        '"all_initially_absent"',
        "ALLOWED_BUILD_ARTIFACTS",
        '"live-mutation-started"',
        '"backup-complete"',
    ):
        assert token in recovery or token in build


def test_failed_publish_cleanup_removes_every_owned_artifact() -> None:
    build = source("build-acp-release-r8.sh")
    for flag, path in (
        ("manifest_published", "${MANIFEST}"),
        ("private_env_published", "${PRIVATE_ENV}"),
        ("release_published", "${NEW_RELEASE}"),
        ("runtime_published", "${NEW_RUNTIME}"),
        ("adapter_published", "${CODEX_ACP_ROOT}"),
        ("herdr_published", "${HERDR_RUNTIME}"),
    ):
        assert f'"${{{flag}}}" -eq 1' in build
        assert path in build
    ordered(
        build,
        "private_env_published=1",
        "manifest_published=1",
        '"${NEW_RELEASE}/release-integrity" write',
        "build_complete=1",
    )
    # Fixed final paths can be raced by an out-of-band creator.  Arm direct
    # cleanup only after rename success so cleanup never deletes an unowned
    # destination; the durable all-initially-absent inventory closes the
    # signal gap between rename and flag assignment.
    ordered(build, 'mv -T "${HERDR_RUNTIME_BUILD}" "${HERDR_RUNTIME}"', "herdr_published=1")
    ordered(build, 'mv -T "${ADAPTER_BUILD}" "${CODEX_ACP_ROOT}"', "adapter_published=1")
    cleanup = re.search(r"cleanup\(\) \{(?P<body>.*?)\n\}", build, re.S)
    assert cleanup is not None
    ordered(
        cleanup.group("body"),
        'if [[ "${build_state_initialized}" -eq 1 ]]',
        'for owned_build_path in "${RUNTIME_BUILD}"',
        'rm -rf -- "${owned_build_path}"',
    )


def test_release_lock_binds_authorized_sources() -> None:
    build = source("build-acp-release-r8.sh")
    lock = source("r8-release-lock.json")
    for token in (
        "0b944031c94e99b9f5fac439a850e8b823e8b1f1",
        "f50af733033089020ace3b15e686299cb8a67f1e",
        "7cb0524624f2e730f48c3dac9b547ca130964ae9",
        "ab2dcb6ab0866775895b245a46069baba357eda07ba98b818ab215d70c2dbf60",
        "67568f327cea67bdd1d197feff621c45476da84c",
    ):
        assert token in lock
        assert token in build
    assert '"git", "-C", source, "status", "--porcelain=v1", "--untracked-files=all"' in build
    assert 'archive "${APP_REVISION}"' in build
    assert 'archive --format=tar --output="${adapter_archive}" "${ADAPTER_REVISION}"' in build
    assert 'archive --format=tar --output="${herdr_archive}"' in build
    assert 'npm ci --offline' in build
    assert 'cargo build --release --locked --offline' in build
    assert 'CARGO_BUILD_JOBS=2' in build
    assert 'HERDR_BUILD_ID="${HERDR_REVISION}"' in build
    assert 'test "${herdr_actual_build_id}" = "${HERDR_ELF_BUILD_ID}"' in build
    assert 'herdr_published=1' in build
    assert 'validate-frozen-release' in build
    assert 'DEPLOYMENT BLOCKED: authorized Herdr identity is required' in build
    assert 'merge-base --is-ancestor' in build
    assert "console_canary_evidence_sha256" not in lock
    assert "HERDR_CANARY_EVIDENCE_SHA256" not in build


def test_prepare_accepts_an_intentionally_stopped_prior_monitor() -> None:
    prepare = source("prepare-acp-release-r8.sh")
    assert 'values.get("ExecMainStatus") not in {"0", "15"}' in prepare


def test_r8_manifest_owns_dynamic_canary_digest_and_reviewed_ancestors() -> None:
    integrity = source("release-integrity-r8.py")
    attest = source("attest-r8.py")
    recover = source("recover-stale-r8-artifacts.py")
    for ancestor in (
        "1f277cd562f3852cfbec4e52b2ffc8c406550fc4",
        "36599949daa64f68494d04f96a3bfee31904a804",
        "9026d9bc5a12d9adc2d9f68ebdc564133e4098b4",
    ):
        assert ancestor in integrity
        assert ancestor in attest
        assert ancestor in recover
    assert '"herdr_console_canary_sha256": canary_sha256' in integrity
    assert 'manifest.get("herdr_console_canary_sha256")' in attest
    assert '"merge-base", "--is-ancestor", ancestor, commit' in recover
    assert "_source_preflight()" in recover


def test_operational_snapshot_is_single_commit_and_packaged_integrity_is_exact(
    tmp_path: Path,
) -> None:
    build = source("build-acp-release-r8.sh")
    ordered(
        build,
        'TOOLING_REVISION="$(git -C "${SOURCE}" rev-parse HEAD)"',
        'git -C "${SOURCE}" archive "${TOOLING_REVISION}"',
        'PACKAGED_DEPLOY=${DEPLOY_SNAPSHOT}/.deploy',
        'python3 -I "${PACKAGED_DEPLOY}/recover-stale-r8-artifacts.py" build',
        'install -m 755 "${PACKAGED_DEPLOY}/herdr-console-canary-r8.py"',
    )
    assert 'install -m 755 "${DEPLOY}/' not in build
    assert 'install -m 644 "${DEPLOY}/' not in build

    integrity = load_integrity()
    live = tmp_path / "live"
    snapshot = tmp_path / "snapshot"
    release_lock = tmp_path / "r8-release-lock.json"
    for relative in integrity.EXPECTED_OPERATIONAL_PATHS | {
        ".deploy/r8-release-lock.json"
    }:
        destination = live / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(DEPLOY.parent / relative, destination)
    shutil.copytree(live, snapshot)
    shutil.copy2(live / ".deploy/r8-release-lock.json", release_lock)
    baseline = integrity.verify_operational_snapshot(snapshot, release_lock)

    live_canary = live / ".deploy/herdr-console-canary-r8.py"
    live_canary.write_bytes(live_canary.read_bytes() + b"# concurrent live edit\n")
    assert integrity.verify_operational_snapshot(snapshot, release_lock) == baseline

    snapshot_canary = snapshot / ".deploy/herdr-console-canary-r8.py"
    original = snapshot_canary.read_bytes()
    snapshot_canary.write_bytes(original + b"# mutation\n")
    with pytest.raises(RuntimeError, match="digest mismatch"):
        integrity.verify_operational_snapshot(snapshot, release_lock)
    snapshot_canary.write_bytes(original)

    deleted = snapshot / ".deploy/attest-r8.py"
    deleted.unlink()
    with pytest.raises(RuntimeError, match="file set is not exact"):
        integrity.verify_operational_snapshot(snapshot, release_lock)
    shutil.copy2(live / ".deploy/attest-r8.py", deleted)

    extra = snapshot / ".deploy/unreviewed-r8.py"
    extra.write_text("unreviewed\n", encoding="utf-8")
    with pytest.raises(RuntimeError, match="file set is not exact"):
        integrity.verify_operational_snapshot(snapshot, release_lock)


def test_every_release_packaging_source_is_in_the_operational_lock() -> None:
    build = source("build-acp-release-r8.sh")
    lock = json.loads(source("r8-release-lock.json"))
    inventory = set(lock["operational_sha256"])
    assert len(inventory) == 27
    referenced = {
        f".deploy/{name}"
        for name in re.findall(r'\$\{DEPLOY\}/([^"\s]+)', build)
        if name != "r8-release-lock.json"
    }
    assert referenced <= inventory
    assert {
        ".deploy/acp-r8-release-guard.service",
        ".deploy/acp-r8-rollback.service",
        ".deploy/acp-r8-rollout.service",
        ".deploy/recover-stale-r8-artifacts.py",
        ".deploy/herdr-console-canary-r8.py",
    } <= inventory
    assert 'os.path.basename(path).startswith("acp-r8-")' in build


def test_systemd_timeouts_cover_all_bounded_recovery_waits() -> None:
    rollback_unit = source("acp-r8-rollback.service")
    rollout_unit = source("acp-r8-rollout.service")
    attest = source("attest-r8.py")
    assert "TimeoutStartSec=600" in rollback_unit
    assert "TimeoutStartSec=1800" in rollout_unit
    assert 'TimeoutStartUSec") != "10min"' in attest
    assert 'TimeoutStartUSec") != "30min"' in attest


def test_idle_migration_requires_quiescent_identical_snapshots() -> None:
    migration = source("acp-migrate-current-pane-r8.sh")
    terminate = re.search(
        r"terminate_visible_codex\(\) \{(?P<body>.*?)\n\}", migration, re.S
    )
    assert terminate is not None
    body = terminate.group("body")
    ordered(
        body,
        'first_agents_json="$(${HERDR} agent list)"',
        "validate_idle_visible_codex",
        "sleep 2",
        'second_agents_json="$(${HERDR} agent list)"',
        "if target(first_raw) != target(second_raw)",
        "validate_idle_visible_codex",
        'FREEZE_IDENTITY="$(mktemp',
        'members.sort(key=lambda item: item["pid"])',
        "write_status process_identity_captured",
        'kill -STOP -- "-${STOPPED_GROUP}"',
        "write_status group_stopped",
        'all(item["state"] == "T" for item in members)',
        "write_status frozen_identity_verified",
        "exited=1",
        'kill -TERM -- "-${STOPPED_GROUP}"',
        'kill -CONT -- "-${STOPPED_GROUP}"',
    )
    for token in (
        '"pid", "comm", "pgrp", "session", "tty_nr", "starttime", "cwd"',
        'shell["tty_nr"] not in member_ttys',
        'shell["session"] not in member_sessions',
        'actual != expected or shell_actual != shell_expected',
        'foreground_groups <= {group, shell_before["pgrp"]}',
        "time.monotonic() + 0.25",
    ):
        assert token in body
    assert "stopped_process_json" not in body
    assert "stopped_agents_json" not in body
    assert "os.killpg(group, signal.SIGSTOP)" not in body
