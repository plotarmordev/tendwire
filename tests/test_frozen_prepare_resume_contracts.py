from __future__ import annotations

import shlex
import re
import subprocess
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
PREPARE = ROOT / ".deploy/prepare-frozen-release.sh"
DEPLOY = ROOT / ".deploy/deploy-frozen-7446533-6c0d0f5.sh"
RESUME = ROOT / ".deploy/resume-frozen-cutover.sh"
GUARD = ROOT / ".deploy/frozen-cutover-recovery.sh"


def _git_repository(path: Path) -> str:
    path.mkdir()
    subprocess.run(["git", "init", "-q", path], check=True)
    subprocess.run(
        ["git", "-C", path, "config", "user.email", "fixture@example.invalid"],
        check=True,
    )
    subprocess.run(
        ["git", "-C", path, "config", "user.name", "Fixture"], check=True
    )
    tracked = path / "tracked.txt"
    tracked.write_text("committed\n", encoding="utf-8")
    subprocess.run(["git", "-C", path, "add", "tracked.txt"], check=True)
    subprocess.run(["git", "-C", path, "commit", "-q", "-m", "fixture"], check=True)
    return subprocess.run(
        ["git", "-C", path, "rev-parse", "HEAD"],
        capture_output=True,
        check=True,
        text=True,
    ).stdout.strip()


def _render_prepare(tmp_path: Path, tendwire: Path, herdres: Path) -> Path:
    tendwire_revision = _git_repository(tendwire)
    herdres_revision = _git_repository(herdres)
    replacements = {
        "readonly TENDWIRE_SOURCE=/home/smith/tendwire": (
            f"readonly TENDWIRE_SOURCE={shlex.quote(str(tendwire))}"
        ),
        "readonly HERDRES_SOURCE=/home/smith/tendwire/.worktrees/herdres-acp-route": (
            f"readonly HERDRES_SOURCE={shlex.quote(str(herdres))}"
        ),
        'build_root="$(mktemp -d /tmp/frozen-acp-release.XXXXXX)"': (
            f"build_root={shlex.quote(str(tmp_path / 'build'))}"
        ),
    }
    source = PREPARE.read_text(encoding="utf-8")
    for original, replacement in replacements.items():
        assert original in source
        source = source.replace(original, replacement, 1)
    source, count = re.subn(
        r"^readonly TENDWIRE_REVISION=[0-9a-f]{40}$",
        f"readonly TENDWIRE_REVISION={tendwire_revision}",
        source,
        count=1,
        flags=re.MULTILINE,
    )
    assert count == 1
    source, count = re.subn(
        r"^readonly HERDRES_REVISION=[0-9a-f]{40}$",
        f"readonly HERDRES_REVISION={herdres_revision}",
        source,
        count=1,
        flags=re.MULTILINE,
    )
    assert count == 1
    source, count = re.subn(
        r"^readonly PREPARE_LOCK=.*$",
        f"readonly PREPARE_LOCK={shlex.quote(str(tmp_path / 'prepare.lock'))}",
        source,
        count=1,
        flags=re.MULTILINE,
    )
    assert count == 1
    rendered = tmp_path / "prepare.sh"
    rendered.write_text(source, encoding="utf-8")
    rendered.chmod(0o700)
    return rendered


@pytest.mark.parametrize("dirty_repository", ["tendwire", "herdres"])
def test_prepare_rejects_tracked_dirty_sources_before_build_or_publish(
    tmp_path: Path, dirty_repository: str
) -> None:
    tendwire = tmp_path / "tendwire"
    herdres = tmp_path / "herdres"
    script = _render_prepare(tmp_path, tendwire, herdres)
    dirty = tendwire if dirty_repository == "tendwire" else herdres
    (dirty / "tracked.txt").write_text("dirty\n", encoding="utf-8")

    result = subprocess.run(
        [
            "/usr/bin/env", "-i", "HOME=/home/smith", "USER=smith",
            "LOGNAME=smith", "PATH=/usr/bin:/bin", "FROZEN_ACP_CLEAN_ENV=1",
            "/bin/bash", "-x", script,
        ],
        capture_output=True,
        check=False,
        text=True,
    )

    assert result.returncode != 0
    assert "status --porcelain --untracked-files=no" in result.stderr
    assert "archive --format=tar" not in result.stderr
    assert "python -m build" not in result.stderr
    assert not (tmp_path / "build").exists()


def _ordered(source: str, *needles: str) -> None:
    position = -1
    for needle in needles:
        next_position = source.find(needle, position + 1)
        assert next_position >= 0, f"missing resume contract step: {needle}"
        assert next_position > position
        position = next_position


def test_resume_validates_frozen_transaction_before_authorizing_recovery() -> None:
    source = RESUME.read_text(encoding="utf-8")
    main = source[source.index('exec 9>"${CUTOVER_LOCK}"') :]

    _ordered(
        main,
        'exec 9>"${CUTOVER_LOCK}"',
        "flock -n 9",
        'current_phase="$(<"${PHASE_FILE}")"',
        'case "${current_phase}" in',
        'test -f "${RESET_STARTED}"',
        'test "$(stat -c \'%u:%a\' "${RESET_STARTED}")" = "$(id -u):600"',
        'test "$(systemctl --user show --value -p MainPID herdr-server.service)" =',
        "rebind_forward_release",
        'test "$(readlink -f "${ACTIVE_LINK}")" = "${NEW_RELEASE}"',
        'cmp -s -- "${NEW_RELEASE}/systemd/${unit}-99.conf"',
        "assert_effective_units",
        'rm -f -- "${RESUME_AUTH}"',
        "printf '%s %s %s\\n' RESUME_FROZEN_ACP_CUTOVER",
        'chmod 0600 "${RESUME_AUTH}.tmp.$$"',
        'mv -T -- "${RESUME_AUTH}.tmp.$$" "${RESUME_AUTH}"',
        "systemctl --user start acp-frozen-release-recovery.service",
        "systemctl --user is-active --quiet acp-frozen-release-recovery.service",
    )


def test_resume_rebinds_only_from_the_verified_stopped_previous_release() -> None:
    source = RESUME.read_text(encoding="utf-8")
    rebind = source[
        source.index("rebind_forward_release() {") : source.index("wait_tendwire() {")
    ]

    _ordered(
        rebind,
        'test "${current}" = "${PREVIOUS_RELEASE}"',
        'assert_unit_quiescent "${unit}"',
        '"${PREVIOUS_RELEASE}/validate-frozen-release"',
        'mv -T -- "${TRANSACTION_ROOT}/release-validation.json"',
        '"${NEW_RELEASE}/validate-frozen-release"',
        "--manifest \"${RELEASE_MANIFEST}\" --create",
        "prepare_private_env",
        'publish_unit "${NEW_RELEASE}/systemd/${unit}-99.conf"',
        "switch_release",
        "systemctl --user daemon-reload",
    )
    assert 'readonly PREVIOUS_VALIDATION=${TRANSACTION_ROOT}/release-validation.6c0d0f5.json' in source
    assert "os.O_NOFOLLOW" in source
    assert "opened = os.fstat(descriptor)" in source
    assert "after = os.fstat(descriptor)" in source
    assert "stat.S_IMODE(opened.st_mode) != 0o600" in source
    assert 'elif [[ ! -e "${TRANSACTION_ROOT}/release-validation.json"' in source
    assert "provisional|validating|validation_failed)" in source
    assert 'assert_previous_validation_file "${PREVIOUS_VALIDATION}"' in source
    assert 'assert_exec acp-frozen-release-recovery.service' in source
    assert 'assert_exec acp-frozen-live-monitor.service' in source
    assert 'assert_exec acp-cutover-recovery.service' in source
    assert 'assert_environment_assignment "${tw_environment}" PYTHONPATH ""' in source
    assert 'active_state="$(systemctl --user show --value -p ActiveState "${unit}")"' in source
    assert 'job="$(systemctl --user show --value -p Job "${unit}")"' in source
    assert "assert_active_validation()" in source
    active_validation = source[
        source.index("assert_active_validation()") : source.index('exec 9>"${CUTOVER_LOCK}"')
    ]
    assert "heartbeat_body_after" in active_validation
    assert "heartbeat_identity_after != heartbeat_identity" in active_validation
    assert 'print("initializing")' in active_validation
    assert 'len(samples) == heartbeat_index + 1' in active_validation
    assert 'test "${unit_start}" -lt "${owner_start}"' in active_validation
    assert "assert_unit_quiescent acp-cutover-recovery.service" in active_validation
    publish = source[source.index("publish_unit() {") : source.index("prepare_private_env() {")]
    _ordered(publish, 'install -m 0644 "${source}" "${temporary}"', 'sync "${temporary}"', 'mv -T -- "${temporary}" "${target}"', 'sync -d "$(dirname "${target}")"')


def test_prepare_packages_tooling_from_named_git_revision() -> None:
    source = PREPARE.read_text(encoding="utf-8")
    archive = (
        'git -C "${TENDWIRE_SOURCE}" archive --format=tar '
        '--output="${build_root}/tooling.tar"'
    )
    extract = 'tar -xf "${build_root}/tooling.tar" -C "${build_root}/tooling-source"'
    install_release = (
        'install -m 0555 "${DEPLOY_SOURCE}/validate-frozen-release.py"'
    )
    assert 'readonly DEPLOY_SOURCE="${build_root}/tooling-source/.deploy"' in source
    assert source.index(archive) < source.index(extract) < source.index(install_release)
    assert '"${TOOLING_REVISION}" -- .deploy' in source


def test_release_shells_use_privileged_absolute_bash_and_sanitize_environment() -> None:
    for path in (PREPARE, DEPLOY, RESUME, GUARD):
        source = path.read_text(encoding="utf-8")
        assert source.startswith("#!/bin/bash -p\nset -Eeuo pipefail\n")
        clean_launch = "exec /usr/bin/env -i"
        path_export = "export PATH=/usr/bin:/bin"
        environment_reset = (
            "unset BASH_ENV ENV CDPATH GLOBIGNORE PYTHONHOME PYTHONPATH TAR_OPTIONS"
        )
        assert source.index(clean_launch) < source.index(path_export)
        assert "done < <(compgen -e)" in source[: source.index(clean_launch)]
        assert "*) return 1 ;;" in source[: source.index(clean_launch)]
        assert source.index(path_export) < source.index(environment_reset)
        assert source.index(environment_reset) < source.index("readonly ")
        launch = source[source.index(clean_launch) : source.index("fi\n", source.index(clean_launch))]
        for forbidden in (
            "TAR_OPTIONS", "GIT_DIR", "LD_PRELOAD", "HTTPS_PROXY", "ALL_PROXY",
            "SSL_CERT_FILE", "SSL_CERT_DIR",
        ):
            assert forbidden not in launch
        assert "/usr/bin/env bash" not in source


def test_prepare_owns_each_publication_before_rename_and_recovers_partial_state() -> None:
    source = PREPARE.read_text(encoding="utf-8")
    publication_pairs = (
        ("published_runtime=1", 'mv -T -- "${runtime_stage}" "${TENDWIRE_RUNTIME}"'),
        ("published_release=1", 'mv -T -- "${release_stage}" "${RELEASE_ROOT}"'),
        ("published_runner=1", 'mv -T -- "${runner_stage}" "${RUNNER_ROOT}"'),
        (
            "published_manifest=1",
            'mv -T -- "${manifest_publish_tmp}" "${MANIFEST}"',
        ),
        (
            "published_complete=1",
            'mv -T -- "${complete_publish_tmp}" "${COMPLETE_MARKER}"',
        ),
    )
    publication = source.index('install -d -m 0755 -- "$(dirname "${TENDWIRE_RUNTIME}")"')
    for flag, rename in publication_pairs:
        assert source.index(flag, publication) < source.index(rename, publication)
    postcheck = '"${TENDWIRE_RUNTIME}/bin/python" -B -I -c'
    marker_move = 'mv -T -- "${complete_publish_tmp}" "${COMPLETE_MARKER}"'
    assert source.index(postcheck, publication) < source.index(marker_move, publication)
    recovery_call = source.index("quarantine_partial_publication\n")
    absence_gate = source.index(
        '[[ ! -e "${TENDWIRE_RUNTIME}" && ! -L "${TENDWIRE_RUNTIME}" ]]'
    )
    assert recovery_call < absence_gate
    recovery = source[source.index("quarantine_partial_publication()") : recovery_call]
    for guard in ("ACTIVE_LINK", "TENDWIRE_CANDIDATE", "HERDRES_CANDIDATE", "TRANSACTION_ROOT"):
        assert guard in recovery
    assert '"${MANIFEST}" "${COMPLETE_MARKER}"' in recovery
    assert '[[ "${present}" -ne 5 ]]' in recovery


def test_resume_starts_and_validates_each_candidate_layer_in_order() -> None:
    source = RESUME.read_text(encoding="utf-8")
    main = source[source.index('exec 9>"${CUTOVER_LOCK}"') :]

    _ordered(
        main,
        "systemctl --user stop herdres-gateway.service herdres.service tendwired.service",
        "systemctl --user start tendwired.service",
        "wait_tendwire",
        '"${TOPIC_PYTHON}" -I "${TOPIC_TOOL}" --apply',
        '"${TOPIC_PYTHON}" -I "${TOPIC_TOOL}" --gate-presenter',
        '"${TOPIC_PYTHON}" -I "${TOPIC_TOOL}" --gate-existing-presenter',
        "systemctl --user start herdres.service",
        "wait_herdres",
        '"${TOPIC_PYTHON}" -I "${TOPIC_TOOL}" --verify-presenter',
        "systemctl --user start herdres-gateway.service",
        "wait_gateway",
        '"${TOPIC_PYTHON}" -I "${TOPIC_TOOL}" --verify-gateway',
        "phase_write provisional",
        "systemctl --user start acp-frozen-live-monitor.service",
        '"$(<"${PHASE_FILE}")" = validating',
        'rm -- "${RESUME_AUTH}"',
    )
    assert "phase_write deployed" not in main
