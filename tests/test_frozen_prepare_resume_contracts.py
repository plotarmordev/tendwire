from __future__ import annotations

import shlex
import re
import subprocess
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
PREPARE = ROOT / ".deploy/prepare-frozen-release.sh"
RESUME = ROOT / ".deploy/resume-frozen-cutover.sh"


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
        "readonly DEPLOY_SOURCE=/home/smith/tendwire/.deploy": (
            f"readonly DEPLOY_SOURCE={shlex.quote(str(tmp_path / 'deploy'))}"
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
        ["bash", "-x", script], capture_output=True, check=False, text=True
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
        'test "$(<"${PHASE_FILE}")" = committing',
        'test -f "${RESET_STARTED}"',
        'test "$(stat -c \'%u:%a\' "${RESET_STARTED}")" = "$(id -u):600"',
        'test "$(readlink -f "${ACTIVE_LINK}")" = "${NEW_RELEASE}"',
        'test "$(systemctl --user show --value -p MainPID herdr-server.service)" =',
        'cmp -s -- "${NEW_RELEASE}/systemd/${unit}-99.conf"',
        'rm -f -- "${RESUME_AUTH}"',
        "printf '%s %s %s\\n' RESUME_FROZEN_ACP_CUTOVER",
        'chmod 0600 "${RESUME_AUTH}.tmp.$$"',
        'mv -T -- "${RESUME_AUTH}.tmp.$$" "${RESUME_AUTH}"',
        "systemctl --user start acp-frozen-release-recovery.service",
        "systemctl --user is-active --quiet acp-frozen-release-recovery.service",
    )


def test_resume_starts_and_validates_each_candidate_layer_in_order() -> None:
    source = RESUME.read_text(encoding="utf-8")
    main = source[source.index('exec 9>"${CUTOVER_LOCK}"') :]

    _ordered(
        main,
        "systemctl --user stop herdres-gateway.service herdres.service tendwired.service",
        "systemctl --user start tendwired.service",
        "wait_tendwire",
        '"${TOPIC_PYTHON}" "${TOPIC_TOOL}" --apply',
        '"${TOPIC_PYTHON}" "${TOPIC_TOOL}" --gate-presenter',
        '"${TOPIC_PYTHON}" "${TOPIC_TOOL}" --gate-existing-presenter',
        "systemctl --user start herdres.service",
        "wait_herdres",
        '"${TOPIC_PYTHON}" "${TOPIC_TOOL}" --verify-presenter',
        "systemctl --user start herdres-gateway.service",
        "wait_gateway",
        '"${TOPIC_PYTHON}" "${TOPIC_TOOL}" --verify-gateway',
        "phase_write provisional",
        "systemctl --user start acp-frozen-live-monitor.service",
        '"$(<"${PHASE_FILE}")" = validating',
        'rm -- "${RESUME_AUTH}"',
    )
    assert "phase_write deployed" not in main
