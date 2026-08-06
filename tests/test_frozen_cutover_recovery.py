from __future__ import annotations

import fcntl
import os
import re
import subprocess
from pathlib import Path


SCRIPT = Path(__file__).parents[1] / ".deploy/frozen-cutover-recovery.sh"


def _render(tmp_path: Path) -> Path:
    transaction = tmp_path / "transaction"
    transaction.mkdir()
    source = SCRIPT.read_text(encoding="utf-8")
    source, count = re.subn(
        r"^readonly TRANSACTION_ROOT=.*$",
        f"readonly TRANSACTION_ROOT={transaction}",
        source,
        count=1,
        flags=re.MULTILINE,
    )
    assert count == 1
    source, count = re.subn(
        r"^readonly CUTOVER_LOCK=.*$",
        f"readonly CUTOVER_LOCK={tmp_path / 'cutover.lock'}",
        source,
        count=1,
        flags=re.MULTILINE,
    )
    assert count == 1
    source = source.replace(
        """stop_targets() {
    systemctl --user stop --no-block \\
        herdres-gateway.service herdres.service tendwired.service || true
}""",
        "stop_targets() { :; }",
    )
    assert "systemctl --user stop" not in source
    source = source.split("if ! phase_is_authorized; then", 1)[0]
    source += "phase_is_authorized\n"
    rendered = tmp_path / "recovery.sh"
    rendered.write_text(source, encoding="utf-8")
    rendered.chmod(0o700)
    return rendered


def test_committing_guard_requires_authorization_and_live_cutover_lock(tmp_path: Path) -> None:
    script = _render(tmp_path)
    transaction = tmp_path / "transaction"
    (transaction / "phase").write_text("committing\n", encoding="utf-8")
    authorization = transaction / "forward-resume-authorized"
    owner_start = Path(f"/proc/{os.getpid()}/stat").read_text().split()[21]
    authorization.write_text(
        f"RESUME_FROZEN_ACP_CUTOVER {os.getpid()} {owner_start}\n",
        encoding="utf-8",
    )
    authorization.chmod(0o600)

    unauthorized = subprocess.run([script], capture_output=True, text=True, check=False)
    assert unauthorized.returncode == 1

    lock_path = tmp_path / "cutover.lock"
    with lock_path.open("w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        authorized = subprocess.run([script], capture_output=True, text=True, check=False)
    assert authorized.returncode == 0


def test_committing_guard_rejects_stale_owner_even_while_child_holds_lock(tmp_path: Path) -> None:
    script = _render(tmp_path)
    transaction = tmp_path / "transaction"
    (transaction / "phase").write_text("committing\n", encoding="utf-8")
    authorization = transaction / "forward-resume-authorized"
    authorization.write_text(
        "RESUME_FROZEN_ACP_CUTOVER 999999999 1\n", encoding="utf-8"
    )
    authorization.chmod(0o600)
    with (tmp_path / "cutover.lock").open("w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        stale = subprocess.run([script], capture_output=True, text=True, check=False)
    assert stale.returncode == 1
