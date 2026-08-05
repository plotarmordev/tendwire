from __future__ import annotations

import json

from tendwire.cli import _daemon_client_timeout_seconds, main
from tendwire.config import Config


def test_command_daemon_timeout_covers_preflight_and_acp_ack() -> None:
    config = Config(
        herdr_timeout_seconds=7,
        acp_request_timeout_seconds=41,
        acp_shutdown_timeout_seconds=5,
    )

    assert _daemon_client_timeout_seconds(config, "command.submit") == 175.5


def test_snapshot_uses_daemon_and_never_reads_store(capsys, monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("TENDWIRE_DATA_DIR", str(tmp_path))
    code = main(["snapshot", "--json"])
    payload = json.loads(capsys.readouterr().out)
    assert code == 1
    assert payload["status"] == "daemon_unavailable"
    assert not (tmp_path / "tendwire.db").exists()


def test_attention_uses_daemon_and_never_reads_store(capsys, monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("TENDWIRE_DATA_DIR", str(tmp_path))
    code = main(["attention", "--json"])
    payload = json.loads(capsys.readouterr().out)
    assert code == 1
    assert payload["status"] == "daemon_unavailable"
    assert not (tmp_path / "tendwire.db").exists()
