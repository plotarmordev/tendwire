from __future__ import annotations

import json
from types import SimpleNamespace

from tendwire.cli import _daemon_client_timeout_seconds, main
from tendwire.config import Config


def test_no_subcommand_prints_help_and_exits_zero(capsys) -> None:
    assert main([]) == 0
    output = capsys.readouterr()
    assert output.out.startswith("usage: tendwire")
    assert output.err == ""


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


def test_doctor_preserves_trusted_daemon_identity(capsys, monkeypatch) -> None:
    monkeypatch.setattr(
        "tendwire.cli._try_daemon_attempt",
        lambda *_args, **_kwargs: SimpleNamespace(
            result={"schema_version": 1, "status": "ok", "pid": 1234},
            response_error=None,
            error_kind=None,
            request_started=True,
        ),
    )

    assert main(["doctor", "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["pid"] == 1234
