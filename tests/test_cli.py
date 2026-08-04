from __future__ import annotations

import json

from tendwire.cli import main


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
