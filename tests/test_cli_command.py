from __future__ import annotations

import io
import json

from tendwire.cli import main


def _run(monkeypatch, capsys, payload):
    monkeypatch.setattr("sys.stdin", io.StringIO(json.dumps(payload)))
    code = main(["command", "--json"])
    return code, json.loads(capsys.readouterr().out)


def test_invalid_command_is_rejected_locally(monkeypatch, capsys) -> None:
    code, payload = _run(monkeypatch, capsys, {"schema_version": 1, "action": "unknown"})
    assert code == 1
    assert payload["ok"] is False


def test_noop_remains_pure(monkeypatch, capsys) -> None:
    code, payload = _run(monkeypatch, capsys, {"schema_version": 1, "action": "noop"})
    assert code == 0
    assert payload["status"] == "noop"


def test_live_mutation_never_falls_back_from_daemon(monkeypatch, capsys, tmp_path) -> None:
    monkeypatch.setenv("TENDWIRE_DATA_DIR", str(tmp_path))
    code, payload = _run(
        monkeypatch,
        capsys,
        {
            "schema_version": 1,
            "action": "send_instruction",
            "request_id": "r1",
            "dry_run": False,
            "target": {"worker_id": "worker"},
            "instruction": {"text": "hello"},
        },
    )
    assert code == 1
    assert payload["status"] == "backend_unavailable"
    assert not (tmp_path / "tendwire.db").exists()
