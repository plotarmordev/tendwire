from __future__ import annotations

from tendwire.backends.acp_coordinator import _discovered_workers
from tendwire.config import Config


def test_pane_label_is_public_but_cwd_and_target_are_private(tmp_path) -> None:
    workers, bindings, omissions = _discovered_workers(
        Config(host_id="h", data_dir=tmp_path, db_path=tmp_path / "db"),
        {"panes": [{
            "workspace_id": "wR9",
            "pane_id": "wR9:pA",
            "terminal_id": "term-private",
            "agent": "claude",
            "label": "Review pane",
            "cwd": "/private/path",
        }]},
        {"agents": []},
        "2026-01-01T00:00:00+00:00",
    )
    assert workers[0].meta["label"] == "Review pane"
    assert "private" not in str(workers[0].to_dict())
    assert bindings[0].target_value == "term-private"
    assert omissions == 0
