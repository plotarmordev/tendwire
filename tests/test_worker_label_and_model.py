from __future__ import annotations

from tendwire.backends.acp_coordinator import _discovered_workers
from tendwire.config import Config
from tendwire.core.turns import Turn


def test_pane_label_is_public_but_cwd_and_target_are_private(tmp_path) -> None:
    workers, bindings = _discovered_workers(
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


def test_llm_model_round_trip_and_turn_id_stability() -> None:
    base = {"host_id": "h", "worker_id": "w", "kind": "turn", "source": "acp", "complete": True}
    plain = Turn.from_dict(base)
    modeled = Turn.from_dict({**base, "model": "claude-fable-5"})
    assert modeled.model == "claude-fable-5"
    assert modeled.to_dict()["model"] == "claude-fable-5"
    assert plain.id == modeled.id
    assert plain.fingerprint != modeled.fingerprint
