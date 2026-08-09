from __future__ import annotations

import importlib.util
from copy import deepcopy
import json
from pathlib import Path
import sys

import pytest


SCRIPT = Path(__file__).resolve().parents[1] / "topology-normalizer-r8.py"


def load_normalizer():
    spec = importlib.util.spec_from_file_location("r8_topology_test", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def snapshot() -> dict[str, object]:
    session = {"agent": "codex", "kind": "id", "source": "herdr:codex", "value": "session"}
    return {
        "result": {
            "snapshot": {
                "version": "0.7.5",
                "protocol": 18,
                "focused_workspace_id": "w53",
                "focused_tab_id": "w53:t7",
                "focused_pane_id": "w53:p8",
                "workspaces": [{
                    "workspace_id": "w53", "number": 1, "label": "Tendwire",
                    "focused": True, "pane_count": 1, "tab_count": 1,
                    "active_tab_id": "w53:t7", "agent_status": "idle", "tokens": {},
                    "worktree": {"repo_key": "r", "repo_name": "tendwire", "repo_root": "/home/smith/tendwire", "checkout_path": "/home/smith/tendwire", "is_linked_worktree": False},
                }],
                "tabs": [{
                    "tab_id": "w53:t7", "workspace_id": "w53", "number": 1,
                    "label": "work", "focused": True, "pane_count": 1,
                    "agent_status": "idle",
                }],
                "panes": [{
                    "pane_id": "w53:p8", "terminal_id": "term_1", "workspace_id": "w53",
                    "tab_id": "w53:t7", "focused": True, "cwd": "/home/smith/tendwire",
                    "foreground_cwd": "/home/smith/tendwire", "label": "Coda", "agent": "codex",
                    "title": "Codex", "terminal_title": "Codex", "terminal_title_stripped": "Codex",
                    "display_agent": "codex", "agent_status": "idle", "input_pending": False,
                    "composer": {"state": "empty"}, "state_labels": {}, "tokens": {},
                    "agent_session": session, "last_completed_turn": {"turn": 7},
                    "turn": 7, "turn_epoch": 1, "scroll": {"offset": 0}, "revision": 9,
                }],
                "layouts": [{
                    "workspace_id": "w53", "tab_id": "w53:t7", "zoomed": False,
                    "area": {"x": 0, "y": 0, "width": 120, "height": 40},
                    "focused_pane_id": "w53:p8",
                    "panes": [{"pane_id": "w53:p8", "focused": True, "rect": {"x": 0, "y": 0, "width": 120, "height": 40}}],
                    "splits": [],
                }],
                "agents": [{"agent_status": "idle"}],
            }
        }
    }


def test_transient_process_telemetry_is_explicitly_excluded() -> None:
    normalizer = load_normalizer()
    first = snapshot()
    second = deepcopy(first)
    pane = second["result"]["snapshot"]["panes"][0]
    pane.update({
        "terminal_id": "term_2",
        "foreground_cwd": "/tmp/live-process",
        "title": "changed live title",
        "terminal_title": "changed terminal title",
        "terminal_title_stripped": "changed stripped title",
        "display_agent": "changed",
        "agent_status": "working",
        "input_pending": True,
        "input_prompt_kind": "prompt",
        "composer": {"state": "changed"},
        "state_labels": {"changed": True},
        "tokens": {"changed": 1},
        "last_completed_turn": {"turn": 8},
        "turn": 9,
        "turn_epoch": 2,
        "scroll": {"offset": 1},
        "revision": 10,
    })
    layout = second["result"]["snapshot"]["layouts"][0]
    layout["area"] = {"x": 9, "y": 8, "width": 70, "height": 20}
    layout["panes"][0]["rect"] = {"x": 1, "y": 2, "width": 30, "height": 10}
    second["result"]["snapshot"]["agents"] = [{"agent_status": "working"}]
    assert normalizer.normalize(first) == normalizer.normalize(second)


@pytest.mark.parametrize(
    ("collection", "field", "value"),
    (
        ("workspaces", "worktree", None),
        ("tabs", "focused", False),
        ("panes", "label", "changed"),
        ("panes", "cwd", "/tmp"),
        ("panes", "agent_session", {"agent": "codex", "kind": "id", "source": "herdr:codex", "value": "other"}),
        ("layouts", "zoomed", True),
    ),
)
def test_every_stable_topology_field_changes_fingerprint(collection: str, field: str, value: object) -> None:
    normalizer = load_normalizer()
    first = snapshot()
    second = deepcopy(first)
    second["result"]["snapshot"][collection][0][field] = value
    assert normalizer.normalize(first) != normalizer.normalize(second)


def test_unknown_schema_field_fails_closed() -> None:
    normalizer = load_normalizer()
    value = snapshot()
    value["result"]["snapshot"]["panes"][0]["future_field"] = True
    with pytest.raises(RuntimeError, match="schema mismatch"):
        normalizer.normalize(value)


def test_split_identity_direction_and_ratio_remain_strict_but_rect_is_transient() -> None:
    normalizer = load_normalizer()
    first = snapshot()
    split = {
        "id": "split-1",
        "direction": "horizontal",
        "ratio": 0.5,
        "rect": {"x": 0, "y": 0, "width": 120, "height": 40},
    }
    first["result"]["snapshot"]["layouts"][0]["splits"] = [split]
    second = deepcopy(first)
    second["result"]["snapshot"]["layouts"][0]["splits"][0]["rect"] = {
        "x": 4, "y": 5, "width": 40, "height": 12,
    }
    assert normalizer.normalize(first) == normalizer.normalize(second)
    for field, value in (("id", "split-2"), ("direction", "vertical"), ("ratio", 0.75)):
        changed = deepcopy(second)
        changed["result"]["snapshot"]["layouts"][0]["splits"][0][field] = value
        assert normalizer.normalize(first) != normalizer.normalize(changed)


@pytest.mark.parametrize(
    "mutate",
    (
        lambda value: value["result"]["snapshot"]["panes"][0].update(pane_id="w53:p9"),
        lambda value: value["result"]["snapshot"]["panes"][0].update(tab_id="w53:t8"),
        lambda value: value["result"]["snapshot"]["workspaces"][0].update(pane_count=2),
        lambda value: value["result"]["snapshot"]["layouts"][0].update(focused_pane_id="w53:p9"),
    ),
)
def test_invalid_structural_references_fail_closed(mutate) -> None:
    normalizer = load_normalizer()
    value = snapshot()
    mutate(value)
    with pytest.raises(RuntimeError):
        normalizer.normalize(value)


def test_duplicate_pane_identity_fails_closed() -> None:
    normalizer = load_normalizer()
    value = snapshot()
    value["result"]["snapshot"]["panes"].append(
        deepcopy(value["result"]["snapshot"]["panes"][0])
    )
    with pytest.raises(RuntimeError, match="duplicated"):
        normalizer.normalize(value)


def test_duplicate_terminal_identity_fails_closed_with_valid_two_pane_topology() -> None:
    normalizer = load_normalizer()
    value = snapshot()
    snapshot_value = value["result"]["snapshot"]
    second = deepcopy(snapshot_value["panes"][0])
    second.update({
        "pane_id": "w53:p9",
        "focused": False,
        "label": "Second",
        "agent_session": {
            "agent": "codex", "kind": "id", "source": "herdr:codex",
            "value": "other-session",
        },
    })
    snapshot_value["panes"].append(second)
    snapshot_value["workspaces"][0]["pane_count"] = 2
    snapshot_value["tabs"][0]["pane_count"] = 2
    snapshot_value["layouts"][0]["panes"].append({
        "pane_id": "w53:p9",
        "focused": False,
        "rect": {"x": 60, "y": 0, "width": 60, "height": 40},
    })
    with pytest.raises(RuntimeError, match="terminal_id is duplicated"):
        normalizer.normalize(value)


def test_main_accepts_terminal_remint_and_requires_exact_session_and_cwd(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    normalizer = load_normalizer()
    first = snapshot()
    second = deepcopy(first)
    second["result"]["snapshot"]["panes"][0]["terminal_id"] = "term_reminted"
    first_path = tmp_path / "first.json"
    second_path = tmp_path / "second.json"
    first_path.write_text(json.dumps(first), encoding="utf-8")
    second_path.write_text(json.dumps(second), encoding="utf-8")
    monkeypatch.setattr(sys, "argv", [
        str(SCRIPT), str(first_path), str(second_path),
        "--session", "session", "--pane", "w53:p8",
        "--cwd", "/home/smith/tendwire",
    ])
    assert normalizer.main() == 0
    monkeypatch.setattr(sys, "argv", [
        str(SCRIPT), str(first_path), str(second_path),
        "--session", "wrong", "--pane", "w53:p8",
        "--cwd", "/home/smith/tendwire",
    ])
    with pytest.raises(RuntimeError, match="target pane identity"):
        normalizer.main()


def test_removed_terminal_argument_is_rejected(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    normalizer = load_normalizer()
    path = tmp_path / "snapshot.json"
    path.write_text(json.dumps(snapshot()), encoding="utf-8")
    monkeypatch.setattr(sys, "argv", [
        str(SCRIPT), str(path), str(path), "--session", "session",
        "--pane", "w53:p8", "--cwd", "/home/smith/tendwire",
        "--terminal", "term_1",
    ])
    with pytest.raises(SystemExit) as error:
        normalizer.main()
    assert error.value.code == 2
