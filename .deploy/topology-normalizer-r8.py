#!/usr/bin/python3 -I
"""Fail-closed comparison of stable Herdr session topology.

Herdr snapshots also contain process telemetry.  Only the explicitly listed
transient fields below are excluded; unknown schema fields fail closed so a
future Herdr release cannot silently weaken the rollback/restart check.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


SESSION_KEYS = {
    "version", "protocol", "focused_workspace_id", "focused_tab_id",
    "focused_pane_id", "workspaces", "tabs", "panes", "layouts", "agents",
}
WORKSPACE_KEYS = {
    "workspace_id", "number", "label", "focused", "pane_count", "tab_count",
    "active_tab_id", "agent_status", "tokens", "worktree",
}
WORKSPACE_REQUIRED = {
    "workspace_id", "number", "label", "focused", "pane_count", "tab_count",
    "active_tab_id", "agent_status",
}
TAB_KEYS = {
    "tab_id", "workspace_id", "number", "label", "focused", "pane_count",
    "agent_status",
}
PANE_KEYS = {
    "pane_id", "terminal_id", "workspace_id", "tab_id", "focused", "cwd",
    "foreground_cwd", "label", "agent", "title", "terminal_title",
    "terminal_title_stripped", "display_agent", "agent_status", "input_pending",
    "input_prompt_kind", "composer", "state_labels", "tokens", "agent_session",
    "last_completed_turn", "turn", "turn_epoch", "scroll", "revision",
}
PANE_REQUIRED = {
    "pane_id", "terminal_id", "workspace_id", "tab_id", "focused",
    "agent_status", "revision",
}
LAYOUT_KEYS = {
    "workspace_id", "tab_id", "zoomed", "area", "focused_pane_id", "panes",
    "splits",
}
RECT_KEYS = {"x", "y", "width", "height"}
LAYOUT_PANE_KEYS = {"pane_id", "focused", "rect"}
LAYOUT_SPLIT_KEYS = {"id", "direction", "ratio", "rect"}

# These values are observational process state, not restorable topology.
WORKSPACE_TRANSIENT = {"agent_status", "tokens"}
TAB_TRANSIENT = {"agent_status"}
PANE_TRANSIENT = {
    "terminal_id", "foreground_cwd", "title", "terminal_title",
    "terminal_title_stripped", "display_agent", "agent_status",
    "input_pending", "input_prompt_kind", "composer", "state_labels",
    "tokens", "last_completed_turn", "turn", "turn_epoch", "scroll",
    "revision",
}
SESSION_TRANSIENT = {"version", "protocol", "agents"}


def _object(value: Any, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise RuntimeError(f"{label} is not an object")
    return value


def _rows(value: Any, label: str) -> list[dict[str, Any]]:
    if not isinstance(value, list) or not value:
        raise RuntimeError(f"{label} is not a non-empty list")
    return [_object(row, label) for row in value]


def _keys(row: dict[str, Any], allowed: set[str], required: set[str], label: str) -> None:
    unknown = set(row) - allowed
    missing = required - set(row)
    if unknown or missing:
        raise RuntimeError(
            f"{label} schema mismatch: unknown={sorted(unknown)} missing={sorted(missing)}"
        )


def _identifier(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value:
        raise RuntimeError(f"{label} is not a non-empty string")
    return value


def _unique(rows: list[dict[str, Any]], key: str, label: str) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for row in rows:
        identifier = _identifier(row.get(key), f"{label} {key}")
        if identifier in result:
            raise RuntimeError(f"{label} {key} is duplicated")
        result[identifier] = row
    return result


def _rect(value: Any, label: str) -> dict[str, Any]:
    row = _object(value, label)
    _keys(row, RECT_KEYS, RECT_KEYS, label)
    if any(type(row[key]) is not int for key in RECT_KEYS):
        raise RuntimeError(f"{label} coordinates are not integers")
    return {key: row[key] for key in sorted(RECT_KEYS)}


def _layout(row: dict[str, Any]) -> dict[str, Any]:
    _keys(row, LAYOUT_KEYS, LAYOUT_KEYS, "layout")
    _identifier(row["workspace_id"], "layout workspace_id")
    _identifier(row["tab_id"], "layout tab_id")
    _identifier(row["focused_pane_id"], "layout focused_pane_id")
    if not isinstance(row["zoomed"], bool):
        raise RuntimeError("layout zoomed is not a boolean")
    _rect(row["area"], "layout area")
    panes = _rows(row["panes"], "layout panes")
    splits = row["splits"]
    if not isinstance(splits, list):
        raise RuntimeError("layout splits is not a list")
    normalized_panes = []
    for pane in panes:
        _keys(pane, LAYOUT_PANE_KEYS, LAYOUT_PANE_KEYS, "layout pane")
        _identifier(pane["pane_id"], "layout pane_id")
        if not isinstance(pane["focused"], bool):
            raise RuntimeError("layout pane focused is not a boolean")
        _rect(pane["rect"], "layout pane rect")
        normalized_panes.append(
            {
                "pane_id": pane["pane_id"],
                "focused": pane["focused"],
            }
        )
    _unique(normalized_panes, "pane_id", "layout pane")
    normalized_splits = []
    for split_value in splits:
        split = _object(split_value, "layout split")
        _keys(split, LAYOUT_SPLIT_KEYS, LAYOUT_SPLIT_KEYS, "layout split")
        _identifier(split["id"], "layout split id")
        _identifier(split["direction"], "layout split direction")
        if isinstance(split["ratio"], bool) or not isinstance(split["ratio"], (int, float)):
            raise RuntimeError("layout split ratio is not numeric")
        _rect(split["rect"], "layout split rect")
        normalized_splits.append(
            {
                "id": split["id"],
                "direction": split["direction"],
                "ratio": split["ratio"],
            }
        )
    _unique(normalized_splits, "id", "layout split")
    return {
        "workspace_id": row["workspace_id"],
        "tab_id": row["tab_id"],
        "zoomed": row["zoomed"],
        "focused_pane_id": row["focused_pane_id"],
        "panes": sorted(normalized_panes, key=lambda item: str(item["pane_id"])),
        "splits": sorted(normalized_splits, key=lambda item: str(item["id"])),
    }


def normalize(raw: Any) -> dict[str, Any]:
    envelope = _object(raw, "response")
    result = _object(envelope.get("result"), "response result")
    snapshot = _object(result.get("snapshot"), "session snapshot")
    _keys(snapshot, SESSION_KEYS, SESSION_KEYS, "session snapshot")

    workspaces = _rows(snapshot["workspaces"], "workspaces")
    workspace_by_id = _unique(workspaces, "workspace_id", "workspace")
    normalized_workspaces = []
    for row in workspaces:
        _keys(row, WORKSPACE_KEYS, WORKSPACE_REQUIRED, "workspace")
        if (
            type(row["number"]) is not int
            or type(row["pane_count"]) is not int
            or type(row["tab_count"]) is not int
            or not isinstance(row["focused"], bool)
        ):
            raise RuntimeError("workspace counters or focus are invalid")
        _identifier(row["label"], "workspace label")
        _identifier(row["active_tab_id"], "workspace active_tab_id")
        normalized_workspaces.append(
            {key: row.get(key) for key in sorted(WORKSPACE_KEYS - WORKSPACE_TRANSIENT)}
        )

    tabs = _rows(snapshot["tabs"], "tabs")
    tab_by_id = _unique(tabs, "tab_id", "tab")
    normalized_tabs = []
    for row in tabs:
        _keys(row, TAB_KEYS, TAB_KEYS, "tab")
        if (
            type(row["number"]) is not int
            or type(row["pane_count"]) is not int
            or not isinstance(row["focused"], bool)
        ):
            raise RuntimeError("tab counters or focus are invalid")
        _identifier(row["label"], "tab label")
        normalized_tabs.append(
            {key: row.get(key) for key in sorted(TAB_KEYS - TAB_TRANSIENT)}
        )

    panes = _rows(snapshot["panes"], "panes")
    pane_by_id = _unique(panes, "pane_id", "pane")
    normalized_panes = []
    for row in panes:
        _keys(row, PANE_KEYS, PANE_REQUIRED, "pane")
        terminal_id = _identifier(row["terminal_id"], "pane terminal_id")
        if not terminal_id.startswith("term_"):
            raise RuntimeError("pane terminal_id is invalid")
        if not isinstance(row["focused"], bool):
            raise RuntimeError("pane focused is not a boolean")
        _identifier(row.get("cwd"), "pane cwd")
        if "agent" in row:
            _identifier(row["agent"], "pane agent")
        if "agent_session" in row and not isinstance(row["agent_session"], dict):
            raise RuntimeError("pane agent_session is not an object")
        for key in (
            "foreground_cwd", "title", "terminal_title",
            "terminal_title_stripped", "display_agent", "agent_status",
            "input_prompt_kind",
        ):
            if key in row and not isinstance(row[key], str):
                raise RuntimeError(f"pane {key} is not a string")
        if "input_pending" in row and not isinstance(row["input_pending"], bool):
            raise RuntimeError("pane input_pending is not a boolean")
        for key in ("composer", "state_labels", "tokens", "last_completed_turn", "scroll"):
            if key in row and not isinstance(row[key], dict):
                raise RuntimeError(f"pane {key} is not an object")
        for key in ("revision", "turn", "turn_epoch"):
            if key in row and type(row[key]) is not int:
                raise RuntimeError(f"pane {key} is not an integer")
        normalized_panes.append(
            {key: row.get(key) for key in sorted(PANE_KEYS - PANE_TRANSIENT)}
        )
    _unique(panes, "terminal_id", "pane")

    layouts = [_layout(row) for row in _rows(snapshot["layouts"], "layouts")]
    layout_by_tab = _unique(layouts, "tab_id", "layout")
    if set(layout_by_tab) != set(tab_by_id):
        raise RuntimeError("layout tabs do not match session tabs")
    for tab_id, tab in tab_by_id.items():
        workspace_id = _identifier(tab.get("workspace_id"), "tab workspace_id")
        if workspace_id not in workspace_by_id:
            raise RuntimeError("tab references an unknown workspace")
        layout = layout_by_tab[tab_id]
        if layout["workspace_id"] != workspace_id:
            raise RuntimeError("layout workspace does not match tab")
        tab_panes = {
            pane_id for pane_id, pane in pane_by_id.items()
            if pane.get("tab_id") == tab_id and pane.get("workspace_id") == workspace_id
        }
        layout_panes = {pane["pane_id"] for pane in layout["panes"]}
        if tab_panes != layout_panes or layout["focused_pane_id"] not in tab_panes:
            raise RuntimeError("layout panes do not match tab panes")
        if tab.get("pane_count") != len(tab_panes):
            raise RuntimeError("tab pane count does not match panes")
    for workspace_id, workspace in workspace_by_id.items():
        workspace_tabs = {
            tab_id for tab_id, tab in tab_by_id.items()
            if tab.get("workspace_id") == workspace_id
        }
        workspace_panes = {
            pane_id for pane_id, pane in pane_by_id.items()
            if pane.get("workspace_id") == workspace_id
        }
        if (
            workspace.get("tab_count") != len(workspace_tabs)
            or workspace.get("pane_count") != len(workspace_panes)
            or workspace.get("active_tab_id") not in workspace_tabs
        ):
            raise RuntimeError("workspace counts or active tab do not match topology")
    for pane in panes:
        workspace_id = _identifier(pane.get("workspace_id"), "pane workspace_id")
        tab_id = _identifier(pane.get("tab_id"), "pane tab_id")
        if (
            workspace_id not in workspace_by_id
            or tab_id not in tab_by_id
            or tab_by_id[tab_id].get("workspace_id") != workspace_id
        ):
            raise RuntimeError("pane references an unknown workspace or tab")
    if (
        snapshot["focused_workspace_id"] not in workspace_by_id
        or snapshot["focused_tab_id"] not in tab_by_id
        or snapshot["focused_pane_id"] not in pane_by_id
    ):
        raise RuntimeError("focused session identity is not in the topology")
    focused_tab = tab_by_id[snapshot["focused_tab_id"]]
    focused_pane = pane_by_id[snapshot["focused_pane_id"]]
    if (
        focused_tab.get("workspace_id") != snapshot["focused_workspace_id"]
        or focused_pane.get("workspace_id") != snapshot["focused_workspace_id"]
        or focused_pane.get("tab_id") != snapshot["focused_tab_id"]
    ):
        raise RuntimeError("focused session identities are not structurally related")
    stable_session = {
        key: snapshot.get(key)
        for key in sorted(SESSION_KEYS - SESSION_TRANSIENT - {"workspaces", "tabs", "panes", "layouts"})
    }
    stable_session.update(
        {
            "workspaces": sorted(normalized_workspaces, key=lambda row: str(row["workspace_id"])),
            "tabs": sorted(normalized_tabs, key=lambda row: str(row["tab_id"])),
            "panes": sorted(normalized_panes, key=lambda row: str(row["pane_id"])),
            "layouts": sorted(layouts, key=lambda row: (str(row["workspace_id"]), str(row["tab_id"]))),
        }
    )
    return stable_session


def load(path: Path) -> dict[str, Any]:
    return normalize(json.loads(path.read_text(encoding="utf-8")))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("first", type=Path)
    parser.add_argument("second", type=Path)
    parser.add_argument("--session", required=True)
    parser.add_argument("--pane", required=True)
    parser.add_argument("--cwd", required=True)
    args = parser.parse_args()
    first = load(args.first)
    second = load(args.second)
    if first != second:
        raise RuntimeError("stable Herdr topology differs")
    targets = [
        row
        for row in second["panes"]
        if row.get("agent_session") == {
            "agent": "codex",
            "kind": "id",
            "source": "herdr:codex",
            "value": args.session,
        }
    ]
    if (
        len(targets) != 1
        or targets[0].get("pane_id") != args.pane
        or targets[0].get("agent") != "codex"
        or targets[0].get("cwd") != args.cwd
    ):
        raise RuntimeError("target pane identity is not exact")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
