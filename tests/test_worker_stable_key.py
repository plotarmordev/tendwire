"""Stable worker continuity from Herdr's persisted workspace/public-pane identity."""

from __future__ import annotations
from concurrent.futures import ThreadPoolExecutor

import hashlib
import hmac
import json
import os
import re
import stat
import subprocess
from copy import deepcopy
from pathlib import Path
from typing import Any

import pytest

from tendwire import worker_identity
from tendwire.backends import herdr_cli
from tendwire.backends.herdr_cli import (
    _private_identity_material_from_item,
    _worker_record_from_item,
    _workers_and_bindings_from_records,
)
from tendwire.backends.herdr_events import HerdrEventBackend
from tendwire.config import Config
from tendwire.core.models import Worker, worker_binding_private_fingerprint
from tendwire.store.sqlite import init_store, latest_snapshot, list_worker_bindings
from tendwire.worker_identity import (
    InstallationKeyError,
    load_or_create_installation_key,
    reset_installation_key,
)

_STABLE_KEY = re.compile(r"^wsk1_[0-9a-f]{64}$")
_FIXTURE_PATH = Path(__file__).parent / "fixtures" / "herdr" / "worker_identity_restore.json"


@pytest.fixture(autouse=True)
def _isolated_home(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    home = tmp_path / "unused-home"
    home.mkdir()
    monkeypatch.setenv("HOME", str(home))


def _fixture() -> dict[str, Any]:
    return json.loads(_FIXTURE_PATH.read_text(encoding="utf-8"))


def _config(data_dir: Path, *, host_id: str = "stable-host") -> Config:
    return Config(
        host_id=host_id,
        data_dir=data_dir,
        db_path=data_dir / "stable-host.db",
        herdr_backend="socket",
        herdr_timeout_seconds=0.5,
    )


def _project(
    config: Config,
    agents: list[dict[str, Any]],
    panes: list[dict[str, Any]] | None = None,
) -> tuple[HerdrEventBackend, list[Any], list[Any], list[Any]]:
    config.data_dir.mkdir(parents=True, mode=0o700, exist_ok=True)
    init_store(Path(config.db_path))
    backend = HerdrEventBackend(config, debounce_seconds=0)
    records = backend._records_from_reconcile_payloads(
        {"agents": deepcopy(agents)},
        {"panes": deepcopy(panes or [])},
    )
    workers, bindings = _workers_and_bindings_from_records(config, records)
    return backend, workers, bindings, records


def _single_worker(config: Config, item: dict[str, Any]) -> Any:
    _backend, workers, _bindings, _records = _project(config, [], [item])
    assert len(workers) == 1
    return workers[0]


def _stable(worker: Any) -> str:
    value = worker.meta.get("stable_key")
    assert isinstance(value, str)
    return value


def _mode(path: Path) -> int:
    return stat.S_IMODE(os.lstat(path).st_mode)


def _tree_snapshot(root: Path) -> dict[str, tuple[int, int, bytes | None]]:
    snapshot: dict[str, tuple[int, int, bytes | None]] = {}
    for entry in [root, *sorted(root.rglob("*"))]:
        current = os.lstat(entry)
        content = entry.read_bytes() if stat.S_ISREG(current.st_mode) else None
        snapshot[str(entry.relative_to(root))] = (
            current.st_ino,
            stat.S_IMODE(current.st_mode),
            content,
        )
    return snapshot


def _reserved_meta_keys(value: Any, *, include_root: bool = True) -> list[str]:
    found: list[str] = []
    if isinstance(value, dict):
        for key, child in value.items():
            compact = str(key).lower().replace("_", "").replace("-", "").replace(".", "")
            if include_root and compact.startswith("stablekey"):
                found.append(str(key))
            found.extend(_reserved_meta_keys(child))
    elif isinstance(value, list):
        for child in value:
            found.extend(_reserved_meta_keys(child))
    return found


def test_restore_fixture_matches_verified_herdr_contract() -> None:
    """Fixture follows authoritative Herdr commit 46174563489273199a17c982356c6e4674ef00d4."""
    fixture = _fixture()
    session = fixture["session_snapshot"]
    workspace = session["workspaces"][0]
    tab = workspace["tabs"][0]
    before = fixture["pre_restore"]
    after = fixture["post_restore"]

    assert session["version"] == 3
    assert workspace["id"] == "wR9"
    assert workspace["public_pane_numbers"] == {"41": 10, "42": 11}
    assert tab["layout"]["Split"]["first"] == {"Pane": 41}
    assert tab["layout"]["Split"]["second"] == {"Pane": 42}
    assert set(tab["panes"]) == {"41", "42"}
    assert before["pane_info"]["pane_id"] == after["pane_info"]["pane_id"] == "wR9:pA"
    assert before["sibling_pane_info"]["pane_id"] == after["sibling_pane_info"]["pane_id"] == "wR9:pB"
    for field in ("raw_pane_id", "runtime_id", "worker_id", "agent_id"):
        assert before[field] != after[field]
    for field in ("terminal_id", "agent"):
        assert before["pane_info"][field] != after["pane_info"][field]
    assert before["pane_info"]["agent_session"] != after["pane_info"]["agent_session"]
    split = fixture["split_creation"]
    assert split["event"] == split["data"]["type"] == "pane_created"
    assert split["data"]["pane"]["pane_id"] == before["sibling_pane_info"]["pane_id"]
    assert split["data"]["pane"]["workspace_id"] == workspace["id"]
    for move in (fixture["same_workspace_move"], fixture["cross_workspace_move"]):
        assert move["event"] == "pane_moved"
        assert move["data"]["type"] == "pane_moved"
        assert "pane" in move["data"]


def test_turn_observation_fields_are_byte_identical_identity_exclusions(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path / "identity-turn-exclusion")
    pane = deepcopy(_fixture()["pre_restore"]["pane_info"])
    pane["meta"] = {"provider": {"label": "stable"}}
    _backend, baseline_workers, baseline_bindings, _records = _project(
        config,
        [],
        [pane],
    )
    observed = deepcopy(pane)
    observed.update(
        {
            "turn": 41,
            "turn_epoch": 99,
            "last_completed_turn": {
                "turn": 41,
                "turn_epoch": 99,
                "completed_unix_ms": 1_700_000_000_000,
            },
            "outcome": "aborted",
            "state_change_seq": 100,
        }
    )
    observed["meta"]["provider"].update(
        {
            "turn": 41,
            "turn_epoch": 99,
            "last_completed_turn": {"turn": 41},
            "outcome": "aborted",
            "state_change_seq": 100,
        }
    )
    _backend, turn_workers, turn_bindings, _records = _project(
        config,
        [],
        [observed],
    )

    assert len(baseline_workers) == len(turn_workers) == 1
    assert json.dumps(
        baseline_workers[0].to_dict(),
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8") == json.dumps(
        turn_workers[0].to_dict(),
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    assert baseline_workers[0].fingerprint == turn_workers[0].fingerprint
    assert baseline_bindings[0].private_fingerprint == (
        turn_bindings[0].private_fingerprint
    )


def test_exact_format_version_and_domain_separated_hmac(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _fixture()
    pane = fixture["pre_restore"]["pane_info"]
    config = _config(tmp_path / "state")
    key = bytes(range(32))
    config.data_dir.mkdir(mode=0o700)
    config.installation_key_path.write_bytes(key)
    os.chmod(config.installation_key_path, 0o600)
    captured_messages: list[bytes] = []
    original_hmac_new = hmac.new

    def capture_hmac(
        hmac_key: bytes,
        message: bytes,
        digestmod: Any,
    ) -> hmac.HMAC:
        captured_messages.append(message)
        return original_hmac_new(hmac_key, message, digestmod)

    monkeypatch.setattr(worker_identity.hmac, "new", capture_hmac)
    worker = _single_worker(config, pane)
    message = (
        b'{"backend":"herdr","domain":"tendwire.worker-stable-key",'
        b'"host_id":"stable-host","pane_id":"wR9:pA","version":1,'
        b'"workspace_id":"wR9"}'
    )
    expected = "wsk1_" + original_hmac_new(key, message, hashlib.sha256).hexdigest()

    assert _stable(worker) == expected
    assert captured_messages == [message]
    assert _STABLE_KEY.fullmatch(expected)
    assert type(worker.meta["stable_key_version"]) is int
    assert worker.meta["stable_key_version"] == 1
    public_meta = worker.to_dict()["meta"]
    assert public_meta["stable_key"] == expected
    assert type(public_meta["stable_key_version"]) is int
    assert public_meta["stable_key_version"] == 1
    assert config.installation_key_marker_path.read_bytes() == hashlib.sha256(key).hexdigest().encode("ascii")


@pytest.mark.parametrize(
    ("workspace_id", "pane_id", "expected"),
    [
        ("w0", "w0:p0", ("w0", "w0:p0")),
        ("w1", "w1:p1", ("w1", "w1:p1")),
        ("wZ", "wZ:pZ", ("wZ", "wZ:pZ")),
        (
            "w65383a2e877513",
            "w65383a2e877513:p4",
            ("w65383a2e877513", "w65383a2e877513:p4"),
        ),
        (
            "w653e50b41be581",
            "w653e50b41be581:pC",
            ("w653e50b41be581", "w653e50b41be581:pC"),
        ),
        (
            "wABCDEFGHJKMNPQRSTVWXYZ0123456789",
            "wABCDEFGHJKMNPQRSTVWXYZ0123456789:"
            "p9876543210ZYXWVTSRQPNMKJHGFEDCBA",
            (
                "wABCDEFGHJKMNPQRSTVWXYZ0123456789",
                "wABCDEFGHJKMNPQRSTVWXYZ0123456789:"
                "p9876543210ZYXWVTSRQPNMKJHGFEDCBA",
            ),
        ),
        (None, "wA:pA", None),
        ("wA", None, None),
        ("", ":pA", None),
        ("w", "w:pA", None),
        ("W1", "W1:p1", None),
        ("wwA", "wwA:pA", None),
        ("wa", "wa:pA", None),
        ("w65383a2e87751", "w65383a2e87751:pA", None),
        ("w65383a2e8775133", "w65383a2e8775133:pA", None),
        ("w65383A2e877513", "w65383A2e877513:pA", None),
        ("w65383g2e877513", "w65383g2e877513:pA", None),
        ("wAa", "wAa:pA", None),
        ("wA-B", "wA-B:pA", None),
        ("wA_B", "wA_B:pA", None),
        ("wI", "wI:pA", None),
        ("wL", "wL:pA", None),
        ("wO", "wO:pA", None),
        ("wU", "wU:pA", None),
        ("wΑ", "wΑ:pA", None),
        ("wＡ", "wＡ:pA", None),
        ("wA", "wA:p", None),
        ("wA", "wA:PA", None),
        ("wA", "wA:pa", None),
        ("wA", "wA:pAa", None),
        ("wA", "wA:pA-B", None),
        ("wA", "wA:pA_B", None),
        ("wA", "wA:pI", None),
        ("wA", "wA:pL", None),
        ("wA", "wA:pO", None),
        ("wA", "wA:pU", None),
        ("wA", "wA:pΑ", None),
        ("wA", "wA:pＡ", None),
        ("wA", "wA:pA:pB", None),
        ("wA", " wA:pA", None),
        ("wA", "wA:pA ", None),
        ("wA", "wB:pA", None),
        ("wA", "wAA:pA", None),
    ],
    ids=[
        "zero-boundary",
        "one-boundary",
        "uppercase-boundary",
        "current-hex-workspace-numbered-pane",
        "current-hex-workspace-uppercase-pane",
        "full-authoritative-alphabet",
        "missing-workspace",
        "missing-pane",
        "empty-workspace",
        "empty-workspace-suffix",
        "uppercase-structural-prefix",
        "extra-workspace-prefix",
        "lowercase-workspace-suffix",
        "short-current-hex-workspace",
        "long-current-hex-workspace",
        "mixed-current-hex-workspace",
        "nonhex-current-workspace",
        "mixed-case-workspace-suffix",
        "hyphenated-workspace-suffix",
        "underscored-workspace-suffix",
        "workspace-ascii-I-confusable",
        "workspace-ascii-L-confusable",
        "workspace-ascii-O-confusable",
        "workspace-ascii-U",
        "workspace-greek-alpha-confusable",
        "workspace-fullwidth-alpha-confusable",
        "empty-pane-suffix",
        "uppercase-pane-structural-prefix",
        "lowercase-pane-suffix",
        "mixed-case-pane-suffix",
        "hyphenated-pane-suffix",
        "underscored-pane-suffix",
        "pane-ascii-I-confusable",
        "pane-ascii-L-confusable",
        "pane-ascii-O-confusable",
        "pane-ascii-U",
        "pane-greek-alpha-confusable",
        "pane-fullwidth-alpha-confusable",
        "injected-pane-structure",
        "leading-pane-whitespace",
        "trailing-pane-whitespace",
        "cross-workspace",
        "cross-workspace-prefix",
    ],
)
def test_canonical_herdr_pane_identity_uses_exact_authoritative_grammar(
    workspace_id: str | None,
    pane_id: str | None,
    expected: tuple[str, str] | None,
) -> None:
    assert worker_identity.canonical_herdr_pane_identity(workspace_id, pane_id) == expected


def test_worker_record_separates_canonical_identity_from_raw_observation() -> None:
    canonical = _worker_record_from_item(
        {
            "workspaceId": "w65383a2e877513",
            "paneId": "w65383a2e877513:pA",
            "agent": "codex",
        },
        pane_info_observed=True,
        identity_source="event:pane.updated",
    )
    assert canonical.workspace_id == "w65383a2e877513"
    assert canonical.pane_id == "w65383a2e877513:pA"
    assert canonical.observed_workspace_id == canonical.workspace_id
    assert canonical.observed_pane_id == canonical.pane_id
    assert canonical.identity_source == "event:pane.updated"
    assert canonical.pane_info_observed is True

    raw_runtime_identity = _worker_record_from_item(
        {
            "workspace_id": 7,
            "pane_id": 41,
            "agent": "codex",
        },
        pane_info_observed=True,
        identity_source="event:pane.agent_status_changed",
    )
    assert raw_runtime_identity.workspace_id is None
    assert raw_runtime_identity.pane_id is None
    assert raw_runtime_identity.observed_workspace_id == "7"
    assert raw_runtime_identity.observed_pane_id == "41"
    assert raw_runtime_identity.identity_source == "event:pane.agent_status_changed"
    assert raw_runtime_identity.pane_info_observed is True
    assert herdr_cli._stable_pane_identity(raw_runtime_identity) is None


@pytest.mark.parametrize(
    ("workspace_id", "pane_id"),
    [
        ("w0", "w0:p0"),
        ("w1", "w1:p1"),
        ("wZ", "wZ:pZ"),
        ("w65383a2e877513", "w65383a2e877513:p4"),
        ("w653e50b41be581", "w653e50b41be581:pC"),
        (
            "wABCDEFGHJKMNPQRSTVWXYZ0123456789",
            "wABCDEFGHJKMNPQRSTVWXYZ0123456789:"
            "p9876543210ZYXWVTSRQPNMKJHGFEDCBA",
        ),
    ],
)
def test_every_supported_identity_form_is_restart_stable_and_private(
    tmp_path: Path,
    workspace_id: str,
    pane_id: str,
) -> None:
    item = deepcopy(_fixture()["pre_restore"]["pane_info"])
    item["workspace_id"] = workspace_id
    item["pane_id"] = pane_id
    data_dir = tmp_path / "state"
    before = _single_worker(_config(data_dir), item)

    restarted_item = deepcopy(item)
    restarted_item["terminal_id"] = "runtime-terminal-after-restart"
    restarted_item["agent"] = "runtime-agent-after-restart"
    restarted_item["agent_session"] = {
        "source": "runtime-source-after-restart",
        "agent": "runtime-agent-after-restart",
        "kind": "id",
        "value": "runtime-session-after-restart",
    }
    after = _single_worker(_config(data_dir), restarted_item)

    assert _stable(after) == _stable(before)
    public = json.dumps(after.to_dict(), sort_keys=True)
    assert pane_id not in public
    assert restarted_item["terminal_id"] not in public
    assert restarted_item["agent_session"]["source"] not in public
    assert restarted_item["agent_session"]["value"] not in public


def test_restore_continuity_ignores_changed_runtime_terminal_agent_and_session(tmp_path: Path) -> None:
    fixture = _fixture()
    config = _config(tmp_path / "state")

    before = _single_worker(config, fixture["pre_restore"]["pane_info"])
    after = _single_worker(config, fixture["post_restore"]["pane_info"])

    assert before.id != after.id
    assert _stable(before) == _stable(after)


def test_split_panes_have_distinct_keys_and_survive_sibling_close_or_reorder(tmp_path: Path) -> None:
    fixture = _fixture()
    primary = fixture["post_restore"]["pane_info"]
    sibling = fixture["post_restore"]["sibling_pane_info"]
    config = _config(tmp_path / "state")

    _backend, first, _bindings, _records = _project(config, [], [primary, sibling])
    by_name = {worker.name: _stable(worker) for worker in first}
    assert len(set(by_name.values())) == 2

    _backend, reordered, _bindings, _records = _project(config, [], [sibling, primary])
    assert {worker.name: _stable(worker) for worker in reordered} == by_name

    primary_after_close = _single_worker(config, primary)
    assert _stable(primary_after_close) == by_name[primary["agent"]]


def test_split_creation_adds_a_distinct_restart_stable_identity(tmp_path: Path) -> None:
    fixture = _fixture()
    primary = deepcopy(fixture["post_restore"]["pane_info"])
    created = deepcopy(fixture["split_creation"]["data"]["pane"])
    config = _config(tmp_path / "state")
    backend, workers, bindings, records = _project(config, [], [primary])
    original = workers[0]
    original_key = _stable(original)
    backend._workers = {original.id: original}
    backend._bindings = {binding.private_fingerprint: binding for binding in bindings}
    backend._pane_terminals = {primary["pane_id"]: primary["terminal_id"]}
    backend._replace_ownership_maps(records, bindings)

    assert backend.queue_event_envelope(fixture["split_creation"], flush=True)

    assert len(backend._workers) == 2
    keys = {_stable(worker) for worker in backend._workers.values()}
    assert original_key in keys
    assert len(keys) == 2
    restarted_created = _single_worker(_config(config.data_dir), created)
    assert _stable(restarted_created) in keys
    public = json.dumps(
        [worker.to_dict() for worker in backend._workers.values()],
        sort_keys=True,
    )
    assert created["pane_id"] not in public
    assert created["terminal_id"] not in public


def test_same_pane_suffix_in_distinct_workspaces_has_distinct_keys(tmp_path: Path) -> None:
    first_item = deepcopy(_fixture()["pre_restore"]["pane_info"])
    first_item["workspace_id"] = "wA"
    first_item["pane_id"] = "wA:p7"
    second_item = deepcopy(first_item)
    second_item["workspace_id"] = "wB"
    second_item["pane_id"] = "wB:p7"
    config = _config(tmp_path / "state")

    first = _single_worker(config, first_item)
    second = _single_worker(config, second_item)

    assert _stable(first) != _stable(second)
    assert "wA:p7" not in json.dumps(first.to_dict(), sort_keys=True)
    assert "wB:p7" not in json.dumps(second.to_dict(), sort_keys=True)


def test_session_targeted_agent_adopts_matched_pane_identity_privately(tmp_path: Path) -> None:
    fixture = _fixture()
    pane = deepcopy(fixture["post_restore"]["pane_info"])
    agent = deepcopy(pane)
    agent["agent"] = "codex"
    agent.pop("workspace_id")
    agent.pop("pane_id")
    config = _config(tmp_path / "state")

    _backend, workers, _bindings, records = _project(config, [agent], [pane])

    assert len(records) == len(workers) == 1
    assert records[0].workspace_id == "wR9"
    assert records[0].pane_id == "wR9:pA"
    assert records[0].turn_target_kind == "codex_session_id"
    assert _STABLE_KEY.fullmatch(_stable(workers[0]))
    public = json.dumps(workers[0].to_dict(), sort_keys=True)
    assert "wR9:pA" not in public
    assert pane["terminal_id"] not in public
    assert pane["agent_session"]["value"] not in public


def test_matched_pane_overrides_conflicting_agent_continuity_and_workspace(
    tmp_path: Path,
) -> None:
    pane = deepcopy(_fixture()["post_restore"]["pane_info"])
    pane["agent"] = "codex"
    agent = {
        "worker_id": "public-conflicting-agent",
        "agent_id": "agent-send-secret",
        "terminal_id": pane["terminal_id"],
        "agent": "codex",
        "agent_status": "working",
        "agent_session": {
            "source": "conflicting-agent-source-secret",
            "agent": "codex",
            "kind": "id",
            "value": "conflicting-agent-session-secret",
        },
        "workspace_id": "wD2",
        "pane_id": "wD2:pA",
    }
    config = _config(tmp_path / "state")

    _backend, pane_workers, _bindings, pane_records = _project(config, [], [pane])
    _backend, agent_workers, _bindings, agent_records = _project(config, [agent])
    _backend, merged_workers, merged_bindings, merged_records = _project(config, [agent], [pane])

    assert len(pane_workers) == len(agent_workers) == len(merged_workers) == 1
    assert len(pane_records) == len(agent_records) == len(merged_records) == 1
    assert _stable(merged_workers[0]) == _stable(pane_workers[0])
    assert "stable_key" not in agent_workers[0].meta
    assert "stable_key_version" not in agent_workers[0].meta
    assert merged_records[0].workspace_id == pane_records[0].workspace_id == "wR9"
    assert merged_records[0].pane_id == pane_records[0].pane_id == "wR9:pA"
    assert merged_records[0].workspace_id != agent_records[0].workspace_id
    assert merged_records[0].pane_id != agent_records[0].pane_id
    assert merged_workers[0].space_id == pane_workers[0].space_id == "wR9"
    assert merged_workers[0].space_id != agent_workers[0].space_id
    assert merged_records[0].turn_target_kind == "codex_session_id"
    assert merged_records[0].turn_target_value == pane["agent_session"]["value"]
    assert merged_workers[0].backend_target == {
        "kind": "agent_id",
        "value": "agent-send-secret",
        "sendable": True,
        "reason": None,
    }
    assert len(merged_bindings) == 1
    assert merged_bindings[0].target_kind == "agent_id"
    assert merged_bindings[0].target_value == "agent-send-secret"
    assert merged_bindings[0].turn_target_kind == "codex_session_id"
    assert merged_bindings[0].turn_target_value == pane["agent_session"]["value"]

    public = json.dumps(merged_workers[0].to_dict(), sort_keys=True)
    for private_value in (
        pane["pane_id"],
        pane["terminal_id"],
        pane["agent_session"]["source"],
        pane["agent_session"]["value"],
        agent["agent_id"],
        agent["pane_id"],
        agent["workspace_id"],
        agent["agent_session"]["source"],
        agent["agent_session"]["value"],
    ):
        assert private_value not in public


@pytest.mark.parametrize(
    ("pane_workspace_id", "pane_id"),
    [
        (None, "wR9:pA"),
        ("wR9", None),
        ("wR9", "wR9:pI"),
    ],
)
def test_matched_incomplete_or_invalid_pane_suppresses_agent_identity_derivation(
    tmp_path: Path,
    pane_workspace_id: str | None,
    pane_id: str | None,
) -> None:
    pane = deepcopy(_fixture()["post_restore"]["pane_info"])
    pane["agent"] = "codex"
    if pane_workspace_id is None:
        pane.pop("workspace_id")
    else:
        pane["workspace_id"] = pane_workspace_id
    if pane_id is None:
        pane.pop("pane_id")
    else:
        pane["pane_id"] = pane_id
    agent = {
        "worker_id": "public-conflicting-agent",
        "agent_id": "agent-send-secret",
        "terminal_id": pane["terminal_id"],
        "agent": "codex",
        "agent_session": deepcopy(pane["agent_session"]),
        "workspace_id": "wD2",
        "pane_id": "wD2:pA",
    }
    config = _config(tmp_path / f"state-{pane_workspace_id}-{pane_id}")

    _backend, pane_workers, _bindings, pane_records = _project(config, [], [pane])
    _backend, agent_workers, _bindings, _agent_records = _project(config, [agent])
    _backend, merged_workers, _bindings, merged_records = _project(config, [agent], [pane])

    assert "stable_key" not in agent_workers[0].meta
    assert "stable_key_version" not in agent_workers[0].meta
    assert "stable_key" not in pane_workers[0].meta
    assert "stable_key" not in merged_workers[0].meta
    assert "stable_key_version" not in merged_workers[0].meta
    assert merged_records[0].workspace_id == pane_records[0].workspace_id
    assert merged_records[0].pane_id == pane_records[0].pane_id
    assert merged_workers[0].space_id == pane_workers[0].space_id
    assert merged_records[0].turn_target_kind == "codex_session_id"
    assert merged_records[0].turn_target_value == pane["agent_session"]["value"]
    assert merged_workers[0].backend_target is not None
    assert merged_workers[0].backend_target["kind"] == "agent_id"
    assert merged_workers[0].backend_target["value"] == "agent-send-secret"


def test_unmatched_agent_list_identity_never_authorizes_continuity(
    tmp_path: Path,
) -> None:
    agent = deepcopy(_fixture()["post_restore"]["pane_info"])
    agent["worker_id"] = "public-unmatched-agent"
    agent["agent_id"] = "unmatched-agent-target-secret"
    agent["agent"] = "codex"
    config = _config(tmp_path / "state")

    _backend, workers, bindings, records = _project(config, [agent])

    assert len(records) == len(workers) == len(bindings) == 1
    assert records[0].pane_info_observed is False
    assert "stable_key" not in workers[0].meta
    assert "stable_key_version" not in workers[0].meta
    assert not config.installation_key_path.exists()
    assert workers[0].backend_target == {
        "kind": "agent_id",
        "value": "unmatched-agent-target-secret",
        "sendable": True,
        "reason": None,
    }
    assert bindings[0].turn_target_kind == "codex_session_id"
    assert bindings[0].turn_target_value == agent["agent_session"]["value"]

    public = json.dumps(workers[0].to_dict(), sort_keys=True)
    for private_value in (
        agent["pane_id"],
        agent["terminal_id"],
        agent["agent_id"],
        agent["agent_session"]["source"],
        agent["agent_session"]["value"],
    ):
        assert private_value not in public


def test_conflicting_match_keys_across_two_panes_fail_closed(
    tmp_path: Path,
) -> None:
    fixture = _fixture()["post_restore"]
    first = deepcopy(fixture["pane_info"])
    second = deepcopy(fixture["sibling_pane_info"])
    second["agent"] = "codex"
    second["agent_session"] = {
        "source": "second-pane-source-secret",
        "agent": "codex",
        "kind": "id",
        "value": "second-pane-session-secret",
    }
    agent = {
        "worker_id": "public-ambiguous-agent",
        "agent_id": "ambiguous-agent-target-secret",
        "workspace_id": first["workspace_id"],
        "pane_id": first["pane_id"],
        "terminal_id": second["terminal_id"],
        "agent": "codex",
        "agent_session": deepcopy(second["agent_session"]),
    }
    config = _config(tmp_path / "state")

    _backend, workers, bindings, records = _project(
        config,
        [agent],
        [first, second],
    )

    assert len(records) == len(workers) == len(bindings) == 1
    assert records[0].pane_info_observed is False
    assert records[0].turn_target_kind is None
    assert records[0].turn_target_value is None
    assert "stable_key" not in workers[0].meta
    assert "stable_key_version" not in workers[0].meta
    assert workers[0].backend_target == {
        "kind": "agent_id",
        "value": "ambiguous-agent-target-secret",
        "sendable": False,
        "reason": "ambiguous_pane_match",
    }
    assert bindings[0].sendable is False
    assert bindings[0].reason == "ambiguous_pane_match"
    assert bindings[0].turn_target_kind is None
    assert bindings[0].turn_target_value is None

    public = json.dumps(workers[0].to_dict(), sort_keys=True)
    for private_value in (
        first["pane_id"],
        first["terminal_id"],
        first["agent_session"]["value"],
        second["pane_id"],
        second["terminal_id"],
        second["agent_session"]["source"],
        second["agent_session"]["value"],
        agent["agent_id"],
    ):
        assert private_value not in public


def test_two_agents_claiming_one_pane_fail_closed_independent_of_order(
    tmp_path: Path,
) -> None:
    pane = deepcopy(_fixture()["post_restore"]["pane_info"])
    pane["agent"] = "codex"
    agents = [
        {
            "worker_id": f"public-agent-{suffix}",
            "agent_id": f"agent-target-{suffix}-secret",
            "workspace_id": pane["workspace_id"],
            "pane_id": pane["pane_id"],
            "terminal_id": pane["terminal_id"],
            "agent": "codex",
            "agent_session": deepcopy(pane["agent_session"]),
        }
        for suffix in ("a", "b")
    ]
    projections: list[tuple[tuple[Any, ...], ...]] = []

    for index, ordered_agents in enumerate((agents, list(reversed(agents)))):
        config = _config(tmp_path / f"state-{index}")
        _backend, workers, bindings, records = _project(
            config,
            ordered_agents,
            [pane],
        )

        assert len(records) == len(workers) == len(bindings) == 2
        assert all(record.pane_info_observed is False for record in records)
        assert all(record.turn_target_kind is None for record in records)
        assert all(record.turn_target_value is None for record in records)
        assert all("stable_key" not in worker.meta for worker in workers)
        assert all("stable_key_version" not in worker.meta for worker in workers)
        assert all(
            worker.backend_target is not None
            and worker.backend_target["sendable"] is False
            and worker.backend_target["reason"] == "ambiguous_pane_match"
            for worker in workers
        )
        assert all(binding.sendable is False for binding in bindings)
        assert all(binding.reason == "ambiguous_pane_match" for binding in bindings)
        assert all(binding.turn_target_kind is None for binding in bindings)
        assert all(binding.turn_target_value is None for binding in bindings)
        assert not config.installation_key_path.exists()
        projections.append(
            tuple(
                sorted(
                    (
                        worker.id,
                        worker.name,
                        worker.space_id,
                        tuple(sorted((worker.backend_target or {}).items())),
                    )
                    for worker in workers
                )
            )
        )

    assert projections[0] == projections[1]


@pytest.mark.parametrize(
    "shared_agent_owner",
    ["backend_target", "turn_target", "send_token"],
)
def test_distinct_panes_with_shared_agent_owner_fail_closed_in_any_order(
    tmp_path: Path,
    shared_agent_owner: str,
) -> None:
    fixture = _fixture()["post_restore"]
    panes = [
        deepcopy(fixture["pane_info"]),
        deepcopy(fixture["sibling_pane_info"]),
    ]
    for pane in panes:
        pane["agent"] = "codex"
        pane.pop("agent_session", None)
    if shared_agent_owner == "send_token":
        panes[1]["terminal_id"] = "shared-agent-target-secret"
    agents = []
    for index, pane in enumerate(panes):
        agents.append(
            {
                "worker_id": f"public-owner-{index}",
                "agent_id": (
                    "shared-agent-target-secret"
                    if shared_agent_owner == "backend_target"
                    or (
                        shared_agent_owner == "send_token"
                        and index == 0
                    )
                    else (
                        None
                        if shared_agent_owner == "send_token"
                        else f"agent-target-{index}-secret"
                    )
                ),
                "workspace_id": pane["workspace_id"],
                "pane_id": pane["pane_id"],
                "terminal_id": pane["terminal_id"],
                "agent": "codex",
                "agent_session": {
                    "source": f"source-{index}-secret",
                    "agent": "codex",
                    "kind": "id",
                    "value": (
                        "shared-session-secret"
                        if shared_agent_owner == "turn_target"
                        else f"session-{index}-secret"
                    ),
                },
            }
        )

    projections: list[tuple[tuple[Any, ...], ...]] = []
    for order, (agent_rows, pane_rows) in enumerate(
        (
            (agents, panes),
            (list(reversed(agents)), list(reversed(panes))),
        )
    ):
        config = _config(tmp_path / f"{shared_agent_owner}-{order}")
        _backend, workers, bindings, records = _project(
            config,
            agent_rows,
            pane_rows,
        )

        assert len(records) == len(workers) == len(bindings) == 2
        assert all(record.pane_info_observed is False for record in records)
        assert all(record.turn_target_kind is None for record in records)
        assert all(record.turn_target_value is None for record in records)
        assert all("stable_key" not in worker.meta for worker in workers)
        assert all(
            worker.backend_target is not None
            and worker.backend_target["sendable"] is False
            and worker.backend_target["reason"] == "ambiguous_pane_match"
            for worker in workers
        )
        assert all(binding.sendable is False for binding in bindings)
        assert all(binding.reason == "ambiguous_pane_match" for binding in bindings)
        assert all(binding.turn_target_kind is None for binding in bindings)
        assert all(binding.turn_target_value is None for binding in bindings)
        projections.append(
            tuple(
                sorted(
                    (
                        worker.id,
                        tuple(sorted((worker.backend_target or {}).items())),
                    )
                    for worker in workers
                )
            )
        )

    assert projections[0] == projections[1]


@pytest.mark.parametrize(
    "shared_owner_key",
    ["terminal_id", "agent_session", "private_fingerprint"],
)
def test_conflicting_pane_owner_key_fails_closed_independent_of_row_order(
    tmp_path: Path,
    shared_owner_key: str,
) -> None:
    fixture = _fixture()["post_restore"]
    first = deepcopy(fixture["pane_info"])
    second = deepcopy(fixture["sibling_pane_info"])
    first["agent"] = "codex"
    second["agent"] = "omp"
    first["agent_session"] = {
        "source": "first-source-secret",
        "agent": "codex",
        "kind": "id",
        "value": "first-session-secret",
    }
    second["agent_session"] = {
        "source": "second-source-secret",
        "agent": "omp",
        "kind": "id",
        "value": "second-session-secret",
    }
    if shared_owner_key == "terminal_id":
        first["terminal_id"] = second["terminal_id"] = "shared-terminal-secret"
    elif shared_owner_key == "agent_session":
        second["agent_session"]["value"] = first["agent_session"]["value"]
    else:
        first.pop("agent_session")
        second.pop("agent_session")
        first["agent_id"] = second["agent_id"] = "shared-agent-target-secret"

    projections: list[tuple[tuple[Any, ...], ...]] = []
    for index, panes in enumerate(([first, second], [second, first])):
        config = _config(tmp_path / f"{shared_owner_key}-{index}")
        _backend, workers, bindings, records = _project(config, [], panes)

        assert len(records) == len(workers) == len(bindings) == 2
        assert all(record.pane_info_observed is False for record in records)
        assert all(record.turn_target_kind is None for record in records)
        assert all(record.turn_target_value is None for record in records)
        assert all("stable_key" not in worker.meta for worker in workers)
        assert all("stable_key_version" not in worker.meta for worker in workers)
        assert all(
            worker.backend_target is not None
            and worker.backend_target["sendable"] is False
            and worker.backend_target["reason"] == "ambiguous_pane_match"
            for worker in workers
        )
        assert all(binding.sendable is False for binding in bindings)
        assert all(binding.reason == "ambiguous_pane_match" for binding in bindings)
        assert all(binding.turn_target_kind is None for binding in bindings)
        assert all(binding.turn_target_value is None for binding in bindings)
        assert not config.installation_key_path.exists()
        projections.append(
            tuple(
                sorted(
                    (
                        worker.id,
                        worker.name,
                        worker.space_id,
                        tuple(sorted((worker.backend_target or {}).items())),
                    )
                    for worker in workers
                )
            )
        )

    assert projections[0] == projections[1]


def test_unmatched_agent_send_token_colliding_with_pane_fails_closed(
    tmp_path: Path,
) -> None:
    pane = deepcopy(_fixture()["post_restore"]["pane_info"])
    pane["terminal_id"] = "shared-send-token-secret"
    agent = {
        "worker_id": "public-unmatched-agent",
        "agent_id": "shared-send-token-secret",
        "workspace_id": "wD2",
        "agent": "other-agent",
    }
    config = _config(tmp_path / "state")

    _backend, workers, bindings, records = _project(
        config,
        [agent],
        [pane],
    )

    assert len(records) == len(workers) == len(bindings) == 2
    assert all(record.pane_info_observed is False for record in records)
    assert all(record.turn_target_kind is None for record in records)
    assert all(record.turn_target_value is None for record in records)
    assert all(
        worker.backend_target is not None
        and worker.backend_target["sendable"] is False
        and worker.backend_target["reason"] == "ambiguous_pane_match"
        for worker in workers
    )
    assert all(binding.sendable is False for binding in bindings)
    assert all(binding.reason == "ambiguous_pane_match" for binding in bindings)
    assert not config.installation_key_path.exists()


def test_exact_duplicate_pane_rows_collapse_without_losing_continuity(
    tmp_path: Path,
) -> None:
    pane = deepcopy(_fixture()["post_restore"]["pane_info"])
    config = _config(tmp_path / "state")

    _backend, workers, bindings, records = _project(config, [], [pane, deepcopy(pane)])

    assert len(records) == len(workers) == len(bindings) == 1
    assert records[0].pane_info_observed is True
    assert _STABLE_KEY.fullmatch(_stable(workers[0]))
    assert bindings[0].sendable is True


def test_matched_pane_replaces_conflicting_pane_scoped_targets(
    tmp_path: Path,
) -> None:
    pane = deepcopy(_fixture()["post_restore"]["pane_info"])
    pane["agent"] = "codex"
    agent = {
        "worker_id": "public-pane-target-conflict",
        "workspace_id": "wD2",
        "pane_id": "wD2:pA",
        "agent": "other-agent",
        "agent_session": deepcopy(pane["agent_session"]),
    }
    config = _config(tmp_path / "state")

    _backend, workers, bindings, records = _project(config, [agent], [pane])

    assert len(records) == len(workers) == len(bindings) == 1
    assert records[0].pane_info_observed is True
    assert _STABLE_KEY.fullmatch(_stable(workers[0]))
    assert records[0].workspace_id == pane["workspace_id"]
    assert records[0].pane_id == pane["pane_id"]
    assert records[0].terminal_id == pane["terminal_id"]
    assert workers[0].space_id == pane["workspace_id"]
    assert workers[0].backend_target == {
        "kind": "terminal_id",
        "value": pane["terminal_id"],
        "sendable": True,
        "reason": None,
    }
    assert bindings[0].target_kind == "terminal_id"
    assert bindings[0].target_value == pane["terminal_id"]
    assert bindings[0].turn_target_kind == "codex_session_id"
    assert bindings[0].turn_target_value == pane["agent_session"]["value"]
    assert agent["pane_id"] not in {
        bindings[0].target_value,
        bindings[0].turn_target_value,
    }

    public = json.dumps(workers[0].to_dict(), sort_keys=True)
    for private_value in (
        pane["pane_id"],
        pane["terminal_id"],
        pane["agent_session"]["source"],
        pane["agent_session"]["value"],
        agent["pane_id"],
    ):
        assert private_value not in public


def test_cli_agent_success_always_enriches_identity_from_matching_pane(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pane = deepcopy(_fixture()["post_restore"]["pane_info"])
    pane["agent"] = "codex"
    agent = {
        "terminal_id": pane["terminal_id"],
        "agent": "codex",
        "agent_status": "working",
        "agent_session": deepcopy(pane["agent_session"]),
    }
    responses = {
        ("workspace", "list"): {"result": {"workspaces": []}},
        ("agent", "list"): {"result": {"agents": [agent]}},
        ("pane", "list"): {"result": {"panes": [pane]}},
    }
    calls: list[tuple[str, ...]] = []

    def fake_run(args: Any, config: Config) -> subprocess.CompletedProcess[str]:
        del config
        calls.append(tuple(args))
        response = responses.get(tuple(args))
        return subprocess.CompletedProcess(
            args=list(args),
            returncode=0 if response is not None else 1,
            stdout=json.dumps(response) if response is not None else "",
            stderr="",
        )

    monkeypatch.setattr(herdr_cli.shutil, "which", lambda _binary: "/usr/bin/herdr")
    monkeypatch.setattr(herdr_cli, "_run_herdr", fake_run)

    _spaces, workers = herdr_cli.fetch_herdr_state(_config(tmp_path / "state"))

    assert calls == [
        ("workspace", "list"),
        ("agent", "list"),
        ("pane", "list"),
    ]
    assert len(workers) == 1
    assert _STABLE_KEY.fullmatch(_stable(workers[0]))


@pytest.mark.parametrize(
    ("workspace_id", "pane_id"),
    [
        (None, "wR9:pA"),
        ("wR9", None),
        ("wR9", "wOther:pA"),
        ("wR9", "wR9:pI"),
        ("wR9", "wR9:pa"),
        ("wR9", "wR9:p"),
        ("wR9", "wR9:A"),
        ("not-herdr-id", "not-herdr-id:pA"),
        ("wI", "wI:pA"),
    ],
)
def test_identity_requires_both_canonical_membership_and_herdr_alphabet(
    tmp_path: Path,
    workspace_id: str | None,
    pane_id: str | None,
) -> None:
    item = deepcopy(_fixture()["pre_restore"]["pane_info"])
    item["terminal_id"] = "durable-terminal-is-not-a-fallback"
    if workspace_id is None:
        item.pop("workspace_id", None)
    else:
        item["workspace_id"] = workspace_id
    if pane_id is None:
        item.pop("pane_id", None)
    else:
        item["pane_id"] = pane_id

    worker = _single_worker(_config(tmp_path / "state"), item)

    assert "stable_key" not in worker.meta
    assert "stable_key_version" not in worker.meta


def test_reconcile_and_identical_events_share_one_canonical_worker_fingerprint(
    tmp_path: Path,
) -> None:
    pane = deepcopy(_fixture()["pre_restore"]["pane_info"])
    config = _config(tmp_path / "state", host_id="fingerprint-stability")
    config.data_dir.mkdir(parents=True, mode=0o700)
    init_store(Path(config.db_path))

    class PaneClient:
        def workspace_list(self, **_kwargs: Any) -> dict[str, Any]:
            return {"workspaces": [{"id": pane["workspace_id"], "name": "Continuity"}]}

        def tab_list(self, **_kwargs: Any) -> dict[str, Any]:
            return {"tabs": []}

        def pane_list(self, **_kwargs: Any) -> dict[str, Any]:
            return {"panes": [deepcopy(pane)]}

        def agent_list(self, **_kwargs: Any) -> dict[str, Any]:
            return {"agents": []}

    backend = HerdrEventBackend(config, debounce_seconds=0)
    client = PaneClient()
    phases: list[tuple[Any, Any]] = []

    def capture() -> None:
        snapshot = latest_snapshot(backend.db_path, backend.config.host_id)
        bindings = list_worker_bindings(backend.db_path, backend.config.host_id, backend="herdr")
        assert snapshot is not None
        assert len(snapshot.workers) == len(bindings) == 1
        phases.append((snapshot.workers[0], bindings[0]))

    backend.reconcile_once(client=client)
    capture()
    envelope = {"event": "pane.focused", "data": {"pane": deepcopy(pane)}}
    assert backend.queue_event_envelope(envelope) is True
    capture()
    backend.reconcile_once(client=client)
    capture()
    assert backend.queue_event_envelope(envelope) is True
    capture()

    workers = [worker for worker, _binding in phases]
    bindings = [binding for _worker, binding in phases]
    assert len({worker.id for worker in workers}) == 1
    assert len({_stable(worker) for worker in workers}) == 1
    assert len({worker.fingerprint for worker in workers}) == 1
    assert len({binding.worker_fingerprint for binding in bindings}) == 1
    assert all(binding.worker_id == worker.id for worker, binding in phases)
    assert all(binding.worker_fingerprint == worker.fingerprint for worker, binding in phases)
    assert len(
        {
            (
                binding.target_kind,
                binding.target_value,
                binding.turn_target_kind,
                binding.turn_target_value,
                binding.private_fingerprint,
                binding.sendable,
                binding.reason,
            )
            for binding in bindings
        }
    ) == 1

    worker = workers[0]
    canonical = Worker(
        id=worker.id,
        name=worker.name,
        status=worker.status,
        space_id=worker.space_id,
        meta=worker.meta,
        last_seen_at=worker.last_seen_at,
        summary=worker.summary,
    )
    pre_key_meta = {
        key: value
        for key, value in worker.meta.items()
        if key not in {"stable_key", "stable_key_version"}
    }
    pre_key = Worker(
        id=worker.id,
        name=worker.name,
        status=worker.status,
        space_id=worker.space_id,
        meta=pre_key_meta,
        last_seen_at=worker.last_seen_at,
        summary=worker.summary,
    )
    assert worker.fingerprint == canonical.fingerprint
    assert worker.fingerprint != pre_key.fingerprint


def test_same_workspace_move_preserves_and_cross_workspace_move_changes_key(tmp_path: Path) -> None:
    fixture = _fixture()
    config = _config(tmp_path / "state")
    initial = fixture["pre_restore"]["pane_info"]
    backend, workers, bindings, _records = _project(config, [initial], [initial])
    original = workers[0]
    backend._workers = {original.id: original}
    backend._bindings = {binding.private_fingerprint: binding for binding in bindings}
    backend._pane_terminals = {initial["pane_id"]: initial["terminal_id"]}

    assert backend.queue_event_envelope(fixture["same_workspace_move"], flush=True)
    same_workspace = backend._workers[original.id]
    assert _stable(same_workspace) == _stable(original)

    assert backend.queue_event_envelope(fixture["cross_workspace_move"], flush=True)
    cross_workspace = backend._workers[original.id]
    assert _stable(cross_workspace) != _stable(original)
    expected = _single_worker(
        config,
        fixture["cross_workspace_move"]["data"]["pane"],
    )
    assert _stable(cross_workspace) == _stable(expected)
    assert cross_workspace.space_id == "wD2"


def test_compatibility_only_partial_move_preserves_authenticated_local_key(
    tmp_path: Path,
) -> None:
    initial = deepcopy(_fixture()["pre_restore"]["pane_info"])
    config = _config(tmp_path / "state")
    backend, workers, bindings, _records = _project(config, [initial], [initial])
    original = workers[0]
    original_key = _stable(original)
    backend._workers = {original.id: original}
    backend._bindings = {binding.private_fingerprint: binding for binding in bindings}
    backend._pane_terminals = {initial["pane_id"]: initial["terminal_id"]}

    assert backend.queue_event_envelope(
        {
            "event": "pane.moved",
            "payload": {
                "previous_pane_id": initial["pane_id"],
                "new_pane_id": "wR9:pB",
            },
        },
        flush=True,
    )

    moved = backend._workers[original.id]
    moved_binding = next(iter(backend._bindings.values()))
    assert _stable(moved) == original_key
    assert moved_binding.target_kind == "pane_id"
    assert moved_binding.target_value == "wR9:pB"


def test_complete_authoritative_move_retains_state_when_rederivation_fails(
    tmp_path: Path,
) -> None:
    initial = deepcopy(_fixture()["pre_restore"]["pane_info"])
    config = _config(tmp_path / "state")
    backend, workers, bindings, _records = _project(config, [initial], [initial])
    original = workers[0]
    original_binding = bindings[0]
    backend._workers = {original.id: original}
    backend._bindings = {binding.private_fingerprint: binding for binding in bindings}
    backend._pane_terminals = {initial["pane_id"]: initial["terminal_id"]}
    replacement = bytes(
        byte ^ 0xFF for byte in config.installation_key_path.read_bytes()
    )
    config.installation_key_path.write_bytes(replacement)
    os.chmod(config.installation_key_path, 0o600)
    moved_pane = deepcopy(initial)
    moved_pane["workspace_id"] = "wD2"
    moved_pane["pane_id"] = "wD2:p7"
    moved_pane["terminal_id"] = "terminal-moved-secret"

    assert backend.queue_event_envelope(
        {
            "event": "pane.moved",
            "data": {
                "previous_pane_id": initial["pane_id"],
                "pane": moved_pane,
            },
        },
        flush=True,
    )

    assert backend._workers[original.id] == original
    assert next(iter(backend._bindings.values())) == original_binding
    assert backend.health.status == "degraded"
    assert backend.health.outcome == "continuity_unavailable"
    diagnostic = json.dumps(backend.health.to_backend_health().to_dict(), sort_keys=True)
    for private_value in (
        initial["pane_id"],
        initial["terminal_id"],
        moved_pane["pane_id"],
        moved_pane["terminal_id"],
        moved_pane["agent_session"]["source"],
        moved_pane["agent_session"]["value"],
    ):
        assert private_value not in diagnostic


def test_partial_event_preserves_local_key_and_authoritative_failure_retains_state(
    tmp_path: Path,
) -> None:
    initial = deepcopy(_fixture()["pre_restore"]["pane_info"])
    config = _config(tmp_path / "state")
    backend, workers, bindings, _records = _project(config, [initial], [initial])
    original = workers[0]
    original_key = _stable(original)
    backend._workers = {original.id: original}
    backend._bindings = {binding.private_fingerprint: binding for binding in bindings}
    backend._pane_terminals = {initial["pane_id"]: initial["terminal_id"]}

    replacement = bytes(byte ^ 0xFF for byte in config.installation_key_path.read_bytes())
    config.installation_key_path.write_bytes(replacement)
    os.chmod(config.installation_key_path, 0o600)

    assert backend.queue_event_envelope(
        {
            "event": "pane_agent_status_changed",
            "data": {"agent": initial["agent"], "status": "blocked"},
        },
        flush=True,
    )
    partial = backend._workers[original.id]
    assert _stable(partial) == original_key

    assert backend.queue_event_envelope(
        {
            "event": "pane_agent_status_changed",
            "data": {"pane_id": initial["pane_id"], "status": "working"},
        },
        flush=True,
    )
    pane_only = backend._workers[original.id]
    binding_before_failure = next(iter(backend._bindings.values()))
    assert _stable(pane_only) == original_key

    full = deepcopy(initial)
    full["agent_status"] = "idle"
    assert backend.queue_event_envelope(
        {
            "event": "pane_agent_status_changed",
            "data": {"pane": full},
        },
        flush=True,
    )

    assert backend._workers[original.id] == pane_only
    assert next(iter(backend._bindings.values())) == binding_before_failure
    assert backend.health.status == "degraded"
    assert backend.health.outcome == "continuity_unavailable"


@pytest.mark.parametrize(
    (
        "observed_workspace_id",
        "observed_pane_id",
    ),
    [
        ("wR9", "wR9:pA"),
        ("wR9", "wR9:pB"),
        ("wD2", "wR9:pA"),
    ],
)
def test_key_loss_rejects_every_authoritative_pane_update(
    tmp_path: Path,
    observed_workspace_id: str,
    observed_pane_id: str,
) -> None:
    initial = deepcopy(_fixture()["pre_restore"]["pane_info"])
    config = _config(tmp_path / "state")
    backend, workers, bindings, records = _project(config, [initial], [initial])
    original = workers[0]
    original_binding = bindings[0]
    backend._workers = {original.id: original}
    backend._bindings = {
        binding.private_fingerprint: binding
        for binding in bindings
    }
    backend._pane_terminals = {
        initial["pane_id"]: initial["terminal_id"],
    }
    backend._replace_ownership_maps(records, bindings)
    config.installation_key_marker_path.unlink()

    observed = deepcopy(initial)
    observed["pane_id"] = observed_pane_id
    observed["workspace_id"] = observed_workspace_id
    observed["agent"] = "codex-runtime-after-key-loss"
    assert backend.queue_event_envelope(
        {
            "event": "pane.created",
            "data": {"pane": observed},
        },
        flush=True,
    )

    assert set(backend._workers) == {original.id}
    current = backend._workers[original.id]
    current_binding = next(iter(backend._bindings.values()))
    assert current == original
    assert current_binding == original_binding
    assert _stable(current) == _stable(original)
    assert backend.health.status == "degraded"
    assert backend.health.outcome == "continuity_unavailable"
    assert backend._pane_terminals == {
        initial["pane_id"]: initial["terminal_id"],
    }
    assert backend._pane_owners == {
        initial["pane_id"]: {original.id},
    }


def test_installations_are_unlinkable_and_host_is_part_of_message(tmp_path: Path) -> None:
    pane = _fixture()["pre_restore"]["pane_info"]
    first = _single_worker(_config(tmp_path / "one"), pane)
    second = _single_worker(_config(tmp_path / "two"), pane)
    other_host = _single_worker(_config(tmp_path / "one", host_id="other-host"), pane)

    assert _stable(first) != _stable(second)
    assert _stable(first) != _stable(other_host)


def test_first_bootstrap_publishes_key_marker_and_initialization_sentinel(
    tmp_path: Path,
) -> None:
    data_dir = tmp_path / "state"
    candidate = bytes(range(32))
    generated_sizes: list[int] = []

    def generated(size: int) -> bytes:
        generated_sizes.append(size)
        return candidate

    assert load_or_create_installation_key(data_dir, random_bytes=generated) == candidate
    assert generated_sizes == [32]
    assert (data_dir / "installation.key").read_bytes() == candidate
    assert (data_dir / "installation.key.sha256").read_bytes() == hashlib.sha256(
        candidate,
    ).hexdigest().encode("ascii")
    assert (data_dir / "installation.key.initialized").read_bytes() == b"1"


def test_valid_pre_sentinel_pair_upgrades_only_after_validation(tmp_path: Path) -> None:
    data_dir = tmp_path / "state"
    data_dir.mkdir(mode=0o700)
    key = b"p" * 32
    (data_dir / "installation.key").write_bytes(key)
    (data_dir / "installation.key.sha256").write_bytes(
        hashlib.sha256(key).hexdigest().encode("ascii"),
    )
    os.chmod(data_dir / "installation.key", 0o600)
    os.chmod(data_dir / "installation.key.sha256", 0o600)
    generated_sizes: list[int] = []

    def generated(size: int) -> bytes:
        generated_sizes.append(size)
        return b"n" * size

    assert load_or_create_installation_key(data_dir, random_bytes=generated) == key
    assert generated_sizes == []
    assert (data_dir / "installation.key.initialized").read_bytes() == b"1"


def test_complete_initialized_pair_loss_fails_without_replacement(tmp_path: Path) -> None:
    data_dir = tmp_path / "state"
    original = load_or_create_installation_key(
        data_dir,
        random_bytes=lambda size: b"a" * size,
    )
    (data_dir / "installation.key").unlink()
    (data_dir / "installation.key.sha256").unlink()
    generated_sizes: list[int] = []

    def generated(size: int) -> bytes:
        generated_sizes.append(size)
        return b"b" * size

    with pytest.raises(InstallationKeyError, match="installation identity is unavailable"):
        load_or_create_installation_key(data_dir, random_bytes=generated)

    assert original == b"a" * 32
    assert generated_sizes == []
    assert not (data_dir / "installation.key").exists()
    assert not (data_dir / "installation.key.sha256").exists()
    assert (data_dir / "installation.key.initialized").read_bytes() == b"1"


def test_explicit_acknowledged_reset_allows_offline_key_rotation(tmp_path: Path) -> None:
    data_dir = tmp_path / "state"
    original = load_or_create_installation_key(
        data_dir,
        random_bytes=lambda size: b"a" * size,
    )

    with pytest.raises(InstallationKeyError, match="reset was not acknowledged"):
        reset_installation_key(data_dir, acknowledge_continuity_break=False)
    assert (data_dir / "installation.key").read_bytes() == original

    reset_installation_key(data_dir, acknowledge_continuity_break=True)
    assert not (data_dir / "installation.key").exists()
    assert not (data_dir / "installation.key.sha256").exists()
    assert not (data_dir / "installation.key.initialized").exists()

    rotated = load_or_create_installation_key(
        data_dir,
        random_bytes=lambda size: b"b" * size,
    )
    assert rotated == b"b" * 32
    assert rotated != original
    assert (data_dir / "installation.key.initialized").read_bytes() == b"1"


def test_initialized_digest_loss_fails_without_rewriting_or_randomness(tmp_path: Path) -> None:
    data_dir = tmp_path / "state"
    key = load_or_create_installation_key(
        data_dir,
        random_bytes=lambda size: b"k" * size,
    )
    marker_path = data_dir / "installation.key.sha256"
    marker_path.unlink()
    key_path = data_dir / "installation.key"
    sentinel_path = data_dir / "installation.key.initialized"
    key_stat = os.lstat(key_path)
    sentinel_stat = os.lstat(sentinel_path)
    generated_sizes: list[int] = []

    def generated(size: int) -> bytes:
        generated_sizes.append(size)
        return b"n" * size

    with pytest.raises(InstallationKeyError, match="installation identity is unavailable"):
        load_or_create_installation_key(data_dir, random_bytes=generated)

    assert generated_sizes == []
    assert key_path.read_bytes() == key
    assert (os.lstat(key_path).st_ino, os.lstat(key_path).st_mtime_ns) == (
        key_stat.st_ino,
        key_stat.st_mtime_ns,
    )
    assert not marker_path.exists()
    assert sentinel_path.read_bytes() == b"1"
    assert (os.lstat(sentinel_path).st_ino, os.lstat(sentinel_path).st_mtime_ns) == (
        sentinel_stat.st_ino,
        sentinel_stat.st_mtime_ns,
    )


def test_initialized_replaced_key_and_digest_loss_fails_without_rewriting_or_randomness(
    tmp_path: Path,
) -> None:
    data_dir = tmp_path / "state"
    original = load_or_create_installation_key(
        data_dir,
        random_bytes=lambda size: b"a" * size,
    )
    key_path = data_dir / "installation.key"
    marker_path = data_dir / "installation.key.sha256"
    sentinel_path = data_dir / "installation.key.initialized"
    replacement = bytes(byte ^ 0xFF for byte in original)
    key_path.write_bytes(replacement)
    marker_path.unlink()
    key_stat = os.lstat(key_path)
    sentinel_stat = os.lstat(sentinel_path)
    generated_sizes: list[int] = []

    def generated(size: int) -> bytes:
        generated_sizes.append(size)
        return b"n" * size

    with pytest.raises(InstallationKeyError, match="installation identity is unavailable"):
        load_or_create_installation_key(data_dir, random_bytes=generated)

    assert generated_sizes == []
    assert key_path.read_bytes() == replacement
    assert (os.lstat(key_path).st_ino, os.lstat(key_path).st_mtime_ns) == (
        key_stat.st_ino,
        key_stat.st_mtime_ns,
    )
    assert not marker_path.exists()
    assert sentinel_path.read_bytes() == b"1"
    assert (os.lstat(sentinel_path).st_ino, os.lstat(sentinel_path).st_mtime_ns) == (
        sentinel_stat.st_ino,
        sentinel_stat.st_mtime_ns,
    )


def test_missing_key_with_marker_fails_closed_without_source_fallback(tmp_path: Path) -> None:
    item = deepcopy(_fixture()["pre_restore"]["pane_info"])
    item["meta"] = {"stableKey": "source-fallback", "stable-key-version": 999}
    config = _config(tmp_path / "state")
    first = _single_worker(config, item)
    assert _stable(first)
    config.installation_key_path.unlink()

    after_loss = _single_worker(config, item)

    assert "stable_key" not in after_loss.meta
    assert "stable_key_version" not in after_loss.meta
    assert "source-fallback" not in json.dumps(after_loss.to_dict())


def test_replaced_key_or_marker_mismatch_fails_closed(tmp_path: Path) -> None:
    pane = _fixture()["pre_restore"]["pane_info"]
    config = _config(tmp_path / "state")
    assert _stable(_single_worker(config, pane))
    original_key = config.installation_key_path.read_bytes()
    replacement = bytes(byte ^ 0xFF for byte in original_key)
    config.installation_key_path.write_bytes(replacement)
    os.chmod(config.installation_key_path, 0o600)

    rotated = _single_worker(config, pane)
    assert "stable_key" not in rotated.meta
    assert "stable_key_version" not in rotated.meta

    config.installation_key_path.write_bytes(original_key)
    config.installation_key_marker_path.write_bytes(b"0" * 64)
    mismatched_marker = _single_worker(config, pane)
    assert "stable_key" not in mismatched_marker.meta


def test_exact_key_and_marker_content_is_required(tmp_path: Path) -> None:
    pane = _fixture()["pre_restore"]["pane_info"]

    short_config = _config(tmp_path / "short")
    short_config.data_dir.mkdir(mode=0o700)
    short_config.installation_key_path.write_bytes(b"x" * 31)
    os.chmod(short_config.installation_key_path, 0o600)
    assert "stable_key" not in _single_worker(short_config, pane).meta
    assert not short_config.installation_key_marker_path.exists()

    marker_config = _config(tmp_path / "marker")
    marker_config.data_dir.mkdir(mode=0o700)
    key = b"k" * 32
    marker_config.installation_key_path.write_bytes(key)
    marker_config.installation_key_marker_path.write_bytes(hashlib.sha256(key).hexdigest().encode("ascii") + b"\n")
    os.chmod(marker_config.installation_key_path, 0o600)
    os.chmod(marker_config.installation_key_marker_path, 0o600)
    assert "stable_key" not in _single_worker(marker_config, pane).meta
    assert not marker_config.installation_key_sentinel_path.exists()


def test_permissive_umask_still_creates_private_modes(tmp_path: Path) -> None:
    pane = _fixture()["pre_restore"]["pane_info"]
    config = _config(tmp_path / "state")
    previous_umask = os.umask(0)
    try:
        assert _stable(_single_worker(config, pane))
    finally:
        os.umask(previous_umask)

    assert _mode(config.data_dir) == 0o700
    assert _mode(config.installation_key_path) == 0o600
    assert _mode(config.installation_key_marker_path) == 0o600
    assert _mode(config.installation_key_sentinel_path) == 0o600


def test_existing_broad_identity_modes_are_narrowed_in_place_idempotently(
    tmp_path: Path,
) -> None:
    data_dir = tmp_path / "state"
    data_dir.mkdir(mode=0o700)
    key = b"m" * 32
    paths = {
        "installation.key": data_dir / "installation.key",
        "installation.key.sha256": data_dir / "installation.key.sha256",
        "installation.key.initialized": data_dir / "installation.key.initialized",
    }
    paths["installation.key"].write_bytes(key)
    paths["installation.key.sha256"].write_bytes(
        hashlib.sha256(key).hexdigest().encode("ascii"),
    )
    paths["installation.key.initialized"].write_bytes(b"1")
    os.chmod(data_dir, 0o755)
    for path in paths.values():
        os.chmod(path, 0o644)

    data_dir_inode = os.lstat(data_dir).st_ino
    before = {
        name: (path.read_bytes(), os.lstat(path).st_ino, os.lstat(path).st_mtime_ns)
        for name, path in paths.items()
    }
    generated_sizes: list[int] = []

    def generated(size: int) -> bytes:
        generated_sizes.append(size)
        return b"n" * size

    assert load_or_create_installation_key(data_dir, random_bytes=generated) == key
    assert generated_sizes == []
    assert (os.lstat(data_dir).st_ino, _mode(data_dir)) == (data_dir_inode, 0o700)
    assert {
        name: (path.read_bytes(), os.lstat(path).st_ino, os.lstat(path).st_mtime_ns)
        for name, path in paths.items()
    } == before
    assert {_mode(path) for path in paths.values()} == {0o600}

    repaired = {
        name: (path.read_bytes(), os.lstat(path).st_ino, os.lstat(path).st_mtime_ns)
        for name, path in paths.items()
    }
    assert load_or_create_installation_key(data_dir, random_bytes=generated) == key
    assert generated_sizes == []
    assert (os.lstat(data_dir).st_ino, _mode(data_dir)) == (data_dir_inode, 0o700)
    assert {
        name: (path.read_bytes(), os.lstat(path).st_ino, os.lstat(path).st_mtime_ns)
        for name, path in paths.items()
    } == repaired
    assert {_mode(path) for path in paths.values()} == {0o600}


def test_equal_or_stricter_private_identity_modes_are_accepted_without_widening(
    tmp_path: Path,
) -> None:
    data_dir = tmp_path / "strict"
    data_dir.mkdir(mode=0o700)
    key = b"m" * 32
    marker = hashlib.sha256(key).hexdigest().encode("ascii")
    (data_dir / "installation.key").write_bytes(key)
    (data_dir / "installation.key.sha256").write_bytes(marker)
    (data_dir / "installation.key.initialized").write_bytes(b"1")
    os.chmod(data_dir / "installation.key", 0o400)
    os.chmod(data_dir / "installation.key.sha256", 0o400)
    os.chmod(data_dir / "installation.key.initialized", 0o400)
    os.chmod(data_dir, 0o500)
    generated_sizes: list[int] = []

    assert load_or_create_installation_key(
        data_dir,
        random_bytes=lambda size: generated_sizes.append(size) or b"n" * size,
    ) == key
    assert generated_sizes == []
    assert _mode(data_dir) == 0o500
    assert _mode(data_dir / "installation.key") == 0o400
    assert _mode(data_dir / "installation.key.sha256") == 0o400
    assert _mode(data_dir / "installation.key.initialized") == 0o400


def test_symlink_identity_files_and_data_dir_fail_closed(tmp_path: Path) -> None:
    pane = _fixture()["pre_restore"]["pane_info"]

    key_link_config = _config(tmp_path / "key-link")
    key_link_config.data_dir.mkdir(mode=0o700)
    outside_key = tmp_path / "outside.key"
    outside_key.write_bytes(b"z" * 32)
    key_link_config.installation_key_path.symlink_to(outside_key)
    assert "stable_key" not in _single_worker(key_link_config, pane).meta
    assert not key_link_config.installation_key_marker_path.exists()

    sentinel_link_config = _config(tmp_path / "sentinel-link")
    sentinel_link_config.data_dir.mkdir(mode=0o700)
    key = b"s" * 32
    sentinel_link_config.installation_key_path.write_bytes(key)
    sentinel_link_config.installation_key_marker_path.write_bytes(
        hashlib.sha256(key).hexdigest().encode("ascii"),
    )
    outside_sentinel = tmp_path / "outside.initialized"
    outside_sentinel.write_bytes(b"1")
    sentinel_link_config.installation_key_sentinel_path.symlink_to(outside_sentinel)
    assert "stable_key" not in _single_worker(sentinel_link_config, pane).meta


    real_dir = tmp_path / "real-dir"
    real_dir.mkdir(mode=0o700)
    linked_dir = tmp_path / "linked-dir"
    linked_dir.symlink_to(real_dir, target_is_directory=True)
    with pytest.raises(InstallationKeyError) as raised:
        load_or_create_installation_key(linked_dir)
    assert str(raised.value) == "installation identity is unavailable"


@pytest.mark.parametrize("operation", ["load-or-create", "acknowledged-reset"])
def test_identity_lifecycle_stays_on_pinned_data_directory_after_leaf_replacement(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    operation: str,
) -> None:
    configured_parent = tmp_path / "configured"
    configured_parent.mkdir()
    data_dir = configured_parent / "state"
    if operation == "acknowledged-reset":
        assert load_or_create_installation_key(
            data_dir,
            random_bytes=lambda size: b"o" * size,
        ) == b"o" * 32

    replacement_target = tmp_path / "replacement-target"
    replacement_target.mkdir(mode=0o750)
    guard = replacement_target / "target.sentinel"
    guard.write_bytes(b"must remain unchanged")
    os.chmod(guard, 0o640)
    target_before = _tree_snapshot(replacement_target)
    detached_data_dir = configured_parent / "detached-state"
    original_prepare = (
        worker_identity.local_state.prepare_and_open_private_directory
    )
    prepare_calls = 0

    def prepare_then_replace(
        path: str | os.PathLike[str],
    ) -> tuple[int, Any]:
        nonlocal prepare_calls
        prepare_calls += 1
        fd, result = original_prepare(path)
        Path(path).rename(detached_data_dir)
        Path(path).symlink_to(replacement_target, target_is_directory=True)
        return fd, result

    monkeypatch.setattr(
        worker_identity.local_state,
        "prepare_and_open_private_directory",
        prepare_then_replace,
    )

    if operation == "load-or-create":
        candidate = b"c" * 32
        assert load_or_create_installation_key(
            data_dir,
            random_bytes=lambda size: candidate[:size],
        ) == candidate
        assert (detached_data_dir / "installation.key").read_bytes() == candidate
        assert (
            detached_data_dir / "installation.key.sha256"
        ).read_bytes() == hashlib.sha256(candidate).hexdigest().encode("ascii")
        assert (
            detached_data_dir / "installation.key.initialized"
        ).read_bytes() == b"1"
    else:
        reset_installation_key(
            data_dir,
            acknowledge_continuity_break=True,
        )
        assert list(detached_data_dir.iterdir()) == []

    assert prepare_calls == 1
    assert data_dir.is_symlink()
    assert _tree_snapshot(replacement_target) == target_before


@pytest.mark.parametrize(
    "relative",
    [
        pytest.param(False, id="absolute"),
        pytest.param(True, id="relative"),
    ],
)
def test_intermediate_symlink_above_identity_data_dir_blocks_load_or_create(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    relative: bool,
) -> None:
    protected_target = tmp_path / "protected-target"
    protected_target.mkdir(mode=0o750)
    guard = protected_target / "target.sentinel"
    guard.write_bytes(b"must remain unchanged")
    os.chmod(guard, 0o640)

    configured_root = tmp_path / "configured"
    configured_root.mkdir()
    (configured_root / "redirect").symlink_to(
        protected_target,
        target_is_directory=True,
    )
    absolute_data_dir = configured_root / "redirect" / "one" / "two" / "state"
    data_dir = (
        Path("configured") / "redirect" / "one" / "two" / "state"
        if relative
        else absolute_data_dir
    )
    if relative:
        monkeypatch.chdir(tmp_path)

    before = _tree_snapshot(protected_target)
    generated_sizes: list[int] = []
    with pytest.raises(InstallationKeyError) as raised:
        load_or_create_installation_key(
            data_dir,
            random_bytes=lambda size: generated_sizes.append(size) or b"x" * size,
        )

    assert str(raised.value) == "installation identity is unavailable"
    assert "configured" not in str(raised.value)
    assert "protected-target" not in str(raised.value)
    assert generated_sizes == []
    assert _tree_snapshot(protected_target) == before


@pytest.mark.parametrize(
    "relative",
    [
        pytest.param(False, id="absolute"),
        pytest.param(True, id="relative"),
    ],
)
def test_intermediate_symlink_above_identity_data_dir_blocks_acknowledged_reset(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    relative: bool,
) -> None:
    protected_target = tmp_path / "protected-target"
    protected_target.mkdir(mode=0o750)
    guard = protected_target / "target.sentinel"
    guard.write_bytes(b"must remain unchanged")
    os.chmod(guard, 0o640)
    target_data_dir = protected_target / "one" / "two" / "state"
    key = load_or_create_installation_key(
        target_data_dir,
        random_bytes=lambda size: b"r" * size,
    )
    assert key == b"r" * 32
    preserved_temp = target_data_dir / ".tendwire-preserved.tmp"
    preserved_temp.write_bytes(b"must not be removed")
    os.chmod(preserved_temp, 0o640)
    os.chmod(target_data_dir, 0o755)
    for name in (
        "installation.key",
        "installation.key.sha256",
        "installation.key.initialized",
    ):
        os.chmod(target_data_dir / name, 0o644)

    configured_root = tmp_path / "configured"
    configured_root.mkdir()
    (configured_root / "redirect").symlink_to(
        protected_target,
        target_is_directory=True,
    )
    absolute_data_dir = configured_root / "redirect" / "one" / "two" / "state"
    data_dir = (
        Path("configured") / "redirect" / "one" / "two" / "state"
        if relative
        else absolute_data_dir
    )
    if relative:
        monkeypatch.chdir(tmp_path)

    before = _tree_snapshot(protected_target)
    with pytest.raises(InstallationKeyError) as raised:
        reset_installation_key(
            data_dir,
            acknowledge_continuity_break=True,
        )

    assert str(raised.value) == "installation identity is unavailable"
    assert "configured" not in str(raised.value)
    assert "protected-target" not in str(raised.value)
    assert _tree_snapshot(protected_target) == before


@pytest.mark.parametrize(
    "relative",
    [
        pytest.param(False, id="absolute"),
        pytest.param(True, id="relative"),
    ],
)
def test_multi_level_missing_parent_identity_bootstrap_uses_resolved_directories(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    relative: bool,
) -> None:
    absolute_data_dir = tmp_path / "bootstrap" / "one" / "two" / "state"
    data_dir = (
        Path("bootstrap") / "one" / "two" / "state"
        if relative
        else absolute_data_dir
    )
    if relative:
        monkeypatch.chdir(tmp_path)
    candidate = bytes(range(32))

    assert load_or_create_installation_key(
        data_dir,
        random_bytes=lambda size: candidate[:size],
    ) == candidate

    for directory in (
        tmp_path / "bootstrap",
        tmp_path / "bootstrap" / "one",
        tmp_path / "bootstrap" / "one" / "two",
        absolute_data_dir,
    ):
        assert directory.is_dir()
        assert _mode(directory) == 0o700
    assert (absolute_data_dir / "installation.key").read_bytes() == candidate
    assert (absolute_data_dir / "installation.key.sha256").read_bytes() == (
        hashlib.sha256(candidate).hexdigest().encode("ascii")
    )
    assert (absolute_data_dir / "installation.key.initialized").read_bytes() == b"1"
    assert {
        _mode(absolute_data_dir / name)
        for name in (
            "installation.key",
            "installation.key.sha256",
            "installation.key.initialized",
        )
    } == {0o600}
    assert list(absolute_data_dir.glob(".tendwire-*.tmp")) == []


@pytest.mark.parametrize(
    "target_name",
    [
        pytest.param(None, id="state-directory"),
        pytest.param("installation.key", id="key"),
        pytest.param("installation.key.sha256", id="digest"),
        pytest.param("installation.key.initialized", id="sentinel"),
    ],
)
def test_nonregular_identity_entries_fail_closed_without_randomness(
    tmp_path: Path,
    target_name: str | None,
) -> None:
    data_dir = tmp_path / "state"
    target = data_dir
    if target_name is None:
        data_dir.write_bytes(b"not a directory")
    else:
        data_dir.mkdir(mode=0o700)
        key = b"r" * 32
        if target_name != "installation.key":
            (data_dir / "installation.key").write_bytes(key)
            os.chmod(data_dir / "installation.key", 0o600)
        if target_name == "installation.key.initialized":
            (data_dir / "installation.key.sha256").write_bytes(
                hashlib.sha256(key).hexdigest().encode("ascii"),
            )
            os.chmod(data_dir / "installation.key.sha256", 0o600)
        target = data_dir / target_name
        target.mkdir()
    generated_sizes: list[int] = []

    with pytest.raises(InstallationKeyError) as raised:
        load_or_create_installation_key(
            data_dir,
            random_bytes=lambda size: generated_sizes.append(size) or b"n" * size,
        )

    assert str(raised.value) == "installation identity is unavailable"
    assert generated_sizes == []
    if target_name is None:
        assert target.read_bytes() == b"not a directory"
    else:
        assert target.is_dir()


def test_wrong_owner_identity_directory_fails_closed_without_changes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_dir = tmp_path / "state"
    key = load_or_create_installation_key(
        data_dir,
        random_bytes=lambda size: b"o" * size,
    )
    paths = (
        data_dir / "installation.key",
        data_dir / "installation.key.sha256",
        data_dir / "installation.key.initialized",
    )
    before = tuple(
        (path.read_bytes(), os.lstat(path).st_ino, os.lstat(path).st_mtime_ns)
        for path in paths
    )
    actual_uid = os.geteuid()
    monkeypatch.setattr(
        worker_identity.local_state.os,
        "geteuid",
        lambda: actual_uid + 1,
    )
    generated_sizes: list[int] = []

    with pytest.raises(InstallationKeyError) as raised:
        load_or_create_installation_key(
            data_dir,
            random_bytes=lambda size: generated_sizes.append(size) or b"n" * size,
        )

    assert str(raised.value) == "installation identity is unavailable"
    assert generated_sizes == []
    assert tuple(
        (path.read_bytes(), os.lstat(path).st_ino, os.lstat(path).st_mtime_ns)
        for path in paths
    ) == before
    assert paths[0].read_bytes() == key


def test_source_stable_key_family_is_recursively_stripped_then_replaced(tmp_path: Path) -> None:
    item = deepcopy(_fixture()["pre_restore"]["pane_info"])
    item["Stable.Key.Future"] = "outer-injection"
    item["meta"] = {
        "stable_key": "snake-injection",
        "StableKeyVersion": 999,
        "stable-key-rotation": "kebab-injection",
        "sTaBlEkEyFuture": "camel-injection",
        "safe": {"stable.key.next": "nested-injection", "kept": "yes"},
        "items": [{"STABLE_KEY_NEXT": "list-injection", "kept": 1}],
    }

    worker = _single_worker(_config(tmp_path / "state"), item)

    assert _STABLE_KEY.fullmatch(_stable(worker))
    assert worker.meta["stable_key_version"] == 1
    assert worker.meta["safe"] == {"kept": "yes"}
    assert worker.meta["items"] == [{"kept": 1}]
    assert set(_reserved_meta_keys(worker.meta)) == {"stable_key", "stable_key_version"}
    public = json.dumps(worker.to_dict())
    for sentinel in (
        "outer-injection",
        "snake-injection",
        "kebab-injection",
        "camel-injection",
        "nested-injection",
        "list-injection",
    ):
        assert sentinel not in public


def test_source_stable_key_injection_without_identity_is_never_preserved(tmp_path: Path) -> None:
    item = deepcopy(_fixture()["pre_restore"]["pane_info"])
    item.pop("workspace_id")
    item["stableKey"] = "top-source"
    item["meta"] = {
        "STABLE-KEY-VERSION": 22,
        "nested": {"stable.key.future": "nested-source", "safe": True},
    }

    worker = _single_worker(_config(tmp_path / "state"), item)

    assert _reserved_meta_keys(worker.meta) == []
    assert worker.meta["nested"] == {"safe": True}
    assert "source" not in json.dumps(worker.to_dict())


def test_key_load_occurs_once_for_a_worker_batch(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    fixture = _fixture()
    calls = 0
    original = herdr_cli.load_or_create_installation_key

    def counted(data_dir: Path) -> bytes:
        nonlocal calls
        calls += 1
        return original(data_dir)

    monkeypatch.setattr(herdr_cli, "load_or_create_installation_key", counted)
    config = _config(tmp_path / "state")
    _backend, workers, _bindings, _records = _project(
        config,
        [],
        [fixture["pre_restore"]["pane_info"], fixture["pre_restore"]["sibling_pane_info"]],
    )

    assert len(workers) == 2
    assert calls == 1


def test_atomic_publication_never_exposes_partial_final_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_dir = tmp_path / "state"
    original_write_all = worker_identity.local_state._write_all

    def interrupted_write(fd: int, content: bytes) -> None:
        del fd, content
        raise OSError("simulated interrupted write")

    monkeypatch.setattr(worker_identity.local_state, "_write_all", interrupted_write)
    with pytest.raises(InstallationKeyError, match="installation identity is unavailable"):
        load_or_create_installation_key(data_dir)

    assert not (data_dir / "installation.key").exists()
    assert not (data_dir / "installation.key.sha256").exists()
    assert not (data_dir / "installation.key.initialized").exists()
    assert list(data_dir.glob(".tendwire-*.tmp")) == []

    monkeypatch.setattr(worker_identity.local_state, "_write_all", original_write_all)
    assert len(load_or_create_installation_key(data_dir)) == 32
    assert (data_dir / "installation.key.initialized").read_bytes() == b"1"


def test_interrupted_sentinel_publication_recovers_without_key_rotation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_dir = tmp_path / "state"
    candidate = b"c" * 32
    original_write_all = worker_identity.local_state._write_all

    def interrupt_sentinel(fd: int, content: bytes) -> None:
        if content == b"1":
            raise OSError("simulated interrupted sentinel write")
        original_write_all(fd, content)

    monkeypatch.setattr(worker_identity.local_state, "_write_all", interrupt_sentinel)
    with pytest.raises(InstallationKeyError, match="installation identity is unavailable"):
        load_or_create_installation_key(
            data_dir,
            random_bytes=lambda size: candidate,
        )

    assert (data_dir / "installation.key").read_bytes() == candidate
    assert (data_dir / "installation.key.sha256").read_bytes() == hashlib.sha256(
        candidate,
    ).hexdigest().encode("ascii")
    assert not (data_dir / "installation.key.initialized").exists()
    assert list(data_dir.glob(".tendwire-*.tmp")) == []

    monkeypatch.setattr(worker_identity.local_state, "_write_all", original_write_all)
    recovered = load_or_create_installation_key(
        data_dir,
        random_bytes=lambda size: b"replacement-that-must-not-be-used"[:size],
    )
    assert recovered == candidate
    assert (data_dir / "installation.key.initialized").read_bytes() == b"1"


def test_concurrent_creators_publish_one_complete_key_marker_and_sentinel(
    tmp_path: Path,
) -> None:
    data_dir = tmp_path / "nested" / "state"
    with ThreadPoolExecutor(max_workers=8) as executor:
        keys = list(executor.map(lambda _index: load_or_create_installation_key(data_dir), range(16)))

    assert len(set(keys)) == 1
    key = keys[0]
    assert (data_dir / "installation.key").read_bytes() == key
    assert (data_dir / "installation.key.sha256").read_bytes() == hashlib.sha256(key).hexdigest().encode("ascii")
    assert (data_dir / "installation.key.initialized").read_bytes() == b"1"
    assert list(data_dir.glob(".tendwire-*.tmp")) == []


def test_public_output_excludes_private_identity_and_binding_fingerprint(tmp_path: Path) -> None:
    fixture = _fixture()
    before = fixture["pre_restore"]
    item = before["pane_info"]
    config = _config(tmp_path / "state")
    installation_key = b"0123456789abcdef0123456789abcdef"
    config.data_dir.mkdir(mode=0o700)
    config.installation_key_path.write_bytes(installation_key)
    os.chmod(config.installation_key_path, 0o600)
    backend, workers, _bindings, records = _project(config, [], [item])
    del backend
    worker = workers[0]
    record = records[0]
    expected_private = worker_binding_private_fingerprint(
        host_id=config.host_id,
        backend="herdr",
        identity_material=_private_identity_material_from_item(item),
    )
    public = json.dumps(worker.to_dict(), sort_keys=True)

    assert record.private_fingerprint == expected_private
    assert record.private_fingerprint != _stable(worker)
    assert record.private_fingerprint not in public
    assert installation_key.decode("ascii") not in public
    assert installation_key.hex() not in public
    assert item["pane_id"] not in public
    assert item["terminal_id"] not in public
    assert item["agent_session"]["source"] not in public
    assert item["agent_session"]["value"] not in public
    for field in ("runtime_id", "worker_id", "agent_id"):
        assert str(before[field]) not in public


def test_stable_derivation_does_not_use_binding_or_public_fingerprint_helpers(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from tendwire.core.models import Worker

    record = herdr_cli._WorkerRecord(
        worker=Worker(id="public", name="Public", status="working", space_id="wR9"),
        private_fingerprint="existing-private-fingerprint",
        workspace_id="wR9",
        pane_id="wR9:pA",
        pane_info_observed=True,
    )

    def forbidden(*args: Any, **kwargs: Any) -> str:
        raise AssertionError("unrelated fingerprint helper was called")

    monkeypatch.setattr(herdr_cli, "worker_binding_private_fingerprint", forbidden)
    monkeypatch.setattr(herdr_cli, "stable_fingerprint", forbidden)
    workers, bindings = _workers_and_bindings_from_records(_config(tmp_path / "state"), [record])

    assert bindings == []
    assert _STABLE_KEY.fullmatch(_stable(workers[0]))
