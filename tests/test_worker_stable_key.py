from __future__ import annotations

import json
import re
import threading
from dataclasses import replace

import pytest

from tendwire.backends.acp_coordinator import (
    AcpCoordinatorError,
    AcpSupervisor,
    _discovered_spaces,
    _discovered_workers,
)
from tendwire.config import Config
from tendwire.core.models import Snapshot, WorkerBinding
from tendwire.store.projection import (
    latest_snapshot,
    list_worker_bindings,
    save_snapshot,
    upsert_worker_bindings,
)
from tendwire.store.schema import init_store


OBSERVED_AT = "2026-01-01T00:00:00+00:00"


def _config(tmp_path) -> Config:
    return Config(host_id="host", data_dir=tmp_path, db_path=tmp_path / "db.sqlite")


def _discover(tmp_path, panes, agents=()):
    workers, bindings, _omissions = _discovered_workers(
        _config(tmp_path),
        {"panes": panes},
        {"agents": list(agents)},
        OBSERVED_AT,
    )
    return workers, bindings


def _pane(**updates):
    pane = {
        "workspace_id": "wR9",
        "pane_id": "wR9:pA",
        "terminal_id": "term-one",
        "agent": "codex",
        "label": "Review",
    }
    pane.update(updates)
    return pane


def test_discovery_derives_opaque_restart_stable_key_across_rename_and_reorder(tmp_path) -> None:
    pane = _pane()
    workers, bindings = _discover(tmp_path, [pane])
    first_key = workers[0].meta["stable_key"]
    renamed = {**pane, "agent": "renamed", "label": "Renamed"}
    reordered_workers, _ = _discover(
        tmp_path, [_pane(pane_id="wR9:pB", terminal_id="term-two"), renamed]
    )
    renamed_worker = next(worker for worker in reordered_workers if worker.meta["stable_key"] == first_key)
    assert renamed_worker.name == "renamed"
    assert re.fullmatch(r"wsk1_[0-9a-f]{64}", first_key)
    assert bindings[0].target_value == "term-one"
    assert "term-one" not in json.dumps(workers[0].to_dict())


@pytest.mark.parametrize("match_mode", ["multi_key", "same_pane"])
@pytest.mark.parametrize("reverse", [False, True])
def test_conflicting_agent_matches_are_non_sendable_in_any_row_order(
    tmp_path, match_mode, reverse
) -> None:
    second_match = (
        {"terminal_id": "term-one"}
        if match_mode == "multi_key"
        else {"pane_id": "wR9:pA"}
    )
    agents = [
        {"agent_id": "by-pane", "pane_id": "wR9:pA", "name": "one"},
        {"agent_id": "second", "name": "two", **second_match},
    ]
    if reverse:
        agents.reverse()
    workers, bindings = _discover(tmp_path, [_pane()], agents)
    assert workers == []
    assert bindings == []


def test_duplicate_private_identity_fences_public_and_private_targets(tmp_path) -> None:
    workers, bindings = _discover(tmp_path, [_pane(), _pane()])
    assert workers == []
    assert bindings == []


def test_duplicate_send_token_is_fenced_even_when_private_identities_differ(tmp_path) -> None:
    panes = [
        _pane(pane_id="wR9:pA", terminal_id="same", agent="A"),
        _pane(pane_id="wR9:pB", terminal_id="same", agent="B"),
    ]
    workers, bindings = _discover(tmp_path, panes)
    assert workers == []
    assert bindings == []


def test_duplicate_canonical_pane_identity_is_omitted_before_snapshot_save(tmp_path) -> None:
    panes = [
        _pane(terminal_id="first", agent="A"),
        _pane(terminal_id="second", agent="B"),
    ]
    workers, bindings, omissions = _discovered_workers(
        _config(tmp_path), {"panes": panes}, {"agents": []}, OBSERVED_AT
    )
    assert workers == []
    assert bindings == []
    assert omissions == 2


def test_missing_and_malformed_pane_identity_fail_closed(tmp_path) -> None:
    workers, bindings = _discover(
        tmp_path,
        [
            {"workspace_id": "bad", "pane_id": "pane", "terminal_id": "term", "agent": "bad"},
            {"workspace_id": "wR9", "agent": "missing"},
        ],
    )
    assert workers == []
    assert bindings == []


def test_irrelevant_shell_pane_does_not_degrade_valid_agent_discovery(tmp_path) -> None:
    shell = {
        "workspace_id": "wR9",
        "pane_id": "wR9:shell",
        "terminal_id": "term-shell",
    }
    workers, bindings, omissions = _discovered_workers(
        _config(tmp_path),
        {"panes": [_pane(), shell]},
        {"agents": []},
        OBSERVED_AT,
    )
    assert len(workers) == len(bindings) == 1
    assert workers[0].name == "codex"
    assert omissions == 0


def test_closed_and_unknown_statuses_are_projected_canonically(tmp_path) -> None:
    panes = [
        _pane(pane_id="wR9:pA", terminal_id="one", agent="A", status="closed"),
        _pane(pane_id="wR9:pB", terminal_id="two", agent="B", status="mystery"),
    ]
    workers, _ = _discover(tmp_path, panes)
    assert {worker.name: worker.status for worker in workers} == {"A": "closed", "B": "unknown"}


def test_authoritative_empty_discovery_expires_prior_binding(tmp_path) -> None:
    config = _config(tmp_path)
    assert config.db_path is not None
    init_store(config.db_path)
    workers, bindings = _discover(tmp_path, [_pane()])
    save_snapshot(
        config.db_path,
        Snapshot(host_id=config.host_id, updated_at=OBSERVED_AT, workers=workers),
    )
    upsert_worker_bindings(config.db_path, bindings)

    class EmptyLifecycleClient:
        def workspace_list(self, *, timeout): return []
        def pane_list(self, *, timeout): return []
        def agent_list(self, *, timeout): return []
        def close(self): return None

    supervisor = AcpSupervisor(
        config,
        threading.Event(),
        endpoint_client_factory=lambda _config: object(),
        discovery_client_factory=lambda _config: EmptyLifecycleClient(),
        connection_factory=lambda *_args, **_kwargs: object(),
    )
    supervisor._discover_continuity()
    snapshot = latest_snapshot(config.db_path, config.host_id)
    assert snapshot is not None and snapshot.workers == []
    assert list_worker_bindings(config.db_path, config.host_id, backend="herdr") == []


def test_discovery_omissions_publish_degraded_health_and_fail_required_start(tmp_path) -> None:
    config = _config(tmp_path)
    assert config.db_path is not None
    init_store(config.db_path)

    class MalformedLifecycleClient:
        def workspace_list(self, *, timeout): return [{"id": "wR9"}]
        def pane_list(self, *, timeout): return [_pane(pane_id="not-canonical")]
        def agent_list(self, *, timeout): return []
        def close(self): return None

    supervisor = AcpSupervisor(
        config,
        threading.Event(),
        endpoint_client_factory=lambda _config: object(),
        discovery_client_factory=lambda _config: MalformedLifecycleClient(),
        connection_factory=lambda *_args, **_kwargs: object(),
    )
    with pytest.raises(AcpCoordinatorError, match="failed to attach"):
        supervisor.start()
    snapshot = latest_snapshot(config.db_path, config.host_id)
    assert snapshot is not None
    health = snapshot.backend_health[0]
    assert health.status == "degraded"
    assert health.outcome == "continuity_unavailable"
    assert health.message == "1 Herdr panes omitted"


def test_supervisor_reconcile_reuses_exact_binding_then_rebinds_target_churn(tmp_path) -> None:
    config = _config(tmp_path)
    assert config.db_path is not None
    init_store(config.db_path)

    class MutableLifecycleClient:
        pane = _pane(agent="original", terminal_id="term-one")

        def workspace_list(self, *, timeout): return [{"id": "wR9"}]
        def pane_list(self, *, timeout): return [dict(self.pane)]
        def agent_list(self, *, timeout): return []
        def close(self): return None

    client = MutableLifecycleClient()
    supervisor = AcpSupervisor(
        config,
        threading.Event(),
        endpoint_client_factory=lambda _config: object(),
        discovery_client_factory=lambda _config: client,
        connection_factory=lambda *_args, **_kwargs: object(),
    )
    supervisor._discover_continuity()
    first = latest_snapshot(config.db_path, config.host_id)
    assert first is not None
    first_worker_id = first.workers[0].id

    first_stable_key = first.workers[0].meta["stable_key"]
    first_route_generation = first.workers[0].meta["route_generation"]
    client.pane = _pane(agent="renamed", terminal_id="term-one")
    supervisor._discover_continuity()
    second = latest_snapshot(config.db_path, config.host_id)
    assert second is not None and second.workers[0].id == first_worker_id

    client.pane = _pane(agent="renamed-again", terminal_id="term-two")
    supervisor._discover_continuity()
    third = latest_snapshot(config.db_path, config.host_id)
    bindings = list_worker_bindings(config.db_path, config.host_id, backend="herdr")
    assert third is not None
    assert third.workers[0].meta["stable_key"] == first_stable_key
    assert third.workers[0].meta["route_generation"] != first_route_generation
    assert bindings[0].worker_id == third.workers[0].id
    assert bindings[0].target_value == "term-two"
    retained = list_worker_bindings(
        config.db_path, config.host_id, backend="herdr", include_expired=True
    )
    assert {binding.private_fingerprint for binding in retained} >= {
        binding.private_fingerprint for binding in bindings
    }
    assert len({binding.private_fingerprint for binding in retained}) >= 2


def test_public_projection_contains_no_raw_private_identifiers(tmp_path) -> None:
    workers, _ = _discover(
        tmp_path,
        [_pane(cwd="/secret/private", terminal_id="term-private")],
        [{"agent_id": "agent-private", "pane_id": "wR9:pA", "name": "codex"}],
    )
    encoded = json.dumps(workers[0].to_dict(), sort_keys=True)
    assert "term-private" not in encoded
    assert "agent-private" not in encoded
    assert "/secret/private" not in encoded
