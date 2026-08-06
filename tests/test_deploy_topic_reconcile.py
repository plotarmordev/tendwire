from __future__ import annotations

import hashlib
import importlib.util
import json
import sys
import types
from contextlib import contextmanager
from pathlib import Path

import pytest


SCRIPT = Path(__file__).parents[1] / ".deploy/reconcile-frozen-telegram-topics.py"


def _load_tool():
    spec = importlib.util.spec_from_file_location("reconcile_frozen_topics_test", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _private_write(path: Path, body: bytes) -> None:
    path.write_bytes(body)
    path.chmod(0o600)


@pytest.fixture
def topic_tool(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    module = _load_tool()
    evidence = tmp_path / "frozen-transaction"
    evidence.mkdir(mode=0o700)
    paths = {
        "PHASE_PATH": evidence / "phase",
        "PLAN_PATH": evidence / "telegram-topic-reset-plan.json",
        "RESET_EVIDENCE_PATH": evidence / "telegram-topic-reset-evidence.json",
        "RESET_STARTED_PATH": evidence / "telegram-topic-reset-started.json",
        "PRESENTER_EVIDENCE_PATH": evidence / "telegram-topic-presenter-evidence.json",
        "GATEWAY_EVIDENCE_PATH": evidence / "telegram-fresh-cursor-evidence.json",
        "STATE_PATH": tmp_path / "candidate-state.json",
        "INGRESS_PATH": tmp_path / "candidate-ingress.db",
    }
    for name, value in paths.items():
        monkeypatch.setattr(module, name, value)
    monkeypatch.setattr(module, "EVIDENCE_ROOT", evidence)
    _private_write(paths["PHASE_PATH"], b"committing\n")
    plan = {
        "schema_version": 1,
        "operation": "reset_all_non_general_topics",
        "chat_id": 100,
        "inventory_topic_ids": [1, 2, 3],
        "reset_topic_ids": [2, 3],
    }
    raw = module.canonical(plan)
    _private_write(paths["PLAN_PATH"], raw)
    return module, paths, raw


class _DeleteTopicHistoryRequest:
    def __init__(self, *, peer: object, top_msg_id: int) -> None:
        self.peer = peer
        self.top_msg_id = top_msg_id


class _Client:
    def __init__(self) -> None:
        self.deleted: list[int] = []

    def iter_dialogs(self):
        return [types.SimpleNamespace(id=100, input_entity="forum-peer")]

    def __call__(self, request: _DeleteTopicHistoryRequest):
        self.deleted.append(request.top_msg_id)
        return object()


def _install_telethon_delete(monkeypatch: pytest.MonkeyPatch) -> None:
    telethon = types.ModuleType("telethon")
    tl = types.ModuleType("telethon.tl")
    functions = types.ModuleType("telethon.tl.functions")
    messages = types.ModuleType("telethon.tl.functions.messages")
    messages.DeleteTopicHistoryRequest = _DeleteTopicHistoryRequest
    monkeypatch.setitem(sys.modules, "telethon", telethon)
    monkeypatch.setitem(sys.modules, "telethon.tl", tl)
    monkeypatch.setitem(sys.modules, "telethon.tl.functions", functions)
    monkeypatch.setitem(sys.modules, "telethon.tl.functions.messages", messages)


def _started_evidence(module, raw: bytes) -> dict[str, object]:
    return {
        "schema_version": 1,
        "operation": "reset_all_non_general_topics",
        "plan_sha256": hashlib.sha256(raw).hexdigest(),
        "reset_count": 2,
    }


def _final_evidence(raw: bytes) -> dict[str, object]:
    digest = hashlib.sha256(raw).hexdigest()
    return {
        "schema_version": 1,
        "operation": "reset_all_non_general_topics",
        "plan_sha256": digest,
        "plan_bytes_sha256": digest,
        "topics_before": 3,
        "topics_reset": 2,
        "topics_after": 1,
        "general_preserved": True,
    }


def test_partial_deletion_resumes_idempotently_and_final_resume_is_noop(
    topic_tool, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
) -> None:
    module, paths, raw = topic_tool
    _install_telethon_delete(monkeypatch)
    module.atomic_private_json(paths["RESET_STARTED_PATH"], _started_evidence(module, raw))
    client = _Client()

    @contextmanager
    def authorized():
        yield client

    inventories = iter(({1, 3}, {1}))
    monkeypatch.setattr(module, "authorized_client", authorized)
    monkeypatch.setattr(module, "require_topic_admin", lambda *_args: None)
    monkeypatch.setattr(module, "forum_topics", lambda *_args: next(inventories))
    monkeypatch.setattr(module, "services_stopped", lambda: None)
    monkeypatch.setattr(module.time, "sleep", lambda _seconds: None)

    assert module.apply_reset() == 0
    first = json.loads(capsys.readouterr().out)
    assert first["topics_reset"] == 2
    assert first.get("resumed") is None
    assert client.deleted == [3]
    assert paths["RESET_EVIDENCE_PATH"].is_file()

    @contextmanager
    def resumed_authorized():
        yield client

    monkeypatch.setattr(module, "authorized_client", resumed_authorized)
    monkeypatch.setattr(module, "forum_topics", lambda *_args: {1})
    monkeypatch.setattr(module, "services_stopped", lambda: None)
    assert module.apply_reset() == 0
    resumed = json.loads(capsys.readouterr().out)
    assert resumed["resumed"] is True
    assert client.deleted == [3]


def test_resumed_reset_still_requires_writers_stopped(
    topic_tool, monkeypatch: pytest.MonkeyPatch,
) -> None:
    module, paths, raw = topic_tool
    _install_telethon_delete(monkeypatch)
    module.atomic_private_json(paths["RESET_EVIDENCE_PATH"], _final_evidence(raw))
    monkeypatch.setattr(
        module,
        "services_stopped",
        lambda: (_ for _ in ()).throw(RuntimeError("Telegram writer is still active")),
    )

    with pytest.raises(RuntimeError, match="writer is still active"):
        module.apply_reset()
    assert paths["RESET_EVIDENCE_PATH"].exists()


def test_resumed_reset_rejects_prestart_forum_drift(
    topic_tool, monkeypatch: pytest.MonkeyPatch,
) -> None:
    module, paths, raw = topic_tool
    _install_telethon_delete(monkeypatch)
    module.atomic_private_json(paths["RESET_EVIDENCE_PATH"], _final_evidence(raw))
    client = _Client()

    @contextmanager
    def authorized():
        yield client

    monkeypatch.setattr(module, "authorized_client", authorized)
    monkeypatch.setattr(module, "require_topic_admin", lambda *_args: None)
    monkeypatch.setattr(module, "forum_topics", lambda *_args: {1, 3})
    monkeypatch.setattr(module, "services_stopped", lambda: None)

    with pytest.raises(RuntimeError, match="drifted before presenter start"):
        module.apply_reset()
    assert client.deleted == []
    assert paths["RESET_EVIDENCE_PATH"].exists()
