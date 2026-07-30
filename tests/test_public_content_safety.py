"""Table-driven value safety contract shared by every public surface."""

from __future__ import annotations

import json
import sqlite3
from types import SimpleNamespace
from typing import Any

import pytest
from tendwire.cli import cmd_snapshot
from tendwire.config import Config
from tendwire.connectors import ConnectorOutboxAPI
from tendwire.core.attention import attention_payload_from_snapshot

from tendwire.core.models import (
    AttentionSignal,
    Snapshot,
    Space,
    SuggestedAction,
    Worker,
    sanitize_canonical_turn_text,
    public_json_dumps,
    sanitize_public_text,
    sanitize_public_value,
)
from tendwire.core.turns import (
    InteractionChoice,
    PendingInteraction,
    Turn,
    segment_canonical_text,
    payload_to_json,
    pending_payload_from_snapshot,
    turns_payload_from_snapshot,
)
from tendwire.daemon_api import error_response, success_response
from tendwire.store.sqlite import (
    SnapshotObservationContext,
    attention_payload_from_store,
    init_store,
    get_turn_content,
    merge_turn_content,
    save_snapshot,
    tail_event_metadata,
    turns_payload_from_store,
)


def _sentinel_corpus() -> dict[str, str]:
    """Build private/provider-shaped values without storing real secret literals."""
    openai_key = "sk-" + "proj-" + "PUBLICSAFETY" + "1234567890"
    github_key = "gh" + "p_" + "PUBLICSAFETY" + "1234567890"
    google_key = "AI" + "za" + "PUBLICSAFETY" + "1234567890abcdef"
    aws_key = "AK" + "IA" + "PUBLICSAFETY12"
    slack_key = "xo" + "xb-" + "PUBLIC-SAFETY-1234567890"
    gitlab_key = "glpat-" + "PUBLICSAFETY1234567890"
    npm_key = "npm_" + "PUBLICSAFETY1234567890ABCDEF"
    pypi_key = "pypi-" + "PUBLICSAFETY1234567890ABCDEF"
    jwt = ".".join(
        (
            "eyJ" + "hbGciOiJIUzI1NiJ9",
            "eyJ" + "zdWIiOiJwdWJsaWMtc2FmZXR5In0",
            "PUBLICSAFETYSIGNATURE",
        )
    )
    bearer = "Bear" + "er " + "publicSafetyBearerToken1234567890"
    telegram_bot = "123456" + ":" + "PublicSafetyTelegramBotToken123"
    basic = "Bas" + "ic " + "UHVibGljU2FmZXR5QmFzaWMxMjM="
    labelled_token = "token:" + "publicSafetyLabelledToken123456"
    labelled_password = "password=" + "publicSafetyLabelledPassword123456"
    labelled_authorization = (
        "authorization:" + "publicSafetyLabelledAuthorization123456"
    )
    return {
        "absolute_path": "/home/alice/.ssh/id_ed25519",
        "home_relative_path": "~/.ssh/id_ed25519",
        "home_path_without_slash": "home/alice/.ssh/id_ed25519",
        "windows_absolute_path": r"C:\Users\Alice\.ssh\id_ed25519",
        "unc_path": r"\\private-host\share\id_ed25519",
        "herdr_socket_path": "/run/user/1000/herdr/private.sock",
        "user_socket_uri": "unix:///run/user/1000/tendwire/private.sock",
        "private_endpoint": "10.42.7.9:5432",
        "credential_url": "https://alice:publicSafetyPassword@internal.example/private",
        "shell_command": "bash -lc 'cat /home/alice/.ssh/id_ed25519'",
        "argv": "--identity-file=/home/alice/.ssh/id_ed25519",
        "environment": "DEPLOY_TOKEN=publicSafetyEnvironmentValue",
        "stdout": "stdout: publicSafetyStdoutValue",
        "stderr": "stderr: publicSafetyStderrValue",
        "openai_key": openai_key,
        "github_key": github_key,
        "google_key": google_key,
        "aws_key": aws_key,
        "gitlab_key": gitlab_key,
        "npm_key": npm_key,
        "pypi_key": pypi_key,
        "slack_key": slack_key,
        "jwt": jwt,
        "bearer": bearer,
        "basic": basic,
        "labelled_token": labelled_token,
        "labelled_password": labelled_password,
        "labelled_authorization": labelled_authorization,
        "pane_id": "w4V:p1",
        "terminal_id": "term_655ad3e5205705",
        "session_id": "session_PUBLICSAFETY123456",
        "backend_target": "backend target: pane-public-safety-private",
        "private_fingerprint": "private fingerprint: 0123456789abcdef01234567",
        "tool_use_id": "toolu_PUBLICSAFETYDECISION01",
        "telegram_bot": telegram_bot,
        "telegram_chat_id": "-1001234567890123",
        "telegram_topic_id": "topic id: 731245",
        "telegram_message_id": "message id: 918273",
    }


def _sentinels_as_dynamic_keys(corpus: dict[str, str]) -> dict[str, str]:
    return {sentinel: f"dynamic-{name}" for name, sentinel in corpus.items()}


def _boundary_straddling_credentials() -> list[Any]:
    corpus = _sentinel_corpus()
    cases = (
        ("credential_url", corpus["credential_url"], "https://alice:"),
        ("environment", corpus["environment"], "DEPLOY_TOKEN="),
        ("openai_key", corpus["openai_key"], "sk-proj"),
        ("github_key", corpus["github_key"], "ghp_"),
        ("google_key", corpus["google_key"], "AIzaPUB"),
        ("aws_key", corpus["aws_key"], "AKIAPUBLIC"),
        ("gitlab_key", corpus["gitlab_key"], "glpat-PUB"),
        ("npm_key", corpus["npm_key"], "npm_PUBLIC"),
        ("pypi_key", corpus["pypi_key"], "pypi-PUBLIC"),
        ("slack_key", corpus["slack_key"], "xoxb-PUB"),
        (
            "jwt",
            corpus["jwt"],
            corpus["jwt"].split(".", maxsplit=1)[0] + ".",
        ),
        ("bearer", corpus["bearer"], "Bearer publ"),
        ("basic", corpus["basic"], "Basic UHVi"),
        ("telegram_bot", corpus["telegram_bot"], "123456:Publ"),
        ("labelled_token", corpus["labelled_token"], "token:"),
        ("labelled_password", corpus["labelled_password"], "password="),
        (
            "labelled_authorization",
            corpus["labelled_authorization"],
            "authorization:",
        ),
        (
            "unbounded_bearer",
            "Bearer " + ("A" * 100_000),
            "Bearer AAAA",
        ),
    )
    return [
        pytest.param(name, credential, visible_prefix, id=name)
        for name, credential, visible_prefix in cases
    ]


def _assert_sentinels_absent(value: Any, corpus: dict[str, str] | None = None) -> None:
    encoded = json.dumps(value, ensure_ascii=False, sort_keys=True)
    for name, sentinel in (corpus or _sentinel_corpus()).items():
        assert sentinel not in encoded, f"{name} sentinel leaked: {sentinel!r}"


def _assert_internal_lifecycle_keys_absent(value: Any) -> None:
    forbidden = {
        "family_key",
        "generation",
        "first_missing_at",
        "missing_observation_count",
        "last_accepted_at",
        "last_observation_key",
        "observation_key",
        "max_notified_severity_rank",
        "stage",
        "migration_group",
        "migration_group_key",
        "canonical_migration_outbox_id",
        "migration_canonical",
        "canonical_outbox_id",
        "terminal_after_lease",
        "lease_token",
    }

    def visit(item: Any) -> None:
        if isinstance(item, dict):
            assert forbidden.isdisjoint(item), forbidden.intersection(item)
            for nested in item.values():
                visit(nested)
        elif isinstance(item, list | tuple):
            for nested in item:
                visit(nested)

    visit(value)


def _unsafe_prompt(corpus: dict[str, str]) -> str:
    labels = {
        "shell_command": "command",
        "argv": "argv",
        "environment": "environment",
        "stdout": "stdout",
        "stderr": "stderr",
        "backend_target": "backend target",
        "private_fingerprint": "private fingerprint",
        "telegram_topic_id": "topic id",
        "telegram_message_id": "message id",
    }
    return "\n".join(
        f"{labels.get(name, name.replace('_', ' '))}: {sentinel}"
        for name, sentinel in corpus.items()
    )


def test_sentinel_assertion_detects_values_under_innocent_keys() -> None:
    corpus = _sentinel_corpus()
    with pytest.raises(AssertionError, match="absolute_path sentinel leaked"):
        _assert_sentinels_absent({"safe": corpus["absolute_path"]}, corpus)
    with pytest.raises(AssertionError, match="tool_use_id sentinel leaked"):
        _assert_sentinels_absent({"note": {"value": corpus["tool_use_id"]}}, corpus)
    with pytest.raises(AssertionError, match="tool_use_id sentinel leaked"):
        _assert_sentinels_absent({corpus["tool_use_id"]: "safe"}, corpus)


def test_shared_public_value_sanitizer_blocks_full_nested_corpus() -> None:
    corpus = _sentinel_corpus()
    dynamic_keys = _sentinels_as_dynamic_keys(corpus)
    markdown = (
        "# Public heading\n\n"
        "- first item\n"
        "  - nested item\n\n"
        "```python\n"
        "print('README.md')\n"
        "```\n\n"
        "Read docs/guide.md and https://example.com/docs?q=public#section."
    )
    dirty = {
        "safe": "kept",
        "user_text": markdown,
        "values": list(corpus.values()),
        "nested": {name: {"innocent": value} for name, value in corpus.items()},
        "dynamic_keys": dynamic_keys,
        "static_structure": {
            "schema_version": 1,
            "status": "working",
            "worker_id": "worker-public",
        },
    }
    sanitized = sanitize_public_value(dirty)

    assert sanitized["safe"] == "kept"
    assert sanitized["user_text"] == markdown
    assert sanitized["dynamic_keys"] == {}
    assert sanitized["static_structure"] == {
        "schema_version": 1,
        "status": "working",
        "worker_id": "worker-public",
    }
    assert "README.md" in public_json_dumps(sanitized)
    assert "https://example.com/docs?q=public#section" in public_json_dumps(sanitized)
    _assert_sentinels_absent(sanitized, corpus)
    final_json = json.loads(public_json_dumps(dirty))
    assert final_json["dynamic_keys"] == {}
    _assert_sentinels_absent(final_json, corpus)


@pytest.mark.parametrize(
    ("public_key", "public_value", "private_neighbor"),
    (
        pytest.param("host_id", "host-public", "provider_host_id", id="host"),
        pytest.param("worker_id", "worker-public", "pane_id", id="worker"),
        pytest.param("space_id", "space-public", "terminal_id", id="space"),
        pytest.param(
            "source_turn_id",
            "turnsrc-" + ("1" * 24),
            "session_id",
            id="source-turn",
        ),
        pytest.param(
            "turn_id",
            "turn-" + ("8" * 24),
            "backend_turn_id",
            id="canonical-turn",
        ),
        pytest.param(
            "segment_id",
            "twseg1.public-segment",
            "provider_segment_id",
            id="content-segment",
        ),
        pytest.param(
            "origin_command_id",
            "command-public",
            "raw_command_id",
            id="origin-command",
        ),
        pytest.param(
            "choice_id",
            "choice-" + ("2" * 24),
            "tool_use_id",
            id="choice",
        ),
        pytest.param("action_id", "inspect-worker", "decision_id", id="action"),
        pytest.param(
            "attention_id",
            "attn-" + ("3" * 24),
            "telegram_message_id",
            id="attention",
        ),
        pytest.param(
            "id",
            "pending-" + ("4" * 24),
            "pending_decision_id",
            id="pending",
        ),
        pytest.param(
            "id",
            "turn-" + ("5" * 24),
            "backend_turn_id",
            id="turn",
        ),
        pytest.param("request_id", "request-public", "provider_request_id", id="request"),
        pytest.param("row_id", 42, "connector_row_id", id="row"),
        pytest.param(
            "content_fingerprint",
            "6" * 24,
            "private_content_fingerprint",
            id="content-fingerprint",
        ),
        pytest.param(
            "worker_fingerprint",
            "7" * 24,
            "backend_worker_fingerprint",
            id="worker-fingerprint",
        ),
    ),
)
def test_public_structural_key_allowlist_keeps_only_schema_keys(
    public_key: str,
    public_value: Any,
    private_neighbor: str,
) -> None:
    sanitized = sanitize_public_value(
        {
            "nested": {
                public_key: public_value,
                private_neighbor: "private-neighbor-value",
            }
        }
    )

    assert sanitized == {"nested": {public_key: public_value}}


def test_public_structural_key_allowlist_rejects_dynamic_id_and_fingerprint_shapes() -> None:
    assert sanitize_public_value(
        {
            "nested": {
                "workspace_id": "workspace-private",
                "custom_fingerprint": "fingerprint-private",
                "workerId": "worker-private",
                "contentFingerprint": "content-private",
                "safe": "kept",
            }
        }
    ) == {"nested": {"safe": "kept"}}
    assert sanitize_public_value({"host_id": "session_PUBLICSAFETY123456"}) == {}


def test_public_submission_verdict_is_closed_vocabulary() -> None:
    assert sanitize_public_value({"submission_verdict": "submitted"}) == {
        "submission_verdict": "submitted"
    }
    assert sanitize_public_value({"submission_verdict": "agent_not_ready"}) == {
        "submission_verdict": "agent_not_ready"
    }
    assert sanitize_public_value(
        {"submission_verdict": "agent_target_ambiguous"}
    ) == {"submission_verdict": "agent_target_ambiguous"}
    assert sanitize_public_value({"submission_verdict": "invented"}) == {}
    assert sanitize_public_value({"submission_verdict": 1}) == {}


def test_public_submission_diagnostics_drop_composer_state() -> None:
    assert sanitize_public_value(
        {
            "submission_verdict": "agent_prompt_stalled",
            "composer_state": "instruction_visible",
        }
    ) == {"submission_verdict": "agent_prompt_stalled"}


@pytest.mark.parametrize("host_id", ("output-excerpt", "pane-id-private"))
def test_public_host_id_provenance_preserves_opaque_field_name_words(host_id: str) -> None:
    snapshot = Snapshot(
        host_id=host_id,
        updated_at="2026-07-10T00:00:00+00:00",
    )

    assert sanitize_public_value({"host_id": host_id}) == {"host_id": host_id}
    assert snapshot.to_dict()["host_id"] == host_id


@pytest.mark.parametrize(
    "private_value",
    (
        pytest.param("/home/alice/.ssh/id_ed25519", id="path"),
        pytest.param("w4V:p1", id="pane-id"),
        pytest.param("term_655ad3e5205705", id="terminal-id"),
        pytest.param("session_PUBLICSAFETY123456", id="session-id"),
        pytest.param("toolu_PUBLICSAFETYDECISION01", id="tool-use-id"),
        pytest.param(
            "backend target: pane-public-safety-private",
            id="backend-target",
        ),
        pytest.param("bash -lc whoami", id="raw-command"),
        pytest.param("sk-" + "proj-PUBLICSAFETY1234567890", id="provider-credential"),
    ),
)
def test_public_structural_field_provenance_still_rejects_private_value_shapes(
    private_value: str,
) -> None:
    assert sanitize_public_value({"host_id": private_value}) == {}


def test_snapshot_turn_and_pending_keep_public_structural_opaque_values() -> None:
    space = Space(
        id="space-" + ("8" * 24),
        name="Public Space",
        fingerprint="9" * 24,
    )
    worker = Worker(
        id="worker-" + ("a" * 24),
        name="Public Worker",
        space_id=space.id,
        fingerprint="b" * 24,
    )
    action = SuggestedAction(
        action_id="inspect-worker",
        label="Inspect worker",
        tendwire_action="snapshot",
        params={"worker_id": worker.id},
    )
    signal = AttentionSignal(
        id="attn-" + ("c" * 24),
        kind="worker_status",
        severity="warning",
        status="waiting",
        reason="Review public progress",
        source=f"worker:{worker.id}",
        suggested_actions=[action],
        fingerprint="d" * 24,
        meta={"worker_id": worker.id, "space_id": space.id},
        host_id="structural-host",
    )
    snapshot = Snapshot(
        host_id="structural-host",
        updated_at="2026-07-10T00:00:00+00:00",
        spaces=[space],
        workers=[worker],
        attention=[signal],
    )
    turn = Turn(
        host_id=snapshot.host_id,
        worker_id=worker.id,
        worker_fingerprint=worker.fingerprint,
        space_id=space.id,
        status="working",
        source_turn_id="turnsrc-" + ("e" * 24),
        origin_command_id="command-public",
    )
    choice = InteractionChoice(
        choice_id="choice-" + ("f" * 24),
        label="Continue",
    )
    pending = PendingInteraction(
        host_id=snapshot.host_id,
        worker_id=worker.id,
        worker_fingerprint=worker.fingerprint,
        space_id=space.id,
        question="Continue?",
        choices=[choice],
        meta={"attention_id": signal.id},
    )
    private_key = "provider_runtime_id"
    private_value = "session_PUBLICSAFETY123456"
    worker.meta[private_key] = "private-key-value"
    worker.meta["late_value"] = private_value
    signal.meta[private_key] = "private-key-value"
    signal.meta["late_value"] = private_value
    turn.meta[private_key] = "private-key-value"
    turn.meta["late_value"] = private_value
    pending.meta[private_key] = "private-key-value"
    pending.meta["late_value"] = private_value

    snapshot_payload = snapshot.to_dict()
    turn_payload = turn.to_dict()
    pending_payload = pending.to_dict()
    attention_wrapper = attention_payload_from_snapshot(snapshot)
    turns_wrapper = turns_payload_from_snapshot(snapshot)
    pending_wrapper = pending_payload_from_snapshot(snapshot)

    assert snapshot_payload["host_id"] == snapshot.host_id
    assert snapshot_payload["content_fingerprint"] == snapshot.content_fingerprint
    assert snapshot_payload["spaces"][0]["id"] == space.id
    assert snapshot_payload["spaces"][0]["fingerprint"] == space.fingerprint
    assert snapshot_payload["workers"][0]["id"] == worker.id
    assert snapshot_payload["workers"][0]["space_id"] == space.id
    assert snapshot_payload["workers"][0]["fingerprint"] == worker.fingerprint
    assert snapshot_payload["attention"][0]["id"] == signal.id
    assert snapshot_payload["attention"][0]["fingerprint"] == signal.fingerprint
    assert snapshot_payload["attention"][0]["meta"]["worker_id"] == worker.id
    assert snapshot_payload["attention"][0]["meta"]["space_id"] == space.id
    assert (
        snapshot_payload["attention"][0]["suggested_actions"][0]["action_id"]
        == action.action_id
    )
    assert (
        snapshot_payload["attention"][0]["suggested_actions"][0]["params"]["worker_id"]
        == worker.id
    )
    assert turn_payload["id"] == turn.id
    assert turn_payload["fingerprint"] == turn.fingerprint
    assert turn_payload["host_id"] == snapshot.host_id
    assert turn_payload["worker_id"] == worker.id
    assert turn_payload["worker_fingerprint"] == worker.fingerprint
    assert turn_payload["space_id"] == space.id
    assert turn_payload["source_turn_id"] == turn.source_turn_id
    assert turn_payload["origin_command_id"] == turn.origin_command_id
    assert pending_payload["id"] == pending.id
    assert pending_payload["fingerprint"] == pending.fingerprint
    assert pending_payload["host_id"] == snapshot.host_id
    assert pending_payload["worker_id"] == worker.id
    assert pending_payload["worker_fingerprint"] == worker.fingerprint
    assert pending_payload["space_id"] == space.id
    assert pending_payload["meta"]["attention_id"] == signal.id
    assert pending_payload["choices"][0]["choice_id"] == choice.choice_id
    assert attention_wrapper["host_id"] == snapshot.host_id
    assert attention_wrapper["content_fingerprint"]
    assert attention_wrapper["attention"][0]["id"] == signal.id
    assert turns_wrapper["host_id"] == snapshot.host_id
    assert turns_wrapper["content_fingerprint"]
    assert turns_wrapper["turns"][0]["id"]
    assert turns_wrapper["turns"][0]["fingerprint"]
    assert turns_wrapper["turns"][0]["worker_id"] == worker.id
    assert turns_wrapper["turns"][0]["worker_fingerprint"] == worker.fingerprint
    assert pending_wrapper["host_id"] == snapshot.host_id
    assert pending_wrapper["content_fingerprint"]
    assert pending_wrapper["pending_health"] == {
        "status": "healthy",
        "counts": {"fresh": 0, "stale": 0, "total": 0},
    }
    assert pending_wrapper["pending_interactions"][0]["id"]
    assert pending_wrapper["pending_interactions"][0]["fingerprint"]
    assert pending_wrapper["pending_interactions"][0]["worker_id"] == worker.id
    assert pending_wrapper["pending_interactions"][0]["worker_fingerprint"] == worker.fingerprint
    assert pending_wrapper["pending_interactions"][0]["meta"]["attention_id"] == signal.id
    for payload in (
        snapshot_payload,
        turn_payload,
        pending_payload,
        attention_wrapper,
        turns_wrapper,
        pending_wrapper,
    ):
        encoded = json.dumps(payload, sort_keys=True)
        assert private_key not in encoded
        assert private_value not in encoded


@pytest.mark.parametrize(
    ("name", "credential", "visible_prefix"),
    _boundary_straddling_credentials(),
)
def test_credentials_are_redacted_before_a_straddling_truncation_boundary(
    name: str,
    credential: str,
    visible_prefix: str,
) -> None:
    preamble = "Public boundary value: "
    marker = "\n[truncated]"
    max_chars = len(preamble) + len(visible_prefix) + len(marker)
    value = (
        preamble
        + credential
        + "\n"
        + ("Safe public continuation. " * (max_chars + 1))
    )
    visible_before_truncation = value[: max_chars - len(marker)]
    assert visible_before_truncation.endswith(visible_prefix), name
    assert credential not in visible_before_truncation, name

    sanitized = sanitize_public_text(value, max_chars=max_chars)

    assert len(sanitized) <= max_chars, name
    assert marker in sanitized, name
    assert visible_prefix not in sanitized, name


def test_numeric_telegram_chat_ids_are_private_but_ordinary_integers_are_not() -> None:
    telegram_chat_id = -1001234567890123
    dirty = {
        "zero": 0,
        "topic_number": 731245,
        "message_number": 918273,
        "near_miss": -100123456789,
        "positive": 1001234567890123,
        "boolean": True,
        "innocent": telegram_chat_id,
        "nested": {"innocent": telegram_chat_id},
        "items": [1, telegram_chat_id, 2],
    }

    sanitized = sanitize_public_value(dirty)

    assert sanitized == {
        "zero": 0,
        "topic_number": 731245,
        "message_number": 918273,
        "near_miss": -100123456789,
        "positive": 1001234567890123,
        "boolean": True,
        "nested": {},
        "items": [1, 2],
    }
    assert sanitize_public_value(telegram_chat_id) is None


def test_snapshot_turn_pending_and_wrappers_resanitize_mutable_values() -> None:
    corpus = _sentinel_corpus()
    dynamic_keys = _sentinels_as_dynamic_keys(corpus)
    safe_markdown = "## Progress\n\n- Read `README.md`\n\n```text\nall good\n```\n\nhttps://example.com/help"
    worker = Worker(
        id="worker-1",
        name="Worker One",
        status="waiting",
        space_id="space-1",
        summary=safe_markdown,
        meta={"safe": "kept"},
    )
    signal = AttentionSignal(
        kind="worker_status",
        severity="warning",
        status="waiting",
        reason="Review public progress",
        source="worker:worker-1",
        meta={"worker_id": "worker-1", "needs_human": True, "safe": "kept"},
        host_id="public-safety-host",
    )
    snapshot = Snapshot(
        host_id="public-safety-host",
        updated_at="2026-07-10T00:00:00+00:00",
        spaces=[Space(id="space-1", name="Public Project")],
        workers=[worker],
        attention=[signal],
    )
    worker.meta["late_values"] = list(corpus.values())
    signal.meta["late_values"] = list(corpus.values())
    worker.meta["late_dynamic_keys"] = dynamic_keys.copy()
    signal.meta["late_dynamic_keys"] = dynamic_keys.copy()
    turn = Turn(
        host_id=snapshot.host_id,
        worker_id=worker.id,
        status="working",
        kind="task",
        user_text=safe_markdown,
        assistant_stream_text=_unsafe_prompt(corpus),
        source_turn_id=corpus["session_id"],
        meta={"safe": "kept"},
    )
    turn.meta["late_values"] = list(corpus.values())
    turn.meta["late_dynamic_keys"] = dynamic_keys.copy()
    choice = InteractionChoice(
        choice_id=corpus["tool_use_id"],
        label="Review safely",
        value={"safe": "approve", "sent": corpus["shell_command"]},
        description=_unsafe_prompt(corpus),
        params={"safe": "kept"},
    )
    choice.params["late_values"] = list(corpus.values())
    choice.params["late_dynamic_keys"] = dynamic_keys.copy()
    pending = PendingInteraction(
        host_id=snapshot.host_id,
        worker_id=worker.id,
        question=_unsafe_prompt(corpus),
        kind="question",
        choices=[choice],
        meta={"safe": "kept"},
    )
    pending.meta["late_values"] = list(corpus.values())
    pending.meta["late_dynamic_keys"] = dynamic_keys.copy()

    surfaces = {
        "snapshot": snapshot.to_dict(),
        "turn": turn.to_dict(),
        "pending": pending.to_dict(),
        "turn_wrapper": turns_payload_from_snapshot(snapshot),
        "attention_wrapper": attention_payload_from_snapshot(snapshot),
        "pending_wrapper": pending_payload_from_snapshot(snapshot),
        "turn_json": json.loads(payload_to_json({"turns": [turn.to_dict()]})),
        "pending_json": json.loads(payload_to_json({"pending_interactions": [pending.to_dict()]})),
        "final_json": json.loads(
            public_json_dumps(
                {
                    "snapshot": snapshot.to_dict(),
                    "turn": turn.to_dict(),
                    "dynamic_keys": dynamic_keys,
                }
            )
        ),
    }

    assert surfaces["snapshot"]["workers"][0]["summary"] == safe_markdown
    assert surfaces["snapshot"]["workers"][0]["meta"]["late_dynamic_keys"] == {}
    assert surfaces["turn"]["meta"]["late_dynamic_keys"] == {}
    assert surfaces["final_json"]["dynamic_keys"] == {}
    assert surfaces["turn"]["user_text"] == safe_markdown
    assert surfaces["turn"]["source_turn_id"].startswith("turnsrc-")
    assert surfaces["pending"]["choices"] == [
        {"choice_id": choice.choice_id, "label": "Review safely"}
    ]
    assert choice.choice_id.startswith("choice-")
    for surface in surfaces.values():
        _assert_sentinels_absent(surface, corpus)


def test_daemon_response_builders_apply_final_value_sanitization() -> None:
    corpus = _sentinel_corpus()
    safe_markdown = "# Status\n\n- safe\n\nhttps://example.com/status"
    result = {
        "safe": "kept",
        "assistant_final_text": safe_markdown,
        "values": list(corpus.values()),
    }

    success = success_response(result, request_id="public-request")
    failure = error_response(
        "public_error",
        "Safe public failure",
        details={"safe": "kept", "values": list(corpus.values())},
        request_id="public-request",
    )

    assert success["result"]["safe"] == "kept"
    assert success["result"]["assistant_final_text"] == safe_markdown
    assert failure["error"]["details"]["safe"] == "kept"
    _assert_sentinels_absent(success, corpus)
    _assert_sentinels_absent(failure, corpus)


def test_store_feed_outbox_and_connector_facade_resanitize_before_delivery(tmp_path) -> None:
    corpus = _sentinel_corpus()
    private_values = {
        **corpus,
        "connector_id": "connector-public-safety-private",
    }
    db_path = tmp_path / "public-boundaries.db"
    host_id = "public-boundary-host"

    def signal(
        *,
        severity: str,
        status: str,
        reason: str,
        observed_at: str,
    ) -> AttentionSignal:
        item = AttentionSignal(
            kind="worker_status",
            severity=severity,
            status=status,
            reason=reason,
            source="worker:worker-1",
            updated_at=observed_at,
            meta={"worker_id": "worker-1", "needs_human": True, "safe": "kept"},
            host_id=host_id,
        )
        item.meta["late_values"] = list(corpus.values())
        item.meta.update(
            {
                "pane_id": corpus["pane_id"],
                "terminal_id": corpus["terminal_id"],
                "session_id": corpus["session_id"],
                "backend_target": corpus["backend_target"],
                "connector_id": private_values["connector_id"],
                "telegram_chat_id": corpus["telegram_chat_id"],
                "telegram_topic_id": corpus["telegram_topic_id"],
                "telegram_message_id": corpus["telegram_message_id"],
            }
        )
        return item

    def snapshot(observed_at: str, attention: list[AttentionSignal]) -> Snapshot:
        return Snapshot(
            host_id=host_id,
            updated_at=observed_at,
            workers=[
                Worker(
                    id="worker-1",
                    name="Worker One",
                    status="waiting",
                    backend_target={
                        "pane_id": corpus["pane_id"],
                        "terminal_id": corpus["terminal_id"],
                        "session_id": corpus["session_id"],
                        "connector_id": private_values["connector_id"],
                        "telegram_chat_id": corpus["telegram_chat_id"],
                    },
                )
            ],
            attention=attention,
        )

    def save_complete(observed_at: str, attention: list[AttentionSignal]) -> None:
        save_snapshot(
            db_path,
            snapshot(observed_at, attention),
            observation=SnapshotObservationContext(
                authority="complete",
                observed_at=observed_at,
            ),
        )

    initial_signal = signal(
        severity="warning",
        status="waiting",
        reason="Review the safe public result",
        observed_at="2026-07-10T00:00:00+00:00",
    )
    assert set(initial_signal.to_dict()) == {
        "id",
        "kind",
        "severity",
        "status",
        "reason",
        "source",
        "updated_at",
        "suggested_actions",
        "fingerprint",
        "meta",
    }

    init_store(db_path)
    save_complete("2026-07-10T00:00:00+00:00", [initial_signal])
    initial_feed = attention_payload_from_store(db_path, host_id)

    escalated_signal = signal(
        severity="critical",
        status="failed",
        reason="Review the safe public result",
        observed_at="2026-07-10T00:00:10+00:00",
    )
    save_complete("2026-07-10T00:00:10+00:00", [escalated_signal])
    escalated_feed = attention_payload_from_store(db_path, host_id)

    save_complete("2026-07-10T00:00:20+00:00", [])
    pending_feed = attention_payload_from_store(db_path, host_id)
    save_complete("2026-07-10T00:02:20+00:00", [])
    resolved_feed = attention_payload_from_store(db_path, host_id)

    recurrence_signal = signal(
        severity="warning",
        status="waiting",
        reason="Review the safe public result",
        observed_at="2026-07-10T00:02:30+00:00",
    )
    save_complete("2026-07-10T00:02:30+00:00", [recurrence_signal])
    recurrence_feed = attention_payload_from_store(db_path, host_id)

    with sqlite3.connect(str(db_path)) as conn:
        snapshot_rows = [
            json.loads(row[0]) for row in conn.execute("SELECT payload FROM snapshots")
        ]
        event_rows = [
            json.loads(row[0]) for row in conn.execute("SELECT payload_json FROM events")
        ]
        attention_rows = [
            json.loads(row[0])
            for row in conn.execute("SELECT payload_json FROM attention_items")
        ]
        pre_migration_outbox_rows = [
            json.loads(row[0])
            for row in conn.execute(
                "SELECT payload_json FROM connector_outbox ORDER BY id"
            )
        ]

    tail = tail_event_metadata(db_path, host_id, limit=100)
    connector_payload = ConnectorOutboxAPI(db_path, host_id).poll(
        {"name": "attention", "limit": 10}
    )

    # Copy the leased lifecycle jobs into a v4-shaped fixture. Migration must
    # preserve live refs privately without putting its grouping markers into
    # either the public attention projection or the connector payload.
    migration_db_path = tmp_path / "migration-public-boundaries.db"
    with (
        sqlite3.connect(str(db_path)) as source,
        sqlite3.connect(str(migration_db_path)) as destination,
    ):
        source.backup(destination)
    with sqlite3.connect(str(migration_db_path)) as conn:
        conn.execute("DROP TABLE attention_lifecycles")
        conn.execute("PRAGMA user_version = 4")

    init_store(migration_db_path)
    migrated_feed = attention_payload_from_store(migration_db_path, host_id)
    with sqlite3.connect(str(migration_db_path)) as conn:
        migrated_attention_rows = [
            json.loads(row[0])
            for row in conn.execute("SELECT payload_json FROM attention_items")
        ]
        migrated_outbox_rows = [
            json.loads(row[0])
            for row in conn.execute(
                "SELECT payload_json FROM connector_outbox ORDER BY id"
            )
        ]
        migration_private_states = [
            json.loads(row[0])
            for row in conn.execute(
                "SELECT private_state_json FROM connector_outbox ORDER BY id"
            )
        ]

    assert initial_feed is not None
    assert [item["severity"] for item in initial_feed["attention"]] == ["warning"]
    assert escalated_feed is not None
    assert [item["severity"] for item in escalated_feed["attention"]] == ["critical"]
    assert pending_feed is not None
    assert pending_feed["attention"] == escalated_feed["attention"]
    assert resolved_feed is not None
    assert resolved_feed["attention"] == []
    assert recurrence_feed is not None
    assert [item["severity"] for item in recurrence_feed["attention"]] == ["warning"]
    assert [
        payload["event_type"] for payload in pre_migration_outbox_rows
    ] == [
        "attention_created",
        "attention_escalated",
        "attention_created",
    ]

    assert migrated_feed is not None
    assert len(migrated_feed["attention"]) == 1
    assert migrated_feed["attention"][0]["reason"] == "Review the safe public result"
    assert connector_payload["ok"] is True
    assert connector_payload["items"]
    assert all(
        item["payload"]["attention"]["meta"]["safe"] == "kept"
        for item in connector_payload["items"]
    )
    assert snapshot_rows
    assert event_rows
    assert attention_rows
    assert migrated_attention_rows
    assert migrated_outbox_rows
    assert any("migration_group" in state for state in migration_private_states)

    public_attention_surfaces = (
        initial_feed,
        escalated_feed,
        pending_feed,
        resolved_feed,
        recurrence_feed,
        migrated_feed,
        attention_rows,
        migrated_attention_rows,
    )
    outbox_payload_surfaces = (
        pre_migration_outbox_rows,
        migrated_outbox_rows,
        [item["payload"] for item in connector_payload["items"]],
    )
    for surface in public_attention_surfaces + outbox_payload_surfaces:
        _assert_internal_lifecycle_keys_absent(surface)

    for surface in (
        *public_attention_surfaces,
        *outbox_payload_surfaces,
        snapshot_rows,
        event_rows,
        tail,
        connector_payload,
    ):
        _assert_sentinels_absent(surface, private_values)


def test_cli_json_output_applies_the_final_public_value_boundary(tmp_path, monkeypatch, capsys) -> None:
    corpus = _sentinel_corpus()
    dynamic_keys = _sentinels_as_dynamic_keys(corpus)
    safe_markdown = "# Result\n\n- kept\n\n```text\npublic\n```\n\nhttps://example.com/result"
    daemon_result = {
        "safe": "kept",
        "assistant_final_text": safe_markdown,
        "values": list(corpus.values()),
        "dynamic_keys": dynamic_keys,
    }
    monkeypatch.setattr(
        "tendwire.cli._try_daemon_attempt",
        lambda *_args, **_kwargs: SimpleNamespace(
            result=daemon_result,
            response_error=None,
            request_started=True,
            error_kind=None,
        ),
    )

    exit_code = cmd_snapshot(
        Config(host_id="public-cli-host", data_dir=tmp_path, db_path=tmp_path / "cli.db"),
        json_output=True,
    )
    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert exit_code == 0
    assert captured.err == ""
    assert payload["safe"] == "kept"
    assert payload["assistant_final_text"] == safe_markdown
    _assert_sentinels_absent(payload, corpus)
    assert payload["dynamic_keys"] == {}


def test_canonical_turn_sanitizer_redacts_once_without_trimming_or_truncating() -> None:
    corpus = _sentinel_corpus()
    raw = (
        " \r\n# Public result\r\n\r\n"
        + _unsafe_prompt(corpus)
        + "\r\n```text\r\n"
        + ("public-content-" * 2_000)
        + "\r\n```\r\n "
    )

    canonical = sanitize_canonical_turn_text(raw)

    assert canonical is not None
    assert canonical.startswith(" \r\n# Public result\r\n\r\n")
    assert canonical.endswith("\r\n```\r\n ")
    assert len(canonical) > 20_000
    assert "[truncated]" not in canonical
    assert "\x00" not in canonical
    _assert_sentinels_absent(canonical, corpus)
    segments = segment_canonical_text(canonical)
    assert "".join(segment.text for segment in segments) == canonical
    for segment in segments:
        assert segment.byte_length <= 48 * 1024
        _assert_sentinels_absent(segment.text, corpus)


def test_generic_public_text_limit_does_not_change_canonical_content_contract() -> None:
    raw = " \r\n" + ("safe😀" * 4_000) + "\r\n "

    generic = sanitize_public_text(raw, max_chars=12_000)
    canonical = sanitize_canonical_turn_text(raw)

    assert generic.endswith("[truncated]")
    assert canonical == raw
    assert canonical is not None
    assert len(canonical) > 12_000


def test_boundary_secret_stays_redacted_through_store_pages_and_plan_outbox(
    tmp_path,
) -> None:
    host_id = "boundary-safety-host"
    worker_id = "worker-1"
    page_bytes = 48 * 1024
    private_secret = _sentinel_corpus()["openai_key"]
    safe_prefix = ("A" * (page_bytes - (len(private_secret) // 2) - 1)) + " "
    dirty_final = (
        safe_prefix
        + private_secret
        + "\npublic-tail-🙂\n"
        + ("Z" * page_bytes)
    )
    secret_offset = dirty_final.encode("utf-8").index(private_secret.encode("utf-8"))
    assert secret_offset < page_bytes < secret_offset + len(private_secret.encode("utf-8"))

    expected = sanitize_canonical_turn_text(dirty_final)
    assert expected is not None
    assert expected.count("[redacted]") == 1
    assert private_secret not in expected

    db_path = tmp_path / "boundary-content-safety.db"
    snapshot = Snapshot(
        host_id=host_id,
        updated_at="2026-07-11T00:00:00+00:00",
        workers=[
            Worker(
                id=worker_id,
                name="Boundary worker",
                status="active",
                meta={
                    "stable_key": "wsk1_" + ("1" * 64),
                    "stable_key_version": 1,
                },
            )
        ],
    )
    init_store(db_path)
    save_snapshot(db_path, snapshot)
    assert merge_turn_content(
        db_path,
        host_id,
        worker_id,
        {
            "source_turn_id": "boundary-content-source",
            "assistant_final_text": dirty_final,
            "complete": True,
            "has_open_turn": False,
        },
        observed_at="2026-07-11T00:00:01+00:00",
    ) == 1

    listed = turns_payload_from_store(
        db_path,
        host_id,
        snapshot=snapshot,
        schema_version=2,
    )
    turn = next(
        item
        for item in listed["turns"]
        if item["content"]["fields"]["assistant_final_text"]["availability"]
        == "complete"
    )
    revision = turn["content"]["content_revision"]
    cursor = None
    pages: list[dict[str, Any]] = []
    while True:
        page = get_turn_content(
            db_path,
            host_id,
            turn_id=turn["id"],
            content_revision=revision,
            field="assistant_final_text",
            cursor=cursor,
        )
        assert page.get("status") is None
        assert page["segment_byte_length"] <= page_bytes
        _assert_sentinels_absent(page, {"boundary_secret": private_secret})
        pages.append(page)
        cursor = page["next_cursor"]
        if cursor is None:
            break

    joined = "".join(page["text"] for page in pages)
    assert joined == expected
    assert "[redacted]" in joined
    assert private_secret not in joined
    assert len(pages) >= 2
    assert turn["content"]["fields"]["assistant_final_text"]["page_count"] == len(pages)

    api = ConnectorOutboxAPI(db_path, host_id)
    ready_poll = api.poll({"name": "turn-final", "limit": 1})
    assert ready_poll["ok"] is True
    assert len(ready_poll["items"]) == 1
    ready_ref = ready_poll["items"][0]["ref"]
    assert ready_poll["items"][0]["key"].startswith("turn-final:revision:twfinal1.")
    begun = api.prepare(
        {
            "schema_version": 1,
            "action": "begin",
            "name": "turn-final",
            "turn_id": turn["id"],
            "content_revision": revision,
            "presentation_version": "boundary-safety-v1",
            "part_count": len(pages),
            "source_ref": ready_ref,
        }
    )
    assert begun["ok"] is True
    plan_token = begun["plan_token"]
    prepared_parts: list[dict[str, Any]] = []
    start_char = 0
    for ordinal, page in enumerate(pages):
        end_char = start_char + page["segment_char_length"]
        part = api.prepare(
            {
                "schema_version": 1,
                "action": "part",
                "name": "turn-final",
                "plan_token": plan_token,
                "ordinal": ordinal,
                "spans": [
                    {
                        "field": "assistant_final_text",
                        "start_char": start_char,
                        "end_char": end_char,
                    }
                ],
            }
        )
        assert part["ok"] is True
        prepared_parts.append(part)
        start_char = end_char
    assert start_char == len(expected)

    committed = api.prepare(
        {
            "schema_version": 1,
            "action": "commit",
            "name": "turn-final",
            "plan_token": plan_token,
            "source_ref": ready_ref,
        }
    )
    assert committed["ok"] is True
    assert committed["job_count"] == len(pages)

    poll_surfaces: list[dict[str, Any]] = []
    job_items: list[dict[str, Any]] = []
    ack_surfaces: list[dict[str, Any]] = []
    for _ in range(len(pages) + 1):
        polled = api.poll({"name": "turn-final", "limit": 100})
        poll_surfaces.append(polled)
        assert polled["ok"] is True
        if not polled["items"]:
            break
        assert len(polled["items"]) == 1
        item = polled["items"][0]
        job_items.append(item)
        acknowledged = api.ack({"name": "turn-final", "ref": item["ref"]})
        assert acknowledged["ok"] is True, acknowledged
        ack_surfaces.append(acknowledged)
    assert len(job_items) == len(pages)
    assert poll_surfaces[-1]["items"] == []

    with sqlite3.connect(str(db_path)) as conn:
        canonical_row = conn.execute(
            """
            SELECT assistant_final_text, final_char_length, final_byte_length
            FROM turn_content_revisions
            WHERE host_id = ? AND turn_id = ? AND content_revision = ?
            """,
            (host_id, turn["id"], revision),
        ).fetchone()
        stored_turn = json.loads(
            conn.execute(
                "SELECT payload_json FROM turns WHERE host_id = ? AND turn_id = ?",
                (host_id, turn["id"]),
            ).fetchone()[0]
        )
        stored_plans = conn.execute(
            """
            SELECT name, plan_token, turn_id, content_revision,
                   presentation_version, part_count, state
            FROM turn_presentation_plans
            WHERE host_id = ?
            """,
            (host_id,),
        ).fetchall()
        stored_jobs = conn.execute(
            """
            SELECT operation, part_ordinal, sequence_index, outbox_id, spans_json
            FROM turn_presentation_jobs
            WHERE plan_id = (
                SELECT id FROM turn_presentation_plans
                WHERE host_id = ? AND plan_token = ?
            )
            ORDER BY sequence_index
            """,
            (host_id, plan_token),
        ).fetchall()
        stored_outbox = [
            json.loads(row[0])
            for row in conn.execute(
                """
                SELECT payload_json FROM connector_outbox
                WHERE host_id = ? AND connector = 'turn-final'
                ORDER BY id
                """,
                (host_id,),
            )
        ]

    assert canonical_row == (expected, len(expected), len(expected.encode("utf-8")))
    assert "assistant_final_text" not in stored_turn
    assert len(stored_plans) == 1
    assert len(stored_jobs) == len(pages)
    assert len(stored_outbox) == len(pages) + 1

    clean_surfaces = (
        listed,
        pages,
        begun,
        prepared_parts,
        committed,
        poll_surfaces,
        job_items,
        ack_surfaces,
        canonical_row,
        stored_turn,
        stored_plans,
        stored_jobs,
        stored_outbox,
    )
    for surface in clean_surfaces:
        _assert_sentinels_absent(surface, {"boundary_secret": private_secret})
