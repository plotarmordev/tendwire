from __future__ import annotations

import sys
import threading
from pathlib import Path

import pytest

from tendwire.backends.acp_client import (
    AcpCapabilityError,
    AcpClient,
    AcpRequestTimeoutError,
    ClientState,
)
from tendwire.backends.acp_protocol import AcpProtocolError, SessionUpdateKind, StopReason


FAKE_AGENT = Path(__file__).parent / "fixtures" / "acp_fake_agent.py"


def client(mode: str = "normal", **kwargs: object) -> AcpClient:
    return AcpClient([sys.executable, "-u", str(FAKE_AGENT), mode], **kwargs)


def test_initialize_capabilities_and_session_lifecycle() -> None:
    with client() as acp:
        initialized = acp.initialize()
        assert initialized.protocol_version == 1
        assert initialized.agent_info == {"name": "fake", "version": "1.0"}
        assert initialized.capabilities.load_session
        assert initialized.capabilities.session_list
        assert initialized.capabilities.session_resume
        assert acp.state is ClientState.INITIALIZED

        created = acp.new_session(
            "/tmp/project",
            additional_directories=["/tmp/other"],
        )
        assert created.session_id == "s-new"
        assert created.modes == {"currentModeId": "default"}
        streamed = acp.next_update(timeout=1)
        assert streamed.update_kind is SessionUpdateKind.AGENT_MESSAGE_CHUNK

        loaded = acp.load_session("s1", "/tmp/project")
        resumed = acp.resume_session("s1", "/tmp/project")
        assert loaded.session_id == "s1"
        assert resumed.config_options[0]["id"] == "model"

        first = acp.list_sessions(cwd="/tmp/project")
        assert first.sessions[0].session_id == "s1"
        assert first.next_cursor == "page-2"
        second = acp.list_sessions(cursor=first.next_cursor)
        assert second.sessions[0].title == "second"
        assert second.next_cursor is None

        acp.initialized()
        assert acp.next_notification(timeout=1).method == "fake/initialized_seen"

    assert acp.state is ClientState.CLOSED
    assert acp.exit is not None
    assert acp.exit.returncode == 0


def test_prompt_stream_and_permission_response_can_run_concurrently() -> None:
    with client() as acp:
        acp.initialize()
        acp.new_session("/tmp/project")
        # Drain the new-session message update.
        acp.next_update(timeout=1)

        outcome: list[object] = []
        failure: list[BaseException] = []

        def run_prompt() -> None:
            try:
                outcome.append(acp.prompt("s-new", "please inspect"))
            except BaseException as exc:  # pragma: no cover - diagnostic path
                failure.append(exc)

        thread = threading.Thread(target=run_prompt)
        thread.start()
        thought = acp.next_update(timeout=1)
        assert thought.update_kind is SessionUpdateKind.AGENT_THOUGHT_CHUNK
        permission = acp.next_permission_request(timeout=1)
        assert permission.options[0].option_id == "allow"
        acp.respond_permission(permission.request_id, option_id="allow")
        plan = acp.next_update(timeout=1)
        assert plan.update_kind is SessionUpdateKind.PLAN
        thread.join(timeout=2)

        assert not thread.is_alive()
        assert not failure
        assert outcome[0].stop_reason is StopReason.END_TURN


def test_cancel_resolves_pending_permissions_as_cancelled() -> None:
    with client() as acp:
        acp.initialize()
        result: list[object] = []
        thread = threading.Thread(target=lambda: result.append(acp.prompt("s1", "wait")))
        thread.start()
        acp.next_update(timeout=1)
        acp.next_permission_request(timeout=1)
        acp.cancel("s1")
        acp.next_update(timeout=1)
        thread.join(timeout=2)
        assert not thread.is_alive()
        assert result[0].stop_reason is StopReason.CANCELLED


def test_optional_methods_require_advertised_capabilities() -> None:
    with client("baseline") as acp:
        acp.initialize()
        with pytest.raises(AcpCapabilityError):
            acp.list_sessions()
        with pytest.raises(AcpCapabilityError):
            acp.load_session("s1", "/tmp")


def test_request_timeout_does_not_poison_transport() -> None:
    with client("slow", request_timeout=0.05) as acp:
        acp.initialize(timeout=1)
        with pytest.raises(AcpRequestTimeoutError):
            acp.list_sessions()
        assert acp.state is ClientState.INITIALIZED


@pytest.mark.parametrize("mode", ["malformed", "oversize"])
def test_malformed_or_oversized_stdout_fails_connection(mode: str) -> None:
    with client(mode, max_frame_bytes=1024) as acp:
        with pytest.raises(AcpProtocolError):
            acp.initialize(timeout=1)
        assert acp.state is ClientState.FAILED


def test_absolute_session_paths_are_enforced_before_write() -> None:
    with client() as acp:
        acp.initialize()
        with pytest.raises(ValueError, match="absolute"):
            acp.new_session("relative/path")
