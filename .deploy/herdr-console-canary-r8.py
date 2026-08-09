#!/usr/bin/python3 -I
"""Isolated disposable-PTY release canary for the r8 Herdr ACP console.

The canary never starts or addresses the installed Herdr server.  A private
newline-JSON Unix socket emulates only the ping and ACP-console exchange
surface needed to exercise the candidate CLI.  All candidate configuration,
state, sockets, and PTYs live below a freshly-created temporary directory.
"""

from __future__ import annotations

import argparse
import contextlib
import dataclasses
import fcntl
import hashlib
import json
import os
import pty
import selectors
import shutil
import signal
import socket
import stat
import subprocess
import tempfile
import termios
import threading
import time
from pathlib import Path
from typing import Any, Iterable, Sequence


RESET = (
    b"\x1b[?2026l\x1b[0m\x1b[?25h\x1b[?1l\x1b[?7h"
    b"\x1b[?1000l\x1b[?1002l\x1b[?1003l\x1b[?1004l"
    b"\x1b[?1005l\x1b[?1006l\x1b[?1015l\x1b[?1016l"
    b"\x1b[?2004l\x1b[>4;0m\x1b[<8u\x1b[=0u\x1b>"
)
RESET_PREFIX = RESET + b"\x1b[?1049l" + RESET
HEX64 = frozenset("0123456789abcdef")
MAX_PROTECTED_ENTRIES = 20_000
MAX_PROTECTED_BYTES = 128 * 1024 * 1024
SAFE_BASE_PATH = "/home/smith/.local/bin:/usr/local/bin:/usr/bin:/bin"
PRIVATE_TARGET = "r8-private-canary"
PRIVATE_GENERATION = 1
PRIVATE_LEASE = "r8-private-lease"


class CanaryFailure(RuntimeError):
    """A release-gating canary assertion failed."""


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1_048_576), b""):
            digest.update(block)
    return digest.hexdigest()


def _validate_sha256(value: str, label: str) -> str:
    if len(value) != 64 or any(character not in HEX64 for character in value):
        raise CanaryFailure(f"{label} is not a lowercase SHA-256 digest")
    return value


def _validate_git_object(value: str, label: str) -> str:
    if len(value) != 40 or any(character not in HEX64 for character in value):
        raise CanaryFailure(f"{label} is not a full lowercase Git object id")
    return value


def regular_executable(path: Path, expected_sha256: str, label: str) -> dict[str, Any]:
    expected_sha256 = _validate_sha256(expected_sha256, f"{label} SHA-256")
    metadata = path.lstat()
    if (
        not stat.S_ISREG(metadata.st_mode)
        or metadata.st_uid != os.geteuid()
        or metadata.st_gid != os.getegid()
        or metadata.st_nlink != 1
        or stat.S_IMODE(metadata.st_mode) != 0o555
    ):
        raise CanaryFailure(f"{label} is not an immutable owner-controlled executable")
    actual = sha256_file(path)
    if actual != expected_sha256:
        raise CanaryFailure(f"{label} SHA-256 mismatch")
    return {
        "path": str(path),
        "sha256": actual,
        "size": metadata.st_size,
        "mode": 0o555,
        "uid": metadata.st_uid,
        "gid": metadata.st_gid,
        "link_count": metadata.st_nlink,
    }


def _fingerprint_entry(digest: Any, root: Path, path: Path, budget: list[int]) -> None:
    relative = "." if path == root else path.relative_to(root).as_posix()
    metadata = path.lstat()
    mode = stat.S_IMODE(metadata.st_mode)
    budget[0] += 1
    if budget[0] > MAX_PROTECTED_ENTRIES:
        raise CanaryFailure("protected-path fingerprint entry budget exceeded")
    if stat.S_ISLNK(metadata.st_mode):
        row = [relative, "link", mode, metadata.st_uid, metadata.st_gid, os.readlink(path)]
    elif stat.S_ISDIR(metadata.st_mode):
        row = [relative, "dir", mode, metadata.st_uid, metadata.st_gid, ""]
    elif stat.S_ISREG(metadata.st_mode):
        budget[1] += metadata.st_size
        if budget[1] > MAX_PROTECTED_BYTES:
            raise CanaryFailure("protected-path fingerprint byte budget exceeded")
        row = [relative, "file", mode, metadata.st_uid, metadata.st_gid, sha256_file(path)]
    elif stat.S_ISSOCK(metadata.st_mode):
        row = [relative, "socket", mode, metadata.st_uid, metadata.st_gid, ""]
    else:
        row = [relative, "other", mode, metadata.st_uid, metadata.st_gid, ""]
    digest.update(json.dumps(row, separators=(",", ":"), ensure_ascii=True).encode() + b"\n")


def protected_fingerprint(paths: Sequence[Path]) -> dict[str, Any]:
    digest = hashlib.sha256()
    budget = [0, 0]
    normalized: list[str] = []
    for raw in sorted(paths, key=lambda item: os.fsencode(str(item.absolute()))):
        path = raw.absolute()
        normalized.append(str(path))
        digest.update(json.dumps([str(path)], separators=(",", ":")).encode() + b"\n")
        if not path.exists() and not path.is_symlink():
            digest.update(b"absent\n")
            continue
        _fingerprint_entry(digest, path, path, budget)
        if path.is_dir() and not path.is_symlink():
            for child in sorted(path.rglob("*"), key=lambda item: item.relative_to(path).as_posix()):
                _fingerprint_entry(digest, path, child, budget)
    return {
        "sha256": digest.hexdigest(),
        "paths": normalized,
        "entries": budget[0],
        "regular_bytes": budget[1],
    }


def atomic_evidence(path: Path, value: dict[str, Any]) -> None:
    if path.exists() or path.is_symlink():
        raise CanaryFailure("canary evidence path already exists")
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o755)
    temporary = path.with_name(f".{path.name}.tmp.{os.getpid()}")
    descriptor = os.open(
        temporary,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC,
        0o444,
    )
    try:
        payload = (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()
        view = memoryview(payload)
        while view:
            written = os.write(descriptor, view)
            if written < 1:
                raise OSError("canary evidence write made no progress")
            view = view[written:]
        os.fchmod(descriptor, 0o444)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    os.replace(temporary, path)
    directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


@dataclasses.dataclass
class ExchangeState:
    version: str
    protocol: int
    mode: str = "normal"
    inputs: list[str] = dataclasses.field(default_factory=list)
    requests: int = 0
    ambiguous_accepts: int = 0
    backpressure_issued: bool = False
    expected_process_group: int | None = None
    lock: threading.Lock = dataclasses.field(default_factory=threading.Lock)

    def snapshot_inputs(self) -> list[str]:
        with self.lock:
            return list(self.inputs)


class PrivateExchangeServer:
    def __init__(self, socket_path: Path, state: ExchangeState):
        self.socket_path = socket_path
        self.state = state
        self._stop = threading.Event()
        self._ready = threading.Event()
        self._failure: BaseException | None = None
        self._listener: socket.socket | None = None
        self._thread = threading.Thread(target=self._run, name="r8-herdr-canary-server", daemon=True)

    def __enter__(self) -> "PrivateExchangeServer":
        self._thread.start()
        if not self._ready.wait(2):
            raise CanaryFailure("private exchange server did not become ready")
        if self._failure is not None:
            raise CanaryFailure(f"private exchange server failed: {self._failure}")
        return self

    def __exit__(self, *_exc: object) -> None:
        self._stop.set()
        if self._listener is not None:
            with contextlib.suppress(OSError):
                self._listener.close()
        with contextlib.suppress(OSError):
            probe = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            try:
                probe.connect(str(self.socket_path))
            finally:
                probe.close()
        self._thread.join(3)
        if self._thread.is_alive():
            raise CanaryFailure("private exchange server did not stop")
        with contextlib.suppress(FileNotFoundError):
            self.socket_path.unlink()
        if self._failure is not None:
            raise CanaryFailure(f"private exchange server failed: {self._failure}")

    def _run(self) -> None:
        try:
            listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            self._listener = listener
            listener.bind(str(self.socket_path))
            listener.listen(8)
            listener.settimeout(0.2)
            self._ready.set()
            while not self._stop.is_set():
                try:
                    connection, _ = listener.accept()
                except TimeoutError:
                    continue
                except OSError:
                    if self._stop.is_set():
                        return
                    raise
                with connection:
                    self._handle(connection)
        except BaseException as error:  # surfaced synchronously by __exit__
            self._failure = error
            self._ready.set()

    @staticmethod
    def _read_line(connection: socket.socket) -> dict[str, Any]:
        connection.settimeout(3)
        body = bytearray()
        while not body.endswith(b"\n"):
            block = connection.recv(65_536)
            if not block:
                raise CanaryFailure("private exchange request ended before newline")
            body.extend(block)
            if len(body) > 1_048_576:
                raise CanaryFailure("private exchange request exceeded limit")
        value = json.loads(body)
        if not isinstance(value, dict):
            raise CanaryFailure("private exchange request is not an object")
        return value

    @staticmethod
    def _send(connection: socket.socket, value: dict[str, Any]) -> None:
        connection.sendall(json.dumps(value, separators=(",", ":")).encode() + b"\n")

    def _handle(self, connection: socket.socket) -> None:
        request = self._read_line(connection)
        request_id = request.get("id")
        method = request.get("method")
        if not isinstance(request_id, str):
            raise CanaryFailure("private exchange request id is invalid")
        if method == "ping":
            self._send(
                connection,
                {
                    "id": request_id,
                    "result": {
                        "type": "pong",
                        "version": self.state.version,
                        "protocol": self.state.protocol,
                        "capabilities": None,
                    },
                },
            )
            return
        if method != "agent.acp_console_exchange":
            raise CanaryFailure(f"unexpected private exchange method: {method!r}")
        params = request.get("params")
        if not isinstance(params, dict):
            raise CanaryFailure("private exchange params are invalid")
        process_id = params.get("process_id")
        deadline = time.monotonic() + 1
        expected_process_group = None
        while expected_process_group is None and time.monotonic() < deadline:
            with self.state.lock:
                expected_process_group = self.state.expected_process_group
            if expected_process_group is None:
                time.sleep(0.01)
        if (
            params.get("target") != PRIVATE_TARGET
            or type(params.get("generation")) is not int
            or params.get("generation") != PRIVATE_GENERATION
            or params.get("lease") != PRIVATE_LEASE
            or params.get("role") != "console"
            or type(process_id) is not int
            or process_id < 2
            or type(params.get("after_input_sequence")) is not int
            or params.get("after_input_sequence") != 0
            or params.get("output") not in (None, [])
            or expected_process_group is None
        ):
            raise CanaryFailure("private exchange console identity is not exact")
        try:
            actual_process_group = os.getpgid(process_id)
        except ProcessLookupError as error:
            raise CanaryFailure("private exchange console process disappeared") from error
        if actual_process_group != expected_process_group:
            raise CanaryFailure("private exchange console process group is not isolated")
        with self.state.lock:
            self.state.requests += 1
        if self.state.mode == "lease_error":
            self._send(
                connection,
                {
                    "id": request_id,
                    "error": {
                        "code": "acp_console_lease_invalid",
                        "message": "private canary lease rejection",
                    },
                },
            )
            return
        input_text = params.get("input")
        if input_text is not None:
            if not isinstance(input_text, str):
                raise CanaryFailure("private exchange input is not text")
            with self.state.lock:
                if input_text.startswith("R8_PREWRITE_") and not self.state.backpressure_issued:
                    self.state.backpressure_issued = True
                    backpressure = True
                else:
                    backpressure = False
            if backpressure:
                self._send(
                    connection,
                    {
                        "id": request_id,
                        "error": {
                            "code": "acp_console_input_backpressure",
                            "message": "private canary one-shot backpressure",
                        },
                    },
                )
                return
            with self.state.lock:
                self.state.inputs.append(input_text)
                if input_text.startswith("R8_AMBIGUOUS_"):
                    self.state.ambiguous_accepts += 1
            if input_text.startswith("R8_AMBIGUOUS_"):
                time.sleep(2.5)
                return
        after = params.get("after_output_sequence", 0)
        if not isinstance(after, int) or after < 0:
            raise CanaryFailure("private exchange output acknowledgement is invalid")
        outputs = self._outputs()
        visible = [item for item in outputs if item["sequence"] > after]
        self._send(
            connection,
            {
                "id": request_id,
                "result": {
                    "type": "agent_acp_console_exchange",
                    "inputs": [],
                    "outputs": visible,
                    "input_floor_sequence": 1,
                    "output_floor_sequence": visible[0]["sequence"] if visible else len(outputs) + 1,
                    "next_input_sequence": 1,
                    "next_output_sequence": len(outputs) + 1,
                },
            },
        )

    def _outputs(self) -> list[dict[str, Any]]:
        raw = [
            ("assistant", "R8 visible answer\x1b[?1004h"),
            ("thought", "R8 visible thought\x1b[2J"),
            ("tool", "R8 visible tool\u009b31m"),
            ("plan", "R8 visible plan\x1b[?1004h"),
            ("status", "usage R8_PRIVATE_STATUS"),
            ("status", "permission> Allow private canary? (1=allow 2=deny)"),
            ("status", "permission selection accepted"),
            ("status", "active turn cancellation requested"),
            ("status", "turn completed"),
            ("error", "private canary safe failure"),
            ("future_private", "R8_PRIVATE_FUTURE"),
        ]
        with self.state.lock:
            accepted = any(
                item.startswith(("R8_PREWRITE_", "R8_AFTER_OVERSIZE_"))
                for item in self.state.inputs
            )
        if accepted:
            raw.extend((("assistant", "R8 private prompt accepted"), ("turn_end", "")))
        return [
            {
                "sequence": index,
                "event_id": f"r8-canary-output-{index}",
                "stream": stream,
                "text": text,
            }
            for index, (stream, text) in enumerate(raw, 1)
        ]


def private_environment(root: Path, socket_path: Path, adapter_bin_dir: Path) -> dict[str, str]:
    config_home = root / "config"
    state_home = root / "state"
    data_home = root / "data"
    temporary = root / "tmp"
    for path in (config_home, state_home, data_home, temporary):
        path.mkdir(mode=0o700)
    # Popen receives this dictionary as its complete environment (env -i
    # semantics).  In particular, HOME and every ambient credential-bearing
    # variable are absent; explicit XDG/config roots below keep all Herdr state
    # private without repurposing the caller's HOME.
    environment = {
        key: os.environ[key]
        for key in ("LANG", "LC_ALL", "LC_CTYPE", "TERM", "COLORTERM")
        if key in os.environ
    }
    environment.update(
        {
            "HERDR_SOCKET_PATH": str(socket_path),
            "HERDR_CONFIG_PATH": str(config_home / "config.toml"),
            "XDG_CONFIG_HOME": str(config_home),
            "XDG_STATE_HOME": str(state_home),
            "XDG_DATA_HOME": str(data_home),
            "TMPDIR": str(temporary),
            "PATH": f"{adapter_bin_dir}:{SAFE_BASE_PATH}",
        }
    )
    return environment


def _read_available(master: int, captured: bytearray, timeout: float) -> None:
    selector = selectors.DefaultSelector()
    selector.register(master, selectors.EVENT_READ)
    deadline = time.monotonic() + timeout
    try:
        while time.monotonic() < deadline:
            events = selector.select(min(0.1, max(0.0, deadline - time.monotonic())))
            if not events:
                continue
            try:
                block = os.read(master, 65_536)
            except BlockingIOError:
                continue
            except OSError:
                return
            if not block:
                return
            captured.extend(block)
    finally:
        selector.close()


def _wait_for(master: int, captured: bytearray, needle: bytes, timeout: float) -> None:
    deadline = time.monotonic() + timeout
    while needle not in captured and time.monotonic() < deadline:
        _read_available(master, captured, min(0.2, deadline - time.monotonic()))
    if needle not in captured:
        raise CanaryFailure(f"candidate console did not emit required marker: {needle!r}")


def _terminate(process: subprocess.Popen[bytes]) -> None:
    if process.poll() is not None:
        return
    with contextlib.suppress(ProcessLookupError):
        os.killpg(process.pid, signal.SIGTERM)
    try:
        process.wait(timeout=2)
    except subprocess.TimeoutExpired:
        with contextlib.suppress(ProcessLookupError):
            os.killpg(process.pid, signal.SIGKILL)
        process.wait(timeout=2)


@contextlib.contextmanager
def console_process(
    herdr: Path,
    environment: dict[str, str],
    shell_sentinel: Path,
    state: ExchangeState,
) -> Iterable[tuple[subprocess.Popen[bytes], int]]:
    wrapper = shell_sentinel.with_name(f".{shell_sentinel.name}.wrapper.sh")
    wrapper.write_text(
        "#!/bin/sh\n"
        "sentinel=$1\n"
        "shift\n"
        '"$@"\n'
        "status=$?\n"
        "printf 'wrapper-resumed\\n' >\"$sentinel\"\n"
        "exit \"$status\"\n",
        encoding="utf-8",
    )
    wrapper.chmod(0o555)
    master, slave = pty.openpty()
    attributes = termios.tcgetattr(slave)
    attributes[3] |= termios.ICANON
    attributes[3] &= ~(termios.ECHO | termios.ECHONL)
    termios.tcsetattr(slave, termios.TCSANOW, attributes)
    process = subprocess.Popen(
        [
            "/bin/sh",
            str(wrapper),
            str(shell_sentinel),
            str(herdr),
            "agent",
            "acp-console",
            PRIVATE_TARGET,
            "--generation",
            str(PRIVATE_GENERATION),
            "--lease",
            PRIVATE_LEASE,
        ],
        stdin=slave,
        stdout=slave,
        stderr=slave,
        env=environment,
        close_fds=True,
        start_new_session=True,
    )
    os.close(slave)
    with state.lock:
        state.expected_process_group = process.pid
    flags = fcntl.fcntl(master, fcntl.F_GETFL)
    fcntl.fcntl(master, fcntl.F_SETFL, flags | os.O_NONBLOCK)
    try:
        yield process, master
    finally:
        _terminate(process)
        with contextlib.suppress(OSError):
            os.close(master)


def _console_command(herdr: Path) -> list[str]:
    return [
        str(herdr), "agent", "acp-console", PRIVATE_TARGET,
        "--generation", str(PRIVATE_GENERATION), "--lease", PRIVATE_LEASE,
    ]


def run_dialogue_scenario(
    herdr: Path,
    environment: dict[str, str],
    socket_path: Path,
    version: str,
    protocol: int,
    token: str,
) -> dict[str, Any]:
    state = ExchangeState(version=version, protocol=protocol)
    captured = bytearray()
    shell_sentinel_path = socket_path.with_name("dialogue-shell-resumed")
    first_server = PrivateExchangeServer(socket_path, state)
    first_server.__enter__()
    try:
        with console_process(herdr, environment, shell_sentinel_path, state) as (process, master):
            _wait_for(master, captured, b"Tendwire ACP session", 3)
            _wait_for(master, captured, b"permission> Allow private canary?", 3)
            baseline = state.snapshot_inputs()
            os.write(master, b"\x1b[I\n\x1b[99;5:1u\n\n")
            time.sleep(0.5)
            if state.snapshot_inputs() != baseline:
                raise CanaryFailure("terminal reports reached the private ACP input queue")

            first_server.__exit__(None, None, None)
            first_server = None
            prewrite = f"R8_PREWRITE_{token}"
            os.write(master, prewrite.encode() + b"\n")
            diagnostic = b"[status] pane input was not sent; retrying safely"
            _wait_for(master, captured, diagnostic, 3)
            if bytes(captured).count(diagnostic) != 1:
                raise CanaryFailure("prewrite retry diagnostic was not emitted exactly once")
            with PrivateExchangeServer(socket_path, state):
                deadline = time.monotonic() + 3
                while prewrite not in state.snapshot_inputs() and time.monotonic() < deadline:
                    _read_available(master, captured, 0.1)
                if state.snapshot_inputs().count(prewrite) != 1:
                    raise CanaryFailure("prewrite private prompt was not accepted exactly once")
                if not state.backpressure_issued:
                    raise CanaryFailure("prewrite prompt did not exercise one-shot backpressure")
                _wait_for(master, captured, b"R8 private prompt accepted", 3)

                ambiguous = f"R8_AMBIGUOUS_{token}"
                os.write(master, ambiguous.encode() + b"\n")
                _wait_for(master, captured, b"pane was locked to prevent a duplicate prompt", 4)
                sentinel = f"R8_SHELL_SENTINEL_{token}"
                os.write(master, sentinel.encode() + b"\n")
                _wait_for(master, captured, b"input ignored: ACP session is disconnected", 2)
                if state.snapshot_inputs().count(ambiguous) != 1 or sentinel in state.snapshot_inputs():
                    raise CanaryFailure("ambiguous acknowledgement was retried or shell sentinel escaped")
                if process.poll() is not None or shell_sentinel_path.exists():
                    raise CanaryFailure("candidate console exited before explicit disconnected /exit")
                os.write(master, b"/exit\n")
                try:
                    return_code = process.wait(timeout=2)
                except subprocess.TimeoutExpired as error:
                    raise CanaryFailure("candidate console ignored explicit disconnected /exit") from error
                _read_available(master, captured, 0.2)
    finally:
        if first_server is not None:
            first_server.__exit__(None, None, None)
    if shell_sentinel_path.read_text(encoding="utf-8") != "wrapper-resumed\n":
        raise CanaryFailure("wrapper shell did not resume exactly after explicit /exit")
    output = bytes(captured)
    if not output.startswith(RESET_PREFIX):
        raise CanaryFailure("candidate console did not reset enhanced terminal modes first")
    for private in (b"R8_PRIVATE_STATUS", b"R8_PRIVATE_FUTURE"):
        if private in output:
            raise CanaryFailure("candidate console exposed a private protocol stream")
    for visible in (
        b"R8 visible answer[?1004h",
        b"[thought] R8 visible thought[2J",
        b"[tool] R8 visible tool31m",
        b"[plan] R8 visible plan[?1004h",
        b"permission selection accepted",
        b"active turn cancellation requested",
        b"private canary safe failure",
        b"R8 private prompt accepted",
    ):
        if visible not in output:
            raise CanaryFailure(f"candidate console hid required visible output: {visible!r}")
    if b"\x1b[?1004h" in output[len(RESET_PREFIX):]:
        raise CanaryFailure("candidate dialogue output retained a terminal control sequence")
    if return_code != 1:
        raise CanaryFailure("disconnected console /exit did not return the safe failure status")
    return {
        "terminal_reset_prefix": True,
        "terminal_reports_rejected": True,
        "prewrite_retry_exactly_once": True,
        "backpressure_retry_exactly_once": True,
        "known_agent_streams_visible": True,
        "private_bookkeeping_hidden": True,
        "actionable_statuses_visible": True,
        "dialogue_controls_sanitized": True,
        "ambiguous_ack_accepted_once": state.ambiguous_accepts == 1,
        "ambiguous_ack_not_retried": state.snapshot_inputs().count(f"R8_AMBIGUOUS_{token}") == 1,
        "shell_fallthrough_blocked": True,
        "explicit_exit_status": return_code,
    }


def run_lease_scenario(
    herdr: Path,
    environment: dict[str, str],
    socket_path: Path,
    version: str,
    protocol: int,
    token: str,
) -> dict[str, Any]:
    state = ExchangeState(version=version, protocol=protocol, mode="lease_error")
    captured = bytearray()
    shell_sentinel_path = socket_path.with_name("lease-shell-resumed")
    with PrivateExchangeServer(socket_path, state), console_process(
        herdr, environment, shell_sentinel_path, state
    ) as (process, master):
        _wait_for(master, captured, b"ACP session was disconnected", 3)
        sentinel = f"R8_LEASE_SENTINEL_{token}"
        os.write(master, sentinel.encode() + b"\n")
        _wait_for(master, captured, b"input ignored: ACP session is disconnected", 2)
        if state.snapshot_inputs() or process.poll() is not None or shell_sentinel_path.exists():
            raise CanaryFailure("invalid lease exposed the shell or accepted pane input")
        os.write(master, b"/exit\n")
        try:
            return_code = process.wait(timeout=2)
        except subprocess.TimeoutExpired as error:
            raise CanaryFailure("invalid-lease console ignored explicit /exit") from error
    if return_code != 1 or shell_sentinel_path.read_text(encoding="utf-8") != "wrapper-resumed\n":
        raise CanaryFailure("invalid-lease console returned an unsafe status")
    return {"invalid_lease_locked": True, "shell_fallthrough_blocked": True}


def run_eof_scenario(
    herdr: Path,
    environment: dict[str, str],
    socket_path: Path,
    version: str,
    protocol: int,
) -> dict[str, Any]:
    state = ExchangeState(version=version, protocol=protocol, mode="lease_error")
    with PrivateExchangeServer(socket_path, state):
        process = subprocess.Popen(
            _console_command(herdr),
            stdin=subprocess.PIPE,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            env=environment,
            close_fds=True,
            start_new_session=True,
        )
        with state.lock:
            state.expected_process_group = process.pid
        try:
            if process.stdin is None:
                raise CanaryFailure("EOF canary stdin was not created")
            process.stdin.close()
            time.sleep(1)
            if process.poll() is not None:
                raise CanaryFailure("stdin EOF exposed the wrapper shell")
        finally:
            _terminate(process)
    return {"stdin_eof_held": True, "shell_fallthrough_blocked": True}


def run_oversize_scenario(
    herdr: Path,
    environment: dict[str, str],
    socket_path: Path,
    version: str,
    protocol: int,
    token: str,
) -> dict[str, Any]:
    state = ExchangeState(version=version, protocol=protocol)
    captured = bytearray()
    master, slave = pty.openpty()
    process: subprocess.Popen[bytes] | None = None
    try:
        with PrivateExchangeServer(socket_path, state):
            process = subprocess.Popen(
                _console_command(herdr),
                stdin=subprocess.PIPE,
                stdout=slave,
                stderr=slave,
                env=environment,
                close_fds=True,
                start_new_session=True,
            )
            os.close(slave)
            slave = -1
            with state.lock:
                state.expected_process_group = process.pid
            flags = fcntl.fcntl(master, fcntl.F_GETFL)
            fcntl.fcntl(master, fcntl.F_SETFL, flags | os.O_NONBLOCK)
            _wait_for(master, captured, b"Tendwire ACP session", 3)
            if process.stdin is None:
                raise CanaryFailure("oversize canary stdin was not created")
            process.stdin.write(b"x" * 16_385 + b"\n")
            process.stdin.flush()
            _wait_for(master, captured, b"input exceeds 4096 characters", 3)
            accepted = f"R8_AFTER_OVERSIZE_{token}"
            process.stdin.write(accepted.encode() + b"\n")
            process.stdin.flush()
            deadline = time.monotonic() + 3
            while accepted not in state.snapshot_inputs() and time.monotonic() < deadline:
                _read_available(master, captured, 0.1)
            if state.snapshot_inputs() != [accepted]:
                raise CanaryFailure("oversize input was not drained before the next input")
            _wait_for(master, captured, b"R8 private prompt accepted", 3)
    finally:
        if process is not None:
            _terminate(process)
        if slave >= 0:
            os.close(slave)
        with contextlib.suppress(OSError):
            os.close(master)
    return {
        "oversize_input_rejected": True,
        "post_oversize_input_exactly_once": True,
    }


def reported_version(herdr: Path, environment: dict[str, str]) -> str:
    completed = subprocess.run(
        [str(herdr), "--version"],
        check=True,
        capture_output=True,
        text=True,
        timeout=5,
        env=environment,
    )
    value = completed.stdout.strip()
    if not value.startswith("herdr ") or "\n" in value:
        raise CanaryFailure("candidate Herdr reported an invalid version")
    return value.removeprefix("herdr ")


def run(args: argparse.Namespace) -> dict[str, Any]:
    herdr = args.herdr.absolute()
    adapter_dir = args.adapter_bin_dir.resolve(strict=True)
    evidence = args.evidence.absolute()
    commit = _validate_git_object(args.expected_herdr_commit, "Herdr commit")
    tree = _validate_git_object(args.expected_herdr_tree, "Herdr tree")
    herdr_info = regular_executable(herdr, args.expected_herdr_sha256, "candidate Herdr")
    adapter = adapter_dir / "codex-acp"
    adapter_info = regular_executable(adapter.resolve(strict=True), args.expected_adapter_sha256, "ACP adapter")
    protected = [path.absolute() for path in args.protect_path]
    if not protected:
        raise CanaryFailure("at least one production path must be fingerprinted")
    if any(
        evidence == path or evidence.is_relative_to(path) or path.is_relative_to(evidence)
        for path in protected
    ):
        raise CanaryFailure("canary evidence path overlaps a protected production path")
    before = protected_fingerprint(protected)
    token = hashlib.sha256(f"{args.release_id}:{commit}:{tree}:{herdr_info['sha256']}".encode()).hexdigest()[:20]
    temporary_path: Path | None = None
    with tempfile.TemporaryDirectory(prefix="herdr-console-r8-") as raw_root:
        root = Path(raw_root)
        temporary_path = root
        if any(root == path or root.is_relative_to(path) or path.is_relative_to(root) for path in protected):
            raise CanaryFailure("private canary root overlaps a protected production path")
        dialogue_socket = root / "dialogue.sock"
        lease_socket = root / "lease.sock"
        eof_socket = root / "eof.sock"
        oversize_socket = root / "oversize.sock"
        environment = private_environment(root, dialogue_socket, adapter_dir)
        version = reported_version(herdr, environment)
        if version != args.expected_herdr_version:
            raise CanaryFailure("candidate Herdr reported-version mismatch")
        dialogue = run_dialogue_scenario(
            herdr, environment, dialogue_socket, version, args.expected_protocol, token
        )
        environment["HERDR_SOCKET_PATH"] = str(lease_socket)
        lease = run_lease_scenario(
            herdr, environment, lease_socket, version, args.expected_protocol, token
        )
        environment["HERDR_SOCKET_PATH"] = str(eof_socket)
        eof = run_eof_scenario(herdr, environment, eof_socket, version, args.expected_protocol)
        environment["HERDR_SOCKET_PATH"] = str(oversize_socket)
        oversize = run_oversize_scenario(
            herdr, environment, oversize_socket, version, args.expected_protocol, token
        )
        if any(
            path.exists() or path.is_symlink()
            for path in (dialogue_socket, lease_socket, eof_socket, oversize_socket)
        ):
            raise CanaryFailure("private canary socket survived its scenario")
    if temporary_path is None or temporary_path.exists():
        raise CanaryFailure("private canary root was not removed")
    after = protected_fingerprint(protected)
    if after != before:
        raise CanaryFailure("a protected production path changed during the private canary")
    value = {
        "schema_version": 1,
        "canary": "herdr_acp_console_r8",
        "release_id": args.release_id,
        "valid": True,
        "isolated": True,
        "historical_recovery": False,
        "production_fingerprint": before,
        "production_unchanged": True,
        "temporary_root_removed": True,
        "herdr": {
            **herdr_info,
            "commit": commit,
            "tree": tree,
            "reported_version": args.expected_herdr_version,
            "protocol": args.expected_protocol,
        },
        "adapter": {**adapter_info, "bin_dir": str(adapter_dir)},
        "checks": {**dialogue, **lease, **eof, **oversize},
        "secret_values_emitted": False,
    }
    atomic_evidence(evidence, value)
    return value


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--herdr", type=Path, required=True)
    parser.add_argument("--adapter-bin-dir", type=Path, required=True)
    parser.add_argument("--expected-herdr-sha256", required=True)
    parser.add_argument("--expected-adapter-sha256", required=True)
    parser.add_argument("--expected-herdr-commit", required=True)
    parser.add_argument("--expected-herdr-tree", required=True)
    parser.add_argument("--expected-herdr-version", required=True)
    parser.add_argument("--expected-protocol", type=int, required=True)
    parser.add_argument("--release-id", required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--protect-path", type=Path, action="append", required=True)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    if args.expected_protocol < 1:
        raise CanaryFailure("expected protocol must be positive")
    value = run(args)
    print(
        json.dumps(
            {
                "schema_version": value["schema_version"],
                "canary": value["canary"],
                "release_id": value["release_id"],
                "valid": True,
                "evidence_sha256": sha256_file(args.evidence),
            },
            sort_keys=True,
            separators=(",", ":"),
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
