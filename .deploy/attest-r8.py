#!/usr/bin/python3 -I
"""Attest installed r8 bytes, effective units, and live process paths."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import stat
import subprocess
from pathlib import Path


RELEASE_ID = "67568f32-0b94403-f50af73-r8"
RELEASE = Path("/home/smith/.local/share/acp-runtime/releases") / RELEASE_ID
RUNTIME = Path("/home/smith/.local/share/tendwire-runtime/acp-0b94403-f50af73-r8")
ACTIVE = Path("/home/smith/.local/share/acp-runtime/active")
MANIFEST = Path("/home/smith/.local/share/acp-runtime/manifests") / f"{RELEASE_ID}.json"
PRIVATE_ENV = Path("/home/smith/.config/herdres/frozen-0b94403-f50af73-r8.env")
CODEX_ACP = Path(
    "/home/smith/.local/share/acp-adapters/"
    "codex-acp-7cb0524624f2e730f48c3dac9b547ca130964ae9"
)
CODEX_ACP_BUNDLE_SHA256 = "ab2dcb6ab0866775895b245a46069baba357eda07ba98b818ab215d70c2dbf60"
CODEX_ACP_PROVENANCE_SHA256 = "a5135f4d0fbbddc0d0bfd423454ded4321753439b240c55d81c260f0ac32b7d1"
HERDR_ROOT = Path(
    "/home/smith/.local/share/herdr-runtime/acp-67568f327cea67bdd1d197feff621c45476da84c"
)
HERDR_BINARY_SHA256 = "9be82154a29ec730dd8ce3a4c063e12847cadb0c1c81fa4c64782057cd4137f0"
HERDR_PROVENANCE_SHA256 = "5c41a0e5b5ba5d26eb1f4229bde8f8f7fae1ff7316b8d809bd1239bb1f4a9b04"
TENDWIRE_REVIEWED_ANCESTOR = "1f277cd562f3852cfbec4e52b2ffc8c406550fc4"
HERDRES_REVIEWED_ANCESTOR = "36599949daa64f68494d04f96a3bfee31904a804"
HERDR_REVIEWED_ANCESTOR = "9026d9bc5a12d9adc2d9f68ebdc564133e4098b4"
TRANSACTION = Path("/home/smith/.local/state/acp-cutover/frozen-0b94403-r8")
ATTESTATION = TRANSACTION / "installed-config-attestation.json"
DROPINS = {
    "herdr-server.service": Path(
        "/home/smith/.config/systemd/user/herdr-server.service.d/99-codex-acp-r8.conf"
    ),
    "tendwired.service": Path(
        "/home/smith/.config/systemd/user/tendwired.service.d/99-frozen-acp-release.conf"
    ),
    "herdres.service": Path(
        "/home/smith/.config/systemd/user/herdres.service.d/99-frozen-acp-release.conf"
    ),
    "herdres-gateway.service": Path(
        "/home/smith/.config/systemd/user/herdres-gateway.service.d/99-frozen-acp-release.conf"
    ),
}
WRITER_UNITS = (
    "tendwired.service",
    "herdres.service",
    "herdres-gateway.service",
)
UNIT_FILES = {
    "acp-frozen-live-monitor.service": Path(
        "/home/smith/.config/systemd/user/acp-frozen-live-monitor.service"
    ),
    "acp-r8-release-guard.service": Path(
        "/home/smith/.config/systemd/user/acp-r8-release-guard.service"
    ),
    "acp-r8-rollback.service": Path(
        "/home/smith/.config/systemd/user/acp-r8-rollback.service"
    ),
    "acp-r8-rollout.service": Path(
        "/home/smith/.config/systemd/user/acp-r8-rollout.service"
    ),
}
EXPECTED_KEYS = {
    "HERDRES_ENV_FILE",
    "HERDRES_INGRESS_PATH",
    "HERDRES_STATE_PATH",
    "HERDRES_TENDWIRE_MODE",
    "TELEGRAM_BOT_TOKEN",
    "TELEGRAM_BOT_USERNAME",
    "TELEGRAM_CHAT_ID",
    "TELEGRAM_CODEX_BOT_TOKEN",
    "TELEGRAM_CODEX_BOT_USERNAME",
    "TELEGRAM_GENERAL_THREAD_ID",
    "TELEGRAM_KIMI_BOT_TOKEN",
    "TELEGRAM_KIMI_BOT_USERNAME",
    "TELEGRAM_OMP_BOT_TOKEN",
    "TELEGRAM_OMP_BOT_USERNAME",
    "TELEGRAM_OWNER_USER_IDS",
    "TENDWIRE_SOCKET_PATH",
}


def digest(path: Path) -> str:
    result = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1_048_576), b""):
            result.update(block)
    return result.hexdigest()


def safe_file(path: Path, mode: int) -> dict[str, object]:
    metadata = path.lstat()
    if (
        not stat.S_ISREG(metadata.st_mode)
        or metadata.st_uid != os.geteuid()
        or metadata.st_gid != os.getegid()
        or stat.S_IMODE(metadata.st_mode) != mode
        or metadata.st_nlink != 1
    ):
        raise RuntimeError("unsafe installed release file")
    return {
        "sha256": digest(path),
        "mode": mode,
        "size": metadata.st_size,
        "regular": True,
        "owner": os.geteuid(),
        "group": os.getegid(),
        "link_count": 1,
    }


def environment() -> dict[str, str]:
    raw = PRIVATE_ENV.read_bytes()
    if len(raw) > 65_536 or b"\0" in raw:
        raise RuntimeError("invalid private environment")
    result: dict[str, str] = {}
    for line in raw.decode().splitlines():
        if not line or line.startswith("#"):
            continue
        key, separator, value = line.partition("=")
        if not separator or key in result:
            raise RuntimeError("invalid private environment")
        result[key] = value
    if set(result) != EXPECTED_KEYS:
        raise RuntimeError("private environment key set changed")
    expected_paths = {
        "HERDRES_ENV_FILE": str(PRIVATE_ENV),
        "HERDRES_INGRESS_PATH": (
            "/home/smith/.local/share/herdres/candidates/f50af73-r8/ingress.db"
        ),
        "HERDRES_STATE_PATH": (
            "/home/smith/.local/share/herdres/candidates/f50af73-r8/state.json"
        ),
        "TENDWIRE_SOCKET_PATH": "/home/smith/.local/share/tendwire/tendwire.sock",
    }
    if any(result.get(key) != value for key, value in expected_paths.items()):
        raise RuntimeError("private environment path mismatch")
    if result.get("HERDRES_TENDWIRE_MODE") != "source":
        raise RuntimeError("Herdres contract-v3 source path is not active")
    return result


def show(unit: str, property_name: str) -> str:
    return subprocess.run(
        [
            "systemctl",
            "--user",
            "show",
            "--value",
            "-p",
            property_name,
            unit,
        ],
        check=True,
        capture_output=True,
        text=True,
        timeout=10,
    ).stdout.strip()


def words(unit: str, property_name: str) -> set[str]:
    return set(show(unit, property_name).split())


def effective_units() -> None:
    expected_commands = {
        "tendwired.service": (
            f"argv[]={RUNTIME}/bin/python -B -I -m tendwire.cli daemon "
            "--db-path /home/smith/.local/share/tendwire/candidates/0b94403-r8/tendwire.db "
            "--socket-path /home/smith/.local/share/tendwire/tendwire.sock"
        ),
        "herdres.service": (
            f"argv[]=/usr/bin/python3 -E -s {RELEASE}/herdres/herdres.py sync --loop 5"
        ),
        "herdres-gateway.service": (
            f"argv[]=/usr/bin/python3 -E -s {RELEASE}/herdres/herdres_gateway.py"
        ),
        "acp-frozen-live-monitor.service": (
            f"argv[]={RELEASE}/monitor-one-hour-strict"
        ),
        "acp-r8-release-guard.service": f"argv[]={RELEASE}/release-guard",
        "acp-r8-rollback.service": f"argv[]={RELEASE}/rollback-r8",
        "acp-r8-rollout.service": f"argv[]={RELEASE}/rollout-acp-release-r8",
    }
    expected_workdirs = {
        "tendwired.service": str(RUNTIME),
        "herdres.service": str(RELEASE / "herdres"),
        "herdres-gateway.service": str(RELEASE / "herdres"),
    }
    forbidden = (
        "9026d9bc-7446533-3659994-r5",
        "candidates/7446533",
        "candidates/3659994/",
    )
    for unit, command in expected_commands.items():
        values = {
            key: show(unit, key)
            for key in (
                "LoadState",
                "NeedDaemonReload",
                "Restart",
                "Type",
                "ExecStart",
                "WorkingDirectory",
                "EnvironmentFiles",
                "Requires",
                "Wants",
                "BindsTo",
                "After",
                "OnFailure",
                "FragmentPath",
                "DropInPaths",
                "ExecStopPost",
                "Conflicts",
                "NotifyAccess",
            )
        }
        combined = "\n".join(values.values())
        if (
            values["LoadState"] != "loaded"
            or values["NeedDaemonReload"] != "no"
            or values["Restart"] not in {"no", "on-failure"}
            or values["Type"] not in {"simple", "oneshot", "notify"}
            or command not in values["ExecStart"]
            or values["ExecStart"].count("argv[]=") != 1
            or any(token in combined for token in forbidden)
        ):
            raise RuntimeError("effective unit mismatch")
        if unit in expected_workdirs and values["WorkingDirectory"] != expected_workdirs[unit]:
            raise RuntimeError("effective working directory mismatch")
    for unit, path in DROPINS.items():
        if str(path) not in show(unit, "DropInPaths"):
            raise RuntimeError("release drop-in not effective")
    expected_adapter_path = (
        "PATH=/home/smith/.local/share/acp-adapters/codex-acp-7cb0524624f2e730f48c3dac9b547ca130964ae9/"
        "bin:/home/smith/.local/bin:/usr/local/bin:/usr/bin:/bin"
    )
    if expected_adapter_path not in show("herdr-server.service", "Environment"):
        raise RuntimeError("Herdr adapter path is not version-pinned")
    for unit, path in UNIT_FILES.items():
        if show(unit, "FragmentPath") != str(path):
            raise RuntimeError("release unit fragment is not exact")
    if show("acp-r8-release-guard.service", "Type") != "notify":
        raise RuntimeError("release guard type changed")
    if show("acp-r8-release-guard.service", "NotifyAccess") != "main":
        raise RuntimeError("release guard readiness ownership changed")
    if show("acp-r8-rollback.service", "Type") != "oneshot":
        raise RuntimeError("rollback type changed")
    if show("acp-r8-rollback.service", "TimeoutStartUSec") != "10min":
        raise RuntimeError("rollback timeout cannot cover bounded recovery")
    if show("acp-r8-rollout.service", "Type") != "oneshot":
        raise RuntimeError("rollout type changed")
    if show("acp-r8-rollout.service", "TimeoutStartUSec") != "30min":
        raise RuntimeError("rollout timeout cannot cover bounded migration")
    enabled = subprocess.run(
        ["systemctl", "--user", "is-enabled", "acp-r8-release-guard.service"],
        check=True,
        capture_output=True,
        text=True,
        timeout=10,
    ).stdout.strip()
    if enabled != "enabled":
        raise RuntimeError("release guard is not enabled")
    env_field = f"{PRIVATE_ENV} (ignore_errors=no)"
    for unit in ("herdres.service", "herdres-gateway.service"):
        if show(unit, "EnvironmentFiles") != env_field:
            raise RuntimeError("private environment is not exact")
    if "herdr-server.service" not in words("tendwired.service", "Requires"):
        raise RuntimeError("Tendwire lacks Herdr dependency")
    legacy_recovery = {
        "acp-frozen-release-recovery.service",
        "acp-cutover-recovery.service",
    }
    for unit in WRITER_UNITS:
        if (
            "acp-r8-release-guard.service" not in words(unit, "BindsTo")
            or "acp-r8-release-guard.service" not in words(unit, "After")
        ):
            raise RuntimeError("writer lacks r8 guard binding")
        live_dependencies = (
            words(unit, "Requires")
            | words(unit, "Wants")
            | words(unit, "BindsTo")
        )
        if live_dependencies & legacy_recovery:
            raise RuntimeError("writer retains a live legacy recovery dependency")
    for unit in legacy_recovery:
        if show(unit, "UnitFileState") != "disabled":
            raise RuntimeError("legacy recovery unit remains enabled")
        if show(unit, "ActiveState") != "inactive":
            raise RuntimeError("legacy recovery unit remains active")
    for stale_guard in (
        "acp-r6-release-guard.service",
        "acp-r7-release-guard.service",
    ):
        if (
            show(stale_guard, "UnitFileState") != "disabled"
            or show(stale_guard, "ActiveState") != "inactive"
        ):
            raise RuntimeError("failed predecessor release guard remains armed")
    if show("acp-cutover-run.service", "ActiveState") != "inactive":
        raise RuntimeError("legacy cutover runner remains active")
    monitor_dependencies = words("acp-frozen-live-monitor.service", "Requires")
    if not {
        "herdr-server.service",
        "tendwired.service",
        "herdres.service",
        "herdres-gateway.service",
    } <= monitor_dependencies:
        raise RuntimeError("monitor dependencies changed")
    for unit in ("acp-frozen-live-monitor.service", "acp-r8-release-guard.service"):
        if "acp-r8-rollback.service" not in words(unit, "OnFailure"):
            raise RuntimeError("rollback failure edge is absent")
        if f"{RELEASE}/monitor-exit-guard" not in show(unit, "ExecStopPost"):
            raise RuntimeError("release exit fence is absent")
    if "acp-r8-rollback.service" not in words(
        "acp-r8-rollout.service", "OnFailure"
    ):
        raise RuntimeError("rollout failure edge is absent")
    rollback_conflicts = words("acp-r8-rollback.service", "Conflicts")
    rollback_after = words("acp-r8-rollback.service", "After")
    if (
        "acp-r8-rollout.service" not in rollback_conflicts
        or "acp-r8-rollout.service" not in rollback_after
    ):
        raise RuntimeError("rollback is not ordered against rollout")
    herdr_exec = show("herdr-server.service", "ExecStart")
    if (
        "argv[]=/home/smith/.local/share/acp-runtime/active/herdr server"
        not in herdr_exec
        or herdr_exec.count("argv[]=") != 1
        or show("herdr-server.service", "NeedDaemonReload") != "no"
        or show("herdr-server.service", "ActiveState") != "active"
    ):
        raise RuntimeError("Herdr effective unit changed")


def installed_files() -> dict[str, dict[str, object]]:
    result: dict[str, dict[str, object]] = {}
    for entrypoint in (
        RELEASE / "herdres/herdres.py",
        RELEASE / "herdres/herdres_gateway.py",
    ):
        safe_file(entrypoint, 0o555)
    release_names = {
        "herdr-server.service": RELEASE / "systemd/herdr-99-acp-adapter.conf",
        "tendwired.service": RELEASE / "systemd/tendwired-99.conf",
        "herdres.service": RELEASE / "systemd/herdres-99.conf",
        "herdres-gateway.service": RELEASE / "systemd/herdres-gateway-99.conf",
        **{
            unit: RELEASE / "systemd" / path.name for unit, path in UNIT_FILES.items()
        },
    }
    for unit, installed in {**DROPINS, **UNIT_FILES}.items():
        installed_info = safe_file(installed, 0o644)
        source_info = safe_file(release_names[unit], 0o444)
        if installed_info["sha256"] != source_info["sha256"]:
            raise RuntimeError("installed unit differs from immutable release")
        result[unit] = installed_info
    result["private_environment"] = safe_file(PRIVATE_ENV, 0o600)
    result["codex_acp_bundle"] = safe_file(CODEX_ACP / "dist/index.js", 0o555)
    result["codex_acp_provenance"] = safe_file(CODEX_ACP / "provenance.json", 0o444)
    result["herdr_binary"] = safe_file(HERDR_ROOT / "herdr", 0o555)
    result["herdr_provenance"] = safe_file(HERDR_ROOT / "provenance.json", 0o444)
    result["herdr_console_canary_evidence"] = safe_file(
        RELEASE / "herdr-console-canary-evidence.json", 0o444
    )
    result["tooling_identity"] = safe_file(RELEASE / "tooling-identity.json", 0o444)
    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
    tooling = json.loads((RELEASE / "tooling-identity.json").read_text(encoding="utf-8"))
    if (
        tooling.get("commit") != manifest.get("tooling_revision")
        or tooling.get("tree") != manifest.get("tooling_tree")
    ):
        raise RuntimeError("tooling identity attestation mismatch")
    if (
        manifest.get("tendwire_reviewed_ancestor") != TENDWIRE_REVIEWED_ANCESTOR
        or manifest.get("herdres_reviewed_ancestor") != HERDRES_REVIEWED_ANCESTOR
        or manifest.get("herdr_reviewed_ancestor") != HERDR_REVIEWED_ANCESTOR
    ):
        raise RuntimeError("reviewed ancestor attestation mismatch")
    if result["private_environment"]["sha256"] != manifest.get("private_env_sha256"):
        raise RuntimeError("private environment digest mismatch")
    if (
        result["codex_acp_bundle"]["sha256"] != CODEX_ACP_BUNDLE_SHA256
        or result["codex_acp_provenance"]["sha256"]
        != CODEX_ACP_PROVENANCE_SHA256
        or manifest.get("codex_acp_bundle_sha256") != CODEX_ACP_BUNDLE_SHA256
        or manifest.get("codex_acp_provenance_sha256")
        != CODEX_ACP_PROVENANCE_SHA256
    ):
        raise RuntimeError("adapter provenance attestation mismatch")
    if (
        result["herdr_binary"]["sha256"] != HERDR_BINARY_SHA256
        or result["herdr_provenance"]["sha256"] != HERDR_PROVENANCE_SHA256
        or manifest.get("herdr_sha256") != HERDR_BINARY_SHA256
        or manifest.get("herdr_provenance_sha256") != HERDR_PROVENANCE_SHA256
    ):
        raise RuntimeError("Herdr provenance attestation mismatch")
    if (
        result["herdr_console_canary_evidence"]["sha256"]
        != manifest.get("herdr_console_canary_sha256")
    ):
        raise RuntimeError("Herdr console canary evidence attestation mismatch")
    return result


def proc_environment(pid: int) -> dict[str, str]:
    result: dict[str, str] = {}
    for entry in Path(f"/proc/{pid}/environ").read_bytes().split(b"\0"):
        if not entry:
            continue
        key, separator, value = entry.partition(b"=")
        if separator:
            result[key.decode()] = value.decode()
    return result


def live_processes(env: dict[str, str]) -> None:
    expected = {
        "tendwired.service": (
            [
                str(RUNTIME / "bin/python"),
                "-B",
                "-I",
                "-m",
                "tendwire.cli",
                "daemon",
                "--db-path",
                "/home/smith/.local/share/tendwire/candidates/0b94403-r8/tendwire.db",
                "--socket-path",
                "/home/smith/.local/share/tendwire/tendwire.sock",
            ],
            str(RUNTIME),
        ),
        "herdres.service": (
            [
                "/usr/bin/python3",
                "-E",
                "-s",
                str(RELEASE / "herdres/herdres.py"),
                "sync",
                "--loop",
                "5",
            ],
            str(RELEASE / "herdres"),
        ),
        "herdres-gateway.service": (
            [
                "/usr/bin/python3",
                "-E",
                "-s",
                str(RELEASE / "herdres/herdres_gateway.py"),
            ],
            str(RELEASE / "herdres"),
        ),
    }
    for unit, (expected_argv, expected_cwd) in expected.items():
        pid = int(show(unit, "MainPID"))
        if (
            pid < 2
            or show(unit, "ActiveState") != "active"
            or show(unit, "SubState") != "running"
            or show(unit, "NRestarts") != "0"
            or show(unit, "NeedDaemonReload") != "no"
        ):
            raise RuntimeError("writer process is not stable")
        argv = [
            value.decode()
            for value in Path(f"/proc/{pid}/cmdline").read_bytes().split(b"\0")
            if value
        ]
        status = Path(f"/proc/{pid}/status").read_text(encoding="ascii")
        cgroup = Path(f"/proc/{pid}/cgroup").read_text(encoding="ascii")
        if (
            argv != expected_argv
            or os.readlink(f"/proc/{pid}/cwd") != expected_cwd
            or "Uid:\t1000\t1000\t1000\t1000" not in status
            or "Gid:\t1000\t1000\t1000\t1000" not in status
            or f"/{unit}" not in cgroup
            or show(unit, "MainPID") != str(pid)
        ):
            raise RuntimeError("writer process identity mismatch")
        if unit in {"herdres.service", "herdres-gateway.service"}:
            actual_env = proc_environment(pid)
            if any(actual_env.get(key) != value for key, value in env.items()):
                raise RuntimeError("live private environment mismatch")
    for unit, marker in (
        ("acp-r8-release-guard.service", "release-guard"),
        ("acp-frozen-live-monitor.service", "monitor-one-hour-strict"),
        ("herdr-server.service", "herdr"),
    ):
        pid = int(show(unit, "MainPID"))
        command = Path(f"/proc/{pid}/cmdline").read_bytes()
        if (
            pid < 2
            or show(unit, "ActiveState") != "active"
            or marker.encode() not in command
            or f"/{unit}" not in Path(f"/proc/{pid}/cgroup").read_text(
                encoding="ascii"
            )
            or show(unit, "MainPID") != str(pid)
        ):
            raise RuntimeError("control process identity mismatch")


def atomic_json(path: Path, value: dict[str, object]) -> None:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp.{os.getpid()}")
    descriptor = os.open(
        temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC, 0o600
    )
    try:
        body = (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()
        view = memoryview(body)
        while view:
            written = os.write(descriptor, view)
            if written < 1:
                raise OSError("attestation write failed")
            view = view[written:]
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    os.replace(temporary, path)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=("installed", "live"))
    args = parser.parse_args()
    if ACTIVE.resolve(strict=True) != RELEASE:
        raise RuntimeError("active release mismatch")
    subprocess.run(
        [str(RELEASE / "release-integrity"), "verify", "--manifest", str(MANIFEST)],
        check=True,
        stdout=subprocess.DEVNULL,
        timeout=120,
    )
    env = environment()
    effective_units()
    files = installed_files()
    if args.mode == "live":
        live_processes(env)
    value: dict[str, object] = {
        "schema_version": 1,
        "release_id": RELEASE_ID,
        "mode": args.mode,
        "valid": True,
        "secret_values_emitted": False,
        "installed_files": files,
    }
    if args.mode == "installed":
        atomic_json(ATTESTATION, value)
    print(
        json.dumps(
            {
                "schema_version": 1,
                "release_id": RELEASE_ID,
                "mode": args.mode,
                "valid": True,
                "secret_values_emitted": False,
            },
            sort_keys=True,
            separators=(",", ":"),
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
