# Tendwire reduction baseline

This baseline was measured at commit `91891bf` on branch `wave-0/baseline` on
2026-08-04. The production root is `src/`.

## SLOC calibration

Canonical production SLOC was measured with:

```console
python3 /home/smith/acp-reduction-goal/tools/sloc_count.py \
  /home/smith/tendwire/.worktrees/wave-0-baseline/src
```

The counter reports **57,007 SLOC**, exactly matching the stated baseline of
57,007: a difference of **0 lines (0.00%)**. The canonical checkout at
`/home/smith/tendwire/src` independently reports the same total.

Physical lines are newline-terminated lines reported by `wc -l` for the same
production Python files. Canonical SLOC excludes blank lines, full-line
comments, and module/class/function docstrings according to
`tools/sloc_count.py`. Tests, docs, tooling, generated code, and vendored code
are outside both tables.

### Directory summary

| Directory | Physical lines | Canonical SLOC |
|---|---:|---:|
| `tendwire/` (direct modules) | 12,337 | 10,978 |
| `tendwire/backends/` | 14,761 | 12,724 |
| `tendwire/connectors/` | 686 | 624 |
| `tendwire/core/` | 6,884 | 5,925 |
| `tendwire/store/` | 28,239 | 26,756 |
| **Total** | **62,907** | **57,007** |

### Per-module measurements

| Module | Physical lines | Canonical SLOC |
|---|---:|---:|
| `tendwire/__init__.py` | 5 | 2 |
| `tendwire/_version.py` | 3 | 1 |
| `tendwire/backends/__init__.py` | 1 | 0 |
| `tendwire/backends/acp_client.py` | 1,688 | 1,483 |
| `tendwire/backends/acp_coordinator.py` | 2,413 | 2,148 |
| `tendwire/backends/acp_ingestion.py` | 714 | 612 |
| `tendwire/backends/acp_permissions.py` | 295 | 263 |
| `tendwire/backends/acp_probe.py` | 360 | 295 |
| `tendwire/backends/acp_projection.py` | 1,299 | 1,117 |
| `tendwire/backends/acp_protocol.py` | 613 | 480 |
| `tendwire/backends/acp_runtime.py` | 1,270 | 1,036 |
| `tendwire/backends/herdr_cli.py` | 2,671 | 2,304 |
| `tendwire/backends/herdr_command.py` | 156 | 125 |
| `tendwire/backends/herdr_events.py` | 2,454 | 2,209 |
| `tendwire/backends/herdr_protocol.py` | 363 | 260 |
| `tendwire/backends/herdr_socket.py` | 464 | 392 |
| `tendwire/cli.py` | 1,856 | 1,669 |
| `tendwire/command_submission.py` | 2,306 | 2,016 |
| `tendwire/config.py` | 726 | 686 |
| `tendwire/connectors/__init__.py` | 5 | 2 |
| `tendwire/connectors/outbox.py` | 681 | 622 |
| `tendwire/core/__init__.py` | 1 | 0 |
| `tendwire/core/actions.py` | 219 | 179 |
| `tendwire/core/agent_events.py` | 393 | 328 |
| `tendwire/core/attention.py` | 196 | 155 |
| `tendwire/core/commands.py` | 1,385 | 1,141 |
| `tendwire/core/models.py` | 1,961 | 1,706 |
| `tendwire/core/projector.py` | 68 | 51 |
| `tendwire/core/turns.py` | 2,661 | 2,365 |
| `tendwire/daemon.py` | 1,361 | 1,226 |
| `tendwire/daemon_api.py` | 1,833 | 1,706 |
| `tendwire/local_state.py` | 3,868 | 3,342 |
| `tendwire/store/__init__.py` | 1 | 0 |
| `tendwire/store/sqlite.py` | 28,238 | 26,756 |
| `tendwire/worker_identity.py` | 379 | 330 |
| **Total** | **62,907** | **57,007** |

## Test baseline

The existing repository virtual environment was used without installing or
upgrading anything. This worktree's source tree was selected explicitly:

```console
env PYTHONPATH=/home/smith/tendwire/.worktrees/wave-0-baseline/src \
  /home/smith/tendwire/.venv/bin/python -m pytest tests/ -x -q
```

Result:

```text
2879 passed, 2 skipped in 496.88s (0:08:16)
```

The authoritative run was performed outside the managed filesystem sandbox.
Inside that sandbox, a hardened Unix-socket test cannot bind through its
`/proc/self/fd/...` path and stopped the first run after 428 passes. The same
isolated test passed outside the sandbox (`1 passed in 3.06s`), after which the
unchanged full-suite command above completed green.
