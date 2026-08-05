# Wave 13 daemon-boundary contract

This evidence freezes the control-plane contract before the Wave 13 reduction. The
baseline is Tendwire commit `af0e5be62d1a59d2c7af391fe12bdeeb954bee33`. The detailed
connector wire contract remains `docs/connector-rpc-contract.md` (baseline SHA-256
`df5678026cbc32ab41d8996faf81182e6e68bf4ff120de633104ed295715d678`).

## Required RPC surface

The following nineteen methods, their closed request fields, and their exact success and
error result shapes are frozen:

```text
ping
health.get
snapshot.get
attention.list
pending.list
turn.list
turn.delta
turn.content.get
command.submit
connector.prepare
connector.poll
connector.ack
connector.fail
connector.defer
connector.renew
connector.release
connector.reclaim
connector.retry
connector.inspect
```

`TendwireDaemonAPI` continues to accept exactly these injected callbacks:
`get_snapshot`, `get_health`, `submit_command`, `get_attention`, `get_turns`,
`get_turn_delta`, `get_turn_content`, `get_pending`, and `connector_call`.

The transport remains one newline-delimited JSON request and response, each bounded to
1 MiB by default. Public JSON must be finite, UTF-8 encodable, bounded, and free of
private connector keys. A failure before request bytes begin keeps
`request_started=False`; timeout, EOF, or failure after transmission begins remains an
uncertain started request and is never retried implicitly.

## Configuration and identity surface

The public `Config` fields and matching explicit-argument/environment precedence are
frozen:

```text
host_id herdr_bin data_dir db_path socket_path herdr_timeout_seconds
acp_thought_policy acp_request_timeout_seconds acp_shutdown_timeout_seconds
acp_max_frame_bytes reconcile_interval_seconds event_retention_days
submission_link_window_seconds submission_hard_ttl_seconds max_outbox_attempts
connector_claim_ttl_seconds connector_max_claim_ttl_seconds connector_ack_ttl_seconds
acknowledged_final_retention_days command_receipt_retention_seconds
snapshot_retention_days snapshot_maintenance_batch_size
store_maintenance_cadence_seconds turn_change_retention_days
turn_change_compaction_batch_size socket_group
```

Except for the existing special defaults, each field continues to use the corresponding
`TENDWIRE_<FIELD>` environment name. Explicit arguments win over environment values,
which win over defaults. Existing validation bounds and CLI names remain unchanged.

Stable worker identity remains version 1 and retains canonical Herdr pane identity,
known derivation vectors, concurrent create semantics, acknowledged reset semantics, and
the exact three-file installation layout:

```text
installation.key
installation.key.sha256
installation.key.initialized
```

Missing, corrupt, mismatched, replaced, symlinked, insecure, or partial identity state
continues to fail closed once the initialization sentinel exists.

## Local security and lifecycle invariants

- The daemon binds its socket before ACP starts, but accepts no requests until startup is
  fully successful.
- Startup rollback and shutdown remove only the exact socket object owned by that server.
- Active, stale, wrong-type, wrong-owner, replaced-parent, and replaced-leaf socket cases
  remain distinct and fail closed.
- Private mode requires an owner-private parent and socket mode `0600`. The client pins
  and rechecks the socket leaf and validates the server UID with `SO_PEERCRED`; missing
  peer-credential support fails the client closed. Group sharing retains its protected
  group parent, configured gid, socket mode `0660`, and current admission semantics.
- Request admission, periodic callbacks, handler threads, deadlines, and shutdown remain
  bounded. A blocked handler cannot create unbounded work or prevent bounded teardown.
- Database paths remain absolute non-URI paths. The database and WAL family remain
  owner-only, no-follow, identity-rechecked objects; SQLite keeps `FULL` synchronous mode
  and `trusted_schema=OFF`.
- ACP is required. Startup, health aggregation, prompt routing, and shutdown continue to
  fail closed without a healthy ACP supervisor.

The local-state seam may be simplified, but the behavior above is frozen. The baseline
symbols consumed by `daemon_api.py`, `worker_identity.py`, and `store/db.py` are an input
inventory, not a compatibility requirement: callers and implementation may be changed
together only after the first daemon/config/identity checkpoint.

## Sequential ownership and numeric gates

Phase B runs first against unchanged `daemon_api.py`, `local_state.py`, and `store/db.py`:

```text
daemon.py             571 -> <=390
config.py             280 -> <=215
worker_identity.py    330 -> <=190
phase total          1181 -> <=795
```

After focused, differential, adversarial, and full-suite review creates an intermediate
checkpoint, Phase A may change the remaining security seam:

```text
daemon_api.py        1794 -> <=930
local_state.py        537 -> <=300
store/db.py           186 -> <=140
phase total          2517 -> <=1370
```

The explicit architecture quartet (`daemon.py`, `daemon_api.py`, `config.py`, and
`worker_identity.py`) must finish at or below 1,725 canonical SLOC. All six production
files must finish at or below 2,165; 2,050 is the preferred checkpoint. If a coherent
implementation cannot meet those limits, the wave stops for reassessment. It must not
pack tables or one-liners, delete negative tests, move code into another component, add a
fallback mode, or weaken any frozen invariant to meet the count.

Phase B owns daemon/config/identity tests, including `test_daemon.py`; Phase A begins only
after B is checkpointed and then owns API/local-state/store security test adaptation.
Every phase must compare behavior to `af0e5be`, run its focused failure matrices, and pass
the full Tendwire suite. Final validation also exercises the authoritative Herdres client
against a real daemon socket.
