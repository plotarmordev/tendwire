# Wave 14 security-boundary redesign contract

This contract replaces the rejected numeric and sequencing plan in the Wave 13
daemon-boundary contract. It records a coherent redesign of the daemon, local Unix
transport, local filesystem authority, and SQLite connection authority before production
work resumes. Its numeric result is an intermediate safety-boundary checkpoint. It does
not satisfy or replace the canonical architecture component budget.

The implementation baseline is Tendwire commit
`5158553ed8f2a49a57ffffe5a2f30d4d195f19e4`. The retained configuration and installation
identity cleanup at that commit is a completed checkpoint, not a future saving available
to this wave.

## Immutable references

The following hashes are the review inputs. A mismatch stops the wave until this contract
is reviewed again.

| Input | SHA-256 |
|---|---|
| `docs/connector-rpc-contract.md` | `6c7b8976b9a8442d5b6ae521a5edbf82033115b5fa9674d0ba5d3f03756ecd48` |
| `docs/evidence/wave13-daemon-boundary-contract.md` | `1eda83c233a45495683768c119d50307bee2afeaa584323a5da02478ef7726ab` |
| reduction `GOAL.md` | `aa94a4860c78ca729c7a5075ed7a06d7befc6a3db746073d74873278beb38bab` |
| reduction `ARCHITECTURE.md` | `67c1c3928e88d698c8672696917d28d607c30ce97877a2f5f0d3094f1fc06036` |
| reduction `WAVES.md` | `6ae5cf909c69a45e4007706d33d37ca001247f853d66556ae14a086bcba8ad18` |
| reduction `REVIEW_PROTOCOL.md` | `6ec831cec45467b9caa520021120a598e9a45919caed84c040aefe76f2c72ff3` |

The production-file hashes and canonical SLOC at the checkpoint are:

| File | SHA-256 | SLOC |
|---|---|---:|
| `src/tendwire/daemon.py` | `f36436d523ad459cb5bfcead414814d49cc4df151c6db2c2e39c4bc41c284b42` | 571 |
| `src/tendwire/daemon_api.py` | `8bb8d4aef8c94b1f48310a11a2c5c227b146ac9c10575059211fd1d0dc46e04b` | 1,794 |
| `src/tendwire/config.py` | `1d3f57633bc2d9c0bd7e9adf3118658212f1ad0fc8f362663d647e0262924faa` | 259 |
| `src/tendwire/worker_identity.py` | `74d152752a367ae28f1eced9c6d91853e7ffe05c62e10cdf32a2c81c6afa0807` | 246 |
| `src/tendwire/local_state.py` | `3fe7ad02eb775c37f6358eb0b5a0a162dad5aa9148a0a735eb8a6f48a2a2c845` | 537 |
| `src/tendwire/store/db.py` | `19730b4a0da17eca11eb8741322a09b771f3b6cfc3d2c3241492963397366bc6` | 186 |

The six-file baseline is 3,593 SLOC. The explicit architecture quartet
(`daemon.py`, `daemon_api.py`, `config.py`, and `worker_identity.py`) is 2,870 SLOC. The
security trio (`daemon_api.py`, `local_state.py`, and `store/db.py`) is 2,517 SLOC.

This contract checkpoint has exactly this documentation allowlist:

```text
docs/connector-rpc-contract.md
docs/evidence/wave14-security-boundary-contract.md
```

The connector document change corrects stale prose to the already deployed,
cross-repository strict request seam. It is not a wire or production behavior change.
No production or test file is authorized in this checkpoint.

## Exact boundary inventory

This section inventories the checkpoint. Names are inputs to the redesign, not automatic
compatibility requirements. The wire contract, public behavior, and error classification
below are compatibility requirements.

### Daemon application surface

`daemon.py` exposes `default_socket_path`, `DaemonHooks`, `TendwireDaemon`, and
`run_daemon`. `TendwireDaemon` accepts `Config`, an optional socket path, optional hooks,
and an optional stop event. Its observable lifecycle is `start`, `serve_forever`,
`request_stop`, and `stop`, with read-only `snapshot` and `server` properties.

At the checkpoint, the daemon owns these application responsibilities:

- repair configured local state before startup work;
- reject an active or unsafe socket before store or backend hooks run;
- initialize the store before publishing a socket;
- bind the socket after store initialization and before ACP starts;
- require a healthy ACP supervisor and a first durable lifecycle snapshot;
- aggregate public health and run bounded retention and connector maintenance;
- expose store-backed snapshot, attention, pending, turn, and content reads;
- construct the connector outbox facade and route command submission to ACP; and
- restore signal handlers and perform ordered, idempotent teardown.

`TendwireDaemonAPI` continues to accept exactly these nine injected callbacks:

```text
get_snapshot get_health submit_command get_attention get_turns
get_turn_delta get_turn_content get_pending connector_call
```

No callback is trusted merely because the daemon supplied it. The API boundary validates
and sanitizes callback results before framing them.

### RPC and framing surface

The nineteen RPC methods remain exactly:

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

A request is an object containing only optional `id`, required `method`, and optional
object-or-null `params`. Null params normalize to an empty object. Non-connector method
fields are closed as follows:

| Method | Permitted params | Additional requirements |
|---|---|---|
| `ping` | none | none |
| `health.get` | none | none |
| `snapshot.get` | none | none |
| `attention.list` | none | none |
| `pending.list` | none | none |
| `turn.list` | `schema_version`, `limit`, `cursor`, `since` | schema 1 or 2; cursor and since are mutually exclusive |
| `turn.delta` | `watermark`, `cursor`, `limit` | watermark and cursor are mutually exclusive |
| `turn.content.get` | `schema_version`, `turn_id`, `content_revision`, `field`, `cursor` | turn id, revision, and supported field are required; schema is 1 |
| `command.submit` | fixed command request | the shared strict command parser remains authoritative |

Connector request fields, opaque-token rules, inner result shapes, recovery semantics,
and queue ownership remain the exact contract in the immutable connector RPC document.
Its frozen request inventory is:

| Method | Frozen closed request inventory |
|---|---|
| `connector.poll` | required `name`; optional `limit`, `lease_seconds` |
| `connector.ack` | required `name`, `ref`; optional public `response` |
| `connector.fail` | required `name`, `ref`, `reason`; optional `delay_seconds`, `available_at`, public `response` |
| `connector.defer` | required `name`, `ref`, `reason`; optional `delay_seconds`, `available_at`, public `response` |
| `connector.renew` | required `name`, `ref`, `lease_seconds` |
| `connector.release` | exactly `name`, `ref` |
| `connector.reclaim` | exactly `name` |
| `connector.inspect` | exactly `schema_version`, `name`, `status`, `limit` |
| `connector.retry` | `schema_version`, `name`, and exactly one of `key` or `final_identity` |
| `connector.prepare` | the strict `begin`, `part`, `commit`, or `recover` action shape |

The authoritative connector document was corrected in this checkpoint to match the live
Tendwire and Herdres seam: fail and defer require `reason`, renew requires
`lease_seconds`, and prepare begin and commit require `source_ref`. Source-less begin and
commit are unsupported; recovery remains the separate `recover` action. This is a
documentation correction with no request-shape change.

The public Python transport surface consumed outside `daemon_api.py` is
`DaemonAPIError`, `DaemonUnavailable`, `DaemonProtocolError`, `TendwireDaemonAPI`,
`UnixSocketJSONServer`, and `DaemonAPIClient`. `DaemonUnavailable` carries `code`,
`timed_out`, and `request_started`; `DaemonProtocolError` carries `request_started`.
`ensure_daemon_socket_not_active` has only a daemon production caller and is not frozen as
a standalone symbol.

Transport framing remains one newline-delimited UTF-8 JSON request and response per
connection, each bounded to 1 MiB by default. JSON must be finite, depth and item count
must remain bounded, and connector-private keys and values must fail closed. Responses
must remain sealed against mutation after boundary validation. A failure before request
bytes begin has `request_started=False`; timeout, EOF, malformed response, or transport
failure after transmission begins is an uncertain started request and is not retried
implicitly.

### Local filesystem authority surface

The current production consumers of `local_state.py` are exact by category:

| Consumer | Symbols consumed at the checkpoint |
|---|---|
| installation identity | `LocalStateError`, `LocalStateErrorCode`, `EntryType`, `EntryIdentity`, `PermissionState`, `lstat_at`, `entry_identity`, `identity_matches`, `verify_entry_identity`, `prepare_and_open_private_directory`, `inspect_private_file_at`, `repair_private_file_at`, `open_private_file_at`, `publish_private_file_at`, `unlink_verified_entry` |
| daemon socket API | `EntryIdentity`, `LocalStateError`, `LocalStateErrorCode`, `open_resolved_parent`, `prepare_resolved_private_parent`, `proc_fd_path`, `resolve_socket_group`, `validate_private_socket_parent_at`, `validate_socket_group_parent_at`, `socket_bind_umask`, `owned_socket_identity_at`, `pin_owned_socket_at`, `pin_socket_for_client_at`, `enforce_bound_socket_permissions_at`, `unlink_verified_socket_at` |
| SQLite connection authority | `EntryType`, `entry_identity`, `identity_matches`, `lstat_at`, `prepare_resolved_private_parent`, `prepare_sqlite_family_at`, `proc_fd_path`, `validate_owned_regular_stat`, `verify_entry_identity` |
| daemon and CLI startup | `repair_config_state` |

`LocalStateKind`, `PermissionResult`, `ConfigStateReport`, and `SocketGroup` are produced or
used only inside `local_state.py` in production. Their test visibility does not make them
part of a frozen production interface. Callers and local-state implementation may change
together; compatibility wrappers are forbidden.

`LocalStateErrorCode` currently distinguishes unsupported platform, invalid entry name,
missing entry, existing entry, wrong type, wrong owner, wrong group, changed entry,
insecure mode, invalid socket group, insecure socket parent, peer-validation failure, and
operation failure. Any redesign may merge internal helpers but must retain the externally
meaningful distinctions used by daemon client and startup behavior.

### SQLite authority surface

`store/db.py` exposes `StorePathError`, `StoreTimestampError`, `canonical_utc`, `utc_now`,
`add_seconds`, `connect_read`, `read_transaction`, `write_transaction`, and
`store_status`. Schema and retention code also consume the internal `_connect` entry
point. Projection, event, receipt, pending, turn, outbox, retention, and schema modules
consume the timestamp and transaction operations.

The module owns absolute local path validation, SQLite connection construction, pragmas,
transaction begin/commit/rollback, connection closure, and bounded read-only health. It
does not own generic path traversal or daemon socket policy.

## Frozen security and lifecycle invariants

The redesign is valid only if all of the following remain true.

### Generic local state

- Every path component is walked from an open directory descriptor with no-follow
  semantics. Empty names, parent traversal, embedded separators, and NULs fail closed.
- Required entries have the expected type and effective owner. Regular private files
  have one link. Identity is pinned by device and inode and rechecked after operations
  that can race with replacement.
- Existing same-owner regular private files may only have permissions narrowed. Symlinks,
  wrong types, wrong owners, hard links, and replaced entries are never repaired through.
- Atomic private-file publication uses exclusive creation, mode `0600`, complete writes,
  file fsync, no-replace publication, and parent-directory fsync. Verified deletion checks
  the exact expected identity immediately before unlink and fsyncs the parent.
- Unsupported platform primitives fail closed. No path-string fallback is permitted.

### Socket endpoint

- Private mode requires an owner-private parent and exact socket mode `0600`.
- Group mode requires the existing protected parent owner, configured gid, exact parent
  mode `0710`, socket gid, exact socket mode `0660`, and current group-admission rules.
- Active, stale, missing, wrong-type, wrong-owner, wrong-group, ambiguous-probe,
  replaced-parent, and replaced-leaf cases remain distinct. Unsafe entries are not
  mutated.
- The server pins the socket it bound. Startup rollback and shutdown unlink only that
  exact socket identity. Replacement or ambiguous cleanup preserves the replacement and
  fails closed where cleanup cannot be proven.
- The client pins and rechecks the socket leaf around connect and validates the server UID
  with `SO_PEERCRED`. Missing peer-credential support, malformed credentials, or a wrong
  UID fails closed.
- The redesigned daemon classifies, cleans, binds, and pins the socket in one locked
  `server.start` transaction before store initialization and ACP startup. It accepts and
  dispatches no request until the complete startup transaction succeeds. Every later
  failure exact-unlinks only the socket identity that transaction bound.

### Bounded transport

- Request workers, queued work, accepted connections, frame bytes, read/write deadlines,
  periodic work, and shutdown grace remain bounded.
- Queue saturation returns a bounded retryable failure. A blocked handler or periodic
  callback cannot create unbounded work or keep teardown waiting beyond its grace.
- Closing the listener stops admission. Queued work is cancelled where possible, active
  sockets are shut down, and daemon worker threads cannot prevent interpreter exit.
- Invalid UTF-8, malformed or deep JSON, non-finite values, oversized requests or
  responses, hostile callback values, and serialization failures produce bounded public
  errors without leaking private input or callback data.

### Database

- A database path is absolute, non-URI, contains no parent traversal, and names a local
  leaf. The securely walked parent remains pinned for the connection lifetime.
- The main database and existing `-wal`, `-shm`, and `-journal` family members are
  same-owner, no-follow, regular, single-link, owner-only objects. The main database
  identity is rechecked during connect and close; family permissions are rechecked after
  SQLite can create sidecars.
- SQLite connects through the anchored descriptor path, never by re-resolving the
  configured string. Reads use read-only mode. Foreign keys and `trusted_schema=OFF`
  remain enabled; writes retain WAL and `synchronous=FULL`.
- Read transactions use `BEGIN`; write transactions use `BEGIN IMMEDIATE`; exceptions
  roll back and all paths close the connection and its retained parent descriptor.
- `store_status` remains bounded, read-only, and fail-closed without repairing or
  recreating the store.

### Daemon and ACP

- ACP remains required. A missing, failed, or unhealthy supervisor aborts startup and
  rolls the socket back.
- Supervisor start, health normalization, prompt routing, permission routing, and bounded
  stop/join behavior remain fail-closed and public-safe.
- Public health validates store, ACP, pending-ingestion, maintenance, snapshot, and
  backend-health shapes. Arbitrary exception text and private identifiers never cross the
  API.
- Signal handlers only request stop. Teardown occurs outside signal context and remains
  idempotent.

## Natural ownership cut

The reduction must remove duplicate proof logic rather than move it to a new file.

### `local_state.py`: generic anchored-entry authority

`local_state.py` owns one cohesive anchored parent/leaf abstraction, or an equivalent
cohesive implementation. It owns the parent descriptor, validated leaf name, stat and
identity checks, pin and recheck operations, verified unlink, and anchored `/proc/self/fd`
address. Identity files, sockets, and SQLite must consume this common authority instead
of rebuilding path walks and race checks.

It also owns the genuinely shared private-directory and atomic-private-file operations
and resolution of private versus configured-group socket policy. It does not own SQLite
suffixes, pragmas, transactions, daemon startup order, RPC validation, or worker-key
semantics. Production-unused report enums may be removed. `repair_config_state` may
return no report because production callers use only its success or exception, but its
eager repair and fail-closed behavior remain.

### `store/db.py`: SQLite-specific authority

`store/db.py` owns the main/sidecar family, single-link and permission policy, the pinned
connection subclass, SQLite URI construction from the anchored descriptor, pragmas,
transactions, timestamps, and store health. Moving these concerns to `local_state.py`
would only relocate store code and is not approved.

Read and write transactions should share one transaction context implementation with the
begin mode as data. Timestamp validation should retain one canonical parser. Neither
consolidation may broaden accepted values or weaken rollback.

### `daemon_api.py`: protocol boundary and generic bounded transport

The file has two legitimate internal halves. The protocol half owns method tables,
closed request validation, public request IDs, exact response envelopes, turn-content
restoration, connector-result validation, and the final forbidden-key/value backstop.
The transport half owns framing, the bounded daemon executor, server admission and
endpoint lifecycle, peer validation, client deadlines, and request-started uncertainty.

These halves may use declarative tables and common exact-JSON walkers. Tables must remain
conventionally formatted and reviewable. The connector callback remains hostile input;
validation cannot be deleted on the claim that `ConnectorOutboxAPI` already sanitized
it. `ThreadPoolExecutor` is not an acceptable replacement for the bounded daemon-worker
queue because its queue and interpreter-shutdown behavior do not satisfy this contract.

Splitting either half into a new production module solely to meet a per-file count is
relocation and is rejected. A later split requires a separate architecture review and
the combined component count remains chargeable to this boundary.

### `daemon.py`: application lifecycle and callbacks

`daemon.py` owns the startup transaction, ACP lifecycle, application health,
maintenance cadence, store-backed API callbacks, connector facade construction, command
routing, and signals. It consumes the socket server as a transport and must not implement
socket inode operations or wire validators.

Store callbacks that differ only by database and host binding may be made uniform, but
the nine-callback API constructor is frozen. Health normalization may become table-driven
only when malformed dependency results continue to degrade rather than leak or raise.

## Server-start classification rule

The current free `ensure_daemon_socket_not_active` function and the later server startup
path duplicate endpoint classification and leave an active-check race. They collapse into
one locked `UnixSocketJSONServer.start` transaction before store initialization. The
required order is:

1. The daemon repairs configured local state, constructs the API and server, and calls
   `server.start` before any store or backend hook.
2. Under the bounded startup lock, `start` securely resolves and validates the parent and
   leaf and performs one endpoint classification. A successful connect is active before
   any response is considered. Connection refused classifies the exact pinned socket as
   stale. Every other probe failure is ambiguous and fails closed. A slow, malformed, or
   non-Tendwire listener therefore remains active and is never removed.
3. The classifier may perform a bounded ping solely to preserve the current public holder
   diagnostic. For a responsive Tendwire listener, a validated bounded version string and
   positive integer PID appear in the conflict message. The ping result cannot change the
   active classification. A missing, slow, malformed, hostile, or non-Tendwire response
   reports an unknown holder. No other response field or arbitrary peer text may be
   copied into the exception or CLI output.
4. For a missing or exact stale endpoint, the same locked transaction
   verified-unlinks only the stale socket it pinned, binds, pins and rechecks the new
   socket, applies its exact mode and gid, listens, and retains the parent, pin, and
   identity for rollback. It does not enter the accept loop.
5. The daemon initializes the store, starts ACP, requires healthy status and the first
   durable snapshot, runs startup maintenance, and only then publishes the server as
   started. `serve_forever` begins admission afterward.
6. Any failure after bind closes the listener, stops bounded executors, and
   verified-unlinks only the socket identity from step 4. This includes every store,
   maintenance, and ACP startup failure.

Removing the free active-check function is approved only with this one locked
classification, stale-cleanup, bind, and pin transaction. The store hook may intentionally
observe the reserved socket path, unlike at the checkpoint, but no request can be accepted
or dispatched. Tests must prove exact cleanup and no admission on every store failure.

Rollback and normal close must share one endpoint-release operation. If verified unlink
cannot establish that the leaf is still the server's socket, the release operation keeps
the replacement intact and closes retained descriptors only when doing so cannot cause a
later unsafe unlink.

## Honest numeric plan and gates

The old gates assumed unachieved configuration and identity reductions and treated two
independent boundaries inside `daemon_api.py` as if they could fit one 930-line budget.
That premise was disproved by the Wave 13 checkpoint.

The achieved 259 SLOC in `config.py` and 246 SLOC in `worker_identity.py` are fixed inputs.
No Wave 14 gate depends on reducing either file further. A mechanical local-state callsite
adaptation may touch `worker_identity.py`, but it must preserve its behavior and finish at
or below the achieved 246 SLOC. `config.py` is not an implementation file in this wave.

| File | Baseline | Honest range | Planning value |
|---|---:|---:|---:|
| `daemon.py` | 571 | 370–400 | 385 |
| `daemon_api.py` | 1,794 | 1,030–1,120 | 1,070 |
| `local_state.py` | 537 | 320–350 | 335 |
| `store/db.py` | 186 | 145–160 | 150 |
| `config.py` | 259 | fixed at 259 | 259 |
| `worker_identity.py` | 246 | at most 246 | 246 |

The planning values produce:

```text
security trio     1070 + 335 + 150 = 1555
quartet            385 + 1070 + 259 + 246 = 1960
all six            385 + 1070 + 335 + 150 + 259 + 246 = 2445
```

The optimistic lower bounds are 1,495 for the security trio, 1,905 for the quartet, and
2,370 for all six. They are floors for reassessment, not delivery targets.

The revised intermediate-checkpoint gates are:

| Gate | Required result |
|---|---:|
| Security trio | at most 1,575 SLOC |
| Architecture quartet | at most 1,980 SLOC |
| All six files | at most 2,475 SLOC |
| Preferred all-six checkpoint | at most 2,450 SLOC |

Individual files must remain within their honest ranges except that a reviewed trade
between `daemon_api.py`, `local_state.py`, and `store/db.py` is allowed when the security
trio and all-six gates both pass and no responsibility moved to the wrong owner. The
upper ends are not additive permission to miss an aggregate gate.

The old Phase A gate of 1,370 is 125 SLOC below the optimistic trio floor. The old quartet
gate of 1,725 is 180 below its optimistic floor. The old all-six gate of 2,165 is 205 below
its optimistic floor, and the old preferred 2,050 is 320 below the optimistic floor.
They are rejected as Wave 14 delivery gates only. The 1,725 quartet gate remains the
binding canonical completion gate. The old 215/190 config/identity stretch numbers are
rejected as Wave 14 gate inputs.

The planned quartet result of 1,960 remains 235 SLOC above the canonical architecture
hard gate of 1,725. Wave 14 therefore cannot claim final component-budget compliance or
reclassify `local_state.py` or `store/db.py` into another row. A later reviewed reduction
or architecture reassessment is still required before the overall reduction goal can
close.

Reaching the old numbers within this Wave 14 boundary plan would require at least one
forbidden tactic: relocating one half of `daemon_api.py`, shifting SQLite or socket proof
into consumers, packing tables or multi-statement lines, deleting hostile-result or race
validation, weakening teardown, or deleting negative tests. No such tactic is authorized.
A later attempt at the canonical 1,725 quartet gate requires a new reviewed design rather
than pressure to compress this implementation.

## Sequential checkpoints and ownership

One owner works at a time. Every checkpoint begins from the reviewed prior checkpoint,
uses conventional formatting before measurement, records focused output and canonical
SLOC, and passes the full Tendwire suite. No compatibility shims survive a checkpoint.

Reviewing this evidence authorizes only a stable contract checkpoint. The implementation
checkpoints below do not begin automatically. Work pauses for owner reassessment of the
remaining canonical 1,725 quartet gate before any major implementation wave begins.

### Checkpoint 1: anchored local state and SQLite authority

The owner redesigns `local_state.py` and `store/db.py` together, then adapts their current
consumers without retaining old aliases.

Production allowlist:

```text
src/tendwire/local_state.py
src/tendwire/store/db.py
src/tendwire/worker_identity.py       # local-state callsite adaptation only
src/tendwire/daemon_api.py            # local-state callsite adaptation only
src/tendwire/daemon.py                # repair callsite adaptation only
src/tendwire/cli.py                   # repair callsite adaptation only
```

Test allowlist:

```text
tests/test_local_state_permissions.py
tests/test_store.py
tests/test_release_readiness.py
tests/test_daemon.py                  # filesystem/socket adaptation only
```

The checkpoint must separately report `local_state.py`, `store/db.py`, and any
`worker_identity.py` count change. It does not claim the final daemon API reduction.

### Checkpoint 2: RPC protocol boundary

The owner reduces only the protocol half of `daemon_api.py`. The nine callbacks,
nineteen methods, request fields, response shapes, exact opaque content, privacy
backstop, and response seal remain unchanged.

Production allowlist:

```text
src/tendwire/daemon_api.py
```

Test allowlist:

```text
tests/test_daemon.py                  # dispatch/framing-independent protocol cases
tests/test_connector_daemon_cli.py
```

Every connector verb receives table-driven valid request, invalid field, exact success,
and exact inner-error coverage before this checkpoint closes.

### Checkpoint 3: bounded transport and endpoint lifecycle

The owner reduces the transport half of `daemon_api.py`, makes `server.start` the sole
locked endpoint classifier as specified above, and makes only the necessary daemon
lifecycle adaptation.

Production allowlist:

```text
src/tendwire/daemon_api.py
src/tendwire/daemon.py
```

Test allowlist:

```text
tests/test_daemon.py
tests/test_daemon_acp.py
tests/test_connector_daemon_cli.py    # real socket framing cases only
```

The checkpoint closes only after the complete socket, deadline, admission, and shutdown
matrices pass together. Protocol tests from checkpoint 2 remain unchanged in intent.

### Checkpoint 4: application daemon

The owner reduces `daemon.py` application lifecycle, health normalization, maintenance,
store callback binding, ACP routing, and signal handling. Socket mechanics and wire
validation are out of scope.

Production allowlist:

```text
src/tendwire/daemon.py
```

Test allowlist:

```text
tests/test_daemon.py                  # application cases only
tests/test_daemon_acp.py
```

### Checkpoint 5: integrated closure

No production redesign occurs here. The owner runs differential and adversarial
validation, the full Tendwire suite, and the authoritative Herdres client against a real
daemon socket. Documentation may record evidence and final counts. A production fix
returns to the owning checkpoint instead of being hidden in closure.

## Required test matrices

Surviving behavior tests may change their internal seam, but negative cases are retained.
Deleting a test because its helper name disappeared is a rejection.

| Matrix | Required cases |
|---|---|
| private paths and files | missing/create, owner-private, permissive repair, symlink at every component, wrong type, wrong owner, hard link, leaf replacement, parent replacement, interrupted write, no-replace collision, verified-unlink replacement |
| installation identity differential | known worker-key and pane vectors, fresh create, concurrent create, partial three-file states, corrupt/mismatched marker and sentinel, same-owner mode repair and reread, replacement during read, unacknowledged reset, interrupted acknowledged reset |
| SQLite family | invalid relative/URI/parent paths, missing read, create, main replacement during connect and close, WAL/SHM/journal wrong type/owner/mode/link, sidecar creation, anchored-parent replacement, read-only health, pragma values, begin modes, commit, rollback, descriptor closure |
| socket classification/start | missing, active Tendwire, active non-Tendwire, slow active peer, stale refused, ambiguous probe, wrong type/owner/group/mode, private and group parents, nonmember group, concurrent start, startup-lock timeout and EINTR, replacement between endpoint classification and bind/pin |
| socket rollback/close | failure before bind, post-bind pin failure, chmod/chown failure, executor-start failure, store failure after reservation exact-cleans the owned socket with no admission, ACP failure exact cleanup, leaf replacement before rollback and close, repeated close |
| client trust | intermediate symlink, parent replacement, leaf replacement before and after connect, missing `SO_PEERCRED`, malformed peer credentials, wrong UID, private/group success |
| framing and uncertainty | empty/malformed/non-UTF-8/deep/non-finite/oversized request, oversized or unserializable response, disconnect before send, timeout before send, disconnect/EOF/timeout after send, exact request-started flags, server survives client disconnect |
| capacity and shutdown | worker cap, queue cap, busy response, recovery after saturation, blocked handler with health/command progress, blocked periodic callback, shutdown grace, queued cancellation, active connection force-close, daemon threads do not hold process exit |
| RPC protocol | unknown top-level field, unknown method, non-object params, closed params for all nineteen methods, request-ID privacy, hostile callbacks, sealed response, exact command disposition, turn list/delta/content paging and text, exact connector tokens and inner envelopes |
| daemon application | locked active refusal and bind before store/backend hooks, store hook may observe only the reserved non-accepting socket, store failure exact rollback with no admission, bind before ACP, no admission before successful startup, ACP missing/start failure/unhealthy, first snapshot required, health malformed dependency shapes, maintenance cadence/failure, prompt and permission routing, signal-only stop request, ordered idempotent teardown |
| real connector integration | all connector verbs needed by Herdres, poll/dedup/ack, ack-loss re-poll, timeout after mutation treated as uncertain, private-key backstop, exact opaque tokens, real daemon socket rather than mocks |

The final review compares externally visible behavior to both the Wave 14 checkpoint and
the original `af0e5be62d1a59d2c7af391fe12bdeeb954bee33` daemon/security baseline where the
config/identity checkpoint did not intentionally change behavior.

## Stop and reassessment conditions

Work stops before merge when any of the following occurs:

- an immutable input hash differs or the connector contract and current request inventory
  disagree in a way that requires a wire decision;
- an implementation checkpoint touches a production or test file outside its allowlist;
- conventional formatting or unpacking moves a checkpoint outside its numeric gate;
- the security trio cannot reach 1,575, the quartet cannot reach 1,980, or all six cannot
  reach 2,475 without crossing a responsibility boundary;
- a proposed reduction depends on lowering the achieved 259/246 config/identity inputs;
- code is moved to a new module, another component, tooling, or generated data to change
  the measured file totals without reducing the boundary;
- a frozen invariant, error phase, exact RPC shape, private-data backstop, or negative race
  case is removed, weakened, skipped, or converted to a fallback;
- the implementation needs a new dependency, configuration knob, compatibility alias,
  subprocess path, filesystem watcher, or alternate durability owner;
- a blocked handler or callback can exceed bounded teardown, or a client can implicitly
  retry an uncertain started mutation;
- focused tests or the full Tendwire suite fail, or the authoritative Herdres client
  fails against a real daemon socket; or
- final review finds packed tables, multi-statement lines, functions over the project
  discipline without justification, unused compatibility residue, or tests deleted for
  surviving behavior.

A stop produces a written reassessment with actual conventionally formatted SLOC and the
failed invariant or ownership constraint. It does not reinstate the rejected Wave 13
sequencing or per-file gates, nor authorize silent test deletion or a force merge. The
canonical architecture completion gate remains binding.
