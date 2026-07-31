# Tendwire

Current release candidate: `0.1.0rc5`, paired with Herdres `0.7.0rc4` on
Python 3.13. Cross-repository and live-provider proofs remain explicit
release-owner operations.

Tendwire is a **local API and durable control plane for Herdr**. It lets apps,
automations, and connectors observe coding agents, read lossless turns and
pending questions, and submit commands without depending on terminal internals.
It stores neutral snapshots and command receipts and exposes a public-safe API
for local clients. Concrete-provider delivery bookkeeping, raw terminal
controls, socket paths, and private Herdr identifiers are not part of public
JSON.

## Herdr plugin

Tendwire is listed as a community Herdr plugin. Install the source checkout and
its read-only actions with:

```sh
herdr plugin install plotarmordev/tendwire
herdr plugin action invoke plotarmordev.tendwire.doctor
```

The plugin actions expose public-safe diagnostics, snapshots, schema-v2 turns,
and pending interactions. They run from Herdr's managed checkout and require
Python 3.13 or newer. Plugin installation does not silently create or start the
background daemon; use [INSTALL.md](INSTALL.md) for persistent service setup.

## Relationship to Herdr, Herdres, and connectors

Herdr is the only concrete runtime backend documented here. Tendwire can observe
Herdr through the conservative CLI one-shot path or, when explicitly enabled,
through the Herdr socket/event backend. Both paths normalize Herdr state into
neutral Tendwire spaces, workers, attention, turns, pending interactions,
command results, connector jobs, and backend health.

Herdres can use Tendwire as its source/control plane while Herdres remains the
Telegram connector. Tendwire owns Herdr observation, private bindings,
turns/pending interactions, attention, command routing, receipts, backend
health, event/projection state, and the neutral connector outbox. Herdres owns
Telegram formatting, topic/message state, replies, rate limits, retries, and
delivery bookkeeping. Hermes, MCP, iOS, AR, UI surfaces, local LLMs, and
concrete connector implementations remain outside this README's active scope.
The connector outbox is only a neutral Tendwire boundary for a separate process
to poll, acknowledge, fail, or defer jobs; it is not a Telegram, UI, or delivery
bridge.

Public Tendwire JSON must not expose raw `pane_id`, `terminal_id`,
`backend_target`, Telegram/chat/topic/message IDs, socket paths, raw target
values, private fingerprints, argv/env/stdout/stderr, tokens, or secrets. Herdr
pane and terminal identifiers may exist only inside private
`WorkerBinding`/store internals used by Tendwire itself.

## Opaque worker continuity metadata

When Tendwire can validate an authoritative Herdr workspace/public-pane
identity, a public worker may carry optional continuity metadata in `meta`.
`meta.stable_key` has the exact format `wsk1_` followed by 64 lowercase
hexadecimal characters (`^wsk1_[0-9a-f]{64}$`), and
`meta.stable_key_version` is the exact integer `1` (not a string or boolean).
Consumers must treat an absent or invalid pair as no continuity claim.

Within one Tendwire installation, the authoritative version-1 pair—not
`worker.id`—is the continuity authority. Restoring the same Herdr public
workspace/pane identity therefore reproduces the exact `meta.stable_key`, even
when Tendwire worker IDs or runtime terminal, agent, and session identifiers
change.

The handle is a Tendwire-owned, opaque public value. Before projection,
Tendwire recursively removes source-supplied fields in the normalized
stable-key family, so Herdr metadata cannot select or spoof this identity.
Tendwire then derives the handle locally from a validated workspace/public-pane
identity and a private 32-byte installation key. Neither the handle nor any
other public field exposes that key or the raw identity used to derive it.
Herdres accepts only the exact public format and version. An absent pair, a
partial pair, a malformed value, or any version other than the integer `1`
fails closed: Herdres quarantines the local binding instead of falling back to
worker ID. It neither possesses the installation key or raw identity nor calls
Herdr to reconstruct them. Herdres also requires the turns source wrapper's
`schema_version` to be the exact integer `1`; a missing, malformed, or
unsupported value stops source state and delivery mutation. After that gate,
legacy private state can adopt an already exact key only through a
deterministic, unique match; conflicting claims remain quarantined.

The installation identity has three artifacts in `data_dir`: the private
32-byte `installation.key`, its nonsecret `installation.key.sha256` digest
marker, and `installation.key.initialized`. The sentinel is the exact
nonsecret one-byte value `1`; Tendwire creates it only after validating and
publishing the key and digest. Ordinary loads validate and reuse an initialized
identity and never rotate it. The default `data_dir` is
`~/.local/share/tendwire` and can be changed with `TENDWIRE_DATA_DIR`. All three
artifacts belong to Tendwire, not Herdr or Herdres, and must be backed up and
restored together from one stopped-service checkpoint.

Continuity is deliberately narrow. Moves that keep the worker in the same
workspace and retain its authoritative public-pane identity, including tab
moves, preserve the handle. A cross-workspace move changes the handle.
Terminal and agent-session identifiers are not continuity inputs: they may be
recreated during restore without changing the handle when Herdr restores the
same logical pane. Destroying and recreating a logical pane does not inherit the
old handle. The same raw worker identity under a different Tendwire installation
key produces an unrelated handle, preventing cross-installation correlation.

With `installation.key.initialized` present, a missing key, digest, or both
fails closed and never bootstraps a replacement. Replaced, mismatched, malformed,
or unsafe identity state also fails closed. An absent sentinel is only initial
bootstrap or legacy-migration state, never a rotation request: Tendwire
validates and publishes the key and digest before publishing the sentinel.
Tendwire does not trust source continuity metadata or publish a locally
unauthenticated handle.

Intentional rotation is an explicit coordinated offline operation. Stop
Tendwire and every identity consumer, then invoke
`tendwire.worker_identity.reset_installation_key(Path(data_dir),
acknowledge_continuity_break=True)` from a controlled operator Python
environment; do not delete identity artifacts by hand. The next eligible load
bootstraps a new three-artifact identity. Every `wsk1_` handle changes, so
Herdres state, bindings, and topics require explicit migration and review;
stale bindings are quarantined and old topics are not silently rebound or
automatically reused.

## Running Tendwire

Tendwire installs a console script named `tendwire`. The primary public entry
points are JSON-only:

```bash
tendwire snapshot --json
echo '{"schema_version":1,"action":"noop"}' | tendwire command --json
tendwire daemon --db-path /path/to/tendwire.db
```

You can also run the CLI module directly:

```bash
python -m tendwire.cli snapshot --json
```

`snapshot --json` prints one neutral JSON snapshot to stdout and exits
successfully, even when no Herdr data is available. `command --json` reads
exactly one schema-v1 JSON command request from stdin. When Tendwire can prove
the result, it prints exactly one schema-v2 command envelope to stdout and exits
`0` for `ok=true` or `1` for `ok=false`. If a mutating daemon request may have
started but no exact authoritative envelope or durable replay can be proven, it
prints no stdout envelope, writes a fixed diagnostic to stderr, and exits `2`;
it never forges a status-only result for that process-level ambiguity. Stdout is
JSON-only for these public machine-readable commands when stdout is present.

Snapshot-adjacent public turn and pending-interaction views are also available:

```bash
tendwire turns --json
tendwire pending --json
```

These commands derive conservative public data from the current Tendwire
snapshot and also exit successfully with empty collections when no Herdr data is
available.

To inspect why Herdr data is absent without changing the snapshot contract, use
the read-only diagnostic command:

```bash
tendwire doctor --json
```

`doctor --json` prints JSON-only diagnostics for `herdr workspace list`,
`herdr agent list`, and `herdr pane list`, with `--json` compatibility variants
run only when a no-flag command is not healthy. It distinguishes missing Herdr
binary, launch error, command timeout, nonzero exit, malformed JSON, empty
healthy output, non-empty healthy output, skipped compatibility probes, and
checks skipped after a timeout or exhausted aggregate deadline. Diagnostics do
not expose raw backend argv. The Herdr binary path, data directory, and database
path expand `~`; each Herdr probe uses `TENDWIRE_HERDR_TIMEOUT_SECONDS` or
`--herdr-timeout` when set, defaulting to 5.0 seconds.

When Herdr 0.7.0 is present, the adapter first tries the no-flag JSON envelopes
(`herdr workspace list`, `herdr agent list`) that wrap records under
`result.workspaces` and `result.agents`, then keeps `--json` list variants as a
compatibility fallback. If no agents are returned, it uses `herdr pane list` as
a worker fallback, keeping only panes that describe an agent. Each attempt is
independent and safe: a missing binary, timeout, or malformed response simply
produces empty spaces or workers rather than failing the snapshot. Timeout
handling is conservative: once a probe times out, Tendwire stops that
compatibility/fallback chain instead of trying every remaining variant.
Snapshot, command-observation, and doctor probe chains also use an aggregate
deadline derived from the per-probe timeout and planned probes; remaining
subprocess timeouts are capped by the time left in that budget.

### Live Herdr smoke harness

The Herdr smoke harness is an **opt-in Tendwire-only check** for the boundary
between Tendwire's public contracts and Herdr's daemon/socket/command surfaces.
Normal `tendwire` commands and ordinary pytest do not contact or mutate live
Herdr. Live contact requires either `--live` or
`TENDWIRE_HERDR_LIVE_SMOKE=1`, and the harness does not add Herdres, source
polling, connector delivery, outbox processing, UI integration, or raw terminal
control.

Live mode proves Tendwire's own Herdr assumptions end to end: discovery,
temporary worker attachment when Herdr exposes a safe operation for it,
observation, high-level send addressing, target validation, event
subscriptions, public binding updates for status/move/close events, degraded
backend behavior, and public-safe evidence generation.

Safe live command:

```bash
python3 scripts/herdr_smoke.py --live
```

The optional `scripts/live_herdr_smoke.sh` wrapper remains only a thin shell
entrypoint for the same opt-in live mode.

The command above gives child Herdr commands an isolated default
`HERDR_SESSION=tendwire-smoke` when the caller has not already chosen a session,
and the live subprocess calls address that selected scope explicitly with
`herdr --session <selected> ...`. The environment variable alone is not treated
as enough isolation. That default is intentional: the smoke suite must never
silently target a daily Herdr session.

Two override paths are available and both are deliberate risk:

- `python3 scripts/herdr_smoke.py --live --session VALUE` sets the child
  `HERDR_SESSION` to `VALUE`.
- Running with an existing caller `HERDR_SESSION` preserves that value as an
  explicit caller override.

The smoke output never prints the actual session value. If you prefer an
environment opt-in instead of the flag, this is equivalent to `--live`:

```bash
TENDWIRE_HERDR_LIVE_SMOKE=1 python3 scripts/herdr_smoke.py
```

Without `--live`, without `TENDWIRE_HERDR_LIVE_SMOKE=1`, and without fixture
replay, the harness prints a valid JSON skip summary and makes no Herdr
subprocess or socket calls. Fixture replay is deterministic and offline:

```bash
python3 scripts/herdr_smoke.py --fixture-dir tests/fixtures/herdr/live_smoke/ok
```

Fixture mode validates the same public summary shape as live mode but reads only
fixture files, so normal offline pytest can import and exercise the harness
without contacting Herdr. Any future live pytest coverage must be explicitly
selected by maintainers rather than running by default.

Live mode first checks that the selected Herdr scope is available. If the default
smoke scope is stopped, unsupported, or unavailable, the harness fails closed
with `ok: false` and does not continue into workspace/agent observation or send
checks. A high-level `herdr agent send` probe only counts as successful when the
command exits zero and the harness records at least one accepted send.

Some Herdr installations do not expose safe live create, move, close, or
degraded-backend operations. When those operations are unsafe or unavailable,
the live harness reports the affected record as `live_skipped_unreliable`
instead of mutating a real workspace. Deterministic fixture or fake-backed
Tendwire validation still proves the public contract for the create/move/close
and degradation scenarios.

Live prerequisites are intentionally narrow:

- The `herdr` binary is on `PATH`, or `--herdr-bin /path/to/herdr` points at it.
- Herdr supports the high-level workspace/agent surfaces and the socket/event
  surfaces used by Tendwire's daemon and command boundary.
- You are willing to create temporary smoke workers in the selected Herdr
  session only when Herdr exposes safe operations for doing so.
- The selected session is a disposable/sandbox session unless you intentionally
  override the isolated default.

The smoke evidence records are:

- `create_attach`
- `observe`
- `send_addressing`
- `target_validation`
- `event_subscription`
- `status_agent_status_changed`
- `pane_moved_binding_update`
- `close_exited`
- `degraded_backend_preserves_workers`
- `public_safety`

Stdout is a public-safe aggregate JSON evidence artifact. Its top-level shape is
limited to neutral fields such as `schema_version`, `ok`, `mode`, `status`,
`summary`, `default_isolated_session`, `explicit_session`, `checks`, and
`failures`. Individual records use aggregate fields such as `name`, `status`,
`required`, `ok`, `exit_code`, `json_status`, `item_count`, `variants`, and
`detail`.

The public smoke summary is recursively sanitized and must not expose raw
`pane_id`, `terminal_id`, backend targets, socket paths, target values,
Telegram or Herdres IDs, tokens, private bindings, private fingerprints,
stdout, stderr, env, argv, secrets, or raw Herdr payloads.

Non-goals:

- No Herdres import, mutation, connector bridge, Telegram delivery integration,
  or connector outbox draining.
- No UI, local LLM, MCP, Hermes, iOS, or AR integration.
- No raw Herdr pane, socket-path, terminal, PTY, shell, or low-level command
  control exposure.
- No default contact with live Herdr from ordinary CLI usage or normal pytest.

### Daemon, socket backend, and Herdr event subscriptions

Tendwire exposes a stdlib-only local daemon:

```bash
tendwire daemon --db-path /path/to/tendwire.db
tendwire daemon --db-path /path/to/tendwire.db --socket-path /run/tendwire/tendwire.sock --socket-group tendwire-clients
```

On POSIX systems the daemon serves a local Unix domain socket JSON
request/response API. Startup loads the normal Tendwire config,
initializes/migrates the SQLite store, performs one authoritative initial
reconcile, persists the resulting snapshot and projections, starts the
background turn-ingestion scheduler, and requests its initial refresh before
publishing the socket. The public methods are `ping`, `health.get`,
`snapshot.get`, `attention.list`, `turn.list`, `turn.content.get`,
`pending.list`, `command.submit`, `connector.poll`, `connector.ack`,
`connector.fail`, `connector.defer`, and `connector.reclaim`. Read handlers
including `health.get`, `snapshot.get`, `attention.list`, `turn.list`, and
`pending.list` serve persisted/cached projections only; they never wait for a
private turn adapter.

The socket server has eight request workers and admits at most 32 in-flight
connections. Excess admissions receive public error `server_busy` with
`details.retryable=true`; capacity becomes available again as requests finish.
Request and response frames remain limited to 1 MiB (1,048,576 bytes). An
oversized request is rejected, and an oversized response is replaced by
`response_too_large` without making the server unavailable.

POSIX local state is private-only by default. Tendwire enforces mode `0700` on
its state directory and mode `0600` on its database family and regular private
files. The default Unix socket is mode `0600`. If an entry owned by the service
account is broader, Tendwire removes excess bits using the intersection of its
current and required modes; it never widens a stricter mode. Symlinks, wrong
owners, and wrong entry types are refused, and private files are created
securely before publication.

Socket group access is an explicit daemon-only option:
`--socket-group GROUP` (or `TENDWIRE_SOCKET_GROUP=GROUP`). At runtime Tendwire
resolves the existing group and verifies that the service account is already a
member before changing group ownership or permissions. Only the socket changes
to mode `0660`; database and other state files stay private. Every validated
group member can invoke the full daemon API, including mutating commands and
connector operations. Put a shared socket in a dedicated parent owned by the
service account, assigned to that group, group-traversable, and inaccessible to
other users (for example, mode `0710`). Never use a shared `/tmp` socket.

Existing CLI commands remain one-shot by default. When `--socket-path` or
`TENDWIRE_SOCKET_PATH` selects a daemon, any received daemon result or domain
error is authoritative. `tendwire turns` performs its single direct-source
fallback only when the daemon is definitely unavailable before request bytes
were sent: for an initial page it runs one refresh with the configured turn
worker bound, per-adapter `TENDWIRE_HERDR_TIMEOUT_SECONDS`, and total bound of
that timeout plus one second, then reads one store page. `--cursor` and
`--since` continuations read the store without refreshing. A timeout after
transmission may mean the daemon started the request, so the CLI returns
`daemon_timeout` and does not run a second refresh.
Other invalid daemon exchanges return `daemon_protocol_error`.

Mutating `command --json` requests (`send_instruction` or `answer_pending` with
`dry_run: false`) do **not** fall back in explicit daemon/socket mode. A
missing/refused daemon proven unavailable before request start returns an exact
schema-v2 `backend_unavailable/no_receipt` envelope and exits `1`, before any
one-shot Herdr send is attempted. After request start, a daemon timeout or
malformed response is not itself command authority: the CLI first attempts an
exact durable receipt replay. It returns that schema-v2 envelope when proven;
otherwise it prints no stdout envelope and exits `2` because the process result
is ambiguous. The daemon API never returns raw Herdr pane IDs, terminal IDs,
backend targets, socket paths, target values, private fingerprints,
argv/env/stdout/stderr, tokens, or secrets.

The Herdr socket/event backend is opt-in:

```bash
TENDWIRE_HERDR_BACKEND=socket tendwire daemon --db-path /path/to/tendwire.db
```

With `TENDWIRE_HERDR_BACKEND=socket`, the daemon connects to Herdr's socket,
performs the initial reconcile, writes the authoritative public snapshot, and
then maintains projections from Herdr events. The official Herdr subscription
method is exactly `events.subscribe`; the params object is exactly a
`subscriptions` array of objects with a string `type`:

```json
{
  "subscriptions": [
    {"type": "workspace.created"},
    {"type": "workspace.updated"},
    {"type": "workspace.renamed"},
    {"type": "workspace.closed"},
    {"type": "workspace.focused"},
    {"type": "pane.created"},
    {"type": "pane.closed"},
    {"type": "pane.focused"},
    {"type": "pane.moved"},
    {"type": "pane.exited"},
    {"type": "pane.agent_detected"},
    {"type": "pane.output_matched"},
    {"type": "pane.agent_status_changed"},
    {"type": "worktree.created"},
    {"type": "worktree.opened"},
    {"type": "worktree.removed"}
  ]
}
```

Subscription builders reject unknown names, non-string names, and empty names.
Tendwire may tolerate legacy inbound aliases only after receiving an event, so
older Herdr payloads can still be harmlessly normalized. Tendwire must never
subscribe to legacy names such as `pane.observed`, `workspace.observed`,
`agent.status_changed`, or `worktree.updated`.

Unknown and malformed events are safe no-ops. Initial reconcile remains
authoritative; event updates are incremental hints applied on top of that
snapshot. Idle event-read timeouts are normal and do not mark Herdr failed.
Disconnects and protocol failures degrade backend health but do not prune
private worker bindings or pretend Herdr is authoritatively empty. Reconnects
resubscribe to the same official event set.

The daemon owns ongoing turn ingestion. It scans eligible durable Codex, OMP,
and pane bindings immediately at startup and every two seconds by default.
Persisted
`pane.created`, `pane.focused`, `pane.moved`, `pane.closed`, `pane.exited`,
`pane.agent_detected`, `pane.agent_status_changed`, and
`pane.output_matched` batches, plus a completed full reconcile, request an
earlier scan. Bursts coalesce into one pending scan; duplicate queued work
coalesces, and a signal for a running target requests at most one follow-up
refresh.

Refreshes are serialized by private binding fingerprint, so one target is
never read concurrently with itself while distinct targets can use the fixed
four-worker pool. The scheduler queue is bounded at 64; a full queue records
aggregate `queue_full` evidence and rotates later scans rather than growing
without bound. Every adapter read uses `TENDWIRE_HERDR_TIMEOUT_SECONDS`;
filesystem adapters receive bounded timeout/cancellation cleanup, and each
result is revalidated against the current binding before commit.
Adapter failures and timeouts make current ingestion health `degraded` until a
later successful refresh clears the consecutive-failure state; the aggregate
`failed` and `timed_out` counters remain cumulative. Binding churn reported as
`stale_binding` increments `failed` for evidence but does not poison current
health. Absence of a recent successful refresh becomes `stale`. Cached daemon
reads remain available while an adapter is blocked or ingestion is degraded.

OMP caching and IPC use a coordinate-only checkpoint: EOF/parse offset,
observed device/inode/size/mtime/ctime, replay offset, open-turn flag, and
validated project root. The checkpoint never contains or transports prompt
IDs, user text, assistant final/stream text, or other turn bodies. An unchanged
stable stat returns `unchanged` without spawning a child, reading the session,
or transporting a frame. When the file changes, an open turn is reconstructed
from its replay coordinate; a completed final compacts to an idle EOF
checkpoint, and only a later eligible user message opens another turn. Its
private checkpoint LRU is bounded by both 64 entries and 64 KiB of serialized
cache-key-plus-checkpoint weight.

Codex bindings accept only exact canonical lowercase, non-nil UUIDs. A rollout
must have the exact path
`sessions/YYYY/MM/DD/rollout-YYYY-MM-DDTHH-MM-SS-<uuid>.jsonl`, with a valid
date/time matching its hierarchy; the identifier is never interpolated into a
glob. Resolution succeeds only for one canonical regular file beneath the
canonical sessions root after symlink and device/inode checks. Missing, unsafe,
over-limit, or multiple exact matches are unavailable.
Every cache hit validates the current sessions-root device/inode; a found result
also validates the rollout inode. A root identity change immediately clears
that root's cached path results and complete index.

The complete Codex index is bounded to 100,000 visited filesystem entries,
100,000 session identities, and 16 MiB retained; its path-result LRU is bounded
to 256 entries and 256 KiB. Negative results live for 2 seconds. A lookup
rebuilds the complete index once its snapshot is 60 seconds old. This
deliberately avoids a 20,000-file walk on each poll, so a duplicate created
after a successful lookup may remain undiscovered for up to one 60-second
snapshot interval; the refreshed index makes that identity ambiguous and
therefore unavailable.

The private Codex parser LRU is bounded to 64 entries and 16 MiB. State commits
only through valid newline-terminated records, retaining a partial final record
until its newline arrives. A record is limited to 8 MiB, cold recovery starts
with 64 KiB and never scans more than 16 MiB or 65,536 records, and one poll
reads at most 64 MiB. Warm append-only polls read only newly appended bytes and
an unchanged poll reads zero source bytes. Truncation, rotation, replacement,
or lost state triggers the same bounded recovery rather than a whole-file
fallback; malformed/oversized input or failure to find a boundary fails the
read without advancing the prior checkpoint.
If one delayed append read contains both a newly completed turn and the start
of a later turn, ingestion publishes and checkpoints the completed turn first.
The next refresh resumes at the following record. A delayed poll therefore
cannot silently replace an unpersisted final with the newer open turn.
No Codex session ID, rollout path, file identity/coordinate, parser/cache state,
or raw record enters public JSON.

OMP reader requests remain capped at 16 KiB. A canonical OMP response has no
total-size ceiling: its exact ordered payload is streamed in frames of at most
1 MiB under the same deadline, with manifest, nonce, end-marker, and EOF
validation, so canonical finals are not truncated by IPC. A Codex parser-state
request is capped at 12 MiB, and each Codex response remains one frame capped
at 64 MiB. The parent performs nonblocking framed socket send and receive,
parsing, and child join
under one adapter deadline without a helper IPC thread. Timeout cleanup spends
at most 250 ms on terminate/kill/join attempts and reaps the child under normal
POSIX scheduling; it does not wait beyond that grace for an OS-uninterruptible
child.
Binding scans prune disappeared cache entries and advance a generation when the
same resolved target's private-fingerprint set changes.

A valid file-reader response yields only a candidate checkpoint. A
content-bearing candidate is published only after the parent revalidates the
exact binding and durably applies the turn; a no-content candidate still
requires exact binding validation. Cancellation, failed apply, stale binding,
disappeared path, or a changed same-path fingerprint generation cannot advance
the cache.

Shutdown first stops socket admission, then flushes/detaches event signaling,
cancels and boundedly drains turn work, and finally stops the event backend.
Repeated stop requests are safe. A stopped daemon or scheduler is not restarted
in place: service restart constructs a new instance, whose immediate scan
reuses durable bindings and preserves already-final turns and connector state.

PR16 adds conservative daemon/runtime tuning knobs for 24/7 and Raspberry Pi
use. They are available as `Config` constructor arguments and environment
variables:

| Config field | Environment variable | Default | Validation |
| --- | --- | --- | --- |
| `event_debounce_seconds` | `TENDWIRE_EVENT_DEBOUNCE_SECONDS` | `0.05` | non-negative float |
| `reconcile_interval_seconds` | `TENDWIRE_RECONCILE_INTERVAL_SECONDS` | `300.0` | non-negative float; `0` disables periodic reconcile |
| `event_retention_days` | `TENDWIRE_EVENT_RETENTION_DAYS` | `7` | integer >= 1 |
| `output_excerpt_chars` | `TENDWIRE_OUTPUT_EXCERPT_CHARS` | `200` | integer >= 1 |
| `max_workers` | `TENDWIRE_MAX_WORKERS` | `512` | integer >= 1 |
| `max_outbox_attempts` | `TENDWIRE_MAX_OUTBOX_ATTEMPTS` | `10` | integer >= 1 |
| `connector_claim_ttl_seconds` | `TENDWIRE_CONNECTOR_CLAIM_TTL_SECONDS` | `60` | integer >= 1 |
| `connector_max_claim_ttl_seconds` | `TENDWIRE_CONNECTOR_MAX_CLAIM_TTL_SECONDS` | `300` | integer >= 1; caps `turn-final` poll and renew requests |
| `connector_ack_ttl_seconds` | `TENDWIRE_CONNECTOR_ACK_TTL_SECONDS` | `60` | integer >= 1; grace period for committed plans awaiting ACK completion |
| `command_retry_horizon_seconds` | `TENDWIRE_COMMAND_RETRY_HORIZON_SECONDS` | `604800` | positive integer no greater than 604800 |
| `command_receipt_retention_seconds` | `TENDWIRE_COMMAND_RECEIPT_RETENTION_SECONDS` | `2592000` | integer at least 691200 and strictly greater than the retry horizon |
| `command_receipt_retention_count` | `TENDWIRE_COMMAND_RECEIPT_RETENTION_COUNT` | `4096` | positive integer; newest bounded inactive receipts per host |
| `acknowledged_final_retention_days` | `TENDWIRE_ACKNOWLEDGED_FINAL_RETENTION_DAYS` | `30` | positive integer |
| `acknowledged_final_retention_count` | `TENDWIRE_ACKNOWLEDGED_FINAL_RETENTION_COUNT` | `4096` | positive integer; newest proven acknowledged finals per host |
| `snapshot_retention_days` | `TENDWIRE_SNAPSHOT_RETENTION_DAYS` | `14` | positive integer |
| `snapshot_retention_count` | `TENDWIRE_SNAPSHOT_RETENTION_COUNT` | `4096` | positive integer; includes each host's latest row |
| `snapshot_maintenance_batch_size` | `TENDWIRE_SNAPSHOT_MAINTENANCE_BATCH_SIZE` | `100` | integer from 1 through 1000 |
| `store_maintenance_cadence_seconds` | `TENDWIRE_STORE_MAINTENANCE_CADENCE_SECONDS` | `3600` | positive integer |
| `turn_refresh_interval_seconds` | `TENDWIRE_TURN_REFRESH_INTERVAL_SECONDS` | `2.0` | finite positive float |
| `turn_refresh_workers` | `TENDWIRE_TURN_REFRESH_WORKERS` | `4` | integer from 1 through 32 and no greater than `max_workers` |
| `turn_model` | `TENDWIRE_TURN_MODEL` | `observed` | `observed`; `legacy`, `dual`, and `shadow` are deprecated aliases with identical observed behavior |

The socket/event backend uses `event_debounce_seconds` for event batching and
`reconcile_interval_seconds` for bounded periodic full reconciles. Set
`TENDWIRE_RECONCILE_INTERVAL_SECONDS=0` on very small hosts if periodic
reconcile is not wanted. `max_workers` is an operational cap only; it does not
create any public worker ID surface. If a healthy reconcile observes more
workers than the cap, Tendwire reports a degraded
`worker_cap_exceeded` backend health state and preserves the previous public
snapshot/projections instead of publishing a truncated authoritative snapshot.
Incremental events that would add workers over the cap are ignored with the
same public-safe degraded evidence.

Snapshot history defaults are sized for a five-minute observation rhythm:
$14 \times 24 \times 12 = 4032$ observations, while the 4096-row count
ceiling (including the latest row) leaves 64 rows of headroom. This is a
changed-history bound rather than a promise to write on every observation:
when a new snapshot has the same content fingerprint as the immediately
preceding snapshot for that host, Tendwire refreshes that row's timestamp and
canonical payload instead of appending another history row. If the content
later changes back, it is a new non-adjacent history row.

An active `agent.list` row must resolve to one authoritative `pane.list` owner
before a healthy source snapshot can replace authenticated worker continuity.
If both probes succeed but that match is missing, Tendwire reports
`continuity_unavailable` and retains the previous authenticated snapshot and
bindings. This treats cross-probe lifecycle skew as non-authoritative without
turning it into a permanent connector quarantine.

`health.get` remains schema-version 1 and includes public-safe operational
fields: daemon status and `started_at`; store status/counts and outbox counts;
snapshot and last event/snapshot/reconcile timestamps when available; backend
runtime readiness when the socket backend is active; backend health; and numeric
`limits` for debounce, reconcile, event retention, output excerpt, worker cap,
outbox attempt/claim TTL, snapshot retention age/count/batch/cadence, exact
acknowledged-final age/count, and exact command retry-horizon and receipt
retention age/count values. `store.command_requests` is aggregate-only:
`total`, the five state counters, `stale_active`, `eligible`,
`retry_horizon_seconds`, `retention_seconds`, `retention_count`, and
`storage_pressure`. It never exposes a request ID, action, canonical request or
fingerprint, resolved worker, instruction, pending choice, or private binding.
`store.final_retention` is also aggregate-only:
`acknowledged`, `unresolved`, `queued`, `leased`, `deferred`, `retry`,
`dead_letter`, `awaiting_ack`, `eligible`,
`acknowledged_final_retention_days`,
`acknowledged_final_retention_count`, and `storage_pressure`. Storage pressure
degrades the public store and overall health status without exposing final
content, turn/revision/final identities, private connector state, or source
paths. The aggregate `turn_ingestion` object contains `status`, `queue`,
`active`, `refreshed`, `failed`, `timed_out`, `coalesced`, `queue_full`,
`last_success`, `last_duration_ms`, `stale_age`, and
`bounds.{refresh_interval_seconds,max_workers,queue_capacity,adapter_timeout_seconds}`.
It never identifies which worker or binding failed. Health output does not
expose daemon socket paths, database paths, Herdr binary paths, backend targets,
raw Herdr payloads, private bindings or fingerprints, connector private state,
tokens, argv/env/stdout/stderr, or low-level terminal identifiers.

Optional local persistence uses the stdlib SQLite store and does not change
stdout:

```bash
tendwire snapshot --json --store
tendwire snapshot --json --store --db-path /path/to/tendwire.db
tendwire attention --json --db-path /path/to/tendwire.db
```

Without `--store`, the CLI does not write the database. `--db-path` overrides
the configured database path for persistence and store-backed public views.

## Milestone 2 neutral snapshot contract

The `snapshot --json` output uses this device-neutral contract:

```json
{
  "schema_version": 2,
  "host_id": "myhostname",
  "updated_at": "2026-06-27T16:18:34+00:00",
  "content_fingerprint": "14afa7e139f55770113291c1",
  "spaces": [],
  "workers": [],
  "attention": [],
  "backend_health": [
    {
      "name": "herdr",
      "status": "healthy",
      "outcome": "empty_healthy",
      "observed_at": "2026-06-27T16:18:34+00:00",
      "message": "Herdr observation is healthy but empty",
      "counts": {
        "spaces": 0,
        "workers": 0
      }
    }
  ]
}
```

Top-level keys:

- `schema_version` — integer contract version; milestone 2 snapshots use `2`.
- `host_id` — string identifying the host.
- `updated_at` — ISO-8601 UTC timestamp string for this snapshot.
- `content_fingerprint` — deterministic hash prefix for the snapshot content,
  excluding `updated_at` and `content_fingerprint` themselves.
- `spaces` — neutral space observations with stable `id`, `name`, canonical
  `status`, optional timestamps/status text, per-item `fingerprint`, and `meta`.
- `workers` — neutral worker observations with stable `id`, `name`, canonical
  `status`, optional `last_seen_at`/summary, per-item `fingerprint`, and `meta`.
- `attention` — deterministic human-actionable attention signals derived from
  the snapshot.
- `backend_health` — public-safe backend observation health. Tendwire includes
  a Herdr entry with fixed fields `name`, `status`, `outcome`, `observed_at`,
  `message`, and optional aggregate `counts` such as spaces and workers.

Canonical statuses are `unknown`, `active`, `idle`, `waiting`, `blocked`,
`warning`, `done`, `failed`, and `closed`. `done`, `complete`, `completed`, and
`success` canonicalize to `done`, which remains eligible for follow-up
instructions. Raw adapter status strings may appear only as sanitized
`meta.raw_status`; connector-specific fields and private backend fields are
stripped before public JSON is emitted.

Snapshot hashing uses the Python standard library only:
`json.dumps(sort_keys=True, separators=(",", ":"), ensure_ascii=False)`, then
SHA-256 with a fixed prefix. Spaces, workers, and attention entries are sorted by
stable ID/fingerprint before the hash is computed. Volatile timestamps,
including backend health `observed_at`, are excluded from the content
fingerprint.

Backend health `status` is one of `healthy`, `degraded`, `unavailable`, or
`unknown`. A healthy non-empty Herdr observation reports
`outcome: "healthy_non_empty"`; a healthy empty Herdr observation reports
`outcome: "empty_healthy"`. Missing Herdr reports `missing_binary`, launch
errors report `launch_error`, timeouts report `timeout` or
`deadline_exhausted`, nonzero exits report `nonzero`, malformed JSON reports
`malformed_json`, worker cap safety stops report `worker_cap_exceeded`, and
unclassified results report `unknown`. Health messages are short sanitized
public strings and do not expose private bindings, raw stdout/stderr, argv,
environment values, or secrets.

Attention signals expose deterministic `id` and `fingerprint` values plus
`kind`, `severity`, `status`, `reason`, `source`, `updated_at`,
`suggested_actions`, and `meta`. The attention fingerprint input includes
`host_id`, `source`, `kind`, `severity`, `reason`, and normalized `status`, so
the same logical condition keeps the same attention ID across snapshots.
Suggested actions are neutral data with `action_id`, `label`,
`tendwire_action`, and `params`; they do not include delivery state or raw shell
command fields.

Attention is for human-actionable items, not a general worker status feed.
Empty snapshots and workers that are idle, active/running, done/completed, or
closed do not create attention items by default. Failed/error workers create
critical attention; blocked/warning workers create warning attention.
Waiting/pending workers create attention only when structured metadata or very
explicit summary text says human input, review, or approval is required. Generic
waiting, responding, or pending wording alone is not enough.

Worker-derived attention `updated_at` uses a source worker timestamp when one is
available, such as `last_seen_at` or a backend `updated_at` normalized into
`last_seen_at`. It does not fall back to the snapshot `updated_at`; when no
worker source time exists, the attention item serializes `updated_at` as `null`.

## Public turn and pending-interaction contract

The `turns --json` and `pending --json` outputs are public snapshot-adjacent
views, not routing or private binding surfaces. They must not expose Telegram
IDs, chat IDs, topic IDs, message IDs, raw pane IDs, terminal IDs, backend
targets, socket paths, raw target values, private bindings, session IDs, private
fingerprints, raw command payloads, argv/env/stdout/stderr, raw terminal
controls, tokens, or secrets.

### Turn-list paging, stability, and schema negotiation

`turn.list` accepts only `schema_version`, `limit`, `cursor`, and `since`.
Schema version defaults to 1 and may be 1 or 2. The limit defaults to 100 and
must be an integer from 1 through 250. `cursor` and `since` are opaque,
nonempty tokens and are mutually exclusive.

A successful page contains exactly the wrapper fields `schema_version`,
`host_id`, `updated_at`, `turns`, `backend_health`, `next_cursor`, `has_more`,
`as_of`, `since`, and `content_fingerprint`. `as_of` and `since` carry the same
watermark token. Pages are ordered by public worker ID, then immutable
per-host insertion sequence newest-first, then turn ID. A continuation is
bound to the original host, schema, limit, optional since position, watermark,
store generation, and last row. Inserts after that watermark cannot enter the
traversal, so a first-page result set remains stable while it is paged; use the
first page's `since` token in a later new request to discover newly inserted
turns.

Continuation cursors have a fixed 900-second TTL measured from the first page;
following a cursor does not extend it. Malformed, tampered, or cross-bound
cursors return `invalid_cursor`; elapsed cursors, removed anchors, retention
that advances the floor beyond the traversal's original floor, or a changed
store generation return `cursor_expired`. Since
tokens have no wall-clock TTL, but a changed store generation or impossible
watermark returns `since_expired`. These are domain results inside a valid
daemon success envelope. Unsupported schema and malformed parameters are API
errors (`unsupported_schema` or `invalid_params`); an unavailable store returns
`store_unavailable`. Every list page remains below the unchanged 1 MiB response
frame bound, even when fewer than the requested number of turns fit.

`turns --json --schema-version 2` and daemon `turn.list` with
`{"schema_version":2}` return the schema-v2 turn-list wrapper. Each turn keeps
the neutral identity/status metadata above and adds a schema-v1 `content`
descriptor:

```json
{
  "schema_version": 1,
  "content_revision": "twrev1.<opaque>",
  "known_incomplete": false,
  "fields": {
    "user_text": {
      "availability": "complete",
      "inline": true,
      "char_length": 12,
      "byte_length": 12,
      "page_count": 1,
      "first_cursor": null
    },
    "assistant_final_text": {
      "availability": "complete",
      "inline": false,
      "char_length": 20000,
      "byte_length": 20000,
      "page_count": 1,
      "first_cursor": "twcur1.<opaque>"
    }
  }
}
```

The two descriptor fields are exactly `user_text` and
`assistant_final_text`. Their `availability` is exactly `absent`, `complete`,
or `known_incomplete`; lengths are exact character and UTF-8 byte counts.
Complete fields of at most 12,000 characters remain inline. A longer complete
field is not copied into the list: the turn contains only a `user_preview` or
`assistant_final_preview` of at most 1,000 characters and the descriptor makes
it pageable. `known_incomplete` means Tendwire inherited content that was
already irrecoverably truncated; it is preview-only, with `inline=false`,
`page_count=0`, and `first_cursor=null`. Consumers must isolate that field or
turn and continue processing eligible turns rather than presenting its preview
as a complete final.

Schema v1 remains a compatibility view only while every present canonical field
is complete and inline. If any present field is long or known incomplete, a v1
request fails clearly with `schema_version=1`, `ok=false`,
`status=upgrade_required`, and `required_turn_schema_version=2`; the CLI exits
nonzero instead of returning a lossy v1 turn. Consumers that support v2 can
remain lazy: only a field with `availability=complete`, `inline=false`, and a
non-null `first_cursor` is eligible for page retrieval.

The daemon method `turn.content.get` and CLI command
`tendwire turn content get` expose one content page at a time. The exact daemon
parameters are `schema_version: 1`, `turn_id`, `content_revision`, `field`
(`user_text` or `assistant_final_text`), and optional `cursor`. Omit `cursor`
or pass null for page zero; thereafter pass only the previous response's opaque
`next_cursor`. A cursor is integrity-bound to the revision, field, segment
index, and exact character/byte start, so malformed coordinates, a modified
cursor, or a cross-field or cross-revision cursor fails with `invalid_cursor`.

Each successful page contains only `schema_version=1`, `turn_id`,
`content_revision`, `field`, `availability=complete`, opaque `segment_id`,
zero-based `index`, `count`, exact `text`, `segment_char_length`,
`segment_byte_length`, `total_char_length`, `total_byte_length`, and nullable
`next_cursor`. Page text
is at most 49,152 UTF-8 bytes and never splits a code point. Concatenating pages
in `first_cursor`/`next_cursor` order reproduces the exact sanitized canonical
field. Paging is deliberately linear: Tendwire reads each page directly from
the one canonical SQLite value and does not store independently editable page
copies.

### Pending interactions

`pending --json` prints a schema-v1 wrapper with `host_id`, `updated_at`,
`content_fingerprint`, public-safe `backend_health`, and
`pending_interactions`. Each pending interaction has deterministic `id`,
`host_id`, `worker_id`, optional `worker_fingerprint`, optional `space_id`,
bounded `kind`, `question`, finite public-safe `choices`, neutral `status`,
optional timestamps, optional `fingerprint`, and sanitized `meta`. Each choice
has exactly a deterministic opaque `choice_id` and a user-facing `label`.
Backend option, tool, or decision identifiers and any values sent to the
backend stay private.

Daemon `pending.list` and the CLI's store fallback use the same durable
projection. One read transaction captures the latest stored public snapshot
and the corresponding `backend_pending` rows, so pending derivation never
mixes snapshots from different moments. Turn ingestion updates each worker's
backend-provided pending state durably with its refresh. A missing, malformed,
wrong-host, or otherwise invalid stored snapshot returns `store_unavailable`
rather than projecting partial state.

Successful reads with an open prompt upsert that prompt; successful reads with
no prompt retain a fresh non-answerable tombstone so snapshot fallback clears
for that worker. Authoritative binding removal deletes the worker's row.
Transient reads retain the last open prompt as `stale` for one fixed,
non-sliding `TENDWIRE_PENDING_STALE_GRACE_SECONDS` window (30 seconds by
default); repeated failures do not extend the deadline. At expiry the prompt
leaves the public overlay, but freshness health remains degraded until a
successful read or authoritative removal. A successfully read malformed prompt
leaves the backend overlay immediately, preserving any valid snapshot-derived
fallback while reporting degraded freshness.

`command.submit` action `answer_pending` accepts exactly the public
`pending_id`, `pending_fingerprint`, and `choice_id` in `params`; a non-dry-run
request also requires `request_id`. The store atomically validates the observed
revision, binding fingerprint, and exact private pane target before returning
only the private picker ordinal to the send path. A changed, stale,
disappeared, or already claimed decision fails before pane mutation.
Once sending starts, an indeterminate failure is reported as request-state
uncertainty and is never automatically retried. After confirmed submission,
the exact answered revision and claim atomically become a fresh non-answerable
tombstone before the accepted result is returned, so an older snapshot fallback
cannot reappear. A concurrently observed newer revision is never overwritten.

Any daemon response is authoritative. Only definite unavailability before
request transmission permits the CLI fallback, which reads that durable store
view without observing Herdr. A timeout after transmission returns the fixed
`daemon_timeout` error; other ambiguous/invalid exchanges return
`daemon_protocol_error`. Neither case starts a second request or source read.

Canonical turn IDs are owner-aware sanitized public identities. Their input
includes host, worker, space, kind, source, command/source-turn lineage, and
either the exact version-1 stable-key pair or an explicit unavailable marker.
The stable pair therefore participates in identity when authenticated, while
`worker_fingerprint`, status, and content affect only the turn fingerprint and
never final routing. Volatile observation timestamps affect neither value.
Backend pending and choice handles separately bind to a one-way digest of the
exact private source binding and pane target, so byte-identical prompts observed
on a replacement source mint different public handles without exposing either
private value. Pending interactions derive only from explicit human-actionable
public attention signals or suggested actions; generic waiting or pending
worker status alone does not create one.

## SQLite store

The current store schema is version 14 (`PRAGMA user_version=14`). Migration is
idempotent and transactional. Schema v6 introduced immutable canonical turn
content revisions and backfilled legacy rows, marking a legacy `[truncated]`
value `known_incomplete` rather than claiming recovery. Schema v7 added
presentation-plan generations, failed-plan lineage, and an immutable recovery
audit. Schema v8 added persisted maintenance state and bounded
snapshot-retention indexes. Schema v9 added an immutable positive
`list_sequence` for each host/turn, a per-host uniqueness constraint and paging
index, and a store-generation row used to bind list cursors and since tokens.
Schema v10 adds explicit pending observation/freshness state, private
revision-bound choice routing, and durable two-phase answer claims. The
v9-to-v10 migration preserves legacy public pending rows but leaves them
unrouted until a fresh binding-scoped observation supplies authoritative
revision and route data.

Schema v11 adds typed durable final-delivery roots, acknowledged-history
retention, and the private per-host `store_maintenance_cursors` table used for
fair automatic service. A routable root copies the exact public `stable_key`
and integer `stable_key_version=1` from persisted turn metadata into the root of
its `schema_version=2` payload. Every materialized range job preserves that
exact source route under `turn`. Neither `worker_id` nor `worker_fingerprint`
is final-delivery routing authority.

Schema v12 rebuilds command receipts around one host-wide
`(host_id, request_id)` authority, canonical resolved public-worker identity,
and the explicit `reserved`, `send_started`, `accepted`, `rejected`, and
`uncertain` lifecycle. The transactional v11-to-v12 migration converts
ambiguous legacy action-scoped collisions into terminal uncertainty rather
than selecting one mutation as authoritative.

Schema v13 adds a private selector proof to new command receipts so exact alias
retries can replay durable outcomes after worker churn without trusting a
mutable snapshot. Migrated v12 receipts receive no invented proof and therefore
fail closed when their original selector cannot be established. Schema v14
repairs legacy nonpositive turn-list coordinates transactionally and preserves
the durable per-host sequence high-water mark, preventing cursor recurrence
after deletion or migration.

A missing, partial, malformed, boolean-valued, or unsupported owner pair creates
a nonroutable `schema_version=1` `final_migration_hold`/`dead_letter`.
Internally classified automation and known-incomplete finals are also
nonpollable holds, even when a schema-v2 pair exists. These safety holds are
permanently nonretryable for that canonical turn identity; a later
owner-authenticated observation has a distinct owner-aware turn identity rather
than converting or reposting the held one.

The conservative v10-to-v11 migration marks a current final
`final_ready`/`delivered` only when a completed lineage has exact canonical
range coverage and exact host-bound all-part ACK proof. A linkable unresolved
plan must additionally carry the authoritative schema-v2
turn/revision/final-identity/stable-key route on every job before it can remain
nonpollable `final_ready`/`awaiting_ack`; every unknown or mismatched case
becomes a migration hold. A valid-owner hold whose only defect was unproven
legacy delivery may be eligible for an exact current-identity retry, but safety
holds are not. The migration never infers delivery from partial evidence or
mass-reposts history.

Migration creates the typed columns, root indexes, and maintenance-cursor table
inside the v10-to-v11 transaction. A partial legacy final-table set, invalid
recovery edge, descriptor failure, or any later error rolls back all table,
column, root, and `PRAGMA user_version` changes. Migrations do not reconstruct
absent bytes or rewrite canonical turn history.

The optional SQLite store keeps canonical snapshot JSON blobs in the `snapshots`
table and maintains Tendwire-local operational tables for attention lifecycle,
command receipts, connector outbox/deliveries, backend health, and private
worker bindings. Schema initialization and migration are idempotent and preserve
the existing `latest_snapshot` and `list_hosts` behavior. The store is an
implementation detail behind public JSON, not a broad public schema expansion.

Snapshot and private-binding projection is monotonic and atomic per host. The
strict winner is the greatest `(updated_at, content_fingerprint)` pair; an exact
replay is accepted without advancing state. An older or losing equal-time
snapshot, even if different or empty, cannot replace history, regress/prune
worker or turn projections, mutate/expire bindings, or duplicate/release a
final root. A winning snapshot refreshes its public projection and upserts or
expires only its same-scope private bindings in the same transaction.

Private Herdr worker bindings are stored separately in the local
`worker_bindings` table. These rows associate a stable public Tendwire
`worker_id` with private backend target material such as Herdr agent, terminal,
or pane identifiers. Bindings are local SQLite records only; they are not public
snapshot fields, command request fields, command response fields, connector
payload fields, or stored snapshot payload fields. Expired bindings are retained
for local history and debugging but ignored by command routing.

Every store connection applies a 30-second SQLite `busy_timeout`; file-backed
databases use WAL journaling, foreign keys are enabled, and synchronous mode is
`NORMAL`. The current safety stance is one local Tendwire service process: its
bounded turn-ingestion workers can persist distinct targets concurrently
through the store's transaction APIs, while external Tendwire writers are not a
supported multi-service event bus. One-shot CLI persistence writes only when
explicitly requested. The store does not persist raw Herdr event payloads,
socket paths, terminal streams, argv/env/stdout/stderr, or connector-specific
state.

### SQLite family lifecycle and race boundary

A file-backed store is one validated family: the main database plus the
optional `-wal`, `-shm`, and `-journal` members. An absent optional sidecar is
normal, and a transient optional that disappears while the family is being
captured is also accepted as absent. Once the main database has been selected,
however, it is mandatory: disappearance or identity replacement fails closed.
Every present member must remain an owned regular file at the selected identity.
A symlink, wrong type, wrong owner, insecure mode on a validation-only path, or
replacement race is rejected without following, changing, or deleting the
hostile entry.

Creation, repair, and inspection are separate authorities. Explicit store
startup/creation uses the SQLite-family prepare operation, which may create only
the missing main database and may narrow permissions on validated present
members. Explicit repair narrows validated existing members only. Neither
operation creates an absent optional sidecar or widens a stricter mode.
Private-mode preparation and repair cannot disturb active Tendwire SQLite
transactions, and a no-op private prepare preserves them. Any main creation or
permission narrowing first requires bounded, nonblocking exclusive authority over
the store parent directory. Current-schema filesystem reads stay cheap and
nonmutating after their schema-version read: they take no exclusive parent
authority and perform no persistent WAL negotiation or schema DDL. An
uninitialized or migrating filesystem store takes that exclusive authority before
persistent WAL negotiation or schema DDL, performs that work under private
creation mode, then revalidates and narrows the resulting main database, `-wal`,
and `-shm` members before restoring retained shared authority. A live Tendwire
connection retains shared parent-directory authority, so a conflicting repair
fails with a typed, path-free error before mutation; that shared authority also
rejects the schema branch before WAL, DDL, or sidecar mutation. A connection
obtains shared authority before preparation, promotes the same authority only for
a necessary mutation, and restores shared authority for the remainder of its lifetime.
Ordinary reads, `store status`, health collection, and `doctor` are
validation-only and non-creating: they do not initialize a missing database or
repair a family. Their finite capture, preflight, and final validation passes do
not recursively retry sidecar churn; stable transient absence succeeds and an
unstable identity fails closed.

Private filesystem failures are typed, path-free `LocalStateError` values.
Public diagnostics and store surfaces map them to fixed aggregate records such
as the `database_permissions` `unsafe` outcome or `store_unavailable`; they do
not expose a path, suffix, owner, inode, raw exception, or private content.
Automatic retention stays batch-bounded and has no compaction authority.
Compaction remains the separately acknowledged offline operation described
below, with the selected source identity revalidated before publication.

The synthetic Goal 08B race/recovery evidence is recorded in
`docs/evidence/goal08b-sqlite-sidecar-race-recovery.md`; its driver and focused
audit are `scripts/sqlite_sidecar_race_benchmark.py` and
`tests/test_sqlite_sidecar_race_benchmark.py`. The evidence uses only generated
private state and an isolated candidate from this source checkout.

Bounded operational store hooks are JSON-only:

```bash
tendwire store status --db-path /path/to/tendwire.db
tendwire store events-tail --limit 20 --db-path /path/to/tendwire.db
tendwire store cleanup --dry-run --db-path /path/to/tendwire.db
tendwire store cleanup --retention-days 14 --max-outbox-attempts 5 \
  --acknowledged-final-retention-days 30 \
  --acknowledged-final-retention-count 4096 \
  --snapshot-retention-days 14 --snapshot-retention-count 4096 \
  --snapshot-batch-size 100 --db-path /path/to/tendwire.db
tendwire store compact --dry-run --db-path /path/to/tendwire.db
tendwire store compact --execute --acknowledge-offline \
  --backup-path /path/to/tendwire.pre-compact.db \
  --db-path /path/to/tendwire.db
```

`store status` returns host-scoped table counts, last event/snapshot timestamps,
and aggregate outbox `pending`, `leased`, `terminal`, and `by_status` counts.
Its `maintenance.snapshot_count`, snapshot backlog, and `final_retention`
pressure are also computed for the requested host; only the persisted automatic
maintenance cadence marker is database-wide. `store events-tail` returns only
bounded event metadata such as row ID, event type, aggregate type, timestamp,
and content fingerprint; it never returns `payload_json` or raw event payloads.
The daemon accepts the retention aggregate only when its `host_id`, configured
policy, nonnegative counts, component totals, eligibility, and pressure
relationships validate. A wrong-host or malformed aggregate degrades health,
as does valid `storage_pressure`. The public surface exposes counts and
pressure only, never final content or turn, revision, final, provider, or
private-state identity.

`store cleanup` performs one bounded online batch and reports aggregate
`retention`, database-wide `snapshots`, `outbox`, `final_retention`,
`command_requests`, and `turn_content` objects. Event, outbox, final,
command-receipt, and turn-content policy defaults come from the normal
configuration. Retention age/count/batch/cadence values must be
positive integers. Day policies are capped at 365000, counts at
9223372036854775807, maintenance batches at 1000, and cadence at 31536000000
seconds; an affected cleanup class rejects an invalid override rather than
applying that class. The flags shown above override event age, retry count,
acknowledged-final age/count, snapshot age/count, and snapshot batch size for
that invocation. Snapshot retention keeps the intersection of the configured
age window and newest-per-host count window: a changed-history row is eligible
when it falls outside either window. The newest row for every host
is always exempt, even when it is old or the count is one. Final retention can
reduce only a whole current turn graph whose completed/superseded plan lineage
contains exactly every declared part, whose matching host/name/turn/revision
part rows are delivered, and whose delivered attempts have durable ACK
timestamps. Every root and plan for that turn must also be resolved; an
unlinked part or any queued, leased, deferred, `retry`, `awaiting_ack`, or
`dead_letter` work blocks cleanup and contributes to pressure. Only then is an
acknowledged graph eligible when older than the 30-day age window or ranked
beyond the newest 4096 per-host count window by default. Cleanup also does not
remove current projections, private worker bindings, live
outbox work or leases, current referenced turn content, active `reserved`
command-owner leases, or any `send_started` row directly. An expired `reserved`
row remains a reclaimable pre-send reservation for the same canonical mutation;
the retry horizon does not convert it to uncertainty. A `send_started` row older
than the retry horizon (604800 seconds by default) becomes `uncertain` instead
of being deleted or retried. The bounded deletion pool contains only expired
`reserved` rows and terminal `accepted`, `rejected`, or `uncertain` rows. A row
in that pool is eligible only when it is both older than the configured age
floor (2592000 seconds by default) and ranked beyond the newest-per-host count
floor (4096 by default).
With `--dry-run`, every maintenance transaction is rolled back; the aggregate
candidate/action counts are predictions and no store row or maintenance marker
is changed.

Final-graph cleanup preserves one immutable delivered-attempt tombstone for the
opaque final key before deleting its root, plans, jobs, canonical revisions, and
turn. A repeated authoritative snapshot consults that tombstone and cannot
recreate or repost the final. Tombstones are deduplicated per key and bounded
per host by the acknowledged-final retention count (4096 by default).

Automatic maintenance is deliberately coarse and bounded. After a stored
snapshot, the daemon consults one persisted database-wide cadence marker (3600
seconds by default). When due, it removes at most 100 snapshot rows, uses a
shared budget of 100 eligible acknowledged-final graphs across hosts, and runs
one separate command-receipt batch of at most 100 transitions/deletions.
Command-receipt maintenance converts stale `send_started` receipts to
`uncertain` before considering bounded inactive deletion. Private per-host
final service cursors
order never-serviced hosts first and then the least-recently serviced,
preventing a lexicographically early busy host from starving others. Later
batches resume backlog; no batch loops to empty, compacts pages, or invokes
`VACUUM`.

`store compact` is an explicit CLI-only database operation, not a daemon API.
Dry-run accepts optional `--snapshot-retention-days`,
`--snapshot-retention-count`, and `--batch-size` overrides, but rejects
`--acknowledge-offline` and `--backup-path`; it opens the current v12 store
read-only, runs `PRAGMA quick_check`, estimates eligible snapshots and disk
headroom, and is strictly non-mutating. Execute requires all writers stopped,
the literal `--acknowledge-offline` flag, and a new nonexistent private backup
path. Its result status is one of `completed`, `invalid_request`,
`store_unavailable`, `schema_not_current`, `permissions_failed`,
`offline_required`, `integrity_failed`, `insufficient_space`, `backup_failed`,
`maintenance_failed`, `checkpoint_failed`, `replacement_failed`,
`rollback_completed`, or `rollback_failed`; dry-run success is `dry_run`.
These statuses describe verified local outcomes, not exactly-once execution.
See `INSTALL.md` for the required maintenance and rollback order.

## Neutral connector outbox boundary

Tendwire exposes a Tendwire-only connector delivery boundary above the SQLite
store. The public daemon methods are `connector.prepare`, `connector.poll`,
`connector.ack`, `connector.fail`, `connector.defer`, `connector.renew`,
`connector.release`, `connector.inspect`, `connector.retry`, and the operational
helper `connector.reclaim`. Matching JSON-only CLI hooks include:

```bash
tendwire connector poll --name attention --limit 10 --lease-seconds 60 --db-path /path/to/tendwire.db
tendwire connector ack --name attention --ref '<opaque-ref>' --response-json '{"delivered":true}' --db-path /path/to/tendwire.db
tendwire connector fail --name attention --ref '<opaque-ref>' --reason temporary --delay-seconds 60 --db-path /path/to/tendwire.db
tendwire connector defer --name attention --ref '<opaque-ref>' --reason scheduled --available-at 2026-01-01T00:10:00+00:00 --db-path /path/to/tendwire.db
tendwire connector renew --name turn-final --ref '<opaque-ref>' --lease-seconds 120 --db-path /path/to/tendwire.db
tendwire connector release --name turn-final --ref '<opaque-ref>' --db-path /path/to/tendwire.db
tendwire connector inspect --name turn-final --status dead_letter --limit 100 --db-path /path/to/tendwire.db
tendwire connector retry --name turn-final --final-identity 'twfinal1.<opaque>' --db-path /path/to/tendwire.db
```

The boundary is neutral and separate from concrete connector integrations.
Public requests use `name`, `ref`, `limit`, `lease_seconds`, `available_at`,
`delay_seconds`, `reason`, optional sanitized `response`, and the strictly
scoped inspect/retry selectors shown above. Dead-letter inspection accepts only
`name=turn-final`, `status=dead_letter`, and a limit from 1 through 100. It
reserves room for a failed plan even when migration holds fill the limit.

An exhausted-root item contains bounded `status`, `created_at`, `updated_at`,
`attempt_count`, sanitized `final`, and an opaque `key` only when that key
validates. A failed-plan item contains `kind=failed_plan`, `status`, opaque
`plan_token`, `final_identity`, and `key`, plus public turn/revision, generation,
`failed_job_count`, and cumulative `attempt_count`. It remains discoverable
when its source link is absent. Retry accepts exactly one inspected
`twfinal1.` identity (or exact key through the API) and returns either a
bounded `requeued` result with `prior_attempt_count` or the existing bounded
`recovered` result. It is never bulk.

Missing/malformed-owner, internal-automation, and known-incomplete safety holds
are inspection-only and exact retry fails closed. Eligible exhausted roots and
unique failed plans can retry only after authoritative current-revision and
route validation.

`name` must otherwise be a neutral queue name: 1-64 ASCII letters, digits, `.`,
`_`, or `-`, and not a concrete provider, delivery, backend, or terminal-routing
token. Public responses never expose `private_state_json`, backend routing,
pane/session/terminal identifiers, socket paths, target values,
Telegram/chat/topic/message IDs, tokens, or connector-specific delivery
internals.

### Ordered range-only final plans

`connector.prepare` is the schema-v1 planning surface for the neutral
`turn-final` queue. It stores canonical coordinate ranges, never copied content
text or provider presentation data. Requests are exact objects with no
additional fields:

- `begin`:
  `schema_version`, `action="begin"`, `name="turn-final"`, `turn_id`,
  `content_revision`, `presentation_version`, and `part_count` from 1 through
  10,000.
- `part`:
  `schema_version`, `action="part"`, `name="turn-final"`, `plan_token`,
  zero-based `ordinal`, and one through 64 `spans`. Every span has exactly
  `field`, `start_char`, and `end_char`; `field` is `user_text` or
  `assistant_final_text`, and the range is nonempty and half-open.
- `commit`:
  `schema_version`, `action="commit"`, `name="turn-final"`, and `plan_token`.
- `recover`:
  `schema_version`, `action="recover"`, `name="turn-final"`,
  `failed_plan_token`, and `request_id`.

`begin`, `part`, and `commit` are idempotent for identical input. Commit accepts
only every declared ordinal with exact, contiguous, ordered coverage of the
selected complete canonical fields, then atomically materializes ordered
neutral `upsert` jobs followed by any required reverse-order `retire` jobs.
Consumers fetch text lazily through `turn.content.get` from each job's ranges;
the plan itself contains no stored page or provider-specific message copy.

A plan without a live source ref is not route-free. Begin and commit reconstruct
and validate an immutable authoritative schema-v2
turn/revision/final-identity/stable-key route from a delivered root or the
current complete revision, reject internal automation and conflicting roots,
and copy that route into every materialized job. Failed-plan recovery retains
the same lineage; a source-less row can be inspected and recovered without
inventing a worker or fingerprint route.

`recover` is an explicit, one-shot operator action, not an automatic retry
loop. It applies only to the latest failed generation for the still-current
immutable content revision, after at least one recorded delivery attempt. The
failed source plan, its outbox rows, and its delivery receipts remain unchanged.
Tendwire retains the contiguous explicitly ACKed prefix, creates a fresh plan
token and next generation for only the unfinished suffix, and preserves ordered
continuity through the predecessor job key. A delivered item after a gap, a
leased suffix, a superseded revision, a nonfailed plan, or a competing
generation fails closed.

The recovery `request_id` is a provider-neutral idempotency key of 1 through
128 ASCII letters, digits, `.`, `_`, or `-`. Repeating the same request ID with
the same failed plan returns the same recovery with
`idempotent_replay=true`; reusing it for another failed plan returns
`request_conflict`, and a second request for an already recovered generation
returns `plan_conflict`. A successful recovery response is deliberately bounded
to exactly `schema_version`, `ok`, `status` (`recovered`),
`failed_plan_token`, `plan_token`, `generation`, `content_revision`, `state`
(`active`), `acknowledged_prefix_count`, `executable_job_count`,
`retained_failed_job_count`, `prior_attempt_count`, and
`idempotent_replay`. Schema v7 records the request, source/recovered plan,
generation, retained prefix, fresh suffix, failed-job count, prior attempts,
and outcome in one immutable recovery audit row.

Only an explicit `connector.ack` proves a delivered prefix. A provider may have
accepted an operation even when its success receipt was lost; that state is
delivery-uncertain and no connector can promise perfect exactly-once external
effects. Recovery therefore reports and preserves Tendwire's durable evidence
without claiming that an unacknowledged provider operation definitely had no
effect.

`connector.poll` atomically leases due `connector_outbox` rows for one `name`
and returns opaque per-attempt refs. A live lease prevents duplicate polling.
Expired leases are reclaimed before polling and before ref-mutating operations;
the daemon first performs a read-only due check on its periodic tick and takes
the reclaim write transaction only when work is actually expired.
`connector.reclaim` can be called directly. `connector.renew` extends a live
lease without creating a delivery attempt. `connector.release` records the live
attempt as released and makes the row immediately pollable again. `connector.ack`
validates the host, name, attempt, lease, and ref before marking the delivery and
outbox item delivered. `connector.fail` records sanitized failure data and
schedules retry availability. `connector.defer` records sanitized defer data and
schedules future availability without treating the item as delivered. Stale,
expired, wrong-host, wrong-name, and superseded refs fail closed with neutral
errors.
When callers omit `lease_seconds`, the daemon and CLI use
`connector_claim_ttl_seconds` from config, defaulting to 60 seconds; an explicit
public `lease_seconds` value still wins. `turn-final` poll and renew requests are
capped by `connector_max_claim_ttl_seconds`, defaulting to 300 seconds.
`max_outbox_attempts` prevents
unbounded retry loops. Once a failed job reaches the configured cap, Tendwire
moves the outbox item to a neutral terminal `dead_letter` state and returns the
public status `attempts_exhausted` without exposing private outbox or delivery
state.

Final-root FIFO is partitioned by the turn's immutable stable worker key, with
the persisted worker ID as the enqueue-time fallback. Legacy backfill prefers
the enqueue-era stable key carried in the outbox payload; rows that predate that
metadata still fall back to the turn's current persisted worker ID. If neither
identity can be resolved, each row receives its own `orphan:<id>` partition so
unrelated degraded rows cannot block one another. A blocked worker therefore
cannot starve another worker, while roots and plan parts for the same worker stay
strictly ordered. `dead_letter`, `superseded`, and `delivered` roots are terminal
for this gate. A committed source in `awaiting_ack` no longer blocks by itself;
its plan jobs carry the ordering obligation, including source-less recovery
plans. Commit stamps an ACK deadline, each acknowledged plan job extends it,
and a valid part lease prevents deadline reclaim from interrupting in-flight
delivery. ACK-deadline recovery does not consume the connector failure-attempt
budget. An expired incomplete plan is otherwise failed and its source requeued.
Missing or unrecoverable plan state terminates the source instead of leaving it
pending.

### Delivery-aware final roots and acknowledged history

Persisting an authoritative, owner-authenticated complete final atomically
creates one neutral durable `final_ready` root before Herdres is available. Its
`schema_version=2` payload binds the exact public opaque `stable_key` and integer
`stable_key_version=1` at the root, copied from persisted turn metadata. A
range plan preserves that route under every job's `turn`; no worker fingerprint
is a fallback.

Missing/malformed ownership creates a nonpollable `schema_version=1`
`final_migration_hold`/`dead_letter`. Known-incomplete or internally classified
automation finals are also nonpollable safety holds. All are permanently
nonretryable for that canonical identity. Polling leases only a routable root so
Herdres can materialize its ordered plan, and the root becomes delivered only
after every part is durably ACKed. Omission, lease expiry, defer, failure,
restart, and exhaustion never make unresolved work retention-eligible.

`dead_letter` means unresolved, not discardable. Bounded inspect keeps roots and
failed plans visible without content. Exact retry revalidates the current
complete owner and route. A unique failed plan, including one with no source
link, uses deterministic recovery that retains the contiguous ACKed prefix and
materializes a fresh suffix. An eligible exhausted root preserves cumulative
attempts and receives a fresh budget. Safety holds, stale/incomplete/resolved
identities, and ambiguous plans fail closed. After acknowledged cleanup, a
bounded immutable delivered tombstone prevents the same final key from being
recreated or reposted. Operators use inspect/retry, never manual SQLite edits.

These source-only contracts and their hermetic Tendwire/Herdres checks do not
claim a live connector run, service restart, migration of live state, or
production deployment.

Connector payloads are generic Tendwire jobs such as `attention_created` and
`attention_escalated`; the existing attention lifecycle writer continues to
enqueue them in `connector_outbox`. This boundary does not add Telegram
delivery, Herdres integration, bot tokens, chat/topic/message IDs, backend
targets, pane/session/terminal routing, shell control, argv handling, UI
delivery, or private connector routing fields to public models or public JSON.

## Milestone 3 neutral command contract

Tendwire now exposes a minimal, safety-first command interface:

```bash
echo '{"schema_version": 1, "action": "noop"}' | tendwire command --json
```

The connector-facing structured Claude decision payload, semantic
`answer_decision` action, validation, and retry behavior are documented in
[docs/answer_decision.md](docs/answer_decision.md).

The `command --json` subcommand reads exactly one schema-v1 JSON request from
stdin. A proven result is exactly one schema-v2 command envelope on JSON-only
stdout with exit `0`/`1`; unresolved process ambiguity is no stdout envelope
and exit `2`. Human argparse errors may use stderr. `command.submit` is the
daemon method for the same contract; mutating Herdr sends require the socket
backend and a healthy authoritative snapshot.

### Send transport (current and planned)

`command.submit` (CLI: `tendwire command --json`) is the **only** public send
path. Callers such as Herdres select a worker by neutral identity
(`worker_id`/`worker_fingerprint`/`space_id`/`name`) and pass instruction text;
they never see or supply `pane_id`, `terminal_id`, `send_keys`, or any
`backend_target`. Those identifiers stay private to Tendwire and never appear in
public snapshot/turns/pending JSON.

Internally, Tendwire resolves the neutral target to a private worker binding and,
for delivery reliability on Herdr today, drives a **private Herdr pane transport**
(pane keystroke submission) behind the public contract. This is an
implementation detail: it is not exposed, not part of the neutral command shape,
and can change without affecting callers.

Planned follow-up (not in this RC): switch the internal transport to Herdr's
semantic `agent.send` API once it is stable, replacing private pane keystroke
submission. The public `command.submit` contract stays the same, so no caller
change is required.

### Command request shape v1

```json
{
  "schema_version": 1,
  "action": "noop",
  "request_id": "optional-id",
  "dry_run": true,
  "target": {
    "worker_id": "...",
    "worker_fingerprint": "...",
    "space_id": "...",
    "name": "..."
  },
  "instruction": {
    "text": "..."
  },
  "params": {}
}
```

- `schema_version` — required; must be the JSON integer `1` exactly. Missing
  values, strings, floats, booleans, null, arrays, objects, and other integers
  are rejected before store, projection, Herdr observation, or backend send work.
- `action` — one of `noop`, `read_snapshot`, `resolve_target`,
  `send_instruction`, or `answer_pending`.
- `request_id` — optional for non-mutating/dry-run work; required for every
  non-dry-run `send_instruction` and `answer_pending`. A required mutating ID
  must match `[A-Za-z0-9._-]{1,128}` exactly. It is opaque ASCII and is never
  trimmed, normalized, or case-folded; the exact supplied bytes round-trip in
  every command envelope that can be authoritatively returned.
- `dry_run` — defaults to `true`; only literal JSON booleans are accepted, and
  a request must explicitly set `dry_run: false` to ask for mutation.
- `target` — optional neutral target descriptor using only `worker_id`,
  `worker_fingerprint`, `space_id`, and `name`. `send_instruction` requires at
  least one non-empty explicit selector.
- `instruction` — optional; for `send_instruction` contains `text` only.
- `params` — optional opaque parameters.
- `response_schema_version` — optional response negotiation. Omit it (or use
  `2`) for the unchanged schema-v2 envelope. A client may explicitly use `3`
  to receive the accepted instruction submission handle described below.

Requests containing connector or low-level terminal fields are rejected before
any backend call. Rejected fields include `telegram`, `chat_id`, `topic_id`,
`message_id`, `thread_id`, `route`, `delivery`, `token`, `bot_token`,
`pane_id`, `terminal_id`, `backend_target`, `tty`, `pty`, `pid`, `tmux`,
`screen_session`, `window_id`, `tab_id`, `argv`, `command`, `shell`,
`target_kind`, `target_value`, `turn_target_kind`, `turn_target_value`, and
`private_fingerprint`.

### Command result/envelope shape v2

The command request remains schema v1. Its command result is a distinct, exact
schema-v2 envelope:

```json
{
  "schema_version": 2,
  "action": "noop",
  "request_id": "optional-id",
  "ok": true,
  "dry_run": true,
  "status": "noop",
  "disposition": "no_receipt",
  "result": {},
  "error": null,
  "warnings": []
}
```

The envelope contains exactly those fields. The local daemon's outer RPC frame
remains schema v1; for `command.submit`, its `result` is this exact schema-v2
command envelope. The CLI unwraps that result and prints the schema-v2 command
envelope itself.

For a non-dry-run `send_instruction`, a client can opt into schema v3 by
setting request `response_schema_version: 3`. Only an accepted response is
projected as v3; its existing `result.turn_id` remains unchanged and
`result.submission_id` is added as an opaque deterministic `twsub1.*` handle.
The default and all current Herdres requests remain byte-for-byte schema v2.
The durable receipt also remains schema v2, so response negotiation does not
change idempotency evidence.

Each instruction that reaches `send_started` is written atomically to the
`turn_submissions` ledger. Submission never creates a `turns` row; only a
source-derived observation can create a public turn. The ledger stores a
whitespace-normalized instruction digest, never another raw instruction copy,
and advances to `submitted` or `uncertain` with the terminal receipt transaction.
Unlinked rows expire after
`TENDWIRE_SUBMISSION_HARD_TTL_SECONDS` (default `86400`); candidate linkage uses
the symmetric observation window configured by
`TENDWIRE_SUBMISSION_LINK_WINDOW_SECONDS` (default `60`). Source-derived
observations settle this ledger only after the matching
window closes: a component links only when it contains exactly one submission
and one unlinked observation; larger components become `ambiguous`. Linkage is
metadata and does not change observation-derived turn identity, turn-list order,
Goal 10 delivery, or Herdres consumption. Lazy and periodic settlement cover
both submission-first and observation-first arrival order.

`TENDWIRE_TURN_MODEL` remains accepted for rollout compatibility. `legacy`,
`dual`, and `shadow` emit a warning and use the same observation-authoritative
behavior as `observed`.

`disposition`, not `status` alone, is the receipt-authority and finality
contract:

- `no_receipt` asserts no terminal receipt authority in this response. It is
  used for non-mutating work, dry runs, validation/pre-authority failures, and
  other results that were not finalized against a canonical durable receipt.
- `in_progress` projects durable `reserved` or `send_started`; it is not
  terminal, and replay of the same request ID does not start a second send.
- `terminal_accepted` projects durable `accepted` and requires
  `ok=true,status=accepted`.
- `terminal_rejected` projects durable `rejected`; it is terminal even when its
  explanatory status is also seen in a `no_receipt` envelope.
- `terminal_uncertain` projects durable `uncertain` and requires
  `ok=false,status=request_state_uncertain`; the effect may have occurred and
  is never silently retried.

Status values include `noop`, `snapshot`, `resolved`, `dry_run`, `accepted`,
`rejected`, `not_found`, `ambiguous_target`, `stale_target`,
`backend_unavailable`, `ambiguous_backend_target`, `backend_unsupported`,
`backend_failed`, `duplicate_request`, `request_state_uncertain`,
`invalid_request`, and `pending`. `pending` is the status paired with
`in_progress`; status text never proves terminality by itself.

In particular, `backend_unavailable` has two authority cases. With
`disposition=no_receipt`, Tendwire could not establish a receipt-authoritative
terminal result (including a daemon failure proven to occur before request
start), so the status alone is not final. With
`disposition=terminal_rejected`, Tendwire established the canonical request,
reserved its durable receipt, and persisted the pre-send rejection; that
combination is terminal and replayable. A disconnect, timeout, protocol
failure, or OS error after send start instead becomes
`request_state_uncertain/terminal_uncertain`.

A valid schema-v2 CLI envelope exits `0` when `ok=true` and `1` when
`ok=false`, including `in_progress`, terminal rejection, and terminal
uncertainty. Exit `2` is outside the envelope protocol: it means the CLI cannot
prove whether the mutating daemon process accepted the request, emits no stdout
JSON, and must not be interpreted by status or replaced with a synthetic
envelope.

Errors use a neutral shape: `code`, `message`, and sanitized `details`. Public
results must never include connector delivery state, Herdr routing objects, bot
tokens, chat/topic/message IDs, raw pane IDs, terminal IDs, PIDs, TTY paths,
backend argv, or route/delivery fields.

### Allowed actions

- `noop` — returns immediately with status `noop`.
- `read_snapshot` — returns the current neutral snapshot under `result.snapshot`.
- `resolve_target` — resolves a target against live workers and returns a single
  target (`resolved`) or a list of sanitized candidates (`not_found`,
  `ambiguous_target`, `stale_target`).
- `send_instruction` — validates the request shape and instruction. Dry runs
  return status `dry_run` without consulting or requiring a backend or store,
  creating a receipt, resolving mutable authority, or calling Herdr. Non-dry runs
  resolve a neutral public worker selector against the authoritative Tendwire
  snapshot, load Tendwire-owned private bindings from the local store, and send
  through Herdr's socket `agent.send` method. The private target value is chosen
  by Tendwire from `WorkerBinding` internals and is never accepted from or
  returned to public clients.
- `answer_pending` — validates the exact public pending ID, fingerprint, and
  choice ID. Dry runs return the same pure `dry_run` preview described above.
  Non-dry runs resolve the authoritative public worker and private
  revision-bound route, then submit the private picker ordinal without exposing
  it.

### Safety rules

- Dry-run by default. A request must explicitly set `dry_run: false` to request
  mutation. A validated mutation dry-run is pure and backend/store independent;
  it never creates a receipt or consults mutable target authority.
- Every non-dry-run `send_instruction` or `answer_pending` requires a
  `request_id`, an available command receipt store, and exact single public
  worker resolution. `send_instruction` also requires at least one explicit
  target selector.
- Targets with status `closed`, `failed`, or `unknown` are rejected for
  `send_instruction`.
- The send adapter never treats public selectors as raw Herdr send targets.
  Clients may use `worker_id` only as a neutral Tendwire selector, never as a
  backend target value. Clients must never provide `terminal_id`, `pane_id`,
  `backend_target`, `target_value`, argv, shell, or backend parameters as raw
  Herdr send targets; low-level backend fields are rejected before mutation.
- Backend-owned Herdr targets are private. Tendwire computes the final socket
  `agent.send` target parameters from private `WorkerBinding` rows for every
  observed worker; if two public workers would send to the same private target,
  all affected targets are non-sendable and mutation fails closed with
  `ambiguous_backend_target`. Missing, empty, unsupported, or otherwise
  non-sendable private targets fail closed with `backend_unsupported`.
- For non-dry-run `send_instruction`, `TENDWIRE_HERDR_BACKEND` must be `socket`
  and the current Herdr backend health must be `healthy`. A disabled socket
  backend, missing Herdr socket, launch/connect failure, unhealthy backend
  health, or unavailable receipt store returns `backend_unavailable` and does
  not execute send. Only a healthy empty socket observation may produce a
  final `not_found`.
- Once Tendwire has reserved a receipt and started Herdr socket `agent.send`,
  disconnects, timeouts, protocol failures, and OS errors return
  `request_state_uncertain`. Tendwire records that uncertainty and will not
  silently retry the same `request_id`.
- Private Herdr binding expiration is guarded by authoritative healthy
  observation. Healthy empty observations may expire stale bindings; degraded or
  unavailable empty observations do not prune bindings as if Herdr were
  authoritatively empty.
- Instruction text is validated: it must be non-empty, no longer than 4096
  characters, and may include LF newlines and tab characters. It must not
  contain NUL, ESC/ANSI/CSI/OSC sequences, bracketed-paste sequences, carriage
  returns, DEL, C1 controls, or other raw control characters.
- Unknown actions are rejected before any backend or store call.

### Idempotency receipts

Every non-dry-run `send_instruction` and `answer_pending` reserves a durable
SQLite receipt before backend mutation. The sole authority key is the
host-wide `(host_id, request_id)` pair; `action` is not part of that unique key.
The receipt table enforces one durable row per key with a unique index.
After authoritative target resolution, Tendwire builds canonical mutation v1
from the action, resolved public `worker_id`, and exact instruction text or
pending-ID/fingerprint/choice triple. The request ID, unresolved selector
spelling, worker observation fingerprint, connector origin, and private binding
are not canonical identity inputs. Different selector forms that resolve to
the same public worker therefore name the same mutation; a selector that names
a different public worker does not.

The durable lifecycle is `reserved`, `send_started`, `accepted`, `rejected`,
or `uncertain`:

- `reserved` means one owner has claimed the request before send start. A
  concurrent replay reports it in progress; only an expired pre-send owner
  lease may be reacquired for the same canonical mutation.
- `send_started` is recorded before the external mutation. It is never
  automatically retried; replay returns `request_state_uncertain`.
- `accepted` and `rejected` are terminal and replay their stored sanitized
  envelope for the same canonical mutation.
- `uncertain` is terminal evidence that the mutation may have occurred. Replay
  returns `request_state_uncertain` and never starts another send.
- Reusing the same host/request ID for a different action, resolved public
  worker, instruction, or pending choice rejects with `duplicate_request`
  before mutation.
- A different `request_id` is an independent mutation and, when otherwise
  valid, sends even when its instruction text matches earlier work. Tendwire
  performs no content-based command suppression or time-window heuristic.

The retry horizon defaults to 604800 seconds and may not exceed 604800. Bounded
maintenance converts only a `send_started` receipt older than that horizon to
`uncertain`; it does not retry the effect. A `reserved` receipt is pre-send
ownership: its active lease remains protected, and an expired lease may be
reacquired for the same canonical mutation rather than converted to
`uncertain`. The bounded deletion pool contains only expired `reserved` rows
and terminal `accepted`, `rejected`, or `uncertain` rows. Within that pool, a
row is deletable only when it is both older than the configured retention age
(2592000 seconds by default) and ranked beyond the newest 4096 rows for its host
by default. Retention seconds have a hard minimum of 691200 and must be strictly
greater than the retry horizon. The unchanged 2592000-second default therefore
remains greater than Herdres's entire configurable connector retry horizon (at
most 604800 seconds), so the connector cannot legitimately outlive Tendwire's
default receipt evidence. Active `reserved` owner leases are outside the
deletion pool, and `send_started` rows are transitioned rather than deleted
directly. Dry-runs never create receipts.

### Unsafe non-goals

The only active Herdr mutation surface is the Tendwire-owned socket
`agent.send` call for literal instruction text. Tendwire does not use
`pane send-text`, `send-keys`, `pane run`, shell/PTY control, signals, paste
buffers, raw argv, client-provided worker IDs as backend targets,
client-provided backend params, fallback terminal-control paths, or connector
delivery as a command transport.

## Non-goals

Tendwire intentionally does **not** include:

- Herdres, Telegram, Hermes, MCP, iOS, AR, UI, or local LLM integrations.
- Modifications to Herdres or local Herdres files.
- Connector-specific routing or delivery state inside core models or snapshots;
  the neutral connector outbox boundary remains separate.
- Telegram delivery, topic mapping, reply rendering, rate limiting, retry
  policy, or connector-specific presentation logic.
- Raw Herdr pane, terminal, socket-path, target-value, shell, PTY, argv, env,
  stdout, or stderr exposure in public JSON.
- Network sync or cross-device transport.
- Runtime dependencies beyond the Python standard library.
