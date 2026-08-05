# Release checklist (Tendwire 0.1.0rc5 / Herdres 0.7.0rc4)

The supported RC runtime is Python 3.13. Tendwire `0.1.0rc5` must be paired
with Herdres `0.7.0rc4` or a reviewed descendant preserving its source
contract. The package version is defined once in `src/tendwire/_version.py`;
Hatch reads that value, and `scripts/release_artifacts.py` validates the
resulting metadata.

Stable Herdr continuity accepts both public workspace formats emitted by the
supported Herdr runtime: legacy Crockford-style IDs and current 14-character
lowercase hexadecimal workspace suffixes. Pane IDs must still contain the exact
workspace ID and a valid public pane number; private terminal/session values are
never continuity inputs.

Automatic CI compiles, runs the full hermetic suite and offline Herdr fixture,
builds and scans both artifacts, and smokes clean wheel and sdist installs. It
does not perform deployment, use secrets, or contact live services.
The job checks out paired Herdres source once because the existing installed-
candidate benchmark executes its real source adapter; the benchmark path comes
from `TENDWIRE_BENCHMARK_HERDRES_ROOT`, never a user-home default.

Build release artifacts from a **clean git checkout only**. Never zip the working
directory directly — it can contain `__pycache__/`, `*.pyc`, `.pytest_cache/`,
local `*.db` state, `installation.key`, `installation.key.sha256`, or
`installation.key.initialized`, none of which may ship. `.gitignore` excludes
these local-state filenames, so building from tracked content is what
guarantees a clean artifact.

## 1. Preconditions

```sh
git status --porcelain            # must be empty
python scripts/release_artifacts.py source
python -m compileall -q src tests scripts
python -m pytest -q               # all green
```

## 2. Build a clean artifact

Source zip/tar (tracked files only, respects `.gitignore`):

```sh
git archive --format=zip -o dist/tendwire-$(git describe --always).zip HEAD
```

Or a Python sdist/wheel (hatchling; packages `src/tendwire` + declared includes):

```sh
python -m build
```

## 3. Verify the artifact is clean

The checked-in validator must return `status: ok` and write `dist/manifest.json`:

```sh
python scripts/release_artifacts.py artifacts dist
python scripts/release_artifacts.py install-smoke dist/*.whl
python scripts/release_artifacts.py install-smoke dist/*.tar.gz
```

## 4. Coherent backup and continuity verification

Before an ordinary Tendwire/Herdres upgrade:

1. Stop Herdres, Tendwire, and every other identity consumer. Capture one
   access-restricted recovery set containing the active Tendwire database,
   `data_dir/installation.key`, `data_dir/installation.key.sha256`,
   `data_dir/installation.key.initialized`, and complete Herdres persistent
   state. The three identity artifacts and all dependent state must come from
   the same stopped-service checkpoint.
2. Confirm the Tendwire data directory is mode `0700`, all three identity files
   are mode `0600`, and the files are owned by the Tendwire service account.
   Confirm that `installation.key.initialized` is the exact nonsecret one-byte
   value `1` and that the release artifact contains none of the three
   filenames.
3. Retain all three identity artifacts through the upgrade. Start Tendwire
   before Herdres and confirm a known same-workspace worker has the same
   exact-format `meta.stable_key` and integer `stable_key_version: 1` as before.
   Then start Herdres and confirm its existing binding/topic remains singular.
4. Verify a same-workspace tab move preserves the handle and a controlled
   cross-workspace move changes it. Also verify a fixture restore preserves the
   handle while terminal/session identifiers change; those volatile identifiers
   are not continuity inputs.

Ordinary load validates and reuses initialized state and never rotates it. With
`installation.key.initialized` present, loss of the key, digest, or both fails
closed; stop every identity consumer and restore the complete coherent recovery
set rather than repairing individual artifacts. The sentinel is created only
after Tendwire has validated and published the key and digest.

Deliberate offline rotation is not release continuity verification. With
Tendwire and every identity consumer stopped, invoke
`tendwire.worker_identity.reset_installation_key(Path(data_dir),
acknowledge_continuity_break=True)` through a controlled operator Python
environment; never delete identity files manually. The next eligible load
bootstraps a new three-artifact identity and changes every `wsk1_` handle.
Herdres state, bindings, and topics require explicit migration and review;
stale bindings are quarantined and old topics are not silently rebound or
automatically reused.

## 5. Goal 07/09 ingestion and pending verification

The release contract uses store schema v14. Its transactional v8-to-v9
migration backfills immutable positive `list_sequence` values independently per
host and creates the uniqueness/paging state used by stable `turn.list`
traversal. The v9-to-v10 migration preserves public pending rows while adding
explicit freshness, binding-scoped revision routing, and durable two-phase
answer claims; migrated rows remain unanswerable until refreshed from an
authoritative binding. The conservative v10-to-v11 migration adds typed final
root columns/indexes and the private per-host fair-maintenance cursor table. A
routable root payload has `schema_version=2` and the exact root-level public
`stable_key` plus integer `stable_key_version=1` captured from persisted turn
metadata; every plan job must preserve the exact
turn/revision/final-identity/stable-key route. No worker-fingerprint fallback is
permitted.

The transactional v11-to-v12 migration replaces action-scoped command rows with
one host-wide `(host_id, request_id)` authority and explicit
`reserved`/`send_started`/`accepted`/`rejected`/`uncertain` state. Canonical
mutation v1 uses the action, resolved public worker identity, and exact
instruction or pending choice, not request ID, raw selector spelling, worker
observation fingerprint, connector origin, or private binding. Ambiguous
legacy collisions become terminal uncertainty; a migration failure rolls back
the v12 rebuild and leaves `PRAGMA user_version=11`.

Delivery requires exact canonical range coverage and host-bound all-part proof.
Linkable unresolved work also requires the exact route on every job. Unknown or
mismatched history, missing/malformed ownership, known-incomplete content, and
internal automation become nonpollable migration holds, never a mass repost;
missing-owner and automation safety holds are permanently nonretryable. A
partial legacy final-table set, invalid recovery edge, descriptor/route failure,
or later error rolls back every v11 table/column/root/cursor change and leaves
`PRAGMA user_version=10`. Validate only an access-restricted scratch copy, never
the sole recovery copy.

The daemon owns short-cadence and event-signaled refresh, with coalescing,
per-target serialization, a fixed worker/queue bound, adapter deadlines, and
aggregate degraded/stale health. Public read handlers remain cache-only.

OMP cache/IPC state must remain a coordinate-only checkpoint and must never
carry prompt, user, final, or stream bodies. An unchanged stable stat must
return unchanged without spawn/read/transport; changed open turns reconstruct
from a replay coordinate; completed finals compact to idle EOF until a new
eligible user message opens a turn. The private cache remains bounded by 64
entries and 64 KiB serialized key-plus-checkpoint weight, with disappeared
bindings pruned and same-path private-fingerprint changes advancing its
generation.

The OMP spawned-reader request remains capped at 16 KiB. A canonical OMP
response has no total-size ceiling: its exact ordered payload is streamed in
frames of at most 1 MiB under the same deadline, with manifest, nonce,
end-marker, and EOF validation, so canonical finals are not truncated by IPC.
Nonblocking framed socket send/receive, parsing, and join share that adapter
deadline without a helper IPC thread.
Timeout teardown spends at most 250 ms on terminate/kill/join attempts and
reaps the child under normal POSIX scheduling; it does not wait beyond that
grace for an OS-uninterruptible child. A
content-bearing file-reader candidate publishes only after exact binding
revalidation and successful durable apply; a no-content candidate still
requires binding validation. Cancellation, failure, stale binding, or a changed
same-path generation cannot advance the cache. Ingestion health must recover
after a later success while preserving cumulative failed/timeout counters;
`stale_binding` churn must not by itself keep health degraded.

Daemon and CLI pending surfaces must share the atomic latest-snapshot plus
durable-`backend_pending` store projection. A malformed snapshot fails as
`store_unavailable`. Definite pre-transmission daemon unavailability permits
only a store read and no Herdr observation. A post-transmission timeout returns
`daemon_timeout`; protocol ambiguity returns `daemon_protocol_error`, and
neither path falls back.

`tendwire turns` direct refresh fallback is limited to one initial-page attempt
when daemon unavailability is definite before request transmission; a timeout
after transmission returns `daemon_timeout` without a second refresh.
Pagination remains bounded to limit 100 by default and 250 maximum, uses stable
watermark-bound cursor/since traversal with a fixed 900-second cursor TTL, and
retains the 1 MiB transport-frame and 48 KiB content-page limits. The API keeps
eight request workers and 32 admitted connections, returning retryable
`server_busy` beyond that bound. Release-visible ingestion metrics must remain
aggregate and must not expose a worker, target, session path, or private
fingerprint.

From the source checkout, run the focused Goal 07 suite:

```sh
PYTHONPATH=src python3 -m pytest -q \
  tests/test_turn_ingestion.py tests/test_herdr_events.py tests/test_daemon.py \
  tests/test_cli.py tests/test_cli_command.py tests/test_store.py \
  tests/test_herdr_turns.py tests/test_turns.py tests/test_config.py \
  tests/test_public_content_safety.py
```

Then run the complete suite:

```sh
PYTHONPATH=src python3 -m pytest -q
```

Run the frozen benchmark command exactly:

```sh
PYTHONPATH=src python3 scripts/turn_ingestion_benchmark.py \
  --workers 8 --blocked-workers 2 --blocked-seconds 5 \
  --warmups 3 --samples 21 --json
```

The benchmark must use generated private fixtures and real Unix-socket requests
with two deterministically blocked adapters, never live state. Record only its
aggregate public-safe output in
`docs/evidence/goal07-turn-ingestion-benchmark.md`. The recorded-host evidence
budgets are cached `turn.list` and `health.get` p95 no greater than 350 ms and
immediate synthetic `command.submit` p95 no greater than 250 ms. These are
recorded-host release evidence rather than ordinary-CI or unit-test timing
gates, and not a generic SLA, scaling guarantee, or statistical service-level
claim. Do not state observed timing or deployment success until the completed
evidence supports it.

## 6. Goal 08 Codex session-reader verification

A Codex binding must be an exact canonical lowercase, non-nil UUID. The only
eligible rollout grammar is
`sessions/YYYY/MM/DD/rollout-YYYY-MM-DDTHH-MM-SS-<uuid>.jsonl`, with a valid
date/time matching the hierarchy and no identifier interpolation into a glob.
Resolution must yield exactly one canonical regular file beneath the canonical
sessions root after symlink and device/inode checks. Missing, unsafe,
over-limit, or multiple exact matches are unavailable.
Every cache hit validates the current sessions-root device/inode; a found result
also validates the rollout inode. A root identity change immediately clears
that root's cached path results and complete index.

The complete index is bounded to 100,000 filesystem visits, 100,000 session
identities, and 16 MiB retained; its path-result LRU is bounded to 256 entries
and 256 KiB. Negative results expire after 2 seconds. A lookup rebuilds the
complete index once its snapshot is 60 seconds old. That interval is the
documented bounded-work tradeoff: a duplicate created after a successful lookup
may remain undiscovered for up to one 60-second snapshot interval, after which
the refreshed index makes the identity ambiguous and unavailable.

The private parser LRU is bounded to 64 entries and 16 MiB. Only valid
newline-terminated records advance the committed offset; a partial final record
waits for its newline. The limits are 8 MiB per record, a 64 KiB initial and
16 MiB maximum/65,536-record recovery scan, and 64 MiB of source data per poll.
Warm append-only reads consume only appended bytes and an unchanged poll reads
zero source bytes. Truncation, rotation, replacement, or missing state uses the
same bounded recovery rather than a whole-file fallback; malformed/oversized
input or no recoverable boundary fails without advancing the prior checkpoint.
When a delayed append batch contains a completed turn followed by a new turn,
the completed turn must publish first and the checkpoint must stop at that
final record. The following refresh consumes the later turn.

Codex parser-state requests are capped at 12 MiB, and each Codex response
remains one frame capped at 64 MiB. A response is only a candidate: content
publishes only after exact binding revalidation and durable apply, and
no-content state still requires binding revalidation. Cancellation, failure,
or a stale/replaced binding cannot advance the cache.
Public JSON must contain no session ID, rollout path,
device/inode, offset, parser/cache state, raw record, or private fingerprint.

Run the focused Goal 08 suite from the source checkout:

```sh
PYTHONPATH=src python3 -m pytest -q \
  tests/test_codex_session_reader.py tests/test_herdr_turns.py \
  tests/test_turn_ingestion.py tests/test_public_content_safety.py
```

Then run the complete suite:

```sh
PYTHONPATH=src python3 -m pytest -q
```

Run the exact synthetic benchmark command:

```sh
PYTHONPATH=src python3 scripts/codex_session_reader_benchmark.py --json
```

The benchmark must use only generated private fixtures, never live Codex state.
Its portable bounded-work gates are: invalid wildcard rejection before any
index build; one complete 20,000-file index build with no more than 100,000
filesystem visits and 8 MiB retained; no extra warm-lookup index build; no more
than 64 KiB read for the benchmark cold resynchronization; exactly append-sized
incremental work; and zero source bytes for an unchanged poll. Its recursive
privacy gate must reject generated paths, session/turn identities, content,
filenames, and UUID-shaped strings from the success report.

Record the exact command and compact aggregate result in
`docs/evidence/goal08-codex-session-reader-benchmark.md`. Documented-host timing
ceilings are release evidence, not ordinary-CI gates, a generic SLA, or a
scaling guarantee.

## 7. Goal 08B SQLite sidecar race/recovery verification

A file-backed store release must treat the main database, `-wal`, `-shm`, and
`-journal` as one identity-validated family. Absent optional sidecars and
optional sidecars that transiently disappear are valid. Once selected, the main
database is mandatory. A wrong type, wrong owner, insecure validation-time
mode, hostile appearance, or identity replacement must fail closed without
following or mutating the entry.

Creation and repair authority must remain explicit and narrow. Prepare may
create only a missing main database and may intersect modes of validated
present family members with `0600`; repair only intersects modes of validated
existing members. Neither creates an optional sidecar, widens a stricter mode,
replaces an entry, or changes ownership. Ordinary reads, diagnostics, health,
and `store status` are validation-only and non-creating. The capture/preflight/
final-validation sequence is bounded and does not recursively retry churn.
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

Private failures remain typed, path-free `LocalStateError` values; public
surfaces emit fixed aggregate records such as `database_permissions: unsafe`
and `store_unavailable`, without private paths, suffixes, ownership, inode, raw
exception, or content.

Automatic maintenance retains only bounded batch/cadence authority and never
compacts. Dry-run compaction remains validation-only. Execute compaction alone
has explicit offline replacement authority after `--acknowledge-offline`, and
must revalidate the selected source identity before publishing a verified
replacement.

Run these Goal 08B release commands from the isolated candidate source checkout,
never against live configuration, a live database, a live daemon socket, or a
running Tendwire/Herdres service. These are verification commands, not
deployment, migration, or restart instructions.

Focused:

```sh
PYTHONPATH=src python3 -m pytest -q \
  tests/test_local_state_permissions.py tests/test_store.py \
  tests/test_diagnostics.py tests/test_cli.py tests/test_daemon.py \
  tests/test_release_readiness.py \
  tests/test_sqlite_sidecar_race_benchmark.py
```

Full:

```sh
PYTHONPATH=src python3 -m pytest -q
```

Compile:

```sh
PYTHONPATH=src python3 -m py_compile $(git ls-files '*.py')
```

Diff hygiene:

```sh
git diff --check
```

Synthetic installed-candidate evidence:

```sh
TENDWIRE_BENCHMARK_HERDRES_ROOT=/absolute/path/to/accepted/herdres \
  python3 scripts/sqlite_sidecar_race_benchmark.py \
  --requests-per-method 64 \
  --herdres-presenter-passes 2 \
  --phase-timeout-seconds 120 \
  --json
```

The driver is `scripts/sqlite_sidecar_race_benchmark.py`, its focused harness
test is `tests/test_sqlite_sidecar_race_benchmark.py`. A current compact
aggregate records the private temporary directory, installed-candidate
import/origin checks, production daemon callback binding, two production
Herdres presenter passes over the real daemon Unix socket, exact file/socket
identity, and bounded file-descriptor/thread/child accounting. Both presenter
passes must be no-ops, with zero provider writes, zero direct Herdres-to-Herdr
calls, and zero outbound network attempts.

The paired Herdres checkout must be clean. The aggregate binds its exact commit,
Git tree, tracked-source SHA-256, and tracked-file count without publishing its
absolute path. A dirty, changing, or unbound checkout fails closed.

The driver removes `PYTHONPATH`, deterministically builds a versioned wheel from
this isolated source checkout, installs it with `pip --no-index --no-deps` into
a private temporary virtual environment, and re-executes the candidate with
isolated Python. It verifies that the imported package originates in that
private installation and not a mutable source checkout. Record the current
revision and generated source-tree/wheel hashes; no historical aggregate may
substitute for the current paired run. Timings remain observations, not
portable CI gates or service-level claims.

## 8. Goal 10 delivery-aware final retention gate

Run the focused Tendwire gate from its isolated source checkout:

```sh
PYTHONPATH=src python3 -m pytest -q \
  tests/test_delivery_retention.py \
  tests/test_delivery_retention_projection.py \
  tests/test_delivery_retention_migration.py \
  tests/test_delivery_retention_recovery.py \
  tests/test_delivery_retention_hardening.py \
  tests/test_connector_daemon_cli.py \
  tests/test_config.py::test_acknowledged_final_retention_has_conservative_documented_defaults \
  tests/test_daemon.py::test_daemon_health_degrades_on_public_safe_final_storage_pressure
```

Then, from the paired Herdres checkout, run only its hermetic connector tests:

```sh
PYTHONPATH=. python3 -m pytest -q \
  tests/test_turn_final_delivery.py::test_twenty_same_worker_ready_anchors_drain_in_order_and_forced_syncs_noop \
  tests/test_offlock_delivery.py::test_stable_job_key_resume_ignores_new_lease_ref_and_preserves_success
```

These commands use temporary SQLite stores and fake connector/Telegram
transports; they require no live Telegram credentials, Herdr process, Tendwire
daemon, or Herdres service. The gate is complete only when the named evidence
remains observable:

The policy defaults checked by this gate are distinct: acknowledged final
history is 30 days and 4096 proven graphs per host, while changed snapshot
history is 14 days and 4096 rows per host. Accepted ranges are positive, with
365000 maximum days, 9223372036854775807 maximum count, 1000 maximum batch size,
and 31536000000 maximum cadence seconds. Automatic maintenance uses a
100-row/graph budget on a 3600-second cadence, services never/least-recently
visited hosts first, and never runs `VACUUM`.

- The outage/restart case preserves every distinct complete final while the
  consumer is absent and reports no work after restart or repeated observation.
  Cleanup's bounded immutable delivered tombstone must continue to suppress the
  same final key after its graph is removed.
- Paired Herdres cases apply ordered jobs, make repeated forced syncs no-ops,
  and resume checkpointed work under a fresh transient lease ref.
- Migration cases prove owner-aware schema-v2 routes, exact coverage and
  all-part proof, nonpollable schema-v1 missing-owner holds, automation holds,
  route-mismatch quarantine, fair-cursor table creation, full rollback at v10,
  idempotence, and no historical repost.
- Projection cases prove that only the winning
  `(updated_at, content_fingerprint)` snapshot atomically refreshes projections
  and same-scope bindings; stale or losing equal-time data changes neither.
- Cleanup cases prove every unresolved root, plan, attempt, or unlinked part
  blocks graph deletion, while one bounded delivered tombstone prevents
  recreation after eligible cleanup.
- Store-status and daemon cases prove validated host-scoped aggregate
  pressure/health without content or identifiers and reject malformed,
  wrong-host, or out-of-range policy data.
- Inspect/retry cases prove fair bounded fields, permanently nonretryable
  missing-owner/automation holds, exact eligible root retry, and source-less
  failed-plan recovery with immutable route lineage, cumulative attempts, and
  retained ACK prefix.
- These contracts preserve Tendwire evidence but do not claim provider-perfect
  exactly-once effects.

Record the two command results; do not substitute a live connector smoke and do
not claim production deployment from this hermetic gate.

## 9. Goal 11 host-wide command idempotency contract

The Goal 11 release evidence is local and public-safe:

- The public command request is schema v1; the authoritative command envelope
  is exact schema v2 and round-trips `request_id`. One receipt owns each
  `(host_id, request_id)` across mutating actions. Required mutating request IDs
  match `[A-Za-z0-9._-]{1,128}` exactly and are never trimmed, normalized, or
  case-folded. The canonical mutation is built after authoritative selector
  resolution to the public worker identity.
- The exact disposition projection is `no_receipt` for no terminal receipt
  authority, `in_progress` for `reserved`/`send_started`,
  `terminal_accepted` for `accepted`, `terminal_rejected` for `rejected`, and
  `terminal_uncertain` for `uncertain`. Status alone is never finality.
  Accordingly, `backend_unavailable/no_receipt` is not a terminal receipt,
  while `backend_unavailable/terminal_rejected` is a persisted pre-send terminal
  rejection.
- A same-ID canonical replay returns in-progress, cached terminal, or uncertain
  disposition without a second send. Changed action, resolved worker,
  instruction, or pending choice rejects as `duplicate_request`.
- A different request ID is always a distinct mutation and, when otherwise
  valid, sends even if instruction content matches an earlier request. There is
  no content-based or time-window command suppression.
- A validated mutation dry-run is pure and backend/store independent: it
  creates no receipt, resolves no mutable target authority, and requests no
  external effect.
- The durable lifecycle is exactly `reserved`, `send_started`, `accepted`,
  `rejected`, and `uncertain`. An active `reserved` lease protects pre-send
  ownership; an expired lease remains reclaimable for the same canonical
  mutation. Once send has started, ambiguous completion is never automatically
  retried or acknowledged as success.
- `TENDWIRE_COMMAND_RETRY_HORIZON_SECONDS=604800`,
  `TENDWIRE_COMMAND_RECEIPT_RETENTION_SECONDS=2592000`, and
  `TENDWIRE_COMMAND_RECEIPT_RETENTION_COUNT=4096` are the conservative defaults.
  The retry horizon is positive and at most 604800 seconds; receipt age is at
  least 691200 seconds and strictly greater than the horizon.
- Bounded maintenance changes only `send_started` rows older than the retry
  horizon to `uncertain`; it does not age `reserved` rows into uncertainty. Its
  deletion pool contains only expired `reserved` rows and terminal `accepted`,
  `rejected`, or `uncertain` rows. A pool row is deleted only after it is both
  older than the age floor and beyond the per-host newest-count floor. Active
  `reserved` leases remain, and `send_started` is transitioned rather than
  deleted directly. Store status and daemon health expose
  only aggregate state/policy/candidate counts and pressure, never request IDs,
  canonical requests, instructions, pending choices, workers, or private
  bindings.
- An exact CLI envelope exits `0` for `ok=true` or `1` for `ok=false`. If a
  mutating daemon request may have started but no exact envelope or durable
  replay can be proven, exit `2` carries no stdout envelope and no forged
  status/disposition. The unchanged 2592000-second default receipt retention is
  greater than Herdres's maximum 604800-second connector retry horizon.

These checks use temporary local SQLite state and fake backend transport. They
do not require or authorize a live Herdr send, connector, network credential,
Tendwire deployment, Herdres service, or production maintenance operation.

## 10. Local hygiene (optional, before building from a dirty tree)

```sh
find . -type d -name __pycache__ -not -path './.git/*' -exec rm -rf {} +
rm -rf .pytest_cache
find . -name '*.pyc' -not -path './.git/*' -delete
```

## Notes

- `HANDOFF.md`, `*.db`, `installation.key`, `installation.key.sha256`, and
  `installation.key.initialized` are git-ignored and never appear in
  `git archive`.
- The public contract shipped is `command.submit` (`tendwire command --json`);
  see the README "Send transport" section. No `pane_id`/`send_keys` is exposed.
- `tests/test_release_readiness.py` guards the public JSON contract (zero
  forbidden keys, no pseudo pane ids).

## 11. Paired RC proof and deployment

Before tagging or deployment, run the paired proof locally from the accepted
clean checkouts:

```sh
# Tendwire
python3 scripts/release_artifacts.py source
python3 -m compileall -q src tests scripts
python3 -m pytest -q
python3 -m build
python3 scripts/release_artifacts.py artifacts dist

# Herdres
python3 -m compileall -q herdres.py herdres_gateway.py herdres_connector tests
python3 -m pytest -q
```

Run the owner-authorized ACP-primary gate in
`docs/acp-primary-release-proof.md`. It uses production Tendwire and Herdres
processes, the production Herdr socket's bounded ACP ownership seam
(`agent.acp_status`, `agent.acp_endpoint`, and
`agent.acp_console_exchange`), the required ACP supervisor, and live Telegram.
Direct Herdr CLI observation, injected daemon or connector callbacks, and
mock-provider substitution for the live happy path are forbidden.

The gate must produce these revision-bound, privacy-scanned files under
`docs/evidence/`:

- `wave16-cross-repo-e2e.json`
- `wave16-live-telegram-e2e.json`
- `wave16-acp-adapter-matrix.json`
- `wave16-release-summary.md`

Missing, stale, skipped, or generic evidence blocks both the RC and deployment.

The paired hermetic contract suite must bind both commits and prove
`direct_herdr_calls=0`, exact turn and pending schemas, command receipt
validation, and production-daemon-socket connector outbox and provider-binding
semantics. The live gate must finish with two independent no-operation
connector-outbox polls and presenter passes that make no Telegram write.

Deployment is a separate owner-authorized step. Before changing installed
artifacts, stop writers and capture a coherent private backup of the Tendwire
database family, installation identity, and Herdres state. Install the exact
wheel and paired Herdres files, reload user units, start Tendwire before
Herdres/gateway, and never restart Herdr. If migration, integrity, paired
contract, or command proof fails, stop the writers, restore the complete backup
and prior artifacts, reload units, and re-run read-only health checks.

The final live record must include: exact commits and artifact digests; SQLite
integrity; private filesystem modes; Herdr status without restart; Tendwire and
Herdres service status; two no-operation production connector-outbox polls and
presenter passes; `direct_herdr_calls=0`; no `Closed by User` spam; legacy timer
inactive and disabled; one inbound Telegram-to-Tendwire command with exact
durable receipt finality and duplicate guard; preserved provider bindings; one
lossless multipart final; and a zero-finding public JSON scan. Missing evidence
means the RC is not complete.
