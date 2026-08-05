# Wave 17 completion audit

Status: evidence only. This audit authorizes no production edit, capability deletion,
schema change, Herdr behavior change, merge, deployment, or service restart.

Audited checkpoints:

- Tendwire `f653026ce12d7122fe7427e84ab2f17858202bb1`; latest production ancestor
  `5158553ed8f2a49a57ffffe5a2f30d4d195f19e4`.
- Herdres `d581368f14f063a146bc700b00f47065a45e8939`.
- visible-console Herdr fork `9026d9bc5a12d9adc2d9f68ebdc564133e4098b4`.
- governing documents in `/home/smith/acp-reduction-goal/` as read on 2026-08-06.

The audit distinguishes current source, tests, active operator documentation, historical
evidence, and missing live proof. A green unit suite or absence grep is not promoted into
end-to-end evidence.

## Completion verdict

The reduction goal is **not complete**.

| Completion contract | Status | Current evidence |
|---|---|---|
| Global and component SLOC | **FAIL** | Tendwire is 20,985, exceeding 14,000 by 6,985. Herdres is 8,115 and passes 11,000 globally, but its component map fails known hard ceilings and has an unallocated decision row. |
| Mandatory deletions | **PARTIAL** | Herdres production deletions are proven. Tendwire has active-document residue, two no-source-callsite Store helpers, compatibility-test/tooling residue, and ambiguous event replay-fingerprint wording. |
| Preserved behavior and green suites | **PARTIAL** | Current source owners and focused tests exist. Fresh checkpoint health was Tendwire 1,047 passed/2 skipped and Herdres 248 passed, but no checked-in completion artifact binds those outputs to a final changed tree. |
| Cross-repo durability contract | **PARTIAL** | Herdres production uses the Tendwire daemon socket and no Tendwire DB/WAL or subprocess CLI. Existing paired tests inject API callbacks and a mock Telegram provider; no full-process crash/restart proof exists. |
| Herdr minimal cleanup-only diff | **FAIL / OWNER EXCEPTION REQUIRED** | The required ACP fork materially changes behavior and source. It cannot satisfy the unchanged cleanup-only contract without a ratified exception or goal amendment. |
| Review protocol and release artifacts | **PARTIAL** | Stable checkpoints and adversarial reviews exist. Final full-process, live Telegram, adapter matrix, release SLOC ledger, wave log, and rollback artifacts do not. |

## SLOC and component evidence

### Tendwire

The exact 39-file one-charge ledger remains 20,985 SLOC. Wave 16 supplies only planning
allocations and prototype gates; none is a measured implementation result. The completion
audit therefore does not use the 13,500 table as progress.

### Herdres

Herdres has 16 production Python files and 8,115 canonical SLOC. A provisional whole-file
attribution exposes three direct upper-bound failures:

| Component | Current | Published high | +15% hard ceiling | Result |
|---|---:|---:|---:|---|
| State/provider facts | 2,180 | 1,600 | 1,840 | fails by 340 |
| Command ingress | 2,978 | 1,600 | 1,840 | fails by 1,138 |
| Tendwire client | 763 | 600 | 690 | fails by 73 |

Presenter is 758, Telegram/rendering 988, and config/support 448. Decision behavior is
embedded across ingress, state, and presentation and has no non-overlapping row. Completion
needs an exact 16-file one-charge ledger and an owner ruling that component ranges are
upper capacities; symmetric lower-bound enforcement would penalize deletion.

`state.record_provider_mutation` is 130 lines against the branch's 120-line function gate.
The passing readiness test is explicitly named
`test_stable_checkpoint_sloc_is_honest_but_not_architecture_release_ready`; it asserts
the state/client/function breaches as debt markers rather than proving them resolved.

## Tendwire mandatory-deletion audit

| GOAL §3 item | Classification | Evidence and remaining action |
|---|---|---|
| Migration chain, legacy DDL, backfills/column repair | **SOURCE GONE; ACTIVE DOC RESIDUE** | `store/schema.py` has one exact schema-v30 DDL, exact validation, and explicit discard. README, RELEASE, INSTALL, and older benchmark prose still describe obsolete versions/migrations. |
| Old Herdr-turn watermark/completion/refresh-retry subsystem | **SOURCE GONE; DOC RESIDUE** | Old modules are absent. Current Tendwire `turn.delta` is a separate public durable feed and is not this deleted subsystem. Active release/install prose still names deleted APIs. |
| `agent_event_tombstones` and replay fingerprinting | **AMBIGUOUS** | Tombstones are gone and retention hard-deletes events. Live `agent_events.payload_fingerprint` and replay-conflict checks remain intentionally required by current ACP atomicity tests. The goal must clarify whether only tombstone-era fingerprinting was meant. `docs/acp-migration.md` incorrectly still claims retention creates tombstones. |
| `backends/herdr_command.py` | **PROVEN GONE** | File, imports, and executable tests absent. |
| `core/actions.py` shell | **PROVEN GONE** | File, imports, and executable tests absent. |
| Legacy/dual/shadow turn models and `route_kind` | **PROVEN GONE** | Production selector/route plumbing absent; negative config tests prove old switches cannot reactivate it. |
| Legacy v0 receipt replay | **SOURCE GONE; TEST/DOC RESIDUE** | Current receipt authority is v1 only. A test fabricates preupgrade terminal state, and active docs retain migration/collision prose. Review whether the negative invariant is still required, then rename/rewrite it without compatibility authority. |
| CLI Store fallback | **SOURCE GONE; ACTIVE DOC RESIDUE** | `cli.py` uses `DaemonAPIClient` and imports no Store/SQLite. Active README text still promises fallback. |
| Test-only Store wrappers | **RESIDUE** | The original wrapper set is gone. `events.record_agent_event` and `turns.apply_turn_refresh` have no source callers; tests and the SQLite sidecar benchmark still call them. Delete both with exports and adapt surviving tests/tooling to current atomic APIs. |
| `herdr_cli` / `herdr_events` observation | **PRODUCTION GONE; TOOLING DECISION REQUIRED** | Runtime modules are gone. `scripts/herdr_smoke.py` remains a 1,635-SLOC subprocess observation harness with fixtures and release-artifact tests. Decide whether it is required external conformance tooling or prohibited residue; it cannot be silently classified either way. |
| Tests of deleted code | **RESIDUE** | Historical suites are gone, but wrapper calls, preupgrade receipt fabrication, live-smoke fixtures, and stale release-artifact assertions remain. Negative absence/security tests are legitimate and stay. |

The current source-preserved Tendwire authorities remain present: durable event/turn/content
projection, attention and pending decisions; privacy sanitization plus connector-edge
forbidden-token rejection; command receipts/submissions/backend claims; exactly one
connector outbox with attempts, leases, dead letters and plan recovery; daemon Unix JSON
API; and ACP-only runtime through the Herdr socket.

## Herdres mandatory-deletion audit

All named Herdres production deletions are **PROVEN GONE** at `d581368`:

- `herdr_turn_adapter.py`, `herdres_pending_hook.py`, transcript readers, `/proc` PID
  resolution, pane inference, and pending-source observation;
- turn-list/delta/pending fallback and rebootstrap;
- stable-key migration planner and legacy ingress/gateway schemas or offsets;
- legacy topic-ID finders and `list_finals_are_authoritative` branching;
- `tendwire_turn_jobs`, local presentation plan/recovery ledgers, partial-final registry,
  legacy health dashboard, post-ACK reconciliation, and accepted-notification journal;
- operator recovery CLI, `outbound_dispatcher.py` WAL watching, and subprocess Tendwire
  client; and
- tests executing those deleted implementations.

The surviving local ingress queue and provider facts are required, not duplicate Tendwire
durability. They own Telegram update acquisition, stable command request IDs, topic and
message ownership, provider acceptance evidence, and at-most-once notice state. Tendwire
alone owns delivery ordering, leases, attempts, plan tokens, dead letters, command receipts,
turns, and connector recovery.

The production seam is daemon-socket-only. `tendwire_client.py` uses bounded AF_UNIX,
pinned endpoint identity, and peer credentials. Herdres production neither imports
Tendwire Store/CLI nor opens Tendwire SQLite/WAL files. Its SQLite use is limited to its own
ingress queue and read-only health observation of that queue.

## Preserved behavior: source present, completion proof missing

| Capability | Current automated evidence | Missing completion evidence |
|---|---|---|
| Topics create/icon/name/retire | reservation, ambiguous acceptance, ownership, exact retire and 429 embargo tests | production processes plus live private Telegram forum; restart after acceptance; no duplicate topic |
| Working cards | guarded checkpoint/send/edit/ACK and one-provider-call tests | real ACP worker → daemon → Herdres → Telegram; repeated edits remain one message |
| Finals replace working | alias-safe replacement, span rendering, prepare/commit tests | production multipart plan, restart/resume, live edit-in-place and no orphan |
| Command ingress | stable IDs, queue replay, digest CAS, lease-loss reuse tests | production gateway/daemon/ACP, lost response and restart, single execution, live reply receipt |
| Provider receipt and ACK | provider-fact replay and ACK-loss socket tests | kill after Telegram acceptance, restart both processes, no duplicate provider send |
| Decision buttons | callback fingerprint/order and guarded local mutation tests | daemon decision → Telegram click → ACP answer → resolved markup, including restart |
| Privacy, 429, oversize | recursive privacy, deterministic provider-stub 429, lossless large rendering | privacy-scanned full-process/live artifacts; real 429 induction is not required |

The current paired test is not full-process evidence: it imports Tendwire classes,
constructs `TendwireDaemonAPI` with injected callbacks, runs a server thread, seeds stores
manually, and uses an in-memory Telegram provider.

### Required failure-mode closure

The following matrix is deliberately separate from the general capability table. A source
owner or unit test does not by itself close the corresponding full-process or live gate.

| ARCHITECTURE §5 required mode | Current source/unit evidence | Missing completion proof |
|---|---|---|
| 1. Telegram accepted, ACK failed | provider-fact/message binding, guarded ACK, repoll and ACK-loss socket tests | kill the production presenter after Telegram acceptance and before ACK; restart both processes and prove the bound message is not sent again |
| 2. Final while working edit is in flight | guarded working/final mutations and final-wins ordering tests | race real production working edit with a final and prove the final replaces the card while later working edits are no-ops |
| 3. Superseded finals | supersession, alias-safe replacement, and exact-retire tests | production restart trace plus live forum proof that obsolete messages are deleted or folded and leave no orphan |
| 4. Telegram 429 | deterministic provider-stub embargo and retry-after tests | full-process forced-429 trace proving lease deferral and no tight loop; a deliberately induced live Telegram 429 is not required |
| 5. Oversize final interrupted mid-split | lossless rendering, ordered parts, plan-token prepare/commit tests | kill the production presenter mid-multipart, restart, and prove resume from the durable plan token without duplicate or missing parts |
| 6. Topic-create crash | topic reservation, provider binding, ambiguous-acceptance and repoll tests | crash after live Telegram topic acceptance but before content send; restart and prove the bound topic is reused rather than duplicated |
| 7. Upgrade with in-flight leases | exact schema mismatch/discard behavior and lease tests exist independently | release procedure and full-process trace must choose and prove drain-or-discard, including loud accounting of discarded state |

Decision-answer idempotency and route-generation fencing are additional cross-cutting
requirements: current fingerprint/order, stale-token, digest-CAS, generation, and lease-loss
tests are necessary, but completion still needs a real Telegram callback routed to the
correct ACP generation exactly once across restart.

## ACP and Herdr audit

The visible-console fork supports exactly `codex`, `claude`, `gemini`, `hermes`, `omp`,
and `kimi`, with fixed executable/argument, version, capability-probe, hash, registration,
generation, and leased-console contracts. Tendwire's unit suite exercises the bounded ACP
transport/runtime with fake adapters and exercises the visible console, but there is no
checked-in six-adapter completion matrix or live delivery artifact.

The Herdr contract is not a minimal diff:

- GOAL baseline `4ffd99c2` to fork `9026d9bc`: 1,077 repository files changed,
  239,248 additions and 27,937 deletions; source/build subset 224 files,
  82,355 additions and 16,566 deletions. Much of this is later upstream synchronization.
- Isolated ACP series base `d293951f` to `9026d9bc`: 36 files,
  6,052 additions and 393 deletions. It adds owned endpoints, visible console, resume,
  validation, OMP/Kimi adapters, ARM CI, and probe fixes.

Prior owner instructions to use the fork explain operational intent but do not make this a
cleanup-only diff. Completion requires an explicit superseding exception, separation of
upstream synchronization from project ACP changes, hunk-level API/behavior review, and
proof that the endpoint/status contract consumed by Tendwire is frozen and compatible.

No Kimi model was used as implementation or review workforce in this audit. Kimi adapter
compatibility remains a product contract; absent authority to execute a live Kimi
conformance run must be recorded as blocked rather than converted into a generic ACP pass.

## Active documentation and artifact gaps

Active Tendwire README/RELEASE/INSTALL claims remain stale around migrations, CLI Store
fallback, tombstone retention, deleted Herdr-turn APIs/tests, and historical receipt
compatibility. Historical baseline/design evidence may remain if labeled historical;
operator and release documentation must match the final source.

Herdres runtime documentation is mostly current. Its Wave 4 design notes still say
implementation is unauthorized and reference deleted modules; they need a clear
historical/superseded banner. Installer non-destructive fixtures should use neutral foreign
unit sentinels instead of deleted module names. The duplicate Tendwire unit example must be
validated against the paired release or removed in favor of Tendwire's authoritative unit.

None of the future Wave 16 completion artifacts currently exists: no bound full-suite
record, production caller/external-client inventory, full-process cross-repo JSON, live
Telegram JSON, six-adapter matrix, cutover/rollback note, final SLOC ledger, or release
summary. Previously observed green suites are checkpoint health, not substitutes.

## Explicit non-goals and scope controls

| GOAL non-goal | Current audit result | Completion control |
|---|---|---|
| No new features or configuration knobs | no feature or configuration edit is authorized by this audit | compare baseline-to-final-head public behavior and configuration-key inventories; any addition requires rejection or explicit owner authority |
| No new production dependencies | no dependency edit is authorized; upstream `agent-client-protocol` 0.11 is already in scope | compare every production manifest/lockfile baseline-to-head and account for each changed dependency |
| No historical message catch-up | explicitly preserved as a non-goal | do not replay old Tendwire or Telegram history; this is distinct from preserving the current live Telegram update cursor and durable in-flight work |
| No long migration chain or backward-compatibility shim | current Tendwire schema is exact v30 with discard on mismatch | reject compatibility loaders, migration ladders, and old-wire aliases introduced after the baseline |
| No Herdr behavior change | the visible-console fork violates the unchanged baseline contract | block completion on the explicit Herdr exception/amended baseline; its ACP endpoint and adapter features cannot be smuggled in as cleanup |
| No unrelated reformat/refactor | no production edit is made in this audit | require a touched-file allowlist and hunk-level review for every implementation wave |
| No relocation of deleted bulk | no third runtime owner has been identified | baseline-to-head caller, dependency, config, helper, script, generated-code, and third-repository inventories must prove deletion rather than relocation |

No historical catch-up also means old Telegram messages are not a release prerequisite.
The release proof starts from a declared live cursor and covers new messages plus already
durable in-flight leases only. `scripts/herdr_smoke.py` is excluded from canonical
production SLOC, but its 1,635 lines still require an explicit conformance-tooling versus
prohibited-observation-residue classification; that exclusion cannot be used as SLOC
counting credit. Likewise, public `turn.delta` and turn-list schema 1 remain outside the
mandatory-removal list unless the owner separately authorizes their deletion.

## Remaining lawful work and hard gates

Behavior-preserving work can continue without deleting public capabilities:

1. remove the two no-source-callsite Store helpers and adapt tests/benchmark directly to
   the current atomic APIs;
2. resolve compatibility-only tests/tooling and correct active documentation;
3. reduce Herdres state, command ingress, Tendwire client, and the 130-line provider
   mutation while preserving provider facts and the one ingress queue;
4. run Wave 16 P1–P5 conventional reductions outside the gated `store/db.py` security
   work, measuring every row and retaining all public APIs; and
5. after code stabilizes, build the full-process, live Telegram, and adapter evidence.

Hard owner gates remain:

- global target and component-band semantics;
- SQLite concurrent same-UID threat contract before `store/db.py`/daemon security work;
- whether event replay fingerprint wording means current live-row conflict fencing;
- Herdr cleanup-only exception/amended baseline;
- whether `turn.delta` or turn-list schema 1 may be deleted if behavior-preserving work
  cannot meet the global ceiling; and
- authority for any live Kimi conformance execution, without using Kimi as work-force.

Until those gates are ruled, the project may become cleaner and smaller but cannot be
declared ready for release or deployment.
