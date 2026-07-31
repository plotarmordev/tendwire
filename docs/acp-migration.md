# ACP primary-event migration

This document defines the experimental migration from backend-specific
transcript readers to Agent Client Protocol (ACP). ACP is not yet Tendwire's
default or a production-wired semantic authority.
Herdr remains authoritative for workspace, pane, worker identity, process
liveness, and command routing until the ACP control path is proven separately.
Tendwire remains authoritative for persistence, reconciliation, public safety,
command receipts, and connector delivery.

## Source policy

`TENDWIRE_AGENT_EVENT_SOURCE` controls projection precedence:

- `legacy`: use the existing Herdr/Codex/OMP turn readers only.
- `acp_shadow`: with an explicitly supplied ACP runtime, ingest ACP events
  durably without projecting them; legacy turns remain authoritative. Automated
  comparison is not implemented yet.
- `acp_preferred`: an experimental integration surface for future per-worker
  ACP authority and legacy fallback. The stock daemon does not discover or
  construct an ACP runtime.
- `acp_required`: use ACP only and fail closed when the binding or stream is not
  healthy. The daemon does not start its legacy turn scheduler in this mode.
  This mode requires an explicitly supplied healthy authority runtime and is
  intended for conformance testing, not rollout.

The default is `legacy`. Selecting an ACP mode does not discover an adapter,
invent an ACP session, or bind a worker. Until a per-worker authority
coordinator exists, operators must not interpret `acp_preferred` as proof that
ACP is authoritative.

## Authority split

| Concern | Authority |
| --- | --- |
| Workspace and logical pane identity | Herdr |
| Public stable worker identity | Tendwire's authenticated Herdr projection |
| ACP session and message identity | ACP agent, stored privately by Tendwire |
| Messages, thoughts, tools, plans, and usage | Future ACP authority coordinator; currently experimental |
| Turn finality and connector eligibility | Tendwire durable projection |
| Telegram presentation and delivery state | Herdres |
| Command idempotency and uncertain outcomes | Tendwire command receipts |

An ACP `sessionId` is never a public worker identity. Tendwire must bind it to
the current private `WorkerBinding` generation and reject events after that
binding expires, moves, or is replaced. Replayed ACP events must deduplicate on
their producer identity without changing the public worker identity.

## Canonical events

The structured event journal accepts these semantic kinds:

- user message
- agent message
- thought
- tool call
- tool call update
- plan
- usage
- session information

Producer IDs, raw inputs, raw outputs, session IDs, terminal IDs, paths, and
reasoning are private. Public turn projection is deliberately narrower:
`user_text`, `assistant_stream_text`, `assistant_final_text`, completion state,
and existing safe metadata. Tool and plan presentation requires its own
sanitizing projection and must not reuse raw ACP payloads.

## Thought policy

`TENDWIRE_ACP_THOUGHT_POLICY` has three values:

- `disabled`: discard thought chunks before persistence.
- `private_summary`: retain a chunk privately only when a trusted adapter sets
  the exact update-level marker
  `_meta["tendwire.dev/thought_kind"] = "summary"`; unclassified, unknown,
  contradictory, and raw chunks are discarded.
- `private_all`: retain every thought chunk privately for explicit local
  diagnostics.

The default is `disabled`. Stable ACP v1 does not define a raw-versus-summary
classification. The `private_summary` marker is only a Tendwire adapter
convention and is not an ACP guarantee; enable it only for a trusted adapter.
No thought policy grants connector delivery. Herdres must never receive a raw
thought event. A future public summary feature requires a separate schema,
sanitizer, explicit operator opt-in, and tests that prove raw reasoning cannot
cross the boundary.

## Upstream upgrade boundary

Tendwire integrates with the stable ACP wire protocol, not an adapter's source
tree. Official adapters such as `codex-acp` and `claude-agent-acp` remain
separately installed executables and must be replaceable without vendoring,
rebasing, or resolving Tendwire source conflicts.

The boundary has four rules:

- negotiate protocol version and capabilities at every process start;
- never import adapter implementation modules or depend on their repository
  layout, generated internal types, commits, or private event handlers;
- ignore unknown standard update variants conservatively and retain explicitly
  namespaced extension metadata only on Tendwire's private side;
- verify adapter releases with black-box ACP compatibility fixtures before
  promotion, while keeping the previously proven executable for rollback.

The wire-process boundary is designed so an adapter upgrade does not require a
Tendwire rebase. The current initialization-only probe is not a promotion gate:
it does not authenticate, create/load a session, prompt, validate updates,
exercise permissions/cancellation, or pin an executable digest. Stateful
conformance fixtures and an immutable rollback manifest are still required.
Session resume/replay reconciliation is likewise incomplete.

## Runtime lifecycle

For ACP v1 stdio, the component that owns the adapter process also owns framing,
initialization, request correlation, stderr handling, cancellation, and bounded
shutdown. Tendwire must not claim an ACP worker healthy until initialization,
capability negotiation, session creation/load/resume, and private worker binding
all succeed.

The stock daemon currently has no production ACP runtime factory or multi-worker
supervisor. Herdr must first provide authenticated per-worker launch/session
metadata, and Tendwire must add per-worker health, authority, reconnect, and
durable projection recovery before ACP can become the default.

Disconnect handling is conservative:

1. Stop accepting events from the disconnected generation.
2. Persist stream health without publishing private adapter details.
3. In `acp_preferred`, allow the next legacy refresh to become authoritative.
4. Reinitialize and rebind before accepting ACP events again.
5. Reconcile replayed messages and tool calls by producer identity.

These disconnect steps are requirements, not a description of the current
implementation.

## Retention

`event_retention_days` also bounds raw structured ACP journal payloads. Due
automatic maintenance and explicit online cleanup replace expired payload rows
with compact identity tombstones in bounded batches. Candidate scanning reads
only bounded identity metadata and the existing payload digest; it does not load
the retired private payload into maintenance memory. A tombstone retains the
original sequence and a replay-contract fingerprint, allowing exact retries to
remain idempotent and conflicting reuse to fail closed without retaining
messages, thoughts, raw tool input/output, or other source payloads.

Each tombstone has bounded per-event identity metadata, but tombstone count is
permanent and therefore grows with the number of distinct source events.
Tombstones are intentionally not deleted automatically: removing them would make
a late replay indistinguishable from a new event. Cleanup asks SQLite to scrub
deleted cells in modified pages, but WAL/checkpoint timing, filesystem snapshots,
and backups have independent operator-managed lifecycles. Logical retention is
not an immediate physical-erasure or cryptographic-erasure guarantee.

## Cross-repository requirements

Herdr needs an ACP-aware launch or proxy surface that exposes enough private
metadata for Tendwire to bind an ACP session to an existing logical pane. The
binding must survive terminal/session churn without making ACP identity a
public continuity input. It must also identify adapter executable/version,
session-open mode, working directory, and binding generation without exposing
those values on public APIs.

Herdres needs optional presentations for sanitized tool and plan progress. It
does not ingest ACP directly: it continues polling Tendwire's neutral outbox so
delivery retries, topic binding, rate limits, and Telegram state remain outside
the agent protocol.

## Rollout gates

Promotion remains blocked at the default `legacy` posture. When the missing
runtime and Herdr prerequisites exist, it may proceed `legacy` -> `acp_shadow`
-> `acp_preferred`. The following
must pass before `acp_required` is considered:

- no missing or duplicated user/final messages across adapter restarts;
- deterministic replay deduplication;
- correct open-to-final turn identity;
- tool lifecycle completion after cancellation and permission denial;
- plan replacement without stale entries;
- thought and raw tool payloads absent from every public API/outbox surface;
- fallback after adapter failure without regressing existing final delivery;
- exact worker continuity across Herdr pane moves and agent-session recreation.

ACP prompt submission, cancellation, and permission handling are a later
control-path migration. They must preserve Tendwire's existing request receipts
and uncertain-outcome rules before replacing Herdr command routing.
