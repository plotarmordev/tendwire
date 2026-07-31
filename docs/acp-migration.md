# ACP primary-event migration

This document defines the experimental migration from backend-specific
transcript readers to Agent Client Protocol (ACP). ACP is not yet Tendwire's
default. The stock daemon now contains the production coordinator and command
path, but activates them only for an explicitly Herdr-owned ACP worker.
Herdr remains authoritative for workspace, pane, worker identity, process
liveness, and command routing until the ACP control path is proven separately.
Tendwire remains authoritative for persistence, reconciliation, public safety,
command receipts, and connector delivery.

## Source policy

`TENDWIRE_AGENT_EVENT_SOURCE` controls projection precedence:

- `legacy`: use the existing Herdr/Codex/OMP turn readers only.
- `acp_shadow`: ingest ACP events
  durably without projecting them; legacy turns remain authoritative. Automated
  comparison is not implemented yet.
- `acp_preferred`: use an explicitly Herdr-owned ACP endpoint when available;
  fall back to legacy only before any ACP command reservation or observable
  send.
- `acp_required`: use ACP only and fail closed when the binding or stream is not
  healthy. The daemon does not start its legacy turn scheduler in this mode.
  This mode requires every eligible worker endpoint to be ACP-owned and healthy.
  Zero observed workers is a valid idle state; if a later worker appears
  without ACP ownership, runtime health immediately becomes degraded.

The default is `legacy`. In an ACP mode the coordinator asks Herdr for a
one-shot private endpoint, validates its worker generation and explicit
`acp_owned_ready` lifecycle, then creates/loads/resumes the ACP session. An
ordinary live PTY session is never treated as ACP-owned.

## Authority split

| Concern | Authority |
| --- | --- |
| Workspace and logical pane identity | Herdr |
| Public stable worker identity | Tendwire's authenticated Herdr projection |
| ACP session and message identity | ACP agent, stored privately by Tendwire |
| Messages, thoughts, tools, plans, and usage | ACP coordinator for ACP-owned workers; currently experimental |
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
- private extension/control state, including available commands, current mode,
  and session configuration updates

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
Adapter promotion and rollback remain operator-managed.

## Runtime lifecycle

For ACP v1 stdio, the component that owns the adapter process also owns framing,
initialization, request correlation, stderr handling, cancellation, and bounded
shutdown. Tendwire must not claim an ACP worker healthy until initialization,
capability negotiation, session creation/load/resume, and private worker binding
all succeed.

The stock daemon has a multi-worker runtime factory. It discovers endpoints
through Herdr's private `agent.acp_endpoint` method, validates the fixed stdio
attach shape, and supervises one runtime per worker generation. Endpoint
tickets are one-shot private values: Tendwire uses one only for its immediate
attach and never persists or publishes it. Reconnect always re-resolves Herdr
authority and mints a fresh endpoint.

While attached, the coordinator uses non-mutating `agent.acp_status` checks
before every prompt and during reconciliation. The reported lifecycle must be
`acp_owned_attached` and its numeric generation must match the attached slot.
A mismatch or unavailable status retires the slot before any prompt frame is
written. Endpoint minting is never used as a status probe.

In `acp_preferred`, the legacy scheduler remains available only for workers not
currently owned by a healthy ACP slot. It rechecks this exclusion after dequeue
and immediately before a legacy read, preventing queued legacy work from
overwriting or duplicating the active ACP worker projection.

Disconnect handling is conservative:

1. Stop accepting events from the disconnected generation.
2. Persist stream health without publishing private adapter details.
3. In `acp_preferred`, allow the next legacy refresh to become authoritative.
4. Reinitialize and rebind before accepting ACP events again.
5. Reconcile replayed messages and tool calls by producer identity.

Command acknowledgement occurs after the complete `session/prompt` request
frame is written, not after the agent finishes the turn. End-of-turn response
and update draining continue under runtime supervision. A failure after the
durable `send_started` transition is terminally uncertain and never falls back
to a second transport.

## Retention

`event_retention_days` also bounds raw structured ACP journal payloads. Due
automatic maintenance and explicit online cleanup replace expired payload rows
with compact identity tombstones in bounded batches. Candidate scanning reads
only bounded identity metadata and the existing payload digest; it does not load
the retired private payload into maintenance memory. A tombstone retains the
original sequence and a replay-contract fingerprint, allowing exact retries to
remain idempotent and conflicting reuse to fail closed without retaining
messages, thoughts, raw tool input/output, or other source payloads.

Schema v26 tombstones also retain the original event `observed_at` as the only
authority time for a one-time repair when the matching owned turn projection
is provably absent. Exact replays never re-merge caller timestamp or content
into an existing live or superseded projection, so they cannot reorder final
connector delivery. Tombstones migrated from pre-v26 stores have no retained
authority time; they remain deduplication evidence but cannot repair a turn.

Each tombstone has bounded per-event identity metadata, but tombstone count is
permanent and therefore grows with the number of distinct source events.
Tombstones are intentionally not deleted automatically: removing them would make
a late replay indistinguishable from a new event. Cleanup asks SQLite to scrub
deleted cells in modified pages, but WAL/checkpoint timing, filesystem snapshots,
and backups have independent operator-managed lifecycles. Logical retention is
not an immediate physical-erasure or cryptographic-erasure guarantee.

## Cross-repository requirements

Herdr provides a private `agent.acp_endpoint` launch/proxy surface containing
adapter identity/version, session-open mode, cwd, generation, and an explicitly
ACP-owned lifecycle. Tendwire accepts only the configured Herdr executable and
the fixed `agent acp-attach` argument shape; arbitrary executable, environment,
or argument injection is rejected.

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

The ACP runtime implements prompt submission, cancellation, permission handling,
per-worker coordination, reconnect, and receipt-backed command routing. ACP
remains non-default until the cross-repository integration and rollout gates
above pass against real adapters.
