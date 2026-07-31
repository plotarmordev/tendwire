# ACP primary-event migration

This document defines the migration from backend-specific transcript readers to
Agent Client Protocol (ACP) as Tendwire's preferred semantic event source.
Herdr remains authoritative for workspace, pane, worker identity, process
liveness, and command routing until the ACP control path is proven separately.
Tendwire remains authoritative for persistence, reconciliation, public safety,
command receipts, and connector delivery.

## Source policy

`TENDWIRE_AGENT_EVENT_SOURCE` controls projection precedence:

- `legacy`: use the existing Herdr/Codex/OMP turn readers only.
- `acp_shadow`: ingest ACP events durably, compare them with legacy turns, and
  keep legacy turns authoritative.
- `acp_preferred`: use ACP for an authenticated, healthy ACP-bound worker and
  fall back to the legacy reader for every other worker.
- `acp_required`: use ACP only and fail closed when the binding or stream is not
  healthy. This mode is intended for conformance testing, not initial rollout.

The default is `acp_preferred`. The default does not invent an ACP session or
silently replace a worker: without a proven binding, legacy observation remains
authoritative.

## Authority split

| Concern | Authority |
| --- | --- |
| Workspace and logical pane identity | Herdr |
| Public stable worker identity | Tendwire's authenticated Herdr projection |
| ACP session and message identity | ACP agent, stored privately by Tendwire |
| Messages, thoughts, tools, plans, and usage | ACP when preferred and healthy |
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
- `private_summary`: retain readable reasoning summaries privately and discard
  raw-reasoning chunks.
- `private_all`: retain every thought chunk privately for explicit local
  diagnostics.

The default is `private_summary`. No thought policy grants connector delivery.
Herdres must never receive a raw thought event. A future public summary feature
requires a separate schema, sanitizer, explicit operator opt-in, and tests that
prove raw reasoning cannot cross the boundary.

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

An adapter upgrade therefore restarts only its owned process/session; it does
not require a Tendwire rebase. A session may resume when the new adapter
advertises that capability. Otherwise Tendwire opens a new transport generation
and reconciles it through the durable semantic journal.

## Runtime lifecycle

For ACP v1 stdio, the component that owns the adapter process also owns framing,
initialization, request correlation, stderr handling, cancellation, and bounded
shutdown. Tendwire must not claim an ACP worker healthy until initialization,
capability negotiation, session creation/load/resume, and private worker binding
all succeed.

Disconnect handling is conservative:

1. Stop accepting events from the disconnected generation.
2. Persist stream health without publishing private adapter details.
3. In `acp_preferred`, allow the next legacy refresh to become authoritative.
4. Reinitialize and rebind before accepting ACP events again.
5. Reconcile replayed messages and tool calls by producer identity.

## Cross-repository requirements

Herdr needs an ACP-aware launch or proxy surface that exposes enough private
metadata for Tendwire to bind an ACP session to an existing logical pane. The
binding must survive terminal/session churn without making ACP identity a
public continuity input.

Herdres needs optional presentations for sanitized tool and plan progress. It
does not ingest ACP directly: it continues polling Tendwire's neutral outbox so
delivery retries, topic binding, rate limits, and Telegram state remain outside
the agent protocol.

## Rollout gates

Promotion proceeds `legacy` -> `acp_shadow` -> `acp_preferred`. The following
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
