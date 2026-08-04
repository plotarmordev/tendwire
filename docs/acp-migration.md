# ACP-required architecture

ACP is Tendwire's required semantic and command protocol for every supported
agent. This is a release boundary, not a runtime rollout switch: rollback means
deploying an earlier release. There are no legacy, shadow, preferred, dual, or
fallback modes in the daemon.

## Authority split

| Concern | Authority |
| --- | --- |
| Workspace, pane, worker lifecycle, and ACP endpoint ownership | Herdr |
| Public stable worker identity | Tendwire's authenticated Herdr projection |
| Agent session, messages, tools, plans, usage, and permissions | ACP |
| Durable journal, public turn projection, privacy, and finality | Tendwire |
| Command idempotency and uncertain outcomes | Tendwire receipts |
| Connector retry and delivery state | Tendwire outbox |
| Telegram presentation | Herdres |

Herdr is not a transcript or command-input transport. Tendwire consumes its
worker/pane lifecycle events, verifies the worker generation, and asks it to
mint a one-shot private ACP endpoint. A supported worker without an explicitly
Herdr-owned healthy ACP endpoint is unavailable and degrades ACP health.

An ACP `sessionId`, endpoint ticket, adapter command, terminal ID, and pane ID
are private. Tendwire binds the session to the current private worker binding
and rejects updates or commands after that generation moves, expires, or is
replaced.

## Runtime and protocol boundary

The daemon supervises one ACP worker session for each eligible Herdr worker.
The supervisor owns reconciliation, generation fencing, reconnect, console
exchange, and command routing. The worker session owns initialize, session
open/load/resume, update draining, permission handling, cancellation, and
bounded shutdown. The bounded connection owns subprocess I/O, JSON-RPC request
correlation, framing limits, stderr limits, and backpressure.

Tendwire uses the official `agent-client-protocol` Python package for generated
ACP schemas and validation. Tendwire retains its bounded stdio connection
because its hard frame-size, queue, write-deadline, shutdown, and privacy
requirements are stricter than the upstream convenience transport. Adapter
executables remain separately installed and replaceable; no adapter source tree
is imported or vendored.

Before every prompt and during reconciliation, Tendwire verifies Herdr's
non-mutating ACP status for the exact worker generation. Reconnect always asks
Herdr for a fresh one-shot endpoint. A missing route fails before receipt
reservation; a failure after the durable `send_started` boundary is terminally
uncertain and is never retried through another transport.

## Durable projection and privacy

The structured journal accepts user and agent messages, thoughts, tool calls,
tool updates, plans, usage, session information, and private extension/control
updates. Producer identity and raw ACP payloads are retained only on the
private side. The public turn projection remains deliberately narrower and
continues to drive finality and connector eligibility.

`TENDWIRE_ACP_THOUGHT_POLICY` controls private thought retention:

- `disabled` discards thoughts before persistence.
- `private_summary` retains only updates carrying Tendwire's exact trusted
  summary marker.
- `private_all` retains raw thought chunks for explicit local diagnostics.

No thought policy grants public or outbox delivery. Raw reasoning, tool input,
tool output, session IDs, paths, terminal data, and adapter metadata must remain
outside every public API and connector payload.

ACP permission requests use the durable pending-decision projection. Only a
sanitized title and numbered public choices are exposed. The private option ID,
tool call, arguments, ACP session, and metadata remain behind the boundary.
`answer_decision` is fenced to the exact worker binding and generation and is
accepted only after the full JSON-RPC response frame is written.

## Retention and replay

`event_retention_days` bounds private structured-event payloads. Cleanup
replaces expired payloads with compact identity tombstones so exact replays
remain idempotent and conflicting producer-identity reuse fails closed.
Tombstones preserve only bounded replay evidence and authority time; they are
not recoverable event content.

Tendwire continues to own durable command receipts and its neutral connector
outbox. ACP adapter restarts therefore do not erase accepted-command evidence,
turn finality, acknowledgement state, or delivery retries.

## Release and conformance gates

An ACP-required release must pass:

- initialization and official-schema validation against each supported
  adapter;
- new/load/resume, prompt, steering, cancellation, and permission flows;
- generation fencing and reconnect with freshly minted Herdr endpoints;
- no missing or duplicated user/final messages across adapter restarts;
- deterministic replay deduplication and exactly-once final projection;
- complete tool and plan lifecycle after cancellation or permission denial;
- absence of raw thoughts, tool payloads, session IDs, and terminal data from
  public APIs and the outbox;
- durable receipt behavior at every pre-send and post-send failure boundary;
- exact worker continuity across Herdr pane moves and agent recreation.

If a release fails these gates, roll back the release. Do not reintroduce a
runtime fallback mode.
