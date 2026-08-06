# ACP-primary release proof

This is the active full-process and live release gate for the paired Tendwire,
Herdres, and Herdr system. It does not authorize deployment or a service
restart. The proof is owner-authorized, starts from clean accepted checkouts,
and uses no direct Herdr CLI observer.

## Owner-ratified stopping boundary

For this checkpoint, the owner supersedes the 14,000-line global reduction
target with the exact canonical Tendwire and Herdres counts recorded in the
bound release summary. Existing component-budget debt remains explicit; it is
not hidden by reclassification. The feature set is frozen, no further
architecture reduction is authorized, and the SQLite/security redesign is out
of scope. Only correctness or privacy fixes found by the gates below may alter
the candidate.

The owner-authorized test-branch cutover is a provisional deployment, not an
RC promotion. It may start the frozen candidate and a fresh Telegram cursor so
the live gates can run, but the transaction remains `validating` until the
uninterrupted-hour evidence and all four artifacts pass. `deployed` is reserved
for the later immutable finalization step; provisional operation cannot be
reported as release-ready.

The ACP-enabled Herdr fork at
`9026d9bc5a12d9adc2d9f68ebdc564133e4098b4` is the accepted Herdr baseline
exception. Its endpoint ownership, visible-console bridge, resume behavior, and
six supported adapters are intentional release inputs rather than a violation
of the older cleanup-only Herdr assumption.

`agent-client-protocol` 0.11.0 remains the upstream schema authority, but its
transport/connection implementation is rejected for this checkpoint. The
reviewed installed 29-file Python source digest is
`f5e621738a5651da9d14559806ab1d3491e8a9da6a72e686baf087e67a87e5f6`.
It is SHA-256 over files sorted by relative POSIX path; each entry is framed as
the four-byte big-endian UTF-8 path length, path bytes, eight-byte big-endian
content length, and raw content bytes.
Its line reader does not impose our hard frame bound after a limit overrun, its
message queues are unbounded, its default stderr pipe is not drained, and its
timeouts and lifecycle differ from the required request-scoped and fail-closed
contracts. Tendwire therefore keeps its bounded transport while using upstream
ACP schemas. The owner's later instruction not to use Kimi supersedes the
earlier bounded-compatibility allowance for this release: no Kimi process or
model is invoked. The adapter matrix must record Kimi as an explicit
owner-exempt row, not as a pass, failure, or silently skipped check.

## Bound inputs

Before starting, record the exact Tendwire, Herdres, and Herdr commit IDs,
source-tree hashes, built artifact hashes, Python version, platform, and service
unit hashes. For each supported adapter (`codex`, `claude`, `gemini`, `hermes`,
`omp`, and `kimi`), record:

- source revision, executable version, and executable SHA-256;
- fixed argument vector and capability-probe result digests;
- adapter registration timeout and ACP protocol version;
- initialization capabilities, adapter hash, session mode, and endpoint
  generation.

Private paths, tickets, sessions, endpoint coordinates, prompts, message IDs,
and credentials must not enter a public artifact. A missing binary, credential,
revision, or execution authority is a blocked adapter entry, never a generic
pass. Exercise initialization and endpoint mint/status for `codex`, `claude`,
`gemini`, `hermes`, and `omp`, and record an explicit pass, fail, or blocked
result for those rows. A failed or blocked row for any of those five adapters
blocks both the RC and deployment; artifact presence alone cannot pass the
gate. Record Kimi separately as `owner_exempt` and do not invoke it.
The existing Kimi-labelled Telegram bot may be identity-checked and polled only
as a compatibility routing receiver. That does not start a Kimi ACP adapter or
model; the matrix and release summary must record
`kimi_model_process_invocations=0`.

Declare a fresh live Telegram update cursor before the first proof message.
Only messages created after that cursor are in scope. Historical catch-up is
not required or permitted.

## Non-disruptive Herdr check

Run `systemctl --user is-active herdr-server.service`. Use status-only commands
if diagnosis is needed. Do not restart Herdr.

Tendwire must use the production Herdr Unix socket only for the read-only
lifecycle discovery methods `workspace.list`, `pane.list`, and `agent.list`,
the ACP ownership methods `agent.acp_status` and `agent.acp_endpoint`, and the
bounded visible-pane transport `agent.acp_console_exchange`. These lifecycle
list methods are the allowed implementation of Herdr's worker/pane lifecycle
ownership; they do not authorize transcript reading, event-list observation,
or pane mutation. Record the public status, adapter identity digest, and
endpoint generation obtained by the production coordinator. Every console
exchange must target the current endpoint generation, hold a live coordinator
lease, accept only bounded console input for that leased adapter session, and
return only bounded console output to the owning process. Reject stale
generations, expired or mismatched leases, unsolicited output, and oversized
input/output. Keep exchanged text and endpoint/session coordinates in the
restricted trace and out of public evidence. Do not invoke `herdr status`,
workspace/agent/pane list, agent send/start, pane move/close, or event-list
subprocesses.

## Production-process topology

Start the installed production Tendwire daemon with its required ACP
supervisor and private daemon socket. Start the installed production Herdres
gateway and presenter against that socket and the live Telegram forum. Use OS
processes and the shipped composition roots; do not import the daemon API into
the proof runner or inject daemon, connector, Store, endpoint, or provider
callbacks.

Before execution, record the literal installed executable paths, fixed argument
vectors, configuration-file hashes, socket path hashes, service/unit names, and
environment-key names for each process in the restricted private trace. Public
artifacts contain only their digests and non-private unit names. Those recorded
invocations are the exact procedure for the bound build; an ad-hoc test entry
point is not an acceptable substitute.

The production path is:

```text
Herdr socket endpoint mint/status + bounded visible-pane console exchange
-> ACP adapter -> Tendwire ingestion/store -> Tendwire daemon/outbox Unix socket
-> Herdres gateway/presenter -> Telegram
```

The proof fails if Herdres calls Herdr directly, reads Tendwire SQLite/WAL
files, invokes the Tendwire CLI as its transport, or if Tendwire observes the
agent through a Herdr CLI subprocess. In every artifact,
`direct_herdr_calls=0` means Herdres made zero direct Herdr calls; it does not
disable Tendwire's six allowed production socket methods.

## Deterministic full-process fault proof

Run these cases with production processes and real Unix sockets. Controlled
process termination and a protocol-faithful Telegram 429 stub are permitted;
in-process callbacks and a general mock provider are not.

1. Lose the ACK after provider acceptance, restart Tendwire and Herdres, and
   prove the existing Telegram provider binding is reconciled rather than
   creating a second message. Only `connector.ack` for the exact live lease ref
   proves delivery; provider acceptance without that ACK remains uncertain and
   must not be reported as exactly once.
2. Race a working-card edit with its final. The final must replace the card and
   every later working edit must be a no-op.
3. Supersede a final across restart and prove the obsolete message is deleted
   or folded without an orphan.
4. Return Telegram 429 with a fixed retry-after. Prove the lease is deferred,
   the embargo is honored, and no tight retry loop occurs.
5. Terminate the presenter during a multipart final, restart it, and prove
   resume from the durable plan token with no missing or duplicate part.
6. Terminate after topic acceptance and before content delivery, restart, and
   prove the accepted topic binding is reused.
7. Exercise the declared upgrade policy with in-flight leases. Record either a
   completed drain or a loud discard count and prove the selected rollback.
8. Replay a decision answer and a stale route generation. Prove one ACP answer,
   idempotent terminal state, and rejection of the stale generation.

Record process IDs only in the private trace. The public artifact records
bounded counts, state transitions, receipt dispositions, restart points, and
outcomes.

## Live Telegram happy path

Use a live configured adapter and the live Telegram forum, never a provider
stub. Starting from the declared cursor:

1. Produce at least two working updates and prove one Telegram card is edited
   rather than duplicated.
2. Produce a final and prove it replaces the working card.
3. Produce a pending decision, select it in Telegram, and prove the exact ACP
   generation receives one answer and the markup resolves.
4. Send one Telegram reply through Herdres gateway to Tendwire
   `command.submit`. Record the opaque request ID, worker ID, public
   fingerprint digest, Tendwire receipt disposition, and ACP submission
   identity. The accepted result must have `status=accepted`,
   `disposition=terminal_accepted`, `delivery_state=submitted`,
   `transport_state=submitted`, the exact target worker, and
   `observed_turn_state` equal to `pending_observation`, `observed`, `complete`,
   or `linked`.
5. Replay the exact same request ID and canonical request and prove Tendwire
   returns its cached durable terminal receipt with no second ACP submission.
   Reuse that ID with changed canonical input and prove
   `status=duplicate_request`, `disposition=terminal_rejected`, and no ACP
   submission.
6. Deliver one lossless multipart final and verify exact ordering, no cutoff,
   and no duplicate parts.

For each Telegram delivery, the presenter must lease work through the
production daemon socket, preserve the provider binding across retries, and
ACK only the exact live lease ref with its sanitized provider result. After the
live cases, run two subsequent no-operation production connector-outbox polls
and presenter passes. Both must report `direct_herdr_calls=0`, return no due
delivery, produce no Telegram write, and leave the durable receipt and provider
binding unchanged.

## Privacy, cleanup, and required artifacts

Recursively scan every public JSON artifact for raw coordinates, private paths,
session/ticket values, backend targets, fingerprints, argv, environment,
stdout/stderr, tokens, secrets, and message content. The finding count must be
zero.

Record final service states, socket and private-file modes, SQLite integrity,
bounded process/thread/file-descriptor counts, and cleanup of proof-created
messages/topics where safe. Stop only the owner-authorized Tendwire and Herdres
proof processes. Do not restart Herdr.

Write these exact files under `docs/evidence/`:

- `wave16-cross-repo-e2e.json`
- `wave16-live-telegram-e2e.json`
- `wave16-acp-adapter-matrix.json`
- `wave16-release-summary.md`

Each artifact binds the recorded revisions and hashes. Missing, stale,
privacy-unsafe, skipped, or generically substituted evidence blocks both the RC
and deployment.

Legacy `source_sync`, forced-source-sync, `source-smoke`, and source-mode wording
in the frozen pre-release goal-pack README, Goals 05, 08b, 10, and 13, and Wave
4 planning records is historical and superseded by this procedure. It is not an
executable release gate and must not be restored to active source, tooling,
installation, or release instructions.
