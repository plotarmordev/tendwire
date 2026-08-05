# Goal 12: Establish a Reproducible Tendwire/Herdres RC Gate

## Objective

Add CI, artifact validation, and a paired release checklist so the reviewed
source-mode system can be announced from reproducible evidence rather than one
machine's environment.

## Confirmed Gaps

- Tendwire has no checked-in `.github/workflows` CI.
- `pyproject.toml` reports version `0.1.0` and `Pre-Alpha`, omits a Python 3.13
  classifier, and the dev extra contains only pytest.
- The sdist include list omits `scripts/` and `RELEASE.md`, although release/live
  smoke documentation relies on those assets.
- The reviewed environment lacked the package-build frontend/backend, so wheel
  and sdist installation were not proven.
- Prior test literals matched provider secret patterns and triggered GitHub
  secret scanning even though they were synthetic.

## Required CI

1. Add a minimal GitHub Actions workflow for supported Python versions. Test
   3.10, 3.11, 3.12, and 3.13 unless support policy is deliberately narrowed and
   documented.
2. Run Python compile checks and the full hermetic pytest suite on every pull
   request and main push.
3. Build wheel and sdist using an isolated PEP 517 build, inspect archive
   contents, install each into a clean virtual environment, and run CLI import/
   `--help`/doctor fixture smokes.
4. Run public-contract/forbidden-value tests and a repository/archive secret
   scan. Test secrets must be assembled at runtime from inert fragments so the
   repository does not itself contain a provider-shaped credential.
5. Run the hermetic Herdr-socket, ACP client/coordinator/daemon, and paired
   daemon-socket tests. Live Herdr, Telegram, systemd, and user home state must
   not be required in default CI, and no Herdr CLI observer may be substituted.
6. Add a paired contract job or documented downstream workflow that tests the
   supported exact Tendwire/Herdres revisions together, including
   `direct_herdr_calls=0`, turn/pending schemas, daemon-socket connector outbox
   behavior, receipt finality, and provider-binding preservation.
7. Pin action major versions and use least-privilege workflow permissions. Pull
   requests must not receive deployment credentials.
8. Keep CI deterministic and bounded. Cache dependencies, not private runtime
   state; fail on flaky retries rather than hiding them.

## Implementation Quality Constraints

- Keep workflow YAML thin and call documented repository commands. Do not copy
  test/build logic into several near-identical jobs or add a release framework.
- Use one source of truth for version/support metadata and validate consistency
  rather than maintaining independent literals throughout docs and scripts.
- Add only scripts that are useful locally and in CI; each must have a narrow
  command contract, deterministic output, and no machine-specific defaults.
- Pin actions and release tooling deliberately, but avoid a large dependency
  stack for archive inspection or secret-pattern checks that Python's standard
  library can perform clearly.
- Keep live/operator proof separate from hermetic CI. Do not make CI silently
  skip a requested proof because credentials or services are absent.
- Prefer deleting stale release instructions over layering another checklist on
  contradictory documentation.

## Required Packaging and Metadata

1. Decide and document the RC version, compatibility pair, support status, and
   Python range. Version, classifiers, changelog/release notes, tag, and artifact
   metadata must agree.
2. Include every file needed by documented install, doctor, license, and
   security workflows in the sdist. Legacy Herdr CLI smoke scripts and fixtures
   must not be shipped.
3. Add build/test tooling to a documented development/release extra without
   adding unnecessary runtime dependencies.
4. Ensure wheel and sdist contain no DBs, sockets, caches, local paths, tokens,
   generated service credentials, private config, worktree artifacts, or ignored
   handoff files.
5. Document source install, packaged install, upgrade, rollback, schema backup,
   permissions, source mode, and known limitations.
6. Keep bot names/tokens and all deployment identities user-configured and
   private. Examples must be generic.

## Final RC Proof Checklist

After Goals 01-11 are independently reviewed and merged, an owner-authorized RC
proof must record all of the following from current commits:

- Tendwire compile and full pytest result.
- Tendwire wheel/sdist build, archive inspection, clean-install smoke, and
  SQLite integrity check.
- Herdres compile and full hermetic suite.
- Non-disruptive Herdr check only:
  `systemctl --user is-active herdr-server.service`; status only if needed.
- The ACP-primary production-process and live Telegram procedure in
  `docs/acp-primary-release-proof.md`.
- Two no-operation production connector-outbox polls and presenter passes,
  proving no duplicate delivery or Telegram repost.
- `direct_herdr_calls=0`.
- No new `Closed by User` spam.
- Legacy `herdr-telegram-topics.timer` inactive and disabled.
- One live inbound proof:
  Telegram reply -> Herdres gateway -> Tendwire `command.submit` -> one ACP
  worker receipt, recording opaque request ID, worker ID, public fingerprint
  digest, command status, and duplicate guard result.
- Long/multipart final proof with no cutoff and no duplicate parts.
- Topic/status/pinned-message behavior remains Herdres-owned and correct.
- Public JSON forbidden-key/value scan returns zero findings.
- Data directory, DB family, local key, and socket modes match Goal 04.

The proof must produce `wave16-cross-repo-e2e.json`,
`wave16-live-telegram-e2e.json`, `wave16-acp-adapter-matrix.json`, and
`wave16-release-summary.md` under `docs/evidence/`. Any missing artifact blocks
the RC and deployment. A failed or blocked adapter-matrix row also blocks both;
artifact presence alone is insufficient.

Do not restart `herdr-server.service`. Restarting `tendwired.service`,
`herdres-gateway.service`, or `herdres.timer` is allowed only in the separately
authorized final deployment/smoke step, never during implementer review.

## Required Tests and Evidence

- CI is green on a pull request from a clean clone.
- Built artifacts install and run without the source checkout on `PYTHONPATH`.
- Archive manifests are attached to the implementation report.
- Release checklist records exact Tendwire/Herdres/Herdr versions and commits.
- A failed migration/install has a documented and exercised rollback path.
- No test count is weakened by deleting/skipping coverage to obtain green CI.

## Completion Report

The final reviewer must report: Done, Tests, Live verification, Service state,
Open-source cleanup, Remaining risks, and next recommended goal. Do not call the
RC complete if any checklist item lacks current evidence.

## Non-Goals

- Do not publish/tag from the implementation branch.
- Do not add unrelated product features.
- Do not deploy or restart any service without separate owner authorization.
