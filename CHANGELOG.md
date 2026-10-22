# Changelog

## 0.1.0-beta.2 — 2026-10-22

Beta.2 adds an interactive cluster monitor, configurable transfers and settings,
and more convenient job and result workflows. Runspoke keeps its single Rust
crate, durable SQLite scheduler, pull-based workers and four-binary distribution.

### Interactive monitoring

- Run `runspk monitor` to browse jobs and workers, inspect attempts and resource
  requests, read live stdout/stderr, and ask the scheduler why a job is queued.
- Search, filter, sort and page through jobs with keyboard controls. Confirm
  cancellation before sending it to the scheduler.
- Polling reconnects automatically and preserves the last successful snapshot
  with its age. The interface handles terminal resizing and restores terminal
  state on ordinary exit, errors, Ctrl-C and SIGTERM.
- Worker details distinguish CPU/RAM/GPU reservations from measured whole-host
  CPU and memory. GPU readings are labeled as registration-time observations.

### Configuration and bounded transfers

- Store non-secret preferences in `settings.json`: transfer budgets, timeouts,
  default job resources, worker settings and monitoring refresh frequency.
  Explicit CLI flags override environment variables, then settings and defaults.
  Existing private connection profiles remain separate and valid.
- Configure project and artifact byte limits, file counts, traversal budgets,
  encoded HTTP body limits and transfer deadlines using human-readable sizes.
  `runspk config --remote` shows effective local and scheduler limits.
- Discover peer capabilities before submission. Project jobs preserve their
  admitted limits, and scheduling checks that a worker supports the complete
  input/output envelope. Legacy peers retain the original default limits.
- Bound concurrent bundle transfers and slow response buffering. Worker journal
  replay loads one entry at a time, and artifact collection is serialized per
  worker. Output envelopes that exceed their encoded budget report an actionable
  error and retain worker files for diagnosis.
- Verify output fingerprints before extraction and preserve the no-overwrite
  rule when retrieving results.

### Job workflows and worker selection

- Give jobs a `--name`, add repeatable `--tag` values, and search/filter history.
- Require worker capabilities with `--require dataset=v1`; advertise them with
  worker `--label dataset=v1`. All scheduling policies enforce these requirements,
  and queue explanations identify incompatible workers.
- Use `runspk repeat JOB_ID --key NEW_KEY` to submit a new execution from a
  completed job's immutable specification and input snapshot.
- Inspect declared artifacts with `runspk fetch JOB_ID --list`, or retrieve
  selected paths with `--path results/summary.json --into new-results`.

### Logs, storage and reliability

- Add bounded incremental log reads with independent UTF-8 byte cursors, tail
  requests and reset/truncation metadata. Text follow/run uses these reads when
  supported; existing ordinary JSON log and run-event formats remain compatible.
- Send changed live log snapshots at most once per second while continuing
  frequent lease reports. Completion includes the final retained snapshot.
- Inspect snapshot, artifact, log, receipt and database/WAL allocation with
  `runspk storage`. Local `backup` now follows the selected connection profile
  when no explicit database path is supplied.
- Expand regression coverage for interrupted/corrupt transfers, slow readers,
  mixed-version peers, restart recovery, reservation safety, terminal state,
  configuration precedence and release installation.

### Upgrade notes

```bash
cargo install runspk --version 0.1.0-beta.2 --locked
runspk doctor
runspk monitor
```

- Rust source builds now require **1.89 or newer**. The Linux x86_64 archive
  includes all four binaries and a checksum; it requires no Rust installation.
- Stop workers gracefully, back up the scheduler database, replace all four
  companion binaries together, and restart using the same profiles and state
  directories. Preserve worker journals and unresolved execution records.
- The database schema remains version 1; optional fields use compatible defaults
  in existing JSON records. No table rebuild or connection-profile migration is
  needed. This does not promise downgrade compatibility.
- Upgrade the scheduler before using new labels, names or tags. New clients
  reject these features if an older scheduler would ignore them. Keep the
  original worker transfer settings until active executions have reconciled.

### Operational limits

- Transfers remain base64 JSON bundles stored in SQLite. Defaults remain
  **32 MiB decoded and 2,048 files**; the decoded safety ceiling is **64 MiB**.
  Raising a limit increases memory needs. There is no byte-range resume; use
  worker-provisioned storage for large datasets and checkpoints.
- Log capture retains the **first 1 MiB per stream**. A tail shows the end of that
  retained prefix, not output discarded after the cap. Automated retention and
  per-job memory measurements remain unavailable.
- The scheduler admits one bundle transfer at a time; concurrent transfers can
  return HTTP 503. Worker completion replay retries without authorizing another
  execution or releasing an uncertain reservation.

See [configuration](docs/CONFIGURATION.md), [monitoring](docs/MONITORING.md),
[deployment](docs/DEPLOYMENT.md) and the [failure model](docs/FAILURE-MODEL.md).

## 0.1.0-beta.1 — 2026-10-21

Initial public beta with a durable SQLite scheduler, pull-based workers, CPU/RAM
and GPU reservations, immutable project snapshots and output artifacts, bounded
logs, worker recovery journals, and four Linux command-line binaries.
