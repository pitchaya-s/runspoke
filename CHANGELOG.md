# Changelog

## 0.1.0-beta.2 — Unreleased

- Add `runspk monitor`: interactive jobs and workers, resource reservations,
  measured host CPU/RAM, live logs, attempts, queue explanations, filtering,
  reconnect handling and confirmed cancellation.
- Add shared non-secret settings with CLI/environment overrides, job and worker
  defaults, monitoring preferences, and `runspk config --remote` inspection.
- Make project/artifact sizes, file counts, traversal budgets, HTTP body limits
  and transfer timeouts configurable. Negotiate scheduler/worker capabilities
  and preserve the admitted limits with immutable project jobs.
- Bound concurrent bundle transfers and slow response buffering. Replay worker
  journals one entry at a time and serialize artifact collection per worker.
- Add worker labels and job requirements, with matching in every scheduling
  policy and scheduler explanations for incompatible workers.
- Add job names/tags, history search, snapshot-preserving `repeat`, artifact
  listing and selective retrieval, and output fingerprint verification.
- Add bounded incremental log retrieval with UTF-8 byte cursors. Reduce repeated
  live log snapshots while continuing lease reports.
- Add storage usage reporting and profile-aware local database backups.
- Expand regression coverage for transfer boundaries, interruptions, mixed
  versions, restart recovery, reservation safety, monitoring and packaging.

### Upgrading and limits

- Source builds require Rust 1.89 or newer. Install all four companion binaries
  together; preserve connection profiles, worker journals and scheduler state.
  Back up the database before upgrading.
- Existing private connection profiles remain valid. Shared preferences belong
  in a separate settings file and contain no administrative token.
- Bundles still use base64 JSON and SQLite blobs. Defaults remain 32 MiB decoded
  and 2,048 files; the decoded safety ceiling is 64 MiB. Transfers do not support
  byte-range resume. Use worker-provisioned storage for larger datasets.
- Workers must support a project's full admitted transfer envelope. Legacy
  peers use the old defaults; unsupported metadata features fail explicitly.
- Logs retain the first 1 MiB per stream. A tail is the end of that retained
  prefix; automated retention and per-job memory measurements remain unavailable.

See [configuration](docs/CONFIGURATION.md), [monitoring](docs/MONITORING.md) and
[deployment](docs/DEPLOYMENT.md) for examples and operational details.

## 0.1.0-beta.1

Initial public beta with a durable SQLite scheduler, pull-based workers, CPU/RAM
and GPU reservations, immutable project snapshots and output artifacts, bounded
logs, worker recovery journals, and four Linux command-line binaries.
