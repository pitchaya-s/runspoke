# Configuration and transfer limits

Connection profiles (`runspk init`, `invite`, `join`, `--config FILE` or
`RUNSPK_CONFIG`) remain private, mode-0600 files containing the administrative
token. Choose separate profile paths for named connections, for example
`runspk --config ~/.config/runspk/lab.json workers`. Invitations grant full
administrative access. Never put a profile inside shared project settings.

Non-secret preferences live in `$XDG_CONFIG_HOME/runspk/settings.json`, or
`~/.config/runspk/settings.json` when XDG_CONFIG_HOME is unset. A missing default
file uses defaults; an explicitly selected missing file is an error. Use
`--settings FILE` or `RUNSPK_SETTINGS` to select another file. Unknown fields,
malformed JSON, symlinks, special files and files exceeding 64 KiB are rejected.
Settings files contain no connection credentials and need no migration from
older connection profiles. A complete example is [settings.json](../examples/settings.json).

```json
{
  "transfers": {
    "project_bytes": "48MiB",
    "artifact_bytes": "64MiB",
    "max_files": 4096,
    "max_entries": 8192,
    "max_depth": 32,
    "http_body_bytes": "96MiB",
    "timeout_secs": 180
  },
  "monitor_refresh_secs": 2,
  "job_defaults": {"cpu": 1, "mem_mib": 512, "timeout_secs": 600},
  "worker": {"cpu": 2, "mem_mib": 2048, "labels": {"dataset": "measurements-v1"}}
}
```

The scheduler, CLI and worker load the same settings format. Use different files
when machines have different budgets. `runspk start` and `join` forward resolved
transfer settings to their companion binaries. Backend consent remains explicit:
worker preferences do not implicitly enable native execution or a container runtime.

Precedence is **explicit CLI flags > RUNSPK_* environment > settings file >
built-in defaults**. `--spec` is a complete job specification and retains its
explicit resources. Ordinary submissions inherit missing CPU, RAM and timeout
values from `job_defaults`. Worker/start/join CPU and memory flags override
`worker` defaults; explicitly supplied label keys override settings labels.

| Setting | CLI flag | Environment | Default | Safety ceiling |
| --- | --- | --- | --- | --- |
| Project decoded bytes | `--project-limit` | `RUNSPK_PROJECT_LIMIT` | 32 MiB | 64 MiB |
| Artifact decoded bytes | `--artifact-limit` | `RUNSPK_ARTIFACT_LIMIT` | 32 MiB | 64 MiB |
| Files per bundle | `--max-files` | `RUNSPK_MAX_FILES` | 2,048 | 16,384 |
| Traversed/represented entries | `--max-entries` | `RUNSPK_MAX_ENTRIES` | 4,096 | 65,536 |
| Path depth | `--max-depth` | `RUNSPK_MAX_DEPTH` | 32 | 32 |
| Encoded HTTP body | `--http-body-limit` | `RUNSPK_HTTP_BODY_LIMIT` | 64 MiB | 128 MiB |
| Transfer timeout (seconds) | `--transfer-timeout` | `RUNSPK_TRANSFER_TIMEOUT` | 120 | 3,600 |

Sizes accept integer bytes or B/KiB/MiB/GiB and decimal KB/MB/GB; K/M/G are
binary shorthand. Fractions, zero, unknown units and overflow are errors.
`max_entries` must be at least `max_files`. CPU and memory budgets retain their
existing resource validation; `RUNSPK_CPU` and `RUNSPK_MEM_MIB` also apply to
start/join and standalone workers. Labels accept repeated `--label KEY=VALUE`
or comma-separated `RUNSPK_LABELS`.

Inspect without exposing the token:

```bash
runspk --settings lab-settings.json config
runspk --settings lab-settings.json config --remote
runspk --settings lab-settings.json run --project . --trusted --dry-run -- python3 main.py
runspk-scheduler --project-limit 48MiB --artifact-limit 64MiB --http-body-limit 96MiB
runspk-worker --id lab --allow-native --project-limit 48MiB --artifact-limit 64MiB --http-body-limit 96MiB
```

`config` validates and prints effective local values. `config --remote` adds
scheduler `/info` capabilities, scheduler limits and the minimum of the local
and scheduler limits. Dry runs use local settings and do not contact the
scheduler; actual submission discovers and applies the smaller remote limits
before reading inputs. The HTTP budget also covers base64 (about 4/3 expansion),
JSON paths, and job metadata. A decoded bundle can fit its byte limit but exceed
a smaller encoded HTTP budget; the client rejects it before sending. Output capture also checks encoded envelope size before journaling a result: an oversized encoded output reports an actionable collection error and retains the workspace, instead of leaving an impossible upload retrying forever.

New project jobs freeze negotiated limits in their immutable specification.
Placement requires a worker that supports the entire envelope, including the
maximum output size, even if the actual inputs are small. Use the same transfer
settings on eligible workers, or intentionally lower submission limits. A legacy
worker advertises the old defaults; jobs above those defaults cannot be assigned
to it. A legacy scheduler's missing limit fields also mean the old defaults.
Clients refuse label/name/tag features that an older scheduler would silently
ignore. Base capabilities and ordinary commands remain compatible.

Lowered scheduler size/traversal defaults affect new submissions. Matching keyed retries keep the original admitted specification; changing explicit requirements with the same key still fails. HTTP request body caps and transfer deadlines apply to subsequent requests, so a retry above a newly lowered body cap can instead submit its original specification through `/jobs` without reuploading the snapshot. Previously admitted inputs
and completion artifacts are validated against their original limits and the
fixed safety ceilings; artifact upload routes retain the hard encoded ceiling
for durable completion replay. Keep the original worker settings while its
executions are active. A smaller client retrieval budget reports an error rather
than allocating an unexpectedly large response; raise its explicit limit to fetch.
Control-plane requests retain their one-second client timeout. Transfer timeouts
do not change execution leases or authorize reservation release.

## Why bundles remain bounded

The wire format is still base64 JSON and SQLite stores immutable bundle blobs.
Validation, fingerprinting and extraction decode through small buffers, but the
encoded bundle, JSON body, journal and SQLite serialization can still coexist in
memory. Raising a limit does not make transfers constant-memory. One scheduler
bundle transfer is admitted at a time, including response delivery and cancelled
requests' database work. Concurrent transfers receive HTTP 503 and should retry;
worker completion replay already does so. Workers enumerate pending journal identities and load one entry at a time for replay, stream journal JSON writes, and serialize stopped artifact capture/upload per worker instead of holding every completion blob in memory. Stopped jobs waiting for delivery conservatively retain their reservations. Slow request bodies and response production have a bounded
processing deadline. Outgoing bodies use a one-frame channel and copied 64 KiB frames, so slow readers cannot pin multiple complete JSON payloads in HTTP connection buffers. Use reasonable job keys when retrying a submission with an
unknown outcome.

Transfers commit atomically or leave no submitted job; paths, base64 and input
fingerprints are checked before launch. Completion replay resends an entire
immutable bundle after interruption. Input bundles deduplicate by fingerprint;
there is no byte-range resume or cross-artifact deduplication. Output downloads
include a fingerprint that new clients check before extraction. Extraction
retains the no-overwrite rule and can leave a partial **new** directory after a
filesystem failure.

GiB datasets/checkpoints still belong in versioned worker-provisioned storage;
see [LARGE-PROJECTS](LARGE-PROJECTS.md). Beyond these ceilings, a future protocol
needs bounded binary chunks, a checksummed manifest, temporary disk staging,
atomic commit, quota accounting and orphan cleanup. That change must preserve
receipts, completion replay, immutable snapshots and mixed-version capability
gating. Merely raising a JSON constant would not provide those guarantees.
