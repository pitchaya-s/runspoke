# Daily workflows

For datasets/environments larger than the transfer limit and reusable Rust build
caches, follow [larger project workflows](LARGE-PROJECTS.md). Code, data and output
boundaries are explicit; no environment installer runs implicitly.

Use `runspk init` for a private connection profile, or the same RUNSPK_TOKEN and RUNSPK_URL as your scheduler. The command after `--` is an argument array; shell constructs require an explicit shell. Without `--project`, paths are resolved on the chosen worker and must already exist there. For small local projects use `--project . --output results`; [project snapshots](WORKSPACES.md) remove the shared-path requirement and `fetch` retrieves outputs.

```bash
# A bounded development build on a worker with this checkout and Cargo installed.
runspk submit --trusted --cpu 2 --mem 2G --timeout 600 --cwd /srv/project -- cargo build

# A Python experiment, safely named for deduplication if submission is retried.
runspk submit --trusted --cpu 1 --mem 512M --timeout 120 --key experiment-001 -- \
    python3 /srv/experiments/run.py

# A high-priority task; effective only under priority or easy policies.
runspk submit --trusted --priority 50 --mem 64M --timeout 10 -- /bin/echo urgent

# Explicit shell with pipe/redirection. Only use trusted scripts natively.
runspk submit --trusted --mem 128M --timeout 60 -- /bin/sh -c 'make report > report.txt'

runspk queue
runspk workers
runspk status JOB_ID
runspk logs JOB_ID --follow
runspk attempts JOB_ID
runspk cancel JOB_ID
runspk stats --json
```

`--retries 2` permits two additional executions after acknowledged stopped failures. Use retries only if outputs and external effects tolerate repetition. A failed CLI request exits 1; syntax errors exit 2. Submission/status/cancel success exits 0 even if the job later fails. Use `wait` or `run` for shell automation. A cancellation racing with completion preserves the first valid terminal outcome.

For ordinary foreground batch work:

```bash
runspk run --trusted --cpu 2 --mem 2G --timeout 3600 -- \
    python3 /srv/experiments/run.py
echo "$?"  # workload exit status
```

The submitted ID goes to stderr; stdout/stderr stream the captured log prefixes. There is no interactive stdin or terminal allocation. `--timeout` limits the workload; optional `--wait-timeout` limits only how long this CLI waits. Ctrl+C/SIGTERM exits 130 and leaves the job submitted. Request/connect timeouts while waiting are retried; API errors such as a missing job or invalid token fail immediately. Logs still have the 1 MiB per-stream retention limit. Prefer file outputs for large experiments.

For disconnect/reconnect workflows:

```bash
id=$(runspk submit --trusted --cpu 2 --mem 2G --timeout 604800 -- \
    python3 /srv/experiments/long_run.py)
# Disconnect; the scheduler/worker services keep running.
runspk status "$id"
runspk logs "$id" --follow
runspk wait "$id" --timeout 3600
```

`wait` prints the terminal job as a readable status (JSON in pipes or with `--json`) and returns its exit status. Successful work exits 0, cancellation exits 130, a wait timeout exits 124, and signal failures use 128 plus the signal number. A failed job without a usable exit code exits 1. `run --json` emits newline-delimited objects with `event` values `submitted`, `logs`, `finished`; `logs` contains a full snapshot, identified by attempt ID, and `job` contains the submission/terminal record. Retry attempts reset stream offsets. Existing `submit --json` and inspection JSON shapes are unchanged.

Inspection and pagination:

```bash
runspk list --state failed --limit 100
runspk list --worker laptop-a --after 1200 --limit 100
runspk queue --limit 100
runspk explain JOB_ID
```

In pipes or with `--json`, each page remains a JSON array in ascending `seq` order. Pass the last record's `seq` as `--after` for the next page; cursors remain valid if earlier history is pruned. Limits are 1–1000. Omitting `--limit` preserves the original unlimited response. Queue filtering happens on the server before pagination. Explanations show backend/capacity/GPU incompatibility, reserved resources, offline/draining workers, retry backoff, or a current fit subject to policy order. A fit is not a dispatch guarantee.

Storage operations run explicitly:

```bash
# Run locally on the scheduler host; this does not need an API token.
runspk backup --db /srv/runspk/state.sqlite --out /srv/backups/runspk-new.sqlite

# Choose a past Unix timestamp in milliseconds; preview before deleting.
runspk prune --before 1792108800000 --limit 100 --dry-run
runspk prune --before 1792108800000 --limit 100
```

Pruning returns `{"jobs":N,"dry_run":BOOL}` and removes at most the requested count of terminal, unkeyed jobs and their captured logs, artifact blobs and unreferenced project snapshots. It skips active/uncertain jobs and any job with an unfinished execution receipt. Submission-key records and finished execution receipts remain; this preserves deduplication and completion replay, so metadata is not capped absolutely. Pruning releases reusable SQLite pages, not necessarily bytes from the database file. Online backup preserves a consistent image; no automatic VACUUM blocks the control plane. Upgrade all workers before pruning because prior workers do not recognize the new HTTP 410 acknowledgement for pruned results.

`stats.scheduling` contains process-lifetime planning counts/timings, placements, starts, dispatch totals/maxima, total queue wait at each attempt start, and retries. Retried starts include elapsed backoff in queue wait. `stats.database` counts operations, total/max operation nanoseconds, and mutex-wait nanoseconds; operations include maintenance, and its response precedes accounting for the stats request itself. Worker health includes last-seen time, online/draining/offline state, capacity and reserved resources. Counters reset with the scheduler; durable per-attempt timestamps remain available through `attempts`.

For isolated work, preload an image on each eligible worker, enable its runtime, then:

```bash
runspk submit --image alpine:3.20 --cpu 1 --mem 128M --timeout 15 -- /bin/echo isolated
```

Container images run with no network or host volumes and a read-only root. Build inputs into the image; captured stdout/stderr is the output channel. Native trusted workflows support regular filesystem outputs under the worker account.

GPU examples and physical-resource consent are in [GPU.md](GPU.md). Test allocation without using hardware by starting a worker with `--simulate-gpus 2` and submitting `--gpu 1 --trusted -- /bin/echo gpu-slot`.

An uncertain job still owns its reservation. Restart its worker with the same data directory to reconcile. If that machine is gone, fence it externally and only then run `runspk resolve JOB_ID --assume-stopped`. The assertion can allow a configured retry; it must reflect actual stopped execution.
