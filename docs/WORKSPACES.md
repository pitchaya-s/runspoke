# Projects and outputs

`--project DIRECTORY` submits a small immutable snapshot alongside a native job. The client reads files once, applies exclusions and computes a SHA-256 fingerprint. The scheduler commits the snapshot and job in the same SQLite transaction. Eligible workers download that fingerprint into a new attempt-local workspace before acknowledging start. There is no requirement for matching client and worker paths. Editing the original files after submission does not change a queued job. Keep project files stable while capture runs; this is not a filesystem-level atomic snapshot.

```bash
runspk run --project . --exclude private --output results \
    --trusted --cpu 1 --mem 512M --timeout 600 -- python3 experiment.py
runspk fetch JOB_ID --into new-results-directory
```

The command's working directory is the workspace root. `--cwd` and `--project` are mutually exclusive. Workers must already have the interpreter, tools and libraries the command needs. Use an explicit setup script if needed; no dependency installer or environment manager runs implicitly. Native code runs with the worker account's filesystem/network privileges. Container workloads keep their preloaded-image model and cannot use project staging.

## Scope and exclusions

Each input or output snapshot permits 32 MiB of decoded contents and 2048 regular files. Traversal is bounded to 4096 entries and 32 directory levels. Paths must be relative UTF-8 without dot/dot-dot components, repeated separators or NUL. Symbolic links, hard links and special files are rejected, including when accessing an explicitly declared nested output. File/directory collisions and duplicate paths are rejected. Empty directories, timestamps, ownership, ACLs and arbitrary permission bits are not preserved; files retain only whether they are executable. Workers create private directories and files.

Default input exclusions include VCS metadata, `.runspk`, `.tools`, `.cache`, virtual environments, build outputs, `node_modules`, Python caches, `.env`/`.env.*`, `.ssh`, `.aws`, `.kube`, `.netrc` and common SSH private-key names. A file recognizable as an runspk connection profile is rejected so an invitation inside a project does not silently upload its token. This is a convenience guard, not a general secret scanner. Inspect `--dry-run` and exclude all other secrets explicitly.

A `.runspkignore` file adds literal relative prefixes, one per line. Blank lines and `#` comments are ignored. `--exclude PREFIX` adds the same rule. There are no wildcards, negation or Gitignore semantics. A 64 KiB limit bounds this file. Excluded directories are not traversed. `.runspkignore` itself is included unless explicitly excluded. Output capture uses the declared paths rather than input exclusions.

## Reusable specifications

JSON specifications reuse the wire job schema, with two local additions: `project` (relative to the specification file) and `exclude`. They do not embed connection tokens. `--spec` cannot be mixed with command/resource/project flags; edit the file or use explicit flags instead.

```json
{
  "command": ["python3", "experiment.py"],
  "resources": {"cpu": 1, "mem_mib": 128},
  "timeout_secs": 30,
  "trusted": true,
  "project": ".",
  "exclude": ["results"],
  "outputs": ["results"]
}
```

See the runnable [Python example](../examples/python-project/). `runspk run --spec FILE --dry-run` works before initialization and prints paths, byte count, resource requests and input fingerprint without uploading contents. Submission keys include the fingerprint: changed inputs with the same key are rejected rather than silently reusing an old job.

## Output delivery and recovery

Declare at most 64 non-overlapping output files or directory prefixes. All must exist at stopped completion, including failures and cancellation. Files are captured only after process-group termination and backend cleanup have been confirmed. A failed command can produce diagnostic artifacts. Missing, oversized or unsafe outputs add an actionable job error; an otherwise successful command becomes exit 74. The worker retains that workspace for manual inspection and cleanup rather than deleting unavailable results. Retained paths appear in the job error.

Successful capture is fsynced with the completion journal, uploaded to immutable per-attempt storage, and acknowledged before publishing completion. Interrupted uploads are replayed. A slow transfer runs outside the worker's heartbeat/polling loop. Output delivery failures conservatively leave the stopped attempt unresolved and its reservation held until replay succeeds or an operator resolves it after confirmed fencing. There is no atomic transaction between application side effects and scheduler results.

`fetch` is available after a finished execution receipt and only creates a new destination. It does not merge or overwrite. A failed extraction can leave a partial new directory; inspect/remove it before retrying. `--attempt UUID` retrieves an earlier attempt's immutable outputs. Copy the full `id` from `runspk attempts JOB_ID`; the attempt number is not its identifier. Omit `--attempt` to retrieve the latest result. Input snapshots are shared by fingerprint, and remain while any job references them. Explicit pruning removes eligible jobs' artifact blobs and unreferenced inputs, while preserving submission keys, execution receipts and unresolved attempts. Keyed jobs are retained.

## Protocol and storage

Authenticated endpoints are `POST /jobs/project`, `GET /workspaces/{id}`, `POST /worker/artifacts`, and `GET /jobs/{id}/artifacts?attempt=ID`. Bundles contain relative paths, base64 contents and executable flags; they are not archive extractors. Transfers have a 120-second client timeout; ordinary control requests keep their one-second timeout. Transfer routes accept request bodies up to 64 MiB for base64 and path encoding, while other API requests remain limited to 16 MiB. The scheduler handles one bundle transfer at a time to bound memory use. `/info` exposes protocol capabilities and the decoded bundle limit with the same bearer authentication as other endpoints.

Workspace and artifact tables persist bounded bundles, with an indexed generated workspace reference on jobs. Project jobs require workers advertising staging capability. Install all binaries together. Small JSON bundles trade some memory, encoding and WAL write overhead for an understandable bounded protocol. For larger projects/datasets, follow the tested [worker-provisioned data and environment workflow](LARGE-PROJECTS.md); streaming/object storage is deliberately outside this release.
