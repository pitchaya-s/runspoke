# Larger projects, datasets and environments

Use small immutable code snapshots with explicitly provisioned worker paths.
Only code/configuration and declared small outputs pass through runspk. Dataset,
environment and build-cache paths are **absolute paths on every eligible
worker**, not paths on the submitting laptop. Keep dataset versions and Python
environments immutable while jobs use them; runspk does not fingerprint external
files. Workers are trusted and native jobs retain the account's filesystem and
network access. There are no implicit mounts, downloads or dependency installs.

## Python experiments

On each worker, provision a versioned CSV dataset and, if needed, a versioned
virtual environment. For example, after reviewing a locked requirements file:

```bash
# Operator setup on each worker, outside runspk. Requires python3-venv.
python3 -m venv "$HOME/runspk-envs/experiment-v1"
"$HOME/runspk-envs/experiment-v1/bin/python" -m pip install --require-hashes -r requirements.lock
```

Use your environment's absolute interpreter path in the job's command. Do not
copy `.venv`, package caches, credentials or large data into the snapshot. The
shipped standard-library example needs no pip packages and accepts an external
CSV with a `value` column:

```bash
# On the client; /srv/datasets/measurements-v1.csv must exist on the workers.
runspk run --project examples/python-project --output results \
  --cpu 1 --mem 128M --timeout 600 --trusted -- \
  /usr/bin/python3 experiment.py --data /srv/datasets/measurements-v1.csv
```

The example streams its data with constant working memory and writes a small
`results/summary.json`. It is tested with worker data larger than the 32 MiB
snapshot limit and a 64 MiB native memory reservation. For your ML application,
request CPU/RAM/GPU explicitly, and set thread counts in your own command when
libraries need them. `--mem` is an address-space limit for native execution;
libraries that reserve large virtual mappings may need substantially more than
their resident RAM. GPU memory is a placement filter, not an enforced limit.

A reusable specification can contain the worker paths without transferring data:

```json
{
  "project": ".",
  "exclude": ["data", "results"],
  "command": ["/usr/bin/python3", "experiment.py", "--data", "/srv/datasets/measurements-v1.csv"],
  "resources": {"cpu": 1, "mem_mib": 128},
  "timeout_secs": 600,
  "trusted": true,
  "outputs": ["results"]
}
```

Save it next to `experiment.py`, then use `runspk submit --spec FILE`. The `project`
path is relative to the specification file. Review `run --spec FILE --dry-run`
before submission. A missing interpreter/data path is a workload failure: use
`status`, `logs` and `attempts`, provision the missing path, then resubmit. There
is no worker-label selection; provision required paths on all eligible workers,
or use a dedicated scheduler pool. GPU identity affinity only selects GPUs.

## Rust builds

Provision Cargo, the required Rust toolchain, cached locked dependencies and a
writable cache directory on every eligible worker. Exclude `target` (already a
default), datasets and irrelevant trees. Keep `Cargo.lock` in the snapshot. For a
small project whose binary is `my-tool`, use an explicit shell for the build and
copy the selected output back into the attempt workspace:

```bash
runspk run --project . --output results --cpu 2 --mem 2G --timeout 1800 --trusted -- \
  /bin/sh -c 'set -eu; cargo build --locked --offline --release --target-dir /srv/runspk-cache/my-tool; mkdir results; cp /srv/runspk-cache/my-tool/release/my-tool results/'
```

The worker's service PATH must contain Cargo (or use its absolute path). Build
tools, caches and toolchain versions are externally managed. Concurrent Cargo
builds serialize on the shared target directory lock; use separate cache paths
for independent projects. Use a fresh results directory and an executable under
32 MiB, or keep a large result on the worker and retrieve it explicitly over SSH.
If sources exceed the snapshot bounds, provision an exact revision on workers
and use `--cwd /absolute/checkout -- cargo build --locked --offline`; this mode
does not provide immutable code capture or `fetch`.

## Results and reproducibility

`fetch JOB_ID --into NEW_DIRECTORY` retrieves only declared bounded outputs.
Large checkpoints, datasets and entire build trees stay on worker storage and
need an operator-managed retention/SSH copy policy. Put a small manifest in
`results` recording the dataset version/checksum, environment lock hash, code
revision and external checkpoint path. Do not put credentials in job arguments,
specifications or results. All API credentials grant administrative access.

Restricted containers remain an alternative for preloaded, reproducible images:
they have no host mounts or network and do not support project snapshots or
`fetch`. This release does not add a container volume/secret interface or
unbounded blob storage.
