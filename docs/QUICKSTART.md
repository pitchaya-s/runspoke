# Your machines. One compute queue.

runspk queues trusted batch commands on your own Linux machines, reserves CPU/RAM/GPU resources, and keeps jobs and output available after your client disconnects. Start with one machine; attach workers when you need them. No cluster platform or paid service is required.

## Install

Use an unprivileged Linux account. Ubuntu 22.04/24.04 are CI targets; Ubuntu 24.04 x86_64 is locally tested. Native execution requires GNU coreutils (`timeout`). The example needs Python 3. A current stable Rust toolchain and a C compiler are needed only for building from source.

Download one Linux x86_64 `.tar.gz` archive and its matching `.sha256` file from [GitHub Releases](https://github.com/pitchaya-s/runspoke/releases) into a new directory. Set `archive` below to the downloaded filename, replacing `VERSION` with its version number.

```bash
archive="runspoke-VERSION-linux-x86_64.tar.gz"
sha256sum --check --strict "$archive.sha256"
mkdir runspoke-release
tar -xzf "$archive" -C runspoke-release
runspoke-release/install.sh "$archive" "$HOME/.local/bin"
export PATH="$HOME/.local/bin:$PATH"
cd runspoke-release
runspk --version
```

All four companions install together. The installer verifies manifest checksums, Linux x86_64/glibc compatibility and matching versions before modifying binaries. The PATH command above applies only to the current shell; choose your own persistent shell setup explicitly. Installation preserves profiles, journals and scheduler data. Checksums detect corruption; obtain both files from the trusted repository. Other architectures and libc implementations are unsupported.

The extracted archive includes this documentation and `examples/python-project`, so all following relative paths work from `runspk-release`. Runtime dependencies are GNU coreutils, glibc and standard Linux utilities; Python 3 is required for the example. Users need no Rust toolchain. For source development instead:

```bash
cargo install --locked --path crates/runspk --bins
```

Source users must put Cargo's bin directory on PATH.

## First project

```bash
runspk init
runspk start --cpu 1 --mem 256M
```

`init` creates a random token in a mode-0600 JSON profile under `$XDG_CONFIG_HOME/runspk` (default `~/.config/runspk`) and private state under `$XDG_STATE_HOME/runspk` (default `~/.local/state/runspk`). It does not print the token. `start` runs the scheduler and a trusted local worker in the foreground, bound to loopback. Keep this terminal open.

In another terminal, from the extracted release directory (or source checkout):

```bash
# Preview names, size, resources and immutable input fingerprint. Nothing uploads.
runspk run --spec examples/python-project/runspk.json --dry-run

# Run a real standard-library Python experiment and stream its output.
runspk run --spec examples/python-project/runspk.json

# Submit detached work, wait, and retrieve declared results.
id=$(runspk submit --spec examples/python-project/runspk.json)
runspk wait "$id"
runspk fetch "$id" --into experiment-results
cat experiment-results/results/summary.json
```

The sample reads CSV measurements and computes a mean, standard deviation and sum of squares. Expected mean: 6.0. `fetch` requires a new destination and never overwrites existing files. `run` returns the workload's exit status. `submit` returns a durable ID; `wait --timeout 60` limits waiting, not execution. Ctrl+C while waiting leaves the job submitted.

For your own small project, using resources that fit the quickstart worker:

```bash
runspk run --project . --output results --cpu 1 --mem 128M --timeout 3600 \
    --trusted -- python3 experiment.py
```

For larger jobs, stop the local scheduler gracefully and restart with an explicit larger worker budget (for example `runspk start --cpu 2 --mem 2G`), within physical capacity and leaving room for the OS. `runspk workers` shows the available budget; `runspk explain JOB_ID` explains a queued request that cannot fit.

Project snapshots contain at most 32 MiB and 2048 regular files. Common VCS, virtualenv/build directories and common credential names are excluded. Add literal relative prefixes to `.runspkignore` or repeat `--exclude`; this is not a Gitignore/glob parser. Recognizable runspk connection files are rejected too. These guards are not a general secret scanner; review `--dry-run` and exclude other credentials explicitly before uploading. Symbolic/hard links, special files and traversal paths are rejected. Empty directories are not preserved. Put larger datasets on workers explicitly or use prepackaged container images. Nothing automatically installs Python packages or changes the worker environment. Run a known interpreter or an explicit setup script when needed.

All declared outputs must exist when the job stops. Output bundles have the same 32 MiB limit. If capture fails, the job reports the error and retains its workspace on the worker for inspection. The captured logs retain the first 1 MiB per stream. Native jobs run as the worker account and require trust; staging is not a sandbox. Container jobs keep their existing prepackaged-image boundary and do not use project staging.

## Add another Linux machine

For a scheduler server, supervised worker/tunnel and laptop client, follow the
[multi-machine setup](MULTI-MACHINE.md). The foreground steps below are a short
connection test. Services can persist after logout when host policy allows it.

Install the same runspk version on the worker. On the scheduler machine:

```bash
runspk invite --out worker-connection.json
scp worker-connection.json worker-host:~/worker-connection.json
```

The invitation is a mode-0600 file containing the administrative token. Transfer it through SSH and keep it private. It is not a scoped or expiring worker credential. When using an existing HTTPS proxy, export that URL with `runspk --url https://scheduler.example invite --out worker-connection.json`.

On the worker, keep an SSH tunnel open if using the default loopback connection:

```bash
ssh -o ExitOnForwardFailure=yes -N \
    -L 127.0.0.1:8765:127.0.0.1:8765 scheduler-host
```

In another worker terminal:

```bash
runspk join ~/worker-connection.json --id workstation --cpu 2 --mem 2G --allow-native
```

The invitation imports into private local configuration and the worker starts in the foreground. It does not change SSH settings, services, drivers or firewalls. Use a distinct worker ID and preserve its state directory across restarts. One worker per host is simplest; partition physical CPU/memory budgets explicitly if running several on one machine.

For an already reserved physical GPU, append `--gpu-ids GPU-UUID` to `join`, then request `--gpu 1` on submission. GPU memory is a capacity filter, not isolation. `--simulate-gpus 1` tests allocation without hardware. No GPU is advertised by default. Existing authenticated workers must be upgraded to support staged projects; old workers remain eligible for ordinary commands.

Back on the client:

```bash
runspk workers
runspk run --project . --output results --gpu 1 --mem 2G --trusted -- python3 train.py
```

## Understand and recover

```bash
runspk doctor
runspk list --limit 20
runspk queue
runspk status JOB_ID
runspk explain JOB_ID
runspk logs JOB_ID --follow
runspk attempts JOB_ID
runspk fetch JOB_ID --attempt ATTEMPT_UUID --into diagnostic-results
```

For an older attempt, copy its full UUID from `runspk attempts JOB_ID`; omit `--attempt` for the latest result.

Terminal output shows readable state, resource reservations and worker health. Redirected inspection output stays JSON for compatibility; `--json` forces JSON and `--human` forces terminal formatting. `run --json` emits submitted/logs/finished events. `runspk completions bash` (also zsh/fish/powershell/elvish) prints shell completion without modifying your shell.

Missing dependencies are workload failures: inspect the error, logs and exit status, fix the worker environment and resubmit. A failed job can still have retrievable diagnostic outputs. Jobs marked `uncertain` retain their resources. Restart the owning worker with its original state to fence and reconcile; never assert stopped execution until it has actually been fenced. See [recovery](FAILURE-MODEL.md).

## Keep it running and upgrade

Client terminals may disconnect. Scheduler, worker and any SSH tunnel must remain supervised. After reviewing the service settings, the optional [user unit](../examples/runspk.service) can be installed explicitly; initialization never installs it. Existing [deployment templates](DEPLOYMENT.md) support separate workers and environment-based configuration too. Foreground start/join is intentional and visible.

Before upgrading: stop workers gracefully, make an online backup with `runspk backup --db STATE/scheduler.sqlite --out NEW_BACKUP.sqlite`, stop the scheduler, install all companion binaries together, then restart using the same profiles and state directories. Preserve worker journals independently; incompatible database formats are refused without changing their contents. There is no downgrade guarantee. Pruning is explicit and preserves keyed jobs, completion receipts and unresolved work. See [storage workflows](WORKFLOWS.md).
