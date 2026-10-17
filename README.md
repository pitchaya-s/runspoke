# Runspoke

**Your machines. One compute queue.**

Runspoke lets you submit a job from your laptop and run it on a Linux machine you trust. It keeps the queue and job state on your scheduler, streams logs, records results, and lets work continue after your client disconnects.

Start with one Linux computer. Add workers when you need more capacity. Runspoke is a small Rust program with a scheduler, pull workers, and a CLI; it does not need a hosted service, message broker, or external database.

## Install

If you have Rust 1.85 or newer, install the published beta from [crates.io](https://crates.io/crates/runspk):

```bash
cargo install runspk --version 0.1.0-beta.1
```

The explicit version selects this prerelease; Cargo skips prereleases unless you request one. Rust 1.85 is the minimum for the 2024 edition used by Runspoke.

For Linux users without Rust, the prebuilt x86_64 archive includes all companion binaries. Download the archive and matching checksum from [GitHub Releases](https://github.com/pitchaya-s/runspoke/releases), then verify and install it:

```bash
sha256sum --check --strict runspoke-VERSION-linux-x86_64.tar.gz.sha256
tar -xzf runspoke-VERSION-linux-x86_64.tar.gz
./install.sh runspoke-VERSION-linux-x86_64.tar.gz "$HOME/.local/bin"
export PATH="$HOME/.local/bin:$PATH"
runspk --version
```

Replace `VERSION` with the release version. The installer checks Linux x86_64, glibc, archive contents, checksums, and companion versions before replacing binaries. It does not change your shell configuration. Ubuntu 22.04 and 24.04 are CI targets; other architectures and libc implementations are unsupported.

Native jobs require GNU coreutils `timeout`. Python 3 is needed only for the example below.

## Run your first job

In the first terminal, initialize a private local profile and start a scheduler with a local worker:

```bash
runspk init
runspk start --cpu 1 --mem 256M
```

The scheduler listens on loopback and stays in the foreground. Keep this terminal open. `init` creates a private token and state directory; it does not install services or change network settings.

In a second terminal, create a small Python project and submit it:

```bash
mkdir -p "$HOME/runspoke-demo"
cd "$HOME/runspoke-demo"
cat > experiment.py <<'PY'
from pathlib import Path

print("running on a Runspoke worker")
Path("results").mkdir(exist_ok=True)
Path("results/summary.txt").write_text("experiment complete\n")
PY

id=$(runspk submit --project . --output results --cpu 1 --mem 128M \
    --timeout 60 --trusted -- python3 experiment.py)
runspk status "$id"
runspk logs "$id" --follow
runspk wait "$id"
runspk fetch "$id" --into fetched
cat fetched/results/summary.txt
```

`submit` prints a durable job ID. You can disconnect after submission; the scheduler and worker keep running. `wait --timeout 60` limits how long the client waits, while `--timeout 60` on submission limits the job's wall time. Fetch writes into a new directory and will not overwrite existing files.

Use `runspk list`, `runspk queue`, `runspk workers`, `runspk explain JOB_ID`, and `runspk attempts JOB_ID` to inspect the queue and diagnose work. `runspk doctor --json` gives machine-readable diagnostics. Piped inspection output is JSON; `--json` and `--human` choose the format explicitly. Shell completions are printed by `runspk completions bash` (also supports zsh, fish, elvish, and PowerShell).

## Add another machine

Run `runspk invite --out worker-connection.json` on the scheduler, transfer the invitation over SSH, and run `runspk join` on the worker. The [multi-machine guide](https://github.com/pitchaya-s/runspoke/blob/main/docs/MULTI-MACHINE.md) shows the SSH tunnel, optional systemd user units, laptop setup, reconnect behavior, and worker recovery.

Invitations contain the administrative token. They grant administrative access and should be transferred privately. Keep the scheduler on loopback and use SSH forwarding, or place it behind a properly configured HTTPS endpoint. Runspoke does not discover machines, modify firewalls, or configure services automatically.

## Inputs, environments, and results

Small project snapshots transfer source files to a worker. Each input or output bundle is limited to 32 MiB and 2,048 regular files; `.runspkignore` and `--exclude` let you leave out local files. `--dry-run` previews what would be sent. Inputs are immutable for an attempt, and declared outputs are captured after execution stops.

Large datasets and installed dependencies are not copied automatically. Put datasets at an agreed path on the worker, or use an existing container image for dependencies and large inputs. The [workspace guide](https://github.com/pitchaya-s/runspoke/blob/main/docs/WORKSPACES.md) and [large-project guide](https://github.com/pitchaya-s/runspoke/blob/main/docs/LARGE-PROJECTS.md) explain both workflows. Failed jobs can still have diagnostic logs and outputs to fetch.

CPU, memory, and GPU requests reserve advertised worker capacity. Reservations do not enforce a native process memory limit. Native commands run as the worker's account and require `--trusted`; they are not sandboxed. Containers use the worker's existing Docker or Podman runtime and preloaded images.

## Recovery and upgrades

Jobs that may still be running keep their reservations until the worker proves termination or an operator applies valid fencing. After a worker restart, preserve and reuse its state directory so it can reconcile its journal. Never resolve an uncertain job until its process is known to have stopped. See the [failure model](https://github.com/pitchaya-s/runspoke/blob/main/docs/FAILURE-MODEL.md) for cancellation, retries, and recovery.

Before upgrading, stop workers gracefully, back up the scheduler database, install all four companion binaries together, and preserve scheduler profiles and worker journals. The [deployment guide](https://github.com/pitchaya-s/runspoke/blob/main/docs/DEPLOYMENT.md) covers the backup and upgrade steps. Run `runspk doctor` after reconnecting.

## What Runspoke is designed for

Runspoke is for a small pool of trusted Linux machines: development builds, experiments, and batch scripts. One job runs on one worker. The scheduler is a single control plane; there is no high availability, tenant isolation, cloud provisioning, or exactly-once guarantee for external side effects.

Read more about [security](https://github.com/pitchaya-s/runspoke/blob/main/docs/SECURITY.md), [scheduling](https://github.com/pitchaya-s/runspoke/blob/main/docs/SCHEDULING.md), [GPU requests](https://github.com/pitchaya-s/runspoke/blob/main/docs/GPU.md), [operations](https://github.com/pitchaya-s/runspoke/blob/main/docs/WORKFLOWS.md), and the [architecture](https://github.com/pitchaya-s/runspoke/blob/main/docs/ARCHITECTURE.md). For development, see [CONTRIBUTING.md](https://github.com/pitchaya-s/runspoke/blob/main/CONTRIBUTING.md).
