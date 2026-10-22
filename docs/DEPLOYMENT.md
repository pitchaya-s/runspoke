# Installation and deployment

Linux x86_64 was tested on Ubuntu 24.04, Rust 1.99.1 and Python 3.12.3. Build dependencies are a current stable Rust toolchain, a C compiler, linker and GNU coreutils. SQLite is bundled, so development headers are unnecessary. Runtime requirements are Linux `/proc`, `/bin/sh`, `/usr/bin/timeout`, writable local state directories, and an unprivileged account for native jobs. Docker/Podman and NVIDIA drivers/toolkit are optional administrator-managed components.

For a new installation, follow the [quickstart](QUICKSTART.md): install a verified archive without Rust, `runspk init`, `runspk start`, project submission and `invite`/`join`. The [multi-machine guide](MULTI-MACHINE.md) supplies explicit scheduler, worker, tunnel, laptop and logout-persistence steps. The environment-based deployment below remains supported. The archive's `install.sh ARCHIVE BIN_DIRECTORY` verifies platform/glibc requirements and all companion versions before replacement.

## Build and validate

```bash
cargo build --release --workspace
scripts/check.sh
# Explicit opt-in integration check; requires Docker and preloaded alpine:3.20:
RUNSPK_TEST_DOCKER=1 scripts/check.sh
```

Build concurrency defaults to two in `.cargo/config.toml`. `cargo install --path crates/runspk --bins --root "$HOME/.local"` installs into your account; the benchmark binary is included. Alternatively copy the three runtime binaries from `target/release` into a user-owned directory on PATH. Build on the target machine, or use compatible architecture and glibc versions. No install or service activation is performed automatically by this project.

## One machine

Generate a token once and save it in a mode-0600 file; use that same token for the daemon and CLI.

```bash
umask 077
mkdir -p "$HOME/.config/runspk"
python3 -c 'import secrets; print("RUNSPK_TOKEN=" + secrets.token_hex(32))' > "$HOME/.config/runspk/token.env"
set -a
. "$HOME/.config/runspk/token.env"
. examples/single.env
set +a
runspk-scheduler --local
```

In another terminal source the same token/env files, then use `runspk`. `single.env` sets two CPU slots and 512 MiB. Adjust to leave room for the OS and existing workloads; limits are budgets within physical capacity, not automatic safe free-resource discovery. The default database is `.runspk/scheduler.sqlite` under the daemon's working directory. Use `--db /absolute/local/path/state.sqlite` for stable service paths. Local worker journals live beside the database in `workers/local`.

## Several machines

Run the scheduler without `--local`. Source `examples/scheduler.env` plus the token file. On each worker choose a unique ID, source a private copy of the same token and `examples/worker.env`, then run `runspk-worker --data-dir /absolute/local/path/workers`.

Forward the scheduler API over SSH from each worker and remote CLI host:

```bash
ssh -N -o ExitOnForwardFailure=yes -o ServerAliveInterval=15 \
    -L 127.0.0.1:8765:127.0.0.1:8765 scheduler-host
```

Then RUNSPK_URL remains `http://127.0.0.1:8765`. If that local port is occupied, choose another local forwarding port and set RUNSPK_URL accordingly. Keep SSH host-key verification enabled. For an existing TLS reverse proxy, publish only HTTPS with a valid certificate and forward to scheduler loopback; retain bearer authentication. The Rust daemon itself does not terminate TLS.

The worker ID and state directory identify one resource owner. Preserve both across upgrades/restarts. Partition budgets when running multiple workers on one physical machine; the scheduler cannot infer that their CPU/RAM pools overlap. GPU UUID advertisements are checked for duplication globally. Do not place SQLite or worker journals on network filesystems.

For example, `taskset --cpu-list 0,2 runspk-worker --id local-a --cpu 2 --mem-mib 256 --allow-native` confines that worker's jobs to two logical CPU IDs. Give another local worker a disjoint list. Inspect `lscpu -p=CPU,CORE,SOCKET` to avoid selecting SMT siblings when physical-core separation matters. Within each worker, concurrent jobs receive disjoint CPU IDs automatically. Idle workers may change budgets on restart; active/uncertain executions must reconcile before shrinking or changing their inventory.

## User services

The profile-based `examples/runspk.service` and environment-based sample `.service` files are **user units**. Choose one deployment style; do not start two schedulers on the same state. Review/copy them into `~/.config/systemd/user/`, install binaries in `~/.local/bin`, and put the matching environment files in `~/.config/runspk`. Add RUNSPK_TOKEN to both protected env files; the sample files intentionally contain no usable token. Set `RUNSPK_WORKER_ID`, budgets and backend consent per worker. The service paths use `~/.local/state/runspk`.

After reviewing the files, an operator can run `systemctl --user daemon-reload` and `systemctl --user enable --now runspk-scheduler runspk-worker`. A remote worker also needs a supervised SSH tunnel or existing HTTPS endpoint. User services do not require privileged system installation. Worker KillMode=process leaves journaled cooperating native groups for restart reconciliation after a hard worker crash; normal SIGTERM cancels and cleans up running work.

## Operations and upgrades

SIGINT and SIGTERM stop dispatch, cancel active work, clean execution resources, and persist final results. An embedded local worker shuts down while the API is still serving completion reports. Separate workers can remain running across scheduler restart; short outages can renew the existing lease. Longer outages stop jobs and keep completions locally until delivery. Cancellation is asynchronous: inspect status until terminal.

`runspk stats`, `workers`, `queue`, `status`, `attempts`, `explain`, and stderr tracing provide basic monitoring. Set RUST_LOG=info for submitted/start/stopped events carrying job, attempt and worker IDs, or debug for request latency, state writes and lease/poll failures. Stats include process-lifetime planning, dispatch, retry and database operation counters; database timing separates operation time from mutex waiting. Protect state files and logs as secrets. Use `list --limit 100 --after SEQ` and indexed state/worker filters for large histories. Explicit bounded pruning can remove old unkeyed terminal job/log records; completion receipts and keyed records remain.

Run `runspk backup --db /path/state.sqlite --out /path/new-backup.sqlite` on the scheduler host for an online snapshot. It uses a separate SQLite read connection and does not acquire the scheduler Store mutex. The destination must be new, is mode 0600, and must not be used until the command succeeds. Failure attempts to remove the copy; a crash may leave an incomplete destination that needs manual removal. Backup retries are bounded to 30 seconds between SQLite steps; an underlying stalled filesystem can still block a step. Preserve worker journals independently. On upgrade, stop workers gracefully, back up state, replace all binaries, then restart with the same paths and token. Incompatible database formats are refused without changing their contents. There is no downgrade compatibility promise. If a worker directory or machine is lost, follow [external fencing and explicit resolution](FAILURE-MODEL.md).

## Preferences, monitoring and storage

See [CONFIGURATION](CONFIGURATION.md) for non-secret settings shared by the CLI,
scheduler and workers, named private profile paths, precedence and bounded
transfer negotiation. See [MONITORING](MONITORING.md) for the interactive monitor,
byte-cursor logs and measurement semantics. `runspk storage` reports database/WAL
allocation, reusable pages, snapshot/artifact encoded bytes, retained UTF-8 logs
and immutable execution receipts. Pruning makes database pages reusable; it does
not shrink the file or expire receipts. Automatic retention remains disabled.
`backup` without `--db` now follows the selected private profile's state directory;
explicit paths remain supported. Restore only to a new database path and preserve
worker journals; a database backup does not contain live worker process identity.

The schema remains version 1: new optional wire/specification fields use serde
defaults and persist inside existing JSON records. No tables are rebuilt. Retain
the original worker transfer settings until active and uncertain executions have
reconciled. Upgrade the scheduler before enabling new label/name/tag features;
new clients refuse these operations when capability discovery says they would be
ignored. Replace all companion binaries together and keep the same token/state
paths. New source builds require Rust 1.89 or newer; runtime packages need no Rust.
