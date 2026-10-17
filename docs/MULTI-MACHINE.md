# A scheduler, a worker and a laptop

Install the same complete Linux release on all three machines. Use unprivileged
accounts. The scheduler listens on loopback; SSH authenticates the machines and
forwards the API. Every runspk invitation grants administrative access. Keep its
mode 0600, transfer it through SSH, and retain SSH host-key verification.

## Scheduler server

```bash
runspk init
runspk start --scheduler-only
```

Confirm startup, then stop the foreground process with Ctrl+C. To supervise it,
copy `examples/runspk.service` to `~/.config/systemd/user/runspk.service` and **edit
its ExecStart** to `%h/.local/bin/runspk start --scheduler-only`. Keep the default
profile and state paths or include your explicit `--config` path. Review the
unit before running:

```bash
mkdir -p "$HOME/.config/systemd/user"
# Copy and edit the unit described above before these commands.
systemctl --user daemon-reload
systemctl --user enable --now runspk.service
journalctl --user -u runspk.service -n 30
runspk doctor
runspk invite --out worker-connection.json
scp worker-connection.json worker-host:~/worker-connection.json
```

Do not run the foreground scheduler at the same time as the service. The database
lock rejects a second scheduler; it does not select between different databases.

## Trusted worker

Authenticate to the server with SSH once and verify its host key. Start a tunnel:

```bash
ssh -NT -o ExitOnForwardFailure=yes -o ServerAliveInterval=15 \
  -o ServerAliveCountMax=3 -L 127.0.0.1:8765:127.0.0.1:8765 scheduler-host
```

In another terminal, import the invitation and test the worker:

```bash
runspk join ~/worker-connection.json --id workstation --cpu 2 --mem 2G --allow-native
```

`join` saves the connection profile; it does not need to run again for every
restart. Stop the test worker with Ctrl+C. Copy `examples/runspk-joined-worker.service`
and `examples/runspk-tunnel.service` into `~/.config/systemd/user/`. Edit the worker
ID/budgets, installation/profile paths and `scheduler-host` before enabling. Use
the same ID and state directory on every restart. If port 8765 is occupied, edit
the tunnel's local port and the profile's URL to match. With an existing HTTPS
proxy, use its URL, and remove the tunnel Wants/After entries from the worker
unit. No tunnel is needed then.

```bash
systemctl --user daemon-reload
systemctl --user enable --now runspk-tunnel.service runspk-joined-worker.service
systemctl --user status runspk-tunnel.service runspk-joined-worker.service
journalctl --user -u runspk-joined-worker.service -n 30
runspk doctor
```

Batch-mode SSH requires a usable key/agent after login and restart. Configure it
explicitly under your normal SSH policy; an encrypted key requiring an absent
agent will not reconnect unattended. Neither runspk nor these templates changes
SSH credentials or disables host-key checks.

For operation after the **last login ends** and for boot startup, the machine's
administrator must allow the user manager to persist. Check
`loginctl show-user "$USER" -p Linger`; if desired, explicitly enable it with
`loginctl enable-linger "$USER"` under local administrator policy. This is an
optional host setting, not something runspk installs. Without it, a user service
may stop on logout. Undo with `loginctl disable-linger "$USER"` when appropriate
for that account's other services.

## Submit from the laptop

On the scheduler, create another private invitation and copy it to the laptop:

```bash
runspk invite --out laptop-connection.json
scp laptop-connection.json laptop-host:~/laptop-connection.json
```

On the laptop, start an SSH tunnel as above. Set connection variables for this
shell only; do not run `join` unless the laptop should also execute jobs:

```bash
export RUNSPK_CONFIG="$HOME/laptop-connection.json"
runspk doctor
runspk workers
id=$(runspk submit --project . --output results --cpu 1 --mem 512M --trusted -- python3 experiment.py)
runspk wait "$id"
runspk fetch "$id" --into experiment-results
```

For a different local tunnel port, also set
`RUNSPK_URL=http://127.0.0.1:18765` after forwarding local 18765 to server 8765.
The laptop can disconnect after submission. Record the job ID, reconnect the
tunnel, and use `status`, `logs --follow`, `wait` and `fetch` with that ID. No
client process needs to remain running to preserve a submitted job.

## Connection loss and recovery

The SSH unit reconnects after the connection fails; the worker keeps retrying
scheduler requests. `doctor` distinguishes private configuration, companion and
API failures. Inspect both tunnel and worker journals for connection errors. On
the scheduler, `workers` reports health and reserved resources; `explain JOB_ID`
shows unavailable workers and constraints.

A network failure longer than the local execution lease **stops active jobs**;
it does not let them continue indefinitely without permission. Stopped
completions remain durable until reconnection. Their configured retry budget
applies after the scheduler acknowledges termination; retries default to zero.
The worker can execute new work after reconnecting. A lost host can leave jobs
`uncertain` with capacity retained. Restart the worker with its original state
to reconcile. If that state/host is lost, fence it externally before
`resolve JOB_ID --assume-stopped`. Never delete journals to free reservations.

Remove these optional services explicitly:

```bash
# On the worker:
systemctl --user disable --now runspk-joined-worker.service runspk-tunnel.service
rm "$HOME/.config/systemd/user/runspk-joined-worker.service" "$HOME/.config/systemd/user/runspk-tunnel.service"
systemctl --user daemon-reload
# On the server, if retiring the scheduler:
systemctl --user disable --now runspk.service
```

These commands retain profiles, scheduler data and worker journals. Back them up
before upgrades. Process/network fault tests use isolated local processes;
physical multi-machine and boot/login behavior require separate validation.
