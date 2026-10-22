# Execution and recovery model

The scheduler provides one valid logical execution lease per job. It does not provide exactly-once side effects. A workload can modify external data, then fail before a completion is durable. An explicitly configured retry can repeat those effects. Use application idempotency keys, atomic output rename or external transactions for important results. By default retries are zero; `--retries N` adds at most N execution retries with durable exponential backoff. Dispatches that expire without a start acknowledgement can be reassigned without consuming the execution budget. A reported pre-start preparation failure terminates the job; retrying a deterministic staging error indefinitely at attempt zero is forbidden.

## Job states

| From | Event | To |
| --- | --- | --- |
| queued | placement committed | assigned |
| assigned | worker start acknowledgement | running |
| assigned | lease expiry before start | queued |
| queued, assigned | cancel | cancelled |
| running | cancel | cancelling |
| running | lease expiry | uncertain |
| uncertain | cancel | cancelling |
| running, uncertain | stopped completion with exit 0 | succeeded |
| running, uncertain | stopped failure, retries remain | queued after backoff |
| running, uncertain | stopped failure, no retries remain | failed |
| cancelling | stopped completion | cancelled |
| terminal | duplicate completion or cancellation | same terminal state |

Attempts have assigned, running, uncertain and finished states. Attempt IDs are UUIDs; every attempt records its worker ID and persisted worker session. Finished attempts are immutable on duplicate reports, including their log snapshots. An older attempt cannot finish a newer attempt or release its resources. Submission keys (`submit --key KEY`) deduplicate identical requests and reject changed specifications for that key.

Leases last five seconds and are renewed by execution reports, not idle worker heartbeats. The worker uses a monotonic three-second deadline measured from the beginning of its renewal request; HTTP requests time out after one second. OS suspension, machine failure or a stuck kernel can still defeat timely process termination. The control plane therefore **retains uncertain reservations** and does not treat expiry as evidence that a process died. The worker is offline for placement after ten seconds without heartbeats. Worker states are online, draining and offline; an authenticated heartbeat restores online state, and shutdown marks draining.

## Durable launch and restart

Wall time starts immediately before the worker opens the launch gate. The worker
deadline and independent GNU watchdog share one public outcome: a workload exit
observed at or after that deadline has exit code 124 and error `wall time
exceeded`. This includes an exit that becomes observable exactly at the boundary.
Early workload exits, including 124 or 137, retain their code and are not inferred
to be timeouts. Lease-renewal requests cannot extend the wall deadline. Cleanup
still confirms termination before releasing reservations or enabling retries.

Before dispatch, SQLite commits the job and attempt in one transaction. The worker writes and fsyncs a prepared journal entry, prepares any project snapshot while renewing assigned leases, acknowledges start, then spawns a process behind a stdin gate. It fsyncs the process PID, `/proc` start ticks and boot ID before opening the gate. A crash before process identity is durable closes the gate without starting the workload. Native GNU timeout supplies a wall-time monitor independent of the worker. Process groups contain cooperating descendants.

Worker state directories have an exclusive file lock and a persistent session. On restart using the **same directory**, the worker checks boot/PID identity, kills recorded process groups, confirms no live non-zombie group members remain, and reports those attempts failed. Prepared entries without PIDs never opened the launch gate and can report failure. Completed results are fsynced locally before sending, and replayed until acknowledged. An identity mismatch or unreadable/corrupt journal stops recovery and retains reservations for investigation. Native workloads that deliberately escape groups violate the trust assumption and cannot be automatically fenced.

Local recovery happens before registration, including when the worker's configured capacity changes. Stopped results are published using the persistent session before attempting the new registration; a scheduler outage leaves them durable for another try. Pending completions cannot be dispatched again. The execution task owns its active journal until it returns; background replay handles only completed tasks. An HTTP renewal cannot wait beyond the current monotonic execution deadline.

Journal disk operations use a single blocking lane. The lane's permit survives cancellation until the actual filesystem operation completes. A delayed identity write cannot open a gate after the start deadline. Filesystem failure or failed process/container cleanup retains the journal and reservation for reconciliation, rather than retrying a potentially running workload.

On scheduler restart, SQLite jobs, attempts, logs, retry times and sessions survive. Workers reconnect and renew leases still valid; expired running attempts become uncertain and are stopped by workers before completion. Expired assigned attempts are safe to return to the queue because every launch requires a current start acknowledgement. The scheduler database also has an exclusive process lock to prevent two control planes sharing one file. Reserved jobs with missing execution records are retained as uncertain for explicit reconciliation.

If a host or worker disk is permanently lost, inspect/fence the host externally. Only after execution is known to have stopped, use `runspk resolve JOB_ID --assume-stopped`. This is an explicit operator assertion, not a remote process-kill mechanism. It can trigger a configured retry. Resolving a still-running job risks duplicate execution and overcommit. Native jobs that escape process groups require manual cleanup before resolution.

No leader failover, cross-host process adoption, shared filesystem, or distributed transaction with application outputs is provided. Keep clocks reasonably synchronized; wall-clock lease persistence may delay recovery after a backward clock adjustment. Worker watchdog deadlines use monotonic time. SQLite requires reliable local storage; durability cannot survive storage loss. Back up SQLite using its backup API or stop the scheduler before copying the database and WAL.

Explicit pruning deletes only old terminal jobs with no unresolved execution and no submission key. Finished execution receipts remain immutable after job/log deletion. An authenticated duplicate report for such a receipt receives HTTP 410; current workers treat it as acknowledgement and remove their already-stopped completion journal. Unknown attempts and wrong sessions do not receive this acknowledgement. There is no automatic receipt expiry.

## Fault validation

Recovery refuses to signal a group with live members when its leader is missing:
the saved start identity can no longer be checked. Investigate and externally
fence those members before explicit resolution. A missing session file alongside
execution journals is also an error; restore the original session from backup
instead of creating a new identity. Corrupt journal errors identify the file and
preserve it. Never delete state to bypass these checks. Container inspection
failure is not proof of absence: cleanup requires successful removal or a
successful empty runtime listing before completion can release a reservation.

Tests SIGKILL only their own worker/scheduler processes, restart with the same metadata, and SIGSTOP/SIGCONT only their own scheduler to simulate unavailable networking. No host firewall or unrelated service is changed. Tests cover delayed completion acknowledgements, retries, expired unstarted dispatch, reservation retention, concurrent claims, cancellation/completion races, scheduler restarts and worker restarts. These are bounded local fault tests; they do not establish behavior under every kernel, disk or network fault.

Project inputs commit atomically with submission. Workspace preparation never launches code before a valid start acknowledgement. Captured outputs commit to the durable completion journal only after confirmed termination; artifact upload precedes completion delivery and is immutable on replay. A stalled upload leaves a stopped result durable and conservatively retains its reservation while worker heartbeats continue. Capture failures retain workspace files for manual inspection. See [project lifecycle](WORKSPACES.md). Tests cover delayed preparation beyond the five-second lease and a stalled output upload without blocking heartbeats.

Project transfer envelopes are durable job metadata. Lowering scheduler defaults does not change a started attempt’s admissible outputs, and new placements require a sufficiently capable worker. Pending completion maintenance enumerates identities and loads one journal at a time; a shared worker permit bounds simultaneous artifact collection/upload. Waiting for that permit happens only after confirmed termination and backend cleanup, keeps the reservation, and leaves the original durable process identity available for restart recovery. Journal writes still use fsync and atomic rename, now with streamed JSON encoding. A corrupt or identity-mismatched journal still refuses recovery and registration.
