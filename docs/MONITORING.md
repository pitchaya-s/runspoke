# Monitor and logs

```bash
runspk monitor
runspk --config ~/.config/runspk/lab.json monitor --refresh 3
```

The Ratatui/Crossterm monitor uses the existing authenticated HTTP API. Standard
input and output must both be terminals; SSH terminals are supported. Other CLI
commands keep their ordinary JSON and scripting behavior. No background process
is installed. Raw input, cursor visibility and the alternate screen are restored
on ordinary exit, errors, Ctrl-C and SIGTERM; panic handling restores them before
printing the panic. SIGKILL cannot run cleanup (`reset` repairs the terminal).

Press `?` for the full shortcut reference. The core controls are:

| Key | Action |
| --- | --- |
| `1`, `2` | Jobs or workers |
| Up/Down or `j`/`k`, `g`/`G` | Select a row, first/last row |
| Enter/`d`, `a`, `e` | Job details, attempts, scheduler explanation |
| `o`, `i` | Live reported stdout or stderr |
| Tab | Cycle detail panes |
| `/`, Esc | Search current page, clear search |
| `f`, `w` | Cycle job state, filter on selected worker |
| `s`, `S` | Cycle sort, reverse sort |
| `n`, `b` | Next/previous page of job history |
| PgUp/PgDn, `[`, `]`, `t` | Scroll details, horizontal scroll, toggle log following |
| Space, `r` | Pause polling, refresh now |
| `c`, then `y` | Confirm cancellation of the selected job |
| `q`, Ctrl-C | Exit; jobs keep running |

Job pages are bounded to 200 records; search/sorting operate on the loaded page.
Use state/worker filters and paging for older/larger histories. The worker view
shows at most 512 workers and labels truncation. Detailed attempts show the latest
100 records. The monitor retains the last successful snapshot during connection
loss, shows errors and snapshot age, and polls again automatically. Input remains
responsive while HTTP requests fail or time out. Refresh defaults to two seconds,
configurable from one to 60 seconds. Small terminals show a resize hint; wider
terminals show list/detail panes side by side. API text is sanitized before terminal
rendering, including control sequences and bidirectional formatting controls.

CPU/RAM/GPU displays are **reservations**, not measured application consumption.
GPU utilization/free memory is the last registration measurement (or explicitly
simulated data), not a live sample. It must never be used to infer that a running
or uncertain attempt has released its resources. Worker details also show measured host CPU busy percentage (two-sample `/proc/stat` delta) and kernel `MemAvailable`/total RAM from `/proc/meminfo`, sampled every two seconds. These are whole-host observations, including other applications and CPUs outside a worker’s affinity; they are not per-job RSS or cgroup usage. Failed samples become unavailable and sample receipt age is shown separately. Per-job RSS remains unavailable. Queue explanations come from the scheduler and do not promise a
future dispatch time; submission position differs from priority/backfill order. Job queue timing is measured since submission to the current start and therefore includes prior execution/backoff history after retries; use individual attempts for that history.

Cancellation always goes through the scheduler and requires confirmation in the
monitor. Cancelling running work is asynchronous. Network failure can leave its
outcome unknown; refresh before requesting it again. The monitor does not expose
fencing/uncertain resolution as a quick action.

## Logs and byte cursors

```bash
runspk logs JOB_ID --follow
runspk logs JOB_ID --tail 16384
runspk --json logs JOB_ID --incremental --stdout-offset 65536 --stderr-offset 0
```

Ordinary `--json logs` retains the legacy full-snapshot shape; `--json run`
retains submitted/logs/finished events. Text log following and text `run` use
bounded incremental reads when supported. `--incremental` explicitly selects JSON
chunks. The endpoint is `GET /jobs/ID/logs/chunk` with `stdout_offset`,
`stderr_offset`, optional `attempt_id`, `limit` and `tail`. Cursors count retained
UTF-8 bytes independently per stream, not lines or original raw bytes. Responses
include actual starting/next offsets, totals and a reset flag. Preserve the
attempt ID with both cursors; stale offsets or a changed attempt reset both.
Limits default to 64 KiB per stream and clamp to 256 KiB. Arbitrary offsets round
down to a character boundary; tail starts round up within the requested byte
window. A byte budget smaller than the next character may return no progress.

Workers retain the first 1 MiB of raw output per stream, drain excess output so
pipes cannot deadlock, and send changed snapshots at most once per second.
Lease reports continue every 200 ms; completion always sends the final snapshot.
New live snapshots withhold trailing partial UTF-8 characters until complete;
invalid bytes are replaced. Earlier workers may rewrite a final partial character,
so cursor consumers of legacy workers should refetch on a reset. stdout and
stderr order are independent; no cross-stream timestamp order is reconstructed.

`--tail` shows the end of the **retained prefix**, not the end of discarded output.
Chunk truncation flags mean the capture may have reached its cap; exact discarded
byte counts are unavailable. The monitor requests only a bounded 64 KiB view and
bounds displayed lines. Use declared output files for complete bounded logs, or
explicit worker-managed storage for large logs. There is no unbounded historical
log service or configurable retention automation in this version.
