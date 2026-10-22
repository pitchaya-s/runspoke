# Scheduling policies

Select `runspk-scheduler --policy fifo|best-fit|priority|easy`. `SchedulingPolicy::plan` is a pure interface shared by live scheduling, unit/property tests and the later simulator. Placement decisions are committed together before dispatch. Jobs occupy one worker; a job cannot span hosts.

| Policy | Queue order | Worker choice | Blocked head |
| --- | --- | --- | --- |
| fifo | submission sequence | first feasible worker ID | waits |
| best-fit | submission sequence | smallest sum of remaining CPU and memory fractions | waits |
| priority | effective priority, sequence tie break | first feasible worker ID | waits |
| easy | effective priority, sequence tie break | first feasible worker ID | backfills without delaying the head reservation |

Priorities are integers -100 through 100. Effective priority increases by one for every five seconds waiting. A waiting job eventually outranks newly arriving jobs at any allowed priority. This assumes finite runtimes, a finite arrival rate, and a worker capable of running it. FIFO and best-fit retain queue order to prevent repeated bypass. Jobs that no online worker could ever fit remain queued but do not block other jobs. Administrative capacity changes or permanently absent workers defeat eventual-start guarantees.

EASY reserves the earliest predicted feasible start on one worker for the head job. A lower job may run on another worker, or finish before that reservation on the reserved worker. Every job has an enforced wall timeout. Predictions include two seconds for dispatch and cleanup. An overdue active execution has no predicted release; it must reconcile before resources are assumed free. Reservations are recomputed each pass, not promises of a real-time start: slow polling, termination, OS scheduling and failures can extend them. This is EASY-style protection of one head, not conservative protection of every queued job.

Configured capacity is an administrator's budget within discovered physical CPU/RAM. CPU units are logical CPUs. Requested resources become reserved resources in assigned/running/cancelling/uncertain states. Reservations are derived from durable active job records, avoiding separate counters that leak. Measured RSS/CPU and GPU utilization are distinct observations and never authorize overcommit. Workers allocate disjoint CPU identities to concurrent jobs within their configured affinity pool. Native jobs inherit that affinity; containers additionally receive cpuset and CPU quota limits. Use one worker per host, or partition multiple local workers with `taskset --cpu-list` and disjoint memory budgets. RLIMIT_AS bounds each native process's virtual address space, not aggregate descendant RSS. Use containers for cgroup memory enforcement.

First fit is simple but can strand large jobs after placing a small job on a large host. Best fit can preserve those hosts, but its two-resource score is a heuristic. Strict queue order can leave holes; EASY trades planning overhead and reliance on runtime bounds for opportunities to fill them. Priority changes fairness and can increase the wait of lower-priority jobs until aging catches up. No policy is claimed universally fastest.

Polling does not invoke the planner when there are no queued jobs or every online worker has exhausted CPU/RAM. An existing assignment is returned by indexed lookup. Other planning passes still read and order the live queue; policy candidates borrow job records and only active/newly placed jobs occupy the planning reservation set. No mutable queue cache changes retry deadlines, priority aging or recovery behavior. When benchmarking, measure Store/policy costs separately from actual execution throughput.

## Worker capabilities

Workers advertise bounded key/value labels through repeated `--label KEY=VALUE`,
`RUNSPK_LABELS`, or settings `worker.labels`. Jobs specify an exact conjunction
through repeated `--require KEY=VALUE` or JSON `required_labels`. For example,
`dataset=measurements-v1` and `python=3.12` select provisioned data and environments.
Every policy checks these requirements in immediate placements and future EASY
reservations. A worker missing a required label is incompatible, even if it has
spare resources. Labels do not reserve resources or verify software installations.
There are no soft preferences in this version.

Project jobs also require the worker's advertised transfer envelope. Legacy
registrations without labels/limits remain eligible only for compatible default
jobs. `runspk explain ID` reports mismatched labels, insufficient transfer limits,
backend/capacity/GPU issues, retry backoff and the job's submission-order queue
position. That position is explicitly separate from priority/backfill order.
