use anyhow::{Context, Result, ensure};
use clap::Parser;
use runspk::{
    hardware,
    model::*,
    policy::{self, Policy, SchedulingPolicy},
};
use serde::Serialize;
use std::{fs::File, io::Write, path::PathBuf, time::Instant};

#[derive(Parser)]
#[command(
    name = "runspk-bench",
    version,
    about = "Bounded discrete-event experiments using runspk's production scheduling policies"
)]
struct Args {
    #[arg(long, default_value = "bench/results/simulation")]
    out: PathBuf,
    #[arg(long, default_value_t = 42)]
    seed: u64,
    #[arg(long,default_value_t=120,value_parser=clap::value_parser!(u32).range(1..=500))]
    jobs: u32,
}
struct Rng(u64);
impl Rng {
    fn next(&mut self, bound: u64) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        (self.0 >> 16) % bound
    }
}
#[derive(Clone, Serialize)]
struct Workload {
    scenario: String,
    seed: u64,
    jobs: Vec<Job>,
    durations_ms: Vec<i64>,
    workers: Vec<Worker>,
}
#[derive(Serialize)]
struct Summary {
    scenario: String,
    policy: Policy,
    seed: u64,
    jobs: usize,
    makespan_s: f64,
    throughput_jobs_s: f64,
    mean_wait_s: f64,
    p95_wait_s: f64,
    max_wait_s: f64,
    reserved_cpu_fraction: f64,
    reserved_mem_fraction: f64,
    stranded_cpu_fraction: f64,
    jain_inverse_slowdown: f64,
    policy_mean_us: f64,
    policy_p95_us: f64,
    policy_max_us: f64,
    policy_total_ms: f64,
    failure_recovery_ms: Option<i64>,
    reexecutions: u32,
}
#[derive(Clone, Serialize)]
struct Trace {
    scenario: String,
    policy: Policy,
    seed: u64,
    job_id: String,
    submitted_ms: i64,
    first_started_ms: i64,
    finished_ms: i64,
    cpu: u32,
    mem_mib: u64,
    gpu: u32,
    priority: i32,
    runtime_ms: i64,
    attempts: u32,
}
fn worker(id: &str, cpu: u32, mem: u64, gpu: u32) -> Worker {
    Worker {
        registration: Registration {
            auth: WorkerAuth {
                id: id.into(),
                session: "simulation".into(),
            },
            capacity: Resources { cpu, mem_mib: mem },
            physical: Resources { cpu, mem_mib: mem },
            allow_native: true,
            runtime: None,
            gpus: hardware::select_gpus(&[], gpu, id).unwrap(),
            project_staging: true,
        },
        state: WorkerState::Online,
        last_seen_ms: 0,
        reserved: Resources::default(),
        reserved_gpus: Vec::new(),
    }
}
fn generate(scenario: &str, seed: u64, n: u32) -> Workload {
    let mut rng = Rng(seed);
    let count = if scenario == "long" { n.min(12) } else { n };
    let workers = if matches!(scenario, "heterogeneous" | "fragmentation" | "failure") {
        vec![
            worker("a", 8, 4096, 0),
            worker("b", 2, 8192, 0),
            worker("c", 4, 2048, 0),
        ]
    } else {
        vec![
            worker("a", 4, 4096, if scenario == "gpu" { 2 } else { 0 }),
            worker("b", 4, 4096, if scenario == "gpu" { 1 } else { 0 }),
        ]
    };
    let mut jobs = Vec::new();
    let mut durations_ms = Vec::new();
    for i in 0..count {
        let cpu = if scenario == "short" {
            1
        } else if scenario == "heterogeneous" {
            1 + rng.next(8) as u32
        } else {
            1 + rng.next(4) as u32
        };
        let mem = if scenario == "short" {
            64
        } else {
            128 * (1 + rng.next(16))
        };
        let duration = if scenario == "long" {
            20000 + rng.next(40000)
        } else if scenario == "short" {
            100 + rng.next(900)
        } else {
            1000 + rng.next(9000)
        };
        let arrival = if scenario == "bursty" {
            (i / 20) as i64 * 4000
        } else if scenario == "priority" && i % 5 == 0 {
            5000 + (i / 5) as i64 * 500
        } else {
            0
        };
        let gpu = if scenario == "gpu" {
            1 + rng.next(2) as u32
        } else {
            0
        };
        let (cpu, mem, duration) = if scenario == "fragmentation" && i % 3 == 0 {
            (8, 2048, 12000)
        } else {
            (cpu, mem, duration)
        };
        let j = Job {
            id: format!("{scenario}-{i}"),
            seq: i as i64,
            spec: JobSpec {
                command: vec!["simulation".into()],
                resources: Resources { cpu, mem_mib: mem },
                timeout_secs: duration.div_ceil(1000),
                trusted: true,
                cwd: None,
                priority: if scenario == "priority" && i % 5 == 0 {
                    20
                } else {
                    0
                },
                max_retries: 1,
                submission_key: None,
                image: None,
                gpu,
                gpu_ids: Vec::new(),
                gpu_mem_mib: 0,
                workspace_id: None,
                outputs: Vec::new(),
            },
            state: JobState::Queued,
            submitted_ms: arrival,
            started_ms: None,
            finished_ms: None,
            attempt_id: None,
            exit_code: None,
            error: None,
            worker_id: None,
            attempts: 0,
            retry_at_ms: 0,
            lease_until_ms: 0,
            allocated_gpus: Vec::new(),
        };
        jobs.push(j);
        durations_ms.push(duration as i64);
    }
    Workload {
        scenario: scenario.into(),
        seed,
        jobs,
        durations_ms,
        workers,
    }
}
fn quantile(values: &[f64], q: f64) -> f64 {
    let mut sorted = values.to_vec();
    sorted.sort_by(f64::total_cmp);
    sorted[((sorted.len() - 1) as f64 * q).ceil() as usize]
}
fn simulate(workload: &Workload, policy: Policy) -> Result<(Summary, Vec<Trace>)> {
    let mut jobs = workload.jobs.clone();
    let mut workers = workload.workers.clone();
    let count = jobs.len();
    let mut ends = vec![None; count];
    let mut first = vec![None; count];
    let mut was_failed = vec![false; count];
    let mut fencing = vec![false; count];
    let mut now = 0i64;
    let mut failed = false;
    let mut returned = false;
    let mut recovery = None;
    let mut cpu_area = 0f64;
    let mut mem_area = 0f64;
    let mut stranded_area = 0f64;
    let mut timings = Vec::new();
    let total_cpu = workers
        .iter()
        .map(|w| w.registration.capacity.cpu as u64)
        .sum::<u64>() as f64;
    let total_mem = workers
        .iter()
        .map(|w| w.registration.capacity.mem_mib)
        .sum::<u64>() as f64;
    for iteration in 0..100_000 {
        for i in 0..count {
            if ends[i].is_some_and(|end| end <= now) {
                ends[i] = None;
                if fencing[i] {
                    fencing[i] = false;
                    jobs[i].state = JobState::Queued;
                    jobs[i].worker_id = None;
                    jobs[i].retry_at_ms = now + 1000;
                    jobs[i].started_ms = None;
                } else {
                    jobs[i].state = JobState::Succeeded;
                    jobs[i].finished_ms = Some(now);
                }
            }
        }
        if workload.scenario == "failure" && !failed && now >= 5000 {
            failed = true;
            workers[0].state = WorkerState::Offline;
            for i in 0..count {
                if policy::active(&jobs[i]) && jobs[i].worker_id.as_deref() == Some("a") {
                    jobs[i].state = JobState::Uncertain;
                    fencing[i] = true;
                    was_failed[i] = true;
                    ends[i] = Some(now + 2000);
                }
            }
        }
        if failed && !returned && now >= 20000 {
            returned = true;
            workers[0].state = WorkerState::Online;
        }
        if jobs.iter().all(|j| j.state.terminal()) {
            break;
        }
        let visible: Vec<_> = jobs
            .iter()
            .filter(|j| j.submitted_ms <= now)
            .cloned()
            .collect();
        let start = Instant::now();
        let placements = policy.plan(&visible, &workers, now);
        timings.push(start.elapsed().as_secs_f64() * 1e6);
        for p in placements {
            let i = jobs
                .iter()
                .position(|j| j.id == p.job_id)
                .context("placement missing")?;
            ensure!(jobs[i].state == JobState::Queued, "duplicate placement");
            let j = &mut jobs[i];
            j.state = JobState::Running;
            j.worker_id = Some(p.worker_id);
            j.allocated_gpus = p.gpu_ids;
            j.started_ms = Some(now);
            j.attempts += 1;
            first[i].get_or_insert(now);
            ends[i] = Some(now + workload.durations_ms[i]);
            if was_failed[i] && recovery.is_none() {
                recovery = Some(now - 5000);
            }
        }
        let mut device_ids = std::collections::BTreeSet::new();
        for w in &workers {
            let active: Vec<_> = jobs
                .iter()
                .filter(|j| {
                    policy::active(j) && j.worker_id.as_deref() == Some(&w.registration.auth.id)
                })
                .collect();
            ensure!(
                active
                    .iter()
                    .map(|j| j.spec.resources.cpu as u64)
                    .sum::<u64>()
                    <= w.registration.capacity.cpu as u64,
                "CPU overcommit"
            );
            ensure!(
                active.iter().map(|j| j.spec.resources.mem_mib).sum::<u64>()
                    <= w.registration.capacity.mem_mib,
                "RAM overcommit"
            );
            for j in active {
                for id in &j.allocated_gpus {
                    ensure!(device_ids.insert(id), "duplicate GPU");
                }
            }
        }
        let mut events: Vec<i64> = jobs
            .iter()
            .filter(|j| j.submitted_ms > now)
            .map(|j| j.submitted_ms)
            .collect();
        events.extend(ends.iter().flatten().copied().filter(|end| *end > now));
        events.extend(
            jobs.iter()
                .filter(|j| j.state == JobState::Queued && j.retry_at_ms > now)
                .map(|j| j.retry_at_ms),
        );
        if workload.scenario == "failure" && !failed {
            events.push(5000);
        }
        if failed && !returned {
            events.push(20000);
        }
        let next = events
            .into_iter()
            .min()
            .context("simulation deadlocked or unsatisfiable workload")?;
        let dt = (next - now) as f64;
        for j in jobs.iter().filter(|j| policy::active(j)) {
            cpu_area += j.spec.resources.cpu as f64 * dt;
            mem_area += j.spec.resources.mem_mib as f64 * dt;
        }
        let ready: Vec<_> = jobs
            .iter()
            .filter(|j| {
                j.state == JobState::Queued && j.submitted_ms <= now && j.retry_at_ms <= now
            })
            .collect();
        if !ready.is_empty() {
            for w in workers.iter().filter(|w| w.state == WorkerState::Online) {
                let free = policy::available(w, &jobs);
                if !ready.iter().any(|j| {
                    j.spec.resources.fits(free) && policy::gpu_selection(w, j, &jobs).is_some()
                }) {
                    stranded_area += free.cpu as f64 * dt;
                }
            }
        }
        now = next;
        ensure!(iteration < 99_999, "simulation event limit exceeded");
    }
    ensure!(
        jobs.iter().all(|j| j.state == JobState::Succeeded),
        "not all simulated jobs finished"
    );
    let waits: Vec<_> = jobs
        .iter()
        .enumerate()
        .map(|(i, j)| (first[i].unwrap() - j.submitted_ms) as f64 / 1000.)
        .collect();
    let fairness: Vec<_> = jobs
        .iter()
        .enumerate()
        .map(|(i, j)| {
            1. / ((j.finished_ms.unwrap() - j.submitted_ms) as f64
                / workload.durations_ms[i].max(1000) as f64)
                .max(1.)
        })
        .collect();
    let summary = Summary {
        scenario: workload.scenario.clone(),
        policy,
        seed: workload.seed,
        jobs: count,
        makespan_s: now as f64 / 1000.,
        throughput_jobs_s: count as f64 / (now as f64 / 1000.),
        mean_wait_s: waits.iter().sum::<f64>() / count as f64,
        p95_wait_s: quantile(&waits, 0.95),
        max_wait_s: quantile(&waits, 1.),
        reserved_cpu_fraction: cpu_area / (total_cpu * now as f64),
        reserved_mem_fraction: mem_area / (total_mem * now as f64),
        stranded_cpu_fraction: stranded_area / (total_cpu * now as f64),
        jain_inverse_slowdown: fairness.iter().sum::<f64>().powi(2)
            / (count as f64 * fairness.iter().map(|v| v * v).sum::<f64>()),
        policy_mean_us: timings.iter().sum::<f64>() / timings.len() as f64,
        policy_p95_us: quantile(&timings, 0.95),
        policy_max_us: quantile(&timings, 1.),
        policy_total_ms: timings.iter().sum::<f64>() / 1000.,
        failure_recovery_ms: recovery,
        reexecutions: jobs.iter().map(|j| j.attempts.saturating_sub(1)).sum(),
    };
    let traces = jobs
        .iter()
        .enumerate()
        .map(|(i, j)| Trace {
            scenario: workload.scenario.clone(),
            policy,
            seed: workload.seed,
            job_id: j.id.clone(),
            submitted_ms: j.submitted_ms,
            first_started_ms: first[i].unwrap(),
            finished_ms: j.finished_ms.unwrap(),
            cpu: j.spec.resources.cpu,
            mem_mib: j.spec.resources.mem_mib,
            gpu: j.spec.gpu,
            priority: j.spec.priority,
            runtime_ms: workload.durations_ms[i],
            attempts: j.attempts,
        })
        .collect();
    Ok((summary, traces))
}
fn main() -> Result<()> {
    let args = Args::parse();
    std::fs::create_dir_all(&args.out)?;
    let scenarios = [
        "short",
        "long",
        "mixed",
        "priority",
        "heterogeneous",
        "failure",
        "fragmentation",
        "gpu",
        "bursty",
    ];
    let workloads: Vec<_> = scenarios
        .iter()
        .map(|s| generate(s, args.seed, args.jobs))
        .collect();
    let mut summaries = Vec::new();
    let mut traces = Vec::new();
    for w in &workloads {
        for policy in [
            Policy::Fifo,
            Policy::BestFit,
            Policy::Priority,
            Policy::Easy,
        ] {
            let (summary, trace) = simulate(w, policy)?;
            summaries.push(summary);
            traces.extend(trace);
        }
    }
    serde_json::to_writer_pretty(File::create(args.out.join("workloads.json"))?, &workloads)?;
    serde_json::to_writer_pretty(File::create(args.out.join("simulation.json"))?, &summaries)?;
    serde_json::to_writer(
        File::create(args.out.join("simulation-traces.json"))?,
        &traces,
    )?;
    let mut csv = File::create(args.out.join("simulation.csv"))?;
    writeln!(
        csv,
        "scenario,policy,seed,jobs,makespan_s,throughput_jobs_s,mean_wait_s,p95_wait_s,max_wait_s,reserved_cpu_fraction,reserved_mem_fraction,stranded_cpu_fraction,jain_inverse_slowdown,policy_mean_us,policy_p95_us,policy_max_us,policy_total_ms,failure_recovery_ms,reexecutions"
    )?;
    for s in &summaries {
        let p = serde_json::to_value(s.policy)?;
        writeln!(
            csv,
            "{},{},{},{},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6},{},{}",
            s.scenario,
            p.as_str().unwrap(),
            s.seed,
            s.jobs,
            s.makespan_s,
            s.throughput_jobs_s,
            s.mean_wait_s,
            s.p95_wait_s,
            s.max_wait_s,
            s.reserved_cpu_fraction,
            s.reserved_mem_fraction,
            s.stranded_cpu_fraction,
            s.jain_inverse_slowdown,
            s.policy_mean_us,
            s.policy_p95_us,
            s.policy_max_us,
            s.policy_total_ms,
            s.failure_recovery_ms
                .map(|n| n.to_string())
                .unwrap_or_default(),
            s.reexecutions
        )?;
    }
    println!(
        "{} policy/scenario comparisons; {} job traces; {}",
        summaries.len(),
        traces.len(),
        args.out.display()
    );
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn fixed_seed_simulation_is_deterministic_and_all_policies_finish() {
        for scenario in [
            "short",
            "long",
            "mixed",
            "priority",
            "heterogeneous",
            "failure",
            "fragmentation",
            "gpu",
            "bursty",
        ] {
            let w = generate(scenario, 42, 16);
            assert_eq!(
                serde_json::to_string(&w).unwrap(),
                serde_json::to_string(&generate(scenario, 42, 16)).unwrap()
            );
            for p in [
                Policy::Fifo,
                Policy::BestFit,
                Policy::Priority,
                Policy::Easy,
            ] {
                let (s, t) = simulate(&w, p).unwrap();
                let (again, trace) = simulate(&w, p).unwrap();
                assert_eq!(s.makespan_s, again.makespan_s);
                assert_eq!(
                    serde_json::to_string(&t).unwrap(),
                    serde_json::to_string(&trace).unwrap()
                );
                assert!(s.reserved_cpu_fraction <= 1.000001 && s.reserved_mem_fraction <= 1.000001);
                assert!((0.0..=1.000001).contains(&s.jain_inverse_slowdown));
            }
        }
    }
}
