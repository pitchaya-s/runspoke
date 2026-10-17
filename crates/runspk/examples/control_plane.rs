//! Store/policy microbenchmarks; no workload processes or physical GPUs are used.
use runspk::{
    model::*,
    policy::{Policy, SchedulingPolicy},
    store::Store,
};
use serde_json::json;
use std::time::Instant;

fn spec() -> JobSpec {
    serde_json::from_value(json!({"command":["/bin/true"],"resources":{"cpu":1,"mem_mib":64},"timeout_secs":10,"trusted":true,"cwd":null})).unwrap()
}
fn measure(mut f: impl FnMut(), n: usize) -> serde_json::Value {
    let mut samples = Vec::new();
    for _ in 0..n {
        let start = Instant::now();
        f();
        samples.push(start.elapsed().as_nanos() as u64);
    }
    samples.sort_unstable();
    json!({"median_us":samples[n/2] as f64/1000.0,"p95_us":samples[(n-1)*95/100] as f64/1000.0,"samples_ns":samples})
}
fn main() {
    let mut rows = Vec::new();
    for repeat in 0..3 {
        for history in [0, 5000] {
            for worker_count in [1, 16] {
                for policy in [
                    Policy::Fifo,
                    Policy::BestFit,
                    Policy::Priority,
                    Policy::Easy,
                ] {
                    let mut s = Store::open(":memory:").unwrap();
                    s.policy = policy;
                    for _ in 0..history {
                        let j = s.submit(spec()).unwrap();
                        s.cancel(&j.id).unwrap();
                    }
                    for _ in 0..1000 {
                        s.submit(spec()).unwrap();
                    }
                    let mut auths = Vec::new();
                    for i in 0..worker_count {
                        let auth = WorkerAuth {
                            id: format!("worker-{i:02}"),
                            session: uuid::Uuid::new_v4().to_string(),
                        };
                        s.register(Registration {
                            auth: auth.clone(),
                            capacity: Resources {
                                cpu: 1,
                                mem_mib: 128,
                            },
                            physical: Resources {
                                cpu: 1,
                                mem_mib: 128,
                            },
                            allow_native: true,
                            runtime: None,
                            gpus: Vec::new(),
                            project_staging: true,
                        })
                        .unwrap();
                        auths.push(auth);
                    }
                    for auth in &auths {
                        let a = s.claim(auth).unwrap().unwrap();
                        s.start(Start {
                            auth: auth.clone(),
                            job_id: a.job.id,
                            attempt_id: a.attempt_id,
                        })
                        .unwrap();
                    }
                    let polls = measure(
                        || {
                            assert!(s.claim(&auths[0]).unwrap().is_none());
                        },
                        50,
                    );
                    let heartbeat = measure(
                        || {
                            s.heartbeat(&auths[0], false).unwrap();
                        },
                        20,
                    );
                    let stats = measure(
                        || {
                            s.stats().unwrap();
                        },
                        10,
                    );
                    let expire = measure(
                        || {
                            s.expire(now_ms()).unwrap();
                        },
                        20,
                    );
                    rows.push(json!({"repeat":repeat,"history":history,"queued":1000,"workers":worker_count,"policy":policy,"poll":polls,"heartbeat":heartbeat,"stats":stats,"expire":expire,"scheduling":s.metrics}));
                }
            }
        }
    }
    // A queue with no feasible worker exposes queue traversal/allocation costs.
    let mut s = Store::open(":memory:").unwrap();
    for _ in 0..5000 {
        s.submit(spec()).unwrap();
    }
    let jobs = s.list().unwrap();
    let mut plans = Vec::new();
    for policy in [
        Policy::Fifo,
        Policy::BestFit,
        Policy::Priority,
        Policy::Easy,
    ] {
        let timing = measure(
            || {
                assert!(policy.plan(&jobs, &[], 0).is_empty());
            },
            20,
        );
        plans.push(json!({"policy":policy,"jobs":jobs.len(),"timing":timing}));
    }
    println!(
        "{}",
        json!({"kind":"in-memory Store and pure policy microbenchmark; no execution or WAL fsync","rows":rows,"impossible_queue":plans})
    );
}
