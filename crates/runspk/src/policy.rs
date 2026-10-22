use crate::model::*;
use clap::ValueEnum;
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, ValueEnum, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum Policy {
    #[default]
    Fifo,
    BestFit,
    Priority,
    Easy,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Placement {
    pub job_id: String,
    pub worker_id: String,
    pub gpu_ids: Vec<String>,
}

pub trait SchedulingPolicy {
    fn plan(&self, jobs: &[Job], workers: &[Worker], now: i64) -> Vec<Placement>;
}
pub fn active(j: &Job) -> bool {
    matches!(
        j.state,
        JobState::Assigned | JobState::Running | JobState::Cancelling | JobState::Uncertain
    )
}
pub fn available(w: &Worker, jobs: &[Job]) -> Resources {
    let mut free = w.registration.capacity;
    for j in jobs
        .iter()
        .filter(|j| active(j) && j.worker_id.as_deref() == Some(&w.registration.auth.id))
    {
        free.cpu = free.cpu.saturating_sub(j.spec.resources.cpu);
        free.mem_mib = free.mem_mib.saturating_sub(j.spec.resources.mem_mib);
    }
    free
}
fn compatible(w: &Worker, j: &Job) -> bool {
    w.state == WorkerState::Online
        && j.spec
            .required_labels
            .iter()
            .all(|(key, value)| w.registration.labels.get(key) == Some(value))
        && (j.spec.workspace_id.is_none() || w.registration.project_staging)
        && (j.spec.workspace_id.is_none()
            || w.registration
                .transfer_limits
                .unwrap_or_default()
                .supports(&j.spec.transfer_limits.unwrap_or_default()))
        && if j.spec.image.is_some() {
            w.registration.runtime.is_some()
        } else {
            w.registration.allow_native
        }
}
pub fn gpu_selection(w: &Worker, j: &Job, jobs: &[Job]) -> Option<Vec<String>> {
    if j.spec.gpu == 0 {
        return Some(Vec::new());
    }
    let used: std::collections::BTreeSet<_> = jobs
        .iter()
        .filter(|j| active(j))
        .flat_map(|j| j.allocated_gpus.iter())
        .collect();
    let mut eligible: Vec<_> = w
        .registration
        .gpus
        .iter()
        .filter(|g| {
            !used.contains(&g.id)
                && g.memory_mib >= j.spec.gpu_mem_mib
                && (j.spec.gpu_ids.is_empty() || j.spec.gpu_ids.contains(&g.id))
        })
        .map(|g| g.id.clone())
        .collect();
    eligible.sort();
    if eligible.len() < j.spec.gpu as usize {
        None
    } else {
        eligible.truncate(j.spec.gpu as usize);
        Some(eligible)
    }
}
fn score(w: &Worker, free: Resources, request: Resources) -> u64 {
    (free.cpu - request.cpu) as u64 * 1_000_000 / w.registration.capacity.cpu as u64
        + (free.mem_mib - request.mem_mib) * 1_000_000 / w.registration.capacity.mem_mib
}
// Include dispatch and termination slack; overdue/unreconciled work has no predicted release.
pub fn expected_end(j: &Job, now: i64) -> i64 {
    if j.state == JobState::Uncertain {
        return i64::MAX;
    }
    let end = j
        .started_ms
        .unwrap_or(now)
        .saturating_add((j.spec.timeout_secs as i64 + 2) * 1000);
    if end <= now { i64::MAX } else { end }
}
fn reservation(head: &Job, workers: &[Worker], jobs: &[Job], now: i64) -> Option<(i64, String)> {
    workers
        .iter()
        .filter(|w| {
            compatible(w, head)
                && head.spec.resources.fits(w.registration.capacity)
                && gpu_selection(w, head, &[]).is_some()
        })
        .filter_map(|w| {
            let mut ends: Vec<i64> = jobs
                .iter()
                .filter(|j| active(j) && j.worker_id.as_deref() == Some(&w.registration.auth.id))
                .map(|j| expected_end(j, now))
                .collect();
            ends.sort_unstable();
            ends.dedup();
            ends.into_iter()
                .filter(|t| *t < i64::MAX)
                .find(|t| {
                    let still: Vec<Job> = jobs
                        .iter()
                        .filter(|j| !active(j) || expected_end(j, now) > *t)
                        .cloned()
                        .collect();
                    head.spec.resources.fits(available(w, &still))
                        && gpu_selection(w, head, &still).is_some()
                })
                .map(|t| (t, w.registration.auth.id.clone()))
        })
        .min()
}
impl SchedulingPolicy for Policy {
    fn plan(&self, jobs: &[Job], workers: &[Worker], now: i64) -> Vec<Placement> {
        let mut waiting: Vec<&Job> = jobs
            .iter()
            .filter(|j| j.state == JobState::Queued && j.retry_at_ms <= now)
            .collect();
        waiting.sort_by_key(|j| {
            if matches!(self, Self::Priority | Self::Easy) {
                (
                    -(j.spec.priority as i64 + (now - j.submitted_ms).max(0) / 5000),
                    j.seq,
                )
            } else {
                (0, j.seq)
            }
        });
        let mut waiting: std::collections::VecDeque<_> = waiting.into();
        let mut workers = workers.to_vec();
        workers.sort_by(|a, b| a.registration.auth.id.cmp(&b.registration.auth.id));
        let mut running: Vec<_> = jobs.iter().filter(|j| active(j)).cloned().collect();
        let mut result = Vec::new();
        while !waiting.is_empty() {
            let head = &waiting[0];
            if !workers.iter().any(|w| {
                compatible(w, head)
                    && head.spec.resources.fits(w.registration.capacity)
                    && gpu_selection(w, head, &[]).is_some()
            }) {
                waiting.pop_front();
                continue;
            }
            let pick = |j: &Job, guard: Option<&(i64, String)>| -> Option<String> {
                let mut feasible: Vec<_> = workers
                    .iter()
                    .filter_map(|w| {
                        let free = available(w, &running);
                        if !compatible(w, j)
                            || !j.spec.resources.fits(free)
                            || gpu_selection(w, j, &running).is_none()
                        {
                            return None;
                        }
                        if let Some((time, id)) = guard
                            && &w.registration.auth.id == id
                            && now.saturating_add((j.spec.timeout_secs as i64 + 2) * 1000) > *time
                        {
                            return None;
                        }
                        Some((
                            if *self == Self::BestFit {
                                score(w, free, j.spec.resources)
                            } else {
                                0
                            },
                            w.registration.auth.id.clone(),
                        ))
                    })
                    .collect();
                feasible.sort();
                feasible.into_iter().next().map(|(_, id)| id)
            };
            let selected = if let Some(w) = pick(head, None) {
                Some((0, w))
            } else if *self == Self::Easy {
                reservation(head, &workers, &running, now).and_then(|guard| {
                    waiting
                        .iter()
                        .enumerate()
                        .skip(1)
                        .find_map(|(i, j)| pick(j, Some(&guard)).map(|w| (i, w)))
                })
            } else {
                None
            };
            let Some((index, worker_id)) = selected else {
                break;
            };
            let mut j = waiting.remove(index).unwrap().clone();
            let w = workers
                .iter()
                .find(|w| w.registration.auth.id == worker_id)
                .unwrap();
            let gpu_ids = gpu_selection(w, &j, &running).unwrap();
            result.push(Placement {
                job_id: j.id.clone(),
                worker_id: worker_id.clone(),
                gpu_ids: gpu_ids.clone(),
            });
            j.allocated_gpus = gpu_ids;
            j.state = JobState::Assigned;
            j.worker_id = Some(worker_id);
            j.started_ms = Some(now);
            running.push(j);
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    fn job(seq: i64, cpu: u32, mem: u64, timeout: u64) -> Job {
        Job {
            id: seq.to_string(),
            seq,
            spec: JobSpec {
                name: None,
                tags: Vec::new(),
                required_labels: Default::default(),
                transfer_limits: None,
                command: vec!["true".into()],
                resources: Resources { cpu, mem_mib: mem },
                timeout_secs: timeout,
                trusted: true,
                cwd: None,
                priority: 0,
                max_retries: 0,
                submission_key: None,
                image: None,
                gpu: 0,
                gpu_ids: Vec::new(),
                gpu_mem_mib: 0,
                workspace_id: None,
                outputs: Vec::new(),
            },
            state: JobState::Queued,
            submitted_ms: 0,
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
        }
    }
    fn worker(id: &str, cpu: u32, mem: u64) -> Worker {
        Worker {
            telemetry: None,
            telemetry_received_ms: None,
            registration: Registration {
                auth: WorkerAuth {
                    id: id.into(),
                    session: "session".into(),
                },
                capacity: Resources { cpu, mem_mib: mem },
                physical: Resources { cpu, mem_mib: mem },
                allow_native: true,
                runtime: None,
                gpus: Vec::new(),
                project_staging: true,
                labels: Default::default(),
                transfer_limits: None,
            },
            state: WorkerState::Online,
            last_seen_ms: 0,
            reserved: Resources::default(),
            reserved_gpus: Vec::new(),
        }
    }
    #[test]
    fn easy_protects_head_and_fills_short_gap() {
        let mut running = job(0, 2, 20, 10);
        running.state = JobState::Running;
        running.worker_id = Some("a".into());
        running.started_ms = Some(0);
        let jobs = vec![
            running,
            job(1, 4, 40, 30),
            job(2, 2, 20, 2),
            job(3, 2, 20, 20),
        ];
        let workers = vec![worker("a", 4, 100)];
        assert!(Policy::Fifo.plan(&jobs, &workers, 0).is_empty());
        assert_eq!(
            Policy::Easy.plan(&jobs, &workers, 0),
            vec![Placement {
                job_id: "2".into(),
                worker_id: "a".into(),
                gpu_ids: Vec::new()
            }]
        );
    }
    #[test]
    fn best_fit_preserves_large_worker_and_priority_ages() {
        let workers = vec![worker("a", 8, 800), worker("b", 2, 200)];
        let jobs = vec![job(1, 2, 100, 1), job(2, 8, 400, 1)];
        assert_eq!(Policy::BestFit.plan(&jobs, &workers, 0).len(), 2);
        assert_eq!(Policy::Fifo.plan(&jobs, &workers, 0).len(), 1);
        let mut jobs = vec![job(1, 1, 1, 1), job(2, 1, 1, 1)];
        jobs[1].spec.priority = 100;
        jobs[1].submitted_ms = 600_000;
        assert_eq!(
            Policy::Priority.plan(&jobs, &[worker("a", 1, 100)], 600_000)[0].job_id,
            "1"
        );
    }
    #[test]
    fn gpu_memory_affinity_and_exclusive_multi_gpu_allocations() {
        let mut w = worker("a", 8, 800);
        w.registration.gpus = crate::hardware::select_gpus(&[], 2, "a").unwrap();
        let mut first = job(1, 1, 100, 10);
        first.spec.gpu = 2;
        first.spec.gpu_mem_mib = 4000;
        let mut second = job(2, 1, 100, 10);
        second.spec.gpu = 1;
        let p = Policy::Easy.plan(&[first.clone(), second.clone()], &[w.clone()], 0);
        assert_eq!(p.len(), 1);
        assert_eq!(p[0].gpu_ids.len(), 2);
        first.spec.gpu_mem_mib = 5000;
        let p = Policy::Fifo.plan(&[first, second.clone()], &[w.clone()], 0);
        assert_eq!(p[0].job_id, "2");
        second.spec.gpu_ids = vec![w.registration.gpus[1].id.clone()];
        assert_eq!(
            Policy::Fifo.plan(&[second], &[w], 0)[0].gpu_ids,
            vec!["sim:a:1"]
        );
    }
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(128))]
        #[test]
        fn plans_never_overcommit(requests in prop::collection::vec((1u32..9,1u64..1000,1u64..50,0u32..4),1..40),cpu in 1u32..17,mem in 1u64..2000) {
            let jobs:Vec<_>=requests.iter().enumerate().map(|(i,(c,m,t,g))|{let mut j=job(i as i64,*c,*m,*t);j.spec.gpu = *g;j}).collect();
            let mut workers=vec![worker("a",cpu,mem),worker("b",cpu+1,mem+1)];for w in &mut workers {w.registration.gpus=crate::hardware::select_gpus(&[],3,&w.registration.auth.id).unwrap();}
            for policy in [Policy::Fifo,Policy::BestFit,Policy::Priority,Policy::Easy] {
                let placements=policy.plan(&jobs,&workers,0);prop_assert_eq!(&placements,&policy.plan(&jobs,&workers,0));
                let mut placed=std::collections::BTreeSet::new();let mut devices=std::collections::BTreeSet::new();let mut running=Vec::new();
                for p in placements {prop_assert!(placed.insert(p.job_id.clone()));let mut j=jobs.iter().find(|j|j.id==p.job_id).unwrap().clone();let w=workers.iter().find(|w|w.registration.auth.id==p.worker_id).unwrap();prop_assert!(j.spec.resources.fits(available(w,&running)));prop_assert_eq!(p.gpu_ids.len(),j.spec.gpu as usize);for id in &p.gpu_ids {prop_assert!(devices.insert(id.clone()));}j.allocated_gpus=p.gpu_ids;j.state=JobState::Running;j.worker_id=Some(p.worker_id);running.push(j);}
            }
        }
    }
}
