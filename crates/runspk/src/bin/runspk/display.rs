//! Terminal presentation only. API JSON and scheduler decisions stay unchanged.
use runspk::model::*;
use serde_json::Value;

pub(super) fn render(kind: &str, value: &Value) -> anyhow::Result<()> {
    match kind {
        "list" | "queue" => {
            let jobs: Vec<Job> = serde_json::from_value(value.clone())?;
            println!(
                "{:<36} {:<11} {:>3} {:>8} {:>3} {:<14} COMMAND",
                "JOB", "STATE", "CPU", "MiB", "GPU", "WORKER"
            );
            for job in &jobs {
                println!(
                    "{:<36} {:<11} {:>3} {:>8} {:>3} {:<14} {}",
                    job.id,
                    state(job.state),
                    job.spec.resources.cpu,
                    job.spec.resources.mem_mib,
                    job.spec.gpu,
                    job.worker_id.as_deref().unwrap_or("—"),
                    job.spec.command.join(" ")
                );
            }
            if jobs.is_empty() {
                println!("No jobs. Submit one with runspk run --trusted -- COMMAND");
            }
        }
        "workers" => {
            let workers: Vec<Worker> = serde_json::from_value(value.clone())?;
            println!(
                "{:<20} {:<10} {:<14} {:<22} {:<12} LAST SEEN",
                "WORKER", "HEALTH", "CPU reserved", "MiB reserved", "GPU reserved"
            );
            for w in &workers {
                println!(
                    "{:<20} {:<10} {:<14} {:<22} {:<12} {}s ago",
                    w.registration.auth.id,
                    format!("{:?}", w.state).to_lowercase(),
                    format!("{}/{}", w.reserved.cpu, w.registration.capacity.cpu),
                    format!("{}/{}", w.reserved.mem_mib, w.registration.capacity.mem_mib),
                    format!("{}/{}", w.reserved_gpus.len(), w.registration.gpus.len()),
                    (now_ms() - w.last_seen_ms).max(0) / 1000
                );
            }
            if workers.is_empty() {
                println!(
                    "No workers. Use runspk start locally, or runspk invite / runspk join for another machine."
                );
            }
        }
        "status" | "cancel" | "resolve" => {
            let job: Job = serde_json::from_value(value.clone())?;
            println!(
                "Job       {}\nState     {}\nCommand   {}\nWorker    {}\nResources {} CPU · {} MiB · {} GPU\nAttempts  {}",
                job.id,
                state(job.state),
                job.spec.command.join(" "),
                job.worker_id.as_deref().unwrap_or("not assigned"),
                job.spec.resources.cpu,
                job.spec.resources.mem_mib,
                job.spec.gpu,
                job.attempts
            );
            if let Some(code) = job.exit_code {
                println!("Exit code {code}");
            }
            if let Some(error) = &job.error {
                println!("Reason    {error}");
            }
            if let Some(id) = &job.spec.workspace_id {
                println!("Project   {id}");
            }
            if job.state == JobState::Queued {
                println!("Next      runspk explain {}", job.id);
            } else if job.state == JobState::Uncertain {
                println!(
                    "Next      restart the owning worker with its original state; fence it before runspk resolve"
                );
            } else {
                println!(
                    "Logs      runspk logs {}\nHistory   runspk attempts {}",
                    job.id, job.id
                );
            }
            if !job.spec.outputs.is_empty() {
                println!("Outputs   runspk fetch {} --into NEW_DIRECTORY", job.id);
            }
        }
        "explain" => {
            println!(
                "Job {} · {} · policy {}",
                value["job_id"].as_str().unwrap_or("?"),
                value["state"].as_str().unwrap_or("?"),
                value["policy"].as_str().unwrap_or("?")
            );
            if value["retry_delayed"] == true {
                println!("Waiting for retry backoff.");
            }
            if let Some(workers) = value["workers"].as_array() {
                if workers.is_empty() {
                    println!("No workers are registered. Use runspk start or add a worker.");
                }
                for w in workers {
                    println!(
                        "{}: {}",
                        w["worker_id"].as_str().unwrap_or("?"),
                        w["reason"].as_str().unwrap_or("unknown")
                    );
                }
            }
        }
        "fetch" => println!(
            "Saved outputs to {} (attempt {})",
            value["directory"].as_str().unwrap_or("?"),
            value["attempt_id"].as_str().unwrap_or("?")
        ),
        "attempts" => {
            let attempts: Vec<Attempt> = serde_json::from_value(value.clone())?;
            println!(
                "{:<36} {:<16} {:<12} {:<5} REASON",
                "ATTEMPT", "WORKER", "STATE", "EXIT"
            );
            for a in attempts {
                println!(
                    "{:<36} {:<16} {:<12} {:<5} {}",
                    a.id,
                    a.auth.id,
                    format!("{:?}", a.state).to_lowercase(),
                    a.exit_code
                        .map(|c| c.to_string())
                        .unwrap_or_else(|| "—".into()),
                    a.error.as_deref().unwrap_or("—")
                );
            }
        }
        "stats" => {
            println!(
                "{} jobs · policy {}",
                value["jobs"],
                value["policy"].as_str().unwrap_or("?")
            );
            if let Some(states) = value["states"].as_object() {
                for (state, count) in states {
                    println!("{state:<12} {count}");
                }
            }
            println!(
                "Resources are reservations, not measured usage. Use runspk workers for capacity."
            );
        }
        "doctor" => {
            if let Some(checks) = value["checks"].as_array() {
                for check in checks {
                    println!(
                        "{} {:<18} {}",
                        if check["ok"] == true { "OK  " } else { "FAIL" },
                        check["check"].as_str().unwrap_or("?"),
                        check["detail"].as_str().unwrap_or("?")
                    );
                }
            }
        }
        _ => println!("{}", serde_json::to_string_pretty(value)?),
    }
    Ok(())
}
fn state(state: JobState) -> String {
    format!("{state:?}").to_lowercase()
}
