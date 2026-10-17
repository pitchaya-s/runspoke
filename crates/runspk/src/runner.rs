use crate::{
    client::Client,
    journal::{Entry, Journal, ProcessIdentity},
    model::*,
};
use anyhow::{Context, Result};
use std::{
    process::Stdio,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
    process::Command,
    sync::watch,
};

struct GroupGuard(i32);
impl GroupGuard {
    fn kill(&mut self) {
        if self.0 > 0 {
            // SAFETY: the positive PID identifies the process group created for this child.
            unsafe {
                libc::kill(-self.0, libc::SIGKILL);
            }
            self.0 = 0;
        }
    }
}
impl Drop for GroupGuard {
    fn drop(&mut self) {
        self.kill();
    }
}
async fn drain(mut pipe: impl AsyncRead + Unpin, output: Arc<Mutex<Vec<u8>>>) {
    let mut buf = [0u8; 4096];
    loop {
        match pipe.read(&mut buf).await {
            Ok(0) | Err(_) => break,
            Ok(n) => {
                let mut out = output.lock().unwrap();
                let remaining = LOG_LIMIT.saturating_sub(out.len());
                out.extend_from_slice(&buf[..n.min(remaining)]);
            }
        }
    }
}
fn snapshot(stdout: &Arc<Mutex<Vec<u8>>>, stderr: &Arc<Mutex<Vec<u8>>>) -> Logs {
    Logs {
        stdout: String::from_utf8_lossy(&stdout.lock().unwrap()).into_owned(),
        stderr: String::from_utf8_lossy(&stderr.lock().unwrap()).into_owned(),
        attempt_id: None,
    }
}
pub(crate) async fn publish(client: &Client, journal: &Arc<Journal>, entry: &Entry) -> Result<()> {
    if let Some(report) = &entry.completion {
        if let Some(bundle) = &entry.artifacts {
            let upload = crate::workspace::ArtifactUpload {
                owner: Start {
                    auth: journal.auth.clone(),
                    job_id: report.job_id.clone(),
                    attempt_id: report.attempt_id.clone(),
                },
                bundle: bundle.clone(),
            };
            if let Err(error) = client
                .post_with_timeout::<()>(
                    "/worker/artifacts",
                    &upload,
                    crate::client::TRANSFER_TIMEOUT,
                )
                .await
            {
                if error
                    .downcast_ref::<crate::client::HttpError>()
                    .is_some_and(|e| e.status == reqwest::StatusCode::GONE)
                {
                    journal.remove(&entry.assignment.attempt_id).await?;
                } else {
                    tracing::warn!(attempt_id=%report.attempt_id,%error,"outputs and completion retained for retry");
                }
                return Ok(());
            }
        }
        match client.post::<Job>("/worker/report", report).await {
            Ok(_) => journal.remove(&entry.assignment.attempt_id).await?,
            Err(e)
                if e.downcast_ref::<crate::client::HttpError>()
                    .is_some_and(|e| e.status == reqwest::StatusCode::GONE) =>
            {
                journal.remove(&entry.assignment.attempt_id).await?;
            }
            Err(e) => {
                tracing::warn!(attempt=%entry.assignment.attempt_id,error=%e,"completion retained for retry")
            }
        }
    }
    Ok(())
}
fn completed(
    status: std::process::ExitStatus,
    deadline: tokio::time::Instant,
) -> (i32, Option<&'static str>) {
    use std::os::unix::process::ExitStatusExt;
    if tokio::time::Instant::now() >= deadline {
        (124, Some("wall time exceeded"))
    } else {
        (
            status
                .code()
                .unwrap_or_else(|| 128 + status.signal().unwrap_or(0)),
            None,
        )
    }
}
pub async fn execute(
    client: Client,
    journal: Arc<Journal>,
    runtime: Option<ContainerRuntime>,
    a: Assignment,
    cpu_ids: Vec<usize>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<String> {
    let id = a.attempt_id.clone();
    let mut entry = Entry {
        assignment: a,
        process: None,
        completion: None,
        runtime,
        artifacts: None,
        keep_workspace: false,
    };
    journal.save(&entry).await?;
    let stdout = Arc::new(Mutex::new(Vec::new()));
    let stderr = Arc::new(Mutex::new(Vec::new()));
    let result = run_process(
        &client,
        &journal,
        &mut entry,
        &mut shutdown,
        &stdout,
        &stderr,
        &cpu_ids,
    )
    .await;
    if let Some(process) = &entry.process {
        process.stop().await?;
    }
    crate::backend::cleanup(&entry).await?;
    let (mut code, mut error) = match result {
        Ok(outcome) => outcome,
        Err(e) => (127, Some(format!("{e:#}"))),
    };
    match crate::staging::collect(&journal, &entry).await {
        Ok(bundle) => entry.artifacts = bundle,
        Err(e) => {
            entry.keep_workspace = true;
            let message = format!(
                "outputs could not be collected: {e:#}; files retained at {}",
                journal.workspace_path(&id)?.display()
            );
            error = Some(error.map_or_else(|| message.clone(), |old| format!("{old}; {message}")));
            if code == 0 {
                code = 74;
            }
        }
    }
    entry.completion = Some(Report {
        auth: journal.auth.clone(),
        job_id: entry.assignment.job.id.clone(),
        attempt_id: id.clone(),
        logs: snapshot(&stdout, &stderr),
        exit_code: Some(code),
        error,
    });
    journal.save(&entry).await?;
    publish(&client, &journal, &entry).await?;
    Ok(id)
}
async fn run_process(
    client: &Client,
    journal: &Arc<Journal>,
    entry: &mut Entry,
    shutdown: &mut watch::Receiver<bool>,
    stdout: &Arc<Mutex<Vec<u8>>>,
    stderr: &Arc<Mutex<Vec<u8>>>,
    cpu_ids: &[usize],
) -> Result<(i32, Option<String>)> {
    entry.assignment.job.spec.validate()?;
    let workspace = crate::staging::prepare(client, journal, entry, shutdown).await?;
    let a = &entry.assignment;
    let lease_started = tokio::time::Instant::now();
    let started: Job = client
        .post(
            "/worker/start",
            &Start {
                auth: journal.auth.clone(),
                job_id: a.job.id.clone(),
                attempt_id: a.attempt_id.clone(),
            },
        )
        .await?;
    anyhow::ensure!(started.state == JobState::Running, "start was cancelled");
    a.job.spec.validate()?;
    // The fixed shell consumes a launch gate; user arguments are passed only through "$@".
    let mut command = Command::new("/bin/sh");
    command
        .args([
            "-c",
            r#"read -r gate || exit 125; exec "$@""#,
            "runspk-gate",
        ])
        .args(crate::backend::launch_args(&a.job, entry.runtime, cpu_ids)?)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .process_group(0)
        .env_remove("RUNSPK_TOKEN")
        .env(
            "CUDA_VISIBLE_DEVICES",
            a.job
                .allocated_gpus
                .iter()
                .filter(|id| !id.starts_with("sim:"))
                .cloned()
                .collect::<Vec<_>>()
                .join(","),
        );
    if let Some(workspace) = workspace {
        command.current_dir(workspace);
    } else if a.job.spec.image.is_none()
        && let Some(cwd) = &a.job.spec.cwd
    {
        command.current_dir(cwd);
    }
    let mem = a.job.spec.resources.mem_mib * 1024 * 1024;
    let native = a.job.spec.image.is_none();
    anyhow::ensure!(
        cpu_ids.len() == a.job.spec.resources.cpu as usize,
        "CPU allocation does not match reservation"
    );
    let cpu_ids = cpu_ids.to_vec();
    // SAFETY: after fork, use only allocation-free syscalls and bounded CPU bitmap operations;
    // all pointers reference initialized local values and the closure takes no locks.
    unsafe {
        command.pre_exec(move || {
            let mut allowed: libc::cpu_set_t = std::mem::zeroed();
            if libc::sched_getaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &mut allowed) != 0
            {
                return Err(std::io::Error::last_os_error());
            }
            let mut selected: libc::cpu_set_t = std::mem::zeroed();
            for cpu in &cpu_ids {
                if *cpu >= libc::CPU_SETSIZE as usize || !libc::CPU_ISSET(*cpu, &allowed) {
                    return Err(std::io::Error::from_raw_os_error(libc::EINVAL));
                }
                libc::CPU_SET(*cpu, &mut selected);
            }
            if cpu_ids.is_empty()
                || libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &selected)
                    != 0
            {
                return Err(std::io::Error::last_os_error());
            }
            let limit = libc::rlimit {
                rlim_cur: mem,
                rlim_max: mem,
            };
            if native && libc::setrlimit(libc::RLIMIT_AS, &limit) != 0 {
                return Err(std::io::Error::last_os_error());
            }
            let limit = libc::rlimit {
                rlim_cur: 0,
                rlim_max: 0,
            };
            if libc::setrlimit(libc::RLIMIT_CORE, &limit) != 0 {
                return Err(std::io::Error::last_os_error());
            }
            if libc::prctl(
                libc::PR_SET_NO_NEW_PRIVS,
                1 as libc::c_ulong,
                0 as libc::c_ulong,
                0 as libc::c_ulong,
                0 as libc::c_ulong,
            ) != 0
            {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let mut child = command.spawn().context("spawn native launch gate")?;
    let pid = child.id().context("child PID missing")? as i32;
    let mut guard = GroupGuard(pid);
    entry.process = Some(ProcessIdentity::capture(pid)?);
    journal.save(entry).await?;
    anyhow::ensure!(
        lease_started.elapsed() < Duration::from_millis(3000) && !*shutdown.borrow(),
        "start lease or worker expired before launch"
    );
    // Start before opening the gate: GNU timeout must never own an earlier deadline.
    let wall_deadline =
        tokio::time::Instant::now() + Duration::from_secs(entry.assignment.job.spec.timeout_secs);
    child
        .stdin
        .take()
        .context("launch gate missing")?
        .write_all(b"go\n")
        .await?;
    let out = tokio::spawn(drain(child.stdout.take().unwrap(), stdout.clone()));
    let err = tokio::spawn(drain(child.stderr.take().unwrap(), stderr.clone()));
    let mut tick = tokio::time::interval(Duration::from_millis(200));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut lease_deadline = lease_started + Duration::from_millis(3000);
    let wall = tokio::time::sleep_until(wall_deadline);
    tokio::pin!(wall);
    let mut last_logs = (0, 0);
    let (code, failure) = loop {
        tokio::select! {
            biased;
            _ = &mut wall => break (124, Some("wall time exceeded")),
            _ = tokio::time::sleep_until(lease_deadline) => {
                break (137, Some("execution lease could not be renewed"));
            }
            _ = shutdown.changed() => break (143, Some("worker shutdown")),
            status = child.wait() => {
                // Tokio timers round to ticks; a completed GNU watchdog can win that tick.
                break completed(status?, wall_deadline);
            }
            _ = tick.tick() => {
                let sent = tokio::time::Instant::now();
                let lengths = (stdout.lock().unwrap().len(),stderr.lock().unwrap().len());
                let mut logs = snapshot(stdout,stderr);
                if lengths == last_logs { logs = Logs::default(); }
                let report = Report {
                    auth:journal.auth.clone(),job_id:entry.assignment.job.id.clone(),
                    attempt_id:entry.assignment.attempt_id.clone(),logs,exit_code:None,error:None,
                };
                // An in-flight HTTP request must not extend the local watchdog deadline.
                let response = tokio::time::timeout_at(lease_deadline.min(wall_deadline),client.post::<Job>("/worker/report",&report)).await;
                match response {
                    Ok(Ok(j)) if j.state == JobState::Running => {
                        last_logs = lengths;
                        lease_deadline = sent + Duration::from_millis(3000);
                    }
                    Ok(Ok(_)) => break (130, Some("execution cancelled or fenced")),
                    Ok(Err(e)) => tracing::debug!(error=%e,"lease renewal failed"),
                    Err(_) if tokio::time::Instant::now() >= wall_deadline => {
                        break (124, Some("wall time exceeded"));
                    }
                    Err(_) => break (137, Some("execution lease could not be renewed")),
                }
            }
        }
    };
    guard.kill();
    let _ = child.wait().await;
    for mut task in [out, err] {
        if tokio::time::timeout(Duration::from_secs(1), &mut task)
            .await
            .is_err()
        {
            task.abort();
        }
    }
    Ok((code, failure.map(str::to_string)))
}
pub fn physical_resources() -> Result<Resources> {
    let mem = std::fs::read_to_string("/proc/meminfo")?;
    let mem_mib = mem
        .lines()
        .find_map(|l| l.strip_prefix("MemTotal:"))
        .context("MemTotal missing")?
        .split_whitespace()
        .next()
        .context("MemTotal empty")?
        .parse::<u64>()?
        / 1024;
    Ok(Resources {
        cpu: std::thread::available_parallelism()?.get() as u32,
        mem_mib,
    })
}

// Keep the existing worker entry point available to library callers.
pub use crate::worker::worker;

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::process::ExitStatusExt;

    #[tokio::test]
    async fn watchdog_exit_and_early_workload_codes_are_distinct() {
        let future = tokio::time::Instant::now() + Duration::from_secs(60);
        for code in [0, 7, 124, 137] {
            assert_eq!(
                completed(std::process::ExitStatus::from_raw(code << 8), future),
                (code, None)
            );
        }
        assert_eq!(
            completed(std::process::ExitStatus::from_raw(libc::SIGKILL), future),
            (137, None)
        );
        let deadline = tokio::time::Instant::now();
        // Await the OS watchdog alone, so its completion deterministically wins.
        let status = Command::new("/usr/bin/timeout")
            .args(["--foreground", "--signal=KILL", "0.01", "/bin/sleep", "60"])
            .status()
            .await
            .unwrap();
        assert!(!status.success());
        assert_eq!(
            completed(status, deadline),
            (124, Some("wall time exceeded"))
        );
        assert_eq!(
            completed(std::process::ExitStatus::from_raw(0), deadline),
            (124, Some("wall time exceeded"))
        );
    }
}
