use crate::{journal::Entry, model::*};
use anyhow::{Context, Result, ensure};
use std::{process::Stdio, time::Duration};
use tokio::process::Command;

pub fn launch_args(
    job: &Job,
    runtime: Option<ContainerRuntime>,
    cpu_ids: &[usize],
) -> Result<Vec<String>> {
    let spec = &job.spec;
    let mut args = vec![
        "/usr/bin/timeout".into(),
        "--foreground".into(),
        "--signal=KILL".into(),
        spec.timeout_secs.to_string(),
    ];
    if let Some(image) = &spec.image {
        let runtime = runtime.context("container runtime is not enabled on this worker")?;
        args.extend([
            runtime.command().into(),
            "run".into(),
            "--name".into(),
            format!(
                "runspk-{}",
                job.attempt_id.as_ref().context("attempt missing")?
            ),
            "--label".into(),
            format!("runspk.attempt={}", job.attempt_id.as_ref().unwrap()),
            "--pull=never".into(),
            "--network=none".into(),
            "--read-only".into(),
            "--cap-drop=ALL".into(),
            "--security-opt=no-new-privileges".into(),
            "--pids-limit=128".into(),
            "--user=65534:65534".into(),
            "--tmpfs=/tmp:rw,noexec,nosuid,nodev,size=16m".into(),
            "--shm-size=8m".into(),
            "--log-driver=none".into(),
            format!("--cpus={}", spec.resources.cpu),
            format!(
                "--cpuset-cpus={}",
                cpu_ids
                    .iter()
                    .map(usize::to_string)
                    .collect::<Vec<_>>()
                    .join(",")
            ),
            format!("--memory={}m", spec.resources.mem_mib),
            format!("--memory-swap={}m", spec.resources.mem_mib),
        ]);
        if let Some(cwd) = &spec.cwd {
            args.extend(["--workdir".into(), cwd.clone()]);
        }
        let real: Vec<_> = job
            .allocated_gpus
            .iter()
            .filter(|id| !id.starts_with("sim:"))
            .cloned()
            .collect();
        if !real.is_empty() {
            ensure!(
                runtime == ContainerRuntime::Docker,
                "physical container GPUs currently require Docker NVIDIA support"
            );
            args.extend(["--gpus".into(), format!("\"device={}\"", real.join(","))]);
        }
        if real.is_empty() {
            args.push("--env=NVIDIA_VISIBLE_DEVICES=void".into());
        }
        args.extend([
            "--entrypoint".into(),
            spec.command[0].clone(),
            image.clone(),
        ]);
        args.extend_from_slice(&spec.command[1..]);
    } else {
        args.extend_from_slice(&spec.command);
    }
    Ok(args)
}
async fn runtime_output(runtime: ContainerRuntime, args: &[&str]) -> Result<std::process::Output> {
    let mut command = Command::new(runtime.command());
    command
        .args(args)
        .stdin(Stdio::null())
        .kill_on_drop(true)
        .env_remove("RUNSPK_TOKEN");
    tokio::time::timeout(Duration::from_secs(3), command.output())
        .await
        .context("container cleanup timed out")?
        .map_err(Into::into)
}
pub async fn cleanup(entry: &Entry) -> Result<()> {
    if entry.assignment.job.spec.image.is_none() {
        return Ok(());
    }
    let runtime = entry.runtime.context("container journal lacks runtime")?;
    let name = format!("runspk-{}", entry.assignment.attempt_id);
    let inspect = runtime_output(
        runtime,
        &[
            "inspect",
            "--format",
            "{{ index .Config.Labels \"runspk.attempt\" }}",
            &name,
        ],
    )
    .await?;
    if !inspect.status.success() {
        // Only a successful empty listing proves absence; inspect can fail on daemon errors.
        let containers = runtime_output(
            runtime,
            &[
                "container",
                "ls",
                "--all",
                "--filter",
                &format!("name={name}"),
                "--format",
                "{{.ID}}",
            ],
        )
        .await?;
        ensure!(
            containers.status.success() && containers.stdout.is_empty(),
            "container absence could not be confirmed; inspect: {}; listing: {}",
            String::from_utf8_lossy(&inspect.stderr),
            String::from_utf8_lossy(&containers.stderr)
        );
        return Ok(());
    }
    ensure!(
        String::from_utf8_lossy(&inspect.stdout).trim() == entry.assignment.attempt_id,
        "container ownership label mismatch; refusing removal"
    );
    let rm = runtime_output(runtime, &["rm", "--force", &name]).await?;
    ensure!(
        rm.status.success(),
        "container removal failed: {}",
        String::from_utf8_lossy(&rm.stderr)
    );
    Ok(())
}

pub async fn check(runtime: ContainerRuntime) -> Result<()> {
    let out = runtime_output(runtime, &["info"]).await?;
    ensure!(
        out.status.success(),
        "container runtime unavailable: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    Ok(())
}
