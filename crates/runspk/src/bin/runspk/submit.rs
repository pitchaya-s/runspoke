use super::parse_mem;
use anyhow::{Context, Result};
use runspk::{
    client::Client,
    model::*,
    workspace::{self, Bundle, ProjectSubmission},
};
use std::path::PathBuf;

#[derive(clap::Args)]
pub(super) struct SubmitArgs {
    #[arg(long)]
    name: Option<String>,
    #[arg(long = "tag")]
    tags: Vec<String>,
    #[arg(long="require",value_parser=runspk::model::parse_label)]
    required_labels: Vec<(String, String)>,
    #[arg(long)]
    cpu: Option<u32>,
    #[arg(long,value_parser=parse_mem)]
    mem: Option<u64>,
    #[arg(long)]
    timeout: Option<u64>,
    #[arg(long)]
    trusted: bool,
    #[arg(long)]
    cwd: Option<String>,
    #[arg(long, default_value_t = 0, allow_hyphen_values = true)]
    priority: i32,
    #[arg(long, default_value_t = 0)]
    retries: u32,
    #[arg(long)]
    key: Option<String>,
    #[arg(long)]
    image: Option<String>,
    #[arg(long, default_value_t = 0)]
    gpu: u32,
    #[arg(long, value_delimiter = ',')]
    gpu_ids: Vec<String>,
    #[arg(long, default_value_t = 0)]
    gpu_mem_mib: u64,
    /// Snapshot a local project; workers need no preprovisioned project path.
    #[arg(long, conflicts_with = "cwd")]
    project: Option<std::path::PathBuf>,
    /// Relative file/directory to retrieve after execution (repeatable).
    #[arg(long = "output")]
    outputs: Vec<String>,
    /// Exclude a literal relative file/directory prefix (repeatable).
    #[arg(long)]
    exclude: Vec<String>,
    /// Read an entire reusable JSON job specification, relative to this file.
    #[arg(long, conflicts_with_all=["cpu","mem","timeout","trusted","cwd","priority","retries","key","image","gpu","gpu_ids","gpu_mem_mib","command","project","outputs","exclude","name","tags","required_labels"])]
    spec: Option<std::path::PathBuf>,
    /// Show the exact file manifest and resources without uploading or running.
    #[arg(long)]
    pub(super) dry_run: bool,

    #[arg(required_unless_present = "spec", last = true)]
    command: Vec<String>,
}

#[derive(serde::Deserialize)]
struct JobFile {
    #[serde(flatten)]
    spec: JobSpec,
    project: Option<PathBuf>,
    #[serde(default)]
    exclude: Vec<String>,
}
pub(super) struct Prepared {
    spec: JobSpec,
    workspace: Option<Bundle>,
    limits: runspk::config::TransferLimits,
}
impl SubmitArgs {
    pub(super) async fn prepare(self, settings: &runspk::config::Settings) -> Result<Prepared> {
        let limits = settings.transfers;
        let (mut spec, project, exclusions) = if let Some(path) = self.spec {
            let data = std::fs::read(&path)
                .with_context(|| format!("read job specification {}", path.display()))?;
            anyhow::ensure!(data.len() <= 1024 * 1024, "job specification exceeds 1 MiB");
            let file: JobFile =
                serde_json::from_slice(&data).context("invalid JSON job specification")?;
            let project = file.project.map(|project| {
                if project.is_absolute() {
                    project
                } else {
                    path.parent()
                        .unwrap_or(std::path::Path::new("."))
                        .join(project)
                }
            });
            (file.spec, project, file.exclude)
        } else {
            (
                JobSpec {
                    name: self.name,
                    tags: self.tags,
                    required_labels: self.required_labels.into_iter().collect(),
                    transfer_limits: None,
                    command: self.command,
                    resources: Resources {
                        cpu: self.cpu.unwrap_or(settings.job_defaults.cpu),
                        mem_mib: self.mem.unwrap_or(settings.job_defaults.mem_mib),
                    },
                    timeout_secs: self.timeout.unwrap_or(settings.job_defaults.timeout_secs),
                    trusted: self.trusted,
                    cwd: self.cwd,
                    priority: self.priority,
                    max_retries: self.retries,
                    submission_key: self.key,
                    image: self.image,
                    gpu: self.gpu,
                    gpu_ids: self.gpu_ids,
                    gpu_mem_mib: self.gpu_mem_mib,
                    workspace_id: None,
                    outputs: self.outputs,
                },
                self.project,
                self.exclude,
            )
        };
        anyhow::ensure!(
            exclusions.is_empty() || project.is_some(),
            "--exclude requires --project"
        );
        let workspace = if let Some(project) = project {
            let bundle = tokio::task::spawn_blocking(move || {
                workspace::capture_project_with(&project, exclusions, limits)
            })
            .await??;
            spec.workspace_id = Some(bundle.id_with(limits)?);
            Some(bundle)
        } else {
            None
        };
        if spec.workspace_id.is_some() {
            spec.transfer_limits = Some(limits);
        }
        spec.validate()?;
        Ok(Prepared {
            spec,
            workspace,
            limits,
        })
    }
}
impl Prepared {
    pub(super) fn manifest(&self) -> Result<serde_json::Value> {
        Ok(
            serde_json::json!({"manifest_version":1,"transfer_limits":self.limits,"spec":self.spec,"bytes":self.workspace.as_ref().map(|b|b.validate_with(self.limits,false)).transpose()?.unwrap_or(0),"files":self.workspace.as_ref().map(|b|b.files.iter().map(|f|&f.path).collect::<Vec<_>>()).unwrap_or_default()}),
        )
    }
}
pub(super) async fn submit(
    client: &Client,
    args: SubmitArgs,
    settings: &runspk::config::Settings,
) -> Result<Job> {
    let info = client.info().await?;
    info.transfer_limits.validate()?;
    let mut settings = settings.clone();
    settings.transfers = settings.transfers.intersect(info.transfer_limits);
    let mut prepared = args.prepare(&settings).await?;
    anyhow::ensure!(
        prepared.spec.required_labels.is_empty() || info.supports("worker-labels-v1"),
        "scheduler does not support worker labels; upgrade before submitting --require"
    );
    anyhow::ensure!(
        (prepared.spec.name.is_none() && prepared.spec.tags.is_empty())
            || info.supports("job-metadata-v1"),
        "scheduler does not support job names/tags; upgrade first"
    );
    if !info.supports("transfer-limits-v1") {
        prepared.spec.transfer_limits = None;
    }
    if let Some(workspace) = prepared.workspace {
        client
            .post_with_timeout(
                "/jobs/project",
                &ProjectSubmission {
                    spec: prepared.spec,
                    workspace,
                },
                settings.transfers.timeout(),
            )
            .await
    } else {
        client.post("/jobs", &prepared.spec).await
    }
}
