use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    time::{SystemTime, UNIX_EPOCH},
};

pub fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct Resources {
    pub cpu: u32,
    pub mem_mib: u64,
}

impl Resources {
    pub fn fits(self, available: Self) -> bool {
        self.cpu <= available.cpu && self.mem_mib <= available.mem_mib
    }
    pub fn validate(self) -> anyhow::Result<()> {
        anyhow::ensure!(self.cpu > 0 && self.cpu <= 4096, "cpu must be in 1..=4096");
        anyhow::ensure!(
            self.mem_mib > 0 && self.mem_mib <= 16 * 1024 * 1024,
            "mem must be in 1..=16777216 MiB"
        );
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct JobSpec {
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub tags: Vec<String>,
    #[serde(default)]
    pub required_labels: BTreeMap<String, String>,
    #[serde(default)]
    pub transfer_limits: Option<crate::config::TransferLimits>,
    pub command: Vec<String>,
    pub resources: Resources,
    pub timeout_secs: u64,
    pub trusted: bool,
    pub cwd: Option<String>,
    #[serde(default)]
    pub priority: i32,
    #[serde(default)]
    pub max_retries: u32,
    #[serde(default)]
    pub submission_key: Option<String>,
    #[serde(default)]
    pub image: Option<String>,
    #[serde(default)]
    pub gpu: u32,
    #[serde(default)]
    pub gpu_ids: Vec<String>,
    #[serde(default)]
    pub gpu_mem_mib: u64,
    #[serde(default)]
    pub workspace_id: Option<String>,
    #[serde(default)]
    pub outputs: Vec<String>,
}

impl JobSpec {
    pub fn validate(&self) -> anyhow::Result<()> {
        self.resources.validate()?;
        if let Some(name) = &self.name {
            anyhow::ensure!(
                valid_metadata(name, 256),
                "job name must be 1..256 bytes without control characters"
            );
        }
        anyhow::ensure!(
            self.tags.len() <= 64 && self.tags.iter().all(|tag| valid_metadata(tag, 64)),
            "at most 64 tags of 1..64 bytes without control characters are supported"
        );
        validate_labels(&self.required_labels)?;
        if let Some(limits) = self.transfer_limits {
            limits.validate()?;
        }
        if let Some(id) = &self.workspace_id {
            anyhow::ensure!(
                id.len() == 64
                    && id
                        .bytes()
                        .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
                "invalid workspace fingerprint"
            );
            anyhow::ensure!(
                self.image.is_none() && self.cwd.is_none(),
                "project staging requires trusted native execution without --cwd; container inputs belong in images"
            );
        }
        anyhow::ensure!(
            self.outputs.is_empty() || self.workspace_id.is_some(),
            "outputs require --project or a workspace_id"
        );
        anyhow::ensure!(
            self.outputs.len() <= 64,
            "at most 64 output paths are supported"
        );
        for path in &self.outputs {
            validate_relative_path(path)?;
        }
        for (i, path) in self.outputs.iter().enumerate() {
            anyhow::ensure!(
                !self.outputs.iter().enumerate().any(|(j, other)| i != j
                    && (path == other
                        || path.strip_prefix(other).is_some_and(|s| s.starts_with('/')))),
                "output paths must not overlap"
            );
        }
        anyhow::ensure!(self.max_retries <= 10, "max retries must be <= 10");
        if let Some(key) = &self.submission_key {
            anyhow::ensure!(
                !key.is_empty() && key.len() <= 128,
                "submission key must be 1..128 bytes"
            );
        }
        anyhow::ensure!(
            (-100..=100).contains(&self.priority),
            "priority must be in -100..=100"
        );
        anyhow::ensure!(
            self.trusted || self.image.is_some(),
            "native execution requires explicit --trusted consent"
        );
        if let Some(image) = &self.image {
            anyhow::ensure!(
                !image.is_empty()
                    && image.len() <= 256
                    && !image.starts_with('-')
                    && image
                        .bytes()
                        .all(|c| c.is_ascii_alphanumeric() || b"._-/:@".contains(&c)),
                "invalid container image reference"
            );
        }
        anyhow::ensure!(
            self.gpu <= 64 && (self.gpu_ids.is_empty() || self.gpu_ids.len() == self.gpu as usize),
            "GPU affinity must specify exactly gpu distinct identities"
        );
        let unique: std::collections::BTreeSet<_> = self.gpu_ids.iter().collect();
        anyhow::ensure!(
            unique.len() == self.gpu_ids.len() && self.gpu_ids.iter().all(|s| valid_gpu_id(s)),
            "invalid or duplicate GPU identity"
        );
        anyhow::ensure!(
            self.gpu > 0 || self.gpu_mem_mib == 0,
            "GPU memory requires a GPU request"
        );
        anyhow::ensure!(
            !self.command.is_empty() && !self.command[0].is_empty(),
            "command is required"
        );
        anyhow::ensure!(
            self.command.len() <= 256
                && self
                    .command
                    .iter()
                    .all(|s| s.len() <= 16384 && !s.contains('\0')),
            "invalid or oversized command"
        );
        anyhow::ensure!(
            (1..=604800).contains(&self.timeout_secs),
            "timeout must be in 1..=604800 seconds"
        );
        if let Some(cwd) = &self.cwd {
            anyhow::ensure!(
                std::path::Path::new(cwd).is_absolute() && cwd.len() <= 4096 && !cwd.contains('\0'),
                "cwd must be an absolute worker-side path"
            );
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, clap::ValueEnum, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum JobState {
    Queued,
    Assigned,
    Running,
    Cancelling,
    Uncertain,
    Succeeded,
    Failed,
    Cancelled,
}
#[derive(Clone, Debug, Default, Serialize, Deserialize, clap::Args)]
pub struct JobFilter {
    /// Case-insensitive substring of ID, name or command.
    #[arg(long)]
    pub search: Option<String>,
    /// Exact job tag.
    #[arg(long)]
    pub tag: Option<String>,
    #[arg(long, value_enum)]
    pub state: Option<JobState>,
    #[arg(long)]
    pub worker: Option<String>,
    /// Return jobs with a sequence greater than this cursor.
    #[arg(long, default_value_t = 0)]
    #[serde(default)]
    pub after: i64,
    #[arg(long, value_parser = clap::value_parser!(u32).range(1..=1000))]
    pub limit: Option<u32>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PruneRequest {
    pub before_ms: i64,
    pub limit: u32,
    #[serde(default)]
    pub dry_run: bool,
}
impl JobState {
    pub fn terminal(self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed | Self::Cancelled)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Job {
    pub id: String,
    pub seq: i64,
    pub spec: JobSpec,
    pub state: JobState,
    pub submitted_ms: i64,
    pub started_ms: Option<i64>,
    pub finished_ms: Option<i64>,
    pub attempt_id: Option<String>,
    pub exit_code: Option<i32>,
    pub error: Option<String>,
    #[serde(default)]
    pub worker_id: Option<String>,
    #[serde(default)]
    pub attempts: u32,
    #[serde(default)]
    pub retry_at_ms: i64,
    #[serde(default)]
    pub lease_until_ms: i64,
    #[serde(default)]
    pub allocated_gpus: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Assignment {
    pub job: Job,
    pub attempt_id: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Logs {
    pub stdout: String,
    pub stderr: String,
    #[serde(default)]
    pub attempt_id: Option<String>,
}

pub const LOG_LIMIT: usize = 1024 * 1024;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WorkerAuth {
    pub id: String,
    pub session: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Heartbeat {
    #[serde(flatten)]
    pub auth: WorkerAuth,
    #[serde(default)]
    pub telemetry: Option<crate::telemetry::HostTelemetry>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Registration {
    pub auth: WorkerAuth,
    pub capacity: Resources,
    pub physical: Resources,
    pub allow_native: bool,
    #[serde(default)]
    pub runtime: Option<ContainerRuntime>,
    #[serde(default)]
    pub gpus: Vec<Gpu>,
    #[serde(default)]
    pub project_staging: bool,
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
    #[serde(default)]
    pub transfer_limits: Option<crate::config::TransferLimits>,
}

impl Registration {
    pub fn validate_metadata(&self) -> anyhow::Result<()> {
        validate_labels(&self.labels)?;
        if let Some(limits) = self.transfer_limits {
            limits.validate()?;
        }
        Ok(())
    }
}

fn valid_metadata(value: &str, max_bytes: usize) -> bool {
    !value.is_empty() && value.len() <= max_bytes && !value.chars().any(char::is_control)
}

pub fn validate_labels(labels: &BTreeMap<String, String>) -> anyhow::Result<()> {
    anyhow::ensure!(labels.len() <= 64, "at most 64 labels are supported");
    for (key, value) in labels {
        anyhow::ensure!(
            !key.is_empty()
                && key.len() <= 64
                && key
                    .bytes()
                    .all(|c| c.is_ascii_alphanumeric() || b"-_./".contains(&c)),
            "label keys must be 1..64 ASCII letters, digits, dots, dashes, underscores or slashes"
        );
        anyhow::ensure!(
            valid_metadata(value, 256),
            "label values must be 1..256 bytes without control characters"
        );
    }
    Ok(())
}

pub fn parse_label(input: &str) -> Result<(String, String), String> {
    let (key, value) = input.split_once('=').ok_or("label must use KEY=VALUE")?;
    let label = (key.to_string(), value.to_string());
    validate_labels(&BTreeMap::from([label.clone()])).map_err(|error| error.to_string())?;
    Ok(label)
}
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WorkerState {
    Online,
    Offline,
    Draining,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Worker {
    #[serde(default)]
    pub telemetry: Option<crate::telemetry::HostTelemetry>,
    #[serde(default)]
    pub telemetry_received_ms: Option<i64>,
    pub registration: Registration,
    pub state: WorkerState,
    pub last_seen_ms: i64,
    pub reserved: Resources,
    #[serde(default)]
    pub reserved_gpus: Vec<String>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Report {
    pub auth: WorkerAuth,
    pub job_id: String,
    pub attempt_id: String,
    pub logs: Logs,
    pub exit_code: Option<i32>,
    pub error: Option<String>,
}

pub const LEASE_MS: i64 = 5000;
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AttemptState {
    Assigned,
    Running,
    Uncertain,
    Finished,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Attempt {
    pub id: String,
    pub job_id: String,
    pub auth: WorkerAuth,
    pub state: AttemptState,
    pub lease_until_ms: i64,
    pub exit_code: Option<i32>,
    pub error: Option<String>,
    pub finished_ms: Option<i64>,
    #[serde(default)]
    pub gpu_ids: Vec<String>,
    #[serde(default)]
    pub assigned_ms: i64,
    #[serde(default)]
    pub started_ms: Option<i64>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Start {
    pub auth: WorkerAuth,
    pub job_id: String,
    pub attempt_id: String,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, clap::ValueEnum, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ContainerRuntime {
    Docker,
    Podman,
}
impl ContainerRuntime {
    pub fn command(self) -> &'static str {
        match self {
            Self::Docker => "docker",
            Self::Podman => "podman",
        }
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Gpu {
    pub id: String,
    pub name: String,
    pub memory_mib: u64,
    pub free_memory_mib: u64,
    pub utilization_percent: u32,
    pub simulated: bool,
}
pub fn valid_gpu_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 128
        && id
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || b"-_:./".contains(&c))
}

pub fn validate_relative_path(path: &str) -> anyhow::Result<()> {
    anyhow::ensure!(
        !path.is_empty()
            && path.len() <= 512
            && !path.contains('\0')
            && path
                .split('/')
                .all(|c| !c.is_empty() && c != "." && c != "..")
            && std::path::Path::new(path)
                .components()
                .all(|c| matches!(c, std::path::Component::Normal(_))),
        "use a relative path without '.', '..', repeated slashes or absolute components: {path:?}"
    );
    anyhow::ensure!(
        path.split('/').count() <= 32,
        "path exceeds 32 directory levels"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn validates_trust_and_resources() {
        let mut s = JobSpec {
            name: None,
            tags: Vec::new(),
            required_labels: BTreeMap::new(),
            transfer_limits: None,
            command: vec!["true".into()],
            resources: Resources {
                cpu: 1,
                mem_mib: 64,
            },
            timeout_secs: 1,
            trusted: false,
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
        };
        assert!(s.validate().is_err());
        s.trusted = true;
        assert!(s.validate().is_ok());
        s.resources.cpu = 0;
        assert!(s.validate().is_err());
    }
}
