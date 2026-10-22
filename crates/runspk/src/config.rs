//! Non-secret user preferences; CLI > environment > settings file > defaults.
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use std::{io::Read, path::PathBuf, str::FromStr, time::Duration};

pub const MIB: usize = 1024 * 1024;
pub const HARD_BUNDLE_BYTES: usize = 64 * MIB;
pub const HARD_HTTP_BYTES: usize = 128 * MIB;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ByteSize(pub usize);
impl FromStr for ByteSize {
    type Err = anyhow::Error;
    fn from_str(value: &str) -> Result<Self> {
        let value = value.trim();
        let split = value
            .find(|c: char| !c.is_ascii_digit())
            .unwrap_or(value.len());
        let number: usize = value[..split]
            .parse()
            .context("size needs a positive integer, e.g. 32MiB or 64MB")?;
        let multiplier = match value[split..].trim().to_ascii_lowercase().as_str() {
            "" | "b" => 1,
            "k" | "ki" | "kib" => 1024,
            "m" | "mi" | "mib" => MIB,
            "g" | "gi" | "gib" => 1024 * MIB,
            "kb" => 1000,
            "mb" => 1_000_000,
            "gb" => 1_000_000_000,
            _ => anyhow::bail!("unknown size unit; use B, KiB, MiB, GiB, KB, MB or GB"),
        };
        let bytes = number
            .checked_mul(multiplier)
            .filter(|n| *n > 0)
            .context("size is zero or overflows this platform")?;
        Ok(Self(bytes))
    }
}
impl Serialize for ByteSize {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&format!("{}B", self.0))
    }
}
impl<'de> Deserialize<'de> for ByteSize {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Value {
            Bytes(usize),
            Text(String),
        }
        match Value::deserialize(d)? {
            Value::Bytes(n) if n > 0 => Ok(Self(n)),
            Value::Bytes(_) => Err(serde::de::Error::custom("size must be positive")),
            Value::Text(s) => s.parse().map_err(serde::de::Error::custom),
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
pub struct TransferLimits {
    pub project_bytes: ByteSize,
    pub artifact_bytes: ByteSize,
    pub max_files: usize,
    pub max_entries: usize,
    pub max_depth: usize,
    pub http_body_bytes: ByteSize,
    pub timeout_secs: u64,
}
impl Default for TransferLimits {
    fn default() -> Self {
        Self {
            project_bytes: ByteSize(32 * MIB),
            artifact_bytes: ByteSize(32 * MIB),
            max_files: 2048,
            max_entries: 4096,
            max_depth: 32,
            http_body_bytes: ByteSize(64 * MIB),
            timeout_secs: 120,
        }
    }
}
impl TransferLimits {
    pub fn validate(&self) -> Result<()> {
        for (name, bytes) in [
            ("project_bytes", self.project_bytes.0),
            ("artifact_bytes", self.artifact_bytes.0),
        ] {
            ensure!(
                (1..=HARD_BUNDLE_BYTES).contains(&bytes),
                "{name} must be 1B..=64MiB; JSON/base64 and SQLite need bounded memory; provision larger datasets externally"
            );
        }
        ensure!(
            (1..=HARD_HTTP_BYTES).contains(&self.http_body_bytes.0),
            "http_body_bytes must be 1B..=128MiB"
        );
        ensure!(
            (1..=16384).contains(&self.max_files),
            "max_files must be 1..=16384"
        );
        ensure!(
            (self.max_files..=65536).contains(&self.max_entries),
            "max_entries must be max_files..=65536"
        );
        ensure!(
            (1..=32).contains(&self.max_depth),
            "max_depth must be 1..=32 (wire path safety ceiling)"
        );
        ensure!(
            (1..=3600).contains(&self.timeout_secs),
            "transfer timeout must be 1..=3600 seconds"
        );
        Ok(())
    }
    pub fn intersect(self, other: Self) -> Self {
        Self {
            project_bytes: ByteSize(self.project_bytes.0.min(other.project_bytes.0)),
            artifact_bytes: ByteSize(self.artifact_bytes.0.min(other.artifact_bytes.0)),
            max_files: self.max_files.min(other.max_files),
            max_entries: self.max_entries.min(other.max_entries),
            max_depth: self.max_depth.min(other.max_depth),
            http_body_bytes: ByteSize(self.http_body_bytes.0.min(other.http_body_bytes.0)),
            timeout_secs: self.timeout_secs.min(other.timeout_secs),
        }
    }
    pub fn supports(&self, required: &Self) -> bool {
        self.project_bytes.0 >= required.project_bytes.0
            && self.artifact_bytes.0 >= required.artifact_bytes.0
            && self.max_files >= required.max_files
            && self.max_entries >= required.max_entries
            && self.max_depth >= required.max_depth
            && self.http_body_bytes.0 >= required.http_body_bytes.0
    }
    pub fn timeout(self) -> Duration {
        Duration::from_secs(self.timeout_secs)
    }
    pub fn hard() -> Self {
        Self {
            project_bytes: ByteSize(HARD_BUNDLE_BYTES),
            artifact_bytes: ByteSize(HARD_BUNDLE_BYTES),
            max_files: 16384,
            max_entries: 65536,
            max_depth: 32,
            http_body_bytes: ByteSize(HARD_HTTP_BYTES),
            timeout_secs: 3600,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct JobDefaults {
    pub cpu: u32,
    pub mem_mib: u64,
    pub timeout_secs: u64,
}
impl Default for JobDefaults {
    fn default() -> Self {
        Self {
            cpu: 1,
            mem_mib: 256,
            timeout_secs: 3600,
        }
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct WorkerDefaults {
    pub cpu: u32,
    pub mem_mib: u64,
    pub labels: std::collections::BTreeMap<String, String>,
}
impl Default for WorkerDefaults {
    fn default() -> Self {
        Self {
            cpu: 1,
            mem_mib: 256,
            labels: Default::default(),
        }
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Settings {
    pub transfers: TransferLimits,
    pub monitor_refresh_secs: u64,
    pub job_defaults: JobDefaults,
    pub worker: WorkerDefaults,
}
impl Default for Settings {
    fn default() -> Self {
        Self {
            transfers: TransferLimits::default(),
            monitor_refresh_secs: 2,
            job_defaults: JobDefaults::default(),
            worker: WorkerDefaults::default(),
        }
    }
}
impl Settings {
    pub fn validate(&self) -> Result<()> {
        self.transfers.validate()?;
        crate::model::Resources {
            cpu: self.worker.cpu,
            mem_mib: self.worker.mem_mib,
        }
        .validate()?;
        crate::model::validate_labels(&self.worker.labels)?;
        ensure!(
            (1..=60).contains(&self.monitor_refresh_secs),
            "monitor_refresh_secs must be 1..=60"
        );
        crate::model::Resources {
            cpu: self.job_defaults.cpu,
            mem_mib: self.job_defaults.mem_mib,
        }
        .validate()?;
        ensure!(
            (1..=604800).contains(&self.job_defaults.timeout_secs),
            "default job timeout must be 1..=604800 seconds"
        );
        Ok(())
    }
}
#[derive(Clone, Debug, Default, clap::Args)]
pub struct SettingsArgs {
    /// Non-secret JSON preferences. Defaults to settings.json beside the XDG connection profile.
    #[arg(long, env = "RUNSPK_SETTINGS", global = true)]
    pub settings: Option<PathBuf>,
    #[arg(long, env = "RUNSPK_PROJECT_LIMIT", global = true)]
    pub project_limit: Option<ByteSize>,
    #[arg(long, env = "RUNSPK_ARTIFACT_LIMIT", global = true)]
    pub artifact_limit: Option<ByteSize>,
    #[arg(long, env = "RUNSPK_MAX_FILES", global = true)]
    pub max_files: Option<usize>,
    #[arg(long, env = "RUNSPK_MAX_ENTRIES", global = true)]
    pub max_entries: Option<usize>,
    #[arg(long, env = "RUNSPK_MAX_DEPTH", global = true)]
    pub max_depth: Option<usize>,
    #[arg(long, env = "RUNSPK_HTTP_BODY_LIMIT", global = true)]
    pub http_body_limit: Option<ByteSize>,
    #[arg(long, env = "RUNSPK_TRANSFER_TIMEOUT", global = true)]
    pub transfer_timeout: Option<u64>,
}
impl SettingsArgs {
    pub fn load(&self) -> Result<Settings> {
        let path = self.settings.clone().map_or_else(
            || {
                Ok::<_, anyhow::Error>(
                    crate::profile::config_path(None)?.with_file_name("settings.json"),
                )
            },
            Ok,
        )?;
        use std::os::unix::fs::OpenOptionsExt;
        let file = {
            std::fs::OpenOptions::new()
                .read(true)
                .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
                .open(&path)
        };
        let mut settings: Settings = match file {
            Ok(file) => {
                ensure!(
                    file.metadata()?.is_file(),
                    "settings must be a regular JSON file"
                );
                let mut bytes = Vec::new();
                file.take(65537).read_to_end(&mut bytes)?;
                ensure!(bytes.len() <= 65536, "settings exceed 64KiB");
                serde_json::from_slice(&bytes)
                    .with_context(|| format!("invalid settings {}", path.display()))?
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound && self.settings.is_none() => {
                Settings::default()
            }
            Err(e) => return Err(e).with_context(|| format!("read settings {}", path.display())),
        };
        macro_rules! assign {
            ($field:ident,$option:ident) => {
                if let Some(value) = self.$option {
                    settings.transfers.$field = value;
                }
            };
        }
        assign!(project_bytes, project_limit);
        assign!(artifact_bytes, artifact_limit);
        assign!(max_files, max_files);
        assign!(max_entries, max_entries);
        assign!(max_depth, max_depth);
        assign!(http_body_bytes, http_body_limit);
        assign!(timeout_secs, transfer_timeout);
        settings.validate()?;
        Ok(settings)
    }
    /// Forward resolved settings through exec without creating temporary credential/config files.
    pub fn apply(&self, command: &mut std::process::Command) -> Result<()> {
        let t = self.load()?.transfers;
        if let Some(path) = &self.settings {
            command.arg("--settings").arg(path);
        }
        for (flag, value) in [
            ("--project-limit", t.project_bytes.0.to_string()),
            ("--artifact-limit", t.artifact_bytes.0.to_string()),
            ("--max-files", t.max_files.to_string()),
            ("--max-entries", t.max_entries.to_string()),
            ("--max-depth", t.max_depth.to_string()),
            ("--http-body-limit", t.http_body_bytes.0.to_string()),
            ("--transfer-timeout", t.timeout_secs.to_string()),
        ] {
            command.arg(flag).arg(value);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;
    #[test]
    fn sizes_and_bounds() {
        assert_eq!("32MiB".parse::<ByteSize>().unwrap().0, 32 * MIB);
        assert_eq!("2MB".parse::<ByteSize>().unwrap().0, 2_000_000);
        for invalid in ["0", "-1", "1.5GiB", "1XB", "18446744073709551615GiB"] {
            assert!(invalid.parse::<ByteSize>().is_err(), "{invalid}");
        }
        assert!(TransferLimits::default().validate().is_ok());
        let t = TransferLimits {
            project_bytes: ByteSize(1024 * MIB),
            ..Default::default()
        };
        assert!(t.validate().is_err());
        assert!(TransferLimits::hard().supports(&TransferLimits::default()));
    }
    #[test]
    fn file_and_cli_precedence_and_invalid_config() {
        #[derive(Parser)]
        struct Cli {
            #[command(flatten)]
            settings: SettingsArgs,
        }
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("settings.json");
        std::fs::write(
            &path,
            r#"{"transfers":{"project_bytes":"8MiB"},"job_defaults":{"cpu":2}}"#,
        )
        .unwrap();
        let cli = Cli::parse_from([
            "test",
            "--settings",
            path.to_str().unwrap(),
            "--project-limit",
            "16MiB",
        ]);
        let s = cli.settings.load().unwrap();
        assert_eq!(s.transfers.project_bytes.0, 16 * MIB);
        assert_eq!(s.job_defaults.cpu, 2);
        assert_eq!(s.transfers.artifact_bytes.0, 32 * MIB);
        std::fs::write(&path, r#"{"transfers":{"max_fiels":4}}"#).unwrap();
        assert!(cli.settings.load().is_err());
        std::fs::write(&path, r#"{"monitor_refresh_secs":0}"#).unwrap();
        assert!(cli.settings.load().is_err());
    }
}
