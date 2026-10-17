//! Private local connection settings. No host services, SSH configuration or shell files change.
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use std::io::Read;
use std::{
    fs::{self, OpenOptions},
    io::Write,
    os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
};

#[derive(Clone, Serialize, Deserialize)]
pub struct Profile {
    pub url: String,
    pub token: String,
    #[serde(default)]
    pub state_dir: Option<PathBuf>,
}
fn xdg(variable: &str, fallback: &str) -> Result<PathBuf> {
    if let Some(path) = std::env::var_os(variable) {
        ensure!(
            Path::new(&path).is_absolute(),
            "{variable} must be absolute"
        );
        return Ok(path.into());
    }
    Ok(PathBuf::from(
        std::env::var_os("HOME")
            .context("HOME is missing; pass --config and use RUNSPK_TOKEN/RUNSPK_URL")?,
    )
    .join(fallback))
}
pub fn config_path(explicit: Option<PathBuf>) -> Result<PathBuf> {
    match explicit {
        Some(path) => Ok(path),
        None => Ok(xdg("XDG_CONFIG_HOME", ".config")?.join("runspk/config.json")),
    }
}
pub fn state_dir() -> Result<PathBuf> {
    Ok(xdg("XDG_STATE_HOME", ".local/state")?.join("runspk"))
}
pub fn private_dir(path: &Path) -> Result<()> {
    fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(path)?;
    let metadata = fs::symlink_metadata(path)?;
    ensure!(
        metadata.is_dir() && metadata.mode() & 0o077 == 0,
        "{} must be a private directory (chmod 700 or choose another path)",
        path.display()
    );
    Ok(())
}
impl Profile {
    pub fn load(path: &Path) -> Result<Option<Self>> {
        let file = match OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
            .open(path)
        {
            Ok(file) => file,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e).with_context(|| format!("read config {}", path.display())),
        };
        let metadata = file.metadata()?;
        // SAFETY: geteuid has no arguments or pointer preconditions.
        ensure!(
            metadata.is_file()
                && metadata.uid() == unsafe { libc::geteuid() }
                && metadata.permissions().mode() & 0o077 == 0,
            "{} must be owned by you and mode 600",
            path.display()
        );
        ensure!(metadata.len() <= 65536, "configuration exceeds 64 KiB");
        let mut data = Vec::new();
        file.take(65537).read_to_end(&mut data)?;
        ensure!(data.len() <= 65536, "configuration exceeds 64 KiB");
        let profile: Self = serde_json::from_slice(&data)
            .with_context(|| format!("invalid JSON config {}", path.display()))?;
        crate::client::Client::new(profile.url.clone(), profile.token.clone())?;
        Ok(Some(profile))
    }
    pub fn save(&self, path: &Path) -> Result<()> {
        crate::client::Client::new(self.url.clone(), self.token.clone())?;
        let parent = path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        if !parent.exists() {
            private_dir(parent)?;
        }
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(path)
            .with_context(|| {
                format!(
                    "create new config {}; existing files are never overwritten",
                    path.display()
                )
            })?;
        file.write_all(&serde_json::to_vec_pretty(self)?)?;
        file.write_all(b"\n")?;
        file.sync_all()?;
        fs::File::open(parent)?.sync_all()?;
        Ok(())
    }
    pub fn local(url: String, state: Option<PathBuf>) -> Result<Self> {
        let state = state.map_or_else(state_dir, Ok)?;
        let state = if state.is_absolute() {
            state
        } else {
            std::env::current_dir()?.join(state)
        };
        private_dir(&state)?;
        Ok(Self {
            url,
            token: format!(
                "{}{}",
                uuid::Uuid::new_v4().simple(),
                uuid::Uuid::new_v4().simple()
            ),
            state_dir: Some(state),
        })
    }
    pub fn state(&self) -> Result<PathBuf> {
        self.state_dir.clone().map_or_else(state_dir, Ok)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn profile_rejects_public_permissions_links_and_overwrite() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.json");
        let profile = Profile::local(
            "http://127.0.0.1:8765".into(),
            Some(dir.path().join("state")),
        )
        .unwrap();
        profile.save(&path).unwrap();
        assert_eq!(Profile::load(&path).unwrap().unwrap().token, profile.token);
        assert!(profile.save(&path).is_err());
        fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();
        assert!(Profile::load(&path).is_err());
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
        let link = dir.path().join("link.json");
        std::os::unix::fs::symlink(&path, &link).unwrap();
        assert!(Profile::load(&link).is_err());
        fs::remove_file(&path).unwrap();
        assert!(Profile::load(&path).unwrap().is_none());
    }
}
