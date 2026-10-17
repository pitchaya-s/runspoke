//! Bounded immutable file snapshots. No archives, links, special files or implicit setup.
use anyhow::{Context, Result, ensure};
use base64::{Engine, engine::general_purpose::STANDARD};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeSet,
    ffi::CString,
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    os::{
        fd::{AsRawFd, FromRawFd},
        unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt},
    },
    path::Path,
};

pub const MAX_BYTES: usize = 32 * 1024 * 1024;
pub const MAX_HTTP_BODY_BYTES: usize = 64 * 1024 * 1024;
pub const MAX_FILES: usize = 2048;

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct Bundle {
    pub files: Vec<SnapshotFile>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct SnapshotFile {
    pub path: String,
    pub data: String,
    pub executable: bool,
}
#[derive(Serialize, Deserialize)]
pub struct ProjectSubmission {
    pub spec: crate::model::JobSpec,
    pub workspace: Bundle,
}
#[derive(Clone, Serialize, Deserialize)]
pub struct ArtifactUpload {
    pub owner: crate::model::Start,
    pub bundle: Bundle,
}
#[derive(Serialize, Deserialize)]
pub struct Artifacts {
    pub attempt_id: String,
    pub bundle: Bundle,
}

pub use crate::model::validate_relative_path as validate_path;

impl Bundle {
    pub fn validate(&self) -> Result<usize> {
        ensure!(
            self.files.len() <= MAX_FILES,
            "snapshot exceeds {MAX_FILES} files"
        );
        let mut bytes = 0;
        let mut paths = BTreeSet::new();
        for file in &self.files {
            validate_path(&file.path)?;
            ensure!(
                paths.insert(file.path.as_str()),
                "duplicate snapshot path: {}",
                file.path
            );
            ensure!(
                file.data.len() <= MAX_BYTES.div_ceil(3) * 4,
                "encoded file exceeds {} MiB",
                MAX_BYTES / (1024 * 1024)
            );
            bytes += STANDARD
                .decode(&file.data)
                .context("invalid snapshot base64")?
                .len();
            ensure!(
                bytes <= MAX_BYTES,
                "snapshot exceeds {} MiB; exclude large inputs and environments",
                MAX_BYTES / (1024 * 1024)
            );
        }
        for path in &paths {
            let mut parent = Path::new(path).parent();
            while let Some(p) = parent {
                ensure!(
                    !paths.contains(p.to_str().context("non-UTF-8 path")?),
                    "snapshot file is also a directory: {}",
                    p.display()
                );
                parent = p.parent();
            }
        }
        Ok(bytes)
    }
    pub fn id(&self) -> Result<String> {
        self.validate()?;
        Ok(format!("{:x}", Sha256::digest(serde_json::to_vec(self)?)))
    }
}

// Every traversal is descriptor-relative and refuses links, including ancestor components.
fn child(dir: &File, name: &str, flags: i32, mode: u32) -> Result<File> {
    let name = CString::new(name)?;
    // SAFETY: dir is live; name is NUL-terminated; mode is provided for O_CREAT.
    let fd = unsafe {
        libc::openat(
            dir.as_raw_fd(),
            name.as_ptr(),
            flags | libc::O_NOFOLLOW | libc::O_CLOEXEC,
            mode,
        )
    };
    ensure!(
        fd >= 0,
        "cannot open snapshot entry (links and special files are unsupported): {}",
        std::io::Error::last_os_error()
    );
    // SAFETY: openat returned a new owned descriptor, transferred exactly once.
    Ok(unsafe { File::from_raw_fd(fd) })
}
fn directory(path: &Path) -> Result<File> {
    OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW)
        .open(path)
        .with_context(|| format!("open real project directory {}", path.display()))
}
fn parent(root: &File, path: &str, create: bool) -> Result<(File, String)> {
    validate_path(path)?;
    let mut parts: Vec<_> = path.split('/').collect();
    let name = parts.pop().context("missing file name")?.to_owned();
    let mut dir = root.try_clone()?;
    for part in parts {
        if create {
            let c = CString::new(part)?;
            // SAFETY: dir and name remain live for mkdirat; the mode is a permission mask.
            if unsafe { libc::mkdirat(dir.as_raw_fd(), c.as_ptr(), 0o700) } != 0 {
                let e = std::io::Error::last_os_error();
                if e.kind() != std::io::ErrorKind::AlreadyExists {
                    return Err(e.into());
                }
            } else {
                dir.sync_all()?;
            }
        }
        dir = child(&dir, part, libc::O_RDONLY | libc::O_DIRECTORY, 0)?;
    }
    Ok((dir, name))
}
fn excluded(path: &str, exclusions: &[String]) -> bool {
    path.split('/').any(|name| {
        [
            ".git",
            ".tools",
            ".cache",
            ".hg",
            ".svn",
            ".runspk",
            ".venv",
            "venv",
            "target",
            "node_modules",
            "__pycache__",
            ".ssh",
            ".aws",
            ".kube",
            ".netrc",
            "id_rsa",
            "id_ed25519",
        ]
        .contains(&name)
            || name == ".env"
            || name.starts_with(".env.")
    }) || exclusions
        .iter()
        .any(|p| path == p || path.strip_prefix(p).is_some_and(|s| s.starts_with('/')))
}

struct Capture<'a> {
    bundle: Bundle,
    bytes: usize,
    entries: usize,
    exclusions: &'a [String],
    project: bool,
}
impl Capture<'_> {
    fn visit(&mut self, dir: &File, name: &str, path: &str) -> Result<()> {
        if self.project && excluded(path, self.exclusions) {
            return Ok(());
        }
        validate_path(path)?;
        self.entries += 1;
        ensure!(
            self.entries <= 4096,
            "project exceeds 4096 entries; add exclusions"
        );
        let mut file = child(dir, name, libc::O_RDONLY | libc::O_NONBLOCK, 0)
            .with_context(|| format!("read {path}; exclude unsupported entries"))?;
        let metadata = file.metadata()?;
        if metadata.is_dir() {
            let mut names = fs::read_dir(format!("/proc/self/fd/{}", file.as_raw_fd()))?
                .take(4097)
                .map(|entry| Ok(entry?.file_name()))
                .collect::<std::io::Result<Vec<_>>>()?;
            ensure!(
                names.len() <= 4096,
                "directory exceeds 4096 entries; add exclusions"
            );
            names.sort();
            for name in names {
                let name = name.to_str().context("snapshot paths must be UTF-8")?;
                self.visit(&file, name, &format!("{path}/{name}"))?;
            }
        } else {
            ensure!(
                metadata.is_file() && metadata.nlink() == 1,
                "links and special files cannot be staged: {path}"
            );
            ensure!(
                self.bundle.files.len() < MAX_FILES,
                "snapshot exceeds {MAX_FILES} files"
            );
            let mut data = Vec::new();
            (&mut file)
                .take((MAX_BYTES - self.bytes + 1) as u64)
                .read_to_end(&mut data)?;
            self.bytes += data.len();
            ensure!(
                self.bytes <= MAX_BYTES,
                "snapshot exceeds {} MiB; exclude large inputs/environments",
                MAX_BYTES / (1024 * 1024)
            );
            ensure!(
                !(self.project
                    && data.len() <= 65536
                    && serde_json::from_slice::<crate::profile::Profile>(&data).is_ok()),
                "runspk connection credentials found in {path}; move or explicitly exclude this file before submitting"
            );
            self.bundle.files.push(SnapshotFile {
                path: path.into(),
                data: STANDARD.encode(data),
                executable: metadata.mode() & 0o111 != 0,
            });
        }
        Ok(())
    }
}
pub fn capture_project(root: &Path, mut exclusions: Vec<String>) -> Result<Bundle> {
    let dir = directory(root)?;
    if root.join(".runspkignore").try_exists()? {
        let mut ignore = child(&dir, ".runspkignore", libc::O_RDONLY | libc::O_NONBLOCK, 0)?;
        ensure!(
            ignore.metadata()?.is_file(),
            ".runspkignore must be a regular file"
        );
        let mut text = String::new();
        (&mut ignore).take(65537).read_to_string(&mut text)?;
        ensure!(text.len() <= 65536, ".runspkignore exceeds 64 KiB");
        exclusions.extend(
            text.lines()
                .map(str::trim)
                .filter(|p| !p.is_empty() && !p.starts_with('#'))
                .map(str::to_owned),
        );
    }
    for path in &mut exclusions {
        *path = path.trim_end_matches('/').to_owned();
        validate_path(path)?;
    }
    let mut capture = Capture {
        bundle: Bundle::default(),
        bytes: 0,
        entries: 0,
        exclusions: &exclusions,
        project: true,
    };
    let mut names = fs::read_dir(format!("/proc/self/fd/{}", dir.as_raw_fd()))?
        .take(4097)
        .map(|e| Ok(e?.file_name()))
        .collect::<std::io::Result<Vec<_>>>()?;
    ensure!(
        names.len() <= 4096,
        "directory exceeds 4096 entries; add exclusions"
    );
    names.sort();
    for name in names {
        let name = name.to_str().context("snapshot paths must be UTF-8")?;
        capture.visit(&dir, name, name)?;
    }
    capture.bundle.files.sort_by(|a, b| a.path.cmp(&b.path));
    capture.bundle.validate()?;
    Ok(capture.bundle)
}
pub fn capture_outputs(root: &Path, outputs: &[String]) -> Result<Bundle> {
    let dir = directory(root)?;
    let mut capture = Capture {
        bundle: Bundle::default(),
        bytes: 0,
        entries: 0,
        exclusions: &[],
        project: false,
    };
    for output in outputs {
        let (parent, name) = parent(&dir, output, false)?;
        capture.visit(&parent,&name,output).with_context(||format!("collect output {output}; create every declared output or remove its --output option"))?;
    }
    capture.bundle.files.sort_by(|a, b| a.path.cmp(&b.path));
    capture.bundle.validate()?;
    Ok(capture.bundle)
}
pub fn materialize(bundle: &Bundle, destination: &Path) -> Result<()> {
    bundle.validate()?;
    fs::DirBuilder::new()
        .mode(0o700)
        .create(destination)
        .with_context(|| {
            format!(
                "create new destination {}; existing paths are never overwritten",
                destination.display()
            )
        })?;
    let root = directory(destination)?;
    for file in &bundle.files {
        let (dir, name) = parent(&root, &file.path, true)?;
        let mut out = child(
            &dir,
            &name,
            libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL,
            if file.executable { 0o700 } else { 0o600 },
        )?;
        out.write_all(&STANDARD.decode(&file.data)?)?;
        out.sync_all()?;
        dir.sync_all()?;
    }
    root.sync_all()?;
    // Persist the new destination name as well as the files and nested directories.
    File::open(
        destination
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new(".")),
    )?
    .sync_all()?;
    Ok(())
}
