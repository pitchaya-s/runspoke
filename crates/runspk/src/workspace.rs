//! Bounded immutable file snapshots. No archives, links, special files or implicit setup.
use crate::config::TransferLimits;
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
    #[serde(deserialize_with = "bounded_files")]
    pub files: Vec<SnapshotFile>,
}
fn bounded_files<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Vec<SnapshotFile>, D::Error> {
    struct Files;
    impl<'de> serde::de::Visitor<'de> for Files {
        type Value = Vec<SnapshotFile>;
        fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            write!(f, "at most 16384 snapshot files")
        }
        fn visit_seq<A: serde::de::SeqAccess<'de>>(
            self,
            mut seq: A,
        ) -> std::result::Result<Self::Value, A::Error> {
            let mut files = Vec::new();
            while let Some(file) = seq.next_element()? {
                if files.len() == 16384 {
                    return Err(serde::de::Error::custom(
                        "snapshot exceeds hard ceiling of 16384 files",
                    ));
                }
                files.push(file);
            }
            Ok(files)
        }
    }
    deserializer.deserialize_seq(Files)
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
    #[serde(default)]
    pub fingerprint: Option<String>,
    pub bundle: Bundle,
}

/// Count serialized bytes without allocating another encoded bundle.
pub fn check_encoded_size(value: &impl Serialize, max_bytes: usize) -> Result<()> {
    struct Count {
        bytes: usize,
        max: usize,
    }
    impl Write for Count {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if bytes.len() > self.max.saturating_sub(self.bytes) {
                return Err(std::io::Error::other(
                    "encoded bundle exceeds HTTP body limit; reduce outputs or raise --http-body-limit before submission",
                ));
            }
            self.bytes += bytes.len();
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    serde_json::to_writer(
        Count {
            bytes: 0,
            max: max_bytes,
        },
        value,
    )?;
    Ok(())
}
pub fn check_artifact_size(owner: &crate::model::Start, bundle: &Bundle, max: usize) -> Result<()> {
    #[derive(Serialize)]
    struct Upload<'a> {
        owner: &'a crate::model::Start,
        bundle: &'a Bundle,
    }
    check_encoded_size(&Upload { owner, bundle }, max)
}
pub fn check_submission_size(
    spec: &crate::model::JobSpec,
    workspace: &Bundle,
    max: usize,
) -> Result<()> {
    #[derive(Serialize)]
    struct Submission<'a> {
        spec: &'a crate::model::JobSpec,
        workspace: &'a Bundle,
    }
    check_encoded_size(&Submission { spec, workspace }, max)
}

pub use crate::model::validate_relative_path as validate_path;

impl Bundle {
    pub fn validate(&self) -> Result<usize> {
        self.validate_with(TransferLimits::default(), false)
    }
    pub fn validate_with(&self, limits: TransferLimits, outputs: bool) -> Result<usize> {
        limits.validate()?;
        ensure!(
            self.files.len() <= limits.max_files,
            "snapshot exceeds {} files; configure --max-files or exclude inputs",
            limits.max_files
        );
        let max_bytes = if outputs {
            limits.artifact_bytes.0
        } else {
            limits.project_bytes.0
        };
        let mut bytes = 0usize;
        let mut paths = BTreeSet::new();
        let mut directories = BTreeSet::new();
        for file in &self.files {
            validate_path(&file.path)?;
            ensure!(
                file.path.split('/').count() <= limits.max_depth,
                "snapshot exceeds configured depth {}",
                limits.max_depth
            );
            ensure!(
                paths.insert(file.path.as_str()),
                "duplicate snapshot path: {}",
                file.path
            );
            ensure!(
                file.data.len() <= max_bytes.div_ceil(3) * 4,
                "encoded file {} exceeds decoded limit {} bytes",
                file.path,
                max_bytes
            );
            // Validate base64 without allocating a second decoded copy of every large file.
            let mut decoder = base64::read::DecoderReader::new(file.data.as_bytes(), &STANDARD);
            let decoded = std::io::copy(&mut decoder, &mut std::io::sink())
                .context("invalid snapshot base64")?;
            bytes = bytes
                .checked_add(usize::try_from(decoded)?)
                .context("snapshot size overflow")?;
            ensure!(
                bytes <= max_bytes,
                "snapshot exceeds configured limit {} bytes; exclude large inputs/environments or adjust transfer limits",
                max_bytes
            );
            let mut parent = Path::new(&file.path).parent();
            while let Some(p) = parent.filter(|p| !p.as_os_str().is_empty()) {
                directories.insert(p.to_str().context("non-UTF-8 path")?);
                parent = p.parent();
            }
            ensure!(
                paths.len() + directories.len() <= limits.max_entries,
                "snapshot exceeds configured {} entries",
                limits.max_entries
            );
        }
        for path in &paths {
            ensure!(
                !directories.contains(path),
                "snapshot file is also a directory: {path}"
            );
        }
        Ok(bytes)
    }
    pub fn id(&self) -> Result<String> {
        self.id_with(TransferLimits::default())
    }
    pub fn id_with(&self, limits: TransferLimits) -> Result<String> {
        self.validate_with(limits, false)?;
        struct HashWriter(Sha256);
        impl Write for HashWriter {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                self.0.update(bytes);
                Ok(bytes.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let mut hash = HashWriter(Sha256::new());
        serde_json::to_writer(&mut hash, self)?;
        Ok(format!("{:x}", hash.0.finalize()))
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
    limits: TransferLimits,
}
impl Capture<'_> {
    fn visit(&mut self, dir: &File, name: &str, path: &str) -> Result<()> {
        if self.project && excluded(path, self.exclusions) {
            return Ok(());
        }
        validate_path(path)?;
        ensure!(
            path.split('/').count() <= self.limits.max_depth,
            "snapshot exceeds configured depth {}",
            self.limits.max_depth
        );
        self.entries += 1;
        ensure!(
            self.entries <= self.limits.max_entries,
            "project exceeds {} entries; add exclusions or adjust --max-entries",
            self.limits.max_entries
        );
        let mut file = child(dir, name, libc::O_RDONLY | libc::O_NONBLOCK, 0)
            .with_context(|| format!("read {path}; exclude unsupported entries"))?;
        let metadata = file.metadata()?;
        if metadata.is_dir() {
            let mut names = fs::read_dir(format!("/proc/self/fd/{}", file.as_raw_fd()))?
                .take(self.limits.max_entries + 1)
                .map(|entry| Ok(entry?.file_name()))
                .collect::<std::io::Result<Vec<_>>>()?;
            ensure!(
                names.len() <= self.limits.max_entries,
                "directory exceeds {} entries; add exclusions",
                self.limits.max_entries
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
                self.bundle.files.len() < self.limits.max_files,
                "snapshot exceeds {} files; add exclusions or adjust --max-files",
                self.limits.max_files
            );
            let max_bytes = if self.project {
                self.limits.project_bytes.0
            } else {
                self.limits.artifact_bytes.0
            };
            ensure!(
                metadata.len() <= (max_bytes - self.bytes) as u64,
                "file {path} exceeds remaining snapshot limit {} bytes; exclude it or adjust transfer limits",
                max_bytes - self.bytes
            );
            let mut data = Vec::new();
            (&mut file)
                .take((max_bytes - self.bytes + 1) as u64)
                .read_to_end(&mut data)?;
            self.bytes += data.len();
            ensure!(
                self.bytes <= max_bytes,
                "snapshot exceeds configured {} bytes; exclude large inputs/environments",
                max_bytes
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
pub fn capture_project(root: &Path, exclusions: Vec<String>) -> Result<Bundle> {
    capture_project_with(root, exclusions, TransferLimits::default())
}
pub fn capture_project_with(
    root: &Path,
    mut exclusions: Vec<String>,
    limits: TransferLimits,
) -> Result<Bundle> {
    limits.validate()?;
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
        limits,
    };
    let mut names = fs::read_dir(format!("/proc/self/fd/{}", dir.as_raw_fd()))?
        .take(limits.max_entries + 1)
        .map(|e| Ok(e?.file_name()))
        .collect::<std::io::Result<Vec<_>>>()?;
    ensure!(
        names.len() <= limits.max_entries,
        "directory exceeds {} entries; add exclusions",
        limits.max_entries
    );
    names.sort();
    for name in names {
        let name = name.to_str().context("snapshot paths must be UTF-8")?;
        capture.visit(&dir, name, name)?;
    }
    capture.bundle.files.sort_by(|a, b| a.path.cmp(&b.path));
    capture.bundle.validate_with(limits, false)?;
    Ok(capture.bundle)
}
pub fn capture_outputs(root: &Path, outputs: &[String]) -> Result<Bundle> {
    capture_outputs_with(root, outputs, TransferLimits::default())
}
pub fn capture_outputs_with(
    root: &Path,
    outputs: &[String],
    limits: TransferLimits,
) -> Result<Bundle> {
    limits.validate()?;
    let dir = directory(root)?;
    let mut capture = Capture {
        bundle: Bundle::default(),
        bytes: 0,
        entries: 0,
        exclusions: &[],
        project: false,
        limits,
    };
    for output in outputs {
        let (parent, name) = parent(&dir, output, false)?;
        capture.visit(&parent,&name,output).with_context(||format!("collect output {output}; create every declared output or remove its --output option"))?;
    }
    capture.bundle.files.sort_by(|a, b| a.path.cmp(&b.path));
    capture.bundle.validate_with(limits, true)?;
    Ok(capture.bundle)
}
pub fn materialize(bundle: &Bundle, destination: &Path) -> Result<()> {
    materialize_with(bundle, destination, TransferLimits::default(), false)
}
pub fn materialize_with(
    bundle: &Bundle,
    destination: &Path,
    limits: TransferLimits,
    outputs: bool,
) -> Result<()> {
    bundle.validate_with(limits, outputs)?;
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
        let mut decoder = base64::read::DecoderReader::new(file.data.as_bytes(), &STANDARD);
        std::io::copy(&mut decoder, &mut out)?;
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
