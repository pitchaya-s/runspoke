use crate::model::*;
use crate::state_lock::StateLock;
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use std::{
    fs::{File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};

pub struct Journal {
    dir: PathBuf,
    pub auth: WorkerAuth,
    io: std::sync::Arc<tokio::sync::Semaphore>,
    _lock: StateLock,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ProcessIdentity {
    pub pid: i32,
    pub start_ticks: u64,
    pub boot: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Entry {
    pub assignment: Assignment,
    pub process: Option<ProcessIdentity>,
    pub completion: Option<Report>,
    #[serde(default)]
    pub runtime: Option<ContainerRuntime>,
    #[serde(default)]
    pub artifacts: Option<crate::workspace::Bundle>,
    #[serde(default)]
    pub keep_workspace: bool,
}

fn atomic_write(path: &Path, write: impl FnOnce(&mut File) -> Result<()>) -> Result<()> {
    let temp = path.with_extension("tmp");
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&temp)?;
    write(&mut file)?;
    file.sync_all()?;
    std::fs::rename(temp, path)?;
    File::open(path.parent().context("journal parent missing")?)?.sync_all()?;
    Ok(())
}
impl Journal {
    pub fn open(root: &Path, id: &str) -> Result<Self> {
        ensure!(
            !id.is_empty()
                && id.len() <= 64
                && id
                    .bytes()
                    .all(|c| c.is_ascii_alphanumeric() || b"-_.".contains(&c))
                && id != "."
                && id != "..",
            "invalid worker id"
        );
        let dir = root.join(id);
        std::fs::create_dir_all(&dir)?;
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(dir.join("lock"))?;
        let lock = StateLock::new(lock).context("worker data directory already in use")?;
        let path = dir.join("session");
        let session = match std::fs::read_to_string(&path) {
            Ok(s) => {
                uuid::Uuid::parse_str(&s)?;
                s
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                for item in std::fs::read_dir(&dir)? {
                    ensure!(
                        !item?.path().extension().is_some_and(|e| e == "json"),
                        "worker session is missing but execution journals remain in {}; restore the original session and keep journals for reconciliation",
                        dir.display()
                    );
                }
                let s = uuid::Uuid::new_v4().to_string();
                atomic_write(&path, |file| Ok(file.write_all(s.as_bytes())?))?;
                s
            }
            Err(e) => return Err(e.into()),
        };
        Ok(Self {
            dir,
            auth: WorkerAuth {
                id: id.into(),
                session,
            },
            _lock: lock,
            io: std::sync::Arc::new(tokio::sync::Semaphore::new(1)),
        })
    }
    // The permit lives in the blocking closure: cancelling an await cannot reorder writes
    // or release the journal lock while an fsync is still in progress.
    pub(crate) async fn blocking<T: Send + 'static>(
        self: &std::sync::Arc<Self>,
        f: impl FnOnce(&Self) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        let permit = self.io.clone().acquire_owned().await?;
        let journal = self.clone();
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            f(&journal)
        })
        .await?
    }
    pub async fn save(self: &std::sync::Arc<Self>, e: &Entry) -> Result<()> {
        let e = e.clone();
        self.blocking(move |j| j.save_entry(&e)).await
    }
    fn save_entry(&self, e: &Entry) -> Result<()> {
        uuid::Uuid::parse_str(&e.assignment.attempt_id)?;
        atomic_write(
            &self.dir.join(format!("{}.json", e.assignment.attempt_id)),
            |file| {
                let mut writer = std::io::BufWriter::new(file);
                serde_json::to_writer(&mut writer, e)?;
                writer.flush()?;
                Ok(())
            },
        )
    }
    /// Enumerate identities without loading all unacknowledged artifact blobs into memory.
    pub async fn ids(self: &std::sync::Arc<Self>) -> Result<Vec<String>> {
        self.blocking(Self::read_ids).await
    }
    fn read_ids(&self) -> Result<Vec<String>> {
        let mut ids = Vec::new();
        for item in std::fs::read_dir(&self.dir)? {
            let path = item?.path();
            if path.extension().is_some_and(|e| e == "json") {
                let id = path
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .context("non-UTF8 execution journal name")?;
                uuid::Uuid::parse_str(id).with_context(|| {
                    format!(
                        "invalid execution journal {}; preserve state before recovery",
                        path.display()
                    )
                })?;
                ids.push(id.to_owned());
            }
        }
        ids.sort();
        Ok(ids)
    }
    pub async fn entry(self: &std::sync::Arc<Self>, attempt: &str) -> Result<Option<Entry>> {
        uuid::Uuid::parse_str(attempt)?;
        let attempt = attempt.to_owned();
        self.blocking(move |j| j.read_entry(&attempt)).await
    }
    fn read_entry(&self, attempt: &str) -> Result<Option<Entry>> {
        use std::os::unix::fs::OpenOptionsExt;
        let path = self.dir.join(format!("{attempt}.json"));
        let file = match OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
            .open(&path)
        {
            Ok(file) => file,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        // Covers the hard encoded transfer ceiling, logs, and bounded job metadata.
        ensure!(
            file.metadata()?.is_file() && file.metadata()?.len() <= 160 * 1024 * 1024,
            "execution journal {} is not a bounded regular file; preserve state for recovery",
            path.display()
        );
        let entry:Entry=serde_json::from_reader(std::io::BufReader::new(std::io::Read::take(file,160*1024*1024+1))).with_context(||format!("invalid execution journal {}; preserve state and restore from a trusted backup before recovery",path.display()))?;
        ensure!(
            entry.assignment.attempt_id == attempt,
            "execution journal {} has a mismatched attempt identity; preserve state",
            path.display()
        );
        Ok(Some(entry))
    }
    pub async fn entries(self: &std::sync::Arc<Self>) -> Result<Vec<Entry>> {
        self.blocking(Self::read_entries).await
    }
    fn read_entries(&self) -> Result<Vec<Entry>> {
        let mut entries = Vec::new();
        for id in self.read_ids()? {
            if let Some(entry) = self.read_entry(&id)? {
                entries.push(entry);
            }
        }
        Ok(entries)
    }
    pub async fn remove(self: &std::sync::Arc<Self>, attempt: &str) -> Result<()> {
        uuid::Uuid::parse_str(attempt)?;
        let attempt = attempt.to_owned();
        self.blocking(move |j| j.remove_entry(&attempt)).await
    }
    fn remove_entry(&self, attempt: &str) -> Result<()> {
        let path = self.dir.join(format!("{attempt}.json"));
        match std::fs::read(&path) {
            Ok(data) => {
                let entry: Entry = serde_json::from_slice(&data)?;
                if !entry.keep_workspace {
                    match std::fs::remove_dir_all(self.workspace_path(attempt)?) {
                        Ok(()) => {}
                        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                        Err(e) => return Err(e.into()),
                    }
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
        match std::fs::remove_file(self.dir.join(format!("{attempt}.json"))) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        };
        File::open(&self.dir)?.sync_all()?;
        Ok(())
    }
    pub fn workspace_path(&self, attempt: &str) -> Result<PathBuf> {
        uuid::Uuid::parse_str(attempt)?;
        Ok(self.dir.join("workspaces").join(attempt))
    }
}
fn boot() -> Result<String> {
    Ok(std::fs::read_to_string("/proc/sys/kernel/random/boot_id")?
        .trim()
        .into())
}
fn proc_fields(pid: i32) -> Result<Option<Vec<String>>> {
    match std::fs::read_to_string(format!("/proc/{pid}/stat")) {
        Ok(s) => Ok(Some(
            s.rsplit_once(") ")
                .context("malformed proc stat")?
                .1
                .split_whitespace()
                .map(str::to_string)
                .collect(),
        )),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e.into()),
    }
}
fn group_alive(group: i32) -> Result<bool> {
    for item in std::fs::read_dir("/proc")? {
        let item = item?;
        let Some(pid) = item
            .file_name()
            .to_str()
            .and_then(|s| s.parse::<i32>().ok())
        else {
            continue;
        };
        if let Some(fields) = proc_fields(pid)?
            && fields.get(2).and_then(|s| s.parse::<i32>().ok()) == Some(group)
            && fields.first().is_some_and(|s| s != "Z" && s != "X")
        {
            return Ok(true);
        }
    }
    Ok(false)
}
impl ProcessIdentity {
    pub fn capture(pid: i32) -> Result<Self> {
        let fields = proc_fields(pid)?.context("process vanished before launch gate")?;
        Ok(Self {
            pid,
            start_ticks: fields.get(19).context("start time missing")?.parse()?,
            boot: boot()?,
        })
    }
    pub async fn stop(&self) -> Result<()> {
        let identity = self.clone();
        tokio::task::spawn_blocking(move || identity.stop_group()).await?
    }
    fn stop_group(&self) -> Result<()> {
        ensure!(
            self.pid > 1,
            "invalid journal process group; refusing to signal"
        );
        if boot()? != self.boot {
            return Ok(());
        }
        if let Some(fields) = proc_fields(self.pid)? {
            ensure!(
                fields
                    .get(19)
                    .context("start time missing")?
                    .parse::<u64>()?
                    == self.start_ticks,
                "PID was reused; refusing to kill unrelated process"
            );
        } else {
            ensure!(
                !group_alive(self.pid)?,
                "process group leader is missing with live members; identity is uncertain, reservation retained"
            );
            return Ok(());
        }
        // SAFETY: PID > 1 makes negation select one process group without integer overflow.
        if unsafe { libc::kill(-self.pid, libc::SIGKILL) } != 0 {
            let error = std::io::Error::last_os_error();
            if error.raw_os_error() == Some(libc::ESRCH) {
                return Ok(());
            }
            return Err(error.into());
        }
        for _ in 0..50 {
            if !group_alive(self.pid)? {
                return Ok(());
            }
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
        anyhow::bail!("process group did not stop; reservation retained")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn identity_listing_streamed_roundtrip_and_tamper_rejection() {
        let root = tempfile::tempdir().unwrap();
        let journal = std::sync::Arc::new(Journal::open(root.path(), "bounded").unwrap());
        let spec=serde_json::from_value(serde_json::json!({"command":["true"],"resources":{"cpu":1,"mem_mib":64},"timeout_secs":10,"trusted":true})).unwrap();
        let job = crate::store::Store::open(":memory:")
            .unwrap()
            .submit(spec)
            .unwrap();
        let id = uuid::Uuid::new_v4().to_string();
        let entry = Entry {
            assignment: Assignment {
                job,
                attempt_id: id.clone(),
            },
            process: None,
            completion: None,
            runtime: None,
            artifacts: Some(crate::workspace::Bundle::default()),
            keep_workspace: false,
        };
        journal.save(&entry).await.unwrap();
        assert_eq!(journal.ids().await.unwrap(), vec![id.clone()]);
        assert_eq!(
            journal
                .entry(&id)
                .await
                .unwrap()
                .unwrap()
                .assignment
                .attempt_id,
            id
        );
        assert!(journal.entry("../escape").await.is_err());
        let other = uuid::Uuid::new_v4().to_string();
        std::fs::copy(
            journal.dir.join(format!("{id}.json")),
            journal.dir.join(format!("{other}.json")),
        )
        .unwrap();
        // Scanning identities never deserializes every artifact; a selected entry still verifies ownership.
        assert_eq!(journal.ids().await.unwrap().len(), 2);
        assert!(
            journal
                .entry(&other)
                .await
                .unwrap_err()
                .to_string()
                .contains("mismatched attempt identity")
        );
        assert!(journal.dir.join(format!("{other}.json")).exists());
        std::fs::remove_file(journal.dir.join(format!("{other}.json"))).unwrap();
        journal.remove(&id).await.unwrap();
        assert!(journal.ids().await.unwrap().is_empty());
        assert!(journal.entry(&id).await.unwrap().is_none());
    }
    #[tokio::test(flavor = "current_thread")]
    async fn stalled_io_is_bounded_and_does_not_block_timers() {
        let dir = tempfile::tempdir().unwrap();
        let journal = std::sync::Arc::new(Journal::open(dir.path(), "slow").unwrap());
        let (started, ready) = tokio::sync::oneshot::channel();
        let (release, wait) = std::sync::mpsc::channel();
        let j = journal.clone();
        let io = tokio::spawn(async move {
            j.blocking(move |_| {
                started.send(()).unwrap();
                wait.recv_timeout(std::time::Duration::from_secs(2))?;
                Ok(())
            })
            .await
        });
        ready.await.unwrap();
        assert_eq!(journal.io.available_permits(), 0);
        let start = std::time::Instant::now();
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(30), journal.entries())
                .await
                .is_err()
        );
        assert!(start.elapsed() < std::time::Duration::from_millis(500));
        release.send(()).unwrap();
        io.await.unwrap().unwrap();
        assert!(journal.entries().await.unwrap().is_empty());
    }
    #[tokio::test(flavor = "current_thread")]
    async fn stalled_identity_write_never_opens_an_expired_launch_gate() {
        use axum::middleware::{self, Next};
        use std::sync::{Arc, Mutex};
        let dir = tempfile::tempdir().unwrap();
        let journal = Arc::new(Journal::open(dir.path(), "gate").unwrap());
        let marker = dir.path().join("must-not-run");
        let mut store = crate::store::Store::open(":memory:").unwrap();
        store
            .register(Registration {
                labels: Default::default(),
                transfer_limits: None,
                auth: journal.auth.clone(),
                capacity: Resources {
                    cpu: 1,
                    mem_mib: 128,
                },
                physical: Resources {
                    cpu: 1,
                    mem_mib: 128,
                },
                allow_native: true,
                runtime: None,
                gpus: Vec::new(),
                project_staging: true,
            })
            .unwrap();
        let spec: JobSpec = serde_json::from_value(serde_json::json!({
            "command":["/usr/bin/touch",marker],"resources":{"cpu":1,"mem_mib":64},
            "timeout_secs":10,"trusted":true,"cwd":null,
        }))
        .unwrap();
        let job = store.submit(spec).unwrap();
        let assignment = store.claim(&journal.auth).unwrap().unwrap();
        let store = Arc::new(Mutex::new(store));
        let token = "test-token-with-at-least-32-characters";
        let io = journal.io.clone();
        let router = crate::api::router(store.clone(), token.into())
            .unwrap()
            .layer(middleware::from_fn(
                move |request: axum::http::Request<axum::body::Body>, next: Next| {
                    let io = io.clone();
                    async move {
                        let starting = request.uri().path() == "/worker/start";
                        let response = next.run(request).await;
                        if starting {
                            let permit = io.acquire_owned().await.unwrap();
                            tokio::task::spawn_blocking(move || {
                                std::thread::sleep(std::time::Duration::from_millis(3100));
                                drop(permit);
                            });
                        }
                        response
                    }
                },
            ));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = crate::client::Client::new(
            format!("http://{}", listener.local_addr().unwrap()),
            token.into(),
        )
        .unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        let (_stop, rx) = tokio::sync::watch::channel(false);
        crate::runner::execute(
            client,
            journal,
            None,
            assignment,
            vec![crate::hardware::cpu_ids().unwrap()[0]],
            rx,
        )
        .await
        .unwrap();
        let job = store.lock().unwrap().get(&job.id).unwrap();
        assert_eq!(job.state, JobState::Failed);
        assert!(job.error.unwrap().contains("expired before launch"));
        assert!(
            !marker.exists(),
            "an expired start acknowledgement must never open the gate"
        );
        server.abort();
    }
    #[test]
    fn identity_and_exclusive_session_persist() {
        let dir = tempfile::tempdir().unwrap();
        let j = Journal::open(dir.path(), "a").unwrap();
        let session = j.auth.session.clone();
        assert!(Journal::open(dir.path(), "a").is_err());
        drop(j);
        assert_eq!(
            Journal::open(dir.path(), "a").unwrap().auth.session,
            session
        );
        assert!(Journal::open(dir.path(), "..").is_err());
        assert!(
            ProcessIdentity::capture(std::process::id() as i32)
                .unwrap()
                .start_ticks
                > 0
        );
    }
    #[tokio::test]
    async fn corrupt_or_missing_session_preserves_journals_and_refuses_recovery() {
        let root = tempfile::tempdir().unwrap();
        let journal = std::sync::Arc::new(Journal::open(root.path(), "corrupt").unwrap());
        let path = journal.dir.join(format!("{}.json", uuid::Uuid::new_v4()));
        std::fs::write(&path, b"invalid journal").unwrap();
        let error = format!("{:#}", journal.entries().await.unwrap_err());
        assert!(error.contains(path.to_str().unwrap()));
        let invalid = root.path().join("invalid-session");
        std::fs::create_dir(&invalid).unwrap();
        let session = invalid.join("session");
        std::fs::write(&session, "invalid session").unwrap();
        assert!(Journal::open(root.path(), "invalid-session").is_err());
        let missing = root.path().join("missing-session");
        std::fs::create_dir(&missing).unwrap();
        std::fs::copy(&path, missing.join(path.file_name().unwrap())).unwrap();
        let error = Journal::open(root.path(), "missing-session")
            .err()
            .unwrap()
            .to_string();
        assert!(error.contains("session is missing"), "{error}");
        assert!(
            !missing.join("session").exists(),
            "missing identities must not be silently replaced"
        );
        assert_eq!(std::fs::read(path).unwrap(), b"invalid journal");
    }
    #[tokio::test]
    async fn missing_leader_with_live_members_is_not_signalled() {
        use std::{
            io::Read,
            os::unix::process::CommandExt,
            process::{Command, Stdio},
        };
        struct TestGroup(i32);
        impl Drop for TestGroup {
            fn drop(&mut self) {
                // SAFETY: this group was created and is owned exclusively by this test.
                unsafe {
                    libc::kill(-self.0, libc::SIGKILL);
                }
            }
        }
        let mut child = Command::new("/bin/sh")
            .args(["-c", "read -r gate; sleep 60 >/dev/null 2>&1 & echo $!"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .process_group(0)
            .spawn()
            .unwrap();
        let _group = TestGroup(child.id() as i32);
        let mut identity = ProcessIdentity::capture(child.id() as i32).unwrap();
        identity.start_ticks += 1;
        assert!(
            identity.stop().await.is_err(),
            "live leader identity must match"
        );
        child.stdin.take().unwrap().write_all(b"go\n").unwrap();
        child.wait().unwrap();
        let mut output = String::new();
        child
            .stdout
            .take()
            .unwrap()
            .read_to_string(&mut output)
            .unwrap();
        assert!(output.trim().parse::<i32>().unwrap() > 1);
        assert!(group_alive(identity.pid).unwrap());
        let error = identity.stop().await.unwrap_err().to_string();
        assert!(error.contains("identity is uncertain"), "{error}");
        assert!(
            group_alive(identity.pid).unwrap(),
            "unidentified members must survive"
        );
    }
    #[tokio::test]
    async fn missing_process_group_cleanup_is_idempotent() {
        for pid in [0, 1, -1, i32::MIN] {
            assert!(
                ProcessIdentity {
                    pid,
                    start_ticks: 0,
                    boot: boot().unwrap(),
                }
                .stop()
                .await
                .is_err()
            );
        }
        let p = ProcessIdentity {
            pid: i32::MAX,
            start_ticks: 0,
            boot: boot().unwrap(),
        };
        let started = std::time::Instant::now();
        for _ in 0..20 {
            p.stop().await.unwrap();
        }
        println!(
            "missing-group cleanup: 20 calls in {} microseconds",
            started.elapsed().as_micros()
        );
    }
}
