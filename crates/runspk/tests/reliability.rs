use runspk::{client::Client, model::*, store::Store};
use std::{
    process::{Child, Command, Stdio},
    sync::{Arc, Mutex},
    time::Duration,
};
fn spec() -> JobSpec {
    JobSpec {
        name: None,
        tags: Vec::new(),
        required_labels: Default::default(),
        transfer_limits: None,
        command: vec!["/bin/sleep".into(), "10".into()],
        resources: Resources {
            cpu: 1,
            mem_mib: 64,
        },
        timeout_secs: 15,
        trusted: true,
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
    }
}
fn registration() -> Registration {
    Registration {
        labels: Default::default(),
        transfer_limits: None,
        auth: WorkerAuth {
            id: "test".into(),
            session: uuid::Uuid::new_v4().to_string(),
        },
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
    }
}
fn report(auth: &WorkerAuth, a: &Assignment, code: i32) -> Report {
    Report {
        auth: auth.clone(),
        job_id: a.job.id.clone(),
        attempt_id: a.attempt_id.clone(),
        logs: Logs::default(),
        exit_code: Some(code),
        error: None,
    }
}

#[test]
fn stopped_completion_corrects_partial_utf8_and_remains_immutable() {
    let mut store = Store::open(":memory:").unwrap();
    let registration = registration();
    let auth = registration.auth.clone();
    store.register(registration).unwrap();
    let job = store.submit(spec()).unwrap();
    let assignment = store.claim(&auth).unwrap().unwrap();
    store
        .start(Start {
            auth: auth.clone(),
            job_id: job.id.clone(),
            attempt_id: assignment.attempt_id.clone(),
        })
        .unwrap();
    let mut progress = report(&auth, &assignment, 0);
    progress.exit_code = None;
    progress.logs.stdout = String::from_utf8_lossy(&[0xc3]).into_owned();
    store.report(progress).unwrap();
    let mut completion = report(&auth, &assignment, 0);
    completion.logs.stdout = "é".into();
    store.report(completion.clone()).unwrap();
    assert_eq!(store.logs(&job.id).unwrap().stdout, "é");
    completion.logs.stdout = "changed on replay".into();
    store.report(completion).unwrap();
    assert_eq!(store.logs(&job.id).unwrap().stdout, "é");
}

#[tokio::test]
async fn pending_completion_is_not_dispatched_again() {
    use axum::{Json, Router, http::StatusCode, routing::post};
    use std::sync::atomic::{AtomicUsize, Ordering};
    let dir = tempfile::tempdir().unwrap();
    let journal = Arc::new(runspk::journal::Journal::open(dir.path(), "test").unwrap());
    let mut r = registration();
    r.auth = journal.auth.clone();
    let mut s = Store::open(":memory:").unwrap();
    let w = s.register(r.clone()).unwrap();
    s.submit(spec()).unwrap();
    let assignment = s.claim(&r.auth).unwrap();
    let starts = Arc::new(AtomicUsize::new(0));
    let counter = starts.clone();
    let router = Router::new()
        .route(
            "/worker/register",
            post({
                let w = w.clone();
                move || async move { Json(w) }
            }),
        )
        .route(
            "/worker/heartbeat",
            post({
                let w = w.clone();
                move || async move { Json(w) }
            }),
        )
        .route("/worker/drain", post(move || async move { Json(w) }))
        .route(
            "/worker/poll",
            post(move || {
                let a = assignment.clone();
                async move { Json(a) }
            }),
        )
        .route(
            "/worker/start",
            post(move || {
                counter.fetch_add(1, Ordering::SeqCst);
                async { StatusCode::SERVICE_UNAVAILABLE }
            }),
        )
        .route(
            "/worker/report",
            post(|| async { StatusCode::SERVICE_UNAVAILABLE }),
        );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let client = Client::new(
        format!("http://{}", listener.local_addr().unwrap()),
        "test-token-with-at-least-32-characters".into(),
    )
    .unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let (stop, rx) = tokio::sync::watch::channel(false);
    let worker = tokio::spawn(runspk::runner::worker(client, r, journal, rx));
    tokio::time::sleep(Duration::from_millis(1200)).await;
    stop.send(true).unwrap();
    worker.await.unwrap().unwrap();
    server.abort();
    assert_eq!(
        starts.load(Ordering::SeqCst),
        1,
        "a durable completion must be replayed, never overwritten by another dispatch"
    );
}
#[test]
fn duplicate_gpu_advertisement_and_oversized_capacity_are_rejected() {
    let mut s = Store::open(":memory:").unwrap();
    let mut r = registration();
    r.gpus = runspk::hardware::select_gpus(&[], 1, "test").unwrap();
    s.register(r.clone()).unwrap();
    r.auth.id = "other".into();
    r.auth.session = uuid::Uuid::new_v4().to_string();
    assert!(s.register(r.clone()).is_err());
    r.gpus.clear();
    r.capacity.cpu = 2;
    assert!(s.register(r).is_err());
}
#[test]
fn idle_worker_can_resize_but_active_worker_cannot() {
    let mut s = Store::open(":memory:").unwrap();
    let mut r = registration();
    r.physical.cpu = 2;
    s.register(r.clone()).unwrap();
    r.capacity.cpu = 2;
    s.register(r.clone()).unwrap();
    let j = s.submit(spec()).unwrap();
    let a = s.claim(&r.auth).unwrap().unwrap();
    r.capacity.cpu = 1;
    assert!(s.register(r.clone()).is_err());
    s.cancel(&j.id).unwrap();
    s.register(r).unwrap();
    assert_eq!(
        s.attempt(&a.attempt_id).unwrap().state,
        AttemptState::Finished
    );
}
#[test]
fn expired_started_attempt_holds_resources_until_reconciled() {
    let mut s = Store::open(":memory:").unwrap();
    let r = registration();
    let auth = r.auth.clone();
    s.register(r).unwrap();
    let j = s.submit(spec()).unwrap();
    let a = s.claim(&auth).unwrap().unwrap();
    s.start(Start {
        auth: auth.clone(),
        job_id: j.id.clone(),
        attempt_id: a.attempt_id.clone(),
    })
    .unwrap();
    s.expire(now_ms() + LEASE_MS + 1).unwrap();
    assert_eq!(s.get(&j.id).unwrap().state, JobState::Uncertain);
    assert_eq!(s.workers().unwrap()[0].reserved.cpu, 1);
    assert!(s.claim(&auth).unwrap().is_none());
    assert!(
        s.start(Start {
            auth: auth.clone(),
            job_id: j.id.clone(),
            attempt_id: a.attempt_id.clone()
        })
        .is_err()
    );
    s.report(report(&auth, &a, 137)).unwrap();
    assert_eq!(s.get(&j.id).unwrap().state, JobState::Failed);
    assert_eq!(s.workers().unwrap()[0].reserved.cpu, 0);
}
#[test]
fn unstarted_expiry_submission_dedup_retry_and_lost_completion_ack() {
    let mut s = Store::open(":memory:").unwrap();
    let r = registration();
    let auth = r.auth.clone();
    s.register(r).unwrap();
    let mut spec = spec();
    spec.max_retries = 1;
    spec.submission_key = Some("daily-build".into());
    let j = s.submit(spec.clone()).unwrap();
    assert_eq!(s.submit(spec.clone()).unwrap().id, j.id);
    spec.command = vec!["different".into()];
    assert!(s.submit(spec).is_err());
    let old = s.claim(&auth).unwrap().unwrap();
    s.expire(now_ms() + LEASE_MS + 1).unwrap();
    assert_eq!(s.get(&j.id).unwrap().state, JobState::Queued);
    assert!(
        s.start(Start {
            auth: auth.clone(),
            job_id: j.id.clone(),
            attempt_id: old.attempt_id
        })
        .is_err()
    );
    let a = s.claim(&auth).unwrap().unwrap();
    s.start(Start {
        auth: auth.clone(),
        job_id: j.id.clone(),
        attempt_id: a.attempt_id.clone(),
    })
    .unwrap();
    let failure = report(&auth, &a, 1);
    assert_eq!(s.report(failure.clone()).unwrap().state, JobState::Queued);
    assert_eq!(s.report(failure).unwrap().attempts, 1);
    assert!(s.claim(&auth).unwrap().is_none());
    std::thread::sleep(Duration::from_millis(1050));
    let a = s.claim(&auth).unwrap().unwrap();
    s.start(Start {
        auth: auth.clone(),
        job_id: j.id.clone(),
        attempt_id: a.attempt_id.clone(),
    })
    .unwrap();
    let done = report(&auth, &a, 0);
    assert_eq!(s.report(done.clone()).unwrap().state, JobState::Succeeded);
    assert_eq!(s.report(done).unwrap().state, JobState::Succeeded);
    s.cancel(&j.id).unwrap();
    assert_eq!(s.get(&j.id).unwrap().state, JobState::Succeeded);
    assert!(
        s.attempts(&j.id)
            .unwrap()
            .iter()
            .all(|a| a.state == AttemptState::Finished)
    );
}
#[test]
fn concurrent_claims_have_one_attempt_and_cancel_completion_races_are_terminal() {
    let mut s = Store::open(":memory:").unwrap();
    let r = registration();
    let auth = r.auth.clone();
    s.register(r).unwrap();
    let j = s.submit(spec()).unwrap();
    let s = Arc::new(Mutex::new(s));
    let handles: Vec<_> = (0..8)
        .map(|_| {
            let s = s.clone();
            let auth = auth.clone();
            std::thread::spawn(move || s.lock().unwrap().claim(&auth).unwrap().unwrap().attempt_id)
        })
        .collect();
    let ids: std::collections::BTreeSet<_> =
        handles.into_iter().map(|h| h.join().unwrap()).collect();
    assert_eq!(ids.len(), 1);
    let attempt_id = ids.into_iter().next().unwrap();
    let a = Assignment {
        job: j.clone(),
        attempt_id,
    };
    s.lock()
        .unwrap()
        .start(Start {
            auth: auth.clone(),
            job_id: j.id.clone(),
            attempt_id: a.attempt_id.clone(),
        })
        .unwrap();
    let cancel = {
        let s = s.clone();
        let id = j.id.clone();
        std::thread::spawn(move || s.lock().unwrap().cancel(&id).unwrap())
    };
    let complete = {
        let s = s.clone();
        let auth = auth.clone();
        std::thread::spawn(move || s.lock().unwrap().report(report(&auth, &a, 0)).unwrap())
    };
    cancel.join().unwrap();
    complete.join().unwrap();
    let mut s = s.lock().unwrap();
    let terminal = s.get(&j.id).unwrap().state;
    assert!(matches!(
        terminal,
        JobState::Succeeded | JobState::Cancelled
    ));
    assert_eq!(s.cancel(&j.id).unwrap().state, terminal);
    assert_eq!(s.workers().unwrap()[0].reserved.cpu, 0);
}

struct Cluster {
    dir: tempfile::TempDir,
    address: String,
    token: String,
    scheduler: Child,
    worker: Option<Child>,
}
impl Cluster {
    fn scheduler(dir: &std::path::Path, address: &str, token: &str) -> Child {
        Command::new(env!("CARGO_BIN_EXE_runspk-scheduler"))
            .args([
                "--bind",
                address,
                "--db",
                dir.join("state.sqlite").to_str().unwrap(),
            ])
            .env("RUNSPK_TOKEN", token)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap()
    }
    async fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let socket = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = socket.local_addr().unwrap().to_string();
        drop(socket);
        let token = "test-token-with-at-least-32-characters".to_string();
        let scheduler = Self::scheduler(dir.path(), &address, &token);
        let c = Self {
            dir,
            address,
            token,
            scheduler,
            worker: None,
        };
        c.ready().await;
        c
    }
    fn client(&self) -> Client {
        Client::new(format!("http://{}", self.address), self.token.clone()).unwrap()
    }
    async fn ready(&self) {
        for _ in 0..100 {
            if self.client().get::<Vec<Job>>("/jobs").await.is_ok() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
        panic!("scheduler unavailable");
    }
    async fn worker(&mut self) {
        self.worker_capacity("1").await;
    }
    async fn worker_capacity(&mut self, cpu: &str) {
        self.worker = Some(
            Command::new(env!("CARGO_BIN_EXE_runspk-worker"))
                .args([
                    "--id",
                    "test",
                    "--allow-native",
                    "--cpu",
                    cpu,
                    "--mem-mib",
                    "128",
                    "--data-dir",
                    self.dir.path().to_str().unwrap(),
                ])
                .env("RUNSPK_URL", format!("http://{}", self.address))
                .env("RUNSPK_TOKEN", &self.token)
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .unwrap(),
        );
        for _ in 0..100 {
            if self
                .client()
                .get::<Vec<Worker>>("/workers")
                .await
                .unwrap()
                .iter()
                .any(|w| w.state == WorkerState::Online)
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
        panic!("worker unavailable");
    }
    async fn wait(&self, id: &str, predicate: impl Fn(&Job) -> bool) -> Job {
        for _ in 0..300 {
            let j: Job = self.client().get(&format!("/jobs/{id}")).await.unwrap();
            if predicate(&j) {
                return j;
            }
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
        panic!("job did not reach expected state");
    }
}
impl Drop for Cluster {
    fn drop(&mut self) {
        if let Some(w) = &mut self.worker {
            let _ = w.kill();
            let _ = w.wait();
        }
        let _ = self.scheduler.kill();
        let _ = self.scheduler.wait();
        if let Ok(entries) = std::fs::read_dir(self.dir.path().join("test")) {
            for e in entries.flatten() {
                if e.path().extension().is_some_and(|x| x == "json")
                    && let Ok(data) = std::fs::read(e.path())
                    && let Ok(entry) = serde_json::from_slice::<runspk::journal::Entry>(&data)
                    && let Some(p) = entry.process
                    && p.pid > 1
                {
                    // SAFETY: the private test journal records a positive child process group.
                    unsafe {
                        libc::kill(-p.pid, libc::SIGKILL);
                    }
                }
            }
        }
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn worker_crash_restart_and_scheduler_restart_reconcile() {
    let mut c = Cluster::new().await;
    c.worker().await;
    let client = c.client();
    let j: Job = client.post("/jobs", &spec()).await.unwrap();
    c.wait(&j.id, |j| j.state == JobState::Running).await;
    // Wait for the launch gate journal, so recovery exercises a real subprocess.
    for _ in 0..100 {
        if std::fs::read_dir(c.dir.path().join("test"))
            .unwrap()
            .flatten()
            .any(|f| {
                f.path().extension().is_some_and(|x| x == "json")
                    && std::fs::read(f.path())
                        .ok()
                        .and_then(|b| serde_json::from_slice::<runspk::journal::Entry>(&b).ok())
                        .is_some_and(|e| e.process.is_some())
            })
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let worker = c.worker.as_mut().unwrap();
    worker.kill().unwrap();
    worker.wait().unwrap();
    c.worker = None;
    c.wait(&j.id, |j| j.state == JobState::Uncertain).await;
    assert_eq!(
        client.get::<Vec<Worker>>("/workers").await.unwrap()[0]
            .reserved
            .cpu,
        1
    );
    let capacity = if runspk::hardware::cpu_ids().unwrap().len() >= 2 {
        "2"
    } else {
        "1"
    };
    c.worker_capacity(capacity).await;
    assert_eq!(
        c.wait(&j.id, |j| j.state.terminal()).await.state,
        JobState::Failed
    );
    assert_eq!(
        client.get::<Vec<Worker>>("/workers").await.unwrap()[0]
            .reserved
            .cpu,
        0
    );
    for _ in 0..100 {
        if client.get::<Vec<Worker>>("/workers").await.unwrap()[0]
            .registration
            .capacity
            .cpu
            == capacity.parse::<u32>().unwrap()
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(
        client.get::<Vec<Worker>>("/workers").await.unwrap()[0]
            .registration
            .capacity
            .cpu,
        capacity.parse::<u32>().unwrap()
    );
    let mut short = spec();
    short.command = vec!["/bin/sleep".into(), "1.5".into()];
    let j: Job = client.post("/jobs", &short).await.unwrap();
    c.wait(&j.id, |j| j.state == JobState::Running).await;
    c.scheduler.kill().unwrap();
    c.scheduler.wait().unwrap();
    c.scheduler = Cluster::scheduler(c.dir.path(), &c.address, &c.token);
    c.ready().await;
    assert_eq!(
        c.wait(&j.id, |j| j.state.terminal()).await.state,
        JobState::Succeeded
    );
    assert_eq!(
        client
            .get::<Vec<Attempt>>(&format!("/jobs/{}/attempts", j.id))
            .await
            .unwrap()
            .len(),
        1
    );
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn partition_stops_execution_and_replays_durable_completion() {
    let mut c = Cluster::new().await;
    c.worker().await;
    let client = c.client();
    let j: Job = client.post("/jobs", &spec()).await.unwrap();
    c.wait(&j.id, |j| j.state == JobState::Running).await;
    // SAFETY: scheduler is an owned, unreaped test child; no host networking is changed.
    unsafe {
        libc::kill(c.scheduler.id() as i32, libc::SIGSTOP);
    }
    tokio::time::sleep(Duration::from_secs(6)).await;
    let entries: Vec<_> = std::fs::read_dir(c.dir.path().join("test"))
        .unwrap()
        .flatten()
        .filter_map(|f| {
            std::fs::read(f.path())
                .ok()
                .and_then(|b| serde_json::from_slice::<runspk::journal::Entry>(&b).ok())
        })
        .collect();
    assert!(
        entries.iter().any(|e| e.completion.is_some()),
        "completion must survive API outage"
    );
    // SAFETY: resume the same owned child paused above.
    unsafe {
        libc::kill(c.scheduler.id() as i32, libc::SIGCONT);
    }
    let j = c.wait(&j.id, |j| j.state.terminal()).await;
    assert_eq!(j.state, JobState::Failed);
    assert!(j.error.unwrap().contains("lease"));
    assert_eq!(
        client.get::<Vec<Worker>>("/workers").await.unwrap()[0]
            .reserved
            .cpu,
        0
    );
}

#[tokio::test]
async fn slow_output_replay_keeps_worker_heartbeats_running() {
    use axum::{Json, Router, http::StatusCode, routing::post};
    use std::sync::atomic::{AtomicUsize, Ordering};
    let dir = tempfile::tempdir().unwrap();
    let journal = Arc::new(runspk::journal::Journal::open(dir.path(), "test").unwrap());
    let mut registration = registration();
    registration.auth = journal.auth.clone();
    let mut store = Store::open(":memory:").unwrap();
    let worker_info = store.register(registration.clone()).unwrap();
    let job = store.submit(spec()).unwrap();
    let assignment = store.claim(&registration.auth).unwrap().unwrap();
    let completion = report(&registration.auth, &assignment, 0);
    let heartbeats = Arc::new(AtomicUsize::new(0));
    let uploads = Arc::new(AtomicUsize::new(0));
    let router = Router::new()
        .route(
            "/worker/register",
            post({
                let worker = worker_info.clone();
                move || async move { Json(worker) }
            }),
        )
        .route(
            "/worker/heartbeat",
            post({
                let worker = worker_info.clone();
                let counter = heartbeats.clone();
                move || {
                    counter.fetch_add(1, Ordering::SeqCst);
                    let worker = worker.clone();
                    async move { Json(worker) }
                }
            }),
        )
        .route(
            "/worker/drain",
            post(move || async move { Json(worker_info) }),
        )
        .route("/worker/poll", post(|| async { Json(None::<Assignment>) }))
        .route(
            "/worker/artifacts",
            post({
                let counter = uploads.clone();
                move || {
                    counter.fetch_add(1, Ordering::SeqCst);
                    async {
                        tokio::time::sleep(Duration::from_secs(30)).await;
                        StatusCode::SERVICE_UNAVAILABLE
                    }
                }
            }),
        );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let client = Client::new(
        format!("http://{}", listener.local_addr().unwrap()),
        "test-token-with-at-least-32-characters".into(),
    )
    .unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let (stop, rx) = tokio::sync::watch::channel(false);
    let worker = tokio::spawn(runspk::runner::worker(
        client,
        registration,
        journal.clone(),
        rx,
    ));
    let deadline = tokio::time::Instant::now() + Duration::from_secs(8);
    while heartbeats.load(Ordering::SeqCst) == 0 {
        assert!(tokio::time::Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    journal
        .save(&runspk::journal::Entry {
            assignment: Assignment { job, ..assignment },
            process: None,
            completion: Some(completion),
            runtime: None,
            artifacts: Some(runspk::workspace::Bundle { files: Vec::new() }),
            keep_workspace: false,
        })
        .await
        .unwrap();
    while uploads.load(Ordering::SeqCst) == 0 {
        assert!(tokio::time::Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let before = heartbeats.load(Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(2200)).await;
    assert!(
        heartbeats.load(Ordering::SeqCst) >= before + 2,
        "slow artifact request blocked worker health"
    );
    stop.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(2), worker)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(
        journal.entries().await.unwrap().len(),
        1,
        "unacknowledged outputs remain durable"
    );
    server.abort();
}
