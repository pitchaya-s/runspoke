use runspk::{client::Client, model::*};
use std::{
    process::{Child, Command, Stdio},
    time::Duration,
};
struct Processes(Vec<Child>);
impl Drop for Processes {
    fn drop(&mut self) {
        for child in &mut self.0 {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_workers_reserve_independently_and_complete() {
    let dir = tempfile::tempdir().unwrap();
    let socket = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = socket.local_addr().unwrap();
    drop(socket);
    let token = "test-token-with-at-least-32-characters";
    let mut processes = Processes(vec![
        Command::new(env!("CARGO_BIN_EXE_runspk-scheduler"))
            .args([
                "--bind",
                &addr.to_string(),
                "--db",
                dir.path().join("db.sqlite").to_str().unwrap(),
            ])
            .env("RUNSPK_TOKEN", token)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    ]);
    let url = format!("http://{addr}");
    let client = Client::new(url.clone(), token.into()).unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        if client.get::<Vec<Job>>("/jobs").await.is_ok() {
            break;
        }
        assert!(tokio::time::Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
    for id in ["worker-a", "worker-b"] {
        processes.0.push(
            Command::new(env!("CARGO_BIN_EXE_runspk-worker"))
                .env("RUNSPK_WORKER_ID", id)
                .env("RUNSPK_CPU", "1")
                .env("RUNSPK_MEM_MIB", "128")
                .env("RUNSPK_ALLOW_NATIVE", "true")
                .env("RUNSPK_DATA_DIR", dir.path())
                .env("RUNSPK_TOKEN", token)
                .env("RUNSPK_URL", &url)
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .unwrap(),
        );
    }
    loop {
        if client.get::<Vec<Worker>>("/workers").await.unwrap().len() == 2 {
            break;
        }
        assert!(tokio::time::Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
    for _ in 0..4 {
        let _: Job = client
            .post(
                "/jobs",
                &JobSpec {
                    name: None,
                    tags: Vec::new(),
                    required_labels: Default::default(),
                    transfer_limits: None,
                    command: vec!["/bin/sleep".into(), "0.3".into()],
                    resources: Resources {
                        cpu: 1,
                        mem_mib: 64,
                    },
                    timeout_secs: 3,
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
                },
            )
            .await
            .unwrap();
    }
    let mut used = std::collections::BTreeSet::new();
    loop {
        let jobs: Vec<Job> = client.get("/jobs").await.unwrap();
        let workers: Vec<Worker> = client.get("/workers").await.unwrap();
        for w in workers {
            assert!(w.reserved.fits(w.registration.capacity));
        }
        for j in &jobs {
            if let Some(id) = &j.worker_id {
                used.insert(id.clone());
            }
        }
        if jobs.iter().all(|j| j.state == JobState::Succeeded) {
            break;
        }
        assert!(tokio::time::Instant::now() < deadline, "jobs: {jobs:?}");
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
    assert_eq!(used.len(), 2);
    assert!(
        client
            .get::<Vec<Worker>>("/workers")
            .await
            .unwrap()
            .iter()
            .all(|w| w.reserved == Resources::default())
    );
    for p in processes.0.iter_mut().skip(1) {
        // SAFETY: p is an owned, unreaped test child with a positive PID.
        unsafe {
            libc::kill(p.id() as i32, libc::SIGINT);
        }
        assert!(p.wait().unwrap().success());
    }
}
