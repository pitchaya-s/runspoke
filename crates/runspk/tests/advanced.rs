use runspk::{client::Client, model::*};
use std::{
    process::{Child, Command, Stdio},
    time::Duration,
};
struct TestCluster {
    dir: tempfile::TempDir,
    client: Client,
    children: Vec<Child>,
    runtime: bool,
    url: String,
}
impl TestCluster {
    async fn new(runtime: bool) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let socket = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = socket.local_addr().unwrap().to_string();
        drop(socket);
        let token = "test-token-with-at-least-32-characters";
        let url = format!("http://{address}");
        let scheduler = Command::new(env!("CARGO_BIN_EXE_runspk-scheduler"))
            .args([
                "--bind",
                &address,
                "--db",
                dir.path().join("state.sqlite").to_str().unwrap(),
            ])
            .env("RUNSPK_TOKEN", token)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let client = Client::new(url.clone(), token.into()).unwrap();
        for _ in 0..100 {
            if client.get::<Vec<Job>>("/jobs").await.is_ok() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
        let mut worker = Command::new(env!("CARGO_BIN_EXE_runspk-worker"));
        worker.args([
            "--id",
            "advanced",
            "--cpu",
            "2",
            "--mem-mib",
            "256",
            "--data-dir",
            dir.path().to_str().unwrap(),
        ]);
        if runtime {
            worker.args(["--runtime", "docker"]);
        } else {
            worker.args(["--allow-native", "--simulate-gpus", "2"]);
        }
        let worker = worker
            .env("RUNSPK_URL", &url)
            .env("RUNSPK_TOKEN", token)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let c = Self {
            dir,
            client,
            children: vec![scheduler, worker],
            runtime,
            url,
        };
        for _ in 0..100 {
            if !c
                .client
                .get::<Vec<Worker>>("/workers")
                .await
                .unwrap()
                .is_empty()
            {
                return c;
            }
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
        panic!("worker not ready");
    }
    async fn wait(&self, id: &str, p: impl Fn(&Job) -> bool) -> Job {
        for _ in 0..400 {
            let j: Job = self.client.get(&format!("/jobs/{id}")).await.unwrap();
            if p(&j) {
                return j;
            }
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
        panic!("job timeout");
    }
    fn spec(&self, command: Vec<&str>) -> JobSpec {
        JobSpec {
            name: None,
            tags: Vec::new(),
            required_labels: Default::default(),
            transfer_limits: None,
            command: command.into_iter().map(str::to_string).collect(),
            resources: Resources {
                cpu: 1,
                mem_mib: 128,
            },
            timeout_secs: 8,
            trusted: !self.runtime,
            cwd: None,
            priority: 0,
            max_retries: 0,
            submission_key: None,
            image: self.runtime.then(|| "alpine:3.20".into()),
            gpu: 0,
            gpu_ids: Vec::new(),
            gpu_mem_mib: 0,
            workspace_id: None,
            outputs: Vec::new(),
        }
    }
}
impl Drop for TestCluster {
    fn drop(&mut self) {
        for child in self.children.iter_mut().rev() {
            if !matches!(child.try_wait(), Ok(None)) {
                continue;
            }
            // SAFETY: child is an owned test process, and the positive PID targets only it.
            unsafe {
                libc::kill(child.id() as i32, libc::SIGTERM);
            }
            for _ in 0..50 {
                if child.try_wait().ok().flatten().is_some() {
                    break;
                }
                std::thread::sleep(Duration::from_millis(20));
            }
            let _ = child.kill();
            let _ = child.wait();
        }
        let _ = &self.dir;
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn simulated_gpu_execution_and_bounded_native_logs() {
    let c = TestCluster::new(false).await;
    let mut spec = c.spec(vec!["/bin/sleep", "0.8"]);
    spec.gpu = 2;
    let first: Job = c.client.post("/jobs", &spec).await.unwrap();
    let first = c.wait(&first.id, |j| j.state == JobState::Running).await;
    assert_eq!(first.allocated_gpus.len(), 2);
    let mut second=c.spec(vec!["python3","-c","import os; assert os.environ['CUDA_VISIBLE_DEVICES']==''; assert 'RUNSPK_TOKEN' not in os.environ; assert len(os.sched_getaffinity(0))<=1; print('simulated-ok')"]);
    second.gpu = 1;
    let second: Job = c.client.post("/jobs", &second).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        c.client
            .get::<Job>(&format!("/jobs/{}", second.id))
            .await
            .unwrap()
            .state,
        JobState::Queued
    );
    assert_eq!(
        c.wait(&second.id, |j| j.state.terminal()).await.state,
        JobState::Succeeded
    );
    let huge = c.spec(vec![
        "python3",
        "-c",
        "import sys; sys.stdout.write('a'*1200000)",
    ]);
    let j: Job = c.client.post("/jobs", &huge).await.unwrap();
    assert_eq!(
        c.wait(&j.id, |j| j.state.terminal()).await.state,
        JobState::Succeeded
    );
    let logs: Logs = c.client.get(&format!("/jobs/{}/logs", j.id)).await.unwrap();
    assert_eq!(logs.stdout.len(), LOG_LIMIT);
    let stats: serde_json::Value = c.client.get("/stats").await.unwrap();
    assert!(stats["scheduling"]["placements"].as_u64().unwrap() >= 3);
    let probe=c.spec(vec!["python3","-c","import os,json,time; print(json.dumps(sorted(os.sched_getaffinity(0))),flush=True); time.sleep(0.4)"]);
    let a: Job = c.client.post("/jobs", &probe).await.unwrap();
    let b: Job = c.client.post("/jobs", &probe).await.unwrap();
    for id in [&a.id, &b.id] {
        assert_eq!(
            c.wait(id, |j| j.state.terminal()).await.state,
            JobState::Succeeded
        );
    }
    let a: Logs = c.client.get(&format!("/jobs/{}/logs", a.id)).await.unwrap();
    let b: Logs = c.client.get(&format!("/jobs/{}/logs", b.id)).await.unwrap();
    let a: Vec<usize> = serde_json::from_str(&a.stdout).unwrap();
    let b: Vec<usize> = serde_json::from_str(&b.stdout).unwrap();
    assert_eq!(a.len(), 1);
    assert_eq!(b.len(), 1);
    assert_ne!(a, b, "concurrent native jobs must receive disjoint CPUs");
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires an accessible Docker daemon and preloaded alpine:3.20; run explicitly"]
async fn docker_limits_isolation_cancellation_and_cleanup() {
    assert!(
        Command::new("docker")
            .args(["image", "inspect", "alpine:3.20"])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .unwrap()
            .success()
    );
    let mut c = TestCluster::new(true).await;
    let spec=c.spec(vec!["/bin/sh","-c","set -eu; test $(id -u) = 65534; test ! -e /var/run/docker.sock; test $(cat /sys/fs/cgroup/memory.max) = 134217728; test \"$(cat /sys/fs/cgroup/cpu.max)\" = '100000 100000'; test $(cat /sys/fs/cgroup/pids.max) = 128; grep -q 'NoNewPrivs:[[:space:]]*1' /proc/self/status; touch /tmp/ok; if touch /forbidden 2>/dev/null; then exit 9; fi; echo container-ok"]);
    let j: Job = c.client.post("/jobs", &spec).await.unwrap();
    let j = c.wait(&j.id, |j| j.state.terminal()).await;
    assert_eq!(j.state, JobState::Succeeded, "{j:?}");
    let logs: Logs = c.client.get(&format!("/jobs/{}/logs", j.id)).await.unwrap();
    assert_eq!(logs.stdout, "container-ok\n");
    let spec = c.spec(vec!["/bin/sleep", "7"]);
    let j: Job = c.client.post("/jobs", &spec).await.unwrap();
    let j = c.wait(&j.id, |j| j.state == JobState::Running).await;
    let name = format!("runspk-{}", j.attempt_id.unwrap());
    let mut observed = false;
    for _ in 0..100 {
        let out = Command::new("docker")
            .args(["inspect", "--format", "{{.State.Running}}", &name])
            .output()
            .unwrap();
        if out.status.success() && String::from_utf8_lossy(&out.stdout).trim() == "true" {
            observed = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
    assert!(observed, "container did not start");
    let _: Job = c
        .client
        .post(&format!("/jobs/{}/cancel", j.id), &())
        .await
        .unwrap();
    assert_eq!(
        c.wait(&j.id, |j| j.state.terminal()).await.state,
        JobState::Cancelled
    );
    assert!(
        !Command::new("docker")
            .args(["inspect", &name])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .unwrap()
            .success()
    );
    let j: Job = c.client.post("/jobs", &spec).await.unwrap();
    let j = c.wait(&j.id, |j| j.state == JobState::Running).await;
    let name = format!("runspk-{}", j.attempt_id.unwrap());
    let mut observed = false;
    for _ in 0..100 {
        let out = Command::new("docker")
            .args(["inspect", "--format", "{{.State.Running}}", &name])
            .output()
            .unwrap();
        if out.status.success() && String::from_utf8_lossy(&out.stdout).trim() == "true" {
            observed = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
    assert!(observed);
    c.children[1].kill().unwrap();
    c.children[1].wait().unwrap();
    c.children[1] = Command::new(env!("CARGO_BIN_EXE_runspk-worker"))
        .args([
            "--id",
            "advanced",
            "--cpu",
            "2",
            "--mem-mib",
            "256",
            "--runtime",
            "docker",
            "--data-dir",
            c.dir.path().to_str().unwrap(),
        ])
        .env("RUNSPK_URL", &c.url)
        .env("RUNSPK_TOKEN", "test-token-with-at-least-32-characters")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    assert_eq!(
        c.wait(&j.id, |j| j.state.terminal()).await.state,
        JobState::Failed
    );
    assert!(
        !Command::new("docker")
            .args(["inspect", &name])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .unwrap()
            .success()
    );
}
