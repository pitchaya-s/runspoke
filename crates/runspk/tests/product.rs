//! Internal product walkthrough: real CLI/daemons, isolated profiles and project directories.
use runspk::{model::*, profile::Profile};
use std::{
    fs,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::{Child, Command, Output, Stdio},
    time::{Duration, Instant},
};

struct Product {
    root: tempfile::TempDir,
    config: PathBuf,
    children: Vec<Child>,
}
impl Product {
    fn command(&self, config: &Path) -> Command {
        let cpus = runspk::hardware::cpu_ids().unwrap();
        let mut command = Command::new("taskset");
        let cpu = if config == self.config || cpus.len() < 2 {
            cpus[0]
        } else {
            cpus[1]
        };
        let binary = std::env::var_os("RUNSPK_PRODUCT_BIN_DIR")
            .map(|path| PathBuf::from(path).join("runspk"))
            .unwrap_or_else(|| env!("CARGO_BIN_EXE_runspk").into());
        command.args(["--cpu-list", &cpu.to_string()]).arg(binary);
        for key in [
            "RUNSPK_URL",
            "RUNSPK_TOKEN",
            "RUNSPK_CONFIG",
            "RUNSPK_BIND",
            "RUNSPK_DB",
            "RUNSPK_DATA_DIR",
            "RUNSPK_CPU",
            "RUNSPK_MEM_MIB",
            "RUNSPK_GPU_IDS",
            "RUNSPK_SIMULATE_GPUS",
            "RUNSPK_POLICY",
            "RUNSPK_RUNTIME",
        ] {
            command.env_remove(key);
        }
        command
            .arg("--config")
            .arg(config)
            // The release walkthrough must work without Cargo/Rustup on PATH.
            .env("PATH", "/usr/bin:/bin")
            .env("XDG_STATE_HOME", self.root.path().join("xdg-state"))
            .env("XDG_CONFIG_HOME", self.root.path().join("xdg-config"))
            .env("RUST_LOG", "warn");
        command
    }
    fn cli(&self, args: &[&str]) -> Output {
        self.command(&self.config).args(args).output().unwrap()
    }
    fn success(&self, args: &[&str]) -> Output {
        let out = self.cli(args);
        assert!(out.status.success(), "{args:?}: {out:?}");
        out
    }
    fn wait_workers(&self, count: usize) {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let out = self.cli(&["workers"]);
            if out.status.success()
                && serde_json::from_slice::<Vec<Worker>>(&out.stdout)
                    .unwrap()
                    .len()
                    >= count
            {
                return;
            }
            assert!(Instant::now() < deadline, "workers did not join: {out:?}");
            std::thread::sleep(Duration::from_millis(30));
        }
    }
    fn submit(&self, args: &[&str]) -> String {
        String::from_utf8(self.success(args).stdout)
            .unwrap()
            .trim()
            .into()
    }
    fn fetch(&self, id: &str, name: &str) {
        let target = self.root.path().join(name);
        self.success(&["fetch", id, "--into", target.to_str().unwrap()]);
        let summary: serde_json::Value =
            serde_json::from_slice(&fs::read(target.join("results/summary.json")).unwrap())
                .unwrap();
        assert_eq!(summary["mean"], 6.0);
        assert!(
            !self
                .cli(&["fetch", id, "--into", target.to_str().unwrap()])
                .status
                .success(),
            "fetch must never overwrite user output"
        );
    }
}
impl Drop for Product {
    fn drop(&mut self) {
        for child in self.children.iter_mut().rev() {
            if child.try_wait().ok().flatten().is_none() {
                // SAFETY: only owned, unreaped test processes receive signals.
                unsafe {
                    libc::kill(child.id() as i32, libc::SIGTERM);
                }
                for _ in 0..100 {
                    if child.try_wait().ok().flatten().is_some() {
                        break;
                    }
                    std::thread::sleep(Duration::from_millis(30));
                }
                let _ = child.kill();
                let _ = child.wait();
            }
        }
    }
}

#[test]
fn fresh_profile_to_python_project_remote_worker_failure_and_fetch() {
    let root = tempfile::tempdir().unwrap();
    let mut p = Product {
        config: root.path().join("config.json"),
        root,
        children: Vec::new(),
    };
    let started = Instant::now();
    let socket = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", socket.local_addr().unwrap());
    drop(socket);
    let state = p.root.path().join("state");
    let out = p.success(&[
        "--json",
        "init",
        "--url",
        &url,
        "--state-dir",
        state.to_str().unwrap(),
    ]);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&out.stdout).unwrap()["created"],
        true
    );
    assert_eq!(
        fs::metadata(&p.config).unwrap().permissions().mode() & 0o077,
        0
    );
    let profile = Profile::load(&p.config).unwrap().unwrap();
    assert!(
        !String::from_utf8(out.stdout)
            .unwrap()
            .contains(&profile.token)
    );
    p.success(&["init"]);
    assert_eq!(
        Profile::load(&p.config).unwrap().unwrap().token,
        profile.token,
        "init must not rotate credentials"
    );
    let log = fs::File::create(p.root.path().join("scheduler.log")).unwrap();
    let mut server = p.command(&p.config);
    server
        .args(["start", "--cpu", "1", "--mem", "128M"])
        .stdout(Stdio::null())
        .stderr(log);
    p.children.push(server.spawn().unwrap());
    p.wait_workers(1);
    p.success(&["doctor"]);
    assert!(
        String::from_utf8(p.success(&["completions", "bash"]).stdout)
            .unwrap()
            .contains("_runspk")
    );
    let source = std::env::var_os("RUNSPK_PRODUCT_EXAMPLE_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../../examples/python-project")
        });
    let project = p.root.path().join("project");
    fs::create_dir(&project).unwrap();
    for name in [
        "experiment.py",
        "measurements.csv",
        "runspk.json",
        ".runspkignore",
    ] {
        fs::copy(source.join(name), project.join(name)).unwrap();
    }
    fs::write(project.join(".env"), "must-not-leave-client").unwrap();
    let spec = project.join("runspk.json");
    let dry = p.success(&["run", "--spec", spec.to_str().unwrap(), "--dry-run"]);
    let manifest: serde_json::Value = serde_json::from_slice(&dry.stdout).unwrap();
    assert!(
        !manifest["files"]
            .as_array()
            .unwrap()
            .iter()
            .any(|f| f == ".env")
    );
    let run = p.success(&["run", "--spec", spec.to_str().unwrap()]);
    assert!(
        String::from_utf8(run.stdout)
            .unwrap()
            .contains("sum_squares")
    );
    let first_job_s = started.elapsed().as_secs_f64();
    let id = p.submit(&["submit", "--spec", spec.to_str().unwrap()]);
    p.success(&["wait", &id, "--timeout", "10"]);
    p.fetch(&id, "first-results");
    let invalid_destination = p.root.path().join("invalid-attempt-results");
    let invalid = p.cli(&[
        "fetch",
        &id,
        "--attempt",
        "1",
        "--into",
        invalid_destination.to_str().unwrap(),
    ]);
    assert!(!invalid.status.success());
    assert!(
        String::from_utf8(invalid.stderr)
            .unwrap()
            .contains("--attempt needs an attempt UUID")
    );
    assert!(!invalid_destination.exists());
    let human = String::from_utf8(p.success(&["--human", "wait", &id]).stdout).unwrap();
    assert!(human.contains("Resources") && human.contains("succeeded"));
    assert!(
        String::from_utf8(p.success(&["--human", "workers"]).stdout)
            .unwrap()
            .contains("CPU reserved")
    );

    let second = Instant::now();
    let invite = p.root.path().join("invitation.json");
    p.success(&["invite", "--out", invite.to_str().unwrap()]);
    let remote_config = p.root.path().join("remote.json");
    let mut join = p.command(&remote_config);
    join.env("XDG_STATE_HOME", p.root.path().join("remote-state"))
        .args([
            "join",
            invite.to_str().unwrap(),
            "--id",
            "workstation",
            "--cpu",
            "1",
            "--mem",
            "128M",
            "--allow-native",
            "--simulate-gpus",
            "1",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    p.children.push(join.spawn().unwrap());
    p.wait_workers(2);
    let second_worker_s = second.elapsed().as_secs_f64();
    // A simulated device affinity forces this job onto the joined worker.
    let remote = p.submit(&[
        "submit",
        "--project",
        project.to_str().unwrap(),
        "--output",
        "results",
        "--cpu",
        "1",
        "--mem",
        "128M",
        "--gpu",
        "1",
        "--gpu-ids",
        "sim:workstation:0",
        "--trusted",
        "--",
        "python3",
        "experiment.py",
    ]);
    p.success(&["wait", &remote, "--timeout", "10"]);
    let remote_job: Job = serde_json::from_slice(&p.success(&["status", &remote]).stdout).unwrap();
    assert_eq!(remote_job.worker_id.as_deref(), Some("workstation"));
    p.fetch(&remote, "remote-results");

    let failed = p.submit(&[
        "submit",
        "--project",
        project.to_str().unwrap(),
        "--output",
        "results",
        "--mem",
        "128M",
        "--trusted",
        "--",
        "python3",
        "experiment.py",
        "--fail",
    ]);
    assert_eq!(
        p.cli(&["wait", &failed, "--timeout", "10"]).status.code(),
        Some(7)
    );
    let logs = p.success(&["logs", &failed]);
    assert!(
        String::from_utf8(logs.stderr)
            .unwrap()
            .contains("Intentional failure")
    );
    p.fetch(&failed, "failure-results");
    let missing = p.submit(&[
        "submit",
        "--project",
        project.to_str().unwrap(),
        "--output",
        "missing",
        "--mem",
        "64M",
        "--trusted",
        "--",
        "/bin/true",
    ]);
    assert_eq!(
        p.cli(&["wait", &missing, "--timeout", "10"]).status.code(),
        Some(74)
    );
    let job: Job = serde_json::from_slice(&p.success(&["status", &missing]).stdout).unwrap();
    assert!(job.error.unwrap().contains("files retained"));

    // Large data stays on the worker; only the small project and summary cross the API.
    let dataset = p.root.path().join("worker-data.csv");
    let mut data = b"value\n".to_vec();
    let row = format!("{}6\n", " ".repeat(256));
    let rows = runspk::workspace::MAX_BYTES / row.len() + 1;
    data.extend(row.repeat(rows).bytes());
    assert!(data.len() > runspk::workspace::MAX_BYTES);
    fs::write(&dataset, data).unwrap();
    let large = p.submit(&[
        "submit",
        "--project",
        project.to_str().unwrap(),
        "--output",
        "results",
        "--mem",
        "64M",
        "--trusted",
        "--",
        "/usr/bin/python3",
        "experiment.py",
        "--data",
        dataset.to_str().unwrap(),
    ]);
    p.success(&["wait", &large, "--timeout", "10"]);
    p.fetch(&large, "large-data-results");
    let summary: serde_json::Value = serde_json::from_slice(
        &fs::read(
            p.root
                .path()
                .join("large-data-results/results/summary.json"),
        )
        .unwrap(),
    )
    .unwrap();
    assert_eq!(summary["samples"], u64::try_from(rows).unwrap());
    assert_eq!(summary["stdev"], 0.0);

    // Exercise the documented backup/stop/restart procedure with the same private state.
    let saved_profile = fs::read(&p.config).unwrap();
    let backup = p.root.path().join("before-upgrade.sqlite");
    p.success(&[
        "backup",
        "--db",
        state.join("scheduler.sqlite").to_str().unwrap(),
        "--out",
        backup.to_str().unwrap(),
    ]);
    assert_eq!(
        runspk::store::Store::open(&backup)
            .unwrap()
            .get(&large)
            .unwrap()
            .state,
        JobState::Succeeded
    );
    let scheduler = &mut p.children[0];
    // SAFETY: the scheduler is an owned, unreaped process.
    unsafe {
        libc::kill(scheduler.id() as i32, libc::SIGTERM);
    }
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(status) = scheduler.try_wait().unwrap() {
            assert!(status.success());
            break;
        }
        assert!(
            Instant::now() < deadline,
            "scheduler did not stop gracefully"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    let mut restart = p.command(&p.config);
    restart
        .args(["start", "--cpu", "1", "--mem", "128M"])
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    p.children[0] = restart.spawn().unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if p.cli(&["doctor"]).status.success() {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "scheduler did not recover after restart"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    let recovered: Job = serde_json::from_slice(&p.success(&["status", &large]).stdout).unwrap();
    assert_eq!(recovered.state, JobState::Succeeded);
    assert_eq!(fs::read(&p.config).unwrap(), saved_profile);
    p.fetch(&large, "after-restart-results");
    println!(
        "{}",
        serde_json::json!({"kind":"internal usability walkthrough; two local worker processes, no SSH or physical GPU","init_to_first_python_job_s":first_job_s,"invite_to_second_worker_s":second_worker_s,"first_job_commands_after_install":3,"second_worker_commands_without_SSH_or_install":2})
    );
}
