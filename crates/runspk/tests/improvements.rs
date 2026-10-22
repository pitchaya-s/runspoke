//! Feature regressions through the installed CLI and authenticated loopback HTTP.
use base64::{Engine, engine::general_purpose::STANDARD};
use runspk::{
    config::{ByteSize, TransferLimits},
    model::{Job, JobState, Logs, Worker},
    profile::Profile,
    workspace::{Bundle, SnapshotFile},
};
use serde_json::{Value, json};
use std::{
    fs,
    path::Path,
    process::{Child, Command, Output, Stdio},
    time::Duration,
};

const TOKEN: &str = "improvements-private-token-at-least-32-characters";

struct Cluster {
    root: tempfile::TempDir,
    url: String,
    children: Vec<Child>,
}

impl Cluster {
    fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        let socket = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}", socket.local_addr().unwrap());
        Profile {
            url: url.clone(),
            token: TOKEN.into(),
            state_dir: Some(root.path().join("scheduler")),
        }
        .save(&root.path().join("profile.json"))
        .unwrap();
        Self {
            root,
            url,
            children: Vec::new(),
        }
    }

    fn command(&self, binary: &str) -> Command {
        let mut command = Command::new(binary);
        command
            .env_clear()
            .env("PATH", "/usr/bin:/bin")
            .env("HOME", self.root.path())
            .env("XDG_CONFIG_HOME", self.root.path().join("xdg-config"))
            .env("XDG_STATE_HOME", self.root.path().join("xdg-state"))
            .env("RUST_LOG", "warn")
            .current_dir(self.root.path())
            .stdin(Stdio::null());
        command
    }

    fn cli(&self) -> tokio::process::Command {
        let mut command = self.command(env!("CARGO_BIN_EXE_runspk"));
        command
            .arg("--config")
            .arg(self.root.path().join("profile.json"));
        let mut command = tokio::process::Command::from(command);
        command.kill_on_drop(true);
        command
    }

    async fn success(&self, args: &[&str]) -> Output {
        let output = tokio::time::timeout(Duration::from_secs(20), self.cli().args(args).output())
            .await
            .unwrap()
            .unwrap();
        assert!(output.status.success(), "{args:?}: {output:?}");
        output
    }

    async fn value(&self, args: &[&str]) -> Value {
        serde_json::from_slice(&self.success(args).await.stdout).unwrap()
    }

    async fn id(&self, args: &[&str]) -> String {
        String::from_utf8(self.success(args).await.stdout)
            .unwrap()
            .trim()
            .into()
    }

    async fn start(&mut self, worker: bool, settings: Option<&Path>) {
        let mut scheduler = self.command(env!("CARGO_BIN_EXE_runspk-scheduler"));
        scheduler
            .args([
                "--bind",
                self.url.trim_start_matches("http://"),
                "--token",
                TOKEN,
            ])
            .arg("--db")
            .arg(self.root.path().join("scheduler/db.sqlite"));
        if let Some(settings) = settings {
            scheduler.arg("--settings").arg(settings);
        }
        self.spawn(scheduler, "scheduler");
        let ready = async {
            loop {
                if let Ok(response) = reqwest::Client::new()
                    .get(format!("{}/info", self.url))
                    .bearer_auth(TOKEN)
                    .send()
                    .await
                    && response.status().is_success()
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(30)).await;
            }
        };
        tokio::time::timeout(Duration::from_secs(10), ready)
            .await
            .expect("scheduler readiness");
        if worker {
            let mut command = self.command(env!("CARGO_BIN_EXE_runspk-worker"));
            command
                .args([
                    "--url",
                    &self.url,
                    "--token",
                    TOKEN,
                    "--id",
                    "target",
                    "--cpu",
                    "1",
                    "--mem-mib",
                    "128",
                    "--allow-native",
                    "--label",
                    "pool=regression",
                ])
                .arg("--data-dir")
                .arg(self.root.path().join("worker"));
            self.spawn(command, "worker");
            tokio::time::timeout(Duration::from_secs(10), async {
                loop {
                    let workers: Vec<Worker> =
                        serde_json::from_value(self.value(&["workers"]).await).unwrap();
                    if workers.iter().any(|w| w.registration.auth.id == "target") {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(30)).await;
                }
            })
            .await
            .expect("worker readiness");
        }
    }

    fn spawn(&mut self, mut command: Command, name: &str) {
        command
            .stdout(Stdio::null())
            .stderr(fs::File::create(self.root.path().join(format!("{name}.log"))).unwrap());
        self.children.push(command.spawn().unwrap());
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        for child in self.children.iter_mut().rev() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

#[tokio::test]
async fn settings_precedence_redacts_secrets_and_monitor_rejects_pipes() {
    let cluster = Cluster::new();
    let path = cluster.root.path().join("settings.json");
    fs::write(
        &path,
        r#"{"transfers":{"project_bytes":"8MiB"},"job_defaults":{"mem_mib":96}}"#,
    )
    .unwrap();
    let args = ["--settings", path.to_str().unwrap(), "config"];
    let file = cluster.value(&args).await;
    assert_eq!(file["settings"]["transfers"]["project_bytes"], "8388608B");
    assert_eq!(file["settings"]["job_defaults"]["mem_mib"], 96);
    let output = cluster
        .cli()
        .args(args)
        .env("RUNSPK_PROJECT_LIMIT", "12MiB")
        .env(
            "RUNSPK_TOKEN",
            "environment-secret-token-at-least-32-characters",
        )
        .output()
        .await
        .unwrap();
    assert!(output.status.success(), "{output:?}");
    let value: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(value["settings"]["transfers"]["project_bytes"], "12582912B");
    for bytes in [&output.stdout, &output.stderr] {
        assert!(!String::from_utf8_lossy(bytes).contains(TOKEN));
        assert!(!String::from_utf8_lossy(bytes).contains("environment-secret"));
    }
    let output = cluster
        .cli()
        .args(args)
        .args(["--project-limit", "16MiB"])
        .env("RUNSPK_PROJECT_LIMIT", "12MiB")
        .output()
        .await
        .unwrap();
    assert!(output.status.success(), "{output:?}");
    let value: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(value["settings"]["transfers"]["project_bytes"], "16777216B");
    for bytes in [&output.stdout, &output.stderr] {
        assert!(!String::from_utf8_lossy(bytes).contains(TOKEN));
        assert!(!String::from_utf8_lossy(bytes).contains("environment-secret"));
    }
    assert!(!file.to_string().contains(TOKEN));
    assert_eq!(
        cluster.value(&["config"]).await["settings"]["transfers"]["project_bytes"],
        "33554432B"
    );
    let output = cluster
        .cli()
        .args(["--project-limit", "65MiB", "config"])
        .output()
        .await
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("64MiB"));
    let output = tokio::time::timeout(
        Duration::from_secs(3),
        cluster.cli().arg("monitor").stdin(Stdio::piped()).output(),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(!output.status.success());
    assert!(
        output.stdout.is_empty(),
        "terminal escape sequences leaked into stdout"
    );
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("terminal"),
        "{output:?}"
    );
}

#[tokio::test]
async fn named_targeted_project_selective_fetch_and_repeat_keep_the_snapshot() {
    let mut cluster = Cluster::new();
    cluster.start(true, None).await;
    let project = cluster.root.path().join("project");
    fs::create_dir(&project).unwrap();
    fs::write(project.join("input"), "original snapshot\n").unwrap();
    let id = cluster.id(&["submit", "--project", project.to_str().unwrap(), "--output", "results",
        "--mem", "64M", "--trusted", "--name", "snapshot regression", "--tag", "smoke",
        "--require", "pool=regression", "--key", "original", "--", "/bin/sh", "-c",
        "mkdir -p results/selected results/selected-other; cp input results/selected/value; cp input results/selected-other/value"]).await;
    cluster.success(&["wait", &id, "--timeout", "10"]).await;
    let original: Job = serde_json::from_value(cluster.value(&["status", &id]).await).unwrap();
    assert_eq!(original.state, JobState::Succeeded);
    assert_eq!(original.worker_id.as_deref(), Some("target"));
    assert_eq!(original.spec.name.as_deref(), Some("snapshot regression"));
    assert_eq!(original.spec.tags, ["smoke"]);
    assert_eq!(original.spec.required_labels["pool"], "regression");
    let listing = cluster.value(&["fetch", &id, "--list"]).await;
    assert_eq!(listing["files"].as_array().unwrap().len(), 2);
    assert!(
        listing["files"]
            .as_array()
            .unwrap()
            .iter()
            .all(|f| f["bytes"] == 18 && f.get("data").is_none())
    );
    let selected = cluster.root.path().join("selected");
    cluster
        .success(&[
            "fetch",
            &id,
            "--into",
            selected.to_str().unwrap(),
            "--path",
            "results/selected",
        ])
        .await;
    assert_eq!(
        fs::read_to_string(selected.join("results/selected/value")).unwrap(),
        "original snapshot\n"
    );
    assert!(!selected.join("results/selected-other").exists());
    let overwrite = cluster
        .cli()
        .args(["fetch", &id, "--into", selected.to_str().unwrap()])
        .output()
        .await
        .unwrap();
    assert!(!overwrite.status.success());

    fs::remove_dir_all(&project).unwrap();
    let repeated = cluster.id(&["repeat", &id]).await;
    let keyed = cluster.id(&["repeat", &id, "--key", "repeat-once"]).await;
    assert_ne!(id, repeated);
    assert_ne!(repeated, keyed);
    assert_eq!(
        keyed,
        cluster.id(&["repeat", &id, "--key", "repeat-once"]).await
    );
    for (job_id, key) in [(&repeated, None), (&keyed, Some("repeat-once"))] {
        cluster.success(&["wait", job_id, "--timeout", "10"]).await;
        let job: Job = serde_json::from_value(cluster.value(&["status", job_id]).await).unwrap();
        let mut expected = original.spec.clone();
        expected.submission_key = key.map(str::to_owned);
        assert_eq!(job.spec, expected);
        assert_ne!(job.attempt_id, original.attempt_id);
        let target = cluster.root.path().join(job_id);
        cluster
            .success(&[
                "fetch",
                job_id,
                "--into",
                target.to_str().unwrap(),
                "--path",
                "results/selected/value",
            ])
            .await;
        assert_eq!(
            fs::read_to_string(target.join("results/selected/value")).unwrap(),
            "original snapshot\n"
        );
    }
}

#[tokio::test]
async fn incremental_logs_use_utf8_byte_cursors_and_preserve_plain_json() {
    let mut cluster = Cluster::new();
    cluster.start(true, None).await;
    let id = cluster
        .id(&[
            "submit",
            "--mem",
            "64M",
            "--trusted",
            "--",
            "/bin/sh",
            "-c",
            "printf 'aé🙂z'; printf 'βerr' >&2",
        ])
        .await;
    cluster.success(&["wait", &id, "--timeout", "10"]).await;
    let ordinary = cluster.value(&["--json", "logs", &id]).await;
    assert_eq!(ordinary.as_object().unwrap().len(), 3);
    let logs: Logs = serde_json::from_value(ordinary.clone()).unwrap();
    assert_eq!(logs.stdout, "aé🙂z");
    assert_eq!(logs.stderr, "βerr");
    let http = reqwest::Client::new();
    let endpoint = format!("{}/jobs/{id}/logs/chunk", cluster.url);
    let chunk: Value = http
        .get(&endpoint)
        .bearer_auth(TOKEN)
        .query(&[
            ("stdout_offset", "2"),
            ("stderr_offset", "1"),
            ("limit", "2"),
        ])
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(chunk["stdout"], "é");
    assert_eq!(chunk["stderr"], "β");
    assert_eq!(chunk["stdout_offset"], 1);
    assert_eq!(chunk["stdout_next"], 3);
    assert_eq!(chunk["stderr_next"], 2);
    assert_eq!(chunk["stdout_total"], logs.stdout.len());
    assert_eq!(chunk["stderr_total"], logs.stderr.len());
    assert_eq!(chunk["attempt_id"], ordinary["attempt_id"]);
    let resumed: Value = http
        .get(&endpoint)
        .bearer_auth(TOKEN)
        .query(&[
            ("stdout_offset", "3"),
            ("stderr_offset", "2"),
            ("attempt_id", logs.attempt_id.as_deref().unwrap()),
        ])
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(resumed["stdout"], "🙂z");
    assert_eq!(resumed["stderr"], "err");
    assert_eq!(resumed["reset"], false);
    let reset: Value = http
        .get(&endpoint)
        .bearer_auth(TOKEN)
        .query(&[
            ("stdout_offset", "3"),
            ("stderr_offset", "2"),
            ("attempt_id", &uuid::Uuid::new_v4().to_string()),
        ])
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(reset["reset"], true);
    assert_eq!(reset["stdout"], logs.stdout);
    assert_eq!(reset["stderr"], logs.stderr);
    let incremental = cluster
        .value(&[
            "--json",
            "logs",
            &id,
            "--incremental",
            "--stdout-offset",
            "3",
            "--stderr-offset",
            "2",
        ])
        .await;
    assert_eq!(incremental["stdout"], "🙂z");
    assert_eq!(cluster.value(&["--json", "logs", &id]).await, ordinary);
}

#[tokio::test]
async fn transfer_limits_and_corrupt_snapshots_reject_without_partial_jobs() {
    let mut cluster = Cluster::new();
    let limits = TransferLimits {
        project_bytes: ByteSize(8),
        http_body_bytes: ByteSize(4096),
        ..Default::default()
    };
    let settings = cluster.root.path().join("settings.json");
    fs::write(
        &settings,
        serde_json::to_vec(&json!({"transfers": limits})).unwrap(),
    )
    .unwrap();
    cluster.start(false, Some(&settings)).await;
    let remote = cluster.value(&["config", "--remote"]).await;
    assert_eq!(remote["effective_transfers"]["project_bytes"], "8B");
    assert!(!remote.to_string().contains(TOKEN));
    let http = reqwest::Client::new();
    let spec = json!({"command":["/bin/true"],"resources":{"cpu":1,"mem_mib":64},"timeout_secs":10,"trusted":true});
    let valid = json!({"spec":spec,"workspace":{"files":[{"path":"input","data":STANDARD.encode(b"eight!!!"),"executable":false}]}});
    let mut too_large = valid.clone();
    too_large["workspace"]["files"][0]["data"] = json!(STANDARD.encode(b"nine!!!!!"));
    let mut corrupt = valid.clone();
    corrupt["spec"]["workspace_id"] = json!("0".repeat(64));
    let mut bad_encoding = valid.clone();
    bad_encoding["workspace"]["files"][0]["data"] = json!("invalid!");
    for request in [too_large, corrupt, bad_encoding] {
        let response = http
            .post(format!("{}/jobs/project", cluster.url))
            .bearer_auth(TOKEN)
            .json(&request)
            .send()
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            reqwest::StatusCode::BAD_REQUEST,
            "{}",
            response.text().await.unwrap()
        );
        assert_eq!(cluster.value(&["list"]).await, json!([]));
    }
    let mut oversized_http = valid.clone();
    oversized_http["padding"] = json!("x".repeat(4096));
    let response = http
        .post(format!("{}/jobs/project", cluster.url))
        .bearer_auth(TOKEN)
        .json(&oversized_http)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(cluster.value(&["list"]).await, json!([]));
    let db = rusqlite::Connection::open(cluster.root.path().join("scheduler/db.sqlite")).unwrap();
    let snapshots: i64 = db
        .query_row("SELECT count(*) FROM workspaces", [], |row| row.get(0))
        .unwrap();
    assert_eq!(snapshots, 0, "rejected submissions left orphaned snapshots");
    let accepted: Job = http
        .post(format!("{}/jobs/project", cluster.url))
        .bearer_auth(TOKEN)
        .json(&valid)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(accepted.state, JobState::Queued);
    assert_eq!(cluster.value(&["list"]).await.as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn fetch_rejects_corrupt_fingerprint_before_creating_destination() {
    let cluster = Cluster::new();
    let bundle = Bundle {
        files: vec![SnapshotFile {
            path: "results/value".into(),
            data: STANDARD.encode("corrupt"),
            executable: false,
        }],
    };
    let artifacts = json!({"attempt_id":uuid::Uuid::new_v4().to_string(),"fingerprint":"0".repeat(64),"bundle":bundle});
    let router = axum::Router::new()
        .route(
            "/info",
            axum::routing::get(|| async {
                axum::Json(json!({"version":env!("CARGO_PKG_VERSION"),"features":["artifacts-v1"]}))
            }),
        )
        .route(
            "/jobs/corrupt/artifacts",
            axum::routing::get(move || async move { axum::Json(artifacts) }),
        );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let target = cluster.root.path().join("must-not-exist");
    let output = cluster
        .cli()
        .args([
            "--url",
            &url,
            "fetch",
            "corrupt",
            "--into",
            target.to_str().unwrap(),
        ])
        .output()
        .await
        .unwrap();
    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("fingerprint mismatch"),
        "{output:?}"
    );
    assert!(!target.exists());
    server.abort();
}

#[tokio::test]
async fn measured_host_heartbeat_is_optional_validated_and_visible() {
    let mut cluster = Cluster::new();
    cluster.start(true, None).await;
    let client = runspk::client::Client::new(cluster.url.clone(), TOKEN.into()).unwrap();
    let measured = tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            let workers: Vec<Worker> = client.get("/workers").await.unwrap();
            if let Some(worker) = workers.into_iter().find(|w| {
                w.telemetry
                    .as_ref()
                    .is_some_and(|t| t.cpu_busy_percent.is_some())
            }) {
                break worker;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("two measured host samples");
    let sample = measured.telemetry.as_ref().unwrap();
    assert!(sample.cpu_busy_percent.unwrap() <= 100);
    assert!(sample.mem_available_mib <= sample.mem_total_mib);
    assert!(measured.telemetry_received_ms.is_some());
    let bad = json!({"id":measured.registration.auth.id,"session":measured.registration.auth.session,"telemetry":{"sampled_ms":1,"cpu_busy_percent":101,"mem_total_mib":1,"mem_available_mib":2}});
    assert!(
        client
            .post::<Worker>("/worker/heartbeat", &bad)
            .await
            .is_err()
    );
    // Legacy heartbeat shape remains accepted and explicitly has no observation.
    let legacy: Worker = client
        .post("/worker/heartbeat", &measured.registration.auth)
        .await
        .unwrap();
    assert!(legacy.telemetry.is_none());
}

#[tokio::test]
async fn streamed_run_reconnects_without_advancing_the_log_cursor() {
    use axum::{
        Json, Router,
        routing::{get, post},
    };
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    let mut job=runspk::store::Store::open(":memory:").unwrap().submit(serde_json::from_value(json!({"command":["true"],"resources":{"cpu":1,"mem_mib":64},"timeout_secs":10,"trusted":true})).unwrap()).unwrap();
    job.state = JobState::Succeeded;
    job.exit_code = Some(0);
    job.finished_ms = Some(runspk::model::now_ms());
    let submitted = job.clone();
    let status = job.clone();
    let calls = Arc::new(AtomicUsize::new(0));
    let received = calls.clone();
    let router = Router::new()
        .route(
            "/info",
            get(|| async { Json(json!({"features":["log-offsets-v1"]})) }),
        )
        .route(
            "/jobs",
            post(move || {
                let job = submitted.clone();
                async move { Json(job) }
            }),
        )
        .route(
            "/jobs/{id}",
            get(move || {
                let job = status.clone();
                async move { Json(job) }
            }),
        )
        .route(
            "/jobs/{id}/logs/chunk",
            get(
                move |axum::extract::Query(q): axum::extract::Query<runspk::store::LogQuery>| {
                    let calls = received.clone();
                    async move {
                        assert_eq!((q.stdout_offset, q.stderr_offset), (0, 0));
                        if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                            tokio::time::sleep(Duration::from_millis(1300)).await;
                        }
                        Json(runspk::store::LogChunk {
                            stdout: "once".into(),
                            stderr: String::new(),
                            attempt_id: Some("test-attempt".into()),
                            stdout_offset: 0,
                            stderr_offset: 0,
                            stdout_next: 4,
                            stderr_next: 0,
                            stdout_total: 4,
                            stderr_total: 0,
                            reset: false,
                            stdout_truncated: false,
                            stderr_truncated: false,
                        })
                    }
                },
            ),
        );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let cluster = Cluster::new();
    let output = tokio::time::timeout(
        Duration::from_secs(8),
        cluster
            .cli()
            .env("RUNSPK_URL", url)
            .args(["run", "--wait-timeout", "5", "--trusted", "--", "true"])
            .output(),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(output.status.success(), "{output:?}");
    assert_eq!(output.stdout, b"once");
    assert!(calls.load(Ordering::SeqCst) >= 2);
    server.abort();
}

#[tokio::test]
async fn encoded_output_budget_fails_capture_and_preserves_worker_files() {
    let mut cluster = Cluster::new();
    let settings = cluster.root.path().join("small-http.json");
    fs::write(&settings, r#"{"transfers":{"http_body_bytes":"2048B"}}"#).unwrap();
    cluster.start(true, Some(&settings)).await;
    let project = cluster.root.path().join("empty-project");
    fs::create_dir(&project).unwrap();
    let id = cluster
        .id(&[
            "submit",
            "--project",
            project.to_str().unwrap(),
            "--output",
            "result",
            "--mem",
            "64M",
            "--trusted",
            "--",
            "/bin/sh",
            "-c",
            "head -c 2048 /dev/zero > result",
        ])
        .await;
    let client = runspk::client::Client::new(cluster.url.clone(), TOKEN.into()).unwrap();
    let job = tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            let job: Job = client.get(&format!("/jobs/{id}")).await.unwrap();
            if job.state.terminal() {
                break job;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(job.state, JobState::Failed);
    assert_eq!(job.exit_code, Some(74));
    assert!(job.error.unwrap().contains("HTTP body limit"));
    let result = cluster
        .root
        .path()
        .join("worker/target/workspaces")
        .join(job.attempt_id.unwrap())
        .join("result");
    assert_eq!(fs::metadata(result).unwrap().len(), 2048);
    let workers: Vec<Worker> = client.get("/workers").await.unwrap();
    assert_eq!(workers[0].reserved.cpu, 0);
}
