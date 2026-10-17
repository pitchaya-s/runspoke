use std::{
    process::{Command, Stdio},
    time::{Duration, Instant},
};
struct Daemon(std::process::Child);
impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
#[test]
fn real_cli_submit_logs_cancel_auth_and_persistence() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("scheduler.sqlite");
    let socket = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = socket.local_addr().unwrap();
    drop(socket);
    let token = "test-token-with-at-least-32-characters";
    let mut daemon = Daemon(
        Command::new(env!("CARGO_BIN_EXE_runspk-scheduler"))
            .arg("--local")
            .env("RUNSPK_BIND", addr.to_string())
            .env("RUNSPK_DB", &db)
            .env("RUNSPK_CPU", "1")
            .env("RUNSPK_MEM_MIB", "128")
            .env("RUNSPK_TOKEN", token)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    );
    let cli = |args: &[&str]| {
        Command::new(env!("CARGO_BIN_EXE_runspk"))
            .args(args)
            .env("RUNSPK_TOKEN", token)
            .env("RUNSPK_URL", format!("http://{addr}"))
            .output()
            .unwrap()
    };
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if cli(&["list"]).status.success() {
            break;
        }
        assert!(Instant::now() < deadline, "daemon startup timed out");
        std::thread::sleep(Duration::from_millis(30));
    }
    let output = cli(&[
        "submit",
        "--trusted",
        "--mem",
        "64M",
        "--",
        "/bin/echo",
        "cli-smoke",
    ]);
    assert!(output.status.success(), "{:?}", output);
    let id = String::from_utf8(output.stdout).unwrap();
    let id = id.trim();
    loop {
        let output = cli(&["status", id]);
        let j: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        if j["state"] == "succeeded" {
            break;
        }
        assert!(Instant::now() < deadline, "job did not finish: {j}");
        std::thread::sleep(Duration::from_millis(30));
    }
    assert_eq!(
        String::from_utf8(cli(&["logs", id]).stdout).unwrap(),
        "cli-smoke\n"
    );
    assert!(cli(&["wait", id]).status.success());
    let output = cli(&[
        "run",
        "--trusted",
        "--mem",
        "64M",
        "--",
        "/bin/sh",
        "-c",
        "printf run-output; printf run-error >&2; exit 7",
    ]);
    assert_eq!(output.status.code(), Some(7), "{output:?}");
    assert_eq!(output.stdout, b"run-output");
    assert!(
        String::from_utf8(output.stderr)
            .unwrap()
            .contains("run-error")
    );
    let output = cli(&[
        "run",
        "--trusted",
        "--mem",
        "64M",
        "--",
        "/bin/sh",
        "-c",
        "kill -TERM $$",
    ]);
    assert_eq!(
        output.status.code(),
        Some(143),
        "signal exit codes must be preserved: {output:?}"
    );
    let output = cli(&[
        "run",
        "--json",
        "--trusted",
        "--mem",
        "64M",
        "--",
        "/bin/echo",
        "json-run",
    ]);
    assert!(output.status.success(), "{output:?}");
    let events: Vec<serde_json::Value> = String::from_utf8(output.stdout)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(events.first().unwrap()["event"], "submitted");
    assert!(
        events
            .iter()
            .any(|event| event["event"] == "logs" && event["logs"]["stdout"] == "json-run\n")
    );
    assert_eq!(events.last().unwrap()["event"], "finished");
    let output = cli(&["list", "--state", "succeeded", "--limit", "1"]);
    let page: Vec<runspk::model::Job> = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(page.len(), 1);
    let output = cli(&[
        "list",
        "--state",
        "succeeded",
        "--after",
        &page[0].seq.to_string(),
        "--limit",
        "1",
    ]);
    let next: Vec<runspk::model::Job> = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(next.len(), 1);
    assert!(next[0].seq > page[0].seq);
    assert!(cli(&["explain", id]).status.success());
    assert!(!cli(&["submit", "--", "/bin/true"]).status.success());
    let output = Command::new(env!("CARGO_BIN_EXE_runspk"))
        .arg("list")
        .env("RUNSPK_TOKEN", "wrong-token-with-at-least-32-characters")
        .env("RUNSPK_URL", format!("http://{addr}"))
        .output()
        .unwrap();
    assert!(!output.status.success());
    let output = cli(&[
        "submit",
        "--trusted",
        "--mem",
        "64M",
        "--",
        "/bin/sleep",
        "5",
    ]);
    let id = String::from_utf8(output.stdout).unwrap();
    let id = id.trim();
    assert_eq!(
        cli(&["wait", id, "--timeout", "0"]).status.code(),
        Some(124)
    );
    let mut waiting = Command::new(env!("CARGO_BIN_EXE_runspk"))
        .args(["wait", id])
        .env("RUNSPK_TOKEN", token)
        .env("RUNSPK_URL", format!("http://{addr}"))
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    std::thread::sleep(Duration::from_millis(200));
    // SAFETY: waiting is an owned, unreaped CLI process.
    unsafe {
        libc::kill(waiting.id() as i32, libc::SIGINT);
    }
    assert_eq!(waiting.wait().unwrap().code(), Some(130));
    let state: serde_json::Value = serde_json::from_slice(&cli(&["status", id]).stdout).unwrap();
    assert_ne!(
        state["state"], "cancelled",
        "interrupting wait must leave the workload alone"
    );
    assert!(cli(&["cancel", id]).status.success());
    loop {
        let j: serde_json::Value = serde_json::from_slice(&cli(&["status", id]).stdout).unwrap();
        if j["state"] == "cancelled" {
            break;
        }
        assert!(Instant::now() < deadline);
        std::thread::sleep(Duration::from_millis(30));
    }
    // SAFETY: daemon is an owned, unreaped test child with a positive PID.
    unsafe {
        libc::kill(daemon.0.id() as i32, libc::SIGINT);
    }
    assert!(daemon.0.wait().unwrap().success());
    let s = runspk::store::Store::open(&db).unwrap();
    assert_eq!(s.list().unwrap().len(), 5);
}
