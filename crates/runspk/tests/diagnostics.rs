use runspk::{profile::Profile, store::Store};
use serde_json::{Value, json};
use std::{
    fs,
    os::unix::fs::PermissionsExt,
    path::Path,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::process::Command;

const TOKEN: &str = "diagnostics-private-token-at-least-32-characters";

fn install() -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    for (name, source) in [
        ("runspk", env!("CARGO_BIN_EXE_runspk")),
        ("runspk-scheduler", env!("CARGO_BIN_EXE_runspk-scheduler")),
        ("runspk-worker", env!("CARGO_BIN_EXE_runspk-worker")),
    ] {
        fs::copy(source, dir.path().join(name)).unwrap();
    }
    dir
}

fn cli(dir: &Path) -> Command {
    let mut command = Command::new(dir.join("runspk"));
    command
        .arg("--config")
        .arg(dir.join("config.json"))
        .env_remove("RUNSPK_URL")
        .env_remove("RUNSPK_TOKEN")
        .env("XDG_STATE_HOME", dir.join("state"));
    command
}

fn save_profile(dir: &Path, url: String) {
    Profile {
        url,
        token: TOKEN.into(),
        state_dir: Some(dir.join("state")),
    }
    .save(&dir.join("config.json"))
    .unwrap();
}

async fn serve(router: axum::Router) -> (String, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    (url, server)
}

fn diagnostic(output: &std::process::Output, healthy: bool) -> Value {
    assert_eq!(output.status.success(), healthy, "{output:?}");
    for bytes in [&output.stdout, &output.stderr] {
        let text = String::from_utf8_lossy(bytes);
        assert!(!text.contains(TOKEN), "credentials appeared in diagnostics");
        assert!(!text.contains("wrong-private-token-at-least-32-characters"));
    }
    let value: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(value["healthy"], healthy);
    assert_eq!(value.as_object().unwrap().len(), 2);
    assert_eq!(value["checks"].as_array().unwrap().len(), 5);
    for check in value["checks"].as_array().unwrap() {
        assert_eq!(check.as_object().unwrap().len(), 3);
        assert!(check["check"].is_string());
        assert!(check["ok"].is_boolean());
        assert!(check["detail"].is_string());
    }
    value
}

fn check<'a>(value: &'a Value, name: &str) -> &'a Value {
    value["checks"]
        .as_array()
        .unwrap()
        .iter()
        .find(|check| check["check"] == name)
        .unwrap()
}

#[tokio::test]
async fn companion_versions_work_without_daemon_arguments() {
    for (name, binary) in [
        ("runspk-scheduler", env!("CARGO_BIN_EXE_runspk-scheduler")),
        ("runspk-worker", env!("CARGO_BIN_EXE_runspk-worker")),
        ("runspk-bench", env!("CARGO_BIN_EXE_runspk-bench")),
    ] {
        let output = Command::new(binary)
            .arg("--version")
            .env("RUNSPK_TOKEN", TOKEN)
            .output()
            .await
            .unwrap();
        assert!(output.status.success(), "{output:?}");
        assert_eq!(
            String::from_utf8_lossy(&output.stdout).trim(),
            format!("{name} {}", env!("CARGO_PKG_VERSION"))
        );
        assert!(output.stderr.is_empty(), "{output:?}");
    }
}

#[tokio::test]
async fn installed_mismatched_companions_fail_start_join_and_doctor() {
    let dir = install();
    let router = runspk::api::router(
        Arc::new(Mutex::new(Store::open(":memory:").unwrap())),
        TOKEN.into(),
    )
    .unwrap();
    let (url, server) = serve(router).await;
    save_profile(dir.path(), url);
    for name in ["runspk-scheduler", "runspk-worker"] {
        let path = dir.path().join(name);
        fs::write(
            &path,
            format!("#!/bin/sh\nprintf '{name} 0.0.0\\n'\nprintf '{TOKEN}\\n' >&2\n"),
        )
        .unwrap();
        fs::set_permissions(path, fs::Permissions::from_mode(0o755)).unwrap();
    }
    for args in [
        vec!["start", "--scheduler-only"],
        vec![
            "join",
            "config.json",
            "--id",
            "diagnostic",
            "--allow-native",
        ],
    ] {
        let output = cli(dir.path())
            .current_dir(dir.path())
            .args(&args)
            .env("RUNSPK_TOKEN", TOKEN)
            .output()
            .await
            .unwrap();
        assert!(!output.status.success(), "{args:?}: {output:?}");
        let error = String::from_utf8(output.stderr).unwrap();
        assert!(error.contains("incompatible"), "{error}");
        assert!(error.contains(env!("CARGO_PKG_VERSION")), "{error}");
        assert!(error.contains("install"), "{error}");
        assert!(
            !error.contains(TOKEN),
            "credentials appeared in diagnostics"
        );
        assert!(output.stdout.is_empty());
    }
    let output = cli(dir.path()).arg("doctor").output().await.unwrap();
    let value = diagnostic(&output, false);
    assert_eq!(check(&value, "runspk-scheduler")["ok"], false);
    assert_eq!(check(&value, "runspk-worker")["ok"], false);
    assert_eq!(check(&value, "scheduler")["ok"], true);
    server.abort();
}

#[tokio::test]
async fn doctor_authenticates_existing_info_request() {
    let dir = install();
    let router = runspk::api::router(
        Arc::new(Mutex::new(Store::open(":memory:").unwrap())),
        TOKEN.into(),
    )
    .unwrap();
    let (url, server) = serve(router).await;
    save_profile(dir.path(), url);
    let output = cli(dir.path()).arg("doctor").output().await.unwrap();
    diagnostic(&output, true);
    let output = cli(dir.path())
        .arg("doctor")
        .env("RUNSPK_TOKEN", "wrong-private-token-at-least-32-characters")
        .output()
        .await
        .unwrap();
    let value = diagnostic(&output, false);
    let scheduler = check(&value, "scheduler");
    assert_eq!(scheduler["ok"], false);
    assert!(scheduler["detail"].as_str().unwrap().contains("401"));
    assert!(
        value["checks"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|check| check["check"] != "scheduler")
            .all(|check| check["ok"] == true)
    );
    server.abort();
}

#[tokio::test]
async fn doctor_rejects_incompatible_scheduler_protocol_and_capabilities() {
    let dir = install();
    let info = json!({
        "version":env!("CARGO_PKG_VERSION"),
        "schema_version":runspk::store::SCHEMA_VERSION,
        "features":["project-snapshots-v1", "artifacts-v1"]
    });
    for (field, incompatible) in [
        ("version", json!("0.0.0")),
        ("schema_version", json!(2)),
        ("features", json!(["project-snapshots-v1"])),
        ("features", json!(["artifacts-v1"])),
        ("features", Value::Null),
    ] {
        let mut response = info.clone();
        response[field] = incompatible;
        let router = axum::Router::new().route(
            "/info",
            axum::routing::get(move || async move { axum::Json(response) }),
        );
        let (url, server) = serve(router).await;
        let output = cli(dir.path())
            .args(["doctor", "--url", &url])
            .env("RUNSPK_TOKEN", TOKEN)
            .output()
            .await
            .unwrap();
        let value = diagnostic(&output, false);
        assert_eq!(check(&value, "scheduler")["ok"], false, "{field}: {value}");
        assert!(
            check(&value, "scheduler")["detail"]
                .as_str()
                .unwrap()
                .contains("incompatible")
        );
        server.abort();
    }
}

#[tokio::test]
async fn doctor_redacts_credentials_reflected_in_api_errors() {
    let dir = install();
    let router = axum::Router::new().route(
        "/info",
        axum::routing::get(|| async { (axum::http::StatusCode::UNAUTHORIZED, TOKEN) }),
    );
    let (url, server) = serve(router).await;
    save_profile(dir.path(), url);
    let output = cli(dir.path()).arg("doctor").output().await.unwrap();
    let value = diagnostic(&output, false);
    let detail = check(&value, "scheduler")["detail"].as_str().unwrap();
    assert!(detail.contains("401"), "{detail}");
    assert!(detail.contains("[redacted]"), "{detail}");
    server.abort();
}

#[tokio::test]
async fn doctor_times_out_hanging_companion_and_continues_checks() {
    let dir = install();
    let router = runspk::api::router(
        Arc::new(Mutex::new(Store::open(":memory:").unwrap())),
        TOKEN.into(),
    )
    .unwrap();
    let (url, server) = serve(router).await;
    save_profile(dir.path(), url);
    fs::write(
        dir.path().join("runspk-scheduler"),
        "#!/bin/sh\nexec /bin/sleep 60\n",
    )
    .unwrap();
    let started = Instant::now();
    let output = cli(dir.path()).arg("doctor").output().await.unwrap();
    assert!(started.elapsed() < Duration::from_secs(10), "{output:?}");
    let value = diagnostic(&output, false);
    let scheduler = check(&value, "runspk-scheduler");
    assert_eq!(scheduler["ok"], false);
    assert!(scheduler["detail"].as_str().unwrap().contains("timed out"));
    assert_eq!(check(&value, "runspk-worker")["ok"], true);
    assert_eq!(check(&value, "scheduler")["ok"], true);
    server.abort();
}
