use runspk::{journal::Entry, model::*, store::Store};
use std::process::Command;

fn container_entry() -> Entry {
    let spec = serde_json::from_value(serde_json::json!({
        "command": ["/bin/true"],
        "resources": {"cpu": 1, "mem_mib": 64},
        "timeout_secs": 10,
        "trusted": false,
        "image": "alpine:3.20"
    }))
    .unwrap();
    let job = Store::open(":memory:").unwrap().submit(spec).unwrap();
    Entry {
        assignment: Assignment {
            job,
            attempt_id: uuid::Uuid::new_v4().to_string(),
        },
        process: None,
        completion: None,
        runtime: Some(ContainerRuntime::Docker),
        artifacts: None,
        keep_workspace: false,
    }
}

#[tokio::test]
#[ignore = "requires an accessible Docker daemon; run explicitly"]
async fn docker_cleanup_requires_confirmed_absence() {
    let entry = container_entry();
    if std::env::var_os("RUNSPK_TEST_MISSING_DOCKER_SOCKET").is_some() {
        let error = runspk::backend::cleanup(&entry).await.unwrap_err();
        assert!(
            error.to_string().contains("absence could not be confirmed"),
            "{error:#}"
        );
        return;
    }

    // Unknown containers are safe to acknowledge repeatedly.
    runspk::backend::cleanup(&entry).await.unwrap();
    runspk::backend::cleanup(&entry).await.unwrap();

    let dir = tempfile::tempdir().unwrap();
    let child = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "docker_cleanup_requires_confirmed_absence",
            "--ignored",
            "--nocapture",
        ])
        .env("RUNSPK_TEST_MISSING_DOCKER_SOCKET", "1")
        .env_remove("DOCKER_CONTEXT")
        .env(
            "DOCKER_HOST",
            format!("unix://{}", dir.path().join("missing.sock").display()),
        )
        .output()
        .unwrap();
    assert!(
        child.status.success(),
        "{}{}",
        String::from_utf8_lossy(&child.stdout),
        String::from_utf8_lossy(&child.stderr)
    );
}
