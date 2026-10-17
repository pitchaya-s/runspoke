use runspk::{model::*, workspace::*};
use std::{fs, os::unix::fs::symlink};

#[test]
fn snapshots_are_reproducible_bounded_and_do_not_follow_links() {
    let dir = tempfile::tempdir().unwrap();
    fs::write(dir.path().join("main.py"), b"print('hello')\n").unwrap();
    fs::write(dir.path().join(".env"), b"secret").unwrap();
    fs::write(dir.path().join("input.bin"), [0, 1, 255]).unwrap();
    fs::write(dir.path().join(".runspkignore"), "private\n").unwrap();
    fs::create_dir(dir.path().join("private")).unwrap();
    fs::write(dir.path().join("private/secret"), "keep local").unwrap();
    let bundle = capture_project(dir.path(), Vec::new()).unwrap();
    assert_eq!(
        bundle
            .files
            .iter()
            .map(|f| f.path.as_str())
            .collect::<Vec<_>>(),
        [".runspkignore", "input.bin", "main.py"]
    );
    assert_eq!(
        bundle.id().unwrap(),
        capture_project(dir.path(), Vec::new())
            .unwrap()
            .id()
            .unwrap()
    );
    let out = dir.path().join("copy");
    materialize(&bundle, &out).unwrap();
    assert_eq!(fs::read(out.join("input.bin")).unwrap(), [0, 1, 255]);
    assert!(materialize(&bundle, &out).is_err());
    symlink("/etc/passwd", dir.path().join("escape")).unwrap();
    assert!(capture_project(dir.path(), vec!["copy".into()]).is_err());
    assert!(capture_outputs(dir.path(), &["escape".into()]).is_err());
    symlink("/etc", dir.path().join("outside")).unwrap();
    assert!(capture_outputs(dir.path(), &["outside/passwd".into()]).is_err());
    for bad in ["../escape", "/tmp/escape", "a/../b", "a//b", ".", "a/./b"] {
        assert!(validate_path(bad).is_err(), "{bad}");
    }
    let mut invalid = bundle.clone();
    invalid.files[0].path = "../escape".into();
    assert!(materialize(&invalid, &dir.path().join("bad")).is_err());
    assert!(!dir.path().join("bad").exists());
    let mut duplicate = bundle.clone();
    duplicate.files.push(duplicate.files[0].clone());
    assert!(duplicate.validate().is_err());
    let mut collision = bundle.clone();
    collision.files[1].path = format!("{}/child", collision.files[0].path);
    assert!(collision.validate().is_err());
    let mut invalid_data = bundle.clone();
    invalid_data.files[0].data = "not-base64!".into();
    assert!(invalid_data.validate().is_err());
    fs::remove_file(dir.path().join("escape")).unwrap();
    fs::remove_file(dir.path().join("outside")).unwrap();
    fs::hard_link(dir.path().join("main.py"), dir.path().join("hardlink")).unwrap();
    assert!(capture_project(dir.path(), vec!["copy".into()]).is_err());
    fs::remove_file(dir.path().join("hardlink")).unwrap();
    fs::write(dir.path().join("large"), vec![b'x'; MAX_BYTES + 1]).unwrap();
    assert!(capture_project(dir.path(), vec!["copy".into()]).is_err());
    fs::remove_file(dir.path().join("large")).unwrap();
    fs::write(
        dir.path().join("invitation.json"),
        br#"{"url":"http://127.0.0.1:8765","token":"private-administrative-connection"}"#,
    )
    .unwrap();
    let error = capture_project(dir.path(), vec!["copy".into()]).unwrap_err();
    assert!(error.to_string().contains("connection credentials"));
}

#[tokio::test]
async fn transfer_routes_accept_bundles_larger_than_the_old_http_limit() {
    use base64::{Engine, engine::general_purpose::STANDARD};
    use runspk::{client::Client, store::Store};
    use std::sync::{Arc, Mutex};

    let payload = STANDARD.encode(vec![b'x'; 13 * 1024 * 1024]);
    let submission = ProjectSubmission {
        spec: serde_json::from_value(serde_json::json!({
            "command": ["true"],
            "resources": {"cpu": 1, "mem_mib": 128},
            "timeout_secs": 10,
            "trusted": true,
            "outputs": ["result.bin"]
        }))
        .unwrap(),
        workspace: Bundle {
            files: vec![SnapshotFile {
                path: "input.bin".into(),
                data: payload.clone(),
                executable: false,
            }],
        },
    };
    assert!(serde_json::to_vec(&submission).unwrap().len() > 16 * 1024 * 1024);

    let store = Arc::new(Mutex::new(Store::open(":memory:").unwrap()));
    let token = "test-token-with-at-least-32-characters";
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let client = Client::new(
        format!("http://{}", listener.local_addr().unwrap()),
        token.into(),
    )
    .unwrap();
    let router = runspk::api::router(store, token.into()).unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });

    let job: Job = client
        .post_with_timeout(
            "/jobs/project",
            &submission,
            runspk::client::TRANSFER_TIMEOUT,
        )
        .await
        .unwrap();
    let registration = Registration {
        auth: WorkerAuth {
            id: "large-transfer-worker".into(),
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
    };
    let _: Worker = client
        .post("/worker/register", &registration)
        .await
        .unwrap();
    let assignment: Assignment = client
        .post::<Option<Assignment>>("/worker/poll", &registration.auth)
        .await
        .unwrap()
        .unwrap();
    let owner = Start {
        auth: registration.auth.clone(),
        job_id: job.id.clone(),
        attempt_id: assignment.attempt_id,
    };
    let started: Job = client.post("/worker/start", &owner).await.unwrap();
    assert_eq!(started.state, JobState::Running);

    let upload = ArtifactUpload {
        owner: owner.clone(),
        bundle: Bundle {
            files: vec![SnapshotFile {
                path: "result.bin".into(),
                data: payload,
                executable: false,
            }],
        },
    };
    assert!(serde_json::to_vec(&upload).unwrap().len() > 16 * 1024 * 1024);
    client
        .post_with_timeout::<()>(
            "/worker/artifacts",
            &upload,
            runspk::client::TRANSFER_TIMEOUT,
        )
        .await
        .unwrap();
    client
        .post::<Job>(
            "/worker/report",
            &Report {
                auth: registration.auth,
                job_id: job.id.clone(),
                attempt_id: owner.attempt_id,
                logs: Logs::default(),
                exit_code: Some(0),
                error: None,
            },
        )
        .await
        .unwrap();
    let artifacts: Artifacts = client
        .get_with_timeout(
            &format!("/jobs/{}/artifacts", job.id),
            runspk::client::TRANSFER_TIMEOUT,
        )
        .await
        .unwrap();
    assert_eq!(artifacts.bundle.validate().unwrap(), 13 * 1024 * 1024);
    server.abort();
}

#[tokio::test]
async fn project_execution_and_outputs_cross_separate_directories() {
    use runspk::{client::Client, journal::Journal, store::Store};
    use std::sync::{Arc, Mutex};
    let dir = tempfile::tempdir().unwrap();
    let project = dir.path().join("client");
    fs::create_dir(&project).unwrap();
    fs::write(project.join("run.py"),"from pathlib import Path\nprint('staged-ok',flush=True)\nPath('results').mkdir()\nPath('results/answer.txt').write_text(Path('input.txt').read_text().upper())\n").unwrap();
    fs::write(project.join("input.txt"), "experiment").unwrap();
    let bundle = capture_project(&project, Vec::new()).unwrap();
    let journal = Arc::new(Journal::open(&dir.path().join("worker"), "test").unwrap());
    let mut store = Store::open(dir.path().join("state.sqlite")).unwrap();
    let registration = Registration {
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
    };
    store.register(registration.clone()).unwrap();
    let spec:JobSpec=serde_json::from_value(serde_json::json!({"command":["python3","run.py"],"resources":{"cpu":1,"mem_mib":128},"timeout_secs":10,"trusted":true,"cwd":null,"outputs":["results"]})).unwrap();
    let request = ProjectSubmission {
        spec: spec.clone(),
        workspace: bundle.clone(),
    };
    let job = store.submit_project(request).unwrap();
    // Mutation after submission must not alter the immutable worker input.
    fs::write(project.join("input.txt"), "changed").unwrap();
    let assignment = store.claim(&registration.auth).unwrap().unwrap();
    let store = Arc::new(Mutex::new(store));
    let token = "test-token-with-at-least-32-characters";
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let client = Client::new(
        format!("http://{}", listener.local_addr().unwrap()),
        token.into(),
    )
    .unwrap();
    let router = runspk::api::router(store.clone(), token.into()).unwrap().layer(axum::middleware::from_fn(
        |request:axum::http::Request<axum::body::Body>,next:axum::middleware::Next|async move {
            if request.uri().path().starts_with("/workspaces/") {tokio::time::sleep(std::time::Duration::from_millis(6100)).await;}
            next.run(request).await
        }
    ));
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let (_stop, rx) = tokio::sync::watch::channel(false);
    runspk::runner::execute(
        client,
        journal.clone(),
        None,
        assignment,
        vec![runspk::hardware::cpu_ids().unwrap()[0]],
        rx,
    )
    .await
    .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    let mut store = store.lock().unwrap();
    let finished = store.get(&job.id).unwrap();
    assert_eq!(finished.state, JobState::Succeeded, "{finished:?}");
    let output = store.artifacts(&job.id, None).unwrap();
    let fetched = dir.path().join("retrieved");
    materialize(&output.bundle, &fetched).unwrap();
    assert_eq!(
        fs::read_to_string(fetched.join("results/answer.txt")).unwrap(),
        "EXPERIMENT"
    );
    assert!(!journal.workspace_path(&output.attempt_id).unwrap().exists());
    let snapshot = finished.spec.workspace_id.unwrap();
    assert_eq!(
        store
            .prune(&PruneRequest {
                before_ms: now_ms(),
                limit: 10,
                dry_run: false,
            })
            .unwrap(),
        1
    );
    assert!(store.get(&job.id).is_err());
    assert!(store.artifacts(&job.id, None).is_err());
    assert!(store.workspace(&snapshot).is_err());
    server.abort();
}

#[test]
fn capability_gating_and_immutable_owned_outputs() {
    use runspk::store::Store;
    let mut store = Store::open(":memory:").unwrap();
    let mut registration: Registration = serde_json::from_value(serde_json::json!({
        "auth":{"id":"old-worker","session":uuid::Uuid::new_v4().to_string()},
        "capacity":{"cpu":1,"mem_mib":128},"physical":{"cpu":1,"mem_mib":128},
        "allow_native":true,"runtime":null,"gpus":[]
    }))
    .unwrap();
    assert!(!registration.project_staging);
    store.register(registration.clone()).unwrap();
    let spec:JobSpec=serde_json::from_value(serde_json::json!({"command":["/bin/true"],"resources":{"cpu":1,"mem_mib":64},"timeout_secs":10,"trusted":true,"cwd":null,"outputs":["results"],"max_retries":3})).unwrap();
    let job = store
        .submit_project(ProjectSubmission {
            spec,
            workspace: Bundle { files: Vec::new() },
        })
        .unwrap();
    assert!(store.claim(&registration.auth).unwrap().is_none());
    registration.project_staging = true;
    store.register(registration.clone()).unwrap();
    let assignment = store.claim(&registration.auth).unwrap().unwrap();
    let owner = Start {
        auth: registration.auth.clone(),
        job_id: job.id.clone(),
        attempt_id: assignment.attempt_id,
    };
    let mut upload = ArtifactUpload {
        owner: owner.clone(),
        bundle: Bundle {
            files: vec![SnapshotFile {
                path: "results/answer".into(),
                data: "b2s=".into(),
                executable: false,
            }],
        },
    };
    assert!(
        store.save_artifacts(upload.clone()).is_err(),
        "pre-start upload forbidden"
    );
    // A deterministic staging failure must terminate instead of retrying forever at attempts=0.
    let failed = store
        .report(Report {
            auth: owner.auth.clone(),
            job_id: job.id.clone(),
            attempt_id: owner.attempt_id.clone(),
            logs: Logs::default(),
            exit_code: Some(125),
            error: Some("staging failed".into()),
        })
        .unwrap();
    assert_eq!(failed.state, JobState::Failed);
    assert_eq!(failed.attempts, 0);
    let mut spec = failed.spec;
    spec.max_retries = 0;
    spec.submission_key = None;
    let next = store.submit(spec).unwrap();
    let assignment = store.claim(&registration.auth).unwrap().unwrap();
    upload.owner = Start {
        auth: registration.auth.clone(),
        job_id: next.id.clone(),
        attempt_id: assignment.attempt_id,
    };
    store.start(upload.owner.clone()).unwrap();
    let mut wrong = upload.clone();
    wrong.owner.auth.session = uuid::Uuid::new_v4().to_string();
    assert!(store.save_artifacts(wrong).is_err());
    let mut wrong = upload.clone();
    wrong.bundle.files[0].path = "private/token".into();
    assert!(store.save_artifacts(wrong).is_err());
    store.save_artifacts(upload.clone()).unwrap();
    store.save_artifacts(upload.clone()).unwrap();
    assert!(
        store.artifacts(&next.id, None).is_err(),
        "not visible before stopped completion"
    );
    let mut changed = upload.clone();
    changed.bundle.files[0].data = "bm8=".into();
    assert!(store.save_artifacts(changed).is_err());
    store
        .report(Report {
            auth: upload.owner.auth,
            job_id: next.id.clone(),
            attempt_id: upload.owner.attempt_id,
            logs: Logs::default(),
            exit_code: Some(0),
            error: None,
        })
        .unwrap();
    assert_eq!(
        store.artifacts(&next.id, None).unwrap().bundle.files[0].data,
        "b2s="
    );
}
