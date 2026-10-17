use runspk::{model::*, store::Store};
fn spec() -> JobSpec {
    JobSpec {
        command: vec!["/bin/echo".into(), "hello".into()],
        resources: Resources {
            cpu: 1,
            mem_mib: 64,
        },
        timeout_secs: 2,
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
        auth: WorkerAuth {
            id: "test".into(),
            session: uuid::Uuid::new_v4().to_string(),
        },
        capacity: Resources {
            cpu: 1,
            mem_mib: 64,
        },
        physical: Resources {
            cpu: 1,
            mem_mib: 64,
        },
        allow_native: true,
        runtime: None,
        gpus: Vec::new(),
        project_staging: true,
    }
}
#[test]
fn durable_queue_completion_and_cancel() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("state.sqlite");
    let mut s = Store::open(&path).unwrap();
    let registration = registration();
    let auth = registration.auth.clone();
    s.register(registration).unwrap();
    let j = s.submit(spec()).unwrap();
    drop(s);
    let mut s = Store::open(&path).unwrap();
    assert_eq!(s.get(&j.id).unwrap().state, JobState::Queued);
    let a = s.claim(&auth).unwrap().unwrap();
    assert_eq!(s.claim(&auth).unwrap().unwrap().attempt_id, a.attempt_id);
    s.start(Start {
        auth: auth.clone(),
        job_id: j.id.clone(),
        attempt_id: a.attempt_id.clone(),
    })
    .unwrap();
    assert!(s.claim(&auth).unwrap().is_none());
    s.cancel(&j.id).unwrap();
    assert_eq!(
        s.report(Report {
            auth: auth.clone(),
            job_id: j.id.clone(),
            attempt_id: a.attempt_id.clone(),
            logs: Logs::default(),
            exit_code: Some(0),
            error: None
        })
        .unwrap()
        .state,
        JobState::Cancelled
    );
    assert_eq!(
        s.report(Report {
            auth: auth.clone(),
            job_id: j.id.clone(),
            attempt_id: a.attempt_id.clone(),
            logs: Logs::default(),
            exit_code: Some(1),
            error: None
        })
        .unwrap()
        .state,
        JobState::Cancelled
    );
    let j = s.submit(spec()).unwrap();
    s.cancel(&j.id).unwrap();
    assert!(s.claim(&auth).unwrap().is_none());
}
#[tokio::test]
async fn execution_and_timeout() {
    use std::sync::{Arc, Mutex};
    let s = Arc::new(Mutex::new(Store::open(":memory:").unwrap()));
    let dir = tempfile::tempdir().unwrap();
    let journal = Arc::new(runspk::journal::Journal::open(dir.path(), "test").unwrap());
    let mut registration = registration();
    registration.auth = journal.auth.clone();
    let auth = registration.auth.clone();
    s.lock().unwrap().register(registration).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let token = "test-token-with-at-least-32-characters".to_string();
    let client = runspk::client::Client::new(
        format!("http://{}", listener.local_addr().unwrap()),
        token.clone(),
    )
    .unwrap();
    let stall_report = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let stall = stall_report.clone();
    let router = runspk::api::router(s.clone(), token)
        .unwrap()
        .layer(axum::middleware::from_fn(
            move |request: axum::extract::Request, next: axum::middleware::Next| {
                let stall = stall.clone();
                async move {
                    if request.uri().path() == "/worker/report"
                        && stall.swap(false, std::sync::atomic::Ordering::SeqCst)
                    {
                        std::future::pending::<()>().await;
                    }
                    next.run(request).await
                }
            },
        ));
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let j = s.lock().unwrap().submit(spec()).unwrap();
    let a = s.lock().unwrap().claim(&auth).unwrap().unwrap();
    let (_tx, rx) = tokio::sync::watch::channel(false);
    runspk::runner::execute(
        client.clone(),
        journal.clone(),
        None,
        a,
        vec![runspk::hardware::cpu_ids().unwrap()[0]],
        rx,
    )
    .await
    .unwrap();
    assert_eq!(
        s.lock().unwrap().get(&j.id).unwrap().state,
        JobState::Succeeded
    );
    assert_eq!(s.lock().unwrap().logs(&j.id).unwrap().stdout, "hello\n");
    for (command, stalled, expected_code, expected_error) in [
        (vec!["/bin/sh", "-c", "exit 124"], false, 124, None),
        (vec!["/bin/sh", "-c", "exit 137"], false, 137, None),
        (
            vec!["/bin/sleep", "10"],
            false,
            124,
            Some("wall time exceeded"),
        ),
        (
            vec!["/bin/sleep", "10"],
            true,
            124,
            Some("wall time exceeded"),
        ),
    ] {
        let mut spec = spec();
        spec.command = command.into_iter().map(str::to_string).collect();
        spec.timeout_secs = 1;
        let j = s.lock().unwrap().submit(spec).unwrap();
        let a = s.lock().unwrap().claim(&auth).unwrap().unwrap();
        stall_report.store(stalled, std::sync::atomic::Ordering::SeqCst);
        let (_tx, rx) = tokio::sync::watch::channel(false);
        runspk::runner::execute(
            client.clone(),
            journal.clone(),
            None,
            a,
            vec![runspk::hardware::cpu_ids().unwrap()[0]],
            rx,
        )
        .await
        .unwrap();
        assert!(!stall_report.load(std::sync::atomic::Ordering::SeqCst));
        let j = s.lock().unwrap().get(&j.id).unwrap();
        assert_eq!(j.state, JobState::Failed);
        assert_eq!(j.exit_code, Some(expected_code));
        assert_eq!(j.error.as_deref(), expected_error);
        assert!(journal.entries().await.unwrap().is_empty());
    }
    server.abort();
}
