use runspk::{
    model::*,
    store::{CompletionPruned, SCHEMA_VERSION, Store, backup_database},
};
use rusqlite::Connection;

fn spec() -> JobSpec {
    serde_json::from_value(serde_json::json!({"command":["/bin/true"],"resources":{"cpu":1,"mem_mib":64},"timeout_secs":10,"trusted":true,"cwd":null})).unwrap()
}
fn registration() -> Registration {
    Registration {
        labels: Default::default(),
        transfer_limits: None,
        auth: WorkerAuth {
            id: "storage".into(),
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
#[test]
fn indexed_history_pagination_and_saturated_polls() {
    let mut s = Store::open(":memory:").unwrap();
    for _ in 0..3000 {
        let j = s.submit(spec()).unwrap();
        s.cancel(&j.id).unwrap();
    }
    for _ in 0..1000 {
        s.submit(spec()).unwrap();
    }
    let r = registration();
    s.register(r.clone()).unwrap();
    let a = s.claim(&r.auth).unwrap().unwrap();
    s.start(Start {
        auth: r.auth.clone(),
        job_id: a.job.id.clone(),
        attempt_id: a.attempt_id,
    })
    .unwrap();
    let passes = s.metrics.passes;
    for _ in 0..100 {
        assert!(s.claim(&r.auth).unwrap().is_none());
    }
    assert_eq!(
        s.metrics.passes, passes,
        "saturated polling must not replan the queue"
    );
    let mut filter = JobFilter {
        search: None,
        tag: None,
        state: Some(JobState::Queued),
        limit: Some(37),
        ..Default::default()
    };
    let mut count = 0;
    loop {
        let page = s.list_filtered(&filter).unwrap();
        if page.is_empty() {
            break;
        }
        assert!(
            page.iter()
                .all(|j| j.state == JobState::Queued && j.seq > filter.after)
        );
        assert!(page.len() <= 37);
        count += page.len();
        filter.after = page.last().unwrap().seq;
    }
    assert_eq!(count, 999);
    assert_eq!(s.stats().unwrap()["jobs"], 4000);
    filter.limit = Some(1001);
    assert!(s.list_filtered(&filter).is_err());
    filter.limit = Some(1);
    filter.after = -1;
    assert!(s.list_filtered(&filter).is_err());
}

#[tokio::test]
async fn pruning_preserves_active_attempts_keys_and_completion_receipts() {
    use std::sync::{Arc, Mutex};
    let dir = tempfile::tempdir().unwrap();
    let journal = Arc::new(runspk::journal::Journal::open(dir.path(), "storage").unwrap());
    let mut s = Store::open(":memory:").unwrap();
    let mut r = registration();
    r.auth = journal.auth.clone();
    s.register(r.clone()).unwrap();
    let mut keyed = spec();
    keyed.submission_key = Some("keep-key".into());
    let key = s.submit(keyed.clone()).unwrap();
    s.cancel(&key.id).unwrap();
    let done = s.submit(spec()).unwrap();
    let a = s.claim(&r.auth).unwrap().unwrap();
    s.start(Start {
        auth: r.auth.clone(),
        job_id: done.id.clone(),
        attempt_id: a.attempt_id.clone(),
    })
    .unwrap();
    let report = Report {
        auth: r.auth.clone(),
        job_id: done.id.clone(),
        attempt_id: a.attempt_id,
        logs: Logs {
            stdout: "keep until pruned".into(),
            ..Default::default()
        },
        exit_code: Some(0),
        error: None,
    };
    let completed = s.report(report.clone()).unwrap();
    let running = s.submit(spec()).unwrap();
    let a = s.claim(&r.auth).unwrap().unwrap();
    s.start(Start {
        auth: r.auth.clone(),
        job_id: running.id.clone(),
        attempt_id: a.attempt_id,
    })
    .unwrap();
    s.expire(now_ms() + LEASE_MS + 1).unwrap();
    let queued = s.submit(spec()).unwrap();
    for _ in 0..3 {
        let j = s.submit(spec()).unwrap();
        s.cancel(&j.id).unwrap();
    }
    let cutoff = now_ms();
    std::thread::sleep(std::time::Duration::from_millis(2));
    let mut request = PruneRequest {
        before_ms: cutoff + 1,
        limit: 2,
        dry_run: true,
    };
    assert_eq!(s.prune(&request).unwrap(), 2);
    assert_eq!(s.list().unwrap().len(), 7);
    request.dry_run = false;
    assert_eq!(s.prune(&request).unwrap(), 2);
    assert_eq!(s.prune(&request).unwrap(), 2);
    assert_eq!(s.prune(&request).unwrap(), 0);
    assert!(s.get(&done.id).is_err());
    assert!(
        s.report(report.clone())
            .unwrap_err()
            .downcast_ref::<CompletionPruned>()
            .is_some()
    );
    assert_eq!(s.submit(keyed).unwrap().id, key.id);
    assert_eq!(s.get(&running.id).unwrap().state, JobState::Uncertain);
    assert_eq!(s.get(&queued.id).unwrap().state, JobState::Queued);
    assert_eq!(s.workers().unwrap()[0].reserved.cpu, 1);
    // A worker with a lost completion acknowledgement must retire its journal after pruning.
    journal
        .save(&runspk::journal::Entry {
            assignment: Assignment {
                job: completed,
                attempt_id: report.attempt_id.clone(),
            },
            process: None,
            completion: Some(report.clone()),
            runtime: None,
            artifacts: None,
            keep_workspace: false,
        })
        .await
        .unwrap();
    let store = Arc::new(Mutex::new(s));
    let token = "test-token-with-at-least-32-characters";
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let client = runspk::client::Client::new(
        format!("http://{}", listener.local_addr().unwrap()),
        token.into(),
    )
    .unwrap();
    let router = runspk::api::router(store, token.into()).unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let error = client
        .post::<Job>("/worker/report", &report)
        .await
        .unwrap_err();
    assert_eq!(
        error
            .downcast_ref::<runspk::client::HttpError>()
            .unwrap()
            .status,
        reqwest::StatusCode::GONE
    );
    let (stop, rx) = tokio::sync::watch::channel(false);
    let worker = tokio::spawn(runspk::runner::worker(client, r, journal.clone(), rx));
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(3);
    while !journal.entries().await.unwrap().is_empty() {
        assert!(tokio::time::Instant::now() < deadline);
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    stop.send(true).unwrap();
    worker.await.unwrap().unwrap();
    server.abort();
}

#[test]
fn initial_schema_reopen_and_online_backup_preserve_records() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("state.sqlite");
    let mut s = Store::open(&path).unwrap();
    let mut keyed = spec();
    keyed.submission_key = Some("persisted-key".into());
    let j = s.submit(keyed.clone()).unwrap();
    drop(s);
    let mut s = Store::open(&path).unwrap();
    assert_eq!(s.submit(keyed.clone()).unwrap().id, j.id);
    let mut changed = keyed.clone();
    changed.command = vec!["/bin/false".into()];
    assert!(s.submit(changed).is_err());
    assert_eq!(s.stats().unwrap()["schema_version"], SCHEMA_VERSION);
    assert!(Store::open(&path).is_err(), "exclusive database ownership");
    let conn = Connection::open(&path).unwrap();
    assert_eq!(
        conn.query_row("PRAGMA user_version", [], |r| r.get::<_, u32>(0))
            .unwrap(),
        SCHEMA_VERSION
    );
    let plan: String = conn.query_row("EXPLAIN QUERY PLAN SELECT data FROM jobs WHERE state IN ('assigned','running','cancelling') AND lease_until_ms>0 AND lease_until_ms<=10", [], |r| r.get(3)).unwrap();
    assert!(plan.contains("jobs_lease"), "{plan}");
    let snapshot = dir.path().join("backup.sqlite");
    // Keep the scheduler's write connection open; the backup reads committed WAL data.
    s.submit(spec()).unwrap();
    backup_database(&path, &snapshot).unwrap();
    use std::os::unix::fs::PermissionsExt;
    assert_eq!(
        std::fs::metadata(&snapshot).unwrap().permissions().mode() & 0o077,
        0
    );
    assert!(
        backup_database(&path, &snapshot).is_err(),
        "backup must not overwrite a file"
    );
    let mut backup = Store::open(&snapshot).unwrap();
    assert_eq!(backup.list().unwrap().len(), 2);
    assert_eq!(backup.submit(keyed).unwrap().id, j.id);
    assert_eq!(
        Connection::open(&snapshot)
            .unwrap()
            .query_row("PRAGMA integrity_check", [], |r| r.get::<_, String>(0))
            .unwrap(),
        "ok"
    );
}

#[test]
fn concurrent_submissions_keep_keys_and_sequences_unique() {
    let store = std::sync::Arc::new(std::sync::Mutex::new(Store::open(":memory:").unwrap()));
    let threads: Vec<_> = (0..8)
        .map(|_| {
            let store = store.clone();
            std::thread::spawn(move || {
                for _ in 0..50 {
                    let mut s = store.lock().unwrap();
                    s.submit(spec()).unwrap();
                    let mut key = spec();
                    key.submission_key = Some("concurrent".into());
                    s.submit(key).unwrap();
                }
            })
        })
        .collect();
    for thread in threads {
        thread.join().unwrap();
    }
    let jobs = store.lock().unwrap().list().unwrap();
    assert_eq!(jobs.len(), 401);
    assert_eq!(
        jobs.iter()
            .map(|j| j.seq)
            .collect::<std::collections::BTreeSet<_>>()
            .len(),
        401
    );
}

#[test]
fn incompatible_and_damaged_databases_are_refused_without_modification() {
    let dir = tempfile::tempdir().unwrap();
    for (name, sql, error) in [
        (
            "future",
            "CREATE TABLE preserved(data TEXT); INSERT INTO preserved VALUES('keep'); PRAGMA user_version=2;",
            "incompatible database schema",
        ),
        (
            "unrecognized",
            "CREATE TABLE preserved(data TEXT); INSERT INTO preserved VALUES('keep');",
            "unrecognized database layout",
        ),
        (
            "unrecognized-prefix",
            "CREATE TABLE sqliteXpreserved(data TEXT); INSERT INTO sqliteXpreserved VALUES('keep');",
            "unrecognized database layout",
        ),
        (
            "incomplete",
            "CREATE TABLE jobs(id TEXT); PRAGMA user_version=1;",
            "incompatible database layout",
        ),
    ] {
        let path = dir.path().join(format!("{name}.sqlite"));
        let conn = Connection::open(&path).unwrap();
        conn.execute_batch(sql).unwrap();
        drop(conn);
        let before = std::fs::read(&path).unwrap();
        let actual = Store::open(&path)
            .err()
            .expect("must refuse incompatible state");
        assert!(actual.to_string().contains(error), "{actual:#}");
        assert_eq!(std::fs::read(&path).unwrap(), before, "{name} was modified");
        assert!(!path.with_extension("sqlite-wal").exists());
    }
    for (name, column) in [
        ("ordinary-column", "finished_ms INTEGER DEFAULT 0"),
        (
            "changed-expression",
            "finished_ms INTEGER GENERATED ALWAYS AS (0) VIRTUAL",
        ),
    ] {
        let path = dir.path().join(format!("{name}.sqlite"));
        drop(Store::open(&path).unwrap());
        let conn = Connection::open(&path).unwrap();
        let sql: String = conn
            .query_row("SELECT sql FROM sqlite_master WHERE name='jobs'", [], |r| {
                r.get(0)
            })
            .unwrap();
        let changed = sql.replace(
            "finished_ms INTEGER GENERATED ALWAYS AS (json_extract(data,'$.finished_ms')) VIRTUAL",
            column,
        );
        assert_ne!(sql, changed);
        conn.execute_batch("DROP TABLE jobs").unwrap();
        conn.execute_batch(&changed).unwrap();
        drop(conn);
        let before = std::fs::read(&path).unwrap();
        assert!(
            Store::open(&path)
                .err()
                .unwrap()
                .to_string()
                .contains("incompatible database layout")
        );
        assert_eq!(std::fs::read(&path).unwrap(), before);
    }
    let corrupt = dir.path().join("corrupt.sqlite");
    std::fs::write(&corrupt, b"damaged database contents").unwrap();
    assert!(Store::open(&corrupt).is_err());
    assert_eq!(
        std::fs::read(&corrupt).unwrap(),
        b"damaged database contents"
    );
}
