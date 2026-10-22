use base64::{Engine, engine::general_purpose::STANDARD};
use runspk::{
    config::{ByteSize, MIB, TransferLimits},
    model::*,
    store::Store,
    workspace::*,
};
use sha2::{Digest, Sha256};

fn spec() -> JobSpec {
    serde_json::from_value(serde_json::json!({"command":["true"],"resources":{"cpu":1,"mem_mib":64},"timeout_secs":30,"trusted":true,"outputs":["results"]})).unwrap()
}
fn file(path: &str, data: &[u8]) -> SnapshotFile {
    SnapshotFile {
        path: path.into(),
        data: STANDARD.encode(data),
        executable: false,
    }
}
fn registration(id: &str, limits: Option<TransferLimits>) -> Registration {
    serde_json::from_value(serde_json::json!({"auth":{"id":id,"session":uuid::Uuid::new_v4().to_string()},"capacity":{"cpu":1,"mem_mib":128},"physical":{"cpu":1,"mem_mib":128},"allow_native":true,"project_staging":true,"transfer_limits":limits})).unwrap()
}

#[test]
fn configurable_boundaries_hash_compatibility_and_preflight_no_overwrite() {
    let root = tempfile::tempdir().unwrap();
    std::fs::write(root.path().join("input"), b"1234").unwrap();
    let limits = TransferLimits {
        project_bytes: ByteSize(4),
        artifact_bytes: ByteSize(3),
        max_files: 1,
        max_entries: 2,
        max_depth: 2,
        ..Default::default()
    };
    let bundle = capture_project_with(root.path(), Vec::new(), limits).unwrap();
    assert_eq!(bundle.validate_with(limits, false).unwrap(), 4);
    assert_eq!(
        bundle.id_with(limits).unwrap(),
        format!("{:x}", Sha256::digest(serde_json::to_vec(&bundle).unwrap()))
    );
    assert!(bundle.validate_with(limits, true).is_err());
    std::fs::write(root.path().join("input"), b"12345").unwrap();
    assert!(capture_project_with(root.path(), Vec::new(), limits).is_err());
    let invalid = Bundle {
        files: vec![SnapshotFile {
            path: "safe".into(),
            data: "!corrupt!".into(),
            executable: false,
        }],
    };
    let out = root.path().join("new");
    assert!(materialize_with(&invalid, &out, limits, false).is_err());
    assert!(!out.exists());
    for path in ["a/b/c", "../escape", "/absolute"] {
        assert!(
            Bundle {
                files: vec![file(path, b"")]
            }
            .validate_with(limits, false)
            .is_err()
        );
    }
    let many = Bundle {
        files: vec![file("a", b""), file("b", b"")],
    };
    assert!(many.validate_with(limits, false).is_err());
    let entries = TransferLimits {
        max_files: 2,
        max_entries: 2,
        ..limits
    };
    assert!(
        Bundle {
            files: vec![file("a/b", b""), file("c", b"")]
        }
        .validate_with(entries, false)
        .is_err()
    );
}

#[test]
fn raised_project_survives_restart_and_frozen_outputs_survive_lowered_settings() {
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("scheduler.sqlite");
    let limits = TransferLimits {
        project_bytes: ByteSize(40 * MIB),
        artifact_bytes: ByteSize(8),
        ..Default::default()
    };
    let mut store = Store::open(&db).unwrap();
    store.transfer_limits = limits;
    let bundle = Bundle {
        files: vec![file("input.bin", &vec![b'x'; 32 * MIB + 1])],
    };
    assert!(bundle.validate().is_err());
    let mut request = spec();
    request.transfer_limits = Some(limits);
    request.submission_key = Some("immutable-input".into());
    let job = store
        .submit_project(ProjectSubmission {
            spec: request.clone(),
            workspace: bundle.clone(),
        })
        .unwrap();
    assert_eq!(
        store
            .submit_project(ProjectSubmission {
                spec: request,
                workspace: bundle
            })
            .unwrap()
            .id,
        job.id
    );
    drop(store);
    let mut store = Store::open(&db).unwrap();
    // The old worker understands project staging, but cannot accept the raised envelope.
    let old = registration("legacy", None);
    store.register(old.clone()).unwrap();
    assert!(store.claim(&old.auth).unwrap().is_none());
    let capable = registration("capable", Some(limits));
    store.register(capable.clone()).unwrap();
    let a = store.claim(&capable.auth).unwrap().unwrap();
    assert_eq!(a.job.id, job.id);
    let workspace = store
        .workspace(job.spec.workspace_id.as_ref().unwrap())
        .unwrap();
    assert_eq!(
        workspace.validate_with(limits, false).unwrap(),
        32 * MIB + 1
    );
    let start = Start {
        auth: capable.auth.clone(),
        job_id: job.id.clone(),
        attempt_id: a.attempt_id.clone(),
    };
    store.start(start.clone()).unwrap();
    store.transfer_limits = TransferLimits {
        project_bytes: ByteSize(1),
        artifact_bytes: ByteSize(1),
        ..Default::default()
    };
    assert!(
        store
            .save_artifacts(ArtifactUpload {
                owner: start.clone(),
                bundle: Bundle {
                    files: vec![file("results/too-large", b"123456789")]
                }
            })
            .is_err()
    );
    store
        .save_artifacts(ArtifactUpload {
            owner: start,
            bundle: Bundle {
                files: vec![file("results/ok", b"12345678")],
            },
        })
        .unwrap();
    store
        .report(Report {
            auth: capable.auth,
            job_id: job.id.clone(),
            attempt_id: a.attempt_id,
            logs: Logs::default(),
            exit_code: Some(0),
            error: None,
        })
        .unwrap();
    assert_eq!(
        store.artifacts(&job.id, None).unwrap().bundle.files[0].data,
        STANDARD.encode(b"12345678")
    );
    assert_eq!(store.storage_usage().unwrap()["artifact_bundles"], 1);
    drop(store);
    assert_eq!(
        Store::open(&db).unwrap().get(&job.id).unwrap().state,
        JobState::Succeeded
    );
}

#[tokio::test]
async fn interrupted_oversized_and_corrupt_requests_never_create_partial_jobs() {
    use runspk::{api, client::Client};
    use std::sync::{Arc, Mutex};
    use tokio::io::AsyncWriteExt;
    let store = Arc::new(Mutex::new(Store::open(":memory:").unwrap()));
    store.lock().unwrap().transfer_limits = TransferLimits {
        http_body_bytes: ByteSize(2048),
        ..Default::default()
    };
    let token = "test-token-with-at-least-32-characters";
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let router = api::router(store.clone(), token.into()).unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let client = Client::new(format!("http://{addr}"), token.into()).unwrap();
    let info: serde_json::Value = client.get("/info").await.unwrap();
    assert_eq!(info["transfer_limits"]["http_body_bytes"], "2048B");
    let mut socket = tokio::net::TcpStream::connect(addr).await.unwrap();
    socket.write_all(format!("POST /jobs/project HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer {token}\r\nContent-Type: application/json\r\nContent-Length: 2000\r\n\r\n{{\"spec\":").as_bytes()).await.unwrap();
    drop(socket);
    let mut s = spec();
    s.outputs.clear();
    let request = ProjectSubmission {
        spec: s,
        workspace: Bundle {
            files: vec![SnapshotFile {
                path: "bad".into(),
                data: "!!!!".into(),
                executable: false,
            }],
        },
    };
    // Wait for the disconnected request to relinquish admission rather than racing its socket teardown.
    for _ in 0..30 {
        let err = client
            .post::<Job>("/jobs/project", &request)
            .await
            .unwrap_err();
        if !err.to_string().contains("503") {
            assert!(err.to_string().contains("invalid snapshot base64"), "{err}");
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    let http = reqwest::Client::new();
    let response = http
        .post(format!("http://{addr}/jobs/project"))
        .bearer_auth(token)
        .header("content-type", "application/json")
        .body("x".repeat(2049))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 413);
    assert!(store.lock().unwrap().list().unwrap().is_empty());
    server.abort();
}

#[tokio::test]
async fn old_info_defaults_and_bounded_responses() {
    use axum::{Json, Router, routing::get};
    use runspk::client::Client;
    let router=Router::new().route("/info",get(||async{Json(serde_json::json!({"features":["project-snapshots-v1","artifacts-v1"],"snapshot_max_bytes":32*MIB}))})).route("/large",get(||async{"x".repeat(4096)}));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let client = Client::new(
        format!("http://{addr}"),
        "test-token-with-at-least-32-characters".into(),
    )
    .unwrap();
    assert_eq!(
        client.negotiated_limits().await.unwrap(),
        TransferLimits::default()
    );
    assert!(!client.info().await.unwrap().supports("worker-labels-v1"));
    let client = client
        .with_limits(TransferLimits {
            http_body_bytes: ByteSize(2048),
            ..Default::default()
        })
        .unwrap();
    let error = client
        .get_with_timeout::<serde_json::Value>("/large", client.transfer_timeout())
        .await
        .unwrap_err();
    assert!(error.to_string().contains("response exceeds 2048"));
    server.abort();
}

#[test]
fn deserialization_bounds_file_inventory_before_full_allocation() {
    let file = r#"{"path":"a","data":"","executable":false}"#;
    let data = format!(
        r#"{{"files":[{}]}}"#,
        std::iter::repeat_n(file, 16385)
            .collect::<Vec<_>>()
            .join(",")
    );
    let error = serde_json::from_str::<Bundle>(&data).unwrap_err();
    assert!(error.to_string().contains("hard ceiling of 16384 files"));
}

#[test]
fn identical_keyed_retry_preserves_admission_envelope_after_defaults_change() {
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("keys.sqlite");
    let mut store = Store::open(&db).unwrap();
    let mut original = spec();
    original.submission_key = Some("legacy-retry".into());
    let bundle = Bundle::default();
    let admitted = store
        .submit_project(ProjectSubmission {
            spec: original.clone(),
            workspace: bundle.clone(),
        })
        .unwrap();
    drop(store);
    let mut store = Store::open(&db).unwrap();
    store.transfer_limits = TransferLimits {
        project_bytes: ByteSize(16 * MIB),
        ..Default::default()
    };
    assert_eq!(
        store
            .submit_project(ProjectSubmission {
                spec: original.clone(),
                workspace: bundle.clone()
            })
            .unwrap()
            .id,
        admitted.id
    );
    assert_eq!(store.submit(admitted.spec.clone()).unwrap().id, admitted.id);
    original.command = vec!["false".into()];
    assert!(
        store
            .submit_project(ProjectSubmission {
                spec: original,
                workspace: bundle
            })
            .is_err()
    );
    assert_eq!(store.list().unwrap().len(), 1);
    assert_eq!(
        store.get(&admitted.id).unwrap().spec.transfer_limits,
        Some(TransferLimits::default())
    );
}

#[test]
fn encoded_budget_counts_paths_and_envelope_without_losing_decoded_boundaries() {
    let bundle = Bundle {
        files: vec![file("result", &[b'x'; 100])],
    };
    let owner = Start {
        auth: registration("wire", None).auth,
        job_id: uuid::Uuid::new_v4().to_string(),
        attempt_id: uuid::Uuid::new_v4().to_string(),
    };
    let actual = serde_json::to_vec(&ArtifactUpload {
        owner: owner.clone(),
        bundle: bundle.clone(),
    })
    .unwrap()
    .len();
    assert!(check_artifact_size(&owner, &bundle, actual).is_ok());
    assert!(check_artifact_size(&owner, &bundle, actual - 1).is_err());
    assert!(bundle.validate().is_ok());
}
