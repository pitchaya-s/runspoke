use base64::{Engine, engine::general_purpose::STANDARD};
use reqwest::StatusCode;
use runspk::{
    api,
    config::{MIB, TransferLimits},
    model::{Job, JobSpec},
    store::Store,
    workspace::{Bundle, ProjectSubmission, SnapshotFile},
};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpSocket},
};

const TOKEN: &str = "backpressure-test-token-at-least-32-characters";

async fn check_slow_reader(disconnect: bool) {
    let spec: JobSpec = serde_json::from_value(serde_json::json!({
        "command": ["true"], "resources": {"cpu": 1, "mem_mib": 64},
        "timeout_secs": 60, "trusted": true
    }))
    .unwrap();
    let mut store = Store::open(":memory:").unwrap();
    store.transfer_limits = TransferLimits {
        timeout_secs: 3,
        ..Default::default()
    };
    let job = store
        .submit_project(ProjectSubmission {
            spec: spec.clone(),
            workspace: Bundle {
                files: vec![SnapshotFile {
                    path: "large.bin".into(),
                    data: STANDARD.encode(vec![b'x'; 12 * MIB]),
                    executable: false,
                }],
            },
        })
        .unwrap();
    let router = api::router(Arc::new(Mutex::new(store)), TOKEN.into()).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let url = format!("http://{addr}");
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(1))
        .build()
        .unwrap();
    let submission = ProjectSubmission {
        spec,
        workspace: Bundle::default(),
    };

    let socket = TcpSocket::new_v4().unwrap();
    socket.set_recv_buffer_size(4096).unwrap();
    let mut socket = socket.connect(addr).await.unwrap();
    socket.write_all(format!(
        "GET /workspaces/{} HTTP/1.1\r\nHost: {addr}\r\nAuthorization: Bearer {TOKEN}\r\nConnection: close\r\n\r\n",
        job.spec.workspace_id.as_ref().unwrap()
    ).as_bytes()).await.unwrap();
    let mut received = vec![0; 256];
    tokio::time::timeout(Duration::from_secs(3), socket.read_exact(&mut received))
        .await
        .unwrap()
        .unwrap();
    assert!(received.starts_with(b"HTTP/1.1 200 OK\r\n"));
    // Leave the response stopped at 256 bytes so the kernel send buffer fills.
    tokio::time::sleep(Duration::from_millis(100)).await;
    let blocked = http
        .post(format!("{url}/jobs/project"))
        .bearer_auth(TOKEN)
        .json(&submission)
        .send()
        .await
        .unwrap();
    assert_eq!(blocked.status(), StatusCode::SERVICE_UNAVAILABLE);
    for path in ["/info".to_owned(), format!("/jobs/{}", job.id)] {
        let response = http
            .get(format!("{url}{path}"))
            .bearer_auth(TOKEN)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        response.json::<serde_json::Value>().await.unwrap();
    }

    let socket = if disconnect {
        drop(socket);
        None
    } else {
        Some(socket)
    };
    let released =
        tokio::time::timeout(Duration::from_secs(if disconnect { 1 } else { 4 }), async {
            loop {
                let response = http
                    .post(format!("{url}/jobs/project"))
                    .bearer_auth(TOKEN)
                    .json(&submission)
                    .send()
                    .await
                    .unwrap();
                if response.status() != StatusCode::SERVICE_UNAVAILABLE {
                    assert_eq!(response.status(), StatusCode::OK);
                    return response.json::<Job>().await.unwrap();
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("transfer admission was not released");
    assert_ne!(released.id, job.id);

    if let Some(mut socket) = socket {
        let read = tokio::time::timeout(Duration::from_secs(5), socket.read_to_end(&mut received))
            .await
            .expect("expired response did not close");
        if let Err(error) = read {
            assert!(
                matches!(
                    error.kind(),
                    std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::UnexpectedEof
                ),
                "{error}"
            );
        }
        let split = received
            .windows(4)
            .position(|bytes| bytes == b"\r\n\r\n")
            .unwrap();
        let headers = std::str::from_utf8(&received[..split]).unwrap();
        let declared: usize = headers
            .lines()
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse().unwrap())
            })
            .expect("the response must preserve Content-Length");
        let body = &received[split + 4..];
        assert!(declared > 12 * MIB);
        assert!(body.len() < declared, "expired download appeared complete");
        assert!(serde_json::from_slice::<Bundle>(body).is_err());
    }
    server.abort();
}

#[tokio::test]
async fn slow_reader_holds_admission_until_disconnect_without_blocking_control() {
    check_slow_reader(true).await;
}

#[tokio::test]
async fn slow_reader_deadline_releases_admission_and_truncates_the_response() {
    check_slow_reader(false).await;
}
