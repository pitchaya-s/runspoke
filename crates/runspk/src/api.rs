use crate::{model::*, store::Store};
use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, Path, Query, State},
    http::{Request, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use std::sync::{Arc, Mutex};
use tokio::sync::Semaphore;

pub type Shared = Arc<Mutex<Store>>;
type ApiResult<T> = Result<Json<T>, ApiError>;
pub struct ApiError(anyhow::Error);
impl From<anyhow::Error> for ApiError {
    fn from(e: anyhow::Error) -> Self {
        Self(e)
    }
}
impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let message = self.0.to_string();
        let status = if self
            .0
            .downcast_ref::<crate::store::CompletionPruned>()
            .is_some()
        {
            StatusCode::GONE
        } else if message.contains("not found") {
            StatusCode::NOT_FOUND
        } else {
            StatusCode::BAD_REQUEST
        };
        (status, Json(serde_json::json!({"error":message}))).into_response()
    }
}
pub async fn db<T: Send + 'static>(
    store: Shared,
    f: impl FnOnce(&mut Store) -> anyhow::Result<T> + Send + 'static,
) -> anyhow::Result<T> {
    tokio::task::spawn_blocking(move || {
        let waiting = std::time::Instant::now();
        let mut store = store
            .lock()
            .map_err(|_| anyhow::anyhow!("store lock poisoned"))?;
        let wait_ns = waiting.elapsed().as_nanos() as u64;
        let started = std::time::Instant::now();
        let result = f(&mut store);
        let elapsed = started.elapsed().as_nanos() as u64;
        store.database_metrics.operations += 1;
        store.database_metrics.total_ns += elapsed;
        store.database_metrics.max_ns = store.database_metrics.max_ns.max(elapsed);
        store.database_metrics.mutex_wait_ns += wait_ns;
        result
    })
    .await?
}
#[derive(Clone)]
struct AuthState {
    token: String,
    transfers: Arc<Semaphore>,
    limits: crate::config::TransferLimits,
}
async fn authorize(
    State(state): State<AuthState>,
    mut request: Request<axum::body::Body>,
    next: Next,
) -> Response {
    if request
        .headers()
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        != Some(&format!("Bearer {}", state.token))
    {
        return (StatusCode::UNAUTHORIZED, "authentication required").into_response();
    }
    let method = request.method().clone();
    let path = request.uri().path().to_owned();
    let transfer = (method == axum::http::Method::POST
        && matches!(path.as_str(), "/jobs/project" | "/worker/artifacts"))
        || (method == axum::http::Method::GET
            && (path.starts_with("/workspaces/") || path.ends_with("/artifacts")));
    let permit = if transfer {
        match state.transfers.clone().try_acquire_owned() {
            Ok(permit) => Some(Arc::new(permit)),
            Err(_) => return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(
                    serde_json::json!({"error":"another bundle transfer is active; retry shortly"}),
                ),
            )
                .into_response(),
        }
    } else {
        None
    };
    if let Some(permit) = &permit {
        request.extensions_mut().insert(permit.clone());
    }
    let started = std::time::Instant::now();
    let deadline = tokio::time::Instant::now() + state.limits.timeout();
    let response = match tokio::time::timeout(if transfer {state.limits.timeout()} else {std::time::Duration::from_secs(30)},next.run(request)).await {
        Ok(response)=>response,
        Err(_)=>return (StatusCode::REQUEST_TIMEOUT,Json(serde_json::json!({"error":"request timed out; check status before retrying a submission"}))).into_response(),
    };
    tracing::debug!(%method,%path,status=response.status().as_u16(),elapsed_us=started.elapsed().as_micros(),"request completed");
    if let Some(permit) = permit {
        let (parts, body) = response.into_parts();
        Response::from_parts(
            parts,
            axum::body::Body::new(TransferBody::new(body, permit, deadline)),
        )
    } else {
        response
    }
}
// Copy bounded frames: Bytes slices would pin the entire JSON allocation in a slow socket.
struct TransferBody {
    receiver: tokio::sync::mpsc::Receiver<Result<axum::body::Bytes, axum::Error>>,
    remaining: Option<u64>,
}
impl TransferBody {
    fn new(
        mut body: axum::body::Body,
        permit: Arc<tokio::sync::OwnedSemaphorePermit>,
        deadline: tokio::time::Instant,
    ) -> Self {
        use axum::body::HttpBody;
        let remaining = body.size_hint().exact();
        let (sender, receiver) = tokio::sync::mpsc::channel(1);
        tokio::spawn(async move {
            let _permit = permit;
            let produce = async {
                while let Some(frame) =
                    std::future::poll_fn(|cx| std::pin::Pin::new(&mut body).poll_frame(cx)).await
                {
                    match frame {
                        Ok(frame) => {
                            if let Ok(bytes) = frame.into_data() {
                                for chunk in bytes.chunks(64 * 1024) {
                                    if sender
                                        .send(Ok(axum::body::Bytes::copy_from_slice(chunk)))
                                        .await
                                        .is_err()
                                    {
                                        return;
                                    }
                                }
                            }
                        }
                        Err(error) => {
                            let _ = sender.send(Err(error)).await;
                            return;
                        }
                    }
                }
            };
            if tokio::time::timeout_at(deadline, produce).await.is_err() {
                // A full channel closes on drop; Content-Length exposes the incomplete transfer too.
                let _ = sender.try_send(Err(axum::Error::new(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "transfer response deadline exceeded",
                ))));
            }
        });
        Self {
            receiver,
            remaining,
        }
    }
}
impl axum::body::HttpBody for TransferBody {
    type Data = axum::body::Bytes;
    type Error = axum::Error;
    fn poll_frame(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        self.receiver.poll_recv(cx).map(|frame| {
            frame.map(|result| {
                result.map(|bytes| {
                    self.remaining = self
                        .remaining
                        .map(|left| left.saturating_sub(bytes.len() as u64));
                    http_body::Frame::data(bytes)
                })
            })
        })
    }
    fn size_hint(&self) -> http_body::SizeHint {
        self.remaining.map_or_else(
            http_body::SizeHint::default,
            http_body::SizeHint::with_exact,
        )
    }
}
pub fn router(store: Shared, token: String) -> anyhow::Result<Router> {
    anyhow::ensure!(
        token.len() >= 32,
        "RUNSPK_TOKEN must contain at least 32 characters"
    );
    let limits = store
        .lock()
        .map_err(|_| anyhow::anyhow!("store lock poisoned"))?
        .transfer_limits;
    limits.validate()?;
    Ok(Router::new()
        .route("/jobs", get(list).post(submit))
        .route(
            "/jobs/project",
            post(submit_project).layer(DefaultBodyLimit::max(limits.http_body_bytes.0)),
        )
        .route("/workspaces/{id}", get(workspace))
        .route("/jobs/{id}/artifacts", get(artifacts))
        .route(
            "/worker/artifacts",
            post(upload_artifacts).layer(DefaultBodyLimit::max(crate::config::HARD_HTTP_BYTES)),
        )
        .route("/info", get(info))
        .route("/jobs/{id}", get(status))
        .route("/jobs/{id}/cancel", post(cancel))
        .route("/jobs/{id}/logs", get(logs))
        .route("/jobs/{id}/logs/chunk", get(log_chunk))
        .route("/jobs/{id}/attempts", get(attempts))
        .route("/jobs/{id}/resolve", post(resolve))
        .route("/jobs/{id}/explain", get(explain))
        .route("/history/prune", post(prune))
        .route("/workers", get(workers))
        .route("/stats", get(stats))
        .route("/storage", get(storage))
        .route("/worker/register", post(register))
        .route("/worker/poll", post(poll))
        .route("/worker/heartbeat", post(heartbeat))
        .route("/worker/drain", post(drain))
        .route("/worker/report", post(report))
        .route("/worker/start", post(start))
        .with_state(store)
        .layer(DefaultBodyLimit::max(16 * 1024 * 1024))
        .layer(middleware::from_fn_with_state(
            AuthState {
                token,
                transfers: Arc::new(Semaphore::new(1)),
                limits,
            },
            authorize,
        )))
}
async fn submit(State(s): State<Shared>, Json(spec): Json<JobSpec>) -> ApiResult<Job> {
    Ok(Json(db(s, move |s| s.submit(spec)).await?))
}
async fn info(State(s): State<Shared>) -> ApiResult<serde_json::Value> {
    Ok(Json(db(s,|s|Ok(serde_json::json!({"version":env!("CARGO_PKG_VERSION"),"schema_version":crate::store::SCHEMA_VERSION,"features":["project-snapshots-v1","artifacts-v1","transfer-limits-v1","worker-labels-v1","job-metadata-v1","log-offsets-v1","artifact-selection-v1","storage-usage-v1","host-telemetry-v1"],"snapshot_max_bytes":s.transfer_limits.project_bytes.0,"transfer_limits":s.transfer_limits}))).await?))
}
async fn submit_project(
    State(s): State<Shared>,
    axum::Extension(permit): axum::Extension<Arc<tokio::sync::OwnedSemaphorePermit>>,
    Json(request): Json<crate::workspace::ProjectSubmission>,
) -> ApiResult<Job> {
    Ok(Json(
        db(s, move |s| {
            let _permit = permit;
            s.submit_project(request)
        })
        .await?,
    ))
}
async fn workspace(
    State(s): State<Shared>,
    Path(id): Path<String>,
    axum::Extension(permit): axum::Extension<Arc<tokio::sync::OwnedSemaphorePermit>>,
) -> ApiResult<crate::workspace::Bundle> {
    Ok(Json(
        db(s, move |s| {
            let _permit = permit;
            s.workspace(&id)
        })
        .await?,
    ))
}
#[derive(serde::Deserialize)]
struct ArtifactQuery {
    attempt: Option<String>,
    path: Option<String>,
    #[serde(default)]
    manifest: bool,
}
#[derive(serde::Serialize)]
#[serde(untagged)]
enum ArtifactResponse {
    Manifest(serde_json::Value),
    Bundle(crate::workspace::Artifacts),
}
async fn artifacts(
    State(s): State<Shared>,
    Path(id): Path<String>,
    Query(query): Query<ArtifactQuery>,
    axum::Extension(permit): axum::Extension<Arc<tokio::sync::OwnedSemaphorePermit>>,
) -> ApiResult<ArtifactResponse> {
    Ok(Json(db(s,move |s| {
        let _permit=permit;
        if let Some(path)=&query.path {crate::workspace::validate_path(path)?;}
        let mut artifacts=s.artifacts(&id,query.attempt.as_deref())?;
        let limits=crate::config::TransferLimits::hard();
        artifacts.bundle.validate_with(limits,true)?;
        if let Some(path)=&query.path {
            artifacts.bundle.files.retain(|f|f.path==*path || f.path.strip_prefix(path).is_some_and(|p|p.starts_with('/')));
            anyhow::ensure!(!artifacts.bundle.files.is_empty(),"output path not found: {path}");
        }
        artifacts.fingerprint=Some(artifacts.bundle.id_with(limits)?);
        if query.manifest {
            Ok(ArtifactResponse::Manifest(serde_json::json!({"attempt_id":artifacts.attempt_id,"fingerprint":artifacts.fingerprint,"bytes":artifacts.bundle.validate_with(limits,true)?,"files":artifacts.bundle.files.iter().map(|f|serde_json::json!({"path":f.path,"bytes":base64::decoded_len_estimate(f.data.len()) - f.data.bytes().rev().take_while(|b|*b==b'=').count(),"executable":f.executable})).collect::<Vec<_>>()})))
        } else {Ok(ArtifactResponse::Bundle(artifacts))}
    }).await?))
}
async fn upload_artifacts(
    State(s): State<Shared>,
    axum::Extension(permit): axum::Extension<Arc<tokio::sync::OwnedSemaphorePermit>>,
    Json(request): Json<crate::workspace::ArtifactUpload>,
) -> ApiResult<()> {
    Ok(Json(
        db(s, move |s| {
            let _permit = permit;
            s.save_artifacts(request)
        })
        .await?,
    ))
}
async fn list(State(s): State<Shared>, Query(filter): Query<JobFilter>) -> ApiResult<Vec<Job>> {
    Ok(Json(db(s, move |s| s.list_filtered(&filter)).await?))
}
async fn explain(State(s): State<Shared>, Path(id): Path<String>) -> ApiResult<serde_json::Value> {
    Ok(Json(db(s, move |s| s.explain(&id)).await?))
}
async fn prune(
    State(s): State<Shared>,
    Json(request): Json<PruneRequest>,
) -> ApiResult<serde_json::Value> {
    Ok(Json(
        db(s, move |s| {
            Ok(serde_json::json!({"jobs":s.prune(&request)?,"dry_run":request.dry_run}))
        })
        .await?,
    ))
}
async fn status(State(s): State<Shared>, Path(id): Path<String>) -> ApiResult<Job> {
    Ok(Json(db(s, move |s| s.get(&id)).await?))
}
async fn cancel(State(s): State<Shared>, Path(id): Path<String>) -> ApiResult<Job> {
    Ok(Json(db(s, move |s| s.cancel(&id)).await?))
}
async fn logs(State(s): State<Shared>, Path(id): Path<String>) -> ApiResult<Logs> {
    Ok(Json(db(s, move |s| s.logs(&id)).await?))
}
async fn workers(State(s): State<Shared>) -> ApiResult<Vec<Worker>> {
    Ok(Json(db(s, |s| s.workers()).await?))
}
async fn stats(State(s): State<Shared>) -> ApiResult<serde_json::Value> {
    Ok(Json(db(s, |s| s.stats()).await?))
}
async fn register(State(s): State<Shared>, Json(r): Json<Registration>) -> ApiResult<Worker> {
    Ok(Json(db(s, move |s| s.register(r)).await?))
}
async fn heartbeat(State(s): State<Shared>, Json(a): Json<Heartbeat>) -> ApiResult<Worker> {
    Ok(Json(db(s, move |s| s.observed_heartbeat(a)).await?))
}
async fn drain(State(s): State<Shared>, Json(a): Json<WorkerAuth>) -> ApiResult<Worker> {
    Ok(Json(db(s, move |s| s.heartbeat(&a, true)).await?))
}
async fn poll(State(s): State<Shared>, Json(a): Json<WorkerAuth>) -> ApiResult<Option<Assignment>> {
    Ok(Json(db(s, move |s| s.claim(&a)).await?))
}
async fn report(State(s): State<Shared>, Json(r): Json<Report>) -> ApiResult<Job> {
    Ok(Json(db(s, move |s| s.report(r)).await?))
}
async fn attempts(State(s): State<Shared>, Path(id): Path<String>) -> ApiResult<Vec<Attempt>> {
    Ok(Json(db(s, move |s| s.attempts(&id)).await?))
}
async fn start(State(s): State<Shared>, Json(r): Json<Start>) -> ApiResult<Job> {
    Ok(Json(db(s, move |s| s.start(r)).await?))
}
async fn resolve(
    State(s): State<Shared>,
    Path(id): Path<String>,
    Json(confirmed): Json<bool>,
) -> ApiResult<Job> {
    if !confirmed {
        return Err(
            anyhow::anyhow!("explicit confirmation that execution stopped is required").into(),
        );
    }
    Ok(Json(db(s, move |s| s.resolve(&id)).await?))
}

async fn log_chunk(
    State(s): State<Shared>,
    Path(id): Path<String>,
    Query(query): Query<crate::store::LogQuery>,
) -> ApiResult<crate::store::LogChunk> {
    Ok(Json(db(s, move |s| s.log_chunk(&id, &query)).await?))
}

async fn storage(State(s): State<Shared>) -> ApiResult<serde_json::Value> {
    Ok(Json(db(s, |s| s.storage_usage()).await?))
}
