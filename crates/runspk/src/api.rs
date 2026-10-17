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
}
async fn authorize(
    State(state): State<AuthState>,
    request: Request<axum::body::Body>,
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
    let _permit = if transfer {
        Some(
            state
                .transfers
                .acquire()
                .await
                .expect("transfer semaphore remains open"),
        )
    } else {
        None
    };
    let started = std::time::Instant::now();
    let response = next.run(request).await;
    tracing::debug!(%method,%path,status=response.status().as_u16(),elapsed_us=started.elapsed().as_micros(),"request completed");
    response
}
pub fn router(store: Shared, token: String) -> anyhow::Result<Router> {
    anyhow::ensure!(
        token.len() >= 32,
        "RUNSPK_TOKEN must contain at least 32 characters"
    );
    Ok(Router::new()
        .route("/jobs", get(list).post(submit))
        .route(
            "/jobs/project",
            post(submit_project)
                .layer(DefaultBodyLimit::max(crate::workspace::MAX_HTTP_BODY_BYTES)),
        )
        .route("/workspaces/{id}", get(workspace))
        .route("/jobs/{id}/artifacts", get(artifacts))
        .route(
            "/worker/artifacts",
            post(upload_artifacts)
                .layer(DefaultBodyLimit::max(crate::workspace::MAX_HTTP_BODY_BYTES)),
        )
        .route("/info", get(info))
        .route("/jobs/{id}", get(status))
        .route("/jobs/{id}/cancel", post(cancel))
        .route("/jobs/{id}/logs", get(logs))
        .route("/jobs/{id}/attempts", get(attempts))
        .route("/jobs/{id}/resolve", post(resolve))
        .route("/jobs/{id}/explain", get(explain))
        .route("/history/prune", post(prune))
        .route("/workers", get(workers))
        .route("/stats", get(stats))
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
            },
            authorize,
        )))
}
async fn submit(State(s): State<Shared>, Json(spec): Json<JobSpec>) -> ApiResult<Job> {
    Ok(Json(db(s, move |s| s.submit(spec)).await?))
}
async fn info() -> Json<serde_json::Value> {
    Json(
        serde_json::json!({"version":env!("CARGO_PKG_VERSION"),"schema_version":crate::store::SCHEMA_VERSION,"features":["project-snapshots-v1","artifacts-v1"],"snapshot_max_bytes":crate::workspace::MAX_BYTES}),
    )
}
async fn submit_project(
    State(s): State<Shared>,
    Json(request): Json<crate::workspace::ProjectSubmission>,
) -> ApiResult<Job> {
    Ok(Json(db(s, move |s| s.submit_project(request)).await?))
}
async fn workspace(
    State(s): State<Shared>,
    Path(id): Path<String>,
) -> ApiResult<crate::workspace::Bundle> {
    Ok(Json(db(s, move |s| s.workspace(&id)).await?))
}
#[derive(serde::Deserialize)]
struct ArtifactQuery {
    attempt: Option<String>,
}
async fn artifacts(
    State(s): State<Shared>,
    Path(id): Path<String>,
    Query(query): Query<ArtifactQuery>,
) -> ApiResult<crate::workspace::Artifacts> {
    Ok(Json(
        db(s, move |s| s.artifacts(&id, query.attempt.as_deref())).await?,
    ))
}
async fn upload_artifacts(
    State(s): State<Shared>,
    Json(request): Json<crate::workspace::ArtifactUpload>,
) -> ApiResult<()> {
    Ok(Json(db(s, move |s| s.save_artifacts(request)).await?))
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
async fn heartbeat(State(s): State<Shared>, Json(a): Json<WorkerAuth>) -> ApiResult<Worker> {
    Ok(Json(db(s, move |s| s.heartbeat(&a, false)).await?))
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
