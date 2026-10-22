use anyhow::{Result, ensure};
use serde::{Serialize, de::DeserializeOwned};
use std::time::Duration;

pub const TRANSFER_TIMEOUT: Duration = Duration::from_secs(120);

#[derive(Clone)]
pub struct Client {
    http: reqwest::Client,
    url: String,
    token: String,
    pub limits: crate::config::TransferLimits,
    artifact_slots: std::sync::Arc<tokio::sync::Semaphore>,
}
#[derive(Debug)]
pub struct HttpError {
    pub status: reqwest::StatusCode,
    body: String,
}
impl std::fmt::Display for HttpError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "API {}: {}", self.status, self.body)
    }
}
impl std::error::Error for HttpError {}
impl Client {
    pub fn new(url: String, token: String) -> Result<Self> {
        ensure!(
            token.len() >= 32,
            "RUNSPK_TOKEN must contain at least 32 characters"
        );
        let parsed = reqwest::Url::parse(&url)?;
        ensure!(
            parsed.scheme() == "https"
                || (parsed.scheme() == "http"
                    && matches!(parsed.host_str(), Some("127.0.0.1" | "localhost" | "[::1]"))),
            "remote HTTP is disabled; use HTTPS or an SSH loopback tunnel"
        );
        ensure!(
            parsed.username().is_empty()
                && parsed.password().is_none()
                && parsed.query().is_none()
                && parsed.fragment().is_none(),
            "scheduler URL must not contain credentials, query parameters or a fragment; use the private bearer token profile"
        );
        let http = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(1))
            .redirect(reqwest::redirect::Policy::none())
            .build()?;
        Ok(Self {
            http,
            url: url.trim_end_matches('/').into(),
            token,
            limits: crate::config::TransferLimits::default(),
            artifact_slots: std::sync::Arc::new(tokio::sync::Semaphore::new(1)),
        })
    }
    pub(crate) async fn artifact_permit(&self) -> Result<tokio::sync::OwnedSemaphorePermit> {
        Ok(self.artifact_slots.clone().acquire_owned().await?)
    }
    pub fn with_limits(mut self, limits: crate::config::TransferLimits) -> Result<Self> {
        limits.validate()?;
        self.limits = limits;
        Ok(self)
    }
    pub fn transfer_timeout(&self) -> Duration {
        self.limits.timeout()
    }
    pub async fn info(&self) -> Result<ServerInfo> {
        self.get("/info").await
    }
    pub async fn negotiated_limits(&self) -> Result<crate::config::TransferLimits> {
        let remote = self.info().await?;
        remote.transfer_limits.validate()?;
        Ok(self.limits.intersect(remote.transfer_limits))
    }
    async fn decode<T: DeserializeOwned>(
        &self,
        mut response: reqwest::Response,
        limit: usize,
    ) -> Result<T> {
        let status = response.status();
        let limit = if status.is_success() { limit } else { 16384 };
        ensure!(
            response.content_length().is_none_or(|n| n <= limit as u64),
            "API response exceeds {} bytes; page inspection with --limit or raise transfer limits for bundle downloads",
            limit
        );
        let mut body = Vec::new();
        while let Some(chunk) = response.chunk().await? {
            ensure!(
                chunk.len() <= limit.saturating_sub(body.len()),
                "API response exceeds {} bytes; page inspection with --limit or raise transfer limits for bundle downloads",
                limit
            );
            body.extend_from_slice(&chunk);
        }
        if !status.is_success() {
            return Err(HttpError {
                status,
                body: String::from_utf8_lossy(&body).replace(&self.token, "[redacted]"),
            }
            .into());
        }
        Ok(serde_json::from_slice(&body)?)
    }
    pub async fn get<T: DeserializeOwned>(&self, path: &str) -> Result<T> {
        self.get_query(path, &()).await
    }
    pub async fn get_query<T: DeserializeOwned>(
        &self,
        path: &str,
        query: &impl Serialize,
    ) -> Result<T> {
        self.decode(
            self.http
                .get(format!("{}{path}", self.url))
                .query(query)
                .bearer_auth(&self.token)
                .send()
                .await?,
            16 * 1024 * 1024,
        )
        .await
    }
    pub async fn post<T: DeserializeOwned>(&self, path: &str, body: &impl Serialize) -> Result<T> {
        self.post_with_timeout(path, body, std::time::Duration::from_secs(1))
            .await
    }
    pub async fn post_with_timeout<T: DeserializeOwned>(
        &self,
        path: &str,
        body: &impl Serialize,
        timeout: std::time::Duration,
    ) -> Result<T> {
        struct BoundedBody {
            data: Vec<u8>,
            limit: usize,
        }
        impl std::io::Write for BoundedBody {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                if bytes.len() > self.limit.saturating_sub(self.data.len()) {
                    return Err(std::io::Error::other(
                        "encoded request exceeds HTTP body limit; reduce snapshot or raise --http-body-limit",
                    ));
                }
                self.data.extend_from_slice(bytes);
                Ok(bytes.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let transfer = matches!(path, "/jobs/project" | "/worker/artifacts");
        let limit = if transfer {
            self.limits.http_body_bytes.0
        } else {
            16 * 1024 * 1024
        };
        let mut encoded = BoundedBody {
            data: Vec::new(),
            limit,
        };
        serde_json::to_writer(&mut encoded, body)?;
        self.decode(
            self.http
                .post(format!("{}{path}", self.url))
                .timeout(timeout)
                .bearer_auth(&self.token)
                .header(reqwest::header::CONTENT_TYPE, "application/json")
                .body(encoded.data)
                .send()
                .await?,
            16 * 1024 * 1024,
        )
        .await
    }
    pub async fn get_with_timeout<T: DeserializeOwned>(
        &self,
        path: &str,
        timeout: std::time::Duration,
    ) -> Result<T> {
        self.decode(
            self.http
                .get(format!("{}{path}", self.url))
                .bearer_auth(&self.token)
                .timeout(timeout)
                .send()
                .await?,
            self.limits.http_body_bytes.0,
        )
        .await
    }
}

#[derive(Clone, Debug, serde::Deserialize)]
pub struct ServerInfo {
    #[serde(default)]
    pub features: Vec<String>,
    #[serde(default)]
    pub transfer_limits: crate::config::TransferLimits,
}
impl ServerInfo {
    pub fn supports(&self, feature: &str) -> bool {
        self.features.iter().any(|f| f == feature)
    }
}
