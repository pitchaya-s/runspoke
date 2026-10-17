use anyhow::{Result, ensure};
use serde::{Serialize, de::DeserializeOwned};
use std::time::Duration;

pub const TRANSFER_TIMEOUT: Duration = Duration::from_secs(120);

#[derive(Clone)]
pub struct Client {
    http: reqwest::Client,
    url: String,
    token: String,
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
        let http = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(1))
            .redirect(reqwest::redirect::Policy::none())
            .build()?;
        Ok(Self {
            http,
            url: url.trim_end_matches('/').into(),
            token,
        })
    }
    async fn decode<T: DeserializeOwned>(response: reqwest::Response) -> Result<T> {
        let status = response.status();
        if !status.is_success() {
            return Err(HttpError {
                status,
                body: response.text().await?,
            }
            .into());
        }
        Ok(response.json().await?)
    }
    pub async fn get<T: DeserializeOwned>(&self, path: &str) -> Result<T> {
        self.get_query(path, &()).await
    }
    pub async fn get_query<T: DeserializeOwned>(
        &self,
        path: &str,
        query: &impl Serialize,
    ) -> Result<T> {
        Self::decode(
            self.http
                .get(format!("{}{path}", self.url))
                .query(query)
                .bearer_auth(&self.token)
                .send()
                .await?,
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
        Self::decode(
            self.http
                .post(format!("{}{path}", self.url))
                .timeout(timeout)
                .bearer_auth(&self.token)
                .json(body)
                .send()
                .await?,
        )
        .await
    }
    pub async fn get_with_timeout<T: DeserializeOwned>(
        &self,
        path: &str,
        timeout: std::time::Duration,
    ) -> Result<T> {
        Self::decode(
            self.http
                .get(format!("{}{path}", self.url))
                .bearer_auth(&self.token)
                .timeout(timeout)
                .send()
                .await?,
        )
        .await
    }
}
