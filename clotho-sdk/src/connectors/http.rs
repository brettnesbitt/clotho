use crate::traits::{Source, Sink, Context};
use anyhow::Result;
use async_trait::async_trait;
use crate::http::{Client, Method};

#[cfg(feature = "batch")]
use polars::prelude::*;

pub struct HttpSink {
    client: Client,
    url: String,
    method: Method,
    headers: Vec<(String, String)>,
}

impl HttpSink {
    pub fn new(url: &str) -> Self {
        Self {
            client: Client::new(),
            url: url.to_string(),
            method: Method::Post,
            headers: Vec::new(),
        }
    }

    pub fn with_header(mut self, key: &str, value: &str) -> Self {
        self.headers.push((key.to_string(), value.to_string()));
        self
    }
}

// THE STREAM SINK (Webhooks, Slack Alerts)
#[async_trait]
impl Sink<serde_json::Value> for HttpSink {
    async fn write(&mut self, ctx: Context<serde_json::Value>) -> Result<()> {
        let mut req = match self.method {
            Method::Get => self.client.get(&self.url),
            Method::Post => self.client.post(&self.url),
            Method::Put => self.client.put(&self.url),
            Method::Patch => self.client.patch(&self.url),
            Method::Delete => self.client.delete(&self.url),
        };

        for (key, value) in &self.headers {
            req = req.header(key, value);
        }

        req = req.header("X-Clotho-Trace-Id", &ctx.span_id);

        let res = req
            .json(&ctx.data)?
            .send()
            .await?;

        if !res.is_success() {
            anyhow::bail!("HttpSink request failed with status {}", res.status());
        }

        Ok(())
    }
}

// THE BATCH SOURCE (API Polling for ETL)
pub struct HttpSource {
    client: Client,
    url: String,
    has_run: bool, // Simple toggle for a one-shot batch pull
}

impl HttpSource {
    pub fn new(url: &str) -> Self {
        Self { client: Client::new(), url: url.to_string(), has_run: false }
    }
}

#[cfg(feature = "batch")]
#[async_trait]
impl Source<DataFrame> for HttpSource {
    async fn next(&mut self) -> Option<Result<Context<DataFrame>>> {
        if self.has_run { return None; } // Only pull once per trigger
        self.has_run = true;

        let res = match self.client.get(&self.url).send().await {
            Ok(r) => r,
            Err(e) => return Some(Err(e.into())),
        };

        if !res.is_success() {
            return Some(Err(anyhow::anyhow!(
                "HttpSource request failed with status {}",
                res.status()
            )));
        }

        // Expecting the API to return a JSON Array: [{"id": 1}, {"id": 2}]
        let bytes = res.into_bytes();

        let cursor = std::io::Cursor::new(bytes);
        
        // Polars can read standard JSON arrays natively
        match polars::io::json::JsonReader::new(cursor).finish() {
            Ok(df) => Some(Ok(Context::root(df, "http_batch"))),
            Err(e) => Some(Err(anyhow::anyhow!("API did not return a valid JSON array: {}", e)))
        }
    }
}