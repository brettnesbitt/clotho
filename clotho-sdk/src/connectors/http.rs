use crate::traits::{Source, Sink, Context};
use anyhow::Result;
use async_trait::async_trait;
use reqwest::{Client, Method, header::HeaderMap};

#[cfg(feature = "batch")]
use polars::prelude::*;

pub struct HttpSink {
    client: Client,
    url: String,
    method: Method,
    headers: HeaderMap,
}

impl HttpSink {
    pub fn new(url: &str) -> Self {
        Self {
            client: Client::new(),
            url: url.to_string(),
            method: Method::POST,
            headers: HeaderMap::new(),
        }
    }

    pub fn with_header(mut self, key: &str, value: &str) -> Self {
        self.headers.insert(
            reqwest::header::HeaderName::from_bytes(key.as_bytes()).unwrap(),
            reqwest::header::HeaderValue::from_str(value).unwrap()
        );
        self
    }
}

// THE STREAM SINK (Webhooks, Slack Alerts)
#[async_trait]
impl Sink<serde_json::Value> for HttpSink {
    async fn write(&mut self, ctx: Context<serde_json::Value>) -> Result<()> {
        let mut req_headers = self.headers.clone();
        
        // DISTRIBUTED TRACING: Pass the Trace ID to the next microservice!
        req_headers.insert(
            "X-Clotho-Trace-Id", 
            reqwest::header::HeaderValue::from_str(&ctx.span_id)?
        );

        self.client.request(self.method.clone(), &self.url)
            .headers(req_headers)
            .json(&ctx.data)
            .send()
            .await?
            .error_for_status()?;

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

        // Expecting the API to return a JSON Array: [{"id": 1}, {"id": 2}]
        let bytes = match res.bytes().await {
            Ok(b) => b.to_vec(),
            Err(e) => return Some(Err(e.into())),
        };

        let cursor = std::io::Cursor::new(bytes);
        
        // Polars can read standard JSON arrays natively
        match polars::io::json::JsonReader::new(cursor).finish() {
            Ok(df) => Some(Ok(Context::root(df, "http_batch"))),
            Err(e) => Some(Err(anyhow::anyhow!("API did not return a valid JSON array: {}", e)))
        }
    }
}