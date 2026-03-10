use crate::traits::{Sink, Source, Context};
use anyhow::Result;
use reqwest::{Client, Method, header};
use std::time::Duration;
use serde::Serialize;

/// HTTP Sink: Sends data to a URL (Webhook)
pub struct HttpSink {
    url: String,
    method: Method,
    client: Client,
    headers: header::HeaderMap,
}

impl HttpSink {
    pub fn new(url_env: &str) -> Self {
        let url = std::env::var(url_env).unwrap_or_else(|_| url_env.to_string());
        
        let mut headers = header::HeaderMap::new();
        headers.insert(header::CONTENT_TYPE, header::HeaderValue::from_static("application/json"));
        
        // Optional: Auth Token
        if let Ok(token) = std::env::var("HTTP_AUTH_TOKEN") {
             let mut auth_val = header::HeaderValue::from_str(&format!("Bearer {}", token)).unwrap();
             auth_val.set_sensitive(true);
             headers.insert(header::AUTHORIZATION, auth_val);
        }

        Self {
            url,
            method: Method::POST, // Default to POST
            client: Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .expect("Failed to build HTTP client"),
            headers,
        }
    }

    pub fn method(mut self, method: Method) -> Self {
        self.method = method;
        self
    }
}

#[async_trait::async_trait]
impl<T> Sink<T> for HttpSink 
where T: Serialize + Send + Sync 
{
    async fn write(&mut self, ctx: Context<T>) -> Result<()> {
        let payload = serde_json::to_value(&ctx.data)?;

        // Simple Retry Logic (Exponential Backoff could go here)
        let mut attempts = 0;
        loop {
            attempts += 1;
            let resp = self.client.request(self.method.clone(), &self.url)
                .headers(self.headers.clone())
                .json(&payload)
                .send()
                .await;

            match resp {
                Ok(r) if r.status().is_success() => break, // Success
                Ok(r) => {
                    eprintln!("HTTP Sink Error: Status {}", r.status());
                    if attempts >= 3 { return Err(anyhow::anyhow!("HTTP {} Failed", r.status())); }
                },
                Err(e) => {
                    eprintln!("HTTP Sink Network Error: {}", e);
                    if attempts >= 3 { return Err(e.into()); }
                }
            }
            // Wait before retry
            tokio::time::sleep(Duration::from_millis(500 * attempts)).await;
        }
        
        Ok(())
    }
}

/// HTTP Source: Polls an API endpoint
pub struct HttpSource<T> {
    url: String,
    client: Client,
    poll_interval: Duration,
    last_seen_id: Option<String>, // Simple Deduplication
    _marker: std::marker::PhantomData<T>,
}

impl<T> HttpSource<T> {
    pub fn new(url: &str, interval_secs: u64) -> Self {
        Self {
            url: url.to_string(),
            client: Client::new(),
            poll_interval: Duration::from_secs(interval_secs),
            last_seen_id: None,
            _marker: std::marker::PhantomData,
        }
    }
}

// We implement Source specifically for JSON arrays or objects
#[async_trait::async_trait]
impl<T> Source<T> for HttpSource<T> 
where T: serde::de::DeserializeOwned + Send + Sync + Clone 
{
    async fn next(&mut self) -> Option<Result<Context<T>>> {
        loop {
            // 1. Wait for the interval
            tokio::time::sleep(self.poll_interval).await;

            // 2. Fetch Data
            let resp = match self.client.get(&self.url).send().await {
                Ok(r) => r,
                Err(e) => {
                    eprintln!("HTTP Poll Failed: {}", e);
                    continue; // Retry next loop
                }
            };

            // 3. Parse JSON
            // We assume the API returns a list of items `[{}, {}]` 
            // OR a single item `{}`. For this example, let's say it returns a single T.
            match resp.json::<T>().await {
                Ok(data) => {
                    // TODO: Implement proper dedup logic here (compare hash or ID)
                    
                    return Some(Ok(Context {
                        id: uuid::Uuid::new_v4().to_string(),
                        trace_id: uuid::Uuid::new_v4().to_string(),
                        data,
                        metadata: std::collections::HashMap::new(),
                    }));
                }
                Err(e) => {
                    eprintln!("HTTP Parse Error: {}", e);
                    continue;
                }
            }
        }
    }
}