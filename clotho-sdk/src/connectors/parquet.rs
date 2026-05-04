use crate::traits::{Sink, Context};
use anyhow::{Result, Context as AnyhowContext};
use async_trait::async_trait;
use serde_json::Value;

// We use the JSON reader to convert Vec<Value> into an Arrow RecordBatch
use arrow::json::ReaderBuilder;
use arrow::datatypes::Schema;
use parquet::arrow::ArrowWriter;
use std::sync::Arc;

pub struct ParquetSink {
    bucket: String,
    object_prefix: String,
    schema: Arc<Schema>,
    http_client: crate::http::Client,
    buffer: Vec<Value>,
    batch_size: usize,
}

impl ParquetSink {
    pub fn new(bucket: &str, object_prefix: &str, schema: Arc<Schema>) -> Self {
        Self {
            bucket: bucket.to_string(),
            object_prefix: object_prefix.to_string(),
            schema,
            http_client: crate::http::Client::new(),
            buffer: Vec::new(),
            batch_size: 100, // Default batch size before flush
        }
    }

    /// Fetch an OAuth2 token from the GCP Metadata Server (Workload Identity).
    async fn get_gcs_token(&self) -> Result<String> {
        // If we are testing locally without metadata server, allow an override
        if let Some(token) = crate::config::var("GCS_OAUTH_TOKEN").ok() {
            return Ok(token);
        }

        let metadata_url = "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token";
        
        let res = self.http_client
            .get(metadata_url)
            .header("Metadata-Flavor", "Google")
            .send()
            .await
            .context("Failed to request GCS token from metadata server")?;

        if !res.is_success() {
            anyhow::bail!("Failed to get GCS token, status: {}", res.status());
        }

        let body = res.text().unwrap_or_default();
        let json: Value = serde_json::from_str(&body)
            .context("Failed to parse token response")?;
            
        json.get("access_token")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .context("access_token missing from metadata response")
    }

    /// Upload a payload to GCS using the REST API.
    async fn upload_to_gcs(&self, object_name: &str, payload: Vec<u8>) -> Result<()> {
        let token = self.get_gcs_token().await?;
        
        // GCS simple upload URI
        let url = format!(
            "https://storage.googleapis.com/upload/storage/v1/b/{}/o?uploadType=media&name={}",
            self.bucket, object_name
        );

        let res = self.http_client
            .post(&url)
            .header("Authorization", &format!("Bearer {}", token))
            .header("Content-Type", "application/octet-stream")
            // Send the raw parquet bytes
            .body(payload)
            .send()
            .await
            .context("Failed to send Parquet payload to GCS")?;

        if !res.is_success() {
            let status = res.status();
            let body = res.text().unwrap_or_default();
            anyhow::bail!("GCS upload failed ({}): {}", status, body);
        }

        eprintln!("[ParquetSink] Uploaded gs://{}/{}", self.bucket, object_name);
        Ok(())
    }
}

// Implement for batch updates (Pipeline::once)
#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl Sink<Vec<Value>> for ParquetSink {
    async fn write(&mut self, ctx: Context<Vec<Value>>) -> Result<()> {
        if ctx.data.is_empty() {
            return Ok(());
        }

        // 1. Serialize Vec<Value> into newline-delimited JSON for the Arrow JSON reader
        let mut ndjson = Vec::new();
        for val in &ctx.data {
            serde_json::to_writer(&mut ndjson, val)?;
            ndjson.push(b'\n');
        }

        // 2. Build Arrow RecordBatch from JSON
        let cursor = std::io::Cursor::new(ndjson);
        let mut reader = ReaderBuilder::new(self.schema.clone())
            .build(cursor)
            .context("Failed to build Arrow JSON reader")?;

        // Expecting one batch since data fits in memory
        let batch = match reader.next() {
            Some(Ok(b)) => b,
            Some(Err(e)) => anyhow::bail!("Arrow read error: {}", e),
            None => return Ok(()),
        };

        // 3. Write Arrow RecordBatch to Parquet in-memory buffer
        let mut parquet_buffer = Vec::new();
        {
            let props = parquet::file::properties::WriterProperties::builder()
                .set_compression(parquet::basic::Compression::SNAPPY)
                .build();
                
            let mut writer = ArrowWriter::try_new(&mut parquet_buffer, self.schema.clone(), Some(props))
                .context("Failed to create Parquet ArrowWriter")?;
                
            writer.write(&batch).context("Failed to write RecordBatch to Parquet")?;
            writer.close().context("Failed to close Parquet writer")?;
        }

        // 4. Generate unique object name using current time / UUID
        // Use Utc::now() from chrono
        let timestamp = chrono::Utc::now().format("%Y%m%d_%H%M%S");
        let unique_id = uuid::Uuid::new_v4().to_string();
        let object_name = format!("{}/{}_{}.parquet", self.object_prefix, timestamp, &unique_id[..8]);

        // 5. Upload to GCS
        self.upload_to_gcs(&object_name, parquet_buffer).await?;

        Ok(())
    }
}

// Implement for streaming updates
#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl Sink<Value> for ParquetSink {
    async fn write(&mut self, ctx: Context<Value>) -> Result<()> {
        self.buffer.push(ctx.data);
        
        if self.buffer.len() >= self.batch_size {
            // Re-use the batch write logic by taking the buffer
            let batch = std::mem::take(&mut self.buffer);
            self.write(Context::root(batch, "flush")).await?;
        }
        
        Ok(())
    }
}
