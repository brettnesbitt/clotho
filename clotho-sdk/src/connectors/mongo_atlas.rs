use crate::traits::{Source, Sink, LookupTarget, Context};
use anyhow::{Context as AnyhowContext, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::json;

#[cfg(feature = "batch")]
use polars::prelude::*;

/// Configuration for the MongoDB Atlas Data API
#[derive(Clone)]
pub struct AtlasConfig {
    pub endpoint: String, // e.g., "https://data.mongodb-api.com/app/<App-ID>/endpoint/data/v1"
    pub api_key: String,
    pub cluster: String,  // "Cluster0"
    pub database: String,
    pub collection: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct AtlasRequest<'a> {
    collection: &'a str,
    database: &'a str,
    data_source: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pipeline: Option<Vec<serde_json::Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    documents: Option<Vec<serde_json::Value>>,
}

#[derive(Deserialize)]
struct AtlasResponse {
    documents: Option<Vec<serde_json::Value>>,
    inserted_ids: Option<Vec<String>>,
}

/// 1. THE ENRICHMENT LOOKUP TARGET (Atlas Aggregation Pipeline)
#[derive(Clone)]
pub struct MongoAtlasLookup {
    config: AtlasConfig,
    lookup_field: String,
    /// The user-defined aggregation pipeline to run AFTER the initial key match
    base_pipeline: Vec<serde_json::Value>,
    client: reqwest::Client,
}

impl MongoAtlasLookup {
    pub fn new(config: AtlasConfig, lookup_field: &str) -> Self {
        Self {
            config,
            lookup_field: lookup_field.to_string(),
            base_pipeline: vec![],
            client: reqwest::Client::new(),
        }
    }

    /// Attach a full MongoDB Aggregation Pipeline to the lookup.
    /// This allows complex $unwind, $lookup, and $project operations 
    /// before the data is returned to Polars.
    pub fn with_pipeline(mut self, pipeline: Vec<serde_json::Value>) -> Self {
        self.base_pipeline = pipeline;
        self
    }
}

#[cfg(feature = "batch")]
#[async_trait]
impl LookupTarget for MongoAtlasLookup {
    async fn lookup_batch(&self, keys: Vec<&str>) -> Result<DataFrame> {
        if keys.is_empty() {
            return Ok(DataFrame::default());
        }

        // 1. Construct the Dynamic Aggregation Pipeline
        // We MUST prepend a $match stage to filter the cluster down to ONLY 
        // the keys present in this specific Kafka batch.
        let mut active_pipeline = vec![
            json!({
                "$match": {
                    &self.lookup_field: { "$in": keys }
                }
            })
        ];
        
        // Append the user's custom aggregation stages
        active_pipeline.extend(self.base_pipeline.clone());

        let payload = AtlasRequest {
            collection: &self.config.collection,
            database: &self.config.database,
            data_source: &self.config.cluster,
            pipeline: Some(active_pipeline),
            documents: None,
        };

        // 2. Execute the HTTP Request
        let url = format!("{}/action/aggregate", self.config.endpoint);
        let res = self.client.post(&url)
            .header("api-key", &self.config.api_key)
            .header("Content-Type", "application/json")
            .json(&payload)
            .send()
            .await?
            .error_for_status()?; // Fails immediately if API Key/Cluster is invalid

        let atlas_res: AtlasResponse = res.json().await?;
        let documents = atlas_res.documents.unwrap_or_default();

        if documents.is_empty() {
            return Ok(DataFrame::default());
        }

        // 3. High-Speed Conversion to Polars
        let mut buffer = Vec::with_capacity(documents.len() * 256);
        for doc in documents {
            serde_json::to_writer(&mut buffer, &doc)?;
            buffer.push(b'\n');
        }

        let io_cursor = std::io::Cursor::new(buffer);
        let df = polars::io::ndjson::JsonLineReader::new(io_cursor)
            .infer_schema_len(Some(100))
            .finish()
            .context("Failed to parse Atlas results into Polars DataFrame")?;

        Ok(df)
    }
}

/// 2. THE MONGO SOURCE (Batch Aggregation Fetch)
pub struct MongoAtlasSource {
    config: AtlasConfig,
    pipeline: Vec<serde_json::Value>,
    client: reqwest::Client,
}

impl MongoAtlasSource {
    pub fn new(config: AtlasConfig) -> Self {
        Self {
            config,
            pipeline: vec![],
            client: reqwest::Client::new(),
        }
    }

    pub fn aggregate(mut self, pipeline: Vec<serde_json::Value>) -> Self {
        self.pipeline = pipeline;
        self
    }
}

#[cfg(feature = "batch")]
#[async_trait]
impl Source<DataFrame> for MongoAtlasSource {
    async fn next(&mut self) -> Option<Result<Context<DataFrame>>> {
        let payload = AtlasRequest {
            collection: &self.config.collection,
            database: &self.config.database,
            data_source: &self.config.cluster,
            pipeline: Some(self.pipeline.clone()), // Use the user's full pipeline
            documents: None,
        };

        let url = format!("{}/action/aggregate", self.config.endpoint);
        
        // Execute request
        let res_result = self.client.post(&url)
            .header("api-key", &self.config.api_key)
            .json(&payload)
            .send()
            .await;

        let res = match res_result {
            Ok(r) => r,
            Err(e) => return Some(Err(e.into())),
        };

        let atlas_res: AtlasResponse = match res.json().await {
            Ok(r) => r,
            Err(e) => return Some(Err(e.into())),
        };

        let documents = atlas_res.documents.unwrap_or_default();
        if documents.is_empty() {
            return None; // Source is empty
        }

        let mut buffer = Vec::new();
        for doc in documents {
            let _ = serde_json::to_writer(&mut buffer, &doc);
            buffer.push(b'\n');
        }

        let io_cursor = std::io::Cursor::new(buffer);
        match polars::io::ndjson::JsonLineReader::new(io_cursor).finish() {
            Ok(df) => Some(Ok(Context::root(df, "atlas_aggregate_batch"))),
            Err(e) => Some(Err(anyhow::anyhow!("Polars parsing error: {}", e))),
        }
    }
}

/// 3. THE MONGO SINK (Bulk Insert)
pub struct MongoAtlasSink {
    config: AtlasConfig,
    client: reqwest::Client,
}

impl MongoAtlasSink {
    pub fn new(config: AtlasConfig) -> Self {
        Self {
            config,
            client: reqwest::Client::new(),
        }
    }
}

#[cfg(feature = "batch")]
#[async_trait]
impl Sink<DataFrame> for MongoAtlasSink {
    async fn write(&mut self, mut ctx: Context<DataFrame>) -> Result<()> {
        if ctx.data.height() == 0 { return Ok(()); }

        let mut buffer = Vec::with_capacity(ctx.data.height() * 256);
        polars::io::ndjson::JsonWriter::new(&mut buffer)
            .finish(&mut ctx.data)?;

        let mut docs_to_insert = Vec::with_capacity(ctx.data.height());
        for line in buffer.split(|&b| b == b'\n') {
            if line.is_empty() { continue; }
            if let Ok(json_val) = serde_json::from_slice::<serde_json::Value>(line) {
                docs_to_insert.push(json_val);
            }
        }

        let payload = AtlasRequest {
            collection: &self.config.collection,
            database: &self.config.database,
            data_source: &self.config.cluster,
            pipeline: None,
            documents: Some(docs_to_insert),
        };

        let url = format!("{}/action/insertMany", self.config.endpoint);
        self.client.post(&url)
            .header("api-key", &self.config.api_key)
            .json(&payload)
            .send()
            .await?
            .error_for_status()?; // Throws if the bulk insert fails

        Ok(())
    }
}