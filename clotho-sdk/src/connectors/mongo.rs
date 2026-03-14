use crate::traits::{Source, Sink, LookupTarget, Context};
use anyhow::{Context as AnyhowContext, Result};
use async_trait::async_trait;
use mongodb::{options::ClientOptions, Client, Collection};
use mongodb::change_stream::event::ChangeStreamEvent;
use bson::{doc, Document};
use futures::stream::StreamExt;
use std::sync::Arc;

#[cfg(feature = "batch")]
use polars::prelude::*;

/// 1. THE ENRICHMENT LOOKUP TARGET
#[derive(Clone)]
pub struct MongoLookup {
    collection: Collection<Document>,
    lookup_field: String,
    /// The user-defined aggregation pipeline to run AFTER the initial key match
    base_pipeline: Vec<Document>,
}

impl MongoLookup {
    pub async fn new(uri: &str, db: &str, coll: &str, lookup_field: &str) -> Result<Self> {
        let mut client_options = ClientOptions::parse(uri).await?;
        client_options.app_name = Some("clotho-pipeline".to_string());
        
        let client = Client::with_options(client_options)?;
        let collection = client.database(db).collection::<Document>(coll);

        Ok(Self {
            collection,
            lookup_field: lookup_field.to_string(),
            base_pipeline: vec![],
        })
    }

    /// Attach a full MongoDB Aggregation Pipeline to the lookup.
    pub fn with_pipeline(mut self, pipeline: Vec<Document>) -> Self {
        self.base_pipeline = pipeline;
        self
    }
}

#[cfg(feature = "batch")]
#[async_trait]
impl LookupTarget for MongoLookup {
    async fn lookup_batch(&self, keys: Vec<&str>) -> Result<DataFrame> {
        if keys.is_empty() {
            return Ok(DataFrame::default());
        }

        // 1. Construct the Dynamic Aggregation Pipeline
        // Prepend the $match stage to filter the cluster down to the incoming batch keys
        let mut active_pipeline = vec![
            doc! {
                "$match": {
                    &self.lookup_field: { "$in": keys }
                }
            }
        ];
        
        // Append the user's custom aggregation stages ($unwind, $project, etc.)
        active_pipeline.extend(self.base_pipeline.clone());

        // 2. Execute the Aggregation
        let mut cursor = self.collection.aggregate(active_pipeline, None).await?;
        
        let mut buffer = Vec::with_capacity(keys.len() * 256);

        // 3. High-Speed Conversion: BSON -> JSON -> NDJSON Buffer
        while let Some(result) = cursor.next().await {
            let doc = result.context("Failed to read Mongo document")?;
            
            // Convert BSON Document to serde_json::Value to utilize Polars JSON parser
            let json: serde_json::Value = doc.into();
            serde_json::to_writer(&mut buffer, &json)?;
            buffer.push(b'\n'); // Newline delimiter
        }

        if buffer.is_empty() {
            return Ok(DataFrame::default());
        }

        let io_cursor = std::io::Cursor::new(buffer);
        let df = polars::io::ndjson::JsonLineReader::new(io_cursor)
            .infer_schema_len(Some(100))
            .finish()
            .context("Failed to parse Mongo results into Polars DataFrame")?;

        Ok(df)
    }
}

/// 2. THE MONGO SOURCE (Aggregation & CDC)
pub struct MongoSource {
    collection: Collection<Document>,
    pipeline: Vec<Document>,
    cdc_stream: Option<mongodb::change_stream::ChangeStream<ChangeStreamEvent<Document>>>,
}

impl MongoSource {
    pub async fn new(uri: &str, db: &str, coll: &str) -> Result<Self> {
        let client = Client::with_options(ClientOptions::parse(uri).await?)?;
        let collection = client.database(db).collection::<Document>(coll);
        Ok(Self { 
            collection, 
            pipeline: vec![],
            cdc_stream: None,
        })
    }

    /// Supply an aggregation pipeline to run on the collection (for Batch mode)
    pub fn aggregate(mut self, pipeline: Vec<Document>) -> Self {
        self.pipeline = pipeline;
        self
    }

    /// Convert this source into a Real-Time Change Data Capture stream (for Stream mode)
    pub async fn watch(mut self) -> Result<Self> {
        // You can pass self.pipeline here if you want to filter the CDC events natively!
        let stream = self.collection.watch(self.pipeline.clone(), None).await?;
        self.cdc_stream = Some(stream);
        Ok(self)
    }
}

// STREAM ENGINE: Yields CDC Events one by one
#[async_trait]
impl Source<serde_json::Value> for MongoSource {
    async fn next(&mut self) -> Option<Result<Context<serde_json::Value>>> {
        if let Some(stream) = &mut self.cdc_stream {
            match stream.next().await {
                Some(Ok(event)) => {
                    let json_event: serde_json::Value = bson::to_document(&event).unwrap_or_default().into();
                    let trace_id = uuid::Uuid::new_v4().to_string();
                    Some(Ok(Context::root(json_event, trace_id)))
                }
                Some(Err(e)) => Some(Err(anyhow::anyhow!("Mongo CDC Error: {}", e))),
                None => None,
            }
        } else {
            Some(Err(anyhow::anyhow!("MongoSource must call .watch() to be used as a Stream Source")))
        }
    }
}

// BATCH ENGINE: Execute the Aggregation Pipeline and return a DataFrame
#[cfg(feature = "batch")]
#[async_trait]
impl Source<DataFrame> for MongoSource {
    async fn next(&mut self) -> Option<Result<Context<DataFrame>>> {
        let mut cursor = match self.collection.aggregate(self.pipeline.clone(), None).await {
            Ok(c) => c,
            Err(e) => return Some(Err(anyhow::anyhow!("Mongo Aggregate Error: {}", e))),
        };

        let mut buffer = Vec::new();
        let mut count = 0;

        while let Some(Ok(doc)) = cursor.next().await {
            let json: serde_json::Value = doc.into();
            let _ = serde_json::to_writer(&mut buffer, &json);
            buffer.push(b'\n');
            count += 1;
            
            if count >= 10_000 { break; } // Safe chunking
        }

        if count == 0 { return None; } 

        let io_cursor = std::io::Cursor::new(buffer);
        match polars::io::ndjson::JsonLineReader::new(io_cursor).finish() {
            Ok(df) => Some(Ok(Context::root(df, "mongo_batch"))),
            Err(e) => Some(Err(anyhow::anyhow!("Polars parsing error: {}", e))),
        }
    }
}

/// 3. THE MONGO SINK (Bulk Inserts)
pub struct MongoSink {
    collection: Collection<Document>,
}

impl MongoSink {
    pub async fn new(uri: &str, db: &str, coll: &str) -> Result<Self> {
        let client = Client::with_options(ClientOptions::parse(uri).await?)?;
        let collection = client.database(db).collection::<Document>(coll);
        Ok(Self { collection })
    }
}

// STREAM ENGINE: Insert documents one by one
#[async_trait]
impl Sink<serde_json::Value> for MongoSink {
    async fn write(&mut self, ctx: Context<serde_json::Value>) -> Result<()> {
        let doc = bson::to_document(&ctx.data)?;
        self.collection.insert_one(doc, None).await?;
        Ok(())
    }
}

// BATCH ENGINE: High-speed bulk inserts
#[cfg(feature = "batch")]
#[async_trait]
impl Sink<DataFrame> for MongoSink {
    async fn write(&mut self, mut ctx: Context<DataFrame>) -> Result<()> {
        if ctx.data.height() == 0 { return Ok(()); }

        let mut buffer = Vec::with_capacity(ctx.data.height() * 256);
        polars::io::ndjson::JsonWriter::new(&mut buffer)
            .finish(&mut ctx.data)?;

        let mut docs_to_insert = Vec::with_capacity(ctx.data.height());
        for line in buffer.split(|&b| b == b'\n') {
            if line.is_empty() { continue; }
            
            let json_val: serde_json::Value = serde_json::from_slice(line)?;
            if let Ok(doc) = bson::to_document(&json_val) {
                docs_to_insert.push(doc);
            }
        }

        if !docs_to_insert.is_empty() {
            self.collection.insert_many(docs_to_insert, None).await?;
        }

        Ok(())
    }
}