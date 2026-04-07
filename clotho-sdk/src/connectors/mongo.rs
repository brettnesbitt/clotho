use crate::traits::{Sink, Context};
use anyhow::Result;
use async_trait::async_trait;

// ═══════════════════════════════════════════════════════════════════════════════
// TRANSPARENT DATA PLANE PROXY
//
// The developer sees ONE API: MongoSink::new(), MongoLookup::new().
// Under the hood, the SDK swaps the engine based on the build target:
//
//   Native (daemon/batch) → Direct TCP via mongodb crate (connection pool)
//   WASM   (Spin jobs)    → HTTP POST to Clotho Data Proxy (DaemonSet)
//
// This solves two problems:
//   1. mongodb crate depends on Tokio networking → can't compile to WASM
//   2. Serverless WASM workers would thrash MongoDB connections without a pooler
// ═══════════════════════════════════════════════════════════════════════════════

// ── Native-only imports ──────────────────────────────────────────────────────
#[cfg(not(target_family = "wasm"))]
use mongodb::{options::ClientOptions, Client, Collection};
#[cfg(not(target_family = "wasm"))]
use bson::Document;

#[cfg(feature = "native")]
use crate::traits::Source;
#[cfg(feature = "native")]
use mongodb::change_stream::event::ChangeStreamEvent;
#[cfg(feature = "native")]
use futures_util::StreamExt;
#[cfg(feature = "native")]
use tokio::sync::Mutex;

#[cfg(feature = "batch")]
use crate::traits::LookupTarget;
#[cfg(feature = "batch")]
use polars::prelude::*;

// ═══════════════════════════════════════════════════════════════════════════════
// 1. MONGO LOOKUP (Enrichment Target)
// ═══════════════════════════════════════════════════════════════════════════════

// ── Native MongoLookup ───────────────────────────────────────────────────────
#[cfg(not(target_family = "wasm"))]
#[derive(Clone)]
pub struct MongoLookup {
    collection: Collection<Document>,
    lookup_field: String,
    base_pipeline: Vec<Document>,
}

#[cfg(not(target_family = "wasm"))]
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

    pub fn with_pipeline(mut self, pipeline: Vec<Document>) -> Self {
        self.base_pipeline = pipeline;
        self
    }
}

// ── WASM MongoLookup (proxy) ─────────────────────────────────────────────────
#[cfg(target_family = "wasm")]
#[derive(Clone)]
pub struct MongoLookup {
    uri: String,
    db: String,
    coll: String,
    lookup_field: String,
    http_client: crate::http::Client,
}

#[cfg(target_family = "wasm")]
impl MongoLookup {
    pub async fn new(uri: &str, db: &str, coll: &str, lookup_field: &str) -> Result<Self> {
        Ok(Self {
            uri: uri.into(),
            db: db.into(),
            coll: coll.into(),
            lookup_field: lookup_field.to_string(),
            http_client: crate::http::Client::new(),
        })
    }

    pub fn with_pipeline(self, _pipeline: Vec<serde_json::Value>) -> Self {
        // Pipeline sent to proxy at query time — stored as-is for now
        self
    }
}

// ── Batch LookupTarget (native only — batch feature requires native) ─────────
#[cfg(all(feature = "batch", not(target_family = "wasm")))]
#[async_trait]
impl LookupTarget for MongoLookup {
    async fn lookup_batch(&self, keys: Vec<&str>) -> Result<DataFrame> {
        if keys.is_empty() {
            return Ok(DataFrame::default());
        }

        let mut active_pipeline = vec![
            bson::doc! {
                "$match": {
                    &self.lookup_field: { "$in": keys }
                }
            }
        ];
        active_pipeline.extend(self.base_pipeline.clone());

        let mut cursor = self.collection.aggregate(active_pipeline, None).await?;
        let mut buffer = Vec::with_capacity(keys.len() * 256);

        while let Some(result) = cursor.next().await {
            let doc = result.context("Failed to read Mongo document")?;
            let json: serde_json::Value = doc.into();
            serde_json::to_writer(&mut buffer, &json)?;
            buffer.push(b'\n');
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

// ═══════════════════════════════════════════════════════════════════════════════
// 2. MONGO SOURCE (Aggregation & CDC — native only)
// ═══════════════════════════════════════════════════════════════════════════════

#[cfg(feature = "native")]
pub struct MongoSource {
    collection: Collection<Document>,
    pipeline: Vec<Document>,
    cdc_stream: Option<std::sync::Arc<tokio::sync::Mutex<mongodb::change_stream::ChangeStream<ChangeStreamEvent<Document>>>>>,
}

#[cfg(feature = "native")]
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

    pub fn aggregate(mut self, pipeline: Vec<Document>) -> Self {
        self.pipeline = pipeline;
        self
    }

    pub async fn watch(mut self) -> Result<Self> {
        // You can pass self.pipeline here if you want to filter the CDC events natively!
        let stream = self.collection.watch(self.pipeline.clone(), None).await?;
        self.cdc_stream = Some(std::sync::Arc::new(tokio::sync::Mutex::new(stream)));
        Ok(self)
    }
}

#[cfg(feature = "native")]
#[async_trait]
impl Source<serde_json::Value> for MongoSource {
    async fn next(&mut self) -> Option<Result<Context<serde_json::Value>>> {
        if let Some(stream) = &self.cdc_stream {
            let mut stream = stream.lock().await;
            match stream.next().await {
                Some(Ok(event)) => {
                    let doc = bson::to_document(&event).unwrap_or_default();
                    let json_event: serde_json::Value = doc.into_iter()
                        .map(|(k, v)| (k, serde_json::to_value(&v).unwrap_or_default()))
                        .collect::<serde_json::Map<String, serde_json::Value>>()
                        .into();
                    let trace_id = uuid::Uuid::new_v4().to_string();
                    Some(Ok(Context::root(json_event, &trace_id)))
                }
                Some(Err(e)) => Some(Err(anyhow::anyhow!("Mongo CDC Error: {}", e))),
                None => None,
            }
        } else {
            Some(Err(anyhow::anyhow!("MongoSource must call .watch() to be used as a Stream Source")))
        }
    }
}

#[cfg(all(feature = "batch", not(target_family = "wasm")))]
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
            if count >= 10_000 { break; }
        }

        if count == 0 { return None; }

        let io_cursor = std::io::Cursor::new(buffer);
        match polars::io::ndjson::JsonLineReader::new(io_cursor).finish() {
            Ok(df) => Some(Ok(Context::root(df, "mongo_batch"))),
            Err(e) => Some(Err(anyhow::anyhow!("Polars parsing error: {}", e))),
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// 3. MONGO SINK
//
// Native: Direct TCP insert via mongodb driver
// WASM:   HTTP POST to Clotho Data Proxy (localhost DaemonSet)
// ═══════════════════════════════════════════════════════════════════════════════

// ── Native MongoSink ─────────────────────────────────────────────────────────
#[cfg(not(target_family = "wasm"))]
pub struct MongoSink {
    collection: Collection<Document>,
}

#[cfg(not(target_family = "wasm"))]
impl MongoSink {
    pub async fn new(uri: &str, db: &str, coll: &str) -> Result<Self> {
        let client = Client::with_options(ClientOptions::parse(uri).await?)?;
        let collection = client.database(db).collection::<Document>(coll);
        Ok(Self { collection })
    }
}

// ── WASM MongoSink (proxy) ───────────────────────────────────────────────────
#[cfg(target_family = "wasm")]
pub struct MongoSink {
    uri: String,
    db: String,
    coll: String,
    http_client: crate::http::Client,
}

#[cfg(target_family = "wasm")]
impl MongoSink {
    pub async fn new(uri: &str, db: &str, coll: &str) -> Result<Self> {
        Ok(Self {
            uri: uri.into(),
            db: db.into(),
            coll: coll.into(),
            http_client: crate::http::Client::new(),
        })
    }

    fn proxy_url(&self) -> String {
        crate::config::var_or("CLOTHO_PROXY_URL", "http://clotho-data-proxy.clotho-system.svc.cluster.local:9090")
    }
}

// ── Sink<Value> — single document insert ─────────────────────────────────────

#[cfg(not(target_family = "wasm"))]
#[async_trait]
impl Sink<serde_json::Value> for MongoSink {
    async fn write(&mut self, ctx: Context<serde_json::Value>) -> Result<()> {
        let doc = bson::to_document(&ctx.data)?;
        match self.collection.insert_one(doc, None).await {
            Ok(_) => Ok(()),
            Err(e) => {
                if let mongodb::error::ErrorKind::Write(
                    mongodb::error::WriteFailure::WriteError(ref we)
                ) = *e.kind {
                    if we.code == 11000 {
                        return Ok(());
                    }
                }
                Err(e.into())
            }
        }
    }
}

#[cfg(target_family = "wasm")]
#[async_trait(?Send)]
impl Sink<serde_json::Value> for MongoSink {
    async fn write(&mut self, ctx: Context<serde_json::Value>) -> Result<()> {
        let payload = serde_json::json!({
            "uri": &self.uri,
            "database": &self.db,
            "collection": &self.coll,
            "document": ctx.data,
        });

        let res = self.http_client
            .post(&format!("{}/v1/mongo/insert", self.proxy_url()))
            .json(&payload)?
            .send()
            .await?;

        if !res.is_success() {
            let status = res.status();
            let body = res.text().unwrap_or_default();
            anyhow::bail!("Clotho Data Proxy error ({}): {}", status, body);
        }
        Ok(())
    }
}

// ── Sink<Vec<Value>> — bulk insert (Pipeline::once payloads) ─────────────────

#[cfg(not(target_family = "wasm"))]
#[async_trait]
impl Sink<Vec<serde_json::Value>> for MongoSink {
    async fn write(&mut self, ctx: Context<Vec<serde_json::Value>>) -> Result<()> {
        let docs: Vec<Document> = ctx.data.iter()
            .filter_map(|v| bson::to_document(v).ok())
            .collect();
        if docs.is_empty() { return Ok(()); }

        let opts = mongodb::options::InsertManyOptions::builder().ordered(false).build();
        match self.collection.insert_many(docs, opts).await {
            Ok(_) => Ok(()),
            Err(e) => {
                if let mongodb::error::ErrorKind::BulkWrite(ref bwe) = *e.kind {
                    if let Some(ref write_errors) = bwe.write_errors {
                        if write_errors.iter().all(|we| we.code == 11000) {
                            return Ok(());
                        }
                    }
                }
                Err(e.into())
            }
        }
    }
}

#[cfg(target_family = "wasm")]
#[async_trait(?Send)]
impl Sink<Vec<serde_json::Value>> for MongoSink {
    async fn write(&mut self, ctx: Context<Vec<serde_json::Value>>) -> Result<()> {
        if ctx.data.is_empty() { return Ok(()); }

        let payload = serde_json::json!({
            "uri": &self.uri,
            "database": &self.db,
            "collection": &self.coll,
            "documents": ctx.data,
            "ordered": false,
        });

        let res = self.http_client
            .post(&format!("{}/v1/mongo/insert-many", self.proxy_url()))
            .json(&payload)?
            .send()
            .await?;

        if !res.is_success() {
            let status = res.status();
            let body = res.text().unwrap_or_default();
            anyhow::bail!("Clotho Data Proxy error ({}): {}", status, body);
        }

        let body = res.text().unwrap_or_default();
        if let Ok(v) = serde_json::from_str::<serde_json::Value>(&body) {
            let inserted = v.get("inserted_count").and_then(|n| n.as_u64()).unwrap_or(0);
            let duplicates = v.get("duplicate_count").and_then(|n| n.as_u64()).unwrap_or(0);
            eprintln!(
                "[MongoSink] insert-many {}.{} inserted={} duplicates={}",
                self.db,
                self.coll,
                inserted,
                duplicates
            );
        }
        Ok(())
    }
}

// ── Sink<DataFrame> — batch engine (native only) ────────────────────────────

#[cfg(all(feature = "batch", not(target_family = "wasm")))]
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