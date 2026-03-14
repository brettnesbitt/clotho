use crate::traits::{Source, Sink, LookupTarget, Context};
use anyhow::{Context as AnyhowContext, Result};
use async_trait::async_trait;
use tokio_postgres::{Client, NoTls}; // Note: use rustls in production

#[cfg(feature = "batch")]
use polars::prelude::*;

#[derive(Clone)]
pub struct PostgresLookup {
    client: std::sync::Arc<Client>,
    table: String,
    lookup_col: String,
}

impl PostgresLookup {
    pub async fn new(connection_string: &str, table: &str, lookup_col: &str) -> Result<Self> {
        let (client, connection) = tokio_postgres::connect(connection_string, NoTls).await?;
        
        // Spawn the connection driver into the background
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("[Clotho] Postgres connection error: {}", e);
            }
        });

        Ok(Self {
            client: std::sync::Arc::new(client),
            table: table.to_string(),
            lookup_col: lookup_col.to_string(),
        })
    }
}

#[cfg(feature = "batch")]
#[async_trait]
impl LookupTarget for PostgresLookup {
    async fn lookup_batch(&self, keys: Vec<&str>) -> Result<DataFrame> {
        if keys.is_empty() { return Ok(DataFrame::default()); }

        // 1. Vectorized Query using Postgres `ANY` array syntax
        let query = format!("SELECT * FROM {} WHERE {} = ANY($1)", self.table, self.lookup_col);
        
        let rows = self.client.query(&query, &[&keys]).await?;

        // 2. Map Postgres Rows -> JSON -> Polars (Safest WASM conversion path)
        let mut buffer = Vec::with_capacity(rows.len() * 128);
        for row in rows {
            // Note: A real implementation would map Postgres types (Int, Varchar) 
            // directly to Polars Series builders for maximum speed. 
            // For brevity, we use the NDJSON string buffer trick here.
            let mut map = serde_json::Map::new();
            for col in row.columns() {
                let name = col.name();
                // (Simplified type extraction)
                if let Ok(val) = row.try_get::<_, String>(name) {
                    map.insert(name.to_string(), serde_json::Value::String(val));
                }
            }
            serde_json::to_writer(&mut buffer, &serde_json::Value::Object(map))?;
            buffer.push(b'\n');
        }

        if buffer.is_empty() { return Ok(DataFrame::default()); }

        let cursor = std::io::Cursor::new(buffer);
        let df = polars::io::ndjson::JsonLineReader::new(cursor)
            .finish()?;

        Ok(df)
    }
}

// =====================================================================
// POSTGRES SOURCE (Batch & Stream)
// =====================================================================
pub struct PostgresSource {
    client: std::sync::Arc<Client>,
    query: String,
    has_run: bool,
}

impl PostgresSource {
    pub async fn new(connection_string: &str, query: &str) -> Result<Self> {
        let (client, connection) = tokio_postgres::connect(connection_string, NoTls).await?;
        tokio::spawn(async move {
            if let Err(e) = connection.await { eprintln!("[Clotho] PG connection error: {}", e); }
        });

        Ok(Self {
            client: std::sync::Arc::new(client),
            query: query.to_string(),
            has_run: false,
        })
    }
}

#[cfg(feature = "batch")]
#[async_trait]
impl Source<DataFrame> for PostgresSource {
    async fn next(&mut self) -> Option<Result<Context<DataFrame>>> {
        if self.has_run { return None; } // Batch queries typically run once per trigger
        self.has_run = true;

        let rows = match self.client.query(&self.query, &[]).await {
            Ok(r) => r,
            Err(e) => return Some(Err(e.into())),
        };

        if rows.is_empty() { return None; }

        // Fast conversion: Postgres Rows -> NDJSON -> Polars C++ Parser
        let mut buffer = Vec::with_capacity(rows.len() * 128);
        for row in rows {
            let mut map = serde_json::Map::new();
            for col in row.columns() {
                let name = col.name();
                // Extract assuming strings for simplicity in this example
                if let Ok(val) = row.try_get::<_, String>(name) {
                    map.insert(name.to_string(), serde_json::Value::String(val));
                }
            }
            let _ = serde_json::to_writer(&mut buffer, &serde_json::Value::Object(map));
            buffer.push(b'\n');
        }

        let cursor = std::io::Cursor::new(buffer);
        match polars::io::ndjson::JsonLineReader::new(cursor).finish() {
            Ok(df) => Some(Ok(Context::root(df, "postgres_batch"))),
            Err(e) => Some(Err(e.into())),
        }
    }
}

// =====================================================================
// POSTGRES SINK (Batch & Stream)
// =====================================================================
pub struct PostgresSink {
    client: std::sync::Arc<Client>,
    table: String,
}

impl PostgresSink {
    pub async fn new(connection_string: &str, table: &str) -> Result<Self> {
        let (client, connection) = tokio_postgres::connect(connection_string, NoTls).await?;
        tokio::spawn(async move {
            if let Err(e) = connection.await { eprintln!("[Clotho] PG connection error: {}", e); }
        });

        Ok(Self { client: std::sync::Arc::new(client), table: table.to_string() })
    }
}

// STREAM ENGINE: Insert a single JSON object into a Postgres JSONB column
#[async_trait]
impl Sink<serde_json::Value> for PostgresSink {
    async fn write(&mut self, ctx: Context<serde_json::Value>) -> Result<()> {
        let query = format!("INSERT INTO {} (payload) VALUES ($1::jsonb)", self.table);
        
        // We push the raw JSON directly into Postgres
        self.client.execute(&query, &[&ctx.data]).await?;
        Ok(())
    }
}

// BATCH ENGINE: High-Speed COPY FROM STDIN
#[cfg(feature = "batch")]
#[async_trait]
impl Sink<DataFrame> for PostgresSink {
    async fn write(&mut self, mut ctx: Context<DataFrame>) -> Result<()> {
        if ctx.data.height() == 0 { return Ok(()); }

        // 1. Convert Polars DataFrame to CSV in memory (Fastest format for Postgres COPY)
        let mut csv_buffer = Vec::with_capacity(ctx.data.height() * 128);
        polars::io::csv::CsvWriter::new(&mut csv_buffer)
            .include_header(false)
            .finish(&mut ctx.data)?;

        // 2. Open a COPY stream to Postgres
        let copy_query = format!("COPY {} FROM STDIN WITH (FORMAT csv)", self.table);
        let sink = self.client.copy_in(&copy_query).await?;
        
        // 3. Blast the bytes over the TCP socket using tokio-postgres's Sink abstraction
        let writer = tokio_postgres::binary_copy::BinaryCopyInWriter::new(sink, &[]);
        tokio::pin!(writer);
        
        // Write the raw CSV bytes
        writer.as_mut().write_all(&csv_buffer).await?;
        writer.as_mut().finish().await?;

        Ok(())
    }
}