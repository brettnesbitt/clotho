use crate::traits::{Sink, Source, Context};
use anyhow::{Context as AnyhowContext, Result};
use async_trait::async_trait;
use crate::http::Client;
use serde::Serialize;

#[cfg(feature = "batch")]
use polars::prelude::*;

#[derive(Clone)]
pub struct SnowflakeConfig {
    pub account: String,     // e.g., "xy12345.us-east-1"
    pub warehouse: String,
    pub database: String,
    pub schema: String,
    pub table: String,
    pub jwt_token: String,   // Snowflake Key-Pair Auth is required for APIs
}

#[derive(Serialize)]
struct SnowflakeApiRequest {
    statement: String,
    warehouse: String,
    database: String,
    schema: String,
}

pub struct SnowflakeSink {
    config: SnowflakeConfig,
    client: Client,
}

impl SnowflakeSink {
    pub fn new(config: SnowflakeConfig) -> Self {
        Self {
            config,
            client: Client::new(),
        }
    }
}

#[cfg(feature = "batch")]
#[async_trait]
impl Sink<DataFrame> for SnowflakeSink {
    async fn write(&mut self, mut ctx: Context<DataFrame>) -> Result<()> {
        if ctx.data.height() == 0 { return Ok(()); }

        // 1. High-Speed Conversion: Polars -> JSON Array of Arrays
        // Snowflake's REST API loves JSON arrays for bulk inserts.
        // E.g., INSERT INTO table SELECT * FROM JSON_TABLE(...)
        let mut buffer = Vec::with_capacity(ctx.data.height() * 256);
        polars::io::ndjson::JsonWriter::new(&mut buffer)
            .finish(&mut ctx.data)?;

        // For absolute maximum throughput in Snowflake via REST, we upload 
        // the NDJSON payload as a literal string in an INSERT statement 
        // using Snowflake's native PARSE_JSON() function.
        let json_payload = String::from_utf8(buffer)?;
        
        let statement = format!(
            "INSERT INTO {} SELECT * FROM TABLE(FLATTEN(INPUT => PARSE_JSON('[{}]')))",
            self.config.table,
            json_payload.replace('\n', ",") // Quick NDJSON to JSON Array trick
        );

        let req_body = SnowflakeApiRequest {
            statement,
            warehouse: self.config.warehouse.clone(),
            database: self.config.database.clone(),
            schema: self.config.schema.clone(),
        };

        let url = format!("https://{}.snowflakecomputing.com/api/v2/statements", self.config.account);

        // 2. Execute via HTTP (100% Wasm Native)
        self.client.post(&url)
            .header("authorization", &format!("Bearer {}", self.config.jwt_token))
            .header("accept", "application/json")
            .json(&req_body)?
            .send()
            .await
            .context("Failed to call Snowflake API")?
            .is_success()
            .then_some(())
            .ok_or_else(|| anyhow::anyhow!("Snowflake API rejected the bulk insert"))?;

        Ok(())
    }
}

// =====================================================================
// SNOWFLAKE SOURCE (Batch Only)
// =====================================================================
pub struct SnowflakeSource {
    config: SnowflakeConfig,
    query: String,
    client: Client,
    has_run: bool,
}

impl SnowflakeSource {
    pub fn new(config: SnowflakeConfig, query: &str) -> Self {
        Self {
            config,
            query: query.to_string(),
            client: Client::new(),
            has_run: false,
        }
    }
}

// BATCH ENGINE: Fetch large datasets from Snowflake
#[cfg(feature = "batch")]
#[async_trait]
impl Source<DataFrame> for SnowflakeSource {
    async fn next(&mut self) -> Option<Result<Context<DataFrame>>> {
        if self.has_run { return None; }
        self.has_run = true;

        let req_body = SnowflakeApiRequest {
            statement: self.query.clone(),
            warehouse: self.config.warehouse.clone(),
            database: self.config.database.clone(),
            schema: self.config.schema.clone(),
        };

        let url = format!("https://{}.snowflakecomputing.com/api/v2/statements", self.config.account);

        // 1. Submit the Query
        let req = match self.client.post(&url)
            .header("authorization", &format!("Bearer {}", self.config.jwt_token))
            .header("accept", "application/json")
            .json(&req_body)
        {
            Ok(r) => r,
            Err(e) => return Some(Err(e)),
        };

        let res = match req.send().await {
            Ok(resp) => resp,
            Err(e) => return Some(Err(e.into())),
        };

        if !res.is_success() {
            return Some(Err(anyhow::anyhow!(
                "Snowflake query failed with status {}",
                res.status()
            )));
        }

        let json_response: serde_json::Value = match res.json() {
            Ok(j) => j,
            Err(e) => return Some(Err(e)),
        };

        // 2. Parse the Snowflake REST API response format
        // Snowflake returns data in a "data" array of arrays: [ ["row1_col1", "row1_col2"], ["row2..."] ]
        let data_rows = json_response.get("data").and_then(|d| d.as_array());
        
        if let Some(rows) = data_rows {
            if rows.is_empty() { return None; }

            // To get this into Polars quickly via NDJSON, we convert the arrays to objects
            // using the column names provided in the response metadata.
            let column_meta = json_response["resultSetMetaData"]["rowType"].as_array().unwrap();
            let mut col_names = Vec::new();
            for col in column_meta {
                col_names.push(col["name"].as_str().unwrap_or("unknown"));
            }

            let mut buffer = Vec::with_capacity(rows.len() * 128);
            for row in rows {
                let mut map = serde_json::Map::new();
                if let Some(row_arr) = row.as_array() {
                    for (i, val) in row_arr.iter().enumerate() {
                        let col_name = col_names.get(i).unwrap_or(&"unknown").to_string();
                        map.insert(col_name, val.clone());
                    }
                }
                let _ = serde_json::to_writer(&mut buffer, &serde_json::Value::Object(map));
                buffer.push(b'\n');
            }

            let cursor = std::io::Cursor::new(buffer);
            match polars::io::ndjson::JsonLineReader::new(cursor).finish() {
                Ok(df) => Some(Ok(Context::root(df, "snowflake_batch"))),
                Err(e) => Some(Err(e.into())),
            }
        } else {
            None // No data returned
        }
    }
}