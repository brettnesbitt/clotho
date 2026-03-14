use crate::traits::Source;
use crate::types::Context;
use anyhow::{Context as AnyhowContext, Result};
use async_trait::async_trait;
use rskafka::client::partition::PartitionClient;
use rskafka::client::consumer::{StreamConsumer, StreamConsumerBuilder};
use std::sync::Arc;
use tokio::time::{timeout, Duration};
use rskafka::record::Record;
use time::OffsetDateTime; // rskafka uses the `time` crate for timestamps

#[cfg(feature = "batch")]
use polars::prelude::*;

/// A high-performance, Wasm-compatible Kafka Source.
pub struct KafkaSource {
    /// The underlying pure-Rust Kafka partition client
    client: Arc<PartitionClient>,
    consumer: StreamConsumer,
    
    // Batching Configuration (Only used if passed to Pipeline::batch)
    max_batch_size: usize,
    max_wait_ms: u64,

    // support for bytes or JSON 
    format: DataFormat,
}

impl KafkaSource {
    /// The Simple Constructor (80% Use Case)
    /// Connects to a plaintext cluster and starts reading from the given offset.
    pub async fn new(
        brokers: Vec<String>, 
        topic: String, 
        partition: i32, 
        offset: i64
    ) -> Result<Self> {
        // Build the native client
        let client = rskafka::client::ClientBuilder::new(brokers)
            .build()
            .await
            .context("Failed to build rskafka client")?;
            
        let partition_client = Arc::new(
            client.partition_client(topic, partition)
            .await
            .context("Failed to create partition client")?
        );

        Self::from_client(partition_client, offset)
    }

    /// The BYOC Constructor (Bring Your Own Client)
    /// Allows the user to configure OAuth, TLS, and custom SASL auth 
    /// using the rskafka ClientBuilder, then pass the fully configured client to Clotho.
    pub fn from_client(partition_client: Arc<PartitionClient>, offset: i64) -> Result<Self> {
        let consumer = StreamConsumerBuilder::new(partition_client.clone(), offset)
            .with_max_wait_ms(100)
            .build();

        Ok(Self {
            client: partition_client,
            consumer,
            max_batch_size: 10_000,
            max_wait_ms: 500,
            format: DataFormat::Json, // Make JSON the intelligent default!
        })
    }

    pub fn with_format(mut self, format: DataFormat) -> Self {
        self.format = format;
        self
    }

    /// Configure the micro-batching parameters for the Polars engine
    pub fn with_batch_config(mut self, size: usize, wait_ms: u64) -> Self {
        self.max_batch_size = size;
        self.max_wait_ms = wait_ms;
        self
    }
}

// =====================================================================
// ENGINE 1: THE STREAM IMPLEMENTATION (Low Latency, Item-by-Item)
// =====================================================================

#[async_trait]
impl Source<Vec<u8>> for KafkaSource {
    async fn next(&mut self) -> Option<Result<Context<Vec<u8>>>> {
        match self.consumer.next().await {
            Some(Ok((record_and_offset, _watermark))) => {
                // Extract the raw bytes
                let data = record_and_offset.record.value.unwrap_or_default();
                
                // Extract Kafka Headers to map to Clotho Trace IDs if they exist
                let mut trace_id = uuid::Uuid::new_v4().to_string();
                if let Some(headers) = record_and_offset.record.headers {
                    for (key, value) in headers {
                        if key == "traceparent" || key == "X-Trace-Id" {
                            trace_id = String::from_utf8_lossy(&value).to_string();
                        }
                    }
                }

                Some(Ok(Context::root(data, trace_id)))
            }
            Some(Err(e)) => Some(Err(anyhow::anyhow!("Kafka consume error: {}", e))),
            None => None, // Stream ended
        }
    }
}

// =====================================================================
// ENGINE 2: THE BATCH IMPLEMENTATION (High Throughput, Polars Columnar)
// =====================================================================

#[cfg(feature = "batch")]
#[async_trait]
impl Source<DataFrame> for KafkaSource {
    async fn next(&mut self) -> Option<Result<Context<DataFrame>>> {
        let mut records: Vec<Vec<u8>> = Vec::with_capacity(self.max_batch_size);
        let start = std::time::Instant::now();
        let timeout_duration = tokio::time::Duration::from_millis(self.max_wait_ms);

        // ... (Micro-batching loop remains the same: collect N records or X ms) ...
        // [omitted for brevity, records.push(val)]
        
        if records.is_empty() {
            return Some(Ok(Context::root(DataFrame::default(), "kafka_batch_idle")));
        }

        // --- NEW: THE DECODING MAGIC ---
        let df = match self.format {
            DataFormat::RawBytes => {
                // The old way: just dump bytes into a single column
                let mut builder = polars::series::BinaryChunkedBuilder::new(
                    "raw_bytes", records.len(), records.iter().map(|v| v.len()).sum()
                );
                for rec in &records { builder.append_value(rec); }
                DataFrame::new(vec![builder.finish().into_series()]).unwrap_or_default()
            },
            DataFormat::Json => {
                // The new way: Join the Kafka messages with newlines (\n)
                let ndjson_buffer = records.join(&b'\n');
                let cursor = std::io::Cursor::new(ndjson_buffer);
                
                // Polars instantly infers the schema and builds a multi-column DataFrame!
                polars::io::ndjson::JsonLineReader::new(cursor)
                    .finish()
                    .unwrap_or_else(|e| {
                        eprintln!("[Clotho] JSON Parsing Error in Batch: {}", e);
                        DataFrame::default() // In a real app, send to DLQ here!
                    })
            }
        };

        Some(Ok(Context::root(df, "kafka_batch_flush")))
    }
}


// =====================================================================
// KafkaSink
// =====================================================================


pub struct KafkaSink {
    client: Arc<PartitionClient>,
    format: DataFormat,
}

impl KafkaSink {
    /// The Simple Constructor
    pub async fn new(brokers: Vec<String>, topic: String, partition: i32) -> Result<Self> {
        let client = rskafka::client::ClientBuilder::new(brokers)
            .build()
            .await
            .context("Failed to build rskafka client")?;
            
        let partition_client = Arc::new(
            client.partition_client(topic, partition)
            .await
            .context("Failed to create partition client")?
        );

        Ok(Self::from_client(partition_client))
    }

    /// The BYOC Constructor (OAuth, TLS, etc.)
    pub fn from_client(partition_client: Arc<PartitionClient>) -> Self {
        Self {
            client: partition_client,
            format: DataFormat::Json, // Intelligent default
        }
    }

    pub fn with_format(mut self, format: DataFormat) -> Self {
        self.format = format;
        self
    }
}

// =====================================================================
// SINK 1: THE STREAM IMPLEMENTATION (Item-by-Item)
// =====================================================================

#[async_trait]
impl Sink<serde_json::Value> for KafkaSink {
    async fn write(&mut self, ctx: Context<serde_json::Value>) -> Result<()> {
        let bytes = serde_json::to_vec(&ctx.data)?;
        
        // Check if the user set a custom Kafka Key in the metadata
        let key = ctx.meta.get("kafka_key").map(|k| k.as_bytes().to_vec());

        let record = Record {
            key,
            value: Some(bytes),
            // DISTRIBUTED TRACING: We inject the Clotho Trace ID into the Kafka Header!
            headers: std::collections::BTreeMap::from([
                ("X-Clotho-Trace-Id".to_string(), ctx.span_id.clone().into_bytes())
            ]),
            timestamp: OffsetDateTime::now_utc(),
        };

        // ProduceType::Await guarantees the broker acknowledged the write. 
        // If this fails, the error bubbles up to the Clotho DLQ!
        self.client.produce(vec![record], rskafka::client::produce::ProduceType::Await).await?;
        
        Ok(())
    }
}

// =====================================================================
// SINK 2: THE BATCH IMPLEMENTATION (High Throughput, Polars Columnar)
// =====================================================================

#[cfg(feature = "batch")]
#[async_trait]
impl Sink<DataFrame> for KafkaSink {
    async fn write(&mut self, mut ctx: Context<DataFrame>) -> Result<()> {
        if ctx.data.height() == 0 {
            return Ok(());
        }

        let mut records = Vec::with_capacity(ctx.data.height());

        match self.format {
            DataFormat::RawBytes => {
                // If the user wants to write raw bytes, they must provide a "raw_bytes" column
                let series = ctx.data.column("raw_bytes")
                    .context("DataFrame must contain a 'raw_bytes' column for RawBytes format")?;
                let chunks = series.binary()?;
                
                for opt_bytes in chunks.into_iter() {
                    if let Some(bytes) = opt_bytes {
                        records.push(Record {
                            key: None,
                            value: Some(bytes.to_vec()),
                            headers: std::collections::BTreeMap::from([
                                ("X-Clotho-Trace-Id".to_string(), ctx.span_id.clone().into_bytes())
                            ]),
                            timestamp: OffsetDateTime::now_utc(),
                        });
                    }
                }
            },
            DataFormat::Json => {
                // --- THE HIGH-SPEED BULK SERIALIZATION TRICK ---
                // Instead of iterating through Polars rows and serializing one by one, 
                // we tell Polars' C++ engine to serialize the entire table to a memory buffer at once.
                let mut buffer = Vec::with_capacity(ctx.data.height() * 256); // Pre-allocate approx size
                polars::io::ndjson::JsonWriter::new(&mut buffer)
                    .finish(&mut ctx.data)
                    .context("Failed to serialize DataFrame to NDJSON")?;

                // Now we just split the buffer by newlines (\n) and wrap them in Kafka Records!
                for line in buffer.split(|&b| b == b'\n') {
                    if line.is_empty() { continue; }
                    
                    records.push(Record {
                        key: None, 
                        value: Some(line.to_vec()),
                        headers: std::collections::BTreeMap::from([
                            ("X-Clotho-Trace-Id".to_string(), ctx.span_id.clone().into_bytes())
                        ]),
                        timestamp: OffsetDateTime::now_utc(),
                    });
                }
            }
        }

        // Produce all 10,000 records in a single network request
        self.client.produce(records, rskafka::client::produce::ProduceType::Await).await?;

        Ok(())
    }
}