# Clotho SDK: Serverless Data Streaming for Rust

[![Crates.io](https://img.shields.io/crates/v/clotho-sdk.svg)](https://crates.io/crates/clotho-sdk)
[![Documentation](https://docs.rs/clotho-sdk/badge.svg)](https://docs.rs/clotho-sdk)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

**Clotho** is a framework for building high-performance, serverless data pipelines in Rust. It compiles your logic into lightweight WebAssembly (Wasm) binaries that run on Kubernetes via [SpinKube](https://spinkube.dev).

**Why Clotho?**
* **Scale to Zero:** Unlike Flink or Spark, Clotho pipelines are event-driven. They consume 0 CPU when idle.
* **Invisible Lineage:** Distributed Trace IDs are automatically propagated through every `map`, `filter`, and `enrich` step. You write business logic; we handle the provenance.
* **Type Safety:** Input and Output schemas are enforced by the Rust compiler.
* **Bleeding Edge Speed:** Built on `tokio` with zero-copy atomic telemetry.

---

## 📦 Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
clotho-sdk = "0.1.0"
serde = { version = "1.0", features = ["derive"] }
serde_json = "1.0"
tokio = { version = "1.0", features = ["macros", "rt-multi-thread"] }
anyhow = "1.0"
```

## 🚀 Quick Start: The "Hello World"

Create a pipeline that wakes up every 60 seconds, checks an API, and logs the result.

```rust
use clotho_sdk::prelude::*;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<()> {
    Pipeline::read(IntervalSource::new(Duration::from_secs(60)))
        .map(|tick| {
            // Context is auto-injected into HTTP headers
            let resp = clotho::http::get("https://api.my-service.internal/health")?;
            Ok(format!("Tick {}: Status {}", tick, resp.status()))
        })
        .run(ConsoleSink::new())
        .await?;
        
    Ok(())
}
```

## 🧠 Core Concepts

### 1. The Context Envelope (Invisible Tracing)

Clotho wraps every data record in a Context<T> struct containing:

* **Trace ID:** The global workflow ID.
* **Span ID:** The current step ID.
* **Genealogy:** A history of parent steps (handling merges/forks).

You do not need to manage this.

The SDK unwraps the context before your closure runs.

The SDK re-wraps your result after your closure returns.

The SDK links the new context to the old one automatically.

### 2. Resilience (Dead Letter Queues)

Never crash on bad data. Configure a generic Sink to catch errors.

```rust
Pipeline::read(KafkaSource::new("transactions"))
    .with_dlq(S3Sink::new("s3://data-lake/failures")) // <--- The Safety Net
    .map(|txn| {
        // If this panics or returns Err, the event is saved to S3
        // and the pipeline continues to the next record.
        parse_complex_format(txn)?
    })
```

## 📚 The Cookbook
## 1. The "Standard" ETL (Kafka -> Postgres)
Type-safe transformation with filtering.

```rust
#[derive(Deserialize)] struct RawEvent { id: String, amount: f64 }
#[derive(Serialize)]   struct DbRow { id: String, value: f64 }

Pipeline::read(KafkaSource::new("payments-topic"))
    .map(|bytes| {
        let event: RawEvent = serde_json::from_slice(&bytes)?;
        Ok(event)
    })
    .filter(|e| e.amount > 0.0) // Drop invalid rows
    .map(|e| {
        Ok(DbRow { id: e.id, value: e.amount })
    })
    .run(PostgresSink::new(env!("PG_URL"), "insert_payments"))
    .await?;
```

## 2. The "DAG" (Enrichment & Branching)
A complex Fraud Detection pipeline that joins data from Redis and forks high-risk events to SNS.

```rust
// 1. Define Sinks
let archive = S3Sink::new("s3://lake/transactions");
let alerts = SnsSink::new("arn:aws:sns:fraud-alerts");

Pipeline::read(KafkaSource::new("transactions"))
    // MERGE: Join Kafka stream with Redis lookup
    .enrich(RedisSource::new("user_profiles"), |txn, profile| {
        EnrichedTxn { txn, profile }
    })
    
    // LOGIC: Calculate Risk
    .map(|data| {
        let score = ml_model::predict(&data);
        Ok(ScoredTxn { data, score })
    })
    
    // FORK: Send high-risk items to SNS (Side Effect)
    .wire_tap(
        |record| record.score > 90.0, 
        alerts
    )
    
    // SINK: Archive everything to S3
    .run(archive)
    .await?;
```

## 🛠 Testing (No Docker Required)
Clotho provides an In-Memory Bus (memory_channel) that mimics Kafka. You can unit test complex, multi-stage pipelines entirely in cargo test.

```rust
#[tokio::test]
async fn test_fraud_logic() -> Result<()> {
    let (mut topic_sink, topic_source) = clotho_sdk::builtins::memory_channel(10);

    // Pipeline A: Ingest
    tokio::spawn(async move {
        Pipeline::read(VecSource::new(vec![100.0, 5000.0]))
            .run(topic_sink).await.unwrap();
    });

    // Pipeline B: Process
    // Use a CaptureSink to inspect results in the test
    let results = Arc::new(Mutex::new(Vec::new()));
    
    Pipeline::read(topic_source)
        .map(|amount| Ok(amount * 2.0))
        .run(CaptureSink::new(results.clone()))
        .await?;
        
    assert_eq!(results.lock().await[0], 200.0);
    Ok(())
}
```

## 🚢 Deployment

### Build

```bash
cargo build --target wasm32-wasi --release
```

### Package

```bash
# Push to your registry (OCI artifact)
spin registry push ghcr.io/my-org/my-pipeline:v1
```

### Deploy

Create a Pipeline Custom Resource in Kubernetes:

```yaml
apiVersion: core.clotho.run/v1alpha1
kind: Pipeline
metadata:
  name: fraud-detector
spec:
  image: ghcr.io/my-org/my-pipeline:v1
  resources:
    limits:
      cpu: "500m"
      memory: "128Mi"
  config:
    - name: DB_PASS
      valueFrom:
        secretKeyRef:
          name: production-db
          key: password
```