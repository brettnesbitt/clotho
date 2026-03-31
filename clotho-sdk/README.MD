# Clotho SDK: Serverless Data Pipelines for Rust

[![Crates.io](https://img.shields.io/crates/v/clotho-sdk.svg)](https://crates.io/crates/clotho-sdk)
[![Documentation](https://docs.rs/clotho-sdk/badge.svg)](https://docs.rs/clotho-sdk)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

**Clotho** is a framework for building high-performance, serverless data pipelines in Rust. It compiles your logic into lightweight WebAssembly (Wasm) binaries or native containers that run on Kubernetes via the Clotho Operator.

**Why Clotho?**

- **Scale to Zero:** Unlike Flink or Spark, Clotho pipelines are event-driven. They consume 0 CPU when idle.
- **Invisible Lineage:** Distributed Trace IDs are automatically propagated through every `map` and `enrich` step. You write business logic; we handle the provenance.
- **Type Safety:** Input and Output schemas are enforced by the Rust compiler.
- **Bleeding Edge Speed:** Built on `tokio` with zero-copy atomic telemetry.
- **DAG Pipelines:** Compose multi-stage pipelines with handoff stages. Each stage scales independently and communicates through the Clotho Bus.

---

## Table of Contents

1. [Installation](#installation)
2. [Pipeline Modes](#pipeline-modes)
3. [Quick Start](#quick-start)
4. [Core Concepts](#core-concepts)
5. [DAG Pipelines (Handoff Stages)](#dag-pipelines-handoff-stages)
6. [The Bus System](#the-bus-system)
7. [Batch Pipelines](#batch-pipelines)
8. [The Cookbook](#the-cookbook)
9. [Testing](#testing)
10. [Deployment](#deployment)
11. [CRD Reference](#crd-reference)

---

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
clotho-sdk = "0.1.0"
serde = { version = "1.0", features = ["derive"] }
serde_json = "1.0"
tokio = { version = "1.0", features = ["macros", "rt-multi-thread"] }
anyhow = "1.0"
```

### Feature Flags

| Feature | Purpose | Default |
|---|---|---|
| `native` | Enables `tokio` runtime, `BusSource`/`BusSink` for inter-stage communication, and native connectors | off |
| `batch` | Enables the Polars-based columnar batch engine | off |

```toml
# For DAG pipelines with inter-stage communication
clotho-sdk = { version = "0.1.0", features = ["native"] }

# For batch/ETL pipelines
clotho-sdk = { version = "0.1.0", features = ["batch"] }
```

---

## Pipeline Modes

Clotho supports three pipeline execution models. Choose the right one for your workload:

| Mode | Builder | Best For | How It Works |
|---|---|---|---|
| **Stream** | `Pipeline::stream(source)` | Webhooks, Alerts, IoT, API integration | Processes records one-by-one, continuously |
| **Once** | `Pipeline::once(data)` | Webhooks, serverless triggers | Processes a single payload and exits |
| **Batch** | `Pipeline::batch(source)` | ETL, Analytics, S3 Archiving | Processes DataFrames in columnar batches (Polars) |

All three modes automatically emit telemetry (lifecycle events, throughput, DLQ records) to the Clotho Agent sidecar.

---

## Quick Start

### Stream Pipeline (Hello World)

Create a pipeline that wakes up every 60 seconds and logs a tick count. Requires the `native` feature for `IntervalSource`.

```rust
use clotho::builtins::{IntervalSource, ConsoleSink};
use clotho::stream::StreamPipeline;
use std::time::Duration;
use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    StreamPipeline::new(IntervalSource::new(Duration::from_secs(60)))
        .map(|tick| {
            Ok(format!("Tick {}", tick))
        })
        .run(ConsoleSink::new())
        .await?;

    Ok(())
}
```

### Once Pipeline (Single Payload)

```rust
use clotho::once::OncePipeline;
use clotho::builtins::ConsoleSink;
use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let payload = "incoming data".to_string();

    OncePipeline::new(payload)
        .map(|data| {
            let processed = data.to_uppercase();
            Ok(processed)
        })
        .run(ConsoleSink::new())
        .await
}
```

---

## Core Concepts

### 1. The Context Envelope (Invisible Tracing)

Clotho wraps every data record in a `Context<T>` struct containing:

- **Trace ID:** The global workflow ID.
- **Span ID:** The current step ID.
- **Genealogy:** A history of parent steps (handling merges/forks).

You do not need to manage this. The SDK unwraps the context before your closure runs, re-wraps your result after your closure returns, and links the new context to the old one automatically.

### 2. Resilience (Dead Letter Queues)

Never crash on bad data. When a transform returns `Err`, the SDK automatically:

1. Increments `records_failed`
2. Emits a DLQ record to the Clotho Agent via UDP (pipeline ID, trace ID, error, payload)
3. Continues processing the next record (stream mode) or breaks the batch (batch mode)

No manual DLQ configuration is needed -- telemetry is built into the pipeline loop.

### 3. Telemetry (Automatic)

Every pipeline mode (stream, once, batch) automatically emits:

- **Lifecycle events:** `STARTUP`, `FIRST_BATCH`/`FIRST_WRITE`, `FINISHED`
- **Throughput metrics:** `records_in`, `records_out`, `records_failed`, `bytes_processed`
- **DLQ records:** Failed records with error context, routed via UDP to the Clotho Agent
- **Data quality events:** Contract validation results from `BatchPipeline::expect()`

Telemetry is fire-and-forget over UDP to `127.0.0.1:8125` (the agent sidecar). It never blocks or crashes your pipeline.

---

## DAG Pipelines (Handoff Stages)

DAG (Directed Acyclic Graph) pipelines let you decompose complex workflows into **independent stages** that communicate through the Clotho Bus. Each stage becomes its own Kubernetes workload, scaling independently.

### When to Use DAG Pipelines

- **Fan-out:** One ingestion stage feeds multiple parallel worker stages
- **Separation of concerns:** Ingest, transform, and load stages with different resource profiles
- **Independent scaling:** Scale CPU-heavy enrichment stages without over-provisioning ingestion
- **Mixed runtimes:** Combine Wasm and native stages in the same pipeline

### Architecture

```
                    ┌─────────────┐
                    │   Ingest    │  (1 replica)
                    │  Stage      │
                    └──────┬──────┘
                           │
                    CLOTHO_BUS_OUT
                           │
               ┌───────────┼───────────┐
               │                       │
        ┌──────┴──────┐        ┌──────┴──────┐
        │   Worker A  │        │   Worker B  │
        │  (2 replicas)│        │  (1 replica) │
        └──────┬──────┘        └──────┬──────┘
               │                       │
        CLOTHO_BUS_OUT          CLOTHO_BUS_OUT
               │                       │
               └───────────┬───────────┘
                           │
                    ┌──────┴──────┐
                    │   Sink      │  (1 replica)
                    │  Stage      │
                    └─────────────┘
```

### CRD Definition

Define stages in the Pipeline custom resource. The operator validates the DAG structure (no cycles, no dangling dependencies) and creates separate workloads for each stage.

```yaml
apiVersion: core.clotho.run/v1alpha1
kind: Pipeline
metadata:
  name: etl-pipeline
  namespace: clotho-pipelines
spec:
  runtime: native
  mode: batch
  image: us-central1-docker.pkg.dev/my-project/clotho/etl-pipeline:v1

  # Global config inherited by all stages
  config:
    - name: MONGO_URI
      valueFrom:
        secretKeyRef:
          name: stockseer-secrets
          key: mongo-uri

  # Stage definitions create the DAG topology
  stages:
    # Entry stage: no dependsOn, so this is a root node
    - name: ingest
      entrypoint: src/ingest.rs
      replicas: 1
      schedule:
        mode: interval
        interval: "5m"

    # Worker stage: depends on ingest, reads from its output bus
    - name: enrich
      entrypoint: src/enrich.rs
      replicas: 2
      dependsOn:
        - ingest
      resources:
        requests:
          cpu: "250m"
          memory: "256Mi"
        limits:
          cpu: "1"
          memory: "512Mi"

    # Terminal stage: depends on enrich, no downstream consumers
    - name: store
      entrypoint: src/store.rs
      replicas: 1
      dependsOn:
        - enrich
      config:
        - name: TARGET_COLLECTION
          value: "enriched_data"
```

### How the Operator Wires Stages

When the operator detects `spec.stages`, it:

1. **Validates the DAG** -- checks for empty names, duplicate names, missing dependencies, and cycles (DFS-based detection).
2. **Creates one workload per stage** -- each stage becomes a separate Deployment (native) or SpinApp (wasm) named `{pipeline}-{stage}` (e.g., `etl-pipeline-ingest`).
3. **Injects bus environment variables:**
   - **Upstream stages** with downstream consumers get `CLOTHO_BUS_OUT={pipeline}-{stage}-out`
   - **Downstream stages** get `CLOTHO_BUS_{UPSTREAM_NAME}={pipeline}-{upstream}-out` for each dependency
4. **Merges configuration** -- parent pipeline config is inherited by all stages, with stage-specific config appended (stage config takes priority).
5. **Inherits resources** -- if a stage doesn't define its own `resources`, it inherits the parent pipeline's resource profile.

### Stage Fields Reference

| Field | Type | Required | Description |
|---|---|---|---|
| `name` | string | Yes | Unique identifier for this stage within the pipeline |
| `entrypoint` | string | Yes | Path to the Rust source file for this stage |
| `replicas` | int32 | No | Number of replicas (default: 1) |
| `dependsOn` | []string | No | Stages that must exist upstream. Creates DAG edges. |
| `resources` | ResourceRequirements | No | CPU/memory for this stage. Inherits from parent if omitted. |
| `config` | []ConfigVar | No | Stage-specific env vars. Merged with parent config. |
| `schedule` | ScheduleSpec | No | Only valid for entry stages (no `dependsOn`). |

### Environment Variables Injected by the Operator

Your stage code reads these to discover its bus connections:

| Variable | Injected When | Value | Purpose |
|---|---|---|---|
| `CLOTHO_STAGE_ENTRYPOINT` | Always (DAG stages) | e.g. `src/ingest.rs` | Path to the Rust source file for this stage |
| `CLOTHO_BUS_OUT` | Stage has downstream consumers | `{pipeline}-{stage}-out` | The bus name to write output to |
| `CLOTHO_BUS_{DEP}` | Stage has `dependsOn` entries | `{pipeline}-{dep}-out` | The bus name to read from upstream stage `{DEP}` |
| `CLOTHO_PIPELINE_ID` | Always (native runtime) | `{pipeline}-{stage}` | Identity of this stage workload |

### Backward Compatibility

**Existing pipelines are fully backward compatible.** If `spec.stages` is omitted or empty, the operator follows the original single-workload reconciliation path. No changes are needed to existing Pipeline CRDs.

| Pipeline Type | Behavior |
|---|---|
| No `stages` field | Single workload, same as before (SpinApp or Deployment) |
| `stages` defined | One workload per stage, bus env vars injected |

---

## The Bus System

The Clotho Bus provides inter-stage communication for DAG pipelines. The SDK exposes `BusSource<T>` and `BusSink<T>` types that implement the standard `Source` and `Sink` traits.

### SDK Usage

```rust
use clotho::{bus_source, bus_sink};
use clotho::stream::StreamPipeline;
use clotho::builtins::ConsoleSink;

// --- Ingest Stage (writes to bus) ---
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let bus_out = std::env::var("CLOTHO_BUS_OUT")?;
    let sink = bus_sink::<String>(bus_out).await?;

    StreamPipeline::new(MySource::new())
        .map(|raw| {
            let record = process(raw)?;
            Ok(record)
        })
        .run(sink)
        .await
}

// --- Worker Stage (reads from bus, writes to bus) ---
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let bus_in = std::env::var("CLOTHO_BUS_INGEST")?;
    let bus_out = std::env::var("CLOTHO_BUS_OUT")?;

    let source = bus_source::<String>(bus_in).await?;
    let sink = bus_sink::<String>(bus_out).await?;

    StreamPipeline::new(source)
        .map_async(|ctx| async move {
            let enriched = call_enrichment_api(&ctx.data).await?;
            Ok(clotho::Context::child_of(&ctx, enriched))
        })
        .run(sink)
        .await
}

// --- Terminal Stage (reads from bus, writes to final sink) ---
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let bus_in = std::env::var("CLOTHO_BUS_ENRICH")?;
    let source = bus_source::<String>(bus_in).await?;

    StreamPipeline::new(source)
        .run(ConsoleSink::new())
        .await
}
```

### Bus Configuration

For advanced use cases, use `BusConfig` directly:

```rust
use clotho::{BusSource, BusConfig};

let config = BusConfig::new("my-pipeline-ingest-out")
    .with_buffer_size(4096); // Broadcast channel buffer (default: 1024)

let source = BusSource::<String>::new(config).await?;
```

### Helper Functions

| Function | Description |
|---|---|
| `bus_source::<T>(name)` | Create a `BusSource` with default config |
| `bus_sink::<T>(name)` | Create a `BusSink` with default config |

### Serialization

Bus messages are serialized as JSON via `serde_json`. Your data types must implement `Serialize + Deserialize`. The full `Context<T>` envelope (including trace lineage) is preserved across bus hops.

---

## Batch Pipelines

The batch engine processes data as columnar DataFrames using [Polars](https://pola.rs). Enable it with the `batch` feature flag.

```toml
clotho-sdk = { version = "0.1.0", features = ["batch"] }
polars = { version = "0.36", features = ["lazy"] }
```

### Batch Pipeline API

The batch engine supports:

- **`.map()`** -- Pure Polars expressions (lazy, zero-copy). Takes a `LazyFrame`, returns a `LazyFrame`.
- **`.map_async()`** -- Async I/O (forces eager `.collect()` first). Takes a `DataFrame`, returns a `DataFrame`.
- **`.enrich()`** -- Bulk lookup join via a `LookupTarget` implementation.
- **`.expect()`** -- Data contract validation. Emits `ContractStatus` (Pass/Warning/Fail) to telemetry.

```rust
use clotho::batch::BatchPipeline;
use clotho::builtins::ConsoleSink;
use polars::prelude::*;

BatchPipeline::new(my_source)
    // Pure Polars expression (lazy, zero-copy)
    .map(|lf| {
        lf.filter(col("amount").gt(lit(0)))
          .with_column((col("amount") * lit(1.1)).alias("adjusted"))
    })
    // Data contract validation
    .expect("non_negative_amounts", |df| {
        let min = df.column("amount").unwrap().min::<f64>().unwrap_or(0.0);
        if min >= 0.0 { ContractStatus::Pass }
        else { ContractStatus::Fail }
    })
    .run(ConsoleSink::new())
    .await?;
```

### Batch Telemetry

The batch engine automatically tracks:

- **`records_in`** / **`records_out`** -- row counts per batch
- **`records_failed`** -- rows lost to transform errors or contract failures
- **`bytes_processed`** -- estimated from DataFrame shape (`rows * cols * 8`)
- **Execution report** -- posted to the Clotho Control Plane API on completion

---

## The Cookbook

### 1. Stream Pipeline with Transform Chain

Type-safe transformation with multiple `map` steps.

```rust
use clotho::stream::StreamPipeline;
use clotho::builtins::{VecSource, ConsoleSink};

#[derive(serde::Deserialize)] struct RawEvent { id: String, amount: f64 }
#[derive(Debug)]               struct Processed { id: String, value: f64 }

StreamPipeline::new(VecSource::new(vec![
    r#"{"id":"a","amount":10.0}"#.to_string(),
    r#"{"id":"b","amount":-5.0}"#.to_string(),
]))
    .map(|bytes| {
        let event: RawEvent = serde_json::from_str(&bytes)?;
        Ok(Processed { id: event.id, value: event.amount.abs() })
    })
    .run(ConsoleSink::new())
    .await?;
```

### 2. Multi-Stage DAG Pipeline

A three-stage pipeline where ingestion, enrichment, and storage are separate workloads.

**Pipeline CRD:**

```yaml
apiVersion: core.clotho.run/v1alpha1
kind: Pipeline
metadata:
  name: market-data
spec:
  runtime: native
  mode: stream
  image: us-central1-docker.pkg.dev/my-project/clotho/market-data:v1
  config:
    - name: API_KEY
      valueFrom:
        secretKeyRef:
          name: market-secrets
          key: api-key
  stages:
    - name: ingest
      entrypoint: src/ingest.rs
      replicas: 1
      schedule:
        mode: interval
        interval: "30s"
    - name: enrich
      entrypoint: src/enrich.rs
      replicas: 3
      dependsOn: [ingest]
      resources:
        requests: { cpu: "500m", memory: "512Mi" }
        limits: { cpu: "2", memory: "1Gi" }
    - name: store
      entrypoint: src/store.rs
      replicas: 1
      dependsOn: [enrich]
```

**Stage code (ingest.rs):**

```rust
use clotho::{bus_sink, Context};
use clotho::stream::StreamPipeline;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let sink = bus_sink::<String>(
        std::env::var("CLOTHO_BUS_OUT")?
    ).await?;

    StreamPipeline::new(MySource::new())
        .map(|raw| Ok(parse_record(raw)?))
        .run(sink)
        .await
}
```

**Stage code (enrich.rs):**

```rust
use clotho::{bus_source, bus_sink, Context};
use clotho::stream::StreamPipeline;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let source = bus_source::<String>(
        std::env::var("CLOTHO_BUS_INGEST")?
    ).await?;
    let sink = bus_sink::<String>(
        std::env::var("CLOTHO_BUS_OUT")?
    ).await?;

    StreamPipeline::new(source)
        .map_async(|ctx| async move {
            let enriched = enrich(&ctx.data).await?;
            Ok(Context::child_of(&ctx, enriched))
        })
        .run(sink)
        .await
}
```

**Stage code (store.rs):**

```rust
use clotho::bus_source;
use clotho::stream::StreamPipeline;
use clotho::builtins::ConsoleSink;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let source = bus_source::<String>(
        std::env::var("CLOTHO_BUS_ENRICH")?
    ).await?;

    StreamPipeline::new(source)
        .run(ConsoleSink::new())
        .await
}
```

---

## Testing

Clotho provides an In-Memory Bus (`memory_channel`) for testing multi-stage pipelines in `cargo test`. Requires the `native` feature.

```rust
use clotho::stream::StreamPipeline;
use clotho::builtins::{VecSource, ConsoleSink, memory_channel};

#[tokio::test]
async fn test_pipeline_chain() -> anyhow::Result<()> {
    let (topic_sink, topic_source) = memory_channel::<String>(10);

    // Pipeline A: produce
    tokio::spawn(async move {
        StreamPipeline::new(VecSource::new(vec!["hello".to_string(), "world".to_string()]))
            .run(topic_sink).await.unwrap();
    });

    // Pipeline B: consume and transform
    StreamPipeline::new(topic_source)
        .map(|s| Ok(s.to_uppercase()))
        .run(ConsoleSink::new())
        .await?;

    Ok(())
}
```

---

## Deployment

### Runtimes

Clotho supports two runtimes:

| Runtime | `spec.runtime` | Workload Type | Best For |
|---|---|---|---|
| **Wasm** (default) | `wasm` | SpinApp via SpinKube | Lightweight, scale-to-zero pipelines |
| **Native** | `native` | Kubernetes Deployment | Pipelines needing full OS access, DAG stages |

### Build (Wasm)

```bash
cargo build --target wasm32-wasip1 --release
spin registry push ghcr.io/my-org/my-pipeline:v1
```

### Build (Native)

```bash
docker build -t us-central1-docker.pkg.dev/my-project/clotho/my-pipeline:v1 .
docker push us-central1-docker.pkg.dev/my-project/clotho/my-pipeline:v1
```

### Deploy (Single-Stage Pipeline)

```yaml
apiVersion: core.clotho.run/v1alpha1
kind: Pipeline
metadata:
  name: fraud-detector
spec:
  image: ghcr.io/my-org/my-pipeline:v1
  resources:
    requests:
      cpu: "100m"
      memory: "64Mi"
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

### Deploy (DAG Pipeline)

See the [DAG Pipelines](#dag-pipelines-handoff-stages) section above for the full CRD schema.

```bash
kubectl apply -f pipeline.yaml -n clotho-pipelines
```

### Scheduling

Pipelines support three schedule modes:

| Mode | Description | Example |
|---|---|---|
| `trigger` (default) | On-demand only, invoked via API | No schedule field needed |
| `interval` | Invoked every N duration | `interval: "5m"` |
| `cron` | Cron expression (planned) | `cron: "0 9 * * *"` |

```yaml
spec:
  schedule:
    mode: interval
    interval: "30s"
```

For DAG pipelines, only **entry stages** (no `dependsOn`) should have schedules. Downstream stages are triggered by bus messages from their upstream dependencies.

---

## CRD Reference

### PipelineSpec

| Field | Type | Default | Description |
|---|---|---|---|
| `runtime` | `wasm` / `native` | `wasm` | Execution target |
| `mode` | `stream` / `once` / `batch` | `stream` | Execution model (affects telemetry) |
| `gitRepository` | string | -- | Git repo URL for Tier 1 builder |
| `reference` | string | `main` | Git branch/tag/commit |
| `gitCredentialsSecret` | string | -- | Secret with `token` key for git auth |
| `path` | string | -- | Subdirectory in monorepo |
| `image` | string | -- | OCI image (Tier 2: set manually; Tier 1: set by builder) |
| `imagePullSecrets` | []LocalObjectReference | -- | Secrets for pulling from private registries |
| `config` | []ConfigVar | -- | Environment variables and secret injections |
| `resources` | ResourceRequirements | 5m/32Mi req, 100m/128Mi limit | CPU/memory profile |
| `policy` | PolicySpec | 30s timeout, 3 retries | Operational guardrails |
| `replicas` | int32 | 1 | Desired replica count |
| `schedule` | ScheduleSpec | -- | When to invoke (trigger/interval/cron) |
| `stages` | []PipelineStage | -- | DAG stage definitions (see below) |
| `messageBus` | MessageBusSpec | -- | Reserved for future transport backends |

### PipelineStage

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | string | **(required)** | Unique stage identifier |
| `entrypoint` | string | **(required)** | Path to Rust source file |
| `replicas` | int32 | 1 | Replicas for this stage |
| `dependsOn` | []string | -- | Upstream stage names (creates DAG edges) |
| `resources` | ResourceRequirements | inherited | Stage-specific resources |
| `config` | []ConfigVar | inherited | Stage-specific env vars (merged with parent) |
| `schedule` | ScheduleSpec | -- | Only valid for entry stages |

### ConfigVar

```yaml
# Literal value
- name: LOG_LEVEL
  value: "info"

# Secret reference
- name: DB_PASSWORD
  valueFrom:
    secretKeyRef:
      name: my-secret
      key: password
```

---

## Migration Guide: Single-Stage to DAG

Converting an existing single-stage pipeline to a DAG pipeline:

1. **No CRD changes required for existing pipelines.** The `stages` field is optional. Omitting it preserves the original single-workload behavior.

2. **To add stages**, move your monolithic logic into separate entrypoints and define the DAG:

```yaml
# Before (single-stage)
spec:
  image: my-pipeline:v1
  config:
    - name: API_KEY
      valueFrom: { secretKeyRef: { name: secrets, key: api-key } }

# After (multi-stage, fully backward compatible)
spec:
  image: my-pipeline:v1
  config:
    - name: API_KEY
      valueFrom: { secretKeyRef: { name: secrets, key: api-key } }
  stages:
    - name: ingest
      entrypoint: src/ingest.rs
    - name: process
      entrypoint: src/process.rs
      dependsOn: [ingest]
    - name: store
      entrypoint: src/store.rs
      dependsOn: [process]
```

3. **Update your Rust code** to use `BusSource` / `BusSink` for inter-stage communication, reading bus names from the `CLOTHO_BUS_*` environment variables.

4. **Shared config is inherited.** Only add stage-specific `config` entries for values that differ per stage.

---

## License

MIT