# Clotho: Serverless Data Pipelines for Rust

**Clotho** is a framework for building high-performance, serverless data pipelines in Rust. It compiles your logic into lightweight WebAssembly (WASM) binaries that run on Kubernetes via [SpinKube](https://spinkube.dev). The Clotho operator handles building, deploying, and scheduling your pipelines.

**Why Clotho?**
* **Scale to Zero:** Unlike Flink or Spark, Clotho pipelines are event-driven. They consume 0 CPU when idle.
* **Pure Business Logic:** You write the *what* (source, transform, sink). The operator handles the *when* and *how*.
* **Type Safety:** Input and Output schemas are enforced by the Rust compiler.
* **Built-in Telemetry:** Automatic lifecycle events and progress tracking sent to the Clotho control plane via UDP.

---

## Installation

Add this to your `Cargo.toml`:

```toml
[lib]
crate-type = ["cdylib"]

[dependencies]
clotho-sdk = "0.0.1-alpha.1"
spin-sdk = "3.0"
anyhow = "1.0"
```

---

## Quick Start

### A Minimal Pipeline

Every Clotho pipeline follows the same pattern: **Source -> Transform -> Sink**.
The `#[clotho::main]` macro wraps your logic into a Spin HTTP component automatically.

```rust
use clotho::Pipeline;
use clotho::builtins::{VecSource, ConsoleSink};
use anyhow::Result;

#[clotho::main]
async fn main() -> Result<()> {
    let items: Vec<String> = vec![
        "AAPL: Buy at $175.50".into(),
        "NVDA: Buy at $880.00".into(),
    ];

    Pipeline::stream(VecSource::new(items))
        .map(|signal| {
            println!("[Signal] {}", signal);
            Ok(signal)
        })
        .run(ConsoleSink::new())
        .await?;

    Ok(())
}
```

That's it. No `tokio::main`, no HTTP handler boilerplate, no runtime configuration.
The macro generates the Spin entrypoint, telemetry hooks, and error handling for you.

---

## Architecture

Clotho separates **pipeline logic** from **orchestration**:

```
Developer writes:          Operator manages:
+-----------------+        +---------------------+
| Source           |        | Build (git -> WASM) |
| Transform (map) |        | Deploy (SpinApp)    |
| Sink             |        | Schedule (cron/interval) |
+-----------------+        | Telemetry collection |
                           +---------------------+
```

### Pipeline = Pure Business Logic

A pipeline runs to completion on each invocation and exits. It should **never** contain
scheduling logic (sleep, loops, timers). A pipeline answers: *"Given data, what do I do with it?"*

### Schedule = Operator Concern

The Clotho operator controls *when* your pipeline runs. Configure this in the Pipeline CRD:

```yaml
apiVersion: core.clotho.run/v1alpha1
kind: Pipeline
metadata:
  name: daily-idea
spec:
  gitRepository: "https://github.com/myorg/my-pipeline"
  reference: "main"
  path: "pipelines/daily-idea"
  replicas: 1

  # Schedule: how often the operator invokes this pipeline
  schedule:
    mode: "cron"                # "cron", "interval", or "trigger"
    cron: "0 9 * * *"          # Run at 9am daily
    # interval: "30s"          # Or: run every 30 seconds
    # mode: "trigger"          # Or: on-demand only (default)
```

| Mode | Behavior |
|------|----------|
| `trigger` | Pipeline runs only when invoked via API (`POST /v1/pipelines/:id/restart`). Default. |
| `interval` | Operator sends an HTTP request to the pipeline every N seconds. |
| `cron` | Operator sends an HTTP request on a cron schedule. |

The pipeline code is identical in all three modes. Only the CRD configuration changes.

---

## Telemetry

Clotho automatically emits telemetry at the SDK level:

- **Lifecycle events**: STARTUP, FINISHED, ERROR
- **Progress events**: Current item count, percentage complete
- **Boot latency**: Time from container start to first record processed

The data flow:

```
Pipeline (WASM)  --UDP:8125-->  Clotho Agent (DaemonSet)  --HTTP-->  Clotho API (SQLite)
                                     |                                    |
                                     | scrapes kubelet                    | serves UI
                                     | for CPU/memory                     |
                                     v                                    v
                               FinOps metrics                       Clotho UI
```

Telemetry is fire-and-forget (UDP). A pipeline **never crashes** because the dashboard is down.

---

## WASM Compatibility

Clotho pipelines compile to `wasm32-wasip1` and run in Spin's WASM sandbox.
Some Rust standard library features are not yet supported in WASI:

| Feature | Status | Workaround |
|---------|--------|------------|
| `std::thread::sleep` | Not supported | Use CRD `schedule.interval` instead |
| `std::net::TcpStream` | Not supported | Use `spin_sdk::http::send` for outbound HTTP |
| `std::fs` | Not supported | Use Spin KV store or outbound HTTP |
| `tokio` runtime | Not supported | SDK is async-trait based, no runtime needed |
| `std::time::Instant` | Supported | Works for timing/benchmarks |
| `serde` / `serde_json` | Supported | Full serialization support |

### The `native` Feature Flag

For pipelines that require full Rust capabilities (tokio, raw sockets, filesystem),
enable the `native` feature and deploy as a standard container instead of WASM:

```toml
[dependencies]
clotho-sdk = { version = "0.0.1-alpha.1", features = ["native"] }
```

This unlocks `IntervalSource`, `MemoryChannel`, `tokio::spawn`, and other
tokio-dependent features. The pipeline API (`Pipeline::stream`, `.map`, `.run`)
is identical in both modes.

---

## Control Plane API

The Clotho API exposes endpoints for the UI and external integrations:

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/v1/pipelines` | List all pipelines (K8s CRs + telemetry merged) |
| `GET` | `/v1/pipelines/:id` | Single pipeline detail |
| `GET` | `/v1/pipelines/:id/pods` | Live pod status |
| `GET` | `/v1/pipelines/:id/builds` | Build job history |
| `GET` | `/v1/pipelines/:id/events` | Telemetry event history |
| `POST` | `/v1/pipelines/:id/restart` | Restart pipeline pods |
| `POST` | `/v1/telemetry` | Telemetry ingestion (agent -> API) |

---

## Built-in Sources and Sinks

### Sources (WASM-safe)

- **`VecSource<T>`** - Emit items from a Vec (batch jobs, testing)

### Sources (native feature only)

- **`IntervalSource`** - Emit ticks on a timer (requires tokio)
- **`MemorySource`** - Receive from an in-memory channel (testing)

### Sinks (WASM-safe)

- **`ConsoleSink`** - Print to stdout

### Custom Sources and Sinks

Implement the `Source<T>` or `Sink<T>` traits:

```rust
use clotho::traits::{Source, Sink};
use async_trait::async_trait;
use anyhow::Result;

struct MyApiSource {
    endpoint: String,
}

#[async_trait]
impl Source<String> for MyApiSource {
    async fn next(&mut self) -> Option<Result<String>> {
        // Fetch from an API using spin_sdk::http::send
        // Return None when exhausted
        None
    }
}
```

---

## Deployment

### 1. Create your pipeline

```bash
cargo new --lib my-pipeline
cd my-pipeline
```

Set up `Cargo.toml`:

```toml
[lib]
crate-type = ["cdylib"]

[dependencies]
clotho-sdk = "0.0.1-alpha.1"
spin-sdk = "3.0"
anyhow = "1.0"
```

### 2. Write your logic in `src/lib.rs`

```rust
use clotho::Pipeline;
use clotho::builtins::{VecSource, ConsoleSink};
use anyhow::Result;

#[clotho::main]
async fn main() -> Result<()> {
    let data = vec!["record-1".into(), "record-2".into()];

    Pipeline::stream(VecSource::new(data))
        .map(|record| {
            // Your business logic here
            Ok(record)
        })
        .run(ConsoleSink::new())
        .await
}
```

### 3. Create `spin.toml`

```toml
spin_manifest_version = 2

[application]
name = "my-pipeline"
version = "0.1.0"

[[trigger.http]]
route = "/..."
component = "my-pipeline"

[component.my-pipeline]
source = "target/wasm32-wasip1/release/my_pipeline.wasm"
allowed_outbound_hosts = ["*://*:*"]

[component.my-pipeline.build]
command = "cargo build --target wasm32-wasip1 --release"
```

### 4. Deploy to Kubernetes

```yaml
apiVersion: core.clotho.run/v1alpha1
kind: Pipeline
metadata:
  name: my-pipeline
  namespace: default
spec:
  gitRepository: "https://github.com/myorg/my-pipeline"
  reference: "main"
  replicas: 1
  schedule:
    mode: "interval"
    interval: "60s"
  resources:
    requests:
      cpu: "100m"
      memory: "64Mi"
    limits:
      cpu: "500m"
      memory: "128Mi"
```

```bash
kubectl apply -f pipeline.yaml
```

The operator will: clone your repo, build the WASM binary, push it to the internal
registry, deploy it as a SpinApp, and invoke it on your configured schedule.

---

## License

Functional Source License, Version 1.1 (FSL-1.1)

Copyright (c) 2026 Brett Nesbitt

The FSL is a source-available license that automatically converts to Apache 2.0 after 2 years.
See [LICENSE](LICENSE) for full terms.

---

## Powered By

Clotho powers the [Stockseer.ai](https://stockseer.ai) financial intelligence platform. It serves all of the backend operations for moving and transforming data.