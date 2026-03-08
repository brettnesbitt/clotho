Markdown
# Clotho SDK: Serverless Data Streaming for Rust

**Clotho** is a framework for building high-performance, serverless data pipelines in Rust. It compiles your logic into lightweight WebAssembly (Wasm) binaries that run on Kubernetes via [SpinKube](https://spinkube.dev).

**Why Clotho?**
* **Scale to Zero:** Unlike Flink or Spark, Clotho pipelines are event-driven. They consume 0 CPU when idle.
* **Invisible Lineage:** Distributed Trace IDs are automatically propagated through every `map` step. You write business logic; we handle the provenance.
* **Type Safety:** Input and Output schemas are enforced by the Rust compiler.
* **Built-in Telemetry:** Automatic progress tracking and lifecycle events sent to the Clotho control plane.

---

## 📦 Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
clotho-sdk = { path = "../clotho-sdk" }  # Or from crates.io when published
serde = { version = "1.0", features = ["derive"] }
serde_json = "1.0"
tokio = { version = "1.0", features = ["sync", "macros", "io-util", "rt", "time"] }
anyhow = "1.0"
async-trait = "0.1"
```

---

## 🚀 Quick Start

### Example 1: Simple Counter Pipeline

Create a pipeline that processes 100 items and emits telemetry:

```rust
use clotho_sdk::prelude::*;
use clotho_sdk::builtins::{VecSource, ConsoleSink};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    // Create a source with test data
    let items = (1..=100).collect::<Vec<u64>>();
    let source = VecSource::new(items);
    
    // Build and run the pipeline
    Pipeline::read(source)
        .map(|num| Ok(num * 2))  // Double each number
        .run(ConsoleSink::new())
        .await?;
    
    Ok(())
}
```

### Example 2: Interval-Based Pipeline

Create a pipeline that runs on a schedule:

```rust
use clotho_sdk::prelude::*;
use clotho_sdk::builtins::{IntervalSource, ConsoleSink};
use std::time::Duration;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    Pipeline::read(IntervalSource::new(Duration::from_secs(60)))
        .map(|tick| {
            Ok(format!("Heartbeat #{}", tick))
        })
        .run(ConsoleSink::new())
        .await?;
    
    Ok(())
}
```

---

## 🧠 Core Concepts

### 1. The Context Envelope (Invisible Tracing)

Clotho wraps every data record in a `Context<T>` struct containing:

- **Trace ID**: The global workflow ID
- **Span ID**: The current step ID  
- **Parents**: A history of parent steps (for lineage tracking)
- **Metadata**: Custom key-value pairs

**You don't manage this manually.** The SDK:
- Unwraps the context before your closure runs
- Re-wraps your result after your closure returns
- Links the new context to the old one automatically

### 2. Resilience (Dead Letter Queues)

Never crash on bad data. Configure a DLQ sink to catch errors:

```rust
use clotho_sdk::prelude::*;
use clotho_sdk::builtins::{VecSource, ConsoleSink, DevNullSink};

Pipeline::read(VecSource::new(vec!["valid", "invalid"]))
    .with_dlq(DevNullSink)  // Errors go here instead of crashing
    .map(|data| {
        if data == "invalid" {
            anyhow::bail!("Bad data!")
        }
        Ok(data.to_uppercase())
    })
    .run(ConsoleSink::new())
    .await?;
```
---

## 📚 Built-in Sources and Sinks

### Sources

- **`VecSource`** - Emit items from a Vec (for testing/batch jobs)
- **`IntervalSource`** - Emit ticks on a schedule (for cron-style pipelines)
- **`MemorySource`** - Receive from an in-memory channel (for testing)
- **`MockByteSource`** - Emit raw bytes (for testing deserialization)

### Sinks

- **`ConsoleSink`** - Print to stdout with trace IDs
- **`DevNullSink`** - Discard data (for performance testing)
- **`MemorySink`** - Send to an in-memory channel (for testing)

### Custom Sources and Sinks

Implement the `Source<T>` or `Sink<T>` traits:

```rust
use clotho_sdk::prelude::*;
use async_trait::async_trait;

struct MySource;

#[async_trait]
impl Source<String> for MySource {
    async fn next(&mut self) -> Option<Result<Context<String>>> {
        // Your logic here
        Some(Ok(Context::root("data".to_string(), "my_source")))
    }
}

struct MySink;

#[async_trait]
impl Sink<String> for MySink {
    async fn write(&mut self, ctx: Context<String>) -> Result<()> {
        // Your logic here
        println!("Received: {}", ctx.data);
        Ok(())
    }
}
```
---

## 🛠 Testing (No Docker Required)

Clotho provides an in-memory bus (`memory_channel`) for testing multi-stage pipelines:

```rust
use clotho_sdk::prelude::*;
use clotho_sdk::builtins::{VecSource, memory_channel};
use std::sync::Arc;
use tokio::sync::Mutex;

#[tokio::test]
async fn test_pipeline_chain() -> Result<()> {
    let (topic_sink, topic_source) = memory_channel::<String>(10);

    // Pipeline A: Producer
    tokio::spawn(async move {
        Pipeline::read(VecSource::new(vec!["hello", "world"]))
            .map(|s| Ok(s.to_uppercase()))
            .run(topic_sink)
            .await
            .unwrap();
    });

    // Pipeline B: Consumer
    let results = Arc::new(Mutex::new(Vec::new()));
    let results_clone = results.clone();
    
    struct CaptureSink {
        vec: Arc<Mutex<Vec<String>>>,
    }
    
    #[async_trait::async_trait]
    impl Sink<String> for CaptureSink {
        async fn write(&mut self, ctx: Context<String>) -> Result<()> {
            self.vec.lock().await.push(ctx.data);
            Ok(())
        }
    }
    
    Pipeline::read(topic_source)
        .run(CaptureSink { vec: results_clone })
        .await?;
        
    let final_data = results.lock().await;
    assert_eq!(final_data[0], "HELLO");
    assert_eq!(final_data[1], "WORLD");
    
    Ok(())
}
```
---

## 🚢 Deployment

### 1. Build for WebAssembly

```bash
# Add WASM target (first time only)
rustup target add wasm32-wasip1

# Build your pipeline
cargo build --target wasm32-wasip1 --release
```

### 2. Create a Spin manifest

Create `spin.toml` in your project:

```toml
spin_manifest_version = 2

[application]
name = "my-pipeline"
version = "0.1.0"

[[trigger.http]]
route = "/..."
component = "pipeline"

[component.pipeline]
source = "target/wasm32-wasip1/release/my-pipeline.wasm"
allowed_outbound_hosts = ["*://*:*"]
```

### 3. Deploy to Kubernetes

Create a Pipeline Custom Resource:

```yaml
apiVersion: core.clotho.run/v1alpha1
kind: Pipeline
metadata:
  name: my-pipeline
  namespace: default
spec:
  gitRepository: "https://github.com/myorg/my-pipeline"
  reference: "main"
  config:
    - name: CLOTHO_PIPELINE_ID
      value: "my-pipeline"
  resources:
    requests:
      cpu: "100m"
      memory: "64Mi"
    limits:
      cpu: "500m"
      memory: "128Mi"
  replicas: 1
```

Apply it:

```bash
kubectl apply -f pipeline.yaml
```

---

## 📊 Telemetry

Clotho automatically emits telemetry events:

- **Lifecycle events**: START, STOP, HEARTBEAT
- **Progress events**: Current item count, percentage complete

Events are sent via UDP to the Clotho agent (DaemonSet) which forwards them to the control plane API. View real-time pipeline status in the Clotho UI.

---

## 📝 License

MIT