# Clotho: Agent Instruction Baseline

This document serves as a baseline set of instructions and patterns for working with the **Clotho** framework.

> [!NOTE]
> Clotho is a Kubernetes data integration platform. Its UI and API components are proprietary Intellectual Property (IP), while the remainder of the framework (operator, SDK, plugins, etc.) is designed as open source.

## 1. Project Topology (Folder Breakdown)
The repository is split into distinct components representing the Control Plane, Data Plane, and SDK:

- **`.clotho-api`**: The proprietary Control Plane API backend. It handles system state, stores configuration, and serves the dashboard UI.
- **`.clotho-ui`**: The proprietary web dashboard frontend for observing telemetry, monitoring pipelines, and executing triggered runs.
- **`clotho-sdk`**: The open source Rust framework providing the core engine, traits (`Source`, `Sink`), and pipeline builders that developers use to write business logic.
- **`clotho-macros`**: A procedural macros crate containing `#[clotho::main]` to auto-generate the Spin HTTP component entrypoint and error boundaries.
- **`clotho-operator`**: The Kubernetes operator that reconciles `Pipeline` CRDs. It schedules, builds (WASM logic), and deploys (SpinKube or Native containers).
- **`clotho-agent`**: A Kubernetes DaemonSet that listens for UDP telemetry from pipelines and scrapes container CPU/memory metrics to forward to the API.
- **`clotho-data-proxy`**: A middle tier service designed to proxy data flows or interface between external inputs and pipelines.
- **`clotho-worker`**: The base worker/template application used by the builder when provisioning WASM instances.
- **`deploy`**: Kubernetes manifests and Helm charts used for deploying the Clotho platform into a cluster.

## 2. Core Concepts
- **Pure Business Logic:** Pipelines are strictly event-driven and execute from start to finish on a given input. They should **never** contain scheduling logic like sleeps, loops, or timers.
- **Pattern:** Every pipeline follows the **Source -> Transform -> Sink** structure.
- **Scale to Zero:** Pipelines consume no CPU when idle.
- **Components:**
  - `Pipeline::stream(source).map(transform).run(sink).await?`
  - Built-in sources: `VecSource<T>`
  - Built-in sinks: `ConsoleSink`
  - A pipeline uses the `#[clotho::main]` macro to automatically generate the Spin HTTP component entrypoint, error handling, and telemetry.

## 3. Pipeline CRD Options
The operator manages scheduling and deployment via the `Pipeline` CRD, which exposes significant configuration possibilities.

### Execution Model
- **`runtime`**: Target execution environment. `wasm` (runs as a SpinApp) or `native` (runs as a standard Kubernetes Deployment). Default is `wasm`.
- **`mode`**: Execution tracking model.
  - `stream` (default): Continuous processing, time-bucket aggregation.
  - `once`: Processes a single request payload (webhook-style).
  - `batch`: Finite record set execution in one run.

### Image & Build Sources
- **`gitRepository`**, **`reference`**, **`path`**: Instructs the built-in builder to clone, build, and push to an internal registry.
- **`image`**: Bring-your-own-registry (BYOR). If defined without git config, builder steps are skipped.
- **`imagePullSecrets`**: Provide private registry credentials.
- **`build`**: External build configuration (Tier 1.5). Allows triggering builds on Cloud Build, GitHub Actions, GitLab CI, Tekton, etc.

### Workload Management
- **`replicas`**: Number of replicas for scaling. Default is 1.
- **`resources`**: Compute restrictions (`requests`/`limits` for cpu and memory).
- **`config`**: Environment variable injections supporting literal values (`value`) or Kubernetes Secret mappings (`valueFrom.secretKeyRef`).
- **`policy`**: Execution guardrails including `timeoutSeconds` (max worker lifespan) and `maxRetries` upon failure.

### Schedulers
A pipeline execution can be controlled under the `schedule` spec:
- **`mode`**: `trigger` (API event-driven, default), `interval` (every N seconds), or `cron`.
- `interval`: E.g., `30s`, `1m`.
- `cron`: Standard cron expressions (e.g., `0 9 * * *`).

### DAG Stages & Topologies
Pipelines can be configured as a Directed Acyclic Graph (DAG) for multi-stage topology involving inter-stage message buses:
- **`stages`**: A list of pipeline stages mapping to specific `entrypoints` along with specific `dependsOn`, `replicas`, `resources`, and `config` properties.
- **`messageBus`**: The transport mechanism for the DAG stages, configured as `type` (`nats-jetstream`, `kafka`, `redis-streams`).

## 4. WASM Limitations & Workarounds
Because pipelines run in Spin's WASM sandbox (`wasm32-wasip1`), some standard Rust features are unavailable:
- **`std::thread::sleep`**: **NOT SUPPORTED**. Use CRD `schedule.interval` instead.
- **`std::net::TcpStream`**: **NOT SUPPORTED**. Use `spin_sdk::http::send` for outbound HTTP.
- **`std::fs`**: **NOT SUPPORTED**. Use Spin KV store or outbound HTTP.
- **`tokio` runtime**: **NOT SUPPORTED**. The Clotho SDK is async-trait based. No runtime is needed.

*Note: If full Rust capabilities (e.g., `tokio`, raw sockets, filesystem) are required, the `runtime: native` option alongside the `native` feature flag can be utilized.*

## 5. Development Best Practices
- **Type Safety**: Leverage Rust's type system to strictly enforce input and output schemas.
- **Custom Sources/Sinks**: Implement standard `clotho::traits::Source<T>` and `clotho::traits::Sink<T>`.
- **Telemetry**: Don't manually add lifecycle metrics inside pipelines; Clotho SDK implicitly handles tracking.
- **CRD Management**: Instead of creating infinite loops within the Rust source, configure continuous operations via the Kubernetes CRD's `.spec.schedule` (`cron` or `interval`).
