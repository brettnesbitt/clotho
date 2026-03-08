# Counter Pipeline Example

A simple example demonstrating the Clotho SDK.

## What it does

- Creates a source with 100 numbers (1-100)
- Doubles each number using `.map()`
- Prints results to console with trace IDs
- Automatically emits telemetry events

## Run locally

```bash
cargo run
```

## Build for WASM

```bash
cargo build --target wasm32-wasip1 --release
```

## Deploy to Kubernetes

Create a Pipeline manifest:

```yaml
apiVersion: core.clotho.run/v1alpha1
kind: Pipeline
metadata:
  name: counter-example
  namespace: default
spec:
  gitRepository: "https://github.com/yourorg/clotho"
  reference: "main"
  config:
    - name: CLOTHO_PIPELINE_ID
      value: "counter-example"
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

The Clotho operator will:
1. Clone the git repository
2. Build the WASM binary from `clotho-sdk/examples/counter`
3. Deploy it as a SpinApp
4. Telemetry will flow: Pipeline → Agent → API → UI
