# Daily Idea Pipeline

Fetches the latest daily trading idea from Stockseer API and outputs it to stdout.

## What It Does

1. Makes HTTP request to `https://stockseer.ai/api/idea-of-the-day`
2. Parses the JSON response
3. Formats and outputs the idea details (symbol, entry, stop, target, rationale)

## Deploy to Clotho

```bash
kubectl apply -f - <<EOF
apiVersion: core.clotho.run/v1alpha1
kind: Pipeline
metadata:
  name: daily-idea
spec:
  gitRepository: https://github.com/brettimus/clotho.git
  reference: main
  path: clotho-sdk/examples/daily-idea
  replicas: 1
EOF
```

## Check Logs

```bash
# Wait for build to complete
kubectl get jobs -l managed-by=clotho

# Check pipeline status
kubectl get pipeline daily-idea

# View the daily idea output
kubectl logs -l core.spinoperator.dev/app-name=daily-idea
```

## Local Testing

```bash
# Build
cargo build --target wasm32-wasip1 --release

# Run with Spin
spin build
spin up

# Trigger the pipeline
curl http://localhost:3000
```
