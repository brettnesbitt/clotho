# Clotho

**Kubernetes-native WebAssembly pipeline orchestration.**

Clotho is a Kubernetes operator that provides a declarative API for deploying WebAssembly workloads. It supports two deployment strategies: automatic builds from Git repositories (Tier 1) or pre-built images from external registries (Tier 2).

## Features

- **Declarative API**: Define pipelines as Kubernetes custom resources
- **Two-tier registry strategy**: Choose between automatic builds or bring-your-own-image
- **Git integration**: Automatic cloning, compilation, and deployment from source
- **Private repository support**: Authenticate with GitHub PATs or similar tokens
- **Native WASM execution**: Uses `containerd-shim-spin` for production-grade runtime
- **Automatic garbage collection**: Owner references ensure clean resource lifecycle

## Quick Start

### Prerequisites

1. Kubernetes cluster (1.28+) with `containerd-shim-spin` runtime installed
2. SpinKube operator installed
3. kubectl configured

### Install Clotho

```bash
# Deploy internal registry
kubectl apply -f config/registry/registry.yaml

# Deploy operator
kubectl apply -f https://raw.githubusercontent.com/brettimus/clotho/main/clotho-operator/dist/install.yaml
```

### Deploy Your First Pipeline

**Option 1: From Git (Tier 1)**

```bash
kubectl apply -f - <<EOF
apiVersion: core.clotho.run/v1alpha1
kind: Pipeline
metadata:
  name: my-pipeline
spec:
  gitRepository: https://github.com/brettimus/clotho.git
  reference: main
  path: clotho-sdk/examples/counter
  replicas: 2
EOF
```

**Option 2: Pre-built Image (Tier 2)**

```bash
kubectl apply -f - <<EOF
apiVersion: core.clotho.run/v1alpha1
kind: Pipeline
metadata:
  name: my-pipeline
spec:
  image: ghcr.io/spinkube/containerd-shim-spin/examples/spin-rust-hello:v0.13.0
  replicas: 2
EOF
```

### Check Status

```bash
# View pipeline status
kubectl get pipeline

# Check builder job (Tier 1 only)
kubectl get jobs -l managed-by=clotho
kubectl logs job/builder-my-pipeline

# Check running pods
kubectl get pods -l core.spinoperator.dev/app-name=my-pipeline
```

## Architecture

See [ARCHITECTURE.md](./ARCHITECTURE.md) for detailed architecture documentation.

### Key Components

- **Clotho Operator**: Reconciles `Pipeline` CRDs into `SpinApp` resources
- **Clotho Builder**: Docker image with Rust toolchain and Spin CLI for compiling from source
- **Internal Registry**: In-cluster OCI registry for Tier 1 deployments
- **SpinKube Integration**: Creates `SpinApp` CRDs with `containerd-shim-spin` executor

### Two-Tier Strategy

**Tier 1 (Batteries Included):** Provide a Git URL → Clotho builds and deploys automatically  
**Tier 2 (BYOR):** Provide a pre-built image → Clotho deploys directly

## Configuration

### Private Git Repositories

Create a secret with your GitHub PAT:

```bash
kubectl create secret generic github-pat \
  --from-literal=token=ghp_your_token_here
```

Reference it in your Pipeline:

```yaml
spec:
  gitRepository: https://github.com/org/private-repo.git
  gitCredentialsSecret: github-pat
```

### Private Container Registries

For Tier 2 deployments with private registries:

```bash
kubectl create secret docker-registry gcr-credentials \
  --docker-server=us-central1-docker.pkg.dev \
  --docker-username=_json_key \
  --docker-password="$(cat key.json)"
```

Reference it in your Pipeline:

```yaml
spec:
  image: us-central1-docker.pkg.dev/project/repo/image:tag
  imagePullSecrets:
    - name: gcr-credentials
```

### Resource Limits

```yaml
spec:
  resources:
    requests:
      cpu: 100m
      memory: 64Mi
    limits:
      cpu: 500m
      memory: 128Mi
```

### Environment Variables

```yaml
spec:
  config:
    - name: DATABASE_URL
      value: "postgres://..."
    - name: API_KEY
      valueFrom:
        secretKeyRef:
          name: api-credentials
          key: key
```

## Development

### Build and Deploy Locally

```bash
# Build operator image
make docker-build IMG=clotho-operator:dev

# Build builder image
cd builder && docker build -t clotho-builder:dev .

# Deploy to cluster
make deploy IMG=clotho-operator:dev
```

### Run Tests

```bash
make test
```

### Regenerate CRDs

```bash
make manifests generate
```

## Production Deployment

### Tier 1 Requirements

The internal registry currently uses HTTP (no TLS). For production:

1. **Option A**: Add TLS to the internal registry (recommended)
2. **Option B**: Configure containerd to allow insecure registries (not recommended for managed K8s)
3. **Option C**: Use Tier 2 (BYOR) exclusively

See [ARCHITECTURE.md](./ARCHITECTURE.md#tier-1-production-deployment-requirements) for details.

### Tier 2 (Production-Ready)

Tier 2 is production-ready and validated. Build images in your CI/CD pipeline and push to any registry:

- GCP Artifact Registry
- AWS ECR
- Azure Container Registry
- Docker Hub
- GitHub Container Registry

## Troubleshooting

### Builder Job Fails

```bash
kubectl logs job/builder-<pipeline-name>
```

Common issues:
- Git authentication failure → Check `gitCredentialsSecret`
- Compilation error → Check Rust code and `spin.toml`

### Pods Not Starting

```bash
kubectl describe spinapp <name>
kubectl describe pod <pod-name>
```

Common issues:
- `ImagePullBackOff` → Registry authentication or TLS issue
- `no runtime for "spin" is configured` → Missing `containerd-shim-spin` on nodes
- `CrashLoopBackOff` → Check pod logs for WASM runtime errors

## Contributing

Contributions welcome! Please open an issue or PR.

## License

MIT