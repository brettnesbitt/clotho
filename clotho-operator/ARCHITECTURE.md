# Clotho Architecture

## Overview

Clotho is a Kubernetes-native platform for deploying WebAssembly pipelines. It provides a declarative API for building, deploying, and managing WASM workloads with three deployment strategies.

## Three-Tier Registry Strategy

### Tier 1: Batteries Included (Internal Registry)

**For rapid development and prototyping.**

Users provide a Git repository URL. Clotho automatically:
1. Clones the source code
2. Compiles Rust to WASM (`wasm32-wasip1`)
3. Packages as OCI artifact with Spin CLI
4. Pushes to in-cluster registry (`clotho-registry.clotho-system.svc.cluster.local:5000`)
5. Deploys as SpinApp with `containerd-shim-spin` executor

**Status:** Functionally complete. **Production deployment requires TLS configuration** (see below).

**Example:**
```yaml
apiVersion: core.clotho.run/v1alpha1
kind: Pipeline
metadata:
  name: my-pipeline
spec:
  gitRepository: https://github.com/org/repo.git
  reference: main
  path: pipelines/example  # Optional: subdirectory in monorepo
  gitCredentialsSecret: github-pat  # Optional: for private repos
  replicas: 3
```

### Tier 1.5: External Builder (Cloud Build, GitHub Actions, etc.)

**For production deployments without managing an internal registry.**

Users provide a Git repository URL, but builds are triggered on external services (Cloud Build, GitHub Actions, etc.) instead of in-cluster. The operator watches the external build and pulls the resulting image from the specified registry once complete.

**Benefits:**
- No internal registry TLS/configuration issues
- Leverages managed build services with caching and parallelization
- Push to any registry (GCP Artifact Registry, AWS ECR, Docker Hub, etc.)

**Status:** 🚧 **Planned - requires controller implementation**

**Example (GCP Cloud Build):**
```yaml
apiVersion: core.clotho.run/v1alpha1
kind: Pipeline
metadata:
  name: my-pipeline
spec:
  gitRepository: https://github.com/org/repo.git
  reference: main
  path: pipelines/example
  build:
    builder: cloudbuild
    registry: us-central1-docker.pkg.dev/my-project/clotho-pipelines
    credentialsSecret: gcr-credentials        # For pulling the image
    serviceAccountSecret: cloudbuild-sa-key   # For triggering Cloud Build
    buildArgs:
      _RUST_VERSION: "1.75"
  replicas: 3
```

**Required Secrets:**

```yaml
# For pulling from private registry (GCR/Artifact Registry)
apiVersion: v1
kind: Secret
metadata:
  name: gcr-credentials
type: kubernetes.io/dockerconfigjson
data:
  .dockerconfigjson: <base64-encoded-docker-config>

# For triggering Cloud Build
apiVersion: v1
kind: Secret
metadata:
  name: cloudbuild-sa-key
type: Opaque
data:
  token: <base64-encoded-gcp-service-account-json>
```

### Tier 2: BYOR (Bring Your Own Registry)

**For production deployments with existing CI/CD.**

Users provide a pre-built WASM OCI image. Clotho skips the builder and directly deploys the SpinApp.

**Status:** ✅ **Production-ready and validated.**

**Example:**
```yaml
apiVersion: core.clotho.run/v1alpha1
kind: Pipeline
metadata:
  name: my-pipeline
spec:
  image: us-central1-docker.pkg.dev/my-project/clotho/my-pipeline:v1.2.3
  imagePullSecrets:  # Optional: for private registries
    - name: gcr-credentials
  replicas: 3
```

## Architecture Components

### 1. Clotho Operator

**Location:** `clotho-operator/`

Kubernetes controller that reconciles `Pipeline` CRDs into `SpinApp` resources.

**Key Logic:**
- **Tier 1:** If `spec.gitRepository` is set and `spec.build` is empty → create in-cluster builder Job → wait for completion → update `spec.image` → create SpinApp
- **Tier 1.5:** If `spec.build` is set → trigger external build → poll/wait for completion → update `spec.image` → create SpinApp
- **Tier 2:** If `spec.image` is set and `spec.gitRepository`/`spec.build` are empty → skip builder → create SpinApp directly
- **Validation:** Checks that referenced secrets exist before deployment
- **Owner References:** Automatic garbage collection of SpinApps and Jobs

**RBAC:**
- Manages: `Pipeline`, `SpinApp`, `Job`, `Secret` (read-only)

### 2. Clotho Builder

**Location:** `clotho-operator/builder/`

Docker image with Rust toolchain, Spin CLI, and build script.

**Capabilities:**
- Clones Git repositories (public or private with PAT)
- Compiles Rust to `wasm32-wasip1`
- Packages with `spin registry push`
- Supports monorepos via `path` parameter

**Authentication:**
- Git: Uses `GIT_TOKEN` env var (injected from `gitCredentialsSecret`)
- Registry: Pushes to internal registry with `--insecure` flag

### 3. Internal Registry

**Location:** `clotho-operator/config/registry/registry.yaml`

Standard Docker Registry v2 deployed in `clotho-system` namespace.

**Configuration:**
- ClusterIP service on port 5000
- No authentication (cluster-internal only)
- Ephemeral storage (emptyDir)

### 4. SpinKube Integration

Clotho creates `SpinApp` CRDs managed by the SpinKube operator.

**Executor:** `containerd-shim-spin` (native Kubelet integration)

**Why not the `spin` executor?**
- The `spin` executor has bugs in v0.6.1 (nil pointer dereferences, missing deployments)
- `containerd-shim-spin` is the architecturally correct choice for production
- Provides native Kubelet integration, proper `imagePullSecrets` support, and better resource management

## Git Authentication

### Public Repositories

No configuration needed. The builder clones directly.

### Private Repositories

Create a Kubernetes Secret with a GitHub Personal Access Token:

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: github-pat
type: Opaque
stringData:
  token: "ghp_abc123..."
```

Reference it in the Pipeline:

```yaml
spec:
  gitRepository: https://github.com/org/private-repo.git
  gitCredentialsSecret: github-pat
```

The builder automatically rewrites the URL to `https://x-access-token:TOKEN@github.com/org/private-repo.git`.

## Tier 1 Production Deployment Requirements

### Current Limitation

The internal registry uses HTTP (no TLS). GKE's containerd defaults to HTTPS and does not trust insecure registries.

**Symptoms:**
- Pods stuck in `ImagePullBackOff`
- Error: `failed to resolve reference: failed to do request: Head "https://clotho-registry...": dial tcp: lookup ... no such host`

### Production Solutions

**Option A: Add TLS to Internal Registry (Recommended)**

1. Deploy cert-manager
2. Create Certificate for `clotho-registry.clotho-system.svc.cluster.local`
3. Configure registry to use TLS
4. Ensure GKE nodes trust the CA (use public DNS + Let's Encrypt, or configure custom CA trust)

**Option B: Configure Containerd for Insecure Registries**

Edit `/etc/containerd/config.toml` on each node:

```toml
[plugins."io.containerd.grpc.v1.cri".registry.mirrors."clotho-registry.clotho-system.svc.cluster.local:5000"]
  endpoint = ["http://clotho-registry.clotho-system.svc.cluster.local:5000"]

[plugins."io.containerd.grpc.v1.cri".registry.configs."clotho-registry.clotho-system.svc.cluster.local:5000".tls]
  insecure_skip_verify = true
```

**Warning:** This requires custom node startup scripts and is not recommended for managed Kubernetes (GKE, EKS, AKS) where nodes are ephemeral.

**Option C: Use Tier 2 (BYOR) for Production**

Skip the internal registry entirely. Build images in your CI/CD pipeline and push to GCP Artifact Registry, AWS ECR, or Docker Hub.

## Runtime Requirements

### Kubernetes Cluster

- **Version:** 1.28+ (tested on GKE 1.34)
- **Node OS:** Ubuntu with containerd (Container-Optimized OS does not support kwasm installation)
- **RuntimeClass:** `wasmtime-spin-v2` (installed via kwasm-operator)

### Installing containerd-shim-spin

```bash
# Install kwasm-operator
helm repo add kwasm http://kwasm.sh/kwasm-operator/
helm install kwasm-operator kwasm/kwasm-operator --namespace kwasm --create-namespace

# Provision nodes with the shim
kubectl annotate node <node-name> kwasm.sh/kwasm-node=true
```

**GKE-specific:** Use Ubuntu node pools, not Container-Optimized OS.

```bash
gcloud container node-pools create wasm-pool \
  --cluster=clotho-cluster \
  --machine-type=e2-standard-4 \
  --image-type=UBUNTU_CONTAINERD \
  --num-nodes=2
```

## Deployment

### Prerequisites

1. Kubernetes cluster with `containerd-shim-spin` installed
2. SpinKube operator installed
3. kubectl configured

### Install Clotho

```bash
# Deploy internal registry
kubectl apply -f config/registry/registry.yaml

# Deploy operator
export IMG=ghcr.io/clotho-framework/clotho-operator:latest
make deploy IMG=$IMG
```

### Create a Pipeline (Tier 1)

```bash
kubectl apply -f - <<EOF
apiVersion: core.clotho.run/v1alpha1
kind: Pipeline
metadata:
  name: counter-example
spec:
  gitRepository: https://github.com/brettnesbitt/clotho.git
  reference: main
  path: clotho-sdk/examples/counter
  replicas: 2
EOF
```

### Create a Pipeline (Tier 2)

```bash
kubectl apply -f - <<EOF
apiVersion: core.clotho.run/v1alpha1
kind: Pipeline
metadata:
  name: my-app
spec:
  image: ghcr.io/spinkube/containerd-shim-spin/examples/spin-rust-hello:v0.13.0
  replicas: 3
EOF
```

## Monitoring

### Check Pipeline Status

```bash
kubectl get pipeline
kubectl describe pipeline <name>
```

### Check Builder Job

```bash
kubectl get jobs -l managed-by=clotho
kubectl logs job/builder-<pipeline-name>
```

### Check SpinApp

```bash
kubectl get spinapp
kubectl get pods -l core.spinoperator.dev/app-name=<pipeline-name>
```

## Troubleshooting

### Builder Job Fails

**Check logs:**
```bash
kubectl logs job/builder-<pipeline-name>
```

**Common issues:**
- Git authentication failure → Check `gitCredentialsSecret`
- Compilation error → Check Rust code and `spin.toml`
- Registry push failure → Check internal registry is running

### Pods Not Starting

**Check SpinApp status:**
```bash
kubectl describe spinapp <name>
```

**Common issues:**
- `ImagePullBackOff` → Registry authentication or TLS issue (see Tier 1 Production Requirements)
- `no runtime for "spin" is configured` → Node missing `containerd-shim-spin` (check kwasm installation)
- `CrashLoopBackOff` → Check pod logs for WASM runtime errors

### SpinApp Shows Ready but No Pods

This is a known bug in spin-operator v0.6.1. The operator creates Deployments but not ReplicaSets.

**Workaround:** Manually create a test pod to verify the image works, then wait for spin-operator updates.

## Next Steps

1. **Control Plane UI:** SolidJS dashboard for visualizing pipelines, logs, and metrics
2. **Observability:** StdoutSink integration for structured logging
3. **Autoscaling:** HPA support based on HTTP request rate
4. **Multi-tenancy:** Namespace isolation and RBAC policies
