# Clotho Control Plane Helm Chart

Deploys the Clotho control plane (API + UI + Data Proxy) backed by MongoDB.

## Prerequisites

- Kubernetes 1.24+
- Helm 3.8+
- MongoDB 6.0+ (external or self-hosted)

## Quick Start

### 1. Using an existing MongoDB instance

```bash
helm install clotho-system ./deploy/helm/clotho-system \
  --namespace clotho-system --create-namespace \
  --set mongo.uri="mongodb://user:pass@mongo-host:27017"
```

### 2. Using an existing Kubernetes Secret

```bash
# Create the secret first
kubectl create secret generic my-mongo-secret \
  --from-literal=uri="mongodb://user:pass@mongo-host:27017" \
  -n clotho-system

# Install the chart
helm install clotho-system ./deploy/helm/clotho-system \
  --namespace clotho-system --create-namespace \
  --set mongo.existingSecret=my-mongo-secret
```

## Configuration

| Parameter | Description | Default |
|-----------|-------------|---------|
| `namespace` | Namespace for control plane components | `clotho-system` |
| `registry` | Container registry URL | `ghcr.io/clotho-framework` |
| `mongo.uri` | MongoDB connection string | `""` |
| `mongo.existingSecret` | Reference existing Secret name | `""` |
| `mongo.database` | Database name | `clotho` |
| `dataProxy.enabled` | Enable data proxy deployment | `true` |
| `dataProxy.image` | Data proxy image name | `clotho-data-proxy` |
| `dataProxy.tag` | Data proxy image tag | `latest` |
| `dataProxy.replicas` | Data proxy replica count | `1` |
| `dataProxy.port` | Data proxy service port | `9090` |
| `api.image` | API image name | `clotho-api` |
| `api.tag` | API image tag | `latest` |
| `api.replicas` | API replica count | `1` |
| `api.port` | API service port | `3000` |
| `api.dataProxyUrl` | URL to data proxy service | `http://clotho-data-proxy:9090` |
| `ui.enabled` | Enable UI deployment | `true` |
| `ui.image` | UI image name | `clotho-ui` |
| `ui.tag` | UI image tag | `latest` |

## Architecture

```
┌─────────────────────────────────────────────────┐
│                 Clotho Control                   │
│                                                  │
│  ┌──────────┐    ┌──────────┐    ┌────────────┐ │
│  │   API    │───▶│   Data   │───▶│  MongoDB   │ │
│  │ (Go)     │    │  Proxy   │    │ (External) │ │
│  └──────────┘    │  (Rust)  │    └────────────┘ │
│       │          └──────────┘                    │
│       │                                          │
│  ┌────┴─────┐                                    │
│  │    UI    │                                    │
│  │ (Solid)  │                                    │
│  └──────────┘                                    │
└─────────────────────────────────────────────────┘
```

## MongoDB Schema

The data proxy uses the following collections:

| Collection | Purpose |
|------------|---------|
| `pipelines` | Pipeline configurations and state |
| `telemetry_state` | Live telemetry heartbeats |
| `events` | Telemetry event log |
| `executions` | Pipeline execution records |
| `dlq_records` | Dead letter queue records |
| `build_history` | Build job history |
| `metrics_buckets` | Time-series throughput metrics |
| `lifecycle_events` | Pipeline lifecycle events |
| `api_keys` | API key authentication |
| `clusters` | Connected cluster registry |
| `command_queue` | Command queue for operators |

## Upgrading

```bash
helm upgrade clotho-system ./deploy/helm/clotho-system \
  --namespace clotho-system \
  --set mongo.uri="mongodb://user:pass@mongo-host:27017"
```
