use axum::{
    extract::State,
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use bson::Document;
use dashmap::DashMap;
use mongodb::{options::ClientOptions, Client};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::{error, info, warn};

// ═══════════════════════════════════════════════════════════════════════════════
// Clotho Data Proxy
//
// A lightweight connection-pooling proxy that sits between WASM Spin jobs and
// MongoDB. Deployed as a DaemonSet (one per node) or a ClusterIP Service.
//
// Endpoints:
//   POST /v1/mongo/insert      — Single document insert (dedup-aware)
//   POST /v1/mongo/insert-many — Bulk insert with ordered:false (dedup-aware)
//   GET  /healthz              — Liveness probe
// ═══════════════════════════════════════════════════════════════════════════════

#[derive(Clone)]
struct AppState {
    clients: Arc<DashMap<String, Client>>,
}

impl AppState {
    fn new() -> Self {
        Self {
            clients: Arc::new(DashMap::new()),
        }
    }

    async fn get_client(&self, uri: &str) -> anyhow::Result<Client> {
        if let Some(client) = self.clients.get(uri) {
            return Ok(client.value().clone());
        }

        info!(uri_prefix = &uri[..uri.len().min(30)], "Creating new MongoDB connection pool");
        let mut opts = ClientOptions::parse(uri).await?;
        opts.app_name = Some("clotho-data-proxy".to_string());
        opts.min_pool_size = Some(2);
        opts.max_pool_size = Some(20);
        let client = Client::with_options(opts)?;

        self.clients.insert(uri.to_string(), client.clone());
        Ok(client)
    }
}

// ── Request / Response types ─────────────────────────────────────────────────

#[derive(Deserialize)]
struct InsertOneRequest {
    uri: String,
    database: String,
    collection: String,
    document: serde_json::Value,
}

#[derive(Deserialize)]
struct InsertManyRequest {
    uri: String,
    database: String,
    collection: String,
    documents: Vec<serde_json::Value>,
    #[serde(default)]
    ordered: bool,
}

#[derive(Serialize)]
struct ProxyResponse {
    ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    inserted_count: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    duplicate_count: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

// ── Handlers ─────────────────────────────────────────────────────────────────

async fn insert_one(
    State(state): State<AppState>,
    Json(req): Json<InsertOneRequest>,
) -> impl IntoResponse {
    let client = match state.get_client(&req.uri).await {
        Ok(c) => c,
        Err(e) => {
            error!(error = %e, "Failed to connect to MongoDB");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ProxyResponse {
                    ok: false,
                    inserted_count: None,
                    duplicate_count: None,
                    error: Some(format!("Connection error: {}", e)),
                }),
            );
        }
    };

    let collection = client
        .database(&req.database)
        .collection::<Document>(&req.collection);

    let doc = match bson::to_document(&req.document) {
        Ok(d) => d,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ProxyResponse {
                    ok: false,
                    inserted_count: None,
                    duplicate_count: None,
                    error: Some(format!("BSON conversion error: {}", e)),
                }),
            );
        }
    };

    match collection.insert_one(doc, None).await {
        Ok(_) => (
            StatusCode::OK,
            Json(ProxyResponse {
                ok: true,
                inserted_count: Some(1),
                duplicate_count: Some(0),
                error: None,
            }),
        ),
        Err(e) => {
            // Duplicate key error = dedup success, not a failure
            if let mongodb::error::ErrorKind::Write(
                mongodb::error::WriteFailure::WriteError(ref we),
            ) = *e.kind
            {
                if we.code == 11000 {
                    return (
                        StatusCode::OK,
                        Json(ProxyResponse {
                            ok: true,
                            inserted_count: Some(0),
                            duplicate_count: Some(1),
                            error: None,
                        }),
                    );
                }
            }
            error!(error = %e, "MongoDB insert_one failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ProxyResponse {
                    ok: false,
                    inserted_count: None,
                    duplicate_count: None,
                    error: Some(format!("Insert error: {}", e)),
                }),
            )
        }
    }
}

async fn insert_many(
    State(state): State<AppState>,
    Json(req): Json<InsertManyRequest>,
) -> impl IntoResponse {
    if req.documents.is_empty() {
        return (
            StatusCode::OK,
            Json(ProxyResponse {
                ok: true,
                inserted_count: Some(0),
                duplicate_count: Some(0),
                error: None,
            }),
        );
    }

    let client = match state.get_client(&req.uri).await {
        Ok(c) => c,
        Err(e) => {
            error!(error = %e, "Failed to connect to MongoDB");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ProxyResponse {
                    ok: false,
                    inserted_count: None,
                    duplicate_count: None,
                    error: Some(format!("Connection error: {}", e)),
                }),
            );
        }
    };

    let collection = client
        .database(&req.database)
        .collection::<Document>(&req.collection);

    let total = req.documents.len();
    let docs: Vec<Document> = req
        .documents
        .iter()
        .filter_map(|v| bson::to_document(v).ok())
        .collect();

    if docs.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(ProxyResponse {
                ok: false,
                inserted_count: None,
                duplicate_count: None,
                error: Some("All documents failed BSON conversion".into()),
            }),
        );
    }

    let opts = mongodb::options::InsertManyOptions::builder()
        .ordered(req.ordered)
        .build();

    match collection.insert_many(docs, opts).await {
        Ok(result) => {
            let inserted = result.inserted_ids.len();
            (
                StatusCode::OK,
                Json(ProxyResponse {
                    ok: true,
                    inserted_count: Some(inserted),
                    duplicate_count: Some(total - inserted),
                    error: None,
                }),
            )
        }
        Err(e) => {
            // With ordered:false, partial success is possible.
            // If all write errors are duplicate key (11000), treat as success.
            if let mongodb::error::ErrorKind::BulkWrite(ref bwe) = *e.kind {
                if let Some(ref write_errors) = bwe.write_errors {
                    let dup_count = write_errors.iter().filter(|we| we.code == 11000).count();
                    if dup_count == write_errors.len() {
                        // All errors were duplicates — partial success
                        let inserted = total - dup_count;
                        return (
                            StatusCode::OK,
                            Json(ProxyResponse {
                                ok: true,
                                inserted_count: Some(inserted),
                                duplicate_count: Some(dup_count),
                                error: None,
                            }),
                        );
                    } else {
                        // Mix of duplicates and real errors
                        let real_errors: Vec<String> = write_errors
                            .iter()
                            .filter(|we| we.code != 11000)
                            .map(|we| format!("code {}: {}", we.code, we.message))
                            .collect();
                        warn!(dup_count, real_error_count = real_errors.len(), "Partial bulk write failure");
                        return (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            Json(ProxyResponse {
                                ok: false,
                                inserted_count: Some(total - write_errors.len()),
                                duplicate_count: Some(dup_count),
                                error: Some(format!("Partial failure: {}", real_errors.join("; "))),
                            }),
                        );
                    }
                }
            }

            error!(error = %e, "MongoDB insert_many failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ProxyResponse {
                    ok: false,
                    inserted_count: None,
                    duplicate_count: None,
                    error: Some(format!("Bulk insert error: {}", e)),
                }),
            )
        }
    }
}

async fn healthz() -> &'static str {
    "ok"
}

// ── Main ─────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "clotho_data_proxy=info".into()),
        )
        .init();

    let port = std::env::var("PORT").unwrap_or_else(|_| "9090".into());
    let addr = format!("0.0.0.0:{}", port);

    let state = AppState::new();

    let app = Router::new()
        .route("/v1/mongo/insert", post(insert_one))
        .route("/v1/mongo/insert-many", post(insert_many))
        .route("/healthz", get(healthz))
        .with_state(state);

    info!(addr = %addr, "Clotho Data Proxy starting");

    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
