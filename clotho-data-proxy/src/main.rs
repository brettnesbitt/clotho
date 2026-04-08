use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{delete, get, post},
    Json, Router,
};
use bson::{doc, Document};
use futures::stream::TryStreamExt;
use mongodb::{options::UpdateOptions, Client, Collection};
use serde::{Deserialize, Serialize};
use tracing::{error, info, warn};

// ═══════════════════════════════════════════════════════════════════════════════
// Clotho Data Proxy — MongoDB CRUD layer (mongodb 2.8)
// ═══════════════════════════════════════════════════════════════════════════════

#[derive(Clone)]
struct AppState {
    client: Client,
    database: String,
}

impl AppState {
    fn collection(&self, name: &str) -> Collection<Document> {
        self.client.database(&self.database).collection::<Document>(name)
    }
}

// ── Request / Response types ─────────────────────────────────────────────────

#[derive(Deserialize)]
struct QueryParams {
    #[serde(default)]
    filter: Option<String>,
    #[serde(default)]
    sort: Option<String>,
    #[serde(default)]
    limit: Option<i64>,
    #[serde(default)]
    skip: Option<i64>,
}

#[derive(Deserialize)]
struct BulkInsertRequest {
    documents: Vec<serde_json::Value>,
}

#[derive(Deserialize)]
struct UpdateManyRequest {
    filter: serde_json::Value,
    update: serde_json::Value,
}

#[derive(Deserialize)]
struct DeleteManyRequest {
    filter: serde_json::Value,
}

#[derive(Deserialize)]
struct AggregateRequest {
    pipeline: Vec<serde_json::Value>,
}

#[derive(Serialize)]
struct DataResponse {
    ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    data: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    count: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

// ── Helpers ──────────────────────────────────────────────────────────────────

fn json_to_doc(v: &serde_json::Value) -> Result<Document, String> {
    bson::to_document(v).map_err(|e| e.to_string())
}

fn doc_to_json(d: &Document) -> Result<serde_json::Value, String> {
    serde_json::to_value(d).map_err(|e| e.to_string())
}

fn parse_filter(s: &str) -> Result<Document, String> {
    let v: serde_json::Value = serde_json::from_str(s).map_err(|e| e.to_string())?;
    json_to_doc(&v)
}

// ── Handlers ─────────────────────────────────────────────────────────────────

async fn healthz() -> &'static str {
    "ok"
}

async fn insert_document(
    State(state): State<AppState>,
    Path(collection): Path<String>,
    Json(body): Json<serde_json::Value>,
) -> impl IntoResponse {
    let col = state.collection(&collection);
    match json_to_doc(&body) {
        Ok(doc) => match col.insert_one(doc, None).await {
            Ok(result) => (
                StatusCode::CREATED,
                Json(DataResponse {
                    ok: true,
                    data: Some(serde_json::json!({
                        "inserted_id": result.inserted_id.to_string()
                    })),
                    count: None,
                    error: None,
                }),
            ),
            Err(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(DataResponse {
                    ok: false,
                    data: None,
                    count: None,
                    error: Some(e.to_string()),
                }),
            ),
        },
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(DataResponse {
                ok: false,
                data: None,
                count: None,
                error: Some(format!("Invalid JSON: {}", e)),
            }),
        ),
    }
}

async fn bulk_insert(
    State(state): State<AppState>,
    Path(collection): Path<String>,
    Json(body): Json<BulkInsertRequest>,
) -> impl IntoResponse {
    let col = state.collection(&collection);
    let docs: Vec<Document> = body
        .documents
        .iter()
        .filter_map(|v| json_to_doc(v).ok())
        .collect();

    if docs.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(DataResponse {
                ok: false,
                data: None,
                count: None,
                error: Some("No valid documents to insert".into()),
            }),
        );
    }

    match col.insert_many(docs, None).await {
        Ok(result) => (
            StatusCode::CREATED,
            Json(DataResponse {
                ok: true,
                data: Some(serde_json::json!({
                    "inserted_count": result.inserted_ids.len()
                })),
                count: Some(result.inserted_ids.len() as i64),
                error: None,
            }),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(DataResponse {
                ok: false,
                data: None,
                count: None,
                error: Some(e.to_string()),
            }),
        ),
    }
}

async fn query_documents(
    State(state): State<AppState>,
    Path(collection): Path<String>,
    Query(params): Query<QueryParams>,
) -> impl IntoResponse {
    let col = state.collection(&collection);
    let filter: Document = match params.filter.as_deref().map(parse_filter).transpose() {
        Ok(Some(f)) => f,
        Ok(None) => doc! {},
        Err(e) => {
            warn!(error = %e, "Invalid filter");
            doc! {}
        }
    };

    let mut opts = mongodb::options::FindOptions::default();
    if let Some(limit) = params.limit {
        opts.limit = Some(limit);
    }
    opts.skip = params.skip.map(|s| s as u64);
    opts.sort = params.sort.as_ref().and_then(|s| parse_filter(s).ok());

    match col.find(filter, Some(opts)).await {
        Ok(cursor) => {
            let docs: Vec<Document> = match cursor.try_collect().await {
                Ok(d) => d,
                Err(e) => {
                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(DataResponse {
                            ok: false,
                            data: None,
                            count: None,
                            error: Some(e.to_string()),
                        }),
                    );
                }
            };
            let json_docs: Vec<serde_json::Value> =
                docs.iter().filter_map(|d| doc_to_json(d).ok()).collect();
            let count = json_docs.len() as i64;
            (
                StatusCode::OK,
                Json(DataResponse {
                    ok: true,
                    data: Some(serde_json::Value::Array(json_docs)),
                    count: Some(count),
                    error: None,
                }),
            )
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(DataResponse {
                ok: false,
                data: None,
                count: None,
                error: Some(e.to_string()),
            }),
        ),
    }
}

async fn get_document(
    State(state): State<AppState>,
    Path((collection, id)): Path<(String, String)>,
) -> impl IntoResponse {
    let col = state.collection(&collection);
    let filter = if let Ok(oid) = bson::oid::ObjectId::parse_str(&id) {
        doc! { "_id": oid }
    } else {
        doc! { "_id": id }
    };

    match col.find_one(filter, None).await {
        Ok(Some(doc)) => match doc_to_json(&doc) {
            Ok(v) => (
                StatusCode::OK,
                Json(DataResponse {
                    ok: true,
                    data: Some(v),
                    count: None,
                    error: None,
                }),
            ),
            Err(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(DataResponse {
                    ok: false,
                    data: None,
                    count: None,
                    error: Some(e.to_string()),
                }),
            ),
        },
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(DataResponse {
                ok: false,
                data: None,
                count: None,
                error: Some("Document not found".into()),
            }),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(DataResponse {
                ok: false,
                data: None,
                count: None,
                error: Some(e.to_string()),
            }),
        ),
    }
}

async fn update_document(
    State(state): State<AppState>,
    Path((collection, id)): Path<(String, String)>,
    Json(body): Json<serde_json::Value>,
) -> impl IntoResponse {
    let col = state.collection(&collection);
    let filter = if let Ok(oid) = bson::oid::ObjectId::parse_str(&id) {
        doc! { "_id": oid }
    } else {
        doc! { "_id": id }
    };

    let update_doc = if let Some(obj) = body.as_object() {
        if obj.keys().any(|k| k.starts_with('$')) {
            json_to_doc(&body).unwrap_or(doc! {})
        } else {
            doc! { "$set": json_to_doc(&body).unwrap_or(doc! {}) }
        }
    } else {
        doc! {}
    };

    match col.update_one(filter, update_doc, None).await {
        Ok(result) => (
            StatusCode::OK,
            Json(DataResponse {
                ok: true,
                data: Some(serde_json::json!({
                    "matched_count": result.matched_count,
                    "modified_count": result.modified_count
                })),
                count: None,
                error: None,
            }),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(DataResponse {
                ok: false,
                data: None,
                count: None,
                error: Some(e.to_string()),
            }),
        ),
    }
}

async fn upsert_document(
    State(state): State<AppState>,
    Path((collection, id)): Path<(String, String)>,
    Json(body): Json<serde_json::Value>,
) -> impl IntoResponse {
    let col = state.collection(&collection);
    let filter = if let Ok(oid) = bson::oid::ObjectId::parse_str(&id) {
        doc! { "_id": oid }
    } else {
        doc! { "_id": id }
    };

    let update_doc = if let Some(obj) = body.as_object() {
        if obj.keys().any(|k| k.starts_with('$')) {
            json_to_doc(&body).unwrap_or(doc! {})
        } else {
            doc! { "$set": json_to_doc(&body).unwrap_or(doc! {}) }
        }
    } else {
        doc! {}
    };

    let opts = UpdateOptions::builder().upsert(true).build();

    match col.update_one(filter, update_doc, opts).await {
        Ok(result) => (
            StatusCode::OK,
            Json(DataResponse {
                ok: true,
                data: Some(serde_json::json!({
                    "matched_count": result.matched_count,
                    "modified_count": result.modified_count,
                    "upserted": result.upserted_id.map(|v| v.to_string())
                })),
                count: None,
                error: None,
            }),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(DataResponse {
                ok: false,
                data: None,
                count: None,
                error: Some(e.to_string()),
            }),
        ),
    }
}

async fn delete_document(
    State(state): State<AppState>,
    Path((collection, id)): Path<(String, String)>,
) -> impl IntoResponse {
    let col = state.collection(&collection);
    let filter = if let Ok(oid) = bson::oid::ObjectId::parse_str(&id) {
        doc! { "_id": oid }
    } else {
        doc! { "_id": id }
    };

    match col.delete_one(filter, None).await {
        Ok(result) => (
            StatusCode::OK,
            Json(DataResponse {
                ok: true,
                data: None,
                count: Some(result.deleted_count as i64),
                error: None,
            }),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(DataResponse {
                ok: false,
                data: None,
                count: None,
                error: Some(e.to_string()),
            }),
        ),
    }
}

async fn count_documents(
    State(state): State<AppState>,
    Path(collection): Path<String>,
    Query(params): Query<QueryParams>,
) -> impl IntoResponse {
    let col = state.collection(&collection);
    let filter: Document = match params.filter.as_deref().map(parse_filter).transpose() {
        Ok(Some(f)) => f,
        Ok(None) => doc! {},
        Err(e) => {
            warn!(error = %e, "Invalid filter");
            doc! {}
        }
    };

    match col.count_documents(filter, None).await {
        Ok(count) => (
            StatusCode::OK,
            Json(DataResponse {
                ok: true,
                data: None,
                count: Some(count as i64),
                error: None,
            }),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(DataResponse {
                ok: false,
                data: None,
                count: None,
                error: Some(e.to_string()),
            }),
        ),
    }
}

async fn aggregate(
    State(state): State<AppState>,
    Path(collection): Path<String>,
    Json(body): Json<AggregateRequest>,
) -> impl IntoResponse {
    let col = state.collection(&collection);
    let pipeline: Vec<Document> = body
        .pipeline
        .iter()
        .filter_map(|v| json_to_doc(v).ok())
        .collect();

    match col.aggregate(pipeline, None).await {
        Ok(cursor) => {
            let docs: Vec<Document> = match cursor.try_collect().await {
                Ok(d) => d,
                Err(e) => {
                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(DataResponse {
                            ok: false,
                            data: None,
                            count: None,
                            error: Some(e.to_string()),
                        }),
                    );
                }
            };
            let json_docs: Vec<serde_json::Value> =
                docs.iter().filter_map(|d| doc_to_json(d).ok()).collect();
            let count = json_docs.len() as i64;
            (
                StatusCode::OK,
                Json(DataResponse {
                    ok: true,
                    data: Some(serde_json::Value::Array(json_docs)),
                    count: Some(count),
                    error: None,
                }),
            )
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(DataResponse {
                ok: false,
                data: None,
                count: None,
                error: Some(e.to_string()),
            }),
        ),
    }
}

async fn update_many(
    State(state): State<AppState>,
    Path(collection): Path<String>,
    Json(body): Json<UpdateManyRequest>,
) -> impl IntoResponse {
    let col = state.collection(&collection);
    let filter = json_to_doc(&body.filter).unwrap_or(doc! {});
    let update = json_to_doc(&body.update).unwrap_or(doc! {});

    match col.update_many(filter, update, None).await {
        Ok(result) => (
            StatusCode::OK,
            Json(DataResponse {
                ok: true,
                data: None,
                count: Some(result.modified_count as i64),
                error: None,
            }),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(DataResponse {
                ok: false,
                data: None,
                count: None,
                error: Some(e.to_string()),
            }),
        ),
    }
}

async fn delete_many(
    State(state): State<AppState>,
    Path(collection): Path<String>,
    Json(body): Json<DeleteManyRequest>,
) -> impl IntoResponse {
    let col = state.collection(&collection);
    let filter = json_to_doc(&body.filter).unwrap_or(doc! {});

    match col.delete_many(filter, None).await {
        Ok(result) => (
            StatusCode::OK,
            Json(DataResponse {
                ok: true,
                data: None,
                count: Some(result.deleted_count as i64),
                error: None,
            }),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(DataResponse {
                ok: false,
                data: None,
                count: None,
                error: Some(e.to_string()),
            }),
        ),
    }
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

    let mongo_uri = std::env::var("MONGO_URI")
        .or_else(|_| std::env::var("MONGODB_URI"))
        .unwrap_or_else(|_| "mongodb://localhost:27017".into());
    let database = std::env::var("MONGO_DB").unwrap_or_else(|_| "clotho".into());
    let port = std::env::var("PORT").unwrap_or_else(|_| "9090".into());
    let addr = format!("0.0.0.0:{}", port);

    info!(mongo_uri = %mongo_uri, database = %database, "Connecting to MongoDB");
    let mut opts = mongodb::options::ClientOptions::parse(&mongo_uri)
        .await
        .expect("Failed to parse MongoDB URI");
    opts.app_name = Some("clotho-data-proxy".to_string());
    opts.min_pool_size = Some(5);
    opts.max_pool_size = Some(50);

    let client = Client::with_options(opts).expect("Failed to create MongoDB client");

    // Verify connection
    match client.list_database_names(None, None).await {
        Ok(dbs) => info!(databases = ?dbs.len(), "Connected to MongoDB"),
        Err(e) => {
            error!(error = %e, "Failed to connect to MongoDB");
            std::process::exit(1);
        }
    }

    let state = AppState { client, database };

    let app = Router::new()
        .route("/healthz", get(healthz))
        .route("/v1/data/:collection", get(query_documents))
        .route("/v1/data/:collection", post(insert_document))
        .route("/v1/data/:collection/bulk", post(bulk_insert))
        .route("/v1/data/:collection/count", get(count_documents))
        .route("/v1/data/:collection/aggregate", post(aggregate))
        .route("/v1/data/:collection/update-many", post(update_many))
        .route("/v1/data/:collection/delete-many", post(delete_many))
        .route("/v1/data/:collection/:id", get(get_document))
        .route("/v1/data/:collection/:id", post(update_document))
        .route("/v1/data/:collection/:id/upsert", post(upsert_document))
        .route("/v1/data/:collection/:id", delete(delete_document))
        .with_state(state);

    info!(addr = %addr, "Clotho Data Proxy starting");

    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}