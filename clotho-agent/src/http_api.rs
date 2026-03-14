use axum::{Router, Json, extract::State, routing::{post, get}, http::StatusCode};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex;

use crate::execution_store::{ExecutionBuffer, ExecutionRecord};

pub type SharedBuffer = Arc<Mutex<ExecutionBuffer>>;

/// Payload the SDK POSTs after each execution completes.
#[derive(Deserialize, Debug)]
pub struct SdkExecutionReport {
    pub pipeline_id: String,
    pub duration_ms: u64,
    pub status: String,
    pub records_in: u64,
    pub records_out: u64,
    pub records_failed: u64,
    pub bytes_processed: u64,
    #[serde(default)]
    pub log_lines: Vec<String>,
}

#[derive(Serialize)]
struct IngestResponse {
    status: String,
    buffered: usize,
}

/// GET /healthz
async fn healthz() -> &'static str {
    "ok"
}

/// POST /v1/execution — SDK reports execution results here.
/// Buffered in memory, forwarded to Control Plane on next flush cycle.
async fn ingest_execution(
    State(buffer): State<SharedBuffer>,
    Json(report): Json<SdkExecutionReport>,
) -> Result<Json<IngestResponse>, StatusCode> {
    let now_secs = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs();
    let record = ExecutionRecord {
        pipeline_id: report.pipeline_id.clone(),
        started_at: format!("{}", now_secs),
        duration_ms: report.duration_ms,
        status: report.status,
        records_in: report.records_in,
        records_out: report.records_out,
        records_failed: report.records_failed,
        bytes_processed: report.bytes_processed,
        log_lines: report.log_lines,
    };

    let mut buf = buffer.lock().await;
    buf.push(record);
    let count = buf.len();
    eprintln!("[http] buffered execution for {} ({} pending)", report.pipeline_id, count);
    Ok(Json(IngestResponse { status: "ok".into(), buffered: count }))
}

pub fn build_router(buffer: SharedBuffer) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/v1/execution", post(ingest_execution))
        .with_state(buffer)
}
