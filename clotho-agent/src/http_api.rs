use axum::{Router, Json, extract::State, routing::{post, get}, http::StatusCode};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex;

use crate::execution_store::{ExecutionBuffer, ExecutionRecord};
use crate::{AgentState, TelemetryEvent, process_sdk_event};

pub type SharedBuffer = Arc<Mutex<ExecutionBuffer>>;
pub type SharedAgentState = Arc<Mutex<AgentState>>;

/// Combined state for all HTTP handlers.
#[derive(Clone)]
pub struct AppState {
    pub buffer: SharedBuffer,
    pub agent: SharedAgentState,
}

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

#[derive(Serialize)]
struct TelemetryResponse {
    status: String,
    processed: usize,
}

/// GET /healthz
async fn healthz() -> &'static str {
    "ok"
}

/// POST /v1/execution — SDK reports execution results here.
/// Buffered in memory, forwarded to Control Plane on next flush cycle.
async fn ingest_execution(
    State(state): State<AppState>,
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

    let mut buf = state.buffer.lock().await;
    buf.push(record);
    let count = buf.len();
    eprintln!("[http] buffered execution for {} ({} pending)", report.pipeline_id, count);
    Ok(Json(IngestResponse { status: "ok".into(), buffered: count }))
}

/// POST /v1/telemetry/events — WASM pipelines batch-send telemetry events here.
/// Events are deserialized and processed through the same pipeline as UDP events.
/// Fire-and-forget from the SDK side; always returns 200.
async fn ingest_telemetry_events(
    State(state): State<AppState>,
    Json(events): Json<Vec<serde_json::Value>>,
) -> Json<TelemetryResponse> {
    let mut processed = 0usize;
    for value in &events {
        match serde_json::from_value::<TelemetryEvent>(value.clone()) {
            Ok(event) => {
                process_sdk_event(&state.agent, event).await;
                processed += 1;
            }
            Err(e) => {
                eprintln!("[http] failed to parse telemetry event: {} — {:?}", e, value);
            }
        }
    }
    eprintln!("[http] ingested {}/{} telemetry events via HTTP", processed, events.len());
    Json(TelemetryResponse { status: "ok".into(), processed })
}

pub fn build_router(buffer: SharedBuffer, agent: SharedAgentState) -> Router {
    let state = AppState { buffer, agent };
    Router::new()
        .route("/healthz", get(healthz))
        .route("/v1/execution", post(ingest_execution))
        .route("/v1/telemetry/events", post(ingest_telemetry_events))
        .with_state(state)
}
