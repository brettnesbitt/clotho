use serde::{Serialize, Deserialize};
use std::net::UdpSocket;
use std::sync::{OnceLock, Mutex};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

const AGENT_ADDR: &str = "127.0.0.1:8125";
const AGENT_HTTP_PORT: u16 = 8126;

/// Execution report collected by pipeline runners, read by the macro.
#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct ExecutionReport {
    pub pipeline_id: String,
    #[serde(default)]
    pub started_at: String,
    pub duration_ms: u64,
    pub status: String,
    pub records_in: u64,
    pub records_out: u64,
    pub records_failed: u64,
    pub bytes_processed: u64,
    pub log_lines: Vec<String>,
}

static EXECUTION_REPORT: OnceLock<Mutex<Option<ExecutionReport>>> = OnceLock::new();

fn report_lock() -> &'static Mutex<Option<ExecutionReport>> {
    EXECUTION_REPORT.get_or_init(|| Mutex::new(None))
}

/// Called by pipeline runners (stream, once, batch) at the end of execution.
pub fn set_execution_report(report: ExecutionReport) {
    if let Ok(mut guard) = report_lock().lock() {
        *guard = Some(report);
    }
}

/// Called by the macro-generated code to retrieve the report.
pub fn take_execution_report() -> Option<ExecutionReport> {
    report_lock().lock().ok().and_then(|mut guard| guard.take())
}

/// Serialize the execution report as JSON bytes (for HTTP POST).
pub fn execution_report_json() -> Option<Vec<u8>> {
    take_execution_report().and_then(|r| serde_json::to_vec(&r).ok())
}

// 1. The Global Stopwatch
// initialized the first time ANY code in the SDK is touched.
static PROCESS_BIRTH: OnceLock<Instant> = OnceLock::new();

/// Call this internally as early as possible
pub fn mark_birth() {
    PROCESS_BIRTH.get_or_init(|| Instant::now());
}

/// Get milliseconds since the process started (with fractional precision)
/// Returns microseconds / 1000.0 to preserve sub-millisecond accuracy
pub fn uptime_ms() -> u64 {
    PROCESS_BIRTH.get()
        .map(|t| {
            let micros = t.elapsed().as_micros() as u64;
            // For sub-millisecond durations, round up to 1ms minimum
            // This ensures we never report 0ms for actual work
            if micros > 0 && micros < 1000 {
                1
            } else {
                micros / 1000
            }
        })
        .unwrap_or(0)
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// Lifecycle event payload matching agent's LifecycleEvent struct
#[derive(Serialize)]
struct LifecyclePayload {
    pipeline_id: String,
    event: String,
    timestamp: u64,
    boot_latency_ms: Option<u64>,
    ttfr_ms: Option<u64>,
    runtime_ms: Option<u64>,
}

/// Tagged telemetry event for lifecycle
#[derive(Serialize)]
struct LifecycleEnvelope {
    #[serde(rename = "type")]
    event_type: String,
    payload: LifecyclePayload,
}

/// Emit a lifecycle event (STARTUP, FIRST_BATCH, FINISHED, etc.) via UDP to the agent.
pub fn emit_lifecycle(pipeline_id: &str, event: &str, boot_ms: Option<u64>, ttfr_ms: Option<u64>) {
    emit_lifecycle_with_runtime(pipeline_id, event, boot_ms, ttfr_ms, None);
}

/// Emit a lifecycle event with an explicit runtime duration (used for FINISHED).
pub fn emit_lifecycle_with_runtime(pipeline_id: &str, event: &str, boot_ms: Option<u64>, ttfr_ms: Option<u64>, runtime_ms: Option<u64>) {
    let ts = now_secs();
    eprintln!(
        "[Clotho Telemetry] ts={} pipeline={} event={} boot_latency_ms={:?} ttfr_ms={:?} runtime_ms={:?} process_uptime_ms={}",
        ts,
        pipeline_id,
        event,
        boot_ms,
        ttfr_ms,
        runtime_ms,
        uptime_ms()
    );

    let envelope = LifecycleEnvelope {
        event_type: "Lifecycle".to_string(),
        payload: LifecyclePayload {
            pipeline_id: pipeline_id.to_string(),
            event: event.to_string(),
            timestamp: ts,
            boot_latency_ms: boot_ms,
            ttfr_ms,
            runtime_ms,
        },
    };

    if let Ok(socket) = UdpSocket::bind("0.0.0.0:0") {
        if let Ok(data) = serde_json::to_vec(&envelope) {
            let _ = socket.send_to(&data, AGENT_ADDR);
        }
    }
}

/// Emit an error event via UDP to the agent.
pub fn emit_error(pipeline_id: &str, error_type: &str, message: &str) {
    eprintln!(
        "[Clotho Telemetry] pipeline={} error={} message={}",
        pipeline_id, error_type, message
    );

    // Send as a lifecycle ERROR event so it appears in the events table
    emit_lifecycle(pipeline_id, &format!("ERROR:{}", error_type), None, None);
}

/// Throughput event payload
#[derive(Serialize)]
struct ThroughputPayload {
    pipeline_id: String,
    records_in: u64,
    records_out: u64,
    records_failed: u64,
    bytes_processed: u64,
    timestamp: u64,
}

/// Tagged telemetry event for throughput
#[derive(Serialize)]
struct ThroughputEvent {
    #[serde(rename = "type")]
    event_type: String,
    payload: ThroughputPayload,
}

/// Emit throughput metrics to the Clotho Agent.
/// Called by the SDK pipeline loop to report records flowing through the pipeline.
pub fn emit_throughput(pipeline_id: &str, records_in: u64, records_out: u64, records_failed: u64, bytes_processed: u64) {
    let event = ThroughputEvent {
        event_type: "Throughput".to_string(),
        payload: ThroughputPayload {
            pipeline_id: pipeline_id.to_string(),
            records_in,
            records_out,
            records_failed,
            bytes_processed,
            timestamp: now_secs(),
        },
    };

    if let Ok(socket) = UdpSocket::bind("0.0.0.0:0") {
        if let Ok(data) = serde_json::to_vec(&event) {
            let _ = socket.send_to(&data, AGENT_ADDR);
        }
    }
}

/// Report progress (rows processed so far). Used by batch pipelines.
pub fn report_progress(pipeline_id: &str, current: u64, total: Option<u64>) {
    eprintln!(
        "[Clotho Telemetry] pipeline={} progress current={} total={:?} uptime_ms={}",
        pipeline_id, current, total, uptime_ms()
    );
    // Also emit as throughput with records_out = current
    emit_throughput(pipeline_id, current, current, 0, 0);
}

/// DLQ event payload
#[derive(Serialize)]
struct DlqPayload {
    pipeline_id: String,
    trace_id: String,
    error: String,
    step: String,
    payload: String,
    timestamp: u64,
}

/// Tagged telemetry event for DLQ
#[derive(Serialize)]
struct DlqEvent {
    #[serde(rename = "type")]
    event_type: String,
    payload: DlqPayload,
}

/// Emit a dead-letter record to the Clotho Agent.
/// Called automatically by the SDK when a pipeline step fails and no custom DLQ is configured.
/// Fire-and-forget over UDP — never crashes the pipeline.
pub fn emit_dlq_record(pipeline_id: &str, trace_id: &str, step: &str, error: &str, payload: &str) {
    let event = DlqEvent {
        event_type: "Dlq".to_string(),
        payload: DlqPayload {
            pipeline_id: pipeline_id.to_string(),
            trace_id: trace_id.to_string(),
            error: error.to_string(),
            step: step.to_string(),
            payload: payload.to_string(),
            timestamp: now_secs(),
        },
    };

    if let Ok(socket) = UdpSocket::bind("0.0.0.0:0") {
        if let Ok(data) = serde_json::to_vec(&event) {
            let _ = socket.send_to(&data, AGENT_ADDR);
        }
    }
    // Silent fail — never crash the pipeline because the dashboard is down
}