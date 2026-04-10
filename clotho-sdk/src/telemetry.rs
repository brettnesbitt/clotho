use serde::{Serialize, Deserialize};
use std::net::UdpSocket;
use std::sync::{OnceLock, Mutex};
use std::sync::atomic::AtomicU64;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

const AGENT_UDP_PORT: u16 = 8125;
const AGENT_HTTP_PORT: u16 = 8126;

/// Step info for tracking pipeline steps in the DAG
#[derive(Clone, Debug)]
pub struct StepInfo {
    pub name: String,
    pub step_type: String, // "source", "transform", "filter", "branch", "tee", "sink"
}

/// Per-step metrics accumulator
#[derive(Default, Debug)]
pub struct StepMetrics {
    pub records_in: AtomicU64,
    pub records_out: AtomicU64,
    pub records_filtered: AtomicU64,
    pub records_branched: AtomicU64,
    pub records_failed: AtomicU64,
}

fn agent_addr() -> String {
    let host = std::env::var("CLOTHO_AGENT_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
    format!("{}:{}", host, AGENT_UDP_PORT)
}

fn agent_http_addr() -> String {
    let host = std::env::var("CLOTHO_AGENT_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
    format!("http://{}:{}", host, AGENT_HTTP_PORT)
}

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
    pub records_branched: u64,
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

/// Initialize the native telemetry agent (UDP sender to DaemonSet).
/// Called by the #[clotho::daemon] macro at process startup.
pub fn init_native_agent() {
    mark_birth();
    eprintln!("[Clotho Telemetry] Native agent initialized (UDP -> {})", agent_addr());
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

    let addr = agent_addr();
    if let Ok(socket) = UdpSocket::bind("0.0.0.0:0") {
        if let Ok(data) = serde_json::to_vec(&envelope) {
            let _ = socket.send_to(&data, &addr);
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
    records_branched: u64,
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
    emit_throughput_with_branched(pipeline_id, records_in, records_out, records_failed, 0, bytes_processed);
}

/// Emit throughput metrics including branched records.
pub fn emit_throughput_with_branched(pipeline_id: &str, records_in: u64, records_out: u64, records_failed: u64, records_branched: u64, bytes_processed: u64) {
    let event = ThroughputEvent {
        event_type: "Throughput".to_string(),
        payload: ThroughputPayload {
            pipeline_id: pipeline_id.to_string(),
            records_in,
            records_out,
            records_failed,
            records_branched,
            bytes_processed,
            timestamp: now_secs(),
        },
    };

    let addr = agent_addr();
    if let Ok(socket) = UdpSocket::bind("0.0.0.0:0") {
        if let Ok(data) = serde_json::to_vec(&event) {
            let _ = socket.send_to(&data, &addr);
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

    let addr = agent_addr();
    if let Ok(socket) = UdpSocket::bind("0.0.0.0:0") {
        if let Ok(data) = serde_json::to_vec(&event) {
            let _ = socket.send_to(&data, &addr);
        }
    }
    // Silent fail — never crash the pipeline because the dashboard is down
}

/// Step metrics payload for tracking individual pipeline step performance
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct StepMetricsPayload {
    pub pipeline_id: String,
    pub stage_name: String,
    pub step_name: String,
    pub step_type: String, // "source", "transform", "filter", "branch", "tee", "sink"
    pub records_in: u64,
    pub records_out: u64,
    pub records_filtered: u64,
    pub records_branched: u64,
    pub records_failed: u64,
    pub duration_ms: u64,
    pub timestamp: u64,
}

/// Tagged telemetry event for step metrics
#[derive(Serialize)]
struct StepMetricsEvent {
    #[serde(rename = "type")]
    event_type: String,
    payload: StepMetricsPayload,
}

/// Emit step-level metrics to the Clotho Agent.
/// Called by the SDK pipeline engine after each step execution.
pub fn emit_step_metrics(
    pipeline_id: &str,
    stage_name: &str,
    step_name: &str,
    step_type: &str,
    records_in: u64,
    records_out: u64,
    records_filtered: u64,
    records_branched: u64,
    records_failed: u64,
    duration_ms: u64,
) {
    let event = StepMetricsEvent {
        event_type: "StepMetrics".to_string(),
        payload: StepMetricsPayload {
            pipeline_id: pipeline_id.to_string(),
            stage_name: stage_name.to_string(),
            step_name: step_name.to_string(),
            step_type: step_type.to_string(),
            records_in,
            records_out,
            records_filtered,
            records_branched,
            records_failed,
            duration_ms,
            timestamp: now_secs(),
        },
    };

    let addr = agent_addr();
    if let Ok(socket) = UdpSocket::bind("0.0.0.0:0") {
        if let Ok(data) = serde_json::to_vec(&event) {
            let _ = socket.send_to(&data, &addr);
        }
    }
}

/// Data quality event payload
#[derive(Serialize)]
struct DataQualityPayload {
    pipeline_id: String,
    rule: String,
    status: String,
    value: Option<String>,
    timestamp: u64,
}

/// Tagged telemetry event for data quality
#[derive(Serialize)]
struct DataQualityEvent {
    #[serde(rename = "type")]
    event_type: String,
    payload: DataQualityPayload,
}

/// Emit a data quality check result to the Clotho Agent.
/// Called by BatchPipeline::expect() to report contract validation results.
pub fn emit_data_quality(pipeline_id: &str, rule: &str, status: crate::types::ContractStatus, value: Option<String>) {
    let status_str = match &status {
        crate::types::ContractStatus::Pass => "pass".to_string(),
        crate::types::ContractStatus::Warning(msg) => format!("warning:{}", msg),
        crate::types::ContractStatus::Fail => "fail".to_string(),
    };

    eprintln!(
        "[Clotho Telemetry] pipeline={} data_quality rule={} status={}",
        pipeline_id, rule, status_str
    );

    let event = DataQualityEvent {
        event_type: "DataQuality".to_string(),
        payload: DataQualityPayload {
            pipeline_id: pipeline_id.to_string(),
            rule: rule.to_string(),
            status: status_str,
            value,
            timestamp: now_secs(),
        },
    };

    let addr = agent_addr();
    if let Ok(socket) = UdpSocket::bind("0.0.0.0:0") {
        if let Ok(data) = serde_json::to_vec(&event) {
            let _ = socket.send_to(&data, &addr);
        }
    }
}
