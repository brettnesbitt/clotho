use serde::{Serialize, Deserialize};
#[cfg(not(target_family = "wasm"))]
use std::net::UdpSocket;
use std::sync::{OnceLock, Mutex};
use std::sync::atomic::AtomicU64;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

const AGENT_UDP_PORT: u16 = 8125;
const AGENT_HTTP_PORT: u16 = 8126;

// --- WASM Event Buffer ---
// On WASM, UDP is unavailable. Events are buffered here and flushed
// via HTTP to the agent at the end of pipeline execution.
static EVENT_BUFFER: OnceLock<Mutex<Vec<serde_json::Value>>> = OnceLock::new();

fn event_buffer() -> &'static Mutex<Vec<serde_json::Value>> {
    EVENT_BUFFER.get_or_init(|| Mutex::new(Vec::new()))
}

/// Buffer a serializable telemetry event (WASM path).
fn buffer_event<T: Serialize>(event: &T) {
    if let Ok(value) = serde_json::to_value(event) {
        if let Ok(mut buf) = event_buffer().lock() {
            buf.push(value);
        }
    }
}

/// Drain all buffered telemetry events. Called by flush_telemetry_http().
pub fn take_buffered_events() -> Vec<serde_json::Value> {
    event_buffer().lock().ok()
        .map(|mut buf| std::mem::take(&mut *buf))
        .unwrap_or_default()
}

/// Send a serialized telemetry event to the agent.
/// Native: fire-and-forget UDP. WASM: buffer for later HTTP flush.
fn send_event<T: Serialize>(event: &T) {
    #[cfg(not(target_family = "wasm"))]
    {
        let addr = agent_addr();
        if let Ok(socket) = UdpSocket::bind("0.0.0.0:0") {
            if let Ok(data) = serde_json::to_vec(event) {
                let _ = socket.send_to(&data, &addr);
            }
        }
    }
    #[cfg(target_family = "wasm")]
    {
        buffer_event(event);
    }
}

/// Flush all buffered telemetry events to the agent via HTTP POST.
/// Called at the end of pipeline execution in once.rs / stream.rs.
/// No-op on native (events are sent immediately via UDP).
pub async fn flush_telemetry_http() {
    #[cfg(target_family = "wasm")]
    {
        let events = take_buffered_events();
        if events.is_empty() {
            return;
        }
        let url = format!("{}/v1/telemetry/events", agent_http_addr());
        eprintln!("[Clotho Telemetry] flushing {} events to {}", events.len(), url);

        let body = match serde_json::to_vec(&events) {
            Ok(b) => b,
            Err(e) => {
                eprintln!("[Clotho Telemetry] flush serialize error: {}", e);
                return;
            }
        };

        use spin_sdk::http::{send, Method, Request, Response};
        let req = Request::builder()
            .method(Method::Post)
            .uri(&url)
            .header("content-type", "application/json")
            .body(body)
            .build();
        match send::<_, Response>(req).await {
            Ok(resp) => eprintln!("[Clotho Telemetry] flush -> {} ({} events)", resp.status(), events.len()),
            Err(e) => eprintln!("[Clotho Telemetry] flush failed: {} (events lost)", e),
        }
    }
    // Native: no-op, events already sent via UDP
}

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

#[cfg(not(target_family = "wasm"))]
fn agent_addr() -> String {
    let host = std::env::var("CLOTHO_AGENT_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
    format!("{}:{}", host, AGENT_UDP_PORT)
}

fn agent_http_addr() -> String {
    let host = crate::config::var_or("CLOTHO_AGENT_HOST", "127.0.0.1");
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
#[cfg(not(target_family = "wasm"))]
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

/// Get current timestamp as RFC3339/ISO8601 string for execution reports.
pub fn now_rfc3339() -> String {
    use std::time::SystemTime;
    let now = SystemTime::now();
    let datetime = chrono::DateTime::<chrono::Utc>::from(now);
    datetime.to_rfc3339()
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

    send_event(&envelope);
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

    send_event(&event);
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

    send_event(&event);
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

    send_event(&event);
}

/// Data sample payload
#[derive(Serialize)]
pub struct DataSamplePayload {
    pub pipeline_id: String,
    pub stage_name: String,
    pub step_name: String,
    pub payload_in: String,
    pub payload_out: String,
    pub timestamp: u64,
}

/// Tagged telemetry event for data sample
#[derive(Serialize)]
pub struct DataSampleEvent {
    #[serde(rename = "type")]
    pub event_type: String,
    pub payload: DataSamplePayload,
}

/// Emit step data sample to the Clotho Agent.
/// Expected to be rate-limited by the caller (e.g. 1 per second per step).
pub fn emit_data_sample(
    pipeline_id: &str,
    stage_name: &str,
    step_name: &str,
    payload_in: &str,
    payload_out: &str,
) {
    let event = DataSampleEvent {
        event_type: "DataSample".to_string(),
        payload: DataSamplePayload {
            pipeline_id: pipeline_id.to_string(),
            stage_name: stage_name.to_string(),
            step_name: step_name.to_string(),
            payload_in: payload_in.to_string(),
            payload_out: payload_out.to_string(),
            timestamp: now_secs(),
        },
    };

    send_event(&event);
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

    send_event(&event);
}
