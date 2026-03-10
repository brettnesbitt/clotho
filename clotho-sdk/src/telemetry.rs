use std::sync::OnceLock;
use std::time::Instant;

// 1. The Global Stopwatch
// initialized the first time ANY code in the SDK is touched.
static PROCESS_BIRTH: OnceLock<Instant> = OnceLock::new();

/// Call this internally as early as possible
pub fn mark_birth() {
    PROCESS_BIRTH.get_or_init(|| Instant::now());
}

/// Get milliseconds since the process started
pub fn uptime_ms() -> u64 {
    PROCESS_BIRTH.get()
        .map(|t| t.elapsed().as_millis() as u64)
        .unwrap_or(0) // Should only happen if mark_birth wasn't called
}

/// Emit a lifecycle event (STARTUP, RUNNING, FINISHED, etc.)
pub fn emit_lifecycle(pipeline_id: &str, event: &str, boot_ms: Option<u64>, ttfr_ms: Option<u64>) {
    eprintln!(
        "[Clotho Telemetry] pipeline={} event={} boot_ms={:?} ttfr_ms={:?} uptime_ms={}",
        pipeline_id, event, boot_ms, ttfr_ms, uptime_ms()
    );
}

/// Emit an error event
pub fn emit_error(pipeline_id: &str, error_type: &str, message: &str) {
    eprintln!(
        "[Clotho Telemetry] pipeline={} error={} message={}",
        pipeline_id, error_type, message
    );
}