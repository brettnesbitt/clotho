use std::sync::OnceLock;
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use serde::Serialize;

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

pub fn emit_data_quality(id: &str, rule: &str, status: ContractStatus, val: Option<String>) {
    emit(TelemetryEvent::DataQuality(DataQualityEvent {
        contract: ContractResult {
            pipeline_id: id.to_string(),
            rule_name: rule.to_string(),
            status,
            value: val,
        },
        timestamp: now(),
    }));
}

#[derive(Serialize)]
pub struct LifecycleEvent {
    pub pipeline_id: String,
    pub event: String,       // "STARTUP", "RUNNING", "STOPPED"
    pub timestamp: u64,
    
    // --- New Cold Start Metrics ---
    #[serde(skip_serializing_if = "Option::is_none")]
    pub boot_latency_ms: Option<u64>, 
    
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ttfr_ms: Option<u64>,
}

// ... existing emit logic ...