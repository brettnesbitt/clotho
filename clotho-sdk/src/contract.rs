use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct ContractResult {
    pub pipeline_id: String,
    pub rule_name: String,
    pub status: ContractStatus,
    pub value: Option<String>, // "0.95" (pass rate) or "bad_value"
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub enum ContractStatus {
    Pass,
    Warning,
    Fail,
}

// Telemetry Event Wrapper
#[derive(Serialize)]
pub struct DataQualityEvent {
    pub contract: ContractResult,
    pub timestamp: u64,
}