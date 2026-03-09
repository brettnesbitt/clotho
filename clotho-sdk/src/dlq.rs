use serde::{Serialize, Deserialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FailureRecord {
    pub original_data: String,
    pub error: String,
    pub step: String,
    pub timestamp: u64,
}