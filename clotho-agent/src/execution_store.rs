use serde::{Deserialize, Serialize};
use std::collections::VecDeque;

/// Max execution reports buffered in memory before oldest are dropped.
const MAX_BUFFER: usize = 500;

/// A single execution record — identical schema to the Control Plane API.
/// The agent holds these in memory until they are successfully forwarded.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ExecutionRecord {
    pub pipeline_id: String,
    pub started_at: String,       // RFC3339
    pub duration_ms: u64,
    pub status: String,           // "completed", "failed", "timeout"
    pub records_in: u64,
    pub records_out: u64,
    pub records_failed: u64,
    pub bytes_processed: u64,
    pub log_lines: Vec<String>,
}

/// Bounded in-memory ring buffer for execution reports.
/// Stateless: pod restart = buffer gone. The Control Plane is the source of truth.
pub struct ExecutionBuffer {
    pending: VecDeque<ExecutionRecord>,
}

impl ExecutionBuffer {
    pub fn new() -> Self {
        Self {
            pending: VecDeque::with_capacity(MAX_BUFFER),
        }
    }

    /// Push an execution report into the buffer.
    /// If the buffer is full, the oldest report is dropped.
    pub fn push(&mut self, record: ExecutionRecord) {
        if self.pending.len() >= MAX_BUFFER {
            let dropped = self.pending.pop_front();
            if let Some(d) = dropped {
                eprintln!("[buffer] dropped oldest execution for {} (buffer full)", d.pipeline_id);
            }
        }
        self.pending.push_back(record);
    }

    /// Drain up to `limit` records for forwarding to Control Plane.
    /// Records are removed from the buffer — caller is responsible for retry on failure.
    pub fn drain(&mut self, limit: usize) -> Vec<ExecutionRecord> {
        let n = limit.min(self.pending.len());
        self.pending.drain(..n).collect()
    }

    /// Re-enqueue records that failed to send (prepend so they retry first).
    pub fn requeue(&mut self, records: Vec<ExecutionRecord>) {
        for record in records.into_iter().rev() {
            if self.pending.len() < MAX_BUFFER {
                self.pending.push_front(record);
            }
        }
    }

    pub fn len(&self) -> usize {
        self.pending.len()
    }
}
