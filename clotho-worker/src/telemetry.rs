use serde::Serialize;
use std::net::UdpSocket;

// The Address of the Agent running on the Node (DaemonSet)
// In K8s, we can target the host IP or a known localhost port if sharing net namespace
const AGENT_ADDR: &str = "127.0.0.1:8125"; 

#[derive(Serialize)]
pub struct MetricPacket {
    pub pipeline_id: String,
    pub timestamp: u64,
    pub records_processed: u32,
    pub bytes_processed: u64,
    pub error_count: u32,
}

impl MetricPacket {
    pub fn new(pipeline_id: &str) -> Self {
        Self {
            pipeline_id: pipeline_id.to_string(),
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            records_processed: 0,
            bytes_processed: 0,
            error_count: 0,
        }
    }
}

// The Hook
pub fn emit(packet: &MetricPacket) {
    // We create a socket on the fly. In Wasm, this is lightweight.
    // Note: This requires the 'wasi:sockets' capability or liberal networking.
    match UdpSocket::bind("0.0.0.0:0") {
        Ok(socket) => {
            // Serialize to JSON (or Bincode for max speed)
            // We use JSON here just for debugging the PoC, switch to Bincode later.
            if let Ok(payload) = serde_json::to_vec(&packet) {
                let _ = socket.send_to(&payload, AGENT_ADDR);
            }
        }
        Err(_e) => {
            // Silently fail. Never crash the pipeline because the dashboard is down.
            // Note: UDP sockets aren't supported in Spin local dev - this only works in K8s.
        }
    }
}