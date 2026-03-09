use serde::Serialize;
use std::sync::OnceLock;
use std::net::UdpSocket;
use std::time::{SystemTime, UNIX_EPOCH};

static SOCKET: OnceLock<UdpSocket> = OnceLock::new();

#[derive(Serialize)]
#[serde(tag = "type", content = "payload")]
enum TelemetryEvent {
    Lifecycle(LifecycleEvent),
    Progress(ProgressEvent),
}

#[derive(Serialize)]
struct LifecycleEvent {
    pipeline_id: String,
    event: String,
    timestamp: u64,
    uptime_ms: u64,
    metadata: std::collections::HashMap<String, String>,
}

#[derive(Serialize)]
struct ProgressEvent {
    pipeline_id: String,
    current: u64,
    total: Option<u64>,
    percent: Option<f64>,
}

pub fn init(pipeline_id: String) {
    if SOCKET.get().is_some() { return; }

    let socket = UdpSocket::bind("0.0.0.0:0").ok();
    if let Some(sock) = socket {
        let _ = sock.set_nonblocking(true);
        let _ = SOCKET.set(sock);
    }

    emit(TelemetryEvent::Lifecycle(LifecycleEvent {
        pipeline_id,
        event: "START".to_string(),
        timestamp: now(),
        uptime_ms: 0,
        metadata: std::collections::HashMap::new(),
    }));
}

pub fn shutdown(pipeline_id: &str) {
    emit(TelemetryEvent::Lifecycle(LifecycleEvent {
        pipeline_id: pipeline_id.to_string(),
        event: "STOP".to_string(),
        timestamp: now(),
        uptime_ms: 0,
        metadata: std::collections::HashMap::new(),
    }));
}

pub fn report_progress(pipeline_id: &str, current: u64, total: Option<u64>) {
    let percent = match total {
        Some(t) if t > 0 => Some((current as f64 / t as f64) * 100.0),
        _ => None,
    };

    emit(TelemetryEvent::Progress(ProgressEvent {
        pipeline_id: pipeline_id.to_string(),
        current,
        total,
        percent,
    }));
}

fn emit(event: TelemetryEvent) {
    if let Some(socket) = SOCKET.get() {
        if let Ok(json) = serde_json::to_string(&event) {
            let _ = socket.send_to(json.as_bytes(), "127.0.0.1:8125");
        }
    }
}

fn now() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs()
}