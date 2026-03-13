mod kubelet;
mod tracker;

use std::sync::Arc;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::net::UdpSocket;
use tokio::sync::Mutex;
use serde::{Deserialize, Serialize};
use anyhow::Result;

// Reads the Agent's Cargo.toml version
const AGENT_VERSION: &str = env!("CARGO_PKG_VERSION");

// --- 1. Protocol Types (SDK -> Agent) ---
#[derive(Deserialize, Serialize, Debug, Clone)] 
#[serde(tag = "type", content = "payload")]
enum TelemetryEvent {
    Lifecycle(LifecycleEvent),
    Progress(ProgressEvent),
    DataQuality(DataQualityEvent),
    Throughput(ThroughputEvent),
    Dlq(DlqEvent),
}

#[derive(Deserialize, Serialize, Debug, Clone)]
struct LifecycleEvent {
    pipeline_id: String,
    event: String,
    timestamp: u64,
    boot_latency_ms: Option<u64>,
    ttfr_ms: Option<u64>,
    runtime_ms: Option<u64>,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
struct ProgressEvent {
    pipeline_id: String,
    current: u64,
    total: Option<u64>,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
struct DataQualityEvent {
    contract: serde_json::Value,
    timestamp: u64,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
struct ThroughputEvent {
    pipeline_id: String,
    records_in: u64,
    records_out: u64,
    records_failed: u64,
    bytes_processed: u64,
    timestamp: u64,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
struct DlqEvent {
    pipeline_id: String,
    trace_id: String,
    error: String,
    step: String,
    payload: String,
    timestamp: u64,
}

// --- 2. API Payload Types (Agent -> Control Plane) ---
#[derive(Serialize, Debug)]
struct AgentPayload {
    pipeline_id: String,
    events: Vec<ApiEvent>,
    usage: Option<ResourceUsage>, // New FinOps Field
}

#[derive(Serialize, Debug, Clone)]
struct ApiEvent {
    event_type: String,
    timestamp: u64,
    payload: serde_json::Value,
}

#[derive(Serialize, Debug, Clone)]
struct ResourceUsage {
    cpu_core_seconds: f64,
    mem_gb_seconds: f64,
}

// --- 3. Shared State ---
struct AgentState {
    // Buffer for events before flushing to API
    // Map<PipelineID, Vec<Events>>
    event_buffer: HashMap<String, Vec<ApiEvent>>,
    
    // The FinOps Calculator
    tracker: tracker::ResourceTracker,
    
    client: reqwest::Client,
    api_url: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Config
    let api_url = std::env::var("CLOTHO_API_URL").unwrap_or("http://localhost:3000/v1/telemetry".into());
    let udp_port = std::env::var("CLOTHO_AGENT_PORT").unwrap_or("8125".into());
    
    println!("🧵 Clotho Agent v0.1");
    println!("   Mode: Kubernetes DaemonSet");
    println!("   Target: {}", api_url);

    // Initialize Components
    let socket = UdpSocket::bind(format!("0.0.0.0:{}", udp_port)).await?;
    let kubelet = kubelet::KubeletClient::new().ok(); // Optional (might fail locally)

    if kubelet.is_none() {
        println!("⚠️  Kubelet client failed to init. FinOps metrics disabled (Local Mode?)");
    }

    let state = Arc::new(Mutex::new(AgentState {
        event_buffer: HashMap::new(),
        tracker: tracker::ResourceTracker::new(),
        client: reqwest::Client::new(),
        api_url,
    }));

    // --- TASK 1: UDP Listener (SDK Events) ---
    let state_udp = state.clone();
    tokio::spawn(async move {
        let mut buf = vec![0u8; 65535];
        loop {
            if let Ok((len, _)) = socket.recv_from(&mut buf).await {
                if let Ok(event) = serde_json::from_slice::<TelemetryEvent>(&buf[..len]) {
                    process_sdk_event(&state_udp, event).await;
                }
            }
        }
    });

    // --- TASK 2: Kubelet Scraper (FinOps) ---
    let state_scrape = state.clone();
    tokio::spawn(async move {
        if let Some(client) = kubelet {
            let mut interval = tokio::time::interval(Duration::from_secs(15));
            loop {
                interval.tick().await;
                if let Ok(summary) = client.get_stats().await {
                    let mut locked = state_scrape.lock().await;
                    
                    for pod in summary.pods {
                        // Only track Clotho pipelines
                        if pod.podRef.name.contains("pipeline") { // Simple filter
                            let cpu = pod.cpu.map(|c| c.usageNanoCores).unwrap_or(0);
                            let mem = pod.memory.map(|m| m.workingSetBytes).unwrap_or(0);
                            
                            // Update the calculator
                            // Note: We use pod.podRef.name as pipeline_id alias for now
                            locked.tracker.update(&pod.podRef.name, cpu, mem);
                        }
                    }
                }
            }
        }
    });

    // --- TASK 3: API Flush Loop ---
    // Sends both Events and Billing Data to Control Plane
    let mut flush_interval = tokio::time::interval(Duration::from_secs(5));
    loop {
        flush_interval.tick().await;
        flush_to_api(&state).await;
    }
}

// --- Helpers ---

async fn process_sdk_event(state: &Arc<Mutex<AgentState>>, event: TelemetryEvent) {
    let mut locked = state.lock().await;
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();

    let (pid, api_evt) = match event {
        TelemetryEvent::Handshake(e) => {
            // THE CHECK
            if e.sdk_version != AGENT_VERSION {
                eprintln!("🚨 VERSION MISMATCH! SDK: {}, Agent: {}", e.sdk_version, AGENT_VERSION);
                eprintln!("   This pipeline is incompatible with the current platform.");
                
                // OPTION 1: Suicide (Kill the Pod)
                // We send a signal to Kubernetes that we are unhealthy
                std::process::exit(1); 
                
                // OPTION 2: Alert Only (Soft Mode)
                // emit_alert_to_control_plane("VersionMismatch", ...);
            }
            // For now, we don't add to buffer, just validate
            return;
        },
        TelemetryEvent::Lifecycle(e) => (
            e.pipeline_id, 
            ApiEvent { event_type: "LIFECYCLE".into(), timestamp: now, payload: serde_json::to_value(e).unwrap() }
        ),
        TelemetryEvent::Progress(e) => (
            e.pipeline_id,
            ApiEvent { event_type: "PROGRESS".into(), timestamp: now, payload: serde_json::to_value(e).unwrap() }
        ),
        TelemetryEvent::DataQuality(e) => (
            // Extract ID from inner payload or pass generic
            "unknown".into(), 
            ApiEvent { event_type: "DATA_QUALITY".into(), timestamp: now, payload: e.contract }
        ),
        TelemetryEvent::Throughput(e) => (
            e.pipeline_id.clone(),
            ApiEvent { event_type: "THROUGHPUT".into(), timestamp: now, payload: serde_json::to_value(e).unwrap() }
        ),
        TelemetryEvent::Dlq(e) => (
            e.pipeline_id.clone(),
            ApiEvent { event_type: "DLQ".into(), timestamp: now, payload: serde_json::to_value(e).unwrap() }
        ),
    };

    locked.event_buffer.entry(pid).or_default().push(api_evt);
}

async fn flush_to_api(state: &Arc<Mutex<AgentState>>) {
    let mut locked = state.lock().await;
    
    // 1. Get Billing Data (Resource Usage)
    // The tracker returns a list of { pod_uid, cpu_seconds, mem_seconds }
    let usage_events = locked.tracker.flush();
    
    // 2. Merge with Event Buffer
    // We iterate known pipelines from the event buffer OR the usage tracker
    let mut pipelines: Vec<String> = locked.event_buffer.keys().cloned().collect();
    for u in &usage_events {
        if !pipelines.contains(&u.pod_uid) {
            pipelines.push(u.pod_uid.clone());
        }
    }

    for pid in pipelines {
        let events = locked.event_buffer.remove(&pid).unwrap_or_default();
        
        // Find matching usage for this pipeline
        let usage = usage_events.iter().find(|u| u.pod_uid == pid).map(|u| ResourceUsage {
            cpu_core_seconds: u.cpu_core_seconds,
            mem_gb_seconds: u.mem_gb_seconds,
        });

        if events.is_empty() && usage.is_none() { continue; }

        let payload = AgentPayload {
            pipeline_id: pid.clone(),
            events,
            usage,
        };

        // Fire and Forget
        let _ = locked.client.post(&locked.api_url).json(&payload).send().await;
    }
}