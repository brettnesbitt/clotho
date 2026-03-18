mod kubelet;
mod tracker;
mod execution_store;
mod http_api;

use std::sync::Arc;
use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::net::UdpSocket;
use tokio::sync::Mutex;
use serde::{Deserialize, Serialize};
use anyhow::Result;

use execution_store::ExecutionBuffer;
use http_api::SharedBuffer;

// Reads the Agent's Cargo.toml version
const AGENT_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Extract the pipeline/deployment name from a K8s pod name.
/// Pod names follow: {deployment}-{rs-hash}-{pod-hash}
/// e.g. "daily-idea-68dd885c4d-chn2d" -> "daily-idea"
fn extract_pipeline_name(pod_name: &str) -> String {
    // Split from the right: first split removes pod hash, second removes RS hash
    let parts: Vec<&str> = pod_name.rsplitn(3, '-').collect();
    if parts.len() == 3 {
        parts[2].to_string()
    } else {
        pod_name.to_string()
    }
}

// --- 1. Protocol Types (SDK -> Agent) ---
#[derive(Deserialize, Serialize, Debug, Clone)] 
#[serde(tag = "type", content = "payload")]
enum TelemetryEvent {
    Handshake(HandshakeEvent),
    Lifecycle(LifecycleEvent),
    Progress(ProgressEvent),
    DataQuality(DataQualityEvent),
    Throughput(ThroughputEvent),
    Dlq(DlqEvent),
}

#[derive(Deserialize, Serialize, Debug, Clone)]
struct HandshakeEvent {
    sdk_version: String,
    pipeline_id: String,
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
    stats: Option<ResourceStats>,
}

#[derive(Serialize, Debug, Clone)]
struct ApiEvent {
    #[serde(rename = "type")]
    event_type: String,
    timestamp: u64,
    payload: serde_json::Value,
}

#[derive(Serialize, Debug, Clone)]
struct ResourceStats {
    cpu_nano: i64,
    mem_bytes: i64,
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
    let api_base = std::env::var("CLOTHO_API_URL").unwrap_or("http://localhost:3000".into());
    let api_url = format!("{}/v1/telemetry", api_base.trim_end_matches('/'));
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

    // In-memory execution buffer (stateless — Control Plane is source of truth)
    let exec_buffer: SharedBuffer = Arc::new(Mutex::new(ExecutionBuffer::new()));
    println!("   Buffer: in-memory (stateless broker)");

    let state = Arc::new(Mutex::new(AgentState {
        event_buffer: HashMap::new(),
        tracker: tracker::ResourceTracker::new(),
        client: reqwest::Client::new(),
        api_url,
    }));

    // --- TASK 0: HTTP API (SDK execution reports) ---
    let http_port: u16 = std::env::var("CLOTHO_HTTP_PORT").unwrap_or("8126".into()).parse().unwrap_or(8126);
    let http_buffer = exec_buffer.clone();
    tokio::spawn(async move {
        let app = http_api::build_router(http_buffer);
        let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", http_port)).await.unwrap();
        eprintln!("[http] listening on 0.0.0.0:{}", http_port);
        axum::serve(listener, app).await.unwrap();
    });

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

    // System namespaces to exclude from tracking
    let system_ns: Vec<String> = vec![
        "kube-system", "kube-public", "kube-node-lease",
        "clotho-system", "clotho", "clotho-control",
        "cert-manager", "gmp-system", "gmp-public",
        "gke-managed-system", "gke-managed-cim",
        "gke-managed-networking-dra-driver", "gke-managed-volumepopulator",
        "kwasm", "spin-operator",
    ].into_iter().map(String::from).collect();

    // --- TASK 2: Kubelet Scraper (FinOps) ---
    let state_scrape = state.clone();
    tokio::spawn(async move {
        if let Some(client) = kubelet {
            let mut interval = tokio::time::interval(Duration::from_secs(15));
            loop {
                interval.tick().await;
                match client.get_stats().await {
                    Ok(summary) => {
                        let mut locked = state_scrape.lock().await;
                        let mut tracked = 0u32;
                        
                        for pod in summary.pods {
                            // Track all non-system pods (these are Clotho pipelines)
                            if !system_ns.iter().any(|ns| ns == &pod.podRef.namespace) {
                                let cpu = pod.cpu.map(|c| c.usageNanoCores).unwrap_or(0);
                                let mem = pod.memory.map(|m| m.workingSetBytes).unwrap_or(0);
                                
                                // Use clean pipeline name, not raw pod name
                                let pipeline_name = extract_pipeline_name(&pod.podRef.name);
                                locked.tracker.update(&pipeline_name, cpu, mem);
                                tracked += 1;
                            }
                        }
                        eprintln!("[scrape] tracked {} pipeline pods", tracked);
                    }
                    Err(e) => {
                        eprintln!("[scrape] kubelet error: {}", e);
                    }
                }
            }
        }
    });

    // --- TASK 3: API Flush Loop ---
    // Sends Events, Billing Data, AND buffered execution reports to Control Plane
    let mut flush_interval = tokio::time::interval(Duration::from_secs(5));
    loop {
        flush_interval.tick().await;
        flush_to_api(&state).await;
        forward_executions(&exec_buffer, &state).await;
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
            e.pipeline_id.clone(), 
            ApiEvent { event_type: "LIFECYCLE".into(), timestamp: now, payload: serde_json::to_value(e).unwrap() }
        ),
        TelemetryEvent::Progress(e) => (
            e.pipeline_id.clone(),
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

/// Forward buffered execution reports to the Control Plane API.
/// Drains the in-memory buffer and POSTs each record. Failed sends are requeued.
async fn forward_executions(buffer: &SharedBuffer, state: &Arc<Mutex<AgentState>>) {
    // Drain up to 50 records from the buffer
    let batch = {
        let mut buf = buffer.lock().await;
        buf.drain(50)
    };

    if batch.is_empty() {
        return;
    }

    // Clone client + URL to avoid holding state mutex across awaits
    let (client, exec_url) = {
        let locked = state.lock().await;
        let base = locked.api_url.trim_end_matches("/v1/telemetry").to_string();
        (locked.client.clone(), format!("{}/v1/executions", base))
    };

    let mut failed = Vec::new();
    for record in batch {
        match client.post(&exec_url).json(&record).send().await {
            Ok(resp) if resp.status().is_success() => {
                eprintln!("[forward] {} -> Control Plane OK", record.pipeline_id);
            }
            Ok(resp) => {
                eprintln!("[forward] {} -> Control Plane {} (requeuing)", record.pipeline_id, resp.status());
                failed.push(record);
            }
            Err(e) => {
                eprintln!("[forward] {} -> failed: {} (requeuing)", record.pipeline_id, e);
                failed.push(record);
            }
        }
    }

    // Requeue failed sends so they retry next cycle
    if !failed.is_empty() {
        let count = failed.len();
        let mut buf = buffer.lock().await;
        buf.requeue(failed);
        eprintln!("[forward] requeued {} failed records", count);
    }
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
        
        // Find matching usage for this pipeline, convert to instantaneous-ish nanocores/bytes
        let stats = usage_events.iter().find(|u| u.pod_uid == pid).map(|u| ResourceStats {
            cpu_nano: u.instant_cpu_nanocores as i64,
            mem_bytes: u.instant_mem_bytes as i64,
        });

        if events.is_empty() && stats.is_none() { continue; }

        let payload = AgentPayload {
            pipeline_id: pid.clone(),
            events,
            stats,
        };

        // Fire and Forget
        match locked.client.post(&locked.api_url).json(&payload).send().await {
            Ok(resp) => eprintln!("[flush] POST {} -> {} (events={})", pid, resp.status(), payload.events.len()),
            Err(e) => eprintln!("[flush] POST {} failed: {}", pid, e),
        }
    }
}