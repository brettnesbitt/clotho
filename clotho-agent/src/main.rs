use tokio::net::UdpSocket;
use std::sync::Arc;
use std::collections::HashMap;
use serde::{Deserialize, Serialize};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex;
use tokio::time::interval;

// --- Protocol Types (Matches SDK) ---

#[derive(Deserialize, Debug, Clone)]
#[serde(tag = "type", content = "payload")]
enum TelemetryEvent {
    Lifecycle(LifecycleEvent),
    Progress(ProgressEvent),
    Metric(MetricEvent),
}

#[derive(Deserialize, Debug, Clone)]
struct LifecycleEvent {
    pipeline_id: String,
    event: String,
    timestamp: u64,
    uptime_ms: u64,
    metadata: HashMap<String, String>,
}

#[derive(Deserialize, Debug, Clone)]
struct ProgressEvent {
    pipeline_id: String,
    current: u64,
    total: Option<u64>,
    percent: Option<f64>,
    eta_seconds: Option<u64>,
}

#[derive(Deserialize, Debug, Clone)]
struct MetricEvent {
    pipeline_id: String,
    name: String,
    value: f64,
}

// --- API Payload Types (Matches Go API) ---

#[derive(Serialize, Debug)]
struct AgentPayload {
    pipeline_id: String,
    events: Vec<ApiEvent>,
    stats: Option<ResourceStats>,
}

#[derive(Serialize, Debug)]
struct ApiEvent {
    #[serde(rename = "type")]
    event_type: String,
    timestamp: i64,
    payload: serde_json::Value,
}

#[derive(Serialize, Debug)]
struct ResourceStats {
    cpu_nano: i64,
    mem_bytes: i64,
}

// --- State Management ---

struct PipelineState {
    events: Vec<ApiEvent>,
    last_seen: Instant,
    cpu_nano: i64,
    mem_bytes: i64,
}

struct AgentState {
    pipelines: HashMap<String, PipelineState>,
    api_url: String,
    client: reqwest::Client,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Configuration
    let api_url = std::env::var("CLOTHO_API_URL")
        .unwrap_or_else(|_| "http://localhost:3000".to_string());
    let udp_port = std::env::var("CLOTHO_AGENT_PORT")
        .unwrap_or_else(|_| "8125".to_string());
    let flush_interval_secs: u64 = std::env::var("CLOTHO_FLUSH_INTERVAL")
        .and_then(|s| s.parse().map_err(|_| std::env::VarError::NotPresent))
        .unwrap_or(5);

    println!("🧵 Clotho Agent Starting...");
    println!("   API URL: {}", api_url);
    println!("   UDP Port: {}", udp_port);
    println!("   Flush Interval: {}s", flush_interval_secs);

    // Initialize state
    let state = Arc::new(Mutex::new(AgentState {
        pipelines: HashMap::new(),
        api_url: format!("{}/v1/telemetry", api_url),
        client: reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()?,
    }));

    // Bind UDP socket
    let socket = UdpSocket::bind(format!("0.0.0.0:{}", udp_port)).await?;
    println!("📡 UDP Listener bound on port {}", udp_port);

    let state_for_listener = state.clone();
    let state_for_flush = state.clone();

    // TASK 1: UDP Listener
    let listener_handle = tokio::spawn(async move {
        let mut buf = vec![0u8; 65535];
        loop {
            match socket.recv_from(&mut buf).await {
                Ok((len, _)) => {
                    if let Ok(event) = serde_json::from_slice::<TelemetryEvent>(&buf[..len]) {
                        handle_telemetry_event(state_for_listener.clone(), event).await;
                    } else {
                        // Try legacy format fallback
                        if let Ok(legacy) = serde_json::from_slice::<serde_json::Value>(&buf[..len]) {
                            println!("⚠️  Legacy format received: {:?}", legacy);
                        }
                    }
                }
                Err(e) => {
                    eprintln!("UDP receive error: {}", e);
                }
            }
        }
    });

    // TASK 2: Periodic Flush
    let flush_handle = tokio::spawn(async move {
        let mut ticker = interval(Duration::from_secs(flush_interval_secs));
        loop {
            ticker.tick().await;
            if let Err(e) = flush_telemetry(state_for_flush.clone()).await {
                eprintln!("❌ Flush error: {}", e);
            }
        }
    });

    // Wait for both tasks
    tokio::select! {
        _ = listener_handle => println!("Listener task exited"),
        _ = flush_handle => println!("Flush task exited"),
    }

    Ok(())
}

async fn handle_telemetry_event(state: Arc<Mutex<AgentState>>, event: TelemetryEvent) {
    let mut state = state.lock().await;

    let (pipeline_id, api_event) = match event {
        TelemetryEvent::Lifecycle(e) => {
            let payload = serde_json::json!({
                "event": e.event,
                "uptime_ms": e.uptime_ms,
                "metadata": e.metadata,
            });
            (
                e.pipeline_id,
                ApiEvent {
                    event_type: "LIFECYCLE".to_string(),
                    timestamp: e.timestamp as i64,
                    payload,
                },
            )
        }
        TelemetryEvent::Progress(e) => {
            let payload = serde_json::json!({
                "current": e.current,
                "total": e.total,
                "percent": e.percent,
                "eta_seconds": e.eta_seconds,
            });
            (
                e.pipeline_id,
                ApiEvent {
                    event_type: "PROGRESS".to_string(),
                    timestamp: now_secs() as i64,
                    payload,
                },
            )
        }
        TelemetryEvent::Metric(e) => {
            let payload = serde_json::json!({
                "name": e.name,
                "value": e.value,
            });
            (
                e.pipeline_id,
                ApiEvent {
                    event_type: "METRIC".to_string(),
                    timestamp: now_secs() as i64,
                    payload,
                },
            )
        }
    };

    // Get or create pipeline state
    let pipeline = state.pipelines.entry(pipeline_id.clone()).or_insert_with(|| {
        println!("🆕 New pipeline registered: {}", pipeline_id);
        PipelineState {
            events: Vec::new(),
            last_seen: Instant::now(),
            cpu_nano: 0,
            mem_bytes: 0,
        }
    });

    pipeline.events.push(api_event);
    pipeline.last_seen = Instant::now();

    // Simple resource estimation (in real impl, use sysinfo crate)
    pipeline.cpu_nano = estimate_cpu_usage();
    pipeline.mem_bytes = estimate_memory_usage();
}

async fn flush_telemetry(state: Arc<Mutex<AgentState>>) -> anyhow::Result<()> {
    let mut state = state.lock().await;

    if state.pipelines.is_empty() {
        return Ok(());
    }

    let mut flushed_count = 0;
    let mut stale_pipelines = Vec::new();

    for (pipeline_id, pipeline) in state.pipelines.iter_mut() {
        // Skip if no new events
        if pipeline.events.is_empty() {
            // Check for stale pipelines (no heartbeat for 60s)
            if pipeline.last_seen.elapsed() > Duration::from_secs(60) {
                stale_pipelines.push(pipeline_id.clone());
            }
            continue;
        }

        // Build payload
        let payload = AgentPayload {
            pipeline_id: pipeline_id.clone(),
            events: std::mem::take(&mut pipeline.events),
            stats: Some(ResourceStats {
                cpu_nano: pipeline.cpu_nano,
                mem_bytes: pipeline.mem_bytes,
            }),
        };

        // Send to API
        match state.client.post(&state.api_url).json(&payload).send().await {
            Ok(resp) if resp.status().is_success() => {
                flushed_count += 1;
            }
            Ok(resp) => {
                eprintln!("⚠️  API returned {} for {}", resp.status(), pipeline_id);
            }
            Err(e) => {
                eprintln!("❌ Failed to send telemetry for {}: {}", pipeline_id, e);
                // Put events back for retry
                pipeline.events = payload.events;
            }
        }
    }

    // Remove stale pipelines
    for id in stale_pipelines {
        println!("💀 Pipeline {} removed (stale)", id);
        state.pipelines.remove(&id);
    }

    if flushed_count > 0 {
        println!("✅ Flushed {} pipelines to API", flushed_count);
    }

    Ok(())
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

// Placeholder for CPU estimation - replace with sysinfo crate
fn estimate_cpu_usage() -> i64 {
    // In real implementation, use sysinfo::System
    0
}

// Placeholder for memory estimation - replace with sysinfo crate
fn estimate_memory_usage() -> i64 {
    // In real implementation, use sysinfo::System
    0
}