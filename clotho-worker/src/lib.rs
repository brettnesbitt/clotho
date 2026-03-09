mod telemetry; // <--- Register the module

use spin_sdk::http::{IntoResponse, Request, Response};
use spin_sdk::http_component;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize)]
struct IncomingData {
    id: String,
    payload: String, 
}

#[http_component]
fn handle_clotho_worker(req: Request) -> anyhow::Result<impl IntoResponse> {
    // 1. Initialize Stats
    // In a real app, get this ID from the env var (injected by the Operator)
    let pipeline_id = std::env::var("PIPELINE_ID").unwrap_or("unknown-pipe".to_string());
    let mut stats = telemetry::MetricPacket::new(&pipeline_id);

    // 2. Measure Input
    let body_len = req.body().len() as u64;
    stats.bytes_processed += body_len;

    // 3. Process Logic
    let response_body = match serde_json::from_slice::<IncomingData>(req.body()) {
        Ok(data) => {
            stats.records_processed += 1; // Assuming 1 record per request for now
            // ... Logic goes here ...
            serde_json::to_vec(&data)?
        },
        Err(_) => {
            stats.error_count += 1;
            b"Error".to_vec()
        }
    };

    // 4. FIRE THE HOOK (Async / Non-blocking effectively via UDP)
    telemetry::emit(&stats);

    Ok(Response::builder()
        .status(200)
        .body(response_body)
        .build())
}