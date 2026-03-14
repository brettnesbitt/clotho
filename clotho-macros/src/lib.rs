use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, ItemFn};

// =============================================================================
// #[clotho::job]  — Serverless, Webhooks, Cron, Batch processing (Spin/WASM)
// =============================================================================
//
// The CLI reads this attribute to know: `cargo build --target wasm32-wasi` + Spin manifest.
//
// Sub-variants (inferred by signature):
//   async fn main() -> Result<()>         → Pipeline job (stream/batch/once)
//   async fn main(req: Request) -> ...    → Webhook handler (one-shot HTTP trigger)

#[proc_macro_attribute]
pub fn job(_args: TokenStream, item: TokenStream) -> TokenStream {
    let input_fn = parse_macro_input!(item as ItemFn);
    let inputs = &input_fn.sig.inputs;

    if inputs.is_empty() {
        impl_job_pipeline(input_fn)
    } else {
        impl_job_webhook(input_fn)
    }
}

/// Pipeline job: wraps user code in a Spin HTTP component that returns the
/// execution report as JSON in the response body. The operator forwards it
/// to the Control Plane API — no outbound networking needed from WASM.
fn impl_job_pipeline(input_fn: ItemFn) -> TokenStream {
    let body = &input_fn.block;

    let expanded = quote! {
        #[spin_sdk::http_component]
        async fn _clotho_generated_handler(
            _req: spin_sdk::http::Request,
        ) -> anyhow::Result<impl spin_sdk::http::IntoResponse> {
            // A. Static Init
            ::clotho::telemetry::mark_birth();
            let pipeline_id = std::env::var("CLOTHO_PIPELINE_ID")
                .unwrap_or_else(|_| "pipeline".into());

            // B. Run User Pipeline Code (runners set ExecutionReport internally)
            let start = std::time::Instant::now();
            let result: anyhow::Result<()> = async { #body }.await;
            let duration = start.elapsed().as_millis() as u64;

            // C. Build response with execution report in body
            // The operator parses this JSON and forwards to the Control Plane API.
            match &result {
                Ok(_) => {
                    let report_json = ::clotho::telemetry::execution_report_json()
                        .unwrap_or_else(|| format!(
                            r#"{{"pipeline_id":"{}","duration_ms":{},"status":"completed","records_in":0,"records_out":0,"records_failed":0,"bytes_processed":0,"log_lines":[]}}"#,
                            pipeline_id, duration
                        ).into_bytes());
                    Ok(spin_sdk::http::Response::builder()
                        .status(200)
                        .header("content-type", "application/json")
                        .header("x-clotho-execution", "true")
                        .body(report_json)
                        .build())
                },
                Err(e) => {
                    ::clotho::telemetry::set_execution_report(::clotho::telemetry::ExecutionReport {
                        pipeline_id: pipeline_id.clone(),
                        duration_ms: duration,
                        status: "failed".into(),
                        log_lines: vec![e.to_string()],
                        ..Default::default()
                    });
                    let report_json = ::clotho::telemetry::execution_report_json()
                        .unwrap_or_else(|| format!(
                            r#"{{"pipeline_id":"{}","duration_ms":{},"status":"failed","records_in":0,"records_out":0,"records_failed":0,"bytes_processed":0,"log_lines":["{}"]}}"#,
                            pipeline_id, duration, e
                        ).into_bytes());
                    Ok(spin_sdk::http::Response::builder()
                        .status(500)
                        .header("content-type", "application/json")
                        .header("x-clotho-execution", "true")
                        .body(report_json)
                        .build())
                }
            }
        }
    };

    TokenStream::from(expanded)
}

/// Webhook job: wraps user code in a Spin HTTP component for one-shot triggers.
fn impl_job_webhook(input_fn: ItemFn) -> TokenStream {
    let body = &input_fn.block;
    let inputs = &input_fn.sig.inputs;

    let expanded = quote! {
        #[spin_sdk::http_component]
        async fn _clotho_generated_handler(#inputs) -> anyhow::Result<impl spin_sdk::http::IntoResponse> {
            // A. Static Init
            ::clotho::telemetry::mark_birth();
            let pipeline_id = std::env::var("CLOTHO_PIPELINE_ID").unwrap_or_else(|_| "http-trigger".into());

            // B. Run User Code (Measured — runners set ExecutionReport internally)
            let start = std::time::Instant::now();
            let result = async { #body }.await;
            let _duration = start.elapsed().as_millis() as u64;

            result
        }
    };

    TokenStream::from(expanded)
}

// =============================================================================
// #[clotho::daemon]  — 24/7 Firehoses, Kafka consumers, continuous streaming
// =============================================================================
//
// The CLI reads this attribute to know: standard `cargo build` + Dockerfile.
//
// Generates a #[tokio::main] entrypoint with:
//   1. Automatic telemetry agent initialization (UDP + HTTP)
//   2. Graceful shutdown on SIGINT/SIGTERM
//   3. Execution report POST to the Control Plane API on exit

#[proc_macro_attribute]
pub fn daemon(_args: TokenStream, item: TokenStream) -> TokenStream {
    let input_fn = parse_macro_input!(item as ItemFn);
    let body = &input_fn.block;

    let expanded = quote! {
        #[tokio::main]
        async fn main() {
            // ── A. Telemetry Bootstrap ──────────────────────────────────
            ::clotho::telemetry::mark_birth();
            let pipeline_id = std::env::var("CLOTHO_PIPELINE_ID")
                .unwrap_or_else(|_| "daemon".into());
            let api_url = std::env::var("CLOTHO_API_URL")
                .unwrap_or_else(|_| "http://clotho-api.clotho-control.svc.cluster.local:3000".into());

            eprintln!("[Clotho] Daemon starting: {}", pipeline_id);
            ::clotho::telemetry::emit_lifecycle(&pipeline_id, "STARTUP", Some(::clotho::telemetry::uptime_ms()), None);

            // ── A2. Replay Listener (Canary testing from Control Plane) ──
            ::clotho::daemon_support::spawn_replay_listener();

            // ── B. Graceful Shutdown Signal ─────────────────────────────
            let shutdown = ::clotho::daemon_support::shutdown_signal();

            // ── C. Run User Code (race against shutdown) ────────────────
            let start = std::time::Instant::now();

            let result: ::clotho::Result<()> = tokio::select! {
                res = async { #body } => res,
                _ = shutdown => {
                    eprintln!("[Clotho] Shutdown signal received, draining...");
                    Ok(())
                }
            };

            // ── D. Report Execution to Control Plane ────────────────────
            let duration_ms = start.elapsed().as_millis() as u64;
            let status = if result.is_ok() { "completed" } else { "failed" };
            let log_lines: Vec<String> = if let Err(ref e) = result {
                vec![e.to_string()]
            } else {
                vec![]
            };

            eprintln!("[Clotho] Daemon {} after {}ms ({})", pipeline_id, duration_ms, status);
            ::clotho::telemetry::emit_lifecycle_with_runtime(
                &pipeline_id, "FINISHED", None, None, Some(duration_ms),
            );

            // Prefer the SDK-collected report (from StreamPipeline::run), fall back to wrapper report
            let report = ::clotho::telemetry::take_execution_report().unwrap_or_else(|| {
                ::clotho::telemetry::ExecutionReport {
                    pipeline_id: pipeline_id.clone(),
                    mode: "stream".into(),
                    started_at: String::new(),
                    duration_ms,
                    status: status.into(),
                    records_in: 0,
                    records_out: 0,
                    records_failed: 0,
                    bytes_processed: 0,
                    log_lines: log_lines.clone(),
                }
            });

            // Fire-and-forget POST to the API (5s timeout, never panic)
            if let Ok(client) = reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(5))
                .build()
            {
                let url = format!("{}/v1/executions", api_url);
                match serde_json::to_value(&report) {
                    Ok(body) => { let _ = client.post(&url).json(&body).send().await; }
                    Err(_) => {}
                }
            }

            if let Err(e) = result {
                eprintln!("[Clotho] Fatal: {:#}", e);
                std::process::exit(1);
            }
        }
    };

    TokenStream::from(expanded)
}

// =============================================================================
// #[clotho::main]  — DEPRECATED: use #[clotho::job] or #[clotho::daemon]
// =============================================================================
//
// Kept for backwards compatibility. Routes to #[clotho::job].

#[proc_macro_attribute]
pub fn main(_args: TokenStream, item: TokenStream) -> TokenStream {
    job(TokenStream::new(), item)
}