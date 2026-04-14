use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, ItemFn};

/// The "Smart" macro that auto-detects based on function signature.
/// Kept for backward compatibility.
#[proc_macro_attribute]
pub fn main(_args: TokenStream, item: TokenStream) -> TokenStream {
    let input_fn = parse_macro_input!(item as ItemFn);
    
    // 1. Inspect the function signature
    let fn_name = &input_fn.sig.ident;
    let inputs = &input_fn.sig.inputs;

    // 2. Logic: Daemon vs. Webhook
    // If the function is named "main" and takes 0 arguments -> Daemon (Stream/Batch)
    // If the function takes arguments (e.g., req: Request) -> Webhook (HTTP)
    
    if fn_name.to_string() == "main" && inputs.is_empty() {
        impl_daemon_entrypoint(input_fn)
    } else {
        impl_webhook_entrypoint(input_fn)
    }
}

/// Explicit Wasm job macro - generates Spin HTTP component.
/// Use this for short-lived, request-response workloads (webhooks, batch jobs).
#[proc_macro_attribute]
pub fn job(_args: TokenStream, item: TokenStream) -> TokenStream {
    let input_fn = parse_macro_input!(item as ItemFn);
    impl_webhook_entrypoint(input_fn)
}

/// Explicit native daemon macro - generates standard Tokio runtime.
/// Use this for long-lived processes (WebSocket firehoses, streaming daemons).
#[proc_macro_attribute]
pub fn daemon(_args: TokenStream, item: TokenStream) -> TokenStream {
    let input_fn = parse_macro_input!(item as ItemFn);
    impl_native_daemon_entrypoint(input_fn)
}

/// Generates a Spin HTTP component wrapper for pipeline-style (main) functions.
/// The execution report is returned as JSON in the response body so the operator
/// can parse it and forward to the Control Plane API. This avoids WASM outbound
/// networking issues entirely.
fn impl_daemon_entrypoint(input_fn: ItemFn) -> TokenStream {
    let body = &input_fn.block;

    let expanded = quote! {
        #[spin_sdk::http_component]
        async fn _clotho_generated_handler(
            _req: spin_sdk::http::Request,
        ) -> anyhow::Result<impl spin_sdk::http::IntoResponse> {
            // A. Static Init
            ::clotho::telemetry::mark_birth();
            let pipeline_id = ::clotho::config::var("CLOTHO_PIPELINE_ID")
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
                    eprintln!("[Clotho] Pipeline FAILED: {:#}", e);
                    // Set a failure report if the runners didn't already
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

/// Generates the Spin Component wrapper for One-Shot triggers
fn impl_webhook_entrypoint(input_fn: ItemFn) -> TokenStream {
    let body = &input_fn.block;
    let inputs = &input_fn.sig.inputs;

    let expanded = quote! {
        #[spin_sdk::http_component]
        async fn _clotho_generated_handler(#inputs) -> anyhow::Result<impl spin_sdk::http::IntoResponse> {
            // A. Static Init
            ::clotho::telemetry::mark_birth();
            let pipeline_id = ::clotho::config::var("CLOTHO_PIPELINE_ID").unwrap_or_else(|_| "http-trigger".into());

            // B. Run User Code (Measured — runners set ExecutionReport internally)
            let start = std::time::Instant::now();
            let result = async { #body }.await;
            let duration = start.elapsed().as_millis() as u64;

            result
        }
    };

    TokenStream::from(expanded)
}

/// Generates a standard Tokio runtime for native Kubernetes pods.
/// Use this for long-lived processes like WebSocket firehoses that need
/// persistent connections and can't work in Wasm's request-response model.
fn impl_native_daemon_entrypoint(input_fn: ItemFn) -> TokenStream {
    let body = &input_fn.block;

    let expanded = quote! {
        #[tokio::main]
        async fn main() -> anyhow::Result<()> {
            // Initialize native telemetry agent (UDP to DaemonSet)
            ::clotho::telemetry::init_native_agent();
            
            let pipeline_id = std::env::var("CLOTHO_PIPELINE_ID")
                .unwrap_or_else(|_| "daemon".into());
            
            eprintln!("[Clotho] Native daemon starting: {}", pipeline_id);
            
            // Run the daemon loop
            let result: anyhow::Result<()> = async { #body }.await;
            
            match &result {
                Ok(_) => {
                    eprintln!("[Clotho] Daemon completed successfully");
                }
                Err(e) => {
                    eprintln!("[Clotho] Daemon failed: {}", e);
                }
            }
            
            result
        }
    };

    TokenStream::from(expanded)
}
