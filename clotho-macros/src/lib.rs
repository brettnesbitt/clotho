use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, ItemFn};

#[proc_macro_attribute]
pub fn main(_args: TokenStream, item: TokenStream) -> TokenStream {
    let input_fn = parse_macro_input!(item as ItemFn);
    
    // 1. Inspect the function signature
    let fn_name = &input_fn.sig.ident;
    let inputs = &input_fn.sig.inputs;
    let body = &input_fn.block;
    let vis = &input_fn.vis;
    let sig = &input_fn.sig;

    // 2. Logic: Daemon vs. Webhook
    // If the function is named "main" and takes 0 arguments -> Daemon (Stream/Batch)
    // If the function takes arguments (e.g., req: Request) -> Webhook (HTTP)
    
    if fn_name.to_string() == "main" && inputs.is_empty() {
        impl_daemon_entrypoint(input_fn)
    } else {
        impl_webhook_entrypoint(input_fn)
    }
}

/// Helper: generates the execution report POST logic used by both entrypoints.
/// Posts directly to CLOTHO_API_URL/v1/executions (Control Plane API).
/// Falls back to agent at 127.0.0.1:8126 if CLOTHO_API_URL is not set.
fn report_post_block() -> proc_macro2::TokenStream {
    quote! {
        if let Some(report_json) = ::clotho::telemetry::execution_report_json() {
            let exec_url = match std::env::var("CLOTHO_API_URL") {
                Ok(base) => format!("{}/v1/executions", base.trim_end_matches('/')),
                Err(_) => "http://127.0.0.1:8126/v1/execution".to_string(),
            };
            let req = spin_sdk::http::Request::builder()
                .method(spin_sdk::http::Method::Post)
                .uri(&exec_url)
                .header("content-type", "application/json")
                .body(report_json)
                .build();
            let _: Result<spin_sdk::http::Response, _> = spin_sdk::http::send(req).await;
        }
    }
}

/// Generates a Spin HTTP component wrapper for pipeline-style (main) functions.
/// The pipeline runs on each HTTP request and returns results as the response body.
fn impl_daemon_entrypoint(input_fn: ItemFn) -> TokenStream {
    let body = &input_fn.block;
    let post_report = report_post_block();

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

            // C. POST execution report to Control Plane API (or agent as fallback)
            #post_report

            // D. Build HTTP Response
            match &result {
                Ok(_) => {
                    Ok(spin_sdk::http::Response::builder()
                        .status(200)
                        .header("content-type", "text/plain")
                        .body(format!("Pipeline completed in {}ms", duration))
                        .build())
                },
                Err(e) => {
                    // Set a failure report if the runners didn't already
                    ::clotho::telemetry::set_execution_report(::clotho::telemetry::ExecutionReport {
                        pipeline_id: pipeline_id.clone(),
                        duration_ms: duration,
                        status: "failed".into(),
                        log_lines: vec![e.to_string()],
                        ..Default::default()
                    });
                    // Try to POST the failure report too
                    #post_report
                    Ok(spin_sdk::http::Response::builder()
                        .status(500)
                        .header("content-type", "text/plain")
                        .body(format!("Pipeline failed: {}", e))
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
    let post_report = report_post_block();

    let expanded = quote! {
        #[spin_sdk::http_component]
        async fn _clotho_generated_handler(#inputs) -> anyhow::Result<impl spin_sdk::http::IntoResponse> {
            // A. Static Init
            ::clotho::telemetry::mark_birth();
            let pipeline_id = std::env::var("CLOTHO_PIPELINE_ID").unwrap_or_else(|_| "http-trigger".into());

            // B. Run User Code (Measured — runners set ExecutionReport internally)
            let start = std::time::Instant::now();
            let result = async { #body }.await;
            let duration = start.elapsed().as_millis() as u64;

            // C. POST execution report to Control Plane API (or agent as fallback)
            #post_report

            result
        }
    };

    TokenStream::from(expanded)
}