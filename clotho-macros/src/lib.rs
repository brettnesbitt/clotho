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

/// Generates the Tokio Runtime wrapper for Long-Running processes
fn impl_daemon_entrypoint(input_fn: ItemFn) -> TokenStream {
    let body = &input_fn.block;

    let expanded = quote! {
        fn main() -> anyhow::Result<()> {
            // A. Static Init (Telemetry Clock)
            ::clotho::telemetry::mark_birth();
            
            // B. Build Tokio Runtime (Current Thread is sufficient for Wasm)
            // We expect the user to have 'clotho' in their dependencies
            let rt = ::tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("Failed to build Tokio runtime");

            // C. Emit Startup Telemetry
            // We can't use async here yet, so we just set the static start time.
            // The Pipeline::run() method inside the body will handle the actual "STARTUP" event emission.

            // D. Run User Code
            rt.block_on(async {
                #body
            })
        }
    };

    TokenStream::from(expanded)
}

/// Generates the Spin Component wrapper for One-Shot triggers
fn impl_webhook_entrypoint(input_fn: ItemFn) -> TokenStream {
    let body = &input_fn.block;
    let inputs = &input_fn.sig.inputs;

    // We assume the first argument is the Request
    // and the return type is Result<Response>
    
    let expanded = quote! {
        // This attribute tells Spin "Here is the entry point"
        #[spin_sdk::http_component]
        async fn _clotho_generated_handler(#inputs) -> anyhow::Result<impl spin_sdk::http::IntoResponse> {
            // A. Static Init
            ::clotho::telemetry::mark_birth();
            let pipeline_id = std::env::var("CLOTHO_PIPELINE_ID").unwrap_or_else(|_| "http-trigger".into());

            // B. Emit Startup
            let boot_ms = ::clotho::telemetry::uptime_ms();
            ::clotho::telemetry::emit_lifecycle(&pipeline_id, "STARTUP", Some(boot_ms), None);

            // C. Run User Code (Measured)
            let start = std::time::Instant::now();
            
            // We wrap the user body in an async block to capture the result
            let result = async { #body }.await;
            
            let duration = start.elapsed().as_millis() as u64;

            // D. Emit Result Telemetry
            match &result {
                Ok(_) => {
                    ::clotho::telemetry::emit_lifecycle(&pipeline_id, "FINISHED", None, Some(duration));
                },
                Err(e) => {
                    ::clotho::telemetry::emit_error(&pipeline_id, "HTTP_FAIL", &e.to_string());
                }
            }

            result
        }
    };

    TokenStream::from(expanded)
}