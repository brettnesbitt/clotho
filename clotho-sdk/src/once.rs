// clotho-sdk/src/once.rs
use crate::traits::{Sink, Context};
use crate::telemetry;
use anyhow::Result;
use std::future::Future;
use std::pin::Pin;

// Matches the exact signature from stream.rs: Returns ownership of data on Err
type AsyncTransformFn<T> = Box<dyn Fn(Context<T>) -> Pin<Box<dyn Future<Output = Result<Context<T>, (anyhow::Error, Context<T>)>> + Send>> + Send + Sync>;

/// Optimized for Webhooks / Serverless HTTP Triggers
pub struct OncePipeline<T> {
    payload: T,
    transforms: Vec<AsyncTransformFn<T>>,
}

impl<T> OncePipeline<T> 
where T: Send + Sync + 'static 
{
    pub fn new(payload: T) -> Self {
        telemetry::mark_birth();
        Self {
            payload,
            transforms: Vec::new(),
        }
    }

    /// Pure logic mapping. Uses the Zero-Copy Ownership Return pattern.
    pub fn map<F>(mut self, op: F) -> Self 
    where F: Fn(T) -> Result<T, (anyhow::Error, T)> + Send + Sync + 'static 
    {
        self.transforms.push(Box::new(move |ctx: Context<T>| {
            let Context { data, span_id, parents, meta } = ctx;
            let result = op(data);
            
            Box::pin(async move {
                match result {
                    Ok(new_data) => Ok(Context { data: new_data, span_id, parents, meta }),
                    Err((e, old_data)) => Err((e, Context { data: old_data, span_id, parents, meta }))
                }
            })
        }));
        self
    }

    /// Async mapping for API calls (e.g., verifying a Stripe webhook signature)
    pub fn map_async<F, Fut>(mut self, op: F) -> Self 
    where 
        F: Fn(Context<T>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Context<T>, (anyhow::Error, Context<T>)>> + Send + 'static
    {
        self.transforms.push(Box::new(move |ctx| {
            Box::pin(op(ctx))
        }));
        self
    }

    /// Run the pipeline and return a Result.
    /// The caller (the Spin HTTP handler) uses this Result to determine the HTTP Status Code.
    pub async fn run<K>(mut self, mut sink: K) -> Result<()> 
    where 
        K: Sink<T>,
        T: serde::Serialize // Required for DLQ
    {
        let pipeline_id = std::env::var("CLOTHO_PIPELINE_ID").unwrap_or("webhook".into());
        let boot_ms = telemetry::uptime_ms();
        let start_time = std::time::Instant::now();

        telemetry::emit_lifecycle(&pipeline_id, "STARTUP", Some(boot_ms), None);

        // 1. Contextualize (Trace ID should ideally be extracted from HTTP Headers prior to this)
        let initial_ctx = Context::root(self.payload, "once_pipeline");
        let trace_id = initial_ctx.span_id.clone();
        
        let mut current = Some(initial_ctx);

        // 2. Transform (Zero-Copy Transfer)
        for op in &self.transforms {
            let ctx_to_process = current.take().unwrap();
            
            match op(ctx_to_process).await {
                Ok(new_ctx) => current = Some(new_ctx),
                Err((e, failed_ctx)) => {
                    let error_msg = e.to_string();
                    eprintln!("[Clotho] Webhook Pipeline Failed: {}", error_msg);
                    
                    let payload_str = serde_json::to_string(&failed_ctx.data)
                        .unwrap_or_else(|_| "Serialization failed".to_string());
                    
                    telemetry::emit_dlq_record(
                        &pipeline_id,
                        &trace_id,
                        "transform",
                        &error_msg,
                        &payload_str,
                    );
                    
                    // Crucial: Return the error so the HTTP framework can return a 500 status!
                    return Err(anyhow::anyhow!("Pipeline execution halted: {}", error_msg));
                }
            }
        }

        // 3. Sink
        if let Some(ctx) = current {
            sink.write(ctx).await?;
        }

        let elapsed = start_time.elapsed();
        let runtime_micros = elapsed.as_micros() as u64;
        let runtime_ms = (runtime_micros + 999) / 1000; // Round up to nearest ms, minimum 1ms
        telemetry::emit_lifecycle_with_runtime(&pipeline_id, "FINISHED", None, None, Some(runtime_ms));

        telemetry::set_execution_report(crate::telemetry::ExecutionReport {
            pipeline_id: pipeline_id.clone(),
            started_at: String::new(),
            duration_ms: runtime_ms,
            status: "completed".into(),
            records_in: 1,
            records_out: 1,
            records_failed: 0,
            bytes_processed: 0,
            log_lines: vec![],
        });

        Ok(())
    }
}