// clotho-sdk/src/stream.rs
use crate::traits::{Source, Sink, Context};
use crate::telemetry;
use anyhow::Result;
use std::future::Future;
use std::pin::Pin;

// The Error now hands ownership of the Context back to the engine!
type AsyncTransformFn<T> = Box<dyn Fn(Context<T>) -> Pin<Box<dyn Future<Output = Result<Context<T>, (anyhow::Error, Context<T>)>> + Send>> + Send + Sync>;

pub struct StreamPipeline<S, T> {
    source: S,
    transforms: Vec<AsyncTransformFn<T>>,
}

impl<S, T> StreamPipeline<S, T> 
where 
    S: Source<T> + 'static,
    T: Send + Sync + 'static 
{
    pub fn new(source: S) -> Self {
        Self { source, transforms: Vec::new() }
    }

    /// Synchronous Transform (CPU Bound - Math, Parsing, Filtering)
    /// Uses the Ownership Return Pattern: If it fails, the user's closure MUST return 
    /// the data in the Err() tuple so the engine can route it to the DLQ safely.
    pub fn map<F>(mut self, op: F) -> Self 
    where F: Fn(T) -> Result<T, (anyhow::Error, T)> + Send + Sync + 'static 
    {
        self.transforms.push(Box::new(move |ctx: Context<T>| {
            // Destructure the context to pass just the data
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

    /// Asynchronous Transform (I/O Bound - DB Lookups, API calls)
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

    pub async fn run<K>(mut self, mut sink: K) -> Result<()> 
    where 
        K: Sink<T>,
        T: serde::Serialize // Required for DLQ serialization. Notice: Clone is NO LONGER required!
    {
        let pipeline_id = std::env::var("PIPELINE_ID")
            .or_else(|_| std::env::var("CLOTHO_PIPELINE_ID"))
            .unwrap_or_else(|_| "unknown".into());

        telemetry::mark_birth();
        let boot_ms = telemetry::uptime_ms();
        let start_time = std::time::Instant::now();

        eprintln!("[Clotho] Pipeline Started: {}", pipeline_id);
        eprintln!("[Clotho] Mode: Stream (item-by-item, zero-copy)");
        telemetry::emit_lifecycle(&pipeline_id, "STARTUP", Some(boot_ms), None);

        let mut records_in: u64 = 0;
        let mut records_out: u64 = 0;
        let mut records_failed: u64 = 0;
        let mut bytes_processed: u64 = 0;
        let mut batch_counter: u64 = 0;
        let mut first_record = true;

        while let Some(ctx_result) = self.source.next().await {
            if first_record {
                eprintln!("[Clotho] First record received from source");
                first_record = false;
            }

            match ctx_result {
                Ok(initial_ctx) => {
                    records_in += 1;
                    
                    let trace_id = initial_ctx.span_id.clone();
                    
                    // We wrap the context in an Option so we can safely take() ownership
                    // and pass it through the transform chain without cloning.
                    let mut current = Some(initial_ctx);

                    // EXECUTE TRANSFORMS (Zero-Copy Ownership Transfer)
                    for op in &self.transforms {
                        let ctx_to_process = current.take().unwrap();
                        
                        match op(ctx_to_process).await {
                            Ok(new_ctx) => current = Some(new_ctx),
                            Err((e, failed_ctx)) => {
                                // We got the context back from the failed transform!
                                records_failed += 1;
                                let error_msg = e.to_string();
                                eprintln!("[Clotho] Transform failed: {} (record routed to DLQ)", error_msg);
                                
                                // Serialize ONLY on the sad path! 
                                let payload_str = serde_json::to_string(&failed_ctx.data)
                                    .unwrap_or_else(|_| "Serialization failed".to_string());
                                
                                crate::telemetry::emit_dlq_record(
                                    &pipeline_id,
                                    &trace_id,
                                    "transform",
                                    &error_msg,
                                    &payload_str,
                                );
                                break; 
                            }
                        }
                    }

                    // ROUTE TO MAIN SINK
                    if let Some(ctx) = current {
                        records_out += 1;
                        if let Err(e) = sink.write(ctx).await {
                            records_failed += 1;
                            eprintln!("[Clotho] Sink write failed: {}", e);
                            return Err(e);
                        }
                    }
                }
                Err(e) => {
                    records_failed += 1;
                    eprintln!("[Clotho] Source error: {}", e);
                }
            }

            // Emit throughput every 100 records
            batch_counter += 1;
            if batch_counter >= 100 {
                eprintln!("[Clotho] Progress: {} in, {} out, {} failed", records_in, records_out, records_failed);
                telemetry::emit_throughput(&pipeline_id, records_in, records_out, records_failed, bytes_processed);
                batch_counter = 0;
            }
        }

        // Final flush
        let elapsed = start_time.elapsed();
        let runtime_micros = elapsed.as_micros() as u64;
        let runtime_ms = (runtime_micros + 999) / 1000; // Round up to nearest ms, minimum 1ms
        eprintln!("[Clotho] Pipeline End: {} records in, {} records out, {} failed ({}µs / {}ms)", 
                  records_in, records_out, records_failed, runtime_micros, runtime_ms);
        telemetry::emit_throughput(&pipeline_id, records_in, records_out, records_failed, bytes_processed);
        telemetry::emit_lifecycle_with_runtime(&pipeline_id, "FINISHED", None, None, Some(runtime_ms));

        // Store execution report for the macro to POST via HTTP
        telemetry::set_execution_report(crate::telemetry::ExecutionReport {
            pipeline_id: pipeline_id.clone(),
            started_at: String::new(),
            duration_ms: runtime_ms,
            status: "completed".into(),
            records_in,
            records_out,
            records_failed,
            bytes_processed,
            log_lines: vec![],
        });

        Ok(())
    }
}