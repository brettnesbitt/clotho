// clotho-sdk/src/stream.rs
use crate::traits::{Source, Sink, Context};
use crate::telemetry;
use anyhow::Result;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

// Internal sentinel values for non-error pipeline control flow.
// These are returned as Err() from transforms but are NOT failures —
// the engine intercepts them to implement filter/branch semantics.
const BRANCH_SENTINEL: &str = "__clotho_branched__";
const FILTER_SENTINEL: &str = "__clotho_filtered__";

fn is_control_flow(e: &anyhow::Error) -> bool {
    let msg = e.to_string();
    msg == BRANCH_SENTINEL || msg == FILTER_SENTINEL
}

// The Error now hands ownership of the Context back to the engine!
type AsyncTransformFn<T> = Box<dyn Fn(Context<T>) -> Pin<Box<dyn Future<Output = Result<Context<T>, (anyhow::Error, Context<T>)>> + Send>> + Send + Sync>;

/// Free function so the compiler sees T: Clone in its own generic scope.
/// (Rust can't propagate a method-level Clone bound into a closure that gets
/// type-erased to AsyncTransformFn<T> whose T only has Send+Sync.)
fn make_branch_transform<T, F, K>(predicate: F, sink: K) -> AsyncTransformFn<T>
where
    T: Send + Sync + Clone + 'static,
    F: Fn(&T) -> bool + Send + Sync + 'static,
    K: Sink<T> + 'static,
{
    let sink = Arc::new(tokio::sync::Mutex::new(sink));
    Box::new(move |ctx: Context<T>| {
        let matches = predicate(&ctx.data);
        let sink = Arc::clone(&sink);
        Box::pin(async move {
            if matches {
                let branch_ctx = Context {
                    data: ctx.data.clone(),
                    span_id: ctx.span_id.clone(),
                    parents: ctx.parents.clone(),
                    meta: ctx.meta.clone(),
                };
                let mut sink = sink.lock().await;
                if let Err(e) = sink.write(branch_ctx).await {
                    eprintln!("[Clotho] Branch sink write failed: {}", e);
                }
                Err((anyhow::anyhow!(BRANCH_SENTINEL), ctx))
            } else {
                Ok(ctx)
            }
        })
    })
}

pub struct StreamPipeline<S, T> {
    source: S,
    transforms: Vec<AsyncTransformFn<T>>,
    tee_sinks: Vec<Box<dyn Sink<T>>>,
}

impl<S, T> StreamPipeline<S, T> 
where 
    S: Source<T> + 'static,
    T: Send + Sync + 'static 
{
    pub fn new(source: S) -> Self {
        Self { 
            source, 
            transforms: Vec::new(),
            tee_sinks: Vec::new(),
        }
    }

    /// Pass-through Sink (Observer Pattern)
    /// Takes a borrowed reference to the data, writes it to the sink asynchronously,
    /// and passes the original data down the pipeline without consuming it.
    /// This is like a T-junction in plumbing - data flows to both the sink and the next stage.
    pub fn tee<K>(mut self, sink: K) -> Self 
    where 
        K: Sink<T> + 'static,
        T: Clone, // Required for tee since we need to pass data to both sink and next stage
    {
        self.tee_sinks.push(Box::new(sink));
        self
    }

    /// Filter: records matching the predicate continue through the pipeline.
    /// Non-matching records are silently dropped — no DLQ, no error.
    pub fn filter<F>(mut self, predicate: F) -> Self
    where F: Fn(&T) -> bool + Send + Sync + 'static
    {
        self.transforms.push(Box::new(move |ctx: Context<T>| {
            let keep = predicate(&ctx.data);
            Box::pin(async move {
                if keep {
                    Ok(ctx)
                } else {
                    Err((anyhow::anyhow!(FILTER_SENTINEL), ctx))
                }
            })
        }));
        self
    }

    /// Conditional Branch: records matching the predicate are routed to the
    /// branch sink and removed from the main pipeline. Records not matching
    /// continue through the remaining transforms to the main sink.
    /// This is the selective version of tee() — only matching records are forked.
    pub fn branch<F, K>(mut self, predicate: F, sink: K) -> Self
    where
        F: Fn(&T) -> bool + Send + Sync + 'static,
        K: Sink<T> + 'static,
        T: Clone,
    {
        self.transforms.push(make_branch_transform(predicate, sink));
        self
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
        T: serde::Serialize + Clone
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
        let mut records_branched: u64 = 0;
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
                    
                    // Estimate bytes processed by serializing the payload
                    // This gives us a rough measure of data volume flowing through
                    if let Ok(json) = serde_json::to_string(&initial_ctx.data) {
                        bytes_processed += json.len() as u64;
                    }
                    
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
                                if is_control_flow(&e) {
                                    // Record was branched or filtered — not a failure.
                                    // It was either routed to a branch sink or silently dropped.
                                    records_branched += 1;
                                    current = None;
                                    break;
                                }
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

                    // ROUTE TO TEE SINKS (Pass-through observers)
                    if let Some(ctx) = &current {
                        for tee_sink in &mut self.tee_sinks {
                            // Clone the context for tee sinks (they are observers)
                            let tee_ctx = Context {
                                data: ctx.data.clone(),
                                span_id: ctx.span_id.clone(),
                                parents: ctx.parents.clone(),
                                meta: ctx.meta.clone(),
                            };
                            
                            if let Err(e) = tee_sink.write(tee_ctx).await {
                                // Tee sink failures are logged but don't fail the pipeline
                                eprintln!("[Clotho] Tee sink write failed: {}", e);
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
                eprintln!("[Clotho] Progress: {} in, {} out, {} branched, {} failed", records_in, records_out, records_branched, records_failed);
                telemetry::emit_throughput(&pipeline_id, records_in, records_out, records_failed, bytes_processed);
                batch_counter = 0;
            }
        }

        // Final flush
        let elapsed = start_time.elapsed();
        let runtime_micros = elapsed.as_micros() as u64;
        let runtime_ms = (runtime_micros + 999) / 1000; // Round up to nearest ms, minimum 1ms
        eprintln!("[Clotho] Pipeline End: {} records in, {} records out, {} branched, {} failed ({}µs / {}ms)", 
                  records_in, records_out, records_branched, records_failed, runtime_micros, runtime_ms);
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