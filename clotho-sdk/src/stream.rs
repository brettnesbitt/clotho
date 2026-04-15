// clotho-sdk/src/stream.rs
use crate::traits::{Source, Sink, Context};
use crate::telemetry::{StepInfo, StepMetrics};
use crate::telemetry;
use anyhow::Result;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use futures_util::lock::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::collections::HashMap;

// Internal sentinel values for non-error pipeline control flow.
// These are returned as Err() from transforms but are NOT failures —
// the engine intercepts them to implement filter/branch semantics.
const BRANCH_SENTINEL: &str = "__clotho_branched__";
const FILTER_SENTINEL: &str = "__clotho_filtered__";

fn is_control_flow(e: &anyhow::Error) -> bool {
    let msg = e.to_string();
    msg == BRANCH_SENTINEL || msg == FILTER_SENTINEL
}

// The Error now hands ownership of the context back to the engine!
#[cfg(not(target_family = "wasm"))]
type AsyncTransformFn<T> = Box<dyn Fn(Context<T>) -> Pin<Box<dyn Future<Output = Result<Context<T>, (anyhow::Error, Context<T>)>> + Send>> + Send + Sync>;

#[cfg(target_family = "wasm")]
type AsyncTransformFn<T> = Box<dyn Fn(Context<T>) -> Pin<Box<dyn Future<Output = Result<Context<T>, (anyhow::Error, Context<T>)>>>>>;

/// Free function so the compiler sees T: Clone in its own generic scope.
/// (Rust can't propagate a method-level Clone bound into a closure that gets
/// type-erased to AsyncTransformFn<T> whose T only has Send+Sync.)
#[cfg(not(target_family = "wasm"))]
fn make_branch_transform<T, F, K>(predicate: F, sink: K) -> AsyncTransformFn<T>
where
    T: Send + Sync + Clone + 'static,
    F: Fn(&T) -> bool + Send + Sync + 'static,
    K: Sink<T> + Send + 'static,
{
    let sink: Arc<Mutex<K>> = Arc::new(Mutex::new(sink));
    Box::new(move |ctx: Context<T>| {
        let matches = predicate(&ctx.data);
        let sink: Arc<Mutex<K>> = Arc::clone(&sink);
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

#[cfg(target_family = "wasm")]
fn make_branch_transform<T, F, K>(predicate: F, sink: K) -> AsyncTransformFn<T>
where
    T: Send + Sync + Clone + 'static,
    F: Fn(&T) -> bool + Send + Sync + 'static,
    K: Sink<T> + 'static,
{
    // WASM does not need `Send` inside the Future, but Arc<Mutex> handles interior mutability.
    let sink: Arc<Mutex<K>> = Arc::new(Mutex::new(sink));
    Box::new(move |ctx: Context<T>| {
        let matches = predicate(&ctx.data);
        let sink: Arc<Mutex<K>> = Arc::clone(&sink);
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
    transform_steps: Vec<StepInfo>, // Track step info for each transform
    tee_sinks: Vec<Box<dyn Sink<T>>>,
    tee_steps: Vec<StepInfo>, // Track step info for each tee
    step_metrics: HashMap<String, StepMetrics>, // Cumulative metrics per step
    step_last_sample: HashMap<String, AtomicU64>, // Rate limiting for data sampling
    step_counter: AtomicU64, // For auto-naming steps
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
            transform_steps: Vec::new(),
            tee_sinks: Vec::new(),
            tee_steps: Vec::new(),
            step_metrics: HashMap::new(),
            step_last_sample: HashMap::new(),
            step_counter: AtomicU64::new(0),
        }
    }

    /// Get the next step index for auto-naming
    fn next_step_idx(&self) -> u64 {
        self.step_counter.fetch_add(1, Ordering::SeqCst)
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
        let idx = self.next_step_idx();
        self.tee_sinks.push(Box::new(sink));
        self.tee_steps.push(StepInfo {
            name: format!("tee_{}", idx),
            step_type: "tee".to_string(),
        });
        self
    }

    /// Filter: records matching the predicate continue through the pipeline.
    /// Non-matching records are silently dropped — no DLQ, no error.
    pub fn filter<F>(mut self, predicate: F) -> Self
    where F: Fn(&T) -> bool + Send + Sync + 'static
    {
        let idx = self.next_step_idx();
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
        self.transform_steps.push(StepInfo {
            name: format!("filter_{}", idx),
            step_type: "filter".to_string(),
        });
        self
    }

    /// Conditional Branch: records matching the predicate are routed to the
    /// branch sink and removed from the main pipeline. Records not matching
    /// continue through the remaining transforms to the main sink.
    /// This is the selective version of tee() — only matching records are forked.
    #[cfg(not(target_family = "wasm"))]
    pub fn branch<F, K>(mut self, predicate: F, sink: K) -> Self
    where
        F: Fn(&T) -> bool + Send + Sync + 'static,
        K: Sink<T> + Send + 'static,
        T: Clone,
    {
        let idx = self.next_step_idx();
        self.transforms.push(make_branch_transform(predicate, sink));
        self.transform_steps.push(StepInfo {
            name: format!("branch_{}", idx),
            step_type: "branch".to_string(),
        });
        self
    }

    #[cfg(target_family = "wasm")]
    pub fn branch<F, K>(mut self, predicate: F, sink: K) -> Self
    where
        F: Fn(&T) -> bool + Send + Sync + 'static,
        K: Sink<T> + 'static,
        T: Clone,
    {
        let idx = self.next_step_idx();
        self.transforms.push(make_branch_transform(predicate, sink));
        self.transform_steps.push(StepInfo {
            name: format!("branch_{}", idx),
            step_type: "branch".to_string(),
        });
        self
    }

    /// Synchronous Transform (CPU Bound - Math, Parsing, Filtering)
    /// Uses the Ownership Return Pattern: If it fails, the user's closure MUST return 
    /// the data in the Err() tuple so the engine can route it to the DLQ safely.
    pub fn map<F>(mut self, op: F) -> Self 
    where F: Fn(T) -> Result<T, (anyhow::Error, T)> + Send + Sync + 'static 
    {
        let idx = self.next_step_idx();
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
        self.transform_steps.push(StepInfo {
            name: format!("map_{}", idx),
            step_type: "transform".to_string(),
        });
        self
    }

    /// Asynchronous Transform (I/O Bound - DB Lookups, API calls)
    #[cfg(not(target_family = "wasm"))]
    pub fn map_async<F, Fut>(mut self, op: F) -> Self 
    where 
        F: Fn(Context<T>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Context<T>, (anyhow::Error, Context<T>)>> + Send + 'static
    {
        let idx = self.next_step_idx();
        self.transforms.push(Box::new(move |ctx| {
            Box::pin(op(ctx))
        }));
        self.transform_steps.push(StepInfo {
            name: format!("map_async_{}", idx),
            step_type: "transform".to_string(),
        });
        self
    }

    #[cfg(target_family = "wasm")]
    pub fn map_async<F, Fut>(mut self, op: F) -> Self 
    where 
        F: Fn(Context<T>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Context<T>, (anyhow::Error, Context<T>)>> + 'static
    {
        let idx = self.next_step_idx();
        self.transforms.push(Box::new(move |ctx| {
            Box::pin(op(ctx))
        }));
        self.transform_steps.push(StepInfo {
            name: format!("map_async_{}", idx),
            step_type: "transform".to_string(),
        });
        self
    }

    pub async fn run<K>(mut self, mut sink: K) -> Result<()> 
    where 
        K: Sink<T>,
        T: serde::Serialize + Clone
    {
        let pipeline_id = crate::config::var("CLOTHO_PIPELINE_ID")
            .or_else(|_| crate::config::var("PIPELINE_ID"))
            .unwrap_or_else(|_| "unknown".into());
        let stage_name = crate::config::var_or("CLOTHO_STAGE_NAME", "");

        telemetry::mark_birth();
        let boot_ms = telemetry::uptime_ms();
        let start_time = std::time::Instant::now();
        let started_at = crate::telemetry::now_rfc3339();

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
                    for (idx, op) in self.transforms.iter().enumerate() {
                        let step_info = self.transform_steps.get(idx);
                        let step_name = step_info.map(|s| s.name.as_str()).unwrap_or("unknown");
                        let step_type = step_info.map(|s| s.step_type.as_str()).unwrap_or("transform");
                        
                        let step_start = std::time::Instant::now();
                        let ctx_to_process = current.take().unwrap();
                        
                        // Update step metrics - record entering this step
                        if let Some(metrics) = self.step_metrics.get(step_name) {
                            metrics.records_in.fetch_add(1, Ordering::Relaxed);
                        } else {
                            let metrics = StepMetrics::default();
                            metrics.records_in.store(1, Ordering::Relaxed);
                            self.step_metrics.insert(step_name.to_string(), metrics);
                        }

                        // Determine if we should sample this record (max 1 per second per step)
                        let now_sec = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_secs();
                            
                        let should_sample = {
                            let last = self.step_last_sample
                                .entry(step_name.to_string())
                                .or_insert_with(|| AtomicU64::new(0));
                            if last.load(Ordering::Relaxed) < now_sec {
                                last.store(now_sec, Ordering::Relaxed);
                                true
                            } else {
                                false
                            }
                        };
                        
                        let payload_in_str = if should_sample {
                            serde_json::to_string(&ctx_to_process.data).unwrap_or_else(|_| "".into())
                        } else {
                            "".to_string()
                        };
                        
                        match op(ctx_to_process).await {
                            Ok(new_ctx) => {
                                // Record successfully passed through this step
                                if let Some(metrics) = self.step_metrics.get(step_name) {
                                    metrics.records_out.fetch_add(1, Ordering::Relaxed);
                                }
                                
                                if should_sample {
                                    let payload_out_str = serde_json::to_string(&new_ctx.data).unwrap_or_else(|_| "".into());
                                    telemetry::emit_data_sample(
                                        &pipeline_id,
                                        &stage_name,
                                        step_name,
                                        &payload_in_str,
                                        &payload_out_str,
                                    );
                                }
                                
                                // Emit step metrics telemetry
                                let duration_ms = step_start.elapsed().as_millis() as u64;
                                telemetry::emit_step_metrics(
                                    &pipeline_id,
                                    &stage_name,
                                    step_name,
                                    step_type,
                                    1, // records_in for this execution
                                    1, // records_out
                                    0,
                                    0,
                                    0,
                                    duration_ms,
                                );
                                
                                current = Some(new_ctx);
                            },
                            Err((e, failed_ctx)) => {
                                let is_control = is_control_flow(&e);
                                let duration_ms = step_start.elapsed().as_millis() as u64;
                                
                                if is_control {
                                    // Record was branched or filtered — not a failure.
                                    // It was either routed to a branch sink or silently dropped.
                                    records_branched += 1;
                                    
                                    // Update step metrics
                                    if let Some(metrics) = self.step_metrics.get(step_name) {
                                        if e.to_string().contains(FILTER_SENTINEL) {
                                            metrics.records_filtered.fetch_add(1, Ordering::Relaxed);
                                        } else {
                                            metrics.records_branched.fetch_add(1, Ordering::Relaxed);
                                        }
                                    }
                                    
                                    // Emit step metrics telemetry
                                    telemetry::emit_step_metrics(
                                        &pipeline_id,
                                        &stage_name,
                                        step_name,
                                        step_type,
                                        1,
                                        0,
                                        if e.to_string().contains(FILTER_SENTINEL) { 1 } else { 0 },
                                        if e.to_string().contains(FILTER_SENTINEL) { 0 } else { 1 },
                                        0,
                                        duration_ms,
                                    );
                                    
                                    current = None;
                                    break;
                                }
                                
                                // We got the context back from the failed transform!
                                records_failed += 1;
                                let error_msg = e.to_string();
                                
                                // Update step metrics
                                if let Some(metrics) = self.step_metrics.get(step_name) {
                                    metrics.records_failed.fetch_add(1, Ordering::Relaxed);
                                }
                                
                                // Emit step metrics telemetry
                                telemetry::emit_step_metrics(
                                    &pipeline_id,
                                    &stage_name,
                                    step_name,
                                    step_type,
                                    1,
                                    0,
                                    0,
                                    0,
                                    1,
                                    duration_ms,
                                );
                                
                                eprintln!("[Clotho] Transform failed: {} (record routed to DLQ)", error_msg);
                                
                                // Serialize ONLY on the sad path! 
                                let payload_str = serde_json::to_string(&failed_ctx.data)
                                    .unwrap_or_else(|_| "Serialization failed".to_string());
                                
                                crate::telemetry::emit_dlq_record(
                                    &pipeline_id,
                                    &trace_id,
                                    step_name,
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
                            eprintln!("[Clotho] Sink write failed: {}", e);
                            telemetry::flush_telemetry_http().await;
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
                telemetry::emit_throughput_with_branched(&pipeline_id, records_in, records_out, records_failed, records_branched, bytes_processed);
                batch_counter = 0;
            }
        }

        // Final flush
        let elapsed = start_time.elapsed();
        let runtime_micros = elapsed.as_micros() as u64;
        let runtime_ms = (runtime_micros + 999) / 1000; // Round up to nearest ms, minimum 1ms
        eprintln!("[Clotho] Pipeline End: {} records in, {} records out, {} branched, {} failed ({}µs / {}ms)", 
                  records_in, records_out, records_branched, records_failed, runtime_micros, runtime_ms);
        telemetry::emit_throughput_with_branched(&pipeline_id, records_in, records_out, records_failed, records_branched, bytes_processed);
        telemetry::emit_lifecycle_with_runtime(&pipeline_id, "FINISHED", None, None, Some(runtime_ms));

        // Store execution report for the macro to POST via HTTP
        telemetry::set_execution_report(crate::telemetry::ExecutionReport {
            pipeline_id: pipeline_id.clone(),
            started_at: started_at.clone(),
            duration_ms: runtime_ms,
            status: "completed".into(),
            records_in,
            records_out,
            records_failed,
            records_branched,
            bytes_processed,
            log_lines: vec![],
        });

        // Flush buffered telemetry events to agent (WASM: HTTP POST, native: no-op)
        telemetry::flush_telemetry_http().await;

        Ok(())
    }
}