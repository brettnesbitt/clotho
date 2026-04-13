// clotho-sdk/src/once.rs
use crate::traits::{Sink, Context};
use crate::telemetry::{StepInfo, StepMetrics};
use crate::telemetry;
use anyhow::Result;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::collections::HashMap;

// Matches the exact signature from stream.rs: Returns ownership of data on Err
type AsyncTransformFn<T> = Box<dyn Fn(Context<T>) -> Pin<Box<dyn Future<Output = Result<Context<T>, (anyhow::Error, Context<T>)>> + Send>> + Send + Sync>;

/// Optimized for Webhooks / Serverless HTTP Triggers
pub struct OncePipeline<T> {
    payload: T,
    transforms: Vec<AsyncTransformFn<T>>,
    transform_steps: Vec<StepInfo>, // Track step info for each transform
    step_metrics: HashMap<String, StepMetrics>, // Cumulative metrics per step
    step_counter: AtomicU64, // For auto-naming steps
    record_count: u64, // Actual record count for batch payloads
}

impl<T> OncePipeline<T> 
where T: Send + Sync + 'static 
{
    pub fn new(payload: T) -> Self {
        telemetry::mark_birth();
        Self {
            payload,
            transforms: Vec::new(),
            transform_steps: Vec::new(),
            step_metrics: HashMap::new(),
            step_counter: AtomicU64::new(0),
            record_count: 1,
        }
    }

    /// Override the record count reported in telemetry.
    /// Use this when the payload is a batch (e.g. Vec<Value>) so metrics show
    /// the actual number of records processed rather than 1.
    pub fn with_record_count(mut self, count: u64) -> Self {
        self.record_count = count;
        self
    }

    /// Get the next step index for auto-naming
    fn next_step_idx(&self) -> u64 {
        self.step_counter.fetch_add(1, Ordering::SeqCst)
    }

    /// Pure logic mapping. Uses the Zero-Copy Ownership Return pattern.
    pub fn map<F>(mut self, op: F) -> Self 
    where F: Fn(T) -> Result<T, (anyhow::Error, T)> + Send + Sync + 'static 
    {
        let idx = self.next_step_idx();
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
        self.transform_steps.push(StepInfo {
            name: format!("map_{}", idx),
            step_type: "transform".to_string(),
        });
        self
    }

    /// Async mapping for API calls (e.g., verifying a Stripe webhook signature)
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
        for (idx, op) in self.transforms.iter().enumerate() {
            let step_info = self.transform_steps.get(idx);
            let step_name = step_info.map(|s| s.name.as_str()).unwrap_or("unknown");
            let step_type = step_info.map(|s| s.step_type.as_str()).unwrap_or("transform");
            
            let step_start = std::time::Instant::now();
            
            // Update step metrics - record entering this step
            if let Some(metrics) = self.step_metrics.get(step_name) {
                metrics.records_in.fetch_add(1, Ordering::Relaxed);
            } else {
                let metrics = StepMetrics::default();
                metrics.records_in.store(1, Ordering::Relaxed);
                self.step_metrics.insert(step_name.to_string(), metrics);
            }
            
            let ctx_to_process = current.take().unwrap();
            
            match op(ctx_to_process).await {
                Ok(new_ctx) => {
                    let duration_ms = step_start.elapsed().as_millis() as u64;
                    
                    // Update step metrics
                    if let Some(metrics) = self.step_metrics.get(step_name) {
                        metrics.records_out.fetch_add(1, Ordering::Relaxed);
                    }
                    
                    // Emit step metrics telemetry
                    telemetry::emit_step_metrics(
                        &pipeline_id,
                        "", // stage_name
                        step_name,
                        step_type,
                        1,
                        1,
                        0,
                        0,
                        0,
                        duration_ms,
                    );
                    
                    current = Some(new_ctx);
                },
                Err((e, failed_ctx)) => {
                    let duration_ms = step_start.elapsed().as_millis() as u64;
                    let error_msg = e.to_string();
                    
                    // Update step metrics
                    if let Some(metrics) = self.step_metrics.get(step_name) {
                        metrics.records_failed.fetch_add(1, Ordering::Relaxed);
                    }
                    
                    // Emit step metrics telemetry
                    telemetry::emit_step_metrics(
                        &pipeline_id,
                        "",
                        step_name,
                        step_type,
                        1,
                        0,
                        0,
                        0,
                        1,
                        duration_ms,
                    );
                    
                    eprintln!("[Clotho] Webhook Pipeline Failed: {}", error_msg);
                    
                    let payload_str = serde_json::to_string(&failed_ctx.data)
                        .unwrap_or_else(|_| "Serialization failed".to_string());
                    
                    telemetry::emit_dlq_record(
                        &pipeline_id,
                        &trace_id,
                        step_name,
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
            let ttfr_ms = telemetry::uptime_ms();
            telemetry::emit_lifecycle(&pipeline_id, "FIRST_WRITE", None, Some(ttfr_ms));
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
            records_in: self.record_count,
            records_out: self.record_count,
            records_failed: 0,
            records_branched: 0,
            bytes_processed: 0,
            log_lines: vec![],
        });

        Ok(())
    }
}