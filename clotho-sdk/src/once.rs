use crate::traits::{Sink, Context};
use crate::telemetry;
use anyhow::Result;

/// Optimized for Webhooks / Serverless Triggers
pub struct OncePipeline<T> {
    payload: T,
    transforms: Vec<Box<dyn Fn(T) -> Result<T> + Send + Sync>>,
}

impl<T> OncePipeline<T> 
where T: Send + Sync + 'static 
{
    pub fn new(payload: T) -> Self {
        // 1. Initialize Telemetry immediately
        telemetry::mark_birth();
        
        Self {
            payload,
            transforms: Vec::new(),
        }
    }

    pub fn map<F>(mut self, op: F) -> Self 
    where F: Fn(T) -> Result<T> + Send + Sync + 'static 
    {
        self.transforms.push(Box::new(op));
        self
    }

    /// Run the pipeline and return a Result.
    /// The caller (the HTTP handler) uses this Result to determine the HTTP Status Code.
    pub async fn run<K>(mut self, mut sink: K) -> Result<()> 
    where K: Sink<T> 
    {
        let pipeline_id = std::env::var("CLOTHO_PIPELINE_ID").unwrap_or("webhook".into());
        let boot_ms = telemetry::uptime_ms();

        // Emit START
        telemetry::emit_lifecycle(&pipeline_id, "STARTUP", Some(boot_ms), None);

        // 1. Contextualize
        let mut context = Context::root(self.payload, "once_pipeline");
        // (In a real impl, we would extract Trace ID from HTTP Headers here!)

        // 2. Transform
        for op in self.transforms {
            match op(context.data) {
                Ok(new_data) => context.data = new_data,
                Err(e) => {
                    telemetry::emit_error(&pipeline_id, "TRANSFORM_FAIL", &e.to_string());
                    return Err(e);
                }
            }
        }

        // 3. Sink
        sink.write(context).await?;

        // Emit SUCCESS
        let runtime_ms = telemetry::uptime_ms() - boot_ms;
        telemetry::emit_lifecycle_with_runtime(&pipeline_id, "FINISHED", None, None, Some(runtime_ms));

        // Store execution report for the macro to POST via HTTP
        telemetry::set_execution_report(telemetry::ExecutionReport {
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