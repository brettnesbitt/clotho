use crate::traits::{Source, Sink, Context};
use crate::telemetry;
use anyhow::Result;
use std::future::Future;
use std::pin::Pin;

// Define a type alias for our async closures to keep the struct clean
type AsyncTransformFn<T> = Box<dyn Fn(Context<T>) -> Pin<Box<dyn Future<Output = Result<Context<T>>> + Send>> + Send + Sync>;

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
    /// User provides a function over the data T, Context is preserved automatically.
    pub fn map<F>(mut self, op: F) -> Self 
    where F: Fn(T) -> Result<T> + Send + Sync + 'static 
    {
        self.transforms.push(Box::new(move |ctx: Context<T>| {
            let Context { data, span_id, parents, meta } = ctx;
            let result = op(data);
            Box::pin(async move {
                result.map(|new_data| Context { data: new_data, span_id, parents, meta })
            })
        }));
        self
    }

    /// Asynchronous Transform (I/O Bound - DB Lookups, API calls)
    pub fn map_async<F, Fut>(mut self, op: F) -> Self 
    where 
        F: Fn(Context<T>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Context<T>>> + Send + 'static
    {
        self.transforms.push(Box::new(move |ctx| {
            // Box the user's future
            Box::pin(op(ctx))
        }));
        self
    }

    pub async fn run<K>(mut self, mut sink: K) -> Result<()> 
    where K: Sink<T> 
    {
        let pipeline_id = std::env::var("PIPELINE_ID")
            .or_else(|_| std::env::var("CLOTHO_PIPELINE_ID"))
            .unwrap_or_else(|_| "unknown".into());

        telemetry::mark_birth();
        let boot_ms = telemetry::uptime_ms();

        eprintln!("[Clotho] Pipeline Started: {}", pipeline_id);
        eprintln!("[Clotho] Mode: Stream (item-by-item)");
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
                    let mut current = Some(initial_ctx);

                    for op in &self.transforms {
                        match op(current.take().unwrap()).await {
                            Ok(new_ctx) => current = Some(new_ctx),
                            Err(e) => {
                                records_failed += 1;
                                eprintln!("[Clotho] Transform failed: {} (record dropped)", e);
                                break;
                            }
                        }
                    }
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
        let runtime_ms = telemetry::uptime_ms() - boot_ms;
        eprintln!("[Clotho] Pipeline End: {} records in, {} records out, {} failed ({}ms)", records_in, records_out, records_failed, runtime_ms);
        telemetry::emit_throughput(&pipeline_id, records_in, records_out, records_failed, bytes_processed);
        telemetry::emit_lifecycle_with_runtime(&pipeline_id, "FINISHED", None, None, Some(runtime_ms));

        // Store execution report for the macro to POST via HTTP
        telemetry::set_execution_report(telemetry::ExecutionReport {
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