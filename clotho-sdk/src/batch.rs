// clotho-sdk/src/batch.rs
use crate::traits::{Source, Sink, LookupTarget};
use crate::types::{Context, ContractStatus};
use crate::telemetry;
use polars::prelude::*;
use anyhow::Result;
use std::marker::PhantomData;
use std::future::Future;
use std::pin::Pin;

pub struct BatchPipeline<S> {
    source: S,
    // Operations take a LazyFrame and return a Future resolving to a LazyFrame
    // This allows mixing pure-Polars sync logic with async I/O lookups.
    transforms: Vec<Box<dyn Fn(LazyFrame) -> Pin<Box<dyn Future<Output = Result<LazyFrame>> + Send>> + Send + Sync>>,
    _marker: PhantomData<S>,
}

impl<S> BatchPipeline<S> 
where S: Source<DataFrame> + 'static 
{
    pub fn new(source: S) -> Self {
        telemetry::mark_birth();
        Self {
            source,
            transforms: Vec::new(),
            _marker: PhantomData,
        }
    }

    /// Pure Logic: Synchronous Polars Expressions
    pub fn map<F>(mut self, op: F) -> Self
    where F: Fn(LazyFrame) -> LazyFrame + Send + Sync + 'static
    {
        self.transforms.push(Box::new(move |lf: LazyFrame| {
            let result = op(lf);
            Box::pin(async move { Ok(result) })
        }));
        self
    }

    /// I/O Bound Logic: For custom API calls or advanced bulk operations
    /// Note: This forces eager evaluation (.collect()) before running the async user code.
    pub fn map_async<F, Fut>(mut self, op: F) -> Self 
    where 
        F: Fn(DataFrame) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<DataFrame>> + Send + 'static
    {
        self.transforms.push(Box::new(move |lf: LazyFrame| {
            let op_ref = &op; // Need to handle lifetimes safely in real impl, simplified here
            Box::pin(async move {
                let eager_df = lf.collect()?;
                let new_df = op(eager_df).await?;
                Ok(new_df.lazy())
            })
        }));
        self
    }

    /// The "Golden Path" macro for Data Loading / Joining
    pub fn enrich<L>(mut self, lookup: L, join_col: &str, mode: JoinMode) -> Self 
    where L: LookupTarget + Send + Sync + 'static 
    {
        let col_name = join_col.to_string();
        
        self.transforms.push(Box::new(move |lf: LazyFrame| {
            let col_name = col_name.clone();
            // We assume lookup is cheap to clone (e.g., an Arc'd connection pool)
            let lookup_clone = lookup.clone(); 
            
            Box::pin(async move {
                let df = lf.collect()?;
                
                // 1. Extract keys from the incoming batch
                let keys: Vec<&str> = df.column(&col_name)?.utf8()?.into_no_null_iter().collect();
                
                // 2. Bulk fetch from external DB
                let enrichment_df = lookup_clone.lookup_batch(keys).await?;
                
                // 3. Polars native vectorized join
                let lazy_df = df.lazy();
                let lazy_enrich = enrichment_df.lazy();
                
                let joined_df = match mode {
                    JoinMode::Inner => lazy_df.inner_join(lazy_enrich, col(&col_name), col(&col_name)),
                    JoinMode::Left => lazy_df.left_join(lazy_enrich, col(&col_name), col(&col_name)),
                    JoinMode::Outer => lazy_df.outer_join(lazy_enrich, col(&col_name), col(&col_name)),
                };
                
                Ok(joined_df)
            })
        }));
        self
    }

    /// Data Contract Validation
    pub fn expect<F>(mut self, rule_name: &str, check: F) -> Self 
    where F: Fn(&DataFrame) -> ContractStatus + Send + Sync + 'static 
    {
        let rule = rule_name.to_string();

        self.transforms.push(Box::new(move |lf: LazyFrame| {
            let rule = rule.clone();
            Box::pin(async move {
                let df = lf.collect().unwrap_or_else(|_| DataFrame::default());
                let status = check(&df);
                
                let pipeline_id = std::env::var("CLOTHO_PIPELINE_ID").unwrap_or("local".into());
                telemetry::emit_data_quality(&pipeline_id, &rule, status.clone(), None);

                match status {
                    ContractStatus::Fail => Ok(DataFrame::default().lazy()), // Drop batch
                    _ => Ok(df.lazy()), 
                }
            })
        }));
        self
    }

    pub async fn run<K>(mut self, mut sink: K) -> Result<()> 
    where K: Sink<DataFrame> 
    {
        let pipeline_id = std::env::var("CLOTHO_PIPELINE_ID")
            .or_else(|_| std::env::var("PIPELINE_ID"))
            .unwrap_or_else(|_| "batch".into());

        let boot_ms = telemetry::uptime_ms();
        let start_time = std::time::Instant::now();

        eprintln!("[Clotho] Pipeline Started: {}", pipeline_id);
        eprintln!("[Clotho] Mode: Batch (columnar, Polars)");
        telemetry::emit_lifecycle(&pipeline_id, "STARTUP", Some(boot_ms), None);

        let mut records_in: u64 = 0;
        let mut records_out: u64 = 0;
        let mut records_failed: u64 = 0;
        let mut bytes_processed: u64 = 0;
        let mut batch_count: u64 = 0;

        while let Some(ctx_result) = self.source.next().await {
            match ctx_result {
                Ok(initial_ctx) => {
                    let batch_rows = initial_ctx.data.height() as u64;
                    records_in += batch_rows;
                    batch_count += 1;

                    // Estimate bytes from DataFrame shape
                    bytes_processed += (initial_ctx.data.height() * initial_ctx.data.width() * 8) as u64;

                    // Apply transforms as LazyFrame chain
                    let mut current_lf = Some(initial_ctx.data.lazy());

                    for op in &self.transforms {
                        if let Some(lf) = current_lf.take() {
                            match op(lf).await {
                                Ok(new_lf) => current_lf = Some(new_lf),
                                Err(e) => {
                                    records_failed += batch_rows;
                                    eprintln!("[Clotho] Batch transform failed: {}", e);
                                    telemetry::emit_dlq_record(
                                        &pipeline_id,
                                        &format!("batch-{}", batch_count),
                                        "transform",
                                        &e.to_string(),
                                        &format!("batch of {} rows", batch_rows),
                                    );
                                    break;
                                }
                            }
                        }
                    }

                    // Sink the result
                    if let Some(lf) = current_lf {
                        match lf.collect() {
                            Ok(df) => {
                                let out_rows = df.height() as u64;
                                records_out += out_rows;
                                let sink_ctx = Context::root(df, &format!("batch-{}", batch_count));
                                if let Err(e) = sink.write(sink_ctx).await {
                                    records_failed += batch_rows;
                                    eprintln!("[Clotho] Sink write failed: {}", e);
                                    return Err(e);
                                }
                            }
                            Err(e) => {
                                records_failed += batch_rows;
                                eprintln!("[Clotho] LazyFrame collect failed: {}", e);
                            }
                        }
                    }
                }
                Err(e) => {
                    records_failed += 1;
                    eprintln!("[Clotho] Source error: {}", e);
                }
            }

            // Emit throughput after each batch
            eprintln!("[Clotho] Batch {} complete: {} rows in, {} rows out", batch_count, records_in, records_out);
            telemetry::emit_throughput(&pipeline_id, records_in, records_out, records_failed, bytes_processed);
        }

        // Final flush
        let elapsed = start_time.elapsed();
        let runtime_micros = elapsed.as_micros() as u64;
        let runtime_ms = (runtime_micros + 999) / 1000;

        eprintln!("[Clotho] Pipeline End: {} records in, {} records out, {} failed ({}µs / {}ms)", 
                  records_in, records_out, records_failed, runtime_micros, runtime_ms);
        telemetry::emit_throughput(&pipeline_id, records_in, records_out, records_failed, bytes_processed);
        telemetry::emit_lifecycle_with_runtime(&pipeline_id, "FINISHED", None, None, Some(runtime_ms));

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