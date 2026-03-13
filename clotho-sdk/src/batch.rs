// clotho-sdk/src/batch.rs

use crate::traits::{Source, Sink};
use crate::types::Context;
use crate::telemetry;
use polars::prelude::*;
use anyhow::Result;
use std::marker::PhantomData;

pub struct BatchPipeline<S> {
    source: S,
    // Operations are Lazy Expressions, not closures!
    transforms: Vec<Box<dyn Fn(LazyFrame) -> LazyFrame + Send + Sync>>,
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

    pub fn enrich<L>(mut self, lookup: L, join_col: &str, mode: JoinMode) -> Self 
    where 
        L: LookupTarget + Send + Sync + 'static 
    {
        let col_name = join_col.to_string();
        
        self.transform_async(move |df| {
            // Note: lookup must be cloned or wrapped in Arc if shared across batches
            let col_name = col_name.clone();
            
            async move {
                // 1. Extract keys from the incoming Kafka batch
                let keys: Vec<&str> = df.column(&col_name)?.utf8()?.into_no_null_iter().collect();
                
                // 2. Fetch the data from Mongo (Vectorized)
                let enrichment_df = lookup.lookup_batch(keys).await?;
                
                // 3. Perform the Polars Join
                let lazy_df = df.lazy();
                let lazy_enrich = enrichment_df.lazy();
                
                let joined_df = match mode {
                    JoinMode::Inner => lazy_df.inner_join(lazy_enrich, col(&col_name), col(&col_name)),
                    JoinMode::Left => lazy_df.left_join(lazy_enrich, col(&col_name), col(&col_name)),
                };
                
                Ok(joined_df.collect()?)
            }
        })
    }

    /// Add a Lazy Transformation (Polars Expression)
    pub fn transform<F>(mut self, op: F) -> Self
    where F: Fn(LazyFrame) -> LazyFrame + Send + Sync + 'static
    {
        self.transforms.push(Box::new(op));
        self
    }

    /// Data Contract Stub (We will implement this next)
    pub fn expect<F>(self, _check: F) -> Self 
    where F: Fn(&DataFrame) -> bool + Send + Sync + 'static 
    {
        // TODO: Implement Contract Logic
        self
    }

    /// Add a Data Contract (Batch-Level)
    /// The closure receives the full DataFrame and returns a Result.
    /// If it returns Err/False, the ENTIRE batch is rejected.
    pub fn expect<F>(mut self, rule_name: &str, check: F) -> Self 
    where F: Fn(&DataFrame) -> ContractStatus + Send + Sync + 'static 
    {
        let rule = rule_name.to_string();

        // We wrap the check. 
        // Note: This forces "Eager" evaluation of the batch at this step.
        // It breaks the "Lazy" chain slightly but guarantees safety before next steps.
        self.transforms.push(Box::new(move |lf: LazyFrame| {
            
            // 1. We must collect to inspect the data (Optimization boundary)
            let df = lf.collect().unwrap_or_else(|_| DataFrame::default());
            
            // 2. Run User Check
            let status = check(&df);
            
            // 3. Emit Telemetry
            let pipeline_id = std::env::var("CLOTHO_PIPELINE_ID").unwrap_or("local".into());
            telemetry::emit_data_quality(&pipeline_id, &rule, status.clone(), None);

            // 4. Decision
            match status {
                ContractStatus::Fail => {
                    // Drop the data. Return empty DataFrame.
                    // Real impl would route 'df' to DLQ Sink.
                    DataFrame::default().lazy() 
                },
                _ => df.lazy(), // Pass/Warning -> Continue
            }
        }));
        
        self
    }

    pub async fn run<K>(mut self, mut sink: K) -> Result<()> 
    where K: Sink<DataFrame> 
    {
        let pipeline_id = std::env::var("CLOTHO_PIPELINE_ID").unwrap_or("local".into());
        let boot_ms = telemetry::uptime_ms();
        
        eprintln!("[Clotho] Pipeline Started: {}", pipeline_id);
        eprintln!("[Clotho] Mode: Batch (columnar/Polars)");
        
        telemetry::emit_lifecycle(&pipeline_id, "STARTUP", Some(boot_ms), None);

        let mut is_first_batch = true;
        let mut records_in: u64 = 0;
        let mut records_out: u64 = 0;
        let mut records_failed: u64 = 0;
        let mut bytes_processed: u64 = 0;
        let mut batch_count: u64 = 0;

        while let Some(ctx_result) = self.source.next().await {
            match ctx_result {
                Ok(mut ctx) => {
                    batch_count += 1;
                    let input_rows = ctx.data.height() as u64;
                    let input_cols = ctx.data.width();
                    records_in += input_rows;

                    if is_first_batch {
                        eprintln!("[Clotho] First batch received: {} rows × {} columns", input_rows, input_cols);
                        let ttfr = telemetry::uptime_ms();
                        telemetry::emit_lifecycle(&pipeline_id, "FIRST_BATCH", Some(boot_ms), Some(ttfr));
                        is_first_batch = false;
                    } else {
                        eprintln!("[Clotho] Batch #{}: {} rows × {} columns", batch_count, input_rows, input_cols);
                    }

                    // 1. Convert Eager DataFrame to Lazy
                    let mut lf = ctx.data.lazy();

                    // 2. Apply the Plan
                    for t in &self.transforms {
                        lf = t(lf);
                    }

                    // 3. EXECUTE (The Heavy Lift)
                    match lf.collect() {
                        Ok(result_df) => {
                            // Track output rows + estimate bytes
                            let output_rows = result_df.height() as u64;
                            let output_cols = result_df.width();
                            records_out += output_rows;
                            bytes_processed += result_df.estimated_size() as u64;

                            eprintln!("[Clotho] Transformed batch #{}: {} rows → {} rows ({} columns)", 
                                batch_count, input_rows, output_rows, output_cols);

                            // 4. Update Context & Sink
                            ctx.data = result_df;
                            
                            // 5. Emit Batch Metrics (Rows Processed)
                            telemetry::report_progress(&pipeline_id, records_out, None);
                            
                            if let Err(e) = sink.write(ctx).await {
                                eprintln!("[Clotho] Sink write failed for batch #{}: {}", batch_count, e);
                                return Err(e);
                            }
                            eprintln!("[Clotho] Sent batch #{} to sink", batch_count);
                        },
                        Err(e) => {
                            // Entire batch failed
                            records_failed += input_rows;
                            eprintln!("[Clotho] Batch #{} execution failed: {} ({} rows sent to DLQ)", batch_count, e, input_rows);
                            telemetry::emit_error(&pipeline_id, "BATCH_FAIL", &e.to_string());
                        }
                    }

                    // Emit cumulative throughput per batch
                    telemetry::emit_throughput(&pipeline_id, records_in, records_out, records_failed, bytes_processed);
                }
                Err(e) => {
                    records_failed += 1;
                    eprintln!("[Clotho] Source error: {}", e);
                }
            }
        }
        
        let runtime_ms = telemetry::uptime_ms() - boot_ms;
        eprintln!("[Clotho] Pipeline End: {} batches, {} records in, {} records out, {} failed ({}ms)", 
            batch_count, records_in, records_out, records_failed, runtime_ms);
        telemetry::emit_lifecycle_with_runtime(&pipeline_id, "FINISHED", None, None, Some(runtime_ms));
        Ok(())
    }
}