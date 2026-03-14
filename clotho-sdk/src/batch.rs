// clotho-sdk/src/batch.rs
use crate::traits::{Source, Sink, LookupTarget};
use crate::types::{Context,ContractStatus};
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
        // ... (The exact same execution loop provided in the previous batch.rs file)
        // ... (Awaits source.next(), loops over transforms with .await, and sinks)
        Ok(())
    }
}