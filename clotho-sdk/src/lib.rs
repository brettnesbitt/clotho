// clotho-sdk/src/lib.rs

pub mod types;
pub mod traits;
pub mod telemetry;
pub mod stream;
pub mod connectors;
pub mod builtins;
pub mod once;

// Only compile Batch engine if requested
#[cfg(feature = "batch")]
pub mod batch;

pub use traits::{Source, Sink};
pub use types::Context;
pub use anyhow::Result;

/// The Global Builder
pub struct Pipeline;

impl Pipeline {
    /// Create a Low-Latency, Item-by-Item pipeline.
    /// Best for: Webhooks, Alerts, IoT, API integration.
    pub fn stream<S, T>(source: S) -> stream::StreamPipeline<S, T> 
    where 
        S: Source<T> + 'static,
        T: Send + Sync + 'static
    {
        stream::StreamPipeline::new(source)
    }

    /// Create a High-Throughput, Columnar pipeline (Polars).
    /// Best for: ETL, Analytics, S3 Archiving, Database Sync.
    #[cfg(feature = "batch")]
    pub fn batch<S>(source: S) -> batch::BatchPipeline<S> 
    where S: Source<polars::prelude::DataFrame> {
        batch::BatchPipeline::new(source)
    }

    /// Create a Single-Shot pipeline for Webhooks/Triggers.
    /// Runs exactly once and returns.
    pub fn once<T>(data: T) -> once::OncePipeline<T> 
    where T: Send + Sync + 'static {
        once::OncePipeline::new(data)
    }
}