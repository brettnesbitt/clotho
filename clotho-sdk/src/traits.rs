use async_trait::async_trait;
use anyhow::Result;
use polars::prelude::DataFrame;
use crate::types::Context;

#[async_trait]
pub trait Source<T>: Send + Sync {
    /// Returns the next item wrapped in Context. 
    /// If the source is raw (like a Timer), it must wrap it in Context::root().
    async fn next(&mut self) -> Option<Result<Context<T>>>;

    // New Method: Returns total items if known (e.g., File lines, Kafka Lag)
    fn size_hint(&self) -> Option<u64> { None }
}

#[async_trait]
pub trait Sink<T>: Send + Sync {
    /// Accepts a Context. Sinks are responsible for serializing the Trace headers.
    async fn write(&mut self, item: Context<T>) -> Result<()>;
}

#[async_trait]
pub trait LookupTarget {
    /// Takes a batch of keys, queries the underlying datastore, 
    /// and returns a Polars DataFrame ready for joining.
    async fn lookup_batch(&self, keys: Vec<&str>) -> Result<DataFrame>;
}