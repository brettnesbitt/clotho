use crate::traits::{Source, Sink, Context};
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
    pub fn map<F>(mut self, op: F) -> Self 
    where F: Fn(Context<T>) -> Result<Context<T>> + Send + Sync + 'static 
    {
        self.transforms.push(Box::new(move |ctx| {
            let res = op(ctx);
            // Wrap the sync result in an immediately resolving future
            Box::pin(async move { res })
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
        while let Some(ctx_result) = self.source.next().await {
            match ctx_result {
                Ok(mut ctx) => {
                    let mut dropped = false;
                    // Execute the chain of async transformations
                    for op in &self.transforms {
                        match op(ctx).await {
                            Ok(new_ctx) => ctx = new_ctx,
                            Err(e) => {
                                // If a user returns an error, we drop the record (or send to DLQ)
                                eprintln!("Dropped record: {}", e);
                                dropped = true;
                                break;
                            }
                        }
                    }
                    if !dropped {
                        sink.write(ctx).await?;
                    }
                }
                Err(e) => eprintln!("Source Error: {}", e),
            }
        }
        Ok(())
    }
}