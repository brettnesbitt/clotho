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
        while let Some(ctx_result) = self.source.next().await {
            match ctx_result {
                Ok(initial_ctx) => {
                    let mut current = Some(initial_ctx);
                    // Execute the chain of async transformations
                    for op in &self.transforms {
                        match op(current.take().unwrap()).await {
                            Ok(new_ctx) => current = Some(new_ctx),
                            Err(e) => {
                                // If a user returns an error, we drop the record (or send to DLQ)
                                eprintln!("Dropped record: {}", e);
                                break;
                            }
                        }
                    }
                    if let Some(ctx) = current {
                        sink.write(ctx).await?;
                    }
                }
                Err(e) => eprintln!("Source Error: {}", e),
            }
        }
        Ok(())
    }
}