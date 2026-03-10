use crate::traits::{Source, Sink};
use crate::types::Context;
use crate::dlq::FailureRecord;
use crate::telemetry;
use anyhow::Result;
use std::marker::PhantomData;

// --- The Builder ---
pub struct Pipeline<S, T> {
    source: S,
    dlq: Option<Box<dyn Sink<FailureRecord>>>,
    _marker: PhantomData<T>,
}

impl<S, T> Pipeline<S, T>
where
    S: Source<T> + 'static,
    T: Send + Sync + 'static,
{
    pub fn read(source: S) -> Self {
        Self { source, dlq: None, _marker: PhantomData }
    }

    pub fn with_dlq<D>(mut self, sink: D) -> Self 
    where D: Sink<FailureRecord> + 'static 
    {
        self.dlq = Some(Box::new(sink));
        self
    }

    pub fn map<F, U>(self, op: F) -> Pipeline<MapSource<S, F, T>, U>
    where
        F: Fn(T) -> Result<U> + Send + Sync + 'static,
        U: Send + Sync + 'static,
    {
        Pipeline {
            source: MapSource { parent: self.source, op, dlq: None, _marker: PhantomData },
            dlq: self.dlq,
            _marker: PhantomData,
        }
    }

    pub fn wire_tap<F, K>(self, predicate: F, sink: K) -> Pipeline<WireTapSource<S, F, K, T>, T>
    where
        F: Fn(&T) -> bool + Send + Sync + 'static,
        K: Sink<T> + 'static,
        T: Clone
    {
         Pipeline {
            source: WireTapSource { parent: self.source, predicate, sink, _marker: PhantomData },
            dlq: self.dlq,
            _marker: PhantomData,
        }
    }

    pub async fn run<K>(mut self, mut sink: K) -> Result<()> 
    where K: Sink<T> 
    {
        // 1. Mark 'Active' Start
        let run_start = Instant::now();
        
        // 2. Calculate Boot Latency
        // (Time since Wasm loaded vs Time right now)
        let boot_latency = telemetry::uptime(); 

        telemetry::emit_lifecycle("STARTUP", Some(boot_latency), None);

        // 3. The Loop
        let mut first_record = true;
        
        while let Some(item) = self.source.next().await {
            
            // 4. Capture "Cold Start" (Time To First Record)
            if first_record {
                let ttfr = telemetry::uptime(); // Total time from birth to data
                telemetry::emit_lifecycle("COLD_START_COMPLETE", Some(boot_latency), Some(ttfr));
                first_record = false;
            }

            // ... process item ...
        }
        Ok(())
    }
}

// --- Internal Wrappers (The Sandwich Logic) ---
pub struct MapSource<S, F, T> {
    parent: S,
    op: F,
    dlq: Option<Box<dyn Sink<FailureRecord>>>, // Use for error routing
    _marker: PhantomData<T>,
}

#[async_trait::async_trait]
impl<S, F, T, U> Source<U> for MapSource<S, F, T>
where
    S: Source<T> + Send + Sync,
    F: Fn(T) -> Result<U> + Send + Sync,
    T: Send + Sync,
    U: Send + Sync,
{
    async fn next(&mut self) -> Option<Result<Context<U>>> {
        let parent_ctx = match self.parent.next().await? {
            Ok(c) => c,
            Err(e) => return Some(Err(e)),
        };

        // Extract what we need before consuming parent_ctx
        let parents = parent_ctx.parents.clone();
        let meta = parent_ctx.meta.clone();
        
        match (self.op)(parent_ctx.data) {
            Ok(new_data) => {
                Some(Ok(Context {
                    data: new_data,
                    span_id: uuid::Uuid::new_v4().to_string(),
                    parents,
                    meta,
                }))
            }
            Err(e) => {
                // DLQ LOGIC
                if let Some(dlq) = &mut self.dlq {
                    let fail = FailureRecord {
                        original_data: "serialization_skipped".into(),
                        error: e.to_string(),
                        step: "map".into(),
                        timestamp: 0,
                    };
                    let fail_ctx = Context::root(fail, "dlq");
                    let _ = dlq.write(fail_ctx).await;
                }
                self.next().await 
            }
        }
    }
}

pub struct WireTapSource<S, F, K, T> {
    parent: S,
    predicate: F,
    sink: K,
    _marker: PhantomData<T>,
}

#[async_trait::async_trait]
impl<S, F, K, T> Source<T> for WireTapSource<S, F, K, T>
where
    S: Source<T> + Send + Sync,
    F: Fn(&T) -> bool + Send + Sync,
    K: Sink<T> + Send + Sync,
    T: Send + Sync + Clone,
{
    async fn next(&mut self) -> Option<Result<Context<T>>> {
        let ctx = self.parent.next().await?.ok()?;
        
        if (self.predicate)(&ctx.data) {
            let side_ctx = Context::child_of(&ctx, ctx.data.clone());
            let _ = self.sink.write(side_ctx).await;
        }
        Some(Ok(ctx))
    }
}

