use crate::traits::{Source, Sink};
use crate::types::Context;
use anyhow::Result;
use async_trait::async_trait;
use std::fmt::Debug;
use tokio::sync::mpsc;
use tokio::time::{self, Duration, Interval};

// ============================================================================
// 1. GENERATORS (SOURCES)
// ============================================================================

/// A Source that emits a static list of items.
/// Great for unit tests or "Batch" jobs.
pub struct VecSource<T> {
    iter: std::vec::IntoIter<T>,
}

impl<T> VecSource<T> {
    pub fn new(items: Vec<T>) -> Self {
        Self {
            iter: items.into_iter(),
        }
    }
}

#[async_trait]
impl<T: Send + Sync> Source<T> for VecSource<T> {
    async fn next(&mut self) -> Option<Result<Context<T>>> {
        // Wrap the raw item in a new Root Context
        self.iter.next().map(|item| Ok(Context::root(item, "vec_source")))
    }
}

/// A Source that emits a "Heartbeat" every interval.
/// Great for Cron-style pipelines (e.g., "Poll API every 60s").
pub struct IntervalSource {
    interval: Interval,
    tick_count: u64,
}

impl IntervalSource {
    pub fn new(period: Duration) -> Self {
        let mut interval = time::interval(period);
        // The first tick completes immediately
        interval.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
        Self {
            interval,
            tick_count: 0,
        }
    }
}

#[async_trait]
impl Source<u64> for IntervalSource {
    async fn next(&mut self) -> Option<Result<Context<u64>>> {
        self.interval.tick().await;
        let count = self.tick_count;
        self.tick_count += 1;
        // Emit the Tick Count as the data
        Some(Ok(Context::root(count, "interval_timer")))
    }
}

// ============================================================================
// 2. DEBUGGERS (SINKS)
// ============================================================================

/// A Sink that prints the Trace ID and Data to stdout.
/// The "Hello World" of Sinks.
pub struct ConsoleSink;

impl ConsoleSink {
    pub fn new() -> Self { Self }
}

#[async_trait]
impl<T: Debug + Send + Sync + 'static> Sink<T> for ConsoleSink {
    async fn write(&mut self, ctx: Context<T>) -> Result<()> {
        println!(
            "[ConsoleSink] TraceID: {} | SpanID: {} | Data: {:?}",
            ctx.parents.first().map(|p| p.trace_id.as_str()).unwrap_or(&ctx.span_id),
            ctx.span_id,
            ctx.data
        );
        Ok(())
    }
}

/// A Sink that silently drops data.
/// Useful for performance testing or "Fire and Forget".
pub struct DevNullSink;

#[async_trait]
impl<T: Send + Sync + 'static> Sink<T> for DevNullSink {
    async fn write(&mut self, _ctx: Context<T>) -> Result<()> {
        // Do nothing. It vanishes into the void.
        Ok(())
    }
}

// ============================================================================
// 3. MEMORY BUS (MOCK KAFKA/QUEUE)
// ============================================================================

/// The MemoryChannel simulates a Topic/Queue.
/// It splits into a Producer (Sink) and Consumer (Source).
/// Use this to test "Pipeline A -> Topic -> Pipeline B" logic locally.
pub fn memory_channel<T>(buffer_size: usize) -> (MemorySink<T>, MemorySource<T>) {
    let (tx, rx) = mpsc::channel(buffer_size);
    (MemorySink { tx }, MemorySource { rx })
}

pub struct MemorySink<T> {
    tx: mpsc::Sender<Context<T>>,
}

#[async_trait]
impl<T: Send + Sync + Debug> Sink<T> for MemorySink<T> {
    async fn write(&mut self, ctx: Context<T>) -> Result<()> {
        // We send the ENTIRE context, preserving Trace IDs across the boundary.
        // This mimics how Kafka headers work.
        self.tx.send(ctx).await.map_err(|_| anyhow::anyhow!("MemoryChannel closed"))
    }
}

pub struct MemorySource<T> {
    rx: mpsc::Receiver<Context<T>>,
}

#[async_trait]
impl<T: Send + Sync> Source<T> for MemorySource<T> {
    async fn next(&mut self) -> Option<Result<Context<T>>> {
        // We receive the Context from the Sink.
        // Note: We might want to "Evolve" it here to mark the "Read" operation,
        // but for a dumb pipe, passing it through is acceptable.
        self.rx.recv().await.map(Ok)
    }
}

// ============================================================================
// 4. MOCK DATA STREAM (MOCK BYTES)
// ============================================================================

/// Simulates reading raw bytes from a file or network.
/// Useful for testing deserialization logic (e.g. JSON/Protobuf parsing).
pub struct MockByteSource {
    data: Vec<Vec<u8>>,
    current: usize,
}

impl MockByteSource {
    pub fn new(mock_payloads: Vec<&str>) -> Self {
        Self {
            data: mock_payloads.iter().map(|s| s.as_bytes().to_vec()).collect(),
            current: 0,
        }
    }
}

#[async_trait]
impl Source<Vec<u8>> for MockByteSource {
    async fn next(&mut self) -> Option<Result<Context<Vec<u8>>>> {
        if self.current >= self.data.len() {
            return None;
        }
        let bytes = self.data[self.current].clone();
        self.current += 1;
        Some(Ok(Context::root(bytes, "mock_bytes")))
    }
}