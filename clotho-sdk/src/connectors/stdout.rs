use crate::traits::{Sink, Context};
use anyhow::Result;
use std::fmt::Debug;

/// A simple sink that prints pipeline records to Standard Output.
/// Perfect for local testing, debugging, and dry-runs.
pub struct StdoutSink {
    prefix: String,
}

impl StdoutSink {
    pub fn new() -> Self {
        Self {
            prefix: "📦 [Clotho]".to_string(),
        }
    }

    /// Optional: Add a custom prefix to distinguish multiple sinks
    pub fn with_prefix(prefix: &str) -> Self {
        Self {
            prefix: format!("📦 [{}]", prefix),
        }
    }
}

// Default implementation so users can just call StdoutSink::default()
impl Default for StdoutSink {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
// Notice the trait bound: `T: Debug`. This is the magic key.
impl<T> Sink<T> for StdoutSink 
where 
    T: Debug + Send + Sync 
{
    async fn write(&mut self, ctx: Context<T>) -> Result<()> {
        // Print the Trace ID/Record ID for observability
        println!("{} Record ID: {}", self.prefix, ctx.id);
        
        // Print the Metadata if it exists (useful for checking Topic/Partition injection)
        if !ctx.metadata.is_empty() {
            println!("   Metadata: {:?}", ctx.metadata);
        }

        // Print the actual payload
        // The "{:#?}" formatter pretty-prints the data structure
        println!("   Data:\n{:#?}", ctx.data);
        println!("---------------------------------------------------");
        
        Ok(())
    }
}