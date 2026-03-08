pub mod types;
pub mod traits;
pub mod pipeline;
pub mod telemetry;
pub mod dlq;
pub mod builtins; // ConsoleSink, etc.

pub mod prelude {
    pub use crate::pipeline::Pipeline;
    pub use crate::traits::{Source, Sink};
    pub use crate::types::Context;
    pub use anyhow::{Result, anyhow};
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::builtins::*;
    use crate::prelude::*;

    #[tokio::test]
    async fn test_decoupled_pipeline() -> Result<()> {
        // 1. Setup the "Topic"
        let (mut topic_sink, topic_source) = memory_channel::<String>(10);

        // 2. Pipeline A: The Producer (Ingest)
        tokio::spawn(async move {
            let inputs = vec!["transaction_1", "transaction_2"];
            Pipeline::read(VecSource::new(inputs))
                .map(|s| Ok(s.to_uppercase())) // "TRANSACTION_1"
                .run(topic_sink)
                .await
                .unwrap();
        });

        // 3. Pipeline B: The Consumer (Processor)
        // This reads what Pipeline A wrote, preserving the Trace ID!
        let results = std::sync::Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let results_clone = results.clone();

        // We create a Custom Test Sink to capture output
        struct CaptureSink { vec: std::sync::Arc<tokio::sync::Mutex<Vec<String>>> }
        #[async_trait::async_trait]
        impl Sink<String> for CaptureSink {
            async fn write(&mut self, ctx: Context<String>) -> Result<()> {
                self.vec.lock().await.push(ctx.data);
                Ok(())
            }
        }

        Pipeline::read(topic_source)
            .map(|s| Ok(format!("Processed: {}", s)))
            .run(CaptureSink { vec: results_clone })
            .await?;

        // 4. Verify
        let final_data = results.lock().await;
        assert_eq!(final_data[0], "Processed: TRANSACTION_1");
        assert_eq!(final_data[1], "Processed: TRANSACTION_2");
        
        println!("✅ In-Memory Bus Test Passed");
        Ok(())
    }
}