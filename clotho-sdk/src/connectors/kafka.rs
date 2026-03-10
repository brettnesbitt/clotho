use rskafka::client::ClientBuilder;
use std::time::Duration;

pub struct KafkaSourceConfig {
    pub brokers: String,
    pub topic: String,
    pub group_id: String,
    // StreamSets-like critical parameters
    pub max_wait_ms: u64,       // Linger: Yield batch if this time expires
    pub max_batch_bytes: i32,   // Yield batch if it hits this size
    pub auto_offset_reset: OffsetReset, // Earliest / Latest
}

pub struct KafkaSource {
    config: KafkaSourceConfig,
    consumer: Option<StreamConsumer>,
}

#[async_trait::async_trait]
impl Source<DataFrame> for KafkaSource { // Note: Yielding DataFrames directly for Batch Engine
    async fn next(&mut self) -> Option<Result<Context<DataFrame>>> {
        let consumer = self.consumer.as_mut().unwrap();
        
        let start_wait = std::time::Instant::now();
        let mut batch_data = Vec::new();
        let mut byte_count = 0;

        // DYNAMIC BATCHING LOOP
        while start_wait.elapsed().as_millis() < self.config.max_wait_ms as u128 {
            // Non-blocking poll or short timeout
            match tokio::time::timeout(Duration::from_millis(10), consumer.next()).await {
                Ok(Ok((record, _))) => {
                    let bytes = record.value.unwrap_or_default();
                    byte_count += bytes.len();
                    batch_data.push(bytes);

                    // Break if we hit the size threshold
                    if byte_count >= self.config.max_batch_bytes as usize {
                        break;
                    }
                }
                _ => continue, // Timeout, keep checking time limit
            }
        }

        // TELEMETRY: Record if we cleared via Size or Time
        let clear_reason = if byte_count >= self.config.max_batch_bytes as usize { "SIZE" } else { "TIME_LINGER" };
        telemetry::emit_metric("kafka_batch_cleared", batch_data.len() as f64, Some(clear_reason));

        if batch_data.is_empty() {
            return Some(Err(anyhow::anyhow!("Empty batch"))); // Pipeline handles and retries
        }

        // Convert the raw JSON bytes into a Polars DataFrame instantly
        let df = convert_json_lines_to_dataframe(batch_data)?;
        Some(Ok(Context::new(df)))
    }
}