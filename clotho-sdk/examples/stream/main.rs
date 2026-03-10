use clotho::prelude::*;


/* Kafka Example */

#[clotho::main]
async fn main() -> Result<()> {
    // The macro sees 'main', builds a Tokio runtime, and runs this.
    Pipeline::stream(KafkaSource::new("brokers", "topic"))
        .run(PostgresSink::new("db", "INSERT..."))
        .await
}