use clotho::Pipeline;
use clotho::connectors::kafka::{KafkaSource, KafkaSink};
use anyhow::Result;

#[clotho::main]
async fn main() -> Result<()> {

    let source = KafkaSource::new(brokers.clone(), "raw_clicks".into(), 0, 0).await?;
    let sink = KafkaSink::new(brokers, "enriched_clicks".into(), 0).await?;

    Pipeline::batch(source)
        .enrich(mongo_db, "user_id", JoinMode::Left)
        .run(sink)
        .await
}