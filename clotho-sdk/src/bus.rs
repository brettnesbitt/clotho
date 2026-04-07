// clotho-sdk/src/bus.rs
// BusSource and BusSink for inter-stage communication in DAG pipelines.
// Uses NATS JetStream for durable, cross-pod message passing.
// Consumer groups ensure load-balanced delivery across replicas.

#[cfg(not(target_family = "wasm"))]
mod nats_bus {
    use crate::traits::{Source, Sink};
    use crate::types::Context;
    use anyhow::Result;
    use async_trait::async_trait;
    use std::fmt::Debug;
    use std::time::Duration;

    /// Configuration for a bus connection
    #[derive(Debug, Clone)]
    pub struct BusConfig {
        /// Name of the bus / NATS subject (e.g., "sieve_in")
        pub name: String,
        /// NATS URL (defaults to CLOTHO_NATS_URL env or nats://localhost:4222)
        pub nats_url: String,
    }

    impl BusConfig {
        pub fn new(name: impl Into<String>) -> Self {
            let nats_url = std::env::var("CLOTHO_NATS_URL")
                .unwrap_or_else(|_| "nats://clotho-system-bus.clotho-system.svc.cluster.local:4222".to_string());
            Self {
                name: name.into(),
                nats_url,
            }
        }

        pub fn with_nats_url(mut self, url: impl Into<String>) -> Self {
            self.nats_url = url.into();
            self
        }
    }

    /// Ensure the JetStream stream exists for a given bus name.
    /// Creates a stream named "CLOTHO_{NAME}" with subject "clotho.bus.{name}".
    async fn ensure_stream(
        jetstream: &async_nats::jetstream::Context,
        bus_name: &str,
    ) -> Result<()> {
        let stream_name = format!("CLOTHO_{}", bus_name.to_uppercase().replace('-', "_"));
        let subject = format!("clotho.bus.{}", bus_name);

        match jetstream.get_stream(&stream_name).await {
            Ok(_) => Ok(()),
            Err(_) => {
                jetstream
                    .create_stream(async_nats::jetstream::stream::Config {
                        name: stream_name.clone(),
                        subjects: vec![subject],
                        retention: async_nats::jetstream::stream::RetentionPolicy::WorkQueue,
                        max_age: Duration::from_secs(24 * 3600),
                        storage: async_nats::jetstream::stream::StorageType::Memory,
                        ..Default::default()
                    })
                    .await
                    .map_err(|e| anyhow::anyhow!("Failed to create stream {}: {}", stream_name, e))?;
                eprintln!("[Bus] Created JetStream stream: {}", stream_name);
                Ok(())
            }
        }
    }

    /// A Source that reads from a NATS JetStream consumer.
    /// Uses a durable pull consumer for load-balanced delivery across replicas.
    pub struct BusSource<T> {
        config: BusConfig,
        messages: async_nats::jetstream::consumer::pull::Stream,
        _phantom: std::marker::PhantomData<T>,
    }

    impl<T> BusSource<T>
    where
        T: serde::de::DeserializeOwned + Send + Sync + 'static,
    {
        pub async fn new(config: BusConfig) -> Result<Self> {
            let client = async_nats::connect(&config.nats_url)
                .await
                .map_err(|e| anyhow::anyhow!("Failed to connect to NATS at {}: {}", config.nats_url, e))?;

            let jetstream = async_nats::jetstream::new(client);
            ensure_stream(&jetstream, &config.name).await?;

            let stream_name = format!("CLOTHO_{}", config.name.to_uppercase().replace('-', "_"));
            let consumer_name = format!("{}-workers", config.name);

            let stream = jetstream.get_stream(&stream_name).await
                .map_err(|e| anyhow::anyhow!("Failed to get stream {}: {}", stream_name, e))?;

            let consumer = stream
                .get_or_create_consumer(&consumer_name, async_nats::jetstream::consumer::pull::Config {
                    durable_name: Some(consumer_name.clone()),
                    ack_policy: async_nats::jetstream::consumer::AckPolicy::Explicit,
                    ..Default::default()
                })
                .await
                .map_err(|e| anyhow::anyhow!("Failed to create consumer {}: {}", consumer_name, e))?;

            let messages = consumer.messages().await
                .map_err(|e| anyhow::anyhow!("Failed to get message stream: {}", e))?;

            eprintln!("[BusSource] Connected to NATS stream {} via consumer {}", stream_name, consumer_name);

            Ok(Self {
                config,
                messages,
                _phantom: std::marker::PhantomData,
            })
        }

        pub fn name(&self) -> &str {
            &self.config.name
        }
    }

    #[async_trait]
    impl<T> Source<T> for BusSource<T>
    where
        T: serde::de::DeserializeOwned + Send + Sync + Debug + 'static,
    {
        async fn next(&mut self) -> Option<Result<Context<T>>> {
            use futures_util::StreamExt;
            match self.messages.next().await {
                Some(Ok(msg)) => {
                    // Ack the message before processing (at-most-once for speed)
                    if let Err(e) = msg.ack().await {
                        eprintln!("[BusSource] Failed to ack message: {}", e);
                    }
                    match serde_json::from_slice::<Context<T>>(&msg.payload) {
                        Ok(ctx) => Some(Ok(ctx)),
                        Err(e) => Some(Err(anyhow::anyhow!("Failed to deserialize bus message: {}", e))),
                    }
                }
                Some(Err(e)) => Some(Err(anyhow::anyhow!("NATS receive error: {}", e))),
                None => None,
            }
        }
    }

    /// A Sink that writes to a NATS JetStream subject.
    pub struct BusSink<T> {
        config: BusConfig,
        jetstream: async_nats::jetstream::Context,
        subject: String,
        _phantom: std::marker::PhantomData<T>,
    }

    impl<T> BusSink<T>
    where
        T: serde::Serialize + Send + Sync + 'static,
    {
        pub async fn new(config: BusConfig) -> Result<Self> {
            let client = async_nats::connect(&config.nats_url)
                .await
                .map_err(|e| anyhow::anyhow!("Failed to connect to NATS at {}: {}", config.nats_url, e))?;

            let jetstream = async_nats::jetstream::new(client);
            ensure_stream(&jetstream, &config.name).await?;

            let subject = format!("clotho.bus.{}", config.name);
            eprintln!("[BusSink] Connected to NATS, publishing to {}", subject);

            Ok(Self {
                config,
                jetstream,
                subject,
                _phantom: std::marker::PhantomData,
            })
        }

        pub fn name(&self) -> &str {
            &self.config.name
        }
    }

    #[async_trait]
    impl<T> Sink<T> for BusSink<T>
    where
        T: serde::Serialize + Send + Sync + Debug + 'static,
    {
        async fn write(&mut self, ctx: Context<T>) -> Result<()> {
            let bytes = serde_json::to_vec(&ctx)
                .map_err(|e| anyhow::anyhow!("Failed to serialize bus message: {}", e))?;

            self.jetstream
                .publish(self.subject.clone(), bytes.into())
                .await
                .map_err(|e| anyhow::anyhow!("Failed to publish to bus '{}': {}", self.config.name, e))?
                .await
                .map_err(|e| anyhow::anyhow!("Publish ack failed for bus '{}': {}", self.config.name, e))?;

            Ok(())
        }
    }

    /// Helper function to create a bus source
    pub async fn bus_source<T>(name: impl Into<String>) -> Result<BusSource<T>>
    where
        T: serde::de::DeserializeOwned + Send + Sync + 'static,
    {
        let config = BusConfig::new(name);
        BusSource::new(config).await
    }

    /// Helper function to create a bus sink
    pub async fn bus_sink<T>(name: impl Into<String>) -> Result<BusSink<T>>
    where
        T: serde::Serialize + Send + Sync + 'static,
    {
        let config = BusConfig::new(name);
        BusSink::new(config).await
    }
}

#[cfg(not(target_family = "wasm"))]
pub use nats_bus::*;
