// clotho-sdk/src/bus.rs
// BusSource and BusSink for inter-stage communication in DAG pipelines

use crate::traits::{Source, Sink};
use crate::types::Context;
use anyhow::Result;
use async_trait::async_trait;
use std::fmt::Debug;
use std::sync::Arc;
use tokio::sync::{broadcast, RwLock};

/// Configuration for a bus connection
#[derive(Debug, Clone)]
pub struct BusConfig {
    /// Name of the bus (e.g., "stage_1_out")
    pub name: String,
    /// Buffer size for the broadcast channel
    pub buffer_size: usize,
}

impl BusConfig {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            buffer_size: 1024,
        }
    }

    pub fn with_buffer_size(mut self, size: usize) -> Self {
        self.buffer_size = size;
        self
    }
}

/// Global bus registry for managing inter-stage communication
/// This is a singleton that manages all bus channels in a process
pub struct BusRegistry {
    buses: Arc<RwLock<std::collections::HashMap<String, broadcast::Sender<Vec<u8>>>>>,
}

impl BusRegistry {
    fn new() -> Self {
        Self {
            buses: Arc::new(RwLock::new(std::collections::HashMap::new())),
        }
    }

    /// Get or create a bus channel
    async fn get_or_create_bus(&self, name: &str, buffer_size: usize) -> broadcast::Sender<Vec<u8>> {
        let buses = self.buses.read().await;
        if let Some(tx) = buses.get(name) {
            return tx.clone();
        }
        drop(buses);

        let mut buses = self.buses.write().await;
        // Double-check after acquiring write lock
        if let Some(tx) = buses.get(name) {
            return tx.clone();
        }

        let (tx, _) = broadcast::channel(buffer_size);
        buses.insert(name.to_string(), tx.clone());
        tx
    }

}

lazy_static::lazy_static! {
    static ref BUS_REGISTRY: BusRegistry = BusRegistry::new();
}

/// A Source that reads from an internal bus channel
/// Used by worker stages to receive data from upstream stages
pub struct BusSource<T> {
    config: BusConfig,
    receiver: broadcast::Receiver<Vec<u8>>,
    _phantom: std::marker::PhantomData<T>,
}

impl<T> BusSource<T>
where
    T: serde::de::DeserializeOwned + Send + Sync + 'static,
{
    pub async fn new(config: BusConfig) -> Result<Self> {
        let tx = BUS_REGISTRY.get_or_create_bus(&config.name, config.buffer_size).await;
        let receiver = tx.subscribe();

        Ok(Self {
            config,
            receiver,
            _phantom: std::marker::PhantomData,
        })
    }

    pub fn name(&self) -> &str {
        &self.config.name
    }
}

#[cfg_attr(not(target_family = "wasm"), async_trait)]
#[cfg_attr(target_family = "wasm", async_trait(?Send))]
impl<T> Source<T> for BusSource<T>
where
    T: serde::de::DeserializeOwned + Send + Sync + Debug + 'static,
{
    async fn next(&mut self) -> Option<Result<Context<T>>> {
        match self.receiver.recv().await {
            Ok(bytes) => {
                // Deserialize the context from bytes
                match serde_json::from_slice::<Context<T>>(&bytes) {
                    Ok(ctx) => Some(Ok(ctx)),
                    Err(e) => Some(Err(anyhow::anyhow!("Failed to deserialize bus message: {}", e))),
                }
            }
            Err(broadcast::error::RecvError::Lagged(skipped)) => {
                eprintln!("[BusSource] Lagged behind, skipped {} messages", skipped);
                // Try to receive again
                match self.receiver.recv().await {
                    Ok(bytes) => {
                        match serde_json::from_slice::<Context<T>>(&bytes) {
                            Ok(ctx) => Some(Ok(ctx)),
                            Err(e) => Some(Err(anyhow::anyhow!("Failed to deserialize bus message: {}", e))),
                        }
                    }
                    Err(e) => Some(Err(anyhow::anyhow!("Bus receive error: {}", e))),
                }
            }
            Err(broadcast::error::RecvError::Closed) => None,
        }
    }
}

/// A Sink that writes to an internal bus channel
/// Used by upstream stages to send data to downstream stages
pub struct BusSink<T> {
    config: BusConfig,
    sender: broadcast::Sender<Vec<u8>>,
    _phantom: std::marker::PhantomData<T>,
}

impl<T> BusSink<T>
where
    T: serde::Serialize + Send + Sync + 'static,
{
    pub async fn new(config: BusConfig) -> Result<Self> {
        let sender = BUS_REGISTRY.get_or_create_bus(&config.name, config.buffer_size).await;

        Ok(Self {
            config,
            sender,
            _phantom: std::marker::PhantomData,
        })
    }

    pub fn name(&self) -> &str {
        &self.config.name
    }

    /// Get the number of active receivers
    pub fn receiver_count(&self) -> usize {
        self.sender.receiver_count()
    }
}

#[cfg_attr(not(target_family = "wasm"), async_trait)]
#[cfg_attr(target_family = "wasm", async_trait(?Send))]
impl<T> Sink<T> for BusSink<T>
where
    T: serde::Serialize + Send + Sync + Debug + 'static,
{
    async fn write(&mut self, ctx: Context<T>) -> Result<()> {
        // Serialize the context to bytes
        let bytes = serde_json::to_vec(&ctx)
            .map_err(|e| anyhow::anyhow!("Failed to serialize bus message: {}", e))?;

        // Send to all receivers
        match self.sender.send(bytes) {
            Ok(receiver_count) => {
                if receiver_count == 0 {
                    eprintln!("[BusSink] Warning: No active receivers for bus '{}'", self.config.name);
                }
                Ok(())
            }
            Err(e) => {
                Err(anyhow::anyhow!("Failed to send to bus '{}': {}", self.config.name, e))
            }
        }
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
