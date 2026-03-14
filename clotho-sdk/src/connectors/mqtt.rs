use crate::traits::{Source, Sink, Context};
use anyhow::{Context as AnyhowContext, Result};
use async_trait::async_trait;
use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS};
use std::time::Duration;

#[cfg(feature = "batch")]
use polars::prelude::*;

pub enum DataFormat {
    RawBytes,
    Json,
}

pub struct MqttSource {
    eventloop: rumqttc::EventLoop,
    format: DataFormat,
    max_batch_size: usize,
    max_wait_ms: u64,
}

impl MqttSource {
    pub async fn new(client_id: &str, host: &str, port: u16, topic: &str) -> Result<(AsyncClient, Self)> {
        let mut mqttoptions = MqttOptions::new(client_id, host, port);
        mqttoptions.set_keep_alive(Duration::from_secs(5));

        let (client, eventloop) = AsyncClient::new(mqttoptions, 100);
        
        // Subscribe to the topic with AtLeastOnce guarantees
        client.subscribe(topic, QoS::AtLeastOnce).await?;

        let source = Self {
            eventloop,
            format: DataFormat::Json, // Intelligent default
            max_batch_size: 10_000,
            max_wait_ms: 500,
        };

        Ok((client, source))
    }

    pub fn with_batch_config(mut self, size: usize, wait_ms: u64) -> Self {
        self.max_batch_size = size;
        self.max_wait_ms = wait_ms;
        self
    }
}

// =====================================================================
// STREAM ENGINE (Item-by-Item, Real-Time IoT Events)
// =====================================================================
#[async_trait]
impl Source<serde_json::Value> for MqttSource {
    async fn next(&mut self) -> Option<Result<Context<serde_json::Value>>> {
        loop {
            match self.eventloop.poll().await {
                Ok(Event::Incoming(Packet::Publish(p))) => {
                    // Extract Clotho Trace ID from MQTT v5 User Properties if it exists
                    let mut trace_id = uuid::Uuid::new_v4().to_string();
                    if let Some(props) = p.properties {
                        for (k, v) in props.user_properties {
                            if k == "X-Clotho-Trace-Id" { trace_id = v; }
                        }
                    }

                    match serde_json::from_slice::<serde_json::Value>(&p.payload) {
                        Ok(json) => return Some(Ok(Context::root(json, trace_id))),
                        Err(e) => return Some(Err(anyhow::anyhow!("MQTT JSON Parse Error: {}", e))),
                    }
                }
                Err(e) => return Some(Err(anyhow::anyhow!("MQTT Connection Lost: {}", e))),
                _ => continue, // Ignore Ping/Ack packets
            }
        }
    }
}

// =====================================================================
// BATCH ENGINE (High Throughput, Polars Columnar)
// =====================================================================
#[cfg(feature = "batch")]
#[async_trait]
impl Source<DataFrame> for MqttSource {
    async fn next(&mut self) -> Option<Result<Context<DataFrame>>> {
        let mut records: Vec<Vec<u8>> = Vec::with_capacity(self.max_batch_size);
        let start = std::time::Instant::now();
        let timeout_duration = tokio::time::Duration::from_millis(self.max_wait_ms);

        while records.len() < self.max_batch_size {
            let elapsed = start.elapsed();
            if elapsed >= timeout_duration { break; }

            // Poll the event loop with a strict timeout for micro-batching
            match tokio::time::timeout(timeout_duration - elapsed, self.eventloop.poll()).await {
                Ok(Ok(Event::Incoming(Packet::Publish(p)))) => {
                    records.push(p.payload.to_vec());
                }
                Ok(Err(e)) => return Some(Err(anyhow::anyhow!("MQTT error: {}", e))),
                Err(_) => break, // Timeout reached
                _ => continue,
            }
        }

        if records.is_empty() {
            return Some(Ok(Context::root(DataFrame::default(), "mqtt_batch_idle")));
        }

        // High-Speed NDJSON Memory Trick
        let ndjson_buffer = records.join(&b'\n');
        let cursor = std::io::Cursor::new(ndjson_buffer);
        
        match polars::io::ndjson::JsonLineReader::new(cursor).finish() {
            Ok(df) => Some(Ok(Context::root(df, "mqtt_batch_flush"))),
            Err(e) => Some(Err(anyhow::anyhow!("MQTT Batch Polars Parse Error: {}", e))),
        }
    }
}