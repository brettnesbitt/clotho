// clotho-sdk/src/connectors/wss.rs
use crate::traits::{Source, Context};
use anyhow::{Context as AnyhowContext, Result};
use async_trait::async_trait;
use futures_util::StreamExt;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message, WebSocketStream, MaybeTlsStream};
use tokio::net::TcpStream;
use tokio::time::{sleep, Duration};

pub struct WebsocketSource {
    url: String,
    stream: Option<WebSocketStream<MaybeTlsStream<TcpStream>>>,
    reconnect_attempts: u32,
    max_reconnects: u32,
}

impl WebsocketSource {
    pub fn new(url: &str) -> Self {
        Self {
            url: url.to_string(),
            stream: None,
            reconnect_attempts: 0,
            max_reconnects: 10,
        }
    }

    /// Internal helper to establish the connection with exponential backoff
    async fn ensure_connected(&mut self) -> Result<()> {
        if self.stream.is_some() {
            return Ok(());
        }

        while self.reconnect_attempts < self.max_reconnects {
            match connect_async(&self.url).await {
                Ok((ws_stream, _response)) => {
                    eprintln!("[Clotho] Connected to WebSocket: {}", self.url);
                    self.stream = Some(ws_stream);
                    self.reconnect_attempts = 0; // Reset on success
                    return Ok(());
                }
                Err(e) => {
                    self.reconnect_attempts += 1;
                    let backoff = Duration::from_secs(2u64.pow(self.reconnect_attempts.min(6)));
                    eprintln!("[Clotho] WSS connection failed: {}. Retrying in {:?}...", e, backoff);
                    sleep(backoff).await;
                }
            }
        }
        Err(anyhow::anyhow!("Max WebSocket reconnection attempts reached"))
    }
}

// THE STREAM ENGINE: Yields messages one-by-one as raw bytes
#[async_trait]
impl Source<Vec<u8>> for WebsocketSource {
    async fn next(&mut self) -> Option<Result<Context<Vec<u8>>>> {
        loop {
            if let Err(e) = self.ensure_connected().await {
                return Some(Err(e));
            }

            let stream = self.stream.as_mut().unwrap();

            match stream.next().await {
                Some(Ok(msg)) => {
                    let trace_id = uuid::Uuid::new_v4().to_string();
                    
                    match msg {
                        // Handle standard JSON/Text WebSockets
                        Message::Text(text) => return Some(Ok(Context::root(text.into_bytes(), &trace_id))),
                        
                        // CRITICAL FOR BLUESKY: Handle Binary DAG-CBOR payloads
                        Message::Binary(bin) => return Some(Ok(Context::root(bin, &trace_id))),
                        
                        // Handle protocol-level pings automatically
                        Message::Ping(_) | Message::Pong(_) => continue,
                        Message::Close(_) => {
                            eprintln!("[Clotho] WSS server closed connection. Reconnecting...");
                            self.stream = None; // Force reconnect on next loop
                            continue;
                        }
                        _ => continue,
                    }
                }
                Some(Err(e)) => {
                    eprintln!("[Clotho] WSS read error: {}. Reconnecting...", e);
                    self.stream = None; // Force reconnect
                    continue;
                }
                None => {
                    self.stream = None; // Stream exhausted, force reconnect
                    continue;
                }
            }
        }
    }
}