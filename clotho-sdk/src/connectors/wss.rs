// clotho-sdk/src/connectors/wss.rs
// WebSocket Source for streaming data from WebSocket endpoints.
// Used by native pipelines (Jetstream, firehoses, etc.)

#[cfg(feature = "native")]
use crate::traits::Source;
#[cfg(feature = "native")]
use crate::types::Context;
#[cfg(feature = "native")]
use anyhow::Result;
#[cfg(feature = "native")]
use async_trait::async_trait;
#[cfg(feature = "native")]
use futures_util::StreamExt;

/// A Source that reads raw bytes from a WebSocket connection.
/// Each message is yielded as a Context<Vec<u8>>.
/// Auto-reconnects on disconnect with exponential backoff (max 30s).
#[cfg(feature = "native")]
pub struct WebsocketSource {
    url: String,
    reader: Option<
        futures_util::stream::SplitStream<
            tokio_tungstenite::WebSocketStream<
                tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
            >,
        >,
    >,
}

#[cfg(feature = "native")]
impl WebsocketSource {
    /// Create a WebSocket source for the given URL.
    /// Connection is established lazily on the first call to `next()`.
    pub fn new(url: &str) -> Self {
        Self {
            url: url.to_string(),
            reader: None,
        }
    }

    /// Establish (or re-establish) the WebSocket connection.
    async fn connect(&mut self) -> Result<()> {
        let (ws_stream, _) = tokio_tungstenite::connect_async(&self.url)
            .await
            .map_err(|e| anyhow::anyhow!("WebSocket connect failed: {}", e))?;
        let (_, reader) = futures_util::StreamExt::split(ws_stream);
        self.reader = Some(reader);
        Ok(())
    }
}

#[cfg(feature = "native")]
#[async_trait]
impl Source<Vec<u8>> for WebsocketSource {
    async fn next(&mut self) -> Option<Result<Context<Vec<u8>>>> {
        use tokio_tungstenite::tungstenite::Message;

        let mut backoff_secs: u64 = 1;
        const MAX_BACKOFF: u64 = 30;

        loop {
            // Lazy-connect or reconnect if the reader is absent
            if self.reader.is_none() {
                match self.connect().await {
                    Ok(()) => {
                        eprintln!("[WebSocket] Connected to {}", self.url);
                        backoff_secs = 1;
                    }
                    Err(e) => {
                        eprintln!("[WebSocket] Connect failed (retry in {}s): {}", backoff_secs, e);
                        tokio::time::sleep(std::time::Duration::from_secs(backoff_secs)).await;
                        backoff_secs = (backoff_secs * 2).min(MAX_BACKOFF);
                        continue;
                    }
                }
            }

            let reader = self.reader.as_mut().unwrap();
            match reader.next().await {
                Some(Ok(msg)) => {
                    let bytes = match msg {
                        Message::Text(t) => t.into_bytes(),
                        Message::Binary(b) => b,
                        Message::Ping(_) | Message::Pong(_) => continue,
                        Message::Close(_) => {
                            eprintln!("[WebSocket] Server sent Close frame, reconnecting...");
                            self.reader = None;
                            continue;
                        }
                        _ => continue,
                    };
                    return Some(Ok(Context::root(bytes, "websocket")));
                }
                Some(Err(e)) => {
                    eprintln!("[WebSocket] Error (reconnecting in {}s): {}", backoff_secs, e);
                    self.reader = None;
                    tokio::time::sleep(std::time::Duration::from_secs(backoff_secs)).await;
                    backoff_secs = (backoff_secs * 2).min(MAX_BACKOFF);
                    continue;
                }
                None => {
                    eprintln!("[WebSocket] Stream ended, reconnecting...");
                    self.reader = None;
                    continue;
                }
            }
        }
    }
}
