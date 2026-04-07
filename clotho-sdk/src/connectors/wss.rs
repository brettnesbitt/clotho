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
#[cfg(feature = "native")]
pub struct WebsocketSource {
    reader: futures_util::stream::SplitStream<
        tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
    >,
}

#[cfg(feature = "native")]
impl WebsocketSource {
    /// Connect to a WebSocket endpoint and return a Source.
    /// The URL should be a full `wss://` or `ws://` URI.
    pub fn new(url: &str) -> Self {
        // We can't async in a constructor easily, so we store the URL
        // and connect lazily. For now, use a synchronous-looking API
        // that panics on connection failure (pipelines fail-fast anyway).
        let url_owned = url.to_string();
        let rt = tokio::runtime::Handle::current();
        let reader = std::thread::spawn(move || {
            rt.block_on(async {
                let (ws_stream, _) = tokio_tungstenite::connect_async(&url_owned)
                    .await
                    .expect(&format!("Failed to connect to WebSocket: {}", url_owned));
                let (_, reader) = futures_util::StreamExt::split(ws_stream);
                reader
            })
        })
        .join()
        .expect("WebSocket connection thread panicked");

        Self { reader }
    }
}

#[cfg(feature = "native")]
#[async_trait]
impl Source<Vec<u8>> for WebsocketSource {
    async fn next(&mut self) -> Option<Result<Context<Vec<u8>>>> {
        use tokio_tungstenite::tungstenite::Message;

        loop {
            match self.reader.next().await {
                Some(Ok(msg)) => {
                    let bytes = match msg {
                        Message::Text(t) => t.into_bytes(),
                        Message::Binary(b) => b,
                        Message::Ping(_) | Message::Pong(_) => continue,
                        Message::Close(_) => return None,
                        _ => continue,
                    };
                    return Some(Ok(Context::root(bytes, "websocket")));
                }
                Some(Err(e)) => {
                    return Some(Err(anyhow::anyhow!("WebSocket error: {}", e)));
                }
                None => return None,
            }
        }
    }
}
