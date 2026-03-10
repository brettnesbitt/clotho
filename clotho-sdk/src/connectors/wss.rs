use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};

pub struct WebsocketSource {
    stream: Option<WebSocketStream<...>>,
    url: String,
}

impl Source<String> for WebsocketSource {
    async fn next(&mut self) -> Option<Result<Context<String>>> {
        if self.stream.is_none() {
            let (ws_stream, _) = connect_async(&self.url).await?;
            self.stream = Some(ws_stream);
        }
        
        while let Some(msg) = self.stream.as_mut().unwrap().next().await {
            match msg {
                Ok(Message::Text(text)) => return Some(Ok(Context::new(text))),
                Ok(Message::Binary(bin)) => continue, // Handle binary?
                Err(_) => return None, // Reconnect needed
                _ => continue,
            }
        }
        None
    }
}