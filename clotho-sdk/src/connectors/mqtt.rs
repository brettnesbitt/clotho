use rumqttc::{MqttOptions, AsyncClient, Event, Packet};

pub struct MqttSource {
    eventloop: rumqttc::EventLoop,
}

impl Source<Vec<u8>> for MqttSource {
    async fn next(&mut self) -> Option<Result<Context<Vec<u8>>>> {
        loop {
            // Poll the event loop
            match self.eventloop.poll().await {
                Ok(Event::Incoming(Packet::Publish(p))) => {
                    return Some(Ok(Context::new(p.payload.to_vec())));
                }
                Err(_) => return None, // Connection lost
                _ => continue, // Ping/Ack events, ignore
            }
        }
    }
}