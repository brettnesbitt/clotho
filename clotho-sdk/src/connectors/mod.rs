// Clotho SDK Connectors
// Sources and Sinks for common integrations

// Working connectors
pub mod stdout;

// MongoDB connector (works on both Spin/WASM and native)
pub mod mongo;

// WebSocket connector (requires tokio runtime + native networking)
#[cfg(feature = "native")]
pub mod wss;

// TODO: Fix these connectors - missing dependencies or incomplete implementations
// pub mod http;
// pub mod kafka;       // needs rskafka + time crates
// pub mod mongo_atlas; // needs batch feature guard cleanup
// pub mod mqtt;
// pub mod postgres;
// pub mod s3;
// pub mod snowflake;
