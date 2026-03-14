// clotho-sdk/src/daemon_support.rs
//
// Runtime support for #[clotho::daemon] pipelines.
// Provides graceful shutdown signal handling and replay endpoint for canary testing.

/// Returns a future that completes when a shutdown signal (SIGINT or SIGTERM) is received.
/// On non-Unix platforms, only Ctrl+C (SIGINT) is handled.
pub async fn shutdown_signal() {
    let ctrl_c = tokio::signal::ctrl_c();

    #[cfg(unix)]
    {
        let mut sigterm = tokio::signal::unix::signal(
            tokio::signal::unix::SignalKind::terminate(),
        )
        .expect("Failed to install SIGTERM handler");

        tokio::select! {
            _ = ctrl_c => { eprintln!("[Clotho] Received SIGINT"); }
            _ = sigterm.recv() => { eprintln!("[Clotho] Received SIGTERM"); }
        }
    }

    #[cfg(not(unix))]
    {
        ctrl_c.await.expect("Failed to listen for Ctrl+C");
        eprintln!("[Clotho] Received Ctrl+C");
    }
}

/// Spawn a lightweight HTTP listener on port 8127 for the canary replay endpoint.
/// This runs as a background task alongside the main pipeline. The Control Plane
/// API hits this to test individual DLQ records against the live pipeline.
///
/// v1: Returns 501 stub. Future versions will accept a ReplayHandler registration
/// so pipeline authors can wire their transform chains into the replay path.
pub fn spawn_replay_listener() {
    tokio::spawn(async move {
        let listener = match tokio::net::TcpListener::bind("0.0.0.0:8127").await {
            Ok(l) => { eprintln!("[Clotho] Replay listener on :8127"); l }
            Err(e) => { eprintln!("[Clotho] Replay listener failed to bind: {}", e); return; }
        };

        loop {
            let (mut stream, _) = match listener.accept().await {
                Ok(conn) => conn,
                Err(_) => continue,
            };

            tokio::spawn(async move {
                use tokio::io::{AsyncReadExt, AsyncWriteExt};
                let mut buf = vec![0u8; 8192];
                let _ = stream.read(&mut buf).await;

                // Check if it's a POST to /clotho/replay
                let req = String::from_utf8_lossy(&buf);
                if req.starts_with("POST /clotho/replay") {
                    let body = r#"{"status":"not_implemented","message":"Replay handler not yet registered. Pipeline must opt-in via clotho::register_replay_handler()"}"#;
                    let resp = format!(
                        "HTTP/1.1 501 Not Implemented\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
                        body.len(), body
                    );
                    let _ = stream.write_all(resp.as_bytes()).await;
                } else if req.starts_with("GET /healthz") || req.starts_with("GET /clotho/health") {
                    let body = r#"{"status":"ok","replay":"stub"}"#;
                    let resp = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
                        body.len(), body
                    );
                    let _ = stream.write_all(resp.as_bytes()).await;
                } else {
                    let resp = "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n";
                    let _ = stream.write_all(resp.as_bytes()).await;
                }
            });
        }
    });
}
