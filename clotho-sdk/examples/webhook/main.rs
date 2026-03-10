use clotho::prelude::*;

#[clotho::main]
async fn handle_webhook(req: Request) -> Result<Response> {
    // The macro sees 'req', builds a Spin component, injects telemetry, and runs this.
    Pipeline::once(req)
        .run(PostgresSink::new("db", "INSERT..."))
        .await?;
    Ok(Response::new(200, "OK"))
}