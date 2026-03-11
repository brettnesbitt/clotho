use spin_sdk::http::{IntoResponse, Request, Response};
use spin_sdk::http_component;

#[http_component]
fn handle_daily_idea(_req: Request) -> anyhow::Result<impl IntoResponse> {
    // Mock daily ideas (in production, this would fetch from Stockseer API)
    let ideas = vec![
        "AAPL: Entry $175.50, Stop $170.00, Target $185.00 - Strong momentum after earnings",
        "NVDA: Entry $880.00, Stop $850.00, Target $920.00 - AI demand continues",
        "TSLA: Entry $195.00, Stop $185.00, Target $210.00 - Production ramp accelerating",
    ];

    let mut output = String::from("Clotho Daily Idea Pipeline\n");
    output.push_str("   Simulating daily idea fetch\n\n");

    for idea in &ideas {
        output.push_str(&format!("[Signal] {}\n", idea));
    }

    output.push_str("\nPipeline completed successfully\n");

    Ok(Response::builder()
        .status(200)
        .header("content-type", "text/plain")
        .body(output)
        .build())
}
