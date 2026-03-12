use clotho::Pipeline;
use clotho::builtins::{VecSource, ConsoleSink};
use anyhow::Result;
use std::time::Duration;

#[clotho::main]
async fn main() -> Result<()> {
    // Mock daily ideas (in production, this would use HttpSource to fetch from Stockseer API)
    let ideas: Vec<String> = vec![
        "AAPL: Entry $175.50, Stop $170.00, Target $185.00 - Strong momentum after earnings".into(),
        "NVDA: Entry $880.00, Stop $850.00, Target $920.00 - AI demand continues".into(),
        "TSLA: Entry $195.00, Stop $185.00, Target $210.00 - Production ramp accelerating".into(),
    ];

    Pipeline::stream(VecSource::new(ideas))
        .map(|idea| {
            println!("[Signal] {}", idea);
            
            // Simulate processing time (30 seconds per idea)
            // This allows us to see the pipeline in "Running" state in the UI
            std::thread::sleep(Duration::from_secs(30));
            
            Ok(idea)
        })
        .run(ConsoleSink::new())
        .await?;

    Ok(())
}
