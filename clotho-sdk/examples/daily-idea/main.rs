use clotho_sdk::Pipeline;
use clotho_sdk::builtins::{VecSource, ConsoleSink};
use anyhow::Result;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    println!("🧵 Clotho Daily Idea Pipeline");
    println!("   Simulating daily idea fetch");
    println!();

    // Mock daily ideas (in production, this would fetch from an external API)
    let ideas: Vec<String> = vec![
        "AAPL: Entry $175.50, Stop $170.00, Target $185.00 - Strong momentum after earnings".into(),
        "NVDA: Entry $880.00, Stop $850.00, Target $920.00 - AI demand continues".into(),
        "TSLA: Entry $195.00, Stop $185.00, Target $210.00 - Production ramp accelerating".into(),
    ];
    
    let source = VecSource::new(ideas);
    
    // Build and run the pipeline
    Pipeline::stream(source)
        .map(|idea| {
            println!("📊 {}", idea);
            Ok(idea)
        })
        .run(ConsoleSink::new())
        .await?;
    
    println!();
    println!("✅ Pipeline completed successfully");
    
    Ok(())
}
