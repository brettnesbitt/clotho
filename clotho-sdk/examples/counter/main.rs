use clotho_sdk::Pipeline;
use clotho_sdk::builtins::{VecSource, ConsoleSink};
use anyhow::Result;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    println!("🧵 Clotho Counter Pipeline");
    println!("   Processing 100 items with telemetry");
    println!();

    // Create a source with 100 items
    let items: Vec<u64> = (1..=100).collect();
    let source = VecSource::new(items);
    
    // Build and run the pipeline
    Pipeline::stream(source)
        .map(|num| {
            // Double each number
            Ok(num * 2)
        })
        .run(ConsoleSink::new())
        .await?;
    
    println!();
    println!("✅ Pipeline completed successfully");
    
    Ok(())
}
