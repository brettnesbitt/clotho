use clotho_sdk::{Pipeline, Result};
use clotho_sdk::connectors::http::HttpSource;
use clotho_sdk::connectors::stdout::StdoutSink;
use serde::Deserialize;

#[derive(Debug, Deserialize, Clone)]
struct DailyIdea {
    symbol: String,
    entry_price: f64,
    stop_price: f64,
    target_price: f64,
    rationale: String,
    timestamp: String,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    println!("🧵 Clotho Daily Idea Pipeline");
    println!("   Fetching latest idea from Stockseer API");
    println!();

    // Fetch daily idea from Stockseer API every 60 seconds
    let api_url = "https://stockseer.ai/api/idea-of-the-day";
    let source = HttpSource::<DailyIdea>::new(api_url, 60);
    
    // Build and run the pipeline
    Pipeline::stream(source)
        .map(|idea: DailyIdea| {
            // Format the output
            let output = format!(
                "📊 Daily Idea: {}\n\
                 💰 Entry: ${:.2}\n\
                 🛑 Stop: ${:.2}\n\
                 🎯 Target: ${:.2}\n\
                 📝 Rationale: {}\n\
                 ⏰ Generated: {}",
                idea.symbol,
                idea.entry_price,
                idea.stop_price,
                idea.target_price,
                idea.rationale,
                idea.timestamp
            );
            
            Ok(output)
        })
        .run(StdoutSink::new())
        .await?;
    
    println!();
    println!("✅ Pipeline completed successfully");
    
    Ok(())
}
