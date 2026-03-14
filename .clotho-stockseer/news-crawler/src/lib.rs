use clotho::Pipeline;
use clotho::connectors::mongo::MongoSink;
use serde::Deserialize;
use serde_json::{json, Value};
use std::time::Duration;

/// Feed configuration loaded from CRAWLER_FEEDS env var.
/// Example: [{"url": "https://example.com/rss", "symbols": ["AAPL"]}]
#[derive(Deserialize, Clone)]
struct FeedConfig {
    url: String,
    #[serde(default)]
    symbols: Vec<String>,
}

/// Fetch and parse all configured RSS feeds, returning articles as Values.
async fn fetch_all_feeds(
    http: &reqwest::Client,
    feeds: &[FeedConfig],
    source_name: &str,
) -> Vec<Value> {
    let mut articles = Vec::new();

    for feed in feeds {
        match http.get(&feed.url).send().await {
            Ok(resp) => match resp.bytes().await {
                Ok(bytes) => match feed_rs::parser::parse(&bytes[..]) {
                    Ok(parsed) => {
                        let count = parsed.entries.len();
                        for entry in &parsed.entries {
                            if let Some(article) = feed_entry_to_value(entry, source_name, &feed.symbols) {
                                articles.push(article);
                            }
                        }
                        eprintln!("[{}] {} → {} entries", source_name, feed.url, count);
                    }
                    Err(e) => eprintln!("[{}] Parse error {}: {}", source_name, feed.url, e),
                },
                Err(e) => eprintln!("[{}] Read error {}: {}", source_name, feed.url, e),
            },
            Err(e) => eprintln!("[{}] Fetch error {}: {}", source_name, feed.url, e),
        }

        // Be polite between feeds
        std::thread::sleep(Duration::from_secs(1));
    }

    articles
}

/// Convert a feed entry into a serde_json::Value matching the existing NewsArticle schema.
fn feed_entry_to_value(
    entry: &feed_rs::model::Entry,
    source: &str,
    symbols: &[String],
) -> Option<Value> {
    let link = entry.links.first()?.href.clone();
    if link.is_empty() {
        return None;
    }

    let title = entry.title.as_ref().map(|t| t.content.clone()).unwrap_or_default();
    let summary = entry.summary.as_ref().map(|s| s.content.clone());
    let content = entry
        .content
        .as_ref()
        .and_then(|c| c.body.clone())
        .or_else(|| summary.clone())
        .unwrap_or_default();
    let author = entry.authors.first().map(|a| a.name.clone()).unwrap_or_default();

    let published = entry.published.or(entry.updated).unwrap_or_else(|| chrono::Utc::now());
    let iso = published.to_rfc3339();
    let now = chrono::Utc::now();

    let mut msg = json!({
        "T": "n",
        "headline": title,
        "content": content,
        "author": author,
        "created_at": iso,
        "updated_at": iso,
        "url": link,
        "source": source,
    });

    if let Some(ref s) = summary {
        msg["summary"] = json!(s);
    }
    if !symbols.is_empty() {
        msg["symbols"] = json!(symbols);
    }

    Some(json!({
        "message": msg,
        "timestamp": { "$date": published.to_rfc3339() },
        "processed_at": { "$date": now.to_rfc3339() },
    }))
}

// ─── Clotho Job ─────────────────────────────────────────────────────────────
// Runs once per invocation. The Clotho operator handles scheduling
// via Pipeline CRD schedule (interval/cron mode).
//
// Flow: Fetch RSS → Collect articles → VecSource → MongoSink
// Dedup via unique index on `message.url` — duplicates silently skipped by MongoSink.

#[clotho::job]
async fn main() -> Result<()> {
    let source_name = std::env::var("CRAWLER_SOURCE").expect("CRAWLER_SOURCE required");
    let feeds_json = std::env::var("CRAWLER_FEEDS").expect("CRAWLER_FEEDS required");
    let mongo_uri = std::env::var("MONGO_URI").expect("MONGO_URI required");
    let mongo_db = std::env::var("MONGO_DB").unwrap_or_else(|_| "production_market_data".into());

    let feeds: Vec<FeedConfig> = serde_json::from_str(&feeds_json).expect("Invalid CRAWLER_FEEDS JSON");

    eprintln!("[{}] Starting — {} feeds", source_name, feeds.len());

    let http = reqwest::Client::builder()
        .user_agent("Stockseer-Crawler/1.0")
        .timeout(Duration::from_secs(30))
        .build()?;

    // 1. Fetch all RSS feeds, collect articles
    let articles = fetch_all_feeds(&http, &feeds, &source_name).await;
    eprintln!("[{}] Collected {} articles", source_name, articles.len());

    if articles.is_empty() {
        eprintln!("[{}] No articles found, exiting", source_name);
        return Ok(());
    }

    // 2. Bulk insert into MongoDB via Pipeline::once
    // Dedup handled via unique index on `message.url` — duplicates silently skipped
    let sink = MongoSink::new(&mongo_uri, &mongo_db, "news").await?;

    Pipeline::once(articles)
        .run(sink)
        .await
}
