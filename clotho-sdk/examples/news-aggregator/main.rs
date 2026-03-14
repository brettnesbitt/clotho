use clotho::prelude::*;
use clotho::connectors::http::HttpSource;
use clotho::connectors::mongo::{MongoSink, MongoLookup};
use polars::prelude::*;
use bson::doc;

#[clotho::main]
async fn main(_req: Request) -> Result<Response> {
    
    // 1. THE SOURCE
    let news_source = HttpSource::new("https://jsonplaceholder.typicode.com/posts");

    // 2. THE SINK (Native TCP/TLS Connection)
    let mongo_uri = std::env::var("MONGO_URI").unwrap_or_else(|_| "mongodb://localhost:27017".into());
    let mongo_sink = MongoSink::new(&mongo_uri, "news_db", "articles").await?;

    // 3. THE PIPELINE
    Pipeline::batch(news_source)
        .map(|lf| {
            lf.select([
                col("title"),
                col("body").alias("description"),
            ])
            .slice(0, 10)
        })
        .run(mongo_sink)
        .await?;

    Ok(Response::new(200, "Synced 10 articles to MongoDB using native drivers!"))
}