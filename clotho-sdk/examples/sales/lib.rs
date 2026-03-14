use clotho::Pipeline;
use clotho::connectors::snowflake::{SnowflakeSource, SnowflakeConfig};
use clotho::connectors::postgres::PostgresLookup;
use clotho::connectors::http::HttpSink;

#[clotho::main]
async fn main() -> Result<()> {

    let sf_source = SnowflakeSource::new(config, "SELECT * FROM daily_sales WHERE date = CURRENT_DATE()");
    let pg_lookup = PostgresLookup::new(pg_conn, "users", "user_id").await?;
    let api_sink = HttpSink::new("https://api.internal.com/sales_sync");

    Pipeline::batch(sf_source)
        .enrich(pg_lookup, "user_id", JoinMode::Left)
        .run(api_sink) // Automatically converts the final DataFrame to a JSON array for the HTTP POST!
        .await

}