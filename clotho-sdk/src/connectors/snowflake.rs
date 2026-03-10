pub struct SnowflakeSink {
    client: reqwest::Client,
    url: String,
    auth_token: String,
}

impl Sink<Value> for SnowflakeSink {
    async fn write(&mut self, ctx: Context<Value>) -> Result<()> {
        // Construct the SQL Statement
        // WARNING: In production, batch this! sending 1 HTTP req per row is slow.
        // Pipeline::batch is better for Snowflake.
        
        let sql = format!(
            "INSERT INTO events (json_data) SELECT PARSE_JSON('{}')", 
            serde_json::to_string(&ctx.data)?.replace("'", "''") // Basic SQL escape
        );

        let body = json!({
            "statement": sql,
            "database": "CLOTHO_DB",
            "schema": "PUBLIC",
            "warehouse": "COMPUTE_WH"
        });

        self.client.post(&self.url)
            .header("Authorization", format!("Bearer {}", self.auth_token))
            .json(&body)
            .send()
            .await?
            .error_for_status()?;
            
        Ok(())
    }
}