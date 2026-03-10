use crate::traits::{Sink, Context};
use anyhow::Result;
use tokio_postgres::{NoTls, Client};

pub struct PostgresSink {
    connection_string: String,
    query: String,
    // We keep the client optional so we can lazy-connect
    client: Option<Client>,
}

impl PostgresSink {
    pub fn new(connection_env: &str, query: &str) -> Self {
        let conn_str = std::env::var(connection_env)
            .expect("Missing DB Connection Env Var");
        
        Self {
            connection_string: conn_str,
            query: query.to_string(),
            client: None,
        }
    }

    async fn connect(&mut self) -> Result<()> {
        if self.client.is_none() {
            // WASI 0.2 allows standard TcpStream!
            let (client, connection) = tokio_postgres::connect(&self.connection_string, NoTls).await?;
            
            // Spawn the connection handler
            tokio::spawn(async move {
                if let Err(e) = connection.await {
                    eprintln!("Postgres connection error: {}", e);
                }
            });
            
            self.client = Some(client);
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl<T> Sink<T> for PostgresSink 
where T: serde::Serialize + Send + Sync 
{
    async fn write(&mut self, ctx: Context<T>) -> Result<()> {
        self.connect().await?;
        
        let client = self.client.as_ref().unwrap();
        
        // This assumes the query uses JSONB or simple parameterized mapping.
        // For a generic sink, we often dump the whole struct as JSONB.
        let json_value = serde_json::to_value(&ctx.data)?;
        
        // Example: "INSERT INTO audits (payload) VALUES ($1)"
        client.execute(&self.query, &[&json_value]).await?;
        
        Ok(())
    }
}