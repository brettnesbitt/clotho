use mongodb::{Client, bson::{doc, Document}};
use crate::traits::LookupTarget;

pub struct MongoLookup {
    collection: mongodb::Collection<Document>,
    // The "Escape Hatch": A user-defined closure that builds the query
    query_builder: Box<dyn Fn(Vec<&str>) -> Document + Send + Sync>,
}

impl MongoLookup {
    /// The Simple Path (What we had before)
    pub async fn new(uri: &str, db: &str, coll: &str, field: &str) -> anyhow::Result<Self> {
        let field_name = field.to_string();
        Self::with_query(uri, db, coll, move |keys| {
            doc! { field_name.clone(): { "$in": keys } }
        }).await
    }

    /// The Advanced Path (The Escape Hatch)
    pub async fn with_query<F>(uri: &str, db: &str, coll: &str, builder: F) -> anyhow::Result<Self> 
    where 
        F: Fn(Vec<&str>) -> Document + Send + Sync + 'static 
    {
        let client = Client::with_uri_str(uri).await?;
        let collection = client.database(db).collection(coll);
        
        Ok(Self {
            collection,
            query_builder: Box::new(builder),
        })
    }
}

#[async_trait::async_trait]
impl LookupTarget for MongoLookup {
    async fn lookup_batch(&self, keys: Vec<&str>) -> anyhow::Result<DataFrame> {
        // 1. Invoke the user's custom query builder
        let custom_query = (self.query_builder)(keys);
        
        // 2. Execute the query
        let mut cursor = self.collection.find(custom_query, None).await?;
        
        // 3. Convert to Polars DataFrame (Hidden from user)
        let mut results = Vec::new();
        while let Some(doc) = cursor.next().await {
            if let Ok(d) = doc {
                // In a real implementation, we'd dynamically parse the BSON types to Arrow arrays
                results.push(d); 
            }
        }
        
        Ok(bson_docs_to_dataframe(results)?)
    }
}