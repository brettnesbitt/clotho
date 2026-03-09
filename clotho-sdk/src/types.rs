use serde::{Deserialize, Serialize};
use uuid::Uuid;
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Provenance {
    pub trace_id: String,
    pub span_id: String,
    pub source: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Context<T> {
    pub data: T,
    pub span_id: String,
    pub parents: Vec<Provenance>,
    pub meta: HashMap<String, String>,
}

impl<T> Context<T> {
    /// Start a new trace (Root)
    pub fn root(data: T, source_name: &str) -> Self {
        Self {
            data,
            span_id: Uuid::new_v4().to_string(),
            parents: vec![Provenance {
                trace_id: Uuid::new_v4().to_string(), // New Trace
                span_id: "root".to_string(),
                source: source_name.to_string(),
            }],
            meta: HashMap::new(),
        }
    }

    /// Linear evolution (A -> B)
    pub fn child_of<U>(parent: &Context<U>, new_data: T) -> Self {
        Self {
            data: new_data,
            span_id: Uuid::new_v4().to_string(),
            // Inherit the primary trace_id from the first parent
            parents: parent.parents.clone(), 
            meta: parent.meta.clone(),
        }
    }

    /// Merge two contexts (A + B -> C)
    pub fn merge<U, V>(primary: &Context<U>, secondary: &Context<V>, new_data: T) -> Self {
        let mut parents = primary.parents.clone();
        // Add secondary parent's history to the genealogy
        parents.extend(secondary.parents.clone());
        
        Self {
            data: new_data,
            span_id: Uuid::new_v4().to_string(),
            parents,
            meta: primary.meta.clone(),
        }
    }
    
    /// Create a tombstone context (for DLQ signal)
    pub fn tombstone() -> Option<anyhow::Result<Self>> {
        None
    }
}