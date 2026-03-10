use serde::Deserialize;
use anyhow::Result;
use std::collections::HashMap;

#[derive(Deserialize, Debug)]
pub struct Summary {
    pub pods: Vec<PodStats>,
}

#[derive(Deserialize, Debug)]
pub struct PodStats {
    pub podRef: PodRef,
    pub cpu: Option<CpuStats>,
    pub memory: Option<MemoryStats>,
}

#[derive(Deserialize, Debug)]
pub struct PodRef {
    pub name: String,
    pub namespace: String,
    pub uid: String,
}

#[derive(Deserialize, Debug)]
pub struct CpuStats {
    pub usageNanoCores: u64, // Instantaneous usage
}

#[derive(Deserialize, Debug)]
pub struct MemoryStats {
    pub workingSetBytes: u64, // The "Real" memory usage (OOM trigger)
}

pub struct KubeletClient {
    client: reqwest::Client,
    token: String,
    node_ip: String,
}

impl KubeletClient {
    pub fn new() -> Result<Self> {
        // In-cluster config to get the token
        let token = std::fs::read_to_string("/var/run/secrets/kubernetes.io/serviceaccount/token")?;
        
        // Use the Node IP from env (Downard API) or localhost if HostNetwork=true
        let node_ip = std::env::var("HOST_IP").unwrap_or("127.0.0.1".into());

        // Create client that ignores self-signed certs (Kubelet usually has one)
        let client = reqwest::Client::builder()
            .danger_accept_invalid_certs(true)
            .build()?;

        Ok(Self { client, token, node_ip })
    }

    pub async fn get_stats(&self) -> Result<Summary> {
        let url = format!("https://{}:10250/stats/summary", self.node_ip);
        
        let resp = self.client.get(&url)
            .header("Authorization", format!("Bearer {}", self.token))
            .send()
            .await?
            .json::<Summary>()
            .await?;
            
        Ok(resp)
    }
}