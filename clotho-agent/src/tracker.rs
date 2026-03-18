use std::collections::HashMap;
use std::time::Instant;

pub struct ResourceTracker {
    // Key: Pod UID
    last_seen: HashMap<String, Instant>,
    // Accumulators (billing)
    cpu_nanocore_seconds: HashMap<String, u128>, 
    mem_byte_seconds: HashMap<String, u128>,
    // Instantaneous (dashboard)
    last_cpu_nanocores: HashMap<String, u64>,
    last_mem_bytes: HashMap<String, u64>,
}

impl ResourceTracker {
    pub fn new() -> Self {
        Self {
            last_seen: HashMap::new(),
            cpu_nanocore_seconds: HashMap::new(),
            mem_byte_seconds: HashMap::new(),
            last_cpu_nanocores: HashMap::new(),
            last_mem_bytes: HashMap::new(),
        }
    }

    pub fn update(&mut self, pod_uid: &str, cpu_nanocores: u64, mem_bytes: u64) {
        let now = Instant::now();
        
        if let Some(last_time) = self.last_seen.get(pod_uid) {
            let delta_sec = now.duration_since(*last_time).as_secs_f64();
            
            // INTEGRATION: usage * time
            // Example: 500m CPU * 5 seconds = 2.5 CPU-Seconds
            let cpu_added = (cpu_nanocores as f64 * delta_sec) as u128;
            let mem_added = (mem_bytes as f64 * delta_sec) as u128;

            *self.cpu_nanocore_seconds.entry(pod_uid.to_string()).or_default() += cpu_added;
            *self.mem_byte_seconds.entry(pod_uid.to_string()).or_default() += mem_added;
        }

        self.last_cpu_nanocores.insert(pod_uid.to_string(), cpu_nanocores);
        self.last_mem_bytes.insert(pod_uid.to_string(), mem_bytes);
        self.last_seen.insert(pod_uid.to_string(), now);
    }

    /// Flush aggregated stats to send to Control Plane
    pub fn flush(&mut self) -> Vec<ResourceUsageEvent> {
        let mut events = Vec::new();
        
        // We clone keys to iterate safely
        let keys: Vec<String> = self.cpu_nanocore_seconds.keys().cloned().collect();

        for uid in keys {
            if let Some(cpu) = self.cpu_nanocore_seconds.remove(&uid) {
                let mem = self.mem_byte_seconds.remove(&uid).unwrap_or(0);
                
                let instant_cpu = self.last_cpu_nanocores.get(&uid).copied().unwrap_or(0);
                let instant_mem = self.last_mem_bytes.get(&uid).copied().unwrap_or(0);
                events.push(ResourceUsageEvent {
                    pod_uid: uid,
                    cpu_core_seconds: cpu as f64 / 1_000_000_000.0, // Convert Nano -> Core
                    mem_gb_seconds: mem as f64 / 1_073_741_824.0,   // Convert Byte -> GB
                    instant_cpu_nanocores: instant_cpu,
                    instant_mem_bytes: instant_mem,
                });
            }
        }
        
        // Don't clear last_seen, we need it for the next delta!
        events
    }
}

#[derive(serde::Serialize, Debug)]
pub struct ResourceUsageEvent {
    pub pod_uid: String,
    pub cpu_core_seconds: f64,
    pub mem_gb_seconds: f64,
    pub instant_cpu_nanocores: u64,
    pub instant_mem_bytes: u64,
}