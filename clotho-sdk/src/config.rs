/// Cross-platform configuration variable access.
///
/// On WASM (Spin), reads from Spin application variables via `spin_sdk::variables::get()`.
/// On native, reads from environment variables via `std::env::var()`.
///
/// Pipeline authors should use `clotho::config::var("name")` instead of `std::env::var("name")`
/// to ensure portability across runtimes.

/// Get a configuration variable by name.
///
/// - **WASM**: Reads Spin application variable (lowercase, e.g. `crawler_source`)
/// - **Native**: Reads environment variable (uppercase, e.g. `CRAWLER_SOURCE`)
///
/// Returns `Err` if the variable is not set.
pub fn var(name: &str) -> Result<String, String> {
    var_inner(name)
}

/// Get a configuration variable, returning a default if not set.
pub fn var_or(name: &str, default: &str) -> String {
    var_inner(name).unwrap_or_else(|_| default.to_string())
}

#[cfg(target_family = "wasm")]
fn var_inner(name: &str) -> Result<String, String> {
    // Spin variables are lowercase with underscores
    let spin_name = name.to_lowercase();
    spin_sdk::variables::get(&spin_name)
        .map_err(|e| format!("Spin variable '{}' not found: {}", spin_name, e))
}

#[cfg(not(target_family = "wasm"))]
fn var_inner(name: &str) -> Result<String, String> {
    std::env::var(name)
        .map_err(|e| format!("Env var '{}' not found: {}", name, e))
}
