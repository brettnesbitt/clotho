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
    let spin_env_name = format!("SPIN_VARIABLE_{}", name.to_uppercase());
    match spin_sdk::variables::get(&spin_name) {
        Ok(value) => Ok(value),
        Err(spin_err) => std::env::var(&spin_env_name)
            .or_else(|_| std::env::var(name))
            .map_err(|env_err| {
                format!(
                    "Spin variable '{}' not found ({}), env var '{}' not found, and env var '{}' not found ({})",
                    spin_name, spin_err, spin_env_name, name, env_err
                )
            }),
    }
}

#[cfg(not(target_family = "wasm"))]
fn var_inner(name: &str) -> Result<String, String> {
    std::env::var(name)
        .map_err(|e| format!("Env var '{}' not found: {}", name, e))
}
