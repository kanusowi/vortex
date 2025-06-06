//! Server configuration (basic placeholder).

// TODO: Implement proper configuration loading (e.g., using the `config` crate)
#[derive(Debug, Clone)]
#[allow(dead_code)] // Fields may be used by consumers or in future
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
}

impl Default for ServerConfig {
    fn default() -> Self {
        ServerConfig {
            host: "127.0.0.1".to_string(),
            port: 3000,
        }
    }
}
