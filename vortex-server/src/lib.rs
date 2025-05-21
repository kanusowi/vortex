// Declare modules to be part of the library crate

pub mod config;
pub mod error;
pub mod handlers;
pub mod models;
pub mod persistence;
pub mod snapshot_manager; // Added snapshot_manager module
pub mod state;
pub mod wal;
pub mod payload_index;
pub mod grpc_api; // For generated gRPC code
pub mod grpc_services; // For gRPC service implementations
// wal_manager is part of the wal module, so it's accessible via pub mod wal;
// wal_integration_tests.rs is a test file, not typically part of lib.rs

// Optionally, re-export key items for easier access if desired, e.g.:
// pub use state::AppState;
// pub use config::ServerConfig;
// etc.
