use std::collections::HashMap;
use std::path::PathBuf; // Added PathBuf
use std::sync::Arc;
use tokio::sync::RwLock;
use vortex_core::{HnswIndex, VectorId};
use serde_json::Value;
use crate::wal::wal_manager::CollectionWalManager; // Corrected path

/// Holds the shared state accessible by all request handlers.
///
/// Contains a map of index names to their corresponding `HnswIndex` instances,
/// each wrapped in an `Arc<RwLock<>>` to allow safe concurrent mutable access.
/// Also contains a separate store for vector metadata.
#[derive(Clone, Debug)] // Add Debug
pub struct AppState {
    pub data_path: PathBuf, // Added data_path field
    // Arc allows multiple threads to own the RwLock safely.
    // RwLock allows multiple readers or one writer for the *HashMap*.
    // The value is Arc<RwLock<HnswIndex>> allowing shared ownership of the lock+index,
    // and the inner RwLock controls mutable access to the HnswIndex itself.
    pub indices: Arc<RwLock<HashMap<String, Arc<RwLock<HnswIndex>>>>>,
    // Stores metadata associated with vectors.
    // Keyed by index name, then by VectorId.
    pub metadata_store: Arc<RwLock<HashMap<String, HashMap<VectorId, Value>>>>,
    // Stores WAL managers for each index
    pub wal_managers: Arc<RwLock<HashMap<String, Arc<CollectionWalManager>>>>,
}

impl AppState {
    /// Creates a new instance of the application state.
    pub fn new(data_path: PathBuf) -> Self {
        AppState {
            data_path,
            indices: Arc::new(RwLock::new(HashMap::new())),
            metadata_store: Arc::new(RwLock::new(HashMap::new())),
            wal_managers: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

// Type alias for the shared state extractor in Axum handlers
// This alias might need to be reconsidered or AppState itself passed around
// if handlers need access to both indices and metadata_store directly.
// For now, keeping it as is, handlers will destructure AppState.
// pub type SharedState = Arc<RwLock<HashMap<String, Arc<RwLock<HnswIndex>>>>>;
// It's better to pass the whole AppState using State(state): State<AppState> in handlers.
// The SharedState alias is removed to avoid confusion.
