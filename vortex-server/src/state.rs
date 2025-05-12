use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
// Using concrete HnswIndex type wrapped in RwLock for mutable access
use vortex_core::HnswIndex;

/// Holds the shared state accessible by all request handlers.
///
/// Contains a map of index names to their corresponding `HnswIndex` instances,
/// each wrapped in an `Arc<RwLock<>>` to allow safe concurrent mutable access.
#[derive(Clone, Debug)] // Add Debug
pub struct AppState {
    // Arc allows multiple threads to own the RwLock safely.
    // RwLock allows multiple readers or one writer for the *HashMap*.
    // The value is Arc<RwLock<HnswIndex>> allowing shared ownership of the lock+index,
    // and the inner RwLock controls mutable access to the HnswIndex itself.
    pub indices: Arc<RwLock<HashMap<String, Arc<RwLock<HnswIndex>>>>>,
}

impl AppState {
    /// Creates a new instance of the application state.
    pub fn new() -> Self {
        AppState {
            indices: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

// Type alias for the shared state extractor in Axum handlers
pub type SharedState = Arc<RwLock<HashMap<String, Arc<RwLock<HnswIndex>>>>>;