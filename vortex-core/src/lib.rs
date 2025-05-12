pub mod config;
pub mod distance;
pub mod error;
pub mod hnsw;
pub mod index;
pub mod vector;
mod utils; // Keep utils internal for now

// Re-export key types/traits for easier use
pub use config::HnswConfig;
pub use distance::DistanceMetric;
pub use error::{VortexError, VortexResult};
pub use index::{Index, HnswIndex}; // HnswIndex needs to be public for server creation/loading
pub use vector::{VectorId, Embedding};