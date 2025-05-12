use thiserror::Error;
use crate::vector::VectorId;
use std::path::PathBuf;

/// The main result type for vortex-core operations.
pub type VortexResult<T> = Result<T, VortexError>;

/// Enum representing possible errors within the vortex-core library.
#[derive(Error, Debug)]
pub enum VortexError {
    #[error("Configuration error: {0}")]
    Configuration(String),

    #[error("Vector dimension mismatch: expected {expected}, got {actual}")]
    DimensionMismatch { expected: usize, actual: usize },

    #[error("Vector ID not found: {0}")]
    NotFound(VectorId),

    #[error("Vector ID already exists: {0}")]
    AlreadyExists(VectorId),

    #[error("Index is empty, cannot perform search")]
    EmptyIndex,

    #[error("I/O error accessing path {path:?}: {source}")]
    IoError {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("Deserialization error: {0}")]
    Deserialization(String),

    #[error("Operation is not supported: {0}")]
    UnsupportedOperation(String),

    #[error("Internal error: {0}")]
    Internal(String),

    #[error("Invalid distance metric specified")]
    InvalidDistanceMetric,
}

// Helper for converting bincode errors
impl From<bincode::Error> for VortexError {
    fn from(err: bincode::Error) -> Self {
        // Box<bincode::ErrorKind> doesn't provide much detail in its Display impl
        // Convert to string for a bit more context if possible.
        VortexError::Serialization(format!("Bincode error: {}", err))
    }
}