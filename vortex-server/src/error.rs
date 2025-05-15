use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::json;
use thiserror::Error;
use tracing::error;
use vortex_core::VortexError; // Import core errors

/// Server-specific error types.
#[derive(Error, Debug)]
pub enum ServerError {
    #[error("Index not found: {0}")]
    IndexNotFound(String),

    #[error("Invalid request body: {0}")]
    BadRequest(String),

    #[error("Core index error: {0}")]
    CoreError(#[from] VortexError), // Automatically convert from VortexError

    #[error("Internal server error: {0}")]
    #[allow(dead_code)] // May be used in future
    Internal(String),

    #[error("Failed to acquire lock: {0}")]
    #[allow(dead_code)] // May be used in future
    LockError(String),

    #[error("WAL error: {0}")]
    WalError(String), // Added for WAL specific errors

    #[error("RocksDB error: {0}")]
    RocksDBError(String),
}

impl From<crate::wal::wal_manager::WalError> for ServerError {
    fn from(wal_error: crate::wal::wal_manager::WalError) -> Self {
        ServerError::WalError(wal_error.to_string())
    }
}

// Implement IntoResponse for ServerError to automatically convert errors into HTTP responses.
impl IntoResponse for ServerError {
    fn into_response(self) -> Response {
        let (status, error_message) = match &self { // Use reference match
            ServerError::IndexNotFound(name) => (StatusCode::NOT_FOUND, format!("Index '{}' not found", name)),
            ServerError::BadRequest(reason) => (StatusCode::BAD_REQUEST, format!("Bad request: {}", reason)),
            ServerError::CoreError(core_err) => match core_err {
                VortexError::NotFound(id) => (StatusCode::NOT_FOUND, format!("Vector ID '{}' not found", id)),
                VortexError::AlreadyExists(id) => (StatusCode::CONFLICT, format!("Vector ID '{}' already exists", id)),
                VortexError::DimensionMismatch { expected, actual } => (
                    StatusCode::BAD_REQUEST,
                    format!("Dimension mismatch: expected {}, got {}", expected, actual),
                ),
                VortexError::Configuration(msg) => (StatusCode::BAD_REQUEST, format!("Configuration error: {}", msg)),
                VortexError::EmptyIndex => (StatusCode::BAD_REQUEST, "Operation requires a non-empty index".to_string()),
                VortexError::UnsupportedOperation(msg) => (StatusCode::NOT_IMPLEMENTED, format!("Operation not supported: {}", msg)),
                VortexError::IoError { path, source } => {
                    error!(path=?path, error=%source, "Core I/O error");
                    (StatusCode::INTERNAL_SERVER_ERROR, "Internal server error (I/O)".to_string())
                },
                VortexError::Serialization(msg) | VortexError::Deserialization(msg) => {
                     error!(error=%msg, "Core serialization/deserialization error");
                    (StatusCode::INTERNAL_SERVER_ERROR, "Internal server error (Serialization)".to_string())
                },
                VortexError::Internal(msg) => {
                    error!(error=%msg, "Core internal error");
                    (StatusCode::INTERNAL_SERVER_ERROR, "Internal server error".to_string())
                },
                VortexError::InvalidDistanceMetric => (StatusCode::BAD_REQUEST, "Invalid distance metric specified".to_string()),
                VortexError::StorageError(msg) => {
                    error!(error=%msg, "Core storage error");
                    (StatusCode::INTERNAL_SERVER_ERROR, format!("Storage error: {}", msg))
                },
                VortexError::InvalidArgument(msg) => {
                    error!(error=%msg, "Core invalid argument error");
                    (StatusCode::BAD_REQUEST, format!("Invalid argument: {}", msg))
                },
                VortexError::StorageFull => {
                    error!("Core storage full error");
                    (StatusCode::INSUFFICIENT_STORAGE, "Storage is full".to_string())
                },
            },
            ServerError::Internal(msg) => {
                error!(error=%msg, "Internal server error");
                (StatusCode::INTERNAL_SERVER_ERROR, "Internal server error".to_string())
            },
             ServerError::LockError(msg) => {
                 error!(error=%msg, "Failed to acquire lock");
                 (StatusCode::INTERNAL_SERVER_ERROR, "Internal server error (locking)".to_string())
             },
            ServerError::WalError(msg) => {
                error!(error=%msg, "WAL operation error");
                (StatusCode::INTERNAL_SERVER_ERROR, format!("Internal server error (WAL): {}", msg))
            },
            ServerError::RocksDBError(msg) => {
                error!(error=%msg, "RocksDB operation error");
                (StatusCode::INTERNAL_SERVER_ERROR, format!("Internal server error (Payload DB): {}", msg))
            }
        };

        // Log the error before returning response
        error!("Responding with status {}: {}", status, error_message);

        let body = Json(json!({
            "error": error_message,
        }));

        (status, body).into_response()
    }
}

// Define a Result type alias for handler functions
pub type ServerResult<T> = Result<T, ServerError>;
