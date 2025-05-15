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

    #[error("Storage error: {0}")]
    StorageError(String),

    #[error("Invalid argument: {0}")]
    InvalidArgument(String),

    #[error("Storage is full and cannot accept new data")]
    StorageFull,
}

impl From<std::io::Error> for VortexError {
    fn from(err: std::io::Error) -> Self {
        // Create a generic path for cases where it's not readily available
        // or to avoid making IoError variant too complex for simple conversions.
        // Alternatively, one could create a new variant like `GenericIoError(String)`.
        // For now, let's use a placeholder path.
        VortexError::IoError {
            path: PathBuf::from("<unknown_io_source>"),
            source: err,
        }
    }
}

// Helper for converting bincode errors
// Commented out as bincode is not a direct dependency for core persistence anymore.
// If bincode is used for other serialization tasks, this can be reinstated.
// impl From<bincode::Error> for VortexError {
//     fn from(err: bincode::Error) -> Self {
//         // Box<bincode::ErrorKind> doesn't provide much detail in its Display impl
//         // Convert to string for a bit more context if possible.
//         VortexError::Serialization(format!("Bincode error: {}", err))
//     }
// }

#[cfg(test)]
mod tests {
    use super::*;
    use std::io;

    #[test]
    fn test_error_display_configuration() {
        let err = VortexError::Configuration("Test config error".to_string());
        assert_eq!(format!("{}", err), "Configuration error: Test config error");
    }

    #[test]
    fn test_error_display_dimension_mismatch() {
        let err = VortexError::DimensionMismatch { expected: 10, actual: 5 };
        assert_eq!(format!("{}", err), "Vector dimension mismatch: expected 10, got 5");
    }

    #[test]
    fn test_error_display_not_found() {
        let err = VortexError::NotFound("vec123".to_string());
        assert_eq!(format!("{}", err), "Vector ID not found: vec123");
    }

    #[test]
    fn test_error_display_already_exists() {
        let err = VortexError::AlreadyExists("vec456".to_string());
        assert_eq!(format!("{}", err), "Vector ID already exists: vec456");
    }

    #[test]
    fn test_error_display_empty_index() {
        let err = VortexError::EmptyIndex;
        assert_eq!(format!("{}", err), "Index is empty, cannot perform search");
    }

    #[test]
    fn test_error_display_io_error() {
        let path = PathBuf::from("/tmp/testfile");
        let io_err = io::Error::new(io::ErrorKind::NotFound, "file not found");
        let err = VortexError::IoError { path: path.clone(), source: io_err };
        // The exact format of the source io::Error might vary slightly by OS/platform,
        // so we check for the main parts.
        assert!(format!("{}", err).contains("I/O error accessing path \"/tmp/testfile\""));
        assert!(format!("{}", err).contains("file not found"));
    }

    #[test]
    fn test_error_display_serialization() {
        let err = VortexError::Serialization("Test serialization error".to_string());
        assert_eq!(format!("{}", err), "Serialization error: Test serialization error");
    }

    #[test]
    fn test_error_display_deserialization() {
        let err = VortexError::Deserialization("Test deserialization error".to_string());
        assert_eq!(format!("{}", err), "Deserialization error: Test deserialization error");
    }

    #[test]
    fn test_error_display_unsupported_operation() {
        let err = VortexError::UnsupportedOperation("Test unsupported op".to_string());
        assert_eq!(format!("{}", err), "Operation is not supported: Test unsupported op");
    }

    #[test]
    fn test_error_display_internal() {
        let err = VortexError::Internal("Test internal error".to_string());
        assert_eq!(format!("{}", err), "Internal error: Test internal error");
    }

    #[test]
    fn test_error_display_invalid_distance_metric() {
        let err = VortexError::InvalidDistanceMetric;
        assert_eq!(format!("{}", err), "Invalid distance metric specified");
    }

    #[test]
    fn test_error_display_invalid_argument() {
        let err = VortexError::InvalidArgument("Test invalid argument".to_string());
        assert_eq!(format!("{}", err), "Invalid argument: Test invalid argument");
    }

    #[test]
    fn test_from_bincode_error() {
        // Create a dummy bincode::Error (this is a bit tricky as ErrorKind is non-exhaustive)
        // We'll simulate one by using a Deserialization error which bincode can produce.
        // let bincode_err_kind = bincode::ErrorKind::DeserializeAnyNotSupported;
        // let bincode_err = Box::new(bincode_err_kind);
        
        // let vortex_err: VortexError = bincode_err.into();
        // match vortex_err {
        //     VortexError::Serialization(msg) => {
        //         assert!(msg.contains("Bincode error"));
        //         // Making the check more general as the exact message for DeserializeAnyNotSupported might vary.
        //         // We've already confirmed it's a Bincode error.
        //     }
        //     _ => panic!("Expected VortexError::Serialization variant"),
        // }
        // Test commented out as From<bincode::Error> is commented out.
    }
}
