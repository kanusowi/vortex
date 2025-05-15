use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use thiserror::Error;
use vortex_core::{DistanceMetric, Embedding, HnswConfig, VectorId};

// Import our own VortexWal and VortexWalOptions
use crate::wal::{VortexWal, VortexWalOptions}; 

#[derive(Error, Debug)]
pub enum WalError {
    #[error("VortexWAL operation failed for path {path:?}: {source}")]
    VortexWLogError { path: PathBuf, source: std::io::Error }, // Assuming VortexWal errors are std::io::Error for now
    #[error("Serialization error: {0}")]
    Serialization(String),
    #[error("Deserialization error: {0}")]
    Deserialization(String),
    #[error("IO error for WAL at {path:?}: {source}")]
    IoError { // This can be for fs operations like create_dir_all
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("WAL Mutex was poisoned")]
    MutexPoisoned,
    #[error("WAL operation failed: {0}")]
    OperationFailed(String), // Generic WAL operational error
}

// We might not need from_wal_error if VortexWal returns std::io::Result or its own error type
// that can be mapped to WalError::VortexWLogError or WalError::OperationFailed.

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum WalRecord {
    CreateIndex {
        index_name: String,
        config: HnswConfig,
        metric: DistanceMetric,
        dimensions: u32,
        capacity: u64,
    },
    AddVector {
        // index_name: String, // Not strictly needed if WAL is per-index, but good for clarity
        vector_id: VectorId,
        vector: Embedding,
        // payload: Option<serde_json::Value>, // Defer payload
    },
    DeleteVector {
        // index_name: String,
        vector_id: VectorId,
    },
}

pub struct CollectionWalManager {
    // Now wraps our VortexWal, which internally handles segments and mmap.
    // VortexWal methods will likely take &mut self, so Mutex is appropriate here for AppState sharing.
    vortex_wal: Mutex<VortexWal>, 
    wal_dir_path: PathBuf, // Keep path for context
}

impl CollectionWalManager {
    pub fn new(index_wal_dir_path: PathBuf, options: VortexWalOptions) -> Result<Self, WalError> {
        // VortexWal::open will handle creating the directory if needed.
        let wal_instance = VortexWal::open(index_wal_dir_path.clone(), options)
            .map_err(|e| WalError::VortexWLogError { path: index_wal_dir_path.clone(), source: e })?;
        
        Ok(Self {
            vortex_wal: Mutex::new(wal_instance),
            wal_dir_path: index_wal_dir_path,
        })
    }

    pub fn log_operation(&self, record: &WalRecord) -> Result<u64, WalError> {
        let mut wal_guard = self.vortex_wal.lock().map_err(|_| WalError::MutexPoisoned)?;
        let serialized_record =
            serde_cbor::to_vec(record).map_err(|e| WalError::Serialization(e.to_string()))?;

        wal_guard.append_bytes(&serialized_record)
            .map_err(|e| WalError::VortexWLogError { path: self.wal_dir_path.clone(), source: e})
    }

    pub fn recover_from_wal(&self) -> Result<Vec<(u64, WalRecord)>, WalError> {
        let wal_guard = self.vortex_wal.lock().map_err(|_| WalError::MutexPoisoned)?;
        let mut recovered_records = Vec::new();
        
        // Assuming VortexWal provides an iterator or a way to get all entries.
        // This part needs to align with VortexWal's API.
        // For now, let's imagine a method like `read_all_entries_bytes() -> Result<Vec<(u64, Vec<u8>)>>`
        // or an iterator.
        // If VortexWal's `entry(lsn)` is the primary way, recovery needs to iterate LSNs.
        // Qdrant's `wal.reader()` is convenient. We'll need similar in VortexWal.
        // Let's assume VortexWal has a method `entries_iter_bytes()` for now.
        
        // Placeholder: Actual iteration will depend on VortexWal's API
        // This is a simplified conceptual loop. The actual loop will use VortexWal's iteration mechanism.
        let first_lsn = wal_guard.first_lsn().unwrap_or(0);
        let last_lsn_opt = wal_guard.last_lsn();

        if let Some(last_lsn) = last_lsn_opt {
            for lsn in first_lsn..=last_lsn {
                if let Some(entry_bytes_view) = wal_guard.read_bytes_by_lsn(lsn) {
                    let entry_bytes = entry_bytes_view.to_vec(); // Convert VortexEntry (Deref<[u8]>) to Vec<u8>
                    match serde_cbor::from_slice(&entry_bytes) {
                        Ok(record) => {
                            recovered_records.push((lsn, record));
                        }
                        Err(e) => {
                             tracing::error!(
                                "Corrupted WAL record at LSN {} in {:?}, skipping: {:?}",
                                lsn, self.wal_dir_path, e
                            );
                            // Potentially return error or have configurable behavior
                            return Err(WalError::Deserialization(format!(
                                "Failed to deserialize record at LSN {}: {}", lsn, e
                            )));
                        }
                    }
                } else {
                    // This might happen if LSNs are not perfectly contiguous due to some internal WAL detail,
                    // or if an entry was corrupted in a way that it's not even returned as bytes.
                    tracing::warn!("Could not read entry for LSN {} in WAL {:?}, it might be part of a corrupted/skipped section.", lsn, self.wal_dir_path);
                }
            }
        }
        Ok(recovered_records)
    }

    pub fn checkpoint(&self, lsn: u64) -> Result<(), WalError> {
        let mut wal_guard = self.vortex_wal.lock().map_err(|_| WalError::MutexPoisoned)?;
        // Use prefix_truncate to remove older segments.
        // The LSN here should be the LSN of the *last record successfully persisted*
        // to the main storage (mmap files). All records up to and including this LSN
        // can be removed from the WAL.
        wal_guard.prefix_truncate_log_until_lsn(lsn + 1) // +1 because prefix_truncate is exclusive for the 'until'
            .map_err(|e| WalError::VortexWLogError { path: self.wal_dir_path.clone(), source: e})
    }

    pub fn get_wal_path_for_index(base_data_path: &Path, index_name: &str) -> PathBuf {
        base_data_path.join(index_name).join("wal")
    }
}

// Basic tests for WAL manager - more comprehensive tests will be integration tests
#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    use vortex_core::HnswConfig; // For WalRecord::CreateIndex

    fn create_test_add_vector_record(id: &str) -> WalRecord {
        WalRecord::AddVector {
            vector_id: id.to_string(),
            vector: Embedding::from(vec![0.1, 0.2]),
        }
    }
    
    fn create_test_create_index_record(name: &str) -> WalRecord {
        WalRecord::CreateIndex {
            index_name: name.to_string(),
            config: HnswConfig::default(),
            metric: DistanceMetric::Cosine,
            dimensions: 2,
            capacity: 100,
        }
    }


    #[test]
    fn test_wal_new_log_recover() -> Result<(), Box<dyn std::error::Error>> {
        let dir = tempdir()?;
        let wal_path = dir.path().join("test_index_wal");
        let wal_manager = CollectionWalManager::new(wal_path)?;

        let record1 = create_test_create_index_record("test_idx");
        let record2 = create_test_add_vector_record("vec1");
        
        let lsn1 = wal_manager.log_operation(&record1)?;
        let lsn2 = wal_manager.log_operation(&record2)?;

        assert!(lsn2 > lsn1);

        let recovered_records = wal_manager.recover_from_wal()?;
        assert_eq!(recovered_records.len(), 2);
        
        // Note: WalRecord does not implement PartialEq, so we compare relevant fields or serialized forms.
        // For simplicity here, we'll just check counts and types, or serialize.
        // A more robust test would involve pattern matching on the recovered records.
        match &recovered_records[0].1 {
            WalRecord::CreateIndex { index_name, .. } => assert_eq!(index_name, "test_idx"),
            _ => panic!("Unexpected record type for record1"),
        }
        match &recovered_records[1].1 {
            WalRecord::AddVector { vector_id, .. } => assert_eq!(vector_id, "vec1"),
            _ => panic!("Unexpected record type for record2"),
        }
        
        Ok(())
    }

    #[test]
    fn test_wal_checkpoint() -> Result<(), Box<dyn std::error::Error>> {
        let dir = tempdir()?;
        let wal_path = dir.path().join("test_checkpoint_wal");
        let wal_manager = CollectionWalManager::new(wal_path)?;

        let record1 = create_test_add_vector_record("vec1");
        let record2 = create_test_add_vector_record("vec2");
        let record3 = create_test_add_vector_record("vec3");

        wal_manager.log_operation(&record1)?;
        let lsn2 = wal_manager.log_operation(&record2)?;
        wal_manager.log_operation(&record3)?;
        
        // Checkpoint up to (and including) lsn2
        wal_manager.checkpoint(lsn2)?;

        let recovered_records = wal_manager.recover_from_wal()?;
        // Only record3 should remain as checkpointing is prefix truncation
        assert_eq!(recovered_records.len(), 1); 
        match &recovered_records[0].1 {
            WalRecord::AddVector { vector_id, .. } => assert_eq!(vector_id, "vec3"),
            _ => panic!("Unexpected record type after checkpoint"),
        }
        
        Ok(())
    }
}
