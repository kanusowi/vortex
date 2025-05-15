use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use tokio::sync::Mutex; // Changed from std::sync::Mutex
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
        metadata: Option<serde_json::Value>, // Added metadata field
    },
    DeleteVector {
        // index_name: String,
        vector_id: VectorId,
    },
}

#[derive(Debug)] // Added Debug derive
pub struct CollectionWalManager {
    // Now wraps our VortexWal, which internally handles segments and mmap.
    // VortexWal methods will likely take &mut self, so Mutex is appropriate here for AppState sharing.
    pub vortex_wal: Mutex<VortexWal>, // Made pub for persistence.rs checkpoint access
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

    pub async fn log_operation(&self, record: &WalRecord) -> Result<u64, WalError> {
        let mut wal_guard = self.vortex_wal.lock().await; // Changed to .await
        let serialized_record =
            serde_cbor::to_vec(record).map_err(|e| WalError::Serialization(e.to_string()))?;

        wal_guard.append_bytes(&serialized_record).await
            .map_err(|e| WalError::VortexWLogError { path: self.wal_dir_path.clone(), source: e})
    }

    pub async fn recover_from_wal(&self, start_recovery_after_lsn: Option<u64>) -> Result<Vec<(u64, WalRecord)>, WalError> { // Made async
        let wal_guard = self.vortex_wal.lock().await; // Changed to .await
        let mut recovered_records = Vec::new();
        
        // Determine the actual starting LSN for recovery.
        // If start_recovery_after_lsn is Some, we want to start from the LSN *after* it.
        // Otherwise, we start from the very first LSN available in the WAL.
        let mut effective_start_lsn = wal_guard.first_lsn().unwrap_or(0);
        if let Some(checkpointed_lsn) = start_recovery_after_lsn {
            effective_start_lsn = checkpointed_lsn + 1;
        }
        
        // Ensure effective_start_lsn is not before the WAL's actual first LSN,
        // especially if checkpointed_lsn + 1 points to a truncated segment.
        if let Some(wal_first_lsn) = wal_guard.first_lsn() {
            if effective_start_lsn < wal_first_lsn {
                effective_start_lsn = wal_first_lsn;
            }
        }


        let last_lsn_opt = wal_guard.last_lsn();

        if let Some(last_lsn) = last_lsn_opt {
            if effective_start_lsn > last_lsn {
                // Nothing to recover if the effective start is past the last known LSN.
                return Ok(recovered_records);
            }
            for lsn in effective_start_lsn..=last_lsn {
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

    pub async fn checkpoint(&self, lsn: u64) -> Result<(), WalError> { // Made async
        let mut wal_guard = self.vortex_wal.lock().await; // Changed to .await
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
            metadata: Some(serde_json::json!({"test_meta": "data"})), // Added test metadata
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


    #[tokio::test] // Added tokio::test
    async fn test_wal_new_log_recover() -> std::result::Result<(), Box<dyn std::error::Error>> { // Made async
        let dir = tempdir()?;
        let wal_path = dir.path().join("test_index_wal");
        let wal_manager = CollectionWalManager::new(wal_path, VortexWalOptions::default())?;

        let record1 = create_test_create_index_record("test_idx");
        let record2 = create_test_add_vector_record("vec1");
        
        let lsn1 = wal_manager.log_operation(&record1).await?; // Added .await
        let lsn2 = wal_manager.log_operation(&record2).await?; // Added .await

        assert!(lsn2 > lsn1);

        // Recover all records initially
        let recovered_records_all = wal_manager.recover_from_wal(None).await?; // Added .await
        assert_eq!(recovered_records_all.len(), 2);
        
        // Note: WalRecord does not implement PartialEq, so we compare relevant fields or serialized forms.
        // For simplicity here, we'll just check counts and types, or serialize.
        // A more robust test would involve pattern matching on the recovered records.
        match &recovered_records_all[0].1 {
            WalRecord::CreateIndex { index_name, .. } => assert_eq!(index_name, "test_idx"),
            _ => panic!("Unexpected record type for record1"),
        }
        match &recovered_records_all[1].1 {
            WalRecord::AddVector { vector_id, .. } => assert_eq!(vector_id, "vec1"),
            _ => panic!("Unexpected record type for record2"),
        }
        
        Ok(())
    }

    #[tokio::test] // Added tokio::test
    async fn test_wal_checkpoint() -> std::result::Result<(), Box<dyn std::error::Error>> { // Made async
        let dir = tempdir()?;
        let wal_path = dir.path().join("test_checkpoint_wal");
        // Use a small segment capacity to force segment rotation
        let options = VortexWalOptions {
            segment_capacity: 128, // Small capacity, e.g., enough for ~2-3 of our test records
            segment_queue_len: 0, // No need for pre-creation queue in this test
        };
        let wal_manager = CollectionWalManager::new(wal_path, options)?;

        let record1 = create_test_add_vector_record("vec1");
        let record2 = create_test_add_vector_record("vec2");
        let record3 = create_test_add_vector_record("vec3");

        wal_manager.log_operation(&record1).await?; // Added .await
        let lsn2 = wal_manager.log_operation(&record2).await?; // Added .await
        wal_manager.log_operation(&record3).await?; // Added .await
        
        // Checkpoint up to (and including) lsn2
        wal_manager.checkpoint(lsn2).await?; // Added .await

        // Recover records after lsn2
        let recovered_records_after_checkpoint = wal_manager.recover_from_wal(Some(lsn2)).await?; // Added .await
        // Only record3 (lsn3) should remain as checkpointing is prefix truncation and we start after lsn2
        assert_eq!(recovered_records_after_checkpoint.len(), 1); 
        match &recovered_records_after_checkpoint[0].1 {
            WalRecord::AddVector { vector_id, .. } => assert_eq!(vector_id, "vec3"),
            _ => panic!("Unexpected record type after checkpoint"),
        }

        // Test recovery from a non-existent checkpoint LSN (e.g., very old)
        let recovered_from_old_checkpoint = wal_manager.recover_from_wal(Some(0)).await?; // Added .await
         assert_eq!(recovered_from_old_checkpoint.len(), 1); // Still only vec3 as WAL was truncated

        // Test recovery when checkpoint LSN is the last LSN
        let lsn3_val = recovered_records_after_checkpoint[0].0; // get actual lsn of record3
        wal_manager.checkpoint(lsn3_val).await?; // Added .await
        let recovered_after_full_checkpoint = wal_manager.recover_from_wal(Some(lsn3_val)).await?; // Added .await
        assert_eq!(recovered_after_full_checkpoint.len(), 0);


        Ok(())
    }
}
