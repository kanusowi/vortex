use crate::error::ServerError;
use vortex_core::VectorId; // Changed import
use rocksdb::{DB, Options};
use serde_json::Value;
use std::path::Path;
use std::sync::Arc; // For Arc<DB>

#[derive(Debug)]
pub struct PayloadIndexRocksDB {
    db: Arc<DB>, // Arc to allow sharing if needed, though maybe not for per-collection instance
}

impl PayloadIndexRocksDB {
    pub fn new(db_path: &Path) -> Result<Self, ServerError> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        // Add other options as needed, e.g., compression, parallelism
        // opts.increase_parallelism(num_cpus::get() as i32);
        // opts.set_compression_type(rocksdb::DBCompressionType::Snappy);

        let db = DB::open(&opts, db_path).map_err(|e| {
            ServerError::RocksDBError(format!( // Changed to RocksDBError
                "Failed to open RocksDB at {:?}: {}",
                db_path, e
            ))
        })?;
        Ok(Self { db: Arc::new(db) })
    }

    pub fn set_payload(
        &self,
        point_id: &VectorId,
        payload: &Value,
    ) -> Result<(), ServerError> {
        let payload_bytes = serde_json::to_vec(payload).map_err(|e| {
            ServerError::RocksDBError(format!( // Changed to RocksDBError (could also be Internal or BadRequest depending on context)
                "Failed to serialize payload for ID {}: {}",
                point_id, e
            ))
        })?;
        self.db.put(point_id.as_bytes(), payload_bytes).map_err(|e| {
            ServerError::RocksDBError(format!( // Changed to RocksDBError
                "Failed to set payload for ID {} in RocksDB: {}",
                point_id, e
            ))
        })
    }

    pub fn get_payload(
        &self,
        point_id: &VectorId,
    ) -> Result<Option<Value>, ServerError> {
        match self.db.get(point_id.as_bytes()) {
            Ok(Some(payload_bytes)) => {
                let payload: Value = serde_json::from_slice(&payload_bytes).map_err(|e| {
                    ServerError::RocksDBError(format!( // Changed to RocksDBError
                        "Failed to deserialize payload for ID {}: {}",
                        point_id, e
                    ))
                })?;
                Ok(Some(payload))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(ServerError::RocksDBError(format!( // Changed to RocksDBError
                "Failed to get payload for ID {} from RocksDB: {}",
                point_id, e
            ))),
        }
    }

    pub fn delete_payload(&self, point_id: &VectorId) -> Result<(), ServerError> {
        self.db.delete(point_id.as_bytes()).map_err(|e| {
            ServerError::RocksDBError(format!( // Changed to RocksDBError
                "Failed to delete payload for ID {} from RocksDB: {}",
                point_id, e
            ))
        })
    }

    // TODO: Add methods for filtering based on payload content.
    // Example:
    // pub fn filter_by_key_value(&self, key: &str, value: &Value) -> Result<Vec<VectorId>, ServerError> {
    //     // This would require iterating over DB or using secondary indexes.
    //     // For basic exact match, iteration might be feasible for small datasets, but inefficient.
    //     // True filtering will need secondary indexes or more advanced RocksDB usage.
    //     unimplemented!("Filtering not yet implemented");
    // }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    use serde_json::json;

    #[test]
    fn test_payload_index_new_set_get_delete() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("payload_db_test");
        let payload_index = PayloadIndexRocksDB::new(&db_path).unwrap();

        let point_id1 = "test_point_1".to_string();
        let payload1 = json!({ "color": "blue", "count": 10 });

        // Set payload
        payload_index.set_payload(&point_id1, &payload1).unwrap();

        // Get payload
        let retrieved_payload1 = payload_index.get_payload(&point_id1).unwrap().unwrap();
        assert_eq!(retrieved_payload1, payload1);

        // Get non-existent payload
        let point_id_non_existent = "does_not_exist".to_string();
        let retrieved_non_existent = payload_index.get_payload(&point_id_non_existent).unwrap();
        assert!(retrieved_non_existent.is_none());

        // Delete payload
        payload_index.delete_payload(&point_id1).unwrap();
        let retrieved_after_delete = payload_index.get_payload(&point_id1).unwrap();
        assert!(retrieved_after_delete.is_none());

        // Test setting multiple payloads
        let point_id2 = "test_point_2".to_string();
        let payload2 = json!({ "city": "New York", "active": true });
        payload_index.set_payload(&point_id2, &payload2).unwrap();

        let point_id3 = "test_point_3".to_string();
        let payload3 = json!({ "value": 12.34, "tags": ["a", "b"] });
        payload_index.set_payload(&point_id3, &payload3).unwrap();
        
        assert_eq!(payload_index.get_payload(&point_id2).unwrap().unwrap(), payload2);
        assert_eq!(payload_index.get_payload(&point_id3).unwrap().unwrap(), payload3);
    }
}
