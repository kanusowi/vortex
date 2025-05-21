use vortex_server::state::AppState;
use std::sync::Arc;
use vortex_server::models::{CreateIndexRequest, AddVectorRequest, SearchRequest}; // Removed VectorResponse and SearchResponse
use serde_json::Value as JsonValue; // Using Value directly for metadata
use vortex_core::index::Index;
// use vortex_core::vector::VectorId; // Unused import
use tokio::sync::RwLock;
// use std::collections::HashMap; // Unused import
use tempfile::tempdir;
use axum::Json; // Required for AddVectorRequest payload
use std::path::PathBuf; // Added for PathBuf
use crate::persistence; // Added for persistence functions

// Helper function to create a new AppState for testing
// Renamed to reflect it creates an empty state, loading is now explicit.
fn create_empty_test_app_state_arc(data_path_str: &str) -> Arc<RwLock<AppState>> {
    let persistence_path = PathBuf::from(data_path_str);
    Arc::new(RwLock::new(AppState::new(persistence_path)))
}

#[tokio::test]
async fn test_wal_recovery_simple_operations() {
    let temp_dir = tempdir().unwrap();
    let data_path_str = temp_dir.path().to_str().unwrap();

    let index_name = "test-index-wal".to_string();
    let vector_id1 = "vec1".to_string();
    let vector_data1 = vec![1.0, 2.0, 3.0];
    let metadata1: Option<JsonValue> = Some(serde_json::json!({"feature": "test1"}));


    // --- First run: Perform operations ---
    {
        let app_state = create_empty_test_app_state_arc(data_path_str); // Use renamed helper

        // 1. Create an index
        // Removed outdated CreateIndexRequest instantiation block that was causing E0560 errors.
        let create_index_req = CreateIndexRequest {
            name: index_name.clone(),
            dimensions: 3, 
            metric: vortex_core::DistanceMetric::L2, 
            config: vortex_core::HnswConfig { // Nested config
                m: 16,
                m_max0: 16 * 2, // Added m_max0, defaulting to m * 2
                ef_construction: 100,
                ef_search: 50,
                ml: 0.33,
                seed: Some(0),   // Corrected seed to Option<u64>
                vector_dim: 3, // Added missing field
                vector_storage_capacity: None,
                graph_links_capacity: None,
            }
        };

        crate::handlers::create_index(axum::extract::State(app_state.clone()), Json(create_index_req))
            .await
            .expect("Failed to create index");

        // Call save_index here to ensure index files are created before further operations
        crate::persistence::save_index(&index_name, &*app_state.read().await, temp_dir.path())
            .await
            .expect("Failed to save index after creation");

        // 2. Add a vector (this operation will be WAL-logged but not part of the initial save)
        let add_vector_req = AddVectorRequest {
            id: vector_id1.clone(),
            vector: vector_data1.clone(), // Use Vec<f32> directly
            metadata: metadata1.clone(),
        };
        crate::handlers::add_vector(
            axum::extract::State(app_state.clone()),
            axum::extract::Path(index_name.clone()), // Corrected: Path is just index_name
            Json(add_vector_req),
        )
        .await
        .expect("Failed to add vector");
        
        // Intentionally do not call save_index *after this add_vector* to simulate a crash 
        // where this operation is only in the WAL.

        // Verify vector exists before "crash"
        let app_state_guard = app_state.read().await;
        let indices_guard = app_state_guard.indices.read().await;
        let index_lock = indices_guard.get(&index_name).unwrap();
        let index_guard = index_lock.read().await;
        let retrieved_vector_opt = index_guard.get_vector(&vector_id1).await.expect("Failed to get vector before crash");
        assert_eq!(retrieved_vector_opt.expect("Vector not found before crash").0.to_vec(), vector_data1);
        
        // Verify metadata using PayloadIndexRocksDB
        let payload_indices_guard = app_state_guard.payload_indices.read().await;
        let payload_db = payload_indices_guard.get(&index_name).expect("Payload DB not found for index");
        let retrieved_payload = payload_db.get_payload(&vector_id1).expect("Failed to get payload from RocksDB").expect("Payload not found in RocksDB for vector_id1");
        assert_eq!(retrieved_payload, metadata1.clone().unwrap());


    } // AppState is dropped here, simulating server shutdown/crash without explicit save

    // --- Second run: Simulate server restart and recovery ---
    {
        // Create a new AppState instance, load data, then wrap in Arc<RwLock<>>
        let persistence_path_buf = PathBuf::from(data_path_str);
        let mut app_state_restarted_instance = AppState::new(persistence_path_buf.clone());
        persistence::load_all_indices_on_startup(&mut app_state_restarted_instance, &persistence_path_buf).await;
        let app_state_restarted = Arc::new(RwLock::new(app_state_restarted_instance));

        // Verify index and vector exist after recovery
        let app_state_restarted_guard = app_state_restarted.read().await;
        assert!(app_state_restarted_guard.indices.read().await.contains_key(&index_name), "Index should exist after WAL recovery");
        
        let indices_restarted_guard = app_state_restarted_guard.indices.read().await;
        let index_lock_restarted = indices_restarted_guard.get(&index_name).unwrap();
        let index_guard_restarted = index_lock_restarted.read().await;
        
        let retrieved_vector_restarted_res = index_guard_restarted.get_vector(&vector_id1).await;
        assert!(retrieved_vector_restarted_res.is_ok() && retrieved_vector_restarted_res.as_ref().unwrap().is_some(), "Vector should be recoverable from WAL");
        assert_eq!(retrieved_vector_restarted_res.unwrap().unwrap().0.to_vec(), vector_data1, "Vector data mismatch after WAL recovery");

        // Verify metadata using PayloadIndexRocksDB after restart
        let payload_indices_restarted_guard = app_state_restarted_guard.payload_indices.read().await;
        let payload_db_restarted = payload_indices_restarted_guard.get(&index_name).expect("Payload DB not found for index after restart");
        let retrieved_payload_restarted = payload_db_restarted.get_payload(&vector_id1).expect("Failed to get payload from RocksDB after restart").expect("Payload not found in RocksDB for vector_id1 after restart");
        assert_eq!(retrieved_payload_restarted, metadata1.clone().unwrap(), "Metadata mismatch after WAL recovery");
        
        // Further checks: e.g., search for the vector
        let search_req = SearchRequest {
            query_vector: vector_data1.clone(), // Use Vec<f32> directly
            k: 1,
            filter: None,
        };
        let _ = crate::handlers::search_vectors( // Assign to _ to mark as used
            axum::extract::State(app_state_restarted.clone()),
            axum::extract::Path(index_name.clone()),
            Json(search_req.clone()) // Clone search_req if it's used later, or ensure it's not. Here it is used again.
        ).await.unwrap(); // Assuming unwrap is fine for test

        let search_result_json = crate::handlers::search_vectors( // Re-assign to avoid borrow error if used later
            axum::extract::State(app_state_restarted.clone()),
            axum::extract::Path(index_name.clone()),
            Json(SearchRequest { query_vector: vector_data1.clone(), k: 1, filter: None })  // Use Vec<f32> directly
        ).await.unwrap();


        let results_vec = search_result_json.0.results; 
        assert_eq!(results_vec.len(), 1, "Search should return one result after WAL recovery");
        assert_eq!(results_vec[0].id, vector_id1, "Search result ID mismatch after WAL recovery");
    }
}


#[tokio::test]
async fn test_wal_recovery_with_checkpointing() {
    let temp_dir = tempdir().unwrap();
    let data_path = temp_dir.path(); // PathBuf
    let data_path_str = data_path.to_str().unwrap();

    let index_name = "test-checkpoint-idx".to_string();
    
    let vector_id1 = "v1_before_checkpoint".to_string();
    let vector_data1 = vec![1.1, 2.2, 3.3];
    let metadata1: Option<JsonValue> = Some(serde_json::json!({"set": "before_cp"}));

    let vector_id2 = "v2_after_checkpoint".to_string();
    let vector_data2 = vec![4.4, 5.5, 6.6];
    let metadata2: Option<JsonValue> = Some(serde_json::json!({"set": "after_cp"}));

    // --- First run: Operations, Save/Checkpoint, More Operations ---
    {
        let app_state = create_empty_test_app_state_arc(data_path_str); // Use renamed helper

        // 1. Create index
        let create_req = CreateIndexRequest {
            name: index_name.clone(),
            dimensions: 3,
            metric: vortex_core::DistanceMetric::L2,
            config: vortex_core::HnswConfig { m: 8, m_max0: 16, ef_construction: 50, ef_search: 20, ml: 0.5, seed: Some(42), vector_dim: 3, vector_storage_capacity: None, graph_links_capacity: None }, // Added missing field
        };
        crate::handlers::create_index(axum::extract::State(app_state.clone()), Json(create_req))
            .await
            .expect("Failed to create index (before checkpoint)");

        // 2. Add vector before checkpoint
        let add_req1 = AddVectorRequest {
            id: vector_id1.clone(),
            vector: vector_data1.clone(), // Use Vec<f32> directly
            metadata: metadata1.clone(),
        };
        crate::handlers::add_vector(
            axum::extract::State(app_state.clone()),
            axum::extract::Path(index_name.clone()), // Corrected: Path is just index_name
            Json(add_req1),
        )
        .await
        .expect("Failed to add vector1 (before checkpoint)");

        // 3. Save index (this should trigger WAL checkpointing)
        crate::persistence::save_index(&index_name, &*app_state.read().await, data_path) // Pass &AppState
            .await
            .expect("Failed to save index and checkpoint WAL");

        // 4. Add vector after checkpoint
        let add_req2 = AddVectorRequest {
            id: vector_id2.clone(),
            vector: vector_data2.clone(), // Use Vec<f32> directly
            metadata: metadata2.clone(),
        };
        crate::handlers::add_vector(
            axum::extract::State(app_state.clone()),
            axum::extract::Path(index_name.clone()), // Corrected: Path is just index_name
            Json(add_req2),
        )
        .await
        .expect("Failed to add vector2 (after checkpoint)");

        // Verify both vectors exist before "crash"
        let app_state_guard = app_state.read().await;
        let indices_guard = app_state_guard.indices.read().await;
        let index_lock = indices_guard.get(&index_name).unwrap();
        let index_guard = index_lock.read().await;
        
        assert_eq!(index_guard.get_vector(&vector_id1).await.expect("v1 get failed before crash").expect("v1 not found before crash").0.to_vec(), vector_data1);
        assert_eq!(index_guard.get_vector(&vector_id2).await.expect("v2 get failed before crash").expect("v2 not found before crash").0.to_vec(), vector_data2);
        
        // Verify metadata using PayloadIndexRocksDB before "crash"
        let payload_indices_guard = app_state_guard.payload_indices.read().await;
        let payload_db = payload_indices_guard.get(&index_name).expect("Payload DB not found for index before crash");
        let retrieved_payload1_before_crash = payload_db.get_payload(&vector_id1).expect("Failed to get payload1 from RocksDB before crash").expect("Payload1 not found in RocksDB before crash");
        assert_eq!(retrieved_payload1_before_crash, metadata1.clone().unwrap());
        let retrieved_payload2_before_crash = payload_db.get_payload(&vector_id2).expect("Failed to get payload2 from RocksDB before crash").expect("Payload2 not found in RocksDB before crash");
        assert_eq!(retrieved_payload2_before_crash, metadata2.clone().unwrap());


    } // AppState dropped, simulating crash

    // --- Second run: Restart and Recover ---
    {
        // Create a new AppState instance, load data, then wrap in Arc<RwLock<>>
        let persistence_path_buf = PathBuf::from(data_path_str);
        let mut app_state_restarted_instance = AppState::new(persistence_path_buf.clone());
        persistence::load_all_indices_on_startup(&mut app_state_restarted_instance, &persistence_path_buf).await;
        let app_state_restarted = Arc::new(RwLock::new(app_state_restarted_instance));

        let app_state_restarted_guard = app_state_restarted.read().await;
        assert!(app_state_restarted_guard.indices.read().await.contains_key(&index_name), "Index should exist after recovery with checkpoint");
        
        let indices_restarted_guard = app_state_restarted_guard.indices.read().await;
        let index_lock_restarted = indices_restarted_guard.get(&index_name).unwrap();
        let index_guard_restarted = index_lock_restarted.read().await;

        // Verify vector1 (before checkpoint)
        let vec1_restarted_res = index_guard_restarted.get_vector(&vector_id1).await;
        assert!(vec1_restarted_res.is_ok() && vec1_restarted_res.as_ref().unwrap().is_some(), "Vector1 (before_cp) should be present after recovery");
        assert_eq!(vec1_restarted_res.unwrap().unwrap().0.to_vec(), vector_data1, "Vector1 data mismatch");

        // Verify vector2 (after checkpoint, recovered from WAL)
        let vec2_restarted_res = index_guard_restarted.get_vector(&vector_id2).await;
        assert!(vec2_restarted_res.is_ok() && vec2_restarted_res.as_ref().unwrap().is_some(), "Vector2 (after_cp) should be present after recovery");
        assert_eq!(vec2_restarted_res.unwrap().unwrap().0.to_vec(), vector_data2, "Vector2 data mismatch");
        
        // Verify metadata using PayloadIndexRocksDB after restart
        let payload_indices_restarted_guard = app_state_restarted_guard.payload_indices.read().await;
        let payload_db_restarted = payload_indices_restarted_guard.get(&index_name).expect("Payload DB not found for index after restart");
        
        let retrieved_payload1_restarted = payload_db_restarted.get_payload(&vector_id1).expect("Failed to get payload1 from RocksDB after restart").expect("Payload1 not found in RocksDB after restart");
        assert_eq!(retrieved_payload1_restarted, metadata1.clone().unwrap(), "Metadata1 mismatch after restart");
        
        let retrieved_payload2_restarted = payload_db_restarted.get_payload(&vector_id2).expect("Failed to get payload2 from RocksDB after restart").expect("Payload2 not found in RocksDB after restart");
        assert_eq!(retrieved_payload2_restarted, metadata2.clone().unwrap(), "Metadata2 mismatch after restart");

        // Search for both vectors
        let search_resp1_json = crate::handlers::search_vectors(
            axum::extract::State(app_state_restarted.clone()),
            axum::extract::Path(index_name.clone()),
            Json(SearchRequest { query_vector: vector_data1.clone(), k: 1, filter: None }) // Use Vec<f32>
        ).await.unwrap();
        assert_eq!(search_resp1_json.0.results[0].id, vector_id1);

        let search_resp2_json = crate::handlers::search_vectors(
            axum::extract::State(app_state_restarted.clone()),
            axum::extract::Path(index_name.clone()),
            Json(SearchRequest { query_vector: vector_data2.clone(), k: 1, filter: None }) // Use Vec<f32>
        ).await.unwrap();
        assert_eq!(search_resp2_json.0.results[0].id, vector_id2);
    }
}

// TODO: Add more test cases:
// - Recovery with multiple indices and vectors
// - Recovery after vector deletions
// - Recovery after vector updates (if update operation modifies vector data directly, or if it's delete + add)
// - Checkpointing interaction: operations -> save_index (checkpoint) -> more operations -> crash -> recovery // This is now covered
// - Recovery from corrupted WAL (e.g. last entry partial - though this might be hard to simulate here, unit tests cover segment corruption)
// - Test with empty WAL file (fresh start)
// - Test with WAL containing only checkpoint record and no further operations
