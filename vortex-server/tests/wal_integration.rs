use vortex_server::state::AppState;
// Import request/response structs directly from models
use vortex_server::models::{CreateIndexRequest, AddVectorRequest, SearchRequest}; // Removed SearchResponse, VectorResponse
// Import handlers separately
use vortex_server::handlers;
use vortex_server::persistence; // Import persistence module
use vortex_core::index::Index;
// Removed VectorId, DistanceMetric, HnswConfig as they are available via vortex_core::
use vortex_core; // Keep this for HnswConfig, DistanceMetric if needed qualified
use std::sync::Arc;
use tokio::sync::RwLock;
// Removed HashMap
use std::path::PathBuf;
use tempfile::tempdir;
use axum::Json; // Required for AddVectorRequest payload
use serde_json::Value; // Added for metadata

// Helper function to create a new AppState for testing
// Takes PathBuf, not &str. AppState::new is not async.
fn create_test_app_state(data_path: PathBuf) -> AppState {
    AppState::new(data_path)
}

#[tokio::test]
async fn test_wal_recovery_simple_operations() {
    let temp_dir = tempdir().unwrap();
    let data_path = PathBuf::from(temp_dir.path()); // Use PathBuf

    let index_name = "test-index-wal".to_string();
    let vector_id1 = "vec1".to_string();
    let vector_data1 = vec![1.0, 2.0, 3.0];
    // Metadata is Option<serde_json::Value>
    let metadata1: Option<Value> = Some(serde_json::json!({"feature": "test1"}));


    // --- First run: Perform operations ---
    {
        // AppState::new is not async, wrap in Arc<RwLock<>>
        let app_state = Arc::new(RwLock::new(create_test_app_state(data_path.clone())));

        // 1. Create an index
        let hnsw_config = vortex_core::HnswConfig {
            m: 16,
            m_max0: 32,
            ef_construction: 100,
            ef_search: 50,
            ml: 0.33,
            seed: None,
            vector_dim: 3, // This is crucial
        };
        let create_index_req = CreateIndexRequest {
            name: index_name.clone(),
            dimensions: 3, // This should match hnsw_config.vector_dim
            metric: vortex_core::DistanceMetric::L2,
            config: hnsw_config,
        };
        handlers::create_index(axum::extract::State(app_state.clone()), Json(create_index_req))
            .await
            .expect("Failed to create index");

        // 2. Add a vector
        let add_vector_req = AddVectorRequest {
            id: vector_id1.clone(), // AddVectorRequest needs id
            vector: vector_data1.clone(), // No Vector() wrapper
            metadata: metadata1.clone(),
        };
        handlers::add_vector(
            axum::extract::State(app_state.clone()),
            axum::extract::Path(index_name.clone()), // Path is just index_name
            Json(add_vector_req),
        )
        .await
        .expect("Failed to add vector");
        
        // Intentionally do not call save_index to simulate a crash before checkpointing

        // Verify vector exists before "crash"
        let app_state_guard = app_state.read().await; // Lock AppState once
        let index_lock = app_state_guard.indices.read().await.get(&index_name).unwrap().clone(); // Access indices map
        let index_guard = index_lock.read().await;
        let retrieved_vector_result = index_guard.get_vector(&vector_id1).await; // This returns Result<Option<Embedding>, VortexError>
        assert!(retrieved_vector_result.is_ok(), "Failed to get vector before crash");
        // unwrap the Result, then unwrap the Option to get Embedding
        assert_eq!(retrieved_vector_result.unwrap().unwrap(), vortex_core::vector::Embedding::from(vector_data1.clone())); // Compare Embedding with Embedding
        
        // Extend lifetime of metadata_store_guard
        let metadata_store_guard = app_state_guard.metadata_store.read().await;
        let metadata_for_index = metadata_store_guard.get(&index_name).unwrap();
        let retrieved_metadata = metadata_for_index.get(&vector_id1).unwrap();
        assert_eq!(retrieved_metadata, metadata1.as_ref().unwrap()); // Compare with &Value

    } // AppState is dropped here, simulating server shutdown/crash without explicit save

    // --- Second run: Simulate server restart and recovery ---
    {
        // AppState::new is not async, wrap in Arc<RwLock<>>
        let app_state_restarted_instance = create_test_app_state(data_path.clone());
        let app_state_restarted_arc = Arc::new(RwLock::new(app_state_restarted_instance));

        // Explicitly call load_all_indices_on_startup to trigger WAL recovery
        // It takes &AppState and &Path. data_path is available.
        // Need to pass a reference to AppState from the Arc<RwLock<AppState>>.
        // This requires locking and getting a ref, or passing the instance before wrapping if possible.
        // Let's pass data_path directly.
        // The function doesn't return a Result, so .expect() is wrong.
        let app_state_ref_for_load = app_state_restarted_arc.read().await;
        persistence::load_all_indices_on_startup(&app_state_ref_for_load, &data_path).await;
        drop(app_state_ref_for_load); // Release lock

        // Verify index and vector exist after recovery
        let app_state_restarted_guard = app_state_restarted_arc.read().await; // Lock AppState once
        assert!(app_state_restarted_guard.indices.read().await.contains_key(&index_name), "Index should exist after WAL recovery");
        
        let index_lock_restarted = app_state_restarted_guard.indices.read().await.get(&index_name).unwrap().clone();
        let index_guard_restarted = index_lock_restarted.read().await;
        
        let retrieved_vector_restarted_result = index_guard_restarted.get_vector(&vector_id1).await; // Returns Result<Option<Embedding>, VortexError>
        assert!(retrieved_vector_restarted_result.is_ok(), "Vector should be recoverable from WAL");
        // unwrap the Result, then unwrap the Option to get Embedding
        assert_eq!(retrieved_vector_restarted_result.unwrap().unwrap(), vortex_core::vector::Embedding::from(vector_data1.clone()), "Vector data mismatch after WAL recovery");

        let metadata_store_restarted_guard = app_state_restarted_guard.metadata_store.read().await;
        let metadata_for_index_restarted = metadata_store_restarted_guard.get(&index_name);
        assert!(metadata_for_index_restarted.is_some(), "Metadata for index should exist after WAL recovery");
        let retrieved_metadata_restarted = metadata_for_index_restarted.unwrap().get(&vector_id1);
        assert!(retrieved_metadata_restarted.is_some(), "Metadata for vector should be recoverable from WAL");
        assert_eq!(retrieved_metadata_restarted.unwrap(), metadata1.as_ref().unwrap(), "Metadata mismatch after WAL recovery");

        // Further checks: e.g., search for the vector
        let search_req = SearchRequest {
            query_vector: vector_data1.clone(), // No Vector() wrapper
            k: 1,
            filter: None,
        };
        let search_result_json = handlers::search_vectors( // Renamed to avoid conflict
            axum::extract::State(app_state_restarted_arc.clone()), // Use the Arc for State
            axum::extract::Path(index_name.clone()),
            Json(search_req)
        ).await.expect("Search after WAL recovery failed"); // Use expect for Result

        let search_response = search_result_json.0; 
        assert_eq!(search_response.results.len(), 1, "Search should return one result after WAL recovery");
        assert_eq!(search_response.results[0].id, vector_id1, "Search result ID mismatch after WAL recovery");
    }
}

// TODO: Add more test cases:
// - Recovery with multiple indices and vectors
// - Recovery after vector deletions
// - Recovery after vector updates (if update operation modifies vector data directly, or if it's delete + add)
// - Checkpointing interaction: operations -> save_index (checkpoint) -> more operations -> crash -> recovery
// - Recovery from corrupted WAL (e.g. last entry partial - though this might be hard to simulate here, unit tests cover segment corruption)
// - Test with empty WAL file (fresh start)
// - Test with WAL containing only checkpoint record and no further operations
