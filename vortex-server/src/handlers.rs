use crate::error::{ServerError, ServerResult};
use crate::models::{
    AddVectorRequest, CreateIndexRequest, SearchRequest, SearchResponse, SearchResultItem,
    StatsResponse, SuccessResponse, VectorResponse,
};
use crate::state::AppState;
use crate::wal::wal_manager::{CollectionWalManager, WalRecord}; // Corrected path
use crate::wal::VortexWalOptions; 


use axum::{
    extract::{Path, Query, State}, // Added Query
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use serde::Deserialize; // Added Deserialize
// use std::collections::HashMap; // Added HashMap (for query params)
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};
use vortex_core::{Embedding, HnswIndex, Index, VectorId, VortexError};


/// Handler for `GET /indices`
/// Lists the names of all available indices.
#[axum::debug_handler]
pub async fn list_indices(
    State(state_arc): State<Arc<RwLock<AppState>>>,
) -> ServerResult<Json<Vec<String>>> {
    debug!("Received request to list indices");
    let app_state_guard = state_arc.read().await;
    let indices_map_guard = app_state_guard.indices.read().await;
    let index_names: Vec<String> = indices_map_guard.keys().cloned().collect();
    debug!(count = index_names.len(), "Returning index list");
    Ok(Json(index_names))
}

/// Handler for `POST /indices`
/// Creates a new vector index.
#[axum::debug_handler]
pub async fn create_index(
    State(state): State<Arc<RwLock<AppState>>>,
    Json(payload): Json<CreateIndexRequest>,
) -> ServerResult<impl IntoResponse> {
    info!(index_name = %payload.name, dims = payload.dimensions, metric = ?payload.metric, "Received request to create index");

    // Validate dimensions
    if payload.dimensions == 0 {
         return Err(ServerError::BadRequest("Dimensions must be greater than 0".to_string()));
    }
     // Validate config
    if let Err(e) = payload.config.validate() {
        return Err(ServerError::CoreError(e));
    }

    let app_state_guard = state.read().await; // Read lock for AppState to access its fields
    let mut indices_map_writer = app_state_guard.indices.write().await;

    if indices_map_writer.contains_key(&payload.name) {
        warn!(index_name = %payload.name, "Attempted to create existing index");
        return Err(ServerError::BadRequest(format!(
            "Index '{}' already exists",
            payload.name
        )));
    }

    // Create the HNSW index instance
    // TODO: Capacity should be configurable via CreateIndexRequest and server defaults.
    const DEFAULT_CAPACITY: u64 = 1_000_000; // Example default capacity

    let data_path_buf = app_state_guard.data_path.clone(); // Clone PathBuf from AppState guard
    let base_path = &data_path_buf; // Use reference to the cloned PathBuf

    let new_index = HnswIndex::new(
        base_path,
        &payload.name,
        payload.config, // This HnswConfig should already contain vector_dim
        payload.metric
        // payload.dimensions as u32, // Removed, should be part of payload.config
        // DEFAULT_CAPACITY // Removed, capacity managed by segments
    )
    .await // Added .await here
    .map_err(ServerError::from)?; // Convert core error to server error

    // Store the index wrapped in Arc<RwLock<>> in the shared state
    let new_index_arc = Arc::new(RwLock::new(new_index));
    indices_map_writer.insert(payload.name.clone(), new_index_arc.clone());
    drop(indices_map_writer); // Release write lock on indices map

    // WAL: Log CreateIndex operation AFTER HnswIndex is successfully created locally
    // but before we return success to the client.
    // The WAL manager itself should be created and stored.
    let wal_path = CollectionWalManager::get_wal_path_for_index(&data_path_buf, &payload.name);
    // TODO: WalOptions should be configurable, perhaps part of CreateIndexRequest or server defaults
    let wal_manager = CollectionWalManager::new(wal_path, VortexWalOptions::default())
        .map_err(|e| ServerError::WalError(format!("Failed to create WAL manager for index {}: {:?}", payload.name, e)))?;
    
    let create_index_record = WalRecord::CreateIndex {
        index_name: payload.name.clone(),
        config: payload.config, // Assuming CreateIndexRequest.config is HnswConfig
        metric: payload.metric, // Assuming CreateIndexRequest.metric is DistanceMetric
        dimensions: payload.dimensions as u32,
        capacity: DEFAULT_CAPACITY, // This should align with HnswIndex::new
    };

    if let Err(e) = wal_manager.log_operation(&create_index_record).await { // Added .await
        // Critical: WAL write failed. Should we attempt to clean up the created HNSW files?
        // For now, log critical error and return failure. Recovery might be complex.
        tracing::error!(index_name = %payload.name, error = ?e, "CRITICAL: Failed to log CreateIndex to WAL after index files were created. Index may be inconsistent on next load if server crashes now.");
        // Attempt to remove the HNSWIndex from memory to prevent further operations on it.
        app_state_guard.indices.write().await.remove(&payload.name); // Re-lock to remove
        // Consider deleting the HNSW files from disk here, or marking for cleanup.
        // This is a complex recovery scenario. For now, error out.
        return Err(ServerError::WalError(format!("Failed to log CreateIndex to WAL: {:?}", e)));
    } // Removed .await?; from here

    // Store the WAL manager
    let mut wal_managers_map_writer = app_state_guard.wal_managers.write().await;
    wal_managers_map_writer.insert(payload.name.clone(), Arc::new(wal_manager));
    drop(wal_managers_map_writer); // Release lock

    info!(index_name = %payload.name, "Index and WAL created successfully");
    Ok((
        StatusCode::CREATED,
        Json(SuccessResponse {
            message: format!("Index '{}' created successfully", payload.name),
        }),
    ))
}

/// Handler for `PUT /indices/{name}/vectors`
/// Adds or updates a vector in the specified index.
#[axum::debug_handler]
pub async fn add_vector(
    State(state): State<Arc<RwLock<AppState>>>,
    Path(index_name): Path<String>,
    Json(payload): Json<AddVectorRequest>,
) -> ServerResult<impl IntoResponse> {
    debug!(index_name = %index_name, vector_id = %payload.id, "Received request to add/update vector");

    let app_state_guard = state.read().await; // Read lock for AppState

    let index_lock_arc: Arc<RwLock<HnswIndex>> = {
        let indices_map_guard = app_state_guard.indices.read().await;
        indices_map_guard
            .get(&index_name)
            .cloned()
            .ok_or_else(|| ServerError::IndexNotFound(index_name.clone()))?
    }; // Read lock on map released here

    // Now acquire write lock on the specific index's RwLock
    let mut index_guard = index_lock_arc.write().await;
    let embedding: Embedding = payload.vector.into();

    // Call the mutable method on the locked index guard
    let vector_id_clone = payload.id.clone(); // Clone for metadata key and logging

    // WAL: Log AddVector operation BEFORE applying to HnswIndex
    let wal_record = WalRecord::AddVector {
        vector_id: payload.id.clone(),
        vector: embedding.clone(), // Clone embedding for WAL record
        metadata: payload.metadata.clone(), // Include metadata in WAL record
    };
    {
        let wal_managers_map_guard = app_state_guard.wal_managers.read().await;
        let wal_manager = wal_managers_map_guard.get(&index_name)
            .ok_or_else(|| ServerError::WalError(format!("WAL manager not found for index {}", index_name)))?;
        wal_manager.log_operation(&wal_record).await
            .map_err(|e| ServerError::WalError(format!("Failed to log AddVector to WAL: {:?}", e)))?;
    }

    let added = index_guard.add_vector(payload.id.clone(), embedding).await?;

    // Store metadata if provided
    if let Some(metadata_value) = payload.metadata {
        let mut metadata_store_writer = app_state_guard.metadata_store.write().await;
        let index_metadata_store = metadata_store_writer
            .entry(index_name.clone())
            .or_insert_with(std::collections::HashMap::new);
        index_metadata_store.insert(vector_id_clone.clone(), metadata_value); // Use cloned vector_id_clone
        debug!(index_name=%index_name, vector_id=%vector_id_clone, "Stored metadata for vector");
    }

    let status_code = if added {
        info!(index_name=%index_name, vector_id=%vector_id_clone, "Vector added");
        StatusCode::CREATED
    } else {
        info!(index_name=%index_name, vector_id=%vector_id_clone, "Vector updated");
        StatusCode::OK
    };

    Ok((status_code, Json(SuccessResponse { message: format!("Vector '{}' processed successfully", vector_id_clone) })))
}


/// Handler for `POST /indices/{name}/search`
/// Searches for nearest neighbors in the specified index.
#[axum::debug_handler]
pub async fn search_vectors(
    State(state): State<Arc<RwLock<AppState>>>,
    Path(index_name): Path<String>,
    Json(payload): Json<SearchRequest>,
) -> ServerResult<Json<SearchResponse>> {
    debug!(index_name = %index_name, k = payload.k, "Received search request");

    let app_state_guard = state.read().await; // Read lock for AppState

    let index_lock_arc: Arc<RwLock<HnswIndex>> = {
        let indices_map_guard = app_state_guard.indices.read().await;
        indices_map_guard
            .get(&index_name)
            .cloned()
            .ok_or_else(|| ServerError::IndexNotFound(index_name.clone()))?
    };

    // Acquire read lock on the specific index's RwLock for search
    let index_guard = index_lock_arc.read().await;

    let embedding: Embedding = payload.query_vector.into();
    let k = payload.k;

    // Dimension check
    if embedding.len() != index_guard.dimensions() {
        return Err(ServerError::CoreError(VortexError::DimensionMismatch {
            expected: index_guard.dimensions(),
            actual: embedding.len(),
        }));
    }

    if k == 0 {
        return Ok(Json(SearchResponse { results: vec![] }));
    }

    // Determine how many initial candidates to fetch if filtering
    let k_to_fetch = if payload.filter.is_some() {
        std::cmp::max(k * 5, k + 50).min(1000) // Fetch more if filtering, up to a limit
    } else {
        k
    };

    // Perform the search using the Index trait method on the read guard
    // Ensure ef_search for HNSW is at least k_to_fetch
    let hnsw_ef_search = std::cmp::max(k_to_fetch, index_guard.config().ef_search);
    
    // Validate filter type if present, before fetching/looping
    if let Some(filter_val) = &payload.filter {
        if !filter_val.is_object() {
            return Err(ServerError::BadRequest("Filter must be a JSON object.".to_string()));
        }
    }

    let initial_search_results = index_guard.search_with_ef(embedding, k_to_fetch, hnsw_ef_search).await.map_err(ServerError::from)?;
    debug!(index_name=%index_name, initial_count=initial_search_results.len(), "Initial search completed");

    let mut final_results: Vec<SearchResultItem> = Vec::with_capacity(k);
    
    let index_specific_metadata_map_opt: Option<std::collections::HashMap<VectorId, serde_json::Value>> = {
        let metadata_store_guard = app_state_guard.metadata_store.read().await;
        metadata_store_guard.get(&index_name).cloned()
    };

    for (id_ref, score_ref) in &initial_search_results {
        if final_results.len() >= k {
            break;
        }

        let id = id_ref.clone();
        let score = *score_ref;

        let passes_filter = match (&payload.filter, &index_specific_metadata_map_opt) {
            (Some(filter_value), Some(actual_metadata_map)) => {
                let filter_obj = filter_value.as_object().expect("Filter should be an object due to earlier check");
                if filter_obj.is_empty() {
                    true
                } else if let Some(metadata_json_value) = actual_metadata_map.get(&id) {
                    if let Some(metadata_actual_obj) = metadata_json_value.as_object() {
                        matches_filter(metadata_actual_obj, filter_obj)
                    } else { false }
                } else { false }
            }
            (None, _) => true, // No filter applied
            (Some(filter_value), None) => { // Filter applied, but no metadata store for this index
                 // If filter is not empty, then it's a mismatch. If filter is empty, it's a match.
                 filter_value.as_object().map_or(true, |obj| obj.is_empty())
            }
        };

        if passes_filter {
            let metadata = index_specific_metadata_map_opt.as_ref()
                .and_then(|store| store.get(&id).cloned());
            final_results.push(SearchResultItem { id, score, metadata });
        }
    }

    Ok(Json(SearchResponse {
        results: final_results,
    }))
}

/// Helper function to check if a metadata object matches a filter object.
/// For V1, this implements a simple exact match for all key-value pairs in the filter.
fn matches_filter(
    metadata: &serde_json::Map<String, serde_json::Value>,
    filter: &serde_json::Map<String, serde_json::Value>,
) -> bool {
    // An empty filter object is handled by the caller to mean "match all".
    // This function assumes filter is non-empty if called.
    // However, to be safe and align with the caller's direct use:
    if filter.is_empty() {
        return true; 
    }
    for (filter_key, filter_value) in filter {
        match metadata.get(filter_key) {
            Some(metadata_value) => {
                if metadata_value != filter_value {
                    return false; // Value mismatch
                }
            }
            None => {
                return false; // Key not found in metadata, so filter condition not met
            }
        }
    }
    true // All filter key-value pairs were found and matched in metadata
}

/// Handler for `GET /indices/{name}/stats`
/// Retrieves statistics about the specified index.
#[axum::debug_handler]
pub async fn get_index_stats(
    State(state): State<Arc<RwLock<AppState>>>,
    Path(index_name): Path<String>,
) -> ServerResult<Json<StatsResponse>> {
    info!(index_name = %index_name, "Received request for index stats");

    let app_state_guard = state.read().await; // Read lock for AppState

    let index_lock_arc: Arc<RwLock<HnswIndex>> = {
        let indices_map_guard = app_state_guard.indices.read().await;
        indices_map_guard
            .get(&index_name)
            .cloned()
            .ok_or_else(|| ServerError::IndexNotFound(index_name.clone()))?
    };

    // Acquire read lock on the specific index's RwLock for stats
    let index_guard = index_lock_arc.read().await;

    // Gather stats using Index trait methods
    let stats = StatsResponse {
        vector_count: index_guard.len(),
        dimensions: index_guard.dimensions(),
        config: index_guard.config(),
        metric: index_guard.distance_metric(),
    };

    info!(index_name=%index_name, vector_count=stats.vector_count, "Returning index stats");
    Ok(Json(stats))
}

/// Handler for `GET /indices/{name}/vectors/{vector_id}`
/// Retrieves a specific vector by its ID.
#[axum::debug_handler]
pub async fn get_vector(
    State(state): State<Arc<RwLock<AppState>>>,
    Path((index_name, vector_id)): Path<(String, VectorId)>,
) -> ServerResult<Json<VectorResponse>> {
    debug!(index_name = %index_name, vector_id = %vector_id, "Received request to get vector");

    let app_state_guard = state.read().await; // Read lock for AppState

    let index_lock_arc: Arc<RwLock<HnswIndex>> = {
        let indices_map_guard = app_state_guard.indices.read().await;
        indices_map_guard
            .get(&index_name)
            .cloned()
            .ok_or_else(|| ServerError::IndexNotFound(index_name.clone()))?
    };

    // Acquire read lock on the specific index's RwLock
    let index_guard = index_lock_arc.read().await;

    match index_guard.get_vector(&vector_id).await.map_err(ServerError::from)? {
        Some(embedding) => {
            debug!(index_name=%index_name, vector_id=%vector_id, "Vector found");
            let metadata = { // New scope for metadata_store read lock
                let metadata_store_guard = app_state_guard.metadata_store.read().await;
                metadata_store_guard
                    .get(&index_name)
                    .and_then(|store| store.get(&vector_id).cloned())
            };

            Ok(Json(VectorResponse {
                id: vector_id,
                vector: embedding.into(), // Convert Embedding back to Vec<f32>
                metadata,
            }))
        }
        None => {
            warn!(index_name=%index_name, vector_id=%vector_id, "Vector not found");
            Err(ServerError::CoreError(VortexError::NotFound(vector_id))) // Return 404 via core error
        }
    }
}


/// Query parameters for listing vectors
#[derive(Deserialize, Debug)]
pub struct ListVectorsParams {
    limit: Option<usize>,
}

/// Handler for `GET /indices/{name}/vectors`
/// Lists vectors in the specified index, with an optional limit.
#[axum::debug_handler]
pub async fn list_vectors(
    State(state): State<Arc<RwLock<AppState>>>,
    Path(index_name): Path<String>,
    Query(params): Query<ListVectorsParams>,
) -> ServerResult<Json<Vec<VectorResponse>>> { // Return a Vec of VectorResponse
    debug!(index_name = %index_name, limit = ?params.limit, "Received request to list vectors");

    let app_state_guard = state.read().await; // Read lock for AppState

    let index_lock_arc: Arc<RwLock<HnswIndex>> = {
        let indices_map_guard = app_state_guard.indices.read().await;
        indices_map_guard
            .get(&index_name)
            .cloned()
            .ok_or_else(|| ServerError::IndexNotFound(index_name.clone()))?
    };

    // Acquire read lock on the specific index's RwLock
    let index_guard = index_lock_arc.read().await;

    // Call the core library method
    let vectors_list = index_guard.list_vectors(params.limit).await?;
    debug!(index_name=%index_name, count=vectors_list.len(), "Listed vectors from core");

    // Convert core results to API response model
    let index_metadata_store_clone = { // New scope for metadata_store read lock
        let metadata_store_guard = app_state_guard.metadata_store.read().await;
        metadata_store_guard.get(&index_name).cloned() // Clone the HashMap if found
    };

    let response_list = vectors_list
        .into_iter()
        .map(|(id, embedding)| {
            let metadata = index_metadata_store_clone
                .as_ref() // Get an Option<&HashMap<...>>
                .and_then(|store| store.get(&id).cloned());
            VectorResponse {
                id,
                vector: embedding.into(), // Convert Embedding to Vec<f32>
                metadata,
            }
        })
        .collect();

    Ok(Json(response_list))
}

/// Handler for `DELETE /indices/{name}/vectors/{vector_id}`
/// Deletes a vector by its ID (marks as deleted in the core library).
#[axum::debug_handler]
pub async fn delete_vector(
    State(state): State<Arc<RwLock<AppState>>>,
    Path((index_name, vector_id)): Path<(String, VectorId)>,
) -> ServerResult<impl IntoResponse> {
    info!(index_name = %index_name, vector_id = %vector_id, "Received request to delete vector (soft delete)");

    let app_state_guard = state.read().await; // Read lock for AppState

    let index_lock_arc: Arc<RwLock<HnswIndex>> = {
        let indices_map_guard = app_state_guard.indices.read().await;
        indices_map_guard
            .get(&index_name)
            .cloned()
            .ok_or_else(|| ServerError::IndexNotFound(index_name.clone()))?
    };

    // Acquire write lock on the specific index's RwLock
    let mut index_guard = index_lock_arc.write().await;

    // Call the mutable method

    // WAL: Log DeleteVector operation BEFORE applying to HnswIndex
    let wal_record = WalRecord::DeleteVector {
        vector_id: vector_id.clone(),
    };
    {
        let wal_managers_map_guard = app_state_guard.wal_managers.read().await;
        let wal_manager = wal_managers_map_guard.get(&index_name)
            .ok_or_else(|| ServerError::WalError(format!("WAL manager not found for index {}", index_name)))?;
        wal_manager.log_operation(&wal_record).await
            .map_err(|e| ServerError::WalError(format!("Failed to log DeleteVector to WAL: {:?}", e)))?;
    }

    let deleted = index_guard.delete_vector(&vector_id).await?;

    if deleted {
        info!(index_name=%index_name, vector_id=%vector_id, "Vector marked as deleted in core index");
        // Also remove metadata
        let mut metadata_store_writer = app_state_guard.metadata_store.write().await;
        if let Some(index_metadata_store) = metadata_store_writer.get_mut(&index_name) {
            if index_metadata_store.remove(&vector_id).is_some() {
                debug!(index_name=%index_name, vector_id=%vector_id, "Removed metadata for deleted vector");
            }
        }
        Ok(StatusCode::NO_CONTENT)
    } else {
        warn!(index_name=%index_name, vector_id=%vector_id, "Vector not found for deletion");
        Err(ServerError::CoreError(VortexError::NotFound(vector_id))) // Return 404
    }
}

/// Handler for `POST /indices/{name}/vectors/batch`
/// Adds multiple vectors to the specified index in a batch.
#[axum::debug_handler]
pub async fn batch_add_vectors(
    State(state): State<Arc<RwLock<AppState>>>,
    Path(index_name): Path<String>,
    Json(payload): Json<crate::models::BatchAddVectorRequest>, // Explicitly use crate::models
) -> ServerResult<Json<crate::models::BatchOperationResponse>> {
    info!(index_name = %index_name, count = payload.vectors.len(), "Received request to batch add vectors");

    if payload.vectors.is_empty() {
        return Ok(Json(crate::models::BatchOperationResponse {
            success_count: 0,
            failure_count: 0,
            message: "No vectors provided in the batch.".to_string(),
        }));
    }

    let app_state_guard = state.read().await; // Read lock for AppState

    let index_lock_arc: Arc<RwLock<HnswIndex>> = {
        let indices_map_guard = app_state_guard.indices.read().await;
        indices_map_guard
            .get(&index_name)
            .cloned()
            .ok_or_else(|| ServerError::IndexNotFound(index_name.clone()))?
    };

    // --- START PRE-FLIGHT DIMENSION CHECK ---
    {
        let index_guard_for_dim_check = index_lock_arc.read().await;
        let expected_dims = index_guard_for_dim_check.dimensions();
        for item in &payload.vectors { // Iterate by reference for check
            if item.vector.len() != expected_dims {
                let error_msg = format!(
                    "Dimension mismatch for vector ID '{}' in batch: index '{}' expects {} dimensions, but vector has {}.",
                    item.id, index_name, expected_dims, item.vector.len()
                );
                warn!("{}", error_msg);
                return Err(ServerError::BadRequest(error_msg)); // Fail entire batch
            }
        }
    }
    // --- END PRE-FLIGHT DIMENSION CHECK ---

    // If all dimensions are correct, proceed with adding
    let mut index_guard = index_lock_arc.write().await; // Lock HNSW index for the whole batch
    
    // Lock metadata store for the duration of the batch operation
    let mut metadata_store_writer = app_state_guard.metadata_store.write().await;
    let index_metadata_store_mut_ref = metadata_store_writer
        .entry(index_name.clone())
        .or_insert_with(std::collections::HashMap::new);

    let mut success_count = 0;
    let mut failure_count = 0;
    let mut error_messages: Vec<String> = Vec::new(); // To collect errors for non-dimension issues

    for item in payload.vectors { // Consumes payload.vectors now
        let embedding: Embedding = item.vector.into();
        // Dimension check already performed in pre-flight

        match index_guard.add_vector(item.id.clone(), embedding).await {
            Ok(_added) => {
                if let Some(metadata_value) = item.metadata {
                    index_metadata_store_mut_ref.insert(item.id.clone(), metadata_value);
                }
                success_count += 1;
            }
            Err(e) => {
                let err_msg = format!("Failed to add vector ID '{}' during batch: {:?}", item.id, e);
                warn!(vector_id = %item.id, index_name = %index_name, error = ?e, "Error in batch add (post-dimension-check)");
                error_messages.push(err_msg);
                failure_count += 1;
            }
        }
    }

    let message = if failure_count > 0 {
        format!(
            "Batch operation for index '{}' partially completed. Success: {}, Failures: {}. Errors: [{}]",
            index_name, success_count, failure_count, error_messages.join("; ")
        )
    } else {
        format!(
            "Batch operation completed successfully for index '{}'. Vectors processed: {}.",
            index_name, success_count
        )
    };
    info!("{}", message);

    // If there were failures after the pre-flight check, it's still a partial success overall for the batch endpoint's execution.
    // The HTTP status could be 207 Multi-Status if we wanted to be very specific, but 200 OK with details is also common.
    Ok(Json(crate::models::BatchOperationResponse {
        success_count,
        failure_count,
        message,
        // Consider adding 'errors: error_messages' to BatchOperationResponse struct if detailed errors are needed by client
    }))
}
