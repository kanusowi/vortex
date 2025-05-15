use crate::error::{ServerError, ServerResult};
use crate::models::{
    AddVectorRequest, CreateIndexRequest, SearchRequest, SearchResponse, SearchResultItem,
    StatsResponse, SuccessResponse, VectorResponse,
};
use crate::state::AppState;

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
pub async fn list_indices(
    State(state): State<AppState>,
) -> ServerResult<Json<Vec<String>>> {
    debug!("Received request to list indices");
    let indices_map = state.indices.read().await; // Read lock on the map
    let index_names: Vec<String> = indices_map.keys().cloned().collect();
    debug!(count = index_names.len(), "Returning index list");
    Ok(Json(index_names))
}

/// Handler for `POST /indices`
/// Creates a new vector index.
pub async fn create_index(
    State(state): State<AppState>,
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

    let mut indices_map = state.indices.write().await; // Lock HashMap for writing

    if indices_map.contains_key(&payload.name) {
        warn!(index_name = %payload.name, "Attempted to create existing index");
        return Err(ServerError::BadRequest(format!(
            "Index '{}' already exists",
            payload.name
        )));
    }

    // Create the HNSW index instance
    // TODO: Capacity should be configurable via CreateIndexRequest and server defaults.
    const DEFAULT_CAPACITY: u64 = 1_000_000; // Example default capacity

    let data_path_str = state.data_path.to_string_lossy().to_string(); // Get data_path from AppState
    let base_path = std::path::Path::new(&data_path_str); // Convert to Path

    let new_index = HnswIndex::new(
        base_path,
        &payload.name,
        payload.config,
        payload.metric,
        payload.dimensions as u32, // Convert usize to u32
        DEFAULT_CAPACITY
    )
    .map_err(ServerError::from)?; // Convert core error to server error

    // Store the index wrapped in Arc<RwLock<>> in the shared state
    indices_map.insert(payload.name.clone(), Arc::new(RwLock::new(new_index)));

    info!(index_name = %payload.name, "Index created successfully");
    Ok((
        StatusCode::CREATED,
        Json(SuccessResponse {
            message: format!("Index '{}' created successfully", payload.name),
        }),
    ))
}

/// Handler for `PUT /indices/{name}/vectors`
/// Adds or updates a vector in the specified index.
pub async fn add_vector(
    State(state): State<AppState>,
    Path(index_name): Path<String>,
    Json(payload): Json<AddVectorRequest>,
) -> ServerResult<impl IntoResponse> {
    debug!(index_name = %index_name, vector_id = %payload.id, "Received request to add/update vector");

    let index_lock_arc: Arc<RwLock<HnswIndex>> = { // Scope for HashMap read lock
        let indices_map = state.indices.read().await;
        indices_map
            .get(&index_name)
            .cloned() // Clone the Arc<RwLock<HnswIndex>>
            .ok_or_else(|| ServerError::IndexNotFound(index_name.clone()))?
    }; // Read lock on map released here

    // Now acquire write lock on the specific index's RwLock
    let mut index_guard = index_lock_arc.write().await;
    let embedding: Embedding = payload.vector.into();

    // Call the mutable method on the locked index guard
    let vector_id_clone = payload.id.clone(); // Clone for metadata key and logging
    let added = index_guard.add_vector(payload.id.clone(), embedding).await?;

    // Store metadata if provided
    if let Some(metadata_value) = payload.metadata {
        let mut metadata_map_guard = state.metadata_store.write().await;
        let index_metadata_store = metadata_map_guard
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
pub async fn search_vectors(
    State(state): State<AppState>,
    Path(index_name): Path<String>,
    Json(payload): Json<SearchRequest>,
) -> ServerResult<Json<SearchResponse>> {
    debug!(index_name = %index_name, k = payload.k, "Received search request");

     let index_lock_arc: Arc<RwLock<HnswIndex>> = { // Scope for HashMap read lock
        let indices_map = state.indices.read().await;
        indices_map
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
    let metadata_map_guard = state.metadata_store.read().await;
    let index_specific_metadata_store = metadata_map_guard.get(&index_name);

    for (id_ref, score_ref) in &initial_search_results { 
        if final_results.len() >= k {
            break;
        }

        let id = id_ref.clone(); 
        let score = *score_ref;   

        let passes_filter = match &payload.filter {
            Some(filter_value) => { // We know filter_value is an object due to the check above
                let filter_obj = filter_value.as_object().unwrap(); // Safe now
                
                if filter_obj.is_empty() {
                    true // An empty filter object matches all items
                } else {
                    match index_specific_metadata_store.and_then(|store| store.get(&id)) {
                        Some(metadata_json_value) => {
                            if let Some(metadata_actual_obj) = metadata_json_value.as_object() {
                                matches_filter(metadata_actual_obj, filter_obj)
                            } else { 
                                false // Metadata is present but not an object, cannot match non-empty object filter
                            }
                        }
                        None => false, // No metadata, so cannot match a non-empty filter
                    }
                }
            }
            None => true, // No filter applied, so all items pass
        };

        if passes_filter {
            let metadata = index_specific_metadata_store.and_then(|store| store.get(&id).cloned());
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
pub async fn get_index_stats(
    State(state): State<AppState>,
    Path(index_name): Path<String>,
) -> ServerResult<Json<StatsResponse>> {
    info!(index_name = %index_name, "Received request for index stats");

     let index_lock_arc: Arc<RwLock<HnswIndex>> = { // Scope for HashMap read lock
        let indices_map = state.indices.read().await;
        indices_map
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
pub async fn get_vector(
    State(state): State<AppState>,
    Path((index_name, vector_id)): Path<(String, VectorId)>,
) -> ServerResult<Json<VectorResponse>> {
    debug!(index_name = %index_name, vector_id = %vector_id, "Received request to get vector");

    let index_lock_arc: Arc<RwLock<HnswIndex>> = { // Scope for HashMap read lock
        let indices_map = state.indices.read().await;
        indices_map
            .get(&index_name)
            .cloned()
            .ok_or_else(|| ServerError::IndexNotFound(index_name.clone()))?
    };

    // Acquire read lock on the specific index's RwLock
    let index_guard = index_lock_arc.read().await;

    match index_guard.get_vector(&vector_id).await.map_err(ServerError::from)? {
        Some(embedding) => {
            debug!(index_name=%index_name, vector_id=%vector_id, "Vector found");
            let metadata_map_guard = state.metadata_store.read().await;
            let metadata = metadata_map_guard
                .get(&index_name)
                .and_then(|store| store.get(&vector_id).cloned());

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
pub async fn list_vectors(
    State(state): State<AppState>,
    Path(index_name): Path<String>,
    Query(params): Query<ListVectorsParams>,
) -> ServerResult<Json<Vec<VectorResponse>>> { // Return a Vec of VectorResponse
    debug!(index_name = %index_name, limit = ?params.limit, "Received request to list vectors");

    let index_lock_arc: Arc<RwLock<HnswIndex>> = { // Scope for HashMap read lock
        let indices_map = state.indices.read().await;
        indices_map
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
    let metadata_map_guard = state.metadata_store.read().await;
    let index_metadata_store = metadata_map_guard.get(&index_name);

    let response_list = vectors_list
        .into_iter()
        .map(|(id, embedding)| {
            let metadata = index_metadata_store
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
pub async fn delete_vector(
    State(state): State<AppState>,
    Path((index_name, vector_id)): Path<(String, VectorId)>,
) -> ServerResult<impl IntoResponse> {
    info!(index_name = %index_name, vector_id = %vector_id, "Received request to delete vector (soft delete)");

    let index_lock_arc: Arc<RwLock<HnswIndex>> = { // Scope for HashMap read lock
        let indices_map = state.indices.read().await;
        indices_map
            .get(&index_name)
            .cloned()
            .ok_or_else(|| ServerError::IndexNotFound(index_name.clone()))?
    };

    // Acquire write lock on the specific index's RwLock
    let mut index_guard = index_lock_arc.write().await;

    // Call the mutable method
    let deleted = index_guard.delete_vector(&vector_id).await?;

    if deleted {
        info!(index_name=%index_name, vector_id=%vector_id, "Vector marked as deleted in core index");
        // Also remove metadata
        let mut metadata_map_guard = state.metadata_store.write().await;
        if let Some(index_metadata_store) = metadata_map_guard.get_mut(&index_name) {
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
pub async fn batch_add_vectors(
    State(state): State<AppState>,
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

    let index_lock_arc: Arc<RwLock<HnswIndex>> = {
        let indices_map = state.indices.read().await;
        indices_map
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
    let mut index_guard = index_lock_arc.write().await; // Lock index for the whole batch
    let mut metadata_map_guard = state.metadata_store.write().await; // Lock metadata store for the whole batch
    
    let index_metadata_store = metadata_map_guard
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
                    index_metadata_store.insert(item.id.clone(), metadata_value);
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
