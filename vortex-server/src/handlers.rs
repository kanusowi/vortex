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
    let new_index = HnswIndex::new(payload.config, payload.metric, payload.dimensions)
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
    let added = index_guard.add_vector(payload.id.clone(), embedding).await?;

    let status_code = if added {
        info!(index_name=%index_name, vector_id=%payload.id, "Vector added");
        StatusCode::CREATED
    } else {
        info!(index_name=%index_name, vector_id=%payload.id, "Vector updated");
        StatusCode::OK
    };

    Ok((status_code, Json(SuccessResponse { message: format!("Vector '{}' processed successfully", payload.id) })))
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

    // Perform the search using the Index trait method on the read guard
    let search_results = index_guard.search(embedding, k).await.map_err(ServerError::from)?;
    debug!(index_name=%index_name, count=search_results.len(), "Search completed");

    // Convert core results to API response model
    let response_items = search_results
        .into_iter()
        .map(|(id, score)| SearchResultItem { id, score })
        .collect();

    Ok(Json(SearchResponse {
        results: response_items,
    }))
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
            Ok(Json(VectorResponse {
                id: vector_id,
                vector: embedding.into(), // Convert Embedding back to Vec<f32>
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
    let response_list = vectors_list
        .into_iter()
        .map(|(id, embedding)| VectorResponse {
            id,
            vector: embedding.into(), // Convert Embedding to Vec<f32>
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
    info!(index_name = %index_name, vector_id = %vector_id, "Received request to delete vector");

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
        info!(index_name=%index_name, vector_id=%vector_id, "Vector deleted");
        Ok(StatusCode::NO_CONTENT)
    } else {
        warn!(index_name=%index_name, vector_id=%vector_id, "Vector not found for deletion");
        Err(ServerError::CoreError(VortexError::NotFound(vector_id))) // Return 404
    }
}
