//! Defines the data structures used for API request and response bodies.

use serde::{Deserialize, Serialize};
use vortex_core::{DistanceMetric, HnswConfig, VectorId};


// --- Request Bodies ---

/// Request body for creating a new index.
#[derive(Deserialize)]
pub struct CreateIndexRequest {
    pub name: String,
    pub config: HnswConfig, // Reuse core config struct
    pub metric: DistanceMetric, // Reuse core enum
    pub dimensions: usize,
}

/// Request body for adding or updating a single vector.
#[derive(Deserialize)]
pub struct AddVectorRequest {
    pub id: VectorId,
    pub vector: Vec<f32>, // Accept standard Vec<f32> for JSON ease
    pub metadata: Option<serde_json::Value>,
}

/// Request body for searching vectors.
#[derive(Deserialize, Clone, Debug)]
pub struct SearchRequest {
    pub query_vector: Vec<f32>, // Accept standard Vec<f32>
    pub k: usize,
    pub filter: Option<serde_json::Value>, // Optional filter based on metadata
}

/// Item for batch vector addition.
#[derive(Deserialize, Debug)]
pub struct AddVectorRequestItem {
    pub id: VectorId,
    pub vector: Vec<f32>,
    pub metadata: Option<serde_json::Value>,
}

/// Request body for batch adding vectors.
#[derive(Deserialize, Debug)]
pub struct BatchAddVectorRequest {
    pub vectors: Vec<AddVectorRequestItem>,
}


// --- Response Bodies ---

/// Represents a single search result item.
#[derive(Serialize)]
pub struct SearchResultItem {
    pub id: VectorId,
    pub score: f32, // Distance or similarity score
    pub metadata: Option<serde_json::Value>,
}

/// Response body for search results.
#[derive(Serialize)]
pub struct SearchResponse {
    pub results: Vec<SearchResultItem>,
}

/// Response body for retrieving a single vector.
#[derive(Serialize)]
pub struct VectorResponse {
    pub id: VectorId,
    pub vector: Vec<f32>, // Return standard Vec<f32>
    pub metadata: Option<serde_json::Value>,
}

/// Response body for index statistics.
#[derive(Serialize)]
pub struct StatsResponse {
    pub vector_count: usize,
    pub dimensions: usize,
    pub config: HnswConfig,
    pub metric: DistanceMetric,
    // Add other stats like memory usage later
}

// --- Generic Responses ---

/// Generic success response (e.g., for creation).
#[derive(Serialize)]
pub struct SuccessResponse {
    pub message: String,
}

/// Response body for batch operations.
#[derive(Serialize, Debug)]
pub struct BatchOperationResponse {
    pub success_count: usize,
    pub failure_count: usize,
    pub message: String,
    // Optional: Consider adding a field for detailed errors per item if needed.
    // pub errors: Vec<String>, 
}
