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
}

/// Request body for searching vectors.
#[derive(Deserialize)]
pub struct SearchRequest {
    pub query_vector: Vec<f32>, // Accept standard Vec<f32>
    pub k: usize,
}

// --- Response Bodies ---

/// Represents a single search result item.
#[derive(Serialize)]
pub struct SearchResultItem {
    pub id: VectorId,
    pub score: f32, // Distance or similarity score
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
