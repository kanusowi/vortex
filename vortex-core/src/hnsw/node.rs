use crate::vector::{VectorId, Embedding};
use parking_lot::RwLock; // Added for interior mutability
use serde::{Serialize, Deserialize};
use std::sync::Arc;

/// Represents a single node (vector) in the HNSW graph.
// Removed Clone from derive, will implement manually below
#[derive(Debug, Serialize, Deserialize)]
pub struct Node {
    /// The unique identifier for the vector associated with this node.
    pub id: VectorId,
    /// The vector embedding. Stored directly in the node for now.
    /// Consider Arc<Embedding> if embeddings are very large and shared, but adds indirection.
    pub vector: Embedding,
    /// List of neighbor connections for each layer, wrapped in RwLock for interior mutability.
    /// `connections[level]` contains a list of indices pointing to neighbor nodes in the main `HnswIndex::nodes` vector.
    pub connections: Vec<RwLock<Vec<usize>>>,
    /// The highest layer this node exists in.
    pub level: usize,
    /// Flag to indicate if a node is conceptually deleted (soft delete).
    /// Graph links are NOT removed in this basic implementation.
    #[serde(default)] // Ensure compatibility with older formats lacking this field
    pub deleted: bool,
}

impl Node {
    /// Creates a new node. Connections are initially empty and sized according to the node's level.
    pub fn new(id: VectorId, vector: Embedding, level: usize) -> Self {
        Node {
            id,
            vector,
            // Initialize connection lists wrapped in RwLock for each layer up to the node's level
            connections: (0..=level).map(|_| RwLock::new(Vec::new())).collect(),
            level,
            deleted: false,
        }
    }
}

// Manual Clone implementation because RwLock is not Clone
impl Clone for Node {
    fn clone(&self) -> Self {
        Node {
            id: self.id.clone(),
            vector: self.vector.clone(),
            // Clone the data within each RwLock into a new RwLock
            connections: self.connections.iter().map(|lock| RwLock::new(lock.read().clone())).collect(),
            level: self.level,
            deleted: self.deleted,
        }
    }
}


/// Type alias for a reference-counted Node.
/// Using Arc allows sharing node data potentially across threads if needed later,
/// and simplifies ownership management when passing nodes around.
pub type ArcNode = Arc<Node>;
