use crate::config::HnswConfig;
// Removed unused imports: calculate_distance, Distance
use crate::distance::DistanceMetric;
use crate::error::{VortexError, VortexResult};
// Removed unused imports: Neighbor, heap_score
use crate::hnsw::{self, ArcNode, Node, original_score}; // Removed unused Neighbor import
use crate::vector::{Embedding, VectorId};
use crate::utils::{create_rng, generate_random_level};
use crate::distance::calculate_distance; // Added for pruning distance calculation

use async_trait::async_trait;
use ndarray::ArrayView1;
use rand::rngs::StdRng;
use serde::{Serialize, Deserialize};
// Removed unused import: BinaryHeap
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tracing::{debug, trace, warn, info, error};


/// The primary trait defining the vector index functionality.
#[async_trait]
pub trait Index: Send + Sync + std::fmt::Debug { // Added Debug requirement
    /// Adds or updates a vector in the index.
    /// Requires mutable access, posing challenges with Arc<dyn Index>.
    /// Implementations might need internal locking or state management redesign.
    async fn add_vector(&mut self, id: VectorId, vector: Embedding) -> VortexResult<bool>;

    /// Searches for the k nearest neighbors to the query vector.
    async fn search(&self, query: Embedding, k: usize) -> VortexResult<Vec<(VectorId, f32)>>;

    /// Saves the index state to a writer using binary encoding.
    // Added + Send bound to Write for async trait compatibility
    async fn save(&self, writer: &mut (dyn Write + Send)) -> VortexResult<()>;

    /// Deletes a vector from the index (marks as deleted).
    /// Requires mutable access.
    /// Returns true if deleted, false if not found.
    async fn delete_vector(&mut self, id: &VectorId) -> VortexResult<bool>;

    /// Retrieves a vector by its ID.
    async fn get_vector(&self, id: &VectorId) -> VortexResult<Option<Embedding>>;

    /// Returns the number of non-deleted vectors in the index.
    fn len(&self) -> usize;

    /// Returns true if the index has no non-deleted vectors.
    fn is_empty(&self) -> bool;

    /// Returns the dimensionality of vectors in the index.
    fn dimensions(&self) -> usize;

     /// Returns the configuration of the index.
    fn config(&self) -> HnswConfig;

    /// Returns the distance metric used by the index.
    fn distance_metric(&self) -> DistanceMetric;
}

/// Data structure representing the HNSW index state for serialization.
#[derive(Serialize, Deserialize)]
struct HnswIndexData {
    config: HnswConfig,
    metric: DistanceMetric,
    dimensions: usize,
    nodes: Vec<Node>, // Store Node directly for serialization
    vector_map: HashMap<VectorId, usize>,
    entry_point: Option<usize>,
    current_max_level: usize,
    deleted_count: usize,
}


/// Implementation of the `Index` trait using the HNSW algorithm.
#[derive(Debug)] // Basic Debug trait
pub struct HnswIndex {
    config: HnswConfig,
    metric: DistanceMetric,
    dimensions: usize,
    nodes: Vec<ArcNode>, // Main storage for all nodes
    vector_map: HashMap<VectorId, usize>, // Maps VectorId to index in `nodes` Vec
    entry_point: Option<usize>, // Index of the entry point node
    current_max_level: usize, // Highest level currently in the graph
    rng: StdRng, // RNG for level generation
    deleted_count: usize, // Count of marked-deleted nodes
}

// Manual implementation of Serialize/Deserialize via HnswIndexData intermediate
// Required because StdRng doesn't implement Serialize/Deserialize
impl Serialize for HnswIndex {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let data = HnswIndexData {
            config: self.config,
            metric: self.metric,
            dimensions: self.dimensions,
            // Clone the inner Node data from each Arc for serialization
            nodes: self.nodes.iter().map(|arc_node| (**arc_node).clone()).collect(),
            vector_map: self.vector_map.clone(),
            entry_point: self.entry_point,
            current_max_level: self.current_max_level,
            deleted_count: self.deleted_count,
        };
        data.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for HnswIndex {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let data = HnswIndexData::deserialize(deserializer)?;
        Ok(HnswIndex {
            config: data.config,
            metric: data.metric,
            dimensions: data.dimensions,
            // Wrap each loaded Node back into an Arc
            nodes: data.nodes.into_iter().map(Arc::new).collect(),
            vector_map: data.vector_map,
            entry_point: data.entry_point,
            current_max_level: data.current_max_level,
            rng: create_rng(data.config.seed), // Recreate RNG based on seed in config
            deleted_count: data.deleted_count,
        })
    }
}


impl HnswIndex {
    /// Creates a new, empty HNSW index.
    pub fn new(config: HnswConfig, metric: DistanceMetric, dimensions: usize) -> VortexResult<Self> {
        config.validate()?;
        if dimensions == 0 {
            return Err(VortexError::Configuration("Dimensions must be greater than 0".to_string()));
        }
        info!(m=config.m, ef_construction=config.ef_construction, metric=?metric, dimensions, "Creating new HNSW index");
        Ok(HnswIndex {
            config,
            metric,
            dimensions,
            nodes: Vec::new(),
            vector_map: HashMap::new(),
            entry_point: None,
            current_max_level: 0,
            rng: create_rng(config.seed),
            deleted_count: 0,
        })
    }

     /// Loads an index from a reader.
    pub fn load(reader: &mut dyn Read, expected_dimensions: usize) -> VortexResult<Self> {
        info!("Loading HNSW index from reader");
        // Wrap reader if it's not already buffered? Bincode might handle buffering.
        let index: HnswIndex = bincode::deserialize_from(reader)
            .map_err(|e| VortexError::Deserialization(format!("Failed to deserialize index: {}", e)))?;

        if index.dimensions != expected_dimensions {
            warn!(loaded_dims=index.dimensions, expected_dims=expected_dimensions, "Loaded index dimension mismatch");
            // Return error on dimension mismatch during load
            return Err(VortexError::DimensionMismatch { expected: expected_dimensions, actual: index.dimensions });
        }
        info!(vector_count=index.len(), "Index loaded successfully");
        Ok(index)
    }

    /// Loads an index from a file path. Convenience wrapper around `load`.
    pub fn load_from_path(path: &Path, expected_dimensions: usize) -> VortexResult<Self> {
        info!(path=?path, "Loading HNSW index from path");
         let file = File::open(path).map_err(|e| VortexError::IoError { path: path.to_path_buf(), source: e })?;
         let mut reader = BufReader::new(file);
         Self::load(&mut reader, expected_dimensions)
    }


    // Internal helper to get node view safely
    fn get_node(&self, index: usize) -> Option<&ArcNode> {
        self.nodes.get(index)
    }

     // Internal helper for search starting from entry point
    fn search_internal(&self, query: ArrayView1<f32>, k: usize, ef_search: usize) -> VortexResult<Vec<(VectorId, f32)>> {
        let current_entry_point = match self.entry_point {
            Some(ep_idx) => {
                // Handle case where entry point might have been deleted
                if let Some(ep_node) = self.get_node(ep_idx) {
                    if ep_node.deleted {
                        // TODO: Implement finding a new valid entry point if the main one is deleted.
                        // For now, return error or empty. Let's return empty.
                        warn!("Search entry point node {} is marked deleted.", ep_idx);
                        return Ok(Vec::new());
                    }
                    ep_idx
                } else {
                     error!("Entry point index {} out of bounds!", ep_idx);
                     return Err(VortexError::Internal("Entry point index invalid".to_string()));
                }
            }
            None => return Ok(Vec::new()), // Empty index
        };


        let mut current_best_neighbor_idx = current_entry_point;
        let top_level = self.current_max_level;

        // Phase 1: Find entry point for layer 0 by traversing down from top layer
        for level in (1..=top_level).rev() {
            trace!(level, start_node=current_best_neighbor_idx, "Searching level (top-down)");
            // Need the single best neighbor to proceed down. search_layer returns ef best.
            let mut candidates = hnsw::search_layer(
                query,
                current_best_neighbor_idx,
                1, // Only need the single best neighbor (ef=1)
                level,
                &self.nodes,
                self.metric,
            )?; // Max-heap (best score first)

            // Get the best candidate from this layer to start the next layer's search
            if let Some(best_candidate) = candidates.pop() { // Pop the best one
                 current_best_neighbor_idx = best_candidate.index;
            } else {
                // If search_layer returns empty (e.g., start node deleted, no valid neighbors)
                // We should ideally handle this better, maybe search from global EP again?
                // For now, stick with the last known good index.
                warn!(level, current_best_neighbor_idx, "Search layer returned no candidates during top-down traversal. Continuing with previous best.");
                // No change needed to current_best_neighbor_idx
            }
        }

        // Phase 2: Perform detailed search on layer 0
        trace!(start_node=current_best_neighbor_idx, ef_search, "Searching layer 0");
        let mut layer0_results_heap = hnsw::search_layer(
            query,
            current_best_neighbor_idx,
            ef_search, // Use ef_search for layer 0
            0,
            &self.nodes,
            self.metric,
        )?; // Returns Max-heap (best score first when popped)

        // Collect top K results, filtering out deleted nodes
        let mut final_results = Vec::with_capacity(k);
        while let Some(neighbor) = layer0_results_heap.pop() { // Pop best results
             if let Some(node) = self.get_node(neighbor.index) {
                 if !node.deleted {
                    let original_score = original_score(self.metric, neighbor.distance);
                    final_results.push((node.id.clone(), original_score));
                    if final_results.len() == k {
                        break;
                    }
                 }
             }
        }

        // Results are already ordered best-first due to popping from max-heap.
        Ok(final_results)
    }

     // Internal helper for insertion logic
    fn insert_vector(&mut self, id: VectorId, vector: Embedding) -> VortexResult<()> {
        let node_level = generate_random_level(self.config.ml, &mut self.rng);
        let new_node_index = self.nodes.len();
        // Removed unnecessary `mut`
        let new_node = Node::new(id.clone(), vector.clone(), node_level); // Clone vector for node storage

        trace!(vector_id=%id, node_index=new_node_index, level=node_level, "Inserting new vector");

        // --- Handle Empty Index ---
        if self.entry_point.is_none() {
            self.nodes.push(Arc::new(new_node));
            self.vector_map.insert(id, new_node_index);
            self.entry_point = Some(new_node_index);
            self.current_max_level = node_level;
            debug!(node_index=new_node_index, level=node_level, "Set new node as entry point");
            return Ok(());
        }

        // --- Find Entry Points for Each Layer ---
        let mut current_ep_candidate_idx = self.find_valid_entry_point()?;
        let graph_top_level = self.current_max_level;
        
        // Determine the highest level to search downwards from.
        // This is either the graph's current top level or the new node's level if it's higher.
        let search_from_level = std::cmp::max(node_level, graph_top_level);

        // entry_points will store the best entry point found for levels <= search_from_level
        // We only need to store entry points down to level 0 for the new node's connections.
        // The size should be max(node_level, graph_top_level) + 1 to handle all relevant levels.
        let mut entry_points = vec![current_ep_candidate_idx; search_from_level + 1];

        // Search downwards from search_from_level to (node_level + 1)
        // This populates entry_points for layers above the new node's own layers,
        // and finds the starting point for searching at new_node.level.
        for current_search_level in ((node_level + 1)..=search_from_level).rev() {
            trace!(level = current_search_level, start_node = current_ep_candidate_idx, "Finding entry point for level below (insertion)");
            let mut candidates = hnsw::search_layer(
                vector.view(),
                current_ep_candidate_idx,
                1, // Only need 1 candidate
                current_search_level,
                &self.nodes,
                self.metric,
            )?;
            if let Some(best_candidate) = candidates.pop() {
                current_ep_candidate_idx = best_candidate.index;
            } else {
                warn!(level = current_search_level, current_ep_candidate_idx, "Search layer returned no candidates during insertion downward traversal. Using previous best.");
            }
            entry_points[current_search_level] = current_ep_candidate_idx;
        }
        
        // For levels from node_level down to 0, the entry point is the `current_ep_candidate_idx`
        // found by the loop above (or the initial one if the loop didn't run, e.g., if node_level >= search_from_level).
        // This `current_ep_candidate_idx` is the starting point for searching at `node_level`.
        for l in 0..=node_level {
            if l < entry_points.len() { // Ensure we don't write out of bounds if node_level was higher than graph_top_level
                 entry_points[l] = current_ep_candidate_idx;
            } else {
                // This case should ideally not be hit if entry_points is sized to search_from_level + 1
                // and search_from_level is max(node_level, graph_top_level).
                // If node_level is very high, entry_points needs to be large enough.
                // Let's ensure entry_points is always large enough for 0..=node_level.
                // The current sizing `search_from_level + 1` is correct.
                // This branch is more of a safeguard / for reasoning.
            }
        }
        // If node_level is 0 and graph_top_level is also 0, the loop above doesn't run.
        // current_ep_candidate_idx is the initial valid entry point. entry_points[0] gets this. Correct.

        // --- Connect New Node ---
        // Phase 2: Connect the new node in layers from its own level (node_level) down to 0
        for level in (0..=node_level).rev() { // Iterate using node_level (which is new_node.level)
            let ep_index = entry_points[level]; 
            trace!(level, start_node=ep_index, ef_construction=self.config.ef_construction, "Searching neighbors for connection");

            // Find ef_construction nearest neighbors at this level
            // Removed unnecessary `mut`
            let neighbors_heap = hnsw::search_layer(
                vector.view(),
                ep_index,
                self.config.ef_construction,
                level,
                &self.nodes,
                self.metric,
            )?; // Max-heap (best score first when popped)

            // Select M neighbors using the heuristic
            let m = if level == 0 { self.config.m_max0 } else { self.config.m };
            let selected_neighbor_indices = hnsw::select_neighbors_heuristic(&neighbors_heap, m);
            trace!(level, count=selected_neighbor_indices.len(), "Selected neighbors");

            // --- Connect New Node to Neighbors ---
            // Lock and assign connections for the new node
            *new_node.connections[level].write() = selected_neighbor_indices.clone();

            // --- Connect Neighbors to New Node & Prune ---
            let m_limit = if level == 0 { self.config.m_max0 } else { self.config.m };

            for &neighbor_idx in &selected_neighbor_indices {
                // Ensure neighbor index is valid and get the ArcNode
                if let Some(neighbor_node) = self.nodes.get(neighbor_idx).cloned() { // Clone Arc to avoid borrowing self
                    // Ensure the neighbor node has connections initialized up to this level
                    if level < neighbor_node.connections.len() {
                        // Acquire write lock on the neighbor's connection list for this level
                        let mut neighbor_connections = neighbor_node.connections[level].write();

                        // Add the new node's index to the neighbor's list
                        neighbor_connections.push(new_node_index);
                        trace!(level, from=%id, to=neighbor_idx, "Added back-connection");

                        // --- Pruning Logic ---
                        if neighbor_connections.len() > m_limit {
                            trace!(level, node=neighbor_idx, count=neighbor_connections.len(), limit=m_limit, "Pruning neighbor connections");
                            // Collect distances from the neighbor to its current connections
                            let mut candidates_with_dist: Vec<(f32, usize)> = Vec::with_capacity(neighbor_connections.len());
                            for &conn_idx in neighbor_connections.iter() {
                                if let Some(connected_node) = self.nodes.get(conn_idx) {
                                    // Calculate distance (use raw distance, not heap score)
                                    let dist = calculate_distance(self.metric, neighbor_node.vector.view(), connected_node.vector.view())?;
                                    candidates_with_dist.push((dist, conn_idx));
                                } else {
                                     warn!(level, neighbor=neighbor_idx, missing_conn=conn_idx, "Could not find node for connection during pruning");
                                }
                            }

                            // Sort candidates: Ascending for L2 (closer is better), Descending for Cosine (higher is better)
                            match self.metric {
                                DistanceMetric::L2 => candidates_with_dist.sort_unstable_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal)),
                                DistanceMetric::Cosine => candidates_with_dist.sort_unstable_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal)), // Note: b.cmp(a) for descending
                            }

                            // Keep only the best M (or M_max0) neighbors
                            candidates_with_dist.truncate(m_limit);

                            // Update the neighbor's connection list
                            neighbor_connections.clear();
                            neighbor_connections.extend(candidates_with_dist.into_iter().map(|(_, idx)| idx));
                            trace!(level, node=neighbor_idx, count=neighbor_connections.len(), "Pruning complete");
                        }
                        // Write lock is released automatically when `neighbor_connections` goes out of scope
                    } else {
                         warn!(level, neighbor=neighbor_idx, neighbor_level=neighbor_node.level, "Neighbor node level too low for back-connection at current level");
                    }
                } else {
                     warn!(level, neighbor=neighbor_idx, "Could not find neighbor node during back-connection");
                }
            }
        }

        // Add the fully prepared new node to the index *after* potential neighbors have been updated
        // This ensures neighbor lists reference a valid index for the new node.
        let node_arc = Arc::new(new_node);
        self.nodes.push(node_arc);
        self.vector_map.insert(id, new_node_index);


        // Update the global entry point if the new node's level is higher
        if node_level > graph_top_level { // Corrected variable name
            self.current_max_level = node_level;
            self.entry_point = Some(new_node_index);
            debug!(node_index=new_node_index, level=node_level, "Updated global entry point");
        }

        Ok(())
    }

    // Helper to find a non-deleted entry point, starting from the current one.
    // Returns an error if no valid entry point can be found.
    fn find_valid_entry_point(&self) -> VortexResult<usize> {
        match self.entry_point {
            None => Err(VortexError::EmptyIndex), // Should be checked before calling
            Some(ep_idx) => {
                if let Some(node) = self.get_node(ep_idx) {
                    if !node.deleted {
                        return Ok(ep_idx);
                    }
                }
                // Current entry point is deleted or invalid, try searching from neighbors?
                // TODO: Implement robust search for a valid entry point.
                // For now, if the main EP is deleted, we might fail insertion/search.
                warn!("Current entry point {} is deleted or invalid. Trying fallback (not implemented).", ep_idx);
                 // Fallback: iterate through nodes to find *any* non-deleted one? Inefficient.
                self.nodes.iter().position(|n| !n.deleted).ok_or_else(|| {
                    error!("No valid (non-deleted) nodes found in the index.");
                    VortexError::Internal("No valid entry point found".to_string())
                })
            }
        }
    }

}


#[async_trait]
impl Index for HnswIndex {
    async fn add_vector(&mut self, id: VectorId, vector: Embedding) -> VortexResult<bool> {
        if vector.len() != self.dimensions {
            return Err(VortexError::DimensionMismatch { expected: self.dimensions, actual: vector.len() });
        }

        if let Some(&existing_index) = self.vector_map.get(&id) {
            // --- Update existing vector ---
            // **BASELINE SIMPLIFICATION:** Overwrite the node with a new one at the same index.
            // This breaks Arc sharing and doesn't handle graph updates well. Needs internal locking/refactor.
            warn!(vector_id=%id, index=existing_index, "Updating vector (simplified: replacing node)");

            // Ensure the existing node exists before trying to access it
            let old_node = self.nodes.get(existing_index).ok_or_else(|| VortexError::Internal(format!("Vector map points to invalid index {}", existing_index)))?.clone(); // Clone Arc

            // Create a new Node with updated vector but preserve level
            let mut updated_node_data = Node::new(id.clone(), vector, old_node.level);
            // An update should make the node active, so set deleted to false.
            updated_node_data.deleted = false;

            // Manually clone connections using the logic from Node::clone
            updated_node_data.connections = old_node.connections.iter().map(|lock| parking_lot::RwLock::new(lock.read().clone())).collect();

            // Replace the Arc in the main list
            self.nodes[existing_index] = Arc::new(updated_node_data);

            // If the node was previously deleted, decrement count as it's now "active" again
            if old_node.deleted {
                 if self.deleted_count > 0 { self.deleted_count -= 1; }
                 debug!(vector_id=%id, "Reactivated previously deleted node during update.");
            }

            Ok(false) // Indicate update
        } else {
            // --- Add new vector ---
            self.insert_vector(id, vector)?;
            Ok(true) // Indicate addition
        }
    }

    async fn search(&self, query: Embedding, k: usize) -> VortexResult<Vec<(VectorId, f32)>> {
        if self.is_empty() {
            return Ok(Vec::new());
        }
        if query.len() != self.dimensions {
            return Err(VortexError::DimensionMismatch { expected: self.dimensions, actual: query.len() });
        }
        if k == 0 {
            return Ok(Vec::new());
        }

        let ef_search = std::cmp::max(k, self.config.ef_search); // Ensure ef_search is at least k
        debug!(k, ef_search, "Performing search");

        // Wrap blocking search logic. Use spawn_blocking for true async.
        // tokio::task::spawn_blocking(move || self.search_internal(...)).await?
        self.search_internal(query.view(), k, ef_search)
    }

    // Added + Send bound to Write for async trait compatibility
    async fn save(&self, writer: &mut (dyn Write + Send)) -> VortexResult<()> {
        info!(vector_count=self.len(), "Saving HNSW index to writer");
        // Wrap in BufWriter for efficiency
        let mut buf_writer = BufWriter::new(writer);
        bincode::serialize_into(&mut buf_writer, self)
            .map_err(|e| VortexError::Serialization(format!("Failed to serialize index: {}", e)))?;
        buf_writer.flush().map_err(|e| VortexError::IoError { path: PathBuf::from("<writer>"), source: e })?; // Ensure buffer is flushed
        info!("Index saved successfully");
        Ok(())
    }

    async fn delete_vector(&mut self, id: &VectorId) -> VortexResult<bool> {
        if let Some(&index) = self.vector_map.get(id) {
             // **BASELINE SIMPLIFICATION:** Mark the node as deleted. Requires mutable access.
             // Using clone-and-replace approach due to Arc limitations without internal locking.

             // Ensure index is valid before proceeding
             if index >= self.nodes.len() {
                  error!(vector_id=%id, index, "Vector map contains invalid index");
                  return Err(VortexError::Internal("Invalid index found in vector map".to_string()));
             }

             let node_arc = self.nodes[index].clone(); // Clone the Arc

             // Check if already deleted before potentially expensive clone-and-modify
             if node_arc.deleted {
                 return Ok(false); // Already deleted
             }

             // Create a modified copy of the Node data
             let mut modified_node_data = (*node_arc).clone(); // Clone the Node data itself
             modified_node_data.deleted = true;

             // Replace the Arc in the vector with a new Arc pointing to the modified data
             self.nodes[index] = Arc::new(modified_node_data);
             self.deleted_count += 1;
             debug!(vector_id=%id, index, "Marked node as deleted (via clone-and-replace)");
             // Note: The vector_map entry remains. Search will skip, get_vector will find.
             Ok(true)
        } else {
            Ok(false) // Not found
        }
    }

    async fn get_vector(&self, id: &VectorId) -> VortexResult<Option<Embedding>> {
        if let Some(&index) = self.vector_map.get(id) {
            if let Some(node) = self.nodes.get(index) {
                // Return vector even if marked deleted
                Ok(Some(node.vector.clone()))
            } else {
                Err(VortexError::Internal(format!("Vector ID {} found in map but not in node list at index {}", id, index)))
            }
        } else {
            Ok(None) // Not found
        }
    }

    fn len(&self) -> usize {
        self.nodes.len() - self.deleted_count
    }

    fn is_empty(&self) -> bool {
        // Check based on non-deleted count, OR if nodes list itself is empty
        self.nodes.is_empty() || self.len() == 0
    }

    fn dimensions(&self) -> usize {
        self.dimensions
    }

     fn config(&self) -> HnswConfig {
        self.config
    }

    fn distance_metric(&self) -> DistanceMetric {
        self.metric
    }

}

// Static assertion to ensure HnswIndex implements Index, Send and Sync
const _: () = {
    fn assert_impl<T: Index + Send + Sync + std::fmt::Debug>() {}
    fn check() { // Renamed from main to avoid conflict
        assert_impl::<HnswIndex>();
    }
};


#[cfg(test)]
mod tests {
    use super::*;
    use crate::vector::Embedding;
    use tempfile::tempdir;
    use std::io::Cursor; // For testing save/load with memory buffer
    use crate::hnsw::ArcNode; // For accessing node connections in tests

    fn create_test_config() -> HnswConfig {
        HnswConfig { m: 5, m_max0: 10, ef_construction: 20, ef_search: 10, ml: 0.5, seed: Some(123) }
    }

    #[tokio::test]
    async fn test_new_index() {
        let config = create_test_config();
        let index = HnswIndex::new(config, DistanceMetric::L2, 4).unwrap();
        assert_eq!(index.dimensions(), 4);
        assert_eq!(index.len(), 0);
        assert!(index.is_empty());
        assert_eq!(index.distance_metric(), DistanceMetric::L2);
    }

    #[tokio::test]
    async fn test_add_vector() {
        let config = create_test_config();
        let mut index = HnswIndex::new(config, DistanceMetric::L2, 2).unwrap();

        let vec1: Embedding = vec![1.0, 2.0].into();
        let vec2: Embedding = vec![3.0, 4.0].into();

        let added1 = index.add_vector("vec1".to_string(), vec1.clone()).await.unwrap();
        assert!(added1);
        assert_eq!(index.len(), 1);
        assert!(!index.is_empty());
        assert!(index.entry_point.is_some());

        let added2 = index.add_vector("vec2".to_string(), vec2.clone()).await.unwrap();
        assert!(added2);
        assert_eq!(index.len(), 2);

        // Test update
        let vec1_updated: Embedding = vec![1.5, 2.5].into();
        let updated1 = index.add_vector("vec1".to_string(), vec1_updated.clone()).await.unwrap();
        assert!(!updated1); // Should return false for update
        assert_eq!(index.len(), 2); // Length should not change on update

        let retrieved = index.get_vector(&"vec1".to_string()).await.unwrap().unwrap();
        assert_eq!(retrieved, vec1_updated);
    }

     #[tokio::test]
    async fn test_add_dimension_mismatch() {
        let config = create_test_config();
        let mut index = HnswIndex::new(config, DistanceMetric::L2, 2).unwrap();
        let vec3d: Embedding = vec![1.0, 2.0, 3.0].into();
        let result = index.add_vector("vec3d".to_string(), vec3d).await;
        assert!(matches!(result, Err(VortexError::DimensionMismatch { expected: 2, actual: 3 })));
    }

    #[tokio::test]
    async fn test_get_vector() {
        let config = create_test_config();
        let mut index = HnswIndex::new(config, DistanceMetric::L2, 2).unwrap();
        let vec1: Embedding = vec![1.0, 2.0].into();
        index.add_vector("vec1".to_string(), vec1.clone()).await.unwrap();

        let retrieved = index.get_vector(&"vec1".to_string()).await.unwrap();
        assert_eq!(retrieved, Some(vec1));

        let not_found = index.get_vector(&"vec_unknown".to_string()).await.unwrap();
        assert_eq!(not_found, None);
    }

    #[tokio::test]
    async fn test_search_basic_l2() {
        let config = create_test_config();
        let mut index = HnswIndex::new(config, DistanceMetric::L2, 2).unwrap();

        // Add vectors roughly along a line
        index.add_vector("vec0".to_string(), vec![0.0, 0.0].into()).await.unwrap();
        index.add_vector("vec1".to_string(), vec![1.0, 1.0].into()).await.unwrap();
        index.add_vector("vec2".to_string(), vec![2.0, 2.0].into()).await.unwrap();
        index.add_vector("vec10".to_string(), vec![10.0, 10.0].into()).await.unwrap();

        // Search near vec1
        let query: Embedding = vec![1.1, 1.1].into();
        let results = index.search(query, 2).await.unwrap();

        assert_eq!(results.len(), 2);
        // Expect vec1 then vec2 (or vec0 depending on exact distances/graph)
        assert_eq!(results[0].0, "vec1"); // Closest
        assert!(results[0].1 < 1.0); // Distance should be small
        // The second result could be vec0 or vec2, check if it's one of them
        assert!(results[1].0 == "vec0" || results[1].0 == "vec2");
        // Check ordering: distance should be increasing for L2
        assert!(results[1].1 >= results[0].1);
    }

     #[tokio::test]
    async fn test_search_basic_cosine() {
        let config = create_test_config();
        let mut index = HnswIndex::new(config, DistanceMetric::Cosine, 3).unwrap();

        index.add_vector("vecA".to_string(), vec![1.0, 0.0, 0.0].into()).await.unwrap();
        index.add_vector("vecB".to_string(), vec![0.9, 0.1, 0.0].into()).await.unwrap(); // Similar to A
        index.add_vector("vecC".to_string(), vec![0.0, 1.0, 0.0].into()).await.unwrap(); // Orthogonal to A
        index.add_vector("vecD".to_string(), vec![-1.0, 0.0, 0.0].into()).await.unwrap(); // Opposite to A

        // Search near vecA
        let query: Embedding = vec![1.0, 0.01, 0.0].into(); // Close to vecA
        let results = index.search(query, 3).await.unwrap();

        assert_eq!(results.len(), 3);
        // Expect A, then B, then C (or D depending on similarity calc)
        assert_eq!(results[0].0, "vecA"); // Most similar
        assert!(results[0].1 > 0.99);   // Similarity score should be high
        assert_eq!(results[1].0, "vecB");
        assert!(results[1].1 > 0.8 && results[1].1 < results[0].1);
        assert_eq!(results[2].0, "vecC"); // C should be more similar than D (0 vs -1)
        assert!(results[2].1 > -0.1 && results[2].1 < 0.1); // Similarity near 0
         // Check ordering: similarity should be decreasing for Cosine
        assert!(results[1].1 <= results[0].1);
        assert!(results[2].1 <= results[1].1);
    }

    #[tokio::test]
    async fn test_search_empty() {
        let config = create_test_config();
        let index = HnswIndex::new(config, DistanceMetric::L2, 2).unwrap();
        let query: Embedding = vec![1.0, 1.0].into();
        let results = index.search(query, 5).await.unwrap();
        assert!(results.is_empty());
    }

     #[tokio::test]
    async fn test_search_k0() {
        let config = create_test_config();
        let mut index = HnswIndex::new(config, DistanceMetric::L2, 2).unwrap();
        index.add_vector("vec0".to_string(), vec![0.0, 0.0].into()).await.unwrap();
        let query: Embedding = vec![1.0, 1.0].into();
        let results = index.search(query, 0).await.unwrap();
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn test_save_load_path() {
        let config = create_test_config();
        let mut index = HnswIndex::new(config, DistanceMetric::Cosine, 3).unwrap();
        let dir = tempdir().unwrap();
        let path = dir.path().join("test_index_path.bin");

        index.add_vector("vecA".to_string(), vec![1.0, 0.0, 0.0].into()).await.unwrap();
        index.add_vector("vecB".to_string(), vec![0.0, 1.0, 0.0].into()).await.unwrap();

        // Save using file path directly
        let file = File::create(&path).unwrap();
        let mut writer = BufWriter::new(file);
        index.save(&mut writer).await.unwrap();
        drop(writer); // Ensure writer is flushed and file closed

        // Load using convenience function
        let loaded_index = HnswIndex::load_from_path(&path, 3).unwrap();

        assert_eq!(loaded_index.dimensions(), 3);
        assert_eq!(loaded_index.len(), 2);
        assert_eq!(loaded_index.config(), config);
        assert_eq!(loaded_index.distance_metric(), DistanceMetric::Cosine);

        // Verify data integrity by searching
        let query: Embedding = vec![0.9, 0.1, 0.0].into();
        let results = loaded_index.search(query, 1).await.unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, "vecA");
    }

     #[tokio::test]
    async fn test_save_load_reader_writer() {
        let config = create_test_config();
        let mut index = HnswIndex::new(config, DistanceMetric::Cosine, 3).unwrap();
        index.add_vector("vecA".to_string(), vec![1.0, 0.0, 0.0].into()).await.unwrap();
        index.add_vector("vecB".to_string(), vec![0.0, 1.0, 0.0].into()).await.unwrap();

        let mut buffer: Vec<u8> = Vec::new();
        index.save(&mut buffer).await.unwrap(); // Save to memory buffer

        assert!(!buffer.is_empty());

        let mut reader = Cursor::new(buffer); // Create reader from buffer
        let loaded_index = HnswIndex::load(&mut reader, 3).unwrap(); // Load from reader

        assert_eq!(loaded_index.dimensions(), 3);
        assert_eq!(loaded_index.len(), 2);

        let query: Embedding = vec![0.1, 0.9, 0.0].into();
        let results = loaded_index.search(query, 1).await.unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, "vecB");
    }

     #[tokio::test]
    async fn test_load_dimension_mismatch() {
        let config = create_test_config();
        let mut index = HnswIndex::new(config, DistanceMetric::L2, 3).unwrap();
        index.add_vector("vecA".to_string(), vec![1.0, 0.0, 0.0].into()).await.unwrap();

        let mut buffer: Vec<u8> = Vec::new();
        index.save(&mut buffer).await.unwrap();

        let mut reader = Cursor::new(buffer);
        let result = HnswIndex::load(&mut reader, 2); // Expect 2 dimensions

        assert!(matches!(result, Err(VortexError::DimensionMismatch { expected: 2, actual: 3 })));
    }

    // Helper to get connections for a node at a specific level for testing
    fn get_node_connections(node: &ArcNode, level: usize) -> Vec<usize> {
        if level < node.connections.len() {
            node.connections[level].read().clone()
        } else {
            Vec::new()
        }
    }

    #[tokio::test]
    async fn test_bidirectional_connections_and_pruning() {
        // Use a small M for easier pruning checks
        let config = HnswConfig { m: 2, m_max0: 4, ef_construction: 10, ef_search: 10, ml: 0.5, seed: Some(42) };
        let mut index = HnswIndex::new(config, DistanceMetric::L2, 2).unwrap();

        // Add initial vector
        index.add_vector("vec0".to_string(), vec![0.0, 0.0].into()).await.unwrap();
        let node0_idx = *index.vector_map.get("vec0").unwrap();

        // Add a few vectors that should connect to vec0
        index.add_vector("vec1".to_string(), vec![0.1, 0.1].into()).await.unwrap();
        let node1_idx = *index.vector_map.get("vec1").unwrap();

        index.add_vector("vec2".to_string(), vec![0.2, 0.2].into()).await.unwrap();
        let _node2_idx = *index.vector_map.get("vec2").unwrap(); // Prefixed as unused for now

        // Check bi-directional (assuming they connect on layer 0)
        // This is a basic check; actual connections depend on HNSW logic and levels
        // We need to find which layer they connected on. For simplicity, let's assume layer 0 for now.
        // A more robust test would inspect all layers of the involved nodes.

        let node0_level0_conns = get_node_connections(&index.nodes[node0_idx], 0);
        let node1_level0_conns = get_node_connections(&index.nodes[node1_idx], 0);

        if node0_level0_conns.contains(&node1_idx) {
            assert!(node1_level0_conns.contains(&node0_idx), "vec1 should connect back to vec0 if vec0 connects to vec1 on layer 0");
        }
        if node1_level0_conns.contains(&node0_idx) {
            assert!(node0_level0_conns.contains(&node1_idx), "vec0 should connect back to vec1 if vec1 connects to vec0 on layer 0");
        }


        // Add more vectors to trigger pruning on vec0 (m_max0 = 4 for layer 0)
        index.add_vector("vec3".to_string(), vec![0.3, 0.0].into()).await.unwrap();
        index.add_vector("vec4".to_string(), vec![0.0, 0.4].into()).await.unwrap();
        index.add_vector("vec5".to_string(), vec![-0.1, 0.0].into()).await.unwrap(); // Should connect and potentially prune

        // After adding vec5, vec0's layer 0 connections should be pruned to m_max0 (4)
        let node0_level0_conns_after_pruning = get_node_connections(&index.nodes[node0_idx], 0);
        assert!(node0_level0_conns_after_pruning.len() <= config.m_max0,
                "vec0 layer 0 connections ({}) should be pruned to at most M_max0 ({})",
                node0_level0_conns_after_pruning.len(), config.m_max0);

        // Verify that the new node (vec5) also has back-connections and its connections are pruned if necessary.
        let node5_idx = *index.vector_map.get("vec5").unwrap();
        let node5_level0_conns = get_node_connections(&index.nodes[node5_idx], 0);
        assert!(node5_level0_conns.len() <= config.m_max0, "vec5 layer 0 connections should be at most M_max0");

        // Check a higher level connection if any node has one (e.g. node0 if it's the entry point)
        if index.nodes[node0_idx].level > 0 {
            let node0_level1_conns = get_node_connections(&index.nodes[node0_idx], 1);
             assert!(node0_level1_conns.len() <= config.m,
                "vec0 layer 1 connections ({}) should be pruned to at most M ({})",
                node0_level1_conns.len(), config.m);
        }
    }


     #[tokio::test]
    async fn test_delete_vector() {
        let config = create_test_config();
        let mut index = HnswIndex::new(config, DistanceMetric::L2, 2).unwrap();

        index.add_vector("vec1".to_string(), vec![1.0, 1.0].into()).await.unwrap();
        index.add_vector("vec2".to_string(), vec![2.0, 2.0].into()).await.unwrap();
        index.add_vector("vec3".to_string(), vec![3.0, 3.0].into()).await.unwrap();

        assert_eq!(index.len(), 3);
        assert_eq!(index.deleted_count, 0);

        // Delete vec2
        let deleted = index.delete_vector(&"vec2".to_string()).await.unwrap();
        assert!(deleted);
        assert_eq!(index.len(), 2); // Active count decreases
        assert_eq!(index.deleted_count, 1);
        assert_eq!(index.nodes.len(), 3); // Node list size remains same

        // Try deleting again
        let deleted_again = index.delete_vector(&"vec2".to_string()).await.unwrap();
        assert!(!deleted_again); // Already deleted
        assert_eq!(index.len(), 2);
        assert_eq!(index.deleted_count, 1);

        // Delete non-existent
        let deleted_non_existent = index.delete_vector(&"vec4".to_string()).await.unwrap();
        assert!(!deleted_non_existent);
        assert_eq!(index.len(), 2);
        assert_eq!(index.deleted_count, 1);

        // Verify get_vector still finds it but search skips it
        let retrieved_deleted = index.get_vector(&"vec2".to_string()).await.unwrap();
        assert!(retrieved_deleted.is_some()); // Found the vector data

        // Search near vec2 - should return vec1 and vec3
        let query: Embedding = vec![2.1, 2.1].into();
        let results = index.search(query.clone(), 3).await.unwrap(); // Ask for 3, clone query

        assert_eq!(results.len(), 2); // Only vec1 and vec3 should be found
        assert!(results.iter().any(|(id, _)| id == "vec1"));
        assert!(results.iter().any(|(id, _)| id == "vec3"));
        assert!(!results.iter().any(|(id, _)| id == "vec2")); // vec2 should not be in results

         // Check node deleted flag (requires finding the index)
        let vec2_node_idx = index.vector_map.get("vec2").unwrap();
        assert!(index.nodes[*vec2_node_idx].deleted);

        // Test update after delete
        let updated_deleted = index.add_vector("vec2".to_string(), vec![2.5, 2.5].into()).await.unwrap();
        assert!(!updated_deleted); // Update returns false
        assert_eq!(index.len(), 3); // Length increases back
        assert_eq!(index.deleted_count, 0); // Deleted count decreases
        let vec2_node_idx = index.vector_map.get("vec2").unwrap();
        assert!(!index.nodes[*vec2_node_idx].deleted); // Node should be marked active again

        // Search again, should find vec2 now
         let results_after_update = index.search(query, 3).await.unwrap();
         assert_eq!(results_after_update.len(), 3);
         assert!(results_after_update.iter().any(|(id, _)| id == "vec2"));

    }
}
