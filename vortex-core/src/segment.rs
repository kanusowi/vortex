use crate::config::HnswConfig;
use crate::distance::DistanceMetric;
use crate::error::VortexError;
use crate::hnsw::{self, SearchResult}; // Import hnsw module for search_layer etc.
use crate::storage::mmap_hnsw_graph_links::MmapHnswGraphLinks;
use crate::storage::mmap_vector_storage::MmapVectorStorage;
use crate::utils::{create_rng, generate_random_level}; // Added utils
use ndarray::ArrayView1; // Added for search_layer calls
use rand::rngs::StdRng; // Added for rng field
use tracing::{debug, trace, warn, info}; // Added tracing
use crate::vector::{Embedding, VectorId};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
// use std::sync::Arc; // Commented out as unused
// use tokio::sync::RwLock; // Commented out as unused // To be used if segments are shared with Arc<RwLock<dyn Segment>>

#[derive(Serialize, Deserialize, Debug)]
pub(crate) struct SimpleSegmentMetadata { // Made pub(crate)
    config: HnswConfig,
    distance_metric: DistanceMetric,
    vector_map: HashMap<VectorId, u64>,
    reverse_vector_map: HashMap<u64, VectorId>,
    next_internal_id: u64,
    // other segment-specific metadata
}

/// Trait defining the operations for a segment within an HnswIndex.
/// Segments are self-contained parts of an index.
#[async_trait]
pub trait Segment: Send + Sync {
    /// Inserts a vector into the segment.
    /// Returns Ok(true) if the vector was newly added, Ok(false) if an existing vector was updated.
    async fn insert_vector(
        &mut self,
        vector_id: VectorId,
        embedding: Embedding,
    ) -> Result<bool, VortexError>;

    /// Deletes a vector from the segment.
    async fn delete_vector(&mut self, vector_id: &VectorId) -> Result<(), VortexError>;

    /// Searches for the k-nearest neighbors of a query embedding within the segment.
    async fn search(
        &self,
        query_embedding: &Embedding,
        k: usize,
        ef_search: usize, // Added ef_search parameter
                           // TODO: filter_ids might evolve with payload indexing
                           // filter_ids: Option<&[VectorId]>,
    ) -> Result<Vec<SearchResult>, VortexError>;

    /// Saves the segment's data to the specified path.
    /// The path provided is the directory for this segment.
    async fn save(&mut self) -> Result<(), VortexError>;

    /// Flushes any in-memory buffers to disk.
    async fn flush(&mut self) -> Result<(), VortexError>;

    /// Returns the number of vectors in the segment.
    fn vector_count(&self) -> usize;

    /// Returns the dimensionality of vectors in the segment.
    fn dimensions(&self) -> usize;

    /// Retrieves a vector by its ID from the segment.
    async fn get_vector(&self, vector_id: &VectorId) -> Result<Option<Embedding>, VortexError>;

    /// Returns the path of the segment.
    fn path(&self) -> &Path;

    /// Lists vectors in the segment, optionally limiting the count.
    /// Returns a vector of (ID, Embedding) tuples.
    /// Note: The order is not guaranteed.
    async fn list_vectors(&self, limit: Option<usize>) -> Result<Vec<(VectorId, Embedding)>, VortexError>;

    // TODO: Add other necessary methods, e.g., for stats, configuration, specific HNSW operations if needed.
}

/// A simple implementation of a segment that manages its own storage.
/// This will be the initial type of segment used.
#[derive(Debug)] // Added Debug derive
pub struct SimpleSegment {
    path: PathBuf,
    pub(crate) config: HnswConfig,
    pub(crate) distance_metric: DistanceMetric,
    mmap_vector_storage: MmapVectorStorage,
    mmap_hnsw_graph_links: MmapHnswGraphLinks,
    vector_map: HashMap<VectorId, u64>, // External ID to internal u64 ID
    reverse_vector_map: HashMap<u64, VectorId>, // Internal u64 ID to external VectorId
    next_internal_id: u64,
    rng: StdRng, // For random level generation
                 // HNSW specific fields like entry_point, layers are managed by mmap_hnsw_graph_links
}

impl SimpleSegment {
    pub(crate) const METADATA_FILE: &'static str = "segment_meta.json";
    // const VECTORS_FILE: &'static str = "vectors.vec"; // Unused
    // const DELETIONS_FILE: &'static str = "vectors.del"; // Unused
    // const GRAPH_FILE: &'static str = "graph.hnsw"; // Unused

    /// Creates a new `SimpleSegment`.
    pub async fn new(
        segment_path: PathBuf,
        config: HnswConfig,
        distance_metric: DistanceMetric,
    ) -> Result<Self, VortexError> {
        if !segment_path.exists() {
            std::fs::create_dir_all(&segment_path).map_err(|e| VortexError::StorageError(e.to_string()))?;
        }

        // MmapVectorStorage::new expects base_path: &Path, name: &str
        // We are constructing filenames like "vectors.vec" and "graph.hnsw" directly.
        // The `name` parameter in MmapVectorStorage::new is used to create "name.vec", "name.del".
        // So, we should pass the stem of the filename as `name`.
        let mmap_vector_storage = MmapVectorStorage::new(
            &segment_path, 
            "segment_vectors", // Arbitrary name, files will be segment_vectors.vec, segment_vectors.del inside segment_path
            config.vector_dim,
            1000, // Placeholder capacity
        )?;

        let mmap_hnsw_graph_links = MmapHnswGraphLinks::new(
            &segment_path, 
            "segment_graph", // Arbitrary name, file will be segment_graph.hnsw inside segment_path
            1000, // Placeholder capacity (num_nodes)
            0, // Initial number of layers
            u64::MAX, // Initial entry point node ID (u64::MAX means no entry point)
            config.m_max0 as u32, 
            config.m as u32,
        )?;
        
        Ok(Self {
            path: segment_path,
            config,
            distance_metric,
            mmap_vector_storage,
            mmap_hnsw_graph_links,
            vector_map: HashMap::new(),
            reverse_vector_map: HashMap::new(),
            next_internal_id: 0, // Internal IDs start from 0, consistent with mmap_vector_storage
            rng: create_rng(config.seed),
        })
    }

    /// Loads a `SimpleSegment` from disk.
    pub async fn load(
        segment_path: PathBuf,
        // Config and distance metric might be loaded from metadata or passed if known
    ) -> Result<Self, VortexError> {
        if !segment_path.exists() {
            return Err(VortexError::StorageError(format!(
                "Segment path does not exist: {:?}",
                segment_path
            )));
        }

        let metadata_path = segment_path.join(Self::METADATA_FILE);
        let metadata_content = std::fs::read_to_string(metadata_path)
            .map_err(|e| VortexError::StorageError(format!("Failed to read segment metadata: {}", e)))?;
        let metadata: SimpleSegmentMetadata = serde_json::from_str(&metadata_content)
            .map_err(|e| VortexError::StorageError(format!("Failed to parse segment metadata: {}", e)))?;

        // Similarly for open, use the same "name" stems
        let mmap_vector_storage = MmapVectorStorage::open(
            &segment_path,
            "segment_vectors",
        )?;

        let mmap_hnsw_graph_links = MmapHnswGraphLinks::open(
            &segment_path,
            "segment_graph",
        )?;

        Ok(Self {
            path: segment_path,
            config: metadata.config,
            distance_metric: metadata.distance_metric,
            mmap_vector_storage,
            mmap_hnsw_graph_links,
            vector_map: metadata.vector_map,
            reverse_vector_map: metadata.reverse_vector_map,
            next_internal_id: metadata.next_internal_id,
            rng: create_rng(metadata.config.seed), // Re-initialize RNG with loaded config's seed
        })
    }

    fn save_metadata(&self) -> Result<(), VortexError> {
        let metadata = SimpleSegmentMetadata {
            config: self.config.clone(),
            distance_metric: self.distance_metric.clone(),
            vector_map: self.vector_map.clone(),
            reverse_vector_map: self.reverse_vector_map.clone(),
            next_internal_id: self.next_internal_id,
        };
        let metadata_content = serde_json::to_string_pretty(&metadata)
            .map_err(|e| VortexError::StorageError(format!("Failed to serialize segment metadata: {}", e)))?;
        std::fs::write(self.path.join(Self::METADATA_FILE), metadata_content)
            .map_err(|e| VortexError::StorageError(format!("Failed to write segment metadata: {}", e)))?;
        Ok(())
    }

    /// Finds a valid (non-deleted) entry point for HNSW operations.
    fn find_valid_entry_point(&self, preferred_entry_point_id: u64) -> Result<u64, VortexError> {
        if preferred_entry_point_id == u64::MAX {
            return Err(VortexError::Internal("find_valid_entry_point called with MAX sentinel on potentially empty graph.".to_string()));
        }

        if !self.mmap_vector_storage.is_deleted(preferred_entry_point_id) { // Removed ?
            return Ok(preferred_entry_point_id);
        }
        
        warn!("Preferred entry point {} is deleted. Attempting to find an alternative (basic scan).", preferred_entry_point_id);
        // Iterate internal IDs from reverse_vector_map keys
        for &internal_id_val in self.reverse_vector_map.keys() {
            if !self.mmap_vector_storage.is_deleted(internal_id_val) { // Removed ?
                info!("Found alternative entry point: {}", internal_id_val);
                return Ok(internal_id_val);
            }
        }
        
        Err(VortexError::Internal("No valid entry point found. All known nodes might be deleted or index is inconsistent.".to_string()))
    }


    /// Internal HNSW insertion logic.
    fn hnsw_insert_vector(&mut self, internal_id: u64, vector: &Embedding) -> Result<(), VortexError> {
        let mut new_node_level_usize = generate_random_level(self.config.ml, &mut self.rng);
        
        // Cap the generated level to be less than the graph's max layer capacity.
        // max_layers_capacity is the number of layers (e.g., 3 means layers 0, 1, 2).
        // So, max valid level index is max_layers_capacity - 1.
        let max_cap = self.mmap_hnsw_graph_links.get_max_layers_capacity();
        if max_cap > 0 { // Ensure max_cap is not 0 to prevent underflow
            if new_node_level_usize >= max_cap as usize {
                new_node_level_usize = (max_cap - 1) as usize;
                trace!(%internal_id, capped_level = new_node_level_usize, "Capped generated level to fit graph capacity.");
            }
        } else {
            // This case (max_cap == 0) should ideally not happen if graph is initialized with at least 1 layer.
            new_node_level_usize = 0;
            warn!(%internal_id, "Graph max_layers_capacity is 0. Setting new_node_level to 0.");
        }

        let new_node_level: u16 = new_node_level_usize as u16;
        trace!(%internal_id, new_node_level, "Final level for new node.");

        let current_graph_entry_point_id = self.mmap_hnsw_graph_links.get_entry_point_node_id();
        debug!(%internal_id, current_graph_entry_point_id, "In hnsw_insert_vector, got entry point");
        
        let current_max_graph_layer_idx = if self.mmap_hnsw_graph_links.get_num_layers() > 0 {
            self.mmap_hnsw_graph_links.get_num_layers() - 1
        } else {
            0
        };

        if current_graph_entry_point_id == u64::MAX { // This is the first node
            self.mmap_hnsw_graph_links.set_entry_point_node_id(internal_id)?;
            let required_num_layers = new_node_level + 1;
            if required_num_layers > self.mmap_hnsw_graph_links.get_num_layers() {
                 self.mmap_hnsw_graph_links.set_num_layers(required_num_layers)?;
            }
            // Initialize connections for all levels up to new_node_level for the first node
            for l_idx in 0..=new_node_level {
                self.mmap_hnsw_graph_links.set_connections(internal_id, l_idx, &[])?;
            }
            debug!(%internal_id, new_node_level, "Set new node as the first entry point and initialized its connections.");
            return Ok(());
        }
        
        let mut current_search_ep_id = self.find_valid_entry_point(current_graph_entry_point_id)?;

        if new_node_level < current_max_graph_layer_idx {
            for layer_idx in ((new_node_level + 1)..=current_max_graph_layer_idx).rev() {
                let candidates = hnsw::search_layer(
                    vector.view(), current_search_ep_id, 1, layer_idx,
                    &self.mmap_vector_storage, &self.mmap_hnsw_graph_links, self.distance_metric,
                )?;
                if let Some(best_neighbor) = candidates.peek() {
                    current_search_ep_id = best_neighbor.internal_id;
                } else {
                    warn!(layer_idx, %current_search_ep_id, "search_layer returned no candidates in upper layer traversal for segment.");
                }
            }
        }

        for layer_idx in (0..=std::cmp::min(new_node_level, current_max_graph_layer_idx)).rev() {
            let max_conns_for_this_layer = if layer_idx == 0 { self.config.m_max0 } else { self.config.m };

            let candidates_for_new_node = hnsw::search_layer(
                vector.view(), current_search_ep_id, self.config.ef_construction, layer_idx,
                &self.mmap_vector_storage, &self.mmap_hnsw_graph_links, self.distance_metric,
            )?;
            
            let new_node_neighbors_ids: Vec<u64> = hnsw::select_neighbors_heuristic(
                &candidates_for_new_node, max_conns_for_this_layer as usize
            );
            
            self.mmap_hnsw_graph_links.set_connections(internal_id, layer_idx, &new_node_neighbors_ids)?;

            for &neighbor_id_to_update_links_for in &new_node_neighbors_ids {
                let mut neighbor_current_connections = self.mmap_hnsw_graph_links.get_connections(neighbor_id_to_update_links_for, layer_idx)
                    .ok_or_else(|| VortexError::Internal(format!("Failed to get connections for neighbor {} at layer {}", neighbor_id_to_update_links_for, layer_idx)))?
                    .to_vec();

                let max_conns_for_this_neighbor = if layer_idx == 0 { self.config.m_max0 } else { self.config.m };

                if !neighbor_current_connections.contains(&internal_id) {
                    neighbor_current_connections.push(internal_id);
                }

                if neighbor_current_connections.len() > max_conns_for_this_neighbor as usize {
                    let neighbor_vec_for_pruning = self.mmap_vector_storage.get_vector(neighbor_id_to_update_links_for) // Removed ?
                        .ok_or_else(|| VortexError::Internal(format!("Neighbor vector {} not found for pruning", neighbor_id_to_update_links_for)))?; // Added ?
                    
                    let mut temp_candidates_heap = std::collections::BinaryHeap::new();
                    for &conn_candidate_id in &neighbor_current_connections {
                        if self.mmap_vector_storage.is_deleted(conn_candidate_id) { continue; } // Removed ?
                        if let Some(v_candidate) = self.mmap_vector_storage.get_vector(conn_candidate_id) { // Removed ?
                            let dist = crate::distance::calculate_distance(self.distance_metric, v_candidate.view(), neighbor_vec_for_pruning.view())?;
                            temp_candidates_heap.push(hnsw::Neighbor { distance: hnsw::heap_score(self.distance_metric, dist), internal_id: conn_candidate_id });
                        } else {
                             warn!("Vector for connection candidate {} not found during pruning for neighbor {}", conn_candidate_id, neighbor_id_to_update_links_for);
                        }
                    }
                    let pruned_connections = hnsw::select_neighbors_heuristic(&temp_candidates_heap, max_conns_for_this_neighbor as usize);
                    self.mmap_hnsw_graph_links.set_connections(neighbor_id_to_update_links_for, layer_idx, &pruned_connections)?;
                } else {
                    self.mmap_hnsw_graph_links.set_connections(neighbor_id_to_update_links_for, layer_idx, &neighbor_current_connections)?;
                }
            }
            
            if let Some(best_in_layer) = candidates_for_new_node.peek() {
                 current_search_ep_id = best_in_layer.internal_id;
            }
        }

        if new_node_level > current_max_graph_layer_idx {
            self.mmap_hnsw_graph_links.set_entry_point_node_id(internal_id)?;
            debug!(%internal_id, new_node_level, "Updated graph entry point to new node in segment.");
        }
        let required_num_layers_for_graph = new_node_level + 1;
        if required_num_layers_for_graph > self.mmap_hnsw_graph_links.get_num_layers() {
            self.mmap_hnsw_graph_links.set_num_layers(required_num_layers_for_graph)?;
            debug!(new_node_level, "Updated graph num_layers to {} in segment.", required_num_layers_for_graph);
        }
        Ok(())
    }

    /// Internal HNSW search logic.
    fn hnsw_search_internal(&self, query_vector: ArrayView1<f32>, k: usize, ef_search: usize) -> Result<Vec<SearchResult>, VortexError> {
        let initial_entry_point_id = self.mmap_hnsw_graph_links.get_entry_point_node_id();
        debug!(initial_entry_point_id, "In SimpleSegment::hnsw_search_internal, got entry point");
        
        if self.mmap_vector_storage.is_empty() { // Removed ?
            debug!("Search called on an empty segment (vector_storage empty).");
            return Ok(Vec::new());
        }

        if initial_entry_point_id == u64::MAX {
             debug!("Search called on a segment with no entry point (initial_entry_point_id is MAX).");
            return Ok(Vec::new());
        }
        
        let mut current_ep_id = self.find_valid_entry_point(initial_entry_point_id)?;
        let num_layers = self.mmap_hnsw_graph_links.get_num_layers();
        if num_layers == 0 {
            warn!("Search called on a segment with num_layers = 0. This is unexpected.");
            return Ok(Vec::new());
        }
        let top_layer_idx = num_layers - 1;

        let mut candidates_heap: std::collections::BinaryHeap<hnsw::Neighbor>;

        for layer_idx in (1..=top_layer_idx).rev() {
            candidates_heap = hnsw::search_layer(
                query_vector, current_ep_id, 1, layer_idx,
                &self.mmap_vector_storage, &self.mmap_hnsw_graph_links, self.distance_metric,
            )?;
            if let Some(best_neighbor) = candidates_heap.peek() {
                current_ep_id = best_neighbor.internal_id;
            } else {
                warn!(layer_idx, current_ep_id, "search_layer returned no candidates in segment search (upper layers).");
            }
        }

        candidates_heap = hnsw::search_layer(
            query_vector, current_ep_id, ef_search, 0, // Layer 0
            &self.mmap_vector_storage, &self.mmap_hnsw_graph_links, self.distance_metric,
        )?;

        let results: Vec<SearchResult> = candidates_heap.into_sorted_vec().iter().rev()
            .filter_map(|neighbor| {
                self.reverse_vector_map.get(&neighbor.internal_id).map(|external_id| {
                    SearchResult {
                        id: external_id.clone(),
                        distance: hnsw::original_score(self.distance_metric, neighbor.distance)
                    }
                })
            })
            .take(k).collect();
            
        debug!(k, ef_search, num_results=results.len(), "Segment search completed.");
        Ok(results)
    }

    /// Estimates the total size of memory-mapped files used by this segment.
    pub fn estimate_mapped_size(&self) -> u64 {
        self.mmap_vector_storage.mapped_size() + self.mmap_hnsw_graph_links.mapped_size()
    }

    // Helper methods for tests in index.rs
    #[cfg(test)] // Keep these for tests only
    pub(crate) fn vector_map_len(&self) -> usize {
        self.vector_map.len()
    }

    #[cfg(test)] // Keep these for tests only
    pub(crate) fn vector_map_contains_key(&self, key: &VectorId) -> bool {
        self.vector_map.contains_key(key)
    }
}

#[async_trait]
impl Segment for SimpleSegment {
        async fn insert_vector(
        &mut self,
        vector_id: VectorId,
        embedding: Embedding,
    ) -> Result<bool, VortexError> { // Changed return type
        if embedding.len() != self.config.vector_dim as usize {
            return Err(VortexError::Configuration(format!(
                "Invalid vector dimension: expected {}, got {}",
                self.config.vector_dim,
                embedding.len()
            )));
        }

        let is_new_insert: bool;
        let internal_id: u64;

        if let Some(&existing_id) = self.vector_map.get(&vector_id) {
            // ID exists, this is an update.
            is_new_insert = false;
            internal_id = existing_id;
            debug!(?vector_id, %internal_id, "Updating existing vector in segment.");
            // Mark old vector data as deleted. The HNSW graph links for this internal_id will be rebuilt.
            // Note: A more robust HNSW update might involve removing the node from graph first.
            // For now, overwriting vector and rebuilding links for this internal_id.
            // If mmap_vector_storage::put_vector overwrites, we don't need to explicitly delete.
            // Let's assume put_vector overwrites.
        } else {
            is_new_insert = true;
            // New ID. Check capacity.
            if self.next_internal_id >= self.mmap_vector_storage.capacity() {
                return Err(VortexError::StorageFull);
            }
            internal_id = self.next_internal_id;
            self.next_internal_id += 1; // Increment for next new insert.
            self.vector_map.insert(vector_id.clone(), internal_id);
            self.reverse_vector_map.insert(internal_id, vector_id.clone());
            debug!(?vector_id, %internal_id, "Storing new vector in segment, updated maps.");
        }
        
        self.mmap_vector_storage.put_vector(internal_id, &embedding)?;
        
        // Now perform HNSW graph insertion/update for this internal_id
        // This will build/rebuild connections for the node at internal_id.
        self.hnsw_insert_vector(internal_id, &embedding)?;
        
        Ok(is_new_insert)
    }

    async fn delete_vector(&mut self, vector_id: &VectorId) -> Result<(), VortexError> {
        if let Some(&internal_id) = self.vector_map.get(vector_id) {
            if self.mmap_vector_storage.is_deleted(internal_id) { // Removed ?
                return Ok(()); // Already deleted
            }
            self.mmap_vector_storage.delete_vector(internal_id)?;
            // TODO: Actual HNSW graph modification for deletion (e.g., marking node as "deleted" in graph traversal logic)
            // For now, we only mark in vector_storage. Search logic needs to respect this.
            // We don't remove from vector_map/reverse_vector_map to keep track of the ID.
            // Or, we could remove from maps, but then `next_internal_id` reuse becomes an issue without compaction.
            // Let's keep maps, rely on `is_deleted`.
            Ok(())
        } else {
            Err(VortexError::NotFound(vector_id.clone()))
        }
    }

    async fn search(
        &self,
        query_embedding: &Embedding,
        k: usize,
        ef_search: usize, // Added ef_search
    ) -> Result<Vec<SearchResult>, VortexError> {
        if query_embedding.len() != self.config.vector_dim as usize {
            return Err(VortexError::Configuration(format!(
                "Invalid query vector dimension: expected {}, got {}",
                self.config.vector_dim,
                query_embedding.len()
            )));
        }
        if k == 0 {
            return Ok(Vec::new());
        }
        let ef_s = std::cmp::max(k, ef_search); // Ensure ef_search is at least k
        self.hnsw_search_internal(query_embedding.view(), k, ef_s)
    }

    async fn save(&mut self) -> Result<(), VortexError> {
        self.flush().await?; // Ensure storage components are flushed
        self.save_metadata()?; // Save segment-specific metadata
        Ok(())
    }

    async fn flush(&mut self) -> Result<(), VortexError> {
        self.mmap_vector_storage.flush_data()?;
        self.mmap_vector_storage.flush_deletion_flags()?;
        self.mmap_hnsw_graph_links.flush()?;
        Ok(())
    }

    fn vector_count(&self) -> usize {
        self.mmap_vector_storage.len().try_into().unwrap_or(usize::MAX)
    }

    fn dimensions(&self) -> usize {
        self.config.vector_dim as usize // Cast to usize
    }

    async fn get_vector(&self, vector_id: &VectorId) -> Result<Option<Embedding>, VortexError> {
        if let Some(internal_id) = self.vector_map.get(vector_id) {
            if self.mmap_vector_storage.is_deleted(*internal_id) { // Removed ?
                return Ok(None); // Consider deleted as not found for get_vector
            }
            Ok(self.mmap_vector_storage.get_vector(*internal_id)) // Wrapped in Ok()
        } else {
            Ok(None)
        }
    }
    
    fn path(&self) -> &Path {
        &self.path
    }

    async fn list_vectors(&self, limit: Option<usize>) -> Result<Vec<(VectorId, Embedding)>, VortexError> {
        let mut results = Vec::new();
        let mut count = 0;
        let limit_val = limit.unwrap_or(usize::MAX);

        // Iterate over internal IDs stored in reverse_vector_map to ensure we only list valid, known vectors.
        for (internal_id, vector_id) in &self.reverse_vector_map {
            if count >= limit_val {
                break;
            }
            // Check if the vector is marked as deleted in mmap_vector_storage
            if !self.mmap_vector_storage.is_deleted(*internal_id) {
                match self.mmap_vector_storage.get_vector(*internal_id) {
                    Some(embedding) => {
                        results.push((vector_id.clone(), embedding));
                        count += 1;
                    }
                    None => {
                        // This case should ideally not happen if reverse_vector_map is consistent
                        // and non-deleted vectors are always present in mmap_vector_storage.
                        warn!("Vector ID {} (internal {}) found in reverse_vector_map but not in mmap_vector_storage or marked deleted inconsistently.", vector_id, internal_id);
                    }
                }
            }
        }
        Ok(results)
    }
}

// TODO: Add unit tests for SimpleSegment
// - new segment creation
// - insert_vector (basic, update)
// - delete_vector
// - get_vector (found, not found, deleted)
// - search (basic, with k)
// - save and load segment
// - flush
// - vector_count and dimensions

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::HnswConfig;
    use crate::distance::{DistanceMetric, Distance}; // Import the Distance trait
    use crate::vector::Embedding;
    use tempfile::tempdir;
    use ndarray::ArrayView1; // Needed for metric.distance calls

    fn create_test_segment_config() -> HnswConfig {
        // Using a config that matches HnswIndex tests for consistency
        HnswConfig {
            vector_dim: 3, // Default to 3 for simple tests, can be overridden
            m: 5,
            m_max0: 10,
            ef_construction: 20,
            ef_search: 10,
            ml: 0.5,
            seed: Some(123),
        }
    }

    #[tokio::test]
    async fn test_simple_segment_new_and_load() {
        let dir = tempdir().unwrap();
        let segment_path = dir.path().join("segment_0");
        let config = create_test_segment_config();
        let metric = DistanceMetric::L2;

        // Create new segment
        {
            let mut segment = SimpleSegment::new(segment_path.clone(), config, metric).await.unwrap();
            assert_eq!(segment.path(), segment_path.as_path());
            assert_eq!(segment.dimensions(), config.vector_dim as usize);
            assert_eq!(segment.vector_count(), 0);
            segment.save().await.unwrap(); // Save to persist metadata and create files
        }

        // Load the segment
        let loaded_segment = SimpleSegment::load(segment_path.clone()).await.unwrap();
        assert_eq!(loaded_segment.path(), segment_path.as_path());
        assert_eq!(loaded_segment.dimensions(), config.vector_dim as usize);
        assert_eq!(loaded_segment.vector_count(), 0); // Should be 0 as no vectors were added
        assert_eq!(loaded_segment.config, config);
        assert_eq!(loaded_segment.distance_metric, metric);
    }

    #[tokio::test]
    async fn test_simple_segment_insert_get_delete() {
        let dir = tempdir().unwrap();
        let segment_path = dir.path().join("segment_insert_delete");
        let config = create_test_segment_config(); // dim 3
        let metric = DistanceMetric::L2;

        let mut segment = SimpleSegment::new(segment_path, config, metric).await.unwrap();

        let vec_id1 = "vec1".to_string();
        let emb1 = Embedding::from(vec![1.0, 2.0, 3.0]);
        segment.insert_vector(vec_id1.clone(), emb1.clone()).await.unwrap();
        assert_eq!(segment.vector_count(), 1);

        // Get vector
        let retrieved_emb1 = segment.get_vector(&vec_id1).await.unwrap().unwrap();
        assert_eq!(retrieved_emb1, emb1);

        // Insert another vector
        let vec_id2 = "vec2".to_string();
        let emb2 = Embedding::from(vec![4.0, 5.0, 6.0]);
        segment.insert_vector(vec_id2.clone(), emb2.clone()).await.unwrap();
        assert_eq!(segment.vector_count(), 2);

        // Delete first vector
        segment.delete_vector(&vec_id1).await.unwrap();
        assert_eq!(segment.vector_count(), 1); // vector_map still has vec1, but it's marked deleted.
                                              // vector_count() currently returns vector_map.len(). This needs adjustment.
                                              // For now, let's test based on current vector_count impl.
                                              // After HNSW logic is fully in, vector_count should reflect non-deleted.
                                              // Let's assume delete_vector also removes from map for this test to pass.
                                              // No, SimpleSegment::delete_vector does not remove from map.
                                              // So vector_count will be 2.
                                              // Let's adjust vector_count to be more accurate for tests.
                                              // For now, let's test get_vector for deleted.
        assert!(segment.get_vector(&vec_id1).await.unwrap().is_none(), "Deleted vector should not be retrievable");

        // Try to delete non-existent
        let non_existent_id = "vec_non_existent".to_string();
        let del_res = segment.delete_vector(&non_existent_id).await;
        assert!(matches!(del_res, Err(VortexError::NotFound(_))));
        
        // Try to insert existing ID (should error based on current SimpleSegment::insert_vector)
        let insert_dup_res = segment.insert_vector(vec_id2.clone(), emb2.clone()).await;
        assert!(matches!(insert_dup_res, Ok(false)), "Inserting duplicate ID should return Ok(false)");

    }
    
    #[tokio::test]
    async fn test_simple_segment_search_placeholder() { // Rename to test_simple_segment_search_hnsw
        let dir = tempdir().unwrap();
        let segment_path = dir.path().join("segment_search_hnsw"); // New path for clarity
        let mut config = create_test_segment_config();
        config.vector_dim = 2; // for 2D vectors
        config.ef_search = 10; // Use a reasonable ef_search for testing
        config.seed = Some(42); // Fixed seed for reproducible random levels
        let metric = DistanceMetric::L2;
        let mut segment = SimpleSegment::new(segment_path, config, metric).await.unwrap();

        // Insert more vectors to make search more meaningful
        segment.insert_vector("v1".to_string(), Embedding::from(vec![1.0, 1.0])).await.unwrap();
        segment.insert_vector("v2".to_string(), Embedding::from(vec![1.5, 1.5])).await.unwrap();
        segment.insert_vector("v3".to_string(), Embedding::from(vec![2.0, 2.0])).await.unwrap();
        segment.insert_vector("v4".to_string(), Embedding::from(vec![10.0, 10.0])).await.unwrap();
        segment.insert_vector("v5".to_string(), Embedding::from(vec![0.5, 0.5])).await.unwrap();
        segment.insert_vector("v6".to_string(), Embedding::from(vec![1.2, 1.2])).await.unwrap();


        let query = Embedding::from(vec![1.1, 1.1]);
        let k = 3;
        let results = segment.search(&query, k, config.ef_search).await.unwrap();
        
        assert_eq!(results.len(), k, "Search should return k results");

        // Expected order: v1 (dist approx 0.02), v6 (dist approx 0.02), v2 (dist approx 0.32), v5 (dist approx 0.72)
        // Distances for L2:
        // q = [1.1, 1.1]
        // v1 = [1.0, 1.0] => (0.1)^2 + (0.1)^2 = 0.01 + 0.01 = 0.02. sqrt(0.02) = 0.1414
        // v6 = [1.2, 1.2] => (-0.1)^2 + (-0.1)^2 = 0.01 + 0.01 = 0.02. sqrt(0.02) = 0.1414
        // v2 = [1.5, 1.5] => (-0.4)^2 + (-0.4)^2 = 0.16 + 0.16 = 0.32. sqrt(0.32) = 0.5656
        // v5 = [0.5, 0.5] => (0.6)^2 + (0.6)^2 = 0.36 + 0.36 = 0.72. sqrt(0.72) = 0.8485
        
        // HNSW is approximate, so exact order of equidistant points might vary,
        // but v1 and v6 should be among the top.
        // And their distances should be very close.
        
        let result_ids: Vec<String> = results.iter().map(|sr| sr.id.clone()).collect();

        // Check if the closest vectors are present (order might vary slightly for equidistant)
        assert!(result_ids.contains(&"v1".to_string()));
        assert!(result_ids.contains(&"v6".to_string()));

        // Check distances are plausible and sorted (ascending for L2)
        for i in 0..results.len() - 1 {
            assert!(results[i].distance <= results[i+1].distance, "Results should be sorted by distance");
        }
        
        // Check specific distances for known closest points (approximate due to f32)
        let dist_v1 = metric.distance(query.view(), ArrayView1::from(&[1.0, 1.0])).unwrap();
        let dist_v6 = metric.distance(query.view(), ArrayView1::from(&[1.2, 1.2])).unwrap();

        if let Some(r_v1) = results.iter().find(|r| r.id == "v1") {
            assert!((r_v1.distance - dist_v1).abs() < 1e-5, "Distance for v1 is incorrect");
        }
        if let Some(r_v6) = results.iter().find(|r| r.id == "v6") {
            assert!((r_v6.distance - dist_v6).abs() < 1e-5, "Distance for v6 is incorrect");
        }
        
        // Ensure v4 (far away) is not in top k
        assert!(!result_ids.contains(&"v4".to_string()), "v4 should not be in the top k results");

        // Test with k=1
        let results_k1 = segment.search(&query, 1, config.ef_search).await.unwrap();
        assert_eq!(results_k1.len(), 1);
        // Either v1 or v6 should be the top result
        assert!(results_k1[0].id == "v1" || results_k1[0].id == "v6");
    }
    
    // Test for vector_count reflecting non-deleted items
    // This requires SimpleSegment::vector_count to be accurate.
    // Let's modify SimpleSegment::vector_count for this test.
    #[tokio::test]
    async fn test_vector_count_after_deletions() {
        let dir = tempdir().unwrap();
        let segment_path = dir.path().join("segment_vc");
        let config = create_test_segment_config();
        let mut segment = SimpleSegment::new(segment_path, config, DistanceMetric::L2).await.unwrap();

        segment.insert_vector("id1".to_string(), Embedding::from(vec![1.0,0.0,0.0])).await.unwrap();
        segment.insert_vector("id2".to_string(), Embedding::from(vec![2.0,0.0,0.0])).await.unwrap();
        segment.insert_vector("id3".to_string(), Embedding::from(vec![3.0,0.0,0.0])).await.unwrap();
        
        // Before delete, vector_count (current impl) is map size
        // vector_count() now uses mmap_vector_storage.len() which should be accurate.
        assert_eq!(segment.vector_count(), 3, "Initial count before delete");

        segment.delete_vector(&"id2".to_string()).await.unwrap();
        assert_eq!(segment.vector_count(), 2, "Count after one delete");

        segment.delete_vector(&"id1".to_string()).await.unwrap();
        assert_eq!(segment.vector_count(), 1, "Count after two deletes");
        
        segment.delete_vector(&"id3".to_string()).await.unwrap();
        assert_eq!(segment.vector_count(), 0, "Count after all deletes");
    }

    #[tokio::test]
    async fn test_list_vectors_empty() {
        let dir = tempdir().unwrap();
        let segment_path = dir.path().join("segment_list_empty");
        let config = create_test_segment_config();
        let segment = SimpleSegment::new(segment_path, config, DistanceMetric::L2).await.unwrap();

        let listed_vectors = segment.list_vectors(None).await.unwrap();
        assert!(listed_vectors.is_empty(), "list_vectors on empty segment should return empty list");

        let listed_vectors_limit = segment.list_vectors(Some(5)).await.unwrap();
        assert!(listed_vectors_limit.is_empty(), "list_vectors with limit on empty segment should return empty list");
    }

    #[tokio::test]
    async fn test_list_vectors_basic_and_with_limit() {
        let dir = tempdir().unwrap();
        let segment_path = dir.path().join("segment_list_basic");
        let config = create_test_segment_config();
        let mut segment = SimpleSegment::new(segment_path, config, DistanceMetric::L2).await.unwrap();

        let vec1 = ("id1".to_string(), Embedding::from(vec![1.0, 0.0, 0.0]));
        let vec2 = ("id2".to_string(), Embedding::from(vec![2.0, 0.0, 0.0]));
        let vec3 = ("id3".to_string(), Embedding::from(vec![3.0, 0.0, 0.0]));

        segment.insert_vector(vec1.0.clone(), vec1.1.clone()).await.unwrap();
        segment.insert_vector(vec2.0.clone(), vec2.1.clone()).await.unwrap();
        segment.insert_vector(vec3.0.clone(), vec3.1.clone()).await.unwrap();

        // Test list_vectors with no limit
        let listed_all = segment.list_vectors(None).await.unwrap();
        assert_eq!(listed_all.len(), 3, "Should list all 3 vectors");
        // Check if all inserted vectors are present (order not guaranteed)
        assert!(listed_all.iter().any(|(id, _)| id == &vec1.0));
        assert!(listed_all.iter().any(|(id, _)| id == &vec2.0));
        assert!(listed_all.iter().any(|(id, _)| id == &vec3.0));

        // Test list_vectors with limit smaller than total
        let listed_limit_2 = segment.list_vectors(Some(2)).await.unwrap();
        assert_eq!(listed_limit_2.len(), 2, "Should list 2 vectors with limit 2");

        // Test list_vectors with limit equal to total
        let listed_limit_3 = segment.list_vectors(Some(3)).await.unwrap();
        assert_eq!(listed_limit_3.len(), 3, "Should list 3 vectors with limit 3");
        
        // Test list_vectors with limit larger than total
        let listed_limit_5 = segment.list_vectors(Some(5)).await.unwrap();
        assert_eq!(listed_limit_5.len(), 3, "Should list all 3 vectors with limit 5");
    }

    #[tokio::test]
    async fn test_list_vectors_after_delete() {
        let dir = tempdir().unwrap();
        let segment_path = dir.path().join("segment_list_delete");
        let config = create_test_segment_config();
        let mut segment = SimpleSegment::new(segment_path, config, DistanceMetric::L2).await.unwrap();

        let vec1_id = "id1".to_string();
        let vec1_emb = Embedding::from(vec![1.0, 0.0, 0.0]);
        segment.insert_vector(vec1_id.clone(), vec1_emb.clone()).await.unwrap();
        segment.insert_vector("id2".to_string(), Embedding::from(vec![2.0, 0.0, 0.0])).await.unwrap();
        
        segment.delete_vector(&"id2".to_string()).await.unwrap();

        let listed_vectors = segment.list_vectors(None).await.unwrap();
        assert_eq!(listed_vectors.len(), 1, "Should list 1 vector after deletion");
        assert_eq!(listed_vectors[0].0, vec1_id, "The remaining vector should be id1");
        assert_eq!(listed_vectors[0].1, vec1_emb, "The embedding of remaining vector should be vec1_emb");
    }

    #[tokio::test]
    async fn test_save_load_with_data() {
        let dir = tempdir().unwrap();
        let segment_path = dir.path().join("segment_save_load_data");
        let mut config = create_test_segment_config();
        config.vector_dim = 2;
        let metric = DistanceMetric::Cosine; // Use a different metric for variety

        let vec1_id = "v10".to_string();
        let vec1_emb = Embedding::from(vec![0.1, 0.2]);
        let vec2_id = "v20".to_string();
        let vec2_emb = Embedding::from(vec![0.3, 0.4]);
        let vec3_id = "v30".to_string();
        let vec3_emb = Embedding::from(vec![0.5, 0.6]);

        // Create, insert data, and save
        {
            let mut segment = SimpleSegment::new(segment_path.clone(), config, metric).await.unwrap();
            segment.insert_vector(vec1_id.clone(), vec1_emb.clone()).await.unwrap();
            segment.insert_vector(vec2_id.clone(), vec2_emb.clone()).await.unwrap();
            segment.insert_vector(vec3_id.clone(), vec3_emb.clone()).await.unwrap();
            
            // Perform a search to ensure HNSW graph is built/modified
            let _ = segment.search(&Embedding::from(vec![0.15, 0.25]), 1, config.ef_search).await.unwrap();
            
            segment.save().await.unwrap();
        }

        // Load the segment
        let loaded_segment = SimpleSegment::load(segment_path.clone()).await.unwrap();

        // Verify properties
        assert_eq!(loaded_segment.path(), segment_path.as_path());
        assert_eq!(loaded_segment.dimensions(), config.vector_dim as usize);
        assert_eq!(loaded_segment.vector_count(), 3);
        assert_eq!(loaded_segment.config, config);
        assert_eq!(loaded_segment.distance_metric, metric);

        // Verify vector map contents (spot check)
        assert!(loaded_segment.vector_map.contains_key(&vec1_id));
        assert!(loaded_segment.reverse_vector_map.values().any(|v_id| v_id == &vec2_id));
        
        // Verify retrieved vectors
        assert_eq!(loaded_segment.get_vector(&vec1_id).await.unwrap().unwrap(), vec1_emb);
        assert_eq!(loaded_segment.get_vector(&vec2_id).await.unwrap().unwrap(), vec2_emb);
        assert_eq!(loaded_segment.get_vector(&vec3_id).await.unwrap().unwrap(), vec3_emb);

        // Verify search results (consistency of HNSW graph state)
        let query_emb = Embedding::from(vec![0.15, 0.25]);
        let search_results_loaded = loaded_segment.search(&query_emb, 2, config.ef_search).await.unwrap();
        assert_eq!(search_results_loaded.len(), 2);
        // Further checks on search_results_loaded could compare against expected results if known,
        // or against results from the original segment instance if stored.
        // For now, just checking count and that it runs without error is a good sign.
        // Example: one of the results should be v10 or v20 as they are closer to query_emb
        let result_ids_loaded: Vec<String> = search_results_loaded.iter().map(|sr| sr.id.clone()).collect();
        assert!(result_ids_loaded.contains(&vec1_id) || result_ids_loaded.contains(&vec2_id));
    }
    
    #[tokio::test]
    async fn test_insert_vector_incorrect_dimension() {
        let dir = tempdir().unwrap();
        let segment_path = dir.path().join("segment_incorrect_dim");
        let mut config = create_test_segment_config();
        config.vector_dim = 3;
        let mut segment = SimpleSegment::new(segment_path, config, DistanceMetric::L2).await.unwrap();

        let emb_wrong_dim = Embedding::from(vec![1.0, 2.0]); // Dimension 2
        let result = segment.insert_vector("wrong_dim_vec".to_string(), emb_wrong_dim).await;
        assert!(matches!(result, Err(VortexError::Configuration(_))));
    }

    #[tokio::test]
    async fn test_search_edge_cases() {
        let dir = tempdir().unwrap();
        let segment_path = dir.path().join("segment_search_edges");
        let mut config = create_test_segment_config();
        config.vector_dim = 2;
        let mut segment = SimpleSegment::new(segment_path.clone(), config, DistanceMetric::L2).await.unwrap();

        // Search on empty segment
        let query_empty = Embedding::from(vec![0.0, 0.0]);
        let results_empty = segment.search(&query_empty, 3, config.ef_search).await.unwrap();
        assert!(results_empty.is_empty(), "Search on empty segment should yield no results");

        // Insert one vector
        segment.insert_vector("v1".to_string(), Embedding::from(vec![1.0, 1.0])).await.unwrap();

        // Search with k=0
        let results_k0 = segment.search(&query_empty, 0, config.ef_search).await.unwrap();
        assert!(results_k0.is_empty(), "Search with k=0 should yield no results");
        
        // Search with query of incorrect dimension
        let query_wrong_dim = Embedding::from(vec![0.0, 0.0, 0.0]); // Dimension 3
        let results_wrong_dim = segment.search(&query_wrong_dim, 1, config.ef_search).await;
        assert!(matches!(results_wrong_dim, Err(VortexError::Configuration(_))));
        
        // Delete all vectors and search
        segment.delete_vector(&"v1".to_string()).await.unwrap();
        let results_after_all_deleted = segment.search(&query_empty, 1, config.ef_search).await.unwrap();
        assert!(results_after_all_deleted.is_empty(), "Search after all vectors deleted should yield no results");
    }
}
