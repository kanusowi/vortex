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
    async fn insert_vector(
        &mut self,
        vector_id: VectorId,
        embedding: Embedding,
    ) -> Result<(), VortexError>;

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
        let new_node_level_usize = generate_random_level(self.config.ml, &mut self.rng);
        let new_node_level: u16 = new_node_level_usize as u16;
        trace!(%internal_id, new_node_level, "Generated level for new node.");

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
    ) -> Result<(), VortexError> {
        if embedding.len() != self.config.vector_dim as usize {
            return Err(VortexError::Configuration(format!(
                "Invalid vector dimension: expected {}, got {}",
                self.config.vector_dim,
                embedding.len()
            )));
        }

        // Check if vector_id already exists. If so, this is an update.
        // HNSW update often means delete then insert. For simplicity, we might just update vector data
        // and not change graph structure, or re-insert.
        // For now, let's assume if ID exists, we update its vector and re-run HNSW logic for it.
        // This is complex. A simpler approach for "update" is to delete then insert.
        // Let's assume for now `insert_vector` is for new vectors.
        // If vector_id exists, we could error or ignore. The trait doesn't specify.
        // Let's make it an error to insert an existing ID for now.
        if self.vector_map.contains_key(&vector_id) {
            return Err(VortexError::StorageError(format!("Vector ID {} already exists in segment", vector_id)));
        }
        
        // Check capacity before assigning new internal ID
        if self.next_internal_id >= self.mmap_vector_storage.capacity() {
            return Err(VortexError::StorageFull);
        }

        let internal_id = self.next_internal_id;
        self.mmap_vector_storage.put_vector(internal_id, &embedding)?;
        
        self.next_internal_id += 1; // Increment only after successful put_vector
        self.vector_map.insert(vector_id.clone(), internal_id);
        self.reverse_vector_map.insert(internal_id, vector_id.clone());
        debug!(?vector_id, %internal_id, "Stored vector in segment, updated maps.");
        
        // Now perform HNSW graph insertion
        self.hnsw_insert_vector(internal_id, &embedding)?;
        Ok(())
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
    use crate::distance::DistanceMetric;
    use crate::vector::Embedding;
    use tempfile::tempdir;

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
        assert!(matches!(insert_dup_res, Err(VortexError::StorageError(_))), "Inserting duplicate ID should error");

    }
    
    #[tokio::test]
    async fn test_simple_segment_search_placeholder() {
        let dir = tempdir().unwrap();
        let segment_path = dir.path().join("segment_search");
        let mut config = create_test_segment_config();
        config.vector_dim = 2; // for 2D vectors
        let metric = DistanceMetric::L2;
        let mut segment = SimpleSegment::new(segment_path, config, metric).await.unwrap();

        segment.insert_vector("v1".to_string(), Embedding::from(vec![1.0, 1.0])).await.unwrap();
        segment.insert_vector("v2".to_string(), Embedding::from(vec![2.0, 2.0])).await.unwrap();
        segment.insert_vector("v3".to_string(), Embedding::from(vec![10.0, 10.0])).await.unwrap();

        let query = Embedding::from(vec![1.1, 1.1]);
        let results = segment.search(&query, 2, config.ef_search).await.unwrap();
        
        // Current search is a placeholder (linear scan).
        // With HNSW logic moved, this test will become more meaningful.
        assert_eq!(results.len(), 2);
        if !results.is_empty() {
            assert_eq!(results[0].id, "v1"); // v1 should be closest
        }
        if results.len() > 1 {
            assert_eq!(results[1].id, "v2"); // v2 should be next
        }
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
}
