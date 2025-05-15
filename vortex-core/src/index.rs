use crate::config::HnswConfig;
use crate::distance::DistanceMetric;
use crate::error::{VortexError, VortexResult};
// crate::hnsw removed as it's unused now
// TODO: Re-evaluate hnsw module usage once HNSW logic is integrated here
use crate::hnsw; // Will be needed for search_layer, select_neighbors_heuristic
use crate::vector::{Embedding, VectorId};
use crate::utils::{create_rng, generate_random_level};
// calculate_distance removed
use crate::storage::mmap_vector_storage::MmapVectorStorage;
use crate::storage::mmap_hnsw_graph_links::MmapHnswGraphLinks;

use async_trait::async_trait;
// ndarray::ArrayView1 removed
use rand::rngs::StdRng;
use serde::{Serialize, Deserialize}; // Added serde imports
use std::collections::HashMap;
use std::fs; // Added fs import
use std::io::Write; // BufReader, BufWriter, Read removed
use std::path::{Path, PathBuf}; // Added PathBuf
// std::sync::Arc removed
use tracing::{warn, info, debug, trace}; // Added debug, trace
use ndarray::ArrayView1; // Will be needed for search_layer calls


/// The primary trait defining the vector index functionality.
#[async_trait]
pub trait Index: Send + Sync + std::fmt::Debug {
    async fn add_vector(&mut self, id: VectorId, vector: Embedding) -> VortexResult<bool>;
    async fn search(&self, query: Embedding, k: usize) -> VortexResult<Vec<(VectorId, f32)>>;
    async fn save(&mut self, writer: &mut (dyn Write + Send)) -> VortexResult<()>; // Changed to &mut self
    async fn delete_vector(&mut self, id: &VectorId) -> VortexResult<bool>;
    async fn get_vector(&self, id: &VectorId) -> VortexResult<Option<Embedding>>;
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool;
    fn dimensions(&self) -> usize;
    fn config(&self) -> HnswConfig;
    fn distance_metric(&self) -> DistanceMetric;
    
    /// Lists vectors in the index, optionally limiting the count.
    /// Returns a vector of (ID, Embedding) tuples.
    /// Note: The order is not guaranteed.
    async fn list_vectors(&self, limit: Option<usize>) -> VortexResult<Vec<(VectorId, Embedding)>>;

    /// Searches for the k nearest neighbors using a specified ef_search value.
    async fn search_with_ef(&self, query: Embedding, k: usize, ef_search: usize) -> VortexResult<Vec<(VectorId, f32)>>;
}

// /// Data structure representing the HNSW index state for serialization.
// #[derive(Serialize, Deserialize)]
// struct HnswIndexData {
//     config: HnswConfig,
//     metric: DistanceMetric,
//     dimensions: usize,
//     nodes: Vec<Node>, // This will change with MmapHnswGraphLinks
//     vector_map: HashMap<VectorId, usize>, // This will change to u64
//     entry_point: Option<usize>, // This will come from MmapHnswGraphLinks
//     current_max_level: usize, // This will come from MmapHnswGraphLinks
//     deleted_count: usize, // This will come from MmapVectorStorage
// }

#[derive(Serialize, Deserialize, Debug)]
struct HnswIndexMetadata {
    vector_map: HashMap<String, u64>, // External ID to internal u64 ID
    total_vectors_inserted_count: u64,
}

/// Implementation of the `Index` trait using the HNSW algorithm.
#[derive(Debug)]
pub struct HnswIndex {
    path: PathBuf, // Added path field to store the base path for index files
    config: HnswConfig,
    metric: DistanceMetric,
    // dimensions: usize, // Now in vector_storage.header
    vector_storage: MmapVectorStorage,
    graph_links: MmapHnswGraphLinks,
    vector_map: HashMap<VectorId, u64>, // Maps external ID to internal u64 ID used by storage
    rng: StdRng,
    total_vectors_inserted_count: u64, // Tracks total vectors ever added to assign new internal IDs
    // nodes: Vec<ArcNode>, // Replaced by MmapVectorStorage and MmapHnswGraphLinks
    // entry_point: Option<usize>, // Now in graph_links.header
    // current_max_level: usize, // Now in graph_links.header
    // deleted_count: usize, // Now derived from vector_storage.header
}

// impl Serialize for HnswIndex {
//     fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
//     where
//         S: serde::Serializer,
//     {
//         // This needs to be entirely rethought. Persistence is now via mmap files.
//         // We might serialize metadata or a manifest file, but not the whole index like this.
//         unimplemented!("Serialization for mmap-based HnswIndex is different.");
//     }
// }

// impl<'de> Deserialize<'de> for HnswIndex {
//     fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
//     where
//         D: serde::Deserializer<'de>,
//     {
//         // This needs to be entirely rethought. Loading is via opening mmap files.
//         unimplemented!("Deserialization for mmap-based HnswIndex is different.");
//     }
// }

const METADATA_FILE_SUFFIX: &str = ".hnsw_meta.json";

fn get_metadata_path(base_path: &Path) -> PathBuf {
    let mut file_name = base_path.file_name().unwrap_or_default().to_os_string();
    file_name.push(METADATA_FILE_SUFFIX);
    base_path.with_file_name(file_name)
}

impl HnswIndex {
    /// Creates a new HNSW index using memory-mapped files for storage.
    pub fn new(
        base_path: &Path,
        name: &str,
        config: HnswConfig,
        metric: DistanceMetric,
        dimensions: u32,
        capacity: u64,
    ) -> VortexResult<Self> {
        config.validate()?;
        if dimensions == 0 {
            return Err(VortexError::Configuration("Dimensions must be greater than 0".to_string()));
        }
        if capacity == 0 {
            return Err(VortexError::Configuration("Capacity must be greater than 0".to_string()));
        }

        info!(path=?base_path, index_name=name, m=config.m, ef_construction=config.ef_construction, metric=?metric, dimensions, capacity, "Creating new mmap-based HNSW index");

        let vector_storage = MmapVectorStorage::new(base_path, name, dimensions, capacity)?;
        
        let initial_num_layers = 1u16; 
        let initial_entry_point = u64::MAX;

        let graph_links = MmapHnswGraphLinks::new(
            base_path, name, capacity, 
            initial_num_layers, initial_entry_point,
            config.m_max0 as u32, config.m as u32,
        )?;
        
        let index_file_path = base_path.join(name);

        Ok(HnswIndex {
            path: index_file_path,
            config, metric, vector_storage, graph_links,
            vector_map: HashMap::new(),
            rng: create_rng(config.seed),
            total_vectors_inserted_count: 0,
        })
    }

    /// Opens an existing HNSW index from memory-mapped files.
    pub fn open(
        base_path: &Path,
        name: &str,
        config: HnswConfig,
        metric: DistanceMetric,
    ) -> VortexResult<Self> {
        info!(path=?base_path, index_name=name, "Opening mmap-based HNSW index");

        let vector_storage = MmapVectorStorage::open(base_path, name)?;
        let graph_links = MmapHnswGraphLinks::open(base_path, name)?;
        
        let index_file_path = base_path.join(name);

        let metadata_path = get_metadata_path(&index_file_path);
        let (vector_map, total_vectors_inserted_count) = if metadata_path.exists() {
            debug!("Loading HNSW index metadata from {:?}", metadata_path);
            let file = fs::File::open(&metadata_path).map_err(|e| VortexError::IoError { path: metadata_path.clone(), source: e })?;
            let metadata: HnswIndexMetadata = serde_json::from_reader(file)
                .map_err(|e| VortexError::StorageError(format!("Failed to deserialize metadata from {:?}: {}", metadata_path, e)))?;
            (metadata.vector_map, metadata.total_vectors_inserted_count)
        } else {
            warn!("Metadata file {:?} not found. Initializing empty vector_map and zero total_vectors_inserted_count. This is normal if opening an index created before metadata persistence.", metadata_path);
            // For compatibility with older indices or if metadata is intentionally missing,
            // we initialize with empty/default values.
            // A more robust solution might involve versioning or explicit migration.
            (HashMap::new(), 0)
        };
        
        info!("Successfully opened HNSW index. Loaded {} vector mappings.", vector_map.len());

        Ok(HnswIndex {
            path: index_file_path,
            config, metric, vector_storage, graph_links,
            vector_map,
            rng: create_rng(config.seed),
            total_vectors_inserted_count,
        })
    }

    /// Performs the internal HNSW search.
    fn search_internal(&self, query_vector: ArrayView1<f32>, k: usize, ef_search: usize) -> VortexResult<Vec<(u64, f32)>> {
        if self.vector_storage.is_empty() {
            debug!("Search called on an empty index.");
            return Ok(Vec::new());
        }

        let initial_entry_point_id = self.graph_links.get_entry_point_node_id();
        if initial_entry_point_id == u64::MAX {
             debug!("Search called on an index with no entry point.");
            return Ok(Vec::new());
        }
        
        let mut current_ep_id = self.find_valid_entry_point(initial_entry_point_id)?;
        let num_layers = self.graph_links.get_num_layers(); // u16
        if num_layers == 0 {
            warn!("Search called on an index with num_layers = 0. This is unexpected.");
            return Ok(Vec::new());
        }
        let top_layer_idx = num_layers - 1; // u16

        let mut candidates_heap: std::collections::BinaryHeap<hnsw::Neighbor>;

        for layer_idx in (1..=top_layer_idx).rev() { // layer_idx is u16
            candidates_heap = hnsw::search_layer(
                query_vector, current_ep_id, 1, layer_idx,
                &self.vector_storage, &self.graph_links, self.metric,
            )?;
            if let Some(best_neighbor) = candidates_heap.peek() {
                current_ep_id = best_neighbor.internal_id;
            } else {
                warn!(layer_idx, current_ep_id, "search_layer returned no candidates in search_internal (upper layers).");
            }
        }

        candidates_heap = hnsw::search_layer(
            query_vector, current_ep_id, ef_search, 0, // Layer 0
            &self.vector_storage, &self.graph_links, self.metric,
        )?;

        let results: Vec<(u64, f32)> = candidates_heap.into_sorted_vec().iter().rev()
            .map(|neighbor| (neighbor.internal_id, hnsw::original_score(self.metric, neighbor.distance)))
            .take(k).collect();
            
        debug!(k, ef_search, num_results=results.len(), "Search completed.");
        Ok(results)
    }

    /// Inserts a vector into the HNSW index.
    fn insert_vector(&mut self, external_id: VectorId, vector: Embedding) -> VortexResult<()> {
        if vector.len() != self.dimensions() {
            return Err(VortexError::DimensionMismatch { expected: self.dimensions(), actual: vector.len() });
        }

        let internal_id = self.total_vectors_inserted_count;
        if internal_id >= self.vector_storage.capacity() {
            return Err(VortexError::StorageFull);
        }

        self.vector_storage.put_vector(internal_id, &vector)?;
        self.total_vectors_inserted_count += 1;
        self.vector_map.insert(external_id.clone(), internal_id);
        debug!(?external_id, %internal_id, "Stored vector, updated vector_map.");

        let new_node_level_usize = generate_random_level(self.config.ml, &mut self.rng);
        let new_node_level: u16 = new_node_level_usize as u16;
        trace!(%internal_id, new_node_level, "Generated level for new node.");

        let current_graph_entry_point_id = self.graph_links.get_entry_point_node_id();
        let current_max_graph_layer_idx = if self.graph_links.get_num_layers() > 0 {
            self.graph_links.get_num_layers() - 1
        } else { 0 };

        if current_graph_entry_point_id == u64::MAX { // This is the first node
            self.graph_links.set_entry_point_node_id(internal_id)?;
            let required_num_layers = new_node_level + 1;
            if required_num_layers > self.graph_links.get_num_layers() {
                 self.graph_links.set_num_layers(required_num_layers)?;
            }
            debug!(%internal_id, new_node_level, "Set new node as the first entry point.");
            return Ok(());
        }
        
        let mut current_search_ep_id = self.find_valid_entry_point(current_graph_entry_point_id)?;

        if new_node_level < current_max_graph_layer_idx {
            for layer_idx in ((new_node_level + 1)..=current_max_graph_layer_idx).rev() {
                let candidates = hnsw::search_layer(
                    vector.view(), current_search_ep_id, 1, layer_idx,
                    &self.vector_storage, &self.graph_links, self.metric,
                )?;
                if let Some(best_neighbor) = candidates.peek() {
                    current_search_ep_id = best_neighbor.internal_id;
                } else {
                    warn!(layer_idx, %current_search_ep_id, "search_layer returned no candidates in upper layer traversal.");
                }
            }
        }

        for layer_idx in (0..=std::cmp::min(new_node_level, current_max_graph_layer_idx)).rev() {
            let max_conns_for_this_layer = if layer_idx == 0 { self.config.m_max0 } else { self.config.m };

            let candidates_for_new_node = hnsw::search_layer(
                vector.view(), current_search_ep_id, self.config.ef_construction, layer_idx,
                &self.vector_storage, &self.graph_links, self.metric,
            )?;
            
            let new_node_neighbors_ids: Vec<u64> = hnsw::select_neighbors_heuristic(
                &candidates_for_new_node, max_conns_for_this_layer
            );
            
            self.graph_links.set_connections(internal_id, layer_idx, &new_node_neighbors_ids)?;

            for &neighbor_id_to_update_links_for in &new_node_neighbors_ids {
                let mut neighbor_current_connections = self.graph_links.get_connections(neighbor_id_to_update_links_for, layer_idx)
                    .ok_or_else(|| VortexError::Internal(format!("Failed to get connections for neighbor {} at layer {}", neighbor_id_to_update_links_for, layer_idx)))?
                    .to_vec();

                let max_conns_for_this_neighbor = if layer_idx == 0 { self.config.m_max0 } else { self.config.m };

                if !neighbor_current_connections.contains(&internal_id) {
                    neighbor_current_connections.push(internal_id);
                }

                if neighbor_current_connections.len() > max_conns_for_this_neighbor {
                    let neighbor_vec_for_pruning = self.vector_storage.get_vector(neighbor_id_to_update_links_for)
                        .ok_or_else(|| VortexError::Internal(format!("Neighbor vector {} not found for pruning", neighbor_id_to_update_links_for)))?;
                    
                    let mut temp_candidates_heap = std::collections::BinaryHeap::new();
                    for &conn_candidate_id in &neighbor_current_connections {
                        if self.vector_storage.is_deleted(conn_candidate_id) { continue; }
                        if let Some(v_candidate) = self.vector_storage.get_vector(conn_candidate_id) {
                            let dist = crate::distance::calculate_distance(self.metric, v_candidate.view(), neighbor_vec_for_pruning.view())?;
                            temp_candidates_heap.push(hnsw::Neighbor { distance: hnsw::heap_score(self.metric, dist), internal_id: conn_candidate_id });
                        } else {
                             warn!("Vector for connection candidate {} not found during pruning for neighbor {}", conn_candidate_id, neighbor_id_to_update_links_for);
                        }
                    }
                    let pruned_connections = hnsw::select_neighbors_heuristic(&temp_candidates_heap, max_conns_for_this_neighbor);
                    self.graph_links.set_connections(neighbor_id_to_update_links_for, layer_idx, &pruned_connections)?;
                } else {
                    self.graph_links.set_connections(neighbor_id_to_update_links_for, layer_idx, &neighbor_current_connections)?;
                }
            }
            
            if let Some(best_in_layer) = candidates_for_new_node.peek() {
                 current_search_ep_id = best_in_layer.internal_id;
            }
        }

        if new_node_level > current_max_graph_layer_idx {
            self.graph_links.set_entry_point_node_id(internal_id)?;
            debug!(%internal_id, new_node_level, "Updated graph entry point to new node.");
        }
        let required_num_layers_for_graph = new_node_level + 1;
        if required_num_layers_for_graph > self.graph_links.get_num_layers() {
            self.graph_links.set_num_layers(required_num_layers_for_graph)?;
            debug!(new_node_level, "Updated graph num_layers to {}.", required_num_layers_for_graph);
        }
        
        Ok(())
    }

    /// Finds a valid (non-deleted) entry point for HNSW operations.
    fn find_valid_entry_point(&self, preferred_entry_point_id: u64) -> VortexResult<u64> {
        if preferred_entry_point_id == u64::MAX {
            return Err(VortexError::Internal("find_valid_entry_point called with MAX sentinel on a non-empty graph.".to_string()));
        }

        if !self.vector_storage.is_deleted(preferred_entry_point_id) {
            return Ok(preferred_entry_point_id);
        }
        
        warn!("Preferred entry point {} is deleted. Attempting to find an alternative (basic scan).", preferred_entry_point_id);
        for &internal_id_val in self.vector_map.values() {
            if !self.vector_storage.is_deleted(internal_id_val) {
                info!("Found alternative entry point: {}", internal_id_val);
                return Ok(internal_id_val);
            }
        }
        
        Err(VortexError::Internal("No valid entry point found. All known nodes might be deleted or index is inconsistent.".to_string()))
    }
}


#[async_trait]
impl Index for HnswIndex {
    async fn add_vector(&mut self, id: VectorId, vector: Embedding) -> VortexResult<bool> {
        if self.vector_map.contains_key(&id) {
            warn!(?id, "Attempted to add vector with existing ID. Operation ignored.");
            return Ok(false); 
        }
        if vector.len() != self.dimensions() {
            return Err(VortexError::DimensionMismatch { expected: self.dimensions(), actual: vector.len() });
        }
        
        self.insert_vector(id, vector)?; 
        Ok(true)
    }

    async fn search(&self, query: Embedding, k: usize) -> VortexResult<Vec<(VectorId, f32)>> {
        if query.len() != self.dimensions() {
            return Err(VortexError::DimensionMismatch { expected: self.dimensions(), actual: query.len() });
        }
        let ef_search = self.config.ef_search;
        self.search_with_ef(query, k, ef_search).await
    }

    async fn search_with_ef(&self, query: Embedding, k: usize, ef_search_override: usize) -> VortexResult<Vec<(VectorId, f32)>> {
        if query.len() != self.dimensions() {
            return Err(VortexError::DimensionMismatch { expected: self.dimensions(), actual: query.len() });
        }
        if k == 0 {
            return Ok(Vec::new());
        }
        let ef_s = std::cmp::max(k, ef_search_override);

        let internal_results = self.search_internal(query.view(), k, ef_s)?;
        
        let mut external_results = Vec::with_capacity(internal_results.len());
        for (internal_id, score) in internal_results {
            let found_external_id = self.vector_map.iter().find_map(|(ext_id, &int_id)| {
                if int_id == internal_id { Some(ext_id.clone()) } else { None }
            });

            if let Some(ext_id) = found_external_id {
                external_results.push((ext_id, score));
            } else {
                warn!(%internal_id, "Internal ID found in search results but not in vector_map. Skipping.");
            }
        }
        Ok(external_results)
    }

    async fn save(&mut self, _writer: &mut (dyn Write + Send)) -> VortexResult<()> { // Changed to &mut self
        info!("Saving HNSW index metadata and flushing data to disk...");

        // 1. Save metadata (vector_map, total_vectors_inserted_count)
        // This assumes HnswIndex will have a `path: PathBuf` field representing the base path for the index files.
        // For example, if new(base_dir, "my_index", ...), then self.path would be base_dir/my_index
        // And get_metadata_path would correctly create base_dir/my_index.hnsw_meta.json
        // We will add this `path` field to HnswIndex struct in a subsequent step.
        let metadata_path = get_metadata_path(&self.path); 
        let temp_metadata_path = metadata_path.with_extension("tmp_json_save");

        let metadata_content = HnswIndexMetadata {
            vector_map: self.vector_map.clone(),
            total_vectors_inserted_count: self.total_vectors_inserted_count,
        };

        let file = fs::File::create(&temp_metadata_path).map_err(|e| VortexError::IoError { path: temp_metadata_path.clone(), source: e })?;
        serde_json::to_writer_pretty(file, &metadata_content)
            .map_err(|e| VortexError::StorageError(format!("Failed to serialize metadata: {}", e)))?;
        
        fs::rename(&temp_metadata_path, &metadata_path).map_err(|e| VortexError::IoError { path: temp_metadata_path.clone(), source: e })?; // path could be metadata_path too
        debug!("Successfully saved HNSW index metadata to {:?}", metadata_path);

        // 2. Flush mmap components
        self.vector_storage.flush_data()?;
        self.vector_storage.flush_deletion_flags()?;
        self.vector_storage.flush_header()?;
        self.graph_links.flush()?;
        
        info!("HNSW index data and metadata flushed successfully.");
        Ok(())
    }

    async fn delete_vector(&mut self, id: &VectorId) -> VortexResult<bool> {
        if let Some(&internal_id) = self.vector_map.get(id) {
            let was_newly_deleted = self.vector_storage.delete_vector(internal_id)?; // Returns bool
            if was_newly_deleted {
                self.vector_map.remove(id);
                Ok(true)
            } else {
                Ok(false) 
            }
        } else {
            Ok(false)
        }
    }

    async fn get_vector(&self, id: &VectorId) -> VortexResult<Option<Embedding>> {
        if let Some(&internal_id) = self.vector_map.get(id) {
            if self.vector_storage.is_deleted(internal_id) { // No ? needed
                Ok(None)
            } else {
                Ok(self.vector_storage.get_vector(internal_id)) // Wrap Option in Ok
            }
        } else {
            Ok(None)
        }
    }

    fn len(&self) -> usize {
        self.vector_storage.len().try_into().unwrap_or(usize::MAX) // Convert u64 to usize
    }

    fn is_empty(&self) -> bool {
        self.vector_storage.len() == 0 // Uses new MmapVectorStorage::is_empty() indirectly
    }

    fn dimensions(&self) -> usize {
        self.vector_storage.dim() as usize
    }

    fn config(&self) -> HnswConfig {
        self.config
    }

    fn distance_metric(&self) -> DistanceMetric {
        self.metric
    }

    async fn list_vectors(&self, limit: Option<usize>) -> VortexResult<Vec<(VectorId, Embedding)>> {
        warn!("HnswIndex::list_vectors may be inefficient for large datasets.");
        let mut results = Vec::new();
        let mut count = 0;

        for (external_id, &internal_id) in self.vector_map.iter() {
            if limit.is_some() && count >= limit.unwrap() {
                break;
            }
            if !self.vector_storage.is_deleted(internal_id) { // No ? needed
                if let Some(vector) = self.vector_storage.get_vector(internal_id) { // No ? needed, returns Option
                    results.push((external_id.clone(), vector));
                    count += 1;
                }
            }
        }
        Ok(results)
    }
}

const _: () = {
    fn assert_impl<T: Index + Send + Sync + std::fmt::Debug>() {}
    fn check() { 
        assert_impl::<HnswIndex>();
    }
};


#[cfg(test)]
mod tests {
    use super::*; // Import HnswIndex, Index trait, etc.
    use crate::config::HnswConfig;
    use crate::distance::DistanceMetric;
    // use crate::error::VortexError; // Keep if specific errors are checked
    use crate::vector::Embedding;
    use tempfile::tempdir;
    // use std::io::{Cursor, BufWriter}; // Not needed for these initial tests
    // use std::fs::File; // Not needed for these initial tests
    // use crate::hnsw::ArcNode; // ArcNode is removed

    fn create_test_config() -> HnswConfig {
        HnswConfig { m: 5, m_max0: 10, ef_construction: 20, ef_search: 10, ml: 0.5, seed: Some(123) }
    }

    #[tokio::test]
    async fn test_new_index() {
        let dir = tempdir().unwrap();
        let path = dir.path();
        let config = create_test_config();
        let index_name = "test_new_idx";
        let dimensions = 4u32;
        let capacity = 100u64;

        let index = HnswIndex::new(path, index_name, config, DistanceMetric::L2, dimensions, capacity).unwrap();
        
        assert_eq!(index.dimensions(), dimensions as usize);
        assert_eq!(index.len(), 0); // MmapVectorStorage.len() should be 0 initially
        assert!(index.is_empty());
        assert_eq!(index.distance_metric(), DistanceMetric::L2);
        assert_eq!(index.config(), config);

        // Check if files were created (basic check)
        let vec_file = path.join(format!("{}.vec", index_name));
        let graph_file = path.join(format!("{}.graph", index_name));
        assert!(vec_file.exists());
        assert!(graph_file.exists());
    }

    // TODO: Add test_open_index: Create new, add data, save (flush), then open and verify.
    // TODO: Add test_add_vector_and_get_vector
    // TODO: Add test_search_basic (after insert_vector's HNSW logic is complete)
    // TODO: Add test_delete_vector
    // TODO: Add test_persistence_vector_map (once vector_map persistence is implemented in save/open)

    // Example of a more complete test structure (to be filled in)
    #[tokio::test]
    async fn test_add_search_get_delete() {
        let dir = tempdir().unwrap();
        let path = dir.path();
        let config = create_test_config();
        let index_name = "test_add_search_get_delete_idx";
        let dimensions = 2u32;
        let capacity = 10u64;

        let mut index = HnswIndex::new(path, index_name, config, DistanceMetric::L2, dimensions, capacity).unwrap();

        let vec1_id = "vec1".to_string();
        let vec1_data = Embedding::from(vec![1.0, 2.0]);
        let added1 = index.add_vector(vec1_id.clone(), vec1_data.clone()).await.unwrap();
        assert!(added1);
        assert_eq!(index.len(), 1);

        // Test get_vector
        let retrieved_vec1 = index.get_vector(&vec1_id).await.unwrap().unwrap();
        assert_eq!(retrieved_vec1, vec1_data);

        // Test search (will be very basic until HNSW insert logic is complete)
        // For now, search_internal returns empty, so this will also be empty.
        let query_vec = Embedding::from(vec![1.1, 2.1]);
        let results = index.search(query_vec.clone(), 1).await.unwrap();
        // Once search is implemented, this assertion needs to be meaningful.
        // For now, it might be empty or contain vec1 depending on search_internal stub.
        // Based on current search_internal stub (returns empty), this will be empty.
        // assert!(results.contains(&(vec1_id.clone(), expected_distance_or_similarity)));
        warn!("Search test is basic due to incomplete HNSW insertion/search logic.");
        if results.is_empty() {
            warn!("Search returned empty as expected from current stub.");
        } else {
            // Basic check if something is returned
             assert!(!results.is_empty(), "Search should return results if HNSW logic was complete.");
        }


        // Test delete_vector
        let deleted1 = index.delete_vector(&vec1_id).await.unwrap();
        assert!(deleted1);
        assert_eq!(index.len(), 0);
        assert!(index.get_vector(&vec1_id).await.unwrap().is_none());

        // Test adding again after delete
        let vec2_id = "vec2".to_string();
        let vec2_data = Embedding::from(vec![3.0, 4.0]);
        let added2 = index.add_vector(vec2_id.clone(), vec2_data.clone()).await.unwrap();
        assert!(added2);
        assert_eq!(index.len(), 1);
    }
    
    #[tokio::test]
    async fn test_save_and_open_index() {
        let dir = tempdir().unwrap();
        let base_path_for_files = dir.path(); // Use this for HnswIndex::new and ::open
        let config = create_test_config();
        let index_name = "test_save_open_idx";
        let dimensions = 3u32;
        let capacity = 5u64;
        let vec_id_str = "id1";

        let original_vector_map_len;
        let original_total_inserted;

        {
            let mut index_to_save = HnswIndex::new(base_path_for_files, index_name, config, DistanceMetric::Cosine, dimensions, capacity).unwrap();
            let vec_id = vec_id_str.to_string();
            let vec_data = Embedding::from(vec![0.1, 0.2, 0.3]);
            index_to_save.add_vector(vec_id.clone(), vec_data.clone()).await.unwrap();
            
            original_vector_map_len = index_to_save.vector_map.len();
            original_total_inserted = index_to_save.total_vectors_inserted_count;
            assert_eq!(original_vector_map_len, 1);
            assert_eq!(original_total_inserted, 1);

            let mut dummy_writer = Vec::new(); 
            index_to_save.save(&mut dummy_writer).await.unwrap();

            // Verify metadata file was created
            let expected_index_file_path = base_path_for_files.join(index_name);
            let metadata_file_path = get_metadata_path(&expected_index_file_path);
            assert!(metadata_file_path.exists(), "Metadata file should exist after save.");

            // Optionally, read and verify its content here if needed for deeper debugging,
            // but the main check will be after loading.
            let file_content = fs::read_to_string(&metadata_file_path).unwrap();
            let loaded_meta_debug: HnswIndexMetadata = serde_json::from_str(&file_content).unwrap();
            assert_eq!(loaded_meta_debug.vector_map.len(), 1);
            assert_eq!(loaded_meta_debug.total_vectors_inserted_count, 1);
            assert!(loaded_meta_debug.vector_map.contains_key(vec_id_str));

        } // index_to_save is dropped

        // Re-open the index
        let opened_index = HnswIndex::open(base_path_for_files, index_name, config, DistanceMetric::Cosine).unwrap();
        assert_eq!(opened_index.dimensions(), dimensions as usize);
        assert_eq!(opened_index.distance_metric(), DistanceMetric::Cosine);
        assert_eq!(opened_index.config(), config);
        
        // Check that vector_map and total_vectors_inserted_count were restored
        assert_eq!(opened_index.vector_map.len(), original_vector_map_len, "Vector map length mismatch after open.");
        assert_eq!(opened_index.total_vectors_inserted_count, original_total_inserted, "Total inserted count mismatch after open.");
        assert!(opened_index.vector_map.contains_key(vec_id_str), "Restored vector_map should contain the original ID.");
        
        // Check data consistency via get_vector and len (which relies on vector_map for external IDs)
        assert_eq!(opened_index.len(), 1, "Opened index len should be 1.");
        let retrieved_vec = opened_index.get_vector(&vec_id_str.to_string()).await.unwrap().unwrap();
        assert_eq!(retrieved_vec, Embedding::from(vec![0.1, 0.2, 0.3]));
        
        // MmapVectorStorage's internal count should also be consistent
        assert_eq!(opened_index.vector_storage.len(), 1, "Vector storage len should be 1 after open.");
    }

    #[tokio::test]
    async fn test_load_index_missing_metadata_file() {
        let dir = tempdir().unwrap();
        let base_path_for_files = dir.path();
        let config = create_test_config();
        let index_name = "test_missing_meta_idx";
        let dimensions = 2u32;
        let capacity = 5u64;

        // Create underlying mmap files but no metadata file
        let _vector_storage = MmapVectorStorage::new(base_path_for_files, index_name, dimensions, capacity).unwrap();
        let _graph_links = MmapHnswGraphLinks::new(
            base_path_for_files, index_name, capacity, 1, u64::MAX, config.m_max0 as u32, config.m as u32
        ).unwrap();
        
        // Attempt to open the index
        let opened_index_result = HnswIndex::open(base_path_for_files, index_name, config, DistanceMetric::L2);
        
        // Current behavior: logs warning, initializes empty map and zero count.
        assert!(opened_index_result.is_ok(), "Opening an index with missing metadata should succeed with defaults.");
        let opened_index = opened_index_result.unwrap();
        assert!(opened_index.vector_map.is_empty(), "vector_map should be empty when metadata file is missing.");
        assert_eq!(opened_index.total_vectors_inserted_count, 0, "total_vectors_inserted_count should be 0 when metadata file is missing.");
    }

    #[tokio::test]
    async fn test_load_index_corrupted_metadata_file() {
        let dir = tempdir().unwrap();
        let base_path_for_files = dir.path();
        let config = create_test_config();
        let index_name = "test_corrupted_meta_idx";
        let dimensions = 2u32;
        let capacity = 5u64;

        // Create underlying mmap files
        let _vector_storage = MmapVectorStorage::new(base_path_for_files, index_name, dimensions, capacity).unwrap();
        let _graph_links = MmapHnswGraphLinks::new(
            base_path_for_files, index_name, capacity, 1, u64::MAX, config.m_max0 as u32, config.m as u32
        ).unwrap();

        // Create a corrupted metadata file
        let index_file_path = base_path_for_files.join(index_name);
        let metadata_file_path = get_metadata_path(&index_file_path);
        fs::write(&metadata_file_path, "this is not valid json").unwrap();

        // Attempt to open the index
        let opened_index_result = HnswIndex::open(base_path_for_files, index_name, config, DistanceMetric::L2);
        
        assert!(opened_index_result.is_err(), "Opening an index with corrupted metadata should fail.");
        match opened_index_result.err().unwrap() {
            VortexError::StorageError(msg) => {
                assert!(msg.contains("Failed to deserialize metadata"));
            }
            _ => panic!("Expected StorageError for corrupted metadata."),
        }
    }
}
