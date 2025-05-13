use crate::config::HnswConfig;
use crate::distance::DistanceMetric;
use crate::error::{VortexError, VortexResult};
use crate::hnsw::{self, ArcNode, Node, original_score};
use crate::vector::{Embedding, VectorId};
use crate::utils::{create_rng, generate_random_level};
use crate::distance::calculate_distance;

use async_trait::async_trait;
use ndarray::ArrayView1;
use rand::rngs::StdRng;
use serde::{Serialize, Deserialize};
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tracing::{debug, trace, warn, info, error};


/// The primary trait defining the vector index functionality.
#[async_trait]
pub trait Index: Send + Sync + std::fmt::Debug {
    async fn add_vector(&mut self, id: VectorId, vector: Embedding) -> VortexResult<bool>;
    async fn search(&self, query: Embedding, k: usize) -> VortexResult<Vec<(VectorId, f32)>>;
    async fn save(&self, writer: &mut (dyn Write + Send)) -> VortexResult<()>;
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

/// Data structure representing the HNSW index state for serialization.
#[derive(Serialize, Deserialize)]
struct HnswIndexData {
    config: HnswConfig,
    metric: DistanceMetric,
    dimensions: usize,
    nodes: Vec<Node>,
    vector_map: HashMap<VectorId, usize>,
    entry_point: Option<usize>,
    current_max_level: usize,
    deleted_count: usize,
}


/// Implementation of the `Index` trait using the HNSW algorithm.
#[derive(Debug)]
pub struct HnswIndex {
    config: HnswConfig,
    metric: DistanceMetric,
    dimensions: usize,
    nodes: Vec<ArcNode>,
    vector_map: HashMap<VectorId, usize>,
    entry_point: Option<usize>,
    current_max_level: usize,
    rng: StdRng,
    deleted_count: usize,
}

impl Serialize for HnswIndex {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let data = HnswIndexData {
            config: self.config,
            metric: self.metric,
            dimensions: self.dimensions,
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
            nodes: data.nodes.into_iter().map(Arc::new).collect(),
            vector_map: data.vector_map,
            entry_point: data.entry_point,
            current_max_level: data.current_max_level,
            rng: create_rng(data.config.seed),
            deleted_count: data.deleted_count,
        })
    }
}


impl HnswIndex {
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

    /// Loads an HNSW index from a reader.
    /// The dimensions of the index are determined from the loaded data.
    pub fn load(reader: &mut dyn Read) -> VortexResult<Self> {
        info!("Loading HNSW index from reader");
        let index: HnswIndex = bincode::deserialize_from(reader)
            .map_err(|e| VortexError::Deserialization(format!("Failed to deserialize index: {}", e)))?;
        
        // Dimension validation can be done by the caller if they have an expectation,
        // or simply trust the loaded dimensions. For server startup, we'll trust.
        info!(dimensions=index.dimensions, vector_count=index.len(), "Index loaded successfully");
        Ok(index)
    }

    /// Loads an HNSW index from a file path.
    /// The dimensions of the index are determined from the loaded data.
    pub fn load_from_path(path: &Path) -> VortexResult<Self> {
        info!(path=?path, "Loading HNSW index from path");
         let file = File::open(path).map_err(|e| VortexError::IoError { path: path.to_path_buf(), source: e })?;
         let mut reader = BufReader::new(file);
         Self::load(&mut reader)
    }

    fn get_node(&self, index: usize) -> Option<&ArcNode> {
        self.nodes.get(index)
    }

    fn search_internal(&self, query: ArrayView1<f32>, k: usize, ef_search: usize) -> VortexResult<Vec<(VectorId, f32)>> {
        let current_entry_point = match self.entry_point {
            Some(ep_idx) => {
                if let Some(ep_node) = self.get_node(ep_idx) {
                    if ep_node.deleted {
                        warn!("Search entry point node {} is marked deleted.", ep_idx);
                        return Ok(Vec::new());
                    }
                    ep_idx
                } else {
                     error!("Entry point index {} out of bounds!", ep_idx);
                     return Err(VortexError::Internal("Entry point index invalid".to_string()));
                }
            }
            None => return Ok(Vec::new()),
        };

        let mut current_best_neighbor_idx = current_entry_point;
        let top_level = self.current_max_level;

        for level in (1..=top_level).rev() {
            trace!(level, start_node=current_best_neighbor_idx, "Searching level (top-down)");
            let mut candidates = hnsw::search_layer(
                query,
                current_best_neighbor_idx,
                1, 
                level,
                &self.nodes,
                self.metric,
            )?;
            if let Some(best_candidate) = candidates.pop() {
                 current_best_neighbor_idx = best_candidate.index;
            } else {
                warn!(level, current_best_neighbor_idx, "Search layer returned no candidates during top-down traversal. Continuing with previous best.");
            }
        }

        trace!(start_node=current_best_neighbor_idx, ef_search, "Searching layer 0");
        let mut layer0_results_heap = hnsw::search_layer(
            query,
            current_best_neighbor_idx,
            ef_search,
            0,
            &self.nodes,
            self.metric,
        )?;

        let mut final_results = Vec::with_capacity(k);
        while let Some(neighbor) = layer0_results_heap.pop() {
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
        Ok(final_results)
    }

    fn insert_vector(&mut self, id: VectorId, vector: Embedding) -> VortexResult<()> {
        let node_level = generate_random_level(self.config.ml, &mut self.rng);
        let new_node_index = self.nodes.len();
        let new_node = Node::new(id.clone(), vector.clone(), node_level);

        trace!(vector_id=%id, node_index=new_node_index, level=node_level, "Inserting new vector");

        if self.entry_point.is_none() {
            self.nodes.push(Arc::new(new_node));
            self.vector_map.insert(id, new_node_index);
            self.entry_point = Some(new_node_index);
            self.current_max_level = node_level;
            debug!(node_index=new_node_index, level=node_level, "Set new node as entry point");
            return Ok(());
        }

        let mut current_ep_candidate_idx = self.find_valid_entry_point()?;
        let graph_top_level = self.current_max_level;
        let search_from_level = std::cmp::max(node_level, graph_top_level);
        let mut entry_points = vec![current_ep_candidate_idx; search_from_level + 1];

        for current_search_level in ((node_level + 1)..=search_from_level).rev() {
            trace!(level = current_search_level, start_node = current_ep_candidate_idx, "Finding entry point for level below (insertion)");
            let mut candidates = hnsw::search_layer(
                vector.view(),
                current_ep_candidate_idx,
                1, 
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
        
        for l in 0..=node_level {
            if l < entry_points.len() { 
                 entry_points[l] = current_ep_candidate_idx;
            }
        }

        for level in (0..=node_level).rev() {
            let ep_index = entry_points[level]; 
            trace!(level, start_node=ep_index, ef_construction=self.config.ef_construction, "Searching neighbors for connection");
            let neighbors_heap = hnsw::search_layer(
                vector.view(),
                ep_index,
                self.config.ef_construction,
                level,
                &self.nodes,
                self.metric,
            )?;
            let m = if level == 0 { self.config.m_max0 } else { self.config.m };
            let selected_neighbor_indices = hnsw::select_neighbors_heuristic(&neighbors_heap, m);
            trace!(level, count=selected_neighbor_indices.len(), "Selected neighbors");

            *new_node.connections[level].write() = selected_neighbor_indices.clone();

            let m_limit = if level == 0 { self.config.m_max0 } else { self.config.m };
            for &neighbor_idx in &selected_neighbor_indices {
                if let Some(neighbor_node) = self.nodes.get(neighbor_idx).cloned() { 
                    if level < neighbor_node.connections.len() {
                        let mut neighbor_connections = neighbor_node.connections[level].write();
                        neighbor_connections.push(new_node_index);
                        trace!(level, from=%id, to=neighbor_idx, "Added back-connection");

                        if neighbor_connections.len() > m_limit {
                            trace!(level, node=neighbor_idx, count=neighbor_connections.len(), limit=m_limit, "Pruning neighbor connections");
                            let mut candidates_with_dist: Vec<(f32, usize)> = Vec::with_capacity(neighbor_connections.len());
                            for &conn_idx in neighbor_connections.iter() {
                                if let Some(connected_node) = self.nodes.get(conn_idx) {
                                    let dist = calculate_distance(self.metric, neighbor_node.vector.view(), connected_node.vector.view())?;
                                    candidates_with_dist.push((dist, conn_idx));
                                } else {
                                     warn!(level, neighbor=neighbor_idx, missing_conn=conn_idx, "Could not find node for connection during pruning");
                                }
                            }
                            match self.metric {
                                DistanceMetric::L2 => candidates_with_dist.sort_unstable_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal)),
                                DistanceMetric::Cosine => candidates_with_dist.sort_unstable_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal)),
                            }
                            candidates_with_dist.truncate(m_limit);
                            neighbor_connections.clear();
                            neighbor_connections.extend(candidates_with_dist.into_iter().map(|(_, idx)| idx));
                            trace!(level, node=neighbor_idx, count=neighbor_connections.len(), "Pruning complete");
                        }
                    } else {
                         warn!(level, neighbor=neighbor_idx, neighbor_level=neighbor_node.level, "Neighbor node level too low for back-connection at current level");
                    }
                } else {
                     warn!(level, neighbor=neighbor_idx, "Could not find neighbor node during back-connection");
                }
            }
        }

        let node_arc = Arc::new(new_node);
        self.nodes.push(node_arc);
        self.vector_map.insert(id, new_node_index);

        if node_level > graph_top_level {
            self.current_max_level = node_level;
            self.entry_point = Some(new_node_index);
            debug!(node_index=new_node_index, level=node_level, "Updated global entry point");
        }
        Ok(())
    }

    fn find_valid_entry_point(&self) -> VortexResult<usize> {
        match self.entry_point {
            None => Err(VortexError::EmptyIndex),
            Some(ep_idx) => {
                if let Some(node) = self.get_node(ep_idx) {
                    if !node.deleted {
                        return Ok(ep_idx);
                    }
                }
                warn!("Current entry point {} is deleted or invalid. Trying fallback (not implemented).", ep_idx);
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
            warn!(vector_id=%id, index=existing_index, "Updating vector (simplified: replacing node)");
            let old_node = self.nodes.get(existing_index).ok_or_else(|| VortexError::Internal(format!("Vector map points to invalid index {}", existing_index)))?.clone();
            let mut updated_node_data = Node::new(id.clone(), vector, old_node.level);
            updated_node_data.deleted = false;
            updated_node_data.connections = old_node.connections.iter().map(|lock| parking_lot::RwLock::new(lock.read().clone())).collect();
            self.nodes[existing_index] = Arc::new(updated_node_data);

            if old_node.deleted {
                 if self.deleted_count > 0 { self.deleted_count -= 1; }
                 debug!(vector_id=%id, "Reactivated previously deleted node during update.");
            }
            Ok(false)
        } else {
            self.insert_vector(id, vector)?;
            Ok(true)
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
        let ef_search = std::cmp::max(k, self.config.ef_search);
        debug!(k, ef_search, "Performing search using config.ef_search");
        self.search_internal(query.view(), k, ef_search)
    }

    async fn search_with_ef(&self, query: Embedding, k: usize, ef_search_override: usize) -> VortexResult<Vec<(VectorId, f32)>> {
        if self.is_empty() {
            return Ok(Vec::new());
        }
        if query.len() != self.dimensions {
            return Err(VortexError::DimensionMismatch { expected: self.dimensions, actual: query.len() });
        }
        if k == 0 {
            return Ok(Vec::new());
        }
        // Ensure ef_search_override is at least k
        let ef_search = std::cmp::max(k, ef_search_override);
        debug!(k, ef_search_override = ef_search, "Performing search with overridden ef_search");
        self.search_internal(query.view(), k, ef_search)
    }

    async fn save(&self, writer: &mut (dyn Write + Send)) -> VortexResult<()> {
        info!(vector_count=self.len(), "Saving HNSW index to writer");
        let mut buf_writer = BufWriter::new(writer);
        bincode::serialize_into(&mut buf_writer, self)
            .map_err(|e| VortexError::Serialization(format!("Failed to serialize index: {}", e)))?;
        buf_writer.flush().map_err(|e| VortexError::IoError { path: PathBuf::from("<writer>"), source: e })?;
        info!("Index saved successfully");
        Ok(())
    }

    async fn delete_vector(&mut self, id: &VectorId) -> VortexResult<bool> {
        if let Some(&index) = self.vector_map.get(id) {
             if index >= self.nodes.len() {
                  error!(vector_id=%id, index, "Vector map contains invalid index");
                  return Err(VortexError::Internal("Invalid index found in vector map".to_string()));
             }
             let node_arc = self.nodes[index].clone();
             if node_arc.deleted {
                 return Ok(false); 
             }
             let mut modified_node_data = (*node_arc).clone();
             modified_node_data.deleted = true;
             self.nodes[index] = Arc::new(modified_node_data);
             self.deleted_count += 1;
             debug!(vector_id=%id, index, "Marked node as deleted (via clone-and-replace)");
             Ok(true)
        } else {
            Ok(false)
        }
    }

    async fn get_vector(&self, id: &VectorId) -> VortexResult<Option<Embedding>> {
        if let Some(&index) = self.vector_map.get(id) {
            if let Some(node) = self.nodes.get(index) {
                Ok(Some(node.vector.clone()))
            } else {
                Err(VortexError::Internal(format!("Vector ID {} found in map but not in node list at index {}", id, index)))
            }
        } else {
            Ok(None)
        }
    }

    fn len(&self) -> usize {
        self.nodes.len() - self.deleted_count
    }

    fn is_empty(&self) -> bool {
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

    async fn list_vectors(&self, limit: Option<usize>) -> VortexResult<Vec<(VectorId, Embedding)>> {
        debug!(limit = ?limit, total_nodes = self.nodes.len(), deleted_count = self.deleted_count, "Listing vectors");
        
        let mut results = Vec::new();
        let effective_limit = limit.unwrap_or(usize::MAX); // Use MAX if no limit

        for node in self.nodes.iter() {
            if results.len() >= effective_limit {
                break; // Stop if limit is reached
            }
            if !node.deleted {
                results.push((node.id.clone(), node.vector.clone()));
            }
        }
        
        debug!(listed_count = results.len(), "Finished listing vectors");
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
    use crate::config::HnswConfig;
    use crate::distance::DistanceMetric;
    use crate::error::VortexError;
    use crate::vector::Embedding; // VectorId is not directly used here, but through HnswIndex methods
    use crate::hnsw::ArcNode; 
    use std::io::{Cursor, BufWriter};
    use std::fs::File; // Added File
    use tempfile::tempdir;
    use super::Index; // To use HnswIndex as Index trait
    use super::HnswIndex; // To use HnswIndex struct directly

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

        let vec1_updated: Embedding = vec![1.5, 2.5].into();
        let updated1 = index.add_vector("vec1".to_string(), vec1_updated.clone()).await.unwrap();
        assert!(!updated1); 
        assert_eq!(index.len(), 2); 

        let retrieved = index.get_vector(&"vec1".to_string()).await.unwrap().unwrap();
        assert_eq!(retrieved, vec1_updated);
    }

    #[tokio::test]
    async fn test_add_identical_embeddings_different_ids() {
        let config = create_test_config();
        let mut index = HnswIndex::new(config, DistanceMetric::L2, 2).unwrap();
        let vec_data: Embedding = vec![1.0, 1.0].into();

        assert!(index.add_vector("id1".to_string(), vec_data.clone()).await.unwrap());
        assert_eq!(index.len(), 1);
        assert!(index.add_vector("id2".to_string(), vec_data.clone()).await.unwrap());
        assert_eq!(index.len(), 2);

        let retrieved1 = index.get_vector(&"id1".to_string()).await.unwrap().unwrap();
        let retrieved2 = index.get_vector(&"id2".to_string()).await.unwrap().unwrap();
        assert_eq!(retrieved1, vec_data);
        assert_eq!(retrieved2, vec_data);
    }

    #[tokio::test]
    async fn test_add_far_and_close_vectors() {
        let config = create_test_config();
        let mut index = HnswIndex::new(config, DistanceMetric::L2, 2).unwrap();
        
        index.add_vector("vec_origin".to_string(), vec![0.0, 0.0].into()).await.unwrap();
        index.add_vector("vec_close".to_string(), vec![0.1, 0.1].into()).await.unwrap();
        index.add_vector("vec_far".to_string(), vec![100.0, 100.0].into()).await.unwrap();
        
        assert_eq!(index.len(), 3);
        let results_near_origin = index.search(vec![0.0, 0.0].into(), 3).await.unwrap();
        assert_eq!(results_near_origin.len(), 3);
        
        let results_near_far = index.search(vec![100.0, 100.0].into(), 1).await.unwrap();
        assert_eq!(results_near_far.len(), 1);
        assert_eq!(results_near_far[0].0, "vec_far");
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

        index.add_vector("vec0".to_string(), vec![0.0, 0.0].into()).await.unwrap();
        index.add_vector("vec1".to_string(), vec![1.0, 1.0].into()).await.unwrap();
        index.add_vector("vec2".to_string(), vec![2.0, 2.0].into()).await.unwrap();
        index.add_vector("vec10".to_string(), vec![10.0, 10.0].into()).await.unwrap();

        let query: Embedding = vec![1.1, 1.1].into();
        let results = index.search(query, 2).await.unwrap();

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].0, "vec1"); 
        assert!(results[0].1 < 1.0); 
        assert!(results[1].0 == "vec0" || results[1].0 == "vec2");
        assert!(results[1].1 >= results[0].1);
    }

     #[tokio::test]
    async fn test_search_basic_cosine() {
        let config = create_test_config();
        let mut index = HnswIndex::new(config, DistanceMetric::Cosine, 3).unwrap();

        index.add_vector("vecA".to_string(), vec![1.0, 0.0, 0.0].into()).await.unwrap();
        index.add_vector("vecB".to_string(), vec![0.9, 0.1, 0.0].into()).await.unwrap(); 
        index.add_vector("vecC".to_string(), vec![0.0, 1.0, 0.0].into()).await.unwrap(); 
        index.add_vector("vecD".to_string(), vec![-1.0, 0.0, 0.0].into()).await.unwrap(); 

        let query: Embedding = vec![1.0, 0.01, 0.0].into(); 
        let results = index.search(query, 3).await.unwrap();

        assert_eq!(results.len(), 3);
        assert_eq!(results[0].0, "vecA"); 
        assert!(results[0].1 > 0.99);   
        assert_eq!(results[1].0, "vecB");
        assert!(results[1].1 > 0.8 && results[1].1 < results[0].1);
        assert_eq!(results[2].0, "vecC"); 
        assert!(results[2].1 > -0.1 && results[2].1 < 0.1); 
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
    async fn test_search_with_vector_in_index() {
        let config = create_test_config();
        let mut index = HnswIndex::new(config, DistanceMetric::L2, 2).unwrap();
        let vec1_data: Embedding = vec![1.0, 2.0].into();
        index.add_vector("vec1".to_string(), vec1_data.clone()).await.unwrap();
        index.add_vector("vec2".to_string(), vec![3.0, 4.0].into()).await.unwrap();

        let results = index.search(vec1_data.clone(), 1).await.unwrap(); 
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, "vec1");
        assert!((results[0].1 - 0.0).abs() < 1e-6); 
    }

    #[tokio::test]
    async fn test_search_with_zero_query_vector() {
        let config = create_test_config();
        let mut index = HnswIndex::new(config, DistanceMetric::L2, 2).unwrap();
        index.add_vector("vec1".to_string(), vec![1.0, 2.0].into()).await.unwrap();
        let zero_query: Embedding = vec![0.0, 0.0].into();
        
        let results_l2 = index.search(zero_query.clone(), 1).await.unwrap();
        assert_eq!(results_l2.len(), 1); 
        assert_eq!(results_l2[0].0, "vec1");
        assert!((results_l2[0].1 - (1.0f32.powi(2) + 2.0f32.powi(2)).sqrt()).abs() < 1e-6);

        let mut index_cosine = HnswIndex::new(config, DistanceMetric::Cosine, 2).unwrap();
        index_cosine.add_vector("vec1".to_string(), vec![1.0, 2.0].into()).await.unwrap();
        let results_cosine = index_cosine.search(zero_query.clone(), 1).await.unwrap(); 
        assert_eq!(results_cosine.len(), 1);
        assert!((results_cosine[0].1 - 0.0).abs() < 1e-6);
    }

    #[tokio::test]
    async fn test_save_load_path() {
        let config = create_test_config();
        let mut index = HnswIndex::new(config, DistanceMetric::Cosine, 3).unwrap();
        let dir = tempdir().unwrap();
        let path = dir.path().join("test_index_path.bin");

        index.add_vector("vecA".to_string(), vec![1.0, 0.0, 0.0].into()).await.unwrap();
        index.add_vector("vecB".to_string(), vec![0.0, 1.0, 0.0].into()).await.unwrap();

        let file = File::create(&path).unwrap();
        let mut writer = BufWriter::new(file);
        index.save(&mut writer).await.unwrap();
        drop(writer); 

        let loaded_index = HnswIndex::load_from_path(&path).unwrap();
        assert_eq!(loaded_index.dimensions(), 3); // Dimensions are inherent in the loaded index
        assert_eq!(loaded_index.len(), 2);
        assert_eq!(loaded_index.config(), config);
        assert_eq!(loaded_index.distance_metric(), DistanceMetric::Cosine);

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
        index.save(&mut buffer).await.unwrap(); 
        assert!(!buffer.is_empty());

        let mut reader = Cursor::new(buffer); 
        let loaded_index = HnswIndex::load(&mut reader).unwrap(); 
        assert_eq!(loaded_index.dimensions(), 3); // Dimensions are inherent
        assert_eq!(loaded_index.len(), 2);

        let query: Embedding = vec![0.1, 0.9, 0.0].into();
        let results = loaded_index.search(query, 1).await.unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, "vecB");
    }

     #[tokio::test]
    async fn test_load_dimension_mismatch() {
        // This test is no longer relevant as load doesn't take expected_dimensions.
        // The server or caller would be responsible for any dimension validation if needed post-load.
        // For example, if loading into an existing system that expects a certain dimension.
        // However, for simple load, the loaded dimension is the source of truth.
        
        // We can test that the loaded dimension is correct.
        let config = create_test_config();
        let mut index_orig = HnswIndex::new(config, DistanceMetric::L2, 3).unwrap();
        index_orig.add_vector("vecA".to_string(), vec![1.0, 0.0, 0.0].into()).await.unwrap();

        let mut buffer: Vec<u8> = Vec::new();
        index_orig.save(&mut buffer).await.unwrap();

        let mut reader = Cursor::new(buffer);
        let loaded_index = HnswIndex::load(&mut reader).unwrap(); 
        assert_eq!(loaded_index.dimensions(), 3); // Check if loaded dimension is correct
    }

    fn get_node_connections(node: &ArcNode, level: usize) -> Vec<usize> {
        if level < node.connections.len() {
            node.connections[level].read().clone()
        } else {
            Vec::new()
        }
    }

    #[tokio::test]
    async fn test_bidirectional_connections_and_pruning() {
        let config = HnswConfig { m: 2, m_max0: 4, ef_construction: 10, ef_search: 10, ml: 0.5, seed: Some(42) };
        let mut index = HnswIndex::new(config, DistanceMetric::L2, 2).unwrap();

        index.add_vector("vec0".to_string(), vec![0.0, 0.0].into()).await.unwrap();
        let node0_idx = *index.vector_map.get("vec0").unwrap();

        index.add_vector("vec1".to_string(), vec![0.1, 0.1].into()).await.unwrap();
        let node1_idx = *index.vector_map.get("vec1").unwrap();

        index.add_vector("vec2".to_string(), vec![0.2, 0.2].into()).await.unwrap();
        let _node2_idx = *index.vector_map.get("vec2").unwrap(); 

        let node0_level0_conns = get_node_connections(&index.nodes[node0_idx], 0);
        let node1_level0_conns = get_node_connections(&index.nodes[node1_idx], 0);

        if node0_level0_conns.contains(&node1_idx) {
            assert!(node1_level0_conns.contains(&node0_idx), "vec1 should connect back to vec0 if vec0 connects to vec1 on layer 0");
        }
        if node1_level0_conns.contains(&node0_idx) {
            assert!(node0_level0_conns.contains(&node1_idx), "vec0 should connect back to vec1 if vec1 connects to vec0 on layer 0");
        }

        index.add_vector("vec3".to_string(), vec![0.3, 0.0].into()).await.unwrap();
        index.add_vector("vec4".to_string(), vec![0.0, 0.4].into()).await.unwrap();
        index.add_vector("vec5".to_string(), vec![-0.1, 0.0].into()).await.unwrap(); 

        let node0_level0_conns_after_pruning = get_node_connections(&index.nodes[node0_idx], 0);
        assert!(node0_level0_conns_after_pruning.len() <= config.m_max0,
                "vec0 layer 0 connections ({}) should be pruned to at most M_max0 ({})",
                node0_level0_conns_after_pruning.len(), config.m_max0);

        let node5_idx = *index.vector_map.get("vec5").unwrap();
        let node5_level0_conns = get_node_connections(&index.nodes[node5_idx], 0);
        assert!(node5_level0_conns.len() <= config.m_max0, "vec5 layer 0 connections should be at most M_max0");

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

        let deleted = index.delete_vector(&"vec2".to_string()).await.unwrap();
        assert!(deleted);
        assert_eq!(index.len(), 2); 
        assert_eq!(index.deleted_count, 1);
        assert_eq!(index.nodes.len(), 3); 

        let deleted_again = index.delete_vector(&"vec2".to_string()).await.unwrap();
        assert!(!deleted_again); 
        assert_eq!(index.len(), 2);
        assert_eq!(index.deleted_count, 1);

        let deleted_non_existent = index.delete_vector(&"vec4".to_string()).await.unwrap();
        assert!(!deleted_non_existent);
        assert_eq!(index.len(), 2);
        assert_eq!(index.deleted_count, 1);

        let retrieved_deleted = index.get_vector(&"vec2".to_string()).await.unwrap();
        assert!(retrieved_deleted.is_some()); 

        let query: Embedding = vec![2.1, 2.1].into();
        let results = index.search(query.clone(), 3).await.unwrap(); 

        assert_eq!(results.len(), 2); 
        assert!(results.iter().any(|(id, _)| id == "vec1"));
        assert!(results.iter().any(|(id, _)| id == "vec3"));
        assert!(!results.iter().any(|(id, _)| id == "vec2")); 

        let vec2_node_idx = index.vector_map.get("vec2").unwrap();
        assert!(index.nodes[*vec2_node_idx].deleted);

        let updated_deleted = index.add_vector("vec2".to_string(), vec![2.5, 2.5].into()).await.unwrap();
        assert!(!updated_deleted); 
        assert_eq!(index.len(), 3); 
        assert_eq!(index.deleted_count, 0); 
        let vec2_node_idx = index.vector_map.get("vec2").unwrap();
        assert!(!index.nodes[*vec2_node_idx].deleted); 

        let results_after_update = index.search(query, 3).await.unwrap();
        assert_eq!(results_after_update.len(), 3);
        assert!(results_after_update.iter().any(|(id, _)| id == "vec2"));
    }
}
