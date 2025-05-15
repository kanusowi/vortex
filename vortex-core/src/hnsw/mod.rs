pub mod node; // Declare the node module
// pub use node::{Node, ArcNode}; // Node and ArcNode are removed
pub mod builder; // Keep builder internal details separate if needed

use crate::distance::DistanceMetric;
use crate::error::{VortexError, VortexResult};
use crate::distance::calculate_distance;
use crate::storage::mmap_vector_storage::MmapVectorStorage; // Corrected path
use crate::storage::mmap_hnsw_graph_links::MmapHnswGraphLinks; // Corrected path

use std::collections::{BinaryHeap, HashSet};
use std::cmp::Ordering;
use ndarray::ArrayView1;

// --- Data Structures for Search ---

/// Represents a single search result.
#[derive(Debug, Clone, PartialEq)]
pub struct SearchResult { // Made pub
    pub id: String, // Assuming VectorId is String
    pub distance: f32,
}

/// Represents an item in the priority queue used during search.
/// Stores (distance, node_index). Ordered by distance.
#[derive(PartialEq, Debug, Clone, Copy)]
pub(crate) struct Neighbor {
    pub distance: f32, // This is the heap_score (e.g., -L2_distance or Cosine_similarity)
    pub internal_id: u64, // Changed from usize to u64
}

// Implement Eq and Ord for Neighbor to use it in BinaryHeap (Max-Heap behavior)
impl Eq for Neighbor {}

impl Ord for Neighbor {
    fn cmp(&self, other: &Self) -> Ordering {
        self.distance.partial_cmp(&other.distance).unwrap_or(Ordering::Equal)
    }
}

impl PartialOrd for Neighbor {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

// Wrapper for Neighbor to make BinaryHeap behave as a Min-Heap
#[derive(PartialEq, Debug, Clone, Copy)]
struct MinHeapNeighbor(Neighbor);

impl Eq for MinHeapNeighbor {}

impl Ord for MinHeapNeighbor {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reverse ordering for Min-Heap behavior
        other.0.distance.partial_cmp(&self.0.distance).unwrap_or(Ordering::Equal)
    }
}

impl PartialOrd for MinHeapNeighbor {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}


// Helper function to get the effective score for heap ordering
#[inline]
pub(crate) fn heap_score(metric: DistanceMetric, score: f32) -> f32 {
    match metric {
        DistanceMetric::L2 => -score, 
        DistanceMetric::Cosine => score, 
    }
}

// Helper function to get the original score from the heap score
#[inline]
pub(crate) fn original_score(metric: DistanceMetric, heap_score: f32) -> f32 {
     match metric {
        DistanceMetric::L2 => -heap_score, 
        DistanceMetric::Cosine => heap_score,
    }
}


// --- Search Algorithms ---

pub(crate) fn search_layer(
    query_vector: ArrayView1<f32>,
    entry_point_id: u64, 
    ef: usize, 
    layer_idx: u16, 
    vector_storage: &MmapVectorStorage,
    graph_links: &MmapHnswGraphLinks,
    distance_metric: DistanceMetric,
) -> VortexResult<BinaryHeap<Neighbor>> { 

    let mut visited_ids: HashSet<u64> = HashSet::new();
    // results_heap will store the top 'ef' candidates.
    // It's a Min-Heap of Neighbors (higher score is better, so min-heap keeps worst of the best at top)
    let mut results_heap: BinaryHeap<MinHeapNeighbor> = BinaryHeap::new(); 
    // explore_queue is a Max-Heap of Neighbors, to explore best candidates first.
    let mut explore_queue: BinaryHeap<Neighbor> = BinaryHeap::new(); 

    if vector_storage.is_deleted(entry_point_id) { 
        return Err(VortexError::Internal(format!(
            "search_layer called with a deleted entry point ID: {}. Layer: {}", entry_point_id, layer_idx
        )));
    }
    
    let entry_vector = vector_storage.get_vector(entry_point_id) 
        .ok_or_else(|| VortexError::Internal(format!( 
            "Entry point ID {} not found in vector storage for layer {}.", entry_point_id, layer_idx
    )))?; 

    let dist = calculate_distance(distance_metric, query_vector, entry_vector.view())?;
    let score = heap_score(distance_metric, dist);

    visited_ids.insert(entry_point_id);
    let initial_neighbor = Neighbor { distance: score, internal_id: entry_point_id };
    results_heap.push(MinHeapNeighbor(initial_neighbor));
    explore_queue.push(initial_neighbor);

    while let Some(current_best_to_explore) = explore_queue.pop() {
        // If the best candidate to explore is already worse than the worst in our current results_heap (when full),
        // we can stop. results_heap.peek() gives the MinHeapNeighbor with the smallest score.
        if results_heap.len() >= ef {
            if let Some(worst_in_results) = results_heap.peek() {
                if current_best_to_explore.distance < worst_in_results.0.distance {
                    break; 
                }
            }
        }

        let neighbor_connection_ids_slice = graph_links.get_connections(current_best_to_explore.internal_id, layer_idx)
            .ok_or_else(|| VortexError::Internal(format!(
                "Failed to get connections for node {} at layer {}. Node might be out of bounds or layer invalid.",
                current_best_to_explore.internal_id, layer_idx
            )))?;

        for &neighbor_id in neighbor_connection_ids_slice { 
            if !visited_ids.contains(&neighbor_id) {
                visited_ids.insert(neighbor_id);

                if vector_storage.is_deleted(neighbor_id) { 
                    continue; 
                }

                let neighbor_vector = vector_storage.get_vector(neighbor_id) 
                    .ok_or_else(|| VortexError::Internal(format!( 
                        "Neighbor ID {} (from graph links) not found in vector storage for layer {}.", neighbor_id, layer_idx
                )))?; 

                let neighbor_dist = calculate_distance(distance_metric, query_vector, neighbor_vector.view())?;
                let neighbor_heap_score = heap_score(distance_metric, neighbor_dist);
                
                let new_neighbor_candidate = Neighbor { distance: neighbor_heap_score, internal_id: neighbor_id };

                if results_heap.len() < ef {
                    results_heap.push(MinHeapNeighbor(new_neighbor_candidate));
                    explore_queue.push(new_neighbor_candidate);
                } else {
                    // results_heap is full (size ef).
                    // If new candidate is better than the worst in results_heap (peek of min-heap), replace.
                    if let Some(worst_of_best) = results_heap.peek() {
                        if new_neighbor_candidate.distance > worst_of_best.0.distance {
                            results_heap.pop(); // Remove the current worst
                            results_heap.push(MinHeapNeighbor(new_neighbor_candidate));
                            explore_queue.push(new_neighbor_candidate); // Still explore this better candidate
                        }
                    }
                }
            }
        }
    }
    // Convert MinHeapNeighbor back to Neighbor for the final result
    let final_results: BinaryHeap<Neighbor> = results_heap.into_iter().map(|min_heap_neighbor| min_heap_neighbor.0).collect();
    Ok(final_results)
}

pub(crate) fn select_neighbors_heuristic(
    candidates: &BinaryHeap<Neighbor>, 
    m: usize,
) -> Vec<u64> { 
    let mut best_first: Vec<_> = candidates.iter().cloned().collect();
    best_first.sort_by(|a, b| b.cmp(a)); // Sorts by distance descending (best first)

    best_first.iter().map(|n| n.internal_id).take(m).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distance::DistanceMetric;
    use crate::vector::Embedding;
    use std::path::Path; 
    use crate::storage::mmap_vector_storage::MmapVectorStorage;
    use crate::storage::mmap_hnsw_graph_links::MmapHnswGraphLinks;
    use tempfile::tempdir;
    use ndarray::array;

    fn create_test_vector_storage(path: &Path, dim: u32, capacity: u64) -> MmapVectorStorage {
        MmapVectorStorage::new(path, "test_vectors_hnsw_mod", dim, capacity).unwrap()
    }

    fn create_test_graph_links(path: &Path, capacity: u64, m0: u32, m: u32, initial_layers: u16) -> MmapHnswGraphLinks {
        MmapHnswGraphLinks::new(path, "test_graph_hnsw_mod", capacity, initial_layers, u64::MAX, m0, m).unwrap()
    }

    #[test]
    fn test_select_neighbors_heuristic() {
        let mut candidates = BinaryHeap::new();
        candidates.push(Neighbor { distance: 0.9, internal_id: 0 });
        candidates.push(Neighbor { distance: 0.8, internal_id: 1 });
        candidates.push(Neighbor { distance: 0.95, internal_id: 2 });
        candidates.push(Neighbor { distance: 0.7, internal_id: 3 });
        candidates.push(Neighbor { distance: 0.85, internal_id: 4 });

        let m = 3;
        let selected = select_neighbors_heuristic(&candidates, m);
        
        assert_eq!(selected.len(), m);
        assert_eq!(selected, vec![2, 0, 4]); // Max-heap means 2 (0.95) is best, then 0 (0.9), then 4 (0.85)

        let m_larger = 10;
        let selected_larger = select_neighbors_heuristic(&candidates, m_larger);
        assert_eq!(selected_larger.len(), candidates.len()); 
        assert_eq!(selected_larger, vec![2, 0, 4, 1, 3]);
    }

    #[test]
    fn test_search_layer_basic() {
        let dir = tempdir().unwrap();
        let path = dir.path();
        let dim = 2;
        let capacity = 5;
        let metric = DistanceMetric::L2;

        let mut vs = create_test_vector_storage(path, dim, capacity);
        // Set m0 = 3 to allow 3 connections for layer 0
        let mut gl = create_test_graph_links(path, capacity, 3, 2, 1); 

        vs.put_vector(0, &Embedding::from(vec![1.0, 1.0])).unwrap();
        vs.put_vector(1, &Embedding::from(vec![2.0, 2.0])).unwrap();
        vs.put_vector(2, &Embedding::from(vec![1.0, 2.0])).unwrap();
        vs.put_vector(3, &Embedding::from(vec![10.0, 10.0])).unwrap();
        vs.put_vector(4, &Embedding::from(vec![0.0, 0.0])).unwrap();
        
        gl.set_entry_point_node_id(0).unwrap(); 
        gl.set_num_layers(1).unwrap(); 

        // Node 0 connects to 1, 2, 4 (3 connections, now allowed by m0=3)
        gl.set_connections(0, 0, &[1, 2, 4]).unwrap(); 
        gl.set_connections(1, 0, &[0, 2]).unwrap();
        gl.set_connections(2, 0, &[0, 1]).unwrap();
        gl.set_connections(4, 0, &[0]).unwrap();

        let query_vec = array![0.9f32, 0.9f32]; 
        let ef = 3; 

        let result_heap = search_layer(query_vec.view(), 0, ef, 0, &vs, &gl, metric).unwrap();
        // The result_heap is a Max-Heap of Neighbors. For assertions, we sort it.
        let mut results: Vec<Neighbor> = result_heap.into_sorted_vec(); 
        // into_sorted_vec sorts from smallest to largest (worst to best for max-heap).
        // We want best first for assertions.
        results.reverse(); 
        
        // Expected order for query [0.9, 0.9] with L2 (lower distance is better, higher heap_score is better):
        // Node 0: [1.0, 1.0], dist sqrt((0.1)^2 + (0.1)^2) = sqrt(0.02) = 0.1414, heap_score -0.1414
        // Node 4: [0.0, 0.0], dist sqrt((0.9)^2 + (0.9)^2) = sqrt(1.62) = 1.2728, heap_score -1.2728
        // Node 2: [1.0, 2.0], dist sqrt((0.1)^2 + (-1.1)^2) = sqrt(0.01 + 1.21) = sqrt(1.22) = 1.1045, heap_score -1.1045
        // Node 1: [2.0, 2.0], dist sqrt((-1.1)^2 + (-1.1)^2) = sqrt(1.21 + 1.21) = sqrt(2.42) = 1.5556, heap_score -1.5556
        //
        // Expected sorted by heap_score (descending): 0, 2, 4
        assert_eq!(results.len(), ef, "Initial search: Should return ef results"); 
        assert_eq!(results[0].internal_id, 0, "Initial search: First result should be node 0"); 
        assert_eq!(results[1].internal_id, 2, "Initial search: Second result should be node 2"); 
        assert_eq!(results[2].internal_id, 4, "Initial search: Third result should be node 4");

        // Test with a deleted entry point
        let mut vs_del_ep = create_test_vector_storage(path, dim, capacity);
        vs_del_ep.put_vector(0, &Embedding::from(vec![1.0, 1.0])).unwrap(); // Re-add to new storage
        vs_del_ep.delete_vector(0).unwrap(); 
        let search_deleted_ep = search_layer(query_vec.view(), 0, ef, 0, &vs_del_ep, &gl, metric);
        assert!(matches!(search_deleted_ep, Err(VortexError::Internal(_))), "Search with deleted EP should fail");
        
        // Test with a deleted neighbor
        let mut vs_del_neighbor = create_test_vector_storage(path, dim, capacity);
        vs_del_neighbor.put_vector(0, &Embedding::from(vec![1.0, 1.0])).unwrap();
        vs_del_neighbor.put_vector(1, &Embedding::from(vec![2.0, 2.0])).unwrap();
        // Node 2 is NOT added to vs_del_neighbor, effectively "deleted" for this test part,
        // but we need to ensure it's explicitly marked deleted if it was part of the graph logic.
        // The current test setup for deleted neighbor is:
        // vs_del_neighbor.put_vector(2, &Embedding::from(vec![1.0, 2.0])).unwrap(); // This was present
        // vs_del_neighbor.delete_vector(2).unwrap(); // Then deleted
        // Let's keep this pattern.
        vs_del_neighbor.put_vector(2, &Embedding::from(vec![1.0, 2.0])).unwrap(); 
        vs_del_neighbor.put_vector(4, &Embedding::from(vec![0.0, 0.0])).unwrap(); 
        vs_del_neighbor.delete_vector(2).unwrap(); // Delete node 2

        // Graph links (gl) are reused, entry point is 0 (not deleted in vs_del_neighbor)
        // Connections from 0 are still [1, 2, 4]. Node 2 will be skipped by search_layer.
        let result_heap_del_neighbor = search_layer(query_vec.view(), 0, ef, 0, &vs_del_neighbor, &gl, metric).unwrap();
        let mut results_del_neighbor: Vec<Neighbor> = result_heap_del_neighbor.into_sorted_vec();
        results_del_neighbor.reverse();
        
        // Expected order for query [0.9,0.9] with L2, node 2 deleted:
        // Node 0: heap_score -0.1414
        // Node 4: heap_score -1.2728
        // Node 1: heap_score -1.5556
        assert_eq!(results_del_neighbor.len(), ef, "Search with deleted neighbor: Should return ef results");
        assert_eq!(results_del_neighbor[0].internal_id, 0, "Search with deleted neighbor: First result should be node 0");
        assert_eq!(results_del_neighbor[1].internal_id, 4, "Search with deleted neighbor: Second result should be node 4");
        assert_eq!(results_del_neighbor[2].internal_id, 1, "Search with deleted neighbor: Third result should be node 1");
        assert!(!results_del_neighbor.iter().any(|n| n.internal_id == 2), "Deleted node 2 should not be in results");
    }
}
