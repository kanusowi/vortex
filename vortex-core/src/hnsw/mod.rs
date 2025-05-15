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
// Duplicate import blocks removed.

/// Represents an item in the priority queue used during search.
/// Stores (distance, node_index). Ordered by distance.
#[derive(PartialEq, Debug, Clone, Copy)]
pub(crate) struct Neighbor {
    pub distance: f32, // This is the heap_score (e.g., -L2_distance or Cosine_similarity)
    pub internal_id: u64, // Changed from usize to u64
}

// Implement Eq and Ord for Neighbor to use it in BinaryHeap
// We want a min-heap based on distance for finding closest neighbors (L2)
// Or a max-heap based on similarity (Cosine)
// The default BinaryHeap is a max-heap. To make it behave as a min-heap for L2,
// we reverse the comparison. For Cosine, the natural max-heap order is correct.
impl Eq for Neighbor {}

impl Ord for Neighbor {
    fn cmp(&self, other: &Self) -> Ordering {
        // Use partial_cmp for f32, handle potential NaN (though unlikely here)
        self.distance.partial_cmp(&other.distance).unwrap_or(Ordering::Equal)
        // For a min-heap (L2): other.distance.partial_cmp(&self.distance)...
        // For a max-heap (Cosine): self.distance.partial_cmp(&other.distance)...
        // Let's make the heap always store Neighbors such that peek() gives the "worst" candidate
        // relative to the search goal (farthest for L2, least similar for Cosine).
        // This means the heap should be a Max-Heap for L2 distance, and Min-Heap for Cosine Similarity.
        // BinaryHeap is Max-Heap by default. So:
        // - For L2 (lower is better): Store distance directly. Heap keeps largest distance at top. Correct.
        // - For Cosine (higher is better): Store similarity directly. Heap keeps largest similarity at top. Correct.
        // The `pop()` operation will yield the "best" according to the heap's ordering (max value).

        // Let's rethink: search_layer needs to maintain the `ef` *best* candidates found so far.
        // A Min-Heap (for L2) or Max-Heap (for Cosine) is suitable for the `candidates` exploration queue.
        // A Max-Heap (for L2) or Min-Heap (for Cosine) is suitable for the `result_heap` tracking the best `ef` results.

        // Let's simplify: Use ONE heap definition. BinaryHeap is a Max-Heap.
        // - For L2: Store negative distance (-d). Max-heap on -d is min-heap on d. `peek` gives largest -d (smallest d). `pop` gives largest -d (smallest d).
        // - For Cosine: Store similarity (s). Max-heap on s. `peek` gives largest s. `pop` gives largest s.

        // Let's stick to storing the raw score (distance or similarity) and define Ord based on metric later if needed.
        // The current Ord makes BinaryHeap a Max-Heap based on the stored `distance` value.
        // This is suitable for Cosine directly. For L2, we need to manually manage it or store negative distance.
        // Storing negative distance seems cleaner.

        // **Decision:** We will adjust the distance value stored in Neighbor based on the metric
        // to always allow using BinaryHeap as a Max-Heap where popping yields the *best* neighbor.
        // - L2: Store `-distance`. Max-heap pops highest value -> smallest distance.
        // - Cosine: Store `similarity`. Max-heap pops highest value -> highest similarity.
        // This requires adjusting distance calculation before pushing to heap.
    }
}

impl PartialOrd for Neighbor {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

// Helper function to get the effective score for heap ordering
#[inline]
pub(crate) fn heap_score(metric: DistanceMetric, score: f32) -> f32 { // Made pub(crate)
    match metric {
        DistanceMetric::L2 => -score, // Negate distance for max-heap to act as min-heap
        DistanceMetric::Cosine => score, // Use similarity directly for max-heap
    }
}

// Helper function to get the original score from the heap score
#[inline]
pub(crate) fn original_score(metric: DistanceMetric, heap_score: f32) -> f32 { // Made pub(crate)
     match metric {
        DistanceMetric::L2 => -heap_score, // Reverse negation
        DistanceMetric::Cosine => heap_score,
    }
}


// --- Search Algorithms ---

/// Finds the nearest neighbors in a specific layer.
/// Implements the SEARCH-LAYER algorithm from the HNSW paper.
/// Returns a Max-Heap ordered by "best" score (highest similarity or lowest distance).
pub(crate) fn search_layer(
    query_vector: ArrayView1<f32>,
    entry_point_id: u64, // Changed from usize
    ef: usize, // Number of candidates to track (ef_search or ef_construction)
    layer_idx: u16, // Changed from u32 to u16 for consistency
    vector_storage: &MmapVectorStorage,
    graph_links: &MmapHnswGraphLinks,
    distance_metric: DistanceMetric,
) -> VortexResult<BinaryHeap<Neighbor>> { // Neighbor.index is now internal_id (u64)

    let mut visited_ids: HashSet<u64> = HashSet::new();
    let mut results_heap: BinaryHeap<Neighbor> = BinaryHeap::new(); // Max-Heap (best score on top)
    let mut explore_queue: BinaryHeap<Neighbor> = BinaryHeap::new(); // Max-Heap (best score on top)

    // Check if entry point is valid and not deleted
    if vector_storage.is_deleted(entry_point_id) { // Removed ?
        return Err(VortexError::Internal(format!(
            "search_layer called with a deleted entry point ID: {}. Layer: {}", entry_point_id, layer_idx
        )));
    }
    
    let entry_vector_opt = vector_storage.get_vector(entry_point_id); // Removed ?
    let entry_vector = entry_vector_opt.ok_or_else(|| VortexError::Internal(format!(
        "Entry point ID {} not found in vector storage for layer {}.", entry_point_id, layer_idx
    )))?;

    let dist = calculate_distance(distance_metric, query_vector, entry_vector.view())?;
    let score = heap_score(distance_metric, dist);

    visited_ids.insert(entry_point_id);
    let initial_neighbor = Neighbor { distance: score, internal_id: entry_point_id };
    results_heap.push(initial_neighbor);
    explore_queue.push(initial_neighbor);

    // --- Main search loop ---
    while let Some(current_best_to_explore) = explore_queue.pop() {
        let worst_score_in_results = results_heap.peek().map_or(f32::NEG_INFINITY, |n| n.distance);

        // Optimization: if current_best_to_explore is already worse than the worst in results_heap (and results_heap is full)
        // (Higher score is better in our heap representation: -L2 distance or Cosine similarity)
        if current_best_to_explore.distance < worst_score_in_results && results_heap.len() >= ef {
            break; 
        }

        // Get connections for the current_best_to_explore.internal_id at layer_idx
        let neighbor_connection_ids_slice = graph_links.get_connections(current_best_to_explore.internal_id, layer_idx)
            .ok_or_else(|| VortexError::Internal(format!(
                "Failed to get connections for node {} at layer {}. Node might be out of bounds or layer invalid.",
                current_best_to_explore.internal_id, layer_idx
            )))?;

        for &neighbor_id in neighbor_connection_ids_slice { // Iterate directly over the slice
            if !visited_ids.contains(&neighbor_id) {
                visited_ids.insert(neighbor_id);

                if vector_storage.is_deleted(neighbor_id) { // Removed ?
                    continue; // Skip deleted nodes
                }

                let neighbor_vector_opt = vector_storage.get_vector(neighbor_id); // Removed ?
                let neighbor_vector = neighbor_vector_opt.ok_or_else(|| VortexError::Internal(format!(
                    "Neighbor ID {} (from graph links) not found in vector storage for layer {}.", neighbor_id, layer_idx
                )))?;

                let neighbor_dist = calculate_distance(distance_metric, query_vector, neighbor_vector.view())?;
                let neighbor_heap_score = heap_score(distance_metric, neighbor_dist);
                
                let current_worst_score_in_results_heap = results_heap.peek().map_or(f32::NEG_INFINITY, |n| n.distance);

                if neighbor_heap_score > current_worst_score_in_results_heap || results_heap.len() < ef {
                    let new_neighbor_candidate = Neighbor { distance: neighbor_heap_score, internal_id: neighbor_id };
                    results_heap.push(new_neighbor_candidate);
                    explore_queue.push(new_neighbor_candidate); // Add to exploration queue as well

                    if results_heap.len() > ef {
                        results_heap.pop(); // Remove the worst if heap exceeds ef
                    }
                }
            }
        }
    }
    Ok(results_heap)
}


/// Selects neighbors using a heuristic (Algorithm 3/4 in paper).
/// Basic greedy approach: Keep the M best neighbors based on distance/similarity.
pub(crate) fn select_neighbors_heuristic(
    candidates: &BinaryHeap<Neighbor>, // Max-heap (best score first when popped)
    m: usize,
    // distance_metric: DistanceMetric, // Not needed if heap score is consistent
) -> Vec<u64> { // Returns Vec of internal_ids (u64)
    let mut best_first: Vec<_> = candidates.iter().cloned().collect();
    best_first.sort_by(|a, b| b.cmp(a)); // Sort descending by score (best first)

    best_first.iter().map(|n| n.internal_id).take(m).collect()
}


// --- Insertion Logic ---
// (Mutability notes from planning phase still apply)
