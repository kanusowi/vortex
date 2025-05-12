pub mod node; // Declare the node module
pub use node::{Node, ArcNode};
pub mod builder; // Keep builder internal details separate if needed

// Removed unused imports: Distance, Embedding, HnswConfig
use crate::distance::DistanceMetric;
use crate::error::{VortexError, VortexResult};
use crate::distance::calculate_distance;

// Removed unused imports: HashMap, Arc
use std::collections::{BinaryHeap, HashSet};
use std::cmp::Ordering;
use ndarray::ArrayView1;

// --- Data Structures for Search ---

/// Represents an item in the priority queue used during search.
/// Stores (distance, node_index). Ordered by distance.
#[derive(PartialEq, Debug, Clone, Copy)]
pub(crate) struct Neighbor {
    pub distance: f32,
    pub index: usize,
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
    query: ArrayView1<f32>,
    entry_point_index: usize,
    ef: usize, // Number of candidates to track (ef_search)
    layer_level: usize,
    nodes: &[ArcNode],
    distance_metric: DistanceMetric,
) -> VortexResult<BinaryHeap<Neighbor>> {

    let mut visited: HashSet<usize> = HashSet::new();
    // Removed unused variable `candidates` based on compiler warning.
    // Visited Queue: Min-heap based on distance/similarity for exploration order. `pop` gives next best to explore.
    // Let's rename `candidates` to `results` and use a separate exploration queue.

    // `results`: Max-Heap storing the best `ef` neighbors found. `peek()` gives the worst neighbor in the set.
    let mut results: BinaryHeap<Neighbor> = BinaryHeap::new();
    // `explore_queue`: Max-Heap used to prioritize which node to visit next (best nodes first). `pop()` gives the best candidate to explore.
    let mut explore_queue: BinaryHeap<Neighbor> = BinaryHeap::new();

    // Calculate distance from entry point
    let entry_node = &nodes[entry_point_index];
    if entry_node.deleted {
         return Err(VortexError::Internal("Search entry point is marked as deleted.".to_string()));
    }
    let dist = calculate_distance(distance_metric, query, entry_node.vector.view())?;
    let score = heap_score(distance_metric, dist);

    visited.insert(entry_point_index);
    let initial_neighbor = Neighbor { distance: score, index: entry_point_index };
    results.push(initial_neighbor);
    explore_queue.push(initial_neighbor);

    while let Some(current_best) = explore_queue.pop() { // Get the best candidate to explore

        // Get the worst neighbor currently in the result set
        let worst_in_results_score = results.peek().map_or(f32::NEG_INFINITY, |n| n.distance);

        // If the current candidate is worse than the worst in the result set,
        // and the result set is already full (ef size), we can stop exploring this path.
        // (Higher score is better in our heap representation)
        if current_best.distance < worst_in_results_score && results.len() >= ef {
             // This condition seems reversed. If current_best is worse (lower score) than the worst in results, stop.
            // break; // Optimization: Stop exploring if candidate is worse than the worst in results
            // Let's trace the logic carefully.
            // Example L2: score = -dist. worst_in_results_score = largest negative distance = smallest distance.
            // current_best.score = -current_dist.
            // If -current_dist < -smallest_dist => current_dist > smallest_dist. Yes, stop if current is farther than the farthest stored.
            // Example Cosine: score = sim. worst_in_results_score = smallest similarity.
            // current_best.score = current_sim.
            // If current_sim < smallest_sim. Yes, stop if current is less similar than the least similar stored.
             break; // Condition seems correct.
        }

        let candidate_node = &nodes[current_best.index];

        // Ensure the node has connections at this layer before iterating
        if let Some(neighbors_lock) = candidate_node.connections.get(layer_level) {
            // Acquire read lock to iterate over connections
            let neighbors_indices = neighbors_lock.read();
            for &neighbor_index in neighbors_indices.iter() { // Iterate over the locked Vec
                if !visited.contains(&neighbor_index) {
                    visited.insert(neighbor_index);
                    let neighbor_node = &nodes[neighbor_index];

                    // Skip deleted nodes during search traversal
                    if neighbor_node.deleted {
                        continue;
                    }

                    let neighbor_dist = calculate_distance(distance_metric, query, neighbor_node.vector.view())?;
                    let neighbor_score = heap_score(distance_metric, neighbor_dist);
                    let current_worst_score = results.peek().map_or(f32::NEG_INFINITY, |n| n.distance);

                    // If neighbor is better than the worst in the result set OR result set is not full
                    if neighbor_score > current_worst_score || results.len() < ef {
                        let new_neighbor = Neighbor { distance: neighbor_score, index: neighbor_index };
                        results.push(new_neighbor);
                        explore_queue.push(new_neighbor); // Add to exploration queue as well

                        // If result heap exceeds ef, remove the worst element
                        if results.len() > ef {
                            results.pop();
                        }
                    }
                }
            }
        }
    }

    // `results` is a Max-Heap containing the best `ef` neighbors (best score first when popped).
    Ok(results)
}


/// Selects neighbors using a heuristic (Algorithm 3/4 in paper).
/// Basic greedy approach: Keep the M best neighbors based on distance/similarity.
pub(crate) fn select_neighbors_heuristic(
    candidates: &BinaryHeap<Neighbor>, // Max-heap (best score first when popped)
    m: usize,
    // distance_metric: DistanceMetric, // Not needed if heap score is consistent
) -> Vec<usize> {
    // The heap `candidates` already contains the best neighbors found during search_layer,
    // ordered by score (best first when popped). We just need to take the top M indices.
    // BinaryHeap::into_sorted_vec sorts ascending. Since our score has 'best' as highest, we need to reverse.
    // Removed unused variable `sorted_candidates` based on compiler warning.
    // let sorted_candidates: Vec<_> = candidates.clone().into_sorted_vec(); // into_sorted_vec sorts ascending, need descending for best first.
    let mut best_first: Vec<_> = candidates.iter().cloned().collect();
    best_first.sort_by(|a, b| b.cmp(a)); // Sort descending by score (best first)

    best_first.iter().map(|n| n.index).take(m).collect()
}


// --- Insertion Logic ---
// (Mutability notes from planning phase still apply)
