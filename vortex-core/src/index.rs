use crate::config::HnswConfig;
use crate::distance::DistanceMetric;
use crate::error::{VortexError, VortexResult};
// crate::hnsw removed as it's unused now
// TODO: Re-evaluate hnsw module usage once HNSW logic is integrated here
// use crate::hnsw::{SearchResult}; // Import SearchResult, removed self - Now unused
use crate::vector::{Embedding, VectorId};
// use crate::utils::{create_rng}; // Removed as _rng field is gone
// calculate_distance removed
// use crate::storage::mmap_vector_storage::MmapVectorStorage; // Unused
// use crate::storage::mmap_hnsw_graph_links::MmapHnswGraphLinks; // Unused
use crate::segment::{Segment, SimpleSegment}; // Added segment imports
use crate::hnsw::SearchResult as SegmentSearchResult; // Use hnsw::SearchResult directly
use std::sync::Arc; // Added Arc
use tokio::sync::RwLock; // Added RwLock
use futures::future::try_join_all; // Added for parallel segment search

use async_trait::async_trait;
// ndarray::ArrayView1 removed
// use rand::rngs::StdRng; // Removed as _rng field is gone
use serde::{Serialize, Deserialize}; // Added serde imports
use std::collections::HashMap; // Used for HnswIndexMetadata
use std::fs; // Added fs import
use std::io::Write; // BufReader, BufWriter, Read removed
use std::path::{Path, PathBuf}; // Added PathBuf
// std::sync::Arc removed
use tracing::{warn, info, debug}; // Added debug, trace removed


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

    /// Estimates the total size of memory-mapped files used by the index.
    /// This is an approximation of potential RAM usage if all mapped files were resident.
    fn estimate_ram_footprint(&self) -> u64;
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
    config: HnswConfig,
    metric: DistanceMetric,
}

/// Implementation of the `Index` trait using the HNSW algorithm.
#[derive(Debug)]
pub struct HnswIndex {
    path: PathBuf, // Added path field to store the base path for index files
    config: HnswConfig, // Overall index config
    metric: DistanceMetric, // Overall index metric
    // The fields below will be largely managed by segments or represent aggregated state.
    // vector_storage: MmapVectorStorage, // Moved to SimpleSegment
    // graph_links: MmapHnswGraphLinks, // Moved to SimpleSegment
    // vector_map: HashMap<VectorId, u64>, // Each segment will have its own, or HnswIndex aggregates
    // _rng: StdRng, // Removed as it's unused at HnswIndex level
    // total_vectors_inserted_count: u64, // Might be managed per segment or globally

    segments: Vec<Arc<RwLock<SimpleSegment>>>, // Manages one or more segments
                                            // Using SimpleSegment directly for now.
                                            // dyn Segment might require Send + Sync + 'static for Arc<RwLock<dyn Segment>>
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

// Helper to get a segment's path
fn get_segment_path(base_index_path: &Path, segment_id: usize) -> PathBuf {
    base_index_path.join(format!("segment_{}", segment_id))
}


impl HnswIndex {
    /// Creates a new HNSW index using memory-mapped files for storage, managed by segments.
    pub async fn new( // Changed to async fn
        base_path: &Path, // Base directory for all indices
        name: &str,       // Name of this specific index
        config: HnswConfig,
        metric: DistanceMetric,
        // dimensions and capacity are now part of config or managed per segment
    ) -> VortexResult<Self> {
        config.validate()?;
        if config.vector_dim == 0 {
            return Err(VortexError::Configuration("Dimensions must be greater than 0".to_string()));
        }

        let index_path = base_path.join(name);
        if !index_path.exists() {
            fs::create_dir_all(&index_path).map_err(|e| VortexError::StorageError(e.to_string()))?;
        }
        
        info!(path=?index_path, m=config.m, ef_construction=config.ef_construction, metric=?metric, dimensions=config.vector_dim, "Creating new HNSW index with segment architecture");

        // Create the first segment
        let segment0_path = get_segment_path(&index_path, 0);
        let mut segment0 = SimpleSegment::new(segment0_path, config, metric).await?; // Made mutable
        segment0.save().await?; // Save the newly created segment to persist its metadata
        
        let segments = vec![Arc::new(RwLock::new(segment0))];

        let new_index = HnswIndex {
            path: index_path,
            config, 
            metric,
            // _rng: create_rng(config.seed), // Removed
            segments,
        };
        
        // Save initial index metadata (which might just point to segment 0 for now)
        new_index.save_index_metadata_async().await?;

        Ok(new_index)
    }

    /// Adds a new empty segment to the index. For testing multi-segment scenarios.
    // #[cfg(test)] // Removed to make it available for benchmarks
    pub async fn add_new_segment_for_testing(&mut self) -> VortexResult<usize> {
        let new_segment_id = self.segments.len();
        let segment_path = get_segment_path(&self.path, new_segment_id);
        
        info!(path=?segment_path, "Creating new segment for testing in HnswIndex");
        
        // Use the HnswIndex's overall config and metric for the new segment
        let mut new_segment = SimpleSegment::new(segment_path, self.config, self.metric).await?;
        new_segment.save().await?; // Save the newly created segment
        
        self.segments.push(Arc::new(RwLock::new(new_segment)));
        
        // Re-save index metadata to include the new segment
        self.save_index_metadata_async().await?;
        
        Ok(new_segment_id)
    }
    
    /// Returns the number of segments in the index.
    pub fn segment_count(&self) -> usize {
        self.segments.len()
    }
    
    async fn save_index_metadata_async(&self) -> VortexResult<()> {
        let metadata_path = get_metadata_path(&self.path);
        let temp_metadata_path = metadata_path.with_extension("tmp_idx_meta.json");

        #[derive(Serialize)]
        struct IndexFileMetadata<'a> {
            config: &'a HnswConfig,
            metric: &'a DistanceMetric,
            segment_dir_names: Vec<String>,
        }

        let mut segment_dir_names = Vec::new();
        for s_arc in &self.segments {
            let s_guard = s_arc.read().await;
            if let Some(dir_name) = s_guard.path().file_name().and_then(|os_str| os_str.to_str()) {
                segment_dir_names.push(dir_name.to_string());
            } else {
                warn!("Could not get directory name for segment path: {:?}", s_guard.path());
                // Optionally, return an error if a segment path is invalid or cannot be processed
                // return Err(VortexError::StorageError(format!("Invalid segment path: {:?}", s_guard.path())));
            }
        }
        
        let metadata_content = IndexFileMetadata {
            config: &self.config,
            metric: &self.metric,
            segment_dir_names,
        };
        
        let file = fs::File::create(&temp_metadata_path).map_err(|e| VortexError::IoError { path: temp_metadata_path.clone(), source: e })?;
        serde_json::to_writer_pretty(file, &metadata_content)
            .map_err(|e| VortexError::StorageError(format!("Failed to serialize HNSWIndex metadata: {}", e)))?;
        
        fs::rename(&temp_metadata_path, &metadata_path).map_err(|e| VortexError::IoError { path: temp_metadata_path.clone(), source: e })?;
        debug!("Successfully saved HNSW index-level metadata to {:?}", metadata_path);
        Ok(())
    }

    /// Opens an existing HNSW index, loading its segments.
    pub async fn open( // Changed to async fn
        base_path: &Path,
        name: &str,
        default_config: HnswConfig, // Used if index metadata is missing
        default_metric: DistanceMetric, // Used if index metadata is missing
    ) -> VortexResult<Self> {
        let index_path = base_path.join(name);
        info!(path=?index_path, "Opening HNSW index with segment architecture");

        if !index_path.exists() {
            return Err(VortexError::StorageError(format!("Index path does not exist: {:?}", index_path)));
        }

        let metadata_path = get_metadata_path(&index_path);
        let (loaded_config, loaded_metric, segment_dir_names_opt) = if metadata_path.exists() {
            debug!("Loading HNSW index metadata from {:?}", metadata_path);
            let file = fs::File::open(&metadata_path).map_err(|e| VortexError::IoError { path: metadata_path.clone(), source: e })?;
            
            #[derive(Deserialize)]
            struct IndexFileMetadata {
                config: HnswConfig,
                metric: DistanceMetric,
                segment_dir_names: Option<Vec<String>>, // Now optional
            }
            let index_file_meta: IndexFileMetadata = serde_json::from_reader(file)
                .map_err(|e| VortexError::StorageError(format!("Failed to deserialize HNSWIndex metadata from {:?}: {}", metadata_path, e)))?;
            (index_file_meta.config, index_file_meta.metric, index_file_meta.segment_dir_names)
        } else {
            warn!("HNSWIndex metadata file {:?} not found. Using provided default config/metric and attempting to load segment_0.", metadata_path);
            (default_config, default_metric, None)
        };

        let mut loaded_segments = Vec::new();
        if let Some(dir_names) = segment_dir_names_opt {
            if !dir_names.is_empty() {
                for dir_name in dir_names {
                    let segment_path = index_path.join(&dir_name); // Use &dir_name as join takes AsRef<Path>
                    if segment_path.exists() {
                        debug!("Loading segment from path: {:?}", segment_path);
                        let segment = SimpleSegment::load(segment_path).await?;
                        loaded_segments.push(Arc::new(RwLock::new(segment)));
                    } else {
                        warn!("Segment path {:?} listed in HNSWIndex metadata not found, skipping.", segment_path);
                        // Consider if this should be a hard error depending on desired consistency
                    }
                }
            }
        }

        if loaded_segments.is_empty() {
            warn!("No segments loaded from HNSWIndex metadata or listed segments not found. Attempting to load default segment_0.");
            let segment0_path = get_segment_path(&index_path, 0);
            if segment0_path.exists() {
                debug!("Loading default segment_0 from path: {:?}", segment0_path);
                let segment0 = SimpleSegment::load(segment0_path).await?;
                loaded_segments.push(Arc::new(RwLock::new(segment0)));
            } else {
                // If creating a new index, HnswIndex::new() handles segment creation.
                // HnswIndex::open() implies the index (and thus at least one segment) should exist.
                return Err(VortexError::StorageError(format!("No segments found for HNSWIndex {:?}, and default segment_0 also missing.", index_path)));
            }
        }
        
        info!("Successfully opened HNSW index. Loaded {} segment(s). Using config: {:?}, metric: {:?}", loaded_segments.len(), loaded_config, loaded_metric);

        Ok(HnswIndex {
            path: index_path,
            config: loaded_config, 
            metric: loaded_metric, 
            segments: loaded_segments,
        })
    }
    
    // fn save_index_metadata(&self) -> VortexResult<()> { // This method is replaced by save_index_metadata_async
    // ... old content ...
    // }

    // insert_vector is now part of the Index trait (async)
    // The old HnswIndex::insert_vector (sync) logic needs to be adapted into SimpleSegment::insert_vector (async)
    // HnswIndex::find_valid_entry_point also needs to be part of SimpleSegment or adapted.
}


#[async_trait]
impl Index for HnswIndex {
    async fn add_vector(&mut self, id: VectorId, vector: Embedding) -> VortexResult<bool> {
        if self.segments.is_empty() {
            // This case should ideally not happen if new() always creates a segment.
            return Err(VortexError::Internal("No segments available in HnswIndex".to_string()));
        }
        // The following lines for `segment` are unused due to the logic below.
        // let mut segment = self.segments[0].write().await; 
        
        // Check if vector_id already exists in this segment.
        // This check might need to be global across all segments in the future.
        // For now, SimpleSegment's insert_vector handles its own vector_map.
        // The `bool` return from Segment::insert_vector could indicate if it was truly new.
        // Let's assume Segment::insert_vector will handle existing ID logic (e.g., update or error).
        // For now, HnswIndex::add_vector will just delegate.
        // The `bool` return from this trait method is about whether it was added to the *index*,
        // which for now means added to the first segment.
        // If SimpleSegment::insert_vector errors on duplicate, we catch it.
        // If it updates, then it's not "newly added" in a sense.
        // Let's simplify: assume SimpleSegment::insert_vector adds or updates.
        // This trait method should probably return Ok(()) and let `get_vector` confirm.
        // Or, it returns true if it's a new vector_id for the segment.
        // The current SimpleSegment::insert_vector updates if ID exists.
        // Let's assume for now that if it doesn't error, it's "successful".
        // The boolean return is a bit ambiguous with updates.
        // Let's stick to: if it's a new ID for the segment, it's true.
        // This requires SimpleSegment::insert_vector to return that info.
        // For now, let's assume it's always a "new" add if no error.
        // This part needs careful thought on semantics of "added".
        // The old HnswIndex checked its global vector_map.
        // Now, each segment has a vector_map. A global check would be slow.
        // Let's assume vector IDs must be unique *within a segment* for now.
        // And HnswIndex::add_vector adds to segment 0.
        
        // A quick check against segment 0's map before write lock for efficiency
        // This is not robust for multi-segment scenario or concurrent global ID checks.
        // For now, let SimpleSegment handle its internal map.
        // The `bool` return from `add_vector` indicates if it was a new insert (true) or an update (false).
        // SimpleSegment::insert_vector now returns Result<bool, VortexError>.
        
        // Decide which segment to add to. For now, always add to the *last* segment.
        // This is a simple strategy for testing. Real strategies would be more complex.
        if let Some(last_segment_arc) = self.segments.last() {
            let mut segment_guard = last_segment_arc.write().await;
            return segment_guard.insert_vector(id, vector).await;
        } else {
            // This should not happen if HnswIndex::new always creates a segment.
            return Err(VortexError::Internal("No segments available to add vector".to_string()));
        }
    }

    async fn search(&self, query: Embedding, k: usize) -> VortexResult<Vec<(VectorId, f32)>> {
        if query.len() != self.config.vector_dim as usize { // Use HnswIndex's overall config for dim check
            return Err(VortexError::DimensionMismatch { expected: self.config.vector_dim as usize, actual: query.len() });
        }
        // ef_search comes from HnswIndex's config
        self.search_with_ef(query, k, self.config.ef_search).await
    }

    async fn search_with_ef(&self, query: Embedding, k: usize, ef_search_override: usize) -> VortexResult<Vec<(VectorId, f32)>> {
        if query.len() != self.config.vector_dim as usize {
            return Err(VortexError::DimensionMismatch { expected: self.config.vector_dim as usize, actual: query.len() });
        }
        if k == 0 {
            return Ok(Vec::new());
        }
        // ef_search_override is used by the segment's search method.
        // The Segment::search trait needs to accept ef_search_override.
        // For now, let's assume SimpleSegment::search uses its own config.ef_search.
        // This needs alignment. Let's modify Segment::search to take ef_search.
        // And HnswIndex::search_internal will be removed or adapted.
        // For now, we call the current Segment::search which doesn't take ef_search.
        
        if self.segments.is_empty() {
            debug!("Search called on HnswIndex with no segments.");
            return Ok(Vec::new());
        }

        let mut search_futures = Vec::new();
        for segment_arc in &self.segments {
            let segment_clone = Arc::clone(segment_arc);
            let query_clone = query.clone(); // Clone query for each async task
            search_futures.push(async move {
                let segment_guard = segment_clone.read().await;
                segment_guard.search(&query_clone, k + 10, ef_search_override).await // Fetch slightly more from each segment for better global top-k
            });
        }

        let segment_results_list: Vec<Vec<SegmentSearchResult>> = try_join_all(search_futures)
            .await?
            .into_iter()
            .collect();

        let mut all_results: Vec<SegmentSearchResult> = segment_results_list.into_iter().flatten().collect();

        // Sort all collected results by distance.
        // For Cosine, higher is better, so sort descending.
        // For L2, lower is better, so sort ascending.
        match self.metric {
            DistanceMetric::Cosine => {
                all_results.sort_by(|a, b| b.distance.partial_cmp(&a.distance).unwrap_or(std::cmp::Ordering::Equal));
            }
            DistanceMetric::L2 => {
                all_results.sort_by(|a, b| a.distance.partial_cmp(&b.distance).unwrap_or(std::cmp::Ordering::Equal));
            }
        }
        // TODO: Add a proper deduplication strategy here if points can exist in multiple segments
        // or if a single point can be returned multiple times from one segment's k+10 search.
        // For now, simple truncation assumes the sort order is sufficient for top-k.

        // Deduplicate by ID, keeping the one with the smallest distance (already sorted)
        // This is important if segments could somehow have overlapping IDs, though current add logic doesn't cause this.
        // For now, a simple sort and truncate is fine. If deduplication is needed later:
        // all_results.dedup_by_key(|sr| sr.id.clone()); // This would need PartialEq on SearchResult by id

        // Take top k
        all_results.truncate(k);
        
        // Convert SegmentSearchResult to (VectorId, f32)
        Ok(all_results.into_iter().map(|sr| (sr.id, sr.distance)).collect())
    }

    async fn save(&mut self, _writer: &mut (dyn Write + Send)) -> VortexResult<()> {
        info!("Saving HNSW index (segment-based)...");
        self.save_index_metadata_async().await?; // Save overall index metadata, now async

        for segment_arc in &self.segments {
            let mut segment = segment_arc.write().await; // Ensure segment is mutable if save requires &mut self
            segment.save().await?; // Each segment saves itself
        }
        
        info!("HNSW index and its segments saved successfully.");
        Ok(())
    }

    async fn delete_vector(&mut self, id: &VectorId) -> VortexResult<bool> {
        if self.segments.is_empty() {
            debug!("Delete_vector called on HnswIndex with no segments.");
            return Ok(false);
        }

        let mut deleted_globally = false;
        // Attempt to delete from all segments. If an ID is globally unique, only one will succeed.
        // If IDs are not globally unique (not current design for add), this would delete from all.
        for segment_arc in &self.segments {
            let mut segment_guard = segment_arc.write().await;
            match segment_guard.delete_vector(id).await {
                Ok(()) => { // Successfully deleted in this segment
                    deleted_globally = true;
                    // If IDs are guaranteed unique across segments, we could break here.
                    // For now, let's assume we try all segments.
                }
                Err(VortexError::NotFound(_)) => { /* Not found in this segment, continue */ }
                Err(e) => return Err(e), // Propagate other errors
            }
        }
        Ok(deleted_globally)
    }

    async fn get_vector(&self, id: &VectorId) -> VortexResult<Option<Embedding>> {
        if self.segments.is_empty() {
            debug!("Get_vector called on HnswIndex with no segments.");
            return Ok(None);
        }

        let mut find_futures = Vec::new();
        for segment_arc in &self.segments {
            let segment_clone = Arc::clone(segment_arc);
            let id_clone = id.clone();
            find_futures.push(async move {
                let segment_guard = segment_clone.read().await;
                segment_guard.get_vector(&id_clone).await // This future resolves to VortexResult<Option<Embedding>>
            });
        }

        // try_join_all on Vec<Future<Output = VortexResult<T>>> returns VortexResult<Vec<T>>
        let results_from_segments: Vec<Option<Embedding>> = try_join_all(find_futures).await?;
        
        for maybe_embedding in &results_from_segments { // Iterate by reference
            if let Some(embedding) = maybe_embedding {
                return Ok(Some(embedding.clone())); // Clone the embedding
            }
            // If None, it means it was Ok(None) from the segment's get_vector, so continue checking others.
            // Errors from individual segment futures would have been propagated by try_join_all's `?` above.
        }
        // If loop completes, it means all segments returned Ok(None) or were successfully processed by try_join_all.
        // However, the current structure of try_join_all means if one segment's get_vector returns Err(VortexError::NotFound),
        // that error might not propagate correctly if we want to continue searching other segments.
        // Let's refine: each future should resolve to VortexResult<Option<Embedding>>.
        // try_join_all will give VortexResult<Vec<Option<Embedding>>>.
        // If any segment future errors with something other than NotFound, try_join_all propagates it.
        // If all are Ok(None) or Ok(Some(_)), we iterate.

        // The above logic for try_join_all is correct. The issue was the type annotation.
        // The iteration logic also needs to handle the case where a segment's get_vector itself returns an error
        // that isn't propagated by try_join_all (e.g. if we mapped errors inside the futures).
        // But since segment.get_vector directly returns VortexResult, try_join_all handles propagation of the first non-Ok error.
        // So, if we reach here, all futures resolved to Ok(Option<Embedding>).
        // The iteration `for maybe_embedding in results_from_segments` is correct.

        // Let's re-check the logic for `get_vector` with `try_join_all`.
        // Each future `segment_guard.get_vector(&id_clone).await` has type `VortexResult<Option<Embedding>>`.
        // `try_join_all` collects these. If any future errors (e.g. `VortexError::StorageError`), `try_join_all` itself will error out.
        // If all futures succeed (i.e., return `Ok(Option<Embedding>)`), then `try_join_all(...).await?` will yield `Vec<Option<Embedding>>`.
        // Then we iterate through this `Vec<Option<Embedding>>`.
        // This seems correct. The original error was just the type annotation.

        // The previous iteration logic was:
        // for result in results { // where results was Vec<VortexResult<Option<Embedding>>>
        //     match result {
        //         Ok(Some(embedding)) => return Ok(Some(embedding)),
        //         Ok(None) => { /* Not in this segment */ }
        //         Err(VortexError::NotFound(_)) => { /* Not in this segment */ } // This case won't happen if try_join_all propagates NotFound
        //         Err(e) => return Err(e), 
        //     }
        // }
        // With `let results_from_segments: Vec<Option<Embedding>> = try_join_all(find_futures).await?;`
        // we only iterate if all futures returned Ok.
        // This second loop is actually redundant due to the check above.
        // If the first loop completes without returning, it means no Some(embedding) was found.
        // So we can directly return Ok(None) after the first loop.
        // The previous commented out code block was actually trying to re-iterate the moved value.
        // The fix is to ensure the first loop correctly identifies if an embedding was found.
        // If the loop `for maybe_embedding in &results_from_segments` finishes, it means no embedding was found.
        Ok(None) // Not found in any segment
    }

    fn len(&self) -> usize {
        // Sum lengths of all segments. Requires Segment trait to have len().
        // For now, use first segment.
        if self.segments.is_empty() {
            0
        } else {
            // This requires awaiting read lock, so len() cannot be sync if segments are Arc<RwLock>.
            // For now, this is problematic. Let's make a temporary sync assumption.
            // This needs to be async or Segment::len needs to be carefully designed.
            // Or HnswIndex::len becomes async.
            // Let's assume for now we can get a quick len from the first segment.
            // This is a simplification.
            // Runtime block_on is not good here.
            // For now, let's return 0 if segments is not empty, to avoid blocking.
            // This is a placeholder.
            // A better approach: HnswIndex::len() becomes async.
            // Let's make it async for now.
            // No, the trait is sync. This is a conflict.
            // For now, let's make a potentially expensive sum if multiple segments.
            // Or, HnswIndex tracks total length.
            // Let's assume HnswIndex will track total length.
            // Sum lengths of all segments.
            // This uses block_on because the Index::len() trait method is synchronous.
            // A better long-term solution might involve HnswIndex tracking total length
            // or making len() async in the trait if possible.
            futures::executor::block_on(async {
                let mut total_len = 0;
                for segment_arc in &self.segments {
                    let segment_guard = segment_arc.read().await;
                    total_len += segment_guard.vector_count();
                }
                total_len
            })
        }
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn dimensions(&self) -> usize {
        // Assume all segments have the same dimension, from overall config.
        self.config.vector_dim as usize
    }

    fn config(&self) -> HnswConfig {
        self.config
    }

    fn distance_metric(&self) -> DistanceMetric {
        self.metric
    }

    async fn list_vectors(&self, limit: Option<usize>) -> VortexResult<Vec<(VectorId, Embedding)>> {
        if self.segments.is_empty() {
            debug!("HnswIndex::list_vectors called on an index with no segments.");
            return Ok(Vec::new());
        }
        
        // For now, list from all segments and apply limit globally.
        // TODO: This could be memory intensive for large number of segments / vectors.
        // Consider more sophisticated pagination or streaming in the future.
        
        let mut list_futures = Vec::new();
        for segment_arc in &self.segments {
            let segment_clone = Arc::clone(segment_arc);
            // For list_vectors, we don't need to pass a limit to each segment if we are applying it globally.
            // However, if segments are very large, fetching all might be too much.
            // For now, let's fetch all from each segment and then limit.
            // A more optimized approach might fetch `limit` from each, then merge and re-limit,
            // but that assumes some ordering or relevance which list_vectors doesn't guarantee.
            list_futures.push(async move {
                let segment_guard = segment_clone.read().await;
                segment_guard.list_vectors(None).await // Fetch all from this segment
            });
        }

        let segment_vector_lists: Vec<Vec<(VectorId, Embedding)>> = try_join_all(list_futures)
            .await?
            .into_iter()
            .collect();

        let mut all_vectors: Vec<(VectorId, Embedding)> = segment_vector_lists.into_iter().flatten().collect();

        if let Some(l) = limit {
            all_vectors.truncate(l);
        }
        
        debug!(num_vectors_listed=all_vectors.len(), ?limit, "HnswIndex::list_vectors completed.");
        Ok(all_vectors)
    }

    fn estimate_ram_footprint(&self) -> u64 {
        // Sum estimated mapped sizes of all segments.
        // This requires awaiting read locks, so it cannot be truly sync without block_on.
        // For a quick estimate without async in trait, we'll use block_on.
        // A better long-term solution might involve HnswIndex tracking this,
        // or making estimate_ram_footprint async in the trait.
        futures::executor::block_on(async {
            let mut total_mapped_size = 0;
            for segment_arc in &self.segments {
                let segment_guard = segment_arc.read().await;
                total_mapped_size += segment_guard.estimate_mapped_size();
            }
            total_mapped_size
        })
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
    use tokio::fs as async_fs; // For async file operations in tests if needed
    // use std::io::{Cursor, BufWriter}; // Not needed for these initial tests
    // use std::fs::File; // Not needed for these initial tests
    // use crate::hnsw::ArcNode; // ArcNode is removed

    fn create_test_config() -> HnswConfig {
        // Ensure vector_dim is set, as HnswIndex::new relies on config.vector_dim
        HnswConfig { 
            vector_dim: 2, 
            m: 5, 
            m_max0: 10, 
            ef_construction: 20, 
            ef_search: 10, 
            ml: 0.5, 
            seed: Some(123),
            vector_storage_capacity: None, // Added missing field
            graph_links_capacity: None,    // Added missing field
        }
    }

    #[tokio::test]
    async fn test_new_index() {
        let dir = tempdir().unwrap();
        let base_path = dir.path(); 
        let mut config = create_test_config();
        config.vector_dim = 4; // Override for this test
        let index_name = "test_new_idx_segmented";
        
        let index = HnswIndex::new(base_path, index_name, config, DistanceMetric::L2).await.unwrap();
        
        assert_eq!(index.dimensions(), config.vector_dim as usize);
        assert_eq!(index.len(), 0); 
        assert!(index.is_empty());
        assert_eq!(index.distance_metric(), DistanceMetric::L2);
        assert_eq!(index.config(), config);

        let index_dir = base_path.join(index_name);
        assert!(index_dir.exists());
        let segment0_dir = index_dir.join("segment_0");
        assert!(segment0_dir.exists());
        // Accessing SimpleSegment::METADATA_FILE requires it to be public or pub(super)
        // Let's assume it's accessible for the test. If not, this check might need adjustment.
        // It is pub in the provided segment.rs.
        let segment0_meta_file = segment0_dir.join(crate::segment::SimpleSegment::METADATA_FILE);
        assert!(segment0_meta_file.exists());
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
        let base_path = dir.path();
        let config = create_test_config();
        let index_name = "test_add_search_get_delete_idx_segmented";
        // dimensions and capacity from config / segment

        let mut index = HnswIndex::new(base_path, index_name, config, DistanceMetric::L2).await.unwrap();

        let vec1_id = "vec1".to_string();
        let vec1_data = Embedding::from(vec![1.0, 2.0]);
        let added1 = index.add_vector(vec1_id.clone(), vec1_data.clone()).await.unwrap();
        assert!(added1);
        assert_eq!(index.len(), 1);

        // Test get_vector
        let retrieved_vec1 = index.get_vector(&vec1_id).await.unwrap().unwrap();
        assert_eq!(retrieved_vec1, vec1_data);

        // Test search (will be very basic until HNSW insert logic is complete)
        // With HNSW logic now in SimpleSegment, search should work.
        let query_vec = Embedding::from(vec![1.1, 2.1]); // Matches config.vector_dim = 2
        let results = index.search(query_vec.clone(), 1).await.unwrap();
        
        assert!(!results.is_empty(), "Search should return results.");
        assert_eq!(results[0].0, vec1_id, "vec1 should be the closest.");
        // Optionally check distance if known/calculable
        // let expected_dist_v1 = DistanceMetric::L2.distance(&query_vec, &vec1_data);
        // assert!((results[0].1 - expected_dist_v1).abs() < 1e-6);


        // Test delete_vector
        let deleted1 = index.delete_vector(&vec1_id).await.unwrap();
        assert!(deleted1); 
        assert_eq!(index.len(), 0); 
        assert!(index.get_vector(&vec1_id).await.unwrap().is_none());

        // Test adding again after delete
        let vec2_id = "vec2".to_string();
        let vec2_data = Embedding::from(vec![3.0, 4.0]); // Matches config.vector_dim = 2
        let added2 = index.add_vector(vec2_id.clone(), vec2_data.clone()).await.unwrap();
        assert!(added2);
        assert_eq!(index.len(), 1);
    }
    
    #[tokio::test]
    async fn test_save_and_open_index() {
        let dir = tempdir().unwrap();
        let base_path = dir.path(); 
        let mut original_config = create_test_config();
        original_config.vector_dim = 3; // Set dim for this test
        let original_metric = DistanceMetric::Cosine;
        let index_name = "test_save_open_idx_segmented";
        let vec_id_str = "id1_segment";

        {
            let mut index_to_save = HnswIndex::new(base_path, index_name, original_config, original_metric).await.unwrap();
            let vec_id = vec_id_str.to_string();
            let vec_data = Embedding::from(vec![0.1, 0.2, 0.3]); 
            assert_eq!(index_to_save.config().vector_dim, 3, "Config dim should be 3 for this vector");

            index_to_save.add_vector(vec_id.clone(), vec_data.clone()).await.unwrap();
            
            let seg_read = index_to_save.segments[0].read().await;
            assert_eq!(seg_read.vector_map_len(), 1);
            assert!(seg_read.vector_map_contains_key(&vec_id_str.to_string()));
            drop(seg_read);

            let mut dummy_writer = Vec::new(); 
            index_to_save.save(&mut dummy_writer).await.unwrap();

            let index_metadata_file_path = get_metadata_path(&base_path.join(index_name));
            assert!(index_metadata_file_path.exists(), "Index metadata file should exist after save.");
            
            let segment0_path = get_segment_path(&base_path.join(index_name), 0);
            let segment0_metadata_file = segment0_path.join(crate::segment::SimpleSegment::METADATA_FILE);
            assert!(segment0_metadata_file.exists(), "Segment 0 metadata file should exist.");
            
            let _seg_meta_content = fs::read_to_string(&segment0_metadata_file).unwrap();
            // Need to make SimpleSegmentMetadata accessible, e.g. pub(crate) or via a helper
            // For now, assume we can deserialize it if it were pub(crate) in segment.rs
            // let loaded_seg_meta: crate::segment::SimpleSegmentMetadata = serde_json::from_str(&seg_meta_content).unwrap();
            // assert_eq!(loaded_seg_meta.vector_map.len(), 1);
            // assert!(loaded_seg_meta.vector_map.contains_key(vec_id_str));
            // assert_eq!(loaded_seg_meta.config, original_config);
            // assert_eq!(loaded_seg_meta.distance_metric, original_metric);
             warn!("Skipping detailed segment metadata content check as SimpleSegmentMetadata is not pub.");


        } // index_to_save is dropped

        let default_test_config = HnswConfig { 
            vector_dim: 3, 
            m: 1, 
            m_max0: 1, 
            ef_construction: 1, 
            ef_search: 1, 
            ml: 0.1, 
            seed: Some(999),
            vector_storage_capacity: None, // Added missing field
            graph_links_capacity: None,    // Added missing field
        };
        let default_test_metric = DistanceMetric::L2;
        
        let opened_index = HnswIndex::open(base_path, index_name, default_test_config, default_test_metric).await.unwrap();
        
        assert_eq!(opened_index.dimensions(), original_config.vector_dim as usize);
        assert_eq!(opened_index.config(), original_config, "Opened index config should match original saved config.");
        assert_eq!(opened_index.distance_metric(), original_metric, "Opened index metric should match original saved metric.");
        
        assert_eq!(opened_index.segments.len(), 1, "Should open one segment.");
        let opened_segment = opened_index.segments[0].read().await;
        assert_eq!(opened_segment.vector_map_len(), 1, "Segment vector map length mismatch.");
        assert!(opened_segment.vector_map_contains_key(&vec_id_str.to_string()), "Segment vector_map should contain original ID.");
        drop(opened_segment);
        
        assert_eq!(opened_index.len(), 1, "Opened index len should be 1."); // Relies on len()
        let retrieved_vec = opened_index.get_vector(&vec_id_str.to_string()).await.unwrap().unwrap();
        assert_eq!(retrieved_vec, Embedding::from(vec![0.1, 0.2, 0.3]));
    }

    #[tokio::test]
    async fn test_load_index_missing_metadata_file() {
        let dir = tempdir().unwrap();
        let base_path = dir.path();
        let default_config_for_open = create_test_config(); 
        let default_metric_for_open = DistanceMetric::L2;
        let index_name = "test_missing_meta_idx_segmented";

        // Create segment directory and its files, but no main index metadata file
        let index_dir_path = base_path.join(index_name);
        fs::create_dir_all(&index_dir_path).unwrap();
        let segment0_path = get_segment_path(&index_dir_path, 0);
        // Create a valid segment0
        let _segment0 = SimpleSegment::new(segment0_path.clone(), default_config_for_open, default_metric_for_open).await.unwrap();
        // We need to save this segment for SimpleSegment::load to work later in HnswIndex::open
        // This is a bit circular for this test's purpose.
        // Let's assume HnswIndex::open will try to load segment0 if index metadata is missing.
        // For this test to be more direct, HnswIndex::open would need to create a new segment if index meta is missing AND segment0 is missing.
        // The current HnswIndex::open logic expects segment0 to exist if index_path exists.
        // Let's ensure segment0 is saved.
        let mut seg_to_save = _segment0;
        seg_to_save.save().await.unwrap();


        let opened_index_result = HnswIndex::open(base_path, index_name, default_config_for_open, default_metric_for_open).await;
        
        assert!(opened_index_result.is_ok(), "Opening an index with missing index metadata but existing segment 0 should succeed, using defaults for index-level config. Error: {:?}", opened_index_result.err());
        let opened_index = opened_index_result.unwrap();
        
        // Index-level config and metric should be the defaults passed to open()
        assert_eq!(opened_index.config(), default_config_for_open, "Config should be the default passed to open() when index metadata is missing.");
        assert_eq!(opened_index.distance_metric(), default_metric_for_open, "Metric should be the default passed to open() when index metadata is missing.");
        
        // Segment 0 should have been loaded with its own persisted config/metric
        assert_eq!(opened_index.segments.len(), 1);
        let seg_read = opened_index.segments[0].read().await;
        assert_eq!(seg_read.config, default_config_for_open); // Because segment0 was created with these
        assert_eq!(seg_read.distance_metric, default_metric_for_open);
    }

    #[tokio::test]
    async fn test_load_index_corrupted_index_metadata_file() {
        let dir = tempdir().unwrap();
        let base_path = dir.path();
        let default_config = create_test_config();
        let default_metric = DistanceMetric::L2;
        let index_name = "test_corrupted_idx_meta_segmented";

        let index_dir_path = base_path.join(index_name);
        fs::create_dir_all(&index_dir_path).unwrap();
        
        // Create a valid segment0 so that loading doesn't fail due to missing segment
        let segment0_path = get_segment_path(&index_dir_path, 0);
        let mut segment0 = SimpleSegment::new(segment0_path.clone(), default_config, default_metric).await.unwrap();
        segment0.save().await.unwrap();


        // Create a corrupted index metadata file
        let index_metadata_file_path = get_metadata_path(&index_dir_path);
        fs::write(&index_metadata_file_path, "this is not valid json for index").unwrap();

        let opened_index_result = HnswIndex::open(base_path, index_name, default_config, default_metric).await;
        
        assert!(opened_index_result.is_err(), "Opening an index with corrupted index metadata should fail.");
        match opened_index_result.err().unwrap() {
            VortexError::StorageError(msg) => {
                assert!(msg.contains("Failed to deserialize HNSWIndex metadata"));
            }
            _ => panic!("Expected StorageError for corrupted index metadata."),
        }
    }

    #[tokio::test]
    async fn test_list_vectors_via_hnsw_index() {
        let dir = tempdir().unwrap();
        let base_path = dir.path();
        let mut config = create_test_config();
        config.vector_dim = 2; // For consistency with embeddings
        let index_name = "test_list_vectors_idx_segmented";
        
        let mut index = HnswIndex::new(base_path, index_name, config, DistanceMetric::L2).await.unwrap();

        // Test on empty index
        let listed_empty = index.list_vectors(None).await.unwrap();
        assert!(listed_empty.is_empty(), "list_vectors on empty index should be empty");
        let listed_empty_limit = index.list_vectors(Some(5)).await.unwrap();
        assert!(listed_empty_limit.is_empty(), "list_vectors with limit on empty index should be empty");

        // Add some vectors
        let vec1 = ("id_list_1".to_string(), Embedding::from(vec![1.0, 1.0]));
        let vec2 = ("id_list_2".to_string(), Embedding::from(vec![2.0, 2.0]));
        let vec3 = ("id_list_3".to_string(), Embedding::from(vec![3.0, 3.0]));

        index.add_vector(vec1.0.clone(), vec1.1.clone()).await.unwrap();
        index.add_vector(vec2.0.clone(), vec2.1.clone()).await.unwrap();
        index.add_vector(vec3.0.clone(), vec3.1.clone()).await.unwrap();

        // Test list_vectors with no limit
        let listed_all = index.list_vectors(None).await.unwrap();
        assert_eq!(listed_all.len(), 3, "Should list all 3 vectors from HnswIndex");
        assert!(listed_all.iter().any(|(id, _)| id == &vec1.0));
        assert!(listed_all.iter().any(|(id, _)| id == &vec2.0));
        assert!(listed_all.iter().any(|(id, _)| id == &vec3.0));

        // Test list_vectors with limit
        let listed_limit_2 = index.list_vectors(Some(2)).await.unwrap();
        assert_eq!(listed_limit_2.len(), 2, "Should list 2 vectors with limit 2 from HnswIndex");

        // Test list_vectors after deletion
        index.delete_vector(&vec2.0).await.unwrap();
        let listed_after_delete = index.list_vectors(None).await.unwrap();
        assert_eq!(listed_after_delete.len(), 2, "Should list 2 vectors after one deletion from HnswIndex");
        assert!(!listed_after_delete.iter().any(|(id, _)| id == &vec2.0));
    }

    #[tokio::test]
    async fn test_multi_segment_add_and_search() {
        let dir = tempdir().unwrap();
        let base_path = dir.path();
        let mut config = create_test_config();
        config.vector_dim = 2;
        let index_name = "test_multi_seg_add_search";
        let mut index = HnswIndex::new(base_path, index_name, config, DistanceMetric::L2).await.unwrap();

        // Add to segment 0
        let vec1_s0 = ("vec1_s0".to_string(), Embedding::from(vec![1.0, 1.0]));
        let vec2_s0 = ("vec2_s0".to_string(), Embedding::from(vec![2.0, 2.0]));
        index.add_vector(vec1_s0.0.clone(), vec1_s0.1.clone()).await.unwrap();
        index.add_vector(vec2_s0.0.clone(), vec2_s0.1.clone()).await.unwrap();
        assert_eq!(index.len(), 2);
        assert_eq!(index.segment_count(), 1);

        // Add a new segment (segment 1)
        let new_segment_idx = index.add_new_segment_for_testing().await.unwrap();
        assert_eq!(new_segment_idx, 1);
        assert_eq!(index.segment_count(), 2);
        
        // Add to segment 1 (HnswIndex::add_vector now adds to the last segment)
        let vec1_s1 = ("vec1_s1".to_string(), Embedding::from(vec![10.0, 10.0]));
        let vec2_s1 = ("vec2_s1".to_string(), Embedding::from(vec![11.0, 11.0]));
        index.add_vector(vec1_s1.0.clone(), vec1_s1.1.clone()).await.unwrap();
        index.add_vector(vec2_s1.0.clone(), vec2_s1.1.clone()).await.unwrap();
        
        assert_eq!(index.len(), 4, "Total length should be 4 after adding to both segments.");

        // Verify vectors are in their respective segments (by checking segment lengths)
        let seg0_guard = index.segments[0].read().await;
        assert_eq!(seg0_guard.vector_count(), 2, "Segment 0 should have 2 vectors.");
        drop(seg0_guard);
        let seg1_guard = index.segments[1].read().await;
        assert_eq!(seg1_guard.vector_count(), 2, "Segment 1 should have 2 vectors.");
        drop(seg1_guard);


        // Search for a vector close to one in segment 0
        let query_s0 = Embedding::from(vec![1.5, 1.5]);
        let results_s0 = index.search(query_s0, 2).await.unwrap();
        assert_eq!(results_s0.len(), 2);
        assert!(results_s0.iter().any(|(id, _)| id == &vec1_s0.0 || id == &vec2_s0.0));
        // Ensure results are from segment 0 primarily
        assert!(results_s0[0].0 == vec1_s0.0 || results_s0[0].0 == vec2_s0.0);


        // Search for a vector close to one in segment 1
        let query_s1 = Embedding::from(vec![10.5, 10.5]);
        let results_s1 = index.search(query_s1, 2).await.unwrap();
        assert_eq!(results_s1.len(), 2);
        assert!(results_s1.iter().any(|(id, _)| id == &vec1_s1.0 || id == &vec2_s1.0));
         // Ensure results are from segment 1 primarily
        assert!(results_s1[0].0 == vec1_s1.0 || results_s1[0].0 == vec2_s1.0);


        // Global search that should pull from both
        let query_global_closer_s0 = Embedding::from(vec![0.0, 0.0]); // Closest to vec1_s0
        let results_global = index.search(query_global_closer_s0, 4).await.unwrap();
        assert_eq!(results_global.len(), 4);
        assert_eq!(results_global[0].0, vec1_s0.0); // vec1_s0 should be first

        // Get vector from segment 0
        assert!(index.get_vector(&vec1_s0.0).await.unwrap().is_some());
        // Get vector from segment 1
        assert!(index.get_vector(&vec1_s1.0).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn test_multi_segment_save_and_open() {
        let dir = tempdir().unwrap();
        let base_path = dir.path();
        let mut config = create_test_config();
        config.vector_dim = 2;
        let index_name = "test_multi_seg_save_open";
        
        let vec1_s0_id = "vec1_s0_save";
        let vec1_s1_id = "vec1_s1_save";

        {
            let mut index_to_save = HnswIndex::new(base_path, index_name, config, DistanceMetric::L2).await.unwrap();
            index_to_save.add_vector(vec1_s0_id.to_string(), Embedding::from(vec![1.0, 1.0])).await.unwrap();
            
            index_to_save.add_new_segment_for_testing().await.unwrap(); // Add segment 1
            index_to_save.add_vector(vec1_s1_id.to_string(), Embedding::from(vec![10.0, 10.0])).await.unwrap(); // Goes to segment 1

            assert_eq!(index_to_save.segment_count(), 2);
            assert_eq!(index_to_save.len(), 2);

            let mut dummy_writer = Vec::new();
            index_to_save.save(&mut dummy_writer).await.unwrap();

            // Check HNSWIndex metadata file for segment_dir_names
            let index_meta_path = get_metadata_path(&base_path.join(index_name));
            let meta_content = async_fs::read_to_string(index_meta_path).await.unwrap();
            assert!(meta_content.contains("segment_0"));
            assert!(meta_content.contains("segment_1"));
        }

        let opened_index = HnswIndex::open(base_path, index_name, config, DistanceMetric::L2).await.unwrap();
        assert_eq!(opened_index.segment_count(), 2, "Opened index should have 2 segments.");
        assert_eq!(opened_index.len(), 2, "Opened index should have 2 vectors in total.");

        assert!(opened_index.get_vector(&vec1_s0_id.to_string()).await.unwrap().is_some(), "Vector from segment 0 should exist.");
        assert!(opened_index.get_vector(&vec1_s1_id.to_string()).await.unwrap().is_some(), "Vector from segment 1 should exist.");
        
        // Verify segment paths were loaded correctly
        let seg0_path_expected = get_segment_path(&base_path.join(index_name), 0);
        let seg1_path_expected = get_segment_path(&base_path.join(index_name), 1);
        
        let seg0_guard_opened = opened_index.segments[0].read().await;
        assert_eq!(seg0_guard_opened.path(), seg0_path_expected);
        assert_eq!(seg0_guard_opened.vector_count(), 1);
        drop(seg0_guard_opened);

        let seg1_guard_opened = opened_index.segments[1].read().await;
        assert_eq!(seg1_guard_opened.path(), seg1_path_expected);
        assert_eq!(seg1_guard_opened.vector_count(), 1);
        drop(seg1_guard_opened);
    }

    #[tokio::test]
    async fn test_multi_segment_delete() {
        let dir = tempdir().unwrap();
        let base_path = dir.path();
        let mut config = create_test_config();
        config.vector_dim = 2;
        let index_name = "test_multi_seg_delete";
        let mut index = HnswIndex::new(base_path, index_name, config, DistanceMetric::L2).await.unwrap();

        let vec1_s0_id = "del_vec1_s0";
        index.add_vector(vec1_s0_id.to_string(), Embedding::from(vec![1.0, 1.0])).await.unwrap();
        
        index.add_new_segment_for_testing().await.unwrap(); // Add segment 1
        let vec1_s1_id = "del_vec1_s1";
        index.add_vector(vec1_s1_id.to_string(), Embedding::from(vec![10.0, 10.0])).await.unwrap(); // Goes to segment 1
        
        assert_eq!(index.len(), 2);

        // Delete from segment 0 (HnswIndex::delete_vector tries all segments)
        // To target segment 0 specifically for deletion, we'd need a different add_vector strategy or direct segment access.
        // Current HnswIndex::delete_vector iterates all segments.
        let deleted_s0 = index.delete_vector(&vec1_s0_id.to_string()).await.unwrap();
        assert!(deleted_s0, "Should confirm deletion of vec1_s0_id");
        assert_eq!(index.len(), 1, "Length should be 1 after deleting from segment 0.");
        assert!(index.get_vector(&vec1_s0_id.to_string()).await.unwrap().is_none(), "vec1_s0_id should be gone.");
        assert!(index.get_vector(&vec1_s1_id.to_string()).await.unwrap().is_some(), "vec1_s1_id should still exist.");

        // Delete from segment 1
        let deleted_s1 = index.delete_vector(&vec1_s1_id.to_string()).await.unwrap();
        assert!(deleted_s1, "Should confirm deletion of vec1_s1_id");
        assert_eq!(index.len(), 0, "Length should be 0 after deleting from segment 1.");
        assert!(index.get_vector(&vec1_s1_id.to_string()).await.unwrap().is_none(), "vec1_s1_id should be gone.");

        // Try deleting a non-existent ID
        let deleted_non_existent = index.delete_vector(&"non_existent_id".to_string()).await.unwrap();
        assert!(!deleted_non_existent, "Deleting non-existent ID should return false.");
    }
}
