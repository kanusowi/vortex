use crate::state::AppState;
use crate::error::ServerResult; 
use vortex_core::VortexError; // Added import
use std::fs::{self, File};
use std::io::{BufWriter, BufReader};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::RwLock; // Not strictly needed here if AppState is passed by ref
use tracing::{info, warn, error, debug};
use vortex_core::{HnswIndex, Index}; // Index trait for HnswIndex methods

const HNSW_INDEX_FILE_EXT: &str = "vortex";
const METADATA_FILE_EXT: &str = "meta.json";

fn get_hnsw_file_path(dir: &Path, index_name: &str) -> PathBuf {
    dir.join(format!("{}.{}", index_name, HNSW_INDEX_FILE_EXT))
}

fn get_metadata_file_path(dir: &Path, index_name: &str) -> PathBuf {
    dir.join(format!("{}.{}", index_name, METADATA_FILE_EXT))
}

/// Saves a single index (both HNSW data and metadata) to disk.
pub async fn save_index(index_name: &str, app_state: &AppState, persistence_path: &Path) -> ServerResult<()> {
    info!(index_name, path = ?persistence_path, "Attempting to save index");

    // Ensure persistence directory exists
    fs::create_dir_all(persistence_path)
        .map_err(|e| VortexError::IoError { path: persistence_path.to_path_buf(), source: e })?; // Re-map to VortexError or a new PersistenceError

    // 1. Save HNSW Index Data
    let hnsw_file_path = get_hnsw_file_path(persistence_path, index_name);
    {
        let indices_map = app_state.indices.read().await; // Still need read lock on map to get the Arc
        if let Some(index_arc_lock) = indices_map.get(index_name) {
            let mut index_guard = index_arc_lock.write().await; // Acquire write lock on the specific index
            debug!(file_path = ?hnsw_file_path, "Saving HNSW data for index (this now includes metadata)");
            // The HnswIndex::save method now handles its own file creation and metadata.
            // The writer argument in the trait is becoming problematic for mmap.
            // For now, we pass a dummy writer as the trait requires it.
            // This part of the trait/impl might need a redesign if the writer is truly unused by mmap.
            let mut dummy_writer = Vec::new(); // Dummy writer
            index_guard.save(&mut dummy_writer).await
                .map_err(|e| {
                    error!(index_name, error = ?e, "Failed to save HNSW data and metadata");
                    e // Return VortexError
                })?;
            info!(index_name, file_path = ?hnsw_file_path, "HNSW data and metadata saved successfully via HnswIndex::save");
        } else {
            warn!(index_name, "Index not found in memory, cannot save.");
            return Err(crate::error::ServerError::IndexNotFound(index_name.to_string()));
        }
    }

    // 2. Save Metadata (This section is now handled by HnswIndex::save directly)
    // We can remove the separate metadata saving logic here if HnswIndex::save covers it.
    // For now, let's assume HnswIndex::save handles the .hnsw_meta.json file.
    // The old metadata logic for server-specific metadata (if any) might still be needed.
    // The current server metadata is just a copy of vector metadata, which is not what HnswIndex::save persists.
    // The server's metadata_store seems to be for payload, which is separate.
    // Let's keep the payload metadata saving logic.
    let metadata_file_path = get_metadata_file_path(persistence_path, index_name);
    {
        let metadata_store_guard = app_state.metadata_store.read().await;
        if let Some(index_metadata_map) = metadata_store_guard.get(index_name) {
            if !index_metadata_map.is_empty() {
                debug!(file_path = ?metadata_file_path, "Saving metadata for index");
                let file = File::create(&metadata_file_path)
                    .map_err(|e| VortexError::IoError { path: metadata_file_path.clone(), source: e })?;
                serde_json::to_writer_pretty(BufWriter::new(file), index_metadata_map)
                    .map_err(|e| {
                        error!(index_name, error = ?e, "Failed to serialize metadata");
                        VortexError::Serialization(format!("Failed to serialize metadata for {}: {}", index_name, e))
                    })?;
                info!(index_name, file_path = ?metadata_file_path, "Metadata saved successfully");
            } else {
                debug!(index_name, "No metadata to save for this index. Skipping metadata file creation.");
                // Optionally remove old metadata file if it exists and current metadata is empty
                if metadata_file_path.exists() {
                    fs::remove_file(&metadata_file_path).map_err(|e| VortexError::IoError { path: metadata_file_path.clone(), source: e})?;
                    debug!(index_name, file_path = ?metadata_file_path, "Removed empty/stale metadata file.");
                }
            }
        } else {
            debug!(index_name, "Index not found in metadata store, or no metadata present. Skipping metadata save.");
             if metadata_file_path.exists() {
                fs::remove_file(&metadata_file_path).map_err(|e| VortexError::IoError { path: metadata_file_path.clone(), source: e})?;
                debug!(index_name, file_path = ?metadata_file_path, "Removed potentially stale metadata file as index is not in metadata store.");
            }
        }
    }
    Ok(())
}


/// Saves all indices currently in memory to disk.
pub async fn save_all_indices(app_state: &AppState, persistence_path: &Path) {
    info!(path = ?persistence_path, "Attempting to save all indices...");
    let index_names: Vec<String> = {
        let indices_map = app_state.indices.read().await;
        indices_map.keys().cloned().collect()
    };

    let mut saved_count = 0;
    let mut error_count = 0;

    for index_name in index_names {
        match save_index(&index_name, app_state, persistence_path).await {
            Ok(_) => {
                info!(index_name, "Successfully saved index.");
                saved_count += 1;
            }
            Err(e) => {
                error!(index_name, error = ?e, "Failed to save index.");
                error_count += 1;
            }
        }
    }
    info!(saved_count, error_count, "Finished saving all indices.");
}

/// Loads all indices from the specified persistence path into the AppState.
pub async fn load_all_indices_on_startup(app_state: &AppState, persistence_path: &Path) {
    info!(path = ?persistence_path, "Attempting to load indices on startup...");
    if !persistence_path.exists() || !persistence_path.is_dir() {
        info!(path = ?persistence_path, "Persistence directory does not exist or is not a directory. No indices to load.");
        return;
    }

    let mut loaded_count = 0;
    let mut error_count = 0;

    match fs::read_dir(persistence_path) {
        Ok(entries) => {
            let mut indices_map_guard = app_state.indices.write().await;
            let mut metadata_store_guard = app_state.metadata_store.write().await;

            for entry_result in entries {
                match entry_result {
                    Ok(entry) => {
                        let path = entry.path();
                        if path.is_file() && path.extension().map_or(false, |ext| ext == HNSW_INDEX_FILE_EXT) {
                            if let Some(file_stem) = path.file_stem().and_then(|s| s.to_str()) {
                                let index_name = file_stem.to_string();
                                info!(index_name, file_path = ?path, "Found potential index file. Attempting to load.");

                                // TODO: HnswConfig and DistanceMetric should be persisted and loaded per index.
                                // Using defaults for now, which might not match the original index settings.
                                // This needs to be addressed by saving config/metric, perhaps in the .hnsw_meta.json.
                                let default_config = vortex_core::HnswConfig::default(); 
                                let default_metric = vortex_core::DistanceMetric::Cosine; // Or another sensible default
                                warn!(index_name, "Loading index with default HnswConfig and DistanceMetric as they are not yet persisted.");

                                // Load HNSW data using HnswIndex::open
                                // Note: HnswIndex::open expects base_path and name separately.
                                // `path` here is persistence_path/index_name.vortex
                                // `persistence_path` is the directory. `index_name` is the stem.
                                match HnswIndex::open(persistence_path, &index_name, default_config, default_metric) {
                                    Ok(hnsw_index) => {
                                        indices_map_guard.insert(index_name.clone(), Arc::new(RwLock::new(hnsw_index)));
                                        
                                        // Load corresponding server-level payload metadata
                                        let metadata_file_path = get_metadata_file_path(persistence_path, &index_name);
                                        if metadata_file_path.exists() {
                                            match File::open(&metadata_file_path) {
                                                Ok(file) => {
                                                    match serde_json::from_reader(BufReader::new(file)) {
                                                        Ok(metadata_map) => {
                                                            metadata_store_guard.insert(index_name.clone(), metadata_map);
                                                            info!(index_name, "Successfully loaded HNSW data and metadata.");
                                                            loaded_count += 1;
                                                        }
                                                        Err(e) => {
                                                            error!(index_name, file_path = ?metadata_file_path, error = ?e, "Failed to deserialize metadata. Index loaded without metadata.");
                                                            // Index is loaded, but metadata isn't. This is a partial success.
                                                            // We could choose to not load the index at all if metadata is corrupt.
                                                            // For now, log error and continue.
                                                            loaded_count += 1; // Count HNSW load as success for now
                                                            error_count +=1; // Count metadata load as error
                                                        }
                                                    }
                                                }
                                                Err(e) => {
                                                    warn!(index_name, file_path = ?metadata_file_path, error = ?e, "Failed to open metadata file. Index loaded without metadata.");
                                                    loaded_count += 1; // HNSW loaded
                                                }
                                            }
                                        } else {
                                            info!(index_name, "No metadata file found. Index loaded without metadata.");
                                            loaded_count += 1;
                                        }
                                    }
                                    Err(e) => {
                                        error!(index_name, file_path = ?path, error = ?e, "Failed to load HNSW index data.");
                                        error_count += 1;
                                    }
                                }
                            }
                        }
                    }
                    Err(e) => {
                        error!(path = ?persistence_path, error = ?e, "Error reading directory entry.");
                        error_count += 1;
                    }
                }
            }
        }
        Err(e) => {
            error!(path = ?persistence_path, error = ?e, "Failed to read persistence directory.");
            // No indices loaded if directory can't be read.
        }
    }
    info!(loaded_count, error_count, "Finished loading indices from disk.");
}

// Removed manual From<VortexError> for ServerError implementation,
// as it's handled by #[from] in ServerError::CoreError(VortexError)
